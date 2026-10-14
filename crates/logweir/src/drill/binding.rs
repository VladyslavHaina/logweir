//! Execution contract **v2**'s two pre-data-plane bindings: the recovery
//! point a plan is bound to (decision D3 §5.5) and the standing rehearsal
//! authorization's signed scope (D3 §4.3).
//!
//! # Not a phase, and that is the point
//!
//! Every check here runs BEFORE `drill::context` — before the rdkafka client
//! is constructed, therefore before a single bootstrap connection is opened,
//! and before any object under `logweir/` is written. The only I/O either
//! check performs is a read of the archive the plan already names, through a
//! read-only handle that `Store::put_create_only` refuses to write through at
//! all. That ordering is what lets these refusals be exit 3 under Global
//! Constraint 11 ("refused by a guard, BEFORE anything runs") rather than a
//! discovery made after a restore had begun.
//!
//! # Why the runner repeats what the controller already proved
//!
//! The controller's check (D3 §4.3, §5.5) is a statement about objects in the
//! API server, and the runner's about the bytes in this pod: a substitution
//! between the two (projected volume replaced, ConfigMap rewritten, archive
//! object swapped) is invisible to the first check and fatal to the second.

use crate::catalog::pin::{self, PinVerdict};
use crate::drill::DrillError;
use logweir_core::execution_contract::{self as wire, PointBinding};
use logweir_core::guard::GuardRefusal;
use logweir_core::spec::DrillSpec;
use logweir_engine_oso::storage::{caps, Store, StoreError};
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
};

/// The prose a point-binding refusal opens with.
///
/// `logweir_core::guard::TERMINAL_STATES` does not list it, so the refusal
/// classifies as the general `GuardRefused` and carries this token in the
/// message, where an operator and a log search find it by name.
pub const POINT_BINDING_MISMATCH: &str = "PointBindingMismatch";
/// The same arrangement for D3 §4.3's scope refusal.
pub const REHEARSAL_SCOPE_VIOLATION: &str = "RehearsalScopeViolation";
/// The token a bound point whose receipt is not signed by a key this run
/// trusts opens with (D3 §5.5 step 6):
/// no evidence keyring mounted, no signature sidecar in the archive, a
/// signature under no mounted key, or a key [`logweir_core::trust::decide`]
/// refuses (revoked, used outside its validity, wrong usage). Exit 3, like
/// [`POINT_BINDING_MISMATCH`]: it is a refusal of the PLAN's point, and no
/// retry makes it trustworthy.
pub const POINT_UNTRUSTED: &str = "PointUntrusted";
/// The token the runner's NOTE opens with when a bound point's
/// receipt pins a manifest version this bucket does not hold — a copy of the
/// archive, a bucket without versioning, or a version that was expired or
/// deleted. Never a refusal: the point is then proven by its digest, as a
/// point without a pin always was, and that digest cannot see segments
/// rewritten under an identical manifest ([`crate::catalog::pin`], "The cost,
/// stated").
pub const POINT_PIN_UNCHECKED: &str = "PointPinUnchecked";
/// The token a refusal opens with when the bound point HOLDS — its
/// receipt, signature and manifest all verify — but the backup set this run
/// would restore is not the set that receipt describes: a plan whose
/// `source.backup` names another set (or `latestCompleted`), or a restored set
/// whose id, manifest key, manifest digest or manifest version is not the one
/// the binding verified. Exit 3, in the same arrangement as
/// [`POINT_BINDING_MISMATCH`] (the message opens with it; the line reads
/// `refusal-reason=GuardRefused`): it is a refusal of the PLAN, and no retry
/// makes it restore the point it was approved for.
pub const POINT_BINDING_SET_MISMATCH: &str = "PointBindingSetMismatch";

fn refuse(message: String) -> DrillError {
    DrillError::Guard(GuardRefusal(message))
}

/// What a VERIFIED recovery-point binding established, beyond "it holds", and
/// what the runner logs beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPoint {
    /// The point this run proved, for the log line — re-derived from the
    /// receipt bytes.
    pub point_id: String,
    /// The bound receipt's per-topic configuration capture coverage,
    /// read from bytes whose digest, identity, signature and manifest were all
    /// checked above it — so phases 3 and 7 may compare a restored topic's
    /// configuration against the manifest's record exactly where this says the
    /// record was `captured`, and say `notAssessed` everywhere else. A receipt
    /// that predates format 1.1.0 carries no block and reads UNKNOWN for every
    /// topic.
    pub config_coverage: logweir_core::backup_receipt::SourceConfigCoverage,
    /// Set when the receipt's pin could not be checked in this
    /// bucket ([`POINT_PIN_UNCHECKED`], then [`crate::catalog::pin::UNCHECKED_NOTE`]).
    /// The runner logs it as a warning and goes on.
    pub pin_note: Option<String>,
    /// The backup set the verified receipt describes, which
    /// [`check_restored_set`] compares with the set the engine is about to
    /// restore before anything is decided from this receipt.
    pub set: BoundSet,
    /// The verified receipt's `source.cluster_id` — read from
    /// the broker at backup time, never from a spec — which an original-name
    /// restore compares with its target's (condition 3).
    pub source_cluster_id: String,
    /// The verified receipt's recorded declarative owners: where its backup
    /// looked, and each source topic's owner.
    pub owners: logweir_core::original_name::ReceiptOwners,
}

/// The owners a verified receipt recorded: its
/// `owner_detection` (absent reads as empty, "not checked") and each source
/// topic's `topic_configuration[t].owner`.
fn receipt_owners(
    receipt: &logweir_core::backup_receipt::BackupReceipt,
) -> logweir_core::original_name::ReceiptOwners {
    logweir_core::original_name::ReceiptOwners {
        owner_detection: receipt.owner_detection.clone().unwrap_or_default(),
        owners: receipt
            .topic_configuration
            .iter()
            .flatten()
            .filter_map(|(topic, model)| model.owner.clone().map(|owner| (topic.clone(), owner)))
            .collect(),
    }
}

/// The backup set a VERIFIED recovery point describes: the only set whose
/// records the receipt's word is about.
///
/// Everything the runner takes from the bound receipt (the configuration
/// capture coverage of phases 3 and 7, the recorded timestamp type of the
/// time basis, and the pin verdict, a judgement of ONE manifest object at ONE
/// version) is a statement about this set. [`check_restored_set`] refuses a
/// run whose restored set is any other, so none of it is ever applied to
/// another set's records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSet {
    /// The point, for the refusal message.
    pub point_id: String,
    /// The receipt's `backup_id`: the set id the engine must be told to
    /// restore (`build_plan` renders `BackupSetRef::backup_id`).
    pub backup_id: String,
    /// The receipt's `archive.manifest_key`: the key the binding read the
    /// manifest back through and judged the pin at (bucket-absolute, the
    /// space `list_manifests` returns keys in), and the key the engine reads
    /// this set at under the plan's storage (`Store::engine_manifest_key`),
    /// which `pick_backup_set` selects by.
    pub manifest_key: String,
    /// The bound `manifest_sha256`, which the binding proved that key's bytes
    /// hash to.
    pub manifest_sha256: String,
    /// The version id the binding's read of that manifest answered: `None` for
    /// a store that answers none (a filesystem archive, an unversioned
    /// bucket). The pin verdict is about the object AT this version.
    pub manifest_version_id: Option<String>,
}

/// Re-verify the recovery point the plan is bound to, against the archive.
///
/// `Ok(None)` when the plan carries no `source.point` — a v1-shaped plan, and
/// the only shape a pre-catalog archive can be restored from. `Ok(Some(point))`
/// names the point this run proved, for the log line, carries its verified
/// receipt's configuration capture coverage, and carries the pin note when
/// the receipt's pin could not be checked in this bucket.
///
/// # The answers, and why they are not one code
///
/// * **a receipt key outside the plan's own set** — exit 3 before any read
///   ([`confine_receipt_key`], FX-36).
/// * **unreadable** — exit 1. The archive did not answer. That may
///   be a rotated credential, a denied prefix or a bucket that is briefly
///   unavailable, and Global Constraint 11 reserves exit 3 for a refusal of
///   the PLAN. Recording a transient storage failure as "the plan was refused"
///   would tell an operator to change an approved document to fix an outage.
/// * **absent, digest mismatch or over the read cap** — exit 3, ONE answer
///   (FX-36, FX-14's rule): the archive does not hold the bytes the approver
///   signed a binding to, the tampered-bundle case exactly, and which of the
///   three it was is not said. Never retryable, never a warning.
/// * **agreement** — the run continues, having touched no broker.
/// * **agreement over a pin this bucket cannot check**: the receipt
///   pins a manifest version this bucket does not hold, as every copy of the
///   archive does; the digest decided, the run continues, and the runner logs
///   [`POINT_PIN_UNCHECKED`]. A pinned version this bucket HOLDS and no longer
///   serves is exit 3, and one that cannot be read is exit 1
///   ([`crate::catalog::pin`]).
pub fn verify_point_binding(
    plan: &DrillSpec,
    archive: &Store,
    evidence_keys: Option<&[u8]>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<VerifiedPoint>, DrillError> {
    let Some(point) = plan.source.point.as_ref() else {
        return Ok(None);
    };
    check_point_shape(point)?;
    // FX-36: WHAT THE PLAN MAY MAKE THIS RUN READ, decided from the plan
    // alone and before any store call: one receipt of the plan's own set.
    confine_receipt_key(plan, point)?;
    // THE TRUST ANCHOR BEFORE ANY ARCHIVE READ. A point-bound plan with no
    // evidence keyring could only be run unverified, and that is the run D3
    // §5.5 step 6 exists to prevent.
    let keyring = evidence_keyring(evidence_keys, &point.point_id)?;

    // The key is bucket-absolute and is not qualified against the store's
    // prefix, like every `logweir/` key (`Store::qualify`: the prefix is a
    // listing root). Qualifying it would report every present point as
    // missing on a destination whose archive sits under a prefix.
    let qualified = point.receipt_key.clone();
    let receipt_bytes = match archive.get_capped(&qualified, caps::SIGNED_DOCUMENT) {
        Ok((bytes, _)) => bytes,
        // FX-36: ABSENT and OVER THE CAP are the answer of "not these bytes":
        // one refusal, one message, one read (FX-14's rule, the restore
        // preflight's `judge_bound_point`).
        Err(StoreError::NotFound(_) | StoreError::TooLarge { .. }) => {
            return Err(receipt_not_held(point))
        }
        Err(error) => {
            return Err(DrillError::Operational(format!(
                "the plan is bound to recovery point {} and its receipt {qualified} could not be \
                 read: {error}; no data operation was started",
                point.point_id
            )))
        }
    };

    // **The digest first, before the bytes are parsed or believed.** Everything
    // read out of the receipt below is trustworthy only because these bytes are
    // the bytes the approval covers: the plan is signed, the plan names this
    // digest, and this line proves the archive's object hashes to it. Parsing
    // first and comparing afterwards would act on unverified bytes. The
    // refusal is the one an absent receipt gets, and names no digest of the
    // bytes found.
    if logweir_core::ids::sha256_prefixed(&receipt_bytes) != point.receipt_sha256 {
        return Err(receipt_not_held(point));
    }
    // The identity is content-derived (D3 §5.1), so it is RE-DERIVED here and
    // never taken from the document. A point id that had to be believed would
    // be a label anyone could relabel.
    let derived = crate::catalog::record::point_id(&receipt_bytes);
    if derived != point.point_id {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. The receipt at {qualified} derives recovery point id \
             {derived}, but the plan is bound to {}; no data operation was started.",
            point.point_id
        )));
    }
    let receipt: logweir_core::backup_receipt::BackupReceipt =
        serde_json::from_slice(&receipt_bytes).map_err(|error| {
            // The digest already matched, so these are the approved bytes and
            // the fault is in the PLAN, not in the archive: a binding was
            // approved to an object that is not a backup receipt. Exit 3.
            refuse(format!(
                "{POINT_BINDING_MISMATCH}. The bytes bound as recovery point {} are not a backup \
                 receipt: {error}; no data operation was started.",
                point.point_id
            ))
        })?;
    if receipt.archive.manifest_sha256 != point.manifest_sha256 {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {}'s receipt attests manifest digest {}, \
             but the plan is bound to {}; no data operation was started.",
            point.point_id, receipt.archive.manifest_sha256, point.manifest_sha256
        )));
    }
    // The plan names the point's own set: `source.backup` is what
    // `pick_backup_set` restores, and the receipt describes one set, so a plan
    // binding point A while naming set B would restore B and judge it by A's
    // record. The controller's readiness check refuses that plan
    // (`CatalogPointBindingMismatch`); this is the runner's half, before any
    // broker is contacted. `latestCompleted` is refused too: it can agree
    // with the point today and stop agreeing after the next backup.
    if plan.source.backup != receipt.backup_id {
        return Err(refuse(plan_set_refusal(
            &point.point_id,
            &receipt.backup_id,
            &plan.source.backup,
        )));
    }
    // The object the engine will read is the one the receipt attests: the
    // engine is told only the plan's storage and the set id and loads
    // `<prefix>/<backup_id>/manifest.json` (`Store::engine_manifest_key`).
    // Unless that is the receipt's key, the restore reads an object the point
    // does not describe (a parent prefix with another same-id set, a copy
    // under another prefix). `pick_backup_set` selects by this same key.
    let engine_key = archive.engine_manifest_key(&receipt.backup_id);
    if receipt.archive.manifest_key != engine_key {
        return Err(refuse(engine_key_refusal(
            &point.point_id,
            &receipt.backup_id,
            &receipt.archive.manifest_key,
            &engine_key,
        )));
    }

    // **THE SIGNATURE, UNDER THE MOUNTED TRUST, BEFORE ANYTHING IS READ ON
    // THE RECEIPT'S WORD** (D3 §5.5 step 6). The digest above proves these are
    // the bytes the approver bound; this proves a key the namespace trusts for
    // `EvidenceSigning` signed them, under the trust the controller rendered
    // for THIS run -- so a key revoked after the catalog last synced is
    // refused here, whatever the catalog's view said.
    // It comes before the manifest read because the manifest key is one of the
    // receipt's own claims.
    verify_receipt_signature(point, &qualified, &receipt_bytes, archive, &keyring, now)?;

    // **And the manifest the receipt names, read back.** The three checks
    // above prove the plan and the receipt agree; this one proves the ARCHIVE
    // does. Without it a point whose receipt is intact and whose manifest was
    // replaced or deleted passes every digest comparison and fails at phase 4,
    // after the broker has been contacted — which is the whole class of
    // failure this binding exists to move earlier.
    // `receipt.archive.manifest_key` is likewise the key the backup runner
    // read the manifest back through (`backup::phase_run::run` calls
    // `store.get_capped(&set.manifest_key, caps::MANIFEST)` on the value `list_manifests` returned),
    // so it is already in the same space.
    let manifest_key = receipt.archive.manifest_key.clone();
    let (manifest_bytes, current_version) = match archive.get_capped(&manifest_key, caps::MANIFEST)
    {
        Ok(read) => read,
        Err(StoreError::NotFound(_)) => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s manifest {manifest_key} is not in the archive; no data \
                 operation was started",
                point.point_id
            )))
        }
        Err(error) => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s manifest {manifest_key} could not be read: {error}; no data \
                 operation was started",
                point.point_id
            )))
        }
    };
    // The pin, judged in this bucket ([`pin::judge`], shared with
    // the catalog's deep check). A receipt taken on a versioned bucket names
    // the version of the manifest it attests. The engine restores from the
    // key's CURRENT version and knows no other, and the digest below cannot
    // always see a rewrite: engine 0.21.0 re-running over a set rewrites its
    // segments in place and can leave the manifest bytes IDENTICAL, with a new
    // version id. But a version id belongs to one bucket, and the same signed
    // point COPIED to another bucket carries the pin and not the version. So
    // when the current version is not the pin, the pinned version is read BY
    // ID: held here and not current → the set was written again HERE, exit 3;
    // not this bucket's history → the pin cannot be checked here, the digest
    // decides and the runner logs a note; any other failure → exit 1, the
    // archive did not answer.
    let mut pin_note = None;
    match pin::judge(
        receipt.archive.manifest_version_id.as_deref(),
        current_version.as_deref(),
        &point.manifest_sha256,
        |version| {
            archive
                .get_version_capped(&manifest_key, version, caps::MANIFEST)
                .map(|(bytes, _)| bytes)
        },
    ) {
        PinVerdict::Unpinned | PinVerdict::Current => {}
        PinVerdict::Superseded {
            pinned,
            current,
            retained_matches,
        } => {
            return Err(refuse(superseded_refusal(
                &point.point_id,
                &manifest_key,
                &pinned,
                current.as_deref(),
                retained_matches,
            )));
        }
        PinVerdict::Unchecked { pinned } => {
            pin_note = Some(format!(
                "{POINT_PIN_UNCHECKED}. Recovery point {}'s manifest {manifest_key} (pinned \
                 version {pinned}): {}",
                point.point_id,
                pin::UNCHECKED_NOTE
            ));
        }
        PinVerdict::Unreadable { pinned, error } => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s manifest {manifest_key} could not be read at the version \
                 its receipt pins ({pinned}): {error}. Whether the set was written again after \
                 the point was signed cannot be told (a 403 here is a principal without \
                 s3:GetObjectVersion on the archive); no data operation was started",
                point.point_id
            )));
        }
    }
    let manifest_digest = logweir_core::ids::sha256_prefixed(&manifest_bytes);
    if manifest_digest != point.manifest_sha256 {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {}'s manifest {manifest_key} hashes to \
             {manifest_digest}, not the bound {}; no data operation was started.{}",
            point.point_id,
            point.manifest_sha256,
            pin_note
                .as_deref()
                .map(|note| format!(" {note}"))
                .unwrap_or_default()
        )));
    }
    Ok(Some(VerifiedPoint {
        point_id: point.point_id.clone(),
        config_coverage: verified_coverage(&point.point_id, &receipt),
        pin_note,
        set: BoundSet {
            point_id: point.point_id.clone(),
            backup_id: receipt.backup_id.clone(),
            manifest_key,
            manifest_sha256: manifest_digest,
            manifest_version_id: current_version,
        },
        source_cluster_id: receipt.source.cluster_id.clone(),
        owners: receipt_owners(&receipt),
    }))
}

/// **FX-36: the receipt key a plan names, confined before any read.** FX-14's
/// confinement of the restore preflight (`check::kinds::restore`,
/// `confine_bound_point`), where the approved plan is executed.
///
/// `source.point.receipt_key` is text an approver signed, and a runner that
/// fetched whatever it names would turn the namespace's archive credential
/// into a probe of the whole bucket: does this object exist, do its bytes hash
/// to the bound digest. In a bucket shared by prefix that reaches other
/// tenants' objects. So the key must be, byte for byte, the key `backup run`
/// writes the PLAN'S OWN set's receipt at
/// (`logweir/backups/<source.backup>/<run id>.receipt.json`,
/// [`crate::catalog::record::receipt_run_id`]); the signature sidecar is
/// derived from it, and the manifest is read only at the key the engine reads
/// under the plan's storage, so every object this binding reads is confined
/// by this one check.
///
/// Two refusals, both exit 3 and both before any store call:
/// * the key is a receipt of ANOTHER set: the plan binds a point of one set
///   and restores another ([`POINT_BINDING_SET_MISMATCH`], the refusal the
///   binding gave after the read before FX-36, with the same text);
/// * anything else (another prefix, a relative or nested path, an object
///   that is not a receipt): [`POINT_BINDING_MISMATCH`], saying it was not
///   read.
pub(crate) fn confine_receipt_key(
    plan: &DrillSpec,
    point: &PointBinding,
) -> Result<(), DrillError> {
    use crate::catalog::record::{receipt_set_and_run, RECEIPTS_PREFIX, RECEIPT_SUFFIX};
    let set = plan.source.backup.as_str();
    let key = point.receipt_key.as_str();
    match receipt_set_and_run(key) {
        Some((own, _)) if own == set => return Ok(()),
        Some((other, _)) => return Err(refuse(plan_set_refusal(&point.point_id, other, set))),
        None => {}
    }
    Err(refuse(format!(
        "{POINT_BINDING_MISMATCH}. The plan's source.point.receipt_key `{key}` is not a receipt \
         of set `{set}` (`{RECEIPTS_PREFIX}{set}/<run id>{RECEIPT_SUFFIX}`); it was not read, and \
         no data operation was started."
    )))
}

/// **FX-36: one answer for "absent", "not these bytes" and "over the read
/// cap"** (FX-14's rule). Either way the archive does not hold the point the
/// plan was approved for, and a difference would tell the plan's author
/// whether an object exists at a key they chose. Exit 3: retention or a
/// rewrite removed the point, and no retry brings it back. A store FAILURE
/// keeps its own answer (exit 1).
fn receipt_not_held(point: &PointBinding) -> DrillError {
    refuse(format!(
        "{POINT_BINDING_MISMATCH}. This archive does not hold the receipt the plan binds as \
         recovery point {} at {}: it is absent, its bytes do not hash to the bound {}, or it is \
         larger than the {}-byte read cap for a receipt; no data operation was started.",
        point.point_id,
        point.receipt_key,
        point.receipt_sha256,
        caps::SIGNED_DOCUMENT
    ))
}

/// The binding's refusal of a plan whose `source.backup` is not the
/// bound point's set.
fn plan_set_refusal(point_id: &str, receipt_backup_id: &str, plan_backup: &str) -> String {
    let named = if plan_backup == "latestCompleted" {
        "`latestCompleted`, which restores whichever set is newest when the run starts".to_string()
    } else {
        format!("set `{plan_backup}`")
    };
    format!(
        "{POINT_BINDING_SET_MISMATCH}. The plan is bound to recovery point {point_id} of backup \
         set `{receipt_backup_id}`, but its source.backup names {named}. The point's receipt \
         describes `{receipt_backup_id}` alone, so nothing it records may be applied to another \
         set's records; a bound plan names its point's own set (source.backup: \
         {receipt_backup_id}). No data operation was started."
    )
}

/// The binding's refusal of a plan under
/// whose storage the engine would read the point's set somewhere other than
/// the manifest the receipt attests. The repair names the prefix the set was
/// written under when the receipt's key has the engine's shape.
fn engine_key_refusal(
    point_id: &str,
    backup_id: &str,
    receipt_key: &str,
    engine_key: &str,
) -> String {
    let written_under = receipt_key
        .strip_suffix(&format!("{backup_id}/manifest.json"))
        .map(|p| p.trim_end_matches('/'));
    let repair = match written_under {
        Some("") => {
            "point source.storage at the bucket root, where the set was written".to_string()
        }
        Some(prefix) => {
            format!("point source.storage at the prefix the set was written under ({prefix})")
        }
        None => format!(
            "no plan storage makes the engine read it, because the receipt's manifest key is \
             not `<prefix>/{backup_id}/manifest.json`"
        ),
    };
    format!(
        "{POINT_BINDING_SET_MISMATCH}. Recovery point {point_id}'s receipt attests set \
         `{backup_id}`'s manifest at {receipt_key}, but under this plan's source.storage the \
         engine would read set `{backup_id}` at {engine_key}: the restore would read an object \
         the point does not describe. To restore this point, {repair}. No data operation was \
         started."
    )
}

/// The set the engine is about to restore is the set the bound receipt
/// describes. Called by the drill immediately after `describe`, before the
/// time basis is decided and before phase 2, so a refused run has created no
/// target topic of this restore and has taken no decision from the receipt.
///
/// It is the engine's set: the binding proved the receipt's manifest key is
/// the key the engine reads under this plan (`Store::engine_manifest_key`),
/// and `pick_backup_set` selects a bound plan's set by that exact key.
///
/// `bound` is `None` for a plan bound to no point (nothing to compare: the set
/// is the one `source.backup` names, as it always was). Otherwise every one of
/// the four must agree, and each closes a different way the restored set can
/// differ from the verified one:
///
/// * **the set id** (`set.backup_id`, what `build_plan` renders for the
///   engine) against the receipt's `backup_id` — `latestCompleted` resolving
///   to a newer set, if a plan ever reached here with it;
/// * **the manifest key** against the receipt's `archive.manifest_key` — the
///   plan's storage prefix pointing at another copy of a set with the same id
///   (the pin verdict was taken at the receipt's key, not at this one);
/// * **the manifest digest** `describe` computed against the bound
///   `manifest_sha256` — different bytes under the same id and key;
/// * **the manifest version** `describe` read against the version the binding
///   read — the manifest written again between the binding and `describe`,
///   with bytes that may be identical, inside this run.
///
/// Exit 3 ([`POINT_BINDING_SET_MISMATCH`]) naming every disagreement.
pub fn check_restored_set(
    bound: Option<&BoundSet>,
    set: &logweir_core::engine::BackupSetRef,
    facts: &logweir_core::engine::BackupSetFacts,
) -> Result<(), DrillError> {
    let Some(bound) = bound else {
        return Ok(());
    };
    let mut faults = Vec::new();
    if set.backup_id != bound.backup_id {
        faults.push(format!(
            "it is backup set `{}`, not the receipt's `{}`",
            set.backup_id, bound.backup_id
        ));
    }
    if set.manifest_key != bound.manifest_key {
        faults.push(format!(
            "its manifest is {}, not the receipt's {} (the manifest the binding verified)",
            set.manifest_key, bound.manifest_key
        ));
    }
    if facts.manifest_sha256 != bound.manifest_sha256 {
        faults.push(format!(
            "its manifest hashes to {}, not the bound {}",
            facts.manifest_sha256, bound.manifest_sha256
        ));
    }
    if facts.manifest_version_id != bound.manifest_version_id {
        let said = |v: &Option<String>| match v {
            Some(version) => format!("version {version}"),
            None => "no version id".to_string(),
        };
        faults.push(format!(
            "its manifest read answered {}, and the binding's read answered {}: the manifest was \
             written again after the point was verified",
            said(&facts.manifest_version_id),
            said(&bound.manifest_version_id)
        ));
    }
    if faults.is_empty() {
        return Ok(());
    }
    Err(refuse(format!(
        "{POINT_BINDING_SET_MISMATCH}. The plan is bound to recovery point {} of backup set `{}`, \
         but the set this run would restore is not that set: {}. Everything this run would take \
         from the point's receipt (its configuration capture coverage and recorded timestamp \
         types, its manifest pin) describes `{}` alone. No target topic of this restore was \
         created and no data operation was started.",
        bound.point_id,
        bound.backup_id,
        faults.join("; "),
        bound.backup_id
    )))
}

/// The coverage a verified point lends phases 3 and 7 — read only
/// after every check in [`verify_point_binding`], so these bytes are the
/// approved ones, signed by a trusted key, over the manifest the archive
/// holds. A signature proves who wrote them, not that they are coherent: a
/// receipt that contradicts itself (`validate_invariants`, the eleven arms
/// both verifiers enforce — e.g. a 1.0.x document carrying `config_coverage`,
/// arm 6) lends NOTHING, and its coverage reads UNKNOWN rather than being
/// believed. The restore itself is not refused on that ground:
/// the binding's own checks all passed, and UNKNOWN only makes parity report
/// `notAssessed`.
fn verified_coverage(
    point_id: &str,
    receipt: &logweir_core::backup_receipt::BackupReceipt,
) -> logweir_core::backup_receipt::SourceConfigCoverage {
    match receipt.validate_invariants() {
        Ok(()) => logweir_core::backup_receipt::SourceConfigCoverage::from_receipt(receipt),
        Err(why) => {
            tracing::warn!(
                point_id,
                why = %why,
                "the bound point's receipt contradicts itself; its configuration capture \
                 coverage is read as UNKNOWN"
            );
            logweir_core::backup_receipt::SourceConfigCoverage::unknown()
        }
    }
}

/// The binding's refusal for a pinned version this bucket HOLDS and
/// no longer serves ([`PinVerdict::Superseded`]): the one cause
/// ([`pin::SUPERSEDED_CAUSE`]), and what the current read answered, AS
/// OBSERVED. A current read answered with no version id is not "an
/// unversioned current version": it may be suspended versioning's `null`,
/// or a proxy that strips the header, so it is said as
/// seen. Fail-closed either way.
fn superseded_refusal(
    point_id: &str,
    manifest_key: &str,
    pinned: &str,
    current: Option<&str>,
    retained_matches: bool,
) -> String {
    let retained = if retained_matches {
        "is still retained at that version, but a restore reads only the current one"
    } else {
        "is at that version but no longer hashes to the attested digest"
    };
    let current = match current {
        Some(version) => format!("the current version is {version}"),
        None => "the current read answered no version id".to_string(),
    };
    format!(
        "{POINT_BINDING_MISMATCH}. Recovery point {point_id}'s manifest {manifest_key} {} (the \
         point pins version {pinned}; {current}). The backup set may have been rewritten under \
         it (its segments are rewritten in place), and the manifest the point attests \
         {retained}; no data operation was started.",
        pin::SUPERSEDED_CAUSE,
    )
}

/// The mounted evidence keyring, parsed -- or the refusal that there is none.
fn evidence_keyring(
    bytes: Option<&[u8]>,
    point_id: &str,
) -> Result<wire::EvidenceKeyring, DrillError> {
    let Some(bytes) = bytes else {
        return Err(refuse(format!(
            "{POINT_UNTRUSTED}. The plan is bound to recovery point {point_id}, and no \
             evidence-signing keyring (--evidence-keys) is mounted, so the receipt's signature \
             cannot be checked against anything this installation trusts; a point-bound plan is \
             never run unverified; no data operation was started."
        )));
    };
    let keyring: wire::EvidenceKeyring = serde_json::from_slice(bytes).map_err(|error| {
        DrillError::Operational(format!(
            "the mounted evidence-signing keyring does not parse: {error}"
        ))
    })?;
    if keyring.format_version != wire::EVIDENCE_KEYRING_FORMAT_VERSION {
        return Err(DrillError::Operational(format!(
            "the mounted evidence-signing keyring is format {:?}; this build reads {:?}",
            keyring.format_version,
            wire::EVIDENCE_KEYRING_FORMAT_VERSION
        )));
    }
    if keyring.keys.is_empty() {
        return Err(refuse(format!(
            "{POINT_UNTRUSTED}. The mounted evidence-signing keyring holds no key, so recovery \
             point {point_id}'s receipt signature could anchor in nothing; no data operation was \
             started."
        )));
    }
    Ok(keyring)
}

/// Verify the bound receipt's DSSE sidecar under the mounted keyring, then
/// judge the key that verified with [`logweir_core::trust::decide`] for
/// `EvidenceSigning` against the receipt's own claimed signing time -- the
/// verifier `logweir drill verify` runs (`verify_detached` under
/// `PAYLOAD_TYPE_BACKUP_RECEIPT`) and the lifecycle rule the controller
/// applies to Backup evidence.
///
/// # The answers
///
/// * no sidecar in the archive -- an UNSIGNED receipt: exit 3 `PointUntrusted`;
/// * the sidecar could not be read for another reason: exit 1, the archive did
///   not answer;
/// * a sidecar that does not parse, or signature data that is malformed:
///   exit 3 -- it signs nothing this run can trust;
/// * no mounted key verifies it (a stranger's key, or altered bytes): exit 3;
/// * the key that verified is refused by `decide` -- revoked for compromise,
///   used outside its validity (a retired key's signature after retirement),
///   or not an `EvidenceSigning` key: exit 3, naming the reason.
fn verify_receipt_signature(
    point: &PointBinding,
    receipt_key: &str,
    receipt_bytes: &[u8],
    archive: &Store,
    keyring: &wire::EvidenceKeyring,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), DrillError> {
    use logweir_core::trust::{decide, EvidenceClaim, IndependentObservation, KeyUsage};

    let sidecar_key = crate::catalog::cli::sidecar_key_of(receipt_key);
    let sidecar_bytes = match archive.get_capped(&sidecar_key, caps::SIDECAR) {
        Ok((bytes, _)) => bytes,
        Err(StoreError::NotFound(_)) => {
            return Err(refuse(format!(
                "{POINT_UNTRUSTED}. Recovery point {}'s receipt carries no signature: \
                 {sidecar_key} is not in the archive. An unsigned receipt is evidence of \
                 nothing; no data operation was started.",
                point.point_id
            )))
        }
        Err(error) => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s receipt signature {sidecar_key} could not be read: {error}; \
                 no data operation was started",
                point.point_id
            )))
        }
    };
    let sidecar: Sidecar = serde_json::from_slice(&sidecar_bytes).map_err(|error| {
        refuse(format!(
            "{POINT_UNTRUSTED}. Recovery point {}'s receipt signature {sidecar_key} is not a \
             DSSE sidecar ({error}); no data operation was started.",
            point.point_id
        ))
    })?;
    let payload_type = logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT;
    let mut verified: Option<&wire::EvidenceKey> = None;
    for key in &keyring.keys {
        let Ok(parsed) = VerifyingKey::from_pem_str(&key.public_key_pem) else {
            continue;
        };
        match verify_detached(&parsed, payload_type, receipt_bytes, &sidecar) {
            Ok(_) => {
                verified = Some(key);
                break;
            }
            Err(EvidenceError::Malformed(message)) => {
                return Err(refuse(format!(
                    "{POINT_UNTRUSTED}. Recovery point {}'s receipt signature data is malformed \
                     ({message}); no data operation was started.",
                    point.point_id
                )))
            }
            Err(EvidenceError::Verify(_) | EvidenceError::Key(_)) => {}
        }
    }
    let Some(key) = verified else {
        return Err(refuse(format!(
            "{POINT_UNTRUSTED}. Recovery point {}'s receipt signature does not verify under any \
             of the {} evidence-signing keys this installation trusts (claimed signer: {}). It \
             was signed by a key the namespace's trust does not list, or the receipt or its \
             signature was altered; no data operation was started.",
            point.point_id,
            keyring.keys.len(),
            sidecar
                .signatures
                .iter()
                .map(|s| s.keyid.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    };
    let json: serde_json::Value = serde_json::from_slice(receipt_bytes).unwrap_or_default();
    let verdict = decide(
        Some(&key.trust),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::from_document(payload_type, &json),
        // A restore Job has no local history of this receipt: a key revoked
        // for compromise therefore verifies nothing (D3 §16).
        &IndependentObservation::none(),
        now,
    );
    // A PASS IS `Valid` ON A `Current`/`Historical` BASIS, not `Valid` alone.
    // `Verdict::may_render_green` is that rule for a
    // decided verdict — the same allow-list `weirkeeper::verification::ValidBasis`
    // applies to a stored one (this crate does not link `weirkeeper`).
    // `decide` pairs `Valid` with those two bases only, so this reads the same
    // today; the allow-list is what keeps it so.
    if !verdict.may_render_green() {
        return Err(refuse(format!(
            "{POINT_UNTRUSTED}. Recovery point {}'s receipt verifies under key {}, and this \
             installation's trust refuses that key for it: {} (key state {}); no data operation \
             was started.",
            point.point_id,
            key.trust.key_id,
            verdict
                .reason
                .map_or("not accepted", logweir_core::trust::UntrustReason::as_str),
            verdict.key_state.as_str()
        )));
    }
    Ok(())
}

/// Refuse a binding that is malformed on its face, before the archive is
/// touched at all.
///
/// A plan naming an empty receipt key would otherwise `get("")` against a real
/// bucket, and a digest that is not `sha256:<64 hex>` can never match anything
/// this build computes — so both are answered without a round trip, and the
/// message says which field.
fn check_point_shape(point: &PointBinding) -> Result<(), DrillError> {
    let faults = point_shape_faults(point);
    if faults.is_empty() {
        return Ok(());
    }
    Err(refuse(format!(
        "{POINT_BINDING_MISMATCH}. The plan's recovery point binding is malformed: {}; no data \
         operation was started.",
        faults.join("; ")
    )))
}

/// What is malformed about `point` on its face, one sentence per
/// field, empty when nothing is. [`check_point_shape`]'s whole judgement, and
/// the restore preflight's (`check::kinds::restore`), so the preview refuses
/// exactly the bindings the runner will.
#[must_use]
pub fn point_shape_faults(point: &PointBinding) -> Vec<String> {
    let mut faults = Vec::new();
    if !point
        .point_id
        .starts_with(crate::catalog::record::POINT_ID_PREFIX)
        || point.point_id.len() != crate::catalog::record::POINT_ID_PREFIX.len() + 32
        || !point.point_id[crate::catalog::record::POINT_ID_PREFIX.len()..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        faults.push(format!(
            "`source.point.point_id` is {:?}, not `{}` plus 32 lowercase hex characters",
            point.point_id,
            crate::catalog::record::POINT_ID_PREFIX
        ));
    }
    if point.receipt_key.trim().is_empty() {
        faults.push("`source.point.receipt_key` is empty".to_string());
    }
    for (field, value) in [
        ("receipt_sha256", &point.receipt_sha256),
        ("manifest_sha256", &point.manifest_sha256),
    ] {
        let hex = value.strip_prefix("sha256:").unwrap_or_default();
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            faults.push(format!(
                "`source.point.{field}` is {value:?}, not `sha256:` plus 64 hex characters"
            ));
        }
    }
    faults
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use logweir_core::backup_receipt::{
        BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
    };
    use std::collections::BTreeMap;

    pub(crate) fn receipt(manifest_key: &str, manifest_sha256: &str) -> BackupReceipt {
        BackupReceipt {
            format_version: "1.0.0".into(),
            run_id: "run-1".into(),
            backup_id: "nightly-7".into(),
            requested_at: chrono::Utc::now(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            exit_code: 0,
            triggered_by: "manual".into(),
            source: ReceiptSource {
                cluster_id: "SOURCE00000000000000000".into(),
                bootstrap_servers: vec!["source:9092".into()],
                auth: ReceiptAuth {
                    mode: "plaintext".into(),
                    username: None,
                },
                topics: vec!["orders".into()],
            },
            engine: ReceiptEngine {
                id: "oso".into(),
                version: "1".into(),
                digest: "sha256:ee".into(),
            },
            archive: ReceiptArchive {
                manifest_key: manifest_key.into(),
                manifest_sha256: manifest_sha256.into(),
                manifest_version_id: None,
                prefix: "logweir/backups/nightly-7/".into(),
            },
            records: BTreeMap::from([("orders".to_string(), 3u64)]),
            covered: ReceiptCovered {
                from_ms: 1,
                to_ms: 2,
            },
            config_coverage: None,
            topic_configuration: None,
            owner_detection: None,
            consumer_positions: None,
            schema_dependency: None,
            generations: None,
            source_watermarks: None,
        }
    }

    const RECEIPT_KEY: &str = "logweir/backups/nightly-7/run-1.receipt.json";
    /// **An archive prefix under `logweir/` is a fixture concession, not a
    /// claim about where a real archive keeps one.** `Store::in_memory` is the
    /// socket-free double this repository's gate requires
    /// (`tests/no_network_in_unit_tests.rs`), and `put_create_only` asserts
    /// Global Constraint 6's root on every key it accepts — so an in-memory
    /// archive cannot hold anything outside it. The archive handle's prefix is
    /// the plan's `source.storage` prefix.
    const ARCHIVE_PREFIX: &str = "logweir";
    /// The key the ENGINE reads set `nightly-7`'s manifest at under
    /// [`ARCHIVE_PREFIX`] (`Store::engine_manifest_key`); the binding refuses
    /// a receipt that attests any other key.
    const MANIFEST_KEY: &str = "logweir/nightly-7/manifest.json";

    struct Archive {
        store: Store,
        binding: PointBinding,
        /// The evidence keyring that trusts the key the receipt was signed by.
        keys: Vec<u8>,
    }

    /// An `EvidenceSigning` key's lifecycle record, active for a day either
    /// side of now -- the fixtures change one field at a time from here.
    pub(crate) fn trusted(key: &SigningKey) -> logweir_core::trust::TrustedKey {
        logweir_core::trust::TrustedKey {
            key_id: key.key_id(),
            principal_id: format!("install:{}", key.key_id()),
            usages: vec![KeyUsage::EvidenceSigning],
            not_before: chrono::Utc::now() - chrono::Duration::days(1),
            not_after: chrono::Utc::now() + chrono::Duration::days(1),
            state: logweir_core::trust::KeyState::Active,
            retired_at: None,
            revoked_at: None,
            revocation_reason: None,
            revocation_effective_from: None,
        }
    }

    /// An [`wire::EvidenceKeyring`] over `(key, lifecycle)` pairs.
    pub(crate) fn evidence_keys(
        entries: Vec<(&SigningKey, logweir_core::trust::TrustedKey)>,
    ) -> Vec<u8> {
        serde_json::to_vec(&wire::EvidenceKeyring {
            format_version: wire::EVIDENCE_KEYRING_FORMAT_VERSION.to_string(),
            keys: entries
                .into_iter()
                .map(|(key, trust)| wire::EvidenceKey {
                    public_key_pem: key.verifying_key().to_public_key_pem().expect("pem"),
                    trust,
                })
                .collect(),
        })
        .expect("the keyring serialises")
    }

    /// Write a receipt AND its DSSE sidecar, signed by `signer`, the way the
    /// backup runner does (`<run>.receipt.json` beside `<run>.receipt.sig`).
    fn put_signed_receipt(store: &Store, receipt_bytes: &[u8], signer: &SigningKey) {
        store
            .put_create_only(RECEIPT_KEY, receipt_bytes)
            .expect("the receipt is written");
        let sidecar = sign_detached(
            signer,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            receipt_bytes,
        )
        .expect("sign");
        store
            .put_create_only(
                &crate::catalog::cli::sidecar_key_of(RECEIPT_KEY),
                &serde_json::to_vec(&sidecar).expect("serialises"),
            )
            .expect("the sidecar is written");
    }

    fn check(
        plan: &DrillSpec,
        store: &Store,
        keys: &[u8],
    ) -> Result<Option<VerifiedPoint>, DrillError> {
        verify_point_binding(plan, store, Some(keys), chrono::Utc::now())
    }

    /// A point proven with nothing to note, from a receipt that carries no
    /// `config_coverage` block (UNKNOWN for every topic), describing the
    /// fixture's set as the binding read its manifest: at
    /// `manifest_version_id`, `None` for a store that answers no version.
    fn proven(point_id: String, manifest_version_id: Option<String>) -> Option<VerifiedPoint> {
        Some(VerifiedPoint {
            point_id: point_id.clone(),
            config_coverage: logweir_core::backup_receipt::SourceConfigCoverage::unknown(),
            pin_note: None,
            set: BoundSet {
                point_id,
                backup_id: "nightly-7".into(),
                manifest_key: MANIFEST_KEY.into(),
                manifest_sha256: logweir_core::ids::sha256_prefixed(br#"{"topics":[]}"#),
                manifest_version_id,
            },
            // The fixture receipt's measured source, and no owner record
            // ("not checked").
            source_cluster_id: "SOURCE00000000000000000".into(),
            owners: logweir_core::original_name::ReceiptOwners::default(),
        })
    }

    /// One internally consistent, SIGNED recovery point in a socket-free
    /// store, with the keyring that trusts its signer.
    fn archive_with_a_point(prefix: &str, key_in_plan: &str) -> Archive {
        let store = Store::in_memory(prefix);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written");
        Archive {
            store,
            binding: PointBinding {
                point_id: crate::catalog::record::point_id(&receipt_bytes),
                receipt_key: key_in_plan.into(),
                receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
                manifest_sha256,
            },
            keys: evidence_keys(vec![(&signer, trusted(&signer))]),
        }
    }

    fn archive() -> Archive {
        archive_with_a_point(ARCHIVE_PREFIX, RECEIPT_KEY)
    }

    /// [`archive_with_a_point`] whose receipt is edited by `edit` before it is
    /// serialised and signed, how a row gets a 1.1.0 receipt.
    fn archive_with_receipt(edit: impl FnOnce(&mut BackupReceipt)) -> Archive {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let mut doc = receipt(MANIFEST_KEY, &manifest_sha256);
        edit(&mut doc);
        let receipt_bytes = serde_json::to_vec(&doc).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written");
        Archive {
            store,
            binding: PointBinding {
                point_id: crate::catalog::record::point_id(&receipt_bytes),
                receipt_key: RECEIPT_KEY.into(),
                receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
                manifest_sha256,
            },
            keys: evidence_keys(vec![(&signer, trusted(&signer))]),
        }
    }

    /// A verified binding hands the drill its receipt's
    /// configuration capture coverage — the ONLY source phases 3 and 7 read it
    /// from — and a 1.0.0 receipt hands over UNKNOWN for every topic.
    #[test]
    fn a_verified_binding_carries_the_receipts_config_coverage_and_a_1_0_0_one_carries_unknown() {
        use logweir_core::backup_receipt::{ConfigCoverage, TopicConfigCoverage};
        let a = archive_with_receipt(|r| {
            r.format_version = "1.1.0".into();
            r.config_coverage = Some(BTreeMap::from([(
                "orders".to_string(),
                TopicConfigCoverage {
                    coverage: "captureDenied".into(),
                    reason: None,
                    timestamp_type: None,
                },
            )]));
        });
        let verified = verify_point_binding(
            &plan_with(Some(a.binding.clone())),
            &a.store,
            Some(&a.keys),
            chrono::Utc::now(),
        )
        .expect("the point verifies")
        .expect("a bound plan names its point");
        assert_eq!(
            verified.config_coverage.of("orders"),
            ConfigCoverage::CaptureDenied
        );
        let old = archive();
        let verified = verify_point_binding(
            &plan_with(Some(old.binding.clone())),
            &old.store,
            Some(&old.keys),
            chrono::Utc::now(),
        )
        .expect("the point verifies")
        .expect("a bound plan names its point");
        assert_eq!(
            verified.config_coverage.of("orders"),
            ConfigCoverage::Unknown,
            "a 1.0.0 receipt carries no block: unknown, never captured"
        );
    }

    /// A receipt both verifiers refuse lends no coverage,
    /// even though a trusted key signed it: here a 1.0.0 document carrying a
    /// `captured` block (arm 6). The binding still verifies (its own checks
    /// pass), and the coverage reads UNKNOWN, never `captured`. Mutant: copy
    /// the block without `validate_invariants` → `Captured`, and this fails.
    #[test]
    fn a_signed_receipt_that_contradicts_itself_lends_unknown_coverage() {
        use logweir_core::backup_receipt::{ConfigCoverage, TopicConfigCoverage};
        let a = archive_with_receipt(|r| {
            // format_version stays "1.0.0": arm 6 refuses the block under it.
            r.config_coverage = Some(BTreeMap::from([(
                "orders".to_string(),
                TopicConfigCoverage {
                    coverage: "captured".into(),
                    reason: None,
                    timestamp_type: None,
                },
            )]));
        });
        let verified = verify_point_binding(
            &plan_with(Some(a.binding.clone())),
            &a.store,
            Some(&a.keys),
            chrono::Utc::now(),
        )
        .expect("the binding's own checks pass: digests, signature, manifest")
        .expect("a bound plan names its point");
        assert_eq!(
            verified.config_coverage.of("orders"),
            ConfigCoverage::Unknown,
            "a self-contradicting receipt's `captured` is not believed"
        );
    }

    /// `backup: nightly-7`, the receipt fixture's set: a bound plan names its
    /// point's own set.
    const PLAN_YAML: &str = r#"
source:
  storage: {backend: filesystem, path: /tmp/logweir-binding-fixture}
  backup: nightly-7
  topics: [orders]
target:
  bootstrap_servers: ["target:9092"]
  mode: scratch
  topic_mapping_prefix: "rehearsal-3f2a91c7-"
  marker_topic: logweir.scratch
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
  max_partitions: 200
objectives: {rto_seconds: 1800, pass_rate: 1.0}
evidence: {backend: filesystem, path: /tmp/logweir-binding-fixture-evidence}
"#;

    pub(crate) fn plan_with(point: Option<PointBinding>) -> DrillSpec {
        let mut plan: DrillSpec = serde_yaml::from_str(PLAN_YAML).expect("the fixture parses");
        plan.source.point = point;
        plan
    }

    #[test]
    fn a_plan_with_no_point_binding_is_the_v1_shape_and_checks_nothing() {
        let a = archive();
        assert_eq!(
            check(&plan_with(None), &a.store, &a.keys).expect("no binding, no check"),
            None
        );
    }

    #[test]
    fn a_point_the_archive_holds_is_proven_and_names_itself() {
        let a = archive();
        let id = a.binding.point_id.clone();
        assert_eq!(
            check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
                .expect("the point verifies"),
            proven(id, None)
        );
    }

    /// A destination whose handle carries a prefix still resolves the SAME
    /// bucket-absolute key. The mutant this kills is a `Store::qualify` added
    /// "for safety": it would rewrite `logweir/backups/…` to
    /// `logweir/logweir/backups/…` and report every present point as missing.
    #[test]
    fn a_receipt_key_is_bucket_absolute_and_is_not_requalified() {
        let a = archive_with_a_point("logweir/", RECEIPT_KEY);
        assert!(
            check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys).is_ok(),
            "a prefixed destination handle must resolve a bucket-absolute receipt key"
        );
    }

    /// THE MUTANT: tolerate a receipt digest that differs. This is the
    /// tampered-bundle case moved to the archive, and it must be exit 3.
    #[test]
    fn a_receipt_digest_that_differs_is_a_refusal_not_a_warning() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.receipt_sha256 = format!("sha256:{}", "0".repeat(64));
        let error = check(&plan_with(Some(binding)), &a.store, &a.keys)
            .expect_err("a tampered point is refused");
        assert!(
            matches!(error, DrillError::Guard(_)),
            "a digest mismatch must be exit 3, got {error}"
        );
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains(POINT_BINDING_MISMATCH),
            "{error}"
        );
    }

    #[test]
    fn a_point_id_that_does_not_derive_from_the_receipt_is_refused() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.point_id = format!("lwp1-{}", "a".repeat(32));
        let error = check(&plan_with(Some(binding)), &a.store, &a.keys)
            .expect_err("a relabelled point is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("derives recovery point id"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_digest_the_receipt_contradicts_is_refused() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.manifest_sha256 = format!("sha256:{}", "1".repeat(64));
        let error = check(&plan_with(Some(binding)), &a.store, &a.keys)
            .expect_err("a contradicted manifest digest is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("attests manifest digest"),
            "{error}"
        );
    }

    /// A receipt that is intact while the manifest it names is gone: every
    /// document agrees with every other, and the archive does not.
    #[test]
    fn a_manifest_the_archive_does_not_hold_is_refused() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        let error = check(&plan_with(Some(binding)), &store, &keys)
            .expect_err("a point whose manifest is gone is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::Operational);
        assert!(
            error.to_string().contains("is not in the archive"),
            "{error}"
        );
    }

    /// **A manifest whose bytes changed under an intact receipt is refused.**
    /// Every document agrees with every other, and the archive holds something
    /// else. KILLS: the digest half of the manifest check disabled.
    ///
    /// Seeded by ATTESTING bytes A and STORING bytes B, because `Store` is
    /// create-only by construction (Global Constraint 6) and a test cannot
    /// overwrite a key it already wrote. The effect is identical to a manifest
    /// replaced in place.
    #[test]
    fn a_manifest_whose_bytes_changed_under_an_intact_receipt_is_refused() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let attested = br#"{"topics":[]}"#.to_vec();
        let attested_sha256 = logweir_core::ids::sha256_prefixed(&attested);
        let substituted = br#"{"topics":["swapped"]}"#.to_vec();
        assert_ne!(
            logweir_core::ids::sha256_prefixed(&substituted),
            attested_sha256
        );
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &attested_sha256)).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        store
            .put_create_only(MANIFEST_KEY, &substituted)
            .expect("the substituted manifest is written");
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            // The plan and the receipt agree, and both are wrong about the
            // bucket.
            manifest_sha256: attested_sha256.clone(),
        };
        let error = check(&plan_with(Some(binding)), &store, &keys)
            .expect_err("a substituted manifest is refused");
        assert_eq!(
            error.exit_code(),
            crate::exit::ExitCode::GuardRefused,
            "the archive does not hold what the approver bound to: {error}"
        );
        let rendered = error.to_string();
        assert!(rendered.contains("hashes to"), "{rendered}");
        assert!(rendered.contains(POINT_BINDING_MISMATCH), "{rendered}");
        assert!(rendered.contains(&attested_sha256), "{rendered}");
    }

    // A point on a versioned bucket pins its manifest's version.

    /// A SIGNED point on a versioned bucket whose receipt pins the manifest's
    /// version id — or, with `pin == false`, the same point written by a build
    /// that pinned nothing — and the handle that can rewrite the manifest the
    /// way the engine's own unconditional put does.
    fn versioned_point(pin: bool) -> (Archive, logweir_engine_oso::storage::VersionedBucket) {
        let (store, bucket) = Store::in_memory_versioned(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let version = store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written")
            .version_id
            .expect("a versioned bucket names the version");
        let mut r = receipt(MANIFEST_KEY, &manifest_sha256);
        if pin {
            r.archive.manifest_version_id = Some(version);
            r.format_version =
                logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.into();
        }
        let receipt_bytes = serde_json::to_vec(&r).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        let archive = Archive {
            store,
            binding: PointBinding {
                point_id: crate::catalog::record::point_id(&receipt_bytes),
                receipt_key: RECEIPT_KEY.into(),
                receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
                manifest_sha256,
            },
            keys: evidence_keys(vec![(&signer, trusted(&signer))]),
        };
        (archive, bucket)
    }

    /// A pinned point whose pinned version IS the current one is proven — the
    /// pin refuses nothing a point without one would have admitted.
    #[test]
    fn a_pinned_point_whose_version_is_current_is_proven() {
        let (a, _bucket) = versioned_point(true);
        let id = a.binding.point_id.clone();
        assert_eq!(
            check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
                .expect("the pinned version is the current one"),
            proven(id, current_version(&a.store))
        );
    }

    /// **THE PIN'S WHOLE POINT.** Engine 0.21.0 re-running over a set rewrites
    /// its segments in place and can put a manifest whose bytes are IDENTICAL
    /// (measured): the digest cannot see it, the version can. A pinned
    /// point is refused, exit 3, with the pinned version read BY ID to say the
    /// attested manifest is still retained; the same rewrite under a point
    /// that pins nothing passes the digest check, which is the limit the pin
    /// exists to close on a versioned bucket.
    #[test]
    fn a_pinned_point_whose_manifest_was_rewritten_with_identical_bytes_is_refused() {
        let (a, bucket) = versioned_point(true);
        bucket.overwrite(MANIFEST_KEY, br#"{"topics":[]}"#);
        let error = check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
            .expect_err("a set written again after its point was signed is not that point");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        assert!(rendered.contains(POINT_BINDING_MISMATCH), "{rendered}");
        assert!(rendered.contains(pin::SUPERSEDED_CAUSE), "{rendered}");
        assert!(
            rendered.contains("is still retained at that version"),
            "the pinned version was read by id: {rendered}"
        );
        assert!(
            !rendered.contains(POINT_PIN_UNCHECKED),
            "a rewrite in THIS bucket is never the copy's note: {rendered}"
        );

        let (unpinned, bucket) = versioned_point(false);
        bucket.overwrite(MANIFEST_KEY, br#"{"topics":[]}"#);
        assert!(
            check(
                &plan_with(Some(unpinned.binding.clone())),
                &unpinned.store,
                &unpinned.keys
            )
            .is_ok(),
            "without a pin the identical rewrite is invisible to the digest — the control that \
             shows what the pin adds"
        );
    }

    /// A rewrite with DIFFERENT bytes is refused by the version comparison
    /// first, before the digest, and says so.
    #[test]
    fn a_pinned_point_whose_manifest_changed_is_refused_as_rewritten() {
        let (a, bucket) = versioned_point(true);
        bucket.overwrite(MANIFEST_KEY, br#"{"topics":["orders"]}"#);
        let rendered = check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
            .expect_err("a changed manifest is refused")
            .to_string();
        assert!(rendered.contains(pin::SUPERSEDED_CAUSE), "{rendered}");
    }

    // A version id belongs to one bucket, and a byte-identical copy of a
    // pinned point is the same point in another place, never "written again".

    /// Every object of `from`, byte for byte, into `to`: `aws s3 sync`, `mc
    /// mirror`, rclone — anything but version-preserving replication.
    fn copy_every_object(from: &Store, to: &Store) {
        for key in from.list_keys("").expect("the source lists") {
            let (bytes, _) = from
                .get_capped(&key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
                .expect("the source reads");
            to.put_create_only(&key, &bytes)
                .expect("the copy is written");
        }
    }

    /// The version a read of the fixture manifest answers in `store` now.
    fn current_version(store: &Store) -> Option<String> {
        store
            .get_capped(
                MANIFEST_KEY,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .expect("the manifest reads")
            .1
    }

    /// The refusal's message, asserting it is an exit-3 guard refusal that
    /// opens with [`POINT_BINDING_SET_MISMATCH`].
    fn set_mismatch(result: Result<Option<VerifiedPoint>, DrillError>) -> String {
        match result {
            Err(DrillError::Guard(GuardRefusal(message))) => {
                assert!(
                    message.starts_with(&format!("{POINT_BINDING_SET_MISMATCH}. ")),
                    "{message}"
                );
                message
            }
            other => panic!("expected a {POINT_BINDING_SET_MISMATCH} refusal, got {other:?}"),
        }
    }

    /// **The plan half.** A plan bound to point A of set `nightly-7`
    /// whose `source.backup` names `nightly-8` is refused exit 3 by the
    /// binding, over the approved bytes, before any broker is contacted —
    /// although the receipt, its signature and its manifest all verify. The
    /// control is the same binding with `backup: nightly-7`
    /// (`a_point_the_archive_holds_is_proven_and_names_itself`).
    ///
    /// KILLS: deleting the comparison (the point is proven); comparing the
    /// receipt's `backup_id` with anything but `source.backup`.
    #[test]
    fn a_plan_naming_another_set_than_its_points_is_refused() {
        let a = archive();
        let mut plan = plan_with(Some(a.binding.clone()));
        plan.source.backup = "nightly-8".into();
        let message = set_mismatch(check(&plan, &a.store, &a.keys));
        assert!(
            message.contains(&a.binding.point_id)
                && message.contains("backup set `nightly-7`")
                && message.contains("names set `nightly-8`")
                && message.contains("source.backup: nightly-7"),
            "the refusal names the point, both sets and the repair: {message}"
        );
        assert!(
            !message.contains(POINT_BINDING_MISMATCH),
            "a distinct token: the binding itself holds"
        );
    }

    /// **`latestCompleted` with a bound point is refused**, even when
    /// the point's set is the newest one: the selector can stop agreeing with
    /// the point at the next backup, and the plan the approver signed would
    /// then restore another set under this receipt.
    #[test]
    fn latest_completed_with_a_bound_point_is_refused() {
        let a = archive();
        let mut plan = plan_with(Some(a.binding.clone()));
        plan.source.backup = "latestCompleted".into();
        let message = set_mismatch(check(&plan, &a.store, &a.keys));
        assert!(
            message.contains("`latestCompleted`, which restores whichever set is newest")
                && message.contains("source.backup: nightly-7"),
            "{message}"
        );
    }

    /// **The verified point carries the set it describes** — the
    /// receipt's set id and manifest key, the bound digest, and the version
    /// the binding read — on an unversioned store (no version) and on a
    /// versioned one (the current version, which is the pinned one).
    ///
    /// KILLS: a `BoundSet` built from the plan instead of the receipt, or with
    /// the version dropped (`check_restored_set` would then refuse every
    /// versioned bucket's run, or none).
    #[test]
    fn a_verified_point_carries_the_set_its_receipt_describes() {
        let a = archive();
        let verified = check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
            .expect("verifies")
            .expect("bound");
        assert_eq!(verified.set.backup_id, "nightly-7");
        assert_eq!(verified.set.manifest_key, MANIFEST_KEY);
        assert_eq!(verified.set.manifest_sha256, a.binding.manifest_sha256);
        assert_eq!(verified.set.manifest_version_id, None);

        let (v, _bucket) = versioned_point(true);
        let verified = check(&plan_with(Some(v.binding.clone())), &v.store, &v.keys)
            .expect("verifies")
            .expect("bound");
        assert!(verified.set.manifest_version_id.is_some());
        assert_eq!(
            verified.set.manifest_version_id,
            current_version(&v.store),
            "the version the binding READ"
        );
    }

    /// One SIGNED point whose receipt (at `receipt_key`) attests its manifest
    /// at `receipt_manifest_key`, in an archive whose handle has `prefix` (an
    /// in-memory handle writes only under its own prefix), with every
    /// `(key, bytes)` of `others` written beside it.
    fn archive_at(
        prefix: &str,
        receipt_key: &str,
        receipt_manifest_key: &str,
        others: &[(&str, &[u8])],
    ) -> Archive {
        let store = Store::in_memory(prefix);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes = serde_json::to_vec(&receipt(receipt_manifest_key, &manifest_sha256))
            .expect("serialises");
        let signer = SigningKey::generate_ed25519();
        store
            .put_create_only(receipt_key, &receipt_bytes)
            .expect("the receipt is written");
        let sidecar = sign_detached(
            &signer,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            &receipt_bytes,
        )
        .expect("sign");
        store
            .put_create_only(
                &crate::catalog::cli::sidecar_key_of(receipt_key),
                &serde_json::to_vec(&sidecar).expect("serialises"),
            )
            .expect("the sidecar is written");
        store
            .put_create_only(receipt_manifest_key, &manifest)
            .expect("the point's manifest is written");
        for (key, bytes) in others {
            store.put_create_only(key, bytes).expect("written");
        }
        Archive {
            store,
            binding: PointBinding {
                point_id: crate::catalog::record::point_id(&receipt_bytes),
                receipt_key: receipt_key.into(),
                receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
                manifest_sha256,
            },
            keys: evidence_keys(vec![(&signer, trusted(&signer))]),
        }
    }

    /// **A nested set under the plan's prefix.** The point's set is nested
    /// under the plan's prefix (`logweir/a/nightly-7/`), and another set with
    /// the same id and other bytes sits where the engine reads under that
    /// prefix (`logweir/nightly-7/manifest.json`). The receipt, its signature
    /// and its manifest all verify; the binding refuses, before any broker
    /// contact, naming both keys and the prefix to use.
    ///
    /// The control: the same layout of the point (receipt attesting the nested
    /// manifest) in an archive read through a handle whose prefix is the one
    /// the set was written under (`logweir/a`) — the engine then reads the
    /// point's own manifest, and the point is proven.
    ///
    /// KILLS: the engine-key comparison removed (the nested point is proven
    /// under the parent prefix); the engine key computed from anything but
    /// this plan's handle.
    #[test]
    fn a_point_the_engine_would_not_read_under_this_plans_storage_is_refused() {
        let nested = "logweir/a/nightly-7/manifest.json";
        let a = archive_at(
            "logweir",
            RECEIPT_KEY,
            nested,
            &[(
                "logweir/nightly-7/manifest.json",
                br#"{"topics":[],"other":"set"}"#,
            )],
        );
        let message = set_mismatch(check(
            &plan_with(Some(a.binding.clone())),
            &a.store,
            &a.keys,
        ));
        assert!(
            message.contains(&format!("attests set `nightly-7`'s manifest at {nested}"))
                && message.contains(
                    "the engine would read set `nightly-7` at logweir/nightly-7/manifest.json"
                )
                && message.contains("the prefix the set was written under (logweir/a)"),
            "{message}"
        );

        // The receipt sits at the WRITER's key (FX-36 confines the binding to
        // it), outside the handle's own prefix, as on a real destination: the
        // versioned double's handle writes there, the store does not.
        let (store, bucket) = Store::in_memory_versioned("logweir/a");
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(nested, &manifest_sha256)).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        let sidecar = sign_detached(
            &signer,
            logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
            &receipt_bytes,
        )
        .expect("sign");
        bucket.overwrite(RECEIPT_KEY, &receipt_bytes);
        bucket.overwrite(
            &crate::catalog::cli::sidecar_key_of(RECEIPT_KEY),
            &serde_json::to_vec(&sidecar).expect("serialises"),
        );
        bucket.overwrite(nested, &manifest);
        let own = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        let own_keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        let verified = check(&plan_with(Some(own)), &store, &own_keys)
            .expect("the same point under the prefix it was written under")
            .expect("bound");
        assert_eq!(verified.set.manifest_key, nested);
    }

    /// A receipt whose manifest key no plan storage maps the set id to is
    /// refused, saying so.
    #[test]
    fn a_receipt_key_the_engine_never_reads_is_refused() {
        let odd = "logweir/backups/nightly-7/run-1.manifest.json";
        let a = archive_at(ARCHIVE_PREFIX, RECEIPT_KEY, odd, &[]);
        let message = set_mismatch(check(
            &plan_with(Some(a.binding.clone())),
            &a.store,
            &a.keys,
        ));
        assert!(
            message.contains("no plan storage makes the engine read it") && message.contains(odd),
            "{message}"
        );
    }

    fn bound_set() -> BoundSet {
        BoundSet {
            point_id: "lwp1-0123456789abcdef0123456789abcdef".into(),
            backup_id: "nightly-7".into(),
            manifest_key: "archive/nightly-7/manifest.json".into(),
            manifest_sha256: format!("sha256:{}", "a".repeat(64)),
            manifest_version_id: Some("v-1".into()),
        }
    }

    /// The set and facts `describe` would hand the drill for exactly the
    /// bound set.
    fn restored_as_bound() -> (
        logweir_core::engine::BackupSetRef,
        logweir_core::engine::BackupSetFacts,
    ) {
        let b = bound_set();
        (
            logweir_core::engine::BackupSetRef {
                backup_id: b.backup_id.clone(),
                manifest_key: b.manifest_key.clone(),
            },
            logweir_core::engine::BackupSetFacts {
                backup_id: b.backup_id.clone(),
                created_at: chrono::Utc::now(),
                source_cluster_id: None,
                manifest_sha256: b.manifest_sha256.clone(),
                manifest_version_id: b.manifest_version_id.clone(),
                consumer_group_snapshot_sha256: None,
                topics: vec![],
            },
        )
    }

    /// **The restored-set half, row by row.** The set `describe` read
    /// is the bound one: proven. A plan bound to no point: nothing to compare.
    /// Then each of the four disagreements ALONE is refused, naming itself —
    /// so a check that ignores any one of them fails its row.
    ///
    /// KILLS (one per row): the set id ignored; the manifest key ignored; the
    /// digest ignored; the version ignored; and the whole check returning `Ok`.
    #[test]
    fn a_restored_set_is_the_bound_one_or_is_refused_naming_what_differs() {
        let (set, facts) = restored_as_bound();
        let bound = bound_set();
        check_restored_set(Some(&bound), &set, &facts).expect("the bound set itself");
        check_restored_set(None, &set, &facts).expect("an unbound plan compares nothing");

        let refused = |set: &logweir_core::engine::BackupSetRef,
                       facts: &logweir_core::engine::BackupSetFacts,
                       needle: &str| {
            let message = set_mismatch(check_restored_set(Some(&bound), set, facts).map(|()| None));
            assert!(message.contains(needle), "{needle:?} not in: {message}");
            assert!(
                message.contains(&bound.point_id)
                    && message.contains("No target topic of this restore was created"),
                "{message}"
            );
            assert_eq!(
                message.matches("; ").count(),
                0,
                "exactly ONE disagreement is planted per row: {message}"
            );
        };

        let (mut other_id, f) = restored_as_bound();
        other_id.backup_id = "nightly-8".into();
        refused(
            &other_id,
            &f,
            "it is backup set `nightly-8`, not the receipt's `nightly-7`",
        );

        let (mut other_key, f) = restored_as_bound();
        other_key.manifest_key = "copy/nightly-7/manifest.json".into();
        refused(
            &other_key,
            &f,
            "its manifest is copy/nightly-7/manifest.json, not the receipt's \
             archive/nightly-7/manifest.json",
        );

        let (s, mut other_digest) = restored_as_bound();
        other_digest.manifest_sha256 = format!("sha256:{}", "b".repeat(64));
        refused(
            &s,
            &other_digest,
            &format!("its manifest hashes to sha256:{}", "b".repeat(64)),
        );

        let (s, mut rewritten) = restored_as_bound();
        rewritten.manifest_version_id = Some("v-2".into());
        refused(
            &s,
            &rewritten,
            "its manifest read answered version v-2, and the binding's read answered version v-1",
        );

        let (s, mut unversioned) = restored_as_bound();
        unversioned.manifest_version_id = None;
        refused(
            &s,
            &unversioned,
            "its manifest read answered no version id, and the binding's read answered version v-1",
        );
    }

    /// Several disagreements are all named, in one refusal.
    #[test]
    fn every_disagreement_is_named_in_one_refusal() {
        let (mut set, mut facts) = restored_as_bound();
        set.backup_id = "nightly-8".into();
        facts.manifest_sha256 = format!("sha256:{}", "c".repeat(64));
        let message =
            set_mismatch(check_restored_set(Some(&bound_set()), &set, &facts).map(|()| None));
        assert!(
            message.contains("backup set `nightly-8`") && message.contains("hashes to"),
            "{message}"
        );
    }

    /// A point proven by its digest with the [`POINT_PIN_UNCHECKED`] note, in
    /// `bucket`. Its bound set carries the
    /// version THIS bucket's read answered — never the receipt's pin, which
    /// this bucket does not hold — or every restore of a pinned point from a
    /// DR copy would be refused at `check_restored_set`, whose `describe`
    /// reads the same version this does.
    fn noted(
        result: Result<Option<VerifiedPoint>, DrillError>,
        id: &str,
        bucket: &Store,
    ) -> String {
        let proven = result
            .expect("a copy of a signed point is that point")
            .expect("the plan is bound");
        assert_eq!(proven.point_id, id);
        assert_eq!(
            proven.set.manifest_version_id,
            current_version(bucket),
            "the bound set carries the version this bucket's read answered"
        );
        let note = proven
            .pin_note
            .expect("the pin could not be checked here, and the runner says so");
        assert!(note.starts_with(POINT_PIN_UNCHECKED), "{note}");
        assert!(note.contains(pin::UNCHECKED_NOTE), "{note}");
        assert!(
            !note.contains(pin::SUPERSEDED_CAUSE),
            "never the superseded cause: {note}"
        );
        note
    }

    /// **The unversioned copy.** A pinned point copied byte for byte into an
    /// UNVERSIONED bucket: the copy answers no version id and cannot be read
    /// by one. It is the same signed point, proven by its digest, with the
    /// note.
    #[test]
    fn a_byte_identical_copy_of_a_pinned_point_in_an_unversioned_bucket_is_proven() {
        let (a, _bucket) = versioned_point(true);
        let copy = Store::in_memory(ARCHIVE_PREFIX);
        copy_every_object(&a.store, &copy);
        let id = a.binding.point_id.clone();
        noted(
            check(&plan_with(Some(a.binding.clone())), &copy, &a.keys),
            &id,
            &copy,
        );
        // And the original bucket, unchanged, needs no note at all.
        assert_eq!(
            check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
                .expect("the original proves"),
            proven(id, current_version(&a.store))
        );
    }

    /// **Coverage and the pin together.** What this build signs on a versioned
    /// bucket is a receipt with coverage PLUS the pin (format 1.2.0). A point-bound run
    /// over it hands the receipt's configuration coverage to phases 3 and 7
    /// whether the pin was checked (the bucket that signed it) or could not be
    /// (a byte-identical copy in an unversioned bucket): the note is added
    /// beside the coverage, never instead of it. Mutant: build the
    /// `VerifiedPoint` with UNKNOWN coverage when a note is set, and the copy
    /// half fails.
    #[test]
    fn a_pinned_point_hands_over_its_coverage_with_or_without_the_note() {
        use logweir_core::backup_receipt::{ConfigCoverage, TopicConfigCoverage};
        let (store, _bucket) = Store::in_memory_versioned(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let version = store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written")
            .version_id
            .expect("a versioned bucket names the version");
        let mut r = receipt(MANIFEST_KEY, &manifest_sha256);
        r.format_version =
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.into();
        r.archive.manifest_version_id = Some(version);
        r.config_coverage = Some(BTreeMap::from([(
            "orders".to_string(),
            TopicConfigCoverage {
                coverage: "captureDenied".into(),
                reason: None,
                timestamp_type: None,
            },
        )]));
        assert_eq!(r.validate_invariants(), Ok(()), "a 1.2.0 receipt with both");
        let receipt_bytes = serde_json::to_vec(&r).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);

        let original = check(&plan_with(Some(binding.clone())), &store, &keys)
            .expect("the signing bucket proves the point")
            .expect("the plan is bound");
        assert_eq!(original.pin_note, None, "the pin was checked here");
        assert_eq!(
            original.config_coverage.of("orders"),
            ConfigCoverage::CaptureDenied
        );

        let copy = Store::in_memory(ARCHIVE_PREFIX);
        copy_every_object(&store, &copy);
        let copied = check(&plan_with(Some(binding)), &copy, &keys)
            .expect("a copy of a signed point is that point")
            .expect("the plan is bound");
        assert!(
            copied
                .pin_note
                .as_deref()
                .is_some_and(|note| note.starts_with(POINT_PIN_UNCHECKED)),
            "{copied:?}"
        );
        assert_eq!(
            copied.config_coverage.of("orders"),
            ConfigCoverage::CaptureDenied,
            "the note never costs the run its verified coverage"
        );
    }

    /// **The versioned copy.** The same copy into a VERSIONED bucket,
    /// which issued its OWN ids: the pinned id is not in its history
    /// (`NotFound`), so the pin cannot be checked here.
    #[test]
    fn a_byte_identical_copy_in_a_versioned_bucket_with_its_own_ids_is_proven() {
        let (a, _bucket) = versioned_point(true);
        let (copy, copy_bucket) = Store::in_memory_versioned(ARCHIVE_PREFIX);
        copy_every_object(&a.store, &copy);
        let (_, original_version) = a
            .store
            .get_capped(
                MANIFEST_KEY,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .expect("the original reads");
        let (_, copy_version) = copy
            .get_capped(
                MANIFEST_KEY,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .expect("the copy reads");
        assert_ne!(
            copy_version, original_version,
            "the copy's version ids are its own (the precondition of this row)"
        );
        assert_eq!(copy_bucket.versions(MANIFEST_KEY).len(), 1);
        noted(
            check(&plan_with(Some(a.binding.clone())), &copy, &a.keys),
            &a.binding.point_id,
            &copy,
        );
    }

    /// The control: the same copy of a point that pins NOTHING is proven with
    /// no note — the note is about the pin, not about copies.
    #[test]
    fn a_copy_of_an_unpinned_point_is_proven_with_no_note() {
        let (u, _bucket) = versioned_point(false);
        let copy = Store::in_memory(ARCHIVE_PREFIX);
        copy_every_object(&u.store, &copy);
        assert_eq!(
            check(&plan_with(Some(u.binding.clone())), &copy, &u.keys)
                .expect("an unpinned point proves by its digest"),
            proven(u.binding.point_id.clone(), None)
        );
    }

    /// **The stated cost.** The set was written again in its ORIGINAL bucket
    /// (identical manifest bytes, a new version), and a lifecycle rule has
    /// since expired the version the receipt pins. To a reader that is the
    /// same answer as a copy — the pinned version is not in this bucket's
    /// history — so the point degrades to the unversioned case: proven by its
    /// digest, with the note, NOT refused. The rewrite the pin detects is the
    /// one whose pinned version is still retained.
    #[test]
    fn a_rewrite_whose_pinned_version_has_expired_degrades_to_the_digest_with_the_note() {
        let (store, bucket) = Store::in_memory_versioned(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written");
        bucket.overwrite(MANIFEST_KEY, &manifest);
        let mut r = receipt(MANIFEST_KEY, &manifest_sha256);
        // The version this receipt pinned, which lifecycle has expired: the
        // bucket's history no longer holds it.
        r.archive.manifest_version_id = Some("expired-by-lifecycle".to_string());
        r.format_version =
            logweir_core::backup_receipt::FORMAT_VERSION_WITH_MANIFEST_VERSION.into();
        let receipt_bytes = serde_json::to_vec(&r).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        let id = binding.point_id.clone();
        noted(
            check(
                &plan_with(Some(binding)),
                &store,
                &evidence_keys(vec![(&signer, trusted(&signer))]),
            ),
            &id,
            &store,
        );
    }

    /// A copy whose manifest bytes DIFFER is still refused, exit 3, by the
    /// digest — the fallback is the digest check, not a pass — and the refusal
    /// carries the note so the operator knows the pin was not what refused it.
    #[test]
    fn a_copy_whose_manifest_differs_is_refused_by_the_digest_with_the_note() {
        let (a, _bucket) = versioned_point(true);
        let copy = Store::in_memory(ARCHIVE_PREFIX);
        for key in a.store.list_keys("").expect("lists") {
            let bytes = if key == MANIFEST_KEY {
                br#"{"topics":["swapped"]}"#.to_vec()
            } else {
                a.store
                    .get_capped(&key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
                    .expect("reads")
                    .0
            };
            copy.put_create_only(&key, &bytes).expect("written");
        }
        let error = check(&plan_with(Some(a.binding.clone())), &copy, &a.keys)
            .expect_err("different bytes are not the attested manifest");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        assert!(rendered.contains("hashes to"), "{rendered}");
        assert!(rendered.contains(POINT_PIN_UNCHECKED), "{rendered}");
        assert!(!rendered.contains(pin::SUPERSEDED_CAUSE), "{rendered}");
    }

    /// A read of the pinned version that fails for any OTHER reason — a
    /// principal without `s3:GetObjectVersion`, an outage — is "could not
    /// tell": exit 1, never the note (which would prove the point by its
    /// digest over a rewrite nobody could rule out) and never exit 3.
    #[test]
    fn a_pinned_version_that_cannot_be_read_is_operational() {
        let (a, bucket) = versioned_point(true);
        bucket.overwrite(MANIFEST_KEY, br#"{"topics":[]}"#);
        bucket.fail_version_reads(
            "Generic S3 error: Error performing GET http://s3/k?versionId=v in 2ms - Server \
             returned non-2xx status code: 403 Forbidden: <Error><Code>AccessDenied</Code>",
        );
        let error = check(&plan_with(Some(a.binding.clone())), &a.store, &a.keys)
            .expect_err("a rewrite that cannot be ruled out is not proven");
        assert_eq!(
            error.exit_code(),
            crate::exit::ExitCode::Operational,
            "{error}"
        );
        let rendered = error.to_string();
        assert!(rendered.contains("s3:GetObjectVersion"), "{rendered}");
        assert!(!rendered.contains(POINT_PIN_UNCHECKED), "{rendered}");
    }

    /// The superseded refusal says what the current read
    /// ANSWERED. With a version id it names it; with none (suspended
    /// versioning's `null`, or a proxy that strips the header) it says so,
    /// rather than calling the current version "unversioned", which the reader
    /// could not observe. The cause is the same one sentence either way.
    #[test]
    fn the_superseded_refusal_says_what_the_current_read_answered() {
        let named = superseded_refusal("lwp1-x", MANIFEST_KEY, "v1", Some("v2"), true);
        assert!(named.starts_with(POINT_BINDING_MISMATCH), "{named}");
        assert!(named.contains(pin::SUPERSEDED_CAUSE), "{named}");
        assert!(
            named.contains("the point pins version v1; the current version is v2"),
            "{named}"
        );
        assert!(named.contains("still retained at that version"), "{named}");
        let none = superseded_refusal("lwp1-x", MANIFEST_KEY, "v1", None, false);
        assert!(none.contains(pin::SUPERSEDED_CAUSE), "{none}");
        assert!(
            none.contains("the point pins version v1; the current read answered no version id"),
            "{none}"
        );
        assert!(!none.contains("unversioned"), "{none}");
        assert!(
            none.contains("no longer hashes to the attested digest"),
            "{none}"
        );
    }

    /// **FX-36: one answer for a receipt that is absent, has other bytes or
    /// is over the read cap**, at the same single read. Each is exit 3 with
    /// the SAME message (the key and the bound digest, which the plan's
    /// author wrote, and nothing about what was found), so the refusal does
    /// not tell whether an object exists at the key. A store that does not
    /// answer is still exit 1 (the control: an `Io` is not folded in).
    ///
    /// KILLS: absent answered apart (exit 1, or another message); the digest
    /// mismatch naming the digest found; `TooLarge` reaching the store-failure
    /// arm; a second read before the refusal.
    #[test]
    fn an_absent_altered_or_oversized_receipt_gets_one_answer_at_one_read() {
        let a = archive();
        // One binding, whose digest no object below hashes to.
        let mut bound = a.binding.clone();
        bound.receipt_sha256 = format!("sha256:{}", "0".repeat(64));
        let refusal = |store: &Store| -> (String, Vec<String>) {
            let (store, log) = store.logging_reads();
            let error = check(&plan_with(Some(bound.clone())), &store, &a.keys)
                .expect_err("not the bound receipt");
            assert_eq!(
                error.exit_code(),
                crate::exit::ExitCode::GuardRefused,
                "{error}"
            );
            let DrillError::Guard(GuardRefusal(message)) = error else {
                panic!("a refusal, got {error}")
            };
            (message, log.reads())
        };
        // Absent: an archive that holds nothing at the key.
        let (absent, absent_reads) = refusal(&Store::in_memory(ARCHIVE_PREFIX));
        // Other bytes at the key.
        let (altered, altered_reads) = refusal(&a.store);
        // Over the cap: a store that reports the receipt larger than the cap.
        let (big, meter) =
            Store::in_memory_misreporting_size(ARCHIVE_PREFIX, caps::SIGNED_DOCUMENT + 1);
        big.put_create_only(RECEIPT_KEY, b"{}").expect("written");
        let (oversized, oversized_reads) = refusal(&big);
        assert_eq!(meter.streamed(), 0, "refused on the reported size");

        assert_eq!(absent, altered, "one answer");
        assert_eq!(altered, oversized, "one answer");
        assert!(
            altered.starts_with(&format!("{POINT_BINDING_MISMATCH}. ")),
            "{altered}"
        );
        assert!(
            !altered.contains(&a.binding.receipt_sha256) && !altered.contains("hashes to sha256"),
            "no digest of what was found: {altered}"
        );
        let one = vec![format!("get {RECEIPT_KEY}")];
        assert_eq!(absent_reads, one);
        assert_eq!(altered_reads, one);
        assert_eq!(oversized_reads, one);
    }

    /// **FX-36: the confinement, and NO store read.** A plan may name any text
    /// as its receipt key. Every key that is not the key `backup run` writes
    /// THIS plan's set's receipt at — another tenant's prefix, another set, a
    /// relative or nested path, an object that is not a receipt — is refused
    /// by name, exit 3, with the archive untouched: not the receipt, not its
    /// signature, not the manifest. Every key under `logweir/` HOLDS the bound
    /// receipt's bytes and their signature in this fixture, so a build that
    /// read them would find the digest it was told to expect and go on.
    ///
    /// CONTROL: the writer's own key, in the same fixture, is read (the
    /// receipt, its sidecar, the manifest) and the point is proven.
    ///
    /// KILLS: the confinement removed (`confine_receipt_key` not called: the
    /// planted keys are read and proven); the confinement moved after the
    /// receipt read (the read log is no longer empty).
    #[test]
    fn a_receipt_key_outside_the_plans_own_set_is_refused_with_no_store_read() {
        let a = archive();
        let receipt_bytes = a
            .store
            .get_capped(RECEIPT_KEY, caps::SIGNED_DOCUMENT)
            .expect("the fixture's receipt")
            .0;
        let sidecar_bytes = a
            .store
            .get_capped(
                &crate::catalog::cli::sidecar_key_of(RECEIPT_KEY),
                caps::SIDECAR,
            )
            .expect("its sidecar")
            .0;
        let not_a_receipt = [
            // another tenant's prefix, in a bucket shared by prefix
            "tenant-b/logweir/backups/nightly-7/run-1.receipt.json",
            "tenant-b/kafka-backups/nightly-7/manifest.json",
            // an absolute spelling
            "/logweir/backups/nightly-7/run-1.receipt.json",
            // relative and nested paths
            "logweir/backups/nightly-7/../nightly-8/run-1.receipt.json",
            "logweir/backups/nightly-7/../../../tenant-b/private.receipt.json",
            "logweir/backups/nightly-7/nested/run-1.receipt.json",
            // objects that are not a receipt
            "logweir/backups/nightly-7/run-1.receipt.sig",
            "logweir/backups/nightly-7/run-1.consumer-positions.json",
            "logweir/catalog/v1/points/lwp1-00000000000000000000000000000000/record.json",
            "logweir/drills/run-1.receipt.json",
            "logweir/nightly-7/manifest.json",
            "kafka-backups/nightly-7/topics/orders/partition=0/segment-0.bin",
        ];
        for key in not_a_receipt {
            if key.starts_with(logweir_engine_oso::storage::LOGWEIR_ROOT) && key != MANIFEST_KEY {
                let _ = a.store.put_create_only(key, &receipt_bytes);
                let _ = a
                    .store
                    .put_create_only(&crate::catalog::cli::sidecar_key_of(key), &sidecar_bytes);
            }
        }
        let (store, log) = a.store.logging_reads();
        for key in not_a_receipt {
            let mut binding = a.binding.clone();
            binding.receipt_key = key.to_string();
            let before = log.reads().len();
            let error = check(&plan_with(Some(binding)), &store, &a.keys)
                .expect_err("a key outside the plan's own set is refused");
            let DrillError::Guard(GuardRefusal(rendered)) = error else {
                panic!("{key}: a refusal, got {error}")
            };
            assert!(
                rendered.starts_with(&format!("{POINT_BINDING_MISMATCH}. "))
                    && rendered.contains("it was not read"),
                "{key}: {rendered}"
            );
            assert!(log.reads()[before..].is_empty(), "{key}: {:?}", log.reads());
        }

        // Another set's receipt, named in full: the plan restores `nightly-7`
        // and binds a point of `nightly-8`. The set refusal, still with
        // nothing read.
        let mut binding = a.binding.clone();
        binding.receipt_key = "logweir/backups/nightly-8/run-1.receipt.json".into();
        let _ = a
            .store
            .put_create_only(&binding.receipt_key, &receipt_bytes);
        let before = log.reads().len();
        let message = set_mismatch(check(&plan_with(Some(binding)), &store, &a.keys));
        assert!(
            message.contains("backup set `nightly-8`") && message.contains("names set `nightly-7`"),
            "{message}"
        );
        assert!(log.reads()[before..].is_empty(), "{:?}", log.reads());

        // CONTROL: the writer's key is read, and the point is proven.
        let before = log.reads().len();
        let id = a.binding.point_id.clone();
        assert_eq!(
            check(&plan_with(Some(a.binding.clone())), &store, &a.keys).expect("proven"),
            proven(id, None)
        );
        assert_eq!(
            log.reads()[before..],
            [
                format!("get {RECEIPT_KEY}"),
                format!("get {}", crate::catalog::cli::sidecar_key_of(RECEIPT_KEY)),
                format!("get {MANIFEST_KEY}"),
            ]
        );
    }

    #[test]
    fn a_malformed_binding_is_refused_before_the_archive_is_touched() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let signer = SigningKey::generate_ed25519();
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        let binding = PointBinding {
            point_id: "not-a-point".into(),
            receipt_key: "  ".into(),
            receipt_sha256: "nope".into(),
            manifest_sha256: "sha256:short".into(),
        };
        let error = check(&plan_with(Some(binding)), &store, &keys)
            .expect_err("a malformed binding is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        for field in [
            "point_id",
            "receipt_key",
            "receipt_sha256",
            "manifest_sha256",
        ] {
            assert!(rendered.contains(field), "{field} unnamed in: {rendered}");
        }
    }

    /// The bytes are the approved ones (the digest matched) and they are not a
    /// receipt: the fault is in the PLAN, so it is exit 3 and not exit 1.
    #[test]
    fn bytes_that_are_not_a_receipt_are_a_plan_refusal() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let signer = SigningKey::generate_ed25519();
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        let bytes = b"{\"not\":\"a receipt\"}".to_vec();
        store.put_create_only(RECEIPT_KEY, &bytes).expect("written");
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_sha256: format!("sha256:{}", "f".repeat(64)),
        };
        let error = check(&plan_with(Some(binding)), &store, &keys)
            .expect_err("non-receipt bytes are refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("are not a backup \nreceipt")
                || error.to_string().contains("are not a backup receipt"),
            "{error}"
        );
    }

    // The bound receipt's signature under current trust (D3 §5.5 step 6).
    // Every refusal row has the same point with one fact changed;
    // `a_point_the_archive_holds_is_proven_and_names_itself` is the control.

    fn untrusted(error: &DrillError, needle: &str) {
        assert_eq!(
            error.exit_code(),
            crate::exit::ExitCode::GuardRefused,
            "exit 3: {error}"
        );
        let rendered = error.to_string();
        assert!(rendered.contains(POINT_UNTRUSTED), "{rendered}");
        assert!(
            rendered.contains(needle),
            "{needle:?} missing from: {rendered}"
        );
        assert!(
            rendered.contains("no data operation was started"),
            "{rendered}"
        );
    }

    /// MUTANT: skip the keyring presence check (and so, the signature).
    #[test]
    fn a_point_bound_plan_with_no_evidence_keyring_is_refused() {
        let a = archive();
        let error = verify_point_binding(
            &plan_with(Some(a.binding.clone())),
            &a.store,
            None,
            chrono::Utc::now(),
        )
        .expect_err("no keyring, no run");
        untrusted(&error, "no evidence-signing keyring");
        // CONTROL: the same point with its keyring runs.
        assert!(check(&plan_with(Some(a.binding)), &a.store, &a.keys).is_ok());
        // An EMPTY keyring anchors in nothing either.
        let a = archive();
        let empty = serde_json::to_vec(&wire::EvidenceKeyring {
            format_version: wire::EVIDENCE_KEYRING_FORMAT_VERSION.to_string(),
            keys: vec![],
        })
        .expect("serialises");
        untrusted(
            &check(&plan_with(Some(a.binding)), &a.store, &empty).expect_err("empty"),
            "holds no key",
        );
    }

    /// MUTANT: treat a missing sidecar as "nothing to check".
    #[test]
    fn an_unsigned_receipt_is_refused() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        store
            .put_create_only(RECEIPT_KEY, &receipt_bytes)
            .expect("written");
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("written");
        let signer = SigningKey::generate_ed25519();
        let keys = evidence_keys(vec![(&signer, trusted(&signer))]);
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        untrusted(
            &check(&plan_with(Some(binding)), &store, &keys).expect_err("unsigned"),
            "carries no signature",
        );
    }

    /// MUTANT: accept the first key's verdict whatever it says, or skip
    /// `verify_detached`. A perfectly good signature by a key the namespace's
    /// trust does not list.
    #[test]
    fn a_receipt_signed_by_a_key_the_trust_does_not_list_is_refused() {
        let a = archive();
        let stranger = SigningKey::generate_ed25519();
        let keys = evidence_keys(vec![(&stranger, trusted(&stranger))]);
        untrusted(
            &check(&plan_with(Some(a.binding)), &a.store, &keys).expect_err("stranger"),
            "does not verify under any",
        );
    }

    /// MUTANT: drop the `decide` verdict. The signing key is listed -- and was
    /// revoked for compromise after the catalog synced.
    #[test]
    fn a_receipt_signed_by_a_key_revoked_for_compromise_is_refused() {
        let store = Store::in_memory(ARCHIVE_PREFIX);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        let signer = SigningKey::generate_ed25519();
        put_signed_receipt(&store, &receipt_bytes, &signer);
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("written");
        let mut revoked = trusted(&signer);
        revoked.state = logweir_core::trust::KeyState::Revoked;
        revoked.revoked_at = Some(chrono::Utc::now());
        revoked.revocation_effective_from = Some(chrono::Utc::now() - chrono::Duration::hours(2));
        revoked.revocation_reason = Some(logweir_core::trust::RevocationReason::KeyCompromise);
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        untrusted(
            &check(
                &plan_with(Some(binding.clone())),
                &store,
                &evidence_keys(vec![(&signer, revoked)]),
            )
            .expect_err("revoked"),
            "Revoked",
        );
        // CONTROL: the same key, active, runs.
        assert!(check(
            &plan_with(Some(binding)),
            &store,
            &evidence_keys(vec![(&signer, trusted(&signer))])
        )
        .is_ok());
    }

    /// A RETIRED key keeps what it signed while it was valid (D3 §7.4) and
    /// refuses what it "signed" after retirement -- judged against the
    /// receipt's own `finished_at`, which is the fixture's `now`.
    #[test]
    fn a_retired_key_verifies_only_what_it_signed_before_retirement() {
        let a = archive();
        let signer_keys: wire::EvidenceKeyring =
            serde_json::from_slice(&a.keys).expect("the fixture keyring");
        let mut before = signer_keys.clone();
        before.keys[0].trust.state = logweir_core::trust::KeyState::Retired;
        before.keys[0].trust.retired_at = Some(chrono::Utc::now() - chrono::Duration::hours(3));
        untrusted(
            &check(
                &plan_with(Some(a.binding.clone())),
                &a.store,
                &serde_json::to_vec(&before).expect("serialises"),
            )
            .expect_err("signed after retirement"),
            "SignedOutsideValidity",
        );
        // CONTROL: retired AFTER the receipt was signed -- historically valid,
        // and judged at a check time AFTER the retirement too, so the instant
        // judged is the receipt's own `finished_at` and not the check's clock
        // (a runner that judged "now" would refuse this).
        let mut after = signer_keys;
        after.keys[0].trust.state = logweir_core::trust::KeyState::Retired;
        after.keys[0].trust.retired_at = Some(chrono::Utc::now() + chrono::Duration::hours(3));
        assert!(verify_point_binding(
            &plan_with(Some(a.binding)),
            &a.store,
            Some(&serde_json::to_vec(&after).expect("serialises")),
            chrono::Utc::now() + chrono::Duration::days(10),
        )
        .is_ok());
    }

    /// Key-usage separation: a listed key that may only APPROVE never attests.
    #[test]
    fn a_receipt_signed_by_a_key_without_evidence_signing_usage_is_refused() {
        let a = archive();
        let mut keys: wire::EvidenceKeyring =
            serde_json::from_slice(&a.keys).expect("the fixture keyring");
        keys.keys[0].trust.usages = vec![KeyUsage::GovernedApproval];
        untrusted(
            &check(
                &plan_with(Some(a.binding)),
                &a.store,
                &serde_json::to_vec(&keys).expect("serialises"),
            )
            .expect_err("wrong usage"),
            "KeyUsageMismatch",
        );
    }

    use logweir_core::trust::KeyUsage;
    use logweir_evidence::keys::SigningKey;
    use logweir_evidence::sign::sign_detached;
}
