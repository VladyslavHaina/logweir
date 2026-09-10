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
//! success without reading anything back is this project's recurring defect
//! (`scripts/e2e-seed.sh`'s own header says so about its own steps).
//!
//! Global Constraint 6 is untouched: the archive handle is
//! `Store::read_only_from_url`'s, which physically cannot put.
use crate::backup::BackupError;
use logweir_core::backup_receipt::{
    BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
};
use logweir_core::engine::{BackupFacts, BackupPlan, DataEngine, PhaseObserver};
use logweir_engine_oso::storage::Store;
use std::collections::BTreeMap;
use std::path::Path;

/// Everything the run and the read-back established.
#[derive(Debug)]
pub struct Ran {
    pub facts: BackupFacts,
    pub manifest_key: String,
    /// `"sha256:<hex>"` over the EXACT bytes read back, via
    /// `logweir_core::ids::sha256_prefixed` (`ids.rs:11-13`) — never over a
    /// re-serialisation of anything.
    pub manifest_sha256: String,
    pub records_per_topic: BTreeMap<String, u64>,
    /// INCLUSIVE start of the covered window, epoch milliseconds: the oldest
    /// `start_timestamp` any segment of this backup set declares.
    pub covered_from_ms: i64,
    /// **EXCLUSIVE** end of the covered window, epoch milliseconds —
    /// interface **I22**, and the one unit conversion in this module.
    ///
    /// The manifest's newest `end_timestamp` is INCLUSIVE (a record exists at
    /// that millisecond); `Backup.status.windowCovered.toMs` and
    /// `BackupReceipt.covered.to_ms` are the exclusive end of a half-open
    /// range. So the measured value is that timestamp plus one millisecond,
    /// converted HERE, once, where the window is measured — see the argument
    /// in `run` below.
    pub covered_to_ms: i64,
}

pub fn run(
    plan: &BackupPlan,
    engine: &dyn DataEngine,
    store: &Store,
    obs: &mut dyn PhaseObserver,
) -> Result<Ran, BackupError> {
    obs.phase_started(-1, "backup");
    let facts = engine.backup(plan, obs);
    obs.phase_finished(
        -1,
        &match &facts {
            Ok(_) => "ok".to_string(),
            Err(e) => format!("failed: {e}"),
        },
    );
    let facts = facts?;

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
    let (manifest_bytes, _version_id) = store
        .get(&set.manifest_key)
        .map_err(|e| BackupError::Operational(e.to_string()))?;
    let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest_bytes);

    let archive = engine.describe(&set)?;

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
    let mut oldest: Option<i64> = None;
    let mut newest: Option<i64> = None;
    for topic in &archive.topics {
        let Some(entry) = records_per_topic.get_mut(&topic.name) else {
            continue;
        };
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
    // A set with no segment FOR ANY NAMED TOPIC bounds no window, and saying so
    // is the only honest answer — the same refusal `Store::manifest_facts` makes for the same
    // reason. An empty min/max would be published as a real window, and a
    // covered range of `[0, 0]` reads as "this archive covers the epoch".
    let (Some(covered_from_ms), Some(newest_inclusive)) = (oldest, newest) else {
        return Err(BackupError::Operational(format!(
            "backup set `{}` declares no segment for any of the named topics, so it bounds no \
             window: the engine exited 0 having captured nothing. Check that the named topics \
             hold records.",
            plan.backup_id
        )));
    };

    // **THE WINDOW IS HALF-OPEN, AND THIS IS WHERE IT BECOMES SO** (I22, and
    // Task 5's review finding F3).
    //
    // `segment.end_timestamp` is the timestamp of a record the archive HOLDS,
    // so the range it bounds is inclusive at both ends. The receipt and
    // `Backup.status.windowCovered` both carry an EXCLUSIVE `to_ms` —
    // `config/crd/backups.yaml` documented it that way before this task, and
    // `BackupReceipt::validate_invariants`'s arm 4 now requires
    // `from_ms < to_ms` strictly. A backup of a topic whose records share one
    // millisecond therefore has to become `[t, t+1)`: a one-millisecond window
    // containing exactly those records, rather than the empty `[t, t]` that
    // arm 4 refuses and that would make a legitimate single-record backup
    // unrepresentable.
    //
    // ONE conversion, in the ONE place that measures the window — the defect
    // `ReceiptCovered`'s own doc comment warns about is two representations of
    // one range with a conversion nobody owns. `saturating_add` and not `+ 1`:
    // an `end_timestamp` of `i64::MAX` is a manifest this build cannot read as
    // a timestamp, and saturating there keeps the arithmetic total rather than
    // panicking in a release build's wrapping.
    let covered_to_ms = newest_inclusive.saturating_add(1);

    Ok(Ran {
        facts,
        manifest_key: set.manifest_key,
        manifest_sha256,
        records_per_topic,
        covered_from_ms,
        covered_to_ms,
    })
}

// ---------------------------------------------------------------------------
// The receipt: built, validated, signed, put — and written locally on request.
// ---------------------------------------------------------------------------

/// Where the receipt landed. Both keys are printed as `backup run`'s final two
/// stdout lines (**I7**) and both are read by Task 17's reconciler.
#[derive(Debug, Clone)]
pub struct Persisted {
    /// `logweir/backups/<backup_id>/<run_id>.receipt.json`
    pub receipt_key: String,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.sig`
    pub sidecar_key: String,
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
    }
}

/// `BackupOutcome` -> the document. A pure projection: every field is a value
/// the outcome already carries, and nothing here measures anything.
///
/// `format_version` is the pinned `1.0.0` of THIS document type (independent
/// of the scorecard's), and `source.auth` is `BackupOutcome::source_auth`
/// rendered as the two strings `ReceiptAuth` holds — **never a password, and
/// no field that could hold one**.
pub fn build_receipt(outcome: &crate::backup::BackupOutcome) -> BackupReceipt {
    BackupReceipt {
        format_version: "1.0.0".to_string(),
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
            auth: receipt_auth(&outcome.source_auth),
            topics: outcome.topics.clone(),
        },
        engine: ReceiptEngine {
            id: outcome.engine.id.clone(),
            version: outcome.engine.version.clone(),
            digest: outcome.engine.digest.clone(),
        },
        archive: ReceiptArchive {
            manifest_key: outcome.manifest_key.clone(),
            manifest_sha256: outcome.manifest_sha256.clone(),
            prefix: outcome.archive_prefix.clone(),
        },
        records: outcome.records_per_topic.clone(),
        covered: ReceiptCovered {
            from_ms: outcome.covered_from_ms,
            // EXCLUSIVE (I22). The conversion happened in `run` above, once.
            to_ms: outcome.covered_to_ms,
        },
    }
}

/// `AuthRender` -> `ReceiptAuth`. The wire spellings, and the ONE place they
/// are chosen for this document.
///
/// **`scramSha512`, and there is ONE spelling in this product** (controller
/// ruling, Task 5b fix round 1; Task 6's review Ruling 3). Task 5 chose
/// `scram-sha-512` here on the argument that the RFC's own name is what a
/// Kafka operator recognises, and that argument does not survive contact with
/// the rest of the tree: `scramSha512` is `AuthSpec`'s `#[serde(tag =
/// "mode")]` value, so it is the string an adopter writes in a
/// `KafkaCluster`/`BackupSpec`; it is `KafkaCluster.spec.auth.mode`'s CRD enum
/// byte for byte (`crates/weirkeeper/tests/crd_shape.rs::
/// the_crd_auth_mode_enum_and_auth_spec_agree`); it is what
/// `Backup.status.auth.mode`'s own CRD description already promised while this
/// function wrote something else, and Task 17 copies THIS field into THAT one;
/// and it is the only value `AuthSpec::mode_str()` — the sole accessor any
/// receipt-writing code can fill the field from — can return. Two spellings
/// meant the same product signed two evidence documents describing one
/// mechanism by different names, with a landed test
/// (`crates/logweir/tests/auth_binding.rs::
/// the_scorecard_auth_block_and_auth_spec_agree`) asserting that
/// `"scram-sha-512"` does not parse at all.
///
/// The set is CLOSED at both readers since this round: `BackupReceipt::
/// validate_invariants`'s arm 5 and `docs/verify_scorecard.py::
/// check_backup_receipt_invariants`'s mirror refuse any third value, so this
/// literal cannot drift back without `logweir backup run` refusing its own
/// receipt before it signs it.
fn receipt_auth(render: &logweir_core::engine::AuthRender) -> ReceiptAuth {
    match render {
        logweir_core::engine::AuthRender::Plaintext => ReceiptAuth {
            mode: "plaintext".to_string(),
            // `None`, never `Some("")`: no username is not an empty username,
            // and `ReceiptAuth::username`'s own doc comment says so.
            username: None,
        },
        logweir_core::engine::AuthRender::ScramSha512 { username, .. } => ReceiptAuth {
            mode: "scramSha512".to_string(),
            username: Some(username.clone()),
        },
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
///    refuses a violating receipt true by EXECUTION (Task 5's review, F4)
///    rather than by assertion.
/// 2. **Serialise** deterministically — the exact bytes that will be signed,
///    stored and (with `--receipt-out`) written to disk. Never a
///    re-serialisation afterwards: a document re-rendered after signing does
///    not verify.
/// 3. **Sign.** Any failure from here to the end of step 4 is Global
///    Constraint 11's exit 4 — "signing or lock-proof failed, nothing
///    uploaded" — and the signing step precedes every put, which is the
///    mechanism rather than a convention.
/// 4. **Put**, create-only, both objects, under `logweir/` (GC6).
/// 5. **Write locally**, last, so a `--receipt-out` path that cannot be
///    written does not leave an operator wondering whether the evidence was
///    uploaded. It was: the two keys are already in the bucket by then.
pub fn persist_receipt(
    outcome: &crate::backup::BackupOutcome,
    signing_key: &Path,
    receipt_out: Option<&Path>,
    store: &Store,
) -> Result<Persisted, BackupError> {
    let sig = |e: String| BackupError::Signing(e);
    let receipt = build_receipt(outcome);

    // 1. Refuse to sign a self-contradicting document.
    receipt.validate_invariants().map_err(|e| {
        sig(format!(
            "the backup receipt this run measured violates its own invariants and was NOT \
             signed: {e}. The archive may exist; the evidence does not. This is a bug in \
             logweir, not in the spec — please report it with this line."
        ))
    })?;

    // 2. The EXACT bytes.
    let bytes = logweir_core::det_json::to_deterministic_json(&receipt)
        .map_err(|e| sig(format!("the backup receipt could not be serialised: {e}")))?;

    // 3. Sign. `logweir-evidence` is the ONE signer (Global Constraint 27):
    //    this is a call into it, exactly as `drill/phase8_score.rs` is, and no
    //    signing primitive lives here.
    let key = logweir_evidence::keys::SigningKey::from_pem_file(signing_key)
        .map_err(|e| sig(e.to_string()))?;
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &bytes,
    )
    .map_err(|e| sig(e.to_string()))?;
    let sidecar_bytes =
        serde_json::to_vec(&sidecar).map_err(|e| sig(format!("DSSE sidecar: {e}")))?;

    // 4. Create-only puts. An object that already exists is REFUSED
    //    (`StoreError::AlreadyExists`), never overwritten, so one run can
    //    never silently replace another's evidence.
    let keys = receipt_keys(&outcome.backup_id, &outcome.run_id);
    store
        .put_create_only(&keys.receipt_key, &bytes)
        .map_err(|e| sig(e.to_string()))?;
    store
        .put_create_only(&keys.sidecar_key, &sidecar_bytes)
        .map_err(|e| sig(e.to_string()))?;

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
    }

    Ok(keys)
}
