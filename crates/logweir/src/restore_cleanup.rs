//! **`logweir restore cleanup`: a TTL clone's cleanup** (PROD-11.1a; design
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §5). The controller runs it as
//! one Job at the clone's `expiresAt`, or when its `Restore` is deleted first.
//!
//! It deletes ONLY topics the run created, proven three ways, and refuses by
//! name anything else:
//!
//! 1. **The record.** The run's own scorecard, fetched from the plan's
//!    evidence store and verified (signature, invariants, `approval.plan_hash`
//!    = this plan's bytes, `run_id`), names what the run created in
//!    `target.creation`. No record, no deletion.
//! 2. **The cluster.** The target's cluster ID must be the scorecard's
//!    `target.cluster_id` (`CloneTargetClusterChanged`): a repointed
//!    `KafkaCluster` deletes nothing.
//! 3. **The topic ID.** A recorded name is deleted only while its current ID
//!    is the one recorded at creation (`CloneTargetReplaced` otherwise; an ID
//!    never recorded or not readable now is `CloneTargetIdentityUnknown`).
//!
//! The deleter is scoped to the plan's mapped target names (PROD-07.2), so a
//! name the plan does not map is refused even if a record named it.
//!
//! **PROD-07.2: an interrupted restore.** The same command removes what an
//! interrupted restore left: `--run-id` then names the reconcile's scorecard
//! (`logweir restore reconcile`), whose `target.creation` lists the topics
//! the interrupted run created and left, with the IDs it recorded. The rules
//! above are unchanged; the attestation's `teardown_policy` is `delete`.
//!
//! The outcome is the existing teardown attestation, `teardown_policy: ttl`,
//! signed and put create-only at `logweir/drills/<run_id>.cleanup.json` (or
//! `.cleanup-<n>.json` for attempt `n`), and one `clone-cleanup=` line.
//!
//! Exit codes: 0 when every recorded topic is deleted, already absent, or left
//! by the ID rule for good; 1 when a delete failed or an ID could not be read
//! (a later attempt may succeed); 3 when the cleanup refused by rule and
//! deleted nothing; 4 when the attestation could not be signed or put.

use crate::drill::DrillError;
use crate::exit::ExitCode;
use crate::signer::ValidatedSigner;
use logweir_core::guard::GuardRefusal;
use logweir_core::scorecard::Scorecard;
use logweir_core::spec::DrillSpec;
use logweir_core::target_creation::{
    check_cluster, cleanup_attestation_key, cleanup_plan, CloneCleanupLeft, CloneCleanupReport,
    CurrentId,
};
use logweir_engine_oso::storage::{caps, Store, StoreError};
use logweir_evidence::keys::VerifyingKey;
use logweir_kafka::reader::{ClusterReader, TopicDeleter};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// `logweir restore cleanup`'s inputs.
#[derive(Debug, Clone)]
pub struct CleanupArgs {
    /// The approved restore plan (the run's own `--spec`).
    pub spec: PathBuf,
    /// The restore run whose clone this removes.
    pub run_id: String,
    /// Signs the cleanup attestation; its public half also verifies the
    /// run's scorecard.
    pub signing_key: PathBuf,
    /// More evidence-signing keys the scorecard may verify under (the
    /// installation's keyring), when the signing key rotated since the run.
    pub evidence_keys: Option<PathBuf>,
    /// Which attempt this is (1 to 3); it names the attestation's key.
    pub attempt: u32,
    /// D2 §3.5's store-contract handshake, as `restore run` takes it.
    pub store_contract_version: Option<String>,
    /// The grant the scorecard is read with, as the controller names it
    /// (`--evidence-read-grant`, D1); `None` for a hand-run.
    pub evidence_read_grant: Option<String>,
}

/// `logweir restore cleanup`: run, print the `clone-cleanup=` line (and a
/// refusal's two lines), and exit by the code table in the module doc.
pub fn run(args: CleanupArgs) -> ExitCode {
    let cleaned = execute(&args);
    // FX-43: `finish` writes its lines into a buffer, and they go out once,
    // last, through the terminal block: sealed with the Job's token, the
    // sealed `runner-exit=`, then the plain lines.
    let mut buffer = Vec::new();
    let code = finish(&args.run_id, cleaned, &mut buffer);
    let mut last = crate::exit::FinalLines::default();
    last.extend(String::from_utf8_lossy(&buffer).lines().map(str::to_string));
    last.print(code);
    code
}

/// What one cleanup attempt did, and whether a later attempt may do more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cleaned {
    /// The `clone-cleanup=` line's report.
    pub report: CloneCleanupReport,
    /// A delete failed or an ID could not be read: exit 1, and the controller
    /// tries again. The ID rule's refusals (a replaced topic, an ID never
    /// recorded) are final and do not set it.
    pub retry: bool,
}

/// Print what `cleaned` says and map it to the exit code.
pub fn finish(
    run_id: &str,
    cleaned: Result<Cleaned, DrillError>,
    out: &mut dyn std::io::Write,
) -> ExitCode {
    match cleaned {
        Ok(Cleaned { report, retry }) => {
            let _ = writeln!(out, "{}", report.line());
            if retry {
                tracing::warn!(
                    run_id = %run_id,
                    left = report.left.len(),
                    "the clone's cleanup left topics a later attempt may remove"
                );
                ExitCode::Operational
            } else {
                ExitCode::Ok
            }
        }
        Err(DrillError::Guard(refusal)) => {
            let report = CloneCleanupReport {
                run_id: run_id.to_string(),
                refused: Some(refusal.0.clone()),
                ..CloneCleanupReport::default()
            };
            let _ = writeln!(out, "{}", report.line());
            let mut w: &mut dyn std::io::Write = &mut *out;
            let _ = crate::exit::print_refusal_to(
                &mut w,
                logweir_core::refusal_detail::RefusingRun::Restore,
                None,
                &refusal.0,
            );
            ExitCode::GuardRefused
        }
        Err(e) => {
            // On one line: the error can repeat a plan value or a broker's
            // message. It goes on the report line too (D1), so the clone's
            // status says why the attempt failed, not only that it did.
            eprintln!("{}", crate::exit::one_line(&e.to_string()));
            let why = match &e {
                // Its own word first (`EvidenceUnreadable: …`), without the
                // `operational: ` the exit code already says.
                DrillError::Operational(m) => m.clone(),
                other => other.to_string(),
            };
            // Cleaned before it is printed, as a refusal's detail is: the
            // controller cleans again, and relies on its own pass.
            let report = CloneCleanupReport {
                run_id: run_id.to_string(),
                error: Some(logweir_core::refusal_detail::clean_message(
                    &crate::exit::one_line(&why),
                )),
                ..CloneCleanupReport::default()
            };
            let _ = writeln!(out, "{}", report.line());
            e.exit_code()
        }
    }
}

/// Build the real inputs (plan, signer, evidence store, target client) and
/// [`clean`].
///
/// # Errors
///
/// [`DrillError`] by the module doc's code table.
pub fn execute(args: &CleanupArgs) -> Result<Cleaned, DrillError> {
    let plan_bytes = std::fs::read(&args.spec).map_err(|e| {
        DrillError::Operational(format!("restore plan {}: {e}", args.spec.display()))
    })?;
    let spec: DrillSpec = serde_yaml::from_slice(&plan_bytes)
        .map_err(|e| DrillError::Operational(format!("drill spec does not parse: {e}")))?;
    refuse_shape(&spec)?;
    crate::credential_binding::check_projected_bindings()?;
    crate::drill::check_projected_credentials()?;
    let signer = ValidatedSigner::load(
        &args.signing_key,
        logweir_evidence::PAYLOAD_TYPE_TEARDOWN,
        b"logweir restore signing readiness probe v1",
        "No evidence was uploaded",
    )
    .map_err(DrillError::SigningPrerequisite)?;
    let mut trusted = vec![signer.verifying_key()];
    if let Some(path) = &args.evidence_keys {
        trusted.extend(keyring(path)?);
    }
    let contract = crate::backup::store_contract::admit(args.store_contract_version.as_deref())?;
    let write = if contract {
        let archive = crate::backup::store_contract::archive_options()?;
        let evidence = crate::backup::store_contract::evidence_options(&archive)?;
        Store::from_url_with(&spec.evidence, &evidence)
    } else {
        Store::from_url(&spec.evidence)
    }
    .map_err(|e| DrillError::Operational(e.to_string()))?;
    // D1: the scorecard is read with the evidence destination's read grant
    // (the archive slot), the attestation written with its write grant.
    let read = crate::evidence_read::read_store(&spec.evidence, contract)
        .map_err(DrillError::Operational)?;
    let store = SplitStores {
        read,
        write,
        grant: args
            .evidence_read_grant
            .clone()
            .unwrap_or_else(|| crate::evidence_read::AMBIENT_GRANT.to_string()),
    };
    let target = target_client(&spec)?;
    clean(
        &Inputs {
            spec: &spec,
            plan_bytes: &plan_bytes,
            run_id: &args.run_id,
            attempt: args.attempt,
            trusted: &trusted,
        },
        &target,
        &target,
        &store,
        &signer,
    )
}

/// A clone shape the runner refuses is refused here too. Whether the plan's
/// run may be cleaned at all is the signed record's to say ([`clean`]).
fn refuse_shape(spec: &DrillSpec) -> Result<(), DrillError> {
    if let Some(refusal) = logweir_core::target_creation::refuse_teardown_shape(spec) {
        return Err(GuardRefusal(refusal).into());
    }
    Ok(())
}

/// The plan's mapped target names: the only names the cleanup's deleter may
/// delete (PROD-07.2).
#[must_use]
pub fn mapped_target_names(spec: &DrillSpec) -> Vec<String> {
    let prefix = logweir_core::spec::target_topic_prefix(spec);
    spec.source
        .topics
        .iter()
        .map(|t| format!("{prefix}{t}"))
        .collect()
}

/// The evidence keyring's public keys.
fn keyring(path: &std::path::Path) -> Result<Vec<VerifyingKey>, DrillError> {
    let bytes = std::fs::read(path).map_err(|e| {
        DrillError::Operational(format!("evidence keyring {}: {e}", path.display()))
    })?;
    let ring: logweir_core::execution_contract::EvidenceKeyring = serde_json::from_slice(&bytes)
        .map_err(|e| GuardRefusal(format!("the evidence keyring does not parse: {e}")))?;
    Ok(ring
        .keys
        .iter()
        .filter_map(|k| VerifyingKey::from_pem_str(&k.public_key_pem).ok())
        .collect())
}

/// The restore target's client, built as `restore run` builds it (interface
/// I1's auth, the projected CA and client certificate), with deletion scoped
/// to the plan's scratch prefix and the source topics' own names protected.
fn target_client(
    spec: &DrillSpec,
) -> Result<logweir_kafka::rdkafka_reader::RdKafkaReader, DrillError> {
    let ca = crate::tls_ca::projected_ca_file(crate::tls_ca::TARGET_TLS_CA_FILE_VAR)
        .map_err(DrillError::Operational)?;
    let certificate = crate::tls_ca::projected_client_certificate(crate::tls_ca::Side::Target)
        .map_err(DrillError::Operational)?;
    let auth = logweir_kafka::reader::AuthConfig::from_spec(
        &spec.target.auth,
        crate::drill::validated_password(crate::drill::TARGET_PASSWORD_VAR)?,
    )
    .map_err(|e| crate::drill::naming_the_password_var(e, crate::drill::TARGET_PASSWORD_VAR))?
    .with_tls_ca_file(ca)?
    .with_client_certificate(certificate)?;
    let reader = logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &spec.target.bootstrap_servers,
        auth,
    )?
    .with_deletable_names(mapped_target_names(spec));
    Ok(reader)
}

/// **Where the cleanup reads the run's scorecard and writes its attestation**
/// (D1): a [`Store`] for both (a hand-run, a test), or [`SplitStores`], the
/// read grant's handle and the write grant's, for a controller-built Job.
pub trait EvidenceStores {
    /// One capped read, with the grant [`EvidenceStores::grant`] names.
    ///
    /// # Errors
    ///
    /// The store's.
    fn read(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError>;
    /// The handle the attestation is written to.
    fn writer(&self) -> &Store;
    /// The read grant, as a failure names it.
    fn grant(&self) -> &str {
        crate::evidence_read::AMBIENT_GRANT
    }
}

impl EvidenceStores for Store {
    fn read(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        self.get_capped(key, max_bytes).map(|(bytes, _)| bytes)
    }
    fn writer(&self) -> &Store {
        self
    }
}

/// The read grant's handle and the write grant's, with the read grant's name.
pub struct SplitStores {
    /// Built from the archive slot: the evidence destination's read grant.
    pub read: Store,
    /// Built from the evidence slot: its write grant.
    pub write: Store,
    /// `evidenceRead of BackupDestination <ns>/<name>`, as the controller
    /// named it.
    pub grant: String,
}

impl EvidenceStores for SplitStores {
    fn read(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        self.read.get_capped(key, max_bytes).map(|(bytes, _)| bytes)
    }
    fn writer(&self) -> &Store {
        &self.write
    }
    fn grant(&self) -> &str {
        &self.grant
    }
}

/// What [`clean`] reads besides the clients.
pub struct Inputs<'a> {
    /// The approved plan, parsed.
    pub spec: &'a DrillSpec,
    /// The plan's bytes, whose digest the scorecard's `approval.plan_hash`
    /// must be.
    pub plan_bytes: &'a [u8],
    /// The run whose clone this removes.
    pub run_id: &'a str,
    /// The attempt, 1 to 3.
    pub attempt: u32,
    /// The keys the scorecard may verify under.
    pub trusted: &'a [VerifyingKey],
}

/// **The cleanup**, over seams: verify the run's record, check the cluster,
/// read each recorded topic's ID, delete by [`cleanup_plan`], and sign what
/// happened.
///
/// # Errors
///
/// [`DrillError::Guard`] for a refusal (nothing deleted),
/// [`DrillError::Operational`] or [`DrillError::Kafka`] when the store or the
/// cluster did not answer, [`DrillError::SigningOrLock`] when the attestation
/// could not be signed or put.
pub fn clean(
    inputs: &Inputs<'_>,
    reader: &dyn ClusterReader,
    deleter: &dyn TopicDeleter,
    store: &dyn EvidenceStores,
    signer: &ValidatedSigner,
) -> Result<Cleaned, DrillError> {
    refuse_shape(inputs.spec)?;
    let (bytes, sc) = verified_scorecard(inputs, store)?;
    let refuse = |why: String| -> DrillError { GuardRefusal(why).into() };
    let Some(creation) = sc.target.creation.as_ref() else {
        return Err(refuse(format!(
            "the run's scorecard (format {}) records no created topic (target.creation), so \
             nothing proves which topics the run created; nothing was deleted",
            sc.format_version
        )));
    };
    let is_clone = creation.teardown_after_seconds.is_some() && sc.target.mode.is_scratch();
    // PROD-07.2: a reconcile's scorecard, which lists what an interrupted run
    // left. Its note is signed, so a plan cannot make one up.
    let is_reconcile = sc
        .sample
        .coverage_note
        .starts_with(crate::drill::reconcile::NOTE_PREFIX);
    if !is_clone && !is_reconcile {
        return Err(refuse(
            "the run's scorecard records neither a clone (target.creation.teardown_after_seconds \
             in scratch mode) nor the reconciliation of an interrupted run; nothing was deleted"
                .to_string(),
        ));
    }
    let prefix = logweir_core::spec::target_topic_prefix(inputs.spec);
    if sc.target.topic_mapping_prefix != prefix {
        return Err(refuse(format!(
            "the run's scorecard maps through {:?} and this plan through {prefix:?}; nothing was \
             deleted",
            sc.target.topic_mapping_prefix
        )));
    }
    let current_cluster = reader.cluster_id()?;
    check_cluster(&sc.target.cluster_id, &current_cluster).map_err(refuse)?;
    let names: Vec<String> = creation.topics.iter().map(|t| t.name.clone()).collect();
    let current: BTreeMap<String, CurrentId> = reader
        .topic_ids(&names)?
        .into_iter()
        .map(|(name, read)| {
            let now = match read {
                logweir_kafka::topic_ids::TopicIdRead::Id(id) => CurrentId::Id(id),
                logweir_kafka::topic_ids::TopicIdRead::NotFound => CurrentId::NotFound,
                other => CurrentId::Unread(
                    other
                        .to_id_read()
                        .reason()
                        .unwrap_or(logweir_core::topic_identity::READ_FAILED)
                        .to_string(),
                ),
            };
            (name, now)
        })
        .collect();
    let plan = cleanup_plan(creation, &current);
    let mut deleted = Vec::new();
    let mut failed: Vec<(String, String)> = Vec::new();
    if !plan.delete.is_empty() {
        // The IDs were read just above, on this same client, and nothing else
        // runs between that read and this request. librdkafka deletes by name,
        // so a topic deleted and created again under a recorded name inside
        // that window would still be deleted; no delete-by-ID exists to close
        // it (docs/kubernetes.md, "Safe clones").
        match deleter.delete_topics(&plan.delete) {
            Ok(results) => {
                for (name, result) in results {
                    match result {
                        Ok(()) => deleted.push(name),
                        Err(e) => failed.push((name, e)),
                    }
                }
            }
            Err(e) => {
                for name in &plan.delete {
                    failed.push((name.clone(), e.to_string()));
                }
            }
        }
    }
    let left: Vec<(String, String)> = plan
        .skipped
        .iter()
        .chain(plan.unreadable.iter())
        .chain(failed.iter())
        .cloned()
        .collect();
    let attestation = crate::drill::phase9_teardown::TeardownAttestation {
        run_id: sc.run_id.clone(),
        target_mode: sc.target.mode,
        scorecard_sha256: logweir_core::ids::sha256_prefixed(&bytes),
        teardown_policy: if is_clone {
            logweir_core::target_creation::TEARDOWN_POLICY_TTL.to_string()
        } else {
            "delete".to_string()
        },
        topics_deleted: deleted.clone(),
        topics_failed: left.clone(),
        // Global Constraint 1: the clock is read here, in `crates/logweir`.
        deleted_at: chrono::Utc::now(),
        // The cleanup applies no configuration (PROD-05.2's transition is
        // the run's own phase 9).
        topic_configuration: None,
    };
    let key = cleanup_attestation_key(&sc.run_id, inputs.attempt);
    persist(&attestation, &key, signer, store.writer())?;
    Ok(Cleaned {
        retry: !plan.unreadable.is_empty() || !failed.is_empty(),
        report: CloneCleanupReport {
            run_id: sc.run_id.clone(),
            deleted,
            already_absent: plan.already_absent,
            left: left
                .into_iter()
                .map(|(topic, reason)| CloneCleanupLeft { topic, reason })
                .collect(),
            attestation_key: Some(key),
            refused: None,
            error: None,
        },
    })
}

/// The run's scorecard and its bytes, verified: the DSSE signature under one
/// of the trusted keys, the invariants, the run id, and `approval.plan_hash`
/// against this plan's bytes.
fn verified_scorecard(
    inputs: &Inputs<'_>,
    store: &dyn EvidenceStores,
) -> Result<(Vec<u8>, Scorecard), DrillError> {
    let refuse = |why: String| -> DrillError { GuardRefusal(why).into() };
    let run_id = inputs.run_id;
    // FX-36: the run id names the only key read here, so it is one run-id
    // segment, refused by name before any read.
    if !logweir_core::ids::is_run_id_segment(run_id) {
        return Err(refuse(format!(
            "--run-id {run_id:?} is not a run id; nothing was read and nothing was deleted"
        )));
    }
    let key = format!("logweir/drills/{run_id}.json");
    let sidecar_key = format!("logweir/drills/{run_id}.sig");
    let read = |key: &str, max_bytes: u64| -> Result<Vec<u8>, DrillError> {
        match store.read(key, max_bytes) {
            Ok(bytes) => Ok(bytes),
            Err(StoreError::NotFound(_)) => Err(refuse(format!(
                "{key} is not in the evidence store: without the run's signed record nothing \
                 proves which topics it created; nothing was deleted"
            ))),
            // D1: a store's refusal names the key, the grant and its class.
            Err(e) => Err(DrillError::Operational(
                crate::evidence_read::unreadable(key, &e, store.grant())
                    .map_or_else(|| format!("{key} could not be read: {e}"), |u| u.sentence),
            )),
        }
    };
    let bytes = read(&key, caps::SIGNED_DOCUMENT)?;
    let sidecar_bytes = read(&sidecar_key, caps::SIDECAR)?;
    let sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar_bytes)
        .map_err(|e| refuse(format!("{sidecar_key} is not a DSSE sidecar ({e})")))?;
    let verified = inputs.trusted.iter().any(|k| {
        logweir_evidence::verify::verify_detached(
            k,
            logweir_evidence::PAYLOAD_TYPE_SCORECARD,
            &bytes,
            &sidecar,
        )
        .is_ok()
    });
    if !verified {
        return Err(refuse(format!(
            "{key}'s signature does not verify under the signing key or the evidence keyring; \
             nothing was deleted"
        )));
    }
    let sc: Scorecard = serde_json::from_slice(&bytes).map_err(|e| {
        refuse(format!(
            "{key} is not a scorecard ({e}); nothing was deleted"
        ))
    })?;
    sc.validate_invariants().map_err(|e| {
        refuse(format!(
            "{key} contradicts itself ({}); nothing was deleted",
            e.0
        ))
    })?;
    if sc.run_id != run_id {
        return Err(refuse(format!(
            "{key} names run {:?}, not {run_id:?}; nothing was deleted",
            sc.run_id
        )));
    }
    let plan_hash = logweir_core::ids::sha256_prefixed(inputs.plan_bytes);
    if sc.approval.plan_hash != plan_hash {
        return Err(refuse(format!(
            "{key} records plan {} and this plan is {plan_hash}: the scorecard is another \
             plan's run; nothing was deleted",
            sc.approval.plan_hash
        )));
    }
    Ok((bytes, sc))
}

/// Sign the attestation with `PAYLOAD_TYPE_TEARDOWN` and put it, and its
/// sidecar, create-only at `key` (`.json` → `.sig`).
fn persist(
    att: &crate::drill::phase9_teardown::TeardownAttestation,
    key: &str,
    signer: &ValidatedSigner,
    store: &Store,
) -> Result<(), DrillError> {
    let sig = |e: String| DrillError::SigningOrLock(e);
    let bytes =
        logweir_core::det_json::to_deterministic_json(att).map_err(|e| sig(e.to_string()))?;
    let sidecar = signer
        .sign(logweir_evidence::PAYLOAD_TYPE_TEARDOWN, &bytes)
        .map_err(|e| sig(e.to_string()))?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(|e| sig(e.to_string()))?;
    store
        .put_create_only(key, &bytes)
        .map_err(|e| sig(e.to_string()))?;
    let sidecar_key = format!("{}.sig", key.trim_end_matches(".json"));
    store
        .put_create_only(&sidecar_key, &sidecar_bytes)
        .map_err(|e| sig(e.to_string()))?;
    Ok(())
}
