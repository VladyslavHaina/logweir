//! Phase 9 — delete the scratch topics the drill created, and attest what
//! actually happened.
//!
//! The attestation has one job the rest of the drill cannot do for it: say
//! honestly whether teardown completed. A topic the broker refused to delete
//! is recorded in `topics_failed` and is NEVER listed in `topics_deleted`, so
//! the signed document can never assert a clean state the cluster is not in.
//! A teardown failure is a WARNING at the call site, never a change of
//! outcome: the drill result is already signed and uploaded by phase 8, and
//! leaving scratch topics behind is an operational annoyance rather than a
//! false claim.
//!
//! What is NOT allowed is silence, and until T0-11 was closed that is exactly
//! what an operator got. The attestation was correct and nobody read it: a JSON
//! blob in an object store nobody opens unless they already suspect something.
//! The warning now travels on the THREE channels an operator actually watches,
//! on the exact run that left the topics behind:
//!
//! * the metric — `logweir_drill_teardown_topics_failed{cluster}`, emitted by
//!   `crate::metrics::write_textfile` on every scorecard-carrying path,
//!   unconditionally, so `0` means "phase 9 cleaned up" and only the whole
//!   file's absence means "no drill result";
//! * the log — one `WARN` from `drill::teardown`, carrying the run id and the
//!   failed topic NAMES both as a field and in the message; and
//! * the console — a clause on `drill run`'s summary line, appended only when
//!   the count is non-zero, so a clean run's line is byte-identical to before.
//!
//! On a CLEAN teardown none of the three says anything special, and that is
//! deliberate: the gauge reads `0`, the summary line is unchanged, and the log
//! carries phase 9's ordinary `phase started` / `phase finished` pair from
//! `record` — which is what tells an operator phase 9 ran at all. (The engine
//! child is spawned with `RUST_LOG=warn` pinned and contributes nothing to a
//! clean run's stream, so those two lines are the whole of it.)
use crate::drill::DrillError;
use crate::signer::ValidatedSigner;
use logweir_core::spec::TargetMode;
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::TopicDeleter;
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TeardownAttestation {
    pub run_id: String,
    /// WHICH restore this was — `scratch` or `newTopic` (interface **I33**).
    ///
    /// It is on the attestation because the attestation's whole job is to say
    /// honestly what phase 9 did, and "nothing, and nothing was owed" is a
    /// different fact from "nothing, and the policy said keep". Without it, an
    /// auditor reading `{topics_deleted: [], topics_failed: []}` cannot tell a
    /// point-in-time recovery — where deleting the restored topic would delete
    /// the recovery — from a scratch drill whose teardown silently did nothing.
    ///
    /// `#[serde(default)]` so a teardown document written before this field
    /// existed still parses, defaulting to `scratch`, which is what every such
    /// document was.
    #[serde(default)]
    pub target_mode: TargetMode,
    /// `sha256:<hex>` of the SIGNED scorecard bytes — not the run id again.
    /// Binding the attestation to `run_id` twice would carry no independent
    /// information and could not identify WHICH signed document this teardown
    /// accompanies.
    pub scorecard_sha256: String,
    pub teardown_policy: String,
    pub topics_deleted: Vec<String>,
    pub topics_failed: Vec<(String, String)>,
    pub deleted_at: chrono::DateTime<chrono::Utc>,
}

/// Teardown goes through `logweir_kafka::reader::TopicDeleter`, NOT through
/// rdkafka directly: `crates/logweir` declares no rdkafka dependency and adding
/// one would break the layering rule that `logweir-kafka` is the only crate
/// that dials a broker. The deleter takes exact names from
/// `topic_mapping.values()` — never a pattern, never a prefix.
pub fn run(
    deleter: &dyn TopicDeleter,
    mapping: &BTreeMap<String, String>,
    policy: &str,
    mode: TargetMode,
    run_id: &str,
    scorecard_sha256: &str,
) -> TeardownAttestation {
    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    // **PHASE 9 TEARS DOWN IN `scratch` MODE AND NOWHERE ELSE** (Global
    // Constraint 19: "tag 1 deletes nothing but the scratch topics a
    // `mode: scratch` restore created, in phase 9"; spec §6.1).
    //
    // The mode is checked BEFORE the policy, and it is not an `&&` inside the
    // existing condition, because the two conditions mean different things and
    // an operator reading this has to be able to tell them apart: `policy`
    // is the adopter's choice for a scratch drill, while the mode is a
    // structural fact about what phase 9 is even allowed to do. In `newTopic`
    // mode the mapped topics ARE the recovery — deleting them at any value of
    // `target.teardown` would delete the thing the restore was run to produce,
    // on a real cluster, immediately after signing a document saying it
    // succeeded.
    //
    // `deleter` is untouched on this path, which is what
    // `crates/logweir/tests/restore_mode.rs::
    // a_new_topic_restore_tears_nothing_down` asserts through a recording
    // double: `calls` empty after a `newTopic` run, non-empty after a
    // `scratch` one.
    if mode == TargetMode::Scratch && policy == "delete" {
        let names: Vec<String> = mapping.values().cloned().collect();
        match deleter.delete_topics(&names) {
            Ok(results) => {
                for (name, r) in results {
                    match r {
                        Ok(()) => deleted.push(name),
                        Err(e) => failed.push((name, e)),
                    }
                }
            }
            // The whole call failed, so nothing is known to have been
            // deleted: every mapped topic is attested as failed rather than
            // the attestation falling silent about them.
            Err(e) => {
                for n in mapping.values() {
                    failed.push((n.clone(), e.to_string()));
                }
            }
        }
    }
    TeardownAttestation {
        run_id: run_id.into(),
        target_mode: mode,
        scorecard_sha256: scorecard_sha256.into(),
        teardown_policy: policy.into(),
        topics_deleted: deleted,
        topics_failed: failed,
        // Global Constraint 1: the clock is read HERE, in `crates/logweir`,
        // never in `logweir-core`.
        deleted_at: chrono::Utc::now(),
    }
}

/// The one note prefix [`failure_notes`] writes and [`failed_count`] parses.
///
/// It exists so the two are never two spellings of the same idea. Change it and
/// both move together; hand-write it at either site and they can drift.
pub const TEARDOWN_FAILED_NOTE_PREFIX: &str = "teardown-failed: ";

/// The topic names the broker refused, in `topics_failed` order.
///
/// Names ONLY, never the broker error strings, so a log line and a phase note
/// cannot disagree about what "the failed topics" means. The errors are long,
/// broker-authored and useful — they go into the notes, where length is not a
/// log-line concern.
pub fn failed_topic_names(att: &TeardownAttestation) -> Vec<String> {
    att.topics_failed.iter().map(|(n, _)| n.clone()).collect()
}

/// `None` when nothing failed. `Some(msg)` naming EVERY failed topic, in the
/// exact wording the `warn!` emits.
///
/// A pure function rather than a `format!` inlined at the call site, for one
/// reason: the message is then testable without a subscriber and identical with
/// one, and `teardown_failure_names_the_topics` asserts exactly that equality.
/// A message built at the call site could drift from anything a test asserts on
/// and nobody would find out.
///
/// The names are in the message and not merely counted, because an operator who
/// reads "teardown failed for 1 topic" still has to go and discover WHICH one,
/// on a cluster whose topic list is not theirs to guess at. The count is
/// already the metric's job.
pub fn teardown_warning(att: &TeardownAttestation) -> Option<String> {
    let names = failed_topic_names(att);
    if names.is_empty() {
        return None;
    }
    Some(format!(
        "teardown left {} scratch {} behind on the target cluster: {}",
        names.len(),
        if names.len() == 1 { "topic" } else { "topics" },
        names.join(", ")
    ))
}

/// The notes phase 9 writes onto its own `PhaseRecord`, one per failed topic.
///
/// Empty when nothing failed, so a clean drill's phase-9 record is byte-identical
/// to the one this repository produced before T0-11 was closed.
pub fn failure_notes(att: &TeardownAttestation) -> Vec<String> {
    att.topics_failed
        .iter()
        .map(|(topic, error)| format!("{TEARDOWN_FAILED_NOTE_PREFIX}{topic}: {error}"))
        .collect()
}

/// The count `metrics::write_textfile` and `drill::summary_line` both read,
/// derived from the scorecard's own phase-9 record. `0` when phase 9 did not
/// run at all.
///
/// WHY THE COUNT TRAVELS ON `PhaseRecord.notes` AND NOT ON A NEW `Scorecard`
/// FIELD — this is the load-bearing design note of T0-11, and it is three
/// facts:
///
/// 1. **It costs no `format_version` event.** `notes: Vec<String>` already
///    exists on `PhaseRecord`. Global Constraint 12 fixes `format_version` at
///    `1.0.0` and lets a minor add optional fields only; a new `Scorecard`
///    field would be a schema change, a golden regeneration and a corpus case,
///    and this task does not own that dance.
/// 2. **No signature has to change to deliver it.** Both terminal paths that
///    reach `finish()` — `Ok(sc)` and `Err(DrillError::NotPass(sc))` — become
///    `Some(&Scorecard)` in `report` and arrive at `finish` through `publish`'s
///    `Some(sc) => finish(args, sc)` arm. The scorecard is already in hand at
///    every point that needs the count, so `execute`, `execute_with`, `report`,
///    `publish`, `finish` and `DrillError` are all untouched.
/// 3. **Nothing written here can reach the signed artifact.** Phase 9's record
///    is pushed AFTER phase 8 froze and signed the bytes;
///    `write_scorecard_artifact` writes `signed.bytes` and never a
///    re-serialisation; and `signed_last_phase_completed` filters `p.phase < 8`.
///    So the notes are a fact about the run that the signed document is
///    incapable of carrying, which is the same reason the teardown attestation
///    is a separate signed document in the first place.
pub fn failed_count(sc: &logweir_core::scorecard::Scorecard) -> u64 {
    sc.phases
        .iter()
        .find(|p| p.phase == 9)
        .map(|p| {
            p.notes
                .iter()
                .filter(|n| n.starts_with(TEARDOWN_FAILED_NOTE_PREFIX))
                .count() as u64
        })
        .unwrap_or(0)
}

/// The failed topic names read back out of a phase-9 record's notes.
///
/// The summary line names the topics, and the only place they survive to that
/// point is the notes `failure_notes` wrote — the attestation itself is long
/// gone by the time `summary_line` runs. A Kafka topic name cannot contain
/// `:`, so the first `": "` is unambiguously the boundary between the name and
/// the broker's error.
pub fn failed_topic_names_from_notes(sc: &logweir_core::scorecard::Scorecard) -> Vec<String> {
    sc.phases
        .iter()
        .find(|p| p.phase == 9)
        .map(|p| {
            p.notes
                .iter()
                .filter_map(|n| n.strip_prefix(TEARDOWN_FAILED_NOTE_PREFIX))
                .map(|rest| rest.split_once(": ").map_or(rest, |(name, _)| name))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn sig<E: std::fmt::Display>(e: E) -> DrillError {
    DrillError::SigningOrLock(e.to_string())
}

/// Signed with `PAYLOAD_TYPE_TEARDOWN` and put create-only next to the
/// scorecard. Without this the payload type would be a media type nothing ever
/// emits, and the segregation evidence — which scratch topics were actually
/// deleted — would never be persisted.
///
/// Signing precedes both puts here for the same reason it does in phase 8, and
/// the same `SigningOrLock` variant carries the failure. It does **not** reach
/// the process exit code: phase 8 has already signed and uploaded the drill
/// result by the time this runs, so the call site (`drill::teardown`) logs a
/// warning and the drill's exit code is unchanged. Whether a teardown that
/// cannot be attested *should* also change the exit code is an open ruling,
/// recorded under "Known limitations" in `docs/stability.md`; stage-2 ruling
/// R-C forbids answering it here.
pub fn persist(
    att: &TeardownAttestation,
    signing_key: &std::path::Path,
    store: &Store,
) -> Result<(), DrillError> {
    let signer = ValidatedSigner::load(
        signing_key,
        logweir_evidence::PAYLOAD_TYPE_TEARDOWN,
        b"logweir restore signing readiness probe v1",
        "No evidence was uploaded",
    )
    .map_err(sig)?;
    persist_with_signer(att, &signer, store)
}

pub(crate) fn persist_with_signer(
    att: &TeardownAttestation,
    signer: &ValidatedSigner,
    store: &Store,
) -> Result<(), DrillError> {
    let bytes = logweir_core::det_json::to_deterministic_json(att).map_err(sig)?;
    let sidecar = signer
        .sign(logweir_evidence::PAYLOAD_TYPE_TEARDOWN, &bytes)
        .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;
    store
        .put_create_only(
            &format!("logweir/drills/{}.teardown.json", att.run_id),
            &bytes,
        )
        .map_err(sig)?;
    store
        .put_create_only(
            &format!("logweir/drills/{}.teardown.sig", att.run_id),
            &sidecar_bytes,
        )
        .map_err(sig)?;
    Ok(())
}
