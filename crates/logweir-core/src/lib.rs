//! logweir-core — format types and the engine boundary. No I/O, no clock, no network.
#![forbid(unsafe_code)]

/// Semver of the scorecard document format. A minor adds optional
/// fields only; a major changes an identity rule.
///
/// `1.1.0` added `topic_parity.not_assessed` — the first
/// field added after the v0.1 tags, so a MINOR bump with a new schema file
/// beside the old one, as `docs/stability.md`'s "The v0.1.0 tag is the
/// compatibility boundary" requires. The payload type stays
/// `…drill-scorecard+json;version=1.0.0`: it names the major-1 envelope, and
/// changing it would make every existing reader refuse every new scorecard at
/// the payload-type comparison. A reader compares MAJORS only
/// (`Scorecard::refuse_unreadable_major`), so a 1.0.0 reader reads a 1.1.0
/// document and ignores the field; the signed 1.0.0 fixtures under
/// `e2e/fixtures/signed/` stay 1.0.0 and keep verifying.
///
/// `1.2.0` added `topic_parity.not_reconstructed`, and `1.3.0` added
/// `source.time_basis`: which source topics a restore selected by producer
/// time, and which it selected by time with no recorded timestamp type. Its
/// first minor is [`scorecard::TIME_BASIS_SINCE_MINOR`]; a renumber moves both,
/// the justfile's `scorecard_schema_version`, `docs/verify_scorecard.py`'s
/// `FORMAT_VERSION` and `SCORECARD_TIME_BASIS_SINCE_MINOR`, and the parity
/// script's `SCORECARD_TIME_BASIS_VERSION`.
///
/// `1.4.0` added `integrity.verification`: whether the
/// verdict covered a sample or every selected record, the structured gap and
/// pruned ranges, and a complete verification's archive integrity and replay
/// comparison. Its first minor is [`scorecard::VERIFICATION_SINCE_MINOR`]; a
/// renumber moves both, the justfile's `scorecard_schema_version`,
/// `docs/verify_scorecard.py`'s `FORMAT_VERSION` and
/// `SCORECARD_VERIFICATION_SINCE_MINOR`, and the parity script's
/// `SCORECARD_VERIFICATION_VERSION`.
pub const FORMAT_VERSION: &str = "1.4.0";

/// Ordinary confirmation and governed approval — the installation policy set,
/// the policy snapshot and authorization document v2, with `now` always an
/// argument.
pub mod approval_policy;
/// The approval scope — everything a second person must be shown
/// before they approve, as a pure function of the request the console signed
/// and the plan it names by hash; complete, or not approvable.
pub mod approval_scope;
pub mod backup_receipt;
pub mod check_contract;
pub mod connection;
/// The receipt's consumer position evidence (format 1.7.0), its
/// closed vocabularies and the rules its arms re-derive.
pub mod consumer_positions;
/// The one line a runner prints about a stopped creation step
/// (`target-topics-appeared=`), written and read through one definition.
pub mod creation_stop;
/// The credential binding for every credential reference — object
/// stores, retention and notification sinks — beside the Kafka connection's.
pub mod credential_binding;
pub mod destination;
pub mod det_json;
pub mod engine;
pub mod execution_contract;
pub mod guard;
pub mod ids;
/// Restore under the original topic name into an absent topic —
/// the conditions contract, the separate approval subject, and the
/// probe's scratch name.
pub mod original_name;
pub mod outcome;
pub mod positions_mapping;
/// The facts a reader takes from a backup receipt without building a
/// tree of it — the shared controller's bounded read.
pub mod receipt_facts;
/// Recovery-point selection over out-of-order timestamps: segment bounds
/// widened by their records, and the `PointInTimeSelectionIncomplete` refusal.
pub mod record_times;
/// A guard refusal's reason code and sentence as one validated,
/// cleaned and bounded `refusal-detail=` line.
pub mod refusal_detail;
/// The scope a standing rehearsal authorization signs over. Types only: the
/// `plan ∈ scope` predicate is not here.
pub mod rehearsal_scope;
/// The replay selection — an inclusive window start and per-topic
/// partition subsets — and the one function preview and execution select by.
pub mod replay_selection;
pub mod schema;
pub mod schema_dependency;
pub mod scorecard;
pub mod spec;
pub mod switchover;
/// The scorecard's `target.creation` block, a clone's declared lifetime
/// (`target.teardown: {ttl_seconds}`) and the rule its cleanup deletes by.
pub mod target_creation;
/// Which clock a restore's time selection reads per source topic, and
/// the `PointInTimeByProducerTime` refusal.
pub mod time_basis;
/// How many topics one backup may name and what its signed documents
/// may weigh — the one place every reader's cap and refusal derive from.
pub mod topic_budget;
/// The topic configuration model's portability table, the capture
/// rule and the detection of declarative owners.
pub mod topic_configuration;
/// The topic ID's canonical text, its closed sets, and the
/// generation rule two points' IDs are read by.
pub mod topic_identity;
/// A restore's target topic configuration in stages (PROD-05.2): the stage
/// table, the desired set and diff, and the phase-9 guard.
pub mod topic_transition;
/// Transaction markers in a restored archive, and the
/// `TransactionMarkersInArchive` refusal.
pub mod transaction_markers;
/// The trust lifecycle — `decide`, `may_sign_new` and `claimed_signing_time`,
/// with `now` always an argument.
pub mod trust;

#[cfg(test)]
mod tests {
    /// The one literal pin of the writer's version. Every
    /// other test derives the number from the constant, so a renumber is this
    /// line and the constant.
    #[test]
    fn format_version_is_one_four_zero() {
        assert_eq!(crate::FORMAT_VERSION, "1.4.0");
    }
}
