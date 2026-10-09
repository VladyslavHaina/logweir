//! logweir-core — format types and the engine boundary. No I/O, no clock, no network.
#![forbid(unsafe_code)]

/// Semver of the scorecard document format (spec §6.1). A minor adds optional
/// fields only; a major changes an identity rule.
///
/// `1.1.0` since FX-4, which added `topic_parity.not_assessed` — the first
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
/// `1.3.0` since FX-8, which added `source.time_basis`: which source topics a
/// restore selected by producer time, and which it selected by time with no
/// recorded timestamp type (1.2.0 is FX-3's, `topic_parity.not_reconstructed`).
/// Its first minor is
/// [`scorecard::TIME_BASIS_SINCE_MINOR`]; a renumber moves both, the justfile's
/// `scorecard_schema_version`, `docs/verify_scorecard.py`'s `FORMAT_VERSION`
/// and `SCORECARD_TIME_BASIS_SINCE_MINOR`, and the parity script's
/// `SCORECARD_TIME_BASIS_VERSION`.
///
/// `1.4.0` since PROD-08.1, which added `integrity.verification`: whether the
/// verdict covered a sample or every selected record, the structured gap and
/// pruned ranges, and a complete verification's archive integrity and replay
/// comparison. Its first minor is [`scorecard::VERIFICATION_SINCE_MINOR`]; a
/// renumber moves both, the justfile's `scorecard_schema_version`,
/// `docs/verify_scorecard.py`'s `FORMAT_VERSION` and
/// `SCORECARD_VERIFICATION_SINCE_MINOR`, and the parity script's
/// `SCORECARD_VERIFICATION_VERSION`.
pub const FORMAT_VERSION: &str = "1.4.0";

/// PLAT-19.2 / decision D0: ordinary confirmation and governed approval —
/// the installation policy set, the policy snapshot and authorization
/// document v2, with `now` always an argument.
pub mod approval_policy;
pub mod backup_receipt;
pub mod check_contract;
pub mod connection;
/// PROD-04.1: the receipt's consumer position evidence (format 1.5.0), its
/// closed vocabularies and the rules its arms re-derive.
pub mod consumer_positions;
/// FX-20: the credential binding for every credential reference — object
/// stores, retention and notification sinks — beside PROD-01.3's Kafka one.
pub mod credential_binding;
pub mod destination;
pub mod det_json;
pub mod engine;
pub mod execution_contract;
pub mod guard;
pub mod ids;
pub mod outcome;
/// PLAT-19.1 / decision D3 §4.3: the scope a standing rehearsal
/// authorization signs over. Types only — the `plan ∈ scope` predicate is
/// W7's, against W5's execution contract v2.
pub mod rehearsal_scope;
/// PROD-11.1: the replay selection — an inclusive window start and per-topic
/// partition subsets — and the one function preview and execution select by.
pub mod replay_selection;
pub mod schema;
pub mod scorecard;
pub mod spec;
/// FX-8: which clock a restore's time selection reads per source topic, and
/// the `PointInTimeByProducerTime` refusal.
pub mod time_basis;
/// PROD-05.1: the topic configuration model's portability table, the capture
/// rule and the detection of declarative owners.
pub mod topic_configuration;
/// PLAT-19.1 / decision D3 §7.4: the trust lifecycle — `decide`,
/// `may_sign_new` and `claimed_signing_time`, with `now` always an argument.
pub mod trust;

#[cfg(test)]
mod tests {
    /// The one literal pin of the writer's version (PROD-08.1: 1.4.0). Every
    /// other test derives the number from the constant, so a renumber is this
    /// line and the constant.
    #[test]
    fn format_version_is_one_four_zero() {
        assert_eq!(crate::FORMAT_VERSION, "1.4.0");
    }
}
