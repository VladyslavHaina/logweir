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
/// `1.2.0` since FX-3, which added `topic_parity.not_reconstructed`: the
/// source settings a `newTopic` restore did not reconstruct, which phase 7
/// labelled `intentionally_deviated` with a scratch-only rationale in every
/// mode. Its first minor is [`scorecard::NOT_RECONSTRUCTED_SINCE_MINOR`]; a
/// renumber moves both, the justfile's `scorecard_schema_version`, and
/// `docs/verify_scorecard.py`'s two constants.
pub const FORMAT_VERSION: &str = "1.2.0";

/// PLAT-19.2 / decision D0: ordinary confirmation and governed approval —
/// the installation policy set, the policy snapshot and authorization
/// document v2, with `now` always an argument.
pub mod approval_policy;
pub mod backup_receipt;
pub mod check_contract;
pub mod connection;
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
pub mod schema;
pub mod scorecard;
pub mod spec;
/// PLAT-19.1 / decision D3 §7.4: the trust lifecycle — `decide`,
/// `may_sign_new` and `claimed_signing_time`, with `now` always an argument.
pub mod trust;

#[cfg(test)]
mod tests {
    /// The one literal pin of the writer's version (FX-3: 1.2.0). Every other
    /// test derives the number from the constant, so a renumber is this line
    /// and the constant.
    #[test]
    fn format_version_is_one_two_zero() {
        assert_eq!(crate::FORMAT_VERSION, "1.2.0");
    }
}
