//! logweir-core — format types and the engine boundary. No I/O, no clock, no network.
#![forbid(unsafe_code)]

/// Semver of the scorecard document format (spec §6.1). A minor adds optional
/// fields only; a major changes an identity rule.
pub const FORMAT_VERSION: &str = "1.0.0";

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
    #[test]
    fn format_version_is_one_zero_zero() {
        assert_eq!(crate::FORMAT_VERSION, "1.0.0");
    }
}
