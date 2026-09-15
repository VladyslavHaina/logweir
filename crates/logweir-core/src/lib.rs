//! logweir-core — format types and the engine boundary. No I/O, no clock, no network.
#![forbid(unsafe_code)]

/// Semver of the scorecard document format (spec §6.1). A minor adds optional
/// fields only; a major changes an identity rule.
pub const FORMAT_VERSION: &str = "1.0.0";

pub mod backup_receipt;
pub mod det_json;
pub mod engine;
pub mod execution_contract;
pub mod guard;
pub mod ids;
pub mod outcome;
pub mod schema;
pub mod scorecard;
pub mod spec;

#[cfg(test)]
mod tests {
    #[test]
    fn format_version_is_one_zero_zero() {
        assert_eq!(crate::FORMAT_VERSION, "1.0.0");
    }
}
