//! Pure names for the saved-connection runner contract (PLAT-07.1).
//!
//! `weirkeeper::connection` resolves a `KafkaCluster` into the environment and
//! mounts a runner Job carries; the `logweir` runner reads them back by these
//! names. The strings live here, in the pure layer both crates already link,
//! for the reason `execution_contract` gives: one wire contract, no
//! controller-to-runner crate dependency.
//!
//! # What crosses the boundary, and what does not
//!
//! A connection reaches a runner as REFERENCES resolved by the kubelet in the
//! Job's own namespace — a `secretKeyRef` for the SASL password and a projected
//! file for a private CA — plus public settings (bootstrap servers, auth mode,
//! username, TLS) carried by the plan or the argv. No credential value is ever
//! a literal in a Job, a ConfigMap, a rendered document or a status.

/// The saved-connection contract version this build resolves and runs.
///
/// A resolution names the version it was produced under
/// (`weirkeeper::connection::ResolvedConnection::contract_version`), so an
/// immutable run snapshot records which field set it was built from.
pub const CONTRACT_VERSION: &str = "v1";

/// The environment variable naming the CA certificate file for the SOURCE
/// side of a run — the probe and the backup runner.
///
/// Its value is a path inside the pod (the projected CA volume), never
/// certificate text, and it is set only when the connection names
/// `auth.tlsCa`. Absent, both TLS clients keep their default trust stores.
pub const SOURCE_TLS_CA_FILE_ENV: &str = "LOGWEIR_SOURCE_TLS_CA_FILE";

/// The TARGET side's twin of [`SOURCE_TLS_CA_FILE_ENV`] — the restore runner.
pub const TARGET_TLS_CA_FILE_ENV: &str = "LOGWEIR_TARGET_TLS_CA_FILE";

/// Why a private CA file cannot be attached to a connection's auth: the
/// transport it would verify is not TLS.
///
/// A CA with no TLS transport is not a harmless extra: an object whose author
/// supplied a trust anchor believes the connection is verified, and dialling
/// it in the clear is the silent downgrade this refusal exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsCaWithoutTls {
    /// The auth mode as the documents spell it (`plaintext`, `scramSha512`).
    pub mode: &'static str,
}

impl std::fmt::Display for TlsCaWithoutTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a TLS CA file was supplied for a `{}` connection without TLS; a CA only verifies a \
             TLS transport, and dialling in the clear while a trust anchor is configured would \
             be a silent downgrade, so the connection is refused instead",
            self.mode
        )
    }
}

impl std::error::Error for TlsCaWithoutTls {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_ca_variables_are_distinct_and_logweir_prefixed() {
        assert_ne!(SOURCE_TLS_CA_FILE_ENV, TARGET_TLS_CA_FILE_ENV);
        for name in [SOURCE_TLS_CA_FILE_ENV, TARGET_TLS_CA_FILE_ENV] {
            assert!(name.starts_with("LOGWEIR_") && name.ends_with("_TLS_CA_FILE"));
        }
    }

    #[test]
    fn the_refusal_names_the_mode_and_not_a_path() {
        let text = TlsCaWithoutTls { mode: "plaintext" }.to_string();
        assert!(text.contains("`plaintext`") && text.contains("without TLS"));
    }
}
