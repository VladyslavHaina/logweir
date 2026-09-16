//! The projected private CA a connection's TLS clients trust — PLAT-07.1,
//! Global Constraint 29's runner half.
//!
//! A runner dials Kafka with TWO TLS clients: Logweir's own librdkafka reader
//! and the engine the runner spawns. Each has its own trust store — librdkafka
//! uses the image's `ca-certificates`, the engine its bundled webpki roots —
//! so a broker certificate signed by a private CA verifies in neither unless
//! both are told about the CA. The controller projects the saved connection's
//! `auth.tlsCa` as a read-only file and names it in one environment variable
//! per side; this module is the one place that variable is read, and the path
//! it returns is handed to BOTH clients (`AuthConfig::with_tls_ca_file` for
//! librdkafka's `ssl.ca.location`, `AuthRender::with_tls_ca_file` for the
//! engine's `ssl_ca_location`), so the two cannot disagree about what they
//! trust.
//!
//! THE FILE IS NOT OPENED HERE. Both clients open it themselves while building
//! their TLS context and name `ssl.ca.location` / the CA path in the error when
//! it is missing or holds no certificate, which is a better message than a
//! second reader could give — and it keeps the probe a process that opens no
//! file of its own (`crates/logweir/tests/cluster_probe.rs`).

/// The SOURCE side's variable — `logweir cluster-probe` and `logweir backup run`.
pub const SOURCE_TLS_CA_FILE_VAR: &str = logweir_core::connection::SOURCE_TLS_CA_FILE_ENV;

/// The TARGET side's variable — `logweir restore run` / `drill run` and `doctor`.
pub const TARGET_TLS_CA_FILE_VAR: &str = logweir_core::connection::TARGET_TLS_CA_FILE_ENV;

/// Read one CA-file variable: `Ok(None)` when unset or blank, `Ok(Some(path))`
/// otherwise.
///
/// **A BLANK VALUE IS UNSET** — plan erratum E19(e), the ruling every other
/// environment read in this workspace follows: a Kubernetes `env:` entry with
/// an empty `value:` reads as `Ok("")`, and a CA location of `""` is not a
/// file anybody meant.
///
/// # Errors
///
/// A value that is not valid UTF-8. The message names the variable only.
pub fn projected_ca_file(var: &str) -> Result<Option<String>, String> {
    match std::env::var(var) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "`{var}` is not valid UTF-8, so it cannot name the CA file both TLS clients load; \
             nothing was dialled"
        )),
    }
}
