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

/// The SOURCE side's client-certificate variables (PROD-01.3, `mtls`).
pub const SOURCE_TLS_CERT_FILE_VAR: &str = logweir_core::connection::SOURCE_TLS_CERT_FILE_ENV;
/// See [`SOURCE_TLS_CERT_FILE_VAR`].
pub const SOURCE_TLS_KEY_FILE_VAR: &str = logweir_core::connection::SOURCE_TLS_KEY_FILE_ENV;
/// The TARGET side's client-certificate variables (PROD-01.3, `mtls`).
pub const TARGET_TLS_CERT_FILE_VAR: &str = logweir_core::connection::TARGET_TLS_CERT_FILE_ENV;
/// See [`TARGET_TLS_CERT_FILE_VAR`].
pub const TARGET_TLS_KEY_FILE_VAR: &str = logweir_core::connection::TARGET_TLS_KEY_FILE_ENV;

/// Which runner side a connection is dialled for — selects the variable pair
/// [`projected_client_certificate`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// `logweir cluster-probe` and `logweir backup run`.
    Source,
    /// `logweir restore run` / `drill run` and `doctor`.
    Target,
}

/// Read one side's projected client-certificate pair (PROD-01.3): `Ok(None)`
/// when NEITHER variable is set (or both are blank), `Ok(Some(files))` when
/// both are.
///
/// **Paths only, never opened here**, for the reason the CA is not: both
/// clients load the files themselves (`ssl.certificate.location` /
/// `ssl.key.location`, the engine's `ssl_certificate_location` /
/// `ssl_key_location`) and name the path in their error. So no Logweir
/// process ever holds the private key's bytes — it can reach no log, status
/// or document because nothing here has it.
///
/// # Errors
///
/// Exactly one of the two set ([`ClientCertificateRefusal::Incomplete`]'s
/// text), or a value that is not UTF-8. The message names variables only.
///
/// [`ClientCertificateRefusal::Incomplete`]: logweir_core::connection::ClientCertificateRefusal::Incomplete
pub fn projected_client_certificate(
    side: Side,
) -> Result<Option<logweir_core::connection::ClientCertificateFiles>, String> {
    let (cert_var, key_var) = match side {
        Side::Source => (SOURCE_TLS_CERT_FILE_VAR, SOURCE_TLS_KEY_FILE_VAR),
        Side::Target => (TARGET_TLS_CERT_FILE_VAR, TARGET_TLS_KEY_FILE_VAR),
    };
    // The same blank-is-unset rule as the CA variable (E19(e)).
    let cert = projected_path(cert_var)?;
    let key = projected_path(key_var)?;
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(cert_file), Some(key_file)) => {
            Ok(Some(logweir_core::connection::ClientCertificateFiles {
                cert_file,
                key_file,
            }))
        }
        _ => Err(format!(
            "{} (`{cert_var}`, `{key_var}`); nothing was dialled",
            logweir_core::connection::ClientCertificateRefusal::Incomplete
        )),
    }
}

/// [`projected_ca_file`]'s rule for any file variable, with a message that
/// names the variable and not a CA.
fn projected_path(var: &str) -> Result<Option<String>, String> {
    match std::env::var(var) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "`{var}` is not valid UTF-8, so it cannot name the file both TLS clients load; \
             nothing was dialled"
        )),
    }
}
