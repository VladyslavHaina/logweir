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

// ---------------------------------------------------------------------------
// PROD-01.3: the auth-mode vocabulary, and the two transport refusals.
// ---------------------------------------------------------------------------

/// `plaintext`: no SASL, no TLS.
pub const AUTH_MODE_PLAINTEXT: &str = "plaintext";
/// `scramSha512`: SASL/SCRAM-SHA-512, over TLS or not.
pub const AUTH_MODE_SCRAM_SHA_512: &str = "scramSha512";
/// `scramSha256`: SASL/SCRAM-SHA-256, over TLS or not (PROD-01.3).
pub const AUTH_MODE_SCRAM_SHA_256: &str = "scramSha256";
/// `plain`: SASL/PLAIN, over TLS ONLY (PROD-01.3). PLAIN sends the password
/// itself on the wire, so a `plain` connection without TLS is refused
/// ([`PlainWithoutTls`]) rather than dialled.
pub const AUTH_MODE_PLAIN: &str = "plain";
/// `mtls`: a TLS client certificate and no SASL (PROD-01.3). TLS by
/// definition, and still switched on by `tls: true` like every other mode: a
/// `mtls` connection with `tls: false` is refused ([`MtlsWithoutTls`]).
pub const AUTH_MODE_MTLS: &str = "mtls";

/// The two modes every Logweir release has written, in the order the
/// documents have always named them. A receipt below format 1.3.0, a scorecard
/// below 1.4.0 and a catalog point below 1.3.0 carry one of these and nothing
/// else.
pub const ORIGINAL_AUTH_MODES: [&str; 2] = [AUTH_MODE_PLAINTEXT, AUTH_MODE_SCRAM_SHA_512];

/// The three modes PROD-01.3 adds. A signed document that names one declares
/// the format version that defines it (receipt and catalog point 1.3.0,
/// scorecard 1.4.0); an older reader refuses such a document as naming a value
/// outside its closed set, which is the safer verdict (OD-7, third case).
pub const PROD_01_3_AUTH_MODES: [&str; 3] =
    [AUTH_MODE_SCRAM_SHA_256, AUTH_MODE_PLAIN, AUTH_MODE_MTLS];

/// Every mode this build implements, in the order the documents name them.
pub const AUTH_MODES: [&str; 5] = [
    AUTH_MODE_PLAINTEXT,
    AUTH_MODE_SCRAM_SHA_512,
    AUTH_MODE_SCRAM_SHA_256,
    AUTH_MODE_PLAIN,
    AUTH_MODE_MTLS,
];

/// Whether `mode` is one of [`PROD_01_3_AUTH_MODES`] — the test every
/// versioned document uses to decide which format version it must declare.
#[must_use]
pub fn is_prod_01_3_auth_mode(mode: &str) -> bool {
    PROD_01_3_AUTH_MODES.contains(&mode)
}

/// The environment variable naming the client CERTIFICATE file of an `mtls`
/// connection, SOURCE side (the probe and the backup runner). Its value is a
/// path inside the pod (the projected client-certificate volume), never
/// certificate text. Set only for `mtls`.
pub const SOURCE_TLS_CERT_FILE_ENV: &str = "LOGWEIR_SOURCE_TLS_CERT_FILE";
/// The client PRIVATE KEY file of an `mtls` connection, SOURCE side. A path,
/// never key material: the key reaches both clients as a file the kubelet
/// projected, and no Logweir process reads its bytes.
pub const SOURCE_TLS_KEY_FILE_ENV: &str = "LOGWEIR_SOURCE_TLS_KEY_FILE";
/// The TARGET side's twin of [`SOURCE_TLS_CERT_FILE_ENV`] — the restore runner.
pub const TARGET_TLS_CERT_FILE_ENV: &str = "LOGWEIR_TARGET_TLS_CERT_FILE";
/// The TARGET side's twin of [`SOURCE_TLS_KEY_FILE_ENV`].
pub const TARGET_TLS_KEY_FILE_ENV: &str = "LOGWEIR_TARGET_TLS_KEY_FILE";

/// **The named reason** for refusing SASL/PLAIN without TLS — the runner's
/// `refusal-reason=` value (`crate::guard::TERMINAL_STATE_PLAIN_WITHOUT_TLS`)
/// and the controller's terminal state, spelled once.
pub const PLAIN_WITHOUT_TLS: &str = "PlainWithoutTls";

/// Why a `plain` connection without TLS is refused.
///
/// SASL/PLAIN carries the password itself in the authentication exchange.
/// Without TLS that is the credential in the clear on every connection, which
/// no setting of Logweir's makes acceptable, so the connection is refused at
/// every layer that can see it (the CRD's admission rule, the controller's
/// resolver, the runner's guard, both clients' builders) instead of dialled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlainWithoutTls;

impl std::fmt::Display for PlainWithoutTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{PLAIN_WITHOUT_TLS}: auth mode `plain` is SASL/PLAIN, which sends the password \
             itself to the broker, and this connection does not set `tls: true`; SASL/PLAIN is \
             accepted only over TLS, so the connection is refused rather than dialled in the \
             clear. Set `tls: true` for a SASL_SSL listener, or use `scramSha256` / \
             `scramSha512` on a listener without TLS"
        )
    }
}

impl std::error::Error for PlainWithoutTls {}

/// Why a `mtls` connection without TLS is refused: a client certificate is
/// presented in a TLS handshake, so there is nothing to dial without one, and
/// guessing that `tls: false` meant `true` would make the one switch that
/// turns TLS on mean two things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MtlsWithoutTls;

impl std::fmt::Display for MtlsWithoutTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "auth mode `mtls` authenticates with a TLS client certificate and this connection \
             does not set `tls: true`; `tls` is the only switch that turns TLS on, so set it, or \
             choose a mode that does not need TLS"
        )
    }
}

impl std::error::Error for MtlsWithoutTls {}

/// The pod-local client certificate and key files of an `mtls` connection —
/// PATHS, never PEM text, exactly as the CA file is. Both clients load them
/// (the engine's `ssl_certificate_location` / `ssl_key_location`,
/// librdkafka's `ssl.certificate.location` / `ssl.key.location`); no Logweir
/// process opens them.
///
/// `Debug` prints the two paths: they name where the kubelet mounted a
/// projected volume, which is public, and never the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCertificateFiles {
    /// The PEM client certificate (chain).
    pub cert_file: String,
    /// The PEM private key, unencrypted (the engine's loader reads PKCS#1,
    /// PKCS#8 or SEC1 and takes no passphrase).
    pub key_file: String,
}

/// Why client-certificate files cannot be attached to, or are missing from, a
/// connection's auth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCertificateRefusal {
    /// Files were supplied for a mode that presents no client certificate.
    /// Refused rather than ignored: an operator who projected a certificate
    /// believes it is being presented.
    NotMtls {
        /// The mode as the documents spell it.
        mode: &'static str,
    },
    /// `mtls` with no files: there is no identity to present.
    Missing,
    /// Only one of the two variables is set.
    Incomplete,
}

impl std::fmt::Display for ClientCertificateRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientCertificateRefusal::NotMtls { mode } => write!(
                f,
                "a TLS client certificate was supplied for a `{mode}` connection; only auth mode \
                 `mtls` presents one, and a certificate the connection would silently not \
                 present is refused instead"
            ),
            ClientCertificateRefusal::Missing => write!(
                f,
                "auth mode `mtls` needs a client certificate and key, and none was projected \
                 into this process; set the *_TLS_CERT_FILE and *_TLS_KEY_FILE variables of the \
                 side being dialled (the controller projects them from \
                 auth.clientCertificate). Nothing was refused: this is operational"
            ),
            ClientCertificateRefusal::Incomplete => write!(
                f,
                "only one of the client certificate and key files was projected; mTLS needs both \
                 the *_TLS_CERT_FILE and the *_TLS_KEY_FILE variable of the side being dialled"
            ),
        }
    }
}

impl std::error::Error for ClientCertificateRefusal {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mode_sets_partition_the_five_modes() {
        let mut joined: Vec<&str> = ORIGINAL_AUTH_MODES.to_vec();
        joined.extend(PROD_01_3_AUTH_MODES);
        assert_eq!(joined, AUTH_MODES.to_vec());
        for mode in ORIGINAL_AUTH_MODES {
            assert!(!is_prod_01_3_auth_mode(mode), "{mode}");
        }
        for mode in PROD_01_3_AUTH_MODES {
            assert!(is_prod_01_3_auth_mode(mode), "{mode}");
        }
    }

    #[test]
    fn the_plain_refusal_opens_with_its_named_reason() {
        let text = PlainWithoutTls.to_string();
        assert!(text.starts_with("PlainWithoutTls: "), "{text}");
        assert!(
            text.contains("`plain`") && text.contains("tls: true"),
            "{text}"
        );
    }

    #[test]
    fn the_client_certificate_variables_are_distinct_and_logweir_prefixed() {
        let all = [
            SOURCE_TLS_CERT_FILE_ENV,
            SOURCE_TLS_KEY_FILE_ENV,
            TARGET_TLS_CERT_FILE_ENV,
            TARGET_TLS_KEY_FILE_ENV,
            SOURCE_TLS_CA_FILE_ENV,
            TARGET_TLS_CA_FILE_ENV,
        ];
        let set: std::collections::BTreeSet<_> = all.iter().collect();
        assert_eq!(set.len(), all.len());
        for name in all {
            assert!(name.starts_with("LOGWEIR_"), "{name}");
        }
    }

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
