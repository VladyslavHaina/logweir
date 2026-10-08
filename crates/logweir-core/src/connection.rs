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

// ---------------------------------------------------------------------------
// PROD-01.3 security follow-up: the credential BINDING.
// ---------------------------------------------------------------------------
//
// THE THREAT. A connection names the Secret its credential is in. Without a
// binding, anyone who may write a `KafkaCluster` (or ask the console to) could
// name ANOTHER connection's credential Secret — one they cannot read — point
// the bootstrap address at a host they control, and have Logweir's runner read
// that Secret and present it there: Logweir would be the deputy that
// exfiltrates it (SASL/PLAIN sends the password itself; SCRAM gives the host an
// offline-guessable exchange). The binding closes it: a credential Secret is
// used only when it carries, under [`CREDENTIAL_BINDING_KEY`], the binding of
// the connection that names it, and the runner compares it with the value the
// controller computed from that connection BEFORE any client exists.
//
// WHAT IS BOUND. The connection's UID — unpredictable before the object exists,
// so no Secret written for one connection can name another — and a digest of
// the endpoint the credential may be presented to: the bootstrap set, the auth
// mode, the username, the TLS switch and the CA reference. `KafkaCluster.spec`
// is immutable, so "change the endpoint, keep the password" is a new object
// with a new UID, which no stored credential names: the credential must be
// entered again. The endpoint digest is the same rule held a second time, for
// an object whose spec changed by a route that bypassed the immutability rule.

/// The data key, in a credential Secret, that holds its binding.
pub const CREDENTIAL_BINDING_KEY: &str = "logweir-binding";

/// **The named reason** a credential whose binding does not name the
/// connection that projected it is refused — the runner's `refusal-reason=`,
/// the controller's terminal state and the probe's condition reason.
pub const CREDENTIAL_BINDING_MISMATCH: &str = "CredentialBindingMismatch";

/// The SOURCE side's projected binding (from the credential Secret, optional:
/// absent when the Secret carries none).
pub const SOURCE_CREDENTIAL_BINDING_ENV: &str = "LOGWEIR_SOURCE_CREDENTIAL_BINDING";
/// The SOURCE side's EXPECTED binding — a literal the controller computed from
/// the connection the Job was built for. Public (a UID and a digest).
pub const SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED";
/// The TARGET side's twin of [`SOURCE_CREDENTIAL_BINDING_ENV`].
pub const TARGET_CREDENTIAL_BINDING_ENV: &str = "LOGWEIR_TARGET_CREDENTIAL_BINDING";
/// The TARGET side's twin of [`SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV`].
pub const TARGET_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "LOGWEIR_TARGET_CREDENTIAL_BINDING_EXPECTED";

/// The binding of a credential to ONE connection and its endpoint:
/// `v1:<uid>:sha256:<hex>`, where the digest covers the endpoint fields named
/// in this section's header. A pure function of public values, so the console
/// API (which writes it into the Secret it creates), the controller (which
/// projects it as the expected value) and an operator (who reads it from
/// `KafkaCluster.status.credentialBinding`) compute one string.
///
/// `ca` is the CA REFERENCE as `<kind>/<name>/<key>`, or `None`.
#[must_use]
pub fn credential_binding(
    uid: &str,
    bootstrap_servers: &[String],
    auth: &crate::spec::AuthSpec,
    ca: Option<&str>,
) -> String {
    let servers: std::collections::BTreeSet<&str> =
        bootstrap_servers.iter().map(|s| s.trim()).collect();
    let canonical = format!(
        "logweir-credential-binding/v1\nuid={uid}\nbootstrap={}\nmode={}\nusername={}\ntls={}\nca={}\n",
        servers.into_iter().collect::<Vec<_>>().join(","),
        auth.mode_str(),
        auth.username().unwrap_or_default(),
        auth.tls(),
        ca.unwrap_or("none"),
    );
    let digest = crate::ids::sha256_prefixed(canonical.as_bytes());
    format!("v1:{uid}:{digest}")
}

/// Why a projected credential is refused: its binding is absent or names
/// another connection. The message opens with
/// [`CREDENTIAL_BINDING_MISMATCH`] and names variables, never a value — not the
/// credential, and not the foreign binding either (which would tell a reader
/// which connection the Secret belongs to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialBindingRefusal {
    /// The variable that carried (or should have carried) the binding.
    pub binding_env: &'static str,
    /// `true` when the Secret carried no binding at all.
    pub absent: bool,
}

impl std::fmt::Display for CredentialBindingRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FX-20: the same refusal guards object-store and notification
        // credentials; their wording names what they are bound to. A Kafka
        // connection's text is PROD-01.3's, byte for byte.
        let other = if self.binding_env.starts_with("NOTIFY_") {
            Some((
                "notification route",
                "sink",
                "Set the key to the route's entry in the ProtectionPolicy's \
                 status.credentialBindings.",
            ))
        } else if self.binding_env.starts_with("LOGWEIR_ARCHIVE_")
            || self.binding_env.starts_with("LOGWEIR_EVIDENCE_")
        {
            Some((
                "destination or archive location",
                "object store",
                "Enter the credential through the console, or add the key with the value in \
                 the BackupDestination's or RetentionPolicy's status.credentialBinding (an \
                 inline archive's location binding: docs/kubernetes.md §20.10).",
            ))
        } else {
            None
        };
        if let Some((object, endpoint, remedy)) = other {
            return if self.absent {
                write!(
                    f,
                    "{CREDENTIAL_BINDING_MISMATCH}: the credential Secret projected for this \
                     {object} carries no `{CREDENTIAL_BINDING_KEY}` key (`{}` is unset), so \
                     nothing shows it was entered for it; it is refused rather than presented \
                     to the {endpoint}. {remedy} Nothing was dialled",
                    self.binding_env
                )
            } else {
                write!(
                    f,
                    "{CREDENTIAL_BINDING_MISMATCH}: the credential Secret projected for this \
                     {object} is bound to a different object or endpoint (`{}` does not equal \
                     the expected binding), so it is refused rather than presented to this \
                     {object}'s {endpoint}. A changed endpoint needs the credential bound again. \
                     Nothing was dialled",
                    self.binding_env
                )
            };
        }
        if self.absent {
            write!(
                f,
                "{CREDENTIAL_BINDING_MISMATCH}: the credential Secret projected for this \
                 connection carries no `{CREDENTIAL_BINDING_KEY}` key (`{}` is unset), so \
                 nothing shows it was entered for this connection; it is refused rather than \
                 presented to the connection's brokers. Enter the credential through the \
                 console, or add the key with the value in the connection's \
                 status.credentialBinding. Nothing was dialled",
                self.binding_env
            )
        } else {
            write!(
                f,
                "{CREDENTIAL_BINDING_MISMATCH}: the credential Secret projected for this \
                 connection is bound to a different connection or endpoint (`{}` does not equal \
                 the connection's own binding), so it is refused rather than presented to this \
                 connection's brokers. A changed endpoint is a new connection, and its \
                 credential must be entered again. Nothing was dialled",
                self.binding_env
            )
        }
    }
}

impl std::error::Error for CredentialBindingRefusal {}

/// The runner's comparison, pure: `expected` is the controller's literal and
/// `projected` what the kubelet projected from the Secret (`None` when the
/// key is absent; a blank value counts as absent).
///
/// `expected` of `None` means no controller asked for a binding — a hand-run
/// `logweir` whose operator supplies their own environment — and is accepted.
///
/// **FX-20: a Secret may carry SEVERAL bindings**, separated by whitespace or
/// commas, and is accepted when ONE of them equals `expected` exactly. Each is
/// an explicit authorization by whoever wrote the Secret — the same act as
/// copying the credential into a second Secret bound to the second object, and
/// no wider: a principal who may write a Secret's data could already put any
/// binding there (Secret `patch` is equivalent to `get` for a credential,
/// `docs/kubernetes.md` §20.9). It lets one S3 key serve, say, the archive and
/// the evidence location of an inline restore, or two destinations, without a
/// copy. A single binding — everything the console writes — reads exactly as
/// before.
///
/// # Errors
///
/// [`CredentialBindingRefusal`].
pub fn check_credential_binding(
    binding_env: &'static str,
    expected: Option<&str>,
    projected: Option<&str>,
) -> Result<(), CredentialBindingRefusal> {
    let Some(expected) = expected.map(str::trim).filter(|e| !e.is_empty()) else {
        return Ok(());
    };
    // FX-20: A FAIL-CLOSED EXPECTATION IS NEVER SATISFIED. A controller that
    // could not compute a binding (no UID) projects `unbound:…`; a Secret that
    // happens to carry the same string must not turn that into an accept.
    if expected.starts_with("unbound:") {
        return Err(CredentialBindingRefusal {
            binding_env,
            absent: projected.map(str::trim).is_none_or(str::is_empty),
        });
    }
    match projected.map(str::trim).filter(|p| !p.is_empty()) {
        None => Err(CredentialBindingRefusal {
            binding_env,
            absent: true,
        }),
        Some(p)
            if p.split(|c: char| c.is_ascii_whitespace() || c == ',')
                .any(|token| token == expected) =>
        {
            Ok(())
        }
        Some(_) => Err(CredentialBindingRefusal {
            binding_env,
            absent: false,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_binding_covers_the_uid_and_every_endpoint_field() {
        use crate::spec::AuthSpec;
        let servers = vec!["b1:9093".to_string(), "b2:9093".to_string()];
        let auth = AuthSpec::Plain {
            username: "u".into(),
            tls: true,
        };
        let base = credential_binding("uid-1", &servers, &auth, Some("configMap/ca/ca.crt"));
        assert!(base.starts_with("v1:uid-1:sha256:"), "{base}");
        // Order and whitespace of the bootstrap list are not a different endpoint.
        let reordered = vec![" b2:9093".to_string(), "b1:9093".to_string()];
        assert_eq!(
            credential_binding("uid-1", &reordered, &auth, Some("configMap/ca/ca.crt")),
            base
        );
        // Every field moves it.
        for other in [
            credential_binding("uid-2", &servers, &auth, Some("configMap/ca/ca.crt")),
            credential_binding(
                "uid-1",
                &["evil:9093".to_string()],
                &auth,
                Some("configMap/ca/ca.crt"),
            ),
            credential_binding(
                "uid-1",
                &servers,
                &AuthSpec::ScramSha512 {
                    username: "u".into(),
                    tls: true,
                },
                Some("configMap/ca/ca.crt"),
            ),
            credential_binding(
                "uid-1",
                &servers,
                &AuthSpec::Plain {
                    username: "v".into(),
                    tls: true,
                },
                Some("configMap/ca/ca.crt"),
            ),
            credential_binding("uid-1", &servers, &auth, None),
            credential_binding("uid-1", &servers, &auth, Some("configMap/evil/ca.crt")),
        ] {
            assert_ne!(other, base);
        }
        // THE TLS SWITCH moves it too (fix round, review F4): the same SASL
        // mode and username over TLS and in the clear are two endpoints, and
        // a credential entered for the TLS one is not the clear one's.
        let scram = |tls| AuthSpec::ScramSha256 {
            username: "u".into(),
            tls,
        };
        assert_ne!(
            credential_binding("uid-1", &servers, &scram(true), None),
            credential_binding("uid-1", &servers, &scram(false), None),
            "the binding ignores the TLS switch"
        );
    }

    #[test]
    fn the_binding_check_refuses_absent_and_foreign_and_accepts_its_own() {
        let env = SOURCE_CREDENTIAL_BINDING_ENV;
        assert_eq!(check_credential_binding(env, None, None), Ok(()));
        assert_eq!(check_credential_binding(env, Some(" "), Some("x")), Ok(()));
        assert_eq!(
            check_credential_binding(env, Some("v1:a"), Some("v1:a")),
            Ok(())
        );
        let absent = check_credential_binding(env, Some("v1:a"), None).unwrap_err();
        assert!(absent.absent);
        assert!(absent
            .to_string()
            .starts_with("CredentialBindingMismatch: "));
        assert_eq!(
            check_credential_binding(env, Some("v1:a"), Some("")).unwrap_err(),
            absent,
            "a blank projected value is absent"
        );
        let foreign = check_credential_binding(env, Some("v1:a"), Some("v1:b")).unwrap_err();
        assert!(!foreign.absent);
        // The message names neither binding.
        assert!(!foreign.to_string().contains("v1:a") && !foreign.to_string().contains("v1:b"));
        // FX-20: several bindings, one of them this one — accepted; none of
        // them this one, or a mere prefix of it — refused.
        assert_eq!(
            check_credential_binding(env, Some("v1:a"), Some("v1:x\nv1:a")),
            Ok(())
        );
        assert_eq!(
            check_credential_binding(env, Some("v1:a"), Some("v1:x, v1:a")),
            Ok(())
        );
        assert!(check_credential_binding(env, Some("v1:a"), Some("v1:x v1:ab")).is_err());
        // A token that is a PREFIX of the expectation (a truncated binding) is
        // not the binding either.
        assert!(check_credential_binding(env, Some("v1:abc"), Some("v1:ab")).is_err());
        assert!(check_credential_binding(env, Some("v1:abc"), Some("v1:x,v1:ab")).is_err());
        assert!(check_credential_binding(env, Some("v1:a"), Some("v1:x\nv1:b")).is_err());
        // FX-20: the fail-closed expectation is refused even when a Secret
        // carries the very same string.
        assert!(
            check_credential_binding(env, Some("unbound:no-uid"), Some("unbound:no-uid")).is_err(),
            "a Secret spelling the fail-closed value must not satisfy it"
        );
        assert!(
            check_credential_binding(env, Some("unbound:no-uid"), None)
                .unwrap_err()
                .absent
        );
    }

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
