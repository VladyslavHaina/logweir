//! Write-only credential entry — the contract a product API (PLAT-17's
//! `logweir-api`) uses to turn a password typed once into a Secret, without
//! ever being able to read one back.
//!
//! # NOT CALLED BY THE CONTROLLER
//!
//! `weirkeeper` never builds, creates or reads a credential Secret; it only
//! names one in a Job (`super::resolve`). This module lives here because the
//! credential's key name, labels and the `auth.secretRef` it produces are the
//! same contract the resolver consumes, and one crate holding both halves is
//! what stops them drifting. `tests/connection.rs` asserts no controller calls
//! it.
//!
//! # The contract, version 1
//!
//! * **Create-only.** The caller `POST`s the returned object and does nothing
//!   else with Secrets: no `get`, `list`, `watch`, `update`, `patch` or
//!   `delete`. A name that already exists answers 409, which the caller reports
//!   as a conflict — it never overwrites, and never reads to compare.
//! * **Never read back.** The API server's create response echoes `data`. The
//!   caller keeps only `metadata.name`, `metadata.uid` and
//!   `metadata.resourceVersion` from it ([`CreatedCredential::from_response`])
//!   and returns the connection's `auth.secretRef` — a name — to its client.
//! * **Fixed shape.** One data key, [`PASSWORD_DATA_KEY`] (`password`, the
//!   resolver's default, so the resulting `KafkaCluster` needs no
//!   `passwordKey`); `type` [`CREDENTIAL_SECRET_TYPE`]; the labels and
//!   annotations below. The username is NOT in the Secret: it is identity, it
//!   is bound into restore plan hashes, and it lives in `auth.username`.
//! * **A custom `type`, deliberately.** `create` on Secrets is broad: in a
//!   namespace it can mint a `kubernetes.io/service-account-token` Secret for
//!   any ServiceAccount there. A distinct, immutable-after-create `type` is
//!   what a ValidatingAdmissionPolicy scoped to the API's ServiceAccount could
//!   require, so the API's create permission cannot be spent on anything but
//!   this shape. NO SUCH POLICY SHIPS:
//!   `config/samples/validatingadmissionpolicy.yaml` carries the one example
//!   this tree has, and a policy for this type belongs with the product API
//!   that creates the Secret (PLAT-17), not with the resolver that only names
//!   one.
//! * **Not `immutable`.** Rotation by an administrator or a secret manager
//!   updates `data.password` in place, and the next Job uses it
//!   (`super`'s header). The product API itself never updates.
//! * **Validated before it exists.** Empty, over-long, control characters,
//!   leading or trailing whitespace, and the five characters the runner
//!   refuses at exit 3 (`logweir_core::guard::credential_is_renderable`) are
//!   refused here, so a credential accepted at entry is one a run can use.
//!   Every refusal names a character CLASS and never the value.
//!
//! # Redaction
//!
//! [`WriteOnlyPassword`] and [`CredentialSecret`] implement `Debug` by hand and
//! print no credential byte; neither implements `Display` or `Serialize`. The
//! only way to the bytes is [`CredentialSecret::into_secret`], whose result is
//! meant to be handed straight to a create call.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;

use crate::crds::kafka_cluster::{CredentialSecretRef, DEFAULT_PASSWORD_KEY};

/// The credential contract's version, recorded on every Secret it builds.
pub const CREDENTIAL_CONTRACT_VERSION: &str = "v1";

/// The one data key. Equal to the resolver's default password key, so a
/// `KafkaCluster` naming this Secret needs no `passwordKey`.
pub const PASSWORD_DATA_KEY: &str = DEFAULT_PASSWORD_KEY;

/// The Secret `type` — see the module header for why it is not `Opaque`.
pub const CREDENTIAL_SECRET_TYPE: &str = "logweir.dev/kafka-sasl-password";

/// `app.kubernetes.io/managed-by`, and its value.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// See [`MANAGED_BY_LABEL`].
pub const MANAGED_BY_VALUE: &str = "logweir";
/// Marks the Secret as a Logweir Kafka credential; the value names its kind.
pub const CREDENTIAL_LABEL: &str = "logweir.dev/credential";
/// See [`CREDENTIAL_LABEL`].
pub const CREDENTIAL_LABEL_VALUE: &str = "kafka-sasl-password";
/// The `KafkaCluster` the credential was entered for (a label, so a namespace
/// administrator can list the credentials of one connection).
pub const CONNECTION_LABEL: &str = "logweir.dev/connection";
/// The contract version annotation.
pub const CONTRACT_ANNOTATION: &str = "logweir.dev/credential-contract";
/// States the write-only rule on the object itself.
pub const WRITE_ONLY_ANNOTATION: &str = "logweir.dev/write-only";
/// The caller's audit/request id, for correlation. Never the actor's token.
pub const REQUEST_ID_ANNOTATION: &str = "logweir.dev/request-id";

/// The longest password accepted, in bytes. Generous for SCRAM and
/// Secrets-Manager-generated values, and a bound on what a request can make the
/// API server store.
pub const MAX_PASSWORD_BYTES: usize = 1024;

/// A password typed once. Its `Debug` prints no byte of it; it has no
/// `Display`, `Clone` or `Serialize`.
pub struct WriteOnlyPassword(String);

impl WriteOnlyPassword {
    /// Take ownership of the entered value.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for WriteOnlyPassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteOnlyPassword(<redacted>)")
    }
}

/// One credential entry request.
#[derive(Debug)]
pub struct NewKafkaCredential<'a> {
    /// The namespace the Secret is created in — the connection's.
    pub namespace: &'a str,
    /// The Secret's name.
    pub secret_name: &'a str,
    /// The `KafkaCluster` this credential is for.
    pub connection_name: &'a str,
    /// The password.
    pub password: WriteOnlyPassword,
    /// An opaque, non-secret correlation id from the caller's audit record.
    pub request_id: Option<&'a str>,
    /// **PROD-01.3 security follow-up.** The connection's credential binding
    /// (`super::ResolvedConnection::credential_binding`), written under
    /// `logweir-binding` so every runner can tell this credential was entered
    /// for THIS connection. `None` builds an unbound Secret, which every
    /// runner refuses (`CredentialBindingMismatch`) — kept only so a caller
    /// that cannot know the UID yet fails closed rather than at compile time.
    pub binding: Option<&'a str>,
    /// The `KafkaCluster`'s UID, for an owner reference: deleting the
    /// connection collects its credential.
    pub owner_uid: Option<&'a str>,
}

/// **PROD-01.3.** An `mtls` client certificate and key typed once. Neither
/// half is printed by `Debug`; there is no `Display`, `Clone` or `Serialize`.
pub struct WriteOnlyClientCertificate {
    certificate_pem: String,
    private_key_pem: String,
}

impl WriteOnlyClientCertificate {
    /// Take ownership of the entered PEM texts.
    #[must_use]
    pub fn new(certificate_pem: String, private_key_pem: String) -> Self {
        Self {
            certificate_pem,
            private_key_pem,
        }
    }
}

impl std::fmt::Debug for WriteOnlyClientCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteOnlyClientCertificate(<redacted>)")
    }
}

/// **PROD-01.3.** One `mtls` client-certificate entry request.
#[derive(Debug)]
pub struct NewKafkaClientCertificate<'a> {
    /// The namespace the Secret is created in — the connection's.
    pub namespace: &'a str,
    /// The Secret's name.
    pub secret_name: &'a str,
    /// The `KafkaCluster` this credential is for.
    pub connection_name: &'a str,
    /// The certificate and key.
    pub certificate: WriteOnlyClientCertificate,
    /// An opaque, non-secret correlation id from the caller's audit record.
    pub request_id: Option<&'a str>,
    /// The connection's credential binding — see [`NewKafkaCredential::binding`].
    pub binding: Option<&'a str>,
    /// The `KafkaCluster`'s UID, for an owner reference.
    pub owner_uid: Option<&'a str>,
}

/// The Secret `type` of an `mtls` client certificate the product creates
/// (PROD-01.3) — distinct from [`CREDENTIAL_SECRET_TYPE`] for the same reason
/// that one exists: a ValidatingAdmissionPolicy can require exactly the
/// shapes the console may create.
pub const CLIENT_CERTIFICATE_SECRET_TYPE: &str = "logweir.dev/kafka-client-certificate";
/// [`CREDENTIAL_LABEL`]'s value on an `mtls` client-certificate Secret.
pub const CLIENT_CERTIFICATE_LABEL_VALUE: &str = "kafka-client-certificate";
/// The longest PEM text accepted for either half of a client certificate.
pub const MAX_PEM_BYTES: usize = 64 * 1024;

/// Why an entry was refused. `Display` never includes the password.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialInputError {
    /// The namespace is not a DNS-1123 label.
    InvalidNamespace,
    /// The Secret name is not a DNS-1123 subdomain.
    InvalidSecretName,
    /// The connection name is not a usable label value (DNS-1123 subdomain of
    /// at most 63 characters).
    InvalidConnectionName,
    /// The request id is empty, too long or not visible ASCII.
    InvalidRequestId,
    /// The password is empty.
    EmptyPassword,
    /// The password is longer than [`MAX_PASSWORD_BYTES`].
    PasswordTooLong,
    /// The password has leading or trailing whitespace, which the engine's
    /// YAML substitution would strip.
    PasswordEdgeWhitespace,
    /// The password contains a character of this class that no run can use.
    PasswordCharacter(&'static str),
    /// PROD-01.3: the client certificate is not PEM certificate text.
    CertificateNotPem,
    /// PROD-01.3: the private key is not an unencrypted PEM private key.
    PrivateKeyNotPem,
    /// PROD-01.3: the private key is encrypted; neither client takes a
    /// passphrase.
    PrivateKeyEncrypted,
    /// PROD-01.3: either half is longer than [`MAX_PEM_BYTES`].
    PemTooLarge,
}

impl std::fmt::Display for CredentialInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNamespace => f.write_str("the namespace is not a DNS-1123 label"),
            Self::InvalidSecretName => f.write_str("the Secret name is not a DNS-1123 subdomain"),
            Self::InvalidConnectionName => f.write_str(
                "the connection name is not a DNS-1123 subdomain of at most 63 characters",
            ),
            Self::InvalidRequestId => {
                f.write_str("the request id must be 1-128 visible ASCII characters")
            }
            Self::EmptyPassword => f.write_str("the password is empty"),
            Self::PasswordTooLong => {
                write!(f, "the password is longer than {MAX_PASSWORD_BYTES} bytes")
            }
            Self::PasswordEdgeWhitespace => f.write_str(
                "the password starts or ends with whitespace, which the engine's configuration \
                 substitution would strip",
            ),
            Self::PasswordCharacter(class) => write!(
                f,
                "the password contains {class}, which a backup or restore run cannot render; \
                 choose a password without it"
            ),
            Self::CertificateNotPem => f.write_str(
                "the client certificate is not PEM certificate text (-----BEGIN CERTIFICATE-----)",
            ),
            Self::PrivateKeyNotPem => {
                f.write_str("the private key is not a PEM private key (PKCS#8, PKCS#1 or SEC1)")
            }
            Self::PrivateKeyEncrypted => f.write_str(
                "the private key is encrypted; neither client takes a passphrase, so enter the \
                 unencrypted key",
            ),
            Self::PemTooLarge => write!(
                f,
                "a client certificate or key is longer than {MAX_PEM_BYTES} bytes"
            ),
        }
    }
}

impl std::error::Error for CredentialInputError {}

/// A built credential Secret, not yet created. `Debug` omits `data`.
pub struct CredentialSecret(Secret);

impl std::fmt::Debug for CredentialSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialSecret")
            .field("metadata", &self.0.metadata)
            .field("type", &self.0.type_)
            .field(
                "data_keys",
                &self
                    .0
                    .data
                    .as_ref()
                    .map(|d| d.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl CredentialSecret {
    /// The Secret to hand to a create call. The only path to the bytes.
    #[must_use]
    pub fn into_secret(self) -> Secret {
        self.0
    }

    /// The `auth.secretRef` a `KafkaCluster` names this credential by — the
    /// value the product API returns instead of anything about the password.
    #[must_use]
    pub fn secret_ref(&self) -> CredentialSecretRef {
        CredentialSecretRef {
            name: self.0.metadata.name.clone().unwrap_or_default(),
            password_key: None,
            unrecognized_fields: BTreeMap::new(),
        }
    }
}

/// The non-secret facts a caller keeps from a create response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedCredential {
    /// `metadata.name`.
    pub name: String,
    /// `metadata.namespace`.
    pub namespace: String,
    /// `metadata.uid`.
    pub uid: Option<String>,
    /// `metadata.resourceVersion`.
    pub resource_version: Option<String>,
}

impl CreatedCredential {
    /// Keep the metadata of a create response and DROP the rest — the echoed
    /// `data` included — before anything else can see it.
    #[must_use]
    pub fn from_response(created: Secret) -> Self {
        let ObjectMeta {
            name,
            namespace,
            uid,
            resource_version,
            ..
        } = created.metadata;
        Self {
            name: name.unwrap_or_default(),
            namespace: namespace.unwrap_or_default(),
            uid,
            resource_version,
        }
    }
}

fn is_dns1123_label(value: &str) -> bool {
    value.len() <= 63 && !value.contains('.') && super::is_dns1123_subdomain(value)
}

/// Validate one entry and build the Secret the caller creates.
///
/// # Errors
///
/// [`CredentialInputError`] for each rule in the module header. The password
/// is checked last, so a malformed request never reaches the value at all.
pub fn build_kafka_credential_secret(
    request: NewKafkaCredential<'_>,
) -> Result<CredentialSecret, CredentialInputError> {
    if !is_dns1123_label(request.namespace) {
        return Err(CredentialInputError::InvalidNamespace);
    }
    if !super::is_dns1123_subdomain(request.secret_name) {
        return Err(CredentialInputError::InvalidSecretName);
    }
    if request.connection_name.len() > 63 || !super::is_dns1123_subdomain(request.connection_name) {
        return Err(CredentialInputError::InvalidConnectionName);
    }
    if let Some(id) = request.request_id {
        if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(CredentialInputError::InvalidRequestId);
        }
    }
    let password = request.password.0;
    check_password(&password)?;
    let mut data = BTreeMap::from([(
        PASSWORD_DATA_KEY.to_string(),
        ByteString(password.into_bytes()),
    )]);
    if let Some(binding) = request.binding {
        data.insert(
            logweir_core::connection::CREDENTIAL_BINDING_KEY.to_string(),
            ByteString(binding.as_bytes().to_vec()),
        );
    }

    let labels = BTreeMap::from([
        (MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string()),
        (
            CREDENTIAL_LABEL.to_string(),
            CREDENTIAL_LABEL_VALUE.to_string(),
        ),
        (
            CONNECTION_LABEL.to_string(),
            request.connection_name.to_string(),
        ),
    ]);
    let mut annotations = BTreeMap::from([
        (
            CONTRACT_ANNOTATION.to_string(),
            CREDENTIAL_CONTRACT_VERSION.to_string(),
        ),
        (WRITE_ONLY_ANNOTATION.to_string(), "true".to_string()),
    ]);
    if let Some(id) = request.request_id {
        annotations.insert(REQUEST_ID_ANNOTATION.to_string(), id.to_string());
    }
    Ok(CredentialSecret(Secret {
        metadata: ObjectMeta {
            name: Some(request.secret_name.to_string()),
            namespace: Some(request.namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: owner_reference(request.connection_name, request.owner_uid),
            ..ObjectMeta::default()
        },
        type_: Some(CREDENTIAL_SECRET_TYPE.to_string()),
        data: Some(data),
        string_data: None,
        immutable: None,
    }))
}

/// The owner reference that makes a credential the connection's: deleting the
/// `KafkaCluster` collects it.
fn owner_reference(
    connection_name: &str,
    owner_uid: Option<&str>,
) -> Option<Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference>> {
    let uid = owner_uid.filter(|u| !u.is_empty())?;
    Some(vec![
        k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "KafkaCluster".to_string(),
            name: connection_name.to_string(),
            uid: uid.to_string(),
            controller: None,
            block_owner_deletion: None,
        },
    ])
}

/// **PROD-01.3.** Validate an `mtls` entry and build the Secret the caller
/// creates: type [`CLIENT_CERTIFICATE_SECRET_TYPE`], the two halves under
/// `tls.crt` / `tls.key` (the keys `auth.clientCertificate` defaults to), and
/// the binding. Nothing about either half is ever echoed: every refusal names
/// a SHAPE.
///
/// # Errors
///
/// [`CredentialInputError`].
pub fn build_kafka_client_certificate_secret(
    request: NewKafkaClientCertificate<'_>,
) -> Result<CredentialSecret, CredentialInputError> {
    if !is_dns1123_label(request.namespace) {
        return Err(CredentialInputError::InvalidNamespace);
    }
    if !super::is_dns1123_subdomain(request.secret_name) {
        return Err(CredentialInputError::InvalidSecretName);
    }
    if request.connection_name.len() > 63 || !super::is_dns1123_subdomain(request.connection_name) {
        return Err(CredentialInputError::InvalidConnectionName);
    }
    if let Some(id) = request.request_id {
        if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(CredentialInputError::InvalidRequestId);
        }
    }
    let WriteOnlyClientCertificate {
        certificate_pem,
        private_key_pem,
    } = request.certificate;
    check_client_certificate(&certificate_pem, &private_key_pem)?;
    let labels = BTreeMap::from([
        (MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string()),
        (
            CREDENTIAL_LABEL.to_string(),
            CLIENT_CERTIFICATE_LABEL_VALUE.to_string(),
        ),
        (
            CONNECTION_LABEL.to_string(),
            request.connection_name.to_string(),
        ),
    ]);
    let mut annotations = BTreeMap::from([
        (
            CONTRACT_ANNOTATION.to_string(),
            CREDENTIAL_CONTRACT_VERSION.to_string(),
        ),
        (WRITE_ONLY_ANNOTATION.to_string(), "true".to_string()),
    ]);
    if let Some(id) = request.request_id {
        annotations.insert(REQUEST_ID_ANNOTATION.to_string(), id.to_string());
    }
    let mut data = BTreeMap::from([
        (
            crate::crds::kafka_cluster::DEFAULT_CLIENT_CERTIFICATE_KEY.to_string(),
            ByteString(certificate_pem.into_bytes()),
        ),
        (
            crate::crds::kafka_cluster::DEFAULT_CLIENT_PRIVATE_KEY_KEY.to_string(),
            ByteString(private_key_pem.into_bytes()),
        ),
    ]);
    if let Some(binding) = request.binding {
        data.insert(
            logweir_core::connection::CREDENTIAL_BINDING_KEY.to_string(),
            ByteString(binding.as_bytes().to_vec()),
        );
    }
    Ok(CredentialSecret(Secret {
        metadata: ObjectMeta {
            name: Some(request.secret_name.to_string()),
            namespace: Some(request.namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: owner_reference(request.connection_name, request.owner_uid),
            ..ObjectMeta::default()
        },
        type_: Some(CLIENT_CERTIFICATE_SECRET_TYPE.to_string()),
        data: Some(data),
        string_data: None,
        immutable: None,
    }))
}

/// **PROD-01.3.** The SHAPE checks on an `mtls` entry, before anything is
/// written: both halves are PEM text within [`MAX_PEM_BYTES`], the
/// certificate carries a `CERTIFICATE` block, and the key a private-key block
/// that is not encrypted. Not a cryptographic check — the brokers are — and
/// never echoing either half.
///
/// # Errors
///
/// [`CredentialInputError`].
pub fn check_client_certificate(
    certificate_pem: &str,
    private_key_pem: &str,
) -> Result<(), CredentialInputError> {
    if certificate_pem.len() > MAX_PEM_BYTES || private_key_pem.len() > MAX_PEM_BYTES {
        return Err(CredentialInputError::PemTooLarge);
    }
    if !certificate_pem.is_ascii()
        || !certificate_pem.contains("-----BEGIN CERTIFICATE-----")
        || !certificate_pem.contains("-----END CERTIFICATE-----")
    {
        return Err(CredentialInputError::CertificateNotPem);
    }
    if private_key_pem.contains("ENCRYPTED") {
        return Err(CredentialInputError::PrivateKeyEncrypted);
    }
    let key_block = ["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"]
        .iter()
        .any(|label| {
            private_key_pem.contains(&format!("-----BEGIN {label}-----"))
                && private_key_pem.contains(&format!("-----END {label}-----"))
        });
    if !private_key_pem.is_ascii() || !key_block {
        return Err(CredentialInputError::PrivateKeyNotPem);
    }
    Ok(())
}

/// The password rules of the module header — the same predicate the runner
/// applies at exit 3, so a credential accepted at entry is one a run can use.
///
/// # Errors
///
/// [`CredentialInputError`], naming a character CLASS and never the value.
pub fn check_password(password: &str) -> Result<(), CredentialInputError> {
    if password.is_empty() {
        return Err(CredentialInputError::EmptyPassword);
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(CredentialInputError::PasswordTooLong);
    }
    if password.starts_with(char::is_whitespace) || password.ends_with(char::is_whitespace) {
        return Err(CredentialInputError::PasswordEdgeWhitespace);
    }
    // THE RUNNER'S OWN PREDICATE DECIDES, and the class is derived from the
    // first offending character rather than parsed out of the refusal's prose
    // (that prose explains the hazard and mentions several classes, so reading
    // it would name the wrong one). Entry and execution therefore refuse
    // exactly the same values: `logweir_core::guard` is the single rule.
    if logweir_core::guard::credential_is_renderable(password).is_err() {
        let offender = password
            .chars()
            .find(|c| logweir_core::guard::UNRENDERABLE_CREDENTIAL_CHARACTERS.contains(c));
        let class = match offender {
            Some('\n') => "a newline",
            Some('\r') => "a carriage return",
            Some('"') => "a double quote",
            Some('\'') => "a single quote",
            Some('$') => "a dollar sign",
            _ => "a character this build cannot render",
        };
        return Err(CredentialInputError::PasswordCharacter(class));
    }
    if password.chars().any(char::is_control) {
        return Err(CredentialInputError::PasswordCharacter(
            "a control character",
        ));
    }
    Ok(())
}
