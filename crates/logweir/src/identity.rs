//! Persistent installation identity bootstrap for the Helm chart.
//!
//! This code deliberately lives in the signer-capable `logweir` runner
//! binary. The chart runs it in a short-lived hook Job whose Role can only get
//! and patch the retained identity objects. The long-lived controller and UI
//! receive neither this code nor Secret-read permission.

use std::collections::BTreeSet;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use logweir_evidence::keys::{KeyAlg, SigningKey};
use serde_json::{json, Value};

use crate::exit::ExitCode;

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const IDENTITY_STATE_ANNOTATION: &str = "logweir.dev/identity-state";
const ESTABLISHED: &str = "established";
const PUBLIC_KEY_ID: &str = "key-id";
const PUBLIC_KEY_PEM: &str = "signing.pub.pem";
const PUBLIC_ALGORITHM: &str = "algorithm";
const TRUST_REFERENCE: &str = "trust-reference";
const DEFAULT_TRUST_REFERENCE: &str = "logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys";

#[derive(Debug)]
pub struct BootstrapArgs {
    pub namespace: String,
    pub secret_name: String,
    pub secret_key: String,
    pub public_configmap_name: String,
    pub external_secret: Option<(String, String)>,
}

#[derive(Debug)]
pub struct DistributeArgs {
    pub source_namespace: String,
    pub target_namespace: String,
    pub secret_name: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Generated,
    Adopted,
    Existing,
    ConcurrentWinner(u16),
}

impl Origin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Generated => "generated",
            Self::Adopted => "adopted",
            Self::Existing => "existing",
            Self::ConcurrentWinner(_) => "concurrent-existing",
        }
    }

    fn contention_status(self) -> Option<u16> {
        match self {
            Self::ConcurrentWinner(status) => Some(status),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Outcome {
    key_id: String,
    origin: Origin,
}

#[derive(Debug, Clone)]
struct PrivateRecord {
    resource_version: String,
    state: Option<String>,
    pem: Option<String>,
    has_any_data: bool,
    has_annotations: bool,
}

#[derive(Debug, Clone)]
struct PublicRecord {
    resource_version: String,
    state: Option<String>,
    key_id: Option<String>,
    spki_pem: Option<String>,
    algorithm: Option<String>,
    trust_reference: Option<String>,
    data_keys: BTreeSet<String>,
    has_any_data: bool,
    has_annotations: bool,
}

impl PublicRecord {
    fn carries_identity(&self) -> bool {
        self.state.as_deref() == Some(ESTABLISHED)
            || self.key_id.is_some()
            || self.spki_pem.is_some()
            || self.algorithm.is_some()
            || self.trust_reference.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicMaterial {
    key_id: String,
    spki_pem: String,
    algorithm: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializeResult {
    Written,
    Contended(u16),
}

trait IdentityStore {
    fn managed_private(&mut self) -> Result<PrivateRecord, String>;
    fn public_record(&mut self) -> Result<PublicRecord, String>;
    fn external_private(&mut self, name: &str, key: &str) -> Result<String, String>;
    fn initialize_private(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String>;
    fn initialize_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
    ) -> Result<InitializeResult, String>;
}

pub fn run(args: &BootstrapArgs) -> ExitCode {
    match KubernetesStore::in_cluster(args).and_then(|mut store| bootstrap(&mut store, args)) {
        Ok(outcome) => {
            print_outcome("identity-ready", &outcome);
            ExitCode::Ok
        }
        Err(error) => {
            // Every error is constructed from object names, paths and HTTP
            // status codes. Private key values are never interpolated.
            eprintln!("identity bootstrap failed: {error}");
            ExitCode::Operational
        }
    }
}

pub fn run_distribution(args: &DistributeArgs) -> ExitCode {
    match KubernetesStore::for_distribution(args).and_then(|mut store| distribute(&mut store, args))
    {
        Ok(outcome) => {
            print_outcome("identity-distributed", &outcome);
            ExitCode::Ok
        }
        Err(error) => {
            eprintln!("identity distribution failed: {error}");
            ExitCode::Operational
        }
    }
}

fn print_outcome(prefix: &str, outcome: &Outcome) {
    print!(
        "{prefix} key-id={} source={}",
        outcome.key_id,
        outcome.origin.as_str()
    );
    if let Some(status) = outcome.origin.contention_status() {
        print!(" contention-http={status}");
    }
    println!();
}

trait DistributionStore {
    fn source_private(&mut self) -> Result<PrivateRecord, String>;
    fn target_private(&mut self) -> Result<PrivateRecord, String>;
    fn initialize_target(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String>;
}

fn distribute(
    store: &mut impl DistributionStore,
    args: &DistributeArgs,
) -> Result<Outcome, String> {
    let source = store.source_private()?;
    let source_pem = source.pem.as_deref().ok_or_else(|| {
        format!(
            "source signing Secret {}/{} is not established; let the primary identity bootstrap finish before distribution",
            args.source_namespace, args.secret_name
        )
    })?;
    let source_key = parse_private(source_pem, "source installation signing Secret")?;
    let source_material = public_material(&source_key)?;
    let target = store.target_private()?;

    if let Some(target_pem) = target.pem.as_deref() {
        let target_key = parse_private(target_pem, "target runner signing Secret")?;
        let target_material = public_material(&target_key)?;
        if target_material.key_id != source_material.key_id {
            return Err(format!(
                "target namespace {} already contains a different signer {}; restore the installation signer {} there—distribution never overwrites an established identity",
                args.target_namespace, target_material.key_id, source_material.key_id
            ));
        }
        return Ok(Outcome {
            key_id: source_material.key_id,
            origin: Origin::Existing,
        });
    }
    if target.has_any_data || target.state.as_deref() == Some(ESTABLISHED) {
        return Err(format!(
            "target namespace {} has an incomplete retained signing Secret; restore the installation identity there before retrying",
            args.target_namespace
        ));
    }

    match store.initialize_target(
        &target.resource_version,
        target.has_annotations,
        &args.secret_key,
        source_pem,
    )? {
        InitializeResult::Written => Ok(Outcome {
            key_id: source_material.key_id,
            origin: Origin::Adopted,
        }),
        InitializeResult::Contended(status) => {
            let winner = store
                .target_private()
                .map_err(|error| contention_error(status, error))?;
            if winner.state.as_deref() != Some(ESTABLISHED) {
                return Err(contention_error(
                    status,
                    format!(
                        "target signing Secret in namespace {} did not contain an established concurrent winner",
                        args.target_namespace
                    ),
                ));
            }
            let winner_pem = winner.pem.as_deref().ok_or_else(|| {
                contention_error(
                    status,
                    format!(
                        "target signing Secret in namespace {} still contains no key",
                        args.target_namespace
                    ),
                )
            })?;
            let winner_key = parse_private(winner_pem, "concurrent target runner signer")
                .map_err(|error| contention_error(status, error))?;
            let winner_material = public_material(&winner_key)?;
            if winner_material.key_id != source_material.key_id {
                return Err(contention_error(
                    status,
                    format!(
                        "target namespace {} concurrently established a different signer {}; refusing to replace it with installation identity {}",
                        args.target_namespace, winner_material.key_id, source_material.key_id
                    ),
                ));
            }
            Ok(Outcome {
                key_id: source_material.key_id,
                origin: Origin::ConcurrentWinner(status),
            })
        }
    }
}

fn bootstrap(store: &mut impl IdentityStore, args: &BootstrapArgs) -> Result<Outcome, String> {
    let private = store.managed_private()?;
    let public = store.public_record()?;

    if let Some(existing_pem) = private.pem.as_deref() {
        let key = parse_private(existing_pem, "managed signing Secret")?;
        let material = public_material(&key)?;

        if let Some((external_name, external_key)) = &args.external_secret {
            let external_pem = store.external_private(external_name, external_key)?;
            let external = parse_private(&external_pem, "configured external signing Secret")?;
            if public_material(&external)?.key_id != material.key_id {
                return Err(format!(
                    "configured external Secret {external_name}/{external_key} does not match the established installation identity {}; explicit adoption never rotates an existing signer",
                    material.key_id
                ));
            }
        }

        ensure_public(store, public, &material)?;
        return Ok(Outcome {
            key_id: material.key_id,
            origin: Origin::Existing,
        });
    }

    if private.has_any_data {
        return Err(format!(
            "the managed signing Secret contains data but not the required key {}; refusing to overwrite an existing Secret",
            args.secret_key
        ));
    }

    if private.state.as_deref() == Some(ESTABLISHED) || public.carries_identity() {
        return Err(
            "the public trust record says an installation identity is established, but the retained private key is absent; restore logweir-signing-key from backup or perform an explicit trust rotation—bootstrap will not silently replace it"
                .to_string(),
        );
    }

    let external_mode = args.external_secret.is_some();
    let candidate = if let Some((name, key)) = &args.external_secret {
        let pem = store.external_private(name, key)?;
        let parsed = parse_private(&pem, "configured external signing Secret")?;
        (parsed, pem)
    } else {
        let key = SigningKey::generate_p256();
        let pem = key
            .to_pkcs8_pem()
            .map_err(|e| format!("could not encode a generated installation key: {e}"))?;
        (key, pem)
    };
    let candidate_material = public_material(&candidate.0)?;

    let (winner, origin) = match store.initialize_private(
        &private.resource_version,
        private.has_annotations,
        &args.secret_key,
        &candidate.1,
    )? {
        InitializeResult::Written => (
            candidate_material,
            if external_mode {
                Origin::Adopted
            } else {
                Origin::Generated
            },
        ),
        InitializeResult::Contended(status) => {
            let current = store
                .managed_private()
                .map_err(|error| contention_error(status, error))?;
            if current.state.as_deref() != Some(ESTABLISHED) {
                return Err(contention_error(
                    status,
                    "the signing Secret did not contain an established concurrent winner",
                ));
            }
            let current_pem = current.pem.as_deref().ok_or_else(|| {
                contention_error(status, "the signing Secret still contains no private key")
            })?;
            let current_key = parse_private(current_pem, "concurrently initialized signing Secret")
                .map_err(|error| contention_error(status, error))?;
            let current_material = public_material(&current_key)?;
            if external_mode && current_material.key_id != candidate_material.key_id {
                return Err(contention_error(
                    status,
                    format!(
                        "another bootstrap established identity {} while external identity {} was being adopted; refusing to rotate either identity",
                        current_material.key_id, candidate_material.key_id
                    ),
                ));
            }
            (current_material, Origin::ConcurrentWinner(status))
        }
    };

    // Reload because a separate bootstrap may have published between the
    // first reads and the successful/contended Secret initialization.
    let current_public = store.public_record()?;
    ensure_public(store, current_public, &winner)?;
    Ok(Outcome {
        key_id: winner.key_id,
        origin,
    })
}

fn ensure_public(
    store: &mut impl IdentityStore,
    record: PublicRecord,
    material: &PublicMaterial,
) -> Result<(), String> {
    if record.carries_identity() {
        return validate_public(&record, material);
    }
    if record.has_any_data {
        return Err(
            "the public trust ConfigMap contains unrelated data; refusing to overwrite a nonempty retained object"
                .to_string(),
        );
    }
    match store.initialize_public(&record.resource_version, record.has_annotations, material)? {
        InitializeResult::Written => Ok(()),
        InitializeResult::Contended(status) => {
            let winner = store
                .public_record()
                .map_err(|error| contention_error(status, error))?;
            validate_public(&winner, material).map_err(|error| contention_error(status, error))
        }
    }
}

fn validate_public(record: &PublicRecord, material: &PublicMaterial) -> Result<(), String> {
    let expected_keys = BTreeSet::from([
        PUBLIC_KEY_ID.to_string(),
        PUBLIC_KEY_PEM.to_string(),
        PUBLIC_ALGORITHM.to_string(),
        TRUST_REFERENCE.to_string(),
    ]);
    let matches = record.data_keys == expected_keys
        && record.state.as_deref() == Some(ESTABLISHED)
        && record.key_id.as_deref() == Some(material.key_id.as_str())
        && record.spki_pem.as_deref() == Some(material.spki_pem.as_str())
        && record.algorithm.as_deref() == Some(material.algorithm.as_str())
        && record.trust_reference.as_deref() == Some(DEFAULT_TRUST_REFERENCE);
    if matches {
        Ok(())
    } else {
        Err(format!(
            "public trust ConfigMap does not exactly match established identity {}; refusing to overwrite verification material needed by existing archives",
            material.key_id
        ))
    }
}

fn contention_error(status: u16, detail: impl std::fmt::Display) -> String {
    format!(
        "conditional Kubernetes patch returned HTTP {status}, and no compatible established winner was found: {detail}"
    )
}

fn parse_private(pem: &str, source: &str) -> Result<SigningKey, String> {
    SigningKey::from_pkcs8_pem(pem)
        .map_err(|_| format!("{source} is not a P-256 or Ed25519 PKCS#8 PEM private key"))
}

fn public_material(key: &SigningKey) -> Result<PublicMaterial, String> {
    let verifying = key.verifying_key();
    Ok(PublicMaterial {
        key_id: verifying.key_id(),
        spki_pem: verifying
            .to_public_key_pem()
            .map_err(|e| format!("could not encode public verification material: {e}"))?,
        algorithm: match key.alg() {
            KeyAlg::EcdsaP256Sha256 => "ecdsa-p256-sha256",
            KeyAlg::Ed25519 => "ed25519",
        }
        .to_string(),
    })
}

struct KubernetesStore {
    agent: ureq::Agent,
    base_url: String,
    token: String,
    namespace: String,
    source_namespace: Option<String>,
    secret_name: String,
    secret_key: String,
    public_configmap_name: String,
}

impl KubernetesStore {
    fn in_cluster(args: &BootstrapArgs) -> Result<Self, String> {
        validate_name("namespace", &args.namespace)?;
        validate_name("Secret", &args.secret_name)?;
        validate_name("ConfigMap", &args.public_configmap_name)?;
        validate_data_key("Secret data key", &args.secret_key)?;
        if let Some((name, key)) = &args.external_secret {
            validate_name("external Secret", name)?;
            validate_data_key("external Secret data key", key)?;
        }

        let host = std::env::var("KUBERNETES_SERVICE_HOST").map_err(|_| {
            "KUBERNETES_SERVICE_HOST is not set; run this command in the chart bootstrap Job"
                .to_string()
        })?;
        let port =
            std::env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".to_string());
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let token_path = format!("{SERVICE_ACCOUNT_DIR}/token");
        let ca_path = format!("{SERVICE_ACCOUNT_DIR}/ca.crt");
        let token = std::fs::read_to_string(&token_path)
            .map_err(|e| format!("could not read ServiceAccount token at {token_path}: {e}"))?;
        let ca = std::fs::read(&ca_path)
            .map_err(|e| format!("could not read Kubernetes CA at {ca_path}: {e}"))?;
        let certs = certificates_from_pem(&ca)
            .map_err(|e| format!("could not parse Kubernetes CA at {ca_path}: {e}"))?;
        if certs.is_empty() {
            return Err(format!(
                "Kubernetes CA at {ca_path} contains no certificate"
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| format!("could not trust Kubernetes CA at {ca_path}: {e}"))?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("could not configure Kubernetes TLS: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let agent = ureq::AgentBuilder::new()
            .tls_config(Arc::new(tls))
            .timeout_connect(Duration::from_secs(10))
            .timeout_read(Duration::from_secs(20))
            .timeout_write(Duration::from_secs(20))
            .redirects(0)
            .build();
        Ok(Self {
            agent,
            base_url: format!("https://{host}:{port}"),
            token: token.trim().to_string(),
            namespace: args.namespace.clone(),
            source_namespace: None,
            secret_name: args.secret_name.clone(),
            secret_key: args.secret_key.clone(),
            public_configmap_name: args.public_configmap_name.clone(),
        })
    }

    fn for_distribution(args: &DistributeArgs) -> Result<Self, String> {
        validate_name("source namespace", &args.source_namespace)?;
        validate_name("target namespace", &args.target_namespace)?;
        validate_name("Secret", &args.secret_name)?;
        validate_data_key("Secret data key", &args.secret_key)?;
        let bootstrap_args = BootstrapArgs {
            namespace: args.target_namespace.clone(),
            secret_name: args.secret_name.clone(),
            secret_key: args.secret_key.clone(),
            public_configmap_name: "unused-by-distribution".to_string(),
            external_secret: None,
        };
        let mut store = Self::in_cluster(&bootstrap_args)?;
        store.source_namespace = Some(args.source_namespace.clone());
        Ok(store)
    }

    fn object(&self, kind: &str, name: &str) -> Result<Value, String> {
        self.object_in(&self.namespace, kind, name)
    }

    fn object_in(&self, namespace: &str, kind: &str, name: &str) -> Result<Value, String> {
        let url = self.url(namespace, kind, name);
        let response = self
            .agent
            .get(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .call()
            .map_err(|e| api_error("get", kind, name, e))?;
        response
            .into_json()
            .map_err(|e| format!("Kubernetes returned invalid JSON for {kind} {name}: {e}"))
    }

    fn patch(&self, kind: &str, name: &str, patch: &Value) -> Result<InitializeResult, String> {
        self.patch_in(&self.namespace, kind, name, patch)
    }

    fn patch_in(
        &self,
        namespace: &str,
        kind: &str,
        name: &str,
        patch: &Value,
    ) -> Result<InitializeResult, String> {
        let url = self.url(namespace, kind, name);
        let result = self
            .agent
            .patch(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .set("Content-Type", "application/json-patch+json")
            .send_json(patch.clone());
        match result {
            Ok(_) => Ok(InitializeResult::Written),
            Err(ureq::Error::Status(status @ (409 | 422), _)) => {
                Ok(InitializeResult::Contended(status))
            }
            Err(error) => Err(api_error("patch", kind, name, error)),
        }
    }

    fn url(&self, namespace: &str, kind: &str, name: &str) -> String {
        format!(
            "{}/api/v1/namespaces/{}/{}/{}",
            self.base_url, namespace, kind, name
        )
    }
}

fn certificates_from_pem(
    pem: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let text = std::str::from_utf8(pem).map_err(|_| "CA bundle is not UTF-8 PEM text")?;
    let mut rest = text;
    let mut certificates = Vec::new();
    while let Some(start) = rest.find(BEGIN) {
        rest = &rest[start + BEGIN.len()..];
        let end = rest
            .find(END)
            .ok_or_else(|| "certificate has no END marker".to_string())?;
        let encoded: String = rest[..end]
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| "certificate body is not valid base64".to_string())?;
        certificates.push(rustls::pki_types::CertificateDer::from(der));
        rest = &rest[end + END.len()..];
    }
    Ok(certificates)
}

impl IdentityStore for KubernetesStore {
    fn managed_private(&mut self) -> Result<PrivateRecord, String> {
        let name = self.secret_name.clone();
        let object = self.object("secrets", &name)?;
        private_record(&object, &name, &self.secret_key)
    }

    fn public_record(&mut self) -> Result<PublicRecord, String> {
        let name = self.public_configmap_name.clone();
        let object = self.object("configmaps", &name)?;
        public_record(&object, &name)
    }

    fn external_private(&mut self, name: &str, key: &str) -> Result<String, String> {
        let object = self.object("secrets", name).map_err(|e| {
            format!("configured external signing Secret {name}/{key} is unavailable: {e}")
        })?;
        decode_secret_key(&object, key).map_err(|e| {
            format!("configured external signing Secret {name}/{key} is unavailable: {e}")
        })
    }

    fn initialize_private(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String> {
        let patch =
            private_initialization_patch(expected_resource_version, has_annotations, key, pem);
        self.patch("secrets", &self.secret_name, &patch)
    }

    fn initialize_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
    ) -> Result<InitializeResult, String> {
        let patch =
            public_initialization_patch(expected_resource_version, has_annotations, material);
        self.patch("configmaps", &self.public_configmap_name, &patch)
    }
}

impl DistributionStore for KubernetesStore {
    fn source_private(&mut self) -> Result<PrivateRecord, String> {
        let namespace = self.source_namespace.as_deref().ok_or_else(|| {
            "identity distribution source namespace is not configured".to_string()
        })?;
        let object = self
            .object_in(namespace, "secrets", &self.secret_name)
            .map_err(|error| {
                format!(
                    "source signing Secret {}/{} is unavailable: {error}",
                    namespace, self.secret_name
                )
            })?;
        private_record(&object, &self.secret_name, &self.secret_key)
    }

    fn target_private(&mut self) -> Result<PrivateRecord, String> {
        self.managed_private().map_err(|error| {
            format!(
                "target signing Secret {}/{} is unavailable: {error}; ensure the authorized namespace exists and Helm can create its prerequisites",
                self.namespace, self.secret_name
            )
        })
    }

    fn initialize_target(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String> {
        let patch =
            private_initialization_patch(expected_resource_version, has_annotations, key, pem);
        self.patch("secrets", &self.secret_name, &patch)
    }
}

fn annotation_patch(has_annotations: bool) -> Value {
    if has_annotations {
        json!({
            "op": "add",
            "path": "/metadata/annotations/logweir.dev~1identity-state",
            "value": ESTABLISHED
        })
    } else {
        json!({
            "op": "add",
            "path": "/metadata/annotations",
            "value": {(IDENTITY_STATE_ANNOTATION): ESTABLISHED}
        })
    }
}

fn private_initialization_patch(
    expected_resource_version: &str,
    has_annotations: bool,
    key: &str,
    pem: &str,
) -> Value {
    let encoded = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
    json!([
        {"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version},
        {"op": "add", "path": "/data", "value": {(key): encoded}},
        annotation_patch(has_annotations)
    ])
}

fn public_initialization_patch(
    expected_resource_version: &str,
    has_annotations: bool,
    material: &PublicMaterial,
) -> Value {
    json!([
        {"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version},
        {"op": "add", "path": "/data", "value": {
            (PUBLIC_KEY_ID): material.key_id,
            (PUBLIC_KEY_PEM): material.spki_pem,
            (PUBLIC_ALGORITHM): material.algorithm,
            (TRUST_REFERENCE): DEFAULT_TRUST_REFERENCE
        }},
        annotation_patch(has_annotations)
    ])
}

fn private_record(object: &Value, name: &str, key: &str) -> Result<PrivateRecord, String> {
    let has_any_data = object
        .get("data")
        .and_then(Value::as_object)
        .is_some_and(|data| !data.is_empty());
    Ok(PrivateRecord {
        resource_version: resource_version(object, "Secret", name)?,
        state: annotation(object, IDENTITY_STATE_ANNOTATION),
        pem: match object.pointer(&format!("/data/{}", json_pointer_escape(key))) {
            Some(value) => Some(decode_base64_string(
                value,
                &format!("Secret {name}/{key}"),
            )?),
            None => None,
        },
        has_any_data,
        has_annotations: object
            .pointer("/metadata/annotations")
            .and_then(Value::as_object)
            .is_some(),
    })
}

fn public_record(object: &Value, name: &str) -> Result<PublicRecord, String> {
    let data = object.get("data").and_then(Value::as_object);
    let get = |key: &str| {
        data.and_then(|map| map.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    Ok(PublicRecord {
        resource_version: resource_version(object, "ConfigMap", name)?,
        state: annotation(object, IDENTITY_STATE_ANNOTATION),
        key_id: get(PUBLIC_KEY_ID),
        spki_pem: get(PUBLIC_KEY_PEM),
        algorithm: get(PUBLIC_ALGORITHM),
        trust_reference: get(TRUST_REFERENCE),
        data_keys: data
            .map(|values| values.keys().cloned().collect())
            .unwrap_or_default(),
        has_any_data: data.is_some_and(|values| !values.is_empty()),
        has_annotations: object
            .pointer("/metadata/annotations")
            .and_then(Value::as_object)
            .is_some(),
    })
}

fn decode_secret_key(object: &Value, key: &str) -> Result<String, String> {
    let value = object
        .pointer(&format!("/data/{}", json_pointer_escape(key)))
        .ok_or_else(|| "required data key is absent".to_string())?;
    decode_base64_string(value, "Secret data")
}

fn decode_base64_string(value: &Value, label: &str) -> Result<String, String> {
    let encoded = value
        .as_str()
        .ok_or_else(|| format!("{label} is not a base64 string"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| format!("{label} is not valid base64"))?;
    String::from_utf8(bytes).map_err(|_| format!("{label} is not UTF-8 PEM text"))
}

fn resource_version(object: &Value, kind: &str, name: &str) -> Result<String, String> {
    object
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("Kubernetes {kind} {name} has no metadata.resourceVersion"))
}

fn annotation(object: &Value, key: &str) -> Option<String> {
    object
        .pointer(&format!(
            "/metadata/annotations/{}",
            json_pointer_escape(key)
        ))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn json_pointer_escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn validate_name(label: &str, value: &str) -> Result<(), String> {
    let max_length = if label.contains("namespace") { 63 } else { 253 };
    let valid_label = |part: &str| {
        !part.is_empty()
            && part.len() <= 63
            && part
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && part
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && part
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
    };
    let valid = !value.is_empty() && value.len() <= max_length && value.split('.').all(valid_label);
    if valid {
        Ok(())
    } else {
        Err(format!("{label} name is not a Kubernetes DNS name"))
    }
}

fn validate_data_key(label: &str, value: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(format!("{label} is not a valid Secret data key"))
    }
}

fn api_error(operation: &str, kind: &str, name: &str, error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            // Drain a bounded amount so pooled connections remain healthy,
            // but never include a Kubernetes body: a Secret GET response may
            // contain private key data.
            let mut sink = [0_u8; 1024];
            let _ = response.into_reader().read(&mut sink);
            format!("Kubernetes {operation} {kind} {name} returned HTTP {code}")
        }
        ureq::Error::Transport(error) => {
            format!("Kubernetes {operation} {kind} {name} failed: {error}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_evidence::{sign, verify, PAYLOAD_TYPE_BACKUP_RECEIPT};
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::thread;

    struct MemoryStore {
        private: PrivateRecord,
        public: PublicRecord,
        external: Result<String, String>,
        private_patch_error: Option<String>,
        concurrent_private: Option<String>,
        empty_private_contention: bool,
        public_conflict: bool,
    }

    impl MemoryStore {
        fn fresh() -> Self {
            Self {
                private: PrivateRecord {
                    resource_version: "1".into(),
                    state: Some("uninitialized".into()),
                    pem: None,
                    has_any_data: false,
                    has_annotations: true,
                },
                public: PublicRecord {
                    resource_version: "1".into(),
                    state: Some("uninitialized".into()),
                    key_id: None,
                    spki_pem: None,
                    algorithm: None,
                    trust_reference: None,
                    data_keys: BTreeSet::new(),
                    has_any_data: false,
                    has_annotations: true,
                },
                external: Err("HTTP 404".into()),
                private_patch_error: None,
                concurrent_private: None,
                empty_private_contention: false,
                public_conflict: false,
            }
        }

        fn establish_public(&mut self, material: &PublicMaterial) {
            self.public.state = Some(ESTABLISHED.into());
            self.public.key_id = Some(material.key_id.clone());
            self.public.spki_pem = Some(material.spki_pem.clone());
            self.public.algorithm = Some(material.algorithm.clone());
            self.public.trust_reference = Some(DEFAULT_TRUST_REFERENCE.into());
            self.public.data_keys = BTreeSet::from([
                PUBLIC_KEY_ID.to_string(),
                PUBLIC_KEY_PEM.to_string(),
                PUBLIC_ALGORITHM.to_string(),
                TRUST_REFERENCE.to_string(),
            ]);
            self.public.has_any_data = true;
            self.public.resource_version = "2".into();
        }
    }

    impl IdentityStore for MemoryStore {
        fn managed_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.private.clone())
        }

        fn public_record(&mut self) -> Result<PublicRecord, String> {
            Ok(self.public.clone())
        }

        fn external_private(&mut self, _name: &str, _key: &str) -> Result<String, String> {
            self.external.clone()
        }

        fn initialize_private(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            _key: &str,
            pem: &str,
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.private.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            if let Some(error) = self.private_patch_error.take() {
                return Err(error);
            }
            if self.empty_private_contention {
                return Ok(InitializeResult::Contended(422));
            }
            if let Some(winner) = self.concurrent_private.take() {
                self.private.pem = Some(winner);
                self.private.has_any_data = true;
                self.private.state = Some(ESTABLISHED.into());
                self.private.resource_version = "2".into();
                return Ok(InitializeResult::Contended(422));
            }
            self.private.pem = Some(pem.to_string());
            self.private.has_any_data = true;
            self.private.state = Some(ESTABLISHED.into());
            self.private.resource_version = "2".into();
            Ok(InitializeResult::Written)
        }

        fn initialize_public(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            material: &PublicMaterial,
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.public.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            self.establish_public(material);
            if self.public_conflict {
                Ok(InitializeResult::Contended(422))
            } else {
                Ok(InitializeResult::Written)
            }
        }
    }

    fn args(external: bool) -> BootstrapArgs {
        BootstrapArgs {
            namespace: "logweir-system".into(),
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
            public_configmap_name: "logweir-signing-trust".into(),
            external_secret: external.then(|| ("company-signer".into(), "identity.pem".into())),
        }
    }

    fn pem(key: &SigningKey) -> String {
        key.to_pkcs8_pem().unwrap()
    }

    struct MemoryDistributionStore {
        source: PrivateRecord,
        target: PrivateRecord,
        concurrent_target: Option<String>,
    }

    impl DistributionStore for MemoryDistributionStore {
        fn source_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.source.clone())
        }

        fn target_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.target.clone())
        }

        fn initialize_target(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            _key: &str,
            pem: &str,
        ) -> Result<InitializeResult, String> {
            if let Some(winner) = self.concurrent_target.take() {
                self.target.pem = Some(winner);
                self.target.has_any_data = true;
                self.target.state = Some(ESTABLISHED.into());
                self.target.resource_version = "2".into();
                return Ok(InitializeResult::Contended(422));
            }
            if expected_resource_version != self.target.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            self.target.pem = Some(pem.to_string());
            self.target.has_any_data = true;
            self.target.state = Some(ESTABLISHED.into());
            self.target.resource_version = "2".into();
            Ok(InitializeResult::Written)
        }
    }

    fn distribution_store(source_key: &SigningKey) -> MemoryDistributionStore {
        MemoryDistributionStore {
            source: PrivateRecord {
                resource_version: "8".into(),
                state: Some(ESTABLISHED.into()),
                pem: Some(pem(source_key)),
                has_any_data: true,
                has_annotations: true,
            },
            target: PrivateRecord {
                resource_version: "1".into(),
                state: None,
                pem: None,
                has_any_data: false,
                has_annotations: false,
            },
            concurrent_target: None,
        }
    }

    fn distribution_args() -> DistributeArgs {
        DistributeArgs {
            source_namespace: "logweir-system".into(),
            target_namespace: "recoveries".into(),
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
        }
    }

    fn store_for_http(base_url: String) -> KubernetesStore {
        KubernetesStore {
            agent: ureq::AgentBuilder::new().redirects(0).build(),
            base_url,
            token: "test-token".into(),
            namespace: "logweir-system".into(),
            source_namespace: None,
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
            public_configmap_name: "logweir-signing-trust".into(),
        }
    }

    fn one_response(status: u16, reason: &'static str) -> (String, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let mut expected = None;
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if expected.is_none() {
                    if let Some(split) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..split]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|value| value.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        expected = Some(split + 4 + length);
                    }
                }
                if expected.is_some_and(|length| request.len() >= length) {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            request
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn fresh_install_generates_persistent_private_and_public_material() {
        let mut store = MemoryStore::fresh();
        let outcome = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(outcome.origin, Origin::Generated);
        assert!(store.private.pem.is_some());
        assert_eq!(store.private.state.as_deref(), Some(ESTABLISHED));
        assert_eq!(
            store.public.key_id.as_deref(),
            Some(outcome.key_id.as_str())
        );
        assert_eq!(
            store.public.trust_reference.as_deref(),
            Some(DEFAULT_TRUST_REFERENCE)
        );
        assert!(!store
            .public
            .spki_pem
            .as_deref()
            .unwrap()
            .contains("PRIVATE"));
    }

    #[test]
    fn explicit_external_key_is_adopted_and_then_idempotent_on_upgrade() {
        let key = SigningKey::generate_ed25519();
        let key_pem = pem(&key);
        let expected = key.key_id();
        let mut store = MemoryStore::fresh();
        store.external = Ok(key_pem.clone());

        let first = bootstrap(&mut store, &args(true)).unwrap();
        assert_eq!(first.origin, Origin::Adopted);
        assert_eq!(first.key_id, expected);
        let stored = store.private.pem.clone();

        let second = bootstrap(&mut store, &args(true)).unwrap();
        assert_eq!(second.origin, Origin::Existing);
        assert_eq!(
            store.private.pem, stored,
            "upgrade must leave private bytes unchanged"
        );
    }

    #[test]
    fn concurrent_bootstrap_accepts_the_atomic_winner_and_publishes_only_it() {
        let winner = SigningKey::generate_p256();
        let winner_id = winner.key_id();
        let mut store = MemoryStore::fresh();
        store.concurrent_private = Some(pem(&winner));

        let outcome = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(outcome.origin, Origin::ConcurrentWinner(422));
        assert_eq!(outcome.key_id, winner_id);
        assert_eq!(store.public.key_id.as_deref(), Some(winner_id.as_str()));
    }

    #[test]
    fn external_contention_accepts_only_the_configured_candidate() {
        let candidate = SigningKey::generate_ed25519();
        let different = SigningKey::generate_ed25519();
        let mut store = MemoryStore::fresh();
        store.external = Ok(pem(&candidate));
        store.concurrent_private = Some(pem(&different));

        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(error.contains("while external identity"), "{error}");
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn malformed_contended_winner_retains_http_422() {
        let mut store = MemoryStore::fresh();
        store.concurrent_private = Some("not a PKCS#8 key".into());

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(error.contains("not a P-256 or Ed25519"), "{error}");
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn contended_patch_without_a_real_established_winner_retains_http_422() {
        let mut store = MemoryStore::fresh();
        store.empty_private_contention = true;

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(
            error.contains("no compatible established winner"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn denied_secret_patch_fails_without_publication_or_key_disclosure() {
        let mut store = MemoryStore::fresh();
        store.private_patch_error =
            Some("Kubernetes patch secrets logweir-signing-key returned HTTP 403".into());
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 403"), "{error}");
        assert!(store.private.pem.is_none());
        assert!(store.public.key_id.is_none());
        assert!(!error.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn configured_external_secret_unavailability_fails_without_fallback_generation() {
        let mut store = MemoryStore::fresh();
        store.external = Err(
            "configured external signing Secret company-signer/identity.pem is unavailable: Kubernetes get secrets company-signer returned HTTP 404".into(),
        );
        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(error.contains("unavailable"), "{error}");
        assert!(
            store.private.pem.is_none(),
            "external mode must never fall back to generation"
        );
    }

    #[test]
    fn missing_private_with_established_public_trust_is_lost_key_not_rotation() {
        let old = SigningKey::generate_p256();
        let material = public_material(&old).unwrap();
        let mut store = MemoryStore::fresh();
        store.establish_public(&material);
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(
            error.contains("restore logweir-signing-key from backup"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert_eq!(
            store.public.key_id.as_deref(),
            Some(material.key_id.as_str())
        );
    }

    #[test]
    fn an_existing_secret_with_the_wrong_data_key_is_never_overwritten() {
        let mut store = MemoryStore::fresh();
        store.private.has_any_data = true;
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(
            error.contains("contains data but not the required key"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert!(store.public.key_id.is_none());
    }

    #[test]
    fn a_nonempty_public_configmap_is_never_treated_as_a_placeholder() {
        let key = SigningKey::generate_p256();
        let material = public_material(&key).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&key));
        store.private.has_any_data = true;
        store.public.has_any_data = true;

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("unrelated data"), "{error}");
        assert!(store.public.key_id.is_none());

        store.public.has_any_data = false;
        store.public.trust_reference = Some(DEFAULT_TRUST_REFERENCE.into());
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("does not exactly match"), "{error}");
        assert_eq!(material.key_id, key.key_id());
    }

    #[test]
    fn established_public_record_rejects_every_extra_data_key() {
        let key = SigningKey::generate_p256();
        let material = public_material(&key).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&key));
        store.private.has_any_data = true;
        store.establish_public(&material);
        store.public.data_keys.insert("unexpected".into());

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("does not exactly match"), "{error}");
    }

    #[test]
    fn upgrade_retains_identity_and_old_archive_verification() {
        let mut store = MemoryStore::fresh();
        let first = bootstrap(&mut store, &args(false)).unwrap();
        let private_before = store.private.pem.clone().unwrap();
        let public_before = store.public.spki_pem.clone().unwrap();
        let signer = SigningKey::from_pkcs8_pem(&private_before).unwrap();
        let archive = br#"{"archive":"created-before-upgrade"}"#;
        let signature = sign::sign_detached(&signer, PAYLOAD_TYPE_BACKUP_RECEIPT, archive).unwrap();

        let second = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(second.origin, Origin::Existing);
        assert_eq!(second.key_id, first.key_id);
        assert_eq!(store.private.pem.as_deref(), Some(private_before.as_str()));
        assert_eq!(
            store.public.spki_pem.as_deref(),
            Some(public_before.as_str())
        );
        let verifier = logweir_evidence::keys::VerifyingKey::from_pem_str(&public_before).unwrap();
        verify::verify_detached(&verifier, PAYLOAD_TYPE_BACKUP_RECEIPT, archive, &signature)
            .expect("an archive signed before upgrade remains verifiable");
    }

    #[test]
    fn external_adoption_cannot_replace_an_established_identity() {
        let established = SigningKey::generate_p256();
        let replacement = SigningKey::generate_p256();
        let material = public_material(&established).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&established));
        store.private.state = Some(ESTABLISHED.into());
        store.establish_public(&material);
        store.external = Ok(pem(&replacement));

        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(
            error.contains("never rotates an existing signer"),
            "{error}"
        );
        assert_eq!(
            store.private.pem.as_deref(),
            Some(pem(&established).as_str())
        );
    }

    #[test]
    fn kubernetes_ca_parser_accepts_a_chain_and_rejects_truncation() {
        let pem = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n\
                    -----BEGIN CERTIFICATE-----\nBAUG\n-----END CERTIFICATE-----\n";
        let certs = certificates_from_pem(pem).unwrap();
        assert_eq!(2, certs.len());
        assert_eq!(certs[0].as_ref(), &[1, 2, 3]);
        assert_eq!(certs[1].as_ref(), &[4, 5, 6]);
        assert!(certificates_from_pem(b"-----BEGIN CERTIFICATE-----\nAQID").is_err());
    }

    #[test]
    fn kubernetes_names_are_validated_before_building_api_urls() {
        assert!(validate_name("namespace", "logweir-system").is_ok());
        assert!(validate_name("Secret", "company.signer-1").is_ok());
        for invalid in [
            "",
            "UPPER",
            "-leading",
            "trailing-",
            "two..dots",
            "slash/name",
        ] {
            assert!(
                validate_name("Secret", invalid).is_err(),
                "accepted {invalid}"
            );
        }
        assert!(validate_name("namespace", &"a".repeat(64)).is_err());
    }

    #[test]
    fn distribution_copies_the_same_identity_and_refuses_namespace_fragmentation() {
        let installation = SigningKey::generate_p256();
        let expected_pem = pem(&installation);
        let expected_id = installation.key_id();
        let mut store = distribution_store(&installation);

        let outcome = distribute(&mut store, &distribution_args()).unwrap();
        assert_eq!(outcome.origin, Origin::Adopted);
        assert_eq!(outcome.key_id, expected_id);
        assert_eq!(store.target.pem.as_deref(), Some(expected_pem.as_str()));

        store.target.pem = Some(pem(&SigningKey::generate_p256()));
        let error = distribute(&mut store, &distribution_args()).unwrap_err();
        assert!(error.contains("different signer"), "{error}");
        assert!(error.contains("never overwrites"), "{error}");
    }

    #[test]
    fn distribution_accepts_only_the_same_concurrent_winner() {
        let installation = SigningKey::generate_ed25519();
        let expected_id = installation.key_id();
        let mut same = distribution_store(&installation);
        same.concurrent_target = Some(pem(&installation));
        let outcome = distribute(&mut same, &distribution_args()).unwrap();
        assert_eq!(outcome.origin, Origin::ConcurrentWinner(422));
        assert_eq!(outcome.key_id, expected_id);

        let mut different = distribution_store(&installation);
        different.concurrent_target = Some(pem(&SigningKey::generate_ed25519()));
        let error = distribute(&mut different, &distribution_args()).unwrap_err();
        assert!(error.contains("concurrently established a different signer"));
        assert!(error.contains("HTTP 422"), "{error}");
    }

    #[test]
    fn json_patch_handles_missing_annotations_and_http_statuses_faithfully() {
        let (base_url, request_handle) = one_response(409, "Conflict");
        let mut store = store_for_http(base_url);
        let result = store
            .initialize_private("17", false, "signing.pem", "private-pem")
            .unwrap();
        assert_eq!(result, InitializeResult::Contended(409));

        let request = request_handle.join().unwrap();
        let split = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let headers = String::from_utf8_lossy(&request[..split]);
        assert!(
            headers.starts_with(
                "PATCH /api/v1/namespaces/logweir-system/secrets/logweir-signing-key HTTP/1.1"
            ),
            "{headers}"
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("content-type: application/json-patch+json"),
            "{headers}"
        );
        let body: Value = serde_json::from_slice(&request[split + 4..]).unwrap();
        assert_eq!(
            body[0],
            json!({"op":"test","path":"/metadata/resourceVersion","value":"17"})
        );
        assert_eq!(body[2]["path"], "/metadata/annotations");
        assert_eq!(body[2]["value"][IDENTITY_STATE_ANNOTATION], ESTABLISHED);

        let present = private_initialization_patch("18", true, "signing.pem", "private-pem");
        assert_eq!(
            present[2]["path"],
            "/metadata/annotations/logweir.dev~1identity-state"
        );

        let (base_url, request_handle) = one_response(422, "Unprocessable Entity");
        let mut store = store_for_http(base_url);
        let result = store
            .initialize_private("19", false, "signing.pem", "private-pem")
            .unwrap();
        request_handle.join().unwrap();
        assert_eq!(result, InitializeResult::Contended(422));
    }
}
