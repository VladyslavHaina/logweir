//! **PROD-03.1: the source registry's snapshot, after the engine**, and the
//! one HTTP client every registry call of this crate goes through (the backup
//! capture, the restore import in `drill::schema_import`, and the two check
//! rows).
//!
//! The pure half — what is read, what the receipt says, the bounds — is
//! `logweir_core::schema_registry`. This module dials: one bounded `ureq`
//! agent (10 s per request, no redirects), HTTP basic auth when the plan names
//! a username, and the connection's `auth.tlsCa`, when it is set, added to the
//! webpki roots. The password comes from `<SIDE>_REGISTRY_PASSWORD` only after
//! its Secret's binding was compared (`crate::credential_binding::
//! registry_password`); it is never logged, and an error names the request,
//! never a header.

use std::io::Read as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use logweir_core::schema_registry::{
    capture, CaptureFailure, ReceiptSchemaRegistry, Rest, Snapshot, MAX_SNAPSHOT_BYTES,
};
use logweir_core::spec::SchemaRegistrySpec;
use serde_json::Value;

/// One request's bound.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The whole capture's bound.
pub const CAPTURE_BUDGET: Duration = Duration::from_secs(300);

/// The bounded agent: connect, read and write at [`REQUEST_TIMEOUT`], no
/// redirect followed (a redirect could carry the credential elsewhere).
fn registry_agent(tls: Option<Arc<rustls::ClientConfig>>) -> ureq::Agent {
    let builder = ureq::AgentBuilder::new()
        .timeout_connect(REQUEST_TIMEOUT)
        .timeout_read(REQUEST_TIMEOUT)
        .timeout_write(REQUEST_TIMEOUT)
        .redirects(0);
    match tls {
        Some(tls) => builder.tls_config(tls),
        None => builder,
    }
    .build()
}

/// The webpki roots plus the PEM certificates in `ca_file`.
fn tls_with(ca_file: &str) -> Result<Arc<rustls::ClientConfig>, String> {
    use rustls::pki_types::pem::PemObject as _;
    let pem = std::fs::read(ca_file)
        .map_err(|e| format!("the connection's CA file {ca_file} could not be read: {e}"))?;
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let mut added = 0usize;
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        let cert = cert.map_err(|e| format!("the CA file {ca_file} is not PEM: {e}"))?;
        roots
            .add(cert)
            .map_err(|e| format!("the CA file {ca_file} could not be trusted: {e}"))?;
        added += 1;
    }
    if added == 0 {
        return Err(format!("the CA file {ca_file} holds no certificate"));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS could not be configured: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(tls))
}

/// A registry, dialled.
pub struct RegistryClient {
    agent: ureq::Agent,
    base: String,
    authorization: Option<String>,
}

impl std::fmt::Debug for RegistryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryClient")
            .field("base", &self.base)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl RegistryClient {
    /// A client for `spec`, with `password` (when the plan names a username)
    /// and the side's CA file.
    ///
    /// # Errors
    ///
    /// The CA file could not be read or parsed.
    pub fn new(
        spec: &SchemaRegistrySpec,
        password: Option<&str>,
        ca_file: Option<&str>,
    ) -> Result<Self, String> {
        use base64::Engine as _;
        let tls = ca_file.map(tls_with).transpose()?;
        let authorization = spec.username.as_ref().map(|user| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{user}:{}", password.unwrap_or_default()))
            )
        });
        Ok(Self {
            agent: registry_agent(tls),
            base: spec.url.trim_end_matches('/').to_string(),
            authorization,
        })
    }
}

impl Rest for RegistryClient {
    fn call(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String> {
        let mut request = self
            .agent
            .request(method, &format!("{}{path}", self.base))
            .set(
                "Accept",
                "application/vnd.schemaregistry.v1+json, application/json",
            );
        if let Some(a) = &self.authorization {
            request = request.set("Authorization", a);
        }
        let answer = match body {
            Some(b) => request
                .set("Content-Type", "application/vnd.schemaregistry.v1+json")
                .send_string(&b.to_string()),
            None => request.call(),
        };
        let response = match answer {
            Ok(r) => r,
            Err(ureq::Error::Status(_, r)) => r,
            Err(ureq::Error::Transport(t)) => {
                return Err(format!("{method} {path}: {}", t.kind()));
            }
        };
        let status = response.status();
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take(MAX_SNAPSHOT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| format!("{method} {path}: the answer could not be read: {e}"))?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(format!(
                "{method} {path}: the answer is over {MAX_SNAPSHOT_BYTES} bytes"
            ));
        }
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }
}

/// The capture, written: the snapshot's bytes beside the receipt and the
/// receipt's block. `bytes` is `Some` exactly when the block says captured.
#[derive(Debug)]
pub struct Captured {
    /// The receipt's `schema_registry` block.
    pub block: ReceiptSchemaRegistry,
    /// The snapshot object's exact bytes.
    pub bytes: Option<Vec<u8>>,
}

/// Snapshot the source registry the plan names, after the engine. Never a
/// failed backup: every failure is the block's reason, logged at `warn`.
#[must_use]
pub fn snapshot(
    spec: &SchemaRegistrySpec,
    backup_id: &str,
    run_id: &str,
    dependency: &std::collections::BTreeMap<
        String,
        logweir_core::backup_receipt::TopicSchemaDependency,
    >,
) -> Captured {
    let failed = |failure: CaptureFailure| {
        tracing::warn!(
            registry = %spec.url,
            reason = %failure,
            "the source Schema Registry was not captured; the backup is signed and records why"
        );
        Captured {
            block: ReceiptSchemaRegistry::not_captured(&failure),
            bytes: None,
        }
    };
    let password = match crate::credential_binding::registry_password(crate::tls_ca::Side::Source) {
        Ok(p) => p,
        // Refused at start-up already (`check_projected_bindings`); a run
        // that reaches here with a mismatch presents nothing.
        Err(e) => return failed(CaptureFailure::Unreachable(e.to_string())),
    };
    let ca = match crate::tls_ca::projected_ca_file(crate::tls_ca::SOURCE_TLS_CA_FILE_VAR) {
        Ok(ca) => ca,
        Err(e) => return failed(CaptureFailure::Unreachable(e)),
    };
    let mut client = match RegistryClient::new(spec, password.as_deref(), ca.as_deref()) {
        Ok(c) => c,
        Err(e) => return failed(CaptureFailure::Unreachable(e)),
    };
    let captured_at = chrono::Utc::now();
    let started = Instant::now();
    let versions = match capture(&mut client, &|| started.elapsed() > CAPTURE_BUDGET) {
        Ok(v) => v,
        Err(failure) => return failed(failure),
    };
    let snapshot = Snapshot {
        format: logweir_core::schema_registry::SNAPSHOT_FORMAT,
        captured_at,
        registry_url: spec.url.clone(),
        versions,
    };
    let bytes = match snapshot.to_bytes() {
        Ok(b) => b,
        Err(failure) => return failed(failure),
    };
    let block = ReceiptSchemaRegistry::captured(
        &snapshot,
        logweir_core::schema_registry::snapshot_key(backup_id, run_id),
        logweir_core::ids::sha256_prefixed(&bytes),
        dependency,
    );
    tracing::info!(
        registry = %spec.url,
        subjects = block.subjects,
        versions = block.versions,
        complete = block.complete(),
        "the source Schema Registry was captured"
    );
    Captured {
        block,
        bytes: Some(bytes),
    }
}
