use std::path::Path;

/// One execution's private key, parsed and exercised before data work starts.
///
/// The value deliberately owns the parsed key. Callers keep it for the whole
/// execution so replacing or removing the mounted file cannot change the
/// identity used by later evidence documents.
pub struct ValidatedSigner {
    key: logweir_evidence::keys::SigningKey,
}

impl ValidatedSigner {
    pub fn load(
        path: &Path,
        probe_payload_type: &str,
        probe: &[u8],
        failure_effect: &str,
    ) -> Result<Self, String> {
        Self::load_with_probe_signer(
            path,
            probe_payload_type,
            probe,
            failure_effect,
            |key, payload_type, bytes| {
                // The current P-256 and Ed25519 software branches are
                // infallible after parsing and always return `Ok`; retain the
                // library's `Result` shape for forward compatibility without
                // pretending an unreadable file exercises this branch.
                logweir_evidence::sign::sign_detached(key, payload_type, bytes)
                    .map_err(|e| e.to_string())
            },
        )
    }

    /// Private readiness seam: production supplies the one software signer,
    /// while the unit test can return a real but non-verifying signature to
    /// exercise the independently implemented verification failure. It does
    /// not inject a synthetic signing error: no such failure is reachable for
    /// either parsed software-key variant today. Keeping the seam here avoids
    /// a public signer-provider abstraction for a backend that does not have
    /// one yet.
    fn load_with_probe_signer(
        path: &Path,
        probe_payload_type: &str,
        probe: &[u8],
        failure_effect: &str,
        sign_probe: impl FnOnce(
            &logweir_evidence::keys::SigningKey,
            &str,
            &[u8],
        ) -> Result<logweir_evidence::Sidecar, String>,
    ) -> Result<Self, String> {
        let prerequisite = |detail: String| {
            format!(
                "signing prerequisite `{}` is not ready: {detail}. Mount a readable P-256 or \
                 Ed25519 PKCS#8 PEM private key at --signing-key and retry. {failure_effect}",
                path.display(),
            )
        };
        let key = logweir_evidence::keys::SigningKey::from_pem_file(path)
            .map_err(|e| prerequisite(e.to_string()))?;

        // Parsing establishes format, not capability. Exercise the same
        // signing implementation used for evidence and independently verify
        // the result with the derived public half before releasing the key.
        let sidecar = sign_probe(&key, probe_payload_type, probe)
            .map_err(|e| prerequisite(format!("the key could not sign a readiness probe: {e}")))?;
        logweir_evidence::verify::verify_detached(
            &key.verifying_key(),
            probe_payload_type,
            probe,
            &sidecar,
        )
        .map_err(|e| {
            prerequisite(format!(
                "a readiness-probe signature did not verify with the key's public half: {e}"
            ))
        })?;

        Ok(Self { key })
    }

    pub fn verifying_key(&self) -> logweir_evidence::keys::VerifyingKey {
        self.key.verifying_key()
    }

    pub fn sign(
        &self,
        payload_type: &str,
        bytes: &[u8],
    ) -> Result<logweir_evidence::Sidecar, String> {
        logweir_evidence::sign::sign_detached(&self.key, payload_type, bytes)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ValidatedSigner;
    use logweir_evidence::keys::SigningKey;

    #[test]
    fn readiness_rejects_a_real_signature_that_does_not_verify_over_its_probe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("signer.pem");
        let key = SigningKey::generate_ed25519();
        std::fs::write(&path, key.to_pkcs8_pem().unwrap()).unwrap();

        let result = ValidatedSigner::load_with_probe_signer(
            &path,
            logweir_evidence::PAYLOAD_TYPE_SCORECARD,
            b"expected readiness probe",
            "No engine data operation was started",
            |key, payload_type, _expected_probe| {
                // This is a genuine, structurally valid signature from the
                // parsed key, but over different bytes. The production
                // verifier—not a boolean test double—must reject it.
                logweir_evidence::sign::sign_detached(key, payload_type, b"DO-NOT-ECHO-WRONG-PROBE")
                    .map_err(|e| e.to_string())
            },
        );
        let error = match result {
            Ok(_) => panic!("a non-verifying readiness signature must fail closed"),
            Err(error) => error,
        };

        assert!(
            error.contains("a readiness-probe signature did not verify with the key's public half"),
            "{error}"
        );
        assert!(error.contains(&path.display().to_string()), "{error}");
        assert!(
            error.contains("Mount a readable P-256 or Ed25519 PKCS#8 PEM private key"),
            "{error}"
        );
        assert!(
            error.contains("No engine data operation was started"),
            "{error}"
        );
        assert!(!error.contains("DO-NOT-ECHO-WRONG-PROBE"), "{error}");
    }
}
