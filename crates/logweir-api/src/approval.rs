//! PLAT-19.2 — the console's half of ordinary confirmation and governed
//! approval (decision D0, "Ordinary versus governed approval contract").
//!
//! # What the console does, and what it never does
//!
//! After an operator's Restore create is accepted, the console builds the
//! AUTHORIZATION DOCUMENT V2 for that exact object — kind, namespace, name,
//! UID, the plan hash, the requester it authenticated, the namespace's bound
//! policy name and snapshot digest, an issue time and an expiry no later than
//! the policy's `maxAgeSeconds` — and signs it with its `ConsoleConfirmation`
//! key. That signature attests ONE thing: which authenticated principal asked.
//!
//! * **Ordinary**: the signed document is the whole authorization, stored as
//!   the `Approval` the Restore references.
//! * **Governed**: the signed document is stored as a CONFIRMATION object
//!   (`<approvalRef>-confirmation`) that authorises nothing; an approver
//!   countersigns the same bytes and submits them, and only then does the
//!   referenced `Approval` exist.
//! * **Unbound** (`legacy-governed-v1`): nothing is signed. The Restore waits
//!   for today's v1 approval exactly as before.
//!
//! The controller is the final gate (D0): it re-verifies the console's
//! signature against the namespace's `TrustPolicy`, the policy digest against
//! its own copy of the same installation document, and — under Governed — the
//! approver's signature and principal. Nothing here is trusted by the
//! controller that it does not re-derive.
//!
//! # The key
//!
//! A PKCS#8 PEM file, mounted from a Secret and read once at startup. It is
//! never logged, never serialised, never in a response; only its key id and
//! public half are published, so an administrator can add the public half to
//! the namespace's `TrustPolicy` with usage `ConsoleConfirmation`. A
//! `ConsoleConfirmation` key can never verify evidence (D3 §7.3,
//! `weirkeeper::verification`), so holding it gives this service no evidence
//! signing capability — which is why linking the signer here is recorded in
//! `scripts/check-one-signer.sh` rather than slipped in.

use std::path::Path;

use chrono::{DateTime, Utc};
use logweir_core::approval_policy::{
    ApprovalMode, ApprovalPolicy, ApprovalPolicySet, AuthorizedSubject, PolicyRef, Requester,
    RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, RESTORE_AUTHORIZATION_FORMAT_VERSION,
    RESTORE_AUTHORIZATION_KIND, SUBJECT_API_VERSION, SUBJECT_KIND_RESTORE,
};
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use logweir_evidence::Sidecar;

/// The suffix of a governed request's confirmation object.
pub const CONFIRMATION_SUFFIX: &str = "-confirmation";

/// The longest `approvalRef.name` whose confirmation name still fits the
/// 253-character object-name limit.
pub const MAX_GOVERNED_APPROVAL_NAME: usize = 253 - CONFIRMATION_SUFFIX.len();

/// The confirmation object's name for a governed request.
#[must_use]
pub fn confirmation_name(approval_name: &str) -> String {
    format!("{approval_name}{CONFIRMATION_SUFFIX}")
}

/// The console's `ConsoleConfirmation` signing key.
pub struct ConfirmationKey {
    key: SigningKey,
    key_id: String,
    public_pem: String,
}

impl std::fmt::Debug for ConfirmationKey {
    /// THE PRIVATE HALF IS NEVER FORMATTED. Only the key id, which is public.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfirmationKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl ConfirmationKey {
    /// Read a PKCS#8 PEM private key file.
    ///
    /// # Errors
    ///
    /// A reason naming the PATH, never the contents.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let key = SigningKey::from_pem_file(path).map_err(|e| {
            format!(
                "cannot read the console confirmation key {}: {e}",
                path.display()
            )
        })?;
        Self::from_key(key)
    }

    /// Wrap an in-memory key (tests, and the file loader above).
    ///
    /// # Errors
    ///
    /// When the public half cannot be rendered.
    pub fn from_key(key: SigningKey) -> Result<Self, String> {
        let public_pem = key
            .verifying_key()
            .to_public_key_pem()
            .map_err(|e| format!("cannot render the confirmation key's public half: {e}"))?;
        Ok(Self {
            key_id: key.key_id(),
            key,
            public_pem,
        })
    }

    /// The key id an administrator puts on the `TrustPolicy`.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The public half, SPKI PEM.
    #[must_use]
    pub fn public_pem(&self) -> &str {
        &self.public_pem
    }

    /// Sign exact document bytes under the v2 payload type.
    ///
    /// # Errors
    ///
    /// When the signer fails.
    pub fn sign(&self, document: &[u8]) -> Result<Sidecar, String> {
        sign_detached(&self.key, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, document)
            .map_err(|e| format!("signing the authorization document: {e}"))
    }
}

/// What this process knows about approval policy: the installation document
/// (the same bytes the controller reads) and the console's key.
#[derive(Debug, Default)]
pub struct ApprovalSettings {
    /// The installation's policies and bindings.
    pub policies: ApprovalPolicySet,
    /// The console's confirmation key; required whenever a served namespace
    /// is bound to an explicit policy.
    pub confirmation: Option<ConfirmationKey>,
}

impl ApprovalSettings {
    /// Load both files and check that they fit the namespaces this console
    /// serves.
    ///
    /// # Errors
    ///
    /// A reason naming the file or the namespace.
    pub fn load(
        policy_file: Option<&Path>,
        key_file: Option<&Path>,
        namespaces: &[String],
    ) -> Result<Self, String> {
        let policies = match policy_file {
            None => ApprovalPolicySet::default(),
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|e| {
                    format!(
                        "cannot read the approval-policy file {}: {e}",
                        path.display()
                    )
                })?;
                ApprovalPolicySet::parse(&text).map_err(|e| format!("{}: {e}", path.display()))?
            }
        };
        let confirmation = key_file.map(ConfirmationKey::from_file).transpose()?;
        let settings = Self {
            policies,
            confirmation,
        };
        settings.check(namespaces)?;
        Ok(settings)
    }

    /// A served namespace bound to an explicit policy needs the console key:
    /// both modes carry the console's signature, and without the key every
    /// Restore there would be created and then never authorisable.
    ///
    /// # Errors
    ///
    /// A reason naming the first such namespace.
    pub fn check(&self, namespaces: &[String]) -> Result<(), String> {
        if self.confirmation.is_some() {
            return Ok(());
        }
        if let Some(namespace) = namespaces
            .iter()
            .find(|ns| !self.policies.resolve(ns).is_legacy())
        {
            return Err(format!(
                "namespace {namespace} is bound to approval policy {} and this console has no \
                 `confirmationKeyFile`: both ordinary and governed requests carry the console's \
                 ConsoleConfirmation signature (D0), so configure the key or unbind the \
                 namespace",
                self.policies.resolve(namespace).name()
            ));
        }
        Ok(())
    }
}

/// Build the authorization document v2 for one created Restore.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn document(
    policy: &ApprovalPolicy,
    namespace: &str,
    name: &str,
    uid: &str,
    plan_hash: &str,
    requester: Requester,
    issued_at: DateTime<Utc>,
    ticket: Option<String>,
) -> RestoreAuthorization {
    RestoreAuthorization {
        format_version: RESTORE_AUTHORIZATION_FORMAT_VERSION.to_string(),
        kind: RESTORE_AUTHORIZATION_KIND.to_string(),
        authorization_mode: policy.mode,
        subject: AuthorizedSubject {
            api_version: SUBJECT_API_VERSION.to_string(),
            kind: SUBJECT_KIND_RESTORE.to_string(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        },
        plan_hash: plan_hash.to_string(),
        requester,
        policy: PolicyRef {
            name: policy.name.clone(),
            digest: policy.digest(),
        },
        issued_at,
        expires_at: issued_at + chrono::Duration::seconds(policy.max_age_seconds),
        ticket,
    }
}

/// Merge an approver's countersignature into the console's confirmation.
///
/// Every console signature is kept, byte for byte, and every NEW key id the
/// approver's sidecar carries is appended; a signature by a key the
/// confirmation already carries is dropped, so a sidecar can never present two
/// different signatures under one key id. The payload types must agree.
///
/// # Errors
///
/// A reason when the approver's sidecar is under another payload type or adds
/// no signature at all.
pub fn merge_countersignature(
    confirmation: &Sidecar,
    submitted: &Sidecar,
) -> Result<Sidecar, String> {
    if submitted.payload_type != PAYLOAD_TYPE_RESTORE_AUTHORIZATION
        || confirmation.payload_type != PAYLOAD_TYPE_RESTORE_AUTHORIZATION
    {
        return Err(format!(
            "the submitted sidecar is for {:?}; a governed approval countersigns an \
             authorization document v2 ({PAYLOAD_TYPE_RESTORE_AUTHORIZATION})",
            submitted.payload_type
        ));
    }
    let mut merged = confirmation.clone();
    for signature in &submitted.signatures {
        if !merged.signatures.iter().any(|s| s.keyid == signature.keyid) {
            merged.signatures.push(signature.clone());
        }
    }
    if merged.signatures.len() == confirmation.signatures.len() {
        return Err(
            "the submitted sidecar adds no signature by a key other than the console's; a \
             governed approval is a SECOND, independent signature over the same bytes"
                .to_string(),
        );
    }
    Ok(merged)
}

/// Whether `mode` needs an approver after the console's confirmation.
#[must_use]
pub const fn awaits_approver(mode: ApprovalMode) -> bool {
    matches!(mode, ApprovalMode::Governed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policies() -> ApprovalPolicySet {
        ApprovalPolicySet::parse(
            "allowOrdinaryConfirmation: true\npolicies:\n  - name: o\n    mode: Ordinary\nnamespaces:\n  team-a: o\n",
        )
        .expect("valid")
    }

    #[test]
    fn a_bound_namespace_without_a_console_key_is_a_startup_refusal() {
        let settings = ApprovalSettings {
            policies: policies(),
            confirmation: None,
        };
        let err = settings
            .check(&["team-a".to_string()])
            .expect_err("refused");
        assert!(
            err.contains("team-a") && err.contains("confirmationKeyFile"),
            "{err}"
        );
        assert!(
            settings.check(&["team-b".to_string()]).is_ok(),
            "unbound is fine"
        );
    }

    #[test]
    fn the_debug_rendering_carries_no_key_material() {
        let key = ConfirmationKey::from_key(SigningKey::generate_ed25519()).expect("key");
        let text = format!("{key:?}");
        assert!(text.contains(key.key_id()));
        assert!(!text.contains("PRIVATE"), "{text}");
        assert!(key.public_pem().contains("PUBLIC KEY"));
    }

    #[test]
    fn a_document_expires_at_the_policy_maximum() {
        let set = policies();
        let binding = set.resolve("team-a");
        let policy = binding.bound().expect("bound");
        let at = Utc::now();
        let doc = document(
            policy,
            "team-a",
            "rst-1",
            "uid",
            "sha256:x",
            Requester {
                issuer: "i".into(),
                subject: "s".into(),
            },
            at,
            None,
        );
        assert_eq!(
            doc.expires_at - doc.issued_at,
            chrono::Duration::seconds(policy.max_age_seconds)
        );
        assert_eq!(doc.policy.digest, policy.digest());
        assert_eq!(doc.authorization_mode, ApprovalMode::Ordinary);
    }

    #[test]
    fn a_countersignature_is_appended_and_a_duplicate_key_is_refused() {
        let console = ConfirmationKey::from_key(SigningKey::generate_ed25519()).expect("key");
        let approver = SigningKey::generate_ed25519();
        let doc = b"{}";
        let confirmation = console.sign(doc).expect("sign");
        let mine = sign_detached(&approver, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, doc).expect("sign");
        let merged = merge_countersignature(&confirmation, &mine).expect("merged");
        assert_eq!(merged.signatures.len(), 2);
        assert_eq!(merged.signatures[0].sig, confirmation.signatures[0].sig);
        assert!(merge_countersignature(&confirmation, &confirmation).is_err());
        let other_type = sign_detached(&approver, "application/other", doc).expect("sign");
        assert!(merge_countersignature(&confirmation, &other_type).is_err());
    }
}
