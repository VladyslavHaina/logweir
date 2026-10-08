//! The runner's half of the credential binding (PROD-01.3 security follow-up).
//!
//! A controller-built Job carries, per side, two variables: the EXPECTED
//! binding — a literal the controller computed from the `KafkaCluster` the Job
//! was built for (`weirkeeper::connection::ResolvedConnection::
//! credential_binding`) — and the PROJECTED binding, which the kubelet reads
//! from the credential Secret's `logweir-binding` key (an OPTIONAL
//! `secretKeyRef`: a Secret without the key projects nothing). This module
//! compares the two, at every runner entry point, BEFORE any client exists and
//! before a projected credential is used, and refuses a mismatch with exit 3,
//! `refusal-reason=CredentialBindingMismatch`.
//!
//! So a connection that names another connection's Secret — a Secret its
//! author could not read — cannot make Logweir present that credential to a
//! broker the author chose: the foreign Secret's binding names the other
//! connection's UID and endpoint, never this one's.
//!
//! A hand-run `logweir` (no controller, no EXPECTED variable) is not checked:
//! its operator supplies their own environment and is the only party whose
//! credential it could present.

use logweir_core::connection::{
    check_credential_binding, CredentialBindingRefusal, SOURCE_CREDENTIAL_BINDING_ENV,
    SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV, TARGET_CREDENTIAL_BINDING_ENV,
    TARGET_CREDENTIAL_BINDING_EXPECTED_ENV,
};

use crate::tls_ca::Side;

/// The two variables of one side: `(projected, expected)`.
#[must_use]
pub const fn variables(side: Side) -> (&'static str, &'static str) {
    match side {
        Side::Source => (
            SOURCE_CREDENTIAL_BINDING_ENV,
            SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV,
        ),
        Side::Target => (
            TARGET_CREDENTIAL_BINDING_ENV,
            TARGET_CREDENTIAL_BINDING_EXPECTED_ENV,
        ),
    }
}

/// Check one side against this process's environment.
///
/// A non-UTF-8 value is read as ABSENT for the projected variable (so it is
/// refused when a binding is expected) and as present-but-unusable for the
/// expected one, which is refused too: a controller never writes one.
///
/// # Errors
///
/// [`CredentialBindingRefusal`].
pub fn check_side(side: Side) -> Result<(), CredentialBindingRefusal> {
    let (projected_var, expected_var) = variables(side);
    let expected = match std::env::var(expected_var) {
        Ok(v) => Some(v),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(CredentialBindingRefusal {
                binding_env: projected_var,
                absent: false,
            })
        }
    };
    let projected = std::env::var(projected_var).ok();
    check_credential_binding(projected_var, expected.as_deref(), projected.as_deref())
}

/// FX-20: check every OBJECT-STORE pair this process carries —
/// [`logweir_core::credential_binding::STORE_BINDING_PAIRS`]: the
/// `AWS_*` credential's (a destination's archive grant, an inline archive's
/// `secretRef`, a retention Job's delete-capable key) and the
/// `LOGWEIR_EVIDENCE_AWS_*` one's. A pair with no expectation is a hand-run
/// process's and is not checked.
///
/// # Errors
///
/// The first pair's [`CredentialBindingRefusal`].
pub fn check_store_bindings() -> Result<(), CredentialBindingRefusal> {
    check_store_bindings_with(&|k| std::env::var(k).ok())
}

/// [`check_store_bindings`] through a lookup, so a caller can test it without
/// the process environment.
///
/// # Errors
///
/// The first pair's [`CredentialBindingRefusal`].
pub fn check_store_bindings_with(
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<(), CredentialBindingRefusal> {
    for (projected, expected) in logweir_core::credential_binding::STORE_BINDING_PAIRS {
        logweir_core::credential_binding::check_pair(projected, expected, get)?;
    }
    Ok(())
}

/// Check BOTH Kafka sides and every object-store pair (FX-20) — what
/// `backup run`, `drill run`/`restore run`, `catalog sync` and a check runner
/// call at start-up, beside `drill::check_projected_credentials`, so no client
/// of any kind is constructed on the refusal path.
///
/// # Errors
///
/// The first pair's [`CredentialBindingRefusal`], as a `GuardRefusal` whose
/// message opens with `CredentialBindingMismatch: ` (so
/// `refusal-reason=CredentialBindingMismatch`).
pub fn check_projected_bindings() -> Result<(), logweir_core::guard::GuardRefusal> {
    for side in [Side::Source, Side::Target] {
        check_side(side).map_err(|e| logweir_core::guard::GuardRefusal(e.to_string()))?;
    }
    check_store_bindings().map_err(|e| logweir_core::guard::GuardRefusal(e.to_string()))?;
    Ok(())
}
