//! The broker half of a check: how a [`ConnectionPlan`] becomes a probe.
//!
//! # This is a sanctioned construction site, and it is the fifth
//!
//! Interface **I1** says a Kafka client's auth is built in exactly one
//! function, [`AuthConfig::from_spec`], and
//! `crates/logweir/tests/no_network_in_unit_tests.rs` derives the list of
//! files allowed to name it from the tree. [`dial`] below is the only function
//! here that names a dialling constructor; everything above it is pure or
//! takes a `&dyn InventoryProbe`, which is what keeps every rule in
//! [`crate::check`] inside the default `cargo test` suite with no socket.
//!
//! # The plan carries a credential REFERENCE, never a credential
//!
//! [`ConnectionPlan::password_env`] is the NAME of an environment variable the
//! kubelet projected. The password is read here, at the one construction site,
//! and handed to `from_spec` as a value — so nothing below this line knows
//! which variable it came from, and nothing above it holds one. An
//! environment entry that is present and BLANK is treated as unset: a
//! `secretKeyRef` to an empty key is not a credential, and reporting it as one
//! would send an operator to the broker's ACLs instead of to the Secret.
//!
//! # A mode this build cannot dial is an authentication answer, not a crash
//!
//! `logweir_core::spec::AuthSpec` has two arms — `plaintext` and
//! `scramSha512`. D2 §4.1 lets a plan spell `scramSha256`, which this build
//! does not implement. That is reported as
//! [`CheckCode::AuthenticationFailed`] naming the mode, because nothing was
//! unreachable and the operator's fix is on the connection, not on the
//! network.

use std::time::Duration;

use logweir_core::check_contract::{CheckCode, ConnectionPlan};
use logweir_core::spec::AuthSpec;
use logweir_kafka::inventory::{CheckFailure, KafkaInventory, ProbeTimeouts};
use logweir_kafka::reader::AuthConfig;

/// The spelling `AuthSpec::Plaintext` serialises as.
pub const AUTH_MODE_PLAINTEXT: &str = "plaintext";
/// The spelling `AuthSpec::ScramSha512` serialises as.
pub const AUTH_MODE_SCRAM_SHA_512: &str = "scramSha512";

/// The plan's `authMode` as an [`AuthSpec`], or the reason this build cannot
/// dial it.
///
/// PURE and separate from [`dial`], so every mode — including the ones that
/// refuse — is covered with no socket.
///
/// # Errors
/// [`CheckFailure`] with [`CheckCode::AuthenticationFailed`].
pub fn auth_spec(plan: &ConnectionPlan) -> Result<AuthSpec, CheckFailure> {
    match plan.auth_mode.as_str() {
        AUTH_MODE_PLAINTEXT => Ok(AuthSpec::Plaintext),
        AUTH_MODE_SCRAM_SHA_512 => match plan.username.as_deref().filter(|u| !u.is_empty()) {
            Some(username) => Ok(AuthSpec::ScramSha512 {
                username: username.to_string(),
                tls: plan.tls.unwrap_or(false),
            }),
            None => Err(CheckFailure::new(
                CheckCode::AuthenticationFailed,
                "the connection asks for scramSha512 and names no username; SASL/SCRAM \
                 authenticates as a named principal, and a check that dialled as nobody would \
                 report a configuration mistake as an unreachable broker",
            )),
        },
        other => Err(CheckFailure::new(
            CheckCode::AuthenticationFailed,
            format!(
                "`{other}` is not an auth mode this build can dial; it accepts \
                 {AUTH_MODE_PLAINTEXT} and {AUTH_MODE_SCRAM_SHA_512}"
            ),
        )),
    }
}

/// The projected password for this connection, or `None`.
///
/// Reads exactly the variable the plan NAMES and no other. A blank value is
/// `None` (see the module header).
#[must_use]
pub fn projected_password(plan: &ConnectionPlan) -> Option<String> {
    let var = plan.password_env.as_deref()?;
    std::env::var(var).ok().filter(|p| !p.is_empty())
}

/// Build this connection's [`AuthConfig`] — interface **I1**, plus PLAT-07.1's
/// projected trust anchor.
///
/// `with_tls_ca_file` REFUSES a CA on a connection that is not TLS, which is
/// D-SEAMS **S5** from the broker side: a trust anchor must never be the thing
/// that decides a transport.
///
/// # Errors
/// [`CheckFailure`] with [`CheckCode::AuthenticationFailed`].
pub fn auth_config(plan: &ConnectionPlan) -> Result<AuthConfig, CheckFailure> {
    let spec = auth_spec(plan)?;
    let password = projected_password(plan);
    if matches!(spec, AuthSpec::ScramSha512 { .. }) && password.is_none() {
        return Err(CheckFailure::new(
            CheckCode::AuthenticationFailed,
            match plan.password_env.as_deref() {
                Some(var) => format!(
                    "the connection asks for scramSha512 and the projected environment variable \
                     `{var}` is absent or empty, so no SASL password reached this check"
                ),
                None => "the connection asks for scramSha512 and names no password environment \
                         variable, so no SASL password could reach this check"
                    .to_string(),
            },
        ));
    }
    AuthConfig::from_spec(&spec, password)
        .and_then(|a| a.with_tls_ca_file(plan.ca_file.clone()))
        // `KafkaError`'s message is this workspace's own prose — never a
        // broker string — and `CheckFailure::new` redacts and caps it anyway.
        .map_err(|e| CheckFailure::new(CheckCode::AuthenticationFailed, e.to_string()))
}

/// The broker addresses, non-empty.
///
/// # Errors
/// [`CheckFailure`] with [`CheckCode::BrokerUnreachable`] when the plan names
/// none: there is no address to dial, and that is not an authentication
/// problem.
pub fn bootstrap(plan: &ConnectionPlan) -> Result<Vec<String>, CheckFailure> {
    let servers: Vec<String> = plan
        .bootstrap_servers
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if servers.is_empty() {
        return Err(CheckFailure::new(
            CheckCode::BrokerUnreachable,
            "the connection names no bootstrap address",
        ));
    }
    Ok(servers)
}

/// **THE ONLY FUNCTION IN THIS CRATE THAT BUILDS A BROKER CLIENT FOR A CHECK.**
///
/// It opens no socket by itself — librdkafka dials lazily — which is why every
/// timeout belongs to a call and comes from [`ProbeTimeouts::for_budget`].
///
/// # Errors
/// [`CheckFailure`] in the closed Kafka vocabulary.
pub fn dial(plan: &ConnectionPlan, budget: Duration) -> Result<KafkaInventory, CheckFailure> {
    let servers = bootstrap(plan)?;
    let auth = auth_config(plan)?;
    KafkaInventory::connect(&logweir_kafka::inventory::ConnectionSettings {
        bootstrap_servers: servers,
        auth,
        timeouts: ProbeTimeouts::for_budget(budget),
    })
}
