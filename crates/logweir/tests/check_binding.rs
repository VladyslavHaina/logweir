//! PROD-01.3 security follow-up: the CHECK runner (topic discovery, preflight,
//! connection test) refuses a projected credential whose binding does not name
//! the connection its Job was built for — before the password is read and
//! before any client exists, as `AuthenticationFailed` with the named reason.
//!
//! ONE TEST IN THIS FILE, ON PURPOSE: it sets process environment variables,
//! and a second test in the same binary could run concurrently and see them.

use logweir_core::check_contract::{CheckCode, ConnectionPlan};

fn plan() -> ConnectionPlan {
    ConnectionPlan {
        bootstrap_servers: vec!["127.0.0.1:1".into()],
        auth_mode: "plain".into(),
        username: Some("logweir".into()),
        password_env: Some("LOGWEIR_SOURCE_PASSWORD".into()),
        tls: Some(true),
        ca_file: None,
        client_cert_file: None,
        client_key_file: None,
        principal: "User:logweir".into(),
    }
}

#[test]
fn the_check_runner_refuses_an_unbound_credential_and_accepts_a_bound_one() {
    // SAFETY of the env writes: the only test in this binary.
    std::env::set_var("LOGWEIR_SOURCE_PASSWORD", "check-row-password-0b1d");
    std::env::set_var(
        "LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED",
        "v1:uid-a:sha256:00",
    );

    // Absent: refused, named, and the password is not in the message.
    std::env::remove_var("LOGWEIR_SOURCE_CREDENTIAL_BINDING");
    let failure = logweir::check::kafka::auth_config(&plan()).expect_err("unbound");
    assert_eq!(failure.code, CheckCode::AuthenticationFailed);
    assert!(
        failure.message.starts_with("CredentialBindingMismatch: "),
        "{}",
        failure.message
    );
    assert!(!failure.message.contains("check-row-password-0b1d"));

    // Foreign: refused.
    std::env::set_var("LOGWEIR_SOURCE_CREDENTIAL_BINDING", "v1:uid-b:sha256:00");
    let failure = logweir::check::kafka::auth_config(&plan()).expect_err("foreign");
    assert!(failure.message.starts_with("CredentialBindingMismatch: "));

    // CONTROL: bound — the auth builds (PLAIN over TLS).
    std::env::set_var("LOGWEIR_SOURCE_CREDENTIAL_BINDING", "v1:uid-a:sha256:00");
    let auth = logweir::check::kafka::auth_config(&plan()).expect("a bound credential builds");
    assert!(format!("{auth:?}").contains("Plain"));
    assert!(!format!("{auth:?}").contains("check-row-password-0b1d"));

    // And PLAIN without TLS is refused by name whatever the binding.
    let mut clear = plan();
    clear.tls = Some(false);
    let failure = logweir::check::kafka::auth_config(&clear).expect_err("PLAIN in the clear");
    assert!(
        failure.message.contains("PlainWithoutTls"),
        "{}",
        failure.message
    );

    for var in [
        "LOGWEIR_SOURCE_PASSWORD",
        "LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED",
        "LOGWEIR_SOURCE_CREDENTIAL_BINDING",
    ] {
        std::env::remove_var(var);
    }

    // THE TARGET SIDE (fix round, review F2): a Restore's Preflight resolves
    // its target `KafkaCluster` as `ConnectionUse::PreflightTarget`, so its
    // check Job carries the TARGET binding pair. A check runner that compared
    // only the source side would let a Preflight on a Restore whose target
    // names another connection's Secret dial the author's host with it.
    let target = ConnectionPlan {
        password_env: Some("LOGWEIR_TARGET_PASSWORD".into()),
        ..plan()
    };
    std::env::set_var("LOGWEIR_TARGET_PASSWORD", "check-row-password-0b1d");
    std::env::set_var(
        "LOGWEIR_TARGET_CREDENTIAL_BINDING_EXPECTED",
        "v1:uid-t:sha256:00",
    );
    std::env::remove_var("LOGWEIR_TARGET_CREDENTIAL_BINDING");
    let failure = logweir::check::kafka::auth_config(&target).expect_err("unbound target");
    assert_eq!(failure.code, CheckCode::AuthenticationFailed);
    assert!(
        failure.message.starts_with("CredentialBindingMismatch: "),
        "{}",
        failure.message
    );
    assert!(
        failure
            .message
            .contains("LOGWEIR_TARGET_CREDENTIAL_BINDING"),
        "the refusal names the target side's variable: {}",
        failure.message
    );
    assert!(!failure.message.contains("check-row-password-0b1d"));
    std::env::set_var("LOGWEIR_TARGET_CREDENTIAL_BINDING", "v1:uid-s:sha256:00");
    let failure = logweir::check::kafka::auth_config(&target).expect_err("foreign target");
    assert!(failure.message.starts_with("CredentialBindingMismatch: "));
    // CONTROL: the target bound to its own connection builds.
    std::env::set_var("LOGWEIR_TARGET_CREDENTIAL_BINDING", "v1:uid-t:sha256:00");
    let auth = logweir::check::kafka::auth_config(&target).expect("a bound target builds");
    assert!(format!("{auth:?}").contains("Plain"));

    for var in [
        "LOGWEIR_TARGET_PASSWORD",
        "LOGWEIR_TARGET_CREDENTIAL_BINDING_EXPECTED",
        "LOGWEIR_TARGET_CREDENTIAL_BINDING",
    ] {
        std::env::remove_var(var);
    }
}
