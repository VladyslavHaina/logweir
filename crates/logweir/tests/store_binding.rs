//! FX-20: the CHECK runner (destination access, readiness, evidence fetch,
//! catalog sync) refuses to build ANY object store while the pod carries an
//! object-store credential whose Secret's `logweir-binding` does not equal the
//! controller's expectation — `CredentialBindingMismatch`, before a request is
//! signed. Every store a check opens goes through `options_for` or
//! `grant_options`, so those two are the gate this file drives.
//!
//! ONE TEST IN THIS FILE, ON PURPOSE: `options_for` reads the process
//! environment, and a second test in the same binary could run concurrently
//! and see the variables this one sets.

use std::time::Duration;

use logweir::check::store::{grant_options, options_for, refuse_unbound_credentials};
use logweir_core::check_contract::{CheckCode, CredentialMode, DestinationPlan, GrantRef};
use logweir_core::credential_binding as cb;
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};

const MINE: &str = "v1:d0000000-0000-4000-8000-00000000000a:sha256:aa";
const THEIRS: &str = "v1:e0000000-0000-4000-8000-00000000000e:sha256:bb";

fn plan() -> DestinationPlan {
    DestinationPlan {
        name: "dest-a".into(),
        uid: "d0000000-0000-4000-8000-00000000000a".into(),
        location: DestinationLocation {
            provider: StorageProvider::S3,
            bucket: "lw-a".into(),
            prefix: "team-a".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("https://127.0.0.1:1".into()),
            addressing: Addressing::PathStyle,
            transport: TransportSecurity::Tls,
        },
        location_digest: "sha256:00".into(),
        ca_file: None,
        credentials: CredentialMode::Static,
    }
}

fn lookup<'a>(pairs: &'a [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |k| {
        pairs
            .iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| (*v).to_string())
    }
}

#[test]
fn a_check_builds_no_store_while_any_credential_is_unbound_or_foreign() {
    let budget = Duration::from_secs(5);

    // ---- every pair, through the lookup seam ----------------------------
    for (projected, expected) in cb::STORE_BINDING_PAIRS {
        for (label, value, refused) in [
            ("absent", None, true),
            ("foreign", Some(THEIRS), true),
            ("bound", Some(MINE), false),
        ] {
            let mut pairs: Vec<(&'static str, &'static str)> = vec![(expected, MINE)];
            if let Some(v) = value {
                pairs.push((projected, v));
            }
            let outcome = refuse_unbound_credentials(&lookup(&pairs));
            if refused {
                let failure = outcome.expect_err(label);
                assert_eq!(
                    failure.code,
                    CheckCode::CredentialBindingMismatch,
                    "{projected}"
                );
                assert!(
                    failure.message.starts_with("CredentialBindingMismatch: ")
                        && failure.message.contains(projected),
                    "{projected} {label}: {}",
                    failure.message
                );
                assert!(!failure.message.contains(THEIRS) && !failure.message.contains(MINE));
            } else {
                assert!(outcome.is_ok(), "{projected} {label}");
            }
        }
    }
    // No expectation anywhere: a hand-run check, nothing to compare.
    assert!(refuse_unbound_credentials(&lookup(&[])).is_ok());

    // ---- grant_options: the evidence-write grant's own pair --------------
    let grant = GrantRef::static_secret("lw-evidence");
    let env = [
        ("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID", "AKIAEVIDENCE"),
        (
            "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY",
            "evidence-secret-value",
        ),
        (cb::EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV, MINE),
        (cb::EVIDENCE_CREDENTIAL_BINDING_ENV, THEIRS),
    ];
    let failure = grant_options(
        &plan(),
        DestinationRole::EvidenceWrite,
        Some(&grant),
        budget,
        &lookup(&env),
    )
    .expect_err("a foreign evidence-write Secret");
    assert_eq!(failure.code, CheckCode::CredentialBindingMismatch);
    assert!(!failure.message.contains("evidence-secret-value"));
    let mut bound = env;
    bound[3] = (cb::EVIDENCE_CREDENTIAL_BINDING_ENV, MINE);
    assert!(
        grant_options(
            &plan(),
            DestinationRole::EvidenceWrite,
            Some(&grant),
            budget,
            &lookup(&bound),
        )
        .is_ok(),
        "CONTROL: the bound evidence-write Secret builds its options"
    );

    // ---- options_for: the destination grant, from the process env -------
    // SAFETY of the env writes: the only test in this binary.
    std::env::set_var("AWS_ACCESS_KEY_ID", "AKIADESTINATION");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "destination-secret-value");
    std::env::set_var(cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV, MINE);
    std::env::set_var(cb::ARCHIVE_CREDENTIAL_BINDING_ENV, THEIRS);
    let failure = options_for(&plan(), budget).expect_err("a foreign destination Secret");
    assert_eq!(failure.code, CheckCode::CredentialBindingMismatch);
    assert!(failure.message.contains(cb::ARCHIVE_CREDENTIAL_BINDING_ENV));
    std::env::remove_var(cb::ARCHIVE_CREDENTIAL_BINDING_ENV);
    let failure = options_for(&plan(), budget).expect_err("an unbound destination Secret");
    assert_eq!(failure.code, CheckCode::CredentialBindingMismatch);
    std::env::set_var(cb::ARCHIVE_CREDENTIAL_BINDING_ENV, MINE);
    assert!(
        options_for(&plan(), budget).is_ok(),
        "CONTROL: the bound destination Secret builds its options"
    );
    for var in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
        cb::ARCHIVE_CREDENTIAL_BINDING_ENV,
    ] {
        std::env::remove_var(var);
    }
}
