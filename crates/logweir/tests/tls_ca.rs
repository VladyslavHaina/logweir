//! The runner half of the saved-connection contract's TLS trust — PLAT-07.1,
//! Global Constraint 29.
//!
//! A private CA has to reach BOTH of a runner's TLS clients: librdkafka's
//! `ssl.ca.location` and the engine's `ssl_ca_location`. The controller
//! projects one file and names it in one variable per side; these rows assert
//! that the variable is read once, that both attachments come from that one
//! value, and that a CA can never sit beside a connection dialled in the
//! clear.
//!
//! NOTHING HERE DIALS. `AuthConfig` and `AuthRender` are values; the engine
//! renderer is a function from a plan to bytes.

use logweir::tls_ca::{projected_ca_file, SOURCE_TLS_CA_FILE_VAR, TARGET_TLS_CA_FILE_VAR};
use logweir_core::engine::AuthRender;
use logweir_core::spec::AuthSpec;
use logweir_kafka::reader::{AuthConfig, KafkaError};

const CA: &str = "/connection/source-ca/ca.crt";

/// The two variables are the contract's own names, and they differ per side.
#[test]
fn the_two_variables_are_the_shared_contract_names() {
    assert_eq!(
        SOURCE_TLS_CA_FILE_VAR,
        logweir_core::connection::SOURCE_TLS_CA_FILE_ENV
    );
    assert_eq!(
        TARGET_TLS_CA_FILE_VAR,
        logweir_core::connection::TARGET_TLS_CA_FILE_ENV
    );
    assert_ne!(SOURCE_TLS_CA_FILE_VAR, TARGET_TLS_CA_FILE_VAR);
}

/// Unset is `None`, **and so is blank** — plan erratum E19(e): a Kubernetes
/// `env:` entry with an empty `value:` reads as `Ok("")`, and a CA location of
/// `""` is not a file anybody meant.
///
/// Each row uses a variable name of its own, so the rows cannot race each
/// other inside one test binary.
#[test]
fn a_blank_ca_variable_is_unset() {
    assert_eq!(
        projected_ca_file("LOGWEIR_TEST_CA_ABSENT_9f1"),
        Ok(None),
        "an unset variable names no CA"
    );
    for (name, value, expected) in [
        ("LOGWEIR_TEST_CA_EMPTY_9f2", "", None),
        ("LOGWEIR_TEST_CA_BLANK_9f3", "   ", None),
        ("LOGWEIR_TEST_CA_SET_9f4", CA, Some(CA.to_string())),
    ] {
        // SAFETY-BY-CONVENTION: each row owns a name no other test reads.
        std::env::set_var(name, value);
        assert_eq!(
            projected_ca_file(name),
            Ok(expected),
            "for {name}={value:?}"
        );
        std::env::remove_var(name);
    }
}

/// librdkafka's half: the path is attached to a SCRAM-over-TLS client and
/// refused for anything else, so a trust anchor never sits beside a clear dial.
#[test]
fn the_client_takes_a_ca_only_over_tls() {
    let scram_tls = AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    };
    let auth = AuthConfig::from_spec(&scram_tls, Some("pw".into()))
        .expect("a password was projected")
        .with_tls_ca_file(Some(CA.to_string()))
        .expect("a CA over TLS is attached");
    match auth {
        AuthConfig::ScramSha512 { tls_ca_file, .. } => {
            assert_eq!(tls_ca_file.as_deref(), Some(CA))
        }
        other => panic!("{other:?}"),
    }
    // …and the Debug still redacts the password while showing the path, which
    // is not a credential.
    let auth = AuthConfig::from_spec(&scram_tls, Some("hunter2".into()))
        .expect("builds")
        .with_tls_ca_file(Some(CA.to_string()))
        .expect("attaches");
    let debugged = format!("{auth:?}");
    assert!(
        debugged.contains(CA) && debugged.contains("***") && !debugged.contains("hunter2"),
        "{debugged}"
    );

    for spec in [
        AuthSpec::Plaintext,
        AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        },
    ] {
        let err = AuthConfig::from_spec(&spec, Some("pw".into()))
            .expect("builds")
            .with_tls_ca_file(Some(CA.to_string()))
            .expect_err("a CA with no TLS transport is refused");
        assert!(matches!(err, KafkaError::Client(_)), "{err:?}");
        let text = err.to_string();
        assert!(
            text.contains("silent downgrade") && text.contains(spec.mode_str()),
            "the refusal says why, and names the mode: {text}"
        );
    }

    // `None` changes nothing at all.
    let plain = AuthConfig::from_spec(&AuthSpec::Plaintext, None)
        .expect("builds")
        .with_tls_ca_file(None)
        .expect("no CA, no change");
    assert!(matches!(plain, AuthConfig::Plaintext));
}

/// The engine's half: the same path becomes `ssl_ca_location`, under
/// `security:`, and only for a TLS connection.
#[test]
fn the_engine_document_carries_the_same_path_as_ssl_ca_location() {
    let with_ca = AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    }
    .to_render()
    .with_tls_ca_file(Some(CA.to_string()))
    .expect("a CA over TLS is attached");
    assert_eq!(
        with_ca,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: true,
            tls_ca_file: Some(CA.to_string()),
        }
    );
    for spec in [
        AuthSpec::Plaintext,
        AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        },
    ] {
        spec.to_render()
            .with_tls_ca_file(Some(CA.to_string()))
            .expect_err("a CA with no TLS transport is refused on the engine side too");
    }
}

/// A backup plan carries the CA only when one was projected, and refuses it on
/// a source that is not SCRAM over TLS.
#[test]
fn a_backup_plan_takes_the_projected_ca_or_refuses_it() {
    let spec = |auth: AuthSpec| logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: vec!["b0.orders:9093".into()],
            auth,
            topics: vec!["orders".into()],
        },
        storage: logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "logweir".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        backup_id: "b1".into(),
        backup: logweir_core::spec::BackupSettings::default(),
    };
    let tls = spec(AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    });
    let plan = logweir::backup::build_plan_with_tls_ca(&tls, "b1", Some(CA.to_string()))
        .expect("a CA over TLS is attached");
    assert_eq!(
        plan.source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: true,
            tls_ca_file: Some(CA.to_string()),
        }
    );
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("the document renders");
    assert!(
        doc.contains(&format!("    ssl_ca_location: \"{CA}\"\n")),
        "the engine's own key, under `security:`, indented with it and quoted like every other \
         interpolated scalar: {doc}"
    );

    // …and with no CA the document is exactly what it was before this field
    // existed: no `ssl_ca_location` line at all.
    let plan = logweir::backup::build_plan_with_tls_ca(&tls, "b1", None).expect("no CA");
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("the document renders");
    assert!(!doc.contains("ssl_ca_location"), "{doc}");

    // A CA on a plaintext source is an operational refusal, before the engine.
    let plaintext = spec(AuthSpec::Plaintext);
    let err = logweir::backup::build_plan_with_tls_ca(&plaintext, "b1", Some(CA.to_string()))
        .expect_err("refused");
    assert_eq!(
        logweir::exit::ExitCode::from(err),
        logweir::exit::ExitCode::Operational,
        "nothing was refused about the PLAN; the controller never projects this shape, so it is \
         a hand-built Job and the fix is configuration"
    );
}
