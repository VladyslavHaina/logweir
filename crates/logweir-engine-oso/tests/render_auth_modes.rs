//! PROD-01.3: the engine's `security:` block for `scramSha256`, `plain` and
//! `mtls`, in all three rendered documents, and the renderer's refusals — the
//! backstops behind the runner's guards.
//!
//! The spellings are the engine's own: `SaslMechanism` is
//! `SCREAMING-KEBAB-CASE`, so `SCRAM-SHA256` (one hyphen) and `PLAIN`
//! [C23/config.rs `SaslMechanism`]; `SecurityProtocol` is
//! `SCREAMING_SNAKE_CASE` (`SSL`, `SASL_SSL`, `SASL_PLAINTEXT`); the client
//! pair is `ssl_certificate_location` / `ssl_key_location`
//! [C23/kafka/tls.rs `build_tls_config`]. Each document's block is asserted
//! as exact LINES, so a key rendered one level too high (an unknown key the
//! engine would drop) or a mechanism spelled librdkafka's way fails here.
use logweir_core::connection::ClientCertificateFiles;
use logweir_core::engine::{
    AuthRender, BackupPlan, BackupSetRef, RestorePlan, StorageUrl, WindowFloorSource,
};
use logweir_engine_oso::render_backup::RenderError;
use logweir_engine_oso::{render_backup, render_restore, render_validation};

const USER: &str = "app-key";
const CA: &str = "/connection/target-ca/ca.crt";
const CERT: &str = "/connection/target-client-cert/tls.crt";
const KEY: &str = "/connection/target-client-cert/tls.key";

fn storage() -> StorageUrl {
    StorageUrl::S3 {
        bucket: "kafka-backups".into(),
        prefix: "auth-modes".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://minio:9000".into()),
        path_style: true,
        allow_http: true,
    }
}

fn restore_plan(auth: AuthRender) -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "b".into(),
            manifest_key: "drills/b/manifest.json".into(),
        },
        storage: storage(),
        target_bootstrap: vec!["broker:9093".into()],
        target_auth: auth,
        topic_mapping: [("orders".to_string(), "drill-orders".to_string())]
            .into_iter()
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        source_partitions: Default::default(),
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/x/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/x/offsets.json".into(),
    }
}

fn backup_plan(auth: AuthRender) -> BackupPlan {
    BackupPlan {
        backup_id: "auth-modes".into(),
        source_bootstrap: vec!["broker:9093".into()],
        source_auth: auth,
        topics: vec!["orders".into()],
        storage: storage(),
        compression: "zstd".into(),
        segment_max_records: 1000,
        segment_max_bytes: 10_485_760,
        max_concurrent_partitions: 3,
    }
}

/// Every document a plan renders, `(name, text)`.
fn all_three(auth: AuthRender) -> Vec<(&'static str, String)> {
    vec![
        (
            "backup",
            render_backup::render(&backup_plan(auth.clone())).expect("renders"),
        ),
        (
            "restore",
            render_restore::render(&restore_plan(auth.clone())).expect("renders"),
        ),
        (
            "validation",
            render_validation::render(&restore_plan(auth), "01J9X", None).expect("renders"),
        ),
    ]
}

fn security_block(doc: &str) -> Vec<String> {
    let lines: Vec<&str> = doc.lines().collect();
    let at = lines
        .iter()
        .position(|l| *l == "  security:")
        .unwrap_or_else(|| panic!("no `  security:` block:\n{doc}"));
    lines[at..]
        .iter()
        .take_while(|l| **l == "  security:" || l.starts_with("    "))
        .map(|l| l.to_string())
        .collect()
}

fn password_placeholder(name: &str) -> &'static str {
    if name == "backup" {
        logweir_engine_oso::yaml::PLACEHOLDER_SOURCE_PASSWORD
    } else {
        logweir_engine_oso::yaml::PLACEHOLDER_TARGET_PASSWORD
    }
}

#[test]
fn scram_sha_256_renders_the_engines_one_hyphen_spelling_over_either_transport() {
    for (tls, protocol) in [(false, "SASL_PLAINTEXT"), (true, "SASL_SSL")] {
        for (name, doc) in all_three(AuthRender::ScramSha256 {
            username: USER.into(),
            tls,
            tls_ca_file: tls.then(|| CA.to_string()),
        }) {
            let mut want = vec![
                "  security:".to_string(),
                format!("    security_protocol: \"{protocol}\""),
                "    sasl_mechanism: \"SCRAM-SHA256\"".to_string(),
                format!("    sasl_username: \"{USER}\""),
                format!("    sasl_password: {}", password_placeholder(name)),
            ];
            if tls {
                want.push(format!("    ssl_ca_location: \"{CA}\""));
            }
            assert_eq!(security_block(&doc), want, "{name}, tls={tls}");
            assert!(
                !doc.contains("SCRAM-SHA-256"),
                "librdkafka's spelling: {doc}"
            );
        }
    }
    assert_eq!(
        logweir_engine_oso::yaml::ENGINE_SCRAM_SHA_256,
        "SCRAM-SHA256"
    );
}

#[test]
fn plain_renders_sasl_ssl_and_only_over_tls() {
    for (name, doc) in all_three(AuthRender::Plain {
        username: USER.into(),
        tls: true,
        tls_ca_file: None,
    }) {
        assert_eq!(
            security_block(&doc),
            vec![
                "  security:".to_string(),
                "    security_protocol: \"SASL_SSL\"".to_string(),
                "    sasl_mechanism: \"PLAIN\"".to_string(),
                format!("    sasl_username: \"{USER}\""),
                format!("    sasl_password: {}", password_placeholder(name)),
            ],
            "{name}"
        );
    }
    // PLAIN WITHOUT TLS IS REFUSED, in every document — never rendered as
    // SASL_PLAINTEXT. KILLS: drop the `!tls` arm in `render_security_block`.
    let clear = AuthRender::Plain {
        username: USER.into(),
        tls: false,
        tls_ca_file: None,
    };
    assert!(matches!(
        render_backup::render(&backup_plan(clear.clone())),
        Err(RenderError::PlainWithoutTls)
    ));
    assert!(matches!(
        render_restore::render(&restore_plan(clear.clone())),
        Err(RenderError::PlainWithoutTls)
    ));
    assert!(matches!(
        render_validation::render(&restore_plan(clear), "01J9X", None),
        Err(RenderError::PlainWithoutTls)
    ));
    assert!(RenderError::PlainWithoutTls
        .to_string()
        .starts_with("PlainWithoutTls: "));
}

#[test]
fn mtls_renders_ssl_and_the_two_file_paths_and_no_sasl_key() {
    let files = ClientCertificateFiles {
        cert_file: CERT.into(),
        key_file: KEY.into(),
    };
    for (name, doc) in all_three(AuthRender::Mtls {
        tls: true,
        tls_ca_file: Some(CA.into()),
        client_certificate: Some(files.clone()),
    }) {
        assert_eq!(
            security_block(&doc),
            vec![
                "  security:".to_string(),
                "    security_protocol: \"SSL\"".to_string(),
                format!("    ssl_ca_location: \"{CA}\""),
                format!("    ssl_certificate_location: \"{CERT}\""),
                format!("    ssl_key_location: \"{KEY}\""),
            ],
            "{name}"
        );
        assert!(
            !doc.contains("sasl_"),
            "{name}: mTLS has no SASL key:\n{doc}"
        );
        assert!(!doc.contains("${"), "{name}: no placeholder at all:\n{doc}");
    }
    // Without TLS, without a certificate: refused, never rendered.
    assert!(matches!(
        render_backup::render(&backup_plan(AuthRender::Mtls {
            tls: false,
            tls_ca_file: None,
            client_certificate: Some(files),
        })),
        Err(RenderError::MtlsWithoutTls)
    ));
    assert!(matches!(
        render_restore::render(&restore_plan(AuthRender::Mtls {
            tls: true,
            tls_ca_file: None,
            client_certificate: None,
        })),
        Err(RenderError::ClientCertificateMissing)
    ));
}

#[test]
fn a_ca_without_tls_is_refused_for_scram_sha_256_too() {
    assert!(matches!(
        render_backup::render(&backup_plan(AuthRender::ScramSha256 {
            username: USER.into(),
            tls: false,
            tls_ca_file: Some(CA.into()),
        })),
        Err(RenderError::TlsCaWithoutTls)
    ));
}

/// `AuthRender`'s attach methods: a CA on any TLS arm, never on a clear one; a
/// client certificate on `Mtls` only.
#[test]
fn the_attach_methods_refuse_what_the_transport_cannot_carry() {
    let ca = Some(CA.to_string());
    for clear in [
        AuthRender::Plaintext,
        AuthRender::ScramSha256 {
            username: USER.into(),
            tls: false,
            tls_ca_file: None,
        },
        AuthRender::Plain {
            username: USER.into(),
            tls: false,
            tls_ca_file: None,
        },
    ] {
        assert!(clear.with_tls_ca_file(ca.clone()).is_err());
    }
    for tls in [
        AuthRender::ScramSha256 {
            username: USER.into(),
            tls: true,
            tls_ca_file: None,
        },
        AuthRender::Plain {
            username: USER.into(),
            tls: true,
            tls_ca_file: None,
        },
        AuthRender::Mtls {
            tls: true,
            tls_ca_file: None,
            client_certificate: None,
        },
    ] {
        assert!(tls.with_tls_ca_file(ca.clone()).is_ok());
    }
    let files = Some(ClientCertificateFiles {
        cert_file: CERT.into(),
        key_file: KEY.into(),
    });
    assert!(AuthRender::Plain {
        username: USER.into(),
        tls: true,
        tls_ca_file: None,
    }
    .with_client_certificate(files.clone())
    .is_err());
    assert!(AuthRender::Mtls {
        tls: true,
        tls_ca_file: None,
        client_certificate: None,
    }
    .with_client_certificate(None)
    .is_err());
    assert!(AuthRender::Mtls {
        tls: true,
        tls_ca_file: None,
        client_certificate: None,
    }
    .with_client_certificate(files)
    .is_ok());
}
