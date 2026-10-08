//! **C15** (`docs/to-do/decisions/PROD-00-engine-route.md` 3.15, acceptance
//! row A-C15-1; PROD-00.3f).
//!
//! From engine 0.22.0 on, and so on the 0.23.3 pin, the engine's YAML path ORs
//! `storage.allow_http` with the endpoint's scheme (`implied_allow_http`,
//! `kafka-backup-core/src/storage/config.rs:116-118` and `storage/mod.rs:58-73`
//! at v0.23.3, unchanged since v0.22.0). A rendered `allow_http: false` beside
//! an `http://` endpoint is therefore not a refusal any more: the engine logs
//! "enabling allow_http" and dials the archive in the clear. Measured with the
//! pinned image (section 12 of the decision record): 0.21.0 stops with
//! "builder error" before any connection; 0.22.0 and 0.23.3 make plaintext
//! connection attempts.
//!
//! So `render_storage_block`, the one function every engine document's storage
//! block goes through, refuses the combination. One row per document, because
//! the refusal is only as complete as the list of documents that reach it.
//! Fixtures name container-side endpoints and construct no client.

use logweir_core::engine::{
    AuthRender, BackupPlan, BackupSetRef, RestorePlan, StorageUrl, WindowFloorSource,
};
use logweir_engine_oso::render_backup::RenderError;
use logweir_engine_oso::{render_backup, render_restore, render_validation};
use std::collections::BTreeMap;

fn s3(endpoint: Option<&str>, allow_http: bool) -> StorageUrl {
    StorageUrl::S3 {
        bucket: "kafka-backups".into(),
        prefix: "drill-demo".into(),
        region: Some("us-east-1".into()),
        endpoint: endpoint.map(str::to_string),
        path_style: true,
        allow_http,
    }
}

fn backup_plan(storage: StorageUrl) -> BackupPlan {
    BackupPlan {
        backup_id: "drill-demo".into(),
        source_bootstrap: vec!["kafka-broker-1:9094".into()],
        source_auth: AuthRender::Plaintext,
        topics: vec!["orders".into()],
        storage,
        compression: "zstd".into(),
        segment_max_records: 1000,
        segment_max_bytes: 10_485_760,
        max_concurrent_partitions: 3,
    }
}

fn restore_plan(storage: StorageUrl) -> RestorePlan {
    let mut topic_mapping = BTreeMap::new();
    topic_mapping.insert("orders".to_string(), "drill-orders".to_string());
    RestorePlan {
        set: BackupSetRef {
            backup_id: "drill-demo".into(),
            manifest_key: "drill-demo/manifest.json".into(),
        },
        storage,
        target_bootstrap: vec!["kafka-broker-1:9094".into()],
        target_auth: AuthRender::Plaintext,
        topic_mapping,
        time_window: (
            chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .into(),
            chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
                .unwrap()
                .into(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        source_partitions: Default::default(),
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/checkpoint".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
    }
}

/// The three documents, each rendered over `storage`.
fn render_all(storage: &StorageUrl) -> [(&'static str, Result<String, RenderError>); 3] {
    [
        (
            "restore",
            render_restore::render(&restore_plan(storage.clone())),
        ),
        (
            "backup",
            render_backup::render(&backup_plan(storage.clone())),
        ),
        (
            "validate-restore",
            render_validation::render(&restore_plan(storage.clone()), "r", None),
        ),
    ]
}

/// A-C15-1, the restore document.
#[test]
fn the_restore_document_refuses_an_http_endpoint_without_allow_http() {
    let storage = s3(Some("http://minio:9000"), false);
    assert_eq!(
        render_restore::render(&restore_plan(storage.clone())).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp
    );
    assert_eq!(
        render_restore::render_and_digest(&restore_plan(storage)).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp,
        "the digesting entry point the drill uses refuses too"
    );
}

/// A-C15-1, the backup document.
#[test]
fn the_backup_document_refuses_an_http_endpoint_without_allow_http() {
    let storage = s3(Some("http://minio:9000"), false);
    assert_eq!(
        render_backup::render(&backup_plan(storage.clone())).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp
    );
    assert_eq!(
        render_backup::render_and_digest(&backup_plan(storage)).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp
    );
}

/// A-C15-1, the validate-restore document.
#[test]
fn the_validation_document_refuses_an_http_endpoint_without_allow_http() {
    let storage = s3(Some("http://minio:9000"), false);
    assert_eq!(
        render_validation::render(&restore_plan(storage.clone()), "r", None).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp
    );
    assert_eq!(
        render_validation::render_and_digest(&restore_plan(storage), "r", None).unwrap_err(),
        RenderError::PlaintextEndpointWithoutAllowHttp
    );
}

/// Spellings of the same scheme are the same combination. The engine's own
/// test is case-sensitive, so the refusal is the stricter of the two.
#[test]
fn every_spelling_of_an_http_endpoint_is_refused() {
    for endpoint in [
        "http://minio:9000",
        "HTTP://minio:9000",
        "Http://minio:9000",
        " http://minio:9000",
        "http://10.0.0.7",
    ] {
        for (doc, result) in render_all(&s3(Some(endpoint), false)) {
            assert_eq!(
                result.unwrap_err(),
                RenderError::PlaintextEndpointWithoutAllowHttp,
                "{doc} over `{endpoint}`"
            );
        }
    }
}

/// The negative side: what the guard must NOT refuse. Plaintext stated
/// explicitly, TLS, and no endpoint at all (AWS) all render, so a guard that
/// refused every S3 document would fail here.
#[test]
fn explicit_plaintext_tls_and_no_endpoint_still_render() {
    for storage in [
        s3(Some("http://minio:9000"), true),
        s3(Some("https://minio:9000"), false),
        s3(Some("https://s3.eu-west-1.amazonaws.com"), false),
        s3(None, false),
        StorageUrl::Gcs {
            bucket: "b".into(),
            prefix: "p".into(),
        },
        StorageUrl::Filesystem {
            path: "/tmp/archive".into(),
        },
    ] {
        for (doc, result) in render_all(&storage) {
            let rendered = result.unwrap_or_else(|e| panic!("{doc} over {storage:?}: {e}"));
            if let StorageUrl::S3 { allow_http, .. } = &storage {
                assert!(
                    rendered.contains(&format!("  allow_http: {allow_http}\n")),
                    "{doc}: the stated transport is rendered as stated"
                );
            }
        }
    }
}

/// The refusal never echoes the endpoint: an endpoint is operator input, and
/// it could carry userinfo.
#[test]
fn the_refusal_names_the_rule_and_not_the_endpoint() {
    let storage = s3(Some("http://user:pa55@minio:9000"), false);
    let message = render_restore::render(&restore_plan(storage))
        .unwrap_err()
        .to_string();
    assert!(message.contains("allow_http is false"), "{message}");
    assert!(message.contains("C15"), "{message}");
    assert!(
        !message.contains("pa55") && !message.contains("minio"),
        "{message}"
    );
}

/// **FX-20 fix round (review F1)**, the renderer's backstop: a region that is
/// not a region name never reaches an engine document, in any of the three;
/// a real region and no region render (the negative control).
#[test]
fn fx20_every_document_refuses_a_region_that_is_not_a_region_name() {
    let with_region = |region: Option<&str>| StorageUrl::S3 {
        bucket: "victim-backups".into(),
        prefix: "team-a".into(),
        region: region.map(str::to_string),
        endpoint: None,
        path_style: true,
        allow_http: false,
    };
    for region in ["x@127.0.0.1:9/", "us-east-1.attacker.example#", "US-EAST-1"] {
        for (doc, result) in render_all(&with_region(Some(region))) {
            let error = result.unwrap_err();
            assert_eq!(
                error,
                RenderError::StorageRegionInvalid,
                "{doc} over {region:?}"
            );
            assert!(!error.to_string().contains(region), "{error}");
        }
    }
    for region in [Some("eu-west-1"), None] {
        for (doc, result) in render_all(&with_region(region)) {
            assert!(result.is_ok(), "{doc} over {region:?}: {result:?}");
        }
    }
}
