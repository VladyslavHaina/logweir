//! FX-20 (and its fix round, review F1/F2): every credential binding form is
//! computed in two places — the product (`logweir_core::credential_binding`
//! and, for a `KafkaCluster`, `logweir_core::connection::credential_binding`:
//! what the controller publishes and every runner Job expects) and the
//! operator's upgrade tool (`scripts/bind-credential.py`, which COMPUTES the
//! binding from the object's spec, compares it with the published one and
//! writes it into a Secret). A fixture both sides read is the agreement;
//! `scripts/test_bind_credential_rows.py` reads the same file.

use logweir_core::credential_binding::{
    archive_location_binding, destination_binding, notification_binding, retention_binding,
    NotificationSink,
};
use logweir_core::destination::{
    Addressing, DestinationLocation, StorageProvider, TransportSecurity,
};
use logweir_core::engine::StorageUrl;
use logweir_core::spec::AuthSpec;
use serde_json::Value;

fn fixture() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../e2e/fixtures/credential-binding/bindings.json");
    serde_json::from_str(&std::fs::read_to_string(&path).expect("the fixture ships")).expect("JSON")
}

fn cases<'a>(doc: &'a Value, kind: &str, at_least: usize) -> &'a Vec<Value> {
    let cases = doc[kind].as_array().expect(kind);
    assert!(
        cases.len() >= at_least,
        "the fixture is the agreement; its {kind} cases must not shrink"
    );
    cases
}

fn text(v: &Value) -> Option<String> {
    v.as_str().map(str::to_string)
}

/// The URL and route as the TOOL takes them (`--location`, `--endpoint`,
/// `--region`, `--path-style`, `--allow-http`), as the runner's storage block.
fn location(case: &Value) -> StorageUrl {
    let url = case["url"].as_str().expect("url");
    let endpoint = case["endpoint"].as_str().expect("endpoint");
    let (scheme, rest) = url.split_once("://").expect("a URL");
    let rest = rest.trim_matches('/');
    let (first, tail) = rest.split_once('/').unwrap_or((rest, ""));
    match scheme {
        "s3" => StorageUrl::S3 {
            bucket: first.to_string(),
            prefix: tail.to_string(),
            region: text(&case["region"]),
            endpoint: (endpoint != "aws").then(|| endpoint.to_string()),
            path_style: case["pathStyle"].as_bool().expect("pathStyle"),
            allow_http: case["allowHttp"].as_bool().expect("allowHttp"),
        },
        "gs" => StorageUrl::Gcs {
            bucket: first.to_string(),
            prefix: tail.to_string(),
        },
        "az" => {
            let (container, prefix) = tail.split_once('/').unwrap_or((tail, ""));
            StorageUrl::Azure {
                account_name: first.to_string(),
                container_name: container.to_string(),
                prefix: prefix.to_string(),
            }
        }
        other => panic!("no fixture case uses {other}"),
    }
}

/// A `BackupDestination` spec (the CRD's shape) as the controller reads it:
/// `DestinationLocation::archive_storage_url`, the route its status binding
/// is computed over.
fn destination_route(spec: &Value) -> StorageUrl {
    let storage = &spec["storage"];
    DestinationLocation {
        provider: StorageProvider::S3,
        bucket: text(&storage["bucket"]).expect("bucket"),
        prefix: text(&storage["prefix"]).unwrap_or_default(),
        region: text(&storage["region"]),
        endpoint: text(&storage["endpoint"]),
        addressing: match storage["addressing"].as_str() {
            Some("PathStyle") => Addressing::PathStyle,
            Some("VirtualHosted") => Addressing::VirtualHosted,
            other => panic!("addressing {other:?}"),
        },
        transport: match spec["transport"]["security"].as_str() {
            Some("TLS") => TransportSecurity::Tls,
            Some("InsecureHTTP") => TransportSecurity::InsecureHttp,
            other => panic!("security {other:?}"),
        },
    }
    .archive_storage_url()
}

#[test]
fn the_tool_and_the_product_compute_one_location_binding() {
    let doc = fixture();
    for case in cases(&doc, "location", 8) {
        assert_eq!(
            archive_location_binding(&location(case)),
            case["binding"].as_str().expect("binding"),
            "{case}"
        );
    }
    // Review F1's shape, in the agreement itself: the victim's bucket on the
    // AWS-default endpoint with an injected region is a DIFFERENT location.
    let by_region: std::collections::BTreeSet<&str> = cases(&doc, "location", 8)
        .iter()
        .filter(|c| c["url"] == "s3://victim-backups/team-a" && c["pathStyle"] == false)
        .map(|c| c["binding"].as_str().expect("binding"))
        .collect();
    assert_eq!(by_region.len(), 2, "the region moves the location binding");
}

#[test]
fn the_tool_and_the_product_compute_one_destination_and_retention_binding() {
    let doc = fixture();
    for case in cases(&doc, "destination", 2) {
        assert_eq!(
            destination_binding(
                case["uid"].as_str().expect("uid"),
                &destination_route(&case["spec"])
            ),
            case["binding"].as_str().expect("binding"),
            "{case}"
        );
    }
    for case in cases(&doc, "retention", 2) {
        assert_eq!(
            retention_binding(
                case["uid"].as_str().expect("uid"),
                &destination_route(&case["destinationSpec"]),
                case["scope"].as_str().expect("scope"),
            ),
            case["binding"].as_str().expect("binding"),
            "{case}"
        );
    }
}

#[test]
fn the_tool_and_the_product_compute_one_notification_binding() {
    let doc = fixture();
    for case in cases(&doc, "notification", 4) {
        let sink = match case["sink"].as_str() {
            Some("pagerduty") => NotificationSink::PagerDuty,
            Some("webhook") => NotificationSink::Webhook,
            Some("slack") => NotificationSink::Slack,
            other => panic!("sink {other:?}"),
        };
        assert_eq!(
            notification_binding(
                case["uid"].as_str().expect("uid"),
                sink,
                case["endpoint"].as_str()
            ),
            case["binding"].as_str().expect("binding"),
            "{case}"
        );
    }
}

/// PROD-01.3's form, from a `KafkaCluster` spec as the controller resolves it
/// (`weirkeeper::connection::resolve` then `credential_binding`).
#[test]
fn the_tool_and_the_product_compute_one_kafka_binding() {
    let doc = fixture();
    for case in cases(&doc, "kafka", 4) {
        let spec = &case["spec"];
        let auth = &spec["auth"];
        let username = || text(&auth["username"]).expect("username");
        let tls = auth["tls"].as_bool().unwrap_or(false);
        let resolved = match auth["mode"].as_str() {
            Some("plaintext") => AuthSpec::Plaintext,
            Some("scramSha512") => AuthSpec::ScramSha512 {
                username: username(),
                tls,
            },
            Some("scramSha256") => AuthSpec::ScramSha256 {
                username: username(),
                tls,
            },
            Some("plain") => AuthSpec::Plain {
                username: username(),
                tls,
            },
            Some("mtls") => AuthSpec::Mtls { tls: true },
            other => panic!("mode {other:?}"),
        };
        let ca = if let Some(r) = auth["tlsCa"]["secretKeyRef"].as_object() {
            Some(format!(
                "secret/{}/{}",
                r["name"].as_str().unwrap(),
                r["key"].as_str().unwrap()
            ))
        } else {
            auth["tlsCa"]["configMapKeyRef"].as_object().map(|r| {
                format!(
                    "configMap/{}/{}",
                    r["name"].as_str().unwrap(),
                    r["key"].as_str().unwrap()
                )
            })
        };
        let servers: Vec<String> = spec["bootstrapServers"]
            .as_array()
            .expect("bootstrapServers")
            .iter()
            .map(|s| s.as_str().expect("server").to_string())
            .collect();
        assert_eq!(
            logweir_core::connection::credential_binding(
                case["uid"].as_str().expect("uid"),
                &servers,
                &resolved,
                ca.as_deref(),
            ),
            case["binding"].as_str().expect("binding"),
            "{case}"
        );
    }
}
