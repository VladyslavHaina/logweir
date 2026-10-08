//! FX-20: the inline-archive LOCATION binding is computed in two places — the
//! product (`credential_binding::archive_location_binding`, what every runner
//! Job expects) and the operator's upgrade tool (`scripts/bind-credential.py`,
//! what it writes into a Secret). A fixture both sides read is the agreement;
//! `scripts/test_bind_credential_rows.py` reads the same file.

use logweir_core::credential_binding::archive_location_binding;
use logweir_core::engine::StorageUrl;

/// The URL and endpoint as the TOOL takes them, as the runner's storage block.
fn storage(url: &str, endpoint: &str) -> StorageUrl {
    let (scheme, rest) = url.split_once("://").expect("a URL");
    let rest = rest.trim_matches('/');
    let (first, tail) = rest.split_once('/').unwrap_or((rest, ""));
    match scheme {
        "s3" => StorageUrl::S3 {
            bucket: first.to_string(),
            prefix: tail.to_string(),
            region: None,
            endpoint: (endpoint != "aws").then(|| endpoint.to_string()),
            path_style: true,
            allow_http: false,
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

#[test]
fn the_tool_and_the_product_compute_one_location_binding() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../e2e/fixtures/credential-binding/location.json");
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("the fixture ships"))
            .expect("JSON");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(
        cases.len() >= 5,
        "the fixture is the agreement; it must not be empty"
    );
    for case in cases {
        let url = case["url"].as_str().expect("url");
        let endpoint = case["endpoint"].as_str().expect("endpoint");
        assert_eq!(
            archive_location_binding(&storage(url, endpoint)),
            case["binding"].as_str().expect("binding"),
            "{url} via {endpoint}"
        );
    }
    // The prefix and the endpoint's spelling are not the location; the bucket
    // and the endpoint are.
    assert_eq!(cases[0]["binding"], cases[1]["binding"]);
    assert_ne!(cases[0]["binding"], cases[2]["binding"]);
}
