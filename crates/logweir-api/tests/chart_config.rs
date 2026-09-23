//! The console configuration the CHART renders is one THIS BINARY accepts.
//!
//! `charts/logweir/templates/ui/api-config.yaml` writes the file and
//! `crate::config` reads it; two sides that must agree are held together by
//! reading the chart's checked-in renders here (the worker rule: a fixture two
//! sides agree on is read by both sides' tests). The PoC chart gaps made this
//! row necessary: `oidc.caBundleFile` (G1) and `trustedProxyService` (G6) are
//! keys the template emits and `deny_unknown_fields` would refuse by name if
//! the two spellings ever drifted.

use std::path::Path;

fn console_config(render: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../charts/logweir/rendered")
        .join(format!("{render}.yaml"));
    let text = std::fs::read_to_string(&path).expect("the render is checked in");
    for doc in serde_yaml::Deserializer::from_str(&text) {
        let value = serde_yaml::Value::deserialize(doc).expect("a YAML document");
        let is_console_config = value["kind"] == "ConfigMap"
            && value["metadata"]["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("logweir-api-config-"));
        if is_console_config {
            return value["data"]["config.yaml"]
                .as_str()
                .expect("the config document")
                .to_string();
        }
    }
    panic!("{render}.yaml renders no console configuration");
}

use serde::Deserialize as _;

#[test]
fn every_rendered_console_configuration_is_one_the_binary_accepts() {
    for render in ["console", "console-shared"] {
        let text = console_config(render);
        logweir_api::config::Config::parse(&text, Path::new("/"))
            .unwrap_or_else(|e| panic!("rendered/{render}.yaml: {e}\n{text}"));
    }
}

#[test]
fn the_shared_render_carries_the_issuer_ca_and_the_proxy_service() {
    let text = console_config("console-shared");
    let config = logweir_api::config::Config::parse(&text, Path::new("/")).unwrap();
    let shared = config.shared().expect("shared mode");
    assert_eq!(
        shared.oidc.ca_bundle_file.as_deref(),
        Some(Path::new("/var/run/logweir/oidc-ca/ca.crt")),
        "G1: the chart mounts the bundle where the file says it is"
    );
    assert!(shared.oidc.system_roots, "the system roots are kept");
    let service = shared
        .trusted_proxy_service
        .as_ref()
        .expect("G6: the ingress controller by its Service");
    assert_eq!(
        (service.namespace.as_str(), service.name.as_str()),
        ("traefik", "traefik")
    );
    assert!(shared.require_trusted_proxy);
}
