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

/// **P10 review L2: the chart renders `rateLimits` only away from THIS
/// binary's defaults**, so an older console image (which refuses the unknown
/// key and does not start) keeps starting on a default install. The values
/// file ships the defaults, the template's guard names them, and the default
/// console renders carry no block and parse to exactly the defaults.
#[test]
fn rate_limits_are_rendered_only_away_from_the_binarys_defaults() {
    let defaults = logweir_api::routes::RunRateLimits::default();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let values: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(root.join("charts/logweir/values.yaml")).expect("values"),
    )
    .expect("YAML");
    let rl = &values["api"]["console"]["rateLimits"];
    assert_eq!(
        rl["manualBackupsPerMinute"].as_u64(),
        Some(u64::from(defaults.manual_backups_per_minute))
    );
    assert_eq!(
        rl["manualRestoresPerMinute"].as_u64(),
        Some(u64::from(defaults.manual_restores_per_minute))
    );
    let template =
        std::fs::read_to_string(root.join("charts/logweir/templates/ui/api-config.yaml"))
            .expect("the template");
    let guard = format!(
        "(ne $rlBackups {}) (ne $rlRestores {})",
        defaults.manual_backups_per_minute, defaults.manual_restores_per_minute
    );
    assert!(template.contains(&guard), "api-config.yaml must carry `{guard}`");
    for render in ["console", "console-shared"] {
        let text = console_config(render);
        assert!(!text.contains("rateLimits"), "{render}: {text}");
        let config = logweir_api::config::Config::parse(&text, Path::new("/")).expect("parses");
        assert_eq!(config.run_rate_limits, defaults, "{render}");
    }
}
