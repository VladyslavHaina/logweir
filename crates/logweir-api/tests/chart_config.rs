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
    assert!(
        template.contains(&guard),
        "api-config.yaml must carry `{guard}`"
    );
    for render in ["console", "console-shared"] {
        let text = console_config(render);
        assert!(!text.contains("rateLimits"), "{render}: {text}");
        let config = logweir_api::config::Config::parse(&text, Path::new("/")).expect("parses");
        assert_eq!(config.run_rate_limits, defaults, "{render}");
    }
}

/// **FX-15: every `api.console` value this binary reads reaches the parsed
/// `Config` at a NON-default value.** `examples/tuned.values.yaml` (localAdmin)
/// and `examples/console-tuned.values.yaml` (shared) set each one away from
/// its default; their renders' configurations are parsed by the real
/// `Config::parse` and read back field by field. CONTROL: the same reads over
/// the default renders (`console`, `console-shared`) give the defaults, so a
/// template or a parser that dropped a value would read as the control does.
///
/// MUTANTS: render `listen` from a literal 8484; drop `expectedVersion` or
/// `sessionMaxAgeSeconds` from the shared template; read `rateLimits` and keep
/// the defaults.
#[test]
fn every_console_value_reaches_the_parsed_configuration_at_a_non_default_value() {
    let parse = |render: &str| {
        logweir_api::config::Config::parse(&console_config(render), Path::new("/"))
            .unwrap_or_else(|e| panic!("rendered/{render}.yaml: {e}"))
    };
    let limits = |backups, restores| logweir_api::routes::RunRateLimits {
        manual_backups_per_minute: backups,
        manual_restores_per_minute: restores,
    };
    let key_versions = |config: &logweir_api::config::Config| {
        let shared = config.shared().expect("shared mode");
        let cursor = match &config.cursor_key {
            logweir_api::config::CursorKeySource::Versioned(key) => key.expected_version,
            other => panic!("shared mode's cursor key is versioned: {other:?}"),
        };
        (shared.session_key.expected_version, cursor)
    };

    // localAdmin: port, localAdminSubject, rateLimits.
    for (render, port, subject, rate) in [
        ("tuned", 8485, "tuned-admin", limits(11, 9)),
        ("console", 8484, "admin", limits(10, 5)),
    ] {
        let config = parse(render);
        assert_eq!(config.listen.port(), port, "{render}: port");
        assert_eq!(
            config.local_admin().expect("localAdmin mode").subject,
            subject,
            "{render}: localAdminSubject"
        );
        assert_eq!(config.run_rate_limits, rate, "{render}: rateLimits");
    }

    // shared: port, keyVersion (both keys), sessionMaxAgeSeconds, rateLimits.
    for (render, port, version, max_age, rate) in [
        ("console-tuned", 8486, 2, 600, limits(12, 7)),
        ("console-shared", 8484, 1, 900, limits(10, 5)),
    ] {
        let config = parse(render);
        assert_eq!(config.listen.port(), port, "{render}: port");
        assert_eq!(
            key_versions(&config),
            (version, version),
            "{render}: keyVersion"
        );
        assert_eq!(
            config
                .shared()
                .expect("shared mode")
                .session_max_age_seconds,
            max_age,
            "{render}: sessionMaxAgeSeconds"
        );
        assert_eq!(config.run_rate_limits, rate, "{render}: rateLimits");
    }
}
