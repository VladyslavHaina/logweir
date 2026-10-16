//! **PROD-03.1/03.2, the connection half**: `KafkaCluster.spec.schemaRegistry`
//! resolves, binds, projects and is checked against a restore plan; its Secret
//! is in the connection's workspace (PROD-19.1 §4.4).

use serde_json::{json, Value};
use weirkeeper::conditions::{
    TERMINAL_STATE_CONNECTION_CONFIG_INVALID, TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
};
use weirkeeper::connection::{resolve, ConnectionUse};
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::workspace::{Referent, Referrer};

const UID: &str = "7d1f4a52-0000-4000-8000-00000000a003";

fn cluster(registry: Value) -> KafkaCluster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {
            "name": "src", "namespace": "logweir", "uid": UID,
            "labels": { "logweir.dev/workspace": "dev" }
        },
        "spec": {
            "bootstrapServers": ["k:9092"],
            "auth": { "mode": "plaintext" },
            "role": "source",
            "schemaRegistry": registry
        }
    }))
    .expect("a KafkaCluster")
}

fn basic() -> KafkaCluster {
    cluster(json!({
        "url": "https://sr.example:8081",
        "username": "logweir",
        "secretRef": { "name": "src-credential" }
    }))
}

#[test]
fn a_registry_resolves_into_the_plan_the_binding_and_the_job() {
    let resolved = resolve(&basic(), ConnectionUse::BackupSource).unwrap();
    let registry = resolved.schema_registry.as_ref().expect("resolved");
    assert_eq!(registry.url, "https://sr.example:8081");
    assert_eq!(registry.password.as_ref().unwrap().key, "registry-password");
    // The binding covers the registry; the same connection without one
    // binds differently.
    let without = resolve(&cluster(Value::Null), ConnectionUse::BackupSource).unwrap();
    let mut c = cluster(Value::Null);
    c.metadata.uid = Some(UID.into());
    assert_ne!(resolved.credential_binding(), without.credential_binding());
    // A plaintext connection with a registry Secret publishes its binding.
    assert!(weirkeeper::controllers::kafka_cluster::credential_binding_of(&basic()).is_some());
    assert!(weirkeeper::controllers::kafka_cluster::credential_binding_of(&c).is_none());
    // The projection: the password and the registry Secret's binding,
    // optional, and the expected binding as a literal. Never a value.
    let projection = resolved.project();
    let names: Vec<(&str, &str, bool)> = projection
        .env_from_secret
        .iter()
        .map(|e| (e.name.as_str(), e.key.as_str(), e.optional))
        .collect();
    assert_eq!(
        names,
        vec![
            ("LOGWEIR_SOURCE_CREDENTIAL_BINDING", "logweir-binding", true),
            (
                "LOGWEIR_SOURCE_REGISTRY_PASSWORD",
                "registry-password",
                true
            ),
            (
                "LOGWEIR_SOURCE_REGISTRY_CREDENTIAL_BINDING",
                "logweir-binding",
                true
            ),
        ]
    );
    assert_eq!(
        projection.env_literal,
        vec![(
            "LOGWEIR_SOURCE_CREDENTIAL_BINDING_EXPECTED".to_string(),
            resolved.credential_binding().unwrap()
        )]
    );
    // The restore side projects the target names.
    let target = resolve(&basic(), ConnectionUse::RestoreTarget).unwrap();
    assert!(target
        .project()
        .env_from_secret
        .iter()
        .any(|e| e.name == "LOGWEIR_TARGET_REGISTRY_PASSWORD"));
}

#[test]
fn a_bad_registry_block_is_refused_by_name() {
    for (block, reason) in [
        (
            json!({"url": "ftp://sr:1"}),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
        ),
        (
            json!({"url": "https://user:pw@sr:1"}),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
        ),
        (
            json!({"url": "https://sr:1", "username": "u"}),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        ),
        (
            json!({"url": "https://sr:1", "secretRef": {"name": "s"}}),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        ),
    ] {
        let err = resolve(&cluster(block.clone()), ConnectionUse::Probe).unwrap_err();
        assert_eq!(err.reason, reason, "{block}");
    }
    let err = resolve(
        &cluster(json!({"url": "https://sr:1", "surprise": true})),
        ConnectionUse::Probe,
    )
    .unwrap_err();
    assert!(
        err.message.contains("spec.schemaRegistry.surprise"),
        "{err}"
    );
}

#[test]
fn a_restore_plan_names_the_connections_registry_or_none() {
    let resolved = resolve(&basic(), ConnectionUse::RestoreTarget).unwrap();
    let plan = |registry: &str| format!("target:\n  bootstrap_servers: [\"k:9092\"]\n{registry}");
    resolved
        .check_restore_plan(&plan(
            "  schema_registry:\n    url: https://sr.example:8081\n    username: logweir\n",
        ))
        .unwrap();
    for other in [
        "",
        "  schema_registry:\n    url: https://evil:8081\n    username: logweir\n",
        "  schema_registry:\n    url: https://sr.example:8081\n    username: other\n",
    ] {
        let err = resolved.check_restore_plan(&plan(other)).unwrap_err();
        assert_eq!(
            err.reason, TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
            "{other}"
        );
    }
    // A connection without a registry refuses a plan that names one.
    let none = resolve(&cluster(Value::Null), ConnectionUse::RestoreTarget).unwrap();
    assert!(none
        .check_restore_plan(&plan("  schema_registry:\n    url: https://sr:1\n"))
        .is_err());
}

/// The registry's Secret is one of the connection's referents, so the
/// workspace gate holds it to the connection's workspace before any Job
/// (`tests/workspace_gate.rs::a_registry_secret_of_another_workspace_is_refused`).
#[test]
fn the_registry_secret_is_a_referent_of_its_connection() {
    assert!(basic()
        .referents()
        .contains(&Referent::Secret("src-credential".into())));
    assert!(!cluster(Value::Null)
        .referents()
        .iter()
        .any(|r| matches!(r, Referent::Secret(_))));
}

#[test]
fn the_restore_status_copies_the_signed_schemas_block() {
    use weirkeeper::controllers::restore::schemas_record;
    let r = schemas_record(
        &json!({"snapshot_sha256": "sha256:x", "target_registry_url": "u",
        "imported_count": 6, "already_present": 1}),
    )
    .unwrap();
    assert_eq!(
        (r.imported, r.already_present, r.not_imported),
        (6, 1, None)
    );
    let r = schemas_record(&json!({"not_imported": "noSnapshot"})).unwrap();
    assert_eq!(r.not_imported.as_deref(), Some("noSnapshot"));
    // A reason outside the closed set is not copied.
    let r = schemas_record(&json!({"not_imported": "<script>"})).unwrap();
    assert_eq!(r.not_imported, None);
    assert!(schemas_record(&json!(null)).is_none());
}
