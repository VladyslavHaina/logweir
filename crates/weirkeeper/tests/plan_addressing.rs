//! **The rendered `backup.yaml`'s archive addressing** — Task 24 fix round 1,
//! closing review finding 2's `endpoint`/`region` half.
//!
//! # Why this is a test binary of its own, with ONE test in it
//!
//! `retention::storage_url_for` reads `AWS_ENDPOINT_URL` and `AWS_REGION` from
//! the PROCESS environment, and the property under test is what it renders
//! with and without them. `std::env::set_var` mutates state shared by every
//! thread in the binary, and `cargo test` runs a binary's rows in parallel by
//! default — so a row that set those variables inside
//! `tests/backup_controller.rs` would be a race against every other row there
//! that renders a plan. A separate integration test is a separate PROCESS with
//! its own environment, and one row in it cannot race itself.
//!
//! No socket, no cluster, no daemon: two calls to a pure renderer.

use serde_json::Value;
use weirkeeper::controllers::backup::{plan_config_map, PLAN_SPEC_KEY};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::kafka_cluster::KafkaCluster;

const NS: &str = "logweir-t24";
const NAME: &str = "demo-backup";
const UID: &str = "7a1f0c33-0000-4000-8000-000000000024";

fn backup() -> Backup {
    serde_json::from_str(&format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
  "metadata": {{ "name": "{NAME}", "namespace": "{NS}", "uid": "{UID}", "generation": 1 }},
  "spec": {{
    "sourceRef": {{ "name": "demo" }},
    "topics": ["orders"],
    "archive": {{ "url": "s3://kafka-backups/k8s-demo", "secretRef": {{ "name": "logweir-s3" }} }},
    "triggeredBy": "manual",
    "deadlineSeconds": 3600
  }}
}}"#
    ))
    .expect("the fixture is a Backup")
}

fn cluster() -> KafkaCluster {
    serde_json::from_str(&format!(
        r#"{{
  "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
  "metadata": {{ "name": "demo", "namespace": "{NS}",
                 "uid": "8b3c1d2e-0000-4000-8000-0000000000c4" }},
  "spec": {{
    "bootstrapServers": ["kafka:9092"],
    "auth": {{ "mode": "plaintext", "tls": false }},
    "role": "source"
  }},
  "status": {{ "reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk" }}
}}"#
    ))
    .expect("the fixture is a KafkaCluster")
}

/// The rendered `backup.yaml`'s `storage` block, as a parsed YAML mapping.
fn rendered_storage() -> serde_yaml::Mapping {
    let cm = plan_config_map(&backup(), &cluster()).expect("the plan renders");
    let text = cm
        .data
        .as_ref()
        .and_then(|d| d.get(PLAN_SPEC_KEY))
        .cloned()
        .expect("the ConfigMap carries the rendered spec");
    let doc: serde_yaml::Value = serde_yaml::from_str(&text).expect("the rendered plan is YAML");
    doc.get("storage")
        .and_then(|s| s.as_mapping())
        .cloned()
        .unwrap_or_else(|| panic!("the rendered plan carries a storage block:\n{text}"))
}

fn key(m: &serde_yaml::Mapping, k: &str) -> Option<serde_yaml::Value> {
    m.get(serde_yaml::Value::String(k.to_string())).cloned()
}

/// **THE RENDERED PLAN CARRIES THE ARCHIVE'S ENDPOINT AND REGION WHEN THEY ARE
/// CONFIGURED, AND OMITS THEM WHEN THEY ARE NOT.**
///
/// # The defect, measured
///
/// `controllers::backup::plan_backup_spec` builds the `storage` block the
/// RUNNER mounts and the pinned engine parses. The engine reads that
/// document's own `endpoint` and `region` keys; **it has never read
/// `AWS_ENDPOINT_URL`**. `storage_url_for` returned `endpoint: None`
/// unconditionally, so a controller configured for MinIO rendered
/// `endpoint: null` into the plan and the engine dialled Amazon — the `Backup`
/// came back `exitCode: 1` with nothing in the bucket, measured on the first
/// Phase B run of Task 24. `path_style` and `allow_http` were already read
/// from the environment here for exactly this reason; these two were the ones
/// that were missing.
///
/// # What a reviewer should read twice
///
/// The mutant that put this row here was "revert `endpoint`/`region` to
/// `None`", and it **survived the entire workspace suite**: every other
/// assertion about the rendered plan compares it against
/// `storage_url_for`'s own output, which moves with the mutant. This row
/// compares it against the ENVIRONMENT instead, which does not.
///
/// KILLS: `region: env_value("AWS_REGION")` → `region: None`, and the same for
/// `endpoint` — the rendered document then names the bucket and nothing about
/// where the bucket IS, and no other row in the tree notices.
#[test]
fn the_rendered_plan_carries_the_configured_endpoint_and_region() {
    // ---- ARM 1: nothing configured -------------------------------------
    //
    // `remove_var`/`set_var` are `unsafe` from the 2024 edition; this crate is
    // 2021, and this binary holds ONE test, so the mutation races nothing.
    // The same decision `crates/logweir/tests/auth_binding.rs` records.
    std::env::remove_var("AWS_ENDPOINT_URL");
    std::env::remove_var("AWS_REGION");
    let storage = rendered_storage();
    assert_eq!(
        key(&storage, "backend"),
        Some(serde_yaml::Value::String("s3".to_string())),
        "the enum is internally tagged and the tag is in the document"
    );
    for k in ["endpoint", "region"] {
        assert_eq!(
            key(&storage, k),
            Some(serde_yaml::Value::Null),
            "with nothing configured the rendered plan says `{k}: null` — an explicit \"no \
             value\", which is the shape a real AWS deployment wants and the shape every gate in \
             this plan runs in. It must NOT be a leftover value from another cluster's \
             configuration. Got {storage:?}"
        );
    }

    // ---- ARM 2: configured, and the VALUES reach the document ----------
    //
    // The demo's own addressing, verbatim: docker-desktop pods reach the
    // compose stack's MinIO only as `host.docker.internal:9000`.
    std::env::set_var("AWS_ENDPOINT_URL", "http://host.docker.internal:9000");
    std::env::set_var("AWS_REGION", "us-east-1");
    let storage = rendered_storage();
    std::env::remove_var("AWS_ENDPOINT_URL");
    std::env::remove_var("AWS_REGION");

    assert_eq!(
        key(&storage, "endpoint"),
        Some(serde_yaml::Value::String(
            "http://host.docker.internal:9000".to_string()
        )),
        "THE DEFECT: the engine reads the PLAN's `endpoint`, never `AWS_ENDPOINT_URL`, so a \
         controller pointed at MinIO that renders no endpoint sends its runner to Amazon. Got \
         {storage:?}"
    );
    assert_eq!(
        key(&storage, "region"),
        Some(serde_yaml::Value::String("us-east-1".to_string())),
        "and the region with it: got {storage:?}"
    );

    // …AND THE DOCUMENT IS STILL ONE THE CLI PARSES. A `storage` block that
    // gained a key the runner's own type does not know is a plan that fails to
    // load, which is worse than the endpoint it fixed.
    let cm = {
        std::env::set_var("AWS_ENDPOINT_URL", "http://host.docker.internal:9000");
        let cm = plan_config_map(&backup(), &cluster()).expect("the plan renders");
        std::env::remove_var("AWS_ENDPOINT_URL");
        cm
    };
    let text = cm
        .data
        .as_ref()
        .and_then(|d| d.get(PLAN_SPEC_KEY))
        .cloned()
        .expect("the rendered spec");
    let parsed: logweir_core::spec::BackupSpec = serde_yaml::from_str(&text)
        .unwrap_or_else(|e| panic!("the rendered backup.yaml does not parse: {e}\n{text}"));
    match parsed.storage {
        logweir_core::engine::StorageUrl::S3 {
            ref endpoint,
            ref bucket,
            ..
        } => {
            assert_eq!(
                endpoint.as_deref(),
                Some("http://host.docker.internal:9000")
            );
            assert_eq!(bucket, "kafka-backups");
        }
        ref other => panic!("`s3://…` renders the S3 variant; got {other:?}"),
    }

    // NO CREDENTIAL REACHED THE DOCUMENT. `AWS_ACCESS_KEY_ID` and
    // `AWS_SECRET_ACCESS_KEY` are deliberately not read here: the runner's
    // archive credential is a `secretKeyRef` on the Job, and a ConfigMap has
    // no encryption at rest.
    let body: Value = serde_json::to_value(cm.data).expect("the data serialises");
    let body = body.to_string();
    for forbidden in ["ACCESS_KEY", "access-key", "secret", "password"] {
        assert!(
            !body.contains(forbidden),
            "the plan ConfigMap must not name `{forbidden}`"
        );
    }
}
