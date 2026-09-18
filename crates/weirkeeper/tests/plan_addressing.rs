//! **The rendered `backup.yaml`'s archive addressing** — Task 24 fix round 1,
//! closing review finding 2's `endpoint`/`region` half.
//!
//! # Why this is a test binary of its own, and why its rows take a lock
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
//! D2 W10 adds the second half of the same property, and it belongs in the same
//! binary for the same reason: **a planted `AWS_ENDPOINT_URL` or
//! `AWS_ALLOW_HTTP` in the CONTROLLER's environment must reach a legacy run's
//! Job (that is how every existing install points its runners at a non-AWS
//! store) and must reach a destination-backed one NOWHERE AT ALL** — defect
//! SEC-ENVHTTP's controller half. Both arms mutate the process environment, so
//! both live here, in ONE test, in a binary of its own.
//!
//! Every row here therefore takes [`ENVIRONMENT`] for its whole body and
//! restores the variables it set before releasing it. That is not decoration:
//! the second row was added with no lock and the first row failed IMMEDIATELY,
//! reading `endpoint: http://planted.invalid:9000` where it asserts `null`.
//!
//! No socket, no cluster, no daemon: calls to pure renderers.

use serde_json::Value;
use weirkeeper::controllers::backup::{
    desired_execution_inputs_for_destination, plan_config_map, runner_job_spec_from_inputs,
    PLAN_SPEC_KEY,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::crds::kafka_cluster::KafkaCluster;

/// The process environment, held for the length of one row.
///
/// `std::env::set_var` mutates state shared by every thread in this binary, and
/// the rows here are ABOUT that state. A `Mutex` is what makes "one row at a
/// time" true rather than "one row exists, so far".
///
/// POISONING IS IGNORED (`unwrap_or_else(PoisonError::into_inner)`): a row that
/// panicked while holding it has already failed, and turning that into a
/// cascade of poisoned-lock panics in every other row hides which one broke.
static ENVIRONMENT: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Take [`ENVIRONMENT`], and restore the four variables when the row ends —
/// including when it ends by panicking, which is when a leaked value would do
/// the most damage to the next row.
struct EnvironmentGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl EnvironmentGuard {
    fn take() -> Self {
        let guard = ENVIRONMENT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear_addressing();
        Self(guard)
    }
}

impl Drop for EnvironmentGuard {
    fn drop(&mut self) {
        clear_addressing();
    }
}

fn clear_addressing() {
    for name in weirkeeper::controllers::backup::ARCHIVE_ADDRESSING_ENV {
        std::env::remove_var(name);
    }
}

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
    let _environment = EnvironmentGuard::take();
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

/// The saved destination the second arm renders against: a DIFFERENT endpoint,
/// a DIFFERENT transport and a DIFFERENT addressing from anything the planted
/// environment says, so a value that leaks through is unmistakable.
fn dest() -> BackupDestination {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": "saved", "namespace": NS,
            "uid": "d0000000-0000-4000-8000-000000000024",
            "generation": 1, "resourceVersion": "1"
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-saved", "prefix": "prod",
                "region": "eu-west-1", "endpoint": "https://minio.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "TLS"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "saved-writer",
                    "accessKeyIdKey": "access-key-id",
                    "secretAccessKeyKey": "secret-access-key"
                }}
            }
        },
        "status": {
            "observedGeneration": 1,
            "conditions": [{"type": "Valid", "status": "True", "reason": "Valid",
                            "observedGeneration": 1}]
        }
    }))
    .expect("the fixture is a BackupDestination")
}

/// [`backup`], re-pointed at `saved`: the sentinel URL the CEL rule requires,
/// and no `secretRef`.
fn destination_backed() -> Backup {
    let mut value: Value =
        serde_json::from_str(&serde_json::to_string(&backup()).expect("it serialises"))
            .expect("it parses");
    value["spec"]["archive"] = serde_json::json!({"url": "logweir-destination://saved"});
    value["spec"]["destinationRef"] = serde_json::json!({"name": "saved"});
    serde_json::from_value(value).expect("the mutated fixture is a Backup")
}

/// **A PLANTED `AWS_*` IN THE CONTROLLER'S OWN ENVIRONMENT REACHES A LEGACY
/// JOB AND NEVER A DESTINATION-BACKED ONE.**
///
/// # The defect, exactly (SEC-ENVHTTP)
///
/// `controllers::backup::archive_addressing_env` forwards this process's
/// `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and
/// `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` into every runner Job, and the engine's
/// `AmazonS3Builder::from_env()` honours every `AWS_*` it finds. A controller
/// started with `AWS_ALLOW_HTTP=true` therefore enables plaintext transport in
/// a runner **whose approved plan says `allow_http: false`** — a global setting
/// overriding approved execution inputs.
///
/// # BOTH HALVES ARE THE ASSERTION, AND THE FIRST ONE IS NOT AN OVERSIGHT
///
/// The legacy arm asserts the forwarding STILL HAPPENS. Those four variables
/// are how every existing installation points its runners at MinIO or Ceph;
/// removing them would break every upgrade at the moment of the upgrade. The
/// route out is `destinations:from-legacy` (D2 §3.12), not a silent behaviour
/// change — so the legacy decision is pinned here rather than left implicit.
///
/// KILLS: `resolve_inputs` forwarding `addressing_env` for a destination-backed
/// run (the planted endpoint then reaches the Job beside the destination's own
/// settings, and `from_env()` decides which wins); and
/// `runner_job_spec_from_inputs` calling `archive_addressing_env()` instead of
/// reading the frozen list.
#[test]
fn the_controller_environment_reaches_a_legacy_job_and_never_a_destination_backed_one() {
    let _environment = EnvironmentGuard::take();
    std::env::set_var("AWS_ENDPOINT_URL", "http://planted.invalid:9000");
    std::env::set_var("AWS_ALLOW_HTTP", "true");
    std::env::set_var("AWS_REGION", "planted-region-1");
    std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true");

    // ---- ARM 1: the legacy path forwards, and that is deliberate ---------
    let legacy = weirkeeper::controllers::backup::desired_execution_inputs(&backup(), &cluster())
        .expect("the legacy inputs resolve");
    let forwarded: Vec<(String, String)> = legacy
        .inputs
        .archive
        .addressing_env
        .iter()
        .map(|v| (v.name.clone(), v.value.clone()))
        .collect();
    assert!(
        forwarded.contains(&(
            "AWS_ENDPOINT_URL".to_string(),
            "http://planted.invalid:9000".to_string()
        )),
        "the legacy path forwards the controller's addressing, which is how an adopter points \
         their runners at MinIO. Got {forwarded:?}"
    );
    assert!(
        forwarded.contains(&("AWS_ALLOW_HTTP".to_string(), "true".to_string())),
        "and that INCLUDES the transport variable, which is the defect itself, recorded rather \
         than quietly changed. Got {forwarded:?}"
    );

    // ---- ARM 2: the destination-backed path forwards NOTHING -------------
    let role = |role| {
        weirkeeper::destination::resolve(
            &dest(),
            role,
            &weirkeeper::check::policy::Policy::defaults(),
        )
        .expect("the destination resolves")
    };
    let resolved = weirkeeper::controllers::backup::BackupDestinations {
        archive: role(weirkeeper::destination::DestinationRole::ArchiveWrite),
        evidence: role(weirkeeper::destination::DestinationRole::EvidenceWrite),
    };
    let backed = destination_backed();
    let frozen = desired_execution_inputs_for_destination(
        &backed,
        &cluster(),
        &weirkeeper::backup_execution::ResolvedSelection::named(&backed.spec),
        Some(&resolved),
    )
    .expect("the destination-backed inputs resolve");
    assert!(
        frozen.inputs.archive.addressing_env.is_empty(),
        "no forwarded addressing is FROZEN for a destination-backed run. Got {:?}",
        frozen.inputs.archive.addressing_env
    );

    let spec = runner_job_spec_from_inputs(&backed, &cluster(), &frozen)
        .expect("the destination-backed Job renders");
    let env: std::collections::BTreeMap<String, String> = spec.env_literal.into_iter().collect();
    assert_eq!(
        env.get("AWS_ENDPOINT_URL"),
        None,
        "ABSENT BY CONSTRUCTION. The endpoint travels inside the plan's own storage block; a \
         variable `from_env()` would sweep up is a second, silent answer to where the bucket is. \
         Got {env:?}"
    );
    assert_eq!(
        env.get("AWS_ALLOW_HTTP").map(String::as_str),
        Some("false"),
        "THE DEFECT, CLOSED: the destination declares TLS, the process says `true`, and the Job \
         says `false`. The value comes from `transport.security` and from nothing else \
         (D-SEAMS S5). Got {env:?}"
    );
    assert_eq!(
        env.get("AWS_REGION").map(String::as_str),
        Some("eu-west-1"),
        "the destination's region and not the planted one: {env:?}"
    );
    assert_eq!(
        env.get("AWS_VIRTUAL_HOSTED_STYLE_REQUEST")
            .map(String::as_str),
        Some("false"),
        "PathStyle, from `storage.addressing` alone — it does not touch the transport line and \
         the process does not touch it either: {env:?}"
    );

    // …AND THE PLAN THE RUNNER MOUNTS NAMES THE DESTINATION'S OWN LOCATION.
    let text = frozen
        .documents()
        .expect("the documents render")
        .get(PLAN_SPEC_KEY)
        .cloned()
        .expect("the rendered spec");
    let parsed: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(&text).expect("the rendered plan parses");
    match parsed.storage {
        logweir_core::engine::StorageUrl::S3 {
            ref bucket,
            ref endpoint,
            allow_http,
            ..
        } => {
            assert_eq!(bucket, "lw-saved");
            assert_eq!(endpoint.as_deref(), Some("https://minio.storage.svc:9000"));
            assert!(
                !allow_http,
                "the plan agrees with the environment it renders"
            );
        }
        ref other => panic!("the destination is S3; got {other:?}"),
    }
}
