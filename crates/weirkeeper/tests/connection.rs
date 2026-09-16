//! The saved-connection contract — PLAT-07.1.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A FIXTURE TEST. Nothing dials a
//! broker, nothing reads a Secret, nothing runs `kubectl`: the resolver's whole
//! job is to turn an object into references, and a reference is checkable
//! without the thing it references.
//!
//! The four properties this file exists for:
//!
//! 1. **One resolution, three Jobs.** `probe_backup_and_restore_jobs_carry_one
//!    _connection` builds all three Jobs from ONE `KafkaCluster` and asserts
//!    they name the same Secret, the same key, the same CA and the same file
//!    path, differing only in the side prefix the runner reads.
//! 2. **Old objects, unchanged.** `legacy_objects_build_the_jobs_they_always
//!    _did` compares against outputs CAPTURED from the pre-PLAT-07 controller
//!    (`tests/fixtures/connection/legacy-golden`, rendered at main 4956785),
//!    not against this build's idea of what they used to be.
//! 3. **No value, anywhere.** `no_seeded_credential_value_reaches_a_job_a_plan
//!    _or_a_refusal` seeds recognisable values in every place a value could
//!    come from and greps the whole rendered output for them.
//! 4. **Refused before anything exists.** Every conflicting or unusable
//!    connection is a refusal from `resolve`, which the three controllers reach
//!    before their first `POST`.

use std::collections::BTreeSet;

use serde_json::{json, Value};

use weirkeeper::backup_execution::{self, ExecutionTrigger};
use weirkeeper::conditions::{
    TERMINAL_STATES, TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
    TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED, TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
    TERMINAL_STATE_CONNECTION_REFERENCE_INVALID, TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
};
use weirkeeper::connection::{
    credential, resolve, CaSourceKind, ConnectionRefusal, ConnectionUse, ResolvedConnection, Side,
    CA_FILE_NAME, CONTRACT_VERSION,
};
use weirkeeper::controllers::{backup, kafka_cluster, restore};
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::kafka_cluster::{KafkaCluster, DEFAULT_PASSWORD_KEY};
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::trust_roster::TrustRoster;
use weirkeeper::job;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-plat07";
const CLUSTER: &str = "orders-prod";
const CLUSTER_UID: &str = "7d1f4a52-0000-4000-8000-0000000007a1";
const BACKUP_UID: &str = "5c2e7b91-0000-4000-8000-0000000007b1";
const RESTORE_UID: &str = "5c2e7b91-0000-4000-8000-0000000007c1";
const KEY_ID: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// The SASL password a Secret holds. **Never a substring of anything the
/// controller renders** — that is what the redaction rows measure.
const SEEDED_PASSWORD: &str = "SEEDED-PASSWORD-1d3b5f79";

/// A `KafkaCluster` with the given `auth` block.
fn cluster_with(auth: Value) -> KafkaCluster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": { "name": CLUSTER, "namespace": NS, "uid": CLUSTER_UID, "generation": 1 },
        "spec": {
            "bootstrapServers": ["b0.orders:9093", "b1.orders:9093"],
            "auth": auth,
            "role": "source",
            "markerTopic": "logweir.scratch"
        },
        "status": { "reachable": true, "clusterId": "SEEDCLUSTERID0000000001" }
    }))
    .expect("the fixture is a KafkaCluster")
}

fn plaintext() -> KafkaCluster {
    cluster_with(json!({ "mode": "plaintext", "tls": false }))
}

fn scram_tls() -> KafkaCluster {
    cluster_with(json!({
        "mode": "scramSha512",
        "username": "logweir",
        "secretRef": { "name": "orders-sasl" },
        "tls": true
    }))
}

fn scram_tls_with_ca(source: Value) -> KafkaCluster {
    cluster_with(json!({
        "mode": "scramSha512",
        "username": "logweir",
        "secretRef": { "name": "orders-sasl", "passwordKey": "sasl-password" },
        "tls": true,
        "tlsCa": source
    }))
}

/// The plan bytes a `Restore` against `cluster` must carry — the same public
/// settings, in the runner's own grammar.
fn plan_for(cluster: &KafkaCluster) -> String {
    let connection = resolve(cluster, ConnectionUse::BackupSource).expect("the fixture resolves");
    let servers = connection.bootstrap_servers.join(", ");
    let auth = match &connection.auth {
        logweir_core::spec::AuthSpec::Plaintext => String::new(),
        logweir_core::spec::AuthSpec::ScramSha512 { username, tls } => {
            format!("  auth:\n    mode: scramSha512\n    username: {username}\n    tls: {tls}\n")
        }
    };
    format!(
        "source:\n  storage:\n    backend: s3\n    bucket: kafka-backups\n    prefix: drill-demo\n \
         \n  backup: latestCompleted\n  topics: [orders]\ntarget:\n  bootstrap_servers: \
         [{servers}]\n{auth}  mode: scratch\n  topic_mapping_prefix: \"drill-\"\n  marker_topic: \
         logweir.scratch\n  default_replication_factor: 1\n  teardown: delete\nsample:\n  \
         window_start: \"2026-09-07T12:00:00Z\"\n  window_end: \"2026-09-07T15:00:00Z\"\n  \
         records_per_partition: 25\n  anchor: head\nobjectives: {{}}\nevidence:\n  backend: s3\n  \
         bucket: logweir-evidence\n  prefix: logweir/\n"
    )
}

fn backup_object() -> Backup {
    let argv = serde_json::to_string(&[
        "backup",
        "run",
        "--spec",
        "/plan/backup.yaml",
        "--allowed-clusters",
        "/plan/allowed-clusters.json",
        "--signing-key",
        "/signing/key.pem",
        "--receipt-out",
        "/work/receipt.json",
        "--triggered-by",
        "manual",
    ])
    .expect("the argv serialises");
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": "connection-backup",
            "namespace": NS,
            "uid": BACKUP_UID,
            "annotations": { "logweir.dev/runner-argv": argv }
        },
        "spec": {
            "sourceRef": { "name": CLUSTER },
            "topics": ["orders"],
            "archive": { "url": "s3://kafka-backups/logweir", "secretRef": { "name": "logweir-s3" } },
            "triggeredBy": "manual",
            "deadlineSeconds": 600
        }
    }))
    .expect("the fixture is a Backup")
}

fn restore_object(plan: &str) -> Restore {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": "connection-restore", "namespace": NS, "uid": RESTORE_UID, "generation": 1 },
        "spec": {
            "planBytes": plan,
            "approvalRef": { "name": "a1" },
            "sourceArchive": { "url": "s3://kafka-backups/logweir", "secretRef": { "name": "logweir-s3" } },
            "backupSetRef": "drill-demo",
            "pointInTime": "2026-09-07T14:05:00Z",
            "target": { "clusterRef": { "name": CLUSTER }, "mode": "scratch", "topicNaming": { "prefix": "drill-" } },
            "deadlineSeconds": 1800
        }
    }))
    .expect("the fixture is a Restore")
}

fn approval_for(plan: &str) -> Approval {
    let hash = logweir_core::ids::sha256_prefixed(plan.as_bytes());
    let doc = format!(
        r#"{{"approver":"sre-oncall@example.com","ticket":"CHG-1","plan_hash":"{hash}","subject_kind":"Restore"}}"#
    );
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": { "name": "a1", "namespace": NS, "uid": "aaaaaaaa-0000-4000-8000-0000000007d1" },
        "spec": {
            "subjectRef": { "kind": "Restore", "name": "connection-restore" },
            "planHash": hash,
            "approvalBytes": doc,
            "sidecarBytes": "{}"
        },
        "status": {
            "verified": true,
            "matchedKeyId": KEY_ID,
            "verifiedSubjectRef": { "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "connection-restore", "namespace": NS, "uid": RESTORE_UID }
        }
    }))
    .expect("the fixture is an Approval")
}

fn roster() -> TrustRoster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustRoster",
        "metadata": { "name": "default", "uid": "dddddddd-0000-4000-8000-0000000007e1" },
        "spec": {
            "approverKeys": [ { "keyId": KEY_ID, "spkiPem": "-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n" } ],
            "signingKeys": [],
            // The value the legacy goldens were captured with: the roster's
            // allowlist reaches the Job as a hashed bundle member, so a
            // different list would be a different Job for reasons that have
            // nothing to do with the connection.
            "allowedClusterIds": ["LEGACY00000000000000001"]
        },
        "status": { "loaded": true }
    }))
    .expect("the fixture is a TrustRoster")
}

/// The three Jobs one `KafkaCluster` produces, as rendered Kubernetes objects.
fn three_jobs(cluster: &KafkaCluster) -> (Value, Value, Value) {
    let probe = job::build(&kafka_cluster::runner_job_spec(cluster).expect("probe spec"));
    let backup_job =
        job::build(&backup::runner_job_spec(&backup_object(), cluster).expect("backup spec"));
    let plan = plan_for(cluster);
    let restore_job = job::build(
        &restore::runner_job_spec(
            &restore_object(&plan),
            cluster,
            &[KEY_ID.to_string()],
            &approval_for(&plan),
            &roster(),
        )
        .expect("restore spec"),
    );
    (
        serde_json::to_value(probe).expect("a Job serialises"),
        serde_json::to_value(backup_job).expect("a Job serialises"),
        serde_json::to_value(restore_job).expect("a Job serialises"),
    )
}

fn container(job: &Value) -> &Value {
    &job["spec"]["template"]["spec"]["containers"][0]
}

fn env_of(job: &Value, name: &str) -> Option<Value> {
    container(job)["env"]
        .as_array()
        .expect("a container has env")
        .iter()
        .find(|e| e["name"] == name)
        .cloned()
}

fn volume_named<'a>(job: &'a Value, name: &str) -> Option<&'a Value> {
    job["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .find(|v| v["name"] == name)
}

fn mount_paths(job: &Value) -> Vec<(String, String)> {
    container(job)["volumeMounts"]
        .as_array()
        .expect("volumeMounts")
        .iter()
        .map(|m| {
            (
                m["name"].as_str().unwrap_or_default().to_string(),
                m["mountPath"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The contract itself
// ---------------------------------------------------------------------------

/// A plaintext connection resolves to settings and NO references.
#[test]
fn a_plaintext_connection_names_no_credential_and_no_ca() {
    let resolved = resolve(&plaintext(), ConnectionUse::Probe).expect("it resolves");
    assert_eq!(resolved.contract_version, CONTRACT_VERSION);
    assert_eq!(resolved.namespace, NS);
    assert_eq!(resolved.cluster_name, CLUSTER);
    assert_eq!(resolved.auth, logweir_core::spec::AuthSpec::Plaintext);
    assert_eq!(resolved.password, None);
    assert_eq!(resolved.tls_ca, None);
    assert!(!resolved.tls());
    let projection = resolved.project();
    assert_eq!(projection, Default::default(), "nothing to project");
}

/// A `scramSha512` connection resolves to the username (identity) and a
/// `secretKeyRef` (reference), and the key defaults to what every earlier
/// release projected.
#[test]
fn a_scram_connection_names_its_principal_and_its_secret_key() {
    let resolved = resolve(&scram_tls(), ConnectionUse::Probe).expect("it resolves");
    assert_eq!(
        resolved.auth,
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".to_string(),
            tls: true
        }
    );
    let password = resolved
        .password
        .clone()
        .expect("a SCRAM password reference");
    assert_eq!(password.name, "orders-sasl");
    assert_eq!(
        password.key, DEFAULT_PASSWORD_KEY,
        "an object written before `passwordKey` existed projects the key it always did"
    );
    assert_eq!(
        password.key,
        restore::TARGET_PASSWORD_SECRET_KEY,
        "the default key and the constant the chart quotes are one value"
    );
    assert!(resolved.tls());

    // An explicit key is used verbatim.
    let explicit = resolve(
        &scram_tls_with_ca(json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } })),
        ConnectionUse::Probe,
    )
    .expect("it resolves");
    assert_eq!(explicit.password.expect("a reference").key, "sasl-password");
}

/// A private CA resolves from either source, and never from both or neither.
#[test]
fn a_private_ca_comes_from_exactly_one_same_namespace_object() {
    let from_config_map = resolve(
        &scram_tls_with_ca(json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } })),
        ConnectionUse::Probe,
    )
    .expect("it resolves");
    let ca = from_config_map.tls_ca.clone().expect("a CA reference");
    assert_eq!(ca.kind, CaSourceKind::ConfigMap);
    assert_eq!((ca.name.as_str(), ca.key.as_str()), ("kafka-ca", "ca.crt"));

    let from_secret = resolve(
        &scram_tls_with_ca(json!({ "secretKeyRef": { "name": "kafka-ca", "key": "bundle.pem" } })),
        ConnectionUse::Probe,
    )
    .expect("it resolves");
    let ca = from_secret.tls_ca.clone().expect("a CA reference");
    assert_eq!(ca.kind, CaSourceKind::Secret);
    assert_eq!(ca.key, "bundle.pem");

    for source in [
        json!({}),
        json!({
            "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" },
            "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" }
        }),
    ] {
        let refusal = resolve(&scram_tls_with_ca(source), ConnectionUse::Probe)
            .expect_err("neither or both is refused");
        assert_eq!(refusal.reason, TERMINAL_STATE_CONNECTION_CONFIG_INVALID);
        assert_eq!(refusal.field, "spec.auth.tlsCa");
    }
}

/// Conflicting configuration is refused, and the refusal names the field.
#[test]
fn conflicting_configuration_is_refused_with_the_field_that_conflicts() {
    let cases: Vec<(Value, &str, &str)> = vec![
        (
            json!({ "mode": "plaintext", "tls": true }),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.auth.tls",
        ),
        (
            json!({
                "mode": "scramSha512",
                "username": "logweir",
                "secretRef": { "name": "orders-sasl" },
                "tls": false,
                "tlsCa": { "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } }
            }),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.auth.tlsCa",
        ),
        (
            json!({
                "mode": "plaintext",
                "tls": false,
                "tlsCa": { "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } }
            }),
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.auth.tlsCa",
        ),
        (
            json!({ "mode": "scramSha512", "secretRef": { "name": "orders-sasl" }, "tls": true }),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
            "spec.auth.username",
        ),
        (
            json!({ "mode": "scramSha512", "username": "  ", "secretRef": { "name": "x" } }),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
            "spec.auth.username",
        ),
        (
            json!({ "mode": "scramSha512", "username": "logweir", "tls": true }),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
            "spec.auth.secretRef.name",
        ),
        (
            json!({ "mode": "scramSha512", "username": "logweir", "secretRef": { "name": " " } }),
            TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
            "spec.auth.secretRef.name",
        ),
        (
            json!({
                "mode": "scramSha512",
                "username": "logweir",
                "secretRef": { "name": "Orders_SASL" }
            }),
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "spec.auth.secretRef.name",
        ),
        (
            json!({
                "mode": "scramSha512",
                "username": "logweir",
                "secretRef": { "name": "orders-sasl", "passwordKey": "pass word" }
            }),
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "spec.auth.secretRef.passwordKey",
        ),
        (
            json!({
                "mode": "scramSha512",
                "username": "logweir",
                "secretRef": { "name": "orders-sasl" },
                "tls": true,
                "tlsCa": { "configMapKeyRef": { "name": "kafka-ca", "key": ".." } }
            }),
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "spec.auth.tlsCa.configMapKeyRef.key",
        ),
        (
            json!({
                "mode": "scramSha512",
                "username": "logweir",
                "secretRef": { "name": "orders-sasl" },
                "tls": true,
                "tlsCa": { "secretKeyRef": { "name": "KAFKA_CA", "key": "ca.crt" } }
            }),
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "spec.auth.tlsCa.secretKeyRef.name",
        ),
    ];
    for (auth, reason, field) in cases {
        let refusal = resolve(&cluster_with(auth.clone()), ConnectionUse::Probe)
            .expect_err("this shape is refused");
        assert_eq!(refusal.reason, reason, "for {auth}");
        assert_eq!(refusal.field, field, "for {auth}");
        assert!(
            TERMINAL_STATES.contains(&refusal.reason),
            "a refusal's reason must be a shared terminal state so a status can carry it"
        );
        // The message is actionable: it names the object and the field.
        assert!(
            refusal.message.contains(CLUSTER) && refusal.message.contains(NS),
            "the refusal names the object it is about: {refusal}"
        );
    }
}

/// An empty or unusable bootstrap list is refused, because the probe joins the
/// list with commas and a plan keeps it as a list.
#[test]
fn a_bootstrap_entry_two_paths_would_dial_differently_is_refused() {
    for servers in [
        json!([]),
        json!([""]),
        json!(["b0.orders:9093,b1.orders:9093"]),
        json!(["b0.orders:9093 b1.orders:9093"]),
    ] {
        let mut value: Value = serde_json::to_value(plaintext()).expect("the fixture serialises");
        value["spec"]["bootstrapServers"] = servers.clone();
        let cluster: KafkaCluster = serde_json::from_value(value).expect("still a KafkaCluster");
        let refusal = resolve(&cluster, ConnectionUse::Probe).expect_err("refused");
        assert_eq!(
            refusal.reason, TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "for {servers}"
        );
    }
}

/// A field this controller does not implement is REFUSED, not ignored — the
/// rollback rail (PLAT-07.1).
///
/// KILLS: drop `unrecognized_fields`, or resolve the connection without the
/// field a newer CRD accepted.
#[test]
fn an_unimplemented_connection_field_is_refused_and_named() {
    let cases: Vec<(&str, Value)> = vec![
        (
            "spec.network",
            json!({ "spec": { "network": { "egressGateway": "gw" } } }),
        ),
        (
            "spec.auth.mtls",
            json!({ "spec": { "auth": { "mtls": { "clientCertRef": { "name": "c" } } } } }),
        ),
        (
            "spec.auth.secretRef.namespace",
            json!({ "spec": { "auth": { "secretRef": { "namespace": "other" } } } }),
        ),
        (
            "spec.auth.tlsCa.configMapKeyRef.namespace",
            json!({ "spec": { "auth": { "tlsCa": { "configMapKeyRef": { "namespace": "other" } } } } }),
        ),
    ];
    for (path, patch) in cases {
        let mut value: Value = serde_json::to_value(scram_tls_with_ca(
            json!({ "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" } }),
        ))
        .expect("the fixture serialises");
        merge(&mut value, &patch);
        let cluster: KafkaCluster =
            serde_json::from_value(value).expect("an unknown field still parses");
        let refusal =
            resolve(&cluster, ConnectionUse::Probe).expect_err("an unimplemented field is refused");
        assert_eq!(refusal.reason, TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED);
        assert!(
            refusal.message.contains(path),
            "the refusal names the field: {refusal}"
        );
        assert!(
            refusal.message.contains(CONTRACT_VERSION),
            "…and the contract version this controller implements: {refusal}"
        );
    }
}

/// A CROSS-NAMESPACE reference cannot be written, and a connection cannot be
/// projected into a Job in another namespace.
#[test]
fn references_never_cross_a_namespace() {
    // The types carry no namespace field at all: a `namespace` key inside a
    // reference is an unimplemented field and is refused (above). What remains
    // is the Job side.
    let resolved = resolve(&scram_tls(), ConnectionUse::Probe).expect("it resolves");
    let refusal = resolved
        .check_job_namespace("another-namespace")
        .expect_err("a Job elsewhere would resolve a different Secret of the same name");
    assert_eq!(refusal.reason, TERMINAL_STATE_CONNECTION_REFERENCE_INVALID);
    resolved
        .check_job_namespace(NS)
        .expect("its own namespace is the only one");

    // And every Job the controllers build is in the connection's namespace.
    let (probe, backup_job, restore_job) = three_jobs(&scram_tls());
    for job in [&probe, &backup_job, &restore_job] {
        assert_eq!(job["metadata"]["namespace"], NS);
    }
}

/// ONE resolution reaches all three Jobs: same Secret, same key, same CA, same
/// file, differing only in the side the runner reads them under.
#[test]
fn probe_backup_and_restore_jobs_carry_one_connection() {
    let cluster = scram_tls_with_ca(json!({
        "configMapKeyRef": { "name": "kafka-ca", "key": "root.pem" }
    }));
    let (probe, backup_job, restore_job) = three_jobs(&cluster);

    for (job, side) in [
        (&probe, Side::Source),
        (&backup_job, Side::Source),
        (&restore_job, Side::Target),
    ] {
        let password = env_of(job, side.password_env()).expect("the password variable");
        assert!(
            password["value"].is_null(),
            "NEVER A LITERAL: {password} would be readable by anyone with Job read"
        );
        assert_eq!(password["valueFrom"]["secretKeyRef"]["name"], "orders-sasl");
        assert_eq!(
            password["valueFrom"]["secretKeyRef"]["key"], "sasl-password",
            "the connection's own key, on every path"
        );
        let ca_file = env_of(job, side.tls_ca_file_env()).expect("the CA file variable");
        assert_eq!(ca_file["value"], side.ca_file_path());
        let volume = volume_named(job, side.ca_volume()).expect("the CA volume");
        assert_eq!(volume["configMap"]["name"], "kafka-ca");
        assert_eq!(
            volume["configMap"]["items"],
            json!([{ "key": "root.pem", "path": CA_FILE_NAME }]),
            "one key, projected under one file name, so the path does not depend on the key"
        );
        assert!(
            mount_paths(job).contains(&(
                side.ca_volume().to_string(),
                side.ca_mount_path().to_string()
            )),
            "the CA volume is mounted where the variable says"
        );
        let mount = container(job)["volumeMounts"]
            .as_array()
            .expect("volumeMounts")
            .iter()
            .find(|m| m["name"] == side.ca_volume())
            .expect("the CA mount");
        assert_eq!(mount["readOnly"], json!(true));
    }

    // The probe and the backup are the same side, so their connection
    // contribution is byte-identical.
    for name in [Side::Source.password_env(), Side::Source.tls_ca_file_env()] {
        assert_eq!(
            env_of(&probe, name),
            env_of(&backup_job, name),
            "the probe and the backup dial one connection, so `{name}` is one entry"
        );
    }
    assert_eq!(
        volume_named(&probe, Side::Source.ca_volume()),
        volume_named(&backup_job, Side::Source.ca_volume())
    );

    // And the restore differs ONLY in the side prefix: same object, same key.
    let source_ca = volume_named(&probe, Side::Source.ca_volume()).expect("a CA volume");
    let target_ca = volume_named(&restore_job, Side::Target.ca_volume()).expect("a CA volume");
    assert_eq!(source_ca["configMap"], target_ca["configMap"]);
    assert_eq!(
        env_of(&probe, Side::Source.password_env()).expect("password")["valueFrom"],
        env_of(&restore_job, Side::Target.password_env()).expect("password")["valueFrom"],
    );
}

/// A Secret-backed CA is projected as a Secret volume; the file name and the
/// variable do not change with the source.
#[test]
fn a_secret_backed_ca_is_projected_as_a_secret_volume() {
    let cluster = scram_tls_with_ca(json!({
        "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" }
    }));
    let (probe, _, restore_job) = three_jobs(&cluster);
    let volume = volume_named(&probe, Side::Source.ca_volume()).expect("the CA volume");
    assert_eq!(volume["secret"]["secretName"], "kafka-ca");
    assert_eq!(
        volume["secret"]["defaultMode"],
        json!(job::SECRET_DEFAULT_MODE),
        "0440 with the pod's fsGroup — the mode every projected Secret in this crate uses"
    );
    assert_eq!(
        env_of(&probe, Side::Source.tls_ca_file_env()).expect("the CA file variable")["value"],
        Side::Source.ca_file_path()
    );
    assert_eq!(
        volume_named(&restore_job, Side::Target.ca_volume()).expect("the CA volume")["secret"]
            ["secretName"],
        "kafka-ca"
    );
}

/// TLS WITHOUT a private CA projects no volume and no variable: both clients
/// keep their default trust stores.
#[test]
fn tls_without_a_private_ca_projects_nothing_extra() {
    let (probe, backup_job, restore_job) = three_jobs(&scram_tls());
    for (job, side) in [
        (&probe, Side::Source),
        (&backup_job, Side::Source),
        (&restore_job, Side::Target),
    ] {
        assert_eq!(env_of(job, side.tls_ca_file_env()), None);
        assert_eq!(volume_named(job, side.ca_volume()), None);
    }
    // …and the plan still says TLS, which is what makes the runner dial SASL_SSL.
    let plan = backup::plan_backup_spec(&backup_object(), &scram_tls()).expect("the plan renders");
    assert_eq!(
        plan.source.auth,
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".to_string(),
            tls: true
        }
    );
}

/// ROTATION: a Job names a Secret, never a value, and carries nothing derived
/// from the value — so the NEXT Job uses whatever the Secret holds then.
///
/// KILLS: stamp a hash or a resourceVersion of the credential into the Job.
#[test]
fn a_job_references_the_secret_and_nothing_derived_from_its_value() {
    let cluster = scram_tls_with_ca(json!({
        "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" }
    }));
    let (first, _, _) = three_jobs(&cluster);
    let (second, _, _) = three_jobs(&cluster);
    assert_eq!(
        first, second,
        "the Job is a pure function of the KafkaCluster: rotating the Secret between two \
         reconciles changes nothing in the Job, which is exactly why the next pod picks the new \
         value up"
    );
    let password = env_of(&first, Side::Source.password_env()).expect("the password variable");
    assert!(password.get("value").is_none() || password["value"].is_null());
    assert!(
        password["valueFrom"]["secretKeyRef"]["optional"].is_null(),
        "not optional: a missing Secret must stop the pod, not start it unauthenticated"
    );
}

/// REDACTION: seeded values never reach a Job, a plan, a refusal or a
/// resolution — the controller never reads them, so there is nothing to leak.
#[test]
fn no_seeded_credential_value_reaches_a_job_a_plan_or_a_refusal() {
    let cluster = scram_tls_with_ca(json!({
        "configMapKeyRef": { "name": "kafka-ca", "key": "ca.crt" }
    }));
    let (probe, backup_job, restore_job) = three_jobs(&cluster);
    let plan = backup::plan_config_map(&backup_object(), &cluster).expect("the plan ConfigMap");
    let resolved = resolve(&cluster, ConnectionUse::Probe).expect("it resolves");
    let rendered = format!(
        "{probe}{backup_job}{restore_job}{}{}{:?}",
        serde_json::to_string(&plan).expect("a ConfigMap serialises"),
        serde_json::to_string(&resolved).expect("a resolution serialises"),
        resolved
    );
    for seeded in [SEEDED_PASSWORD, "-----BEGIN CERTIFICATE-----"] {
        assert!(
            !rendered.contains(seeded),
            "a credential or certificate VALUE reached a rendered object; the controller holds \
             no read on Secrets and must render references only"
        );
    }

    // A refusal quotes field names and object names, never values — including
    // the value of an unimplemented field, which is adopter text.
    let mut value: Value = serde_json::to_value(scram_tls()).expect("serialises");
    value["spec"]["auth"]["inlinePassword"] = json!(SEEDED_PASSWORD);
    let cluster: KafkaCluster = serde_json::from_value(value).expect("still parses");
    let refusal =
        resolve(&cluster, ConnectionUse::Probe).expect_err("an unimplemented field is refused");
    assert!(
        refusal.message.contains("spec.auth.inlinePassword")
            && !refusal.message.contains(SEEDED_PASSWORD),
        "the refusal names the KEY and never the VALUE: {refusal}"
    );
}

/// The plan a backup renders carries identity and never a credential.
#[test]
fn the_rendered_plan_carries_the_connection_and_no_reference_to_a_credential() {
    let cluster = scram_tls_with_ca(json!({
        "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" }
    }));
    let plan = backup::plan_backup_spec(&backup_object(), &cluster).expect("the plan renders");
    assert_eq!(
        plan.source.bootstrap_servers,
        cluster.spec.bootstrap_servers
    );
    assert_eq!(
        plan.source.auth,
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".to_string(),
            tls: true
        }
    );
    let text = serde_yaml::to_string(&plan).expect("the plan serialises");
    for absent in ["orders-sasl", "sasl-password", "kafka-ca", "password"] {
        assert!(
            !text.contains(absent),
            "the plan document names no credential reference at all ({absent}); the CA path and \
             the password variable reach the runner through the Job's environment: {text}"
        );
    }
}

/// The restore's approved plan must name the same target as the saved
/// connection; the resolver is where the two are compared.
#[test]
fn a_restore_plan_must_agree_with_the_saved_connection() {
    let cluster = scram_tls();
    let resolved = resolve(&cluster, ConnectionUse::Probe).expect("it resolves");
    let plan = plan_for(&cluster);
    resolved
        .check_restore_plan(&plan)
        .expect("a plan built from the connection agrees");

    // The same servers in another order are the same cluster.
    let reordered = plan.replace(
        "[b0.orders:9093, b1.orders:9093]",
        "[b1.orders:9093, b0.orders:9093]",
    );
    assert_ne!(reordered, plan, "the fixture really was reordered");
    resolved
        .check_restore_plan(&reordered)
        .expect("order is not a different cluster");

    for (bad, what) in [
        (plan.replace("b1.orders:9093", "elsewhere:9093"), "address"),
        (
            plan.replace("username: logweir", "username: someone-else"),
            "principal",
        ),
        (plan.replace("tls: true", "tls: false"), "transport"),
        (String::from("not: a plan\n"), "unreadable target"),
    ] {
        let refusal = resolved
            .check_restore_plan(&bad)
            .expect_err("a plan that names another target is refused");
        assert_eq!(
            refusal.reason, TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
            "for a changed {what}"
        );
    }
}

/// OLD OBJECTS BUILD THE JOBS THEY ALWAYS DID — against outputs captured from
/// the controller as it was BEFORE this contract (main 4956785).
///
/// The two deliberate exceptions are named here and nowhere else: a
/// `scramSha512` connection with no `secretRef` and a `plaintext` connection
/// with `tls: true` are now refused rather than run, and the goldens record
/// exactly what they used to do.
#[test]
fn legacy_objects_build_the_jobs_they_always_did() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/connection");
    let mut checked = 0usize;
    let mut refused = 0usize;
    let mut entries: Vec<_> = std::fs::read_dir(dir.join("legacy"))
        .expect("the legacy fixtures ship")
        .map(|e| e.expect("a readable entry").path())
        .collect();
    entries.sort();
    for path in entries {
        let name = path.file_name().expect("a file name").to_owned();
        let cluster: KafkaCluster =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("readable"))
                .expect("the fixture is a KafkaCluster");
        let golden: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join("legacy-golden").join(&name)).expect("a golden"),
        )
        .expect("the golden is JSON");

        // The two refused shapes, named by fixture.
        let file = name.to_string_lossy().to_string();
        if file == "scram-without-secret-ref.json" || file == "plaintext-tls.json" {
            let refusal =
                resolve(&cluster, ConnectionUse::Probe).expect_err("this shape is refused now");
            assert!(
                TERMINAL_STATES.contains(&refusal.reason),
                "{file}: {refusal}"
            );
            refused += 1;
            continue;
        }

        let probe = serde_json::to_value(job::build(
            &kafka_cluster::runner_job_spec(&cluster).expect("probe spec"),
        ))
        .expect("serialises");
        assert_eq!(
            redact_image(probe),
            golden["probe"]["job"],
            "{file}: the probe Job differs from what the pre-PLAT-07 controller built"
        );

        let backup_spec =
            backup::runner_job_spec(&backup_for_legacy(&cluster), &cluster).expect("backup spec");
        let mut built_job =
            redact_image(serde_json::to_value(job::build(&backup_spec)).expect("serialises"));
        assert_eq!(
            built_job["spec"]["template"]["spec"]["containers"][0]["args"],
            json!(backup_execution::runner_argv(
                ExecutionTrigger::Manual,
                BACKUP_UID
            )),
            "{file}: PLAT-06.1 DERIVES the argv from the run identity instead of executing the \
             `logweir.dev/runner-argv` annotation, so a legacy object's Job now carries \
             `--backup-id-override <the object UID>` where the annotation carried none"
        );
        built_job["spec"]["template"]["spec"]["containers"][0]["args"] = golden["backup"]["job"]
            ["job"]["spec"]["template"]["spec"]["containers"][0]["args"]
            .clone();
        assert_eq!(
            built_job, golden["backup"]["job"]["job"],
            "{file}: the backup Job differs from what the pre-PLAT-07 controller built in \
             something OTHER than the derived argv — the connection contract changes no \
             environment, mount, volume, ServiceAccount or security setting for an object that \
             names neither passwordKey nor tlsCa"
        );

        let plan = backup::plan_config_map(&backup_for_legacy(&cluster), &cluster)
            .expect("the plan ConfigMap");
        let mut built_plan = serde_json::to_value(plan).expect("serialises");
        // PLAT-06.1's THREE named additions to the plan ConfigMap, asserted and
        // then removed so everything else is compared byte for byte against the
        // pre-PLAT-07 object. Removing them is not weakening the row: an
        // addition this list does not name stays in `built_plan` and fails the
        // equality below.
        assert_eq!(
            built_plan["immutable"].take(),
            json!(true),
            "{file}: PLAT-06.1 freezes the plan"
        );
        let snapshot = built_plan["data"][backup_execution::INPUTS_KEY].take();
        let snapshot_text = snapshot.as_str().expect("the snapshot is a string");
        let annotations = built_plan["metadata"]["annotations"].take();
        assert_eq!(
            annotations[backup_execution::INPUTS_SHA256_ANNOTATION],
            json!(logweir_core::ids::sha256_prefixed(snapshot_text.as_bytes())),
            "{file}: PLAT-06.1 annotates the snapshot digest"
        );
        assert_eq!(
            annotations[backup_execution::EXECUTION_ID_ANNOTATION],
            json!(BACKUP_UID),
            "{file}: PLAT-06.1 annotates the run identity"
        );
        // THE SNAPSHOT RECORDS WHAT THE RESOLVER DECIDED, which is how a later
        // pass detects a changed connection (PLAT-07.1 × PLAT-06.1).
        let snapshot: Value = serde_json::from_str(snapshot_text).expect("the snapshot is JSON");
        let resolved = resolve(&cluster, ConnectionUse::BackupSource).expect("it resolves");
        assert_eq!(
            snapshot["source"]["bootstrapServers"],
            json!(resolved.bootstrap_servers)
        );
        assert_eq!(snapshot["source"]["auth"], json!(resolved.auth));
        assert_eq!(
            snapshot["source"].get("tlsCa").cloned(),
            resolved.tls_ca.as_ref().map(|ca| json!(ca)),
            "{file}: the frozen snapshot carries the CA reference the resolver decided, and \
             carries no `tlsCa` key at all when the connection names none"
        );
        // Remove the now-absent key so the `data` map compares as it was.
        built_plan["data"]
            .as_object_mut()
            .expect("data is a map")
            .remove(backup_execution::INPUTS_KEY);
        built_plan["metadata"]
            .as_object_mut()
            .expect("metadata is a map")
            .remove("annotations");
        built_plan
            .as_object_mut()
            .expect("the ConfigMap is a map")
            .remove("immutable");
        assert_eq!(
            built_plan, golden["backup"]["plan"]["configMap"],
            "{file}: the rendered plan differs from the pre-PLAT-07 one in something OTHER than \
             PLAT-06.1's snapshot key, immutability and annotations — in particular \
             `backup.yaml` and `allowed-clusters.json` are byte-identical"
        );

        let plan_bytes = golden["restore"]["planBytes"]
            .as_str()
            .expect("the golden carries the plan it used")
            .to_string();
        let restore_spec = restore::runner_job_spec(
            &legacy_restore(&cluster, &plan_bytes),
            &cluster,
            &[KEY_ID.to_string()],
            &approval_for(&plan_bytes),
            &roster(),
        )
        .expect("restore spec");
        assert_eq!(
            redact_image(serde_json::to_value(job::build(&restore_spec)).expect("serialises")),
            golden["restore"]["job"]["job"],
            "{file}: the restore Job differs"
        );
        checked += 1;
    }
    assert_eq!(
        (checked, refused),
        (5, 2),
        "five legacy shapes must be byte-identical and two are the documented refusals"
    );
}

/// The legacy goldens were captured with a `Backup` of this exact shape.
fn backup_for_legacy(cluster: &KafkaCluster) -> Backup {
    let mut value: Value = serde_json::to_value(backup_object()).expect("serialises");
    value["metadata"]["name"] = json!("legacy-backup");
    value["spec"]["sourceRef"]["name"] = json!(cluster.metadata.name.clone().expect("a name"));
    serde_json::from_value(value).expect("still a Backup")
}

/// …and a `Restore` of this one.
fn legacy_restore(cluster: &KafkaCluster, plan: &str) -> Restore {
    let mut value: Value = serde_json::to_value(restore_object(plan)).expect("serialises");
    value["metadata"]["name"] = json!("legacy-restore");
    value["spec"]["target"]["clusterRef"]["name"] =
        json!(cluster.metadata.name.clone().expect("a name"));
    serde_json::from_value(value).expect("still a Restore")
}

/// The goldens carry `<runner-image>` where the pinned digest reference is,
/// because that reference may be named exactly once under `crates/`
/// (`crd_shape::the_runner_image_is_named_once`).
fn redact_image(mut job: Value) -> Value {
    job["spec"]["template"]["spec"]["containers"][0]["image"] = json!("<runner-image>");
    job
}

/// The legacy goldens' own provenance, asserted so a future editor cannot
/// silently "refresh" them from the build they are supposed to constrain.
#[test]
fn the_legacy_goldens_say_where_they_came_from() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/connection/legacy-golden");
    for entry in std::fs::read_dir(&dir).expect("the goldens ship") {
        let path = entry.expect("a readable entry").path();
        let golden: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("readable")).expect("JSON");
        assert_eq!(
            golden["capturedFrom"],
            "main 4956785 (pre-PLAT-07)",
            "{}: a golden that was regenerated from the CURRENT build proves nothing about \
             compatibility",
            path.display()
        );
    }
}

/// `spec.role` and `markerTopic` are NOT connection settings: the resolver
/// ignores them, and the probe still passes the marker topic through.
#[test]
fn the_resolver_reads_the_connection_and_not_the_role() {
    let mut value: Value = serde_json::to_value(scram_tls()).expect("serialises");
    value["spec"]["role"] = json!("target");
    let as_target: KafkaCluster = serde_json::from_value(value).expect("still a cluster");
    assert_eq!(
        resolve(&as_target, ConnectionUse::Probe).expect("resolves"),
        resolve(&scram_tls(), ConnectionUse::Probe).expect("resolves"),
        "a role is a label the controller reports; it decides nothing about the connection"
    );
    let argv = kafka_cluster::runner_argv(
        &as_target,
        &resolve(&as_target, ConnectionUse::Probe).expect("resolves"),
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--marker-topic" && w[1] == "logweir.scratch"),
        "the marker topic is still passed through: {argv:?}"
    );
}

/// Merge `patch` into `value`, recursively — the tests' own tiny helper.
fn merge(value: &mut Value, patch: &Value) {
    match (value, patch) {
        (Value::Object(target), Value::Object(source)) => {
            for (k, v) in source {
                merge(target.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (target, source) => *target = source.clone(),
    }
}

// ---------------------------------------------------------------------------
// The write-only credential entry
// ---------------------------------------------------------------------------

/// The Secret a product API creates: one key, the Logweir labels, the custom
/// type, and a reference that needs no `passwordKey`.
#[test]
fn a_credential_secret_is_create_only_one_key_and_labelled() {
    let built = credential::build_kafka_credential_secret(credential::NewKafkaCredential {
        namespace: NS,
        secret_name: "orders-sasl",
        connection_name: CLUSTER,
        password: credential::WriteOnlyPassword::new(SEEDED_PASSWORD.to_string()),
        request_id: Some("01JB7Z0000000000000000000A"),
    })
    .expect("a well-formed entry builds");
    assert_eq!(
        built.secret_ref().name,
        "orders-sasl",
        "what the API returns is a NAME"
    );
    assert_eq!(
        built.secret_ref().password_key,
        None,
        "the fixed key is the resolver's default, so the KafkaCluster needs no passwordKey"
    );
    let secret = built.into_secret();
    assert_eq!(secret.metadata.namespace.as_deref(), Some(NS));
    assert_eq!(
        secret.type_.as_deref(),
        Some(credential::CREDENTIAL_SECRET_TYPE),
        "a distinct type is what a narrow admission rule can require, so `create` on Secrets \
         cannot be spent on a ServiceAccount token"
    );
    assert_eq!(
        secret.immutable, None,
        "rotation updates the value in place"
    );
    let data = secret.data.expect("data");
    assert_eq!(
        data.keys().collect::<Vec<_>>(),
        vec![credential::PASSWORD_DATA_KEY],
        "one key, and no username: identity is `auth.username`, which plan hashes bind"
    );
    assert_eq!(
        data[credential::PASSWORD_DATA_KEY].0,
        SEEDED_PASSWORD.as_bytes()
    );
    let labels = secret.metadata.labels.expect("labels");
    assert_eq!(
        labels.get(credential::CREDENTIAL_LABEL).map(String::as_str),
        Some(credential::CREDENTIAL_LABEL_VALUE)
    );
    assert_eq!(
        labels.get(credential::CONNECTION_LABEL).map(String::as_str),
        Some(CLUSTER)
    );
    let annotations = secret.metadata.annotations.expect("annotations");
    assert_eq!(
        annotations
            .get(credential::CONTRACT_ANNOTATION)
            .map(String::as_str),
        Some(credential::CREDENTIAL_CONTRACT_VERSION)
    );
    assert_eq!(
        annotations
            .get(credential::WRITE_ONLY_ANNOTATION)
            .map(String::as_str),
        Some("true")
    );
}

/// The entered value never reaches a `Debug`, a `Display` or an error.
#[test]
fn a_credential_is_redacted_in_every_rendering_but_the_secret_itself() {
    let password = credential::WriteOnlyPassword::new(SEEDED_PASSWORD.to_string());
    assert_eq!(format!("{password:?}"), "WriteOnlyPassword(<redacted>)");
    let request = credential::NewKafkaCredential {
        namespace: NS,
        secret_name: "orders-sasl",
        connection_name: CLUSTER,
        password,
        request_id: None,
    };
    let debugged = format!("{request:?}");
    assert!(
        !debugged.contains(SEEDED_PASSWORD),
        "the request's Debug carries no value: {debugged}"
    );
    let built = credential::build_kafka_credential_secret(request).expect("builds");
    let debugged = format!("{built:?}");
    assert!(
        !debugged.contains(SEEDED_PASSWORD) && debugged.contains("data_keys"),
        "the built Secret's Debug shows the KEYS and never the bytes: {debugged}"
    );

    // Every refusal, over a value that would be recognisable if it leaked.
    for (password, expected) in [
        (
            String::new(),
            credential::CredentialInputError::EmptyPassword,
        ),
        (
            format!("{SEEDED_PASSWORD} "),
            credential::CredentialInputError::PasswordEdgeWhitespace,
        ),
        (
            format!("{SEEDED_PASSWORD}$"),
            credential::CredentialInputError::PasswordCharacter("a dollar sign"),
        ),
        (
            format!("{SEEDED_PASSWORD}\n"),
            credential::CredentialInputError::PasswordEdgeWhitespace,
        ),
        (
            format!("{SEEDED_PASSWORD}\u{7}x"),
            credential::CredentialInputError::PasswordCharacter("a control character"),
        ),
        (
            "x".repeat(credential::MAX_PASSWORD_BYTES + 1),
            credential::CredentialInputError::PasswordTooLong,
        ),
    ] {
        let error = credential::build_kafka_credential_secret(credential::NewKafkaCredential {
            namespace: NS,
            secret_name: "orders-sasl",
            connection_name: CLUSTER,
            password: credential::WriteOnlyPassword::new(password),
            request_id: None,
        })
        .expect_err("refused");
        assert_eq!(error, expected);
        let text = error.to_string();
        assert!(
            !text.contains(SEEDED_PASSWORD),
            "a refusal names a class, never the value: {text}"
        );
    }
}

/// Malformed names are refused before the password is looked at.
#[test]
fn a_credential_entry_refuses_names_the_kubelet_could_never_resolve() {
    for (namespace, secret, connection, expected) in [
        (
            "Not A Namespace",
            "orders-sasl",
            CLUSTER,
            credential::CredentialInputError::InvalidNamespace,
        ),
        (
            NS,
            "Orders_SASL",
            CLUSTER,
            credential::CredentialInputError::InvalidSecretName,
        ),
        (
            NS,
            "orders-sasl",
            "a-name-that-is-far-too-long-to-be-a-label-value-and-so-cannot-identify-a-connection",
            credential::CredentialInputError::InvalidConnectionName,
        ),
    ] {
        let error = credential::build_kafka_credential_secret(credential::NewKafkaCredential {
            namespace,
            secret_name: secret,
            connection_name: connection,
            password: credential::WriteOnlyPassword::new(SEEDED_PASSWORD.to_string()),
            request_id: None,
        })
        .expect_err("refused");
        assert_eq!(error, expected);
    }
}

/// A create response is kept as METADATA and nothing else — the echoed `data`
/// is dropped where it arrives.
#[test]
fn a_create_response_is_kept_as_metadata_only() {
    let built = credential::build_kafka_credential_secret(credential::NewKafkaCredential {
        namespace: NS,
        secret_name: "orders-sasl",
        connection_name: CLUSTER,
        password: credential::WriteOnlyPassword::new(SEEDED_PASSWORD.to_string()),
        request_id: None,
    })
    .expect("builds");
    let mut echoed = built.into_secret();
    echoed.metadata.uid = Some("11111111-0000-4000-8000-00000000000f".to_string());
    echoed.metadata.resource_version = Some("4242".to_string());
    let kept = credential::CreatedCredential::from_response(echoed);
    assert_eq!(kept.name, "orders-sasl");
    assert_eq!(kept.namespace, NS);
    assert_eq!(kept.resource_version.as_deref(), Some("4242"));
    let debugged = format!("{kept:?}");
    assert!(
        !debugged.contains(SEEDED_PASSWORD),
        "what survives a create response carries no value: {debugged}"
    );
}

/// THE CONTROLLER NEVER CALLS THE CREDENTIAL BUILDER. It is a library for the
/// product API; the control plane names Secrets and reads none.
///
/// KILLS: call `build_kafka_credential_secret` from a reconciler.
#[test]
fn no_controller_builds_a_credential() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut seen = 0usize;
    walk(&src, &mut |path, text| {
        let is_credential_module = path.ends_with("connection/credential.rs");
        if is_credential_module {
            return;
        }
        seen += 1;
        for needle in [
            "build_kafka_credential_secret",
            "WriteOnlyPassword",
            "NewKafkaCredential",
        ] {
            if text.contains(needle) {
                offenders.push(format!("{}: {needle}", path.display()));
            }
        }
    });
    assert!(seen >= 5, "the walk must see src/, got {seen} files");
    assert!(
        offenders.is_empty(),
        "the credential entry is a library the product API calls; nothing in the control plane \
         may build one: {offenders:?}"
    );
}

fn walk(dir: &std::path::Path, f: &mut impl FnMut(&std::path::Path, &str)) {
    for entry in std::fs::read_dir(dir).expect("a readable directory") {
        let path = entry.expect("a readable entry").path();
        if path.is_dir() {
            walk(&path, f);
        } else if path.extension().is_some_and(|x| x == "rs") {
            let text = std::fs::read_to_string(&path).expect("a readable source file");
            f(&path, &text);
        }
    }
}

/// The resolution a run can freeze carries references and settings only, and
/// round-trips: PLAT-06.1 freezes it, PLAT-17.1 returns it.
#[test]
fn a_resolution_serialises_as_references_and_settings() {
    let resolved = resolve(
        &scram_tls_with_ca(json!({ "secretKeyRef": { "name": "kafka-ca", "key": "ca.crt" } })),
        ConnectionUse::RestoreTarget,
    )
    .expect("resolves");
    let text = serde_json::to_string(&resolved).expect("serialises");
    let back: ResolvedConnection = serde_json::from_str(&text).expect("round-trips");
    assert_eq!(back, resolved);
    let value: Value = serde_json::from_str(&text).expect("JSON");
    let keys: BTreeSet<String> = value
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        keys,
        [
            "auth",
            "bootstrapServers",
            "bootstrapSha256",
            "clusterName",
            "connectionUse",
            "contractVersion",
            "execution",
            "generation",
            "namespace",
            "password",
            "principal",
            "tlsCa",
            "uid"
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
    );
    assert_eq!(value["contractVersion"], CONTRACT_VERSION);
    assert_eq!(
        value["password"],
        json!({ "name": "orders-sasl", "key": "sasl-password" })
    );
    assert_eq!(
        value["tlsCa"],
        json!({ "kind": "secret", "name": "kafka-ca", "key": "ca.crt" })
    );
    // THE WHOLE POINT OF THE KEY SET ABOVE. Every added key is a SETTING, a
    // REFERENCE or an IDENTITY, and the serialised document is what PLAT-06.1
    // freezes and PLAT-17.1 returns: a password or a certificate appearing
    // here would be a credential in a run snapshot and an API response.
    assert_eq!(value["connectionUse"], "restoreTarget");
    assert_eq!(value["principal"], "User:logweir");
    assert_eq!(value["uid"], CLUSTER_UID);
    assert_eq!(
        value["execution"],
        json!({
            "namespace": NS,
            "serviceAccountName": "logweir-runner",
            "side": "target",
            "automountServiceAccountToken": false
        })
    );
    assert!(
        value["bootstrapSha256"]
            .as_str()
            .expect("a digest")
            .starts_with("sha256:"),
        "the bootstrap digest is a prefixed hex digest"
    );
}

/// The digest is of the address SET, which is the rule
/// `check_restore_plan` compares by: reordering, repeating or padding the same
/// addresses is not a different cluster, and a consumer that keyed a cache or
/// a check result on the list order would re-check on every harmless edit.
#[test]
fn the_bootstrap_digest_is_the_address_set_and_not_its_spelling() {
    let one = resolve(&scram_tls(), ConnectionUse::Probe).expect("resolves");
    let mut reordered = scram_tls();
    let servers = one.bootstrap_servers.clone();
    assert!(servers.len() > 1, "the fixture needs two addresses");
    reordered.spec.bootstrap_servers = servers.iter().rev().cloned().collect();
    reordered.spec.bootstrap_servers.push(servers[0].clone());
    let two = resolve(&reordered, ConnectionUse::BackupSource).expect("resolves");
    assert_eq!(one.bootstrap_sha256, two.bootstrap_sha256);
    assert_ne!(
        one.bootstrap_servers, two.bootstrap_servers,
        "the list itself is still carried verbatim and in order"
    );

    // A DIFFERENT ADDRESS IS A DIFFERENT DIGEST — the negative control that
    // makes the equality above mean something.
    let mut elsewhere = scram_tls();
    elsewhere.spec.bootstrap_servers = vec!["kafka.elsewhere.svc:9093".to_string()];
    let three = resolve(&elsewhere, ConnectionUse::Probe).expect("resolves");
    assert_ne!(one.bootstrap_sha256, three.bootstrap_sha256);
}

/// The use decides the SIDE and the execution context, and nothing else: a
/// discovery or preflight check must refuse exactly what the run refuses, or
/// it is a green light for a run that cannot happen.
#[test]
fn the_use_chooses_the_side_and_never_the_answer() {
    for connection_use in ConnectionUse::ALL {
        let resolved = resolve(&scram_tls(), connection_use).expect("resolves");
        assert_eq!(resolved.connection_use, connection_use);
        assert_eq!(resolved.execution.side, connection_use.side());
        assert_eq!(
            resolved.execution.service_account_name,
            connection_use.service_account_name()
        );
        assert_eq!(resolved.execution.namespace, resolved.namespace);
        assert!(!resolved.execution.automount_service_account_token);

        // The projection follows the side, with no second argument to get
        // wrong.
        let projection = resolved.project();
        assert_eq!(
            projection.env_from_secret[0].name,
            connection_use.side().password_env()
        );

        // Same settings and same references, whatever the use.
        let probe = resolve(&scram_tls(), ConnectionUse::Probe).expect("resolves");
        assert_eq!(resolved.auth, probe.auth);
        assert_eq!(resolved.password, probe.password);
        assert_eq!(resolved.tls_ca, probe.tls_ca);
        assert_eq!(resolved.bootstrap_sha256, probe.bootstrap_sha256);
        assert_eq!(resolved.principal, probe.principal);

        // And the same refusal, field for field.
        let bad = cluster_with(json!({ "mode": "scramSha512", "tls": true }));
        assert_eq!(
            resolve(&bad, connection_use).expect_err("refused"),
            resolve(&bad, ConnectionUse::Probe).expect_err("refused"),
            "{} must refuse exactly what a probe refuses",
            connection_use.as_str()
        );
    }

    // Every use spells itself once, so a status or a log line is unambiguous.
    let spellings: BTreeSet<&str> = ConnectionUse::ALL.iter().map(|u| u.as_str()).collect();
    assert_eq!(spellings.len(), ConnectionUse::ALL.len());
}

/// An unauthenticated connection still names a principal, because an ACL on a
/// plaintext listener is written for one.
#[test]
fn a_plaintext_connection_authenticates_as_anonymous() {
    let resolved = resolve(&plaintext(), ConnectionUse::Discovery).expect("resolves");
    assert_eq!(resolved.principal, "User:ANONYMOUS");
    assert_eq!(
        resolve(&scram_tls(), ConnectionUse::Discovery)
            .expect("resolves")
            .principal,
        "User:logweir"
    );
}

/// A refusal is `Display`-able as `<TerminalState>: <message>`, which is what
/// the three controllers put on a status.
#[test]
fn a_refusal_displays_as_its_terminal_state_and_message() {
    let refusal: ConnectionRefusal = resolve(
        &cluster_with(json!({ "mode": "plaintext", "tls": true })),
        ConnectionUse::Probe,
    )
    .expect_err("refused");
    let text = refusal.to_string();
    assert!(text.starts_with(TERMINAL_STATE_CONNECTION_CONFIG_INVALID));
    assert!(text.contains(&refusal.message));
}
