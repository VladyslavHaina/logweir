//! The chart lint: `charts/logweir` held to the tree it is derived from, by
//! reading checked-in bytes. Task 35 (post-plan).
//!
//! # What is asserted here, and what is asserted in `scripts/check-chart.sh`
//!
//! Global Constraint 22: every test here parses files under the working tree,
//! shells nothing and dials nothing. The gate SCRIPT is the half that needs
//! `helm` — `helm lint`, `helm template` into `charts/logweir/rendered/` with
//! the drift check, and the schema refusal of `--set demoKafka.enabled=yes` —
//! and these tests read what the script rendered: the checked-in
//! `rendered/*.yaml`. A test that ran `helm` from a `#[test]` would be a shell
//! gate in the unit suite, which is the thing GC22 forbids; a rendered file
//! that is not current fails the script's porcelain arm, so what these tests
//! read is what the templates say.
//!
//! # The properties
//!
//! * The chart's `crds/` and `ui/` are byte-identical copies of `config/crd/`
//!   and the fourteen shipped UI files — asserted here with `std::fs` and in
//!   the script with `cmp`, so the property holds whichever runs first.
//! * The DEFAULT render and `logweir.yaml` agree on the substance of the
//!   control plane: the Deployment's image, pull policy, args, every env name
//!   and value, both security contexts and the ServiceAccount; the same four
//!   ClusterRoles with the same rules as sets; the same six CRD names. The one
//!   permitted difference is `LOGWEIR_RUNNER_IMAGE` — Task 33's override, which
//!   `logweir.yaml` does not set — whose default must be the compiled-in
//!   constant, read from `crates/weirkeeper/src/job.rs` and never spelt here
//!   (`crd_shape.rs::the_runner_image_is_named_once` counts that string under
//!   `crates/`). There is no namespace-derived env in the shipped Deployment,
//!   so nothing else is excepted.
//! * `demoKafka`, `minio` and `ui` render NOTHING under the defaults and under
//!   the minimal example, and render their named objects under the demo
//!   example: two StatefulSets and a seed Job whose command names the marker
//!   topic; the MinIO Deployment, its seed Job with both bucket names, and the
//!   `logweir-s3` Secret; the UI Deployment with `--accept-paths` present and
//!   measured, the ConfigMap holding exactly the fourteen UI files' bytes and
//!   no key material, the ServiceAccount, the RoleBinding to the chart's own
//!   role and never `cluster-admin`, the Service — and no Ingress anywhere.
//! * `values.schema.json` types the three flags as booleans.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_yaml::Value;

// ---------------------------------------------------------------- plumbing

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two levels above crates/logweir")
}

fn read(rel: &str) -> String {
    let p = repo().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("could not read {}: {e}", p.display()))
}

fn read_bytes(rel: &str) -> Vec<u8> {
    let p = repo().join(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("could not read {}: {e}", p.display()))
}

/// Every regular file under `rel`, as paths relative to the repository root,
/// sorted.
fn files_under(rel: &str) -> Vec<String> {
    let root = repo().join(rel);
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("could not list {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(
                    path.strip_prefix(repo())
                        .expect("under the repository")
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }
    out.sort();
    out
}

/// One parsed document carrying `apiVersion` and `kind`.
#[derive(Debug, Clone)]
struct Doc {
    kind: String,
    value: Value,
}

impl Doc {
    fn name(&self) -> String {
        self.value["metadata"]["name"]
            .as_str()
            .unwrap_or("<unnamed>")
            .to_string()
    }
}

/// Every manifest in a multi-document YAML file, selected by the parse
/// (`manifest_lint.rs`'s rule): a document is a manifest when it carries both
/// `apiVersion` and `kind`.
fn docs_in(rel: &str) -> Vec<Doc> {
    let text = read(rel);
    let mut out = Vec::new();
    for de in serde_yaml::Deserializer::from_str(&text) {
        let Ok(value) = Value::deserialize(de) else {
            continue;
        };
        let (Some(_), Some(kind)) = (
            value.get("apiVersion").and_then(Value::as_str),
            value.get("kind").and_then(Value::as_str),
        ) else {
            continue;
        };
        out.push(Doc {
            kind: kind.to_string(),
            value: value.clone(),
        });
    }
    assert!(
        !out.is_empty(),
        "{rel} carries no manifest at all — is it current? `just chart-check` regenerates it"
    );
    out
}

fn rendered(name: &str) -> Vec<Doc> {
    docs_in(&format!("charts/logweir/rendered/{name}.yaml"))
}

fn find<'a>(docs: &'a [Doc], kind: &str, name: &str) -> &'a Doc {
    docs.iter()
        .find(|d| d.kind == kind && d.name() == name)
        .unwrap_or_else(|| {
            let have: Vec<String> = docs
                .iter()
                .map(|d| format!("{}/{}", d.kind, d.name()))
                .collect();
            panic!("no {kind}/{name} among {have:?}")
        })
}

fn names_of(docs: &[Doc], kind: &str) -> BTreeSet<String> {
    docs.iter()
        .filter(|d| d.kind == kind)
        .map(Doc::name)
        .collect()
}

/// The one container of a Deployment/StatefulSet/Job.
fn container(doc: &Doc) -> &Value {
    let containers = doc.value["spec"]["template"]["spec"]["containers"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{}/{} has no containers", doc.kind, doc.name()));
    assert_eq!(
        1,
        containers.len(),
        "{}/{} must carry exactly one container",
        doc.kind,
        doc.name()
    );
    &containers[0]
}

fn pod_spec(doc: &Doc) -> &Value {
    &doc.value["spec"]["template"]["spec"]
}

/// `env` as a map from name to the whole entry minus its name, so a `value`
/// and a `valueFrom` compare the same way.
fn env_of(container: &Value) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for e in container["env"].as_sequence().expect("an env sequence") {
        let name = e["name"].as_str().expect("an env name").to_string();
        let mut rest = e.clone();
        if let Some(m) = rest.as_mapping_mut() {
            m.remove(Value::String("name".into()));
        }
        assert!(
            out.insert(name.clone(), rest).is_none(),
            "env {name} appears twice"
        );
    }
    out
}

/// A ClusterRole's rules as a SET of `(apiGroups, resources, verbs)` triples,
/// each list sorted — the comparison is about content, never order.
fn rules_of(doc: &Doc) -> BTreeSet<(Vec<String>, Vec<String>, Vec<String>)> {
    let take = |r: &Value, k: &str| -> Vec<String> {
        let mut v: Vec<String> = r[k]
            .as_sequence()
            .unwrap_or_else(|| panic!("{}: a rule with no `{k}`", doc.name()))
            .iter()
            .map(|s| s.as_str().expect("a string").to_string())
            .collect();
        v.sort();
        v
    };
    doc.value["rules"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{} has no rules", doc.name()))
        .iter()
        .map(|r| {
            assert!(
                r.get("resourceNames").is_none(),
                "{}: a rule carries resourceNames, which this comparison does not model",
                doc.name()
            );
            (take(r, "apiGroups"), take(r, "resources"), take(r, "verbs"))
        })
        .collect()
}

/// `weirkeeper::job::RUNNER_IMAGE`, read out of its source file so this file
/// never spells the runner image — `crd_shape.rs::the_runner_image_is_named_once`
/// counts occurrences of that string under `crates/`.
fn runner_image_constant() -> String {
    let src = read("crates/weirkeeper/src/job.rs");
    let marker = "pub const RUNNER_IMAGE: &str =";
    let at = src.find(marker).expect("job.rs declares RUNNER_IMAGE");
    let rest = &src[at + marker.len()..];
    let open = rest.find('"').expect("a string literal follows");
    let close = rest[open + 1..].find('"').expect("the literal closes");
    rest[open + 1..open + 1 + close].to_string()
}

fn is_digest_reference(reference: &str) -> bool {
    let Some((name, digest)) = reference.split_once("@sha256:") else {
        return false;
    };
    !name.is_empty()
        && digest.len() == 64
        && digest
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

/// The fourteen shipped UI files: everything under `ui/` except `*.md` and
/// `tests/` — `scripts/check-ui-offline.sh`'s scope, by construction.
fn shipped_ui_files() -> Vec<String> {
    let files: Vec<String> = files_under("ui")
        .into_iter()
        .filter(|p| !p.ends_with(".md") && !p.starts_with("ui/tests/"))
        .collect();
    assert_eq!(
        14,
        files.len(),
        "the shipped UI is fourteen files (ui/*.html, ui/*.js, ui/*.css, ui/pages/*); found {files:?}"
    );
    files
}

/// A UI file's ConfigMap key — the template's `logweir.ui.key`: the path under
/// `ui/` with `/` spelt `__`.
fn ui_key(rel: &str) -> String {
    rel.trim_start_matches("ui/").replace('/', "__")
}

// ============================================================== the copies

/// **The chart's `crds/` is a byte-identical copy of `config/crd/`** — the six
/// files, no seventh, no missing one. The script says the same with `cmp`.
#[test]
fn chart_lint_crds_are_byte_identical_copies() {
    let source: BTreeSet<String> = files_under("config/crd")
        .into_iter()
        .filter(|p| p.ends_with(".yaml") && !p.ends_with("kustomization.yaml"))
        .map(|p| p.trim_start_matches("config/crd/").to_string())
        .collect();
    let copy: BTreeSet<String> = files_under("charts/logweir/crds")
        .into_iter()
        .map(|p| p.trim_start_matches("charts/logweir/crds/").to_string())
        .collect();
    assert_eq!(6, source.len(), "config/crd holds six CRDs: {source:?}");
    assert_eq!(
        source, copy,
        "charts/logweir/crds must hold exactly the files config/crd holds"
    );
    for name in &source {
        assert_eq!(
            read_bytes(&format!("config/crd/{name}")),
            read_bytes(&format!("charts/logweir/crds/{name}")),
            "charts/logweir/crds/{name} is not byte-identical to config/crd/{name}; copy it"
        );
    }
}

/// **The chart's `ui/` is a byte-identical copy of the fourteen shipped UI
/// files and holds nothing else** — no `tests/` (which carries a throwaway
/// keypair), no README.
#[test]
fn chart_lint_ui_copy_is_byte_identical_and_carries_nothing_else() {
    let shipped = shipped_ui_files();
    let copy: BTreeSet<String> = files_under("charts/logweir/ui")
        .into_iter()
        .map(|p| p.trim_start_matches("charts/logweir/").to_string())
        .collect();
    let want: BTreeSet<String> = shipped.iter().cloned().collect();
    assert_eq!(
        want, copy,
        "charts/logweir/ui must hold exactly the fourteen shipped files"
    );
    for rel in &shipped {
        assert_eq!(
            read_bytes(rel),
            read_bytes(&format!("charts/logweir/{rel}")),
            "charts/logweir/{rel} is not byte-identical to {rel}; copy it"
        );
    }
}

// ================================================ the default render vs logweir.yaml

/// **The default render and `logweir.yaml` agree on the substance of the
/// control plane.** Image, pull policy, args, every env name and value, both
/// security contexts, the ServiceAccount, the four ClusterRoles' rules as
/// sets, the ClusterRoleBinding's subject, the six CRD names, the
/// NetworkPolicy's spec. The ONE permitted difference is `LOGWEIR_RUNNER_IMAGE`,
/// whose default must be `job::RUNNER_IMAGE`.
#[test]
fn chart_lint_default_render_agrees_with_the_install_file() {
    let chart = rendered("default");
    let install = docs_in("logweir.yaml");

    // The Deployment.
    let cd = find(&chart, "Deployment", "weirkeeper");
    let id = find(&install, "Deployment", "weirkeeper");
    let cc = container(cd);
    let ic = container(id);
    assert_eq!(ic["image"], cc["image"], "the controller image differs");
    assert!(
        is_digest_reference(cc["image"].as_str().unwrap_or("")),
        "the default render's controller image is not a digest: {:?}",
        cc["image"]
    );
    assert_eq!(
        ic["imagePullPolicy"], cc["imagePullPolicy"],
        "imagePullPolicy differs"
    );
    assert_eq!(ic["args"], cc["args"], "args differ");
    assert_eq!(
        ic["securityContext"], cc["securityContext"],
        "the container securityContext differs"
    );
    assert_eq!(ic["resources"], cc["resources"], "resources differ");
    assert_eq!(
        pod_spec(id)["securityContext"],
        pod_spec(cd)["securityContext"],
        "the pod securityContext differs"
    );
    assert_eq!(
        pod_spec(id)["serviceAccountName"],
        pod_spec(cd)["serviceAccountName"],
        "serviceAccountName differs"
    );
    assert!(
        pod_spec(cd).get("automountServiceAccountToken").is_none(),
        "the controller PodSpec is the one automount exemption and must leave the field unset, as the source does"
    );
    let mut chart_env = env_of(cc);
    let install_env = env_of(ic);
    let runner = chart_env
        .remove("LOGWEIR_RUNNER_IMAGE")
        .expect("the chart renders LOGWEIR_RUNNER_IMAGE (Task 33's override) on the Deployment");
    assert_eq!(
        Some(runner_image_constant().as_str()),
        runner["value"].as_str(),
        "LOGWEIR_RUNNER_IMAGE's default must be weirkeeper::job::RUNNER_IMAGE, so the default \
         install creates the Jobs it always created"
    );
    assert_eq!(
        install_env, chart_env,
        "the Deployment's env differs between logweir.yaml and the default render (LOGWEIR_RUNNER_IMAGE \
         is the one permitted extra, already removed)"
    );

    // The four ClusterRoles, as sets of rules.
    for role in [
        "weirkeeper",
        "logweir-viewer",
        "logweir-operator",
        "logweir-approver",
    ] {
        assert_eq!(
            rules_of(find(&install, "ClusterRole", role)),
            rules_of(find(&chart, "ClusterRole", role)),
            "ClusterRole {role}'s rules differ from logweir.yaml's"
        );
    }
    assert_eq!(
        names_of(&install, "ClusterRole"),
        names_of(&chart, "ClusterRole"),
        "the default render carries a ClusterRole logweir.yaml does not, or lacks one"
    );

    // The ClusterRoleBinding binds the same role to the same ServiceAccount.
    let icrb = find(&install, "ClusterRoleBinding", "weirkeeper");
    let ccrb = find(&chart, "ClusterRoleBinding", "weirkeeper");
    assert_eq!(icrb.value["roleRef"], ccrb.value["roleRef"]);
    assert_eq!(
        icrb.value["subjects"][0]["name"], ccrb.value["subjects"][0]["name"],
        "the bound ServiceAccount differs"
    );
    assert_eq!(
        Some("logweir-system"),
        ccrb.value["subjects"][0]["namespace"].as_str(),
        "rendered with -n logweir-system, the subject's namespace is the release namespace"
    );

    // The six CRDs, by name, and byte-for-byte the same documents.
    assert_eq!(
        names_of(&install, "CustomResourceDefinition"),
        names_of(&chart, "CustomResourceDefinition"),
        "the CRD names differ"
    );
    assert_eq!(6, names_of(&chart, "CustomResourceDefinition").len());
    for name in names_of(&chart, "CustomResourceDefinition") {
        assert_eq!(
            find(&install, "CustomResourceDefinition", &name).value["spec"],
            find(&chart, "CustomResourceDefinition", &name).value["spec"],
            "CRD {name}'s spec differs"
        );
    }

    // The NetworkPolicy and the two ServiceAccounts.
    assert_eq!(
        find(&install, "NetworkPolicy", "logweir-runner-egress").value["spec"],
        find(&chart, "NetworkPolicy", "logweir-runner-egress").value["spec"],
        "the NetworkPolicy's spec differs"
    );
    let runner_sa = find(&chart, "ServiceAccount", "logweir-runner");
    assert_eq!(
        Some(false),
        runner_sa.value["automountServiceAccountToken"].as_bool(),
        "the runner ServiceAccount sets automountServiceAccountToken: false"
    );
    find(&chart, "ServiceAccount", "weirkeeper");

    // Nothing the install file does not have, kind by kind — except the runner
    // ServiceAccount the chart renders into the release namespace by ruling.
    let mut chart_kinds: BTreeSet<String> = chart.iter().map(|d| d.kind.clone()).collect();
    let install_kinds: BTreeSet<String> = install
        .iter()
        .map(|d| d.kind.clone())
        .filter(|k| k != "Namespace")
        .collect();
    chart_kinds.remove("Namespace");
    assert_eq!(
        install_kinds, chart_kinds,
        "the default render's kinds must be logweir.yaml's (minus the Namespace, which --create-namespace makes)"
    );
    // Every namespaced object landed in the release namespace.
    for d in &chart {
        if let Some(ns) = d.value["metadata"]["namespace"].as_str() {
            assert_eq!(
                "logweir-system",
                ns,
                "{}/{} rendered into {ns}, not the release namespace",
                d.kind,
                d.name()
            );
        }
    }
}

/// **`values.yaml` pins the tree's own digests, and every image value is a
/// digest with its provenance beside it.**
#[test]
fn chart_lint_values_pin_the_shipped_digests() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let deployment = read("config/manager/deployment.yaml");
    let controller = deployment
        .lines()
        .find_map(|l| l.trim().strip_prefix("image: "))
        .expect("deployment.yaml names the controller image")
        .trim()
        .to_string();
    assert_eq!(
        Some(controller.as_str()),
        values["controllerImage"].as_str(),
        "values.yaml controllerImage must be config/manager/deployment.yaml's image"
    );
    assert_eq!(
        Some(runner_image_constant().as_str()),
        values["runnerImage"].as_str(),
        "values.yaml runnerImage must be weirkeeper::job::RUNNER_IMAGE"
    );
    for (path, v) in [
        ("controllerImage", &values["controllerImage"]),
        ("runnerImage", &values["runnerImage"]),
        ("minio.image", &values["minio"]["image"]),
        ("minio.mcImage", &values["minio"]["mcImage"]),
        ("demoKafka.image", &values["demoKafka"]["image"]),
        ("ui.image", &values["ui"]["image"]),
    ] {
        let s = v.as_str().unwrap_or_else(|| panic!("{path} is a string"));
        assert!(
            is_digest_reference(s),
            "{path} is not a digest reference: {s}"
        );
    }
    // The two MinIO images are the compose stack's, byte for byte.
    let compose = read("e2e/compose/docker-compose.yml");
    for path in ["minio.image", "minio.mcImage"] {
        let key = path.split('.').nth(1).unwrap();
        let s = values["minio"][key].as_str().unwrap();
        assert!(
            compose.contains(&format!("image: {s}")),
            "{path} ({s}) is not the reference e2e/compose/docker-compose.yml pins"
        );
    }
    // The two digests this task resolved carry the command and the date.
    let text = read("charts/logweir/values.yaml");
    for command in [
        "docker buildx imagetools inspect apache/kafka:3.7.1",
        "docker buildx imagetools inspect registry.k8s.io/kubectl:v1.34.1",
    ] {
        assert!(
            text.contains(command),
            "values.yaml must record the resolution command `{command}` beside the digest (the kindest/node idiom)"
        );
    }
    assert!(
        text.contains("2026-09-12"),
        "values.yaml must record the date the digests were resolved"
    );
    // The defaults are the shipped install: nothing optional on.
    for flag in ["minio", "demoKafka", "ui"] {
        assert_eq!(
            Some(false),
            values[flag]["enabled"].as_bool(),
            "{flag}.enabled defaults to false"
        );
    }
    assert_eq!(Some("IfNotPresent"), values["imagePullPolicy"].as_str());
}

// ============================================ the three flags, off and on

const SHIPPED_KINDS: [&str; 6] = [
    "CustomResourceDefinition",
    "ServiceAccount",
    "ClusterRole",
    "ClusterRoleBinding",
    "Deployment",
    "NetworkPolicy",
];

/// **Under the defaults and under the minimal example, the three optional
/// components render NOTHING** — no StatefulSet, no Job, no Service, no
/// ConfigMap, no Secret, no PVC, no RoleBinding, no object named after them.
#[test]
fn chart_lint_optional_components_render_nothing_under_defaults() {
    for name in ["default", "minimal", "author-only"] {
        let docs = rendered(name);
        let kinds: BTreeSet<String> = docs.iter().map(|d| d.kind.clone()).collect();
        let allowed: BTreeSet<String> = SHIPPED_KINDS.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            allowed, kinds,
            "rendered/{name}.yaml carries kinds outside the shipped install's; the optional components \
             must render nothing when their flags are off"
        );
        for d in &docs {
            let n = d.name();
            assert!(
                !n.starts_with("logweir-minio")
                    && !n.starts_with("logweir-kafka-")
                    && !n.starts_with("logweir-ui")
                    && n != "logweir-s3",
                "rendered/{name}.yaml carries {}/{n}, an optional component's object, under a render with every flag off",
                d.kind
            );
        }
        assert_eq!(
            1,
            docs.iter().filter(|d| d.kind == "Deployment").count(),
            "rendered/{name}.yaml: exactly one Deployment (the controller)"
        );
    }
}

/// **`demoKafka.enabled` renders two one-replica StatefulSets advertising
/// their ClusterIP Service names, their Services, and a post-install seed Job
/// whose command creates `orders` and `payments` on the source and the marker
/// topic `logweir.scratch` on the target.**
#[test]
fn chart_lint_demo_kafka_renders_two_brokers_and_a_seed_with_the_marker_topic() {
    let docs = rendered("demo");
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let image = values["demoKafka"]["image"].as_str().unwrap();
    let mut cluster_ids: BTreeSet<String> = BTreeSet::new();
    for role in ["source", "target"] {
        let name = format!("logweir-kafka-{role}");
        let ss = find(&docs, "StatefulSet", &name);
        assert_eq!(
            Some(1),
            ss.value["spec"]["replicas"].as_u64(),
            "{name}: one replica"
        );
        assert_eq!(
            Some(format!("{name}-headless").as_str()),
            ss.value["spec"]["serviceName"].as_str()
        );
        let c = container(ss);
        assert_eq!(
            Some(image),
            c["image"].as_str(),
            "{name}: the pinned broker image"
        );
        let env = env_of(c);
        assert_eq!(
            Some(format!("PLAINTEXT://{name}.logweir-system.svc.cluster.local:9092").as_str()),
            env["KAFKA_ADVERTISED_LISTENERS"]["value"].as_str(),
            "{name}: the advertised listener is the ClusterIP Service's DNS name"
        );
        assert_eq!(
            Some("controller,broker"),
            env["KAFKA_PROCESS_ROLES"]["value"].as_str()
        );
        let id = env["CLUSTER_ID"]["value"].as_str().unwrap_or_else(|| {
            panic!(
                "{name}: CLUSTER_ID is set — the image's default is one fixed id for every broker"
            )
        });
        assert_eq!(
            22,
            id.len(),
            "{name}: a KRaft cluster id is 22 base64url characters"
        );
        assert_ne!(
            "5L6g3nShT-eMCtK--X86sw", id,
            "{name}: the image's own default cluster id is not a per-broker id"
        );
        cluster_ids.insert(id.to_string());
        assert_eq!(
            Some("false"),
            env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"]["value"].as_str()
        );
        assert_eq!(
            Some(false),
            pod_spec(ss)["automountServiceAccountToken"].as_bool(),
            "{name}: a broker pod holds no cluster credential"
        );
        find(&docs, "Service", &name);
        let headless = find(&docs, "Service", &format!("{name}-headless"));
        assert_eq!(Some("None"), headless.value["spec"]["clusterIP"].as_str());
        for svc in [find(&docs, "Service", &name), headless] {
            assert_eq!(
                Some(9092),
                svc.value["spec"]["ports"][0]["port"].as_u64(),
                "{}: port 9092",
                svc.name()
            );
        }
    }
    assert_eq!(
        2,
        cluster_ids.len(),
        "the two brokers must carry DIFFERENT cluster ids: {cluster_ids:?} — phase 0's target != source rail refuses a scratch restore between two brokers that share one"
    );
    let seed = find(&docs, "Job", "logweir-kafka-seed");
    let ann = &seed.value["metadata"]["annotations"];
    assert_eq!(
        Some("post-install"),
        ann["helm.sh/hook"].as_str(),
        "the Kafka seed is a post-install hook"
    );
    assert_eq!(
        Some("before-hook-creation,hook-succeeded"),
        ann["helm.sh/hook-delete-policy"].as_str()
    );
    let c = container(seed);
    assert_eq!(
        Some(image),
        c["image"].as_str(),
        "the seed runs from the same image"
    );
    let script = c["args"][0].as_str().expect("the seed's script");
    for needle in [
        "--topic orders",
        "--topic payments",
        "--topic logweir.scratch",
        "--partitions 1 --replication-factor 1 --topic logweir.scratch",
        "kafka-console-producer.sh",
        "--property parse.key=true",
        "wait_for \"$SOURCE\"",
        "wait_for \"$TARGET\"",
    ] {
        assert!(
            script.contains(needle),
            "the seed's command must carry `{needle}`:\n{script}"
        );
    }
    assert!(
        script.contains("--bootstrap-server \"$TARGET\" --create --if-not-exists --partitions 1 --replication-factor 1 --topic logweir.scratch"),
        "the marker topic is created on the TARGET, one partition, replication factor 1 — the compose topic-setup's form"
    );
    let env = env_of(c);
    assert_eq!(
        Some("200"),
        env["RECORDS"]["value"].as_str(),
        "the records count defaults to 200 (demoKafka.seed.recordsPerTopic)"
    );
    assert_eq!(
        Some("logweir-kafka-source.logweir-system.svc.cluster.local:9092"),
        env["SOURCE"]["value"].as_str()
    );
    assert_eq!(
        Some("logweir-kafka-target.logweir-system.svc.cluster.local:9092"),
        env["TARGET"]["value"].as_str()
    );
}

/// **`minio.enabled` renders the backend: the Deployment, the Service on 9000,
/// the seed Job naming both buckets, the `logweir-s3` Secret with the two data
/// keys the code reads — and points the controller at it.**
#[test]
fn chart_lint_minio_renders_the_backend_and_the_archive_secret() {
    let docs = rendered("demo");
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let minio = find(&docs, "Deployment", "logweir-minio");
    assert_eq!(
        values["minio"]["image"].as_str(),
        container(minio)["image"].as_str(),
        "the MinIO Deployment runs the pinned image"
    );
    assert_eq!(
        Some(false),
        pod_spec(minio)["automountServiceAccountToken"].as_bool()
    );
    let svc = find(&docs, "Service", "logweir-minio");
    assert_eq!(Some(9000), svc.value["spec"]["ports"][0]["port"].as_u64());
    let seed = find(&docs, "Job", "logweir-minio-seed");
    let ann = &seed.value["metadata"]["annotations"];
    assert_eq!(
        Some("post-install,post-upgrade"),
        ann["helm.sh/hook"].as_str()
    );
    assert_eq!(
        Some("before-hook-creation,hook-succeeded"),
        ann["helm.sh/hook-delete-policy"].as_str()
    );
    let c = container(seed);
    assert_eq!(
        values["minio"]["mcImage"].as_str(),
        c["image"].as_str(),
        "the seed runs the pinned mc image"
    );
    let script = c["args"][0].as_str().expect("the seed's script");
    for bucket in ["local/kafka-backups", "local/logweir-evidence"] {
        assert!(
            script.contains(&format!("mc mb --ignore-existing {bucket}")),
            "the seed creates {bucket}"
        );
    }
    let s3 = find(&docs, "Secret", "logweir-s3");
    let data = &s3.value["stringData"];
    assert!(
        data.get("access-key-id").is_some(),
        "logweir-s3 carries access-key-id"
    );
    assert!(
        data.get("secret-access-key").is_some(),
        "logweir-s3 carries secret-access-key"
    );
    assert_eq!(
        Some("logweir-system"),
        s3.value["metadata"]["namespace"].as_str()
    );
    // The controller is pointed at it: the k8s-demo overlay's five env, derived.
    let env = env_of(container(find(&docs, "Deployment", "weirkeeper")));
    assert_eq!(
        Some("s3://kafka-backups/logweir"),
        env["LOGWEIR_ARCHIVE_URL"]["value"].as_str()
    );
    assert_eq!(
        Some("http://logweir-minio.logweir-system.svc:9000"),
        env["AWS_ENDPOINT_URL"]["value"].as_str()
    );
    assert_eq!(Some("us-east-1"), env["AWS_REGION"]["value"].as_str());
    assert_eq!(Some("true"), env["AWS_ALLOW_HTTP"]["value"].as_str());
    assert_eq!(
        Some("false"),
        env["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"]["value"].as_str()
    );
    // The demo example runs on an emptyDir; the default persistence is a PVC.
    assert!(
        pod_spec(minio)["volumes"][0]["emptyDir"].is_mapping(),
        "the demo example's MinIO volume is an emptyDir"
    );
    assert_eq!(
        Some(true),
        values["minio"]["persistence"]["enabled"].as_bool()
    );
}

/// The proxy's path filter, measured from `ui/api.js` and `ui/pages/*.js`: the
/// page addresses `/apis/logweir.dev/v1alpha1/…` and nothing on `/api/v1`.
const ACCEPT_PATHS: &str = "--accept-paths=^/(ui/|apis/logweir\\.dev/v1alpha1/)";

/// **`ui.enabled` renders the proxy with its measured paths, the ConfigMap
/// holding exactly the fourteen UI files' bytes and no key material, the
/// ServiceAccount, the RoleBinding to the chart's own role (never
/// `cluster-admin`), the Service on 8001 — and no Ingress.**
#[test]
fn chart_lint_ui_renders_the_proxy_with_its_paths_and_a_narrow_role() {
    let docs = rendered("demo");
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");

    // The Deployment.
    let ui = find(&docs, "Deployment", "logweir-ui");
    let c = container(ui);
    assert_eq!(
        values["ui"]["image"].as_str(),
        c["image"].as_str(),
        "the pinned kubectl image"
    );
    let args: Vec<&str> = c["args"]
        .as_sequence()
        .expect("args")
        .iter()
        .map(|a| a.as_str().expect("a string arg"))
        .collect();
    assert_eq!("proxy", args[0]);
    for needed in [
        "--www=/ui",
        "--www-prefix=/ui/",
        "--address=0.0.0.0",
        "--port=8001",
        ACCEPT_PATHS,
    ] {
        assert!(
            args.contains(&needed),
            "the proxy's args must carry `{needed}`: {args:?}"
        );
    }
    assert!(
        !args.iter().any(|a| a.contains("--disable-filter")),
        "`--disable-filter` must never be passed: the path filter IS the authorisation boundary"
    );
    assert!(
        !args.iter().any(
            |a| a.starts_with("--accept-paths=") && (a.ends_with("=.*") || a.ends_with("=^.*"))
        ),
        "the accept-paths filter must not be the match-all `.*`"
    );
    assert_eq!(
        Some("logweir-ui"),
        pod_spec(ui)["serviceAccountName"].as_str()
    );
    assert_eq!(
        Some(true),
        pod_spec(ui)["automountServiceAccountToken"].as_bool(),
        "the proxy pod needs its token: it is the credential the proxy attaches"
    );
    let volumes = pod_spec(ui)["volumes"].as_sequence().expect("volumes");
    let cm_volume = volumes
        .iter()
        .find(|v| v["configMap"].is_mapping())
        .expect("a configMap volume");
    let items: BTreeMap<String, String> = cm_volume["configMap"]["items"]
        .as_sequence()
        .expect("items")
        .iter()
        .map(|i| {
            (
                i["key"].as_str().unwrap().to_string(),
                i["path"].as_str().unwrap().to_string(),
            )
        })
        .collect();

    // The ConfigMap: exactly the fourteen files, byte for byte, mapped back to
    // their paths, and no key material.
    let cm = find(&docs, "ConfigMap", "logweir-ui");
    let data = cm.value["data"].as_mapping().expect("data");
    let shipped = shipped_ui_files();
    assert_eq!(
        shipped.len(),
        data.len(),
        "the ConfigMap holds exactly the fourteen files"
    );
    assert_eq!(
        shipped.len(),
        items.len(),
        "every key is mapped into the volume"
    );
    for rel in &shipped {
        let key = ui_key(rel);
        let got = data
            .get(Value::String(key.clone()))
            .unwrap_or_else(|| panic!("the ConfigMap has no key {key} for {rel}"))
            .as_str()
            .unwrap_or_else(|| panic!("{key} is a string"));
        assert_eq!(
            read(rel),
            got,
            "the ConfigMap's {key} is not the tree's {rel} byte for byte"
        );
        assert_eq!(
            Some(rel.trim_start_matches("ui/").to_string()).as_deref(),
            items.get(&key).map(String::as_str),
            "{key} must be mounted at its path under /ui"
        );
    }
    for (k, v) in data {
        let text = v.as_str().unwrap_or("");
        for line in text.lines() {
            let t = line.trim();
            assert!(
                !(t.starts_with("-----BEGIN") && t.contains("PRIVATE KEY-----")),
                "Global Constraint 28: {} carries a private-key PEM header",
                k.as_str().unwrap_or("?")
            );
        }
    }

    // The ServiceAccount, the roles, the binding.
    find(&docs, "ServiceAccount", "logweir-ui");
    let role = find(&docs, "ClusterRole", "logweir-ui");
    let want: BTreeSet<(Vec<String>, Vec<String>, Vec<String>)> = BTreeSet::from([
        (
            vec!["logweir.dev".into()],
            vec![
                "approvals".into(),
                "backups".into(),
                "backupschedules".into(),
                "kafkaclusters".into(),
                "restores".into(),
            ],
            vec!["get".into(), "list".into()], // engine-token-ok: an RBAC verb the page issues, never an engine subcommand
        ),
        (
            vec!["logweir.dev".into()],
            vec![
                "approvals".into(),
                "backupschedules".into(),
                "kafkaclusters".into(),
                "restores".into(),
            ],
            vec!["create".into()],
        ),
        (
            vec!["logweir.dev".into()],
            vec!["backupschedules".into()],
            vec!["patch".into()],
        ),
    ]);
    assert_eq!(
        want,
        rules_of(role),
        "the page's ClusterRole carries exactly the measured verbs"
    );
    let roster = find(&docs, "ClusterRole", "logweir-ui-trustrosters");
    assert_eq!(
        BTreeSet::from([(
            vec!["logweir.dev".to_string()],
            vec!["trustrosters".to_string()],
            vec!["list".to_string()] // engine-token-ok: an RBAC verb the page issues, never an engine subcommand
        )]),
        rules_of(roster)
    );
    let bindings: Vec<&Doc> = docs.iter().filter(|d| d.kind == "RoleBinding").collect();
    assert!(
        !bindings.is_empty(),
        "the page's role is bound by a RoleBinding"
    );
    for rb in bindings {
        assert_eq!(Some("ClusterRole"), rb.value["roleRef"]["kind"].as_str());
        assert_eq!(
            Some("logweir-ui"),
            rb.value["roleRef"]["name"].as_str(),
            "the RoleBinding points at the chart's own role — never cluster-admin, never a shipped human role"
        );
        assert_eq!(
            Some("ServiceAccount"),
            rb.value["subjects"][0]["kind"].as_str()
        );
        assert_eq!(Some("logweir-ui"), rb.value["subjects"][0]["name"].as_str());
    }
    for crb in docs.iter().filter(|d| d.kind == "ClusterRoleBinding") {
        assert_ne!(
            Some("cluster-admin"),
            crb.value["roleRef"]["name"].as_str(),
            "no ClusterRoleBinding in the chart names cluster-admin"
        );
    }
    let svc = find(&docs, "Service", "logweir-ui");
    assert_eq!(Some(8001), svc.value["spec"]["ports"][0]["port"].as_u64());
    assert_eq!(Some("ClusterIP"), svc.value["spec"]["type"].as_str());
    assert!(
        !docs.iter().any(|d| d.kind == "Ingress"),
        "the chart renders no Ingress: reachability of the Service is the authorisation boundary"
    );
}

/// **Every PodSpec of the optional components refuses a ServiceAccount token
/// except the proxy's, which needs it.** Under the demo render, every Pod-
/// carrying object is enumerated; the controller (the shipped exemption) and
/// the UI proxy are the only two that keep a token.
#[test]
fn chart_lint_only_the_proxy_and_the_controller_hold_a_token() {
    let docs = rendered("demo");
    let mut seen = 0usize;
    for d in &docs {
        if !matches!(d.kind.as_str(), "Deployment" | "StatefulSet" | "Job") {
            continue;
        }
        seen += 1;
        let automount = pod_spec(d)["automountServiceAccountToken"].as_bool();
        match d.name().as_str() {
            "weirkeeper" => assert_eq!(
                None, automount,
                "the controller leaves the field unset (the shipped exemption)"
            ),
            "logweir-ui" => assert_eq!(Some(true), automount, "the proxy pod needs its token"),
            other => assert_eq!(
                Some(false),
                automount,
                "{}/{other} must set automountServiceAccountToken: false",
                d.kind
            ),
        }
    }
    assert_eq!(
        7, seen,
        "the demo render carries seven Pod-carrying objects: 3 Deployments, 2 StatefulSets, 2 Jobs"
    );
}

/// **Every rendered image is a digest, except in the author-only render, by
/// name** — its premise is a locally built tag.
#[test]
fn chart_lint_every_rendered_image_is_a_digest_except_the_author_only_example() {
    let mut total = 0usize;
    for rel in files_under("charts/logweir/rendered") {
        let name = rel.trim_start_matches("charts/logweir/rendered/");
        for d in docs_in(&rel) {
            if !matches!(d.kind.as_str(), "Deployment" | "StatefulSet" | "Job") {
                continue;
            }
            let image = container(&d)["image"]
                .as_str()
                .expect("an image")
                .to_string();
            total += 1;
            if name == "author-only.yaml" {
                assert!(
                    image == "weirkeeper:check" || is_digest_reference(&image),
                    "author-only.yaml: the controller is the locally built tag; found {image}"
                );
                continue;
            }
            assert!(
                is_digest_reference(&image),
                "{name}: {}/{} references {image} by tag",
                d.kind,
                d.name()
            );
        }
    }
    assert!(
        total >= 10,
        "only {total} image references across the rendered files"
    );
}

/// **`values.schema.json` types the three flags as booleans** (the script
/// proves the refusal with `--set demoKafka.enabled=yes`; this reads the
/// schema) and the four image values as strings.
#[test]
fn chart_lint_values_schema_types_the_three_flags_as_booleans() {
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("values.schema.json parses");
    for flag in ["minio", "demoKafka", "ui"] {
        assert_eq!(
            Some("boolean"),
            schema["properties"][flag]["properties"]["enabled"]["type"].as_str(),
            "{flag}.enabled must be typed boolean"
        );
        assert!(
            schema["properties"][flag]["required"]
                .as_array()
                .map(|a| a.iter().any(|v| v == "enabled"))
                .unwrap_or(false),
            "{flag}.enabled must be required"
        );
    }
    for (a, b) in [
        ("minio", "image"),
        ("minio", "mcImage"),
        ("demoKafka", "image"),
        ("ui", "image"),
    ] {
        assert_eq!(
            Some("string"),
            schema["properties"][a]["properties"][b]["type"].as_str()
        );
    }
    for top in ["controllerImage", "runnerImage"] {
        assert_eq!(Some("string"), schema["properties"][top]["type"].as_str());
    }
    assert_eq!(
        Some(false),
        schema["additionalProperties"].as_bool(),
        "a typo'd top-level key is refused"
    );
}

// ==================================================== the scripts and the docs

/// **`scripts/check-chart.sh` carries every arm the ruling names, and refuses
/// without helm by naming the version.**
#[test]
fn chart_lint_the_gate_script_carries_every_arm() {
    let script = read("scripts/check-chart.sh");
    for needle in [
        "cmp -s \"$src\" \"$CHART/crds/$base\"",
        "cmp -s \"$src\" \"$CHART/ui/$rel\"",
        "helm lint \"$CHART\"",
        "helm template \"$RELEASE\" \"$CHART\" -n \"$NAMESPACE\" --include-crds",
        "git status --porcelain -- \"$RENDERED\"",
        "@sha256:",
        "DIGEST_EXEMPT=\"author-only.yaml\"",
        "--set \"$flag=yes\"",
        "HELM_MIN_MAJOR=4",
        "crates/weirkeeper/src/job.rs",
        "config/manager/deployment.yaml",
    ] {
        assert!(
            script.contains(needle),
            "scripts/check-chart.sh must carry `{needle}`"
        );
    }
    assert!(
        script.contains("command -v helm"),
        "the script refuses without helm, naming the version it wants"
    );
}

/// **`scripts/helm-demo.sh` names its context on every `kubectl` line, reads
/// exactly its three parameters, and never spells the other cluster.**
#[test]
fn chart_lint_helm_demo_names_its_context_on_every_kubectl_line() {
    let script = read("scripts/helm-demo.sh");
    let mut seen = 0usize;
    let mut offenders = Vec::new();
    for (i, line) in script.lines().enumerate() {
        if line.split_whitespace().next() != Some("kubectl") {
            continue;
        }
        seen += 1;
        if !line.contains(r#"--context "$LOGWEIR_KUBE_CONTEXT""#) {
            offenders.push(format!("line {}: {}", i + 1, line.trim()));
        }
    }
    assert!(
        seen >= 40,
        "only {seen} lines begin with `kubectl` — this lint would pass vacuously"
    );
    assert!(
        offenders.is_empty(),
        "{} kubectl line(s) do not name the context (STANDING RULE 12):\n{}",
        offenders.len(),
        offenders.join("\n")
    );
    for param in [
        "LOGWEIR_KUBE_CONTEXT=\"${LOGWEIR_KUBE_CONTEXT:-docker-desktop}\"",
        "LOGWEIR_HELM_RELEASE=\"${LOGWEIR_HELM_RELEASE:-logweir}\"",
        "LOGWEIR_HELM_NAMESPACE=\"${LOGWEIR_HELM_NAMESPACE:-logweir-system}\"",
    ] {
        assert!(script.contains(param), "helm-demo.sh must read `{param}`");
    }
    let executed: String = script
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        !executed.contains("kind-logweir"),
        "no executed line of the walk spells the CI cluster; the workflow sets it"
    );
    assert!(
        script.contains("node ui/tests/emit-restore-body.js"),
        "the Restore's planBytes come from the page's own emitter"
    );
    assert!(
        script.contains("logweir drill approve"),
        "the approval is minted on the host with the shipped CLI"
    );
    assert!(
        script.contains("logweir drill verify") && script.contains("docs/verify_scorecard.py"),
        "both readers"
    );
    assert!(
        script.contains("port-forward"),
        "the UI is reached through a port-forward"
    );
    assert!(
        script.contains("trap teardown EXIT"),
        "the teardown is the EXIT trap"
    );
}

/// **The docs and the examples say what the ruling fixes**: the author-only
/// example says it is not evidence of publication; the README carries the
/// authority paragraph and the CRD-upgrade-by-hand instruction; the minimal
/// example names its three values.
#[test]
fn chart_lint_examples_and_readme_say_what_they_are() {
    let author_only = read("charts/logweir/examples/author-only.values.yaml");
    assert!(author_only.contains("NOT EVIDENCE OF PUBLICATION"));
    assert!(author_only.contains("imagePullPolicy: Never"));
    assert!(author_only.contains("controllerImage: weirkeeper:check"));
    assert!(author_only.contains("runnerImage: logweir:check"));
    let minimal = read("charts/logweir/examples/minimal.values.yaml");
    for key in ["url:", "endpoint:", "region:"] {
        assert!(minimal.contains(key), "minimal.values.yaml sets `{key}`");
    }
    let demo = read("charts/logweir/examples/demo.values.yaml");
    for line in [
        "minio:\n  enabled: true",
        "demoKafka:\n  enabled: true",
        "ui:\n  enabled: true",
    ] {
        assert!(demo.contains(line), "demo.values.yaml turns on `{line}`");
    }
    let readme = read("charts/logweir/README.md");
    for needle in [
        "anyone who can reach that Service acts with that",
        "Helm installs `crds/` once",
        "registered trademarks of the Apache Software",
        "kubectl port-forward",
        "not evidence of publication",
    ] {
        assert!(
            readme.contains(needle),
            "charts/logweir/README.md must say `{needle}`"
        );
    }
    let notes = read("charts/logweir/templates/NOTES.txt");
    assert!(
        notes.contains("port-forward"),
        "NOTES.txt prints the port-forward command"
    );
    assert!(
        notes.contains("acts with that ServiceAccount's authority"),
        "NOTES.txt states whose authority the page acts with"
    );
}
