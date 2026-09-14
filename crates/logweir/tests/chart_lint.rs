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
//!   control plane: the Deployment's args, every other env name and value,
//!   both security contexts, the resources and the ServiceAccount; the same
//!   four ClusterRoles with the same rules as sets; the same six CRD names.
//!   **Exactly FOUR differences are permitted**, all of them the chart's image
//!   ruling of 2026-09-12 (`values.yaml`'s header): the controller image (the
//!   SAME repository, at `:latest` rather than at the install file's digest —
//!   asserted, not assumed), `imagePullPolicy` (`Always` rather than
//!   `IfNotPresent`), and the two envs `LOGWEIR_RUNNER_IMAGE` (Task 33) and
//!   `LOGWEIR_RUNNER_PULL_POLICY` (Task 37), which `logweir.yaml` does not set
//!   at all. The runner reference is read from `crates/weirkeeper/src/job.rs`
//!   and never spelt here (`crd_shape.rs::the_runner_image_is_named_once`
//!   counts that string under `crates/`). There is no namespace-derived env in
//!   the shipped Deployment, so nothing else is excepted.
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

/// The controller image `config/manager/deployment.yaml` pins, read out of that
/// file so this test never spells a registry path.
fn controller_image_pin() -> String {
    read("config/manager/deployment.yaml")
        .lines()
        .find_map(|l| l.trim().strip_prefix("image: "))
        .expect("config/manager/deployment.yaml names the controller image")
        .trim()
        .to_string()
}

/// The REPOSITORY half of a `<repository>@sha256:<64 hex>` reference.
///
/// DERIVED AND NEVER SPELT. Both Logweir repositories come from the tree —
/// `config/manager/deployment.yaml` and `weirkeeper::job::RUNNER_IMAGE` — so a
/// namespace change there propagates into the chart without editing this file,
/// and `crd_shape.rs::the_runner_image_is_named_once` keeps finding the runner
/// reference named exactly once under `crates/`.
fn repository_of(reference: &str) -> String {
    let (repository, _) = reference.split_once("@sha256:").unwrap_or_else(|| {
        panic!("the tree pins `{reference}` by digest, as Global Constraint 7 requires")
    });
    assert!(
        !repository.is_empty(),
        "an empty repository in `{reference}`"
    );
    repository.to_string()
}

/// The tag this chart's defaults give the two Logweir images — **the owner's
/// decision of 2026-09-12**, in place of the digests these values carried
/// before it. `config/`, `logweir.yaml` and `weirkeeper::job::RUNNER_IMAGE` are
/// untouched by that ruling and still pin digests (Global Constraint 7).
const LOGWEIR_TAG: &str = "latest";

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
/// control plane.** Args, every other env name and value, both security
/// contexts, the resources, the ServiceAccount, the four ClusterRoles' rules as
/// sets, the ClusterRoleBinding's subject, the six CRD names, the
/// NetworkPolicy's spec.
///
/// **EXACTLY FOUR DIFFERENCES ARE PERMITTED, and each is asserted rather than
/// skipped:**
///
/// 1. the controller IMAGE — the SAME REPOSITORY as the install file's, at
///    `:latest` rather than at its digest (the owner's decision of 2026-09-12);
///    the repository equality is the assertion that keeps this from becoming
///    "any image at all";
/// 2. `imagePullPolicy` — `Always` here, `IfNotPresent` there, because a
///    mutable tag under a policy that does not refresh is a pod running
///    whatever its node cached first;
/// 3. `LOGWEIR_RUNNER_IMAGE` (Task 33), whose default is the repository of
///    `job::RUNNER_IMAGE` at the same tag;
/// 4. `LOGWEIR_RUNNER_PULL_POLICY` (Task 37), whose default is `Always` for the
///    same reason as (2).
///
/// Everything else — securityContexts, args, the other envs, the SA, the
/// resources, the rules, the CRD specs, the NetworkPolicy — is still identical.
#[test]
fn chart_lint_default_render_agrees_with_the_install_file() {
    let chart = rendered("default");
    let install = docs_in("logweir.yaml");

    // The Deployment.
    let cd = find(&chart, "Deployment", "weirkeeper");
    let id = find(&install, "Deployment", "weirkeeper");
    let cc = container(cd);
    let ic = container(id);
    // DIFFERENCE 1: the same REPOSITORY, a different reference.
    let install_image = ic["image"].as_str().expect("logweir.yaml names an image");
    let chart_image = cc["image"].as_str().expect("the render names an image");
    assert!(
        is_digest_reference(install_image),
        "logweir.yaml still pins the controller image BY DIGEST (Global Constraint 7): \
         {install_image}"
    );
    assert_eq!(
        format!("{}:{LOGWEIR_TAG}", repository_of(install_image)),
        chart_image,
        "the default render's controller image must be logweir.yaml's REPOSITORY at \
         `:{LOGWEIR_TAG}` — the same repository, the chart's own tag ruling"
    );
    assert!(
        !is_digest_reference(chart_image),
        "and it is a tag, not a digest: {chart_image}"
    );
    // DIFFERENCE 2: the pull policy follows the tag.
    assert_eq!(
        Some("IfNotPresent"),
        ic["imagePullPolicy"].as_str(),
        "logweir.yaml's pull policy, unchanged by this chart's ruling"
    );
    assert_eq!(
        Some("Always"),
        cc["imagePullPolicy"].as_str(),
        "the chart's default follows its `:latest` image — Kubernetes' own default for a tag"
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
    // DIFFERENCE 3: the runner image env, at the same repository and tag.
    let runner = chart_env
        .remove("LOGWEIR_RUNNER_IMAGE")
        .expect("the chart renders LOGWEIR_RUNNER_IMAGE (Task 33's override) on the Deployment");
    assert_eq!(
        Some(format!(
            "{}:{LOGWEIR_TAG}",
            repository_of(&runner_image_constant())
        ))
        .as_deref(),
        runner["value"].as_str(),
        "LOGWEIR_RUNNER_IMAGE's default must be weirkeeper::job::RUNNER_IMAGE's REPOSITORY at \
         `:{LOGWEIR_TAG}`"
    );
    // DIFFERENCE 4: the runner pull policy env, which follows that tag.
    let runner_policy = chart_env.remove("LOGWEIR_RUNNER_PULL_POLICY").expect(
        "the chart renders LOGWEIR_RUNNER_PULL_POLICY (Task 37's override) on the Deployment — \
         a runner image named by tag under a policy that never pulls is a Job no kubelet starts",
    );
    assert_eq!(
        Some("Always"),
        runner_policy["value"].as_str(),
        "and its default is `Always`, for the same reason the controller's is"
    );
    assert_eq!(
        install_env, chart_env,
        "the Deployment's env differs between logweir.yaml and the default render \
         (LOGWEIR_RUNNER_IMAGE and LOGWEIR_RUNNER_PULL_POLICY are the two permitted extras, \
         already removed)"
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

/// **`values.yaml` names the tree's own REPOSITORIES at `latest`, and every
/// other image value is a digest with its provenance beside it.**
///
/// THE OWNER'S DECISION OF 2026-09-12, and the whole of what changed. These two
/// values named digests until that day; they now name the repository half of
/// the tree's own two pins, followed by `:latest`. The repositories are DERIVED
/// from `config/manager/deployment.yaml` and `weirkeeper::job::RUNNER_IMAGE`,
/// never spelt here, so a namespace change in the tree propagates and a digest
/// written back into either value is a red.
///
/// THE FOUR THIRD-PARTY IMAGES ARE UNTOUCHED: MinIO, mc, apache/kafka and
/// kubectl are still digests, and still carry the resolution command and the
/// date beside them — the `kindest/node` provenance idiom. That assertion moved
/// HERE, to those four, because the two Logweir values no longer have a digest
/// to record the provenance of.
#[test]
fn chart_lint_values_name_the_shipped_repositories_at_latest() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let controller_repo = repository_of(&controller_image_pin());
    let runner_repo = repository_of(&runner_image_constant());
    assert_eq!(
        Some(format!("{controller_repo}:{LOGWEIR_TAG}")).as_deref(),
        values["controllerImage"].as_str(),
        "values.yaml controllerImage must be config/manager/deployment.yaml's REPOSITORY at \
         `:{LOGWEIR_TAG}` — the repository so a namespace change propagates, the tag because the \
         owner decided on 2026-09-12 that this chart's defaults name a tag"
    );
    assert_eq!(
        Some(format!("{runner_repo}:{LOGWEIR_TAG}")).as_deref(),
        values["runnerImage"].as_str(),
        "values.yaml runnerImage must be weirkeeper::job::RUNNER_IMAGE's REPOSITORY at \
         `:{LOGWEIR_TAG}`"
    );
    for (path, v) in [
        ("controllerImage", &values["controllerImage"]),
        ("runnerImage", &values["runnerImage"]),
    ] {
        let s = v.as_str().unwrap_or_else(|| panic!("{path} is a string"));
        assert!(
            !is_digest_reference(s),
            "{path} is a digest ({s}) — that is this chart's ruling reverted, not a tightening. \
             The tree's own pins stay digests; this chart's two defaults do not"
        );
    }
    // THE TREE ITSELF IS STILL PINNED BY DIGEST. Global Constraint 7 governs
    // `config/`, `logweir.yaml` and the Rust constant, and this ruling does not
    // reach them — `repository_of` above would have panicked otherwise, and this
    // says so out loud.
    assert!(
        is_digest_reference(&controller_image_pin()),
        "config/manager/deployment.yaml still pins the controller image BY DIGEST (GC7)"
    );
    assert!(
        is_digest_reference(&runner_image_constant()),
        "weirkeeper::job::RUNNER_IMAGE is still a digest (GC7)"
    );
    // The four third-party images, still digests, still with provenance.
    for (path, v) in [
        ("minio.image", &values["minio"]["image"]),
        ("minio.mcImage", &values["minio"]["mcImage"]),
        ("demoKafka.image", &values["demoKafka"]["image"]),
        ("ui.image", &values["ui"]["image"]),
    ] {
        let s = v.as_str().unwrap_or_else(|| panic!("{path} is a string"));
        assert!(
            is_digest_reference(s),
            "{path} is not a digest reference: {s} — the third-party images are NOT part of the \
             `latest` ruling"
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
    // The two digests the chart resolved itself carry the command and the date.
    //
    // MOVED, NOT DELETED (Task 38, ruling 15): these four assertions read
    // `charts/logweir/values.yaml` until 2026-09-12, when the owner asked for a
    // values file a reader can take in at a glance — one short line per key and
    // no prose. The PROSE moved to `charts/logweir/README.md` and so did the
    // assertions, unchanged in substance: a third-party digest whose provenance
    // nobody wrote down is still the defect they were added for.
    let text = read("charts/logweir/README.md");
    for command in [
        "docker buildx imagetools inspect apache/kafka:3.7.1",
        "docker buildx imagetools inspect registry.k8s.io/kubectl:v1.34.1",
    ] {
        assert!(
            text.contains(command),
            "charts/logweir/README.md must record the resolution command `{command}` beside the \
             THIRD-PARTY digest (the kindest/node idiom). It moved here from values.yaml under \
             ruling 15; do not drop it"
        );
    }
    assert!(
        text.contains("2026-09-12"),
        "charts/logweir/README.md must record the date the third-party digests were resolved — \
         and the date the owner decided the two Logweir images are named by tag"
    );
    // And the way back to a digest is written down, because a default whose
    // cost is real must say how to undo it.
    assert!(
        text.contains("docker buildx imagetools inspect")
            && text.contains("--set runnerImagePullPolicy="),
        "charts/logweir/README.md must carry the pinning recipe: how to resolve a tag to a \
         digest, and the four --set values that pin it back (moved here from values.yaml under \
         ruling 15)"
    );
    // AND values.yaml POINTS AT THE FILE THE PROSE MOVED TO. A short values file
    // that does not say where the explanations went is a file that lost them.
    assert!(
        read("charts/logweir/values.yaml").contains("charts/logweir/README.md"),
        "values.yaml is short by ruling 15, so it must name `charts/logweir/README.md` once as \
         the place every explanation lives"
    );
    // The defaults are the shipped install: nothing optional on.
    for flag in ["minio", "demoKafka", "ui"] {
        assert_eq!(
            Some(false),
            values[flag]["enabled"].as_bool(),
            "{flag}.enabled defaults to false"
        );
    }
    // AND THE TWO POLICIES FOLLOW THE TAG. `Always` is Kubernetes' own default
    // for a `:latest` reference; `IfNotPresent` under a mutable tag is a pod
    // that never refreshes, and `Never` is a pod that cannot start at all.
    assert_eq!(
        Some("Always"),
        values["imagePullPolicy"].as_str(),
        "the CONTROLLER's pull policy follows its `:latest` image"
    );
    assert_eq!(
        Some("Always"),
        values["runnerImagePullPolicy"].as_str(),
        "and so does the RUNNER Jobs' — rendered as LOGWEIR_RUNNER_PULL_POLICY (Task 37)"
    );
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

/// **Every rendered image is a digest, except the two Logweir images — which
/// must be exactly `<repository>:latest` — and except the author-only render,
/// by name.**
///
/// THE TWO EXCEPTIONS ARE DIFFERENT KINDS OF THING. The Logweir images carry a
/// tag by the owner's decision of 2026-09-12, and the tag is checked EXACTLY:
/// a digest there is the ruling reverted and any other tag is a value nobody
/// chose. The author-only render is exempt by NAME because its whole premise is
/// a locally built tag under `imagePullPolicy: Never`, and a locally built
/// digest changes on every build (plan erratum E19(a)).
///
/// The third-party images — MinIO, mc, apache/kafka, kubectl — are untouched by
/// the ruling and are still digests, in every render including this one.
#[test]
fn chart_lint_every_rendered_image_is_a_digest_except_the_two_logweir_images_and_the_author_only_example(
) {
    let logweir_repos = [
        repository_of(&controller_image_pin()),
        repository_of(&runner_image_constant()),
    ];
    let mut total = 0usize;
    let mut tagged = 0usize;
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
            if let Some(repo) = logweir_repos.iter().find(|r| {
                image.starts_with(&format!("{r}:")) || image.starts_with(&format!("{r}@"))
            }) {
                tagged += 1;
                assert_eq!(
                    format!("{repo}:{LOGWEIR_TAG}"),
                    image,
                    "{name}: {}/{} names a Logweir image as {image}; this chart names both by \
                     `<repository>:{LOGWEIR_TAG}`",
                    d.kind,
                    d.name()
                );
                continue;
            }
            assert!(
                is_digest_reference(&image),
                "{name}: {}/{} references {image} by tag — only the two Logweir images may",
                d.kind,
                d.name()
            );
        }
    }
    assert!(
        total >= 10,
        "only {total} image references across the rendered files"
    );
    assert!(
        tagged >= 1,
        "no rendered file names a Logweir repository at all — the tag arm would be vacuous"
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
    // THE TWO PULL POLICIES ARE A CLOSED ENUM, and `runnerImagePullPolicy` is
    // required — Task 37. An install that set the runner image and forgot its
    // policy would be an install whose Jobs cannot start.
    for policy in ["imagePullPolicy", "runnerImagePullPolicy"] {
        assert_eq!(
            Some("string"),
            schema["properties"][policy]["type"].as_str(),
            "{policy} is a string"
        );
        let enumerated: BTreeSet<String> = schema["properties"][policy]["enum"]
            .as_array()
            .unwrap_or_else(|| panic!("{policy} must carry an enum of the three Kubernetes values"))
            .iter()
            .map(|v| v.as_str().expect("a string").to_string())
            .collect();
        assert_eq!(
            BTreeSet::from([
                "Always".to_string(),
                "IfNotPresent".to_string(),
                "Never".to_string()
            ]),
            enumerated,
            "{policy}'s enum is Kubernetes' three and nothing else"
        );
        assert!(
            schema["required"]
                .as_array()
                .map(|a| a.iter().any(|v| v == policy))
                .unwrap_or(false),
            "{policy} must be required at the top level"
        );
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
        // Task 37: the tag is a named constant, the two repositories are
        // DERIVED from the tree, and arm 5 reads them.
        "LOGWEIR_TAG=\"latest\"",
        "controller_repo=\"${tree_controller%@sha256:*}\"",
        "runner_repo=\"${tree_runner%@sha256:*}\"",
        "$controller_repo:$LOGWEIR_TAG",
        "$runner_repo:$LOGWEIR_TAG",
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
    // Task 37: the runner Jobs' policy is its own value now, and this example
    // is the one place `Never` is still right — images LOADED onto the node.
    assert!(
        author_only.contains("runnerImagePullPolicy: Never"),
        "author-only.values.yaml must set runnerImagePullPolicy: Never beside runnerImage — the \
         chart's default is Always, which would try to pull a tag no registry holds"
    );
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

// ==================================================== Task 38: a chart a stranger can use
//
// Everything below reads CHECKED-IN BYTES — the rendered files
// `scripts/check-chart.sh` regenerates, and the template sources themselves.
// GC22 forbids shelling out from a `#[test]`, so a refusal that can only be
// observed by running `helm` (an unsupported mechanism, both Kafka blocks on at
// once) is asserted as the `fail` the template carries, and the SCRIPT gate is
// what proves `helm` acts on it.

/// Every example under `examples/` has a rendered file beside it, and vice
/// versa — `scripts/check-chart.sh` renders the directory by glob, so this is
/// what catches an example added without its render being committed.
#[test]
fn chart_lint_every_example_has_a_rendered_file() {
    let examples: BTreeSet<String> = files_under("charts/logweir/examples")
        .into_iter()
        .filter_map(|p| {
            p.rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".values.yaml"))
                .map(str::to_string)
        })
        .collect();
    let renders: BTreeSet<String> = files_under("charts/logweir/rendered")
        .into_iter()
        .filter_map(|p| {
            p.rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".yaml"))
                .map(str::to_string)
        })
        .filter(|n| n != "default")
        .collect();
    assert_eq!(
        examples, renders,
        "every charts/logweir/examples/<name>.values.yaml must have a checked-in \
         charts/logweir/rendered/<name>.yaml (plus default.yaml, which has no example). Run \
         `just chart-check` and commit what it regenerates"
    );
    assert!(
        examples.contains("msk"),
        "examples/msk.values.yaml is the copy-and-use example for a real cluster (ruling 13); \
         found {examples:?}"
    );
}

/// **`values.yaml` is SHORT and shows every knob** — the owner's ruling of
/// 2026-09-12: a reader opens it, sees every option with its default, and knows
/// what to set in under a minute. A knob a reader cannot see does not exist, so
/// the empty defaults are listed too.
#[test]
fn chart_lint_values_yaml_is_short_and_shows_every_option() {
    let text = read("charts/logweir/values.yaml");
    let lines = text.lines().count();
    assert!(
        lines <= 130,
        "charts/logweir/values.yaml is {lines} lines. The owner asked for a values file that is \
         read, not skimmed past (~120 lines): one short line per key, no paragraphs, and every \
         explanation in charts/logweir/README.md"
    );
    let values: Value = serde_yaml::from_str(&text).expect("values.yaml parses");
    // EVERY option the chart supports, INCLUDING the empty ones. A missing key
    // here is a knob that exists in a template and nowhere a reader can find it.
    for path in [
        "environment",
        "kubernetes.namespace",
        "kubernetes.nodeSelector",
        "kubernetes.tolerations",
        "kubernetes.affinity",
        "imagePullSecrets",
        "controllerImage",
        "runnerImage",
        "imagePullPolicy",
        "runnerImagePullPolicy",
        "controller.logLevel",
        "controller.resources",
        "controller.nodeSelector",
        "controller.tolerations",
        "controller.affinity",
        "archive.url",
        "archive.s3.endpoint",
        "archive.s3.region",
        "archive.s3.allowHttp",
        "archive.s3.virtualHostedStyle",
        "kafka.enabled",
        "kafka.name",
        "kafka.bootstrapServers",
        "kafka.security.protocol",
        "kafka.security.mechanism",
        "kafka.username",
        "kafka.secretRef",
        "kafka.secretKey",
        "kafka.target.name",
        "kafka.target.bootstrapServers",
        "kafka.target.security.protocol",
        "kafka.target.security.mechanism",
        "kafka.target.username",
        "kafka.target.secretRef",
        "kafka.target.secretKey",
        "kafka.target.markerTopic",
        "minio.enabled",
        "minio.image",
        "minio.mcImage",
        "minio.rootUser",
        "minio.rootPassword",
        "minio.persistence.enabled",
        "minio.persistence.size",
        "minio.persistence.storageClassName",
        "minio.resources",
        "minio.nodeSelector",
        "minio.tolerations",
        "minio.affinity",
        "demoKafka.enabled",
        "demoKafka.image",
        "demoKafka.clusterIds.source",
        "demoKafka.clusterIds.target",
        "demoKafka.seed.recordsPerTopic",
        "demoKafka.resources",
        "demoKafka.nodeSelector",
        "demoKafka.tolerations",
        "demoKafka.affinity",
        "ui.enabled",
        "ui.image",
        "ui.namespaces",
        "ui.nodeSelector",
        "ui.tolerations",
        "ui.affinity",
    ] {
        let mut node = &values;
        for segment in path.split('.') {
            node = &node[segment];
        }
        assert!(
            !node.is_null(),
            "values.yaml does not carry `{path}`. Every option the chart supports appears there \
             with its default, the empty ones included — ruling 15"
        );
    }
    // AND THE DEFAULTS ARE TODAY'S BEHAVIOUR: each new key renders nothing.
    assert_eq!(Some(""), values["environment"].as_str());
    assert_eq!(Some(""), values["kubernetes"]["namespace"].as_str());
    assert_eq!(Some(false), values["kafka"]["enabled"].as_bool());
    assert_eq!(
        Some(0),
        values["imagePullSecrets"].as_sequence().map(Vec::len),
        "imagePullSecrets defaults to an empty list"
    );
    for (owner, key) in [
        ("kubernetes", "tolerations"),
        ("controller", "tolerations"),
        ("minio", "tolerations"),
        ("demoKafka", "tolerations"),
        ("ui", "tolerations"),
    ] {
        assert_eq!(
            Some(0),
            values[owner][key].as_sequence().map(Vec::len),
            "{owner}.{key} defaults to an empty list"
        );
    }
    // The schema accepts exactly this set — `additionalProperties: false` means
    // a key in values.yaml that the schema forgot refuses every install.
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("the schema parses");
    let typed: BTreeSet<String> = schema["properties"]
        .as_object()
        .expect("a properties object")
        .keys()
        .cloned()
        .collect();
    let written: BTreeSet<String> = values
        .as_mapping()
        .expect("a mapping")
        .keys()
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect();
    assert_eq!(
        written, typed,
        "values.schema.json is additionalProperties:false, so every top-level key in values.yaml \
         must be typed there and nothing may be typed that values.yaml does not show"
    );
}

/// **`kafka.enabled: false` renders no `KafkaCluster` at all** — every example
/// that predates ruling 10 renders exactly as it did.
#[test]
fn chart_lint_kafka_renders_nothing_unless_enabled() {
    for name in ["default", "minimal", "demo", "author-only"] {
        let docs = rendered(name);
        let clusters = names_of(&docs, "KafkaCluster");
        assert!(
            clusters.is_empty(),
            "rendered/{name}.yaml carries KafkaCluster {clusters:?} — `kafka.enabled` defaults to \
             false and these values do not set it"
        );
    }
}

/// **The `kafka:` block renders the two cluster objects, with the SASL_SSL +
/// SCRAM-SHA-512 mapping the CRD declares** — the owner's own values file,
/// corrected to this task's keys, rendered and read back.
#[test]
fn chart_lint_kafka_renders_the_cluster_objects_with_the_mapped_auth() {
    let docs = rendered("msk");
    assert_eq!(
        BTreeSet::from(["source".to_string(), "target".to_string()]),
        names_of(&docs, "KafkaCluster"),
        "examples/msk.values.yaml renders a source and a target KafkaCluster"
    );
    let example = read("charts/logweir/examples/msk.values.yaml");
    for (name, role, secret) in [
        ("source", "source", "kafbat-ui-msk-credentials"),
        ("target", "target", "kafbat-scratch-credentials"),
    ] {
        let doc = find(&docs, "KafkaCluster", name);
        let spec = &doc.value["spec"];
        assert_eq!(Some(role), spec["role"].as_str(), "{name}'s role");
        // THE MAPPING, which is the whole point of the block: SASL_SSL +
        // SCRAM-SHA-512 is `scramSha512` over TLS, and the CRD's enum has no
        // third value.
        assert_eq!(
            Some("scramSha512"),
            spec["auth"]["mode"].as_str(),
            "{name}: SASL_SSL + SCRAM-SHA-512 maps to auth.mode scramSha512"
        );
        assert_eq!(
            Some(true),
            spec["auth"]["tls"].as_bool(),
            "{name}: SASL_SSL is TLS; a chart that rendered tls:false here would install and fail \
             to connect"
        );
        assert_eq!(Some("kafbat"), spec["auth"]["username"].as_str());
        assert_eq!(
            Some(secret),
            spec["auth"]["secretRef"]["name"].as_str(),
            "{name}: the Secret the adopter named, unread by Logweir"
        );
        // THE ADDRESSES ARE THE EXAMPLE'S, SPLIT OUT OF ITS COMMA-SEPARATED
        // STRING — the shape the AWS console hands you.
        let servers: Vec<&str> = spec["bootstrapServers"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{name}.spec.bootstrapServers is a list"))
            .iter()
            .map(|v| v.as_str().expect("a host:port string"))
            .collect();
        assert!(
            !servers.is_empty(),
            "{name} renders at least one bootstrap server"
        );
        for server in &servers {
            assert!(
                example.contains(server),
                "{name} renders `{server}`, which examples/msk.values.yaml does not name — the \
                 splitter invented an address"
            );
            assert!(
                server.ends_with(":9096"),
                "{name}: `{server}` — SASL/SCRAM on MSK is port 9096"
            );
        }
    }
    // A comma-separated STRING became a LIST of more than one entry: the split
    // actually happened, rather than one long value being passed through.
    assert!(
        find(&docs, "KafkaCluster", "source").value["spec"]["bootstrapServers"]
            .as_sequence()
            .map(|s| s.len() >= 2)
            .unwrap_or(false),
        "the source's comma-separated string must be split into two or more entries, or this \
         assertion proves nothing about the splitter"
    );
    // The scratch proof travels with the target and nowhere else.
    assert_eq!(
        Some("logweir.scratch"),
        find(&docs, "KafkaCluster", "target").value["spec"]["markerTopic"].as_str(),
        "the target carries the marker topic that proves it is scratch"
    );
    assert!(
        find(&docs, "KafkaCluster", "source").value["spec"]["markerTopic"].is_null(),
        "a SOURCE has no marker topic — that field is what authorises a scratch RESTORE target"
    );
    // RENDERED INTO THE RELEASE NAMESPACE, with the chart's labels. The release
    // namespace is `helm -n`'s — `logweir-system`, what `scripts/check-chart.sh`
    // renders every example with, and what every other template in this chart
    // writes. It is NOT `kubernetes.namespace`, which this example sets to
    // `kafka` and which ruling 11 says does not move the install: a template
    // that reached for `.Values.kubernetes.namespace` here would put the cluster
    // objects in a namespace Helm never installed into, where neither the
    // operator nor the runner ServiceAccount is.
    let key_namespace =
        serde_yaml::from_str::<Value>(&read("charts/logweir/examples/msk.values.yaml"))
            .expect("the example parses")["kubernetes"]["namespace"]
            .as_str()
            .expect("examples/msk.values.yaml sets kubernetes.namespace")
            .to_string();
    assert_ne!(
        "logweir-system", key_namespace,
        "examples/msk.values.yaml's kubernetes.namespace must differ from the release namespace, \
         or the assertion below cannot tell the two sources apart"
    );
    for name in ["source", "target"] {
        let doc = find(&docs, "KafkaCluster", name);
        assert_eq!(
            Some("logweir-system"),
            doc.value["metadata"]["namespace"].as_str(),
            "{name} must render into the RELEASE namespace (these files are rendered with \
             `helm template -n logweir-system`), never into `kubernetes.namespace: \
             {key_namespace}` — that key feeds ui.namespaces and nothing else"
        );
        assert_eq!(
            Some("logweir"),
            doc.value["metadata"]["labels"]["app.kubernetes.io/name"].as_str(),
            "{name} carries the chart's labels"
        );
    }
}

/// **Every protocol/mechanism pair the chart accepts, and the refusal for every
/// other one** — read from the template that does the mapping, because a
/// refusal cannot be observed in a rendered file by definition, and because the
/// two arms a rendered example does not exercise would otherwise be untested.
#[test]
fn chart_lint_kafka_maps_every_supported_mechanism_and_refuses_the_rest() {
    let helpers = read("charts/logweir/templates/_helpers.tpl");
    // PLAINTEXT -> plaintext, no TLS.
    assert!(
        helpers.contains(r#"{{- if eq $protocol "PLAINTEXT" -}}"#)
            && helpers.contains(r#"{{- $mode = "plaintext" -}}"#),
        "_helpers.tpl must map PLAINTEXT onto auth.mode plaintext"
    );
    // SASL_SSL + SCRAM-SHA-512 -> scramSha512 over TLS.
    assert!(
        helpers.contains(
            r#"{{- else if and (eq $protocol "SASL_SSL") (eq $mechanism "SCRAM-SHA-512") -}}"#
        ) && helpers.contains(r#"{{- $tls = true -}}"#),
        "_helpers.tpl must map SASL_SSL + SCRAM-SHA-512 onto scramSha512 WITH tls"
    );
    // SASL_PLAINTEXT + SCRAM-SHA-512 -> scramSha512 without TLS. The arm no
    // rendered example exercises, and the one a careless edit collapses into
    // the SASL_SSL branch.
    let sasl_plaintext = helpers
        .find(r#"{{- else if and (eq $protocol "SASL_PLAINTEXT") (eq $mechanism "SCRAM-SHA-512") -}}"#)
        .expect("_helpers.tpl must map SASL_PLAINTEXT + SCRAM-SHA-512 onto scramSha512");
    let after = &helpers[sasl_plaintext..];
    let arm_end = after
        .find("{{- else -}}")
        .expect("the mapping ends in an else");
    assert!(
        !after[..arm_end].contains("$tls = true"),
        "the SASL_PLAINTEXT arm must NOT set tls — SASL_PLAINTEXT is SASL over a cleartext \
         transport, and a chart claiming TLS there installs and cannot connect"
    );
    // AND EVERYTHING ELSE IS A REFUSAL AT RENDER TIME, naming what is supported.
    let refusal = helpers
        .split("{{- else -}}")
        .nth(1)
        .expect("a final else arm")
        .to_string();
    assert!(
        refusal.contains("fail") && refusal.contains("is not supported"),
        "an unsupported protocol/mechanism pair must be a render-time `fail`, not a rendered \
         object: the CRD's enum is plaintext|scramSha512 and the client speaks SCRAM-SHA-512 only"
    );
    for named in ["SCRAM-SHA-512", "SASL_SSL", "SASL_PLAINTEXT", "PLAINTEXT"] {
        assert!(
            refusal.contains(named),
            "the refusal must name `{named}` — a reader who hit it has to learn what IS supported"
        );
    }
    // The secret key the operator actually projects, and no other.
    let projected = read("crates/weirkeeper/src/controllers/restore.rs")
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("pub const TARGET_PASSWORD_SECRET_KEY: &str = \"")
                .and_then(|r| r.strip_suffix("\";"))
                .map(str::to_string)
        })
        .expect("restore.rs declares TARGET_PASSWORD_SECRET_KEY");
    assert!(
        helpers.contains(&format!(r#"(ne $c.secretKey "{projected}")"#)),
        "_helpers.tpl must refuse any `secretKey` but `{projected}` — the ONE key name the \
         operator projects into the probe. Any other value renders a KafkaCluster whose probe \
         cannot read its credential"
    );
    // And both blocks on at once is a refusal too, in the template that renders
    // the objects.
    let template = read("charts/logweir/templates/kafka/kafkacluster.yaml");
    assert!(
        template.contains("{{- fail \"kafka.enabled and demoKafka.enabled are both true."),
        "kafka.enabled beside demoKafka.enabled must be a render-time `fail`: demoKafka brings \
         its own two brokers and its own cluster objects"
    );
    assert!(
        template.contains("demoKafka.enabled: false") && template.contains("kafka.enabled: false"),
        "that refusal must say which flag to turn off, for each of the two intents"
    );
}

/// **Node placement reaches every pod the chart renders** — the controller,
/// MinIO and its seed Job, the UI (from the MSK example) and a demo broker and
/// its seed Job (from the demo example, the only render that carries them).
#[test]
fn chart_lint_placement_reaches_every_pod() {
    let msk_selector =
        serde_yaml::from_str::<Value>(&read("charts/logweir/examples/msk.values.yaml"))
            .expect("the example parses")["kubernetes"]["nodeSelector"]
            .clone();
    let msk_keys: Vec<String> = msk_selector
        .as_mapping()
        .expect("examples/msk.values.yaml sets kubernetes.nodeSelector")
        .keys()
        .map(|k| k.as_str().expect("a string key").to_string())
        .collect();
    assert!(
        !msk_keys.is_empty(),
        "examples/msk.values.yaml must set a nodeSelector, or this test proves nothing"
    );

    let mut checked = 0usize;
    for (render, kind, name) in [
        ("msk", "Deployment", "weirkeeper"),
        ("msk", "Deployment", "logweir-minio"),
        ("msk", "Job", "logweir-minio-seed"),
        ("msk", "Deployment", "logweir-ui"),
    ] {
        let docs = rendered(render);
        let spec = pod_spec(find(&docs, kind, name));
        for key in &msk_keys {
            assert!(
                !spec["nodeSelector"][key.as_str()].is_null(),
                "{render}: {kind}/{name} has no nodeSelector key `{key}` — kubernetes.nodeSelector \
                 must reach EVERY pod the chart renders, or the chart cannot be installed on a \
                 cluster whose nodes are labelled"
            );
        }
        let tolerations = spec["tolerations"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{render}: {kind}/{name} carries no tolerations"));
        assert!(
            tolerations
                .iter()
                .any(|t| t["key"].as_str() == Some("kafka")),
            "{render}: {kind}/{name} does not tolerate the tainted nodepool"
        );
        checked += 1;
    }

    // THE DEMO BROKERS AND THEIR SEED JOB, from the one render that has them.
    let demo_values: Value =
        serde_yaml::from_str(&read("charts/logweir/examples/demo.values.yaml"))
            .expect("demo.values.yaml parses");
    let demo_keys: Vec<String> = demo_values["kubernetes"]["nodeSelector"]
        .as_mapping()
        .expect("examples/demo.values.yaml sets kubernetes.nodeSelector")
        .keys()
        .map(|k| k.as_str().expect("a string key").to_string())
        .collect();
    let demo = rendered("demo");
    for (kind, name) in [
        ("StatefulSet", "logweir-kafka-source"),
        ("StatefulSet", "logweir-kafka-target"),
        ("Job", "logweir-kafka-seed"),
    ] {
        let spec = pod_spec(find(&demo, kind, name));
        for key in &demo_keys {
            assert!(
                !spec["nodeSelector"][key.as_str()].is_null(),
                "demo: {kind}/{name} has no nodeSelector key `{key}` — the demo brokers are pods \
                 this chart renders, so placement must reach them too"
            );
        }
        checked += 1;
    }
    assert_eq!(7, checked, "seven pod specs were checked, not {checked}");

    // AND THE DEFAULT RENDER HAS NONE OF IT: an empty value renders nothing.
    for doc in rendered("default") {
        let spec = pod_spec(&doc);
        if spec.is_null() {
            continue;
        }
        for key in ["nodeSelector", "tolerations", "affinity"] {
            assert!(
                spec[key].is_null(),
                "rendered/default.yaml {}/{} carries `{key}` — the placement defaults are empty \
                 and must render nothing at all",
                doc.kind,
                doc.name()
            );
        }
    }
}

/// **`imagePullSecrets` reach the controller ServiceAccount AND the runner
/// ServiceAccount** — the runner one is the whole mechanism by which Jobs the
/// OPERATOR creates can pull from a private registry without an operator
/// change, because a pod inherits its ServiceAccount's pull secrets.
#[test]
fn chart_lint_image_pull_secrets_reach_both_service_accounts() {
    let example: Value = serde_yaml::from_str(&read("charts/logweir/examples/msk.values.yaml"))
        .expect("the example parses");
    let wanted: Vec<String> = example["imagePullSecrets"]
        .as_sequence()
        .expect("examples/msk.values.yaml sets imagePullSecrets")
        .iter()
        .map(|s| s["name"].as_str().expect("a secret name").to_string())
        .collect();
    assert!(
        !wanted.is_empty(),
        "the example must name at least one pull secret, or this test proves nothing"
    );

    let docs = rendered("msk");
    for account in ["weirkeeper", "logweir-runner", "logweir-ui"] {
        let sa = find(&docs, "ServiceAccount", account);
        let got: Vec<String> = sa.value["imagePullSecrets"]
            .as_sequence()
            .unwrap_or_else(|| {
                panic!(
                    "ServiceAccount/{account} carries no imagePullSecrets. On `logweir-runner` \
                     that is the ONLY path a runner Job has to a private registry"
                )
            })
            .iter()
            .map(|s| s["name"].as_str().expect("a name").to_string())
            .collect();
        assert_eq!(wanted, got, "ServiceAccount/{account}'s pull secrets");
    }
    // The pods that run under no ServiceAccount of ours carry them directly.
    for (kind, name) in [
        ("Deployment", "logweir-minio"),
        ("Job", "logweir-minio-seed"),
    ] {
        let spec = pod_spec(find(&docs, kind, name));
        let got: Vec<String> = spec["imagePullSecrets"]
            .as_sequence()
            .unwrap_or_else(|| {
                panic!("{kind}/{name} runs under the `default` ServiceAccount, so its pull secrets must be on its PodSpec")
            })
            .iter()
            .map(|s| s["name"].as_str().expect("a name").to_string())
            .collect();
        assert_eq!(wanted, got, "{kind}/{name}'s pull secrets");
    }
    // Empty by default, everywhere.
    for doc in rendered("default") {
        assert!(
            doc.value["imagePullSecrets"].is_null(),
            "rendered/default.yaml {}/{} carries imagePullSecrets — the default is an empty list \
             and must render nothing",
            doc.kind,
            doc.name()
        );
    }
}

/// **`environment` is a label on every object the chart renders, and nothing
/// else.** The six CRDs are the one exception and not an omission: Helm copies
/// `crds/` verbatim and never templates it, which `check-chart.sh` holds
/// byte-identical to `config/crd/`.
#[test]
fn chart_lint_the_environment_label_is_on_every_rendered_object() {
    let example: Value = serde_yaml::from_str(&read("charts/logweir/examples/msk.values.yaml"))
        .expect("the example parses");
    let want = example["environment"]
        .as_str()
        .expect("examples/msk.values.yaml sets environment");
    assert!(!want.is_empty(), "the example must set a non-empty value");

    let mut labelled = 0usize;
    let mut missing = Vec::new();
    for doc in rendered("msk") {
        if doc.kind == "CustomResourceDefinition" {
            continue;
        }
        match doc.value["metadata"]["labels"]["logweir.dev/environment"].as_str() {
            Some(v) if v == want => labelled += 1,
            other => missing.push(format!("{}/{} -> {other:?}", doc.kind, doc.name())),
        }
    }
    assert!(
        missing.is_empty(),
        "{} rendered object(s) do not carry `logweir.dev/environment: {want}`:\n{}",
        missing.len(),
        missing.join("\n")
    );
    assert!(
        labelled >= 15,
        "only {labelled} object(s) were checked for the environment label; the MSK example \
         renders far more than that, so this lint would be near-vacuous"
    );
    // AND IT SWITCHES NOTHING. The default is empty and renders no label at all.
    for doc in rendered("demo") {
        assert!(
            doc.value["metadata"]["labels"]["logweir.dev/environment"].is_null(),
            "rendered/demo.yaml {}/{} carries an environment label — `environment` defaults to \
             empty and must render nothing",
            doc.kind,
            doc.name()
        );
    }
}

/// **The copy-and-use path is written down**: the chart README opens by saying
/// to copy the directory and install it, the MSK example is short and carries
/// the keys this task defines, and the MSK facts a values file has no room for
/// live in the README.
#[test]
fn chart_lint_the_readme_opens_with_copy_this_directory() {
    let readme = read("charts/logweir/README.md");
    assert!(
        readme.contains("## Copy this directory and install it"),
        "charts/logweir/README.md must OPEN with `## Copy this directory and install it` — the \
         chart is self-contained and one command installs it (ruling 13)"
    );
    let copy = readme
        .find("## Copy this directory and install it")
        .expect("the heading is there");
    let installs = readme
        .find("## What it installs")
        .expect("the second section");
    assert!(
        copy < installs,
        "the copy-and-use section must come FIRST, before `## What it installs`"
    );
    assert!(
        readme[..installs].contains("helm upgrade --install logweir . -n"),
        "the copy-and-use section must show the whole command"
    );
    // The MSK facts, measured 2026-09-12, that an adopter on MSK needs and a
    // short values file cannot hold.
    for fact in [
        "9096",
        "Secrets Manager",
        "UNRENDERABLE_CREDENTIAL_CHARACTERS",
        "DescribeCluster",
        "MSK IAM authentication is not implemented",
    ] {
        assert!(
            readme.contains(fact),
            "charts/logweir/README.md must state the MSK fact `{fact}`"
        );
    }
    // The named gap: runner Jobs get pull secrets and do NOT get placement.
    // Read against a whitespace-collapsed copy, because both sentences are
    // wrapped across lines: a needle that pinned the wrap column would go red on
    // a reflow that changed no meaning, and a needle that avoided the wrap would
    // be too short to mean anything.
    let flowed = readme.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flowed.contains("Node placement **does not**"),
        "the README must NAME the runner-Job placement gap rather than leave it silent"
    );
    assert!(
        flowed.contains(
            "`logweir-runner` ServiceAccount, because a pod inherits its ServiceAccount's pull \
             secrets"
        ),
        "the README must say HOW pull secrets reach a runner Job — through the runner \
         ServiceAccount, which is the whole reason they reach Jobs the chart never renders"
    );
    // The two Docker Hub facts an adopter needs (ruling 16).
    assert!(
        readme.contains("created by a push is public by default"),
        "the README must say that a Docker Hub repository created by a push is public by default \
         — the opposite of GHCR's"
    );
    assert!(
        readme.contains("rate-limited per source IP"),
        "the README must say anonymous Docker Hub pulls are rate-limited per IP, which is why a \
         pull secret is useful on a public image too"
    );
    // The example itself: short, and the keys this task defines.
    let example = read("charts/logweir/examples/msk.values.yaml");
    let lines = example.lines().count();
    assert!(
        lines <= 60,
        "examples/msk.values.yaml is {lines} lines; ruling 15 asks for the same short shape as \
         values.yaml, with the MSK facts in the README"
    );
    for key in [
        "environment:",
        "kubernetes:",
        "nodeSelector:",
        "tolerations:",
        "imagePullSecrets:",
        "kafka:",
        "enabled: true",
        "secretRef:",
    ] {
        assert!(
            example.contains(key),
            "examples/msk.values.yaml must set `{key}`"
        );
    }
}

/// **`docs/install.md` carries path (d), and it says the two things that make
/// it necessary**: nothing is published, and the controller image cannot be
/// cross-compiled.
#[test]
fn chart_lint_install_md_carries_bring_your_own_registry() {
    let install = read("docs/install.md");
    assert!(
        install.contains("### (d) Bring your own registry"),
        "docs/install.md must carry the section `### (d) Bring your own registry`"
    );
    let section = install
        .split("### (d) Bring your own registry")
        .nth(1)
        .expect("the section body")
        .split("\n---")
        .next()
        .expect("the section ends")
        .to_string();
    for needle in [
        "blocked: images not published",
        "aws-lc-sys",
        "E19(c)",
        "ecr",
        "controllerImage",
        "runnerImage",
        "imagePullPolicy",
        "imagePullSecrets",
    ] {
        assert!(
            section.to_lowercase().contains(&needle.to_lowercase()),
            "docs/install.md (d) must name `{needle}`: the images are not published, the \
             controller image cannot be cross-compiled, and the four values a bring-your-own \
             registry install sets"
        );
    }
    // Ruling 17's sentence: a push-created repository takes the account's
    // default privacy, and a private one is the owner's exact 403.
    assert!(
        section.contains("403 Forbidden"),
        "docs/install.md (d) must name the exact error a private auto-created Docker Hub \
         repository answers an anonymous pull with"
    );
    assert!(
        section.contains("created by a push is public by default"),
        "docs/install.md (d) must say a Docker Hub repository created by a push is public by \
         default — GHCR's packages start private, which is the opposite"
    );
    assert!(
        section.contains("rate-limited per source IP"),
        "docs/install.md (d) must say anonymous Docker Hub pulls are rate-limited per IP"
    );
    // And nothing in it claims a publication.
    assert!(
        !section.contains("has been published") && !section.contains("are published"),
        "docs/install.md (d) must not claim anything is published — release.yml has never run"
    );
}
