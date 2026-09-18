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
//!   and the twenty-two shipped UI files — asserted here with `std::fs` and in
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

/// The twenty-two shipped UI files: everything under `ui/` except `*.md` and
/// `tests/` — `scripts/check-ui-offline.sh`'s scope, by construction.
fn shipped_ui_files() -> Vec<String> {
    let files: Vec<String> = files_under("ui")
        .into_iter()
        .filter(|p| !p.ends_with(".md") && !p.starts_with("ui/tests/"))
        .collect();
    assert_eq!(
        22,
        files.len(),
        "the shipped UI is twenty-two files (ui/*.html, ui/*.js, ui/*.css, ui/pages/*); found \
         {files:?}"
    );
    files
}

/// **The UI image's repository, DERIVED** — the namespace of
/// `weirkeeper::job::RUNNER_IMAGE` with the name `logweir-ui`, which is what
/// `Dockerfile.ui` builds and what `release.yml` publishes.
///
/// It is derived rather than spelt for the reason `scripts/check-chart.sh`'s
/// arm 7 gives: the UI image has no digest pin in the tree to read a repository
/// off — this chart is its only reference — so what must not drift is the
/// NAMESPACE, and taking it from the runner pin is what makes a namespace move
/// carry all three images at once.
///
/// `ui_key` — a UI file's ConfigMap key (`ui/pages/approvals.js` ->
/// `pages__approvals.js`) — lived here until Task 39 and is DELETED with the
/// ConfigMap it keyed and with the `logweir.ui.key` template helper. A test
/// helper for an object nothing renders is a helper the next reader has to
/// prove is dead, and `cargo clippy -- -D warnings` refuses it anyway.
fn ui_repository() -> String {
    let runner = repository_of(&runner_image_constant());
    let (namespace, _) = runner
        .rsplit_once('/')
        .unwrap_or_else(|| panic!("RUNNER_IMAGE's repository `{runner}` names no namespace"));
    format!("{namespace}/logweir-ui")
}

// ============================================================== the copies

/// **The chart's `crds/` is a byte-identical copy of `config/crd/`** — every
/// file, no extra, no missing one. The script says the same with `cmp`.
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
    // THE NUMBER IS WRITTEN DOWN, and this crate cannot read
    // `weirkeeper::crds::KINDS` to derive it — `logweir` declares no edge to
    // `weirkeeper` and adding one to satisfy a test would change the
    // dependency graph the one-signer and pure-core gates police. ADR 0008
    // records fourteen kinds: Amendment A's six, Amendment F's three and
    // Amendment G's five.
    assert_eq!(
        14,
        source.len(),
        "config/crd holds fourteen CRDs: {source:?}"
    );
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

/// **The chart carries NO copy of the UI.** The proxy serves the page from the
/// `logweir-ui` IMAGE; its only ConfigMap mount is the non-secret namespace
/// context that the actual browser runtime reads.
///
/// RENAMED FROM `chart_lint_ui_copy_is_byte_identical_and_carries_nothing_else`
/// (STANDING RULE 19), and the rename is the whole change of substance. That
/// test asserted that `charts/logweir/ui/` — fourteen duplicated files — was
/// byte-identical to `ui/`. Both the directory and the ConfigMap built from it
/// are gone, so the assertion it made is not weakened here, it MOVED, to
/// `scripts/check-image-ui.sh` check 1: that gate computes the sha256 of every
/// file inside the image that `kubectl proxy --www=/ui` will serve and of every
/// file under `ui/`, and compares them. A copy in a chart can be right while
/// the artefact a browser loads is wrong; the image gate reads the artefact.
///
/// WHAT THIS TEST CAN STILL SAY, and does: the chart holds no second copy of
/// the page and the only UI ConfigMap contains one generated runtime context,
/// not page bytes. A mount at `/ui` would shadow the image and make the image
/// hash gate meaningless; a narrowly mounted `/ui/runtime` context does not.
#[test]
fn chart_lint_the_chart_carries_no_ui_copy_and_mounts_only_runtime_namespace_context() {
    // The twenty-two are in the tree, where the image gate hashes
    // them from. If this ever drifts, `scripts/check-image-ui.sh` check 1 is
    // comparing against the wrong set.
    let shipped = shipped_ui_files();
    assert_eq!(22, shipped.len());

    let under_chart: Vec<String> = files_under("charts/logweir")
        .into_iter()
        .filter(|p| p.starts_with("charts/logweir/ui/"))
        .collect();
    assert!(
        under_chart.is_empty(),
        "charts/logweir/ui/ is back: {under_chart:?}. Task 39 deleted it — the page is \
         delivered by `ui.image` (Dockerfile.ui, docker.io/<ns>/logweir-ui), and \
         scripts/check-image-ui.sh hashes what the image serves against ui/. A copy under the \
         chart is a second source of the page that nothing hashes."
    );

    // The demo render is the one with `ui.enabled: true`. Its only UI
    // ConfigMap is one generated JS file describing the same namespace set as
    // the RoleBindings; it cannot contain an alternate page.
    let docs = rendered("demo");
    let ui_configmaps: Vec<String> = docs
        .iter()
        .filter(|d| d.kind == "ConfigMap" && d.name().contains("-ui"))
        .map(|d| d.name())
        .collect();
    assert_eq!(
        1,
        ui_configmaps.len(),
        "exactly one runtime ConfigMap is rendered"
    );
    assert!(ui_configmaps[0].starts_with("logweir-ui-runtime-"));
    let runtime = docs
        .iter()
        .find(|d| d.kind == "ConfigMap" && d.name() == ui_configmaps[0])
        .expect("the named runtime ConfigMap exists");
    assert_eq!(
        Some(true),
        runtime.value["immutable"].as_bool(),
        "runtime JS is immutable once installed"
    );
    let context = runtime.value["data"]["runtime.js"]
        .as_str()
        .expect("the runtime ConfigMap has its JS context");
    assert_eq!(
        "window.LOGWEIR_NAMESPACE_CONTEXT = Object.freeze({\"allowed\": [\"logweir-system\"], \"selected\": \"logweir-system\"});\n",
        context,
        "the single release namespace reaches the browser runtime explicitly"
    );
    let ui = find(&docs, "Deployment", "logweir-ui");
    let volumes = pod_spec(ui)["volumes"].as_sequence().expect("volumes");
    let runtime_volume = volumes
        .iter()
        .find(|v| v["name"].as_str() == Some("runtime"))
        .expect("the proxy mounts its runtime namespace context");
    assert_eq!(
        Some(ui_configmaps[0].as_str()),
        runtime_volume["configMap"]["name"].as_str(),
        "the runtime volume names the generated context, not a page ConfigMap"
    );
    let mounts = container(ui)["volumeMounts"]
        .as_sequence()
        .expect("volumeMounts");
    assert!(
        mounts.iter().any(|m| m["name"].as_str() == Some("runtime") && m["mountPath"].as_str() == Some("/ui/runtime.js") && m["subPath"].as_str() == Some("runtime.js") && m["readOnly"].as_bool() == Some(true)),
        "the only ConfigMap mount replaces the read-only runtime context file, never /ui: {mounts:?}"
    );
    assert!(
        !mounts
            .iter()
            .any(|m| m["mountPath"].as_str() == Some("/ui")),
        "nothing may be mounted over /ui: the image remains the page source"
    );
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
    let mut chart_roles = names_of(&chart, "ClusterRole");
    assert!(
        chart_roles.remove("logweir-identity-singleton"),
        "managed Helm must carry the authority-free singleton marker"
    );
    assert_eq!(
        names_of(&install, "ClusterRole"),
        chart_roles,
        "apart from the managed identity singleton, ClusterRoles must match logweir.yaml"
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

    // Every CRD, by name, and byte-for-byte the same documents.
    assert_eq!(
        names_of(&install, "CustomResourceDefinition"),
        names_of(&chart, "CustomResourceDefinition"),
        "the CRD names differ"
    );
    assert_eq!(14, names_of(&chart, "CustomResourceDefinition").len());
    for name in names_of(&chart, "CustomResourceDefinition") {
        assert_eq!(
            find(&install, "CustomResourceDefinition", &name).value["spec"],
            find(&chart, "CustomResourceDefinition", &name).value["spec"],
            "CRD {name}'s spec differs"
        );
    }

    // The NetworkPolicy and the controller/runner ServiceAccounts. The
    // identity bootstrap account is asserted separately with its narrow Role.
    let base_policy = &find(&install, "NetworkPolicy", "logweir-runner-egress").value["spec"];
    let mut managed_policy =
        find(&chart, "NetworkPolicy", "logweir-runner-egress").value["spec"].clone();
    managed_policy["podSelector"]["matchExpressions"]
        .as_sequence_mut()
        .expect("managed policy expressions")
        .retain(|expression| expression["key"].as_str() != Some("logweir.dev/identity-authority"));
    assert_eq!(
        base_policy, &managed_policy,
        "the Helm policy may only add the identity-authority exclusion; the low-level manifest runs no bootstrap hook"
    );
    let runner_sa = find(&chart, "ServiceAccount", "logweir-runner");
    assert_eq!(
        Some(false),
        runner_sa.value["automountServiceAccountToken"].as_bool(),
        "the runner ServiceAccount sets automountServiceAccountToken: false"
    );
    find(&chart, "ServiceAccount", "weirkeeper");

    // The chart additionally carries PLAT-02.1's retained identity objects,
    // namespaced Role/Binding and short-lived hook Job. Every other kind still
    // agrees with the base install file.
    let mut chart_kinds: BTreeSet<String> = chart.iter().map(|d| d.kind.clone()).collect();
    let install_kinds: BTreeSet<String> = install
        .iter()
        .map(|d| d.kind.clone())
        .filter(|k| k != "Namespace")
        .collect();
    chart_kinds.remove("Namespace");
    let mut expected_kinds = install_kinds;
    expected_kinds.extend(
        ["Secret", "ConfigMap", "Role", "RoleBinding", "Job"]
            .into_iter()
            .map(str::to_string),
    );
    assert_eq!(expected_kinds, chart_kinds, "the only new default-render kinds are PLAT-02.1's retained identity objects and bootstrap resources");
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
    // TASK 39'S THIRD LOGWEIR IMAGE. `ui.image` stopped being a third-party
    // kubectl digest and became Logweir's own `logweir-ui`, under the namespace
    // the runner pin names — derived, never spelt, so a namespace move carries
    // all three. `scripts/check-chart.sh` arm 7 says the same in shell.
    assert_eq!(
        Some(format!("{}:{LOGWEIR_TAG}", ui_repository())).as_deref(),
        values["ui"]["image"].as_str(),
        "values.yaml ui.image must be `<the runner pin's namespace>/logweir-ui:{LOGWEIR_TAG}` — \
         the image Dockerfile.ui builds and release.yml publishes. A `registry.k8s.io/kubectl` \
         digest here is the page back in a ConfigMap"
    );
    for (path, v) in [
        ("controllerImage", &values["controllerImage"]),
        ("runnerImage", &values["runnerImage"]),
        ("ui.image", &values["ui"]["image"]),
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
    // The THREE third-party images left, still digests, still with provenance.
    // There were four until Task 39: `ui.image` was a `registry.k8s.io/kubectl`
    // digest, and that digest did not disappear — it moved to `Dockerfile.ui`'s
    // `FROM`, where it is still pinned under Global Constraint 7 and where
    // `scripts/check-image-ui.sh` check 3 holds the image's own label and its
    // baked inventory to it.
    for (path, v) in [
        ("minio.image", &values["minio"]["image"]),
        ("minio.mcImage", &values["minio"]["mcImage"]),
        ("demoKafka.image", &values["demoKafka"]["image"]),
    ] {
        let s = v.as_str().unwrap_or_else(|| panic!("{path} is a string"));
        assert!(
            is_digest_reference(s),
            "{path} is not a digest reference: {s} — the third-party images are NOT part of the \
             `latest` ruling"
        );
    }
    // AND THE UI IMAGE'S BASE IS STILL THE PINNED KUBECTL, in Dockerfile.ui.
    // Without this the digest would simply have vanished from every assertion
    // in this file when it left values.yaml.
    let dockerfile_ui = read("Dockerfile.ui");
    assert!(
        dockerfile_ui.contains("FROM registry.k8s.io/kubectl@sha256:"),
        "Dockerfile.ui must build FROM a DIGEST-pinned registry.k8s.io/kubectl (Global \
         Constraint 7). That pin moved here from values.yaml's ui.image in Task 39; a tag there \
         would let the base change under a build nobody re-ran"
    );
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
    // `registry.k8s.io/kubectl:v1.34.1` IS STILL HERE AFTER TASK 39 and that is
    // deliberate: the digest moved from `ui.image` to `Dockerfile.ui`'s `FROM`,
    // but it is still a third-party image this project redistributes, and the
    // command and date that resolved it are still what a reader needs.
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

const SHIPPED_KINDS: [&str; 11] = [
    "CustomResourceDefinition",
    "ServiceAccount",
    "ClusterRole",
    "ClusterRoleBinding",
    "Deployment",
    "NetworkPolicy",
    "Secret",
    "ConfigMap",
    "Role",
    "RoleBinding",
    "Job",
];

/// **Under the defaults and under the minimal example, the three optional
/// components render NOTHING** — no StatefulSet, no Job, no Service, no
/// ConfigMap, no Secret, no PVC, no RoleBinding, no object named after them.
#[test]
fn chart_lint_optional_components_render_nothing_under_defaults() {
    for name in ["default", "minimal", "author-only", "identity-external"] {
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

/// PLAT-02.1: the chart renders no private bytes, retains both halves of the
/// identity record, and delegates bootstrap to a short-lived signer-capable
/// Job with name-scoped API access. The controller's ClusterRole remains
/// Secret-blind.
#[test]
fn chart_lint_identity_bootstrap_is_persistent_public_and_least_privilege() {
    let docs = rendered("default");
    let secret = find(&docs, "Secret", "logweir-signing-key");
    let public = find(&docs, "ConfigMap", "logweir-signing-trust");
    for object in [secret, public] {
        assert_eq!(
            Some("keep"),
            object.value["metadata"]["annotations"]["helm.sh/resource-policy"].as_str(),
            "{}/{} must survive uninstall/reinstall",
            object.kind,
            object.name()
        );
        assert_eq!(
            Some("uninitialized"),
            object.value["metadata"]["annotations"]["logweir.dev/identity-state"].as_str()
        );
        assert!(
            object.value["data"]
                .as_mapping()
                .is_some_and(|data| data.is_empty()),
            "{}/{} is a placeholder; Helm must render no key bytes",
            object.kind,
            object.name()
        );
        assert_eq!(
            Some("pre-install,pre-upgrade"),
            object.value["metadata"]["annotations"]["helm.sh/hook"].as_str(),
            "{}/{} must be a creation-only hook, never an ordinary rollback-reconciled manifest",
            object.kind,
            object.name()
        );
    }
    let rendered_bytes = read("charts/logweir/rendered/default.yaml");
    assert!(
        !rendered_bytes.contains("BEGIN PRIVATE KEY"),
        "a private key must never enter Helm render/release state"
    );
    let template = read("charts/logweir/templates/identity.yaml");
    assert!(
        template.contains("lookup \"v1\" \"Secret\"")
            && template.contains("lookup \"v1\" \"ConfigMap\""),
        "connected lookup must omit established retained objects instead of importing private bytes into Helm state"
    );
    assert!(
        !template.contains("helm.sh/hook: pre-install,pre-upgrade,pre-rollback"),
        "stored old revisions must never recreate empty placeholders during rollback"
    );

    let job = find(&docs, "Job", "logweir-identity-bootstrap");
    assert_eq!(
        Some(false),
        find(&docs, "ServiceAccount", "logweir-identity-bootstrap").value
            ["automountServiceAccountToken"]
            .as_bool(),
        "the bootstrap account defaults to no token; only its hook Pod explicitly opts in"
    );
    assert_eq!(
        Some("post-install,post-upgrade,post-rollback"),
        job.value["metadata"]["annotations"]["helm.sh/hook"].as_str()
    );
    assert!(
        is_digest_reference(container(job)["image"].as_str().expect("bootstrap image")),
        "the Secret-authorized bootstrap image must be immutable"
    );
    assert_eq!(
        Some(true),
        pod_spec(job)["automountServiceAccountToken"].as_bool()
    );
    assert_eq!(
        Some("logweir-identity-bootstrap"),
        pod_spec(job)["serviceAccountName"].as_str()
    );
    let args: Vec<&str> = container(job)["args"]
        .as_sequence()
        .expect("bootstrap args")
        .iter()
        .map(|v| v.as_str().expect("string arg"))
        .collect();
    assert_eq!(
        vec![
            "identity",
            "bootstrap",
            "--namespace",
            "logweir-system",
            "--public-configmap-name",
            "logweir-signing-trust"
        ],
        args
    );

    let role = find(&docs, "Role", "logweir-identity-bootstrap");
    let rules = role.value["rules"].as_sequence().expect("bootstrap rules");
    assert_eq!(
        3,
        rules.len(),
        "get Secret, patch managed Secret, get/patch public ConfigMap"
    );
    let all_verbs: BTreeSet<&str> = rules
        .iter()
        .flat_map(|rule| rule["verbs"].as_sequence().expect("verbs"))
        .map(|verb| verb.as_str().expect("verb"))
        .collect();
    assert_eq!(BTreeSet::from(["get", "patch"]), all_verbs);
    for rule in rules {
        assert!(
            rule["resourceNames"]
                .as_sequence()
                .is_some_and(|names| !names.is_empty()),
            "every bootstrap permission is resourceNames-scoped"
        );
    }
    let controller_rules = find(&docs, "ClusterRole", "weirkeeper").value["rules"]
        .as_sequence()
        .expect("controller rules");
    assert!(controller_rules.iter().all(|rule| {
        rule["resources"]
            .as_sequence()
            .is_none_or(|resources| resources.iter().all(|r| r.as_str() != Some("secrets")))
    }));

    let singleton = find(&docs, "ClusterRole", "logweir-identity-singleton");
    assert_eq!(
        Some("keep"),
        singleton.value["metadata"]["annotations"]["helm.sh/resource-policy"].as_str()
    );
    assert_eq!(
        Some("pre-install,pre-upgrade"),
        singleton.value["metadata"]["annotations"]["helm.sh/hook"].as_str()
    );
    assert!(singleton.value["rules"]
        .as_sequence()
        .is_some_and(Vec::is_empty));

    let ordinary = find(&docs, "NetworkPolicy", "logweir-runner-egress");
    assert!(ordinary.value["spec"]["podSelector"]["matchExpressions"]
        .as_sequence()
        .is_some_and(|expressions| expressions.iter().any(|expression| {
            expression["key"].as_str() == Some("logweir.dev/identity-authority")
                && expression["operator"].as_str() == Some("DoesNotExist")
        })));
    let bootstrap_policy = find(
        &docs,
        "NetworkPolicy",
        "logweir-identity-kubernetes-api-egress",
    );
    for egress in bootstrap_policy.value["spec"]["egress"]
        .as_sequence()
        .expect("bootstrap egress rules")
    {
        assert!(
            egress.get("to").is_some(),
            "bootstrap must have no port-only destination rule"
        );
    }
}

#[test]
fn chart_lint_authorized_namespace_receives_the_same_identity_distribution_contract() {
    let docs = rendered("identity-multinamespace");
    let target = docs
        .iter()
        .find(|doc| {
            doc.kind == "Secret"
                && doc.name() == "logweir-signing-key"
                && doc.value["metadata"]["namespace"].as_str() == Some("recoveries")
        })
        .expect("retained target signing Secret");
    assert_eq!(
        Some("recoveries"),
        target.value["metadata"]["namespace"].as_str()
    );
    assert_eq!(
        Some("pre-install,pre-upgrade"),
        target.value["metadata"]["annotations"]["helm.sh/hook"].as_str()
    );
    assert!(target.value["data"]
        .as_mapping()
        .is_some_and(|data| data.is_empty()));
    assert!(docs.iter().any(|doc| {
        doc.kind == "ServiceAccount"
            && doc.name() == "logweir-runner"
            && doc.value["metadata"]["namespace"].as_str() == Some("recoveries")
    }));
    let job = docs
        .iter()
        .find(|doc| {
            doc.kind == "Job"
                && doc.value["metadata"]["namespace"].as_str() == Some("recoveries")
                && doc.name().starts_with("logweir-identity-distribute-")
        })
        .expect("distribution Job in the authorized namespace");
    let args: Vec<&str> = container(job)["args"]
        .as_sequence()
        .expect("distribution args")
        .iter()
        .map(|value| value.as_str().expect("string arg"))
        .collect();
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--source-namespace", "logweir-system"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--target-namespace", "recoveries"]));
    assert!(!args.contains(&"bootstrap"));

    let source_role = find(&docs, "Role", "logweir-identity-distribution-source");
    assert_eq!(
        Some("logweir-system"),
        source_role.value["metadata"]["namespace"].as_str()
    );
    assert_eq!(
        Some("get"),
        source_role.value["rules"][0]["verbs"][0].as_str()
    );
    let target_role = docs
        .iter()
        .find(|doc| {
            doc.kind == "Role"
                && doc.name() == "logweir-identity-distributor"
                && doc.value["metadata"]["namespace"].as_str() == Some("recoveries")
        })
        .expect("target distributor role");
    assert_eq!(
        vec!["get", "patch"],
        target_role.value["rules"][0]["verbs"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
    );
}

#[test]
fn chart_lint_external_identity_is_explicit_get_only_adoption() {
    let docs = rendered("identity-external");
    let job = find(&docs, "Job", "logweir-identity-bootstrap");
    let args: Vec<&str> = container(job)["args"]
        .as_sequence()
        .expect("bootstrap args")
        .iter()
        .map(|v| v.as_str().expect("string arg"))
        .collect();
    assert!(args
        .windows(2)
        .any(|w| w == ["--external-secret-name", "company-logweir-signer"]));
    assert!(args
        .windows(2)
        .any(|w| w == ["--external-secret-key", "identity.pem"]));

    let role = find(&docs, "Role", "logweir-identity-bootstrap");
    let rules = role.value["rules"].as_sequence().expect("rules");
    let external_rules: Vec<&Value> = rules
        .iter()
        .filter(|rule| {
            rule["resourceNames"].as_sequence().is_some_and(|names| {
                names
                    .iter()
                    .any(|n| n.as_str() == Some("company-logweir-signer"))
            })
        })
        .collect();
    assert_eq!(1, external_rules.len());
    assert_eq!(
        vec!["get"],
        external_rules[0]["verbs"]
            .as_sequence()
            .expect("verbs")
            .iter()
            .map(|v| v.as_str().expect("verb"))
            .collect::<Vec<_>>()
    );
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

/// **`ui.enabled` renders the proxy with its measured paths, the ServiceAccount,
/// the RoleBinding to the chart's own role (never `cluster-admin`), the Service
/// on 8001 — and no Ingress.**
///
/// EDITED BY TASK 39, NOT RENAMED: every arm this test had about the args, the
/// roles, the binding, the Service and the absent Ingress is byte-for-byte what
/// it was, and they are the arms the name is about. What is gone is the arm
/// that read the ConfigMap — the fourteen keys, their bytes, the volume `items`
/// mapping each key back to a path, and the private-key-PEM scan over the
/// ConfigMap's data. That object no longer renders, so those assertions could
/// not "stay green unedited": they would panic looking for it. They moved, and
/// each of them got stronger on the way:
///
///   * the twenty-two files' BYTES -> `scripts/check-image-ui.sh` check 1, which
///     hashes what the image serves against `ui/` rather than what a template
///     inlined;
///   * "and nothing else" -> the same check, which fails naming every extra
///     file (measured: an image built with `COPY ui /ui` reported 53 files and
///     listed `tests/fixtures/approver.pub.pem`);
///   * Global Constraint 28's no-key-material scan -> the same check, over the
///     served set, plus `chart_lint_the_chart_carries_no_copy_of_the_ui_and_mounts_no_configmap`
///     which holds the ConfigMap and its volume absent.
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
        "the Deployment's image is values.yaml's ui.image"
    );
    assert_eq!(
        Some(format!("{}:{LOGWEIR_TAG}", ui_repository())).as_deref(),
        c["image"].as_str(),
        "Task 39: the proxy runs LOGWEIR'S OWN `logweir-ui` image — kubectl with the twenty-two \
         shipped files copied in at /ui — under the namespace the runner pin names, at \
         `:{LOGWEIR_TAG}` like the other two Logweir images. A bare kubectl digest here is the \
         page back in a ConfigMap."
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
    // `tmp` is writable for kubectl; `runtime` is the one read-only, generated
    // namespace context. The page itself remains in the image.
    let volumes = pod_spec(ui)["volumes"].as_sequence().expect("volumes");
    assert_eq!(
        2,
        volumes.len(),
        "the proxy pod carries exactly two volumes — writable `tmp` and read-only runtime context: \
         {volumes:?}"
    );
    assert!(
        volumes[0]["emptyDir"].is_mapping(),
        "the first volume is an emptyDir for /tmp: {:?}",
        volumes[0]
    );
    assert!(
        volumes[1]["configMap"]["name"]
            .as_str()
            .is_some_and(|name| name.starts_with("logweir-ui-runtime-")),
        "the second volume is the content-addressed namespace runtime context"
    );

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
        // D2 §7.4 — the three kinds Amendment F added, READ ONLY. No
        // `create`, no `patch`: `ui/api.js`'s `WRITABLE_PLURALS` is unchanged
        // and the page has no form for any of them; the new flows are
        // console-only. Without the read a destination-backed schedule renders
        // as though its archive were unconfigured.
        (
            vec!["logweir.dev".into()],
            vec![
                "backupdestinations".into(),
                "preflights".into(),
                "topicdiscoveries".into(),
            ],
            vec!["get".into(), "list".into()], // engine-token-ok: an RBAC verb the page issues, never an engine subcommand
        ),
    ]);
    assert_eq!(
        want,
        rules_of(role),
        "the page's ClusterRole carries exactly the measured verbs"
    );
    // AND NO WRITE VERB ON THE THREE NEW KINDS, asserted separately so the
    // table above cannot be widened in a diff that reads as a reformatting.
    for (_, resources, verbs) in rules_of(role) {
        if resources
            .iter()
            .any(|r| r == "backupdestinations" || r == "topicdiscoveries" || r == "preflights")
        {
            assert_eq!(
                verbs,
                vec!["get".to_string(), "list".to_string()], // engine-token-ok: an RBAC verb the page issues, never an engine subcommand
                "the legacy proxy reads the three new kinds and writes none of them (D2 §7.4): \
                 {resources:?} carries {verbs:?}"
            );
        }
    }
    // AND NO VERB ON `configmaps`, which is what keeps the page out of an
    // inventory's pages: the chunk documents a `TopicDiscovery` owns are
    // ConfigMaps, and reading them is the console API's job with its own
    // owner-UID, immutability and digest checks (D2 §5.6).
    for (groups, resources, _) in rules_of(role) {
        assert!(
            !(groups.iter().any(|g| g.is_empty()) && resources.iter().any(|r| r == "configmaps")),
            "the legacy proxy must hold no verb on `configmaps`"
        );
    }
    let roster = find(&docs, "ClusterRole", "logweir-ui-trustrosters");
    assert_eq!(
        BTreeSet::from([(
            vec!["logweir.dev".to_string()],
            vec!["trustrosters".to_string()],
            vec!["list".to_string()] // engine-token-ok: an RBAC verb the page issues, never an engine subcommand
        )]),
        rules_of(roster)
    );
    let bindings: Vec<&Doc> = docs
        .iter()
        .filter(|d| {
            d.kind == "RoleBinding" && d.value["roleRef"]["name"].as_str() == Some("logweir-ui")
        })
        .collect();
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
/// except the proxy and the short-lived identity bootstrap, which need it.**
/// Under the demo render, every Pod-carrying object is enumerated.
#[test]
fn chart_lint_only_api_callers_hold_a_token() {
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
            "logweir-identity-bootstrap" => assert_eq!(
                Some(true),
                automount,
                "the short-lived bootstrap calls the Kubernetes API"
            ),
            other => assert_eq!(
                Some(false),
                automount,
                "{}/{other} must set automountServiceAccountToken: false",
                d.kind
            ),
        }
    }
    assert_eq!(
        8, seen,
        "the demo render carries eight Pod-carrying objects including identity bootstrap"
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
fn chart_lint_every_rendered_image_is_a_digest_except_the_three_logweir_images_and_the_author_only_example(
) {
    // RENAMED FROM
    // `chart_lint_every_rendered_image_is_a_digest_except_the_two_logweir_images_and_the_author_only_example`
    // (Task 39, STANDING RULE 19): there are three Logweir images now, and the
    // number is in the name because the number is the assertion.
    let logweir_repos = [
        repository_of(&controller_image_pin()),
        repository_of(&runner_image_constant()),
        ui_repository(),
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
                    image == "weirkeeper:check"
                        || image == "logweir:check"
                        || is_digest_reference(&image),
                    "author-only.yaml: controller/bootstrap use the locally built tags; found {image}"
                );
                continue;
            }
            if d.value["metadata"]["labels"]["app.kubernetes.io/component"]
                .as_str()
                .is_some_and(|component| {
                    component == "identity-bootstrap" || component == "identity-distribution"
                })
            {
                assert!(
                    is_digest_reference(&image),
                    "{name}: privileged identity image is not digest-pinned: {image}"
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
                    "{name}: {}/{} names a Logweir image as {image}; this chart names all three \
                     by `<repository>:{LOGWEIR_TAG}`",
                    d.kind,
                    d.name()
                );
                continue;
            }
            assert!(
                is_digest_reference(&image),
                "{name}: {}/{} references {image} by tag — only the three Logweir images may",
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

/// **`values.schema.json` types the four flags as booleans** (the script
/// proves the refusal with `--set demoKafka.enabled=yes`; this reads the
/// schema) and the four image values as strings.
#[test]
fn chart_lint_values_schema_types_the_four_flags_as_booleans() {
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("values.schema.json parses");
    for flag in ["minio", "demoKafka", "ui", "identity"] {
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

// ============================================ D2 §4.4 — the installation policy

/// The `policy.json` document one rendered file carries, parsed.
fn policy_json(render: &str) -> serde_json::Value {
    let docs = rendered(render);
    let cm = find(&docs, "ConfigMap", "weirkeeper-policy");
    assert_eq!(
        Some("logweir-system"),
        cm.value["metadata"]["namespace"].as_str(),
        "the policy lives in the RELEASE namespace, not in a tenant's: who may write THIS \
         ConfigMap is the whole of what makes an attestation an administrator statement"
    );
    let raw = cm.value["data"]["policy.json"]
        .as_str()
        .unwrap_or_else(|| panic!("{render}: weirkeeper-policy carries no `policy.json` key"));
    serde_json::from_str(raw)
        .unwrap_or_else(|e| panic!("{render}: `policy.json` is not JSON: {e}\n{raw}"))
}

/// **The policy `ConfigMap` is rendered, it is the values file, and the
/// Deployment points at it** — D2 §4.4, §10.
///
/// THE KEY SET IS ASSERTED EXACTLY, and that is not pedantry:
/// `weirkeeper::check::policy` parses this document with
/// `deny_unknown_fields`, so a key this template invents makes the whole
/// policy `Unreadable` — which fails CLOSED (no attestations, no evidence
/// allowlist) and reports itself only as one advisory row on a `Preflight`
/// nobody may be looking at. A missing key inside `checks`, `discovery` or
/// `preflight` does the same, because those three structs carry no per-field
/// `serde(default)`.
///
/// MUTANT: rename any key below — `maxActiveTotal` to `maxTotal`, say — and
/// the install still succeeds, the controller still starts, and every
/// completeness verdict silently becomes `unknown`. This test is what fails
/// instead.
#[test]
fn chart_lint_the_policy_config_map_is_the_values_file_and_the_deployment_points_at_it() {
    let policy = policy_json("default");
    let top: BTreeSet<String> = policy
        .as_object()
        .expect("policy.json is an object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        top,
        BTreeSet::from([
            "version".to_string(),
            "checks".to_string(),
            "discovery".to_string(),
            "preflight".to_string(),
            "engine".to_string(),
            "evidence".to_string(),
            "legacyArchiveAddressing".to_string(),
        ]),
        "policy.json's top-level keys are `weirkeeper::check::policy::Policy`'s field set, \
         exactly: it is parsed with deny_unknown_fields"
    );
    assert_eq!(policy["version"], 1, "POLICY_VERSION");
    assert_eq!(
        policy["checks"]
            .as_object()
            .expect("a checks object")
            .keys()
            .cloned()
            .collect::<BTreeSet<String>>(),
        BTreeSet::from([
            "maxActivePerNamespace".to_string(),
            "maxActiveTotal".to_string(),
            "maxActiveDiscoveriesPerConnection".to_string(),
            "maxEvidenceFetchActivePerNamespace".to_string(),
        ])
    );
    assert_eq!(
        policy["discovery"]
            .as_object()
            .expect("a discovery object")
            .keys()
            .cloned()
            .collect::<BTreeSet<String>>(),
        BTreeSet::from([
            "freshSeconds".to_string(),
            "retentionSeconds".to_string(),
            "keepPerConnection".to_string(),
            "defaultMaxTopics".to_string(),
            "hardMaxTopics".to_string(),
            "visibilityAttestations".to_string(),
        ])
    );
    assert_eq!(
        policy["preflight"]
            .as_object()
            .expect("a preflight object")
            .keys()
            .cloned()
            .collect::<BTreeSet<String>>(),
        BTreeSet::from([
            "defaultTimeoutSeconds".to_string(),
            "retentionSeconds".to_string(),
        ])
    );

    // AND EVERY NUMBER IS THE VALUES FILE'S, so the document cannot drift from
    // the knob an adopter turns.
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    for (json_path, yaml_path) in [
        (
            "checks.maxActivePerNamespace",
            "checks.maxActivePerNamespace",
        ),
        ("checks.maxActiveTotal", "checks.maxActiveTotal"),
        (
            "checks.maxActiveDiscoveriesPerConnection",
            "checks.maxActiveDiscoveriesPerConnection",
        ),
        (
            "checks.maxEvidenceFetchActivePerNamespace",
            "checks.maxEvidenceFetchActivePerNamespace",
        ),
        ("discovery.freshSeconds", "checks.discovery.freshSeconds"),
        (
            "discovery.retentionSeconds",
            "checks.discovery.retentionSeconds",
        ),
        (
            "discovery.keepPerConnection",
            "checks.discovery.keepPerConnection",
        ),
        (
            "discovery.defaultMaxTopics",
            "checks.discovery.defaultMaxTopics",
        ),
        ("discovery.hardMaxTopics", "checks.discovery.hardMaxTopics"),
        (
            "preflight.defaultTimeoutSeconds",
            "checks.preflight.defaultTimeoutSeconds",
        ),
        (
            "preflight.retentionSeconds",
            "checks.preflight.retentionSeconds",
        ),
    ] {
        let mut j = &policy;
        for seg in json_path.split('.') {
            j = &j[seg];
        }
        let mut y = &values;
        for seg in yaml_path.split('.') {
            y = &y[seg];
        }
        assert_eq!(
            j.as_u64(),
            y.as_u64(),
            "policy.json's `{json_path}` is not values.yaml's `{yaml_path}`"
        );
        assert!(
            j.is_u64(),
            "policy.json's `{json_path}` must be a JSON INTEGER; a float or a string is a \
             `deny_unknown_fields` parse failure that fails closed"
        );
    }
    // THE TWO COLLECTIONS ARE EMPTY BY DEFAULT — with no attestation nothing
    // can be `attestedComplete`, and with no allowlist an unlisted evidence
    // location is refused.
    assert_eq!(
        Some(0),
        policy["discovery"]["visibilityAttestations"]
            .as_array()
            .map(Vec::len)
    );
    assert_eq!(
        Some(0),
        policy["evidence"]["controllerIdentityLocations"]
            .as_array()
            .map(Vec::len)
    );
    assert_eq!(policy["engine"]["allowUnverifiedCustomCa"], false);

    // AND THE DEPLOYMENT POINTS AT IT. The override ships EMPTY and the
    // namespace comes from the pod's own `metadata.namespace`, which is the
    // pair `check::policy::configured_ref` resolves: an explicit
    // `<ns>/<name>`, else `weirkeeper-policy` in the installation namespace,
    // else nothing.
    let docs = rendered("default");
    let env = env_of(container(find(&docs, "Deployment", "weirkeeper")));
    assert_eq!(
        env.get("LOGWEIR_POLICY_CONFIGMAP")
            .and_then(|v| v["value"].as_str()),
        Some(""),
        "the explicit override ships empty; the namespace below is what resolves the document"
    );
    assert_eq!(
        env.get("LOGWEIR_INSTALLATION_NAMESPACE")
            .and_then(|v| v["valueFrom"]["fieldRef"]["fieldPath"].as_str()),
        Some("metadata.namespace"),
        "a namespace STRING in a manifest is one an operator has to keep in step with \
         `helm -n`; the downward API cannot drift"
    );

    // NO CREDENTIAL REACHES THIS DOCUMENT. Every value in it comes from
    // values.yaml and none of them is a secret; this is the assertion that
    // makes that checkable rather than merely intended.
    let raw = find(&docs, "ConfigMap", "weirkeeper-policy").value["data"]["policy.json"]
        .as_str()
        .unwrap_or_default()
        .to_lowercase();
    for needle in [
        "password",
        "secretkeyref",
        "accesskey",
        "secret-access-key",
        "-----begin",
        "minioadmin",
    ] {
        assert!(
            !raw.contains(needle),
            "the policy ConfigMap names `{needle}`. Nothing secret may reach a ConfigMap, and \
             nothing in this document's inputs is secret"
        );
    }
}

/// **The `legacyArchiveAddressing` block is the Deployment's own addressing
/// env, and it is ABSENT-SHAPED when the Deployment forwards none** — D2
/// §3.12 (b).
///
/// WHY THIS PAIRING IS THE WHOLE POINT. `destinations:from-legacy` may label a
/// derived location `installationConfig` only when it has actually read the
/// installation's addressing; before this template existed the route could not
/// read it and therefore never emitted that provenance. A block that said
/// something the Deployment does not is worse than no block at all — it is a
/// provenance label naming a source that was never read.
///
/// MUTANT: copy `deployment.yaml`'s bare `ternary .Values.archive.s3.allowHttp
/// true $explicit` out of its `{{ if $endpoint }}` guard. An install with no
/// endpoint (plain Amazon S3, the DEFAULT render) then publishes
/// `allowHttp: true` — a plaintext-HTTP fact nobody configured — and the first
/// assertion below fails. That is D-SEAMS S5, transport security is never
/// derived, in the one direction that matters.
#[test]
fn chart_lint_the_policy_legacy_addressing_is_the_deployments_own_addressing() {
    // (a) THE DEFAULT RENDER FORWARDS NOTHING, so the block is empty-shaped.
    let policy = policy_json("default");
    let env = env_of(container(find(
        &rendered("default"),
        "Deployment",
        "weirkeeper",
    )));
    assert!(
        !env.contains_key("AWS_ENDPOINT_URL") && !env.contains_key("AWS_ALLOW_HTTP"),
        "the default render forwards no addressing env"
    );
    assert_eq!(policy["legacyArchiveAddressing"]["endpoint"], "");
    assert_eq!(
        policy["legacyArchiveAddressing"]["allowHttp"], false,
        "an install that forwards no addressing does not have `allowHttp: true` — it has no \
         allowHttp at all, and `false` is the only honest rendering of that"
    );
    assert_eq!(
        policy["legacyArchiveAddressing"]["virtualHostedStyle"],
        false
    );

    // (b) A RENDER THAT DOES FORWARD IT AGREES, VALUE FOR VALUE.
    let demo = policy_json("demo");
    let demo_env = env_of(container(find(
        &rendered("demo"),
        "Deployment",
        "weirkeeper",
    )));
    let block = &demo["legacyArchiveAddressing"];
    assert_eq!(
        block["endpoint"].as_str(),
        demo_env["AWS_ENDPOINT_URL"]["value"].as_str(),
        "the endpoint the policy publishes is the one the controller forwards"
    );
    assert_eq!(
        block["allowHttp"].as_bool().map(|b| b.to_string()),
        demo_env["AWS_ALLOW_HTTP"]["value"]
            .as_str()
            .map(str::to_string),
        "`allowHttp` is the AWS_ALLOW_HTTP the same render sets"
    );
    assert_eq!(
        block["virtualHostedStyle"].as_bool().map(|b| b.to_string()),
        demo_env["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"]["value"]
            .as_str()
            .map(str::to_string)
    );
    assert_eq!(
        block["region"].as_str(),
        demo_env["AWS_REGION"]["value"].as_str()
    );
    assert!(
        block["endpoint"].as_str().is_some_and(|e| !e.is_empty()),
        "this arm proves nothing unless the demo render really does forward an endpoint"
    );
}

/// **An administrator's attestation and evidence allowlist reach the document
/// whole** — the one route to `attestedComplete` (D2 §5.4).
#[test]
fn chart_lint_an_attestation_reaches_the_policy_document_with_every_field() {
    let policy = policy_json("admission-policy");
    let attestations = policy["discovery"]["visibilityAttestations"]
        .as_array()
        .expect("an attestation array");
    assert_eq!(attestations.len(), 1);
    let a = &attestations[0];
    assert_eq!(
        a.as_object()
            .expect("an attestation object")
            .keys()
            .cloned()
            .collect::<BTreeSet<String>>(),
        BTreeSet::from([
            "id".to_string(),
            "namespace".to_string(),
            "kafkaCluster".to_string(),
            "clusterId".to_string(),
            "principal".to_string(),
            "attestedBy".to_string(),
            "attestedAt".to_string(),
            "expiresAt".to_string(),
            "statement".to_string(),
        ]),
        "`logweir_core::check_contract::Attestation` has nine fields, all required, and the \
         document is parsed with deny_unknown_fields"
    );
    for field in ["namespace", "kafkaCluster", "clusterId", "principal"] {
        assert!(
            a[field].as_str().is_some_and(|v| !v.trim().is_empty()),
            "`{field}` must be non-blank: `attestation_candidate` fails closed on a blank one, \
             so a blank field is an attestation that matches nothing while LOOKING like one an \
             operator can rely on"
        );
    }
    let locations = policy["evidence"]["controllerIdentityLocations"]
        .as_array()
        .expect("an allowlist array");
    assert_eq!(locations.len(), 1);
    assert_eq!(
        locations[0]
            .as_object()
            .expect("a location object")
            .keys()
            .cloned()
            .collect::<BTreeSet<String>>(),
        BTreeSet::from([
            "endpoint".to_string(),
            "region".to_string(),
            "bucket".to_string(),
        ])
    );
}

// ======================================= D2 §7.3 — the console admission policy

/// **The ValidatingAdmissionPolicy renders only when it is asked for, and when
/// it does it fences the console's `create secrets` to the two Logweir
/// credential types.**
///
/// OFF BY DEFAULT FOR ONE REASON, and it is not a security opinion:
/// `admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy is Kubernetes
/// 1.30+ and this chart's floor is 1.29 (Global Constraint 25), where the
/// document is rejected with `no matches for kind` and the whole install
/// fails.
///
/// MUTANTS: (a) drop `matchConditions` — the policy then applies to EVERY
/// subject in the cluster and, with `failurePolicy: Fail`, breaks
/// `kubernetes.io/service-account-token` creation cluster-wide; (b) change
/// `validationActions` to `["Warn"]` — the grant stays exactly as wide as it
/// is today while reading, on an audit surface, as though it did not; (c) drop
/// the owner-label validation — a console that creates a Secret of the right
/// type but somebody else's label is no longer distinguishable from one that
/// owns it.
#[test]
fn chart_lint_the_admission_policy_renders_only_when_enabled_and_names_both_credential_types() {
    // (a) NOT IN ANY OTHER RENDER.
    for name in [
        "default",
        "minimal",
        "demo",
        "author-only",
        "msk",
        "identity-external",
        "identity-multinamespace",
    ] {
        let docs = rendered(name);
        assert!(
            !docs
                .iter()
                .any(|d| d.kind.starts_with("ValidatingAdmissionPolicy")),
            "`{name}` renders a ValidatingAdmissionPolicy; the kind is Kubernetes 1.30+ and \
             this chart's floor is 1.29, so it must be reached only through \
             `admissionPolicy.enabled`"
        );
    }

    // (b) AND EXACTLY WHAT IT IS WHEN IT IS ON.
    let docs = rendered("admission-policy");
    let policy = find(
        &docs,
        "ValidatingAdmissionPolicy",
        "logweir-console-credentials-only",
    );
    assert_eq!(
        Some("admissionregistration.k8s.io/v1"),
        policy.value["apiVersion"].as_str()
    );
    assert_eq!(
        Some("Fail"),
        policy.value["spec"]["failurePolicy"].as_str(),
        "fail CLOSED — which is safe precisely because matchConditions skips every other \
         subject in the cluster"
    );
    let rules = policy.value["spec"]["matchConstraints"]["resourceRules"]
        .as_sequence()
        .expect("resourceRules");
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0]["operations"]
            .as_sequence()
            .expect("operations")
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>(),
        vec!["CREATE"],
        "CREATE and nothing else: `logweir-api` holds no update, no delete and no read verb \
         on Secrets, so there is no other operation to fence"
    );
    assert_eq!(
        rules[0]["resources"]
            .as_sequence()
            .expect("resources")
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>(),
        vec!["secrets"]
    );

    // THE SUBJECT TEST, and both principals the example configures.
    let conditions = policy.value["spec"]["matchConditions"]
        .as_sequence()
        .expect("the policy must carry matchConditions");
    assert_eq!(conditions.len(), 1);
    let expression = conditions[0]["expression"].as_str().expect("an expression");
    assert!(
        expression.contains("request.userInfo.username"),
        "the subject test reads the requesting username: {expression}"
    );
    for principal in [
        "system:serviceaccount:logweir-system:logweir-api",
        "system:serviceaccount:team-a:logweir-api",
    ] {
        assert!(
            expression.contains(principal),
            "the release-namespace account and every `extraPrincipals` entry are in the \
             subject list; `{principal}` is not: {expression}"
        );
    }

    // THE TWO VALIDATIONS: the credential type, and the owner label.
    let validations = policy.value["spec"]["validations"]
        .as_sequence()
        .expect("validations");
    assert_eq!(validations.len(), 2);
    let all: String = validations
        .iter()
        .filter_map(|v| v["expression"].as_str())
        .collect::<Vec<_>>()
        .join(" ");
    for needle in [
        "logweir.dev/object-store-credential",
        "logweir.dev/kafka-sasl-password",
        "app.kubernetes.io/managed-by",
        "has(object.type)",
    ] {
        assert!(
            all.contains(needle),
            "the policy must require `{needle}`; the two credential types are exactly the \
             values `logweir-api`'s destination builder and PLAT-07.1's connection builder \
             stamp, and the label is what both of them set: {all}"
        );
    }
    for v in validations {
        assert_eq!(
            Some("Forbidden"),
            v["reason"].as_str(),
            "a refusal reason an operator can read in the API server's answer"
        );
        assert!(
            v["message"].as_str().is_some_and(|m| m.len() > 40),
            "every validation carries a message naming what is required"
        );
    }

    // THE BINDING DENIES.
    let binding = find(
        &docs,
        "ValidatingAdmissionPolicyBinding",
        "logweir-console-credentials-only",
    );
    assert_eq!(
        binding.value["spec"]["policyName"].as_str(),
        Some("logweir-console-credentials-only")
    );
    assert_eq!(
        binding.value["spec"]["validationActions"]
            .as_sequence()
            .expect("validationActions")
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>(),
        vec!["Deny"],
        "`Warn` or `Audit` would leave the grant exactly as wide as it is today"
    );
}

/// **The `config/` copy of the admission policy carries the same two documents
/// and is reachable from no kustomization** — the kustomize half of D2 §7.3.
#[test]
fn chart_lint_the_config_copy_of_the_admission_policy_is_applied_by_hand() {
    let sample = read("config/samples/console-credential-admission-policy.yaml");
    for needle in [
        "kind: ValidatingAdmissionPolicy",
        "kind: ValidatingAdmissionPolicyBinding",
        "logweir.dev/object-store-credential",
        "logweir.dev/kafka-sasl-password",
        "app.kubernetes.io/managed-by",
        "request.userInfo.username",
        // SPELT IN TWO PIECES, exactly as `label_gate.rs` spells its own
        // needle: `scripts/check-unverified-labels.sh` greps the tree for the
        // bare token and refuses one that carries no description, and a test
        // asserting the mark exists must not itself look like an unlabelled
        // mark.
        concat!("[", "UNVERIFIED —"),
    ] {
        assert!(
            sample.contains(needle),
            "config/samples/console-credential-admission-policy.yaml must carry `{needle}`"
        );
    }
    // IT IS NOT IN THE INSTALL FILE, and it cannot be: a
    // ValidatingAdmissionPolicy applied to a 1.29 cluster is rejected with
    // `no matches for kind`, and `logweir.yaml` has to apply unedited on the
    // stated floor (spec §16 clause 1).
    let root: Value = serde_yaml::from_str(&read("config/kustomization.yaml"))
        .expect("the install root's kustomization parses");
    let resources: Vec<String> = root["resources"]
        .as_sequence()
        .expect("a resources list")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    assert!(
        !resources.iter().any(|r| r.contains("samples")),
        "config/samples/ is reachable from no kustomization; the install root names \
         {resources:?}"
    );
    assert!(
        !std::path::Path::new(&repo().join("config/samples/kustomization.yaml")).exists(),
        "config/samples/ has no kustomization of its own either"
    );
    assert!(
        !read("logweir.yaml").contains("ValidatingAdmissionPolicy"),
        "logweir.yaml must carry no ValidatingAdmissionPolicy: minimum Kubernetes is 1.29"
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
        // TASK 39. Arm 2 was `cmp -s "$src" "$CHART/ui/$rel"` — the byte-copy
        // loop over the chart's duplicate of `ui/`. The directory is gone, the
        // loop with it, and what the arm asserts now is that the copy has not
        // come back. The needle below is the REPLACEMENT NAMED IN THE SCRIPT'S
        // OWN HEADER, so a future editor who deletes arm 2 outright fails here
        // rather than quietly losing both halves.
        "scripts/check-image-ui.sh",
        "if [ -e \"$CHART/ui\" ]; then",
        "helm lint \"$CHART\"",
        "helm template \"$RELEASE\" \"$CHART\" -n \"$NAMESPACE\" --include-crds",
        "diff -u \"$RENDERED/$name.yaml\" \"$tmp/expected-$name.yaml\"",
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
        // Task 39: the third repository, its namespace DERIVED from the runner
        // pin and only its name spelt.
        "UI_IMAGE_NAME=\"logweir-ui\"",
        "ui_repo=\"${runner_repo%/*}/$UI_IMAGE_NAME\"",
        "$ui_repo:$LOGWEIR_TAG",
        // D2 W11: the installation policy's own schema refusals. A policy
        // document `weirkeeper::check::policy` refuses fails CLOSED and
        // SILENTLY, so the schema has to refuse the same values at install
        // time, where the operator is still looking.
        "--set 'checks.discovery.keepPerConnection=0'",
        "--set-string 'checks.discovery.visibilityAttestations[0].id=att-partial'",
        // Fix round 1, review F1 and F4. The three bounds `Policy::validate`
        // enforces that an install used to walk straight past, and the two
        // spellings of "the admission policy's subject is missing" — `--set`
        // for a real null (which used to render `%!s(<nil>)`) and
        // `--set-string` for the empty string.
        "'checks.discovery.hardMaxTopics=100000'",
        "'checks.maxActiveTotal=2'",
        "'checks.discovery.defaultMaxTopics=60000'",
        "--set 'admissionPolicy.consoleServiceAccountName=null'",
        "--set-string 'admissionPolicy.consoleServiceAccountName='",
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

/// **The identity bootstrap default is a runner digest** — PLAT-02.1's clean
/// install needs no key and no image hash, and never a mutable image.
#[test]
fn chart_lint_values_pin_the_identity_bootstrap_to_a_runner_digest() {
    // The bootstrap hook can read and patch the retained signer, so the default
    // a stranger installs must name the tree's runner repository by digest with
    // the development override off. An empty value is a render refusal and a
    // tag is a mutable grant of signing-key authority; both are regressions.
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let bootstrap = values["identity"]["bootstrapImage"]
        .as_str()
        .expect("identity.bootstrapImage is a string");
    assert!(
        is_digest_reference(bootstrap),
        "identity.bootstrapImage must ship as `<repository>@sha256:<64 hex>`, found `{bootstrap}`"
    );
    assert_eq!(
        repository_of(&runner_image_constant()),
        repository_of(bootstrap),
        "identity.bootstrapImage must pin the tree's own runner repository"
    );
    assert_eq!(
        Some(false),
        values["identity"]["allowMutableBootstrapImageForDevelopment"].as_bool(),
        "the shipped values must keep the mutable development override off"
    );
    let rendered = read("charts/logweir/rendered/default.yaml");
    assert!(
        rendered.contains(&format!("image: {bootstrap}")),
        "rendered/default.yaml must carry the shipped bootstrap digest, not a test substitute"
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
    // RAISED FROM 130 TO 170 BY D2 W11, and the reason is written here rather
    // than in a commit message. The installation policy (D2 §4.4) is FOUR
    // nested blocks an adopter genuinely sets — the check-pool ceilings, the
    // two retention windows the collector acts on, the discovery bounds, and
    // the completeness attestations that are the only route to
    // `attestedComplete` — plus the admission policy's three keys. The owner's
    // rule is unchanged and is what the rest of this test enforces: ONE SHORT
    // LINE PER KEY, no paragraphs, every explanation in
    // `charts/logweir/README.md`. The budget moved because the number of
    // OPTIONS moved, not because the prose did.
    assert!(
        lines <= 170,
        "charts/logweir/values.yaml is {lines} lines. The owner asked for a values file that is \
         read, not skimmed past: one short line per key, no paragraphs, and every explanation \
         in charts/logweir/README.md"
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
        "identity.enabled",
        "identity.bootstrapImage",
        "identity.bootstrapImagePullPolicy",
        "identity.allowMutableBootstrapImageForDevelopment",
        "identity.publicConfigMapName",
        "identity.authorizedRunnerNamespaces",
        "identity.kubernetesApiCIDRs",
        "identity.externalSecret.name",
        "identity.externalSecret.key",
        "identity.resources",
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
        // D2 §4.4 / §10 — the installation policy, rendered by
        // `templates/policy.yaml` into the `weirkeeper-policy` ConfigMap.
        "checks.maxActivePerNamespace",
        "checks.maxActiveTotal",
        "checks.maxActiveDiscoveriesPerConnection",
        "checks.maxEvidenceFetchActivePerNamespace",
        "checks.discovery.freshSeconds",
        "checks.discovery.retentionSeconds",
        "checks.discovery.keepPerConnection",
        "checks.discovery.defaultMaxTopics",
        "checks.discovery.hardMaxTopics",
        "checks.discovery.visibilityAttestations",
        "checks.preflight.defaultTimeoutSeconds",
        "checks.preflight.retentionSeconds",
        "engine.allowUnverifiedCustomCa",
        "evidence.controllerIdentityLocations",
        // D2 §7.3 — the console credential admission policy.
        "admissionPolicy.enabled",
        "admissionPolicy.consoleServiceAccountName",
        "admissionPolicy.extraPrincipals",
        // D3 W14 / NOTIFY-INSECURE-SINK-UNEXPOSED — the installation-only hatch.
        "notify.allowInsecureSinks",
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
    // THE ADMISSION POLICY SHIPS OFF, and the ONE reason is that
    // `admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy is
    // Kubernetes 1.30+ while this chart's floor is 1.29 (Global Constraint
    // 25), where the document is rejected with `no matches for kind`. It is
    // not a security opinion, and `charts/logweir/README.md` says so where an
    // adopter reads it.
    assert_eq!(Some(false), values["admissionPolicy"]["enabled"].as_bool());
    // AND THE POLICY DEFAULTS ARE THE DECISION'S OWN NUMBERS (D2 §4.4), so a
    // silent drift between the document and the controller's compiled-in
    // `Policy::defaults()` is a diff here.
    for (path, want) in [
        ("checks.maxActivePerNamespace", 4u64),
        ("checks.maxActiveTotal", 20),
        ("checks.maxActiveDiscoveriesPerConnection", 1),
        ("checks.maxEvidenceFetchActivePerNamespace", 4),
        ("checks.discovery.freshSeconds", 900),
        ("checks.discovery.retentionSeconds", 86_400),
        ("checks.discovery.keepPerConnection", 5),
        ("checks.discovery.defaultMaxTopics", 20_000),
        ("checks.discovery.hardMaxTopics", 50_000),
        ("checks.preflight.defaultTimeoutSeconds", 120),
        ("checks.preflight.retentionSeconds", 3_600),
    ] {
        let mut node = &values;
        for segment in path.split('.') {
            node = &node[segment];
        }
        assert_eq!(
            node.as_u64(),
            Some(want),
            "values.yaml's `{path}` is not D2 §4.4's default"
        );
    }
    assert_eq!(
        Some(0),
        values["checks"]["discovery"]["visibilityAttestations"]
            .as_sequence()
            .map(Vec::len),
        "EMPTY, and that is the safe direction: with no attestation nothing can ever be \
         `attestedComplete`"
    );
    assert_eq!(
        Some(0),
        values["evidence"]["controllerIdentityLocations"]
            .as_sequence()
            .map(Vec::len),
        "EMPTY, and that is the closed direction: an unlisted location is refused with \
         `ControllerIdentityNotAllowlisted`"
    );
    assert_eq!(
        Some(false),
        values["engine"]["allowUnverifiedCustomCa"].as_bool()
    );
    assert_eq!(Some(true), values["identity"]["enabled"].as_bool());
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
        ("msk", "Job", "logweir-identity-bootstrap"),
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
        ("Job", "logweir-identity-bootstrap"),
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
    assert_eq!(9, checked, "nine pod specs were checked, not {checked}");

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
    for account in [
        "weirkeeper",
        "logweir-runner",
        "logweir-ui",
        "logweir-identity-bootstrap",
    ] {
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

/// Registry overrides remain documented independently of historical release status.
#[test]
fn chart_lint_install_md_carries_bring_your_own_registry() {
    let install = read("docs/install.md");
    let section = install
        .split("### (d) Bring your own registry")
        .nth(1)
        .expect("install guide documents bringing your own registry")
        .split("\n---")
        .next()
        .expect("the section body");
    for needle in [
        "ecr",
        "controllerImage",
        "runnerImage",
        "ui.image",
        "imagePullPolicy",
        "runnerImagePullPolicy",
        "imagePullSecrets",
        "amd64",
    ] {
        assert!(
            section.contains(needle),
            "registry instructions must document {needle}"
        );
    }
}
