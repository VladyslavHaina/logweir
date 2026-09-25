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
//!   and the twenty-six shipped UI files — asserted here with `std::fs` and in
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

/// The twenty-six shipped UI files: everything under `ui/` except `*.md` and
/// `tests/` — `scripts/check-ui-offline.sh`'s scope, by construction.
///
/// It was twenty-two until D3 W12 added `operation-watch.js` and the three
/// pages `#/operations`, `#/protection` and `#/catalog` are rendered by. The
/// NUMBER is asserted rather than taken from the tree for the reason
/// `check-image-ui.sh` states beside its own copy: a count read from the same
/// directory the image was built from stays green when files are deleted from
/// both sides at once.
fn shipped_ui_files() -> Vec<String> {
    let files: Vec<String> = files_under("ui")
        .into_iter()
        .filter(|p| !p.ends_with(".md") && !p.starts_with("ui/tests/"))
        .collect();
    assert_eq!(
        26,
        files.len(),
        "the shipped UI is twenty-six files (ui/*.html, ui/*.js, ui/*.css, ui/pages/*); found \
         {files:?}"
    );
    files
}

#[test]
fn both_ui_bearing_image_gates_pin_the_current_shipped_file_count() {
    let expected = shipped_ui_files().len();
    for script in ["scripts/check-image-ui.sh", "scripts/check-image-api.sh"] {
        let source = read(script);
        for counter in ["tree_count", "image_count"] {
            let guard = format!("[ \"${counter}\" -ne {expected} ]");
            assert!(
                source.contains(&guard),
                "{script} must pin {counter} to the same {expected}-file UI set as the tree; \
                 missing `{guard}`"
            );
        }
        assert!(
            !source.contains("twenty-two"),
            "{script} still documents the pre-operation-pages UI count"
        );
    }
}

/// The CLASS, swept: every chart, workflow and Dockerfile sentence that
/// counts the shipped page files counts twenty-six. `values.yaml`,
/// `templates/ui/api-config.yaml` and `api-deployment.yaml` still said
/// "twenty-two" after the gates above had moved (the PoC review's G-wording
/// note), because only the two gate scripts were read.
#[test]
fn no_chart_workflow_or_dockerfile_counts_the_old_page_set() {
    assert_eq!(shipped_ui_files().len(), 26, "update the prose rule below");
    let mut paths: Vec<String> = files_under("charts/logweir")
        .into_iter()
        .filter(|p| !p.starts_with("charts/logweir/rendered/"))
        .collect();
    paths.extend(files_under(".github/workflows"));
    paths.extend(
        [
            "Dockerfile",
            "Dockerfile.ui",
            "Dockerfile.console",
            "Dockerfile.weirkeeper",
        ]
        .map(String::from),
    );
    let stale: Vec<String> = paths
        .iter()
        .filter(|p| read(p).contains("twenty-two"))
        .cloned()
        .collect();
    assert!(
        stale.is_empty(),
        "these files still count twenty-two shipped page files (there are twenty-six): {stale:?}"
    );
}

/// **Chart gap G5: the controller has a liveness and a readiness probe, in
/// every render and in the install file, and both are the binary's own
/// `--probe` against its loopback listener.** An `httpGet` probe would need a
/// pod-IP listener; a `tcpSocket` probe would pass while the runtime is wedged
/// (the kernel accepts the connection) — only an answered request proves the
/// runtime turns, which is what `weirkeeper --probe` asks for.
#[test]
fn chart_lint_the_controller_is_probed_by_its_own_binary() {
    let mut deployments: Vec<(String, Value)> = files_under("charts/logweir/rendered")
        .into_iter()
        .map(|p| {
            let docs = docs_in(&p);
            let doc = find(&docs, "Deployment", "weirkeeper");
            (p, container(doc).clone())
        })
        .collect();
    let install = docs_in("config/manager/deployment.yaml");
    deployments.push((
        "config/manager/deployment.yaml".to_string(),
        container(find(&install, "Deployment", "weirkeeper")).clone(),
    ));
    assert!(deployments.len() >= 11, "the walk has gone quiet");
    for (source, c) in deployments {
        for (probe, word) in [("livenessProbe", "live"), ("readinessProbe", "ready")] {
            let command: Vec<&str> = c[probe]["exec"]["command"]
                .as_sequence()
                .unwrap_or_else(|| panic!("{source}: the controller has no exec {probe}"))
                .iter()
                .map(|v| v.as_str().unwrap_or_default())
                .collect();
            assert_eq!(
                command,
                ["/usr/local/bin/weirkeeper", "--probe", word],
                "{source}: {probe}"
            );
            let timeout = c[probe]["timeoutSeconds"].as_u64().unwrap_or(1);
            assert!(
                timeout > 2,
                "{source}: {probe}.timeoutSeconds {timeout} is not above the probe's own two-second \
                 deadline, so a slow answer would be read as a failure"
            );
        }
        assert!(
            c["ports"].is_null(),
            "{source}: the health listener is loopback-only; the controller exposes no port"
        );
    }
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

/// **The console image's repository, DERIVED** — the same namespace with the
/// name `logweir-console`, which is what `Dockerfile.console` builds.
///
/// D0 stage 7's fourth image, and the two names it carries are deliberate:
/// D0 owns the word `console` (the file, the image, `api.console.*`) while the
/// crate, the binary and the landed RBAC values block own `api` — see
/// `charts/logweir/README.md`. Derived, not spelt, for the reason
/// `ui_repository` gives above: a namespace move must carry all four.
fn console_repository() -> String {
    let runner = repository_of(&runner_image_constant());
    let (namespace, _) = runner
        .rsplit_once('/')
        .unwrap_or_else(|| panic!("RUNNER_IMAGE's repository `{runner}` names no namespace"));
    format!("{namespace}/logweir-console")
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
    // The twenty-six are in the tree, where the image gate hashes
    // them from. If this ever drifts, `scripts/check-image-ui.sh` check 1 is
    // comparing against the wrong set.
    let shipped = shipped_ui_files();
    assert_eq!(26, shipped.len());

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
    for flag in ["minio", "demoKafka", "ui", "retention", "api"] {
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

/// **No file names a MinIO image upstream withdrew, and every MinIO image
/// reference in the tree is the chart's own mirror digest.**
///
/// MinIO deleted its server and client images from Docker Hub on 2026-09-11,
/// and its quay.io repositories began refusing anonymous pulls on 2026-09-24;
/// CI's `e2e` job then failed at `just e2e-up`. What the compose stack, the
/// chart's demo MinIO, the PoC grants Job and the live harnesses run is now
/// the rebuild `third_party/minio-mirror/` publishes, pinned by digest. A
/// reference that goes back to an upstream name is a stack that cannot pull;
/// a harness that pins a DIFFERENT mirror digest from the chart's measures a
/// different server from the one the chart ships. `third_party/minio-mirror/`
/// and `THIRD_PARTY_NOTICES.md` name the upstream images on purpose — they are
/// the provenance record — and so do the trackers under `docs/to-do/`, which
/// are history and are never pulled; nothing else is exempt.
#[test]
fn chart_lint_every_minio_image_reference_is_the_mirror_digest() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let pins: Vec<(&str, String)> = vec![
        (
            "docker.io/vladyslavhaina/minio-mirror",
            values["minio"]["image"]
                .as_str()
                .expect("minio.image")
                .to_string(),
        ),
        (
            "docker.io/vladyslavhaina/mc-mirror",
            values["minio"]["mcImage"]
                .as_str()
                .expect("minio.mcImage")
                .to_string(),
        ),
    ];
    let mut offenders = Vec::new();
    for (repository, pin) in &pins {
        if !(is_digest_reference(pin) && pin.starts_with(&format!("{repository}@sha256:"))) {
            offenders.push(format!(
                "charts/logweir/values.yaml: must pin the MinIO mirror {repository} by digest, got {pin}"
            ));
        }
    }
    // The withdrawn images AS REFERENCES — a name with a tag or a digest, which
    // is what a pull uses (`quay.io/…` and `docker.io/…` spellings contain these
    // too). Prose that names the upstream repositories without a tag, as the
    // trackers' history does, is not a reference. Split so that this file does
    // not carry them.
    let withdrawn = [
        concat!("minio/", "minio:"),
        concat!("minio/", "minio@"),
        concat!("minio/", "mc:"),
        concat!("minio/", "mc@"),
    ];
    let mut files: Vec<String> = Vec::new();
    for root in [
        ".github",
        "charts",
        "config",
        "crates",
        "dashboards",
        "deploy",
        "docs",
        "e2e",
        "examples",
        "schemas",
        "scripts",
        "third_party",
        "ui",
        "xtask",
    ] {
        if repo().join(root).is_dir() {
            files.extend(files_under(root));
        }
    }
    for entry in std::fs::read_dir(repo()).expect("the repository root lists") {
        let path = entry.expect("a readable directory entry").path();
        if path.is_file() {
            files.push(path.file_name().unwrap().to_string_lossy().to_string());
        }
    }
    let mut mirror_mentions = BTreeSet::new();
    for file in &files {
        // The provenance record, the generated notices, and the orchestrator's
        // trackers (history, never pulled) may name the upstream images.
        if file.starts_with("third_party/minio-mirror/")
            || file == "THIRD_PARTY_NOTICES.md"
            || file.starts_with("docs/to-do/")
        {
            continue;
        }
        // Binary fixtures are not image references.
        let Ok(text) = std::fs::read_to_string(repo().join(file)) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            for name in withdrawn {
                if line.contains(name) {
                    offenders.push(format!("{file}:{}: names `{name}`", n + 1));
                }
            }
            for (repository, pin) in &pins {
                let short = repository.trim_start_matches("docker.io/");
                let mut rest = line;
                while let Some(at) = rest.find(short) {
                    rest = &rest[at + short.len()..];
                    mirror_mentions.insert(file.clone());
                    if let Some(digest) = rest.strip_prefix("@sha256:") {
                        let digest: String = digest.chars().take(64).collect();
                        if !pin.ends_with(&digest) {
                            offenders.push(format!(
                                "{file}:{}: pins {short}@sha256:{digest}, not the chart's {pin}",
                                n + 1
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "MinIO image references that are not the mirror digest the chart pins \
         (third_party/minio-mirror/README.md says why):\n  {}",
        offenders.join("\n  ")
    );
    // NOT VACUOUS: the sweep reached the files that run MinIO, so a walker that
    // listed nothing could not pass this test.
    for must in [
        "e2e/compose/docker-compose.yml",
        "charts/logweir/rendered/demo.yaml",
        "deploy/poc/minio-grants.yaml",
        "scripts/test-k8s-scram.py",
        "e2e/k8s/d2/d2_live.py",
    ] {
        assert!(
            mirror_mentions.contains(must),
            "{must} names no MinIO mirror image: the sweep did not reach it, or it lost its pin"
        );
    }
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
///   * the twenty-six files' BYTES -> `scripts/check-image-ui.sh` check 1, which
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
        "Task 39: the proxy runs LOGWEIR'S OWN `logweir-ui` image — kubectl with the twenty-six \
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
        // D1 §8.5/§8.6 (PLAT-06.2): `backups` joined this rule so legacy mode
        // can perform "Back up now" instead of refusing it by name. The verb
        // is `create` and the resource set is otherwise byte-for-byte what it
        // was; `chart_lint_the_legacy_page_creates_a_backup_and_never_edits_one`
        // below is the row that pins what `backups` may and may not carry.
        (
            vec!["logweir.dev".into()],
            vec![
                "approvals".into(),
                "backups".into(),
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

/// **The legacy page may CREATE a `Backup` and may never edit or delete one
/// (D1 §8.5/§8.6, PLAT-06.2).**
///
/// THE VERB SET FOR ONE RESOURCE, COLLECTED ACROSS EVERY RULE, because that is
/// the only way a grant actually reads: RBAC unions the rules, so a table that
/// checked each rule on its own would miss a second rule that added `delete` to
/// `backups` beside the first one's `create`. This gathers every verb any rule
/// in this role grants on `backups` and compares the whole set.
///
/// WHY EXACTLY THESE THREE. `get` and `list` are what the runs page and the
/// schedule card have always read. `create` is PLAT-06.2's one addition: a
/// manual run is an ordinary object (D1 §8.1) and the page creates it directly
/// in this mode, under the same deterministic name the product API derives.
/// `patch`, `update` and `delete` are absent BY DESIGN — a run's inputs are
/// frozen (PLAT-06.1), the task's migration rule is "do not mutate an existing
/// execution to retry it", and a second run is a second object. A page that
/// could patch a `Backup` could retarget a frozen plan; a page that could
/// delete one could erase evidence.
///
/// THE MUTANT: add `"delete"` (or `"patch"`, or `"update"`) to the `backups`
/// rule in `charts/logweir/templates/ui/ui.yaml` and this fails naming the verb,
/// as does `chart_lint_ui_renders_the_proxy_with_its_paths_and_a_narrow_role`'s
/// whole-table comparison. Removing `create` fails here too, which is what
/// keeps the legacy button from going back to refusing itself.
#[test]
fn chart_lint_the_legacy_page_creates_a_backup_and_never_edits_one() {
    let want: BTreeSet<String> = ["create", "get", "list"] // engine-token-ok: Kubernetes RBAC verbs the legacy page issues, never an engine subcommand
        .into_iter()
        .map(str::to_string)
        .collect();
    // BOTH RENDERS THAT HAVE A PAGE AT ALL, because `ui.enabled` is off by
    // default and a grant proved in one values file says nothing about the
    // other. `demo` is the local lab's shape and `msk` is the managed-cloud
    // one; they are the two renders `logweir-ui` appears in.
    for render in ["demo", "msk"] {
        let docs = rendered(render);
        let role = find(&docs, "ClusterRole", "logweir-ui");
        let mut on_backups: BTreeSet<String> = BTreeSet::new();
        for (groups, resources, verbs) in rules_of(role) {
            if groups.iter().any(|g| g == "logweir.dev") && resources.iter().any(|r| r == "backups")
            {
                on_backups.extend(verbs);
            }
        }
        assert_eq!(
            want, on_backups,
            "in the {render} render, the legacy page's ClusterRole must grant exactly get, list \
             and create on `backups` (D1 §8.5/§8.6): a manual run is an ordinary create, and a \
             run is never edited or deleted from a browser. Found {on_backups:?}"
        );
    }
}

// ================================================= D3 W13: the console/API principal

/// The four kube verbs `crates/logweir-api/src/kube.rs` can spend. The adapter
/// has no other method, and `logweir-api`'s
/// `linkage.rs::the_adapter_calls_only_the_four_permitted_kubernetes_verbs`
/// is what keeps that true crate-side.
const CONSOLE_VERBS: [&str; 4] = ["create", "get", "list", "patch"]; // engine-token-ok: the four Kubernetes RBAC verbs the console adapter spends; this file parses ClusterRoles and invokes no engine

/// The two of them a read costs. Named rather than spelled at each use site, so
/// the two loops below need no escape comment of their own.
const READ_VERBS: [&str; 2] = ["get", "list"]; // engine-token-ok: Kubernetes RBAC verbs, never the denied kafka-backup subcommand

/// Rust type -> the RBAC plural an rule names it by, for every kind the console
/// adapter's seals can carry.
fn console_plural(ty: &str) -> &'static str {
    match ty {
        "KafkaCluster" => "kafkaclusters",
        "BackupSchedule" => "backupschedules",
        "Backup" => "backups",
        "Restore" => "restores",
        "Approval" => "approvals",
        "BackupDestination" => "backupdestinations",
        "TopicDiscovery" => "topicdiscoveries",
        "Preflight" => "preflights",
        "ProtectionPolicy" => "protectionpolicies",
        "RehearsalSchedule" => "rehearsalschedules",
        "RecoveryCatalog" => "recoverycatalogs",
        "RetentionPolicy" => "retentionpolicies",
        "TrustPolicy" => "trustpolicies",
        other => panic!(
            "`crates/logweir-api/src/kube.rs` seals the type `{other}`, which this test cannot \
             map to an RBAC plural. Add the mapping — a sealed kind with no plural is a kind \
             whose grant cannot be checked."
        ),
    }
}

/// Every `impl <trait> for <Type> {}` line in the console adapter, as plurals.
///
/// TEXTUAL, ON PURPOSE. `crates/logweir` cannot depend on `logweir-api` (it is
/// the CLI), so the seal is read the way `manifest_lint`'s `api_callers` reads
/// `weirkeeper`: from the source. A seal that stopped being spelled this way
/// would empty the set, so every caller of this helper asserts a floor on its
/// size.
fn console_sealed(trait_name: &str) -> BTreeSet<String> {
    let text = read("crates/logweir-api/src/kube.rs");
    let needle = format!("impl {trait_name} for ");
    let mut out = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix(&needle) else {
            continue;
        };
        let ty = rest
            .trim_end_matches("{}")
            .trim()
            .rsplit("::")
            .next()
            .expect("a type name")
            .trim();
        out.insert(console_plural(ty).to_string());
    }
    out
}

/// **`api.enabled` renders the console/API principal, and its grants are the
/// sealed adapter's `(verb, resource)` pairs — in both directions.**
///
/// # Why this role exists at all, and why it is wider than the proxy's
///
/// `<release>-ui` acts for WHOEVER REACHES ITS SERVICE, so its role is measured
/// from the page's own request sites and is as small as the page. `<release>-api`
/// acts for a SERVICE that authorizes every request itself
/// (`crates/logweir-api/src/authz.rs`), so its role is the union of what every
/// route may ever need and the per-actor narrowing happens above it. Neither
/// shape can be used to justify the other, which is why the two are separate
/// objects with separate arguments and this test never compares them.
///
/// # The two directions, and what each one catches
///
/// **Every call has a grant.** The adapter's three seals are read out of
/// `kube.rs` and every sealed kind must be readable through this role;
/// `CancellableCheck`'s two kinds must additionally be patchable. A kind added
/// to `ProductResource` with no rule here is a console whose new route 403s in
/// production while every route-table test stays green — the defect
/// `manifest_lint::every_call_site_has_a_grant` exists for, on the other
/// service.
///
/// **Every grant has a caller.** No resource outside the seals plus the two
/// core objects, no verb outside the adapter's four, and every `(resource,
/// verb)` pair that the seals do not yet explain is listed in
/// [`GRANTED_AHEAD_OF_ITS_CALLER`] with its reason — and that list is
/// SELF-LIQUIDATING: an entry whose kind has since reached the seal fails here,
/// naming it, so the exemption cannot outlive the wave it was written for.
///
/// # Mutants
///
/// Drop the D3 read rule: direction two's floor and the exact table both fail.
/// Add `watch` to any rule: the verb allowlist fails naming it. Add `get` on
/// `secrets`: `no_shipped_role_may_write_or_read_a_secret_it_does_not_name` in
/// `manifest_lint` fails over the rendered file, and the exact table here fails
/// too. Add a ninth kind to `ProductResource` without a rule: direction one
/// fails naming the plural.
#[test]
fn chart_lint_the_console_principal_holds_exactly_what_the_sealed_adapter_spends() {
    let docs = rendered("demo");
    find(&docs, "ServiceAccount", "logweir-api");
    let role = find(&docs, "ClusterRole", "logweir-api");
    let cluster_role = find(&docs, "ClusterRole", "logweir-api-trustpolicies");

    // --- the exact table ---------------------------------------------------
    let eight_sealed = [
        "approvals",
        "backupdestinations",
        "backups",
        "backupschedules",
        "kafkaclusters",
        "preflights",
        "restores",
        "topicdiscoveries",
    ];
    let want: BTreeSet<(Vec<String>, Vec<String>, Vec<String>)> = BTreeSet::from([
        (
            vec!["logweir.dev".to_string()],
            eight_sealed.iter().map(|s| s.to_string()).collect(),
            vec!["get".to_string(), "list".to_string()], // engine-token-ok: an RBAC verb the console spends, never an engine subcommand
        ),
        (
            // All eight since PLAT-19.2: `approvals` is created by the console
            // for an ordinary confirmation, a governed confirmation object and
            // an approver's countersigned submission.
            vec!["logweir.dev".to_string()],
            eight_sealed.iter().map(|s| s.to_string()).collect(),
            vec!["create".to_string()],
        ),
        (
            vec!["logweir.dev".to_string()],
            [
                "backupdestinations",
                "backupschedules",
                "preflights",
                "topicdiscoveries",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            vec!["patch".to_string()],
        ),
        (
            vec!["logweir.dev".to_string()],
            [
                "protectionpolicies",
                "recoverycatalogs",
                "rehearsalschedules",
                "retentionpolicies",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            vec!["get".to_string(), "list".to_string()], // engine-token-ok: an RBAC verb the console spends, never an engine subcommand
        ),
        (
            vec!["logweir.dev".to_string()],
            vec!["recoverycatalogs".to_string()],
            vec!["create".to_string()],
        ),
        (
            vec![String::new()],
            vec!["configmaps".to_string()],
            vec!["get".to_string()],
        ),
        (
            vec![String::new()],
            vec!["secrets".to_string()],
            vec!["create".to_string()],
        ),
    ]);
    assert_eq!(
        want,
        rules_of(role),
        "the console principal's namespaced ClusterRole carries exactly the sealed adapter's \
         pairs"
    );
    assert_eq!(
        BTreeSet::from([(
            vec!["logweir.dev".to_string()],
            vec!["trustpolicies".to_string()],
            vec!["get".to_string(), "list".to_string()], // engine-token-ok: an RBAC verb the console spends, never an engine subcommand
        )]),
        rules_of(cluster_role),
        "the cluster-scoped half is `TrustPolicy`, READ ONLY: D3 §10 keeps trust writes off the \
         API in v1 and the supported path is `kubectl apply` under `logweir-trust-admin`"
    );
    // THE ONE ROSTER READ (PREFLIGHT-TRUSTROSTER-STALE): `get` on
    // `trustrosters/default` and nothing else — the adapter's
    // `get_trust_roster` reads that one object by a name it does not take.
    // Compared as YAML, because `rules_of` refuses `resourceNames` by design
    // and this is the one rule whose whole point is its name list.
    let roster_role = find(&docs, "ClusterRole", "logweir-api-trustroster");
    let roster_rules = roster_role.value["rules"]
        .as_sequence()
        .expect("logweir-api-trustroster has rules");
    assert_eq!(roster_rules.len(), 1, "one rule on the roster role");
    assert_eq!(
        serde_yaml::to_string(&roster_rules[0]).unwrap(),
        "apiGroups:\n- logweir.dev\nresources:\n- trustrosters\nresourceNames:\n- default\nverbs:\n- get\n",
        "the console reads TrustRoster/default and no other roster, and never lists them: a \
         list would be every roster's key material, which no route needs"
    );
    let adapter = read("crates/logweir-api/src/kube.rs");
    assert!(
        adapter
            .contains("self.bounded(\"get\", \"trustrosters\", api.get(weirkeeper::ROSTER_NAME))"),
        "the roster grant exists for `KubeAdapter::get_trust_roster`, which reads \
         `weirkeeper::ROSTER_NAME`; a grant with no such caller is critique B M18"
    );
    let roster_binding = find(&docs, "ClusterRoleBinding", "logweir-api-trustroster");
    assert_eq!(
        roster_binding.value["roleRef"]["name"],
        "logweir-api-trustroster"
    );
    assert_eq!(roster_binding.value["subjects"][0]["name"], "logweir-api");
    assert_eq!(
        roster_binding.value["subjects"][0]["namespace"],
        "logweir-system"
    );

    // --- direction one: every call has a grant -----------------------------
    let namespaced = console_sealed("ProductResource");
    assert!(
        namespaced.len() >= 8,
        "the `ProductResource` scan over crates/logweir-api/src/kube.rs found only {} kinds; \
         every assertion below would then be vacuous: {namespaced:?}",
        namespaced.len()
    );
    let granted = |doc: &Doc, resource: &str, verb: &str| -> bool {
        rules_of(doc).iter().any(|(_, resources, verbs)| {
            resources.iter().any(|r| r == resource) && verbs.iter().any(|v| v == verb)
        })
    };
    for plural in &namespaced {
        for verb in READ_VERBS {
            assert!(
                granted(role, plural, verb),
                "`crates/logweir-api/src/kube.rs` seals `{plural}` into `ProductResource`, so \
                 every route may `{verb}` one, and the console's ClusterRole grants no such \
                 verb. A sealed kind with no rule is a route that 403s in production while \
                 every route-table test stays green"
            );
        }
    }
    for plural in console_sealed("CancellableCheck") {
        assert!(
            granted(role, &plural, "patch"),
            "`{plural}` is a `CancellableCheck` — its `spec.cancelRequested` may be raised — and \
             the console's ClusterRole grants no `patch` on it"
        );
    }
    for plural in console_sealed("ClusterResource") {
        for verb in READ_VERBS {
            assert!(
                granted(cluster_role, &plural, verb),
                "`{plural}` is sealed into `ClusterResource` and the cluster-scoped half of the \
                 console's RBAC grants no `{verb}` on it"
            );
        }
    }

    // --- direction two: every grant has a caller ---------------------------
    //
    // The `(resource, verb)` pairs granted AHEAD of the seal that will explain
    // them, each with the wave that lands the caller. D3 W11's API is on its
    // own branch; this wave lands the RBAC half so that merging it is not also
    // an RBAC change, which is the split D3 §14 draws between W11 and W13.
    //
    // SELF-LIQUIDATING, asserted below: when the kind reaches the seal the
    // entry must go, or this test fails naming it.
    // LIQUIDATED 2026-09-19: D3 W11 (0ca3386) landed every caller this list
    // named — the mechanical rule above now covers all of them, so the list is
    // empty and the guard that emptied it stays.
    const GRANTED_AHEAD_OF_ITS_CALLER: [(&str, &str, &str); 0] = [];
    let sealed_now: BTreeSet<String> = namespaced
        .union(&console_sealed("ClusterResource"))
        .cloned()
        .collect();
    for (plural, _, wave) in GRANTED_AHEAD_OF_ITS_CALLER {
        assert!(
            !sealed_now.contains(plural),
            "`{plural}` now reaches the console adapter's seal ({wave} has landed), so it is no \
             longer granted ahead of its caller: remove its rows from \
             GRANTED_AHEAD_OF_ITS_CALLER and let the mechanical rule above cover it. An \
             exemption that outlives its reason is an exemption nobody re-reads"
        );
    }
    let known: BTreeSet<String> = sealed_now
        .iter()
        .cloned()
        .chain(["configmaps".to_string(), "secrets".to_string()])
        .chain(
            GRANTED_AHEAD_OF_ITS_CALLER
                .iter()
                .map(|(r, _, _)| (*r).to_string()),
        )
        .collect();
    let mut checked = 0usize;
    for doc in [role, cluster_role] {
        for (groups, resources, verbs) in rules_of(doc) {
            for g in &groups {
                assert!(
                    g == "logweir.dev" || g.is_empty(),
                    "{}: a rule names the apiGroup `{g}`; the console adapter reaches \
                     `logweir.dev` and core only",
                    doc.name()
                );
            }
            for r in &resources {
                assert!(
                    known.contains(r),
                    "{}: grants a verb on `{r}`, which is neither a kind sealed into the \
                     console adapter nor one of the two core objects nor a recorded \
                     ahead-of-its-caller row. A grant with no caller is critique B M18",
                    doc.name()
                );
                assert!(
                    !r.contains('/'),
                    "{}: names the subresource `{r}`. A reader gets `status` from the object \
                     itself; naming `/status` grants nothing and reads, to an auditor, as \
                     though this service could write one (`config/rbac/viewer_role.yaml`)",
                    doc.name()
                );
            }
            for v in &verbs {
                assert!(
                    CONSOLE_VERBS.contains(&v.as_str()),
                    "{}: grants `{v}`, which the sealed adapter cannot spend — it calls \
                     {CONSOLE_VERBS:?} and has no other method. `watch` in particular is a \
                     long-lived connection this service never opens: D3 §10's event stream is \
                     server-sent events over the service's own reads",
                    doc.name()
                );
                checked += resources.len();
            }
        }
    }
    assert!(
        checked >= 30,
        "only {checked} (resource, verb) pairs were examined across the console's two \
         ClusterRoles; the walk has gone quiet"
    );

    // --- the negatives, by name --------------------------------------------
    for (groups, resources, verbs) in rules_of(role) {
        if groups.iter().any(String::is_empty) && resources.iter().any(|r| r == "secrets") {
            assert_eq!(
                verbs,
                vec!["create".to_string()],
                "the console may CREATE a credential Secret and never read, patch or delete \
                 one — that missing read verb is what makes a console-written credential \
                 write-only, and the SHAPE of the create is fenced by the \
                 ValidatingAdmissionPolicy because RBAC cannot express it"
            );
        }
        if groups.iter().any(String::is_empty) && resources.iter().any(|r| r == "configmaps") {
            assert_eq!(
                verbs,
                vec!["get".to_string()],
                "the console GETs a named ConfigMap — a check's stored result, a catalog view's \
                 page — and never lists them: a console that could page through every ConfigMap \
                 in a namespace is an inventory of somebody else's configuration"
            );
        }
    }

    // --- the bindings, AND THE NAMESPACES THEY ARE IN ----------------------
    //
    // Review finding **F1**, and the reviewer's surviving mutant R3. This block
    // used to assert only that SOME `RoleBinding logweir-api` existed, that its
    // `roleRef.kind` was `ClusterRole`, and that no `ClusterRoleBinding` named
    // the namespaced role. It said nothing about WHICH namespaces — so
    // `logweir.api.namespaces` rewritten to append `kube-system` rendered a
    // console binding in a namespace no value names, and `chart_lint`,
    // `manifest_lint`, `render-install --check` and `just chart-check` all
    // exited 0.
    //
    // That is the one property the RoleBinding-not-ClusterRoleBinding shape
    // exists to buy. Binding the console in an unconfigured namespace is a
    // console read in a namespace the installation never granted — the
    // authorizer would refuse it, but the whole point of the binding shape is
    // that a defect in the authorizer must not reach beyond the configured set.
    //
    // SO THE EXPECTATION IS DERIVED, NOT RESTATED. The set is computed from the
    // example's own values the way `logweir.api.namespaces` computes it — the
    // release namespace, plus `api.namespaces`, or `kubernetes.namespace` when
    // that list is empty — so a mutant that widens the helper fails here, and a
    // mutant that widens the EXAMPLE does not silently move the goalposts.
    for (example, release_namespace) in [
        (Some("demo"), "logweir-system"),
        (Some("identity-multinamespace"), "logweir-system"),
    ] {
        let name = example.expect("an example name");
        let values: Value = serde_yaml::from_str(&read(&format!(
            "charts/logweir/examples/{name}.values.yaml"
        )))
        .expect("the example values parse");
        if values["api"]["enabled"].as_bool() != Some(true) {
            continue;
        }
        let mut want: BTreeSet<String> = BTreeSet::from([release_namespace.to_string()]);
        let listed: Vec<String> = values["api"]["namespaces"]
            .as_sequence()
            .map(|s| {
                s.iter()
                    .map(|v| v.as_str().expect("a namespace name").to_string())
                    .collect()
            })
            .unwrap_or_default();
        if listed.is_empty() {
            if let Some(fallback) = values["kubernetes"]["namespace"].as_str() {
                if !fallback.is_empty() {
                    want.insert(fallback.to_string());
                }
            }
        } else {
            want.extend(listed);
        }

        let rendered_docs = rendered(name);
        let bindings: Vec<&Doc> = rendered_docs
            .iter()
            .filter(|d| {
                d.kind == "RoleBinding"
                    && d.value["roleRef"]["name"].as_str() == Some("logweir-api")
            })
            .collect();
        let got: BTreeSet<String> = bindings
            .iter()
            .map(|d| {
                d.value["metadata"]["namespace"]
                    .as_str()
                    .unwrap_or("<no namespace>")
                    .to_string()
            })
            .collect();
        assert_eq!(
            want, got,
            "rendered/{name}.yaml binds the console's namespaced role in {got:?}, and \
             `api.namespaces` (plus the release namespace) says {want:?}. A binding in a \
             namespace no value names is a console grant the installation never asked for, and \
             it is exactly what the per-namespace RoleBinding shape exists to make visible"
        );
        assert_eq!(
            want.len(),
            bindings.len(),
            "rendered/{name}.yaml carries {} RoleBindings for {} namespaces — one of them is a \
             duplicate, which Helm refuses at install time rather than at render time",
            bindings.len(),
            want.len()
        );
        for rb in bindings {
            assert_eq!(Some("ClusterRole"), rb.value["roleRef"]["kind"].as_str());
            assert_eq!(
                Some("logweir-api"),
                rb.value["subjects"][0]["name"].as_str()
            );
            assert_eq!(
                Some("logweir-system"),
                rb.value["subjects"][0]["namespace"].as_str(),
                "every binding names the ONE ServiceAccount, in the release namespace"
            );
        }
    }
    let bindings: Vec<&Doc> = docs
        .iter()
        .filter(|d| {
            d.kind == "RoleBinding" && d.value["roleRef"]["name"].as_str() == Some("logweir-api")
        })
        .collect();
    assert!(
        !bindings.is_empty(),
        "the namespaced role is bound by a RoleBinding and never by a ClusterRoleBinding: the \
         console's own authorizer refuses an ungranted namespace, and a cluster-wide binding \
         would mean a defect in that check reaches every namespace instead of the configured \
         ones"
    );
    for crb in docs.iter().filter(|d| d.kind == "ClusterRoleBinding") {
        let name = crb.value["roleRef"]["name"].as_str().unwrap_or_default();
        assert_ne!(
            "logweir-api", name,
            "the NAMESPACED role is never bound cluster-wide"
        );
        assert_ne!("cluster-admin", name);
    }

    // AND THE ADMISSION POLICY POINTS AT THIS ACCOUNT. The fence's whole effect
    // is its `matchConditions` subject list; a default that named a different
    // account would install, read as enabled, and fence nobody.
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    assert_eq!(
        Some("logweir-api"),
        values["admissionPolicy"]["consoleServiceAccountName"].as_str(),
        "`admissionPolicy.consoleServiceAccountName`'s default must be the account \
         `api.enabled` renders under the default release name. They are two values that have \
         to agree, and nothing else would notice if they stopped"
    );

    // AND THE FENCE'S SUBJECT CANNOT DIVERGE FROM THE ACCOUNT — review F3.
    //
    // The two names used to be spelled separately: this template rendered
    // `{{ .Release.Name }}-api` and `admission-policy.yaml` took the fixed
    // string `admissionPolicy.consoleServiceAccountName`, whose default is
    // `logweir-api`. They agree under the default release name and NOWHERE
    // ELSE, so `helm install myrel …` rendered `ServiceAccount myrel-api`
    // beside a policy whose only `matchConditions` expression named
    // `logweir-api` — a fence that installs, reads as enabled, and matches no
    // request, leaving the console's `create` on Secrets with no bound at all
    // (RBAC cannot narrow a Secret by SHAPE; that policy is the only thing
    // that can).
    //
    // ASSERTED AS TEXT, and the reason is Global Constraint 22: a `#[test]`
    // may not shell out, so a render under a non-default release name cannot
    // happen here. `scripts/check-chart.sh` performs that render in both
    // directions — divergent pair refused, aligned pair rendering with the
    // release-derived principal — and `chart_lint_the_gate_script_carries_every_arm`
    // holds the script to those arms. What this row holds is the property that
    // makes the refusal possible: ONE definition, called by both files.
    let helpers = read("charts/logweir/templates/_helpers.tpl");
    assert!(
        helpers.contains("define \"logweir.api.serviceAccountName\""),
        "`_helpers.tpl` must define `logweir.api.serviceAccountName`: the console account's \
         name is needed by two templates, and two spellings of it is exactly how the fence \
         came to name a principal the chart does not create"
    );
    for (file, why) in [
        (
            "charts/logweir/templates/ui/api-rbac.yaml",
            "renders the account",
        ),
        (
            "charts/logweir/templates/admission-policy.yaml",
            "fences it",
        ),
    ] {
        let text = read(file);
        assert!(
            text.contains("include \"logweir.api.serviceAccountName\""),
            "{file} {why}, so it must take the name from `logweir.api.serviceAccountName` and \
             never spell `printf \"%s-api\" .Release.Name` again"
        );
        assert!(
            !text.contains("printf \"%s-api\" .Release.Name"),
            "{file} spells the console account's name itself. There is one helper for it, and \
             a second spelling is a second thing to keep in step"
        );
    }
    assert!(
        read("charts/logweir/templates/admission-policy.yaml")
            .contains("if and .Values.api.enabled (ne $name $rendered)"),
        "`admission-policy.yaml` must REFUSE at render time when this chart creates a console \
         account and the fence names a different one. It must not silently substitute the \
         rendered name either: an installation may legitimately fence a console deployed out \
         of band, and `api.enabled` false is how it says so"
    );

    // AND NOTHING OF IT RENDERS WITH THE FLAG OFF.
    for name in ["default", "minimal"] {
        for d in rendered(name) {
            assert!(
                !d.name().starts_with("logweir-api"),
                "rendered/{name}.yaml carries {}/{} with api.enabled off",
                d.kind,
                d.name()
            );
        }
    }
}
// ======================= WHAT AN ACCOUNT CAN REACH, DERIVED FROM THE BINDINGS
//
// trust-stale review LOW-1 and LOW-2. Every RBAC row above FINDS a role or a
// binding BY NAME and reads its rules or its `subjects[0]`. That shape has two
// holes, and the reviewer planted a mutant through each:
//
// * **E** — `<release>-api-trustroster`'s binding gains a second subject,
//   `Group system:authenticated`. `subjects[0]` is still the API account, so
//   every row passed, and every authenticated principal in the cluster could
//   read the roster. (`-trustpolicies`' binding had no subject row at all.)
// * **F** — a NEW ClusterRole, under a name no row knows, granting `list` on
//   `trustrosters`, bound to the API account by a new ClusterRoleBinding. No
//   row looks up a role it does not already know the name of, so every gate
//   passed and the account could list every roster's key material.
//
// So the rows below start from the BINDINGS, not from the roles. For every
// rendered variant, every RoleBinding and ClusterRoleBinding whose subjects
// REACH an account — the account itself, its `system:serviceaccount:` user
// name, or a group it is a member of — must name that account and nobody
// else; and the union of every rule so reached, per scope, must equal a pinned
// allowlist. A new role under any name, a wildcard, a `resourceNames` dropped
// or a binding in an unconfigured namespace all change that union.

/// The namespace every checked-in render is made with (`check-chart.sh`'s
/// `-n logweir-system`).
const RENDER_NAMESPACE: &str = "logweir-system";

/// The console account's grants in EACH namespace it is bound in, as
/// `verb group/resource[@name]` (core is `core`). The sealed adapter's pairs:
/// `chart_lint_the_console_principal_holds_exactly_what_the_sealed_adapter_spends`
/// derives them from `kube.rs`; this list pins what is REACHABLE.
const API_NAMESPACED_GRANTS: [&str; 39] = [
    "get logweir.dev/approvals",
    "list logweir.dev/approvals",
    "create logweir.dev/approvals",
    "get logweir.dev/backupdestinations",
    "list logweir.dev/backupdestinations",
    "create logweir.dev/backupdestinations",
    "patch logweir.dev/backupdestinations",
    "get logweir.dev/backups",
    "list logweir.dev/backups",
    "create logweir.dev/backups",
    "get logweir.dev/backupschedules",
    "list logweir.dev/backupschedules",
    "create logweir.dev/backupschedules",
    "patch logweir.dev/backupschedules",
    "get logweir.dev/kafkaclusters",
    "list logweir.dev/kafkaclusters",
    "create logweir.dev/kafkaclusters",
    "get logweir.dev/preflights",
    "list logweir.dev/preflights",
    "create logweir.dev/preflights",
    "patch logweir.dev/preflights",
    "get logweir.dev/restores",
    "list logweir.dev/restores",
    "create logweir.dev/restores",
    "get logweir.dev/topicdiscoveries",
    "list logweir.dev/topicdiscoveries",
    "create logweir.dev/topicdiscoveries",
    "patch logweir.dev/topicdiscoveries",
    "get logweir.dev/protectionpolicies",
    "list logweir.dev/protectionpolicies",
    "get logweir.dev/recoverycatalogs",
    "list logweir.dev/recoverycatalogs",
    "create logweir.dev/recoverycatalogs",
    "get logweir.dev/rehearsalschedules",
    "list logweir.dev/rehearsalschedules",
    "get logweir.dev/retentionpolicies",
    "list logweir.dev/retentionpolicies",
    "get core/configmaps",
    "create core/secrets",
];

/// The console account's CLUSTER-WIDE grants: `TrustPolicy` read-only, and
/// the ONE roster by name (PREFLIGHT-TRUSTROSTER-STALE) — never a `list` of
/// rosters, which would be every roster's key material.
const API_CLUSTER_GRANTS: [&str; 3] = [
    "get logweir.dev/trustpolicies",
    "list logweir.dev/trustpolicies",
    "get logweir.dev/trustrosters@default",
];

/// The controller's CLUSTER-WIDE grants when `controller.watchNamespaces` is
/// set (D0 stage 5): the two cluster-scoped trust kinds and nothing
/// namespaced. `chart_lint_the_shared_console_is_outside_the_controllers_job_authority`
/// explains each.
const CONTROLLER_SCOPED_CLUSTER_GRANTS: [&str; 8] = [
    "get logweir.dev/trustrosters",
    "list logweir.dev/trustrosters",
    "watch logweir.dev/trustrosters",
    "patch logweir.dev/trustrosters/status",
    "list logweir.dev/trustpolicies",
    "watch logweir.dev/trustpolicies",
    // The compromise-revocation finalizer (TRUSTPOLICY-DELETE-DROPS-REVOCATION):
    // `metadata.finalizers` only, from one pinned call site.
    "patch logweir.dev/trustpolicies",
    "patch logweir.dev/trustpolicies/status",
];

/// Does this binding subject include `system:serviceaccount:<ns>:<sa>`? A
/// ServiceAccount by name, the account's user name, and the three groups every
/// ServiceAccount token carries. An unknown subject kind is assumed to reach,
/// so it is checked rather than skipped.
fn subject_reaches(subject: &Value, ns: &str, sa: &str) -> bool {
    let name = subject["name"].as_str().unwrap_or_default();
    match subject["kind"].as_str().unwrap_or_default() {
        "ServiceAccount" => name == sa && subject["namespace"].as_str() == Some(ns),
        "User" => name == format!("system:serviceaccount:{ns}:{sa}"),
        "Group" => {
            name == "system:authenticated"
                || name == "system:serviceaccounts"
                || name == format!("system:serviceaccounts:{ns}")
        }
        _ => true,
    }
}

/// A Role's or ClusterRole's rules flattened to `verb group/resource[@name]`
/// atoms, one per combination — so two rules that grant the same thing compare
/// equal, and a wildcard or a dropped `resourceNames` is a different atom.
fn grant_atoms(role: &Doc) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if role.value.get("aggregationRule").is_some() {
        out.insert(format!(
            "<aggregationRule on {}: its rules are whatever the cluster aggregates>",
            role.name()
        ));
    }
    let strings = |r: &Value, k: &str| -> Vec<String> {
        r[k].as_sequence()
            .map(|s| {
                s.iter()
                    .map(|v| v.as_str().expect("an RBAC string").to_string())
                    .collect()
            })
            .unwrap_or_default()
    };
    for rule in role.value["rules"].as_sequence().into_iter().flatten() {
        let verbs = strings(rule, "verbs");
        for url in strings(rule, "nonResourceURLs") {
            for verb in &verbs {
                out.insert(format!("{verb} nonResourceURL:{url}"));
            }
        }
        let names = strings(rule, "resourceNames");
        for group in strings(rule, "apiGroups") {
            let group = if group.is_empty() {
                "core".to_string()
            } else {
                group
            };
            for resource in strings(rule, "resources") {
                for verb in &verbs {
                    if names.is_empty() {
                        out.insert(format!("{verb} {group}/{resource}"));
                    }
                    for name in &names {
                        out.insert(format!("{verb} {group}/{resource}@{name}"));
                    }
                }
            }
        }
    }
    out
}

/// Every grant `system:serviceaccount:<ns>:<sa>` holds in one render, keyed by
/// scope (`cluster`, or the namespace a RoleBinding is in) — after refusing any
/// binding that reaches the account and names anyone else beside it (mutant E).
fn reachable_grants(
    render: &str,
    docs: &[Doc],
    ns: &str,
    sa: &str,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for binding in docs
        .iter()
        .filter(|d| d.kind == "RoleBinding" || d.kind == "ClusterRoleBinding")
    {
        let subjects: &[Value] = binding.value["subjects"]
            .as_sequence()
            .map(Vec::as_slice)
            .unwrap_or_default();
        if !subjects.iter().any(|s| subject_reaches(s, ns, sa)) {
            continue;
        }
        let only = &subjects[0];
        assert!(
            subjects.len() == 1
                && only["kind"].as_str() == Some("ServiceAccount")
                && only["name"].as_str() == Some(sa)
                && only["namespace"].as_str() == Some(ns),
            "rendered/{render}.yaml: {}/{} reaches ServiceAccount {ns}/{sa}, so its subjects \
             must be exactly that ServiceAccount and nothing else; they are {}. A second \
             subject hands the account's whole grant to whoever it names — trust-stale review \
             LOW-1, mutant E, was `Group system:authenticated` on the roster binding",
            binding.kind,
            binding.name(),
            serde_yaml::to_string(subjects).unwrap_or_default().trim()
        );
        let scope = if binding.kind == "ClusterRoleBinding" {
            "cluster".to_string()
        } else {
            binding.value["metadata"]["namespace"]
                .as_str()
                .unwrap_or_else(|| panic!("RoleBinding {} has no namespace", binding.name()))
                .to_string()
        };
        let role_kind = binding.value["roleRef"]["kind"]
            .as_str()
            .unwrap_or_default();
        let role_name = binding.value["roleRef"]["name"]
            .as_str()
            .unwrap_or_default();
        let role = docs.iter().find(|d| {
            d.kind == role_kind
                && d.name() == role_name
                && (role_kind == "ClusterRole"
                    || d.value["metadata"]["namespace"].as_str() == Some(scope.as_str()))
        });
        let atoms = out.entry(scope).or_default();
        match role {
            Some(role) => atoms.extend(grant_atoms(role)),
            // A role the render does not carry (`cluster-admin`, `edit`, a
            // typo): its rules are unknowable here, so it can never equal the
            // allowlist.
            None => {
                atoms.insert(format!("<unrendered {role_kind}/{role_name}>"));
            }
        }
    }
    out
}

/// An example's values (or the chart's defaults for `default`).
fn variant_values(render: &str) -> Value {
    let path = format!("charts/logweir/examples/{render}.values.yaml");
    if render == "default" {
        return Value::Null;
    }
    serde_yaml::from_str(&read(&path)).expect("the example values parse")
}

fn string_list(v: &Value) -> Vec<String> {
    v.as_sequence()
        .map(|s| {
            s.iter()
                .map(|x| x.as_str().expect("a string").to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// `logweir.api.namespaces`, recomputed from an example's values: the release
/// namespace, plus `api.namespaces`, or `kubernetes.namespace` when that list
/// is empty (the chart default of both is empty).
fn api_namespaces(values: &Value) -> BTreeSet<String> {
    let mut want = BTreeSet::from([RENDER_NAMESPACE.to_string()]);
    let listed = string_list(&values["api"]["namespaces"]);
    if listed.is_empty() {
        if let Some(fallback) = values["kubernetes"]["namespace"].as_str() {
            if !fallback.is_empty() {
                want.insert(fallback.to_string());
            }
        }
    } else {
        want.extend(listed);
    }
    want
}

fn pinned(atoms: &[&str]) -> BTreeSet<String> {
    atoms.iter().map(|s| (*s).to_string()).collect()
}

/// `want == got`, reported as the per-scope DIFFERENCE — the whole maps are a
/// hundred lines each and the one extra atom is what the reader needs.
fn assert_grants(
    render: &str,
    account: &str,
    want: &BTreeMap<String, BTreeSet<String>>,
    got: &BTreeMap<String, BTreeSet<String>>,
    why: &str,
) {
    let empty = BTreeSet::new();
    let mut diff = Vec::new();
    for scope in want.keys().chain(got.keys()).collect::<BTreeSet<_>>() {
        let w = want.get(scope).unwrap_or(&empty);
        let g = got.get(scope).unwrap_or(&empty);
        for extra in g.difference(w) {
            diff.push(format!("  + [{scope}] {extra}"));
        }
        for missing in w.difference(g) {
            diff.push(format!("  - [{scope}] {missing}"));
        }
    }
    assert!(
        diff.is_empty(),
        "rendered/{render}.yaml: what ServiceAccount {RENDER_NAMESPACE}/{account} can reach, \
         through EVERY binding that names it, is not its pinned grant (+ reachable and not \
         pinned, - pinned and not reachable):\n{}\n{why}",
        diff.join("\n")
    );
}

/// Mutants E and F, and the class they stand for: the console account and the
/// controller account each hold, in every rendered variant, EXACTLY a pinned
/// set of grants per scope, computed from every binding that reaches them.
#[test]
fn chart_lint_every_grant_reaching_the_api_and_controller_accounts_is_pinned() {
    let renders: Vec<String> = files_under("charts/logweir/rendered")
        .into_iter()
        .filter_map(|p| {
            p.strip_prefix("charts/logweir/rendered/")
                .and_then(|f| f.strip_suffix(".yaml"))
                .map(str::to_string)
        })
        .collect();
    assert!(
        renders.len() >= 10,
        "only {} rendered variants were found; the walk has gone quiet: {renders:?}",
        renders.len()
    );
    let controller_role = docs_in("config/rbac/role.yaml");
    let controller_grants = grant_atoms(find(&controller_role, "ClusterRole", "weirkeeper"));
    assert!(
        controller_grants.len() >= 30,
        "config/rbac/role.yaml's weirkeeper role flattened to {} grants",
        controller_grants.len()
    );
    let mut api_variants = 0usize;
    for render in &renders {
        let docs = rendered(render);
        let values = variant_values(render);

        // THE CONSOLE ACCOUNT.
        let mut want: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        if values["api"]["enabled"].as_bool() == Some(true) {
            api_variants += 1;
            want.insert("cluster".to_string(), pinned(&API_CLUSTER_GRANTS));
            for ns in api_namespaces(&values) {
                want.insert(ns, pinned(&API_NAMESPACED_GRANTS));
            }
            // CHART GAP G6: a shared console that names the ingress
            // controller's Service holds ONE more atom, in THAT namespace
            // only — the `list_page endpointslices` pair `logweir-api`'s
            // `linkage.rs` pins — and nothing else anywhere.
            let console = &values["api"]["console"];
            let proxy = &console["trustedProxyService"];
            if console["enabled"].as_bool() == Some(true)
                && console["mode"].as_str() == Some("shared")
            {
                if let (Some(ns), Some(_)) = (proxy["namespace"].as_str(), proxy["name"].as_str()) {
                    want.entry(ns.to_string())
                        .or_default()
                        .insert("list discovery.k8s.io/endpointslices".to_string());
                }
            }
        }
        assert_grants(
            render,
            "logweir-api",
            &want,
            &reachable_grants(render, &docs, RENDER_NAMESPACE, "logweir-api"),
            "A role under a name no other row looks up still lands here — trust-stale review \
             LOW-2, mutant F, was a new ClusterRole granting `list trustrosters`. Widen \
             API_NAMESPACED_GRANTS / API_CLUSTER_GRANTS only together with the route that \
             spends the grant",
        );

        // THE CONTROLLER ACCOUNT. Its role is `config/rbac/role.yaml`'s,
        // which `manifest_lint` holds call for call; this row holds what is
        // BOUND to it to that role and nothing more.
        let watched = string_list(&values["controller"]["watchNamespaces"]);
        let mut want: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        if watched.is_empty() {
            want.insert("cluster".to_string(), controller_grants.clone());
        } else {
            want.insert(
                "cluster".to_string(),
                pinned(&CONTROLLER_SCOPED_CLUSTER_GRANTS),
            );
            for ns in watched {
                want.insert(ns, controller_grants.clone());
            }
            want.insert(
                RENDER_NAMESPACE.to_string(),
                pinned(&["get core/configmaps@weirkeeper-policy"]),
            );
        }
        assert_grants(
            render,
            "weirkeeper",
            &want,
            &reachable_grants(render, &docs, RENDER_NAMESPACE, "weirkeeper"),
            "The controller holds `config/rbac/role.yaml`'s weirkeeper role — cluster-wide, or \
             per watched namespace when `controller.watchNamespaces` is set — and nothing else",
        );
    }
    assert!(
        api_variants >= 4,
        "only {api_variants} rendered variants enable the console; the API half has gone quiet"
    );
}

// ============================================ D0 stage 7: the console WORKLOAD
//
// `chart_lint_the_console_principal_holds_exactly_what_the_sealed_adapter_spends`
// above is about the GRANT. The three rows below are about the POD that finally
// uses it — the half `templates/ui/api-rbac.yaml`'s own header recorded as
// missing ("There is no Deployment, no Service, no image and no Ingress here").

/// A `defaultMode` as KUBERNETES reads it, which is not how this test's YAML
/// parser reads it.
///
/// `defaultMode: 0440` is the idiom every Kubernetes example uses and is what
/// an operator expects to see in `helm template` output, so the template writes
/// it. But the two parsers disagree about that token: the API server's
/// (go-yaml, YAML 1.1) treats a leading zero as OCTAL and stores 288, while
/// `serde_yaml` (YAML 1.2 core schema) does not accept a leading zero as an
/// integer at all and hands back the string `"0440"`. Neither is wrong; they
/// implement different versions of the specification.
///
/// So this helper reads it the way the cluster does — a plain integer stays as
/// it is, and a leading-zero string is parsed base 8 — and the divergence is
/// written down here rather than discovered again by whoever next asserts a
/// file mode.
fn file_mode(value: &Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    let text = value.as_str()?;
    let digits = text.strip_prefix('0').unwrap_or(text);
    u64::from_str_radix(digits, 8).ok()
}

/// The console config document of one rendered file, parsed.
///
/// There is exactly one ConfigMap whose name begins `<release>-api-config-` in
/// a render that has the console on, it is `immutable: true`, and its single
/// key is `config.yaml` — the file `logweir-api --config` reads.
fn console_config(render: &str) -> (String, Value) {
    let docs = rendered(render);
    let maps: Vec<&Doc> = docs
        .iter()
        .filter(|d| d.kind == "ConfigMap" && d.name().starts_with("logweir-api-config-"))
        .collect();
    assert_eq!(
        1,
        maps.len(),
        "{render}.yaml must carry exactly one console ConfigMap; found {:?}",
        maps.iter().map(|d| d.name()).collect::<Vec<_>>()
    );
    let map = maps[0];
    assert_eq!(
        Some(true),
        map.value["immutable"].as_bool(),
        "{render}.yaml: the console configuration is immutable — nobody with `patch configmaps` \
         changes the role table under a running console, and the name is content-addressed so an \
         edit is a new object and a pod roll"
    );
    let data = map.value["data"].as_mapping().expect("a data mapping");
    assert_eq!(
        1,
        data.len(),
        "{render}.yaml: the console ConfigMap carries config.yaml and nothing else"
    );
    let text = map.value["data"]["config.yaml"]
        .as_str()
        .expect("config.yaml")
        .to_string();
    let parsed: Value = serde_yaml::from_str(&text).expect("the console config parses as YAML");
    (text, parsed)
}

/// **The console pod is non-root, read-only, holds its own token, and probes
/// only where a probe can reach it.**
///
/// EVERY CLAUSE HERE IS ONE AN OPERATOR WOULD OTHERWISE HAVE TO TAKE ON TRUST,
/// and two of them are the ones that make `readOnlyRootFilesystem` and
/// `runAsNonRoot` true statements about the running process rather than about
/// the template: the image declares `USER 65532:65532` (asserted by
/// `scripts/check-image-api.sh` check 4) and the kubelet REFUSES to start a
/// container whose image declares `USER root` under `runAsNonRoot: true`, so
/// the two halves have to agree or the console never starts at all.
///
/// THE PROBES ARE ASSERTED IN BOTH DIRECTIONS, which is the part worth reading.
/// `shared` mode binds the Pod IP and gets `/healthz` and `/readyz`.
/// `localAdmin` mode binds loopback — `crates/logweir-api/src/config.rs` refuses
/// anything else — and gets NEITHER, because the kubelet probes the POD IP from
/// the node's network namespace: a readiness probe would hold a working console
/// permanently NotReady and a liveness probe would restart it forever. A probe
/// that cannot succeed is an outage, not a weaker check.
///
/// MUTANT: delete the container `securityContext` block from
/// `templates/ui/api-deployment.yaml`, or drop `runAsNonRoot`, or give
/// localAdmin mode an httpGet probe — each fails here naming the field.
#[test]
fn chart_lint_the_console_pod_is_non_root_read_only_and_probes_only_where_it_can() {
    for (render, mode) in [("console", "localAdmin"), ("console-shared", "shared")] {
        let docs = rendered(render);
        let deployment = find(&docs, "Deployment", "logweir-api");
        let pod = pod_spec(deployment);

        assert_eq!(
            Some("logweir-api"),
            pod["serviceAccountName"].as_str(),
            "{render}.yaml: the console pod must run as the principal api-rbac.yaml renders; any \
             other account holds no grant on any Logweir kind"
        );
        assert_eq!(
            Some(true),
            pod["automountServiceAccountToken"].as_bool(),
            "{render}.yaml: this service composes every Kubernetes call itself through one sealed \
             adapter, so it needs its projected token — unlike the runner Jobs, which mount none"
        );
        assert_eq!(
            Some(true),
            pod["securityContext"]["runAsNonRoot"].as_bool(),
            "{render}.yaml: pod securityContext.runAsNonRoot"
        );
        assert_eq!(
            Some(65532),
            pod["securityContext"]["runAsUser"].as_u64(),
            "{render}.yaml: the same UID all four images declare"
        );
        assert_eq!(
            Some(65532),
            pod["securityContext"]["runAsGroup"].as_u64(),
            "{render}.yaml: pod securityContext.runAsGroup"
        );
        assert_eq!(
            Some("RuntimeDefault"),
            pod["securityContext"]["seccompProfile"]["type"].as_str(),
            "{render}.yaml: pod securityContext.seccompProfile"
        );

        // THE KEY FILES MUST BE READABLE BY THE UID THAT READS THEM, and this
        // pair is here because the first live install of this component
        // CrashLoopBackOff'd on exactly it: the kubelet writes a Secret volume
        // owned by ROOT, so `defaultMode: 0400` means "readable by root alone"
        // and the container, running as 65532, got `cannot read the
        // configuration file …/cursor.key: Permission denied (os error 13)`.
        // A file mode is only wrong relative to a UID, so no render assertion
        // that looked at the mode alone could have caught it — this one reads
        // the two together.
        assert_eq!(
            Some(65532),
            pod["securityContext"]["fsGroup"].as_u64(),
            "{render}.yaml: the pod must set fsGroup: 65532, or the kubelet leaves its Secret \
             volumes root-owned and the key files are unreadable by the process that needs them"
        );
        for volume in pod["volumes"].as_sequence().expect("volumes") {
            let Some(secret) = volume.get("secret") else {
                continue;
            };
            let name = volume["name"].as_str().unwrap_or("<unnamed>");
            let mode = file_mode(&secret["defaultMode"]).unwrap_or_else(|| {
                panic!("{render}.yaml: secret volume `{name}` names no defaultMode")
            });
            assert_eq!(
                0o440, mode,
                "{render}.yaml: secret volume `{name}` has mode {mode:o}. It must be 0440: the \
                 group bit is what the process reads through under fsGroup: 65532 (0400 is a \
                 CrashLoopBackOff), and the world bit must stay off."
            );
            assert_eq!(
                Some(true),
                volume["secret"]
                    .get("optional")
                    .map_or(Some(true), |o| Some(o.as_bool() != Some(true))),
                "{render}.yaml: secret volume `{name}` must not be optional — a missing key \
                 Secret should hold the pod in ContainerCreating with an event naming it, not \
                 start a console that invented its own key"
            );
        }

        let c = container(deployment);
        let sc = &c["securityContext"];
        assert!(
            sc.is_mapping(),
            "{render}.yaml: the console container has NO securityContext. Without it the container \
             keeps every default capability, may escalate privileges, and writes to its root \
             filesystem — on the one pod in this installation that mounts a session key, a cursor \
             MAC key and an OIDC client secret."
        );
        assert_eq!(
            Some(false),
            sc["allowPrivilegeEscalation"].as_bool(),
            "{render}.yaml: container securityContext.allowPrivilegeEscalation"
        );
        assert_eq!(
            Some(true),
            sc["readOnlyRootFilesystem"].as_bool(),
            "{render}.yaml: container securityContext.readOnlyRootFilesystem — the page is read \
             into memory at startup and nothing else is written"
        );
        assert_eq!(
            Some(&Value::from(vec!["ALL"])),
            sc["capabilities"].get("drop"),
            "{render}.yaml: container securityContext.capabilities.drop must be [\"ALL\"]"
        );

        // The one writable path, and it holds no state.
        let mounts = c["volumeMounts"].as_sequence().expect("volumeMounts");
        let writable: Vec<String> = mounts
            .iter()
            .filter(|m| m["readOnly"].as_bool() != Some(true))
            .map(|m| m["mountPath"].as_str().unwrap_or("<none>").to_string())
            .collect();
        assert_eq!(
            vec!["/tmp".to_string()],
            writable,
            "{render}.yaml: /tmp is the only writable mount under readOnlyRootFilesystem"
        );

        assert_eq!(
            Some(&Value::from(vec![
                "--config",
                "/etc/logweir/api/config.yaml"
            ])),
            c.get("args"),
            "{render}.yaml: the console takes its whole configuration from the mounted file; the \
             image declares no CMD so this argument vector is the only one"
        );
        assert_eq!(
            Some(format!("{}:{LOGWEIR_TAG}", console_repository()).as_str()),
            c["image"].as_str(),
            "{render}.yaml: the console pod runs the logweir-console image, never ui.image — \
             those are two different principals with two different arguments"
        );

        let has_probe = c.get("readinessProbe").is_some_and(|p| !p.is_null())
            || c.get("livenessProbe").is_some_and(|p| !p.is_null());
        if mode == "shared" {
            assert_eq!(
                Some("/readyz"),
                c["readinessProbe"]["httpGet"]["path"].as_str(),
                "{render}.yaml: shared mode binds the Pod IP, so the kubelet can and must probe it"
            );
            assert_eq!(
                Some("/healthz"),
                c["livenessProbe"]["httpGet"]["path"].as_str(),
                "{render}.yaml: /healthz reports process liveness only and consults nothing"
            );
        } else {
            assert!(
                !has_probe,
                "{render}.yaml: localAdmin mode binds 127.0.0.1 (config.rs refuses anything else), \
                 and the kubelet probes the POD IP. A probe here cannot connect, so it would hold \
                 a working console NotReady or restart it forever."
            );
        }
    }

    // THE Service IS `shared` MODE'S ALONE, and this pair is the assertion the
    // review turned into a requirement. A loopback listener with a Service in
    // front of it has populated Endpoints — the pod is Ready the moment it
    // starts, there being no readiness probe — and refuses every connection: a
    // security property to a reader of the template, an outage to every monitor
    // in the cluster, and an object any pod can dial. `kubectl port-forward`
    // takes a Deployment directly, so the in-cluster administrator mode needs
    // no Service and now renders none.
    assert!(
        !names_of(&rendered("console"), "Service").contains("logweir-api"),
        "console.yaml: the in-cluster administrator mode must render NO Service. Its listener is \
         127.0.0.1 (config.rs refuses anything else), so a Service there would refuse every \
         connection while advertising a ready endpoint. The documented path is \
         `kubectl port-forward deploy/<release>-api`."
    );
    assert!(
        names_of(&rendered("console-shared"), "Service").contains("logweir-api"),
        "console-shared.yaml: shared mode binds the Pod IP and its Ingress needs a backend, so \
         this is the one mode that renders a Service"
    );
    // …and the NetworkPolicy tells the same story: an allow rule where there is
    // something to reach, an explicit deny where there is not.
    let local_docs = rendered("console");
    let local_np = find(&local_docs, "NetworkPolicy", "logweir-api");
    assert_eq!(
        Some(0),
        local_np.value["spec"]["ingress"]
            .as_sequence()
            .map(Vec::len),
        "console.yaml: the in-cluster administrator mode's NetworkPolicy must carry an EMPTY \
         ingress list — with `Ingress` in policyTypes that is deny, and it is the honest rule \
         when there is no Service and the listener is loopback"
    );
    let shared_docs = rendered("console-shared");
    let shared_np = find(&shared_docs, "NetworkPolicy", "logweir-api");
    assert_eq!(
        Some(1),
        shared_np.value["spec"]["ingress"]
            .as_sequence()
            .map(Vec::len),
        "console-shared.yaml: shared mode admits the configured ingress controller, and only it"
    );

    // A PodDisruptionBudget only where one can do any good: the default render
    // is one replica and must NOT have one (it would block `kubectl drain`
    // forever on the node carrying the only console pod); the shared example
    // runs two and does.
    assert!(
        !names_of(&rendered("console"), "PodDisruptionBudget").contains("logweir-api"),
        "console.yaml runs one replica: a PDB over it turns an availability object into a node \
         drain that never completes"
    );
    let shared = rendered("console-shared");
    assert_eq!(
        Some(2),
        find(&shared, "Deployment", "logweir-api").value["spec"]["replicas"].as_u64()
    );
    assert_eq!(
        Some(1),
        find(&shared, "PodDisruptionBudget", "logweir-api").value["spec"]["maxUnavailable"]
            .as_u64(),
        "above one replica the budget is a rolling disruption, never a simultaneous one"
    );
}

/// **The console's configuration carries no credential — only paths — and names
/// the in-cluster identity.**
///
/// A ConfigMap is readable by anything with `get configmaps` in the namespace,
/// it appears in `helm get manifest`, and it is checked into
/// `charts/logweir/rendered/`. The console is the one component in this chart
/// that HAS three credentials (a session key, a cursor MAC key and an OIDC
/// client secret), so "none of them is in this object" is the assertion that
/// has to be made about it rather than assumed.
///
/// THE SCAN IS OVER THE PARSED DOCUMENT AND OVER ITS TEXT, because the two miss
/// different things: the parse catches a credential-named key that holds a
/// value instead of a path, and the raw text catches one hidden in a comment or
/// in a key this test does not know the name of.
///
/// It also pins the two settings that decide WHOSE authority the console acts
/// with, and neither is a chart value: `kubernetes.source: inCluster` (a
/// kubeconfig would be an identity nobody audits — routinely cluster-admin on a
/// developer's laptop) and `uiDirectory: /ui` (the path both page-carrying
/// images use).
///
/// MUTANT: put `clientSecret: hunter2` — or any literal — into the rendered
/// config document, or switch `kubernetes.source` to `kubeconfig`; each fails
/// here naming the key.
#[test]
fn chart_lint_the_console_config_map_carries_no_credential() {
    // Keys whose VALUE would be a credential. `*File`/`*Secret`/`*Ref` names
    // are paths and references and are deliberately not on this list.
    const CREDENTIAL_KEYS: [&str; 8] = [
        "clientSecret",
        "password",
        "token",
        "key",
        "secret",
        "privateKey",
        "sessionKeyValue",
        "cursorKeyValue",
    ];

    for (render, mode) in [("console", "localAdmin"), ("console-shared", "shared")] {
        let (text, config) = console_config(render);

        fn walk(node: &Value, path: &str, render: &str) {
            match node {
                Value::Mapping(map) => {
                    for (k, v) in map {
                        let name = k.as_str().unwrap_or("<non-string>");
                        let here = if path.is_empty() {
                            name.to_string()
                        } else {
                            format!("{path}.{name}")
                        };
                        if CREDENTIAL_KEYS.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                            panic!(
                                "{render}.yaml: the console ConfigMap carries `{here}`. Every \
                                 credential this service reads is a PATH into a mounted Secret \
                                 volume — `oidc.clientSecretFile`, `sessionKey.file`, \
                                 `cursorKey.file`/`cursorKeyFile` — and never a value. A \
                                 ConfigMap is readable by anything with `get configmaps`, it is \
                                 in `helm get manifest`, and it is checked into this repository."
                            );
                        }
                        walk(v, &here, render);
                    }
                }
                Value::Sequence(items) => {
                    for (i, item) in items.iter().enumerate() {
                        walk(item, &format!("{path}[{i}]"), render);
                    }
                }
                _ => {}
            }
        }
        walk(&config, "", render);

        // Every path-shaped setting points INTO a mount, so a "path" that is
        // really an inline value cannot pass the arm above by being renamed.
        for field in ["oidc.clientSecretFile", "sessionKey.file", "cursorKey.file"] {
            let mut node = &config;
            let mut present = true;
            for part in field.split('.') {
                match node.get(part) {
                    Some(next) => node = next,
                    None => {
                        present = false;
                        break;
                    }
                }
            }
            if present {
                let value = node.as_str().unwrap_or("");
                assert!(
                    value.starts_with("/var/run/logweir/"),
                    "{render}.yaml: `{field}` is `{value}`, which is not a path into the \
                     console's read-only Secret mounts under /var/run/logweir/"
                );
            }
        }
        if mode == "localAdmin" {
            let value = config["cursorKeyFile"].as_str().unwrap_or("");
            assert!(
                value.starts_with("/var/run/logweir/"),
                "{render}.yaml: `cursorKeyFile` is `{value}`"
            );
        }

        assert_eq!(
            Some("inCluster"),
            config["kubernetes"]["source"].as_str(),
            "{render}.yaml: the console reads its Kubernetes identity from the projected \
             ServiceAccount token and NEVER from a kubeconfig — which on a developer's laptop is \
             routinely cluster-admin, and would make api-rbac.yaml's whole `auth can-i` argument \
             about an account nothing runs as"
        );
        assert!(
            config.get("kubeconfig").is_none() && !text.contains("kubeconfig"),
            "{render}.yaml: no kubeconfig path reaches the console's configuration"
        );
        assert_eq!(
            Some("/ui"),
            config["uiDirectory"].as_str(),
            "{render}.yaml: /ui is the path Dockerfile.console COPYs the twenty-six shipped files \
             to, and the same path Dockerfile.ui uses"
        );
        assert_eq!(
            Some(mode),
            config["mode"].as_str(),
            "{render}.yaml: the mode is explicit — logweir-api has no default and refuses a file \
             that forgets to name one, rather than reading it as the more permissive mode"
        );

        let listen = config["listen"].as_str().expect("a listen address");
        if mode == "localAdmin" {
            assert!(
                listen.starts_with("127.0.0.1:"),
                "{render}.yaml: localAdmin mode binds loopback — `{listen}`. This is the property \
                 that makes it a safe default: nothing answers at the Pod IP, so turning the \
                 console on cannot put an unauthenticated shared console on a ClusterIP the way \
                 the legacy kubectl-proxy component does."
            );
            assert_eq!(
                Some(format!("http://{listen}").as_str()),
                config["publicOrigin"].as_str(),
                "{render}.yaml: config.rs refuses a publicOrigin whose port is not the listen port"
            );
            assert!(
                config.get("oidc").is_none() && config.get("roles").is_none(),
                "{render}.yaml: localAdmin mode has no identity provider and no role table; \
                 config.rs refuses both fields BY NAME in this mode"
            );
        } else {
            assert_eq!(
                Some("0.0.0.0:8484"),
                Some(listen),
                "{render}.yaml: shared mode binds the Pod IP; TLS terminates at the Ingress"
            );
            assert!(
                config.get("publicOrigin").is_none(),
                "{render}.yaml: shared mode derives the origin from publicBaseUrl, so the origin \
                 checked and the redirect URI registered cannot disagree; config.rs refuses \
                 publicOrigin here by name"
            );
        }
    }
}

/// **TLS is not optional for the shared console, and it is refused in three
/// places rather than documented in one.**
///
/// D0: "TLS is mandatory at the shared ingress. Startup rejects a non-HTTPS
/// `publicBaseUrl` in shared mode." A console that comes up on plain HTTP looks
/// like it is working, which is why the refusal is not left to the operator's
/// reading:
///
///   1. `values.schema.json` TYPES it, so `--set api.console.publicBaseUrl=http://…`
///      is refused before a template runs. The same pattern refuses a path, a
///      query, userinfo and a trailing slash — the OIDC redirect URI is this
///      value plus `/auth/callback`.
///   2. `templates/ui/api-config.yaml` refuses at RENDER time, naming the
///      field: a non-HTTPS base URL, an Ingress with no TLS Secret, and an
///      Ingress in front of `localAdmin` mode at all (D0: that path "must not
///      bind 0.0.0.0, get an Ingress, or be described as shared-console mode").
///   3. `crates/logweir-api/src/config.rs` refuses at STARTUP, exit 2, before
///      any socket exists.
///
/// This row reads the first two out of checked-in bytes; `scripts/check-chart.sh`
/// arm 6 is the half that actually runs `helm template` and reads its status,
/// because GC22 forbids shelling out from a `#[test]`.
///
/// MUTANT: delete the `pattern` from the schema's `publicBaseUrl`, or delete
/// either `fail` from `api-config.yaml` — each fails here naming what is gone.
#[test]
fn chart_lint_the_shared_console_cannot_be_published_without_tls() {
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("the schema parses");
    let url = &schema["properties"]["api"]["properties"]["console"]["properties"]["publicBaseUrl"];
    let pattern = url["pattern"].as_str().expect(
        "api.console.publicBaseUrl must carry a `pattern`: without it the schema accepts \
                 `http://console.example.com` and TLS at the shared entry point is left to prose",
    );
    assert!(
        pattern.contains("https://"),
        "the publicBaseUrl pattern `{pattern}` does not require https://"
    );
    let re = regex_lite_matches(pattern);
    assert!(
        re("https://console.example.com"),
        "pattern {pattern} rejects a valid HTTPS base URL"
    );
    assert!(
        !re("http://console.example.com"),
        "pattern {pattern} ACCEPTS plain HTTP"
    );
    assert!(
        !re("https://console.example.com/"),
        "pattern {pattern} accepts a trailing slash"
    );
    assert!(
        !re("https://user@console.example.com"),
        "pattern {pattern} accepts userinfo"
    );

    let template = read("charts/logweir/templates/ui/api-config.yaml");
    for needle in [
        "api.console.publicBaseUrl is %q. Shared mode requires the EXACT https:// URL",
        "api.console.ingress.tlsSecretName is empty.",
        "api.console.ingress.enabled with api.console.mode=localAdmin is refused.",
    ] {
        assert!(
            template.contains(needle),
            "templates/ui/api-config.yaml must still refuse at render time with `{needle}`"
        );
    }

    // …and the shared render actually carries the TLS block it forced.
    let shared = rendered("console-shared");
    let ingress = find(&shared, "Ingress", "logweir-api");
    let tls = ingress.value["spec"]["tls"]
        .as_sequence()
        .expect("the console Ingress carries a tls block");
    assert_eq!(1, tls.len());
    assert!(
        tls[0]["secretName"].as_str().is_some_and(|s| !s.is_empty()),
        "the console Ingress names a TLS Secret"
    );
    assert_eq!(
        ingress.value["spec"]["rules"][0]["host"].as_str(),
        tls[0]["hosts"][0].as_str(),
        "the certificate and the rule name the same host, or the browser gets a name mismatch"
    );
    assert!(
        !names_of(&rendered("console"), "Ingress").contains("logweir-api"),
        "the default (localAdmin) console render must carry no Ingress at all"
    );
}

/// **`api.console.mode` has no default, and the two modes are two different
/// shapes — one of which is not exposed in the cluster at all.**
///
/// THE MISSING DEFAULT IS THE POINT. `crates/logweir-api/src/config.rs` refuses
/// a configuration file that forgets to name a mode rather than reading it as
/// the more permissive one, and after review this chart does the same: enabling
/// the console without naming a mode is a render-time `fail` naming the field,
/// not a quiet fall-through to either shape.
///
/// The two shapes, as this row holds them:
///
/// * **the in-cluster administrator mode** (`localAdmin`) — a loopback
///   listener, NO Service, no Ingress, no ingress NetworkPolicy rule, the
///   narrowly bound `<release>-api` ServiceAccount, and readiness not gated on
///   OIDC because there is no OIDC. Nothing in the cluster can dial it;
///   `kubectl port-forward deploy/<release>-api` is the documented path, and
///   `create pods/portforward` in that namespace is therefore equivalent to
///   full console administrator authority.
/// * **`shared`** — the only mode that may be exposed through a Service or an
///   Ingress.
///
/// `scripts/check-chart.sh` arm 8 is the half that runs `helm` and reads its
/// status; GC22 forbids shelling out from a `#[test]`, so this half reads the
/// checked-in `fail` and the two renders.
///
/// MUTANT: give `api.console.mode` a default again in `values.yaml`, or delete
/// the `fail` — this row fails naming what is gone.
#[test]
fn chart_lint_the_console_mode_has_no_default_and_only_shared_is_exposed() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let mode = values["api"]["console"]["mode"]
        .as_str()
        .expect("api.console.mode must be present in values.yaml, even though it has no default");
    assert!(
        mode.is_empty(),
        "charts/logweir/values.yaml ships `api.console.mode: {mode}`. There must be NO default: \
         the binary refuses a configuration that forgets to name a mode rather than reading it \
         as the more permissive one, and a chart default would be exactly that fall-through, \
         one layer up. Ship an empty string and let the template refuse it by name."
    );

    let template = read("charts/logweir/templates/ui/api-config.yaml");
    assert!(
        template.contains("api.console.mode is empty, and there is no default."),
        "templates/ui/api-config.yaml must refuse an empty mode at render time, naming the field"
    );
    assert!(
        template.contains("They must name the SAME authority"),
        "templates/ui/api-config.yaml must refuse an ingress.host that is not publicBaseUrl's \
         authority: a mismatched pair renders, lints and installs clean, and then the service \
         answers 421 misdirected_request to every browser that arrives through the Ingress it \
         just published"
    );

    // The schema types the two names AND lets the empty string through, so that
    // a values file with the console OFF still validates; the template is what
    // refuses the empty one when it is ON.
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("the schema parses");
    let modes = schema["properties"]["api"]["properties"]["console"]["properties"]["mode"]["enum"]
        .as_array()
        .expect("api.console.mode carries an enum");
    let modes: Vec<&str> = modes.iter().filter_map(|m| m.as_str()).collect();
    assert_eq!(
        vec!["", "localAdmin", "shared"],
        modes,
        "the schema must accept exactly the empty string and the two mode names"
    );

    // And the Ingress the shared example renders names publicBaseUrl's own
    // authority, which is the positive half of the refusal above.
    let shared = rendered("console-shared");
    let ingress = find(&shared, "Ingress", "logweir-api");
    let host = ingress.value["spec"]["rules"][0]["host"]
        .as_str()
        .expect("the Ingress names a host");
    let (_, config) = console_config("console-shared");
    let base = config["publicBaseUrl"].as_str().expect("a publicBaseUrl");
    assert_eq!(
        format!("https://{host}"),
        base,
        "the Ingress host and publicBaseUrl must name the same authority"
    );
}

/// **The shared console is rendered outside the controller's Job-create
/// authority (D0 stage 5), and never beside the legacy proxy.**
///
/// WHAT THE SCOPED RENDER MUST SAY. In `console-shared`, which sets
/// `controller.watchNamespaces: [team-a, team-b]`:
///
/// * no `ClusterRoleBinding/weirkeeper` — the cluster-wide grant that put every
///   namespace, the key namespace included, inside the controller's authority;
/// * one `RoleBinding/weirkeeper` to the unchanged `weirkeeper` ClusterRole in
///   EXACTLY the watched namespaces, and none in the release namespace;
/// * `ClusterRole/weirkeeper-cluster-scope`, bound cluster-wide, naming the two
///   cluster-scoped trust kinds and nothing else — no Job, Pod, ConfigMap,
///   Secret or namespaced kind;
/// * in the release namespace, `get` on the one policy ConfigMap by name and
///   nothing else;
/// * the controller's `LOGWEIR_WATCH_NAMESPACES` equal to the bound set, so
///   the watch it starts is one the API server allows;
/// * the console's configuration naming the Kubernetes principal it writes as
///   and requiring its trusted proxy.
///
/// And the default render keeps the cluster-wide binding and renders no
/// variable (the install file comparison holds that byte for byte).
///
/// MUTANT: render the `weirkeeper` ClusterRoleBinding unconditionally, bind the
/// release namespace, or widen `weirkeeper-cluster-scope` with a namespaced
/// kind — each fails here naming the object.
#[test]
fn chart_lint_the_shared_console_is_outside_the_controllers_job_authority() {
    const RELEASE_NS: &str = "logweir-system";
    let shared = rendered("console-shared");
    assert!(
        !names_of(&shared, "ClusterRoleBinding").contains("weirkeeper"),
        "console-shared renders ClusterRoleBinding/weirkeeper: the controller would hold Job \
         create in the release namespace, where the console's keys live (D0 stage 5)"
    );
    let bound: BTreeSet<String> = shared
        .iter()
        .filter(|d| {
            d.kind == "RoleBinding"
                && d.value["roleRef"]["kind"] == "ClusterRole"
                && d.value["roleRef"]["name"] == "weirkeeper"
        })
        .map(|d| {
            assert_eq!(
                d.value["subjects"][0]["name"], "weirkeeper",
                "a weirkeeper RoleBinding binds someone else"
            );
            assert_eq!(d.value["subjects"][0]["namespace"], RELEASE_NS);
            d.value["metadata"]["namespace"]
                .as_str()
                .expect("a namespaced RoleBinding")
                .to_string()
        })
        .collect();
    assert_eq!(
        bound,
        BTreeSet::from(["team-a".to_string(), "team-b".to_string()]),
        "the weirkeeper ClusterRole must be bound in exactly the watched namespaces"
    );
    assert!(!bound.contains(RELEASE_NS));

    let cluster_scope = find(&shared, "ClusterRole", "weirkeeper-cluster-scope");
    let resources: BTreeSet<String> = rules_of(cluster_scope)
        .into_iter()
        .flat_map(|(groups, resources, _)| {
            assert_eq!(groups, vec!["logweir.dev".to_string()]);
            resources
        })
        .collect();
    assert_eq!(
        resources,
        BTreeSet::from([
            "trustpolicies".to_string(),
            "trustpolicies/status".to_string(),
            "trustrosters".to_string(),
            "trustrosters/status".to_string(),
        ]),
        "weirkeeper-cluster-scope must hold the two cluster-scoped trust kinds and nothing else"
    );
    let binding = find(&shared, "ClusterRoleBinding", "weirkeeper-cluster-scope");
    assert_eq!(binding.value["roleRef"]["name"], "weirkeeper-cluster-scope");
    assert_eq!(binding.value["subjects"][0]["name"], "weirkeeper");

    let policy_role = find(&shared, "Role", "weirkeeper-installation-policy");
    assert_eq!(policy_role.value["metadata"]["namespace"], RELEASE_NS);
    let rules = policy_role.value["rules"].as_sequence().expect("rules");
    assert_eq!(rules.len(), 1, "one rule in the release namespace");
    assert_eq!(
        serde_yaml::to_string(&rules[0]).unwrap(),
        "apiGroups:\n- ''\nresources:\n- configmaps\nresourceNames:\n- weirkeeper-policy\nverbs:\n- get\n"
    );
    // Nothing else in the render grants the controller anything in the
    // release namespace.
    for doc in shared.iter().filter(|d| {
        d.kind == "RoleBinding"
            && d.value["metadata"]["namespace"] == RELEASE_NS
            && d.value["subjects"]
                .as_sequence()
                .is_some_and(|s| s.iter().any(|x| x["name"] == "weirkeeper"))
    }) {
        assert_eq!(
            doc.name(),
            "weirkeeper-installation-policy",
            "an unexpected grant to weirkeeper in the release namespace"
        );
    }

    let controller = find(&shared, "Deployment", "weirkeeper");
    let env = env_of(container(controller));
    let watched = env
        .get("LOGWEIR_WATCH_NAMESPACES")
        .and_then(|e| e["value"].as_str())
        .expect("the scoped controller carries LOGWEIR_WATCH_NAMESPACES");
    let watched: BTreeSet<String> = watched.split(',').map(str::to_string).collect();
    assert_eq!(
        watched, bound,
        "the watch and the grant must be the same list"
    );

    let (_, config) = console_config("console-shared");
    assert_eq!(
        config["kubernetes"]["principal"],
        "system:serviceaccount:logweir-system:logweir-api"
    );
    assert_eq!(config["requireTrustedProxy"], true);
    // THE SHIPPED RANGE IS A NARROW PLACEHOLDER (review M3): a documentation
    // range, no wider than /24, never the 10.0.0.0/8 that contains every pod
    // of a typical cluster.
    for cidr in config["trustedProxyCidrs"].as_sequence().expect("CIDRs") {
        let cidr = cidr.as_str().expect("a string");
        let prefix: u8 = cidr.rsplit('/').next().unwrap().parse().unwrap();
        assert!(prefix >= 24, "the example trusts {cidr}");
        assert!(
            cidr.starts_with("192.0.2."),
            "{cidr} is not a documentation placeholder"
        );
    }

    // THE DEFAULT RENDER IS UNCHANGED: the cluster-wide binding, no variable.
    let default = rendered("default");
    assert!(names_of(&default, "ClusterRoleBinding").contains("weirkeeper"));
    assert!(!names_of(&default, "ClusterRole").contains("weirkeeper-cluster-scope"));
    assert!(
        !env_of(container(find(&default, "Deployment", "weirkeeper")))
            .contains_key("LOGWEIR_WATCH_NAMESPACES")
    );

    let template = read("charts/logweir/templates/ui/api-config.yaml");
    for needle in [
        "api.console.mode=shared requires controller.watchNamespaces (D0 stage 5).",
        "includes the release namespace %q, which holds the shared console's keys",
        "api.console.mode=shared with ui.enabled=true is refused.",
        "which is not in controller.watchNamespaces",
        "api.console.requireTrustedProxy needs api.console.trustedProxyService",
        "or api.console.trustedProxyCidrs: with nothing to trust",
        "With requireTrustedProxy a range this wide trusts the pods the gate exists to refuse",
    ] {
        assert!(
            template.contains(needle),
            "templates/ui/api-config.yaml must still refuse at render time with `{needle}` \
             (scripts/check-chart.sh arm 8 runs it)"
        );
    }
}

/// A tiny matcher for the ONE anchored alternation shape this schema uses
/// (`^$|^https://[^/?#@]+$`). Written here rather than pulling in a regex crate
/// for a test: a dependency added to assert one pattern is a dependency the
/// whole workspace then carries, and `cargo deny` has to answer for.
fn regex_lite_matches(pattern: &str) -> impl Fn(&str) -> bool + '_ {
    move |candidate: &str| {
        pattern.split('|').any(|branch| {
            let branch = branch
                .strip_prefix('^')
                .and_then(|b| b.strip_suffix('$'))
                .unwrap_or_else(|| panic!("this matcher only handles anchored branches: {branch}"));
            if branch.is_empty() {
                return candidate.is_empty();
            }
            let Some((literal, class)) = branch.split_once("[^") else {
                return candidate == branch;
            };
            let Some((excluded, repeat)) = class.split_once(']') else {
                panic!("unterminated character class in {branch}")
            };
            assert_eq!("+", repeat, "this matcher only handles a trailing `+`");
            let Some(rest) = candidate.strip_prefix(literal) else {
                return false;
            };
            !rest.is_empty() && !rest.chars().any(|c| excluded.contains(c))
        })
    }
}

/// **`retention.enabled` renders the enforcement Job's identity — an account
/// with no token and no role — and nothing else.**
///
/// The name is `weirkeeper::controllers::retention_policy::SERVICE_ACCOUNT`,
/// compiled into every `mode: Enforce` Job. Until it exists the Job's pod is
/// admitted by nobody, which is fail-closed by accident; that is the
/// merge-ordering constraint that controller's own header records against W13.
///
/// **NO Role AND NO RoleBinding, asserted.** The retention worker makes zero
/// Kubernetes API calls: it reads a mounted plan, talks to an object store and
/// exits. The delete capability that makes it the one irreversible component in
/// the product is an object-store credential scoped to the policy's own prefix,
/// and no Kubernetes verb widens or narrows it. A binding that appeared here
/// would be a capability nobody asked for on the one pod that can delete.
///
/// MUTANT: give the account any RoleBinding, or set
/// `automountServiceAccountToken: true` — each fails naming it.
#[test]
fn chart_lint_retention_renders_an_identity_with_no_grant_and_no_token() {
    let docs = rendered("demo");
    let sa = find(&docs, "ServiceAccount", "logweir-retention");
    assert_eq!(
        Some(false),
        sa.value["automountServiceAccountToken"].as_bool(),
        "the pod that holds a delete-capable storage credential is the last pod in the \
         installation that should also carry a cluster token"
    );
    for d in &docs {
        if matches!(d.kind.as_str(), "RoleBinding" | "ClusterRoleBinding") {
            let subjects = d.value["subjects"]
                .as_sequence()
                .cloned()
                .unwrap_or_default();
            for s in subjects {
                assert_ne!(
                    Some("logweir-retention"),
                    s["name"].as_str(),
                    "{}/{} binds `logweir-retention` to `{}`. The retention worker makes zero \
                     Kubernetes API calls; its delete capability is an object-store credential \
                     and no Kubernetes verb reaches it",
                    d.kind,
                    d.name(),
                    d.value["roleRef"]["name"].as_str().unwrap_or_default()
                );
            }
        }
        assert!(
            !(d.kind == "ClusterRole" || d.kind == "Role") || d.name() != "logweir-retention",
            "the chart renders a role named `logweir-retention`; it grants nothing and needs none"
        );
    }
    for name in ["default", "minimal"] {
        for d in rendered(name) {
            assert_ne!(
                "logweir-retention",
                d.name(),
                "rendered/{name}.yaml carries {}/logweir-retention with retention.enabled off",
                d.kind
            );
        }
    }

    // AND IT EXISTS IN EVERY NAMESPACE THAT RUNS A JOB — review finding **F2**.
    //
    // It used to render in the release namespace alone. `RetentionPolicy`s live
    // with the workload they protect, and
    // `controllers::retention_policy` creates the Job with
    // `Api::<Job>::namespaced(client, &self.namespace)` — so an operator who
    // armed enforcement in `team-a` got a Job whose pod was never admitted,
    // with nothing in any status naming the account that was missing. The
    // account now follows `identity.authorizedRunnerNamespaces`, which is the
    // shape `templates/identity.yaml` already uses for `logweir-runner` and is
    // the same list for the same reason: the namespaces this installation runs
    // Logweir Jobs in.
    //
    // DERIVED FROM THE EXAMPLE'S OWN VALUES, like the console binding set
    // above, so a mutant that narrows the range fails here and a mutant that
    // edits the example cannot move the goalposts.
    for name in ["demo", "identity-multinamespace"] {
        let values: Value = serde_yaml::from_str(&read(&format!(
            "charts/logweir/examples/{name}.values.yaml"
        )))
        .expect("the example values parse");
        if values["retention"]["enabled"].as_bool() != Some(true) {
            continue;
        }
        let mut want: BTreeSet<String> = BTreeSet::from(["logweir-system".to_string()]);
        if let Some(extra) = values["identity"]["authorizedRunnerNamespaces"].as_sequence() {
            want.extend(
                extra
                    .iter()
                    .map(|v| v.as_str().expect("a namespace name").to_string()),
            );
        }
        let got: BTreeSet<String> = rendered(name)
            .iter()
            .filter(|d| d.kind == "ServiceAccount" && d.name() == "logweir-retention")
            .map(|d| {
                d.value["metadata"]["namespace"]
                    .as_str()
                    .unwrap_or("<no namespace>")
                    .to_string()
            })
            .collect();
        assert_eq!(
            want, got,
            "rendered/{name}.yaml puts the enforcement account in {got:?}, and the namespaces \
             this installation runs Jobs in are {want:?}. A namespace that runs a \
             `RetentionPolicy` and has no `logweir-retention` is enforcement that fails closed \
             with nothing in any status saying which gate was shut"
        );
    }

    // AND THE LOW-LEVEL PATH HAS A FRAGMENT, so the two documents that point at
    // one are not pointing at nothing. It carries no namespace, exactly as
    // `backup-runner-serviceaccount.yaml` does not, because a hard-coded one
    // would be wrong in every namespace an enforcement Job actually runs in.
    let fragment = read("config/rbac/retention-serviceaccount.yaml");
    for needle in [
        "name: logweir-retention",
        "automountServiceAccountToken: false",
    ] {
        assert!(
            fragment.contains(needle),
            "config/rbac/retention-serviceaccount.yaml must carry `{needle}`"
        );
    }
    assert!(
        !fragment.contains("\n  namespace:"),
        "the fragment names no namespace: it is applied into the namespace the `RetentionPolicy` \
         lives in, and a hard-coded one would be wrong everywhere"
    );
    assert!(
        !read("config/rbac/kustomization.yaml").contains("retention-serviceaccount.yaml\n  -")
            && !read("config/rbac/kustomization.yaml")
                .lines()
                .any(|l| l.trim() == "- retention-serviceaccount.yaml"),
        "the fragment must NOT be listed in config/rbac/kustomization.yaml: rendering it into \
         `logweir.yaml` would create the account in the one namespace no enforcement Job runs in"
    );
    assert!(
        read("docs/install.md").contains("config/rbac/retention-serviceaccount.yaml"),
        "docs/install.md step 4 must name the fragment; before fix round 1 both this template's \
         header and install.md pointed at a step that covered `logweir-runner` only"
    );
}

/// **`controller.failFastSeconds` and `controller.jobTtlSeconds` render the two
/// environment variables the controller reads, and render NOTHING when unset.**
///
/// D3 §2.3 and §2.7. `weirkeeper::diagnostics` reads
/// `LOGWEIR_FAIL_FAST_SECONDS` and `LOGWEIR_JOB_TTL_SECONDS`, clamps each to
/// its floor and falls back to a compiled-in default; before this the two
/// variables were set by nothing, so "configurable" was a property of the
/// controller and of no installation.
///
/// THE EMPTY STRING IS NOT ZERO, and that is the one distinction this test is
/// really about: `failFastSeconds: 0` is `FAIL_FAST_NEVER` — fail-fast
/// disabled — while `""` means the build's own 300 s. A template that rendered
/// `""` as `0` would silently disable fail-fast on every default install.
///
/// MUTANTS: render the variables unconditionally (the default render stops
/// agreeing with `logweir.yaml`); use `if .Values…` instead of
/// `ne (toString …) ""` (a configured `0` renders nothing and the operator's
/// "never" becomes 300 s).
#[test]
fn chart_lint_the_two_controller_windows_render_only_when_configured() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    for key in ["failFastSeconds", "jobTtlSeconds"] {
        assert_eq!(
            Some(""),
            values["controller"][key].as_str(),
            "`controller.{key}` ships as the EMPTY STRING — this build's own default — so a \
             default install renders no environment variable and stays byte-identical to \
             logweir.yaml"
        );
    }
    for name in ["default", "minimal", "demo", "msk"] {
        let docs = rendered(name);
        let env = env_of(container(find(&docs, "Deployment", "weirkeeper")));
        for var in ["LOGWEIR_FAIL_FAST_SECONDS", "LOGWEIR_JOB_TTL_SECONDS"] {
            assert!(
                !env.contains_key(var),
                "rendered/{name}.yaml sets {var} although no example configures it; an unset \
                 value must render nothing at all"
            );
        }
    }
    // AND THE TEMPLATE DISTINGUISHES `""` FROM `0`. Read as text, because a
    // configured zero cannot be observed in a checked-in render that does not
    // configure one, and `0` is the value whose meaning is inverted.
    let template = read("charts/logweir/templates/deployment.yaml");
    for var in ["failFastSeconds", "jobTtlSeconds"] {
        assert!(
            template.contains(&format!("ne (toString .Values.controller.{var}) \"\"")),
            "templates/deployment.yaml must guard `controller.{var}` with \
             `ne (toString …) \"\"` and never with a bare `if`: Helm's `if` is false for the \
             number 0, and `failFastSeconds: 0` is FAIL_FAST_NEVER — the operator asking for \
             fail-fast to be switched OFF. A bare `if` would render nothing and give them 300 \
             seconds of patience instead of infinite"
        );
    }
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
    // D0 stage 7 adds the FOURTH: `logweir-console`, the image the console pod
    // runs. The test's NAME still says three and is left alone on purpose —
    // STANDING RULE 19 renames a test when its assertion changes, and this
    // assertion did not: "every rendered image is a digest except the Logweir
    // ones at :latest". The COUNT moved, and the count lives in this list,
    // which is derived from the tree rather than spelt.
    let logweir_repos = [
        repository_of(&controller_image_pin()),
        repository_of(&runner_image_constant()),
        ui_repository(),
        console_repository(),
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
                    "{name}: {}/{} names a Logweir image as {image}; this chart names all four \
                     by `<repository>:{LOGWEIR_TAG}`",
                    d.kind,
                    d.name()
                );
                continue;
            }
            assert!(
                is_digest_reference(&image),
                "{name}: {}/{} references {image} by tag — only the four Logweir images may",
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
        // Fix round 1, review F3: the fence's subject under a NON-DEFAULT
        // release name, in both directions. Every other render in that script
        // uses `$RELEASE` = `logweir`, which is the one name under which the
        // divergence could not be seen.
        "OTHER_RELEASE=notlogweir",
        "--set \"admissionPolicy.consoleServiceAccountName=$OTHER_RELEASE-api\"",
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
    //
    // RAISED FROM 170 TO 190 BY D3 W13, for FIVE keys and their two section
    // headers, and the same rule decided each one. D3 introduced a great deal
    // of configuration and almost none of it is an INSTALLATION setting: a
    // catalog's sync interval, a protection policy's evaluation window, a
    // rehearsal's budget and a retention policy's mode are per-OBJECT fields
    // with defaults compiled into the CRD schema, so a chart value for any of
    // them would render nothing and mean nothing. What did land here is the
    // five that reach something:
    //
    //   * `controller.failFastSeconds` and `controller.jobTtlSeconds` — two
    //     environment variables `weirkeeper::diagnostics` already read and that
    //     nothing set (D3 §2.3, §2.7);
    //   * `retention.enabled` — the ServiceAccount every enforcement Job names,
    //     without which that path is admitted by nobody;
    //   * `api.enabled` and `api.namespaces` — the console principal's identity
    //     and the namespaces it is bound in.
    //
    // Twenty lines for five keys is the owner's own ratio: one short line per
    // key plus a two-line section comment apiece, everything else in
    // `charts/logweir/README.md`.
    //
    // RAISED FROM 190 TO 225 BY D0 STAGE 7, for the `api.console` block, and
    // the ratio is the same one: thirty-eight lines for thirty-six keys plus a
    // two-line section header. The rule the owner set decides each of them, and
    // it decided what is NOT here just as often:
    //
    //   * EVERY key reaches something an installation must state and the chart
    //     cannot derive — the image and its pull policy, the MODE (the binary
    //     has no default and neither does this), the listen/Service port, the
    //     names of three Secrets, the exact OIDC issuer/client, the exact
    //     HTTPS public URL, the role table, the Ingress host/class/certificate
    //     and the ingress-controller selectors a NetworkPolicy needs. None of
    //     them has a defensible default and none can be discovered.
    //
    //   * The OIDC claim names, the allowed JWS algorithms and the requested
    //     scopes are NOT here, although `logweir-api` reads all three: each has
    //     a working default in `crates/logweir-api/src/config.rs`, so a chart
    //     value would be a second place the same default is written. An
    //     installation that needs another `groupsClaim` needs a change here and
    //     a line in the README; until one does, the key would be prose.
    //
    //   * There is no `service.type` (ClusterIP is the only supported answer —
    //     D0: "No NodePort/LoadBalancer by default"), no `kubernetes.source`
    //     (`inCluster`, always — a kubeconfig would be an identity nobody
    //     audits), and no PodDisruptionBudget key (it renders from `replicas`,
    //     because a PDB over a single replica blocks node drains).
    //
    // Thirty-six keys is what a console with SSO, TLS, per-namespace product
    // roles and a network boundary costs to configure. The alternative was not
    // fewer keys; it was defaults nobody chose.
    //
    // PLAT-19.2 added the `approvalPolicy` block — FOUR keys and one header
    // line, and no shorter spelling exists: the policies, the namespace
    // bindings, D0's `allowOrdinaryConfirmation` floor and the console's
    // confirmation-key Secret are each a decision an administrator must make
    // explicitly, and the README's `approvalPolicy` section carries the prose.
    //
    // PLAT-17.2 (D0 stage 5) added three lines in parallel with PLAT-19.2 and
    // used the slack the ceiling then had: `controller.watchNamespaces` (a key
    // and the one continuation line its two refusals need) and
    // `api.console.requireTrustedProxy`. Each branch fitted its own ceiling;
    // integrated, the file is the sum of both, so the ceiling is too.
    //
    // RAISED FROM 232 TO 240 FOR THE PoC CHART GAPS, SIX KEYS AND NO PROSE:
    // `kubernetes.connectionsNamespace` (G3, where the chart's own connection
    // objects land), `api.console.trustedProxyService` (G6, the ingress
    // controller by its Service), `api.console.hostAliases` (G2, the issuer's
    // name inside the cluster), `api.console.oidc.caBundle` and
    // `api.console.oidc.systemRoots` (G1, a private issuer CA by reference),
    // and `api.console.networkPolicy.oidcPeers` (the in-cluster IdP path an
    // enforcing CNI can match, which an `ipBlock` for a ClusterIP is not).
    // Each is something an installation must state and the chart cannot
    // derive; the README carries every explanation.
    assert!(
        lines <= 240,
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
        // P10 — the manual-run pool, in the same ConfigMap.
        "runs.maxManualBackupsActivePerNamespace",
        "runs.maxManualRestoresActivePerNamespace",
        "engine.allowUnverifiedCustomCa",
        "evidence.controllerIdentityLocations",
        // D2 §7.3 — the console credential admission policy.
        "admissionPolicy.enabled",
        "admissionPolicy.consoleServiceAccountName",
        "admissionPolicy.extraPrincipals",
        // D3 W14 / NOTIFY-INSECURE-SINK-UNEXPOSED — the installation-only hatch.
        "notify.allowInsecureSinks",
        // D3 W13 — the two controller windows `weirkeeper::diagnostics` reads,
        // and the two RBAC-only components.
        "controller.failFastSeconds",
        "controller.jobTtlSeconds",
        "retention.enabled",
        "api.enabled",
        "api.namespaces",
        "api.console.enabled",
        "api.console.image",
        "api.console.imagePullPolicy",
        "api.console.mode",
        "api.console.replicas",
        "api.console.port",
        "api.console.localAdminSubject",
        "api.console.keySecret",
        "api.console.keyVersion",
        "api.console.publicBaseUrl",
        "api.console.sessionMaxAgeSeconds",
        // P10 — the per-person manual-run create ceilings, both modes.
        "api.console.rateLimits.manualBackupsPerMinute",
        "api.console.rateLimits.manualRestoresPerMinute",
        "api.console.trustedProxyCidrs",
        "api.console.trustedProxyService.namespace",
        "api.console.trustedProxyService.name",
        "api.console.hostAliases",
        "api.console.oidc.issuer",
        "api.console.oidc.caBundle.configMap",
        "api.console.oidc.caBundle.secret",
        "api.console.oidc.caBundle.key",
        "api.console.oidc.systemRoots",
        "api.console.networkPolicy.oidcPeers",
        "kubernetes.connectionsNamespace",
        "api.console.oidc.clientId",
        "api.console.oidc.clientSecret",
        "api.console.roles.revision",
        "api.console.roles.bindings",
        "api.console.ingress.enabled",
        "api.console.ingress.className",
        "api.console.ingress.host",
        "api.console.ingress.tlsSecretName",
        "api.console.ingress.annotations",
        "api.console.networkPolicy.enabled",
        "api.console.networkPolicy.ingressNamespace",
        "api.console.networkPolicy.ingressPodLabels",
        "api.console.networkPolicy.oidcCIDRs",
        "api.console.resources",
        "api.console.nodeSelector",
        "api.console.tolerations",
        "api.console.affinity",
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
        // P10: `RunsPolicy::default()`'s and `RunRateLimits::default()`'s
        // numbers — the ones the templates render NOTHING for.
        ("runs.maxManualBackupsActivePerNamespace", 4),
        ("runs.maxManualRestoresActivePerNamespace", 2),
        ("api.console.rateLimits.manualBackupsPerMinute", 10),
        ("api.console.rateLimits.manualRestoresPerMinute", 5),
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

// ------------------------------------------- PLAT-19.2: approval-policy parity

/// The `approvalPolicy` document a values file would render, exactly the three
/// fields `templates/approval-policy.yaml` writes into the ConfigMap.
fn approval_policy_document(values: &Value) -> String {
    let block = values.get("approvalPolicy").cloned().unwrap_or(Value::Null);
    let mut document = serde_yaml::Mapping::new();
    for field in ["allowOrdinaryConfirmation", "policies", "namespaces"] {
        if let Some(v) = block.get(field) {
            document.insert(Value::String(field.to_string()), v.clone());
        }
    }
    serde_yaml::to_string(&Value::Mapping(document)).expect("a document")
}

/// **Every approval-policy document the chart is meant to refuse, the binary
/// refuses** (review M3). A document the binary refuses stops the whole
/// controller at start, so the chart's refusals and the binary's must be the
/// same set. The cases are listed ONCE, in `scripts/approval-policy-refusals/`;
/// `scripts/check-chart.sh` section 9 proves `helm template` refuses each (a
/// `#[test]` may not shell out to helm, Global Constraint 22), and this proves
/// `ApprovalPolicySet::parse` refuses each. Add a rule to either side and its
/// case here, and both gates must refuse it.
#[test]
fn chart_lint_every_approval_policy_refusal_is_the_binarys() {
    let cases = files_under("scripts/approval-policy-refusals");
    let cases: Vec<&String> = cases
        .iter()
        .filter(|f| f.ends_with(".values.yaml"))
        .collect();
    assert!(cases.len() >= 12, "the refusal list shrank: {cases:?}");
    for case in cases {
        let text = read(case);
        assert!(
            text.lines().any(|l| l.starts_with("# expect: ")),
            "{case} names no `# expect:` diagnostic for check-chart.sh"
        );
        let values: Value = serde_yaml::from_str(&text).expect("a values file");
        let document = approval_policy_document(&values);
        assert!(
            logweir_core::approval_policy::ApprovalPolicySet::parse(&document).is_err(),
            "{case}: the chart refuses this document but the binary ACCEPTS it:\n{document}"
        );
    }
}

/// And the other direction: the shipped example the chart RENDERS is a
/// document the binary ACCEPTS, read from the rendered ConfigMap itself.
#[test]
fn chart_lint_the_rendered_approval_policy_is_one_the_binary_accepts() {
    let docs = rendered("approval-policy");
    let map = docs
        .iter()
        .find(|d| {
            d.kind == "ConfigMap"
                && d.value["metadata"]["labels"]["app.kubernetes.io/component"].as_str()
                    == Some("approval-policy")
        })
        .expect("the approval-policy example renders its ConfigMap");
    let document = map.value["data"]["approval-policy.yaml"]
        .as_str()
        .expect("the document");
    let parsed = logweir_core::approval_policy::ApprovalPolicySet::parse(document)
        .unwrap_or_else(|e| panic!("the rendered example is refused by the binary: {e}"));
    assert!(!parsed.is_empty(), "the example configures something");
    let example: Value =
        serde_yaml::from_str(&read("charts/logweir/examples/approval-policy.values.yaml"))
            .expect("the example");
    assert_eq!(
        logweir_core::approval_policy::ApprovalPolicySet::parse(&approval_policy_document(
            &example
        ))
        .expect("the example's document"),
        parsed,
        "the rendered ConfigMap is the example's document"
    );
}

/// **P10 review L2: a DEFAULT render carries neither manual-run block**, so an
/// image-only rollback of a default install reads documents an older binary
/// accepts. An older controller parses `policy.json` with unknown fields
/// rejected and would refuse the WHOLE policy (failing closed: no attestation,
/// no evidence allowlist); an older console refuses a configuration file with
/// an unknown key and does not start. The templates render `runs` and
/// `rateLimits` only when a value differs from the binaries' defaults — this
/// row holds the default half on every checked-in render, the values file to
/// those defaults, and the templates to the same numbers; `check-chart.sh`
/// holds the non-default half (a changed value IS rendered).
///
/// NEGATIVE CONTROL: rendering either block unconditionally fails this row on
/// `default.yaml` and `console.yaml` respectively.
#[test]
fn chart_lint_a_default_render_carries_neither_manual_run_block() {
    for render in [
        "default",
        "minimal",
        "demo",
        "msk",
        "console",
        "console-shared",
        "approval-policy",
        "admission-policy",
        "identity-external",
        "identity-multinamespace",
    ] {
        let policy = policy_json(render);
        assert!(
            policy.get("runs").is_none(),
            "{render}.yaml: a default install's policy.json must not carry `runs`: {policy}"
        );
    }
    for render in ["console", "console-shared"] {
        let (text, _) = console_config(render);
        assert!(
            !text.contains("rateLimits"),
            "{render}.yaml: a default console configuration must not carry `rateLimits`:\n{text}"
        );
    }
    let policy = read("charts/logweir/templates/policy.yaml");
    assert!(
        policy.contains("(ne $runsBackups 4) (ne $runsRestores 2)"),
        "templates/policy.yaml renders `runs` only away from RunsPolicy::default() (4, 2)"
    );
    let config = read("charts/logweir/templates/ui/api-config.yaml");
    assert!(
        config.contains("(ne $rlBackups 10) (ne $rlRestores 5)"),
        "templates/ui/api-config.yaml renders `rateLimits` only away from RunRateLimits::default() (10, 5)"
    );
}
