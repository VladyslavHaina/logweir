//! The chart lint: `charts/logweir` held to the tree it is derived from, by
//! reading checked-in bytes.
//!
//! # What is asserted here, and what is asserted in `scripts/check-chart.sh`
//!
//! Global Constraint 22: every test here parses files under the working tree,
//! shells nothing and dials nothing. The gate SCRIPT runs `helm` (`helm lint`,
//! `helm template` into `charts/logweir/rendered/` with the drift check, and
//! the schema refusals), and these tests read what it rendered: a rendered
//! file that is not current fails the script's porcelain arm.
//!
//! # The properties
//!
//! * The chart is the one install and `crds/` the one copy of the CRDs, held
//!   to the Rust emitter by `just crds-check`. The runner
//!   reference is read from `crates/weirkeeper/src/job.rs` and never spelt
//!   here (`crd_shape.rs::the_runner_image_is_named_once` counts that string
//!   under `crates/`); the other Logweir repositories share its namespace.
//! * `demoKafka`, `minio` and `ui` render NOTHING under the defaults and under
//!   the minimal example, and render their named objects under the demo
//!   example: two StatefulSets and a seed Job whose command names the marker
//!   topic; the MinIO Deployment, its seed Job with both bucket names, and the
//!   `logweir-s3` Secret; the UI Deployment with `--accept-paths` present and
//!   measured, the ConfigMap holding exactly the fourteen UI files' bytes and
//!   no key material, the ServiceAccount, the RoleBinding to the chart's own
//!   role and never `cluster-admin`, the Service — and no Ingress anywhere.
//! * `values.schema.json` types the three flags as booleans.

mod support;
use support::{is_digest_reference, repo};

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_yaml::Value;

fn read(rel: &str) -> String {
    let p = repo().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("could not read {}: {e}", p.display()))
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

/// The REPOSITORY half of a `<repository>@sha256:<64 hex>` reference.
///
/// DERIVED AND NEVER SPELT. The Logweir repositories come from the tree's one
/// digest pin, `weirkeeper::job::RUNNER_IMAGE`, so a namespace change there
/// propagates into the chart without editing this file, and
/// `crd_shape.rs::the_runner_image_is_named_once` keeps finding the runner
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

/// The tag this chart's defaults give the two Logweir images (the owner's
/// decision). `weirkeeper::job::RUNNER_IMAGE` still pins a digest (Global
/// Constraint 7).
const LOGWEIR_TAG: &str = "latest";

/// The twenty-six shipped UI files: everything under `ui/` except `*.md` and
/// `tests/` — `scripts/check-ui-offline.sh`'s scope, by construction.
///
/// The NUMBER is asserted rather than taken from the tree: a count read from
/// the same directory the image was built from stays green when files are
/// deleted from both sides at once.
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
fn the_ui_bearing_image_gate_pins_the_current_shipped_file_count() {
    let expected = shipped_ui_files().len();
    let script = "scripts/check-image-api.sh";
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

/// Every chart, workflow and Dockerfile sentence that counts the shipped page
/// files counts twenty-six.
#[test]
fn no_chart_workflow_or_dockerfile_counts_the_old_page_set() {
    assert_eq!(shipped_ui_files().len(), 26, "update the prose rule below");
    let mut paths: Vec<String> = files_under("charts/logweir")
        .into_iter()
        .filter(|p| !p.starts_with("charts/logweir/rendered/"))
        .collect();
    paths.extend(files_under(".github/workflows"));
    paths.extend(["Dockerfile", "Dockerfile.console", "Dockerfile.weirkeeper"].map(String::from));
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

/// **The controller has a liveness and a readiness probe, in
/// every render, and both are the binary's own
/// `--probe` against its loopback listener.** An `httpGet` probe would need a
/// pod-IP listener; a `tcpSocket` probe would pass while the runtime is wedged
/// (the kernel accepts the connection) — only an answered request proves the
/// runtime turns, which is what `weirkeeper --probe` asks for.
#[test]
fn chart_lint_the_controller_is_probed_by_its_own_binary() {
    let deployments: Vec<(String, Value)> = files_under("charts/logweir/rendered")
        .into_iter()
        .map(|p| {
            let docs = docs_in(&p);
            let doc = find(&docs, "Deployment", "weirkeeper");
            (p, container(doc).clone())
        })
        .collect();
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

/// **The console image's repository, DERIVED** — the namespace of
/// `weirkeeper::job::RUNNER_IMAGE` with the name `logweir-console`, which is
/// what `Dockerfile.console` builds.
///
/// `console` names the file and the image while the crate and the RBAC values
/// block own `api` (`charts/logweir/README.md`). Derived because the console
/// image has no digest pin in the tree, so what must not drift is the
/// NAMESPACE, and taking it from the runner pin moves all three images at once.
fn console_repository() -> String {
    logweir_repository("logweir-console")
}

/// **The controller image's repository, DERIVED** the same way, with the name
/// `weirkeeper`: the controller has no digest pin in the tree.
fn controller_repository() -> String {
    logweir_repository("weirkeeper")
}

/// A Logweir image's repository: the runner pin's namespace and `name`.
fn logweir_repository(name: &str) -> String {
    let runner = repository_of(&runner_image_constant());
    let (namespace, _) = runner
        .rsplit_once('/')
        .unwrap_or_else(|| panic!("RUNNER_IMAGE's repository `{runner}` names no namespace"));
    format!("{namespace}/{name}")
}

/// **The chart carries NO copy of the UI.** The page ships inside the console
/// image, and `scripts/check-image-api.sh` check 1 hashes what that image
/// serves against `ui/`. A copy under the chart would be a second source of the
/// serves against `ui/`. A copy under the chart would be a second source of the
/// page that nothing hashes.
#[test]
fn chart_lint_the_chart_carries_no_ui_copy() {
    let shipped = shipped_ui_files();
    assert_eq!(26, shipped.len());

    let under_chart: Vec<String> = files_under("charts/logweir")
        .into_iter()
        .filter(|p| p.starts_with("charts/logweir/ui/"))
        .collect();
    assert!(
        under_chart.is_empty(),
        "charts/logweir/ui/ is back: {under_chart:?}. The page is delivered by \
         `api.console.image` (Dockerfile.console), and scripts/check-image-api.sh hashes what the \
         image serves against ui/. A copy under the chart is a second source of the page that \
         nothing hashes."
    );
    // And no render carries a page ConfigMap for it.
    for render in ["default", "demo", "msk"] {
        let docs = rendered(render);
        assert!(
            !docs
                .iter()
                .any(|d| d.kind == "ConfigMap" && d.name().contains("-ui")),
            "the {render} render carries a UI ConfigMap"
        );
    }
}

/// **The default render keeps the runner account tokenless and every namespaced
/// object in the release namespace.**
#[test]
fn chart_lint_default_render_keeps_the_runner_tokenless_and_one_namespace() {
    let chart = rendered("default");
    let runner_sa = find(&chart, "ServiceAccount", "logweir-runner");
    assert_eq!(
        Some(false),
        runner_sa.value["automountServiceAccountToken"].as_bool(),
        "the runner ServiceAccount sets automountServiceAccountToken: false"
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
/// The two Logweir values name the repository half of the tree's runner pin
/// and its namespace, followed by `:latest`, DERIVED from
/// `weirkeeper::job::RUNNER_IMAGE` and never spelt here, so a namespace change
/// propagates and a digest written back into either value is a red. The four
/// third-party images (MinIO, mc, apache/kafka, kubectl) are digests that
/// carry the resolution command and the date beside them, the `kindest/node`
/// provenance idiom.
#[test]
fn chart_lint_values_name_the_shipped_repositories_at_latest() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let controller_repo = controller_repository();
    let runner_repo = repository_of(&runner_image_constant());
    assert_eq!(
        Some(format!("{controller_repo}:{LOGWEIR_TAG}")).as_deref(),
        values["controllerImage"].as_str(),
        "values.yaml controllerImage must be `<the runner pin's namespace>/weirkeeper` at \
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
    // the Rust constant, and this ruling does not reach it — `repository_of`
    // above would have panicked otherwise, and this says so out loud.
    assert!(
        is_digest_reference(&runner_image_constant()),
        "weirkeeper::job::RUNNER_IMAGE is still a digest (GC7)"
    );
    // The THREE third-party images, still digests, still with provenance.
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
    // The two digests the chart resolved itself carry the command and the
    // date, in `charts/logweir/README.md` (the values file holds one short
    // line per key): a third-party digest whose provenance nobody wrote down
    // is the defect.
    let text = read("charts/logweir/README.md");
    let command = "docker buildx imagetools inspect apache/kafka:3.7.1";
    assert!(
        text.contains(command),
        "charts/logweir/README.md must record the resolution command `{command}` beside the \
         THIRD-PARTY digest (the kindest/node idiom). It moved here from values.yaml under \
         ruling 15; do not drop it"
    );
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
    for flag in ["minio", "demoKafka", "retention", "api"] {
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
/// MinIO withdrew its server and client images from Docker Hub and its quay.io
/// repositories refuse anonymous pulls, so what the compose stack, the
/// chart's demo MinIO and the live harnesses run is the rebuild
/// `third_party/minio-mirror/` publishes, pinned by digest. A reference to an
/// upstream name is a stack that cannot pull; a harness that pins a DIFFERENT
/// mirror digest measures a different server from the one the chart ships.
/// `third_party/minio-mirror/`, `THIRD_PARTY_NOTICES.md` and the trackers
/// under `docs/to-do/` name the upstream images as provenance and history;
/// nothing else is exempt.
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
        // The provenance record, the generated notices, and the trackers
        // (history, never pulled) may name the upstream images.
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

/// The renders carry no CRDs: `crds/` is checked once, by
/// `just crds-check`.
const SHIPPED_KINDS: [&str; 10] = [
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

/// The chart renders no private bytes, retains both halves of the
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
    // The default render (the bootstrap feature on, no console) passes the
    // hook only the revocation of the install-time trust grant, a no-op where
    // no grant was rendered: no console key, no trust step and no marker
    // without a console
    // (`chart_lint_the_hook_passes_only_flags_its_pinned_image_runs`).
    assert_eq!(
        vec![
            "identity",
            "bootstrap",
            "--namespace",
            "logweir-system",
            "--public-configmap-name",
            "logweir-signing-trust",
            "--revoke-trust-binding",
            "logweir-identity-trust",
        ],
        args
    );
    assert!(
        !docs.iter().any(|d| d.name() == "logweir-identity-trust"),
        "no trust grant without the bootstrap feature and a console (review M2)"
    );
    // With the feature and a console (the fresh-install example), the whole
    // argument list.
    let fresh = rendered("console-fresh-install");
    let fresh_args: Vec<&str> = container(find(&fresh, "Job", "logweir-identity-bootstrap"))
        ["args"]
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
            "logweir-signing-trust",
            "--console-secret-name",
            "logweir-console-confirmation",
            "--console-public-configmap-name",
            "logweir-console-trust",
            "--installation-trust-policy",
            "logweir-installation",
            "--mark-fresh-install-confirm",
            "--revoke-trust-binding",
            "logweir-identity-trust",
        ],
        fresh_args
    );

    // THE TRUST GRANT, least privilege: `list` to see what trust the cluster
    // has and `create` for the one policy (RBAC cannot narrow `create` by
    // name, the residual docs/kubernetes.md §8 states), `get` on the one
    // roster name, `delete` on its OWN binding by name (the hook revokes it
    // after the trust step); no get/update/patch/delete on TrustPolicy, no
    // Secret, no wildcard. Bound to the bootstrap account alone, INSTALL-ONLY
    // AND TRANSIENT: a `post-install` hook (never upgrade or rollback),
    // weighted before the Job that spends it, deleted by Helm when the
    // install's post-install hooks have run, succeeded or failed.
    let trust_role = find(&fresh, "ClusterRole", "logweir-identity-trust");
    let job_weight: i64 = find(&fresh, "Job", "logweir-identity-bootstrap").value["metadata"]
        ["annotations"]["helm.sh/hook-weight"]
        .as_str()
        .and_then(|w| w.parse().ok())
        .expect("the Job's weight");
    for (kind, object) in [
        ("ClusterRole", trust_role),
        (
            "ClusterRoleBinding",
            find(&fresh, "ClusterRoleBinding", "logweir-identity-trust"),
        ),
    ] {
        let annotations = &object.value["metadata"]["annotations"];
        assert_eq!(
            annotations["helm.sh/hook"].as_str(),
            Some("post-install"),
            "{kind}: the trust grant exists for the install alone — never post-upgrade or \
             post-rollback"
        );
        let policy = annotations["helm.sh/hook-delete-policy"]
            .as_str()
            .unwrap_or_default();
        for needed in ["hook-succeeded", "hook-failed", "before-hook-creation"] {
            assert!(
                policy.split(',').any(|p| p == needed),
                "{kind}: delete policy {policy:?} lacks {needed}: no standing grant may remain"
            );
        }
        let weight: i64 = annotations["helm.sh/hook-weight"]
            .as_str()
            .and_then(|w| w.parse().ok())
            .expect("a weight");
        assert!(
            weight < job_weight,
            "{kind}: weight {weight} must precede the Job's {job_weight}"
        );
    }
    let trust_rules = trust_role.value["rules"]
        .as_sequence()
        .expect("trust rules");
    let as_strings = |v: &Value| -> Vec<String> {
        v.as_sequence()
            .map(|s| {
                s.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let flat: Vec<(Vec<String>, Vec<String>, Vec<String>)> = trust_rules
        .iter()
        .map(|r| {
            (
                as_strings(&r["resources"]),
                as_strings(&r["verbs"]),
                as_strings(&r["resourceNames"]),
            )
        })
        .collect();
    assert_eq!(
        flat,
        vec![
            (
                vec!["trustpolicies".to_string()],
                vec!["list".to_string(), "create".to_string()], // engine-token-ok: the Kubernetes RBAC verbs of the hook's TrustPolicy grant, never the denied kafka-backup subcommand
                Vec::new()
            ),
            (
                vec!["trustrosters".to_string()],
                vec!["get".to_string()],
                vec!["default".to_string()]
            ),
            (
                vec!["clusterrolebindings".to_string()],
                vec!["delete".to_string()],
                vec!["logweir-identity-trust".to_string()]
            ),
        ],
        "the identity hook's cluster grant is exactly list+create TrustPolicy, get the roster \
         and delete its own binding"
    );
    // THE GUARD that keeps it off an upgrade and off an existing identity is
    // the template's (`helm template` cannot render a lookup): check-chart.sh
    // renders `--is-upgrade` and requires the grant and its arguments absent.
    let template = read("charts/logweir/templates/identity.yaml");
    assert!(
        template.contains(
            "{{- $freshTrust := and $trust.enabled .Release.IsInstall (not $existingSecret) \
             (not $externalName) (include \"logweir.freshInstallConfirm\" .) }}"
        ),
        "the trust grant is rendered only on a first install into a namespace with no identity, \
         with the managed console key and no external identity (review M2)"
    );
    // THE HELM FLOOR: refused, by name, where Helm would keep the
    // grant after a failed hook. check-chart.sh runs the helper with fixed
    // versions and, given a pre-3.19 Helm, the refusal itself.
    assert!(
        template.contains(
            "{{- if and $freshTrust (not (include \"logweir.helmCleansFailedHooks\" \
             .Capabilities.HelmVersion.Version)) }}\n{{- fail (printf \"identity.installationTrust \
             needs Helm 3.19.0 or newer"
        ),
        "the trust grant renders only under a Helm that deletes it when the hook fails"
    );
    assert!(
        read("charts/logweir/templates/approval-policy.yaml")
            .contains("{{- if semverCompare \">=3.19.0-0\" . -}}true{{- end -}}"),
        "the floor is 3.19.0 (the first Helm whose execHook deletes earlier hook-succeeded hooks)"
    );
    let gate = read("scripts/check-chart.sh");
    assert!(gate.contains("the trust grant's Helm floor"));
    assert!(template.contains("{{- if $freshTrust }}\n# PROD-16.1: THE FRESH INSTALL'S TRUST"));
    assert!(gate.contains("the install-only trust grant is absent from an upgrade render"));
    let trust_binding = find(&fresh, "ClusterRoleBinding", "logweir-identity-trust");
    assert_eq!(
        trust_binding.value["subjects"],
        serde_yaml::from_str::<Value>(
            "- kind: ServiceAccount\n  name: logweir-identity-bootstrap\n  namespace: logweir-system\n"
        )
        .expect("subjects"),
        "bound to the hook's account alone"
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

// The console/API principal.

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
/// `<release>-ui` acts for WHOEVER REACHES ITS SERVICE, so its role is as
/// small as the page. `<release>-api` acts for a SERVICE that authorizes every
/// request itself (`crates/logweir-api/src/authz.rs`), so its role is the
/// union of what every route may need, and the two are never compared.
///
/// **Every call has a grant**: every kind in the adapter's three seals (read
/// out of `kube.rs`) is readable through this role, and `CancellableCheck`'s
/// two kinds are patchable, or a new console route 403s in production.
///
/// **Every grant has a caller**: no resource outside the seals plus the two
/// core objects, no verb outside the adapter's four, and every pair the seals
/// do not explain is listed in [`GRANTED_AHEAD_OF_ITS_CALLER`] with its
/// reason. That list is SELF-LIQUIDATING: an entry whose kind has since
/// reached the seal fails here, naming it.
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
            // All eight: `approvals` is created by the console for an
            // ordinary confirmation, a governed confirmation object and an
            // approver's countersigned submission.
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
    // THE ONE ROSTER READ: `get` on
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
    // them, each with the wave that lands the caller. SELF-LIQUIDATING,
    // asserted below: when the kind reaches the seal the entry must go, or
    // this test fails naming it.
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
                     though this service could write one",
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
    // A console binding in a namespace no value names is a console read the
    // installation never granted: the authorizer would refuse it, but the
    // binding shape exists so that a defect in the authorizer cannot reach
    // beyond the configured set. SO THE EXPECTATION IS DERIVED, NOT RESTATED:
    // the set is computed from the example's own values the way
    // `logweir.api.namespaces` computes it (the release namespace, plus
    // `api.namespaces`, or `kubernetes.namespace` when that list is empty), so
    // a helper that widens it fails here and a widened EXAMPLE does not move
    // the goalposts.
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

    // AND THE FENCE'S SUBJECT CANNOT DIVERGE FROM THE ACCOUNT. Spelled
    // separately, `{{ .Release.Name }}-api` and the fixed
    // `admissionPolicy.consoleServiceAccountName` agree only under the default
    // release name; anywhere else the policy matches no request and the
    // console's `create` on Secrets has no bound (RBAC cannot narrow a Secret
    // by SHAPE). Asserted as text (Global Constraint 22: no shelling out);
    // `scripts/check-chart.sh` renders under a non-default release name, and
    // `chart_lint_the_gate_script_carries_every_arm` holds the script to
    // those arms. This row holds ONE definition, called by both files.
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
// WHAT AN ACCOUNT CAN REACH, DERIVED FROM THE BINDINGS.
//
// A row that FINDS a role or a binding BY NAME and reads its rules or its
// `subjects[0]` misses two things: a second subject on a known binding
// (`Group system:authenticated`), and a NEW ClusterRole under a name no row
// knows, bound to the account by a new ClusterRoleBinding. So the rows below
// start from the BINDINGS: for every rendered variant, every RoleBinding and
// ClusterRoleBinding whose subjects REACH an account (the account itself, its
// `system:serviceaccount:` user name, or a group it is a member of) must name
// that account and nobody else, and the union of every rule so reached, per
// scope, must equal a pinned allowlist.

/// The namespace every checked-in render is made with (`check-chart.sh`'s
/// `-n logweir-system`).
const RENDER_NAMESPACE: &str = "logweir-system";

/// The console account's grants in EACH namespace it is bound in, as
/// `verb group/resource[@name]` (core is `core`). The sealed adapter's pairs:
/// `chart_lint_the_console_principal_holds_exactly_what_the_sealed_adapter_spends`
/// derives them from `kube.rs`; this list pins what is REACHABLE.
const API_NAMESPACED_GRANTS: [&str; 37] = [
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
/// binding that reaches the account and names anyone else beside it.
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

/// `logweir.freshInstallConfirm`, recomputed from an example's values: the
/// managed identity, the bootstrap image's fresh-install feature, a console over
/// the API, and the managed console key (no other key Secret named) — the one
/// install the identity hook ever marks, and so the one whose readers read
/// the marker.
fn fresh_install_confirm(values: &Value) -> bool {
    let named = values["approvalPolicy"]["confirmationKeySecret"]
        .as_str()
        .unwrap_or_default();
    values["identity"]["enabled"].as_bool() != Some(false)
        && values["identity"]["bootstrapFeatures"]["consoleKey"].as_bool() != Some(false)
        && values["api"]["enabled"].as_bool() == Some(true)
        && values["api"]["console"]["enabled"].as_bool() == Some(true)
        && (named.is_empty() || named == "logweir-console-confirmation")
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

/// The console account and the controller account each hold, in every
/// rendered variant, EXACTLY a pinned set of grants per scope, computed from
/// every binding that reaches them.
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
    let controller_role = rendered("default");
    let controller_grants = grant_atoms(find(&controller_role, "ClusterRole", "weirkeeper"));
    assert!(
        controller_grants.len() >= 30,
        "the default render's weirkeeper role flattened to {} grants",
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
            // A console over the managed identity holds ONE more
            // atom, in the release namespace: `get` on the public identity
            // ConfigMap by name, which `approval::effective_policies` spends
            // (`KubeAdapter::get_installation_identity`) to read the
            // fresh-install marker.
            if fresh_install_confirm(&values) {
                want.entry(RENDER_NAMESPACE.to_string())
                    .or_default()
                    .insert("get core/configmaps@logweir-signing-trust".to_string());
            }
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

        // THE CONTROLLER ACCOUNT. Its role is the default render's,
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
            // With a console over the managed identity, also the
            // public identity ConfigMap the marker poller reads.
            let marker = fresh_install_confirm(&values);
            want.insert(
                RENDER_NAMESPACE.to_string(),
                if marker {
                    pinned(&[
                        "get core/configmaps@weirkeeper-policy",
                        "get core/configmaps@logweir-signing-trust",
                    ])
                } else {
                    pinned(&["get core/configmaps@weirkeeper-policy"])
                },
            );
        }
        let reached = reachable_grants(render, &docs, RENDER_NAMESPACE, "weirkeeper");
        assert_grants(
            render,
            "weirkeeper",
            &want,
            &reached,
            "The controller holds the weirkeeper role — cluster-wide, or \
             per watched namespace when `controller.watchNamespaces` is set — and nothing else",
        );
        // EVERY JOB-OWNING RECONCILER READS ITS JOBS' `FailedCreate`
        // EVENTS, so wherever the controller may create a Job it must also be
        // able to list events — in both binding modes. A scope that could
        // create a Job and not explain why its pod never started would put a
        // refused pod back on its Job's deadline, silently: the read is best
        // effort and a 403 reads as "no event".
        for (scope, atoms) in &reached {
            if atoms.contains("create batch/jobs") {
                assert!(
                    atoms.contains("list core/events"),
                    "rendered/{render}.yaml: the controller may create Jobs in scope `{scope}` \
                     but cannot list events there, so a pod refused at creation there is \
                     reported only at its Job's deadline (FX-11)"
                );
            }
        }
    }
    assert!(
        api_variants >= 4,
        "only {api_variants} rendered variants enable the console; the API half has gone quiet"
    );
}

// The console WORKLOAD: the three rows below are about the POD that uses the
// grant above.

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
/// So this helper reads it the way the cluster does: a plain integer stays as
/// it is, and a leading-zero string is parsed base 8.
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
/// `readOnlyRootFilesystem` and `runAsNonRoot` hold of the running process
/// because the image declares `USER 65532:65532` (`scripts/check-image-api.sh`
/// check 4) and the kubelet refuses a `USER root` image under `runAsNonRoot:
/// true`.
///
/// THE PROBES ARE ASSERTED IN BOTH DIRECTIONS. `shared` mode binds the Pod IP
/// and gets `/healthz` and `/readyz`. `localAdmin` mode binds loopback
/// (`crates/logweir-api/src/config.rs` refuses anything else) and gets
/// NEITHER, because the kubelet probes the POD IP: a readiness probe would
/// hold a working console NotReady and a liveness probe would restart it
/// forever.
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

        // THE KEY FILES MUST BE READABLE BY THE UID THAT READS THEM: the
        // kubelet writes a Secret volume owned by ROOT, so `defaultMode: 0400`
        // is "readable by root alone" and the container, running as 65532,
        // cannot read its keys. A file mode is only wrong relative to a UID,
        // so this reads the two together.
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
            // The ONE exception is the MANAGED console key — the
            // identity hook fills its Secret AFTER this pod starts (a
            // post-install hook), so it is optional and `logweir-api` reads
            // the key on first use, refusing a confirmation until then. It is
            // never a key the console invented.
            let managed_console_key = name == "confirmation"
                && volume["secret"]["secretName"].as_str() == Some("logweir-console-confirmation");
            assert_eq!(
                Some(!managed_console_key),
                volume["secret"]
                    .get("optional")
                    .map_or(Some(true), |o| Some(o.as_bool() != Some(true))),
                "{render}.yaml: secret volume `{name}` must not be optional — a missing key \
                 Secret should hold the pod in ContainerCreating with an event naming it, not \
                 start a console that invented its own key (the managed console key alone is \
                 optional, and must be)"
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
            "{render}.yaml: the console pod runs the logweir-console image"
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

    // THE Service IS `shared` MODE'S ALONE. A loopback listener behind a
    // Service has populated Endpoints (no readiness probe) and refuses every
    // connection: an outage to every monitor and an object any pod can dial.
    // `kubectl port-forward` takes a Deployment directly, so `localAdmin`
    // needs no Service.
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
             to"
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
/// the more permissive one, and this chart does the same: enabling the console
/// without naming a mode is a render-time `fail` naming the field.
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
/// variable.
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
    // The public identity ConfigMap the fresh-install marker is
    // read from joins this rule only where the hook can write a marker
    // (`logweir.freshInstallConfirm`: the bootstrap feature, which this shared
    // example leaves off until the re-pin; `chart_lint_every_grant_reaching_
    // the_api_and_controller_accounts_is_pinned` holds both shapes).
    assert_eq!(
        serde_yaml::to_string(&rules[0]).unwrap(),
        if fresh_install_confirm(&variant_values("console-shared")) {
            "apiGroups:\n- ''\nresources:\n- configmaps\nresourceNames:\n- weirkeeper-policy\n- logweir-signing-trust\nverbs:\n- get\n"
        } else {
            "apiGroups:\n- ''\nresources:\n- configmaps\nresourceNames:\n- weirkeeper-policy\nverbs:\n- get\n"
        }
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
    // THE SHIPPED RANGE IS A NARROW PLACEHOLDER: a documentation
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
/// compiled into every `mode: Enforce` Job; without it the Job's pod is
/// admitted by nobody.
///
/// **NO Role AND NO RoleBinding, asserted.** The retention worker makes zero
/// Kubernetes API calls: it reads a mounted plan, talks to an object store and
/// exits. The delete capability that makes it the one irreversible component in
/// the product is an object-store credential scoped to the policy's own prefix,
/// and no Kubernetes verb widens or narrows it. A binding that appeared here
/// would be a capability nobody asked for on the one pod that can delete.
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

    // AND IT EXISTS IN EVERY NAMESPACE THAT RUNS A JOB. `RetentionPolicy`s
    // live with the workload they protect, and `controllers::retention_policy`
    // creates the Job in the policy's namespace, so the account follows
    // `identity.authorizedRunnerNamespaces` (the shape `templates/identity.yaml`
    // uses for `logweir-runner`, for the same reason). DERIVED FROM THE
    // EXAMPLE'S OWN VALUES, like the console binding set above.
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
    let fragment = read("config/samples/retention-serviceaccount.yaml");
    for needle in [
        "name: logweir-retention",
        "automountServiceAccountToken: false",
    ] {
        assert!(
            fragment.contains(needle),
            "config/samples/retention-serviceaccount.yaml must carry `{needle}`"
        );
    }
    assert!(
        !fragment.contains("\n  namespace:"),
        "the fragment names no namespace: it is applied into the namespace the `RetentionPolicy` \
         lives in, and a hard-coded one would be wrong everywhere"
    );
    assert!(
        read("docs/install.md").contains("config/samples/retention-serviceaccount.yaml"),
        "docs/install.md step 4 must name the fragment; before fix round 1 both this template's \
         header and install.md pointed at a step that covered `logweir-runner` only"
    );
}

/// **`controller.failFastSeconds` and `controller.jobTtlSeconds` render the two
/// environment variables the controller reads, and render NOTHING when unset.**
///
/// D3 §2.3 and §2.7. `weirkeeper::diagnostics` reads
/// `LOGWEIR_FAIL_FAST_SECONDS` and `LOGWEIR_JOB_TTL_SECONDS`, clamps each to
/// its floor and falls back to a compiled-in default.
///
/// THE EMPTY STRING IS NOT ZERO, and that is the one distinction this test is
/// really about: `failFastSeconds: 0` is `FAIL_FAST_NEVER` — fail-fast
/// disabled — while `""` means the build's own 300 s. A template that rendered
/// `""` as `0` would silently disable fail-fast on every default install.
#[test]
fn chart_lint_the_two_controller_windows_render_only_when_configured() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    for key in ["failFastSeconds", "jobTtlSeconds"] {
        assert_eq!(
            Some(""),
            values["controller"][key].as_str(),
            "`controller.{key}` ships as the EMPTY STRING — this build's own default — so a \
             default install renders no environment variable"
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
/// except the console and the short-lived identity bootstrap, which need it.**
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
            "logweir-api" => assert_eq!(
                Some(true),
                automount,
                "the console composes its own Kubernetes calls and needs its token"
            ),
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
/// tag by the owner's decision, checked EXACTLY: a digest there is the ruling
/// reverted and any other tag is a value nobody chose. The author-only render
/// is exempt by NAME because its premise is a locally built tag under
/// `imagePullPolicy: Never`, and a locally built digest changes on every build.
///
/// The third-party images (MinIO, mc, apache/kafka, kubectl) are digests in
/// every render.
#[test]
fn chart_lint_every_rendered_image_is_a_digest_except_the_three_logweir_images_and_the_author_only_example(
) {
    // The Logweir repositories, derived from the tree rather than spelt; the
    // count lives in this list.
    let logweir_repos = [
        controller_repository(),
        repository_of(&runner_image_constant()),
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
    for flag in ["minio", "demoKafka", "identity"] {
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
    // required: an install that set the runner image and forgot its policy
    // would be an install whose Jobs cannot start.
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

// The installation policy (D2 §4.4).

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
        ("discovery.hardMaxTopics", "checks.discovery.hardMaxTopics"),
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
    // THE TWO WITHDRAWN FIELDS ARE NOT THE VALUES FILE'S. values.yaml does not
    // carry them; the document does, at the fixed values an older controller
    // requires (it refuses a document without them), and
    // `weirkeeper/tests/chart_policy.rs` pins both to the parser.
    for (json_path, yaml_path, compat) in [
        (
            "discovery.defaultMaxTopics",
            "checks.discovery.defaultMaxTopics",
            20_000u64,
        ),
        (
            "preflight.defaultTimeoutSeconds",
            "checks.preflight.defaultTimeoutSeconds",
            120,
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
        assert!(
            y.is_null(),
            "values.yaml carries the withdrawn `{yaml_path}`"
        );
        assert_eq!(
            j.as_u64(),
            Some(compat),
            "policy.json's `{json_path}` is the fixed compatibility value"
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
/// WHY THIS PAIRING IS THE WHOLE POINT. A block that said something the
/// Deployment does not is worse than no block at all: a reader would take it
/// for the installation's addressing when it was never configured.
///
/// An install with no endpoint (plain Amazon S3, the DEFAULT render) must not
/// publish `allowHttp: true`, a plaintext-HTTP fact nobody configured:
/// transport security is never derived.
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

// The console admission policy (D2 §7.3).

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
    // IT IS NOT IN THE INSTALL, and it cannot be: a ValidatingAdmissionPolicy
    // applied to a 1.29 cluster is rejected with `no matches for kind`.
    assert!(
        !std::path::Path::new(&repo().join("config/samples/kustomization.yaml")).exists(),
        "config/samples/ has no kustomization of its own either"
    );
}

/// **`scripts/check-chart.sh` carries every arm the ruling names, and refuses
/// without helm by naming the version.**
#[test]
fn chart_lint_the_gate_script_carries_every_arm() {
    let script = read("scripts/check-chart.sh");
    for needle in [
        // Arm 2 asserts the chart's copy of `ui/` has not come back, and names
        // the gate that hashes the page the console image serves.
        "scripts/check-image-api.sh",
        "if [ -e \"$CHART/ui\" ]; then",
        "helm lint \"$CHART\"",
        "helm template \"$RELEASE\" \"$CHART\" -n \"$NAMESPACE\" -f \"$file\"",
        "diff -u \"$RENDERED/$name.yaml\" \"$tmp/expected-$name.yaml\"",
        "@sha256:",
        "DIGEST_EXEMPT=\"author-only.yaml\"",
        "--set \"$flag=yes\"",
        "HELM_MIN_MAJOR=4",
        "crates/weirkeeper/src/job.rs",
        // The tag is a named constant, the two repositories are
        // DERIVED from the tree, and arm 5 reads them.
        "LOGWEIR_TAG=\"latest\"",
        "controller_repo=\"${runner_repo%/*}/$CONTROLLER_IMAGE_NAME\"",
        "runner_repo=\"${tree_runner%@sha256:*}\"",
        "$controller_repo:$LOGWEIR_TAG",
        "$runner_repo:$LOGWEIR_TAG",
        // The third repository, its namespace DERIVED from the runner pin and
        // only its name spelt.
        "CONSOLE_IMAGE_NAME=\"logweir-console\"",
        "console_repo=\"${runner_repo%/*}/$CONSOLE_IMAGE_NAME\"",
        "$console_repo:$LOGWEIR_TAG",
        // The withdrawn `ui.*` block is refused by name, with
        // the same render without it as the control.
        "console_refuses \"the withdrawn ui.* block (ui.enabled=true)\" \"ui.* is withdrawn\"",
        "console_refuses \"the withdrawn ui.* block (ui.enabled=false)\" \"use the console, api.console.*\"",
        "control: the same render with no ui block renders",
        // The installation policy's own schema refusals: a policy document
        // `weirkeeper::check::policy` refuses fails CLOSED and SILENTLY, so the
        // schema refuses the same values at install time. And the fence's
        // subject under a NON-DEFAULT release name, in both directions.
        "OTHER_RELEASE=notlogweir",
        "--set \"admissionPolicy.consoleServiceAccountName=$OTHER_RELEASE-api\"",
        "--set 'checks.discovery.keepPerConnection=0'",
        "--set-string 'checks.discovery.visibilityAttestations[0].id=att-partial'",
        // The three bounds `Policy::validate` enforces, and the two spellings
        // of "the admission policy's subject is missing": `--set` for a real
        // null and `--set-string` for the empty string.
        "'checks.discovery.hardMaxTopics=100000'",
        "'checks.maxActiveTotal=2'",
        // The withdrawn values are accepted, ignored and named in the
        // notes; and every chart value, changed alone, changes the render.
        "'checks.discovery.defaultMaxTopics=60000'",
        "'checks.preflight.defaultTimeoutSeconds=900'",
        "cmp -s \"$tmp/withdrawn-base.yaml\" \"$tmp/withdrawn-set.yaml\"",
        "KUBECONFIG=/dev/null helm install \"$RELEASE\" \"$CHART\" -n \"$NAMESPACE\" --dry-run=client",
        "WITHDRAWN VALUES ARE SET AND IGNORED",
        "bash scripts/check-chart-values.sh",
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
    // The runner Jobs' policy is its own value, and this example
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
        "api:\n  enabled: true\n  console:\n    enabled: true\n    mode: localAdmin",
    ] {
        assert!(demo.contains(line), "demo.values.yaml turns on `{line}`");
    }
    let readme = read("charts/logweir/README.md");
    for needle in [
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
}

// A chart a stranger can use. Everything below reads CHECKED-IN BYTES (the
// rendered files `scripts/check-chart.sh` regenerates, and the templates); a
// refusal only `helm` can show is asserted as the `fail` the template carries,
// and the SCRIPT gate proves `helm` acts on it.

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

/// **The identity bootstrap default is a runner digest**: a clean
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

/// **`values.yaml` is SHORT and shows every knob** (the owner's ruling): a
/// reader opens it, sees every option with its default, and knows
/// what to set in under a minute. A knob a reader cannot see does not exist, so
/// the empty defaults are listed too.
#[test]
fn chart_lint_values_yaml_is_short_and_shows_every_option() {
    let text = read("charts/logweir/values.yaml");
    let lines = text.lines().count();
    // The budget is ONE SHORT LINE PER KEY plus a short section header, every
    // explanation in `charts/logweir/README.md`; it moves only when the number
    // of OPTIONS moves. A key belongs here only when an installation must
    // state it and the chart cannot derive it: per-object settings with CRD
    // defaults, and settings `logweir-api` already defaults (OIDC claim names,
    // JWS algorithms, scopes), stay out. No `service.type` (ClusterIP only),
    // no `kubernetes.source` (`inCluster`, always), and no PDB key (it renders
    // from `replicas`).
    assert!(
        lines <= 248,
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
        // D2 §4.4 / §10 — the installation policy, rendered by
        // `templates/policy.yaml` into the `weirkeeper-policy` ConfigMap.
        "checks.maxActivePerNamespace",
        "checks.maxActiveTotal",
        "checks.maxActiveDiscoveriesPerConnection",
        "checks.maxEvidenceFetchActivePerNamespace",
        "checks.discovery.freshSeconds",
        "checks.discovery.retentionSeconds",
        "checks.discovery.keepPerConnection",
        "checks.discovery.hardMaxTopics",
        "checks.discovery.visibilityAttestations",
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
        // The two controller windows `weirkeeper::diagnostics` reads,
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
    // PROD-14.1: `identity.renderedOffline` is shown UNSET, its default: Helm's
    // `lookup` decides, and a render that cannot ask the cluster is refused
    // (scripts/check-chart.sh arm 12). A `true` or `false` here would make
    // every offline render of the published chart pass silently.
    assert!(
        values["identity"]
            .as_mapping()
            .is_some_and(|identity| identity.get("renderedOffline").is_some())
            && values["identity"]["renderedOffline"].is_null(),
        "values.yaml must carry `identity.renderedOffline: null`"
    );
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
        ("checks.discovery.hardMaxTopics", 50_000),
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
    // TWO VALUES ARE WITHDRAWN. They changed nothing (the CRDs default
    // the request fields at admission), so values.yaml does not show them —
    // and the schema still ACCEPTS them, optional, so a values file or a
    // `--reuse-values` upgrade that carries one does not fail on the closed
    // schema. scripts/check-chart.sh proves the render ignores them and the
    // install notes name them.
    let schema_checks: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json")).expect("the schema");
    for (block, key) in [
        ("discovery", "defaultMaxTopics"),
        ("preflight", "defaultTimeoutSeconds"),
    ] {
        assert!(
            values["checks"][block][key].is_null(),
            "values.yaml shows the withdrawn `checks.{block}.{key}`"
        );
        let node = &schema_checks["properties"]["checks"]["properties"][block];
        assert!(
            node["properties"][key].is_object(),
            "values.schema.json must still accept `checks.{block}.{key}`: its block is \
             additionalProperties:false, so dropping it fails every upgrade that carries it"
        );
        assert!(
            !node["required"]
                .as_array()
                .is_some_and(|r| r.iter().any(|f| f == key)),
            "`checks.{block}.{key}` must not be required"
        );
    }

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
    ] {
        assert_eq!(
            Some(0),
            values[owner][key].as_sequence().map(Vec::len),
            "{owner}.{key} defaults to an empty list"
        );
    }
    // The schema accepts exactly this set — `additionalProperties: false` means
    // a key in values.yaml that the schema forgot refuses every install. A
    // WITHDRAWN key is the one exception: the schema accepts it only so the
    // render can refuse it by name, and values.yaml no longer shows it.
    const WITHDRAWN: [&str; 1] = ["notify"];
    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("the schema parses");
    let typed: BTreeSet<String> = schema["properties"]
        .as_object()
        .expect("a properties object")
        .keys()
        .filter(|k| !WITHDRAWN.contains(&k.as_str()))
        .cloned()
        .collect();
    let mut written: BTreeSet<String> = values
        .as_mapping()
        .expect("a mapping")
        .keys()
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect();
    // A WITHDRAWN key is typed and not shown: the schema accepts it only so a
    // template can refuse it BY NAME (`ui`, refused by
    // templates/ui/ui.yaml pointing at the console).
    written.insert("ui".to_string());
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
/// MinIO and its seed Job (from the MSK example) and a demo broker and
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
    assert_eq!(8, checked, "eight pod specs were checked, not {checked}");

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
    for account in ["weirkeeper", "logweir-runner", "logweir-identity-bootstrap"] {
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
/// `crds/` verbatim and never templates it, which `just crds-check` holds to
/// the Rust emitter.
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
    // The MSK facts an adopter on MSK needs and a short values file cannot
    // hold. Written down, never measured on an MSK cluster (the README says
    // so): this pins that the facts stay on the page.
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
        "api.console.image",
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

/// **PROD-14.1: the version and architecture constraints show before
/// installation**, and they are the chart's own: the Kubernetes floor read
/// from `Chart.yaml`'s `kubeVersion` (which Helm enforces on install, upgrade
/// and template), the Helm floor read from `logweir.helmCleansFailedHooks`
/// (which the chart enforces by name), and both node architectures, on the
/// three pages an operator reads first: `helm show chart` (the description),
/// the chart README's first screen and install.md's first section.
///
/// CONTROL: the numbers are derived, so moving `kubeVersion` to
/// `>=1.30.0-0` (or the helper to 3.20) without the pages fails here; so does
/// a bootstrap pin whose fixture no longer names both platforms.
#[test]
fn chart_lint_constraints_show_before_install() {
    let chart = read("charts/logweir/Chart.yaml");
    let kube = chart
        .lines()
        .find_map(|l| l.strip_prefix("kubeVersion: "))
        .expect("Chart.yaml carries kubeVersion")
        .trim_matches('"')
        .strip_prefix(">=")
        .expect("kubeVersion is a floor")
        .trim_end_matches(".0-0")
        .to_string();
    let helper = read("charts/logweir/templates/approval-policy.yaml");
    let helm = helper
        .split("semverCompare \">=")
        .nth(1)
        .and_then(|rest| rest.split(".0-0\"").next())
        .expect("logweir.helmCleansFailedHooks names the Helm floor")
        .to_string();
    let flat = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
    let description = flat(
        chart
            .split("description: >-")
            .nth(1)
            .and_then(|rest| rest.split("\ntype:").next())
            .expect("Chart.yaml's description"),
    );
    let readme = read("charts/logweir/README.md");
    let first_screen = flat(
        readme
            .split("## Copy this directory and install it")
            .next()
            .expect("the README's first screen"),
    );
    let install = read("docs/install.md");
    let first_section = flat(
        install
            .split("\n---")
            .next()
            .expect("install.md's first section"),
    );
    for (page, text, needles) in [
        (
            "Chart.yaml's description",
            &description,
            vec![
                format!("Kubernetes {kube}+"),
                format!("Helm {helm}+"),
                "linux/amd64 or linux/arm64".to_string(),
            ],
        ),
        (
            "charts/logweir/README.md's first screen",
            &first_screen,
            vec![
                format!("Kubernetes **{kube}** or newer"),
                format!("Helm **{helm}** or newer"),
                "**linux/amd64 or linux/arm64**".to_string(),
                "support-matrix.md#brokers".to_string(),
            ],
        ),
        (
            "docs/install.md's first section",
            &first_section,
            vec![
                format!("**Minimum Kubernetes: {kube}.**"),
                format!("**Helm {helm} or newer, or 4.x.**"),
                "**Nodes: linux/amd64 or linux/arm64.**".to_string(),
                "support-matrix.md#brokers".to_string(),
            ],
        ),
    ] {
        for needle in needles {
            assert!(
                text.contains(&needle),
                "{page} must state `{needle}` before installation (PROD-14.1)"
            );
        }
    }
    // The identity hook is one of those images: its pin is an index of both.
    let help = read("crates/logweir/tests/fixtures/bootstrap-image-help.txt");
    assert!(
        help.contains("# (an index of linux/amd64 and linux/arm64;"),
        "the pinned bootstrap image's fixture must record that it is an index of both platforms"
    );
    let values = read("charts/logweir/values.yaml");
    assert!(
        values
            .lines()
            .any(|l| l.trim_start().starts_with("bootstrapImage:") && l.ends_with("amd64 + arm64")),
        "values.yaml's identity.bootstrapImage comment must name both platforms"
    );
}

// Approval-policy parity.

/// The `approvalPolicy` document a values file would render, exactly the
/// fields `templates/approval-policy.yaml` writes into the ConfigMap — and
/// `default`, which it writes as `defaultMode` when set. The mode
/// spellings are passed through: the binary reads `confirm`/`strict` as the
/// chart's translation does (`chart_lint_the_operator_mode_names_render_as_the_internal_ones`).
fn approval_policy_document(values: &Value) -> String {
    let block = values.get("approvalPolicy").cloned().unwrap_or(Value::Null);
    let mut document = serde_yaml::Mapping::new();
    for field in ["allowOrdinaryConfirmation", "policies", "namespaces"] {
        if let Some(v) = block.get(field) {
            document.insert(Value::String(field.to_string()), v.clone());
        }
    }
    if let Some(default) = block
        .get("default")
        .and_then(Value::as_str)
        .filter(|d| !d.is_empty())
    {
        document.insert(
            Value::String("defaultMode".to_string()),
            Value::String(default.to_string()),
        );
    }
    serde_yaml::to_string(&Value::Mapping(document)).expect("a document")
}

/// **Every approval-policy document the chart is meant to refuse, the binary
/// refuses**. A document the binary refuses stops the whole
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
    assert!(cases.len() >= 19, "the refusal list shrank: {cases:?}");
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

/// **A DEFAULT render carries neither manual-run block**, so an
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

// Values that reach nothing.

/// Every leaf path of a `values.yaml`-shaped document: a mapping that is
/// EMPTY, and every sequence and scalar, is one leaf — the same rule
/// `scripts/check-chart-values.sh`'s header states.
fn value_leaves(node: &Value, prefix: &str, out: &mut BTreeSet<String>) {
    match node.as_mapping() {
        Some(map) if !map.is_empty() => {
            for (k, v) in map {
                let k = k.as_str().expect("values.yaml keys are strings");
                let path = if prefix.is_empty() {
                    k.to_string()
                } else {
                    format!("{prefix}.{k}")
                };
                value_leaves(v, &path, out);
            }
        }
        _ => {
            out.insert(prefix.to_string());
        }
    }
}

/// **Every chart value has a row in `scripts/check-chart-values.sh`, which
/// renders it ALONE at a non-default value and fails unless the render
/// changes.**
///
/// Every committed render carries every value at its default, which is also
/// what a template that ignores the value renders, so only the script (it
/// needs `helm`, GC22) can see a value that reaches nothing. This row keeps it
/// complete: its table must name EXACTLY the leaves `values.yaml` has.
#[test]
fn chart_lint_every_chart_value_has_a_probe_row_that_must_change_the_render() {
    let script = read("scripts/check-chart-values.sh");
    let start = script
        .find("<<'ROWS'\n")
        .expect("the table opens with <<'ROWS'")
        + "<<'ROWS'\n".len();
    let end = start
        + script[start..]
            .find("\nROWS\n")
            .expect("the table closes with ROWS");
    let mut rows = BTreeSet::new();
    let mut seen = 0usize;
    for line in script[start..end].lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.splitn(5, '|').collect();
        assert_eq!(fields.len(), 5, "a malformed row: `{line}`");
        let (leaf, base, flag) = (fields[0], fields[1], fields[3]);
        assert!(
            base == "default"
                || repo()
                    .join(format!("charts/logweir/examples/{base}.values.yaml"))
                    .is_file(),
            "row `{leaf}` names base `{base}`, which is not an example"
        );
        assert!(
            ["--set", "--set-string", "--set-json"].contains(&flag.trim_start_matches('!')),
            "row `{leaf}` uses flag `{flag}`"
        );
        assert!(rows.insert(leaf.to_string()), "two rows for `{leaf}`");
        seen += 1;
    }
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    let mut leaves = BTreeSet::new();
    value_leaves(&values, "", &mut leaves);
    let missing: Vec<&String> = leaves.difference(&rows).collect();
    let stale: Vec<&String> = rows.difference(&leaves).collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "scripts/check-chart-values.sh must have exactly one row per values.yaml leaf.\n  \
         values with no row: {missing:?}\n  rows with no value: {stale:?}"
    );
    assert!(seen >= 150, "only {seen} rows: the table has gone quiet");
    // AND THE GATE RUNS IT, with the bootstrap override every other render gets.
    assert!(
        read("scripts/check-chart.sh").contains(
            "bash scripts/check-chart-values.sh ${bootstrap_render_args[@]+\"${bootstrap_render_args[@]}\"}"
        ),
        "scripts/check-chart.sh must run scripts/check-chart-values.sh"
    );
}

// No approver key by default.

/// The bootstrap Job's arguments in a render.
fn bootstrap_args(docs: &[Doc]) -> Vec<String> {
    container(find(docs, "Job", "logweir-identity-bootstrap"))["args"]
        .as_sequence()
        .expect("bootstrap args")
        .iter()
        .map(|v| v.as_str().expect("string arg").to_string())
        .collect()
}

/// The flags `identity bootstrap --help` of the PINNED bootstrap image lists
/// (`crates/logweir/tests/fixtures/bootstrap-image-help.txt`, refreshed at
/// every re-pin), and the digest of the image that help came from (the digest
/// only: the runner repository is named once under `crates/`).
fn pinned_bootstrap_flags() -> (String, BTreeSet<String>) {
    let help = read("crates/logweir/tests/fixtures/bootstrap-image-help.txt");
    let image = help
        .lines()
        .find_map(|l| l.strip_prefix("# digest: "))
        .expect("the fixture names the digest its help came from")
        .trim()
        .to_string();
    let flags = help
        .lines()
        .filter(|l| !l.starts_with('#'))
        .flat_map(|l| l.split(|c: char| c.is_whitespace() || c == ','))
        .filter(|w| w.starts_with("--") && w.len() > 2)
        .map(|w| w.split('=').next().unwrap_or(w).to_string())
        .collect();
    (image, flags)
}

/// The `--flags` among a hook's arguments that `flags` does not list.
fn flags_not_run(args: &[String], flags: &BTreeSet<String>) -> Vec<String> {
    args.iter()
        .filter(|a| a.starts_with("--") && !flags.contains(a.as_str()))
        .cloned()
        .collect()
}

/// **The chart passes the hook only flags its pinned image runs.** The
/// bootstrap image is a digest published by CI AFTER the commit that teaches
/// it new flags, so the chart may pass them only once it is re-pinned: every
/// render that keeps the chart's own `identity.bootstrapFeatures.consoleKey`
/// passes only what the pinned image's `--help` lists, and the fixture names
/// the pinned digest. A re-pin without refreshing the fixture, or a flipped
/// feature without a re-pin, is a red gate instead of a failed install.
#[test]
fn chart_lint_the_hook_passes_only_flags_its_pinned_image_runs() {
    let values: Value = serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values");
    let pinned = values["identity"]["bootstrapImage"]
        .as_str()
        .expect("the pinned bootstrap image")
        .to_string();
    let feature = values["identity"]["bootstrapFeatures"]["consoleKey"]
        .as_bool()
        .expect("identity.bootstrapFeatures.consoleKey");
    let (digest, flags) = pinned_bootstrap_flags();
    assert_eq!(
        Some(digest.as_str()),
        pinned.split_once('@').map(|(_, d)| d),
        "crates/logweir/tests/fixtures/bootstrap-image-help.txt is the help of another image: \
         refresh it from `docker run --rm --platform linux/amd64 {pinned} identity bootstrap \
         --help` when you re-pin"
    );
    for flag in ["--namespace", "--public-configmap-name"] {
        assert!(flags.contains(flag), "the fixture lists {flag}: {flags:?}");
    }
    let mut checked = 0;
    for path in files_under("charts/logweir/rendered") {
        let Some(render) = path
            .rsplit('/')
            .next()
            .and_then(|f| f.strip_suffix(".yaml"))
        else {
            continue;
        };
        let example = variant_values(render);
        if example["identity"]["enabled"].as_bool() == Some(false) {
            continue;
        }
        let image = example["identity"]["bootstrapImage"]
            .as_str()
            .unwrap_or(&pinned);
        let its_feature = example["identity"]["bootstrapFeatures"]["consoleKey"]
            .as_bool()
            .unwrap_or(feature);
        if image != pinned || its_feature != feature {
            // Another image (author-only), or the example that documents the
            // feature ahead of its default (console-fresh-install).
            continue;
        }
        let args = bootstrap_args(&rendered(render));
        assert_eq!(
            flags_not_run(&args, &flags),
            Vec::<String>::new(),
            "rendered/{render}.yaml passes the pinned bootstrap image flags its `--help` does not \
             list; re-pin identity.bootstrapImage (and refresh the fixture) before flipping \
             identity.bootstrapFeatures"
        );
        checked += 1;
    }
    assert!(checked >= 5, "the rule read {checked} renders");

    // While the feature is off, the default render passes none of the
    // console-key flags: merging is inert with the pin.
    let default_args = bootstrap_args(&rendered("default"));
    let prod_16_1 = [
        "--console-secret-name",
        "--console-secret-key",
        "--console-public-configmap-name",
        "--installation-trust-policy",
        "--allowed-target-cluster-id",
        "--mark-fresh-install-confirm",
        "--revoke-trust-binding",
    ];
    if !feature {
        for flag in prod_16_1 {
            assert!(
                !default_args.iter().any(|a| a == flag),
                "the default render passes {flag} while identity.bootstrapFeatures.consoleKey \
                 is false"
            );
        }
    }
    // NEGATIVE CONTROL: the rule reports exactly the flags a help text lacks:
    // the feature example's arguments against a help without the console-key
    // flags (the pinned image's today).
    let fresh = bootstrap_args(&rendered("console-fresh-install"));
    let old_help: BTreeSet<String> = flags
        .iter()
        .filter(|f| !prod_16_1.contains(&f.as_str()))
        .cloned()
        .collect();
    let missing = flags_not_run(&fresh, &old_help);
    assert_eq!(
        missing,
        vec![
            "--console-secret-name",
            "--console-public-configmap-name",
            "--installation-trust-policy",
            "--mark-fresh-install-confirm",
            "--revoke-trust-binding",
        ],
        "the subset rule must name every flag an older image refuses"
    );
}

/// **A fresh install with a console needs no key handled by a person**
/// In the console render the identity hook also manages the
/// console's ConsoleConfirmation key — empty, retained, creation-only
/// placeholders, `get`/`patch` by name and nothing else — creates the
/// installation trust, and marks the fresh install `confirm`; the controller
/// and the console are told where the marker is read; the console mounts the
/// managed key `optional` (the hook fills it after the pod starts). The
/// default render (no console) manages no console key and marks nothing:
/// without a console nothing could confirm.
#[test]
fn chart_lint_a_console_render_generates_the_console_key_and_marks_the_fresh_install() {
    // The fresh-install example: a console with the bootstrap image's
    // fresh-install feature (`identity.bootstrapFeatures.consoleKey`).
    let docs = rendered("console-fresh-install");
    for (kind, name) in [
        ("Secret", "logweir-console-confirmation"),
        ("ConfigMap", "logweir-console-trust"),
    ] {
        let object = find(&docs, kind, name);
        let annotations = &object.value["metadata"]["annotations"];
        assert_eq!(
            Some("keep"),
            annotations["helm.sh/resource-policy"].as_str()
        );
        assert_eq!(
            Some("pre-install,pre-upgrade"),
            annotations["helm.sh/hook"].as_str(),
            "{kind}/{name} is a creation-only placeholder"
        );
        assert_eq!(
            Some("uninitialized"),
            annotations["logweir.dev/identity-state"].as_str()
        );
        assert!(
            object.value["data"]
                .as_mapping()
                .is_some_and(serde_yaml::Mapping::is_empty),
            "{kind}/{name}: Helm renders no key bytes"
        );
    }
    let args = bootstrap_args(&docs);
    for pair in [
        ["--console-secret-name", "logweir-console-confirmation"],
        ["--console-public-configmap-name", "logweir-console-trust"],
        ["--installation-trust-policy", "logweir-installation"],
    ] {
        assert!(
            args.windows(2).any(|w| w[0] == pair[0] && w[1] == pair[1]),
            "console render: the hook is given {pair:?}: {args:?}"
        );
    }
    assert!(
        args.iter().any(|a| a == "--mark-fresh-install-confirm"),
        "console render: a fresh install with a console starts in confirm: {args:?}"
    );
    // The Role: every rule named, no list, no wildcard, the console objects in.
    let role = find(&docs, "Role", "logweir-identity-bootstrap");
    let mut named: BTreeSet<String> = BTreeSet::new();
    for rule in role.value["rules"].as_sequence().expect("rules") {
        let names = rule["resourceNames"]
            .as_sequence()
            .filter(|n| !n.is_empty())
            .expect("every bootstrap permission is resourceNames-scoped");
        for verb in rule["verbs"].as_sequence().expect("verbs") {
            assert!(
                matches!(verb.as_str(), Some("get" | "patch")),
                "bootstrap verbs are get and patch only: {verb:?}"
            );
        }
        named.extend(names.iter().map(|n| n.as_str().expect("name").to_string()));
    }
    for name in ["logweir-console-confirmation", "logweir-console-trust"] {
        assert!(
            named.contains(name),
            "{name} is reachable by name: {named:?}"
        );
    }
    // The readers of the marker.
    let deployment = find(&docs, "Deployment", "weirkeeper");
    let env = container(deployment)["env"].as_sequence().expect("env");
    assert!(
        env.iter().any(
            |e| e["name"].as_str() == Some("LOGWEIR_IDENTITY_PUBLIC_CONFIGMAP")
                && e["value"].as_str() == Some("logweir-signing-trust")
        ),
        "the controller reads the marker from the public identity ConfigMap"
    );
    let (_, config) = console_config("console-fresh-install");
    assert_eq!(
        config["installationIdentity"]["namespace"].as_str(),
        Some("logweir-system")
    );
    assert_eq!(
        config["installationIdentity"]["publicConfigMap"].as_str(),
        Some("logweir-signing-trust")
    );
    assert_eq!(
        config["confirmationKeyFile"].as_str(),
        Some("/var/run/logweir/confirmation/confirmation.key")
    );
    // The managed key, and only it, may be missing at start.
    assert_eq!(config["confirmationKeyManaged"].as_bool(), Some(true));

    // NEGATIVE CONTROL: the same console with the bootstrap feature OFF (an
    // operator pinning an older bootstrap image,
    // `examples/console-older-bootstrap.values.yaml`) passes none of its
    // flags and renders none of its objects or readers.
    let off = rendered("console-older-bootstrap");
    let off_args = bootstrap_args(&off);
    assert!(
        !off_args.iter().any(|a| a.starts_with("--console")
            || a == "--installation-trust-policy"
            || a == "--mark-fresh-install-confirm"
            || a == "--revoke-trust-binding"),
        "the console render without the bootstrap feature passes no PROD-16.1 flag: {off_args:?}"
    );
    assert!(
        !off.iter()
            .any(|d| d.name() == "logweir-console-confirmation"
                || d.name() == "logweir-console-trust"
                || d.name() == "logweir-identity-trust"
                || d.name() == "logweir-api-installation"),
        "no managed key, no trust grant and no marker read without the bootstrap feature"
    );

    // NEGATIVE CONTROL: the default render.
    let default = rendered("default");
    let args = bootstrap_args(&default);
    assert!(
        !args
            .iter()
            .any(|a| a == "--mark-fresh-install-confirm" || a.starts_with("--console")),
        "the default render (no console) marks nothing and manages no console key: {args:?}"
    );
    assert!(
        !default
            .iter()
            .any(|d| d.name() == "logweir-console-confirmation"
                || d.name() == "logweir-console-trust"),
        "no console key placeholder without a console"
    );
    let env = container(find(&default, "Deployment", "weirkeeper"))["env"]
        .as_sequence()
        .expect("env");
    assert!(
        !env.iter()
            .any(|e| e["name"].as_str() == Some("LOGWEIR_IDENTITY_PUBLIC_CONFIGMAP")),
        "the default controller reads no marker"
    );
}

/// The operator's three mode names are one mapping, and the chart's half of
/// it is the core's: `confirm` renders `Ordinary`, `strict` renders
/// `Governed` (`templates/approval-policy.yaml`,
/// `logweir.approvalPolicy.internalMode`), checked against
/// `logweir_core::approval_policy::ApprovalRoute` and the binary's parser.
#[test]
fn chart_lint_the_operator_mode_names_render_as_the_internal_ones() {
    let template = read("charts/logweir/templates/approval-policy.yaml");
    for (operator, internal) in [("confirm", "Ordinary"), ("strict", "Governed")] {
        assert!(
            template.contains(&format!(
                "(eq $m \"{operator}\") (eq $m \"{internal}\") -}}}}{internal}"
            )),
            "the chart maps {operator} to {internal}"
        );
        let parsed = logweir_core::approval_policy::ApprovalPolicySet::parse(&format!(
            "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: {operator}\nnamespaces:\n  team-a: p\n"
        ))
        .expect("the binary reads the operator name too");
        let effective = parsed.resolve("team-a");
        assert_eq!(effective.mode().as_str(), internal);
        assert_eq!(
            logweir_core::approval_policy::ApprovalRoute::of(&effective).as_str(),
            operator
        );
    }
    // check-chart.sh renders them (a #[test] may not shell out to helm).
    let gate = read("scripts/check-chart.sh");
    assert!(gate.contains("operator mode names render as the internal ones"));
}

/// **A `two-person` policy renders as `Governed` with
/// `approverSignature: Console`, only with the shared console, and the binary
/// reads the rendered document as two-person.**
///
/// - The checked-in render of `examples/console-two-person.values.yaml`
///   carries the document in the internal words, the binary parses it, the
///   bound namespace resolves to `two-person`, and the strict policy beside
///   it carries no `approverSignature` key and stays strict.
/// - The same document written in the operator's words (the example's own
///   values) is the SAME set to the binary: one policy, one digest.
/// - The template refuses the setting unless `api.console.enabled` and
///   `api.console.mode` is `shared`, and `scripts/check-chart.sh` runs that
///   refusal through `helm` (no console, the principal alone, the
///   administrator console; both spellings), with the shared console as the
///   control.
/// - NO OTHER checked-in render carries the key, so an older binary still
///   reads every document that does not ask for two-person.
#[test]
fn chart_lint_two_person_renders_as_the_console_setting_and_only_with_the_shared_console() {
    use logweir_core::approval_policy::{ApprovalPolicySet, ApprovalRoute, ApproverSignature};
    let docs = rendered("console-two-person");
    let map = docs
        .iter()
        .find(|d| {
            d.kind == "ConfigMap"
                && d.value["metadata"]["labels"]["app.kubernetes.io/component"].as_str()
                    == Some("approval-policy")
        })
        .expect("the two-person example renders its approval-policy ConfigMap");
    let document = map.value["data"]["approval-policy.yaml"]
        .as_str()
        .expect("the document");
    assert!(
        document.contains("approverSignature: Console"),
        "{document}"
    );
    assert!(
        !document.contains("mode: two-person") && document.contains("mode: Governed"),
        "the internal words: {document}"
    );
    assert_eq!(
        document.matches("approverSignature").count(),
        1,
        "only the two-person policy carries the key:\n{document}"
    );
    let parsed = ApprovalPolicySet::parse(document)
        .unwrap_or_else(|e| panic!("the rendered two-person document is refused: {e}"));
    let pair = parsed.resolve("team-a");
    assert_eq!(
        ApprovalRoute::of(&pair),
        ApprovalRoute::SecondPersonInConsole
    );
    assert_eq!(
        pair.bound().map(|p| p.approver_signature),
        Some(ApproverSignature::Console)
    );
    let strict = parsed.resolve("team-b");
    assert_eq!(ApprovalRoute::of(&strict), ApprovalRoute::PersonalKey);
    assert_eq!(
        strict.bound().map(|p| p.approver_signature),
        Some(ApproverSignature::PersonalKey)
    );
    // The operator's words are the same set.
    let example: Value = serde_yaml::from_str(&read(
        "charts/logweir/examples/console-two-person.values.yaml",
    ))
    .expect("the example");
    assert_eq!(
        ApprovalPolicySet::parse(&approval_policy_document(&example))
            .expect("the example's own document"),
        parsed
    );
    assert_eq!(example["api"]["console"]["mode"].as_str(), Some("shared"));

    // The console of that render is the shared one.
    let (_, config) = console_config("console-two-person");
    assert_eq!(config["mode"].as_str(), Some("shared"), "{config:?}");

    // No other render carries the key.
    for file in files_under("charts/logweir/rendered") {
        if file.ends_with("console-two-person.yaml") {
            continue;
        }
        assert!(
            !read(&file).contains("approverSignature"),
            "{file} carries approverSignature"
        );
    }

    // The template's refusal, and the gate that runs it.
    let template = read("charts/logweir/templates/approval-policy.yaml");
    assert!(
        template.contains(
            "{{- if not (and $.Values.api.enabled $.Values.api.console.enabled (eq \
             ($.Values.api.console.mode | default \"\") \"shared\")) -}}"
        ),
        "the two-person refusal is tied to the shared console"
    );
    assert!(template.contains("else if eq $m \"two-person\" -}}Governed"));
    let gate = read("scripts/check-chart.sh");
    for row in [
        "a two-person policy with no console at all",
        "a two-person policy with the in-cluster administrator console",
        "approverSignature: Console with the in-cluster administrator console",
        "a strict policy renders no approverSignature key",
    ] {
        assert!(gate.contains(row), "check-chart.sh runs: {row}");
    }
}

/// **A shared console the chart publishes names the proxy in front of it, or
/// opts out by name.**
///
/// The console caps the connections one peer outside its trusted-proxy set may
/// hold, and the sign-in limiter counts each client behind a trusted proxy;
/// with no trusted proxy configured neither can tell the ingress from any other
/// peer, so the cap is off and every sign-in shares one budget. So with
/// `api.console.ingress.enabled` the chart refuses to render unless
/// `trustedProxyService` or `trustedProxyCidrs` names the proxy, or
/// `api.console.trustedProxy` is `none`. This half reads the checked-in values,
/// schema, `fail`s and every values file the repository renders with the chart
/// today; `scripts/check-chart.sh` runs the refusal, the opt-out, the
/// contradiction and the localAdmin rows through `helm`.
#[test]
fn chart_lint_a_published_shared_console_names_its_trusted_proxy_or_opts_out() {
    let values: Value =
        serde_yaml::from_str(&read("charts/logweir/values.yaml")).expect("values.yaml parses");
    assert_eq!(
        values["api"]["console"]["trustedProxy"].as_str(),
        Some(""),
        "api.console.trustedProxy must default to empty: the opt-out is never the default"
    );

    let schema: serde_json::Value =
        serde_json::from_str(&read("charts/logweir/values.schema.json"))
            .expect("the schema parses");
    let opt_out =
        &schema["properties"]["api"]["properties"]["console"]["properties"]["trustedProxy"];
    assert_eq!(
        opt_out["enum"],
        serde_json::json!(["", "none"]),
        "api.console.trustedProxy must be typed as exactly \"\" or \"none\""
    );

    let template = read("charts/logweir/templates/ui/api-config.yaml");
    for needle in [
        "{{- if and (not $c.trustedProxyCidrs) (not $proxySvc.name) (ne (toString $c.trustedProxy) \"none\") -}}",
        "api.console.ingress.enabled in shared mode needs the trusted proxy named",
        "api.console.trustedProxy=none says the console trusts no proxy",
        "api.console.trustedProxy belongs to api.console.mode=shared.",
    ] {
        assert!(
            template.contains(needle),
            "templates/ui/api-config.yaml must still carry `{needle}`"
        );
    }
    // The requirement lives inside the Ingress arm: a shared console with no
    // Ingress of the chart's own is not refused for it.
    let ingress_arm = template
        .find("{{- if $c.ingress.enabled -}}\n{{- if not $c.ingress.host -}}")
        .expect("the shared Ingress arm");
    let requirement = template
        .find("api.console.ingress.enabled in shared mode needs the trusted proxy named")
        .expect("the requirement");
    let arm_end = template[ingress_arm..]
        .find("{{- else -}}\n{{- fail (printf \"api.console.mode is %q.")
        .map(|offset| ingress_arm + offset)
        .expect("the end of the shared arm");
    assert!(
        ingress_arm < requirement && requirement < arm_end,
        "the trusted-proxy requirement must sit inside the shared mode's Ingress arm"
    );

    // Every values file this repository renders with the current chart that
    // publishes a shared console names its proxy (or opts out by name), so
    // none of them stops rendering. The R2 rehearsal file is rendered with
    // f49849d's chart, not this one, and is out of scope.
    let mut files = files_under("charts/logweir/examples");
    files.push("deploy/poc/logweir.values.yaml".to_owned());
    let mut published = 0;
    for file in files.iter().filter(|f| f.ends_with(".values.yaml")) {
        let v: Value = serde_yaml::from_str(&read(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        let c = &v["api"]["console"];
        if c["mode"].as_str() != Some("shared") || c["ingress"]["enabled"].as_bool() != Some(true) {
            continue;
        }
        published += 1;
        let cidrs = c["trustedProxyCidrs"]
            .as_sequence()
            .is_some_and(|s| !s.is_empty());
        let service = c["trustedProxyService"]["name"]
            .as_str()
            .is_some_and(|s| !s.is_empty());
        let none = c["trustedProxy"].as_str() == Some("none");
        assert!(
            cidrs || service || none,
            "{file} publishes a shared console without naming its trusted proxy: the chart \
             now refuses to render it"
        );
    }
    assert!(
        published >= 2,
        "expected the shared example and the PoC values to publish a shared console; found \
         {published}"
    );
}
