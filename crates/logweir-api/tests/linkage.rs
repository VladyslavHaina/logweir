//! The linkage boundary and the source-level refusals.
//!
//! WHAT THIS PROVES. This crate reaches the CRD types and the verifying half
//! of the evidence machinery and does not reach the SIGNING half
//! (`logweir-evidence`), the engine wrapper (`logweir-engine-oso`) or the
//! broker client (`logweir-kafka`) — over every declared dependency kind,
//! from `cargo metadata` rather than from a text search. And that no source
//! file here names a Secret, Pod, log, exec, Job or delete API, a
//! non-merge patch, a cluster-wide list, or a raw request path.
//!
//! WHAT THIS DOES NOT PROVE: what the service's Kubernetes identity is
//! ALLOWED to do. That is RBAC, and it is a later stage's chart work.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// How long `cargo metadata` may take before this test kills it and fails.
///
/// `--offline --no-deps` is a manifest read, but it still takes the package
/// cache lock, and several workers build in this checkout at once. A lock this
/// test cannot get is a failure to report, never a wait to sit in: see the
/// module documentation of `tests/local_admin.rs` for what an unbounded child
/// cost here once.
const METADATA_LIMIT: Duration = Duration::from_secs(120);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-api sits two levels under the workspace root")
        .to_path_buf()
}

/// Run `command` to completion within [`METADATA_LIMIT`], draining both pipes
/// on their own threads so a full pipe buffer cannot be mistaken for a hang.
fn bounded_output(mut command: Command, label: &str) -> Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("{label}: does not start: {error}"));
    let mut out_pipe = child.stdout.take().expect("stdout was piped");
    let mut err_pipe = child.stderr.take().expect("stderr was piped");
    let out_reader = std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = out_pipe.read_to_end(&mut buffer);
        buffer
    });
    let err_reader = std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = err_pipe.read_to_end(&mut buffer);
        buffer
    });
    let collect = |reader: std::thread::JoinHandle<Vec<u8>>| reader.join().unwrap_or_default();

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("the child is waitable") {
            break status;
        }
        if started.elapsed() >= METADATA_LIMIT {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = String::from_utf8_lossy(&collect(err_reader)).into_owned();
            panic!("{label}: still running after {METADATA_LIMIT:?}; killed. stderr: {stderr}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    Output {
        status,
        stdout: collect(out_reader),
        stderr: collect(err_reader),
    }
}

fn workspace_graph() -> BTreeMap<String, BTreeSet<String>> {
    let mut command = Command::new(env!("CARGO"));
    command
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
        ])
        .current_dir(workspace_root());
    let out = bounded_output(command, "cargo metadata");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let packages = meta["packages"].as_array().unwrap();
    let members: BTreeSet<String> = packages
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();
    packages
        .iter()
        .map(|p| {
            let deps = p["dependencies"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|d| d["name"].as_str())
                .filter(|n| members.contains(*n))
                .map(str::to_string)
                .collect();
            (p["name"].as_str().unwrap().to_string(), deps)
        })
        .collect()
}

fn reaches(graph: &BTreeMap<String, BTreeSet<String>>, root: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut frontier = vec![root.to_string()];
    while let Some(node) = frontier.pop() {
        for d in graph.get(&node).into_iter().flatten() {
            if seen.insert(d.clone()) {
                frontier.push(d.clone());
            }
        }
    }
    seen
}

#[test]
fn the_api_links_the_crds_and_the_verifier_and_never_the_signer_or_the_engine() {
    let graph = workspace_graph();
    let reach = reaches(&graph, "logweir-api");
    for required in ["weirkeeper", "logweir-core", "logweir-verify"] {
        assert!(
            reach.contains(required),
            "logweir-api must reach {required}; it reaches {reach:?}"
        );
    }
    for forbidden in ["logweir-evidence", "logweir-engine-oso", "logweir-kafka"] {
        assert!(
            !reach.contains(forbidden),
            "logweir-api must NOT reach {forbidden} (the signing half, the engine wrapper and the \
             broker client); it reaches {reach:?}"
        );
    }
    // Nothing depends on this crate: it is a leaf binary.
    assert!(
        graph
            .iter()
            .all(|(name, deps)| name == "logweir-api" || !deps.contains("logweir-api")),
        "no crate may depend on logweir-api"
    );
}

#[test]
fn the_signer_gate_lists_this_crate_where_the_graph_puts_it() {
    let script =
        std::fs::read_to_string(workspace_root().join("scripts/check-one-signer.sh")).unwrap();
    let value = |name: &str| {
        let prefix = format!("{name}=\"");
        let line = script
            .lines()
            .find(|l| l.starts_with(&prefix))
            .unwrap_or_else(|| panic!("{name}"));
        line[prefix.len()..]
            .trim_end_matches('"')
            .split_whitespace()
            .collect::<Vec<_>>()
    };
    // It reaches the verifying crate and the primitives through weirkeeper, so
    // it is on those two allowlists...
    assert!(value("ALLOWED_VERIFY_LINK").contains(&"logweir-api"));
    assert!(value("ALLOWED_PRIMITIVE").contains(&"logweir-api"));
    // ...and on neither signing allowlist.
    assert!(!value("ALLOWED_LINK").contains(&"logweir-api"));
    assert!(!value("ALLOWED_SOURCE").contains(&"logweir-api"));
}

fn sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push((path.clone(), std::fs::read_to_string(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut out);
    assert!(out.len() > 10, "the walk found {} files", out.len());
    out
}

/// Code lines only: the module documentation discusses Secrets and Jobs at
/// length, and a comment is not a call.
fn code_lines(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.lines().enumerate().filter(|(_, line)| {
        let trimmed = line.trim_start();
        !trimmed.starts_with("//") && !trimmed.starts_with("/*") && !trimmed.starts_with('*')
    })
}

#[test]
fn no_source_here_names_a_forbidden_kubernetes_api() {
    // Each token is a call shape, not a word: `Secret` alone appears in
    // `credentialRef` documentation and in field names.
    let forbidden = [
        "k8s_openapi::api::core",
        "k8s_openapi::api::batch",
        "Api::<Secret>",
        "Api<Secret>",
        "Api<Pod>",
        "Api<Job>",
        "Api<ConfigMap>",
        "Api<Namespace>",
        "Api::all",
        "Api::all_with",
        ".delete(",
        ".delete_collection(",
        ".replace(&",
        "Patch::Json",
        "Patch::Apply",
        "Patch::Strategic",
        "logs(",
        "log_stream(",
        "exec(",
        "attach(",
        "portforward(",
        "client.request",
        "Request::builder()",
        // Sending an impersonation header, in either spelling. READING the
        // kubeconfig's own `impersonate` fields is how `refuse_impersonation`
        // refuses such a context, and is asserted positively below.
        "Impersonate-User",
        "Impersonate-Group",
        "Impersonate-Uid",
        "config.headers.push",
        "headers.insert(\"impersonate",
    ];
    let mut hits = Vec::new();
    for (path, text) in sources() {
        for (number, line) in code_lines(&text) {
            for token in forbidden {
                if line.contains(token) {
                    hits.push(format!(
                        "{}:{}: {}",
                        path.display(),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "forbidden Kubernetes API in this crate:\n{}",
        hits.join("\n")
    );
}

#[test]
fn the_kubernetes_adapter_is_the_only_module_that_names_the_client() {
    let mut naming = BTreeSet::new();
    for (path, text) in sources() {
        for (_, line) in code_lines(&text) {
            if line.contains("kube::Api")
                || line.contains("Api::namespaced")
                || line.contains("kube::Client")
                || line.contains("ListParams")
                || line.contains("PostParams")
                || line.contains("PatchParams")
            {
                naming.insert(path.file_name().unwrap().to_string_lossy().to_string());
            }
        }
    }
    assert_eq!(
        naming,
        BTreeSet::from(["kube.rs".to_string(), "lib.rs".to_string()]),
        "the Kubernetes client is named outside the adapter"
    );
}

#[test]
fn the_manifest_pins_the_one_new_dependency() {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    let axum = manifest
        .lines()
        .find(|l| l.starts_with("axum = "))
        .expect("axum is declared");
    assert!(axum.contains("default-features = false"), "{axum}");
    assert!(axum.contains(r#"features = ["http1", "tokio"]"#), "{axum}");
    assert!(
        manifest.contains("publish = false"),
        "the binary is not a release artifact yet"
    );
    // The CRD types and the hash helper come from the workspace, not copies.
    assert!(manifest.contains(r#"weirkeeper = { path = "../weirkeeper" }"#));
    assert!(manifest.contains(r#"logweir-core = { path = "../logweir-core" }"#));
    // Dependency ENTRIES, not the comments that explain the boundary.
    let entries: Vec<&str> = manifest
        .lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#') && l.contains('='))
        .collect();
    for forbidden in ["logweir-evidence", "logweir-engine-oso", "logweir-kafka"] {
        assert!(
            !entries.iter().any(|l| l.starts_with(forbidden)),
            "{forbidden} must not be a dependency of this crate"
        );
    }
}

/// The two refusals this crate makes about impersonation exist, and say so.
#[test]
fn both_impersonation_refusals_are_present() {
    let http =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/http.rs")).unwrap();
    assert!(
        http.contains(r#"starts_with("impersonate-")"#),
        "the inbound Impersonate-* refusal is gone from the boundary guard"
    );
    let kube =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/kube.rs")).unwrap();
    assert!(
        kube.contains("pub fn refuse_impersonation") && kube.contains("a.impersonate.is_some()"),
        "the kubeconfig impersonation refusal is gone from the adapter"
    );
}
