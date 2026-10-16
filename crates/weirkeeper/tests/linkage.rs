//! The properties this half of `weirkeeper` exists to establish: the linkage
//! boundary, the manifest, the double, the startup path, and the status-write
//! contract (`conditions.rs`) the reconcilers share. Each group's header says
//! what it proves and what it does not.
//!
//! WHY SO MANY OF THESE READ A FILE INSTEAD OF CALLING CODE. The properties
//! are properties of a MANIFEST and of a dependency GRAPH, and a build failure
//! is not a named property: when someone drops `features = ["util"]` the crate
//! stops compiling, nothing records why, and once the compile error is made to
//! go away by other means the property is gone with no test to notice.
//! Reading the manifest asserts the decision at assertion time, without a
//! build.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use http_body_util::BodyExt;
use weirkeeper::testing::{
    answer, mock_client, mock_client_recording, recorder, Route, SeenRequest,
};

// Shared helpers

/// The workspace root: this crate's manifest directory is
/// `crates/weirkeeper`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn manifest_text() -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The value text of `key` inside `[section]`, comments skipped.
///
/// A hand-rolled read of two TOML shapes rather than a `toml` dependency: this
/// crate's manifest is the only file it ever parses. Comment lines are dropped
/// FIRST: the manifest's own comments discuss `[dev-dependencies]`,
/// `features = ["util"]` and `default-features = false`, and a parser that
/// read them would pass while the real entries said something else.
fn manifest_entry(text: &str, section: &str, key: &str) -> Option<String> {
    let mut current = String::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            current = line[1..line.len() - 1].to_string();
            continue;
        }
        if current != section {
            continue;
        }
        let Some((lhs, rhs)) = line.split_once('=') else {
            continue;
        };
        if lhs.trim() == key {
            return Some(rhs.trim().to_string());
        }
    }
    None
}

/// Every dependency every workspace member DECLARES, of every kind, as a
/// name → names map restricted to workspace members.
///
/// `--no-deps` deliberately: it resolves nothing, downloads nothing and takes
/// no package-cache lock, so this test cannot hang behind a concurrent build.
/// Restricting the graph to workspace members loses nothing: a registry crate
/// cannot depend on a `path` dependency, so every route from `weirkeeper` to
/// `logweir-evidence` runs through workspace members only.
fn workspace_dependency_graph() -> std::collections::BTreeMap<String, BTreeSet<String>> {
    let out = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
        ])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        out.status.success(),
        "cargo metadata --no-deps failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata emits JSON");
    let packages = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .expect("metadata carries packages");
    let members: BTreeSet<String> = packages
        .iter()
        .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
        .map(str::to_string)
        .collect();
    let mut graph = std::collections::BTreeMap::new();
    for p in packages {
        let name = p
            .get("name")
            .and_then(|n| n.as_str())
            .expect("a package has a name")
            .to_string();
        let deps: BTreeSet<String> = p
            .get("dependencies")
            .and_then(|d| d.as_array())
            .expect("a package carries a dependency array")
            .iter()
            .filter_map(|d| d.get("name").and_then(|n| n.as_str()))
            .filter(|n| members.contains(*n))
            .map(str::to_string)
            .collect();
        graph.insert(name, deps);
    }
    graph
}

/// The transitive closure of `root` over that graph.
fn transitive(
    graph: &std::collections::BTreeMap<String, BTreeSet<String>>,
    root: &str,
) -> BTreeSet<String> {
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

// G-SIGN, first half: the linkage boundary

/// The half of the one-signer rule a test can reach.
///
/// WHAT THIS PROVES: this crate reaches the VERIFYING crate and does not reach
/// the SIGNING one, over every declared dependency kind: a dev edge onto the
/// signer would link it just as hard as a normal one.
///
/// WHAT THIS DOES NOT PROVE: anything about the CAPABILITY to sign. No
/// control-plane crate LINKS the signer, and the capability is unbroken while
/// this controller holds Job CRUD over the signing key's namespace: the key is
/// a Kubernetes Secret, and a Job the controller creates can mount it with no
/// Logweir crate involved. That residual is accepted and stated on every
/// surface; `scripts/check-withdrawn-claim.sh` keeps a stronger reading of
/// this test off every shipped surface, this file included.
#[test]
fn weirkeeper_links_logweir_verify_and_never_logweir_evidence() {
    let graph = workspace_dependency_graph();
    let reach = transitive(&graph, "weirkeeper");
    assert!(
        reach.contains("logweir-verify"),
        "weirkeeper must reach logweir-verify (it verifies the DSSE signatures the UI renders); \
         it reaches {reach:?}"
    );
    assert!(
        !reach.contains("logweir-evidence"),
        // The two API names this refusal is about are deliberately NOT spelled
        // here: `scripts/check-one-signer.sh` check 3 greps every `crates/**/*.rs`
        // for them outside a comment, and `weirkeeper` is not on
        // `ALLOWED_SOURCE`, so naming them in an assertion message would make this
        // test's own text a violation of the gate it corroborates.
        "weirkeeper must NOT reach logweir-evidence — that crate holds the signing half of the \
         evidence machinery (Global Constraint 27, guard G-SIGN). It reaches {reach:?}"
    );
}

// The manifest: three reads, three separate decisions

/// The three already-resolved entries, and the feature that makes one of them
/// work.
#[test]
fn the_three_declared_entries_carry_their_features() {
    let text = manifest_text();
    for name in ["tower", "http", "http-body-util"] {
        assert!(
            manifest_entry(&text, "dependencies", name).is_some(),
            "{name} must be declared under [dependencies]: src/testing.rs is a src/ module, so a \
             [dev-dependencies] placement does not compile"
        );
        assert!(
            manifest_entry(&text, "dev-dependencies", name).is_none(),
            "{name} must NOT be under [dev-dependencies] — see above"
        );
    }
    let tower = manifest_entry(&text, "dependencies", "tower").expect("checked above");
    assert!(
        tower.contains(r#"features = ["util"]"#),
        "tower must carry features = [\"util\"]: `service_fn` sits behind it and tower 0.5's own \
         defaults are [\"log\"]. Got: {tower}"
    );
}

/// The one dependency decision of this plan, asserted as written.
#[test]
fn kube_is_declared_without_default_features() {
    let text = manifest_text();
    let kube = manifest_entry(&text, "dependencies", "kube").expect("kube is declared");
    assert!(
        kube.contains("default-features = false"),
        "kube must be declared with default-features = false — its default set is \
         [client, rustls-tls, ring], and taking the defaults leaves the feature set free to \
         drift with the upstream crate. Got: {kube}"
    );
    // `unstable-runtime`: three empty cfg features, no package
    // (`no_new_package_enters_the_graph_for_the_mock` measures that), for
    // `Controller::for_stream`: the `BackupSchedule` watch filtered to spec
    // revisions so its own status writes stop waking it.
    assert!(
        kube.contains(
            r#"features = ["client", "runtime", "derive", "rustls-tls", "unstable-runtime"]"#
        ),
        "kube's feature list must be exactly [\"client\", \"runtime\", \"derive\", \
         \"rustls-tls\", \"unstable-runtime\"] — no openssl-tls, no oauth, no gzip. Got: {kube}"
    );
    let openapi =
        manifest_entry(&text, "dependencies", "k8s-openapi").expect("k8s-openapi is declared");
    assert!(
        openapi.contains(r#"features = ["v1_29"]"#),
        "k8s-openapi must carry exactly the v1_29 feature (Global Constraint 25: minimum \
         Kubernetes 1.29). Got: {openapi}"
    );
}

/// The entries that claim to be declarations of packages the workspace already
/// resolves add none.
///
/// TWO COUNTS, MEASURED, NOT ARGUED. The workspace is copied to a temp
/// overlay, `cargo metadata` is counted there, those entries are commented out
/// in the copy, and it is counted again. Equality is the property. `rustls` is
/// one of them: the binary installs a process-level `CryptoProvider`, which
/// means naming `rustls` here, and `ring` and `rustls` are both already in
/// `Cargo.lock`.
///
/// THE SET OF ENTRIES IS DISCOVERED, NOT HARD-CODED, AND THAT IS WHAT KILLS
/// THE MUTANT. Against a literal list, a further manifest entry pulling a
/// package that is not in `Cargo.lock` would be present in BOTH overlays, both
/// counts would rise together and equality would hold. Discovering every
/// plain third-party version requirement (everything under `[dependencies]`
/// that is not `kube`, not `k8s-openapi`, not a `path` dependency and not
/// `workspace = true`) puts that entry in the commented-out set, where it makes
/// the two numbers differ. The set is asserted to be exactly the four AFTER
/// the counts are compared, so the count difference is the failure a reviewer
/// sees.
///
/// `--offline` throughout: a unit test never reaches the network, and the
/// overlay carries the same `Cargo.lock`, so pruning edges resolves out of the
/// local cache.
#[test]
fn no_new_package_enters_the_graph_for_the_mock() {
    let root = workspace_root();
    let overlay = Overlay::of(&root);

    let with = package_count(overlay.path());

    let manifest = overlay.path().join("crates/weirkeeper/Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("the overlay carries the manifest");
    let claimed = claimed_already_resolved(&text);
    assert!(
        !claimed.is_empty(),
        "the discovery found no plain third-party entry to comment out, so this test would \
         compare a graph against itself"
    );
    let out: String = text
        .lines()
        .map(|line| {
            let key = line.trim_start().split('=').next().unwrap_or("").trim();
            if claimed.iter().any(|c| c == key) && !line.trim_start().starts_with('#') {
                format!("# {line}")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&manifest, out).expect("the overlay is writable");

    let without = package_count(overlay.path());

    // Recorded, not merely compared: the two numbers are the measurement.
    // Visible with `cargo test -- --nocapture`.
    println!(
        "no_new_package_enters_the_graph_for_the_mock: {claimed:?} present → {with} packages; \
         commented out → {without} packages"
    );

    assert_eq!(
        with, without,
        "the entries {claimed:?} must add no package to the resolved graph (Global Constraint \
         38): with them the graph holds {with} packages, with them commented out {without}. An \
         entry naming a package that is not already in Cargo.lock is what makes these differ."
    );
    assert_eq!(
        claimed,
        vec!["tower", "http", "http-body-util", "rustls"],
        "the plain third-party entries in this manifest are fixed at these four (Global \
         Constraint 38 closes the workspace graph); got {claimed:?}"
    );
}

/// Every `[dependencies]` key this manifest declares as a plain third-party
/// version requirement, in manifest order.
///
/// Excluded: `kube` and `k8s-openapi`, which this crate deliberately ADDS;
/// anything carrying `path =`, a workspace member; anything carrying
/// `workspace = true` or written in the dotted `name.workspace = true` form,
/// which takes a requirement the root manifest already holds. What is left is
/// the set of entries whose justification is "already in `Cargo.lock`".
fn claimed_already_resolved(text: &str) -> Vec<String> {
    let mut section = String::new();
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].to_string();
            continue;
        }
        if section != "dependencies" {
            continue;
        }
        let Some((lhs, rhs)) = line.split_once('=') else {
            continue;
        };
        let key = lhs.trim();
        if key.contains('.') || key == "kube" || key == "k8s-openapi" {
            continue;
        }
        if rhs.contains("workspace = true") || rhs.contains("path =") {
            continue;
        }
        out.push(key.to_string());
    }
    out
}

/// `cargo metadata`'s package count for a workspace, offline.
fn package_count(dir: &Path) -> usize {
    let out = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--offline"])
        .current_dir(dir)
        .output()
        .expect("cargo metadata runs");
    assert!(
        out.status.success(),
        "cargo metadata in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata emits JSON");
    meta.get("packages")
        .and_then(|p| p.as_array())
        .expect("metadata carries packages")
        .len()
}

/// A throwaway copy of the workspace's manifests and sources. `target/` is
/// excluded: it is the one directory that would make this copy expensive, and
/// `cargo metadata` never reads it. A directory under `std::env::temp_dir()`
/// plus a `Drop` replaces a `tempfile` dependency.
struct Overlay(PathBuf);

impl Overlay {
    fn of(root: &Path) -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("weirkeeper-graph-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("the temp dir is creatable");
        // Everything cargo needs to load the workspace, and nothing else.
        for entry in [
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            "crates",
            "e2e",
            "xtask",
        ] {
            let src = root.join(entry);
            assert!(src.exists(), "the workspace carries {entry}");
            let st = Command::new("cp")
                .arg("-a")
                .arg(&src)
                .arg(dir.join(entry))
                .status()
                .expect("cp runs");
            assert!(st.success(), "cp -a {} failed", src.display());
        }
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// The Secret refusal

/// The controller never reads a Secret's data: a source-level refusal, not a
/// permission and not a comment. A controller that reads a SCRAM password or a
/// signing key directly is holding credential material it has no business
/// holding; the credential goes to the Job, by reference, and the Job mounts
/// it. The one Secret handle is the workspace gate's (`workspace.rs`), and it
/// reads metadata only (PROD-19.1): its labels, never its data.
#[test]
fn the_controller_never_reads_a_secret() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut files = 0usize;
    let mut gate_reads = 0usize;
    walk(&src, &mut |path, text| {
        files += 1;
        let gate = path.ends_with("workspace.rs");
        for needle in ["Api::<Secret>", "Api<Secret>", "\"secrets\""] {
            if text.contains(needle) && !gate {
                offenders.push(format!("{}: {needle}", path.display()));
            }
        }
        if gate {
            // One Secret handle, and its one call is the metadata read
            // (`manifest_lint::weirkeeper_reads_secret_metadata_only` holds
            // the method set across the crate).
            gate_reads = text.matches("Api<Secret>").count();
            if gate_reads != 1 || !text.contains("api.get_metadata_opt(&name)") {
                offenders.push(format!(
                    "{}: the gate's Secret read changed",
                    path.display()
                ));
            }
        }
    });
    assert!(files >= 3, "the walk must see src/, got {files} files");
    assert!(gate_reads >= 1, "the gate's Secret read was not found");
    assert!(
        offenders.is_empty(),
        "weirkeeper must not read Secrets' data: {offenders:?}"
    );
}

fn walk(dir: &Path, f: &mut impl FnMut(&Path, &str)) {
    for e in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display())) {
        let e = e.expect("a readable directory entry");
        let p = e.path();
        if p.is_dir() {
            walk(&p, f);
        } else if p.extension().is_some_and(|x| x == "rs") {
            let text = std::fs::read_to_string(&p).expect("a readable source file");
            f(&p, &text);
        }
    }
}

// The double

fn one_route() -> Vec<Route> {
    vec![Route {
        method: "GET",
        path_suffix: "/namespaces/logweir-t15/configmaps/weirkeeper-probe",
        status: 200,
        body: r#"{"kind":"ConfigMap"}"#.to_string(),
    }]
}

/// An unmatched request is a panic naming the method and the path.
///
/// Asserted against [`answer`], the route resolution the double's service
/// calls, because that is where the refusal is OBSERVABLE:
/// `kube::Client::new` wraps the service in a `tower::buffer::Buffer` that
/// drives it on a spawned task, and tokio's task harness catches the unwind
/// before it can reach this thread. See `src/testing.rs`'s module
/// documentation. The end-to-end path is asserted below over a matched route.
#[test]
#[should_panic(expected = "no route for GET /api/v1/namespaces/logweir-t15/pods/weirkeeper-0")]
fn mock_client_panics_on_an_unmatched_request() {
    let routes = one_route();
    let rec = recorder();
    let _ = answer(
        &routes,
        &rec,
        "GET",
        "/api/v1/namespaces/logweir-t15/pods/weirkeeper-0",
    );
}

/// [`mock_client`] — the plain form every later reconciler test will reach
/// for — is the same double without the recorder end.
#[tokio::test]
async fn mock_client_is_the_plain_form_of_the_double() {
    let client = mock_client(one_route());
    let req = http::Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/logweir-t15/configmaps/weirkeeper-probe")
        .body(Vec::new())
        .expect("the request builds");
    assert_eq!(
        client
            .request_text(req)
            .await
            .expect("the recorded route answers 200"),
        r#"{"kind":"ConfigMap"}"#
    );
}

/// The recorded route is answered, and the request is recorded in order —
/// through the real `kube::Client`, so the `tower`/`http`/`kube::client::Body`
/// wiring is exercised and not merely described.
#[tokio::test]
async fn the_double_answers_a_recorded_route_through_the_client_and_records_it() {
    let (client, rec) = mock_client_recording(one_route());
    let req = http::Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/logweir-t15/configmaps/weirkeeper-probe")
        .body(Vec::new())
        .expect("the request builds");
    let text = client
        .request_text(req)
        .await
        .expect("the recorded route answers 200");
    assert_eq!(text, r#"{"kind":"ConfigMap"}"#);
    let seen = rec.lock().expect("the recorder is readable").clone();
    assert_eq!(
        seen,
        vec![SeenRequest {
            method: "GET".to_string(),
            uri: "/api/v1/namespaces/logweir-t15/configmaps/weirkeeper-probe".to_string(),
        }],
        "the double records every request in order"
    );
}

/// The response body the double builds is the recorded body, byte for byte —
/// read back through `http_body_util::BodyExt`, which is why that entry is in
/// the manifest.
#[tokio::test]
async fn the_double_builds_the_recorded_body() {
    let routes = one_route();
    let rec = recorder();
    let resp = answer(
        &routes,
        &rec,
        "get",
        "/api/v1/namespaces/logweir-t15/configmaps/weirkeeper-probe?limit=1",
    );
    assert_eq!(resp.status().as_u16(), 200);
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("the recorded body collects")
        .to_bytes();
    assert_eq!(
        String::from_utf8(bytes.to_vec()).expect("the recorded body is UTF-8"),
        r#"{"kind":"ConfigMap"}"#,
        "the method match is case-insensitive and the query string is not part of the path"
    );
}

// The binary

/// `weirkeeper --version` exits 0 and prints one line.
///
/// Runs the binary cargo has ALREADY built for this test target, never `cargo
/// run`, which would re-enter cargo while `cargo test --workspace` holds the
/// build lock. `scripts/check-image-weirkeeper.sh` check 2 runs exactly this
/// argv against the built image.
#[test]
fn weirkeeper_version_exits_zero() {
    let out = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .arg("--version")
        .output()
        .expect("the built binary runs");
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).expect("the version line is UTF-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "one line, got {lines:?}");
    assert!(
        lines[0].starts_with("weirkeeper "),
        "the line must begin `weirkeeper `, got {:?}",
        lines[0]
    );

    // The second arm: an unrecognised argv exits 1 and names what it got.
    let out = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .arg("--reconcile-everything")
        .output()
        .expect("the built binary runs");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).expect("the refusal is UTF-8");
    assert!(
        stderr.contains("--reconcile-everything"),
        "the refusal must name what it got, got {stderr:?}"
    );
}

/// **PROD-19.1: no install, no controller.** Without the install's namespace,
/// or with a workspace list that is absent or malformed, the binary refuses to
/// start (exit 1) and names the variable, before any client is built.
/// CONTROL: the start-up rows above set both and run.
#[test]
fn weirkeeper_refuses_to_start_without_its_namespace_and_workspaces() {
    for (namespace, workspaces, named) in [
        (None, Some("dev"), "LOGWEIR_INSTALLATION_NAMESPACE"),
        (Some("logweir"), None, "LOGWEIR_WORKSPACES"),
        (Some("logweir"), Some("dev,Dev"), "LOGWEIR_WORKSPACES"),
        (Some("logweir"), Some("dev,dev"), "LOGWEIR_WORKSPACES"),
    ] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_weirkeeper"));
        cmd.env("RUST_LOG", "info")
            .env("KUBECONFIG", "/nonexistent")
            .env_remove("LOGWEIR_INSTALLATION_NAMESPACE")
            .env_remove("LOGWEIR_WORKSPACES")
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env_remove("KUBERNETES_SERVICE_PORT");
        if let Some(n) = namespace {
            cmd.env("LOGWEIR_INSTALLATION_NAMESPACE", n);
        }
        if let Some(w) = workspaces {
            cmd.env("LOGWEIR_WORKSPACES", w);
        }
        let out = cmd.output().expect("the built binary runs");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(1), "{named}: {text}");
        assert!(
            text.contains("refusing to start") && text.contains(named),
            "{named}: {text}"
        );
    }
}

/// `weirkeeper --help` exits 0 and prints one paragraph.
#[test]
fn weirkeeper_help_exits_zero_with_one_paragraph() {
    let out = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .arg("--help")
        .output()
        .expect("the built binary runs");
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).expect("the help text is UTF-8");
    assert_eq!(
        stdout.lines().filter(|l| l.trim().is_empty()).count(),
        0,
        "one paragraph means no blank line: {stdout}"
    );
    assert!(stdout.contains("weirkeeper"), "got {stdout:?}");
}

// The no-argv startup path.
//
// rustls 0.23 auto-selects a process-level CryptoProvider only when EXACTLY
// ONE of `ring` / `aws-lc-rs` is enabled, and this workspace's unified graph
// enables both (`aws-lc-rs` through `object_store` → `reqwest`, `ring`
// through `ureq 2.12.1`), so without an explicit install `weirkeeper` with
// no argv aborts at exit 101 inside `kube::Client::try_default()`, before
// `main`'s `error!("no Kubernetes client…")` branch.
//
// TWO TESTS: the first drives the startup path IN-PROCESS, so the provider
// install has an assertion of its own; the second runs the SHIPPED BINARY,
// because exit codes and log lines are properties of a process that an
// in-process test cannot observe.
//
// NEITHER DIALS. Building a `kube::Client` constructs a connector; it opens no
// socket, and no `Api` call is ever made. The fixture's apiserver is
// `https://127.0.0.1:1`: privileged, unused, and never contacted, so a
// regression which DOES dial fails loudly and instantly (connection refused)
// instead of hanging on a routable host. It is not one of
// `crates/logweir/tests/no_network_in_unit_tests.rs`'s `DIAL_TOKENS`, so this
// file needs no `ALLOWED` entry there.

/// The unreachable apiserver every fixture in this section points at.
const FIXTURE_APISERVER: &str = "https://127.0.0.1:1";

/// A kubeconfig that PARSES and yields a usable `Config`.
///
/// No credential material of any kind: no `certificate-authority-data`, no
/// token, no client certificate, no exec plugin. A controller starting against
/// this reaches exactly as far as building a client and no further.
fn good_kubeconfig() -> String {
    format!(
        "apiVersion: v1\n\
         kind: Config\n\
         clusters:\n\
         - name: weirkeeper-fixture\n\
         \x20 cluster:\n\
         \x20   server: {FIXTURE_APISERVER}\n\
         contexts:\n\
         - name: weirkeeper-fixture\n\
         \x20 context:\n\
         \x20   cluster: weirkeeper-fixture\n\
         \x20   user: weirkeeper-fixture\n\
         current-context: weirkeeper-fixture\n\
         users:\n\
         - name: weirkeeper-fixture\n\
         \x20 user: {{}}\n"
    )
}

/// A kubeconfig whose current context names a cluster that is not there.
///
/// Valid YAML, valid kubeconfig shape, unresolvable context, so
/// `Config::infer()` returns a clean `Err` and `main`'s error branch is
/// REACHED rather than merely written.
fn unresolvable_kubeconfig() -> String {
    "apiVersion: v1\n\
     kind: Config\n\
     clusters: []\n\
     contexts: []\n\
     current-context: weirkeeper-no-such-context\n\
     users: []\n"
        .to_string()
}

/// A kubeconfig written to a private temp directory, removed on drop: the
/// same `std::env::temp_dir()` + `Drop` shape as [`Overlay`].
struct Fixture(PathBuf);

impl Fixture {
    fn write(tag: &str, text: &str) -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "weirkeeper-kubeconfig-{tag}-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("the temp dir is creatable");
        let path = dir.join("kubeconfig.yaml");
        std::fs::write(&path, text).expect("the fixture is writable");
        Self(dir)
    }

    fn path(&self) -> PathBuf {
        self.0.join("kubeconfig.yaml")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `true` only on macOS, and only for kube's `NoValidNativeRootCA` whose cause
/// is the Security framework failing to read a trust-settings domain (an
/// `ErrorKind::Os` from `rustls-native-certs`'s `macos.rs`). Everything else —
/// every other error, and this one off macOS — is `false`, so it fails the
/// caller exactly as it did before this predicate existed.
fn macos_platform_roots_unreadable(e: &kube::Error) -> bool {
    if !cfg!(target_os = "macos") {
        return false;
    }
    match e {
        kube::Error::RustlsTls(kube::client::RustlsTlsError::NoValidNativeRootCA(io)) => {
            let text = io.to_string();
            text.contains("trust settings") && text.contains("kind: Os(")
        }
        _ => false,
    }
}

/// The startup path, in-process: the provider is installed, and the client
/// builds instead of panicking.
///
/// WHAT THIS PROVES. `weirkeeper::install_default_crypto_provider()` leaves a
/// process-level `CryptoProvider` installed, and with one installed the two
/// calls `main` makes (a `Config` off a kubeconfig, then
/// `kube::Client::try_from`) complete cleanly. `Client::try_from` builds the
/// rustls HTTPS connector through `ConfigExt`, and `ClientConfig::builder()` is
/// the call that panics without a provider.
///
/// WHAT THIS DOES NOT PROVE. Nothing about reaching an apiserver: building a
/// client is not a connection, and the outcome here is `Ok`. The clean `Err`
/// half of `main`'s branch is the second arm below, and the process-level
/// behaviour is the next test.
///
/// The install's effect is asserted separately so the mutant (delete the
/// install, or empty the function) fails at the `get_default().is_some()` line
/// with a named message rather than a rustls stack trace.
///
/// ONE PLATFORM ERROR IS NAMED, ON macOS ONLY, AND IT IS NOT A SKIP. With no
/// `certificate-authority-data` in the fixture, `Client::try_from` asks
/// `rustls-native-certs` for the platform roots, and on macOS that
/// Security-framework call sometimes fails on a loaded host with
/// `NoValidNativeRootCA(… "failed to load system trust settings" … Os { code:
/// -36, "I/O error" })`: the OS failing to READ ITS TRUST STORE, reached only
/// AFTER `ClientConfig::builder()` has run
/// (`kube-client-0.99.0/src/client/tls.rs`:
/// `ClientConfig::builder().with_native_roots()`). On exactly that error the
/// test says so on stderr and REQUIRES the same `Config` to build an `Ok`
/// client with an explicit, empty trust set (`root_cert = Some(vec![])`),
/// which takes kube's other branch
/// (`ClientConfig::builder().with_root_certificates(..)`) and never touches the
/// platform roots. Without a provider, `ClientConfig::builder()` panics on BOTH
/// branches. Any other error, and this error on any other OS, fails the test.
#[tokio::test]
async fn the_startup_path_builds_a_client_from_a_kubeconfig_without_panicking() {
    weirkeeper::install_default_crypto_provider();
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_some(),
        "install_default_crypto_provider() must leave a process-level CryptoProvider \
         installed. Without one, rustls 0.23 tries to pick from its crate features, this \
         workspace enables BOTH ring (via ureq) and aws-lc-rs (via object_store -> reqwest), \
         and rustls panics at rustls-0.23.43/src/crypto/mod.rs:249 — which is the exit-101 \
         abort of the no-argv path (review finding H1)."
    );

    let fixture = Fixture::write("good", &good_kubeconfig());
    let kubeconfig =
        kube::config::Kubeconfig::read_from(fixture.path()).expect("the fixture kubeconfig parses");
    let cfg = kube::Config::from_custom_kubeconfig(
        kubeconfig,
        &kube::config::KubeConfigOptions::default(),
    )
    .await
    .expect("the fixture kubeconfig yields a Config");
    assert!(
        cfg.cluster_url.to_string().starts_with(FIXTURE_APISERVER),
        "the fixture points at the unreachable apiserver, got {}",
        cfg.cluster_url
    );

    // THE CALL THAT ABORTS THE PROCESS WITHOUT A PROVIDER.
    let client = match kube::Client::try_from(cfg.clone()) {
        Ok(client) => client,
        Err(e) if macos_platform_roots_unreadable(&e) => {
            eprintln!(
                "NOTE (macOS only): the platform roots are unavailable on this host — the \
                 Security framework could not read its trust settings ({e:?}). \
                 ClientConfig::builder() already ran without panicking; re-building the same \
                 Config with an explicit empty trust set, which needs no platform roots, and \
                 requiring Ok."
            );
            let mut pinned = cfg;
            pinned.root_cert = Some(Vec::new());
            kube::Client::try_from(pinned).expect(
                "with an explicit trust set, kube::Client::try_from must build a client from the \
                 fixture Config — it constructs a connector and opens no socket",
            )
        }
        Err(e) => panic!(
            "kube::Client::try_from must build a client from the fixture Config — it constructs \
             a connector and opens no socket: {e:?}"
        ),
    };
    assert_eq!(
        client.default_namespace(),
        "default",
        "a context with no namespace resolves to `default`, which is the value main logs"
    );

    // THE CLEAN-ERROR HALF: main's `error!(\"no Kubernetes client…\")` branch is
    // reachable, and reached by a typed Err rather than by an unwind.
    let broken = Fixture::write("unresolvable", &unresolvable_kubeconfig());
    let kubeconfig =
        kube::config::Kubeconfig::read_from(broken.path()).expect("the shape still parses");
    let err = kube::Config::from_custom_kubeconfig(
        kubeconfig,
        &kube::config::KubeConfigOptions::default(),
    )
    .await
    .expect_err("a context that names no cluster must be a clean Err, not a panic");
    let text = err.to_string();
    assert!(
        !text.is_empty(),
        "the error main logs must have a Display form"
    );
}

/// How long the OS may take to let the binary run `--version` once, before the
/// timed arm starts. Separate from arm 1's 10 s product bound: a first exec
/// on a loaded development host has taken 13.7 s, and 30 s leaves room for
/// that without charging it to the controller.
const EXEC_WARM_UP_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// Exec `bin --version` once and wait, at most `bound`, for it to exit 0.
///
/// This exercises nothing under test (`--version` precedes every client and
/// every subscriber in `main`) and exists only so the OS's first-exec
/// assessment of a freshly uplifted executable is not measured as controller
/// startup. The child is bounded and killed on expiry, and its output goes to
/// /dev/null so no pipe can fill.
fn warm_the_executable(bin: &str, bound: std::time::Duration) -> std::time::Duration {
    let started = std::time::Instant::now();
    let mut child = Command::new(bin)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("the built binary runs");
    loop {
        if let Some(status) = child.try_wait().expect("the child is waitable") {
            assert!(
                status.success(),
                "the exec warm-up `{bin} --version` must exit 0, got {:?}",
                status.code()
            );
            return started.elapsed();
        }
        if started.elapsed() >= bound {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the OS did not let `{bin} --version` finish within {bound:?}. This is the \
                 exec warm-up, BEFORE the timed no-argv arm: the host could not start the \
                 executable at all, and nothing about the controller's startup path was measured."
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// The shipped binary, with no argv and a `KUBECONFIG`, end to end.
///
/// Runs the binary cargo has ALREADY built for this test target, never `cargo
/// run`.
///
/// ARM 1, the no-argv clause. With a kubeconfig it can build a client from,
/// the binary installs its subscriber, logs ONE line naming the registered
/// controller count, and exits **0** on SIGTERM; without a crypto provider it
/// exits **101** with a rustls panic on stderr.
///
/// ARM 2, the error branch. With a kubeconfig whose context resolves to
/// nothing, the binary exits **1** having logged `no Kubernetes client`.
///
/// STDERR IS ASSERTED EMPTY IN BOTH ARMS: every readable line goes to STDOUT
/// as JSON (the pod log API has no stream selector), so a non-empty stderr
/// means a panic or a subscriber that never installed.
///
/// NO SOCKET. Nothing is listening on `127.0.0.1:1` and no `Api` call is made,
/// so the process builds a client, logs, and waits for a signal.
///
/// OUTPUT GOES TO FILES, NOT PIPES: a long-lived child whose stdout is a pipe
/// nobody drains can block on a full buffer, and the exit code is read from
/// `Child::wait()` directly, never through a pipe.
///
/// THE OS'S FIRST EXEC IS PAID BEFORE THE CLOCK STARTS. cargo re-uplifts
/// `CARGO_BIN_EXE_weirkeeper` with a NEW inode on every `cargo test`, and macOS
/// assesses a never-seen executable on its first exec (8.1 s against 0.12 s
/// after, with the child printing nothing while it waits). So
/// [`warm_the_executable`] execs the same path once with `--version` (which
/// builds no client and is asserted by `weirkeeper_version_exits_zero`), and
/// only then does arm 1 start its 10 s clock. The warm-up has its OWN bound
/// ([`EXEC_WARM_UP_BOUND`]); a host that cannot exec the binary even then
/// fails with a message saying the timed arm never started.
#[test]
fn the_no_argv_binary_starts_and_exits_zero_on_sigterm() {
    let warm_up = warm_the_executable(env!("CARGO_BIN_EXE_weirkeeper"), EXEC_WARM_UP_BOUND);
    eprintln!("exec warm-up (`weirkeeper --version`) took {warm_up:?}");

    // ---- ARM 1: the good kubeconfig, SIGTERM, exit 0 -------------------
    let fixture = Fixture::write("proc-good", &good_kubeconfig());
    let out_path = fixture.path().with_file_name("stdout.log");
    let err_path = fixture.path().with_file_name("stderr.log");

    // Chart gap G5: this process's loopback health listener, on a port chosen
    // now so two runs on one host cannot collide on the default.
    let health_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
        .to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .env("KUBECONFIG", fixture.path())
        .env("LOGWEIR_HEALTH_ADDR", &health_addr)
        .env("LOGWEIR_INSTALLATION_NAMESPACE", "logweir")
        .env("LOGWEIR_WORKSPACES", "dev,prd")
        // The subscriber filters from RUST_LOG; `info` is what makes the
        // startup line and the SIGTERM line observable.
        .env("RUST_LOG", "info")
        // Never let a stray in-cluster environment win over the fixture:
        // `Config::infer()` tries in-cluster FIRST, and a workstation that
        // happens to export these would take a different branch.
        .env_remove("KUBERNETES_SERVICE_HOST")
        .env_remove("KUBERNETES_SERVICE_PORT")
        .stdout(std::fs::File::create(&out_path).expect("the log file is creatable"))
        .stderr(std::fs::File::create(&err_path).expect("the log file is creatable"))
        .spawn()
        .expect("the built binary runs");

    // Wait for the startup line, or for an early exit, whichever comes first.
    // 10 s is a generous ceiling on "install a subscriber and build a
    // connector"; measured, it is milliseconds. The executable is already warm,
    // so this clock measures the controller.
    let arm_1_started = std::time::Instant::now();
    let deadline = arm_1_started + std::time::Duration::from_secs(10);
    let mut early = None;
    loop {
        if let Some(status) = child.try_wait().expect("the child is waitable") {
            early = Some(status);
            break;
        }
        if std::fs::read_to_string(&out_path)
            .unwrap_or_default()
            .contains("weirkeeper started")
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the no-argv binary never logged `weirkeeper started` within 10 s.\n\
                 stdout: {:?}\nstderr: {:?}",
                std::fs::read_to_string(&out_path).unwrap_or_default(),
                std::fs::read_to_string(&err_path).unwrap_or_default()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }

    eprintln!(
        "arm 1 reached `weirkeeper started` (or exited) after {:?}",
        arm_1_started.elapsed()
    );
    let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
    assert!(
        early.is_none(),
        "the no-argv binary must NOT exit on its own: it exited {:?}.\n\
         This is review finding H1: exit 101 with `Could not automatically determine the \
         process-level CryptoProvider` means the provider install in main is gone.\n\
         stdout: {stdout:?}\nstderr: {stderr:?}",
        early.map(|s| s.code())
    );
    assert!(
        stderr.is_empty(),
        "nothing may reach stderr — every readable line is JSON on stdout (Global Constraint \
         11), so a non-empty stderr is a panic or a missing subscriber. Got: {stderr:?}"
    );
    assert!(
        stdout.contains("\"controllers\":14"),
        "the startup line names the registered controller count: fourteen, from \
         `controllers::trust_roster` and `controllers::approval` (Task 16) to \
         `controllers::retention_policy` (D3 W9) and `controllers::switchover` (PROD-04.2). D3 W7's `controllers::rehearsal_schedule` \
         is withdrawn in v1 (OD-12). A count that is not 14 means `main`'s registration \
         point lost a `controllers.push(…)` line. \
         Got: {stdout:?}"
    );

    // The kubelet's exec probe is this same executable with `--probe`, against
    // the running controller's loopback listener. Both answer ok once
    // `weirkeeper started` is logged (the fixture's apiserver is unreachable
    // and every controller rides that out with backoff, which is not a reason
    // to restart). NEGATIVE CONTROLS: an unknown probe word, and a port nothing
    // listens on, exit 1. Each probe is a subprocess with its OWN outer bound,
    // independent of the binary's 2 s deadlines: a probe that hangs is killed
    // at 10 s and fails the row.
    let probe = |word: &str, addr: &str| {
        let out_path = fixture.path().with_file_name(format!("probe-{word}.out"));
        let err_path = fixture.path().with_file_name(format!("probe-{word}.err"));
        let mut child = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
            .args(["--probe", word])
            .env("LOGWEIR_HEALTH_ADDR", addr)
            .stdout(std::fs::File::create(&out_path).expect("the log file is creatable"))
            .stderr(std::fs::File::create(&err_path).expect("the log file is creatable"))
            .spawn()
            .expect("the built binary runs");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.try_wait().expect("the probe is waitable") {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("`weirkeeper --probe {word}` did not exit within 10 s");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        std::process::Output {
            status,
            stdout: std::fs::read(&out_path).unwrap_or_default(),
            stderr: std::fs::read(&err_path).unwrap_or_default(),
        }
    };
    for word in ["live", "ready"] {
        let out = probe(word, &health_addr);
        assert_eq!(
            out.status.code(),
            Some(0),
            "`weirkeeper --probe {word}` against the running controller: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    assert_eq!(probe("healthy", &health_addr).status.code(), Some(1));
    let nobody = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
        .to_string();
    let out = probe("live", &nobody);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no health listener"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // SIGTERM through the shell's builtin rather than a `kill` binary, which is
    // not present on every image; the exit code that matters is read from
    // `Child::wait()` below, not from this process.
    let pid = child.id();
    let signalled = Command::new("/bin/sh")
        .args(["-c", &format!("kill -TERM {pid}")])
        .status()
        .expect("the shell runs");
    assert!(
        signalled.success(),
        "SIGTERM could not be delivered to {pid}"
    );

    let status = child.wait().expect("the child is waitable");
    let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
    assert_eq!(
        status.code(),
        Some(0),
        "SIGTERM must be an ordinary shutdown: a controller that exits non-zero on `kubectl \
         delete pod` turns a rollout into a CrashLoopBackOff.\nstdout: {stdout:?}\nstderr: \
         {stderr:?}"
    );
    assert!(
        stdout.contains("SIGTERM"),
        "the shutdown is logged, not silent. Got: {stdout:?}"
    );
    assert!(stderr.is_empty(), "still nothing on stderr: {stderr:?}");

    // ---- ARM 2: the unresolvable kubeconfig, exit 1, named error --------
    let broken = Fixture::write("proc-unresolvable", &unresolvable_kubeconfig());
    let out = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .env("KUBECONFIG", broken.path())
        .env("LOGWEIR_INSTALLATION_NAMESPACE", "logweir")
        .env("LOGWEIR_WORKSPACES", "dev,prd")
        .env("RUST_LOG", "info")
        .env_remove("KUBERNETES_SERVICE_HOST")
        .env_remove("KUBERNETES_SERVICE_PORT")
        .output()
        .expect("the built binary runs");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a kubeconfig that resolves to nothing is an exit-1 named error, never a panic \
         (finding H1b).\nstdout: {stdout:?}\nstderr: {stderr:?}"
    );
    assert!(
        stdout.contains("no Kubernetes client"),
        "the error branch names itself on stdout. Got: {stdout:?}"
    );
    assert!(
        stderr.is_empty(),
        "the error branch logs; it does not panic. Got: {stderr:?}"
    );
}

// The status-write contract.
//
// `conditions::merge_condition`, `::apply_merge_patch` and `::status_unchanged`
// are the ONE implementation of a rule every reconciler obeys. The reconciler
// suites assert what each reconciler DOES with them; these assert what they
// are, so a change to the rule fails here by name instead of somewhere in a
// route table.

use serde_json::json;
use weirkeeper::conditions::{
    apply_merge_patch, current_condition, merge_condition, status_unchanged,
};
use weirkeeper::crds::Condition;

fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("a literal RFC 3339 instant")
        .with_timezone(&chrono::Utc)
}

fn cond(status: &str, reason: &str, message: &str, when: &str) -> Condition {
    Condition {
        r#type: "Ready".to_string(),
        status: status.to_string(),
        observed_generation: Some(1),
        last_transition_time: Some(at(when)),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
    }
}

/// `lastTransitionTime` moves on a transition and ONLY on a transition: the
/// `metav1.Condition` contract, without which a reconciler rewrites its
/// status on every pass.
#[test]
fn a_transition_time_moves_only_when_the_status_or_the_reason_moves() {
    let old = cond(
        "True",
        "Scheduled",
        "fired at midnight",
        "2026-09-10T00:00:00Z",
    );
    let now = "2026-09-10T12:00:00Z";

    // The MESSAGE differs and nothing else: NOT a transition.
    let same = merge_condition(
        Some(&old),
        cond("True", "Scheduled", "next firing 2026-09-11", now),
    );
    assert_eq!(
        same.last_transition_time,
        Some(at("2026-09-10T00:00:00Z")),
        "the message carries instants that move by design; comparing it would make every \
         message change a transition and put the hot loop straight back"
    );
    assert_eq!(
        same.message.as_deref(),
        Some("next firing 2026-09-11"),
        "and the new message IS written — only the timestamp is inherited"
    );

    // The STATUS differs: a transition.
    assert_eq!(
        merge_condition(Some(&old), cond("False", "Scheduled", "m", now)).last_transition_time,
        Some(at(now)),
        "True -> False is a transition"
    );
    // The REASON differs: a transition.
    assert_eq!(
        merge_condition(Some(&old), cond("True", "Suspended", "m", now)).last_transition_time,
        Some(at(now)),
        "Scheduled -> Suspended is a transition even at the same status"
    );
    // No previous condition at all: the first appearance IS a transition.
    assert_eq!(
        merge_condition(None, cond("True", "Scheduled", "m", now)).last_transition_time,
        Some(at(now)),
        "a condition that did not exist has just transitioned into existence"
    );
}

/// The lookup half finds a condition by `type` and nothing else.
#[test]
fn current_condition_is_keyed_by_type() {
    let mut ready = cond("True", "Scheduled", "m", "2026-09-10T00:00:00Z");
    ready.r#type = "Ready".to_string();
    let mut failed = cond("False", "Operational", "m", "2026-09-10T01:00:00Z");
    failed.r#type = "Failed".to_string();
    let conditions = vec![ready, failed];

    assert_eq!(
        current_condition(Some(&conditions), "Failed").map(|c| c.status.clone()),
        Some("False".to_string())
    );
    assert!(current_condition(Some(&conditions), "Admitted").is_none());
    assert!(current_condition(None, "Ready").is_none());
}

/// `apply_merge_patch` is RFC 7386, including the two rules that decide
/// whether a status write is a no-op.
#[test]
fn the_merge_patch_is_rfc_7386() {
    // An omitted key leaves its value alone; a present key replaces it.
    let mut target = json!({"phase": "Running", "jobRef": {"name": "j1"}});
    apply_merge_patch(&mut target, &json!({"phase": "Succeeded"}));
    assert_eq!(
        target,
        json!({"phase": "Succeeded", "jobRef": {"name": "j1"}}),
        "an omitted key means LEAVE IT ALONE, which is why `status_unchanged` asks whether the \
         patch would change anything rather than comparing two objects"
    );

    // Objects merge RECURSIVELY.
    apply_merge_patch(&mut target, &json!({"jobRef": {"uid": "u1"}}));
    assert_eq!(target["jobRef"], json!({"name": "j1", "uid": "u1"}));

    // `null` DELETES, and deleting an absent key changes nothing.
    apply_merge_patch(&mut target, &json!({"jobRef": null}));
    assert!(target.get("jobRef").is_none(), "null deletes: {target}");
    let before = target.clone();
    apply_merge_patch(&mut target, &json!({"jobRef": null}));
    assert_eq!(before, target, "deleting what is not there changes nothing");

    // Arrays are REPLACED, never merged. This is why every condition in this
    // crate is built by serialising `Condition`: the element is compared whole.
    let mut arrays = json!({"conditions": [{"type": "Ready", "status": "True"}]});
    apply_merge_patch(&mut arrays, &json!({"conditions": [{"type": "Failed"}]}));
    assert_eq!(
        arrays,
        json!({"conditions": [{"type": "Failed"}]}),
        "an array patch replaces the whole array: {arrays}"
    );

    // A non-object target under an object patch becomes an object first.
    let mut absent = serde_json::Value::Null;
    apply_merge_patch(&mut absent, &json!({"phase": "Pending"}));
    assert_eq!(
        absent,
        json!({"phase": "Pending"}),
        "a first write onto nothing"
    );
}

/// `status_unchanged` answers "would this patch change the object", which is
/// not "are these two objects equal".
#[test]
fn status_unchanged_is_a_question_about_the_patch_and_not_about_equality() {
    let current = json!({
        "phase": "Running",
        "jobRef": {"name": "j1"},
        "conditions": [{"type": "Ready", "status": "True", "reason": "Scheduled"}]
    });

    assert!(
        status_unchanged(Some(&current), &json!({"status": {"phase": "Running"}})),
        "a patch repeating what is there changes nothing — and it does NOT matter that it \
         mentions one key out of three"
    );
    assert!(
        status_unchanged(
            Some(&current),
            &json!({"status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True", "reason": "Scheduled"}]
            }})
        ),
        "including when it repeats the whole condition array"
    );
    assert!(
        status_unchanged(Some(&current), &json!({"status": {"exitCode": null}})),
        "a null on a key the object never had deletes nothing"
    );
    assert!(
        !status_unchanged(Some(&current), &json!({"status": {"phase": "Succeeded"}})),
        "a different value IS a change"
    );
    assert!(
        !status_unchanged(
            Some(&current),
            &json!({"status": {
                "conditions": [{
                    "type": "Ready", "status": "True", "reason": "Scheduled",
                    "lastTransitionTime": "2026-09-10T12:00:00Z"
                }]
            }})
        ),
        "and so is ONE extra field inside one condition — which is exactly what a fresh \
         `lastTransitionTime` is, and why writing it unconditionally spun three reconcilers"
    );
    assert!(
        !status_unchanged(None, &json!({"status": {"phase": "Running"}})),
        "an object with no status at all is changed by any status write"
    );
    assert!(
        status_unchanged(Some(&current), &json!({"metadata": {"x": 1}})),
        "a body with no `status` key changes no status"
    );
}
