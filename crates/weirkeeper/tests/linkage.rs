//! The properties this half of `weirkeeper` exists to establish: the linkage
//! boundary, the manifest, and the double.
//!
//! ONE FILE, BECAUSE THE PLAN NAMES ONE. Every `#[test]` this task lands lives
//! here. They fall into four groups — the G-SIGN linkage half, the three
//! manifest reads, the source-level Secret refusal, and the `mock_client`
//! double — and each group's header says what it proves and what it does not.
//!
//! WHY SO MANY OF THESE READ A FILE INSTEAD OF CALLING CODE. The properties
//! are properties of a MANIFEST and of a dependency GRAPH, and a build failure
//! is not a named property: when someone drops `features = ["util"]` the crate
//! stops compiling, nothing records why, and the moment the compile error is
//! made to go away by some other means the property is gone with no test to
//! notice. Reading the manifest asserts the decision at assertion time,
//! without a build.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use http_body_util::BodyExt;
use weirkeeper::testing::{
    answer, mock_client, mock_client_recording, recorder, Route, SeenRequest,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

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
/// A hand-rolled read of two TOML shapes rather than a `toml` dependency:
/// Global Constraint 38 closes the workspace graph, and this crate's manifest
/// is the only file it ever parses. Comment lines are dropped FIRST, and that
/// matters — the manifest's own comments discuss `[dev-dependencies]`,
/// `features = ["util"]` and `default-features = false` at length, and a
/// parser that read them would pass while the real entries said something
/// else.
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
/// no package-cache lock, which keeps this a millisecond-scale test that
/// cannot hang behind a concurrent build (Global Constraint 22's 15 s bound).
/// Restricting the graph to workspace members loses nothing for the question
/// being asked: a registry crate cannot depend on a `path` dependency, so
/// every route from `weirkeeper` to `logweir-evidence` runs through workspace
/// members only.
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

// ---------------------------------------------------------------------------
// G-SIGN, first half — the linkage boundary
// ---------------------------------------------------------------------------

/// Global Constraint 27, the half a test can reach.
///
/// WHAT THIS PROVES: this crate reaches the VERIFYING crate and does not reach
/// the SIGNING one, over every declared dependency kind — a dev edge onto the
/// signer would link it just as hard as a normal one, which is why the walk
/// counts all kinds.
///
/// WHAT THIS DOES NOT PROVE: anything at all about the CAPABILITY to sign.
/// Global Constraint 27 is the position that holds: no control-plane crate
/// LINKS the signer, and the capability is unbroken while this controller
/// holds Job CRUD over the signing key's namespace — the key is a Kubernetes
/// Secret, and a Job the controller creates can mount it with no Logweir crate
/// involved. Residual O1, accepted, O0 default (a), stated on every surface.
/// A stronger reading of this test was asserted once, was RED on the tree it
/// was asserted about, and was withdrawn; `scripts/check-withdrawn-claim.sh`
/// is what keeps it off every shipped surface, this file included.
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
        // `ALLOWED_SOURCE`. Naming them in an assertion message would make this
        // test's own text a violation of the gate the test corroborates —
        // measured, on the first run of that gate against this crate.
        "weirkeeper must NOT reach logweir-evidence — that crate holds the signing half of the \
         evidence machinery (Global Constraint 27, guard G-SIGN). It reaches {reach:?}"
    );
}

// ---------------------------------------------------------------------------
// The manifest — three reads, three separate decisions
// ---------------------------------------------------------------------------

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
    assert!(
        kube.contains(r#"features = ["client", "runtime", "derive", "rustls-tls"]"#),
        "kube's feature list must be exactly [\"client\", \"runtime\", \"derive\", \
         \"rustls-tls\"] — no openssl-tls, no oauth, no gzip. Got: {kube}"
    );
    let openapi =
        manifest_entry(&text, "dependencies", "k8s-openapi").expect("k8s-openapi is declared");
    assert!(
        openapi.contains(r#"features = ["v1_29"]"#),
        "k8s-openapi must carry exactly the v1_29 feature (Global Constraint 25: minimum \
         Kubernetes 1.29). Got: {openapi}"
    );
}

/// Global Constraint 38: the entries that claim to be declarations of packages
/// the workspace already resolves add none.
///
/// TWO COUNTS, MEASURED, NOT ARGUED. The workspace is copied to a temp
/// overlay, `cargo metadata` is counted there, those entries are commented out
/// in the copy, and it is counted again. Equality is the property.
///
/// THE SET OF ENTRIES IS DISCOVERED, NOT HARD-CODED, AND THAT IS WHAT KILLS
/// THE MUTANT. Written against a literal `["tower", "http",
/// "http-body-util"]`, this test would PASS after someone adds a fourth
/// manifest entry pulling a package that is not in `Cargo.lock`: the fourth
/// entry would be present in BOTH overlays, so both counts would rise together
/// and equality would still hold. Discovering every plain third-party version
/// requirement — everything under `[dependencies]` that is not `kube`, not
/// `k8s-openapi`, not a `path` dependency and not `workspace = true` — puts
/// the fourth entry in the commented-out set, where it makes the two numbers
/// differ. The set is asserted to be exactly the three AFTER the counts are
/// compared, so the count difference is the failure a reviewer sees.
///
/// `--offline` throughout: a unit test never reaches the network (STANDING
/// RULE 7), and the overlay carries the same `Cargo.lock`, so pruning edges
/// resolves out of the local cache.
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

    // Recorded, not merely compared: the two numbers are the measurement, and
    // a report that says "equal" without saying equal to WHAT is not a
    // measurement. Visible with `cargo test -- --nocapture`.
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
        vec!["tower", "http", "http-body-util"],
        "the plain third-party entries in this manifest are fixed at these three (Global \
         Constraint 38 closes the workspace graph); got {claimed:?}"
    );
}

/// Every `[dependencies]` key this manifest declares as a plain third-party
/// version requirement, in manifest order.
///
/// Excluded, each for a stated reason: `kube` and `k8s-openapi`, which are the
/// packages this task deliberately ADDS; anything carrying `path =`, which is
/// a workspace member and not a third-party package; anything carrying
/// `workspace = true` or written in the dotted `name.workspace = true` form,
/// which takes a requirement the root manifest already holds. What is left is
/// precisely the set of entries whose justification is "already in
/// `Cargo.lock`" — the claim this test measures.
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

/// A throwaway copy of the workspace's manifests and sources.
///
/// `target/` is excluded — it is the one directory that would make this copy
/// expensive, and `cargo metadata` never reads it. No `tempfile`
/// dev-dependency: Global Constraint 38 fixes this crate's manifest additions,
/// and a directory under `std::env::temp_dir()` plus a `Drop` is the whole
/// requirement.
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

// ---------------------------------------------------------------------------
// The Secret refusal, landed here so every chain-O task inherits it
// ---------------------------------------------------------------------------

/// The controller never reads a Secret.
///
/// Not a permission and not a comment: a source-level refusal, landed in the
/// FIRST `weirkeeper` commit so that every task from 15b onward inherits a
/// test it has to argue with rather than a convention it can forget. A
/// controller that reads a SCRAM password or a signing key directly is holding
/// credential material it has no business holding — the credential goes to the
/// Job, by reference, and the Job mounts it.
#[test]
fn the_controller_never_reads_a_secret() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut files = 0usize;
    walk(&src, &mut |path, text| {
        files += 1;
        for needle in ["Api::<Secret>", "\"secrets\""] {
            if text.contains(needle) {
                offenders.push(format!("{}: {needle}", path.display()));
            }
        }
    });
    assert!(files >= 3, "the walk must see src/, got {files} files");
    assert!(
        offenders.is_empty(),
        "weirkeeper must not read Secrets: {offenders:?}"
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

// ---------------------------------------------------------------------------
// The double
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The binary
// ---------------------------------------------------------------------------

/// `weirkeeper --version` exits 0 and prints one line.
///
/// Runs the binary cargo has ALREADY built for this test target, never `cargo
/// run`, which would re-enter cargo while `cargo test --workspace` holds the
/// build lock (the idiom is `crates/logweir/tests/guard_cli.rs:7`). Task 23's
/// `scripts/check-image-weirkeeper.sh` check 2 runs exactly this argv against
/// the built image.
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
