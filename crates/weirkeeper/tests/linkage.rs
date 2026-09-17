//! The properties this half of `weirkeeper` exists to establish: the linkage
//! boundary, the manifest, and the double.
//!
//! ONE FILE, BECAUSE THE PLAN NAMES ONE. Every `#[test]` this task lands lives
//! here. They fall into four groups — the G-SIGN linkage half, the three
//! manifest reads, the source-level Secret refusal, and the `mock_client`
//! double — and each group's header says what it proves and what it does not.
//! **Task 16b adds a fifth**: `conditions.rs`'s status-write contract, whose
//! three functions are this crate's other piece of shared machinery and which
//! belongs beside the double for the same reason — six reconcilers depend on
//! it, so its own contract is not any one of their tests' business.
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
/// THE FOURTH ENTRY IS `rustls`, ADDED IN FIX ROUND 1, AND IT IS MEASURED
/// HERE RATHER THAN ARGUED. The binary has to install a process-level
/// `CryptoProvider` (review finding H1), which means naming `rustls` in this
/// manifest; `ring 0.17.14` and `rustls 0.23.43` are both already in
/// `Cargo.lock`, so the claim is the same claim the other three make and it is
/// tested by the same measurement — comment the entry out and the resolved
/// package count must not move.
///
/// THE SET OF ENTRIES IS DISCOVERED, NOT HARD-CODED, AND THAT IS WHAT KILLS
/// THE MUTANT. Written against a literal `["tower", "http",
/// "http-body-util"]`, this test would PASS after someone adds a further
/// manifest entry pulling a package that is not in `Cargo.lock`: that entry
/// would be present in BOTH overlays, so both counts would rise together
/// and equality would still hold. Discovering every plain third-party version
/// requirement — everything under `[dependencies]` that is not `kube`, not
/// `k8s-openapi`, not a `path` dependency and not `workspace = true` — puts
/// that entry in the commented-out set, where it makes the two numbers
/// differ. The set is asserted to be exactly the four AFTER the counts are
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
        vec!["tower", "http", "http-body-util", "rustls"],
        "the plain third-party entries in this manifest are fixed at these four (Global \
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

// ---------------------------------------------------------------------------
// The no-argv startup path — review finding H1
// ---------------------------------------------------------------------------
//
// WHAT WAS BROKEN. `weirkeeper` with no argv aborted at **exit 101** inside
// rustls: "Could not automatically determine the process-level CryptoProvider
// from Rustls crate features". rustls 0.23 auto-selects only when EXACTLY ONE
// of `ring` / `aws-lc-rs` is enabled, and this workspace's unified graph
// enables both — `aws-lc-rs` through `object_store` → `reqwest`, `ring`
// through `ureq 2.12.1` — so two behaved as none. The abort happened INSIDE
// `kube::Client::try_default()`, which is why `main`'s
// `error!("no Kubernetes client…")` branch was unreachable and why eleven
// tests and eight mutants missed it: nothing exercised the path.
//
// WHY THERE ARE TWO TESTS AND NOT ONE. The first drives the startup path
// IN-PROCESS, so the provider install has an assertion of its own and the
// failure is a named expectation rather than a stack trace. The second runs
// the SHIPPED BINARY, because the property the brief states is a property of
// a process — its exit code and its log lines — and an in-process test cannot
// observe `main`'s `ExitCode` or the panic-versus-log distinction at all.
//
// NEITHER DIALS. Building a `kube::Client` constructs a connector; it opens no
// socket, and no `Api` call is ever made. The fixture's apiserver is
// `https://127.0.0.1:1`: privileged, unused, and never contacted — the address
// exists so that a regression which DOES dial fails loudly and instantly
// (connection refused) instead of hanging on a routable host. It is not one of
// `crates/logweir/tests/no_network_in_unit_tests.rs`'s thirteen `DIAL_TOKENS`
// (which name the two dialling constructors, the compose stack's 9092/9000
// endpoints and ureq's agentless builders), so STANDING RULE 18 needs no new
// `ALLOWED` entry — `the_default_suite_dials_nothing` is green unchanged.

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
/// Valid YAML, valid kubeconfig shape, unresolvable context — so
/// `Config::infer()` returns a clean `Err` and `main`'s error branch is
/// REACHED rather than merely written. Finding H1b was that this branch was
/// unreachable; this fixture is what proves it no longer is.
fn unresolvable_kubeconfig() -> String {
    "apiVersion: v1\n\
     kind: Config\n\
     clusters: []\n\
     contexts: []\n\
     current-context: weirkeeper-no-such-context\n\
     users: []\n"
        .to_string()
}

/// A kubeconfig written to a private temp directory, removed on drop.
///
/// The same `std::env::temp_dir()` + `Drop` shape as [`Overlay`], and for the
/// same reason: Global Constraint 38 fixes this crate's manifest additions, so
/// there is no `tempfile` dev-dependency to reach for.
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

/// The startup path, in-process: the provider is installed, and the client
/// builds instead of panicking.
///
/// WHAT THIS PROVES. `weirkeeper::install_default_crypto_provider()` leaves a
/// process-level `CryptoProvider` installed, and with one installed the exact
/// two calls `main` makes — a `Config` off a kubeconfig, then
/// `kube::Client::try_from` — complete cleanly. `Client::try_from` is where the
/// abort used to happen: it builds the rustls HTTPS connector through
/// `ConfigExt`, and `ClientConfig::builder()` is the call that panicked.
///
/// WHAT THIS DOES NOT PROVE. Nothing about reaching an apiserver. Building a
/// client is not a connection, and the measured outcome here is `Ok`, not
/// `Err` — which is precisely why the brief's clause for the no-argv path is
/// "logs one line and exits 0 on SIGTERM" and not "exits non-zero". The clean
/// `Err` half of `main`'s branch is asserted in the second arm below, and the
/// process-level behaviour in the test after this one.
///
/// WHY `install_default_crypto_provider` IS CALLED AND ITS EFFECT ASSERTED
/// SEPARATELY. The assertion is the mutant's landing site: delete the install
/// (or empty the function) and this fails at the `get_default().is_some()`
/// line with a named message, rather than unwinding out of rustls with a
/// stack trace a reader has to interpret.
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

    // THE CALL THAT USED TO ABORT THE PROCESS.
    let client = kube::Client::try_from(cfg).expect(
        "kube::Client::try_from must build a client from the fixture Config — it constructs a \
         connector and opens no socket",
    );
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

/// The shipped binary, with no argv and a `KUBECONFIG`: the brief's clause,
/// end to end.
///
/// Runs the binary cargo has ALREADY built for this test target — never `cargo
/// run`, which would re-enter cargo while `cargo test --workspace` holds the
/// build lock (the idiom is `crates/logweir/tests/guard_cli.rs:7`).
///
/// ARM 1 — the brief's no-argv clause. With a kubeconfig it can build a client
/// from, the binary installs its subscriber, logs ONE line naming the
/// registered controller count, and exits **0** on SIGTERM. Before the fix this
/// arm produced exit **101** and a rustls panic on stderr, so this is the
/// assertion the provider-removal mutant lands on.
///
/// ARM 2 — the error branch, named. With a kubeconfig whose context resolves to
/// nothing, the binary exits **1** having logged `no Kubernetes client`.
///
/// STDERR IS ASSERTED EMPTY IN BOTH ARMS, and that is the point rather than a
/// detail: Global Constraint 11 puts every readable line on STDOUT as JSON
/// (the pod log API has no stream selector), so on this binary a non-empty
/// stderr means a panic or a subscriber that never installed. "Exit code plus
/// the log line" is therefore spelled as "exit code, the line on stdout, and
/// nothing at all on stderr".
///
/// NO SOCKET. Nothing is listening on `127.0.0.1:1`, no `Api` call is made and
/// no reconciler is registered — `controllers` is 0 until Task 16 — so the
/// process builds a client, logs, and waits for a signal.
///
/// OUTPUT GOES TO FILES, NOT PIPES. A long-lived child whose stdout is a pipe
/// nobody drains can block on a full pipe buffer, and the exit code is read
/// from `Child::wait()` directly — never through a pipe (STANDING RULE 20).
#[test]
fn the_no_argv_binary_starts_and_exits_zero_on_sigterm() {
    // ---- ARM 1: the good kubeconfig, SIGTERM, exit 0 -------------------
    let fixture = Fixture::write("proc-good", &good_kubeconfig());
    let out_path = fixture.path().with_file_name("stdout.log");
    let err_path = fixture.path().with_file_name("stderr.log");

    let mut child = Command::new(env!("CARGO_BIN_EXE_weirkeeper"))
        .env("KUBECONFIG", fixture.path())
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
    // connector"; measured, it is milliseconds. STANDING RULE 22 bounds each
    // test at 15 s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
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
        stdout.contains("\"controllers\":13"),
        "the startup line names the registered controller count. It was 0 until Task 16, which \
         registered TWO — `controllers::trust_roster` and `controllers::approval`, in that \
         order — Task 18 registered the THIRD, `controllers::backup_schedule`, Task 17 the \
         FOURTH, `controllers::backup`, Task 20 the FIFTH, `controllers::restore`, Task 15c \
         the SIXTH, `controllers::kafka_cluster`, D2 W7 the SEVENTH, \
         `controllers::backup_destination`, PLAT-19.1 the EIGHTH, \
         `controllers::trust_policy`, D2 W8 the NINTH, \
         `controllers::topic_discovery`, D3 W8 the TENTH, \
         `controllers::recovery_catalog`, D2 W9 the ELEVENTH, \
         `controllers::preflight`, D3 W6 (PLAT-14.2) the TWELFTH, \
         `controllers::protection_policy`, and D3 W7 (PLAT-14.3) the \
         THIRTEENTH, `controllers::rehearsal_schedule` — which D3 §14 numbers \
         FOURTEENTH because it sequences W9's `RetentionPolicy` before it; W9 \
         had not merged when this branch was cut, so this number moves to 14 \
         when it does. A count \
         that is not 13 means `main`'s registration point lost a `controllers.push(…)` line. \
         Got: {stdout:?}"
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

// ===========================================================================
// GROUP 5 — THE STATUS-WRITE CONTRACT (Task 16b, plan erratum E11(d))
// ===========================================================================
//
// `conditions::merge_condition`, `::apply_merge_patch` and `::status_unchanged`
// are the ONE implementation of a rule six reconcilers obey. The reconciler
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

/// `lastTransitionTime` moves on a transition and ONLY on a transition.
///
/// The `metav1.Condition` contract, and the rule whose absence from two
/// reconcilers Task 15c measured at 12,107 and 7,114 reconciles per 90 s.
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
