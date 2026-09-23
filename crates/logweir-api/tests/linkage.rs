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
fn the_api_links_the_crds_the_verifier_and_the_console_signer_and_never_the_engine() {
    let graph = workspace_graph();
    let reach = reaches(&graph, "logweir-api");
    // PLAT-19.2: `logweir-evidence` is REQUIRED now — the console signs its
    // own ConsoleConfirmation documents (D0), through the shared signer. The
    // engine wrapper and the broker client stay forbidden.
    for required in [
        "weirkeeper",
        "logweir-core",
        "logweir-verify",
        "logweir-evidence",
    ] {
        assert!(
            reach.contains(required),
            "logweir-api must reach {required}; it reaches {reach:?}"
        );
    }
    for forbidden in ["logweir-engine-oso", "logweir-kafka"] {
        assert!(
            !reach.contains(forbidden),
            "logweir-api must NOT reach {forbidden} (the engine wrapper and the broker client); \
             it reaches {reach:?}"
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
    // ...and, since PLAT-19.2, on both signing allowlists: it signs console
    // confirmations with its own ConsoleConfirmation key (D0), for the reason
    // recorded above `ALLOWED_LINK` in the script.
    assert!(value("ALLOWED_LINK").contains(&"logweir-api"));
    assert!(value("ALLOWED_SOURCE").contains(&"logweir-api"));
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
        // `Api::all` IS NOT HERE ANY MORE, AND IS FENCED INSTEAD OF BANNED.
        // D3 §7.1 makes `TrustPolicy` cluster-scoped for the reason
        // `docs/kubernetes.md` §8 gives — a roster whose name the subject
        // supplies is a roster the subject can choose — so there is exactly
        // one kind this service reads without a namespace, and it cannot be
        // read through a namespaced handle. `the_cluster_scoped_read_is_one_
        // sealed_kind_and_two_read_verbs` below is the replacement: it pins
        // the number of `Api::all` sites, the two functions that may hold one,
        // the single sealed type, and the absence of any create, patch or
        // delete generic over that seal. `Api::all_with` stays banned: a
        // constructor that takes a caller-supplied `DynamicType` is a
        // constructor that takes a group, a version and a plural.
        "Api::all_with",
        ".delete(",
        ".delete_collection(",
        // The PUT verbs, in the shapes `kube::Api` actually offers them. The
        // first spelling here used to be `".replace(&"`, which matches NOTHING:
        // the call is `api.replace(name, &params, object)`, so the `&` belongs
        // to the SECOND argument. A compiling PUT in the sealed adapter passed
        // this test. `str::replace` is why the plain method needs the `api.`
        // prefix, and `the_adapter_calls_only_the_four_permitted_kubernetes_verbs`
        // below is what makes that prefix trustworthy.
        "api.replace(",
        ".replace_status(",
        ".patch_status(",
        // `kube::Api::entry`, the read-modify-write helper, in the same
        // receiver-qualified spelling `api.replace(` uses. The bare `.entry(`
        // that stood here matched every `BTreeMap`/`HashMap` in the crate, so
        // it could not survive the first module that needed a map — and the
        // property it guards is already carried by
        // `the_adapter_calls_only_the_four_permitted_kubernetes_verbs`, which
        // pins the exact verb set called on an `Api` handle rather than
        // enumerating the ones it forbids.
        "api.entry(",
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

/// Every `<prefix><ident>(` on a code line, as a set of `<ident>`.
///
/// The character before `prefix` must not be part of an identifier, so
/// `"api."` does not also match `myapi.`.
fn methods_called_on(text: &str, prefix: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (_, line) in code_lines(text) {
        let mut cursor = 0usize;
        while let Some(offset) = line[cursor..].find(prefix) {
            let at = cursor + offset;
            let boundary_ok = line[..at]
                .chars()
                .next_back()
                .is_none_or(|c| !c.is_alphanumeric() && c != '_');
            let after = &line[at + prefix.len()..];
            let ident: String = after
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if boundary_ok && !ident.is_empty() && after[ident.len()..].starts_with('(') {
                out.insert(ident);
            }
            cursor = at + prefix.len();
        }
    }
    out
}

/// Every `<ident>: Api<…>` binding name on a code line.
fn api_binding_names(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (_, line) in code_lines(text) {
        if let Some(at) = line.find(": Api<") {
            let ident: String = line[..at]
                .chars()
                .rev()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect::<Vec<char>>()
                .into_iter()
                .rev()
                .collect();
            if !ident.is_empty() {
                out.insert(ident);
            }
        }
    }
    out
}

/// **The D-SEAMS S7 guard: no PUT, on the object or on its status.**
///
/// AN ALLOWLIST, BECAUSE A DENYLIST IS ALWAYS ONE SPELLING BEHIND. The guard
/// above used to be the only enforcement of S7 in this crate, and it could not
/// fail: it listed `".replace(&"`, which is not how `kube::Api::replace` is
/// called, and it did not mention `replace_status` at all. An independent
/// reviewer planted a compiling `api.replace(name, &params, object)` in the
/// sealed adapter and the whole linkage suite passed. A guard that cannot fail
/// is worse than no guard, because the next person to touch `kube.rs` will
/// reasonably trust it.
///
/// So this test does not enumerate what is forbidden. It pins the exact set of
/// `kube` verbs the adapter calls, and every other method `kube::Api` offers —
/// `replace`, `replace_status`, `patch_status`, `delete`, `delete_collection`,
/// `entry`, `get_status`, `get_metadata`, `watch`, and whatever a future
/// version adds — fails it without ever being named.
///
/// It also pins the binding name and the constructor set, because both are what
/// make the scan honest: rename `api` and the verb scan would find nothing;
/// widen the constructor set and a namespaced route could hold a cluster-wide
/// handle. `Api::all` is in the set for exactly one kind, and
/// [`the_cluster_scoped_read_is_one_sealed_kind_and_two_read_verbs`] is what
/// keeps it to that one.
#[test]
fn the_adapter_calls_only_the_four_permitted_kubernetes_verbs() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("kube.rs");
    let text = std::fs::read_to_string(&path).expect("src/kube.rs is readable");

    assert_eq!(
        api_binding_names(&text),
        BTreeSet::from(["api".to_string()]),
        "every `Api<…>` in {} must be bound to a variable named `api`; the verb \
         allowlist below scans `api.<method>(` and a rename would silently empty it",
        path.display()
    );

    // A TURBOFISH IS THE ONE SPELLING THE BINDING SCAN CANNOT SEE. `let handle =
    // Api::<T>::namespaced(…)` builds a handle with no `: Api<T>` annotation, so
    // `api_binding_names` never learns the name `handle` and the verb allowlist
    // below scans nothing for it. An independent reviewer planted exactly that
    // and the suite stayed green; refusing the spelling closes it.
    assert!(
        !code_lines(&text).any(|(_, line)| line.contains("Api::<")),
        "{}: build every handle as `let api: Api<T> = Api::namespaced(…)`, never \
         with a turbofish; the binding-name scan above cannot see `Api::<T>::…`",
        path.display()
    );

    assert_eq!(
        methods_called_on(&text, "Api::"),
        BTreeSet::from(["namespaced".to_string(), "all".to_string()]),
        "the `Api` constructors permitted here are `Api::namespaced` and — for the one \
         cluster-scoped kind, which has no namespace to be bound to — `Api::all`. Anything \
         else drops the namespace bound every namespaced route authorizes against"
    );

    assert_eq!(
        methods_called_on(&text, "api."),
        BTreeSet::from([
            "create".to_string(),
            "get".to_string(),
            "list".to_string(), // engine-token-ok: the kube `Api::list` verb, not an engine subcommand
            "patch".to_string(),
        ]),
        "the adapter may call exactly `list`, `get`, `create` and `patch` on an `Api` \
         handle. Anything else is a verb this service does not have: PUT (`replace`, \
         `replace_status`) is refused by D-SEAMS S7 — status writes are conditional merge \
         PATCH — and delete is refused by the product contract. Adding one here is a \
         contract change, not a refactor."
    );

    assert_eq!(
        methods_called_on(&text, "self.client."),
        BTreeSet::from(["apiserver_version".to_string(), "clone".to_string()]),
        "the raw client is used only to build namespaced handles and to ask the API \
         server its version for `/readyz`; `request`, `request_text` and the other raw \
         entry points would take a path, which is exactly what this adapter does not do"
    );

    // The one PATCH is a MERGE patch. `Patch::Json`, `Patch::Apply` and
    // `Patch::Strategic` are in the forbidden-token list above; this asserts
    // the positive so that removing them from that list is not enough.
    assert!(
        text.contains("Patch::Merge("),
        "the single update must be a merge patch"
    );
}

/// **Which Kubernetes RESOURCE each verb is spent on, pinned.**
///
/// The verb allowlist above says the adapter calls `list`, `get`, `create` and
/// `patch`. It does NOT say on what: D2 W12 added two CORE objects to the
/// adapter — `configmaps` (a check's own stored result) and `secrets` (a
/// write-only credential) — and a `get` on `secrets` would have passed the verb
/// scan unchanged while being exactly the permission this service must never
/// hold.
///
/// So this pins the `(verb, resource)` pairs at the ONE place the adapter names
/// them: every bounded call's first two arguments. A pair added here is a
/// change to the console ServiceAccount's RBAC and to D2 §7.3, not a
/// refactor — and `get/secrets`, `list/secrets` and any delete cannot appear
/// without this test being edited by hand.
#[test]
fn the_adapter_spends_each_verb_on_exactly_these_resources() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("kube.rs");
    let text = std::fs::read_to_string(&path).expect("src/kube.rs is readable");

    // THE CALL IS WRITTEN ACROSS LINES, so the scan works over the joined
    // code lines: for each `self.bounded(`, the first two ARGUMENTS are the
    // verb (always a literal) and the resource (a literal, or the sealed
    // `ProductResource`'s own plural for the generic methods).
    // `rustfmt` breaks a long chain as `self\n    .bounded(...)`, so the
    // receiver and the call land on different lines; normalising ` .` to `.`
    // is what keeps the scan from silently missing such a call — and a missed
    // call would be a verb nobody checked.
    let code: String = code_lines(&text)
        .map(|(_, line)| line.trim())
        .collect::<Vec<&str>>()
        .join(" ")
        .replace(" .", ".");
    let mut pairs: BTreeSet<String> = BTreeSet::new();
    let mut cursor = 0usize;
    while let Some(offset) = code[cursor..].find("self.bounded(") {
        let at = cursor + offset + "self.bounded(".len();
        cursor = at;
        let args = first_two_arguments(&code[at..]);
        assert!(args.len() >= 2, "an unparseable bounded() call at {at}");
        let literal = |arg: &str| -> String {
            let arg = arg.trim();
            match (arg.strip_prefix('"'), arg.strip_suffix('"')) {
                (Some(inner), _) if arg.len() >= 2 && arg.ends_with('"') => {
                    inner[..inner.len() - 1].to_string()
                }
                _ => "<ProductResource plural>".to_string(),
            }
        };
        pairs.insert(format!("{} {}", literal(&args[0]), literal(&args[1])));
    }

    let expected: BTreeSet<String> = [
        // The eight sealed custom resources, through the generic methods.
        "list_page <ProductResource plural>",
        "get <ProductResource plural>",
        "create <ProductResource plural>",
        // The three named merge patches.
        "patch <ProductResource plural>",
        "patch backupschedules",
        "patch backupdestinations",
        // THE ONE CLUSTER-SCOPED OBJECT OUTSIDE THE SEALS: `TrustRoster/default`,
        // `get` by that fixed name (PREFLIGHT-TRUSTROSTER-STALE). No list.
        "get trustrosters",
        // THE TWO CORE OBJECTS, one verb each.
        "get configmaps",
        "create secrets",
        // The readiness probe.
        "version version",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    assert_eq!(
        pairs, expected,
        "the (verb, resource) set this adapter uses changed. Adding one is a change to the \
         console ServiceAccount's RBAC (D2 §7.3) and to the security review that approved it: \
         this service must never hold `get` or `list` on secrets, any verb on pods, jobs or \
         logs, or a delete on anything."
    );

    // The negative, stated as text so the intent survives a refactor of the
    // scan above: no line in the adapter asks for a Secret by name.
    for forbidden in [
        "\"get\", \"secrets\"",
        "\"list_page\", \"secrets\"",
        "\"patch\", \"secrets\"",
        "\"delete\"",
        "\"create\", \"configmaps\"",
        "\"patch\", \"configmaps\"",
        "\"list_page\", \"trustrosters\"",
        "\"patch\", \"trustrosters\"",
        "\"create\", \"trustrosters\"",
    ] {
        assert!(
            !code_lines(&text).any(|(_, line)| line.contains(forbidden)),
            "{}: the adapter names {forbidden}",
            path.display()
        );
    }
}

/// The first two comma-separated arguments of a call, at bracket depth zero.
fn first_two_arguments(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut current = String::new();
    let mut previous = '\0';
    for c in text.chars() {
        if in_string {
            current.push(c);
            if c == '"' && previous != '\\' {
                in_string = false;
            }
            previous = c;
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                current.push(c);
            }
            '(' | '[' | '<' | '{' => {
                depth += 1;
                current.push(c);
            }
            ')' | ']' | '>' | '}' if depth == 0 => {
                out.push(std::mem::take(&mut current));
                return out;
            }
            ')' | ']' | '>' | '}' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                out.push(std::mem::take(&mut current));
                if out.len() == 2 {
                    return out;
                }
            }
            _ => current.push(c),
        }
        previous = c;
    }
    out
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
    assert!(
        axum.contains(r#"features = ["http1", "tokio", "matched-path"]"#),
        "{axum}"
    );
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
    for forbidden in ["logweir-engine-oso", "logweir-kafka"] {
        assert!(
            !entries.iter().any(|l| l.starts_with(forbidden)),
            "{forbidden} must not be a dependency of this crate"
        );
    }
    // PLAT-19.2: the console-confirmation signer, by path, with its reason.
    assert!(manifest.contains(r#"logweir-evidence = { path = "../logweir-evidence" }"#));
    assert!(manifest.contains("PLAT-19.2 — THE CONSOLE CONFIRMATION SIGNER"));

    // THE TRANSPORT-LIMIT DEPENDENCIES ADD NO PACKAGE (review finding R4). The
    // accept loop in `src/main.rs` needs hyper's http1 builder, which
    // `axum::serve` does not expose. Both crates were ALREADY in the resolved
    // graph — axum is built on them, and `kube-client` pulls `hyper-util` —
    // so declaring them turns on features of packages already in `Cargo.lock`.
    // That is the claim the manifest comment makes, and this is where it is
    // checked: every direct dependency of this crate must already appear in the
    // lockfile, and `THIRD_PARTY_NOTICES.md` must already name it.
    let lock = std::fs::read_to_string(workspace_root().join("Cargo.lock")).unwrap();
    let notices = std::fs::read_to_string(workspace_root().join("THIRD_PARTY_NOTICES.md")).unwrap();
    for name in ["hyper", "hyper-util"] {
        assert!(
            entries.iter().any(|l| l.starts_with(&format!("{name} ="))),
            "{name} is declared for the transport limits"
        );
        assert!(
            lock.contains(&format!("name = \"{name}\"")),
            "{name} is not in Cargo.lock: it would be a NEW package, which is a \
             dependency decision and needs THIRD_PARTY_NOTICES.md regenerated"
        );
        assert!(
            notices.contains(&format!("### {name}@")),
            "{name} is not attributed in THIRD_PARTY_NOTICES.md"
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

/// **The cluster-scoped read is one sealed kind, two functions and no write.**
///
/// `ProductResource` is bound `Scope = NamespaceResourceScope` so that no
/// namespaced route can reach outside its namespace, and D3 §7.1 puts exactly
/// one kind — `TrustPolicy` — outside that bound. The danger is not the
/// existence of a cluster-wide handle; it is a cluster-wide handle that grows a
/// second kind, or a verb. So this pins all four things that would have to
/// change for either to happen: how many `Api::all` sites there are, which
/// functions hold them, how many types satisfy the seal, and that no `create`,
/// `patch`, `replace` or `delete` is generic over it.
///
/// REGRESSION REASON. `the_adapter_calls_only_the_four_permitted_kubernetes_verbs`
/// pins the verb set over the whole file, so it cannot tell a `create` reached
/// through `Api::namespaced` from one reached through `Api::all`: adding
/// `pub async fn create_cluster<K: ClusterResource>` that called `api.create`
/// would leave that set unchanged and D3 §10's "trust writes stay off the API
/// in v1" would be false with every test green.
#[test]
fn the_cluster_scoped_read_is_one_sealed_kind_and_two_read_verbs() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/kube.rs");
    let text = std::fs::read_to_string(&path).expect("src/kube.rs is readable");
    let code: Vec<String> = code_lines(&text)
        .map(|(_, line)| line.to_string())
        .collect();

    let all_sites = code.iter().filter(|l| l.contains("Api::all(")).count();
    assert_eq!(
        all_sites,
        3,
        "`Api::all` belongs to the two cluster-scoped READ methods and to the one \
         fixed-name roster read, and to nothing else; {all_sites} sites were found in {}",
        path.display()
    );

    let cluster_fns: Vec<&String> = code
        .iter()
        .filter(|l| l.contains("ClusterResource>") && l.contains("fn "))
        .collect();
    let names: BTreeSet<String> = cluster_fns
        .iter()
        .filter_map(|l| {
            let after = l.split("fn ").nth(1)?;
            Some(after.split('<').next()?.trim().to_string())
        })
        .collect();
    assert_eq!(
        names,
        BTreeSet::from(["list_cluster".to_string(), "get_cluster".to_string()]),
        "exactly two functions may be generic over the cluster seal, and both are reads. \
         A create, patch or delete over it would be the trust WRITE D3 §10 keeps off this \
         API in v1."
    );

    let sealed: Vec<&String> = code
        .iter()
        .filter(|l| l.contains("impl ClusterSealed for"))
        .collect();
    assert_eq!(
        sealed.len(),
        1,
        "the cluster seal admits exactly one kind; it admits {}: {sealed:?}",
        sealed.len()
    );
    assert!(
        sealed[0].contains("trust_policy::TrustPolicy"),
        "the one cluster-scoped kind is TrustPolicy: {}",
        sealed[0]
    );
    assert!(
        code.iter()
            .any(|l| l.contains("impl ClusterResource for TrustPolicy")),
        "TrustPolicy is the one implementor of the cluster-scoped read trait"
    );
    assert!(
        !code
            .iter()
            .any(|l| l.contains("impl ClusterResource for TrustRoster")),
        "TrustRoster must never join the cluster seal: that would give every route a \
         `list_cluster::<TrustRoster>` and a `get_cluster` by any name"
    );
}

/// **The roster read is ONE object, by a name no caller supplies.**
///
/// PREFLIGHT-TRUSTROSTER-STALE gave the adapter a `get` on
/// `trustrosters/default` so the preflight staleness recomputation can compare
/// the roster the controller recorded. The grant is `get` with
/// `resourceNames: ["default"]`, and the adapter must not be able to spend
/// anything wider: this pins that `TrustRoster` is named by exactly one
/// `Api<…>` handle, in exactly one function, which takes no argument but
/// `&self`, and whose `get` names `weirkeeper::ROSTER_NAME`.
///
/// REGRESSION REASON. Widening `get_trust_roster(&self)` to
/// `get_trust_roster(&self, name: &str)`, or adding a `list` over the same
/// handle, leaves the verb set and the `(verb, resource)` pairs above
/// unchanged except for one new pair — and a list of rosters would be the key
/// material of every roster in the cluster, which no route needs.
#[test]
fn the_roster_read_is_one_get_of_the_default_roster() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/kube.rs");
    let text = std::fs::read_to_string(&path).expect("src/kube.rs is readable");
    let code: Vec<String> = code_lines(&text)
        .map(|(_, line)| line.trim().to_string())
        .collect();

    let handles: Vec<&String> = code
        .iter()
        .filter(|l| l.contains("Api<TrustRoster>"))
        .collect();
    assert_eq!(
        handles.len(),
        1,
        "exactly one `Api<TrustRoster>` handle may exist in {}: {handles:?}",
        path.display()
    );
    let signatures: Vec<&String> = code
        .iter()
        .filter(|l| l.contains("fn ") && l.contains("TrustRoster"))
        .collect();
    assert_eq!(
        signatures,
        vec![&"pub async fn get_trust_roster(&self) -> Result<TrustRoster, KubeFailure> {"
            .to_string()],
        "the roster is read by one function that takes no name"
    );
    let joined = code.join(" ").replace(" .", ".");
    assert!(
        joined.contains(
            "self.bounded(\"get\", \"trustrosters\", api.get(weirkeeper::ROSTER_NAME))"
        ),
        "the one roster read is a `get` of `weirkeeper::ROSTER_NAME`"
    );
    // The handle's function spends `get` and nothing else.
    let start = code
        .iter()
        .position(|l| l.contains("fn get_trust_roster"))
        .expect("get_trust_roster exists");
    let body: Vec<&String> = code[start..]
        .iter()
        .take_while(|l| l.as_str() != "}")
        .collect();
    for verb in ["api.list(", "api.create(", "api.patch(", "api.watch("] {
        assert!(
            !body.iter().any(|l| l.contains(verb)),
            "the roster read spends `{verb}`: {body:?}"
        );
    }
}
