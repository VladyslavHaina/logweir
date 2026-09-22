//! D0 stage 5: the controller watches and acts in the execution namespaces it
//! is given, and nowhere else (`weirkeeper::scope`).
//!
//! THE CLASS THIS GUARDS. A namespaced kind reached through `Api::all` is a
//! cluster-wide watch or list, which a controller bound by per-namespace
//! RoleBindings is refused — so it silently stops reconciling — and which, in a
//! cluster-wide install, reads every namespace. Every such site in this crate
//! now goes through `scope::api` or `scope::list_everywhere`; the source scan
//! below fails when a new one is written directly, and the behavioural rows
//! prove what the two helpers actually ask the API server for.
//!
//! ONE PROCESS-WIDE SCOPE PER BINARY. `scope::init` is a `OnceLock`, like the
//! variable it stands for, so this binary sets the scoped shape once and the
//! cluster-wide default is proved on the pure `WatchScope` value instead.

use std::path::{Path, PathBuf};

use kube::api::ListParams;
use weirkeeper::crds::restore::Restore;
use weirkeeper::scope::{self, WatchScope};
use weirkeeper::testing::{mock_client_recording, Route};

fn src_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            src_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// **No namespaced kind is reached through `Api::all` outside `scope.rs`.**
///
/// Every remaining `Api::all(` in the crate must be typed, on its own line, as
/// one of the two cluster-scoped kinds. An untyped site cannot be checked and
/// fails too.
///
/// REGRESSION REASON. Before D0 stage 5 twenty-six sites built namespaced
/// watches and lists with `Api::all`; under the scoped chart each of them is a
/// reconciler the API server refuses. MUTANT: put
/// `let api: Api<Backup> = Api::all(client.clone());` back in
/// `controllers/backup.rs` and this row names the file and line.
#[test]
fn no_namespaced_kind_is_reached_through_api_all() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    src_files(&root, &mut files);
    let mut offenders = Vec::new();
    let mut cluster_sites = 0;
    for file in files {
        if file.ends_with("scope.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        for (number, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            if !code.contains("Api::all(") && !code.contains("Api::all_with(") {
                continue;
            }
            let cluster_scoped = code.contains("Api<TrustPolicy>")
                || code.contains("Api<TrustRoster>")
                || code.contains("Api<crate::crds::trust_policy::TrustPolicy>");
            if cluster_scoped {
                cluster_sites += 1;
            } else {
                offenders.push(format!(
                    "{}:{}: {}",
                    file.strip_prefix(&root).unwrap().display(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a namespaced kind is reached through Api::all; use weirkeeper::scope::api or \
         scope::list_everywhere so the scoped install can reconcile it:\n{}",
        offenders.join("\n")
    );
    // The cluster-scoped sites still exist: this is not a scan that stopped
    // seeing anything.
    assert!(cluster_sites >= 8, "{cluster_sites}");
}

/// **Every reconciler of a namespaced kind runs through `run_everywhere`.**
#[test]
fn every_namespaced_reconciler_runs_once_per_watched_namespace() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers");
    for name in [
        "approval",
        "backup",
        "backup_destination",
        "backup_schedule",
        "kafka_cluster",
        "preflight",
        "protection_policy",
        "recovery_catalog",
        "rehearsal_schedule",
        "restore",
        "retention_policy",
        "topic_discovery",
    ] {
        let text = std::fs::read_to_string(dir.join(format!("{name}.rs"))).unwrap();
        assert!(
            text.contains("crate::scope::run_everywhere("),
            "controllers/{name}.rs does not start its watch through crate::scope::run_everywhere"
        );
    }
}

/// **The cluster-wide default is one watch over everything, exactly as
/// before**, and a scoped list covers exactly its names.
#[test]
fn the_default_is_one_cluster_wide_watch() {
    assert_eq!(WatchScope::Cluster.watches(), vec![None]);
    let scoped = scope::configured(Ok("team-b,team-a".into())).unwrap();
    assert_eq!(
        scoped.watches(),
        vec![Some("team-a".to_string()), Some("team-b".to_string())]
    );
    assert!(!scoped.covers("logweir-system"));
}

/// **Scoped, a cross-object list reads each watched namespace and never the
/// cluster.** The recording double answers only the two namespaced paths; a
/// cluster-wide `/restores` request would have no route and fail. NEGATIVE
/// CONTROL: the requests are asserted exactly, so a helper that ALSO listed
/// cluster-wide would be seen.
#[tokio::test]
async fn a_scoped_list_asks_for_each_namespace_and_never_the_cluster() {
    scope::init(scope::configured(Ok("team-a,team-b".into())).unwrap());
    let empty = || {
        serde_json::json!({
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RestoreList",
            "metadata": {"resourceVersion": "1"},
            "items": []
        })
        .to_string()
    };
    let (client, seen) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/team-a/restores",
            status: 200,
            body: empty(),
        },
        Route {
            method: "GET",
            path_suffix: "/namespaces/team-b/restores",
            status: 200,
            body: empty(),
        },
    ]);
    let items: Vec<Restore> = scope::list_everywhere(&client, &ListParams::default())
        .await
        .expect("both namespaced lists are answered");
    assert!(items.is_empty());
    let paths: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.uri.split('?').next().unwrap().to_string())
        .collect();
    assert_eq!(
        paths,
        vec![
            "/apis/logweir.dev/v1alpha1/namespaces/team-a/restores".to_string(),
            "/apis/logweir.dev/v1alpha1/namespaces/team-b/restores".to_string(),
        ]
    );
}

/// **Scoped, a reconciler starts once per watched namespace, with that
/// namespace** — never the cluster-wide `None`, never only the first entry.
#[tokio::test]
async fn a_scoped_reconciler_starts_once_per_namespace() {
    scope::init(scope::configured(Ok("team-a,team-b".into())).unwrap());
    let started = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = std::sync::Arc::clone(&started);
    scope::run_everywhere(move |namespace| {
        let log = std::sync::Arc::clone(&log);
        async move {
            log.lock().unwrap().push(namespace);
        }
    })
    .await;
    let mut seen = started.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        vec![Some("team-a".to_string()), Some("team-b".to_string())]
    );
}

/// **No standalone watch stream spins on errors.** `Controller` backs off its
/// own trigger watches; a `reflector(…, watcher(…))` this crate spawns itself
/// must say `.default_backoff()` or a refused LIST — a 403 from a namespace
/// bound without its grant, an API-server outage — becomes a tight retry
/// loop. Found by the PLAT-17.2 live run (~175 retries a second per stream).
#[test]
fn every_standalone_watch_stream_backs_off() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    src_files(&root, &mut files);
    let mut offenders = Vec::new();
    let mut seen = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let mut at = 0;
        while let Some(rel) = text[at..].find("reflector::reflector(") {
            let start = at + rel;
            let window = &text[start..(start + 400).min(text.len())];
            seen += 1;
            if !window.contains(".default_backoff()") {
                offenders.push(format!(
                    "{}:{}",
                    file.strip_prefix(&root).unwrap().display(),
                    text[..start].lines().count() + 1
                ));
            }
            at = start + 1;
        }
    }
    assert!(seen >= 1, "{seen}");
    assert!(
        offenders.is_empty(),
        "watch streams without backoff: {offenders:?}"
    );
}

/// **The cluster-scoped `TrustPolicy` reflector is built once per reconciler,
/// not once per watched namespace** (review L3): no `reflector::reflector(`
/// inside a `controller_in`, and backup and restore take the shared store.
#[test]
fn the_trust_policy_reflector_is_not_per_namespace() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers");
    for name in ["backup", "restore"] {
        let text = std::fs::read_to_string(dir.join(format!("{name}.rs"))).unwrap();
        let body = &text[text.find("fn controller_in(").expect("controller_in")..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            !body.contains("reflector::reflector(") && !body.contains("reflector::store::<"),
            "controllers/{name}.rs builds a TrustPolicy reflector per watched namespace"
        );
        assert!(
            text.contains("crate::trust::spawn_policy_reflector(&client)"),
            "{name}"
        );
    }
}
