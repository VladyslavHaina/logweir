//! The controller watches the install's one namespace (PROD-19.1), and every
//! namespaced reconcile passes the workspace gate first.
//!
//! THE CLASS THIS GUARDS. A namespaced kind reached through `Api::all` is a
//! cluster-wide watch or list, which the controller's namespaced Role refuses
//! (so it silently stops reconciling). The source scan below fails when one is
//! written.

use std::path::{Path, PathBuf};

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

/// **No namespaced kind is reached through `Api::all`.**
///
/// Every remaining `Api::all(` in the crate must be typed, on its own line, as
/// one of the two cluster-scoped kinds. An untyped site cannot be checked and
/// fails too.
///
/// MUTANT: put `let api: Api<Backup> = Api::all(client.clone());` back in
/// `controllers/backup.rs` and this row names the file and line.
#[test]
fn no_namespaced_kind_is_reached_through_api_all() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    src_files(&root, &mut files);
    let mut offenders = Vec::new();
    let mut cluster_sites = 0;
    for file in files {
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
        "a namespaced kind is reached through Api::all; use \
         Api::namespaced(client, crate::workspace::namespace()):\n{}",
        offenders.join("\n")
    );
    // The cluster-scoped sites still exist: this is not a scan that stopped
    // seeing anything.
    assert!(cluster_sites >= 8, "{cluster_sites}");
}

/// **Every reconciler of a namespaced kind watches the install's namespace
/// and gates each object on its workspace** (`crate::workspace::gate`).
///
/// MUTANT: drop the `gate` call from any controller and this row names it.
#[test]
fn every_namespaced_reconciler_watches_one_namespace_and_gates_workspaces() {
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
        "restore",
        "retention_policy",
        "switchover",
        "topic_discovery",
    ] {
        let text = std::fs::read_to_string(dir.join(format!("{name}.rs"))).unwrap();
        assert!(
            text.contains("Api::namespaced(client.clone(), crate::workspace::namespace())"),
            "controllers/{name}.rs does not watch the install's namespace"
        );
        assert!(
            text.contains("if let Some(action) = crate::workspace::gate(&ctx."),
            "controllers/{name}.rs does not gate its objects on their workspace"
        );
    }
}

/// **No standalone watch stream spins on errors.** `Controller` backs off its
/// own trigger watches; a `reflector(…, watcher(…))` this crate spawns itself
/// must say `.default_backoff()` or a refused LIST (a 403 from a namespace
/// bound without its grant, an API-server outage) becomes a tight retry loop,
/// measured live at ~175 retries a second per stream.
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

/// **The cluster-scoped `TrustPolicy` reflector is built once per reconciler**: no `reflector::reflector(` inside a
/// `controller_in`, and backup and restore take the shared store.
#[test]
fn the_trust_policy_reflector_is_shared() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers");
    for name in ["backup", "restore"] {
        let text = std::fs::read_to_string(dir.join(format!("{name}.rs"))).unwrap();
        let body = &text[text.find("fn controller_in(").expect("controller_in")..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            !body.contains("reflector::reflector(") && !body.contains("reflector::store::<"),
            "controllers/{name}.rs builds a TrustPolicy reflector inside its watch"
        );
        assert!(
            text.contains("crate::trust::spawn_policy_reflector(&client)"),
            "{name}"
        );
    }
}
