//! The manifest-lint gate: every shipped Kubernetes manifest, parsed.
//!
//! # The selection rule, fixed here and inherited by Tasks 22 and 23
//!
//! Every file under `config/**` and `examples/**`, plus `logweir.yaml`, is
//! parsed with [`serde_yaml::Deserializer::from_str`] as a MULTI-DOCUMENT
//! stream, and a document is treated as a Kubernetes manifest when it carries
//! **both `apiVersion` and `kind`** — never by filename and never by extension.
//!
//! That is not fastidiousness. `examples/` holds `allowed-clusters.json` and
//! `approval.json` (JSON, which is YAML, and neither is a manifest),
//! `drill.yaml` (a drill SPEC, no `apiVersion`), and `backup.yaml` /
//! `restore.yaml` (Logweir specs, likewise). A `*.yaml` glob that asserted "every
//! file names an image digest" would fail on three of those and would have to
//! grow a filename special case per exception — at which point the gate is a
//! list of names rather than a property.
//! [`manifest_lint_selects_by_parsed_api_version_and_kind`] asserts the
//! exceptions are skipped BY THE PARSE.
//!
//! # Why all of this is in-process
//!
//! Global Constraint 22: every test here parses checked-in bytes, shells
//! nothing, dials nothing, and finishes in milliseconds. Two acceptance lines
//! for this task read as though they were tests —
//! `bash scripts/render-install.sh --check` and `just check-secrets <ns>` —
//! and both shell out, one to `kubectl kustomize` and one to `just`. They are
//! `just` recipes and recorded transcript steps. The PROPERTIES they check are
//! tested here without a subprocess: [`install_yaml_has_no_drift`] compares the
//! rendered file against the source manifests document by document, and
//! [`check_secrets_refuses_a_missing_secret`] parses the recipe.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_yaml::Value;

// ---------------------------------------------------------------------------
// Locating the tree
// ---------------------------------------------------------------------------

/// `logweir/` — the crate root's grandparent, which is Global Constraint 36's
/// CWD for every acceptance line in this plan.
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

/// Every regular file under `rel`, sorted, recursing.
fn files_under(rel: &str) -> Vec<PathBuf> {
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
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// The selection rule
// ---------------------------------------------------------------------------

/// One parsed document that carries both `apiVersion` and `kind`.
#[derive(Debug, Clone)]
struct Manifest {
    /// Path relative to `logweir/`, for failure messages.
    origin: String,
    /// Index of this document within its file.
    index: usize,
    api_version: String,
    kind: String,
    value: Value,
}

impl Manifest {
    fn name(&self) -> String {
        self.value["metadata"]["name"]
            .as_str()
            .unwrap_or("<unnamed>")
            .to_string()
    }

    fn namespace(&self) -> Option<String> {
        self.value["metadata"]["namespace"]
            .as_str()
            .map(str::to_string)
    }

    fn id(&self) -> String {
        format!("{}/{}", self.kind, self.name())
    }
}

/// Parse one file's multi-document stream and keep the documents that are
/// manifests.
///
/// A file that is not YAML at all yields NOTHING rather than a panic: the walk
/// covers whole directories, and a future `.png` under `examples/` must not
/// turn this gate red. A file that IS YAML and parses is judged only by whether
/// its documents carry `apiVersion` and `kind`.
fn manifests_in(path: &Path) -> Vec<Manifest> {
    let rel = path
        .strip_prefix(repo())
        .unwrap_or(path)
        .to_string_lossy()
        .to_string();
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (index, de) in serde_yaml::Deserializer::from_str(&text).enumerate() {
        let Ok(value) = Value::deserialize(de) else {
            // A document that does not parse is not a manifest. It is also the
            // one case worth naming, because a broken shipped manifest is a
            // real defect — `every_shipped_yaml_file_parses` below is what
            // fails on it, with the path.
            continue;
        };
        let (Some(api_version), Some(kind)) = (
            value.get("apiVersion").and_then(Value::as_str),
            value.get("kind").and_then(Value::as_str),
        ) else {
            continue;
        };
        out.push(Manifest {
            origin: rel.clone(),
            index,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            value: value.clone(),
        });
    }
    out
}

use serde::Deserialize;

/// Every manifest under the three shipped locations.
fn all_manifests() -> Vec<Manifest> {
    let mut out = Vec::new();
    for dir in ["config", "examples"] {
        for f in files_under(dir) {
            out.extend(manifests_in(&f));
        }
    }
    out.extend(manifests_in(&repo().join("logweir.yaml")));
    out
}

fn install_file() -> Vec<Manifest> {
    manifests_in(&repo().join("logweir.yaml"))
}

/// One `ClusterRole`'s rules, as `(apiGroups, resources, verbs)` triples with
/// each list sorted so a comparison is about content and not about order.
fn rules_of(rel: &str, role_name: &str) -> Vec<(Vec<String>, Vec<String>, Vec<String>)> {
    let docs = manifests_in(&repo().join(rel));
    let role = docs
        .iter()
        .find(|m| m.kind == "ClusterRole" && m.name() == role_name)
        .unwrap_or_else(|| panic!("{rel} carries no ClusterRole named {role_name}"));
    let rules = role.value["rules"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{rel}: {role_name} has no `rules` sequence"));
    rules
        .iter()
        .map(|r| {
            let take = |k: &str| -> Vec<String> {
                let mut v: Vec<String> = r[k]
                    .as_sequence()
                    .unwrap_or_else(|| panic!("{rel}: {role_name} has a rule with no `{k}`"))
                    .iter()
                    .map(|s| {
                        s.as_str()
                            .unwrap_or_else(|| panic!("{rel}: {role_name} `{k}` is not a string"))
                            .to_string()
                    })
                    .collect();
                v.sort();
                v
            };
            // `resourceNames` is the fourth and last field an RBAC rule may
            // carry, and no rule in this install uses it; a rule that grew one
            // would be invisible to this comparison, so it is asserted absent.
            assert!(
                r.get("resourceNames").is_none(),
                "{rel}: {role_name} has a rule with `resourceNames`; the four-role table in \
                 `the_four_cluster_roles_are_exactly_as_specified` does not model it"
            );
            (take("apiGroups"), take("resources"), take("verbs"))
        })
        .collect()
}

const SIX_KINDS: [&str; 6] = [
    "approvals",
    "backups",
    "backupschedules",
    "kafkaclusters",
    "restores",
    "trustrosters",
];

fn v(items: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = items.iter().map(|s| s.to_string()).collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// The Namespace, and the order it appears in
// ---------------------------------------------------------------------------

/// `logweir.yaml` creates the namespace it installs into, and creates it FIRST.
///
/// Critique B **H12**, interface **I27**. Without the Namespace document the
/// first apply on a clean cluster fails with `namespaces "logweir-system" not
/// found` and X-APPLY is unsatisfiable — which is the whole "a stranger applies
/// one file" claim of spec §1. Without the ORDER, the same thing happens: the
/// documents are applied in file order and a ServiceAccount cannot precede its
/// namespace.
#[test]
fn logweir_yaml_creates_its_own_namespace() {
    let docs = install_file();
    let namespaces: Vec<&Manifest> = docs.iter().filter(|m| m.kind == "Namespace").collect();
    assert_eq!(
        namespaces.len(),
        1,
        "logweir.yaml must carry EXACTLY ONE `kind: Namespace` document; found {}: {:?}",
        namespaces.len(),
        namespaces.iter().map(|m| m.name()).collect::<Vec<_>>()
    );
    assert_eq!(
        namespaces[0].name(),
        "logweir-system",
        "the namespace logweir.yaml creates must be `logweir-system` — \
         `config/rbac/role_binding.yaml`'s subject names that literal"
    );

    let ns_at = docs
        .iter()
        .position(|m| m.kind == "Namespace")
        .expect("just asserted there is one");
    for (i, m) in docs.iter().enumerate() {
        if m.namespace().is_some() {
            assert!(
                i > ns_at,
                "namespaced document {} (index {i}) precedes the Namespace (index {ns_at}) in \
                 logweir.yaml; `kubectl apply -f` applies documents in file order, so this one \
                 fails on a clean cluster",
                m.id()
            );
        }
    }
}

/// No document in `logweir.yaml` is a Logweir custom resource.
///
/// The CRD-not-yet-established ordering failure is avoided BY CONSTRUCTION and
/// not by a `kubectl wait`: a `KafkaCluster` in the same file as the CRD that
/// defines it races the establishment on the first apply and races it
/// differently on the second — and the second apply is the one X-APPLY reads.
#[test]
fn logweir_yaml_contains_no_custom_resource() {
    for m in install_file() {
        assert_ne!(
            m.api_version,
            "logweir.dev/v1alpha1",
            "logweir.yaml carries a custom resource ({} at document {}); samples are separate \
             files, applied second",
            m.id(),
            m.index
        );
    }
}

// ---------------------------------------------------------------------------
// The `weirkeeper` ClusterRole — interface I28
// ---------------------------------------------------------------------------

/// `pods/log` is named in exactly one rule, with `get` and nothing else, and no
/// rule anywhere names `pods/exec` or `pods/attach`.
///
/// Critique B **H8**, spec §9 amendment 4a, claim **C96**. `pods/log` is a
/// SUBRESOURCE: `get` on `pods` does not grant it, and without an explicit rule
/// the API server answers 403 for every
/// `GET /api/v1/namespaces/<ns>/pods/<p>/log`. The consequence is not a crash —
/// it is every `status.evidence.*` key on `Backup` and `Restore` staying
/// unpopulated, Task 24's verification having nothing to fetch, and a 403 that
/// looks like a transient API error.
#[test]
fn weirkeeper_can_read_pod_logs_and_nothing_more() {
    let rules = rules_of("config/rbac/role.yaml", "weirkeeper");

    let log_rules: Vec<_> = rules
        .iter()
        .filter(|(_, res, _)| res.iter().any(|r| r == "pods/log"))
        .collect();
    assert_eq!(
        log_rules.len(),
        1,
        "the weirkeeper ClusterRole must name `pods/log` in EXACTLY ONE rule; found {}",
        log_rules.len()
    );
    let (groups, resources, verbs) = log_rules[0];
    assert_eq!(groups, &v(&[""]), "`pods/log` is in the core API group");
    assert_eq!(
        resources,
        &v(&["pods/log"]),
        "the `pods/log` rule names that subresource and nothing else, so its verb list cannot \
         quietly widen another resource"
    );
    assert_eq!(
        verbs,
        &v(&["get"]),
        "`pods/log` carries `get` and nothing else — nothing is written and no process is started"
    );
    assert!(
        !verbs.iter().any(|x| x == "create"),
        "`create` on `pods/log` is how a process is STARTED in a pod, not how a log is read"
    );

    for (_, resources, _) in &rules {
        for forbidden in ["pods/exec", "pods/attach"] {
            assert!(
                !resources.iter().any(|r| r == forbidden),
                "the weirkeeper ClusterRole names `{forbidden}`; that lets the controller run a \
                 process inside the pod holding the signing key, which is strictly more than \
                 reading that pod's stdout"
            );
        }
    }
}

/// No rule in the `weirkeeper` ClusterRole names `secrets`, under any API
/// group, at any verb.
///
/// Spec §9, `design-operator.md:278-281`. The runner's signing key, approval
/// bundle and SCRAM credential reach its pod because the KUBELET projects them
/// from references this controller writes into a PodSpec — writing a reference
/// is not reading a value.
///
/// THIS BOUNDS READS AND NOT CAPABILITY (Global Constraint 27, O1/O0 default
/// (a)): Job create in a namespace holding the signing key is equivalent to
/// holding the key, because this controller can create a pod that mounts it.
/// The narrow statement is the one asserted here.
#[test]
fn weirkeeper_has_no_verb_on_secrets() {
    for (groups, resources, verbs) in rules_of("config/rbac/role.yaml", "weirkeeper") {
        for r in &resources {
            assert!(
                r != "secrets" && !r.starts_with("secrets/"),
                "the weirkeeper ClusterRole grants {verbs:?} on `{r}` (apiGroups {groups:?}); \
                 spec §9 gives it no verb on Secrets anywhere"
            );
        }
    }
}

/// Every verb granted in `config/rbac/role.yaml` is named by at least one
/// `Api::` call under `crates/weirkeeper/src/`, and `delete` is granted nowhere.
///
/// Critique B **M18**. A verb nobody calls is a capability nobody audits. The
/// table below maps each RBAC verb to the call that needs it; a verb with no
/// table row fails loudly rather than silently passing, so a future `*` or
/// `deletecollection` cannot slip in.
/// The `Api<T>` methods actually called under `crates/weirkeeper/src/`, per
/// Rust type — DERIVED from the source, never listed.
///
/// # Why a derived map and not a table of needles
///
/// The verb-granular predecessor asked "does the string `.get(&` appear
/// ANYWHERE under `crates/weirkeeper/src/`", which is true as soon as any one
/// resource is `get`-ed. Task 21's review (LOW-1) found what that cannot see:
/// `get` and `watch` were granted on `pods` while every `Api<Pod>` call in the
/// tree is `.list(&ListParams…)` or `.logs(…)`. A caller table has to bind the
/// verb to the RESOURCE, and the only honest way to bind them is to read which
/// method is called on which typed handle.
///
/// # How the binding is read
///
/// `Api<T>` appears in this crate in exactly two declaration shapes —
/// `let x: Api<T> = Api::namespaced(…)` and a parameter `x: &Api<T>` — and
/// rustfmt puts the closing brace of a top-level item in column 0. So each
/// declaration is scoped to the text from itself to whichever comes first: the
/// end of the enclosing item (`"\n}"`), or the next declaration binding the
/// same identifier. Inside that region, `ident.method(` records
/// `(T, method)`, and `Controller::new(ident`, `.owns(ident` or
/// `.watches(ident` records `(T, <controller-watch>)` — a watcher LISTs and
/// then WATCHes, which is the only caller `list`/`watch` on a reconciled kind
/// ever has.
fn api_callers() -> BTreeMap<String, BTreeSet<String>> {
    const WATCH: &str = "<controller-watch>";
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for f in files_under("crates/weirkeeper/src") {
        if f.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let src = std::fs::read_to_string(&f).expect("a readable .rs");
        let bytes: Vec<char> = src.chars().collect();

        // (declaration end, identifier, type)
        let mut decls: Vec<(usize, String, String)> = Vec::new();
        let mut at = 0usize;
        while let Some(rel) = src[at..].find("Api<") {
            let open = at + rel;
            at = open + 4;
            let Some(close_rel) = src[at..].find('>') else {
                break;
            };
            let ty = src[at..at + close_rel].to_string();
            if ty.is_empty() || !ty.chars().all(is_word) {
                continue;
            }
            // Walk back over `&`, whitespace, `:`, whitespace, the identifier.
            let mut i = src[..open].chars().count();
            let back = |i: &mut usize, pred: &dyn Fn(char) -> bool| {
                while *i > 0 && pred(bytes[*i - 1]) {
                    *i -= 1;
                }
            };
            back(&mut i, &|c| c.is_whitespace() || c == '&');
            if i == 0 || bytes[i - 1] != ':' {
                continue;
            }
            i -= 1;
            back(&mut i, &|c| c.is_whitespace());
            let ident_end = i;
            back(&mut i, &is_word);
            if i == ident_end {
                continue;
            }
            let ident: String = bytes[i..ident_end].iter().collect();
            if ident.is_empty() || ident.chars().next().is_some_and(|c| c.is_numeric()) {
                continue;
            }
            decls.push((at + close_rel + 1, ident, ty));
        }

        for (n, (decl_end, ident, ty)) in decls.iter().enumerate() {
            let mut stop = src[*decl_end..]
                .find("\n}")
                .map_or(src.len(), |r| decl_end + r);
            for (p2, id2, _) in &decls[n + 1..] {
                if id2 == ident && *p2 < stop {
                    stop = *p2;
                    break;
                }
            }
            let region = &src[*decl_end..stop];
            let mut k = 0usize;
            while let Some(rel) = region[k..].find(ident.as_str()) {
                let hit = k + rel;
                k = hit + ident.len();
                let before_ok = hit == 0 || !region[..hit].ends_with(is_word);
                let after_ok = !region[k..].starts_with(is_word);
                if !before_ok || !after_ok {
                    continue;
                }
                let tail = region[k..].trim_start();
                if let Some(rest) = tail.strip_prefix('.') {
                    let m: String = rest
                        .trim_start()
                        .chars()
                        .take_while(|c| is_word(*c))
                        .collect();
                    if !m.is_empty() && rest.trim_start()[m.len()..].trim_start().starts_with('(') {
                        out.entry(ty.clone()).or_default().insert(m);
                    }
                }
                let head = region[..hit].trim_end();
                if ["Controller::new(", ".owns(", ".watches("]
                    .iter()
                    .any(|k| head.ends_with(k))
                {
                    out.entry(ty.clone()).or_default().insert(WATCH.to_string());
                }
            }
        }
    }
    out
}

/// Every granted **(resource, verb)** pair has a caller on that resource's own
/// typed handle.
///
/// Sharpened in Task 22 from a verb-granular test (Task 21 review, LOW-1): the
/// old form asked only whether a verb's call shape appeared anywhere in the
/// crate, so `get` and `watch` on `pods` — which nothing calls — passed. The
/// mutant that proves the difference is re-adding either verb to the `pods`
/// rule: the verb-granular test stayed green, this one fails naming the pair.
#[test]
fn every_granted_verb_has_a_caller() {
    const WATCH: &str = "<controller-watch>";
    let callers = api_callers();
    assert!(
        callers.len() >= 8,
        "the Api<T> scan found only {} types; every assertion below would then be vacuous: {:?}",
        callers.len(),
        callers
    );

    // resource -> the Rust type its handle has.
    let ty_of = |resource: &str| -> &'static str {
        match resource.split('/').next().expect("a resource name") {
            "pods" => "Pod",
            "jobs" => "Job",
            "configmaps" => "ConfigMap",
            "approvals" => "Approval",
            "backupdestinations" => "BackupDestination",
            "topicdiscoveries" => "TopicDiscovery",
            "backups" => "Backup",
            "backupschedules" => "BackupSchedule",
            "kafkaclusters" => "KafkaCluster",
            "recoverycatalogs" => "RecoveryCatalog",
            "restores" => "Restore",
            "trustrosters" => "TrustRoster",
            // PLAT-19.1, the other direction of the same mapping.
            "trustpolicies" => "TrustPolicy",
            other => panic!(
                "role.yaml grants a verb on `{other}`, which this test cannot map to an \
                 `Api<T>`. Add the mapping — a resource with no type is a grant nobody can \
                 check."
            ),
        }
    };

    // (resource, verb) -> the methods that satisfy it. Subresources take their
    // OWN row: `patch` on `<kind>/status` is `patch_status`, and a `patch` on
    // the main resource would not satisfy it.
    let methods_for = |resource: &str, verb: &str| -> Vec<&'static str> {
        let sub = resource.split_once('/').map(|(_, s)| s);
        match (sub, verb) {
            (Some("log"), "get") => vec!["logs"],
            (Some("status"), "patch") => vec!["patch_status"],
            (Some("status"), "update") => vec!["replace_status"],
            (Some(other), v) => panic!("no caller model for subresource `{other}` verb `{v}`"),
            (None, "create") => vec!["create"],
            (None, "get") => vec!["get", "get_opt"],
            (None, "list") => vec!["list", WATCH], // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
            (None, "watch") => vec![WATCH],
            (None, "patch") => vec!["patch"],
            (None, "update") => vec!["replace"],
            (None, "delete") => vec!["delete", "delete_opt"],
            (None, v) => panic!(
                "role.yaml grants the verb `{v}`, which this caller model does not know. Add \
                 a row naming the `Api::` method that needs it, or remove the grant."
            ),
        }
    };

    /// The ONE (resource, verb) pair granted without a caller, with the reason.
    ///
    /// Found BY this sharpening, and recorded rather than narrowed. Spec §9's
    /// shape grants `get`/`list`/`watch` uniformly over the six kinds; five of
    /// the six have a `get`-shaped caller and `backupschedules` does not,
    /// because its reconciler only ever receives the object from the
    /// `Controller`'s own watcher and then patches its status. Splitting the
    /// rule would leave one of six kinds silently un-`get`-able to anyone
    /// reading the install file, which is a worse surprise than a grant that
    /// is written down; the security delta is nil, since `list` already
    /// returns every one of these objects in full.
    ///
    /// EXACTLY ONE ENTRY, asserted below: a second one must show up in a diff.
    const GRANTED_FOR_UNIFORMITY: [(&str, &str); 1] = [("backupschedules", "get")];

    let rules = rules_of("config/rbac/role.yaml", "weirkeeper");
    assert!(!rules.is_empty(), "role.yaml granted no verb at all");

    // THE NAMED ONE. `delete` must appear in no rule, on any resource: the API
    // server's TTL controller removes a finished Job (this controller patches
    // `ttlSecondsAfterFinished` after the status write) and ownerReference
    // garbage collection removes the plan ConfigMap and the probe Job.
    for (groups, resources, verbs) in &rules {
        assert!(
            !verbs.iter().any(|x| x == "delete"),
            "the weirkeeper ClusterRole grants `delete` on {resources:?} (apiGroups {groups:?}); \
             no reconciler calls `Api::delete` anywhere"
        );
    }
    for called in callers.values() {
        for forbidden in ["delete", "delete_opt"] {
            assert!(
                !called.contains(forbidden),
                "`Api::{forbidden}` is now called under crates/weirkeeper/src/ — a reconciler \
                 deletes something, and this test's premise (and Global Constraint 6) has \
                 changed"
            );
        }
    }

    let mut checked = 0usize;
    for (_, resources, verbs) in &rules {
        for resource in resources {
            for verb in verbs {
                if GRANTED_FOR_UNIFORMITY
                    .iter()
                    .any(|(r, v)| r == resource && v == verb)
                {
                    continue;
                }
                let ty = ty_of(resource);
                let want = methods_for(resource, verb);
                let called = callers.get(ty).cloned().unwrap_or_default();
                assert!(
                    want.iter().any(|m| called.contains(*m)),
                    "role.yaml grants `{verb}` on `{resource}` and nothing under \
                     crates/weirkeeper/src/ calls it: `Api<{ty}>` is used for {called:?}, and \
                     this verb needs one of {want:?}. A verb with no caller on its own resource \
                     is critique B M18 — narrow the rule, or record the pair in \
                     GRANTED_FOR_UNIFORMITY with the reason."
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 20,
        "only {checked} (resource, verb) pairs were checked; the walk has gone quiet"
    );
    assert_eq!(
        GRANTED_FOR_UNIFORMITY.len(),
        1,
        "exactly one pair is granted without a caller, and it is written down"
    );
    // And the pods rule, by name, because it is the one this sharpening was
    // carried for.
    let pods: Vec<&Vec<String>> = rules
        .iter()
        .filter(|(_, r, _)| r == &vec!["pods".to_string()])
        .map(|(_, _, v)| v)
        .collect();
    assert_eq!(
        pods,
        vec![&vec!["list".to_string()]], // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        "the pods rule is `list` and nothing else (Task 21 review, LOW-1): `Api<Pod>` is only \
         ever `.list(&ListParams…)` and `.logs(…)`, and `pods/log` carries its own `get`"
    );
}

/// The OTHER direction: every **call site's** (resource, verb) is granted by
/// the role every install actually ships.
///
/// # Why this exists (W0)
///
/// [`every_granted_verb_has_a_caller`] above walks grants and looks for
/// callers. It is silent about the inverse — a call whose verb is granted
/// NOWHERE — and that silence shipped a P0: the `BackupSchedule` reconciler
/// reserved a `Forbid` slot with `Api::replace_status`, which kube issues as
/// `PUT` and the API server authorises as the verb `update` on
/// `backupschedules/status`. The role grants `patch` on the status
/// subresources and, deliberately, `update` on nothing. With the default
/// `concurrencyPolicy: Forbid` that is a 403 on every due slot of every
/// schedule: no scheduled Backup is ever created on a shipped install, while
/// every unit test — which answers whatever route it is asked for — stays
/// green. This test is the one that fails on it, naming the method, the
/// resource and the verb.
///
/// # What makes it more than a text match
///
/// It reads no needle. Both halves are DERIVED: the calls from
/// [`api_callers`], which binds a method to the typed handle it was called on
/// by walking each `Api<T>` declaration's own region of the source, and the
/// grants from the parsed YAML of every shipped copy of the role. Renaming
/// `reservation_body`, moving the reservation into another function or another
/// module, or reaching it through a different control-flow path changes
/// nothing here: the test sees `Api<BackupSchedule>::replace_status` wherever
/// it is written. Adding a `Api<T>` method with no row in `needs` is a loud
/// panic rather than a silent pass, so a new call shape cannot slip in.
///
/// # What it CANNOT catch
///
/// 1. **Untyped calls.** `Api<DynamicObject>`, `client.request(…)` and
///    anything built from a `GroupVersionKind` at runtime carry no Rust type
///    to bind a resource to. None exists in this crate today; the `resource_of`
///    panic below is what a future one hits.
/// 2. **`Api<T>` reached in a shape the scan does not read.** [`api_callers`]
///    reads `let x: Api<T> = …` and `x: &Api<T>` parameters. A handle returned
///    from a function (`fn api() -> Api<Job>`) and immediately used would be
///    invisible to BOTH directions of this pair.
/// 3. **Calls made by something that is not this crate** — a runner Job's own
///    ServiceAccount, the UI, kubectl in a doc. Other roles, other tests.
/// 4. **Anything RBAC decides beyond (group, resource, verb)**: namespace
///    scope, `resourceNames`, aggregation, admission webhooks, ValidatingAdmissionPolicy.
///    A grant this test accepts can still be refused in a cluster whose
///    RoleBinding is namespaced differently.
/// 5. **Verbs the API server derives from a request rather than from the
///    method name** — a server-side apply that CREATES needs `create` as well
///    as `patch`. This crate sends no `Patch::Apply`; the `patch` row says so.
/// 6. It compares against the manifests in the tree, never against a live
///    cluster. `kubectl auth can-i` is the live half, and it is a live-evidence
///    step, not a unit test.
#[test]
fn every_call_site_has_a_grant() {
    const WATCH: &str = "<controller-watch>";

    // The typed handle -> the (apiGroup, resource) an RBAC rule names it by.
    let resource_of = |ty: &str| -> (&'static str, &'static str) {
        match ty {
            "Approval" => ("logweir.dev", "approvals"),
            "Backup" => ("logweir.dev", "backups"),
            // D2 W7 (PLAT-08.1): the seventh kind. Its two rules are in
            // `config/rbac/role.yaml` beside the six-kind list rather than
            // inside it, so the `logweir-viewer` role is untouched.
            "BackupDestination" => ("logweir.dev", "backupdestinations"),
            // D2 W8 (PLAT-09.1): the eighth kind. `controllers/topic_discovery.rs`
            // reaches it for exactly two things — the `Controller::new` watch and
            // `patch_status` — and `config/rbac/role.yaml` grants exactly those.
            "TopicDiscovery" => ("logweir.dev", "topicdiscoveries"),
            "BackupSchedule" => ("logweir.dev", "backupschedules"),
            // D3 W8 (PLAT-15.1): the eighth kind. Its two rules are in
            // `config/rbac/role.yaml` beside the six-kind list, with a NOTE FOR
            // W13 that folds them into D3's own RBAC pass.
            "RecoveryCatalog" => ("logweir.dev", "recoverycatalogs"),
            "KafkaCluster" => ("logweir.dev", "kafkaclusters"),
            "Restore" => ("logweir.dev", "restores"),
            "TrustRoster" => ("logweir.dev", "trustrosters"),
            // PLAT-19.1. Added with the `TrustPolicy` reconciler, which is
            // what makes this mapping necessary: the panic below is a HARD
            // failure on an unmapped type, so a new `Api<T>` cannot reach a
            // release with its grant unchecked.
            "TrustPolicy" => ("logweir.dev", "trustpolicies"),
            "Job" => ("batch", "jobs"),
            "ConfigMap" => ("", "configmaps"),
            "Pod" => ("", "pods"),
            other => panic!(
                "crates/weirkeeper/src/ calls the Kubernetes API through `Api<{other}>`, and \
                 this test cannot map that type to an (apiGroup, resource). Add the mapping — \
                 a call whose resource is unknown is a call whose grant cannot be checked."
            ),
        }
    };

    // The `Api` method -> every (resource, verb) the API server authorises it
    // against. A subresource is its own resource string, which is the whole
    // point: `patch_status` is NOT satisfied by `patch` on the main resource,
    // and `replace_status` is `update` on `<resource>/status` — the pair this
    // test was written for.
    let needs = |resource: &str, method: &str| -> Vec<(String, &'static str)> {
        let status = format!("{resource}/status");
        match method {
            "create" => vec![(resource.to_string(), "create")],
            "get" | "get_opt" => vec![(resource.to_string(), "get")],
            "list" => vec![(resource.to_string(), "list")], // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
            "patch" => vec![(resource.to_string(), "patch")],
            "replace" => vec![(resource.to_string(), "update")],
            "delete" | "delete_opt" => vec![(resource.to_string(), "delete")],
            "get_status" => vec![(status, "get")],
            "patch_status" => vec![(status, "patch")],
            "replace_status" => vec![(status, "update")],
            "logs" => vec![(format!("{resource}/log"), "get")],
            // A watcher LISTs once and then WATCHes, and needs both.
            WATCH => vec![
                (resource.to_string(), "list"), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
                (resource.to_string(), "watch"),
            ],
            other => panic!(
                "`Api<_>::{other}` is called under crates/weirkeeper/src/ and this test does \
                 not know which RBAC verb it needs. Add a row naming the verb — an unmodelled \
                 call shape is exactly how `replace_status` reached a shipped release."
            ),
        }
    };

    let callers = api_callers();
    assert!(
        callers.len() >= 8,
        "the Api<T> scan found only {} types; every assertion below would then be vacuous: {:?}",
        callers.len(),
        callers
    );

    // EVERY SHIPPED COPY, not just the source manifest. An operator applies
    // `logweir.yaml` or renders the chart; a fix that lands in `config/rbac`
    // and nowhere else is still a broken install. The Helm TEMPLATE
    // (`charts/logweir/templates/clusterrole.yaml`) is not parseable YAML on
    // its own — it carries `{{ include }}` lines — and is held to the install
    // file by `chart_lint`'s render comparison, over these rendered outputs.
    let mut files = vec![
        "config/rbac/role.yaml".to_string(),
        "logweir.yaml".to_string(),
    ];
    for path in files_under("charts/logweir/rendered") {
        let rel = path
            .strip_prefix(repo())
            .expect("a path under the repository")
            .to_string_lossy()
            .to_string();
        if manifests_in(&path)
            .iter()
            .any(|m| m.kind == "ClusterRole" && m.name() == "weirkeeper")
        {
            files.push(rel);
        }
    }
    assert!(
        files.len() >= 4,
        "only {} shipped copies of the weirkeeper ClusterRole were found: {files:?}",
        files.len()
    );

    let mut checked = 0usize;
    for file in &files {
        let rules = rules_of(file, "weirkeeper");
        for (ty, methods) in &callers {
            let (group, resource) = resource_of(ty);
            for method in methods {
                for (needed_resource, verb) in needs(resource, method) {
                    let granted = rules.iter().any(|(groups, resources, verbs)| {
                        groups.iter().any(|g| g == group || g == "*")
                            && resources.iter().any(|r| *r == needed_resource || r == "*")
                            && verbs.iter().any(|v| v == verb || v == "*")
                    });
                    assert!(
                        granted,
                        "crates/weirkeeper/src/ calls `Api<{ty}>::{method}`, which the API \
                         server authorises as `{verb}` on `{needed_resource}` (apiGroup \
                         {group:?}), and {file} grants no such verb. Every call the controller \
                         makes must be authorised by the role the install ships, or the call \
                         403s in production while every route-table test stays green. Change \
                         the call to an authorised one, or widen the role deliberately and say \
                         why in its comment."
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(
        checked >= 100,
        "only {checked} (call site, grant) pairs were checked across {} files; the scan has \
         gone quiet",
        files.len()
    );
}

/// The four ClusterRoles, verb for verb.
///
/// A table test and not four assertions, because the property is the WHOLE rule
/// set of each role: an extra rule is as much a defect as a wrong verb, and a
/// per-rule assertion cannot see one.
#[test]
fn the_four_cluster_roles_are_exactly_as_specified() {
    // --- `weirkeeper` (interface I28) -------------------------------------
    //
    // Three places where this differs from the letter of the task brief, each
    // for the same reason — the role grants what the reconcilers CALL:
    //
    //  * `patch` and not `update` on the status subresources. Every status
    //    write in the crate is `Api::patch_status` with `Patch::Merge`; no
    //    `Api::replace` exists. Spec §9's "`update` on their `/status` only"
    //    is carrying the word ONLY (the subresource, not the object); a literal
    //    `update` verb would be a grant with no caller.
    //  * `patch` IS granted on `jobs`, which the brief's rule list omits:
    //    `ttlSecondsAfterFinished` is patched on after the status write by
    //    three reconcilers, and without it finished Jobs never expire.
    //  * `configmaps` gets `create` and `get` only (plan erratum E5a and Task
    //    17's request fragment), not `create/get/list/watch`: nothing watches
    //    ConfigMaps.
    //
    // And one rule no source for this task listed at all: `create` on
    // `backups`, which is how a `BackupSchedule` produces a run.
    let weirkeeper = vec![
        (
            v(&["logweir.dev"]),
            v(&SIX_KINDS),
            v(&["get", "list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        // D2 W7 (PLAT-08.1): the `BackupDestination` reconciler's `list`/`watch`
        // and `destination::resolve_ref`'s `get_opt`. A SEPARATE rule, so the
        // six-kind list the `logweir-viewer` role also names stays exactly what
        // it was.
        (
            v(&["logweir.dev"]),
            v(&["backupdestinations"]),
            v(&["get", "list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        // D2 W8 (PLAT-09.1): the `TopicDiscovery` reconciler's `Controller::new`
        // watch, and NOTHING else. No `get`: the reconciler never re-reads a
        // discovery, and a granted verb with no caller is critique B M18.
        (
            v(&["logweir.dev"]),
            v(&["topicdiscoveries"]),
            v(&["list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        // D3 W8 (PLAT-15.1): `Controller::new(api, …)` in
        // `controllers/recovery_catalog.rs`, and NO `get` — that reconciler
        // never reads a `RecoveryCatalog` by name. A SEPARATE rule for the same
        // reason `backupdestinations` and `topicdiscoveries` have one: folding
        // a ninth kind into the six-kind list would silently widen
        // `logweir-viewer` in a diff that reads as a controller change.
        (
            v(&["logweir.dev"]),
            v(&["recoverycatalogs"]),
            v(&["list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        (v(&["logweir.dev"]), v(&["backups"]), v(&["create"])),
        (
            v(&["logweir.dev"]),
            v(&[
                "approvals/status",
                "backups/status",
                "backupschedules/status",
                "kafkaclusters/status",
                "restores/status",
                "trustrosters/status",
            ]),
            v(&["patch"]),
        ),
        // D2 W7: `controllers/backup_destination.rs`'s `Api::patch_status`, a
        // merge PATCH — seam S7, and no `update` anywhere.
        (
            v(&["logweir.dev"]),
            v(&["backupdestinations/status"]),
            v(&["patch"]),
        ),
        // D2 W8: `controllers/topic_discovery.rs`'s `Api::patch_status`, a merge
        // PATCH carrying `metadata.resourceVersion` — seam S7.
        (
            v(&["logweir.dev"]),
            v(&["topicdiscoveries/status"]),
            v(&["patch"]),
        ),
        // D3 W8: `controllers/recovery_catalog.rs`'s `Api::patch_status`, a
        // merge PATCH carrying `metadata.resourceVersion` — seam S7.
        (
            v(&["logweir.dev"]),
            v(&["recoverycatalogs/status"]),
            v(&["patch"]),
        ),
        (
            v(&["batch"]),
            v(&["jobs"]),
            v(&["create", "get", "list", "watch", "patch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        // `list` ALONE since Task 22 (Task 21 review, LOW-1): `Api<Pod>` is
        // only ever `.list(&ListParams…)` or `.logs(…)`, so `get` and `watch`
        // — spec §9's shape, carried verbatim by Task 21 — had no caller.
        (v(&[""]), v(&["pods"]), v(&["list"])), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        (v(&[""]), v(&["pods/log"]), v(&["get"])),
        (v(&[""]), v(&["configmaps"]), v(&["create", "get"])),
        // PLAT-19.1 — the `TrustPolicy` reconciler's own two rows, added by the
        // wave-1 trust worker because `every_call_site_has_a_grant` above is a
        // hard failure without them. TWO SEPARATE RULES rather than a seventh
        // entry in the six-kinds rule: that list is shared verbatim with
        // `viewer_role.yaml`, and whether an ordinary viewer may read a trust
        // policy is the wave-4 RBAC worker's decision, not this one's.
        //
        // NO `get`. Nothing in the crate calls `Api<TrustPolicy>::get` — a
        // namespace never names its own trust (D3 §7.1), so there is no name
        // to get, and the conflict rule needs the whole set anyway. A granted
        // `get` here would be a verb with no caller, which is exactly what
        // `every_granted_verb_has_a_caller` above exists to refuse.
        //
        // NO `update` ANYWHERE. `update` on `trustpolicies` is PLAT-19.1's
        // authorized-update half and belongs to the new `logweir-trust-admin`
        // ClusterRole, which the wave-4 worker adds. The controller holds
        // none of it: it reads policies and writes their status, and an
        // administrator is the only thing that edits a key's lifecycle.
        (
            v(&["logweir.dev"]),
            v(&["trustpolicies"]),
            v(&["list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
        ),
        (
            v(&["logweir.dev"]),
            v(&["trustpolicies/status"]),
            v(&["patch"]),
        ),
    ];
    assert_eq!(
        rules_of("config/rbac/role.yaml", "weirkeeper"),
        weirkeeper,
        "the `weirkeeper` ClusterRole's rules are not the expected set"
    );

    // --- `logweir-viewer` -------------------------------------------------
    // One rule. NO `/status` resource is named: `get` on the object already
    // returns its status, and naming the subresource here would read to an
    // auditor as though a viewer could write one.
    let viewer = vec![(
        v(&["logweir.dev"]),
        v(&SIX_KINDS),
        v(&["get", "list", "watch"]), // engine-token-ok: the Kubernetes RBAC verb `list`, never the denied kafka-backup subcommand — this file parses ClusterRoles and invokes no engine
    )];
    let got = rules_of("config/rbac/viewer_role.yaml", "logweir-viewer");
    assert_eq!(
        got, viewer,
        "`logweir-viewer`'s rules are not the expected set"
    );
    for (_, resources, _) in &got {
        for r in resources {
            assert!(
                !r.contains('/'),
                "`logweir-viewer` names the subresource `{r}`; spec §9 says no `/status`"
            );
        }
    }

    // --- `logweir-operator` -----------------------------------------------
    // `create` on the four operational kinds, plus PLAIN `update` AND `patch`
    // on `backupschedules`. What may change is the CRD's CEL rule — see
    // `the_schedule_edit_restriction_is_cel_not_rbac`. `patch` is there because
    // PLAT-05.1 makes the policy editable and the two commands an operator
    // runs (`kubectl edit`, `kubectl apply`) both send a PATCH, so `update`
    // alone is a grant nobody can use.
    let operator = vec![
        (
            v(&["logweir.dev"]),
            v(&["backups", "backupschedules", "kafkaclusters", "restores"]),
            v(&["create"]),
        ),
        (
            v(&["logweir.dev"]),
            v(&["backupschedules"]),
            v(&["update", "patch"]),
        ),
    ];
    assert_eq!(
        rules_of("config/rbac/operator_role.yaml", "logweir-operator"),
        operator,
        "`logweir-operator`'s rules are not the expected set"
    );

    // --- `logweir-approver` -----------------------------------------------
    // EXACTLY ONE RULE: `create` on `approvals`, and nothing else. No `get`, no
    // `update` (which would let the bytes be swapped under a `Verified` status
    // computed from different bytes), no `delete` (which would erase an
    // authorisation a `Restore` already ran against).
    let approver = rules_of("config/rbac/approver_role.yaml", "logweir-approver");
    assert_eq!(
        approver.len(),
        1,
        "`logweir-approver` must have EXACTLY ONE rule; it has {}",
        approver.len()
    );
    assert_eq!(
        approver,
        vec![(v(&["logweir.dev"]), v(&["approvals"]), v(&["create"]))],
        "`logweir-approver` is `create` on `approvals` and nothing else"
    );
}

/// What an operator may change on a `BackupSchedule` is the CRD's CEL rule, and
/// `operator_role.yaml` says so instead of trying to express it.
///
/// Critique B **H13**, spec §9 amendment 4b, claim **C97**. An RBAC `rules[]`
/// entry is `apiGroups`/`resources`/`verbs`/`resourceNames` and nothing else —
/// there is no field a CEL expression could go in. An implementer who tries
/// produces either a ClusterRole the API server rejects or a key it silently
/// ignores, and the second is worse: the install succeeds and the restriction
/// does not exist.
///
/// PLAT-05.1 INVERTED WHAT THE RULE SAYS, NOT WHERE IT LIVES. The seal used to
/// name every field but `suspend`; it now names `sourceRef` alone, because a
/// schedule's policy is editable and its identity is the cluster it protects.
/// The RBAC half is unchanged in kind and grew one verb: `kubectl edit` and
/// `kubectl apply` send a PATCH, so an `update`-only grant left an operator
/// unable to perform the edit the CRD now permits.
#[test]
fn the_schedule_edit_restriction_is_cel_not_rbac() {
    let role = read("config/rbac/operator_role.yaml");

    // --- no CEL, structurally -------------------------------------------
    //
    // A CEL expression written into this file has to appear as a key or a value
    // in the PARSED document, so the parse is where it is caught. An RBAC rule
    // is `apiGroups`/`resources`/`verbs`/`resourceNames` and nothing else
    // (claim C97), and the API server SILENTLY IGNORES an unknown key — which
    // is the dangerous half: the install succeeds and the restriction does not
    // exist.
    const RULE_KEYS: [&str; 4] = ["apiGroups", "resources", "verbs", "resourceNames"];
    const DOC_KEYS: [&str; 5] = ["apiVersion", "kind", "metadata", "rules", "aggregationRule"];
    let doc = &manifests_in(&repo().join("config/rbac/operator_role.yaml"))[0];
    for key in doc
        .value
        .as_mapping()
        .expect("a ClusterRole is a mapping")
        .keys()
        .filter_map(|k| k.as_str())
    {
        assert!(
            DOC_KEYS.contains(&key),
            "config/rbac/operator_role.yaml carries the top-level key `{key}`, which is not one \
             of {DOC_KEYS:?}. RBAC has no expression language and the API server ignores what it \
             does not know."
        );
    }
    for rule in doc.value["rules"]
        .as_sequence()
        .expect("`logweir-operator` has rules")
    {
        for key in rule
            .as_mapping()
            .expect("a rule is a mapping")
            .keys()
            .filter_map(|k| k.as_str())
        {
            assert!(
                RULE_KEYS.contains(&key),
                "config/rbac/operator_role.yaml has a rule carrying `{key}`. A `rules[]` entry is \
                 {RULE_KEYS:?} and nothing else, so \"CEL-restricted `update`\" is not \
                 expressible here (critique B H13, claim C97)."
            );
        }
    }
    // And no CEL written as free text either — `oldSelf` is the token a
    // transition rule cannot be spelled without.
    for token in ["oldSelf", "self.spec", "self.suspend"] {
        assert!(
            !role.contains(token),
            "config/rbac/operator_role.yaml contains `{token}`. RBAC has no expression language; \
             the restriction to `suspend` is the CRD's own CEL rule and cannot be written in a \
             ClusterRole."
        );
    }

    // The sentence that says where the restriction actually is.
    assert!(
        role.contains("no field in which a CEL")
            && role.contains("config/crd/backupschedules.yaml"),
        "config/rbac/operator_role.yaml must carry the one-sentence explanation: that an RBAC \
         rule has no field a CEL expression could go in, and that the restriction lives in \
         config/crd/backupschedules.yaml"
    );

    // And the CRD really does carry it, over `sourceRef` and nothing else
    // (Task 15b's shape, D1 §5.1's content).
    let crd = manifests_in(&repo().join("config/crd/backupschedules.yaml"));
    let schema =
        &crd[0].value["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let validations = schema["x-kubernetes-validations"]
        .as_sequence()
        .expect("config/crd/backupschedules.yaml carries an object-level `.spec` CEL rule");
    // ONE SEAL, PLUS WHATEVER VALIDATION RULES SIT BESIDE IT. The seal is the
    // rule that names `oldSelf`; the others (D2's destination sentinel) are
    // ordinary validation rules that run on CREATE, where a transition rule is
    // skipped. Picking the seal by that property rather than by index is what
    // keeps this assertion about the RESTRICTION and not about how many other
    // rules the kind happens to carry.
    let seals: Vec<&str> = validations
        .iter()
        .filter_map(|v| v["rule"].as_str())
        .filter(|r| r.contains("oldSelf"))
        .collect();
    assert_eq!(
        seals.len(),
        1,
        "expected exactly one object-level `.spec` TRANSITION rule on BackupSchedule; got \
         {seals:?}"
    );
    let rule = seals[0];
    assert!(
        rule.contains("self.sourceRef == oldSelf.sourceRef")
            && rule.contains("has(self.sourceRef) == has(oldSelf.sourceRef)"),
        "the object-level CEL rule does not seal `sourceRef`, so an `update` or a `patch` could \
         re-point a schedule at a different cluster and mix two clusters' history under one \
         object"
    );
    let editable: Vec<&str> = schema["properties"]
        .as_mapping()
        .expect("BackupSchedule.spec has properties")
        .keys()
        .filter_map(|k| k.as_str())
        .filter(|k| *k != "sourceRef")
        .collect();
    assert!(
        editable.len() >= 12,
        "BackupSchedule.spec should carry at least twelve editable fields after PLAT-04.2 and \
         PLAT-05.1; got {editable:?}"
    );
    for field in &editable {
        assert!(
            !rule.contains(&format!("self.{field} == oldSelf.{field}")),
            "the CEL rule seals `{field}`, which PLAT-05.1 makes editable policy — and the \
             operator role's `update`/`patch` would then be grants nobody can use"
        );
    }
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// Runner pods automount no ServiceAccount token; the controller Deployment is
/// the one exemption, and the exemption is a literal path list.
///
/// Critique B **H14**, spec §9. Runner pods make zero Kubernetes API calls and
/// hold the signing key; the controller is the only Kubernetes API client in
/// the design (`design-operator.md:355-359`) and cannot open a watch without
/// its token.
#[test]
fn runner_pods_do_not_automount_a_token() {
    // THE EXEMPTION. A literal list, never a pattern: a third exempt PodSpec is
    // a diff in these lines, reviewed on its own merits. Each entry is
    // `<file>::<Kind>/<name>`, and the rendered install file carries the same
    // two objects a second time — so they are named twice rather than matched
    // by a rule that would also match something else.
    const EXEMPT: [&str; 4] = [
        "config/manager/deployment.yaml::Deployment/weirkeeper",
        "config/rbac/service_account.yaml::ServiceAccount/weirkeeper",
        "logweir.yaml::Deployment/weirkeeper",
        "logweir.yaml::ServiceAccount/weirkeeper",
    ];

    // --- arm (a), the shipped manifests -----------------------------------
    let mut checked = 0usize;
    for m in all_manifests() {
        // Every place a PodSpec can appear in this tree, plus the ServiceAccount
        // object's own top-level field.
        let pod_specs: Vec<(&str, &Value)> = match m.kind.as_str() {
            "Deployment" | "Job" | "ReplicaSet" | "StatefulSet" | "DaemonSet" => {
                vec![("spec.template.spec", &m.value["spec"]["template"]["spec"])]
            }
            "CronJob" => vec![(
                "spec.jobTemplate.spec.template.spec",
                &m.value["spec"]["jobTemplate"]["spec"]["template"]["spec"],
            )],
            "Pod" => vec![("spec", &m.value["spec"])],
            "ServiceAccount" => vec![("(object)", &m.value)],
            _ => Vec::new(),
        };
        for (where_, spec) in pod_specs {
            if spec.is_null() {
                continue;
            }
            let key = format!("{}::{}", m.origin, m.id());
            let exempt = EXEMPT.contains(&key.as_str());
            let field = spec
                .get("automountServiceAccountToken")
                .and_then(Value::as_bool);
            if exempt {
                assert!(
                    field.is_none(),
                    "{key} ({where_}) is the `automountServiceAccountToken` EXEMPTION and must \
                     not set the field at all; it is set to {field:?}. The controller is the only \
                     Kubernetes API client in the design and removing its token stops it opening \
                     a watch."
                );
            } else {
                assert_eq!(
                    field,
                    Some(false),
                    "{key} ({where_}) does not carry `automountServiceAccountToken: false`, and \
                     it is not one of the {} exempt objects {EXEMPT:?}",
                    EXEMPT.len()
                );
            }
            checked += 1;
        }
    }
    assert!(
        checked >= 4,
        "only {checked} PodSpec-carrying documents were examined; this test would then be \
         asserting almost nothing"
    );

    // --- arm (a), continued: what `job::build` renders ---------------------
    //
    // `crates/logweir` does not (and must not) depend on `crates/weirkeeper`,
    // so the rendered Job cannot be built here. The statement in the source is
    // asserted instead, and the three tests that assert the RENDERED result are
    // named so a reviewer can find them:
    // `weirkeeper/tests/backup_controller.rs:594`,
    // `kafka_cluster_controller.rs:345`, `restore_controller.rs:1522`.
    let job_rs = read("crates/weirkeeper/src/job.rs");
    assert!(
        job_rs.contains("automount_service_account_token: Some(false)"),
        "crates/weirkeeper/src/job.rs no longer sets \
         `automount_service_account_token: Some(false)` on the runner PodSpec; every runner pod \
         would then receive a projected token in the one pod that holds the signing key"
    );

    // --- arm (b), the controller Deployment -------------------------------
    let deployment = manifests_in(&repo().join("config/manager/deployment.yaml"));
    let pod = &deployment[0].value["spec"]["template"]["spec"];
    assert_eq!(
        pod["serviceAccountName"].as_str(),
        Some("weirkeeper"),
        "the controller Deployment must name the `weirkeeper` ServiceAccount; a PodSpec with none \
         silently gets `default`"
    );
    assert!(
        pod.get("automountServiceAccountToken").is_none(),
        "the controller Deployment must NOT set `automountServiceAccountToken`. It is the only \
         component that talks to the API server; setting it to `false` ships a control plane that \
         cannot start a watch, and setting it to `true` is a redundant line that invites the \
         first form."
    );
}

/// `logweir.yaml`'s header carries Global Constraint 37's literal.
#[test]
fn the_install_file_records_blocked_images_not_published() {
    let text = read("logweir.yaml");
    assert!(
        text.contains("blocked: images not published"),
        "logweir.yaml's header must contain the literal `blocked: images not published` (Global Constraint \
         37): the images it references are referenced by TAG, no such image has been pushed, and \
         a locally built one is author-only and never satisfies spec §16 clause 1"
    );
    // It is a HEADER, not a line buried mid-file: a stranger reads the top.
    let head: String = text.lines().take(60).collect::<Vec<_>>().join("\n");
    assert!(
        head.contains("blocked: images not published"),
        "the `blocked: images not published` line must be in logweir.yaml's header comment (first 60 lines)"
    );
}

/// The `TrustRoster` sample is named `default`, and says the name is fixed.
///
/// Interface **I16**: `weirkeeper::controllers::approval::ROSTER_NAME` is the
/// literal `"default"` and `load_roster` reads that name and no other. A roster
/// under any other name is stored, reconciled, listed — and consulted by no
/// approval check.
#[test]
fn the_trustroster_sample_is_named_default() {
    let path = "config/samples/trustroster.yaml";
    let docs = manifests_in(&repo().join(path));
    let roster = docs
        .iter()
        .find(|m| m.kind == "TrustRoster")
        .unwrap_or_else(|| panic!("{path} carries no TrustRoster"));
    assert_eq!(
        roster.name(),
        "default",
        "the TrustRoster sample must be named `default` (interface I16)"
    );
    assert_eq!(
        roster.api_version, "logweir.dev/v1alpha1",
        "the TrustRoster sample must be on the shipped API group"
    );
    let text = read(path);
    assert!(
        text.contains("ROSTER_NAME") && text.contains("THE NAME IS FIXED"),
        "{path} must carry a comment saying the name is fixed, and name the constant \
         (`ROSTER_NAME`) that fixes it"
    );
    // The roster is cluster-scoped: a `namespace:` here would be dropped by the
    // API server and would teach an adopter the wrong thing.
    assert!(
        roster.namespace().is_none(),
        "TrustRoster is cluster-scoped; the sample must not set a namespace"
    );
}

// ---------------------------------------------------------------------------
// The selection rule itself, and drift
// ---------------------------------------------------------------------------

/// The five non-manifest files under `examples/` are skipped BY THE PARSE, and
/// `logweir.yaml` yields exactly the expected document count.
///
/// The mutant this exists for: selecting manifests by a `*.yaml` glob. That
/// passes every other test in this file and fails here, on `drill.yaml`.
#[test]
fn manifest_lint_selects_by_parsed_api_version_and_kind() {
    for rel in [
        "examples/drill.yaml",
        "examples/backup.yaml",
        "examples/restore.yaml",
        "examples/allowed-clusters.json",
        "examples/approval.json",
    ] {
        let path = repo().join(rel);
        assert!(
            path.exists(),
            "{rel} is gone; this test's premise has changed"
        );
        // It PARSES — so it is not skipped for being unreadable...
        let text = std::fs::read_to_string(&path).expect("readable");
        let parsed: Vec<Value> = serde_yaml::Deserializer::from_str(&text)
            .map(|de| Value::deserialize(de).expect("every file here is valid YAML (JSON is YAML)"))
            .collect();
        assert!(!parsed.is_empty(), "{rel} parsed to nothing at all");
        // ...it is skipped because no document carries BOTH keys.
        assert!(
            manifests_in(&path).is_empty(),
            "{rel} was selected as a Kubernetes manifest. It is not one: it carries no \
             `apiVersion`/`kind` pair. A selection rule that took it would be selecting by \
             filename."
        );
    }

    // The one file under examples/ that IS a manifest, so the walk is not
    // passing by finding nothing anywhere.
    let cronjob = manifests_in(&repo().join("examples/cronjob-drill.yaml"));
    assert_eq!(
        cronjob.len(),
        2,
        "examples/cronjob-drill.yaml carries a ServiceAccount and a CronJob"
    );

    // And the install file's exact shape: 1 Namespace + 14 CRDs + 1
    // ServiceAccount + 4 ClusterRoles + 1 ClusterRoleBinding + 1 Deployment +
    // 1 NetworkPolicy. The CRD count is ADR 0008's kind list — Amendment A's
    // six, Amendment F's three and Amendment G's five — and a kind that
    // reaches `config/crd/` without reaching
    // `config/crd/kustomization.yaml` shows up here as a count that did not
    // move.
    let docs = install_file();
    let mut by_kind: BTreeMap<String, usize> = BTreeMap::new();
    for m in &docs {
        *by_kind.entry(m.kind.clone()).or_default() += 1;
    }
    assert_eq!(
        by_kind,
        BTreeMap::from([
            ("Namespace".to_string(), 1),
            ("CustomResourceDefinition".to_string(), 14),
            ("ServiceAccount".to_string(), 1),
            ("ClusterRole".to_string(), 4),
            ("ClusterRoleBinding".to_string(), 1),
            ("Deployment".to_string(), 1),
            ("NetworkPolicy".to_string(), 1),
        ]),
        "logweir.yaml's document census changed"
    );
    assert_eq!(
        docs.len(),
        23,
        "logweir.yaml must hold exactly 23 documents"
    );
}

/// `logweir.yaml` is what `config/` renders to — checked without a subprocess.
///
/// `scripts/render-install.sh --check` is the shell form and a `just` recipe;
/// this is the same property, asserted in-process (Global Constraint 22). No
/// kustomize transformer in `config/kustomization.yaml` rewrites anything — no
/// `namePrefix`, no `namespace`, no `commonLabels` — so each rendered document
/// must be VALUE-EQUAL to the source document it came from, and the two
/// document sets must match exactly.
#[test]
fn install_yaml_has_no_drift() {
    // The sources the install root actually names, in one flat list.
    let mut sources: Vec<Manifest> = Vec::new();
    for rel in ["config/manager", "config/crd", "config/rbac"] {
        for f in files_under(rel) {
            let name = f.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            if name == "kustomization.yaml" {
                continue;
            }
            // The two Task 17 REQUEST fragments are deliberately not listed by
            // `config/rbac/kustomization.yaml` and must not appear in the
            // rendered file. Asserted below rather than skipped silently.
            if name == "backup-reconciler-rbac-request.yaml"
                || name == "backup-runner-serviceaccount.yaml"
            {
                for m in manifests_in(&f) {
                    assert!(
                        !install_file().iter().any(|r| r.id() == m.id()),
                        "{} is a REQUEST fragment / a per-namespace file and must not be rendered \
                         into logweir.yaml, but {} is in there",
                        name,
                        m.id()
                    );
                }
                continue;
            }
            sources.extend(manifests_in(&f));
        }
    }

    let rendered = install_file();
    let src_ids: BTreeSet<String> = sources.iter().map(Manifest::id).collect();
    let out_ids: BTreeSet<String> = rendered.iter().map(Manifest::id).collect();
    assert_eq!(
        src_ids, out_ids,
        "logweir.yaml's documents are not the documents config/ carries. Run `just install-yaml`."
    );

    for s in &sources {
        let r = rendered
            .iter()
            .find(|r| r.id() == s.id())
            .unwrap_or_else(|| panic!("{} is in config/ and not in logweir.yaml", s.id()));
        assert_eq!(
            s.value,
            r.value,
            "logweir.yaml's `{}` differs from `{}`. Either config/ changed and logweir.yaml was \
             not regenerated, or logweir.yaml was edited by hand. Both are fixed by \
             `just install-yaml`.",
            s.id(),
            s.origin
        );
    }
}

/// The author-only overlay reaches the same bases as the install root.
///
/// `config/overlays/local-images` cannot say `resources: [../..]` — that is a
/// cycle, because the overlay lives inside `config/` — so it repeats the base
/// list. This is what keeps the repetition honest: a base added to one and not
/// the other would make the author-only install path quietly incomplete.
#[test]
fn the_local_images_overlay_covers_the_same_bases() {
    let list = |rel: &str, strip: &str| -> BTreeSet<String> {
        let docs = manifests_in(&repo().join(rel));
        docs[0]["resources"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{rel} has no `resources`"))
            .iter()
            .map(|r| {
                r.as_str()
                    .expect("a resource entry is a string")
                    .trim_start_matches(strip)
                    .to_string()
            })
            .collect()
    };
    assert_eq!(
        list("config/kustomization.yaml", ""),
        list("config/overlays/local-images/kustomization.yaml", "../../"),
        "config/overlays/local-images/kustomization.yaml does not name the same bases as \
         config/kustomization.yaml"
    );
}

impl std::ops::Index<&str> for Manifest {
    type Output = Value;
    fn index(&self, k: &str) -> &Value {
        &self.value[k]
    }
}

// ---------------------------------------------------------------------------
// Install docs and the two recipes
// ---------------------------------------------------------------------------

/// The install document carries a `kubectl create secret` line for each
/// operator-managed Secret. New Restore Jobs receive a controller-generated,
/// immutable per-Restore approval bundle rather than a global Secret.
///
/// **The document moved, and this test followed it** (Task 29, chain W). Step 1
/// was in `docs/kubernetes.md` §13; the tree now carries ONE install document,
/// `docs/install.md`, and `docs/kubernetes.md` §13 is the pointer at it. The
/// The property asserted here is that every Secret an adopter must provision
/// has a runnable command, while the migration prose identifies the old global
/// approval bundle as legacy-only.
#[test]
fn install_docs_name_all_operator_managed_secrets() {
    let docs = read("docs/install.md");
    for secret in ["logweir-s3", "logweir-evidence-ro"] {
        // A `kubectl create secret` COMMAND, not a mention. Each occurrence
        // of `create secret generic` opens a window over the rest of the
        // command — line continuations included — and the name has to be
        // inside one of them.
        let found = docs.match_indices("create secret generic").any(|(at, _)| {
            let end = (at + 400).min(docs.len());
            docs.get(at..end)
                .is_some_and(|window| window.contains(secret))
        });
        assert!(
            found,
            "docs/install.md has no `kubectl create secret` command for `{secret}` (a mention \
             is not a command: the install docs have to carry a line an adopter can run)"
        );
    }
    assert!(
        !docs.match_indices("create secret generic").any(|(at, _)| {
            let end = (at + 400).min(docs.len());
            docs.get(at..end)
                .is_some_and(|window| window.contains("logweir-signing-key"))
        }),
        "the managed Helm path must not teach users to create the signing Secret"
    );
    assert!(
        docs.contains("short-lived bootstrap Job")
            && docs.contains("identity.authorizedRunnerNamespaces")
            && docs.contains("low-level `logweir.yaml`"),
        "managed signer bootstrap/distribution and low-level provisioning must be distinct"
    );
    assert!(
        docs.contains("logweir-approval-bundle") && docs.contains("legacy"),
        "docs/install.md must retain explicit migration guidance for the legacy global approval bundle"
    );
    // The fifth is the per-cluster SCRAM credential, whose NAME is the
    // adopter's (`KafkaCluster.spec.auth.secretRef`); what is fixed is its data
    // key, which the runner reads and nothing else spells.
    assert!(
        docs.contains("--from-literal=password="),
        "docs/install.md must show how to create the per-cluster SCRAM credential, whose data \
         key is `password` (`TARGET_PASSWORD_SECRET_KEY`)"
    );
    assert!(
        !docs.contains("-out signing.pem")
            && docs.contains(
                "openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem"
            ),
        "managed installs mint no local signing key; the independent approver recipe remains runnable"
    );
}

/// `just check-secrets` is its own recipe, names the current Secrets with
/// `logweir-signing-key` first, exits 1, and is NOT part of `just apply-install`.
///
/// Critique B **M15**, interface **I26**. `just apply-check` was two different
/// jobs under one name: X-APPLY is "apply the file twice and read both exit
/// codes", and the missing-Secret check is "inspect a namespace and refuse".
/// Folding the second into the first would make the install refuse on a clean
/// cluster — which is precisely the state X-APPLY is defined against. The live
/// `rc=1` run is a recorded transcript step; the recipe's SHAPE is asserted
/// here, in-process (Global Constraint 22 forbids a `#[test]` that shells
/// `just`).
#[test]
fn check_secrets_refuses_a_missing_secret() {
    let justfile = read("justfile");
    let recipe = |name: &str| -> String {
        let start = justfile
            .find(&format!("\n{name}"))
            .unwrap_or_else(|| panic!("the justfile has no `{name}` recipe"));
        let rest = &justfile[start + 1..];
        // A recipe body is the indented lines after the header.
        let mut out = String::new();
        for line in rest.lines() {
            if !out.is_empty() && !line.starts_with([' ', '\t']) && !line.trim().is_empty() {
                break;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    };

    let check = recipe("check-secrets ");
    for secret in ["logweir-signing-key", "logweir-s3", "logweir-evidence-ro"] {
        assert!(
            check.contains(secret),
            "`just check-secrets` does not name `{secret}`"
        );
    }
    let first = check
        .find("logweir-signing-key")
        .expect("just asserted it is there");
    for other in ["logweir-s3", "logweir-evidence-ro"] {
        assert!(
            first < check.find(other).expect("just asserted it is there"),
            "`logweir-signing-key` must be the FIRST Secret `just check-secrets` looks for, so it \
             is the first name an operator sees; `{other}` comes before it"
        );
    }
    assert!(
        !check.contains("logweir-approval-bundle"),
        "new controller-managed Restore Jobs must not require the legacy global approval bundle"
    );
    assert!(
        check.contains("exit 1"),
        "`just check-secrets` must exit 1 when a Secret is absent"
    );

    let apply = recipe("apply-install");
    assert!(
        !apply.contains("check-secrets"),
        "`just apply-install` invokes `check-secrets`. X-APPLY is defined against a CLEAN cluster \
         that has no Secrets at all; folding the check in makes the install refuse in exactly the \
         state it is specified to succeed in (critique B M15)."
    );
    // And X-APPLY really is two applies with their exit codes read directly.
    assert_eq!(
        apply.matches("apply --server-side -f logweir.yaml").count(),
        2,
        "`just apply-install` is X-APPLY: `kubectl --context docker-desktop apply --server-side \
         -f logweir.yaml`, TWICE"
    );
    // STANDING RULE 12: the context is named explicitly, always — as the
    // variable the demo drivers export, whose DEFAULT is the laptop's cluster.
    // The literal `--context docker-desktop` was what failed the first CI run
    // of the demo on a `kind` cluster (2026-09-12).
    assert!(
        apply.contains("--context \"${LOGWEIR_KUBE_CONTEXT:-docker-desktop}\""),
        "STANDING RULE 12: `just apply-install` must pass `--context \"${{LOGWEIR_KUBE_CONTEXT:-docker-desktop}}\"` — the driver's context, defaulting to the laptop's"
    );
    assert!(
        !apply.contains("| grep") && !apply.contains("|grep"),
        "STANDING RULE 20: an exit code is never read through a pipe"
    );
}

/// Every YAML file the gate walks actually parses.
///
/// [`manifests_in`] returns nothing for a file it cannot read or parse, which
/// is the right behaviour for a directory walk and the wrong behaviour for a
/// shipped manifest. This is the assertion that a `.yaml` under `config/` is
/// never silently skipped for being broken.
#[test]
fn every_shipped_yaml_file_parses() {
    let mut seen = 0usize;
    for dir in ["config", "examples"] {
        for f in files_under(dir) {
            if f.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let text = std::fs::read_to_string(&f).expect("a readable .yaml");
            for (i, de) in serde_yaml::Deserializer::from_str(&text).enumerate() {
                Value::deserialize(de).unwrap_or_else(|e| {
                    panic!("{}: document {i} does not parse: {e}", f.display())
                });
            }
            seen += 1;
        }
    }
    assert!(seen >= 20, "only {seen} YAML files were walked");
}

// ---------------------------------------------------------------------------
// Task 22 — the approval bundle is a Secret, in every manifest
// ---------------------------------------------------------------------------

/// The four files at `/approval` are the KEYS of a Secret, in every shipped
/// manifest, and **never** of a ConfigMap.
///
/// # Why this walks every manifest instead of naming one file
///
/// The property is about the tree, not about `examples/cronjob-drill.yaml`. A
/// future task adding a second CronJob, a sample, or an overlay that projected
/// the approver's public key or the cluster allowlist from a ConfigMap would
/// reopen exactly the hole this move closed — a subject with `patch configmaps`
/// in the namespace replacing the material that decides who may authorise a
/// run and which clusters it may write into. A ConfigMap's `patch` and a
/// Secret's are different RBAC verbs; that difference IS the control, and a
/// per-file assertion cannot see a new file.
///
/// The `Restore` Job the operator builds is not a shipped manifest and is
/// asserted on the other side, in
/// `crates/weirkeeper/tests/restore_controller.rs`.
#[test]
fn manifest_lint_approval_bundle_comes_from_a_secret() {
    /// The files whose PROJECTION is a security boundary.
    const AUTHORISING: [&str; 4] = [
        "approval.json",
        "approval.sig",
        "approver.pub.pem",
        "allowed-clusters.json",
    ];
    const BUNDLE: &str = "logweir-approval-bundle";

    let manifests = all_manifests();
    assert!(
        manifests.len() >= 20,
        "only {} manifests were walked; this gate would pass vacuously",
        manifests.len()
    );

    let mut seen_in_a_secret = 0usize;
    for m in &manifests {
        // Every `volumes:` list anywhere in the document, at any depth: a
        // PodSpec lives under `spec.template.spec` in a Deployment, under
        // `spec.jobTemplate.spec.template.spec` in a CronJob, and directly
        // under `spec` in a bare Pod. Walking for the KEY rather than for a
        // path is what makes a new kind of workload visible to this test.
        let mut stack = vec![&m.value];
        while let Some(node) = stack.pop() {
            match node {
                Value::Mapping(map) => {
                    for (k, val) in map {
                        if k.as_str() == Some("volumes") {
                            for vol in val.as_sequence().into_iter().flatten() {
                                let name = vol["name"].as_str().unwrap_or("<unnamed>");
                                let keys: Vec<&str> = ["configMap", "secret"]
                                    .into_iter()
                                    .filter(|src| !vol[*src].is_null())
                                    .collect();
                                for src in keys {
                                    let items: Vec<&str> = vol[src]["items"]
                                        .as_sequence()
                                        .into_iter()
                                        .flatten()
                                        .filter_map(|i| i["key"].as_str())
                                        .collect();
                                    let hits: Vec<&&str> = AUTHORISING
                                        .iter()
                                        .filter(|a| items.contains(&**a))
                                        .collect();
                                    if hits.is_empty() {
                                        continue;
                                    }
                                    assert_eq!(
                                        src, "secret",
                                        "{} doc {} volume `{name}` projects {hits:?} from a \
                                         `{src}`. A ConfigMap's patch verb is not a Secret's, \
                                         and that difference is the whole control: these four \
                                         files decide who may authorise this run and which \
                                         clusters it may write into.",
                                        m.origin, m.index
                                    );
                                    let secret_name =
                                        vol["secret"]["secretName"].as_str().unwrap_or("");
                                    assert_eq!(
                                        secret_name, BUNDLE,
                                        "{} doc {} volume `{name}` projects {hits:?} from the \
                                         Secret `{secret_name}`. It must be `{BUNDLE}` and not, \
                                         for instance, `logweir-signing-key`: folding approver \
                                         material into the signing-key Secret widens what a \
                                         single `get` returns to include BOTH the key that signs \
                                         and the material that authorises (spec §7 amendment 4c).",
                                        m.origin, m.index
                                    );
                                    seen_in_a_secret += 1;
                                }
                            }
                        }
                        stack.push(val);
                    }
                }
                Value::Sequence(seq) => stack.extend(seq.iter()),
                _ => {}
            }
        }
    }
    assert_eq!(
        seen_in_a_secret, 1,
        "exactly one shipped manifest projects the approval bundle today \
         (examples/cronjob-drill.yaml). Finding none means the walk stopped seeing it and this \
         gate went quiet; finding more is fine only if this number is updated deliberately."
    );
}

/// The volume comment in `examples/cronjob-drill.yaml` states the REASON for
/// the Secret, and no longer states the reason for the ConfigMap.
///
/// A manifest whose comment still says "it belongs in a ConfigMap" beside a
/// `secret:` volume is worse than either alone: the next editor reads the
/// comment, believes the move was accidental, and reverts it. The comment is
/// the only place the *why* survives a `kubectl get -o yaml`, which strips it.
#[test]
fn the_bundle_comment_states_the_reason() {
    let text = read("examples/cronjob-drill.yaml");
    assert!(
        !text.contains("it belongs in a ConfigMap"),
        "the old rationale is still in the file beside a Secret volume"
    );
    // The COMMENT prose, with the `#` markers and the line wrapping taken out,
    // so the assertion is about what an editor reads and not about where the
    // lines happen to break.
    let flat: String = text
        .lines()
        .map(str::trim_start)
        .filter(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        flat.contains(
            "reading a Secret is a different RBAC verb from reading a ConfigMap, so the four \
             files that decide WHO may authorise this run and WHICH clusters it may touch are \
             no longer replaceable by any subject holding `patch configmaps` in this namespace"
        ),
        "the one-sentence reason for the Secret must be in the volume's comment: {text}"
    );
    assert!(
        flat.contains("None of this stops a cluster-admin"),
        "and O0 is stated on this surface too, never implied away"
    );
}

/// `docs/stability.md` records the `kube`/`k8s-openapi` pair Task 15 resolved
/// and the toolchain they were pinned against — read from the manifests, so
/// the page cannot drift from the build.
#[test]
fn the_stability_note_records_the_resolved_kube_pair() {
    let stability = read("docs/stability.md");
    let manifest = read("crates/weirkeeper/Cargo.toml");
    let toolchain = read("rust-toolchain.toml");

    let channel = toolchain
        .lines()
        .find_map(|l| l.split_once("channel"))
        .and_then(|(_, r)| r.split('"').nth(1))
        .expect("rust-toolchain.toml declares a channel")
        .to_string();
    assert_eq!(channel, "1.89.0", "the pin this note names");

    for (crate_name, want) in [("kube", "0.99"), ("k8s-openapi", "0.24")] {
        assert!(
            manifest.contains(&format!("{crate_name} = {{ version = \"{want}\"")),
            "crates/weirkeeper/Cargo.toml must still declare {crate_name} {want}"
        );
    }
    // The RESOLVED versions, which are what the note names — the manifest
    // carries the requirement, `Cargo.lock` carries what it resolved to.
    let lock = read("Cargo.lock");
    for want in ["kube 0.99.0", "k8s-openapi 0.24.0"] {
        let (name, version) = want.split_once(' ').unwrap();
        assert!(
            lock.contains(&format!("name = \"{name}\"\nversion = \"{version}\"")),
            "Cargo.lock must still resolve {want}"
        );
        assert!(
            stability.contains(&format!("`{name} {version}`")),
            "docs/stability.md must name the resolved `{want}`"
        );
    }
    assert!(
        stability.contains(&format!("**`{channel}`**")),
        "docs/stability.md must name the toolchain {channel} the pair was resolved against"
    );
    assert!(
        stability.contains("O0, default (a)")
            && stability.contains("none of this stops a cluster-admin"),
        "the same section carries the O0 sentence, accepted and stated"
    );
}

// ---------------------------------------------------------------------------
// Task 23 — digest-only image references, and the org-root anchor
// ---------------------------------------------------------------------------

/// Every container image reference in every shipped manifest, as
/// `(where, reference)`.
///
/// THE WALK IS OVER THE PARSED DOCUMENTS, not over the text. A `grep` for
/// `image:` would also match `imagePullPolicy`, the `images:` list in a
/// kustomization, and any comment that mentions one — and would miss an
/// `initContainers` entry in a manifest whose indentation it did not expect.
/// Every PodSpec-carrying kind this tree ships is enumerated here; a seventh
/// kind arrives as a diff in this list.
fn image_references() -> Vec<(String, String)> {
    all_manifests().iter().flat_map(images_in).collect()
}

/// Every container image reference in ONE parsed manifest.
///
/// Split out of [`image_references`] so that
/// [`the_image_walk_covers_a_manifest_that_is_not_in_any_list`] can hand it a
/// document that is not in the tree at all. A hard-coded list of files — the
/// mutant — passes every other test here and fails that one.
fn images_in(m: &Manifest) -> Vec<(String, String)> {
    let pod_specs: Vec<&Value> = match m.kind.as_str() {
        "Deployment" | "Job" | "ReplicaSet" | "StatefulSet" | "DaemonSet" => {
            vec![&m.value["spec"]["template"]["spec"]]
        }
        "CronJob" => vec![&m.value["spec"]["jobTemplate"]["spec"]["template"]["spec"]],
        "Pod" => vec![&m.value["spec"]],
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for spec in pod_specs {
        if spec.is_null() {
            continue;
        }
        for list in ["initContainers", "containers", "ephemeralContainers"] {
            let Some(seq) = spec.get(list).and_then(Value::as_sequence) else {
                continue;
            };
            for (i, c) in seq.iter().enumerate() {
                let Some(image) = c.get("image").and_then(Value::as_str) else {
                    continue;
                };
                out.push((
                    format!("{}::{} {}[{}]", m.origin, m.id(), list, i),
                    image.to_string(),
                ));
            }
        }
    }
    out
}

/// `@sha256:` followed by exactly 64 lowercase hex characters, and nothing
/// after it.
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

/// **Global Constraint 7: pinning is by digest, never by tag** — in every
/// shipped Kubernetes manifest.
///
/// WHY THIS IS A SECURITY GATE AND NOT TIDINESS. `examples/cronjob-drill.yaml`
/// referenced `logweir:v0.1.0`, a MUTABLE TAG, under the mandatory
/// `imagePullPolicy: Never` on a single node — so the org-root anchor baked
/// into that image could be replaced by a `docker build -t logweir:v0.1.0` on
/// that node **without touching a single Kubernetes object**. The controller
/// Deployment had the same shape, which made the whole control plane
/// replaceable the same way. A digest names bytes; a tag names whatever was
/// built last.
///
/// THE SELECTION IS THE PARSE (Task 21's rule, inherited). `examples/drill.yaml`,
/// `backup.yaml`, `restore.yaml` and the two `.json` files carry no
/// `apiVersion`/`kind` pair, so they are skipped by
/// [`manifests_in`] and never by a filename special case — see
/// [`manifest_lint_selects_by_parsed_api_version_and_kind`].
#[test]
fn manifest_lint_every_image_reference_is_a_digest() {
    let refs = image_references();
    assert!(
        refs.len() >= 3,
        "only {} container image references were found across config/**, examples/** and \
         logweir.yaml; this gate would then be asserting almost nothing. Found: {refs:?}",
        refs.len()
    );
    for (where_, reference) in &refs {
        assert!(
            is_digest_reference(reference),
            "{where_} references the image by TAG: `{reference}`. Global Constraint 7 pins by \
             digest and never by tag — a tag under `imagePullPolicy: Never` on a single node can \
             be replaced by a local `docker build` without touching any Kubernetes object, which \
             defeats the org-root anchor baked into the image. Use \
             `<name>@sha256:<64 hex>`."
        );
        assert!(
            !reference.contains(":latest"),
            "{where_} references `:latest` (`{reference}`)"
        );
    }
}

/// The image walk is derived from the PARSE and covers a manifest that is in
/// no list anywhere.
///
/// THE MUTANT THIS EXISTS FOR: replacing the parsed selection with a hard-coded
/// list of files — `["config/manager/deployment.yaml", "examples/cronjob-drill.yaml",
/// "logweir.yaml"]`. That mutant passes every other assertion in this file,
/// because those are in fact the files that carry images today. It fails here,
/// because this document is written to a temporary directory the repository
/// has never heard of and is still selected and still has its image read.
///
/// The document is a `Job`, a kind no shipped manifest uses yet and one the
/// `Backup`/`Restore` reconcilers create at runtime — so this is also the
/// assertion that the day a `Job` manifest ships, its image is covered without
/// anyone remembering to add it.
#[test]
fn the_image_walk_covers_a_manifest_that_is_not_in_any_list() {
    let dir = std::env::temp_dir().join(format!(
        "logweir-manifest-lint-{}-{}",
        std::process::id(),
        "sixth"
    ));
    std::fs::create_dir_all(&dir).expect("a writable temp directory");
    let path = dir.join("a-sixth-manifest.yaml");
    std::fs::write(
        &path,
        "apiVersion: batch/v1\n\
         kind: Job\n\
         metadata:\n  name: a-sixth-manifest\n\
         spec:\n  template:\n    spec:\n      containers:\n        - name: runner\n\
         \x20         image: example.invalid/nothing:a-mutable-tag\n",
    )
    .expect("the temp manifest is written");

    let docs = manifests_in(&path);
    assert_eq!(
        docs.len(),
        1,
        "a document carrying apiVersion and kind was not selected; the selection is not the parse"
    );
    let images = images_in(&docs[0]);
    assert_eq!(
        images.len(),
        1,
        "the image walk did not reach a manifest it had never seen before: {images:?}"
    );
    assert!(
        !is_digest_reference(&images[0].1),
        "the digest rule must reject `{}` — otherwise \
         `manifest_lint_every_image_reference_is_a_digest` would pass on a tag",
        images[0].1
    );

    std::fs::remove_file(&path).ok();
    std::fs::remove_dir(&dir).ok();
}

/// `third_party/org-root.fingerprint` is exactly one `sha256:`-prefixed line of
/// 64 hex characters.
///
/// WHAT THE VALUE IS, SAID ONCE AND HERE: the SHA-256 of the
/// SubjectPublicKeyInfo DER encoding of the org root's **public** key,
/// `third_party/org-root.pub.pem` — the same definition of "fingerprint"
/// `docs/keys.md` gives for a signing key, and the same one stage-2 Task 16's
/// T1 and `docs/platform/02-k8s-transition-plan.md:449-451` give for this
/// anchor. It is a PUBLIC key's hash; no private key material is in the file,
/// in either image, or anywhere in this repository's `third_party/`.
///
/// ONE LINE IS LOAD-BEARING, not tidiness: `just check-org-root` and
/// `scripts/check-image-weirkeeper.sh` check 4 compare this file BYTE FOR BYTE
/// against what `docker run --entrypoint cat` prints out of the image. A second
/// line — a comment, a blank line, a second fingerprint during a rotation —
/// makes the comparison depend on how a shell captured the output. The
/// explanation lives in `docs/kubernetes.md` §14 and in both Dockerfiles, where
/// it costs nothing.
#[test]
fn org_root_fingerprint_is_a_single_sha256_line() {
    let text = read("third_party/org-root.fingerprint");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "third_party/org-root.fingerprint must be EXACTLY ONE line; it has {}: {lines:?}",
        lines.len()
    );
    assert!(
        text.ends_with('\n'),
        "third_party/org-root.fingerprint must end with a newline — `docker run --entrypoint cat` \
         prints one, and a byte-identity comparison against a file without one always differs"
    );
    let line = lines[0];
    let digest = line.strip_prefix("sha256:").unwrap_or_else(|| {
        panic!("third_party/org-root.fingerprint must be `sha256:`-prefixed; it is `{line}`")
    });
    assert_eq!(
        digest.len(),
        64,
        "a SHA-256 is 64 hex characters; `{digest}` is {}",
        digest.len()
    );
    assert!(
        digest
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "the fingerprint must be lowercase hex; it is `{digest}`"
    );
    // The bytes it is the hash OF are checked in beside it, so the value is
    // reproducible rather than a number nobody can falsify:
    //     openssl pkey -pubin -in third_party/org-root.pub.pem -outform DER \
    //       | openssl dgst -sha256
    let pubkey = read("third_party/org-root.pub.pem");
    assert!(
        pubkey.starts_with("-----BEGIN PUBLIC KEY-----"),
        "third_party/org-root.pub.pem must be a PEM PUBLIC key — the bytes the fingerprint above \
         is the SHA-256 of. Without it the fingerprint is 64 hex characters nobody can recompute."
    );
    assert!(
        !pubkey.contains("PRIVATE KEY"),
        "third_party/org-root.pub.pem carries PRIVATE key material. The anchor is the hash of a \
         PUBLIC key and nothing else; the private half of this keypair was generated outside the \
         tree and destroyed (docs/kubernetes.md §14)."
    );
}

/// BOTH images bake the anchor, at the same path.
///
/// The runner image is what a drill pod runs; the controller image is what a
/// cluster owner is handed. The whole point of baking the fingerprint is that
/// neither can be changed without producing a DIFFERENT image, so a `COPY` in
/// only one of them leaves half the claim standing.
///
/// THE IMAGE-INSPECTING HALF IS `just check-org-root`, deliberately not a
/// `#[test]` (critique B M14): a test that shells `docker run` twice breaks
/// Global Constraint 22's 15 s bound and STANDING RULE 7, and would make
/// `just lint` require both images to exist. This half reads two checked-in
/// files and finishes in microseconds.
#[test]
fn both_dockerfiles_copy_the_fingerprint() {
    // Split so this file's own text is not a match for the grep in
    // `the_fingerprint_is_not_read_at_runtime` below.
    let needle = format!("COPY third_party/org-root.fingerprint {}", "/etc/logweir/");
    for dockerfile in ["Dockerfile", "Dockerfile.weirkeeper"] {
        let text = read(dockerfile);
        let hits = text
            .lines()
            .filter(|l| l.trim_start().starts_with(&needle))
            .count();
        assert_eq!(
            hits, 1,
            "{dockerfile} must carry EXACTLY ONE `{needle}` instruction; it has {hits}. The \
             org-root anchor is baked into both images (stage-2 Task 16's T1) at the path that \
             `COPY` names, and `just check-org-root` `cat`s it out of each."
        );
    }
}

/// Nothing under `crates/` opens the baked anchor. **Phase 0 does not read it.**
///
/// This is the assertion that the task ships an anchor and not a control it
/// does not have. The file and its build-time pinning exist so that the anchor
/// is in place BEFORE anything verifies against it — G5's pod-side `--org-key`
/// refusal is Phase 3 — and saying so with a test is the difference between
/// "not yet wired" and "quietly believed to be wired".
///
/// THE MUTANT: read the fingerprint at phase 0. That is a real, attractive
/// change — it looks like a hardening — and it is out of scope here because the
/// refusal it would implement has no `--org-key` flag to refuse, no roster to
/// check and no test for either. This fails the moment the path appears in a
/// `.rs` file.
#[test]
fn the_fingerprint_is_not_read_at_runtime() {
    // Built from pieces: this file lives under `crates/` and a literal here
    // would be its own first hit.
    let runtime_path = format!("/etc/{}/org-root.fingerprint", "logweir");
    let mut offenders = Vec::new();
    let mut scanned = 0usize;
    let mut code_lines = 0usize;
    for f in files_under("crates") {
        if f.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        scanned += 1;
        let Ok(text) = std::fs::read_to_string(&f) else {
            continue;
        };
        // CODE LINES ONLY. A `//` or `///` line that NAMES the path is
        // documentation — `job.rs` says what the anchor is and where it lives,
        // and it must keep saying so. What must not exist is a line that could
        // OPEN it. The mutant ("read the fingerprint at phase 0") is a code
        // line and is caught; a comment is not the control being asserted.
        for line in text.lines() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            code_lines += 1;
            if line.contains(&runtime_path) {
                offenders.push(format!(
                    "{}: {}",
                    f.strip_prefix(repo()).unwrap_or(&f).display(),
                    line.trim()
                ));
            }
        }
    }
    assert!(
        scanned > 50,
        "only {scanned} `.rs` files were scanned; the walk is not reaching `crates/`"
    );
    assert!(
        code_lines > 10_000,
        "only {code_lines} code lines were scanned; the comment filter is eating the tree"
    );
    assert!(
        offenders.is_empty(),
        "these files name the baked anchor's RUNTIME path: {offenders:?}. Phase 0 does not read \
         `{runtime_path}` in tag 1 — the anchor ships so that it EXISTS before the check that \
         verifies against it (G5's pod-side `--org-key` refusal, Phase 3). A reader arriving here \
         needs the refusal, the flag and its own mutant round, which is a different task."
    );
}

/// Interface **I15**, at the digest: the runner image is named in exactly one
/// place under `crates/`, and the tag form is gone.
///
/// `crates/weirkeeper/tests/crd_shape.rs`'s `the_runner_image_is_named_once`
/// asserts the same one-place property over the REGISTRY PATH. This one is
/// about the FORM: after this task the constant is the runner repository
/// followed by `@sha256:` and 64 hex, so a second call site introduced during a
/// digest bump — the mutant, "duplicate the runner image string into
/// `restore.rs`" — fails here as well, and a silent reversion to a mutable tag
/// fails here and nowhere else. (The registry path is not spelled out anywhere
/// in this file, for the reason below: this file lives under `crates/`, and
/// `crd_shape.rs::the_runner_image_is_named_once` counts occurrences there.)
///
/// THE NEEDLES ARE BUILT FROM PIECES, for the reason `crd_shape.rs` gives: this
/// file is under `crates/`, so a literal would be its own second occurrence.
#[test]
fn the_runner_image_lives_in_exactly_one_place() {
    let digest_form = format!("{}{}@sha256:", "docker.io/vladyslavhaina/", "logweir");
    let tag_form = format!("{}{}:v", "docker.io/vladyslavhaina/", "logweir");

    let mut total = 0usize;
    let mut where_: Vec<String> = Vec::new();
    let mut tagged: Vec<String> = Vec::new();
    for f in files_under("crates") {
        if f.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&f) else {
            continue;
        };
        let n = text.matches(digest_form.as_str()).count();
        if n > 0 {
            total += n;
            where_.push(format!(
                "{} ({n}x)",
                f.strip_prefix(repo()).unwrap_or(&f).display()
            ));
        }
        if text.contains(tag_form.as_str()) {
            tagged.push(f.strip_prefix(repo()).unwrap_or(&f).display().to_string());
        }
    }
    assert_eq!(
        total, 1,
        "`{digest_form}` must appear EXACTLY ONCE under `crates/` — a second occurrence is how a \
         digest bump updates one call site and misses another. Found: {where_:?}"
    );
    assert_eq!(
        where_,
        vec!["crates/weirkeeper/src/job.rs (1x)".to_string()],
        "the one occurrence is `crates/weirkeeper/src/job.rs`'s RUNNER_IMAGE"
    );
    assert!(
        tagged.is_empty(),
        "the BARE-TAG form `{tag_form}…` still survives under `crates/` in {tagged:?}. Global \
         Constraint 7 pins by digest and never by tag; Task 23 replaced the tag, and a file that \
         still carries it would resolve to whatever was built last."
    );
}

/// `just image-weirkeeper` takes its platform from the environment, builds ONE
/// platform, and loads the result.
///
/// THREE PROPERTIES, THREE MUTANTS.
///
/// (a) Hard-coding `linux/arm64` makes Task 31's GitHub-hosted amd64 runner
/// compile this workspace under QEMU — STANDING RULE 10's forbidden case,
/// measured at 33x in `docs/stability.md`.
///
/// (b) `--platform linux/amd64,linux/arm64` cannot `--load`: the local image
/// store holds single-platform images only, so a multi-platform build must
/// `--push` to a registry, and nothing in this tree pushes — publication is
/// `release.yml` on a pushed tag, which has never run (Global Constraint 37,
/// `blocked: images not published`).
/// `weirkeeper:check` would then not exist as a local tag,
/// `scripts/check-image-weirkeeper.sh` would have nothing to inspect and
/// `imagePullPolicy: Never` would find no image on the node (critique B H16).
///
/// (c) An `image-weirkeeper-release` recipe is NOT created: multi-arch is
/// produced in exactly one place, `release.yml`'s image job (Task 30b), and
/// Task 30b is not a member of chain J and may not edit the justfile
/// (STANDING RULE 17). A recipe whose body is `docker buildx build --push` on a
/// laptop with no registry is a recipe that has never run.
#[test]
fn the_weirkeeper_image_recipe_takes_its_platform_from_the_environment() {
    let justfile = read("justfile");
    let recipe = |name: &str| -> String {
        let start = justfile
            .find(&format!("\n{name}"))
            .unwrap_or_else(|| panic!("the justfile has no `{name}` recipe"));
        let rest = &justfile[start + 1..];
        let mut out = String::new();
        for line in rest.lines() {
            if !out.is_empty() && !line.starts_with([' ', '\t']) && !line.trim().is_empty() {
                break;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    };

    let body = recipe("image-weirkeeper:");
    assert!(
        body.contains("${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}"),
        "`just image-weirkeeper` must build \
         `--platform \"${{LOGWEIR_IMAGE_PLATFORM:-linux/arm64}}\"`, so the developer default is \
         this host's own architecture and a CI runner sets its own rather than emulating the \
         builder stage (STANDING RULE 10). The recipe is:\n{body}"
    );
    assert!(
        !body.contains("linux/amd64,") && !body.contains(",linux/"),
        "`just image-weirkeeper` must build ONE platform. A multi-platform `docker build` cannot \
         `--load`, so `weirkeeper:check` would not exist as a local tag (critique B H16). The \
         recipe is:\n{body}"
    );
    assert!(
        body.contains("--load"),
        "`just image-weirkeeper` must `--load` the result into the local daemon: every gate in \
         this plan inspects a local tag under `imagePullPolicy: Never`. The recipe is:\n{body}"
    );
    assert!(
        body.contains("-f Dockerfile.weirkeeper") && body.contains("-t weirkeeper:check"),
        "`just image-weirkeeper` is the NAMED PRODUCER of `weirkeeper:check` from \
         `Dockerfile.weirkeeper`. The recipe is:\n{body}"
    );
    assert!(
        !body.contains("--push"),
        "`just image-weirkeeper` must not push: publication is `release.yml`'s, on a pushed \
         tag (Global Constraint 37, `blocked: images not published`)"
    );
    // A RECIPE HEADER STARTS AT COLUMN 0; a comment that NAMES the absent
    // recipe does not. The justfile's own prose says at length why there is no
    // release recipe, and a whole-file `contains` would report that explanation
    // as the violation — the same mistake `scripts/check-dod.sh`'s Global
    // Constraint 14 arm avoids.
    assert!(
        !justfile
            .lines()
            .any(|l| l.starts_with("image-weirkeeper-release")),
        "there is no `image-weirkeeper-release` recipe. Multi-arch is produced in exactly one \
         place — `release.yml`'s image job (Task 30b) — and a recipe whose body is \
         `docker buildx build --push` cannot be executed on a laptop with no registry."
    );
}

/// The controller image owes no MIT notice, and its checker says so.
///
/// `Dockerfile` copies `third_party/LICENSE-MIT` into
/// `/usr/share/licenses/kafka-backup/LICENSE` because the runner image
/// REDISTRIBUTES upstream's binary. `Dockerfile.weirkeeper` carries no OSO code
/// at all, so shipping that notice would be claiming a redistribution that does
/// not happen — and `scripts/check-image-weirkeeper.sh` check 3 asserts the
/// directory is ABSENT, which is the exact opposite of `check-image.sh`'s
/// check 5 (critique B **H15**).
///
/// THE ASSERTION IS OVER INSTRUCTION LINES, NOT THE WHOLE FILE, and that is
/// deliberate: `Dockerfile.weirkeeper`'s comments say at length why there is no
/// MIT notice and why `/usr/share/licenses/kafka-backup/` is absent, and a
/// whole-file grep would report that explanation as the violation — the same
/// mistake `scripts/check-dod.sh`'s Global Constraint 14 arm avoids by looking
/// only at the three places a name would actually make something Logweir's own
/// artefact. The mutant this kills — "fix" a red `check-image.sh` run by
/// copying the MIT licence in — adds an INSTRUCTION, and is caught.
#[test]
fn the_controller_image_owes_no_mit_notice() {
    let text = read("Dockerfile.weirkeeper");
    let instructions: Vec<&str> = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#') && !l.trim().is_empty())
        .collect();
    assert!(
        instructions.len() > 10,
        "only {} instruction lines were found in Dockerfile.weirkeeper; the comment filter is \
         eating the file",
        instructions.len()
    );
    for forbidden in ["LICENSE-MIT", "kafka-backup", "osodevops"] {
        let hits: Vec<&&str> = instructions
            .iter()
            .filter(|l| l.contains(forbidden))
            .collect();
        assert!(
            hits.is_empty(),
            "Dockerfile.weirkeeper has an INSTRUCTION naming `{forbidden}`: {hits:?}. The \
             controller image carries no OSO code and owes no MIT notice; an image that shipped \
             the notice anyway would claim a redistribution it does not make. If \
             `scripts/check-image.sh weirkeeper:check` was run and failed, the fix is to run \
             `scripts/check-image-weirkeeper.sh` instead — not to copy the licence in."
        );
    }
    // Logweir's OWN licence, notice and third-party inventory do ship — Global
    // Constraint 15 governs Logweir's redistribution exactly as it governs
    // upstream's, and the inventory is the arm of it that covers the 392
    // packages this binary is statically linked against.
    //
    // THREE FILES, NOT TWO (Task 29). Task 23 landed this line naming LICENSE
    // and NOTICE, because `THIRD_PARTY_NOTICES.md` did not exist yet. The
    // exhaustive, COPY-parsing form of this assertion is
    // `crates/logweir/tests/doc_lint.rs::every_image_copies_the_licence_and_the_notice`,
    // which checks both images; this stays as the literal-prefix backstop.
    assert!(
        instructions.iter().any(|l| l.starts_with(
            "COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md /usr/share/licenses/logweir/"
        )),
        "Dockerfile.weirkeeper must `COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md \
         /usr/share/licenses/logweir/`"
    );

    // And the checker asserts the ABSENCE, rather than merely not asserting the
    // presence. A gate that only stopped checking would be no gate at all.
    let checker = read("scripts/check-image-weirkeeper.sh");
    assert!(
        checker.contains("test ! -e /usr/share/licenses/kafka-backup"),
        "scripts/check-image-weirkeeper.sh must assert `/usr/share/licenses/kafka-backup/` is \
         ABSENT from the controller image (interface I25, check 3)"
    );
    assert!(
        checker.contains("/usr/share/licenses/logweir/LICENSE")
            && checker.contains("/usr/share/licenses/logweir/NOTICE"),
        "scripts/check-image-weirkeeper.sh must assert BOTH of Logweir's own licence files are \
         non-empty (interface I25, check 3)"
    );
    // Built from pieces, so this file carries no CODE line naming the runtime
    // path — see `the_fingerprint_is_not_read_at_runtime`, which scans exactly
    // that.
    let anchor_path = format!("/etc/{}/org-root.fingerprint", "logweir");
    assert!(
        checker.contains(&anchor_path),
        "scripts/check-image-weirkeeper.sh must assert the baked org-root anchor (interface I25, \
         check 4)"
    );
    assert!(
        checker.contains("--version"),
        "scripts/check-image-weirkeeper.sh must run `weirkeeper --version` (interface I25, check 2)"
    );
    assert!(
        checker.contains("ldd /usr/local/bin/weirkeeper"),
        "scripts/check-image-weirkeeper.sh must run `ldd /usr/local/bin/weirkeeper` (interface \
         I25, check 1)"
    );
}

/// Pinning a LOCALLY BUILT digest does not make the install file publishable.
///
/// Global Constraint 37 and spec §11's amendment 5: "published" means a PULL
/// from a registry the author does not control. `docker inspect --format
/// '{{index .RepoDigests 0}}'` on a locally built image returns the digest of
/// bytes that exist on exactly one laptop — and if that image was pushed to a
/// local `registry:2` to obtain one, the bytes exist on exactly one laptop
/// still. So the install file now carries `@sha256:` references AND still reads
/// `blocked: images not published`, and those two facts are not in tension: the first is
/// Global Constraint 7, the second is Global Constraint 37.
///
/// THIS IS NOT A DUPLICATE OF [`the_install_file_records_blocked_images_not_published`].
/// That one asserts the literal is present. This one asserts the literal
/// survived the digest pin AND that the header still says, in the same breath,
/// that a locally built image is author-only and never satisfies spec §16
/// clause 1 — the mutant being "record the `registry:2` run as satisfying it",
/// which deletes the qualifying sentence and leaves the four words behind.
#[test]
fn the_install_file_still_records_blocked_images_not_published() {
    let text = read("logweir.yaml");
    let head: String = text.lines().take(60).collect::<Vec<_>>().join("\n");

    // The pin really did land — otherwise this test would pass on a tree where
    // nothing changed.
    let deployment = install_file()
        .into_iter()
        .find(|m| m.kind == "Deployment" && m.name() == "weirkeeper")
        .expect("logweir.yaml carries the weirkeeper Deployment");
    let images = images_in(&deployment);
    assert_eq!(images.len(), 1, "the Deployment has one container");
    assert!(
        is_digest_reference(&images[0].1),
        "logweir.yaml's controller image must be pinned by digest; it is `{}`",
        images[0].1
    );

    assert!(
        head.contains("blocked: images not published"),
        "logweir.yaml's header must STILL carry Global Constraint 37's literal \
         `blocked: images not published` after the digest pin. A digest that names bytes on one laptop is \
         not a publication."
    );
    assert!(
        head.contains("author-only"),
        "logweir.yaml's header must say that a locally built or locally loaded image is \
         `author-only`"
    );
    assert!(
        head.contains("never satisfies spec §16 clause 1"),
        "logweir.yaml's header must say that an author-only image `never satisfies spec §16 \
         clause 1`. Deleting that clause while leaving `blocked: images not published` behind is how a local \
         `registry:2` run comes to be recorded as a publication."
    );
    assert!(
        !head.contains("referenced by tag"),
        "logweir.yaml's header still says the images are `referenced by tag`. They are referenced \
         by DIGEST after Task 23, and a header that describes the previous state is worse than no \
         header: it is the one comment a stranger reads."
    );
}
