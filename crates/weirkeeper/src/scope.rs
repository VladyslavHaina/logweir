//! The namespaces this controller watches and acts in — D0 stage 5.
//!
//! WHY THIS EXISTS. `weirkeeper` creates Jobs, and a Job can mount any Secret
//! in its namespace. Holding Job-create authority in a namespace is therefore
//! holding every Secret there, which is residual O1 (`docs/kubernetes.md`
//! §15.4). With one cluster-wide `ClusterRoleBinding` that authority covers
//! EVERY namespace, including the one that holds the shared console's session
//! and cursor keys, and D0 says shared mode must not be declared secure until
//! the controller's namespaced authority is scoped away from it. Scoping is two
//! halves that must agree: the chart binds the namespaced rules with one
//! `RoleBinding` per execution namespace instead of the cluster-wide binding,
//! and this process watches, lists and acts in exactly those namespaces —
//! because a controller whose `Api::all` watch the API server refuses is not a
//! narrower controller, it is a stopped one.
//!
//! THE SWITCH. `LOGWEIR_WATCH_NAMESPACES` — a comma- or whitespace-separated
//! list of namespace names. Unset or empty is TODAY'S BEHAVIOUR, the whole
//! cluster through `Api::all`, so an existing installation upgrades without a
//! change it did not ask for. Set, each namespaced reconciler runs one watch
//! per listed namespace (`Api::namespaced`), and every cross-object read that
//! used to be cluster-wide ([`list_everywhere`]) reads the listed namespaces
//! and nothing else. The two cluster-scoped kinds, `TrustRoster` and
//! `TrustPolicy`, stay `Api::all` in both shapes: they have no namespace to be
//! bound to, and reading them is the only cluster-scoped authority the scoped
//! chart grants.
//!
//! A MALFORMED LIST REFUSES TO START. An entry that is not a DNS label cannot
//! name a namespace, and silently dropping it would leave a namespace the
//! operator meant to protect unreconciled with no sign of why.
//! [`configured`] is the pure predicate; `main` reads the variable once and
//! exits on the error.

use std::sync::OnceLock;

use futures::future::join_all;
use k8s_openapi::NamespaceResourceScope;
use kube::api::ListParams;
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;

/// The environment variable naming the watched namespaces.
pub const WATCH_NAMESPACES_ENV: &str = "LOGWEIR_WATCH_NAMESPACES";

/// Where the namespaced reconcilers watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchScope {
    /// Every namespace, through `Api::all` — the default, and the behaviour of
    /// every release before D0 stage 5.
    Cluster,
    /// Exactly these namespaces, sorted and deduplicated.
    Namespaces(Vec<String>),
}

impl WatchScope {
    /// One entry per watch a namespaced reconciler starts: `None` for the
    /// cluster-wide watch, `Some(namespace)` for each listed namespace.
    #[must_use]
    pub fn watches(&self) -> Vec<Option<String>> {
        match self {
            WatchScope::Cluster => vec![None],
            WatchScope::Namespaces(namespaces) => namespaces.iter().cloned().map(Some).collect(),
        }
    }

    /// Whether `namespace` is one this controller acts in.
    #[must_use]
    pub fn covers(&self, namespace: &str) -> bool {
        match self {
            WatchScope::Cluster => true,
            WatchScope::Namespaces(namespaces) => namespaces.iter().any(|n| n == namespace),
        }
    }
}

/// Parse the variable. Pure, so a test can hand it the empty string a
/// Kubernetes `env:` entry with an empty `value:` produces.
///
/// # Errors
///
/// A sentence naming the entry that is not a namespace name.
pub fn configured(raw: Result<String, std::env::VarError>) -> Result<WatchScope, String> {
    let Ok(raw) = raw else {
        return Ok(WatchScope::Cluster);
    };
    let mut namespaces: Vec<String> = Vec::new();
    for entry in raw.split([',', ' ', '\n', '\t']).map(str::trim) {
        if entry.is_empty() {
            continue;
        }
        if !is_dns_label(entry) {
            return Err(format!(
                "{WATCH_NAMESPACES_ENV} names `{entry}`, which is not a namespace name (a DNS \
                 label: lower-case letters, digits and `-`, at most 63 characters)"
            ));
        }
        if !namespaces.iter().any(|n| n == entry) {
            namespaces.push(entry.to_string());
        }
    }
    if namespaces.is_empty() {
        return Ok(WatchScope::Cluster);
    }
    namespaces.sort();
    Ok(WatchScope::Namespaces(namespaces))
}

fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

static SCOPE: OnceLock<WatchScope> = OnceLock::new();

/// Fix the scope for this process. `main` calls it once, before any
/// reconciler starts; a second call is ignored and returns `false`.
pub fn init(scope: WatchScope) -> bool {
    SCOPE.set(scope).is_ok()
}

/// The scope in force. A process (or a test) that never called [`init`]
/// watches the whole cluster, which is the pre-stage-5 behaviour.
#[must_use]
pub fn current() -> &'static WatchScope {
    SCOPE.get_or_init(|| WatchScope::Cluster)
}

/// The API handle for one watch: `Api::all` for the cluster-wide watch,
/// `Api::namespaced` for one namespace.
#[must_use]
pub fn api<K>(client: &Client, namespace: Option<&str>) -> Api<K>
where
    K: Resource<Scope = NamespaceResourceScope>,
    <K as Resource>::DynamicType: Default,
{
    match namespace {
        None => Api::all(client.clone()),
        Some(namespace) => Api::namespaced(client.clone(), namespace),
    }
}

/// Run one copy of a namespaced reconciler per watch in [`current`], until
/// the process ends.
///
/// Each copy is independent: its own watch, its own store, its own queue. A
/// reconciler that used `Controller::store()` as "every object of this kind"
/// now sees every object of this kind IN ITS NAMESPACE, which is exactly the
/// set it may act on.
pub async fn run_everywhere<F, Fut>(make: F)
where
    F: Fn(Option<String>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    join_all(current().watches().into_iter().map(make)).await;
}

/// Every object of a namespaced kind this controller may see: one cluster-wide
/// LIST, or one LIST per watched namespace, concatenated.
///
/// For the cross-object reads that must not miss an object (the retention
/// protection set, the installation-wide check ceiling). Call it with the kind
/// named, `list_everywhere::<Restore>(…)`, so the RBAC lint sees the `list`.
/// The list parameters
/// apply to each call; there is no pagination across namespaces, exactly as
/// there was none across the cluster.
///
/// # Errors
///
/// The first [`kube::Error`].
pub async fn list_everywhere<K>(client: &Client, params: &ListParams) -> Result<Vec<K>, kube::Error>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + std::fmt::Debug,
    <K as Resource>::DynamicType: Default,
{
    let mut out = Vec::new();
    for namespace in current().watches() {
        // NO `let x: Api<K>` BINDING HERE, deliberately: `manifest_lint.rs`
        // maps every typed `Api<T>` binding in this crate to an RBAC resource,
        // and a generic `K` has none. Callers name the kind with a turbofish —
        // `list_everywhere::<Restore>(…)` — which is what that lint reads as a
        // `list` on the kind.
        out.extend(
            api::<K>(client, namespace.as_deref())
                .list(params)
                .await?
                .items,
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_or_empty_is_the_whole_cluster() {
        assert_eq!(
            configured(Err(std::env::VarError::NotPresent)),
            Ok(WatchScope::Cluster)
        );
        assert_eq!(configured(Ok(String::new())), Ok(WatchScope::Cluster));
        assert_eq!(configured(Ok(" , \n".into())), Ok(WatchScope::Cluster));
        assert_eq!(WatchScope::Cluster.watches(), vec![None]);
        assert!(WatchScope::Cluster.covers("anything"));
    }

    #[test]
    fn a_list_is_sorted_deduplicated_and_exact() {
        let scope = configured(Ok("team-b, team-a team-b\nteam-c".into())).unwrap();
        assert_eq!(
            scope,
            WatchScope::Namespaces(vec!["team-a".into(), "team-b".into(), "team-c".into()])
        );
        assert_eq!(
            scope.watches(),
            vec![
                Some("team-a".to_string()),
                Some("team-b".to_string()),
                Some("team-c".to_string())
            ]
        );
        assert!(scope.covers("team-a"));
        // EXACT: no prefix, no pattern, and the release namespace is not
        // implied.
        assert!(!scope.covers("team"));
        assert!(!scope.covers("logweir-system"));
    }

    #[test]
    fn an_entry_that_is_not_a_namespace_refuses_to_start() {
        for bad in [
            "Team-A",
            "team_a",
            "-a",
            "a-",
            "team-a/x",
            "*",
            &"a".repeat(64),
        ] {
            let error = configured(Ok(format!("team-b,{bad}"))).unwrap_err();
            assert!(error.contains(WATCH_NAMESPACES_ENV), "{error}");
        }
    }
}
