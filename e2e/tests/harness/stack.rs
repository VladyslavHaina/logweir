//! **Which compose stack this process addresses** (PROD-01.5).
//!
//! The compose project name and every published host port of
//! `e2e/compose/docker-compose.yml` are parameters, so two stacks can run side
//! by side on one Docker host. Each parameter's DEFAULT is the value the stack
//! was pinned to before it became a parameter, so a process that sets none of
//! them addresses exactly the stack it always did.
//!
//! **There is one list of those variables**, `e2e/compose/stack-lib.sh`, and
//! this module COMPILES IT IN (`STACK_LIB`) rather than keeping a copy: the
//! port table, the stride, the slot cap and the default project below are all
//! read from it. `e2e/tests/stack_params.rs` checks the compose file's
//! `${VAR:-N}` spellings against the same list, and runs the shell's and this
//! module's coherence rules over one matrix of environments.
//!
//! Every accessor that leads to the stack (`project()`, `port()` and so the
//! addresses, the scratch directory, and the harness's compose calls) first
//! runs [`ensure_coherent`], which PANICS on an environment that is not one
//! coherent stack — before anything dials a port or runs `docker compose`.
//!
//! No `#![cfg]` here on purpose: the harness includes it under the `e2e`
//! feature, and `stack_params.rs` includes it in the DEFAULT test set.

use std::path::PathBuf;
use std::sync::OnceLock;

/// THE ONE LIST: `e2e/compose/stack-lib.sh`, compiled in.
pub const STACK_LIB: &str = include_str!("../../compose/stack-lib.sh");

/// `docker compose` reads this ahead of the file's `name:`, so it is the
/// project parameter; nothing Logweir-specific is layered on top of it.
pub const PROJECT_VAR: &str = "COMPOSE_PROJECT_NAME";

/// The port variables the harness reads by name (defaults: the list's).
pub const KAFKA_PORT: &str = "LOGWEIR_E2E_KAFKA_PORT";
pub const K8S_PORT: &str = "LOGWEIR_E2E_K8S_PORT";
pub const SASL_PORT: &str = "LOGWEIR_E2E_SASL_PORT";
pub const S3_PORT: &str = "LOGWEIR_E2E_S3_PORT";
/// Profile `acl` (FX-4): kafka-acl's PLAINTEXT (super user) and SCRAM
/// (restricted) host ports.
pub const ACL_PORT: &str = "LOGWEIR_E2E_ACL_PORT";
pub const ACL_SASL_PORT: &str = "LOGWEIR_E2E_ACL_SASL_PORT";
/// Profile `cluster2`: kafka-cluster2's host port (PROD-15.1 is its first Rust
/// reader).
pub const CLUSTER2_PORT: &str = "LOGWEIR_E2E_CLUSTER2_PORT";
/// Profile `autocreate` (PROD-15.1): kafka-autocreate's host port.
pub const AUTOCREATE_PORT: &str = "LOGWEIR_E2E_AUTOCREATE_PORT";
/// Profile `cluster3` (FX-3 is its first Rust reader): the three nodes'
/// host-side PLAINTEXT ports, each advertised as `localhost:<port>`.
pub const C3_PORTS: [&str; 3] = [
    "LOGWEIR_E2E_C3_1_PORT",
    "LOGWEIR_E2E_C3_2_PORT",
    "LOGWEIR_E2E_C3_3_PORT",
];

/// One row of the list: a published host port's variable, its default, and
/// the optional profile that publishes it (`None` for the always-on services).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortParam {
    pub var: &'static str,
    pub default: u16,
    pub profile: Option<&'static str>,
}

/// `NAME=value` from the list, for its scalar settings.
fn lib_scalar(name: &str) -> &'static str {
    STACK_LIB
        .lines()
        .find_map(|l| l.strip_prefix(name).and_then(|r| r.strip_prefix('=')))
        .map(str::trim)
        .unwrap_or_else(|| panic!("e2e/compose/stack-lib.sh defines no {name}="))
}

/// The DEFAULT stack's project (the compose file's `name:`).
pub fn default_project() -> &'static str {
    lib_scalar("LW_E2E_DEFAULT_PROJECT")
}

/// Slot N moves every port by `stride() * N`.
pub fn stride() -> u16 {
    lib_scalar("LW_E2E_STRIDE")
        .parse()
        .expect("LW_E2E_STRIDE is a number")
}

/// The highest slot (the owner's cap on parallel agents is four).
pub fn max_slot() -> u16 {
    lib_scalar("LW_E2E_MAX_SLOT")
        .parse()
        .expect("LW_E2E_MAX_SLOT is a number")
}

/// Every row of the list's port table, in its order.
pub fn all_ports() -> Vec<PortParam> {
    let block = STACK_LIB
        .split("LW_E2E_PORT_TABLE=\"")
        .nth(1)
        .and_then(|r| r.split('"').next())
        .expect("e2e/compose/stack-lib.sh carries LW_E2E_PORT_TABLE=\"…\"");
    block
        .lines()
        .filter_map(|l| {
            let mut w = l.split_whitespace();
            let var = w.next()?;
            let default = w.next()?;
            let profile = w.next()?;
            Some(PortParam {
                var,
                default: default
                    .parse()
                    .unwrap_or_else(|_| panic!("{var}'s default {default:?} is not a port")),
                profile: (profile != "-").then_some(profile),
            })
        })
        .collect()
}

/// The rows of the always-on services.
pub fn core_ports() -> Vec<PortParam> {
    all_ports()
        .into_iter()
        .filter(|p| p.profile.is_none())
        .collect()
}

/// The list's default for port variable `var`.
pub fn default_of(var: &str) -> u16 {
    all_ports()
        .into_iter()
        .find(|p| p.var == var)
        .map(|p| p.default)
        .unwrap_or_else(|| panic!("{var} is not in e2e/compose/stack-lib.sh's port table"))
}

/// The project name slot `n` uses: the default for 0, `logweir-e2e-s<n>` after.
pub fn slot_project(n: u16) -> String {
    if n == 0 {
        default_project().to_string()
    } else {
        format!("{}-s{n}", default_project())
    }
}

/// The host port slot `n` gives `p`.
pub fn slot_port(p: &PortParam, n: u16) -> u16 {
    p.default + stride() * n
}

/// The slot of `project`: 0 for the default project, N for `logweir-e2e-sN`
/// (1..=max_slot), `None` for any other name.
pub fn slot_of(project: &str) -> Option<u16> {
    if project == default_project() {
        return Some(0);
    }
    let n: u16 = project
        .strip_prefix(default_project())?
        .strip_prefix("-s")?
        .parse()
        .ok()?;
    (1..=max_slot()).contains(&n).then_some(n)
}

/// **Why the environment `get` describes is not ONE coherent stack**, or
/// `None` when it is. PURE — the environment is a parameter — so the same
/// rule can be tested over any matrix; `names` are the environment's variable
/// names (for the unknown-variable check).
///
/// Coherent means: the project is the default (or unset) or `logweir-e2e-sN`,
/// and EVERY port in the list is exactly that slot's (unset only counts as
/// the default, so only on slot 0); no other `LOGWEIR_E2E_*_PORT` is set. The
/// shell twin is `lw_e2e_check_coherent` in `e2e/compose/stack-lib.sh`.
pub fn incoherence_in(get: &dyn Fn(&str) -> Option<String>, names: &[String]) -> Option<String> {
    let mut bad = Vec::new();
    let project = get(PROJECT_VAR)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default_project().to_string());
    let slot = slot_of(&project);
    if slot.is_none() {
        bad.push(format!(
            "{PROJECT_VAR}={project} is not a stack project ({dp} is slot 0, {dp}-s1..s{m} are \
             slots 1..{m})",
            dp = default_project(),
            m = max_slot()
        ));
    }
    let ports = all_ports();
    for p in &ports {
        let val = get(p.var)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        match (val, slot) {
            (None, Some(n)) if n != 0 => bad.push(format!(
                "{} is unset, but {project} is slot {n}, which needs {}",
                p.var,
                slot_port(p, n)
            )),
            (None, _) => {}
            (Some(v), _) if v.parse::<u16>().is_err() => {
                bad.push(format!("{}={v} is not a TCP port", p.var))
            }
            (Some(v), Some(n)) => {
                let want = slot_port(p, n);
                if v.parse::<u16>() != Ok(want) {
                    bad.push(format!(
                        "{}={v}, but {project} is slot {n}, which needs {want}",
                        p.var
                    ));
                }
            }
            (Some(_), None) => {}
        }
    }
    for name in names {
        if name.starts_with("LOGWEIR_E2E_")
            && name.ends_with("_PORT")
            && !ports.iter().any(|p| p.var == name)
        {
            bad.push(format!(
                "{name} is not a stack variable (e2e/compose/stack-lib.sh lists them)"
            ));
        }
    }
    (!bad.is_empty()).then(|| bad.join("; "))
}

/// [`incoherence_in`] over this process's environment.
pub fn incoherence() -> Option<String> {
    let names: Vec<String> = std::env::vars_os()
        .filter_map(|(k, _)| k.into_string().ok())
        .collect();
    incoherence_in(&|k| std::env::var(k).ok(), &names)
}

/// PANICS, naming every reason, unless this process's environment is one
/// coherent stack. Checked once per process; every accessor below that leads
/// to the stack calls it, and so do the harness's `docker compose` helpers, so
/// a half-set environment fails a test before it dials a port or reaches a
/// project it did not mean (PROD-01.5, review M1).
pub fn ensure_coherent() {
    static CHECKED: OnceLock<Option<String>> = OnceLock::new();
    if let Some(why) = CHECKED.get_or_init(incoherence) {
        panic!(
            "e2e stack: this environment is not one coherent stack: {why}. Set all of them at \
             once: eval \"$(e2e/compose/stack-env.sh --slot N)\" (N=0 is the default stack)"
        );
    }
}

/// The project this process addresses.
pub fn project() -> String {
    ensure_coherent();
    match std::env::var(PROJECT_VAR) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => default_project().to_string(),
    }
}

/// The host port this process uses for port variable `var` (checked coherent
/// first, so a value that is not a port has already been refused).
pub fn port(var: &str) -> u16 {
    ensure_coherent();
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse().expect("checked by ensure_coherent"),
        _ => default_of(var),
    }
}

/// True when this process addresses the default stack: the default project.
pub fn is_default_stack() -> bool {
    project() == default_project()
}

/// The host-side plaintext bootstrap (`EXTERNAL`), as the broker advertises it.
pub fn bootstrap() -> String {
    format!("localhost:{}", port(KAFKA_PORT))
}

/// The host-side SCRAM bootstrap (`SASLEXT`), as the broker advertises it.
pub fn bootstrap_sasl() -> String {
    format!("localhost:{}", port(SASL_PORT))
}

/// The pod-side bootstrap (`K8S`), as the broker advertises it: the same
/// `LOGWEIR_K8S_ADVERTISED_HOST` the compose file reads, and the slot's port.
pub fn bootstrap_k8s() -> String {
    let host = match std::env::var("LOGWEIR_K8S_ADVERTISED_HOST") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => "host.docker.internal".to_string(),
    };
    format!("{host}:{}", port(K8S_PORT))
}

/// MinIO's S3 endpoint as a host-side client reaches it.
pub fn s3_endpoint() -> String {
    format!("http://localhost:{}", port(S3_PORT))
}

/// Profile `acl`: kafka-acl's host-side PLAINTEXT bootstrap, where every
/// client is User:ANONYMOUS, a SUPER USER (FX-4).
pub fn bootstrap_acl() -> String {
    format!("localhost:{}", port(ACL_PORT))
}

/// Profile `acl`: kafka-acl's host-side SASL_PLAINTEXT/SCRAM-SHA-512
/// bootstrap, where `logweir` is the RESTRICTED principal a row's ACLs name.
pub fn bootstrap_acl_sasl() -> String {
    format!("localhost:{}", port(ACL_SASL_PORT))
}

/// Profile `autocreate`: kafka-autocreate's host-side PLAINTEXT bootstrap, a
/// cluster whose brokers AUTO-CREATE topics (PROD-15.1).
pub fn bootstrap_autocreate() -> String {
    format!("localhost:{}", port(AUTOCREATE_PORT))
}

/// Profile `cluster2`: kafka-cluster2's host-side PLAINTEXT bootstrap, a
/// second independent cluster with auto-creation off.
pub fn bootstrap_cluster2() -> String {
    format!("localhost:{}", port(CLUSTER2_PORT))
}

/// Profile `cluster3`: the three-node cluster's host-side PLAINTEXT
/// bootstrap, all three nodes, comma-separated (replication factor 3 and
/// `min.insync.replicas` 2 by default).
pub fn bootstrap_c3() -> String {
    C3_PORTS
        .iter()
        .map(|var| format!("localhost:{}", port(var)))
        .collect::<Vec<_>>()
        .join(",")
}

/// A per-stack scratch directory under `base` (relative to the workspace
/// root): `base` itself for the default stack — byte for byte where it always
/// was — and `base/<project>` for any other, so two stacks driven from ONE
/// checkout never share an approval, an allowlist or a scorecard file.
pub fn scratch_dir(base: &str) -> PathBuf {
    if is_default_stack() {
        PathBuf::from(base)
    } else {
        PathBuf::from(base).join(project())
    }
}

/// The default-stack addresses a checked-in spec names, and what this stack
/// calls them — the pairs `rebind_addresses` rewrites.
pub fn address_rebinds() -> Vec<(String, String)> {
    vec![
        (format!("localhost:{}", default_of(KAFKA_PORT)), bootstrap()),
        (
            format!("http://localhost:{}", default_of(S3_PORT)),
            s3_endpoint(),
        ),
    ]
}
