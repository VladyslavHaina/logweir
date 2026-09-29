//! **Which compose stack this process addresses** (PROD-01.5).
//!
//! The compose project name and every published host port of
//! `e2e/compose/docker-compose.yml` are parameters, so two stacks can run side
//! by side on one Docker host. Each parameter's DEFAULT is the value the stack
//! was pinned to before it became a parameter, so a process that sets none of
//! them addresses exactly the stack it always did.
//!
//! This file is the Rust half of a three-way agreement:
//!
//! * `e2e/compose/docker-compose.yml` spells each default as `${VAR:-N}`;
//! * `e2e/compose/stack-lib.sh` resolves the same variables for the scripts;
//! * this module resolves them for the harness.
//!
//! `e2e/tests/stack_params.rs` reads all three and fails if any default, any
//! variable name or the slot rule differs, so they cannot drift apart silently
//! (WORKER-RULES "a fixture two sides must agree on is read by both sides").
//!
//! No `#![cfg]` here on purpose: the harness includes it under the `e2e`
//! feature, and `stack_params.rs` includes it in the DEFAULT test set.

use std::path::PathBuf;

/// The compose project a process with no `COMPOSE_PROJECT_NAME` addresses —
/// the top-level `name:` of `e2e/compose/docker-compose.yml`.
pub const DEFAULT_PROJECT: &str = "logweir-e2e";

/// `docker compose` reads this ahead of the file's `name:`, so it is the
/// project parameter; nothing Logweir-specific is layered on top of it.
pub const PROJECT_VAR: &str = "COMPOSE_PROJECT_NAME";

/// One published host port: the variable that moves it and its default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortParam {
    pub var: &'static str,
    pub default: u16,
    /// The profile that publishes it; `None` for the always-on services.
    pub profile: Option<&'static str>,
    /// What listens behind it, for messages and the guide.
    pub what: &'static str,
}

/// `EXTERNAL` — the host-side plaintext bootstrap. Published AND advertised.
pub const KAFKA_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_KAFKA_PORT",
    default: 9092,
    profile: None,
    what: "kafka-broker-1 EXTERNAL (plaintext, host side)",
};
/// `K8S` — the listener a pod on docker-desktop dials. Published AND advertised.
pub const K8S_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_K8S_PORT",
    default: 9095,
    profile: None,
    what: "kafka-broker-1 K8S (plaintext, for pods)",
};
/// `SASLEXT` — the host-side SCRAM-SHA-512 bootstrap. Published AND advertised.
pub const SASL_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_SASL_PORT",
    default: 9097,
    profile: None,
    what: "kafka-broker-1 SASLEXT (SCRAM-SHA-512, host side)",
};
/// MinIO's S3 API.
pub const S3_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_S3_PORT",
    default: 9000,
    profile: None,
    what: "minio S3 API",
};
/// MinIO's console.
pub const S3_CONSOLE_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_S3_CONSOLE_PORT",
    default: 9001,
    profile: None,
    what: "minio console",
};

/// Every port the always-on services publish.
pub const CORE_PORTS: [PortParam; 5] = [KAFKA_PORT, K8S_PORT, SASL_PORT, S3_PORT, S3_CONSOLE_PORT];

/// Every port an OPTIONAL profile publishes (`COMPOSE_PROFILES`). Published
/// only while that profile is active, and moved by a slot like the rest.
pub const PROFILE_PORTS: [PortParam; 8] = [
    AUTH_PLAIN_PORT,
    AUTH_SCRAM256_PORT,
    AUTH_MTLS_PORT,
    C3_1_PORT,
    C3_2_PORT,
    C3_3_PORT,
    CLUSTER2_PORT,
    OBJSTORE_PORT,
];

/// `auth`: SASL_SSL with PLAIN on `kafka-auth`. Published AND advertised.
pub const AUTH_PLAIN_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_AUTH_PLAIN_PORT",
    default: 9102,
    profile: Some("auth"),
    what: "kafka-auth PLAINTLS (SASL_SSL, PLAIN)",
};
/// `auth`: SASL_PLAINTEXT with SCRAM-SHA-256 on `kafka-auth`.
pub const AUTH_SCRAM256_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_AUTH_SCRAM256_PORT",
    default: 9103,
    profile: Some("auth"),
    what: "kafka-auth SCRAM256 (SASL_PLAINTEXT, SCRAM-SHA-256)",
};
/// `auth`: SSL with a required client certificate on `kafka-auth`.
pub const AUTH_MTLS_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_AUTH_MTLS_PORT",
    default: 9104,
    profile: Some("auth"),
    what: "kafka-auth MTLS (SSL, client certificate required)",
};
/// `cluster3`: node 1's host-side plaintext listener.
pub const C3_1_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_C3_1_PORT",
    default: 9112,
    profile: Some("cluster3"),
    what: "kafka-c3-1 EXTERNAL (plaintext)",
};
/// `cluster3`: node 2's host-side plaintext listener.
pub const C3_2_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_C3_2_PORT",
    default: 9113,
    profile: Some("cluster3"),
    what: "kafka-c3-2 EXTERNAL (plaintext)",
};
/// `cluster3`: node 3's host-side plaintext listener.
pub const C3_3_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_C3_3_PORT",
    default: 9114,
    profile: Some("cluster3"),
    what: "kafka-c3-3 EXTERNAL (plaintext)",
};
/// `objectstore`: SeaweedFS's S3 API (container port 8333).
pub const OBJSTORE_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_OBJSTORE_PORT",
    default: 9130,
    profile: Some("objectstore"),
    what: "objectstore S3 API (SeaweedFS)",
};
/// `cluster2`: the second cluster's host-side plaintext listener.
pub const CLUSTER2_PORT: PortParam = PortParam {
    var: "LOGWEIR_E2E_CLUSTER2_PORT",
    default: 9122,
    profile: Some("cluster2"),
    what: "kafka-cluster2 EXTERNAL (plaintext)",
};

/// The whole port table, in the order `e2e/compose/stack-env.sh` lists it.
pub fn all_ports() -> Vec<PortParam> {
    CORE_PORTS
        .iter()
        .chain(PROFILE_PORTS.iter())
        .copied()
        .collect()
}

/// A slot adds `SLOT_STRIDE * n` to every default port. Slots 1..=4 keep every
/// port below 49152, the first ephemeral port on macOS, so a slot never races
/// an outbound connection for its listening port on the development host.
pub const SLOT_STRIDE: u16 = 10_000;
/// The highest slot `e2e/compose/stack-env.sh` hands out (the owner's cap on
/// parallel agents is four).
pub const MAX_SLOT: u16 = 4;

/// The project name slot `n` uses: the default for 0, `logweir-e2e-s<n>` after.
pub fn slot_project(n: u16) -> String {
    if n == 0 {
        DEFAULT_PROJECT.to_string()
    } else {
        format!("{DEFAULT_PROJECT}-s{n}")
    }
}

/// The host port slot `n` gives `p`.
pub fn slot_port(p: &PortParam, n: u16) -> u16 {
    p.default + SLOT_STRIDE * n
}

/// The project this process addresses.
pub fn project() -> String {
    match std::env::var(PROJECT_VAR) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => DEFAULT_PROJECT.to_string(),
    }
}

/// The host port this process uses for `p`. A value that is not a port is a
/// configuration error and PANICS, naming the variable: silently falling back
/// to the default would point this process at somebody else's stack.
pub fn port(p: &PortParam) -> u16 {
    match std::env::var(p.var) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse().unwrap_or_else(|_| {
            panic!(
                "{}={v:?} is not a TCP port; unset it or run \
                 `eval \"$(e2e/compose/stack-env.sh --slot N)\"`",
                p.var
            )
        }),
        _ => p.default,
    }
}

/// True when this process addresses the default stack: the default project.
pub fn is_default_stack() -> bool {
    project() == DEFAULT_PROJECT
}

/// Why this environment is NOT a coherent stack, or `None` when it is.
///
/// A non-default project on a default port collides with the default stack's
/// published port. The reverse — the DEFAULT project with a moved port — is
/// the dangerous one: `docker compose up` would RECREATE the default stack's
/// containers with the new ports under whoever is using them, and
/// `docker compose down` would remove them. `e2e/compose/stack-env.sh --check`
/// applies the same rule before `just e2e-up` / `just e2e-down`.
pub fn incoherence() -> Option<String> {
    let default = is_default_stack();
    let mut bad = Vec::new();
    for p in CORE_PORTS {
        let v = port(&p);
        if default && v != p.default {
            bad.push(format!(
                "{}={v} but the project is the default {DEFAULT_PROJECT}",
                p.var
            ));
        }
        if !default && v == p.default {
            bad.push(format!(
                "{}={v} is the DEFAULT stack's port but the project is {}",
                p.var,
                project()
            ));
        }
    }
    if bad.is_empty() {
        None
    } else {
        Some(bad.join("; "))
    }
}

/// The host-side plaintext bootstrap (`EXTERNAL`), as the broker advertises it.
pub fn bootstrap() -> String {
    format!("localhost:{}", port(&KAFKA_PORT))
}

/// The host-side SCRAM bootstrap (`SASLEXT`), as the broker advertises it.
pub fn bootstrap_sasl() -> String {
    format!("localhost:{}", port(&SASL_PORT))
}

/// The pod-side bootstrap (`K8S`), as the broker advertises it: the same
/// `LOGWEIR_K8S_ADVERTISED_HOST` the compose file reads, and the slot's port.
pub fn bootstrap_k8s() -> String {
    let host = match std::env::var("LOGWEIR_K8S_ADVERTISED_HOST") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => "host.docker.internal".to_string(),
    };
    format!("{host}:{}", port(&K8S_PORT))
}

/// MinIO's S3 endpoint as a host-side client reaches it.
pub fn s3_endpoint() -> String {
    format!("http://localhost:{}", port(&S3_PORT))
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
        (format!("localhost:{}", KAFKA_PORT.default), bootstrap()),
        (
            format!("http://localhost:{}", S3_PORT.default),
            s3_endpoint(),
        ),
    ]
}
