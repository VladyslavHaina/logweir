//! **PROD-01.5: the compose stack is parameterized, and its readers agree.**
//!
//! Two stacks can run side by side only if every published host port and the
//! project name are parameters, AND every reader of the stack — the compose
//! file, the scripts' `e2e/compose/stack-lib.sh`, the slot helper
//! `e2e/compose/stack-env.sh`, and the harness's `harness/stack.rs` — agrees
//! on the variable names and their defaults. A reader that silently kept a
//! literal `9092` would talk to WHOEVER owns the default stack; that is the
//! defect class this file guards.
//!
//! The text rows run in the DEFAULT test set: no Docker, no stack, no network.
//! The render rows at the bottom need Docker (`docker compose config` only —
//! no container is started) and sit behind the `e2e` feature.

#[allow(dead_code)]
#[path = "harness/stack.rs"]
mod stack;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .expect("the workspace root resolves")
}

fn read(rel: &str) -> String {
    let p = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

const COMPOSE: &str = "e2e/compose/docker-compose.yml";
const STACK_LIB: &str = "e2e/compose/stack-lib.sh";
const STACK_ENV: &str = "e2e/compose/stack-env.sh";

/// Profiles `just e2e-up` runs itself; every OTHER profile is user-facing and
/// must be listed by `stack-env.sh --profiles-list`.
const INTERNAL_PROFILES: [&str; 2] = ["setup", "tools"];

fn compose_yaml() -> serde_yaml::Value {
    serde_yaml::from_str(&read(COMPOSE)).expect("the compose file is YAML")
}

/// Every `${VAR:-DEFAULT}` in `text` whose VAR is a `LOGWEIR_E2E_*_PORT`, with
/// each default it is spelled with (a variable spelled with two different
/// defaults is itself a finding).
fn port_defaults_in(text: &str) -> BTreeMap<String, BTreeSet<u16>> {
    let mut out: BTreeMap<String, BTreeSet<u16>> = BTreeMap::new();
    let mut rest = text;
    while let Some(i) = rest.find("${LOGWEIR_E2E_") {
        rest = &rest[i + 2..];
        let end = rest.find('}').expect("an interpolation closes");
        let inner = &rest[..end];
        if let Some((var, default)) = inner.split_once(":-") {
            if var.ends_with("_PORT") {
                let d: u16 = default
                    .parse()
                    .unwrap_or_else(|_| panic!("{var}'s default {default:?} is not a port"));
                out.entry(var.to_string()).or_default().insert(d);
            }
        } else if inner.ends_with("_PORT") {
            panic!("${{{inner}}} in {COMPOSE} has NO default, so an unset variable renders an empty port");
        }
        rest = &rest[end..];
    }
    out
}

fn registry() -> BTreeMap<String, u16> {
    stack::all_ports()
        .into_iter()
        .map(|p| (p.var.to_string(), p.default))
        .collect()
}

// ---------------------------------------------------------------------------
// 1. The compose file agrees with the harness.
// ---------------------------------------------------------------------------

/// The project name and every port default the compose file spells are the
/// harness's, and there is no port variable one side has and the other lacks.
///
/// Mutant: change `${LOGWEIR_E2E_S3_PORT:-9000}` to `:-9002` in the compose
/// file → fails naming `LOGWEIR_E2E_S3_PORT`.
#[test]
fn the_compose_defaults_are_the_harness_defaults() {
    let text = read(COMPOSE);
    assert!(
        text.lines()
            .any(|l| l == format!("name: {}", stack::DEFAULT_PROJECT)),
        "{COMPOSE} must keep `name: {}` — COMPOSE_PROJECT_NAME is the override, the file \
         carries the default",
        stack::DEFAULT_PROJECT
    );
    let found = port_defaults_in(&text);
    let want = registry();
    for (var, defaults) in &found {
        assert_eq!(
            defaults.len(),
            1,
            "{var} is spelled with {} different defaults in {COMPOSE}: {defaults:?}",
            defaults.len()
        );
        let d = *defaults.iter().next().unwrap();
        assert_eq!(
            want.get(var),
            Some(&d),
            "{COMPOSE} spells ${{{var}:-{d}}} but e2e/tests/harness/stack.rs says {:?}",
            want.get(var)
        );
    }
    let missing: Vec<_> = want.keys().filter(|v| !found.contains_key(*v)).collect();
    assert!(
        missing.is_empty(),
        "stack.rs names port variable(s) the compose file never reads: {missing:?}"
    );
}

/// Every published host port is a parameter: each `ports:` entry of every
/// service reads `"${LOGWEIR_E2E_*_PORT:-N}:<container port>"`. A literal host
/// port is what makes two stacks collide.
///
/// Mutant: restore `ports: ["9092:9092", …]` on `kafka-broker-1` → fails
/// naming the service and the entry.
#[test]
fn every_published_host_port_is_a_parameter() {
    let doc = compose_yaml();
    let services = doc["services"].as_mapping().expect("services:");
    let mut seen = 0;
    let mut bad = Vec::new();
    for (name, svc) in services {
        let Some(ports) = svc.get("ports").and_then(|p| p.as_sequence()) else {
            continue;
        };
        for p in ports {
            seen += 1;
            let s = p.as_str().unwrap_or_else(|| {
                panic!("{name:?}: a `ports:` entry that is not a short-syntax string: {p:?}")
            });
            let ok = s.starts_with("${LOGWEIR_E2E_")
                && s.contains("_PORT:-")
                && s.split("}:")
                    .nth(1)
                    .is_some_and(|c| c.parse::<u16>().is_ok());
            if !ok {
                bad.push(format!("{}: {s}", name.as_str().unwrap_or("?")));
            }
        }
    }
    assert!(
        seen >= 5,
        "found only {seen} published ports — reading the wrong file?"
    );
    assert!(
        bad.is_empty(),
        "published host port(s) that are not a LOGWEIR_E2E_*_PORT parameter: {bad:#?}"
    );
}

/// A broker's host-facing advertisement carries the SAME variable as the host
/// port it is published on. Otherwise a client bootstrapped on a slot's port
/// is redirected by the broker's metadata to the DEFAULT port — another
/// stack, or nothing.
///
/// Mutant: advertise `EXTERNAL://localhost:9092` while publishing
/// `${LOGWEIR_E2E_KAFKA_PORT:-9092}:9092` → fails naming the listener.
#[test]
fn every_host_facing_advertisement_moves_with_its_published_port() {
    let doc = compose_yaml();
    let services = doc["services"].as_mapping().expect("services:");
    let mut checked = 0;
    for (name, svc) in services {
        let name = name.as_str().unwrap_or("?");
        let env = &svc["environment"];
        let Some(adv) = env
            .get("KAFKA_ADVERTISED_LISTENERS")
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        let published: BTreeSet<String> = svc
            .get("ports")
            .and_then(|p| p.as_sequence())
            .map(|ports| {
                ports
                    .iter()
                    .filter_map(|p| p.as_str())
                    .filter_map(|s| s.strip_prefix("${").and_then(|r| r.split(":-").next()))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        for entry in adv.split(',') {
            let (listener, addr) = entry.split_once("://").expect("NAME://host:port");
            let host_facing = addr.starts_with("localhost:")
                || addr.starts_with("host.docker.internal:")
                || addr.starts_with("${LOGWEIR_K8S_ADVERTISED_HOST");
            if !host_facing {
                continue;
            }
            checked += 1;
            let port_part = addr.rsplit_once(':').map(|(_, p)| p).unwrap_or("");
            // `${VAR:-N}` — the rsplit above lands inside it, so re-find it.
            let var = addr
                .rfind("${LOGWEIR_E2E_")
                .map(|i| &addr[i + 2..])
                .and_then(|r| r.split(":-").next());
            match var {
                Some(v) => assert!(
                    published.contains(v),
                    "{name}: {listener} is advertised on ${{{v}}} but the service publishes \
                     {published:?} — the advertisement and the published port must be the same \
                     variable"
                ),
                None => panic!(
                    "{name}: {listener}://{addr} is a HOST-FACING advertisement with a literal \
                     port ({port_part}); it must be the published port's LOGWEIR_E2E_*_PORT \
                     variable"
                ),
            }
        }
    }
    assert!(
        checked >= 3,
        "checked only {checked} host-facing advertisement(s) — kafka-broker-1 alone has three"
    );
}

// ---------------------------------------------------------------------------
// 2. The scripts' library and the slot helper agree with the harness.
// ---------------------------------------------------------------------------

/// `stack-lib.sh` resolves the same variables with the same defaults, in both
/// of the shapes it spells them (the assignment and the coherence tuple).
///
/// Mutant: `LW_E2E_S3_PORT="${LOGWEIR_E2E_S3_PORT:-9002}"` → fails.
#[test]
fn the_shell_library_agrees_with_the_harness() {
    let lib = read(STACK_LIB);
    assert!(
        lib.lines()
            .any(|l| l == format!("LW_E2E_DEFAULT_PROJECT={}", stack::DEFAULT_PROJECT)),
        "{STACK_LIB} must set LW_E2E_DEFAULT_PROJECT={}",
        stack::DEFAULT_PROJECT
    );
    assert!(
        lib.lines().any(|l| l
            == format!(
                "LW_E2E_PROJECT=\"${{{}:-{}}}\"",
                stack::PROJECT_VAR,
                stack::DEFAULT_PROJECT
            )),
        "{STACK_LIB} must resolve the project from {} with the default {}",
        stack::PROJECT_VAR,
        stack::DEFAULT_PROJECT
    );
    for p in stack::CORE_PORTS {
        let assignment = format!("=\"${{{}:-{}}}\"", p.var, p.default);
        assert!(
            lib.lines()
                .any(|l| l.starts_with("LW_E2E_") && l.ends_with(&assignment)),
            "{STACK_LIB} does not resolve {} with the default {} (want a line ending {assignment})",
            p.var,
            p.default
        );
        let tuple = format!("\"{}:$", p.var);
        let line = lib
            .lines()
            .find(|l| l.trim_start().starts_with(&tuple))
            .unwrap_or_else(|| {
                panic!(
                    "{STACK_LIB}'s lw_e2e_check_coherent does not check {} — a half-set slot \
                     on that port would pass",
                    p.var
                )
            });
        assert!(
            line.contains(&format!(":{}\"", p.default)),
            "{STACK_LIB}'s coherence tuple for {} does not carry the default {}: {line}",
            p.var,
            p.default
        );
    }
}

/// The slot helper's port table IS the registry, its stride and cap are the
/// harness's, and its broker lines are well formed.
#[test]
fn the_slot_helper_agrees_with_the_harness() {
    let env = read(STACK_ENV);
    let block = env
        .split("PORTS=\"")
        .nth(1)
        .and_then(|r| r.split('"').next())
        .expect("stack-env.sh carries a PORTS=\"…\" table");
    let table: BTreeMap<String, u16> = block
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut w = l.split_whitespace();
            let var = w.next().unwrap().to_string();
            let d = w
                .next()
                .and_then(|d| d.parse().ok())
                .unwrap_or_else(|| panic!("PORTS row without a port: {l:?}"));
            (var, d)
        })
        .collect();
    assert_eq!(
        table,
        registry(),
        "{STACK_ENV}'s PORTS table and e2e/tests/harness/stack.rs disagree"
    );
    assert!(
        env.lines()
            .any(|l| l == format!("STRIDE={}", stack::SLOT_STRIDE)),
        "{STACK_ENV} must use STRIDE={}",
        stack::SLOT_STRIDE
    );
    assert!(
        env.lines()
            .any(|l| l == format!("MAX_SLOT={}", stack::MAX_SLOT)),
        "{STACK_ENV} must cap slots at {}",
        stack::MAX_SLOT
    );
    assert!(
        env.contains(&format!(
            "export COMPOSE_PROJECT_NAME={}-s$slot",
            stack::DEFAULT_PROJECT
        )),
        "{STACK_ENV} must name slot N's project {}-sN, as stack::slot_project does",
        stack::DEFAULT_PROJECT
    );
}

/// No two slots share a project or a host port, and every slot's port stays
/// below macOS's first ephemeral port (49152), where a listening port could
/// race an outbound connection.
#[test]
fn slots_never_share_a_project_or_a_port() {
    let mut projects = BTreeSet::new();
    let mut ports: BTreeMap<u16, String> = BTreeMap::new();
    for n in 0..=stack::MAX_SLOT {
        assert!(
            projects.insert(stack::slot_project(n)),
            "slot {n} reuses a project name"
        );
        for p in stack::all_ports() {
            let v = stack::slot_port(&p, n);
            assert!(
                v < 49152,
                "slot {n} puts {} on {v}, an ephemeral port",
                p.var
            );
            if let Some(other) = ports.insert(v, format!("slot {n} {}", p.var)) {
                panic!("port {v} is both {other} and slot {n} {}", p.var);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Profiles are torn down, and listed.
// ---------------------------------------------------------------------------

fn compose_profiles() -> BTreeSet<String> {
    let doc = compose_yaml();
    let mut out = BTreeSet::new();
    for (_, svc) in doc["services"].as_mapping().expect("services:") {
        if let Some(ps) = svc.get("profiles").and_then(|p| p.as_sequence()) {
            for p in ps {
                out.insert(p.as_str().expect("a profile name").to_string());
            }
        }
    }
    out
}

/// `just e2e-down` names every profile the compose file declares. `--profile`
/// flags REPLACE `COMPOSE_PROFILES` (measured on compose v5.0.2), and `down`
/// removes only the active profiles' containers — so a profile missing from
/// that line outlives every teardown.
///
/// Mutant: drop `--profile tools` from `e2e-down` → fails naming `tools`.
#[test]
fn e2e_down_names_every_profile() {
    let just = read("justfile");
    let mut lines = just.lines();
    lines
        .by_ref()
        .find(|l| l.starts_with("e2e-down:"))
        .expect("the justfile declares `e2e-down`");
    let down = lines
        .take_while(|l| l.starts_with(' ') || l.starts_with('\t'))
        .find(|l| l.contains(" down "))
        .expect("`e2e-down` runs `docker compose … down`");
    let named: BTreeSet<String> = down
        .split("--profile ")
        .skip(1)
        .filter_map(|r| r.split_whitespace().next())
        .map(str::to_string)
        .collect();
    let declared = compose_profiles();
    let missing: Vec<_> = declared.difference(&named).collect();
    assert!(
        missing.is_empty(),
        "`just e2e-down` does not name profile(s) {missing:?}; their containers would survive \
         `down -v`. The line is: {down}"
    );
}

/// Every user-facing profile is listed by `stack-env.sh --profiles-list`
/// (which is what `--profiles` validates against), and nothing it lists is
/// missing from the compose file.
#[test]
fn the_slot_helper_lists_exactly_the_user_facing_profiles() {
    let env = read(STACK_ENV);
    let block = env
        .split("PROFILES_LIST=\"")
        .nth(1)
        .and_then(|r| r.split('"').next())
        .expect("stack-env.sh carries a PROFILES_LIST=\"…\" table");
    let listed: BTreeSet<String> = block
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .map(str::to_string)
        .collect();
    let user_facing: BTreeSet<String> = compose_profiles()
        .into_iter()
        .filter(|p| !INTERNAL_PROFILES.contains(&p.as_str()))
        .collect();
    assert_eq!(
        listed, user_facing,
        "{STACK_ENV}'s PROFILES_LIST and the compose file's user-facing profiles disagree"
    );
}

// ---------------------------------------------------------------------------
// 4. The class sweep: no reader keeps a literal default address.
// ---------------------------------------------------------------------------

/// The default stack's host-side addresses, spelled as a reader would
/// hard-code them.
fn default_address_literals() -> Vec<String> {
    let mut v = Vec::new();
    for p in stack::CORE_PORTS {
        for host in ["localhost", "127.0.0.1", "host.docker.internal"] {
            v.push(format!("{host}:{}", p.default));
        }
    }
    v
}

/// Code lines (comment lines skipped) of `rel` that spell a default-stack
/// host address.
fn literal_addresses(rel: &str, text: &str, comment: &str) -> Vec<String> {
    let lits = default_address_literals();
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim_start().starts_with(comment))
        .filter_map(|(i, l)| {
            lits.iter()
                .find(|lit| l.contains(lit.as_str()))
                .map(|lit| format!("{rel}:{}: {lit}: {}", i + 1, l.trim()))
        })
        .collect()
}

/// **The sweep's guard.** No e2e suite, and none of the scripts that address
/// the stack, spells a default-stack host address in CODE: every one goes
/// through `harness/stack.rs` or `stack-lib.sh`, so a slot reaches its own
/// stack and never the default one. (Comments may still name the defaults.)
///
/// Mutant: put `"http://localhost:9000"` back into `pitr_boundary.rs`'s
/// restore spec → fails naming the file and line.
#[test]
fn no_stack_reader_hard_codes_a_default_host_address() {
    let mut found = Vec::new();
    let dir = root().join("e2e/tests");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("e2e/tests is readable")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.push(dir.join("harness/mod.rs"));
    files.push(dir.join("harness/stack.rs"));
    files.sort();
    let mut scanned = 0;
    for f in &files {
        let rel = f
            .strip_prefix(root())
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "e2e/tests/stack_params.rs" {
            continue;
        }
        scanned += 1;
        found.extend(literal_addresses(&rel, &read(&rel), "//"));
    }
    for rel in [
        "scripts/demo.sh",
        "scripts/demo-approve.sh",
        "scripts/mvp-demo.sh",
        "scripts/e2e-seed.sh",
    ] {
        scanned += 1;
        found.extend(literal_addresses(rel, &read(rel), "#"));
    }
    assert!(scanned >= 12, "scanned only {scanned} files");
    assert!(
        found.is_empty(),
        "{} code line(s) hard-code a DEFAULT-stack host address, so on another slot they \
         reach whoever owns the default stack. Use harness::bootstrap()/s3_endpoint()/… or \
         stack-lib.sh's LW_E2E_* instead:\n  {}",
        found.len(),
        found.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// 5. The render: Docker, but no container (behind `e2e`).
// ---------------------------------------------------------------------------

/// `docker compose config --format json`, with `env` applied on top of an
/// environment with every stack variable REMOVED, bounded by a deadline.
#[cfg(feature = "e2e")]
fn render(env: &[(String, String)]) -> serde_json::Value {
    use std::process::{Command, Stdio};
    let mut c = Command::new("docker");
    c.args(["compose", "-f", COMPOSE, "config", "--format", "json"])
        .current_dir(root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for p in stack::all_ports() {
        c.env_remove(p.var);
    }
    c.env_remove(stack::PROJECT_VAR);
    c.env_remove("COMPOSE_PROFILES");
    c.env_remove("LOGWEIR_K8S_ADVERTISED_HOST");
    for (k, v) in env {
        c.env(k, v);
    }
    let mut child = c.spawn().expect("docker compose config");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if child.try_wait().expect("try_wait").is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("`docker compose config` did not finish in 60 s");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let out = child.wait_with_output().expect("output");
    assert!(
        out.status.success(),
        "`docker compose config` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("config --format json is JSON")
}

#[cfg(feature = "e2e")]
fn published(doc: &serde_json::Value, service: &str) -> BTreeSet<(u16, u16)> {
    doc["services"][service]["ports"]
        .as_array()
        .unwrap_or_else(|| panic!("{service} publishes no ports"))
        .iter()
        .map(|p| {
            (
                p["published"].as_str().unwrap().parse().unwrap(),
                p["target"].as_u64().unwrap() as u16,
            )
        })
        .collect()
}

/// **The defaults are today's, and a slot moves every host port and its
/// advertisement — and nothing else.** A pure render: nothing is started.
///
/// Mutant: hard-code `EXTERNAL://localhost:9092` in the advertisement → the
/// slot arm fails (the rendered listener still says 9092).
#[cfg(feature = "e2e")]
#[test]
fn a_slot_moves_every_host_port_and_the_default_render_does_not() {
    let base = render(&[]);
    assert_eq!(base["name"], stack::DEFAULT_PROJECT);
    assert_eq!(
        published(&base, "kafka-broker-1"),
        BTreeSet::from([(9092, 9092), (9095, 9095), (9097, 9097)])
    );
    assert_eq!(
        published(&base, "minio"),
        BTreeSet::from([(9000, 9000), (9001, 9001)])
    );
    let adv = |doc: &serde_json::Value| {
        doc["services"]["kafka-broker-1"]["environment"]["KAFKA_ADVERTISED_LISTENERS"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(
        adv(&base),
        "PLAINTEXT://kafka-broker-1:9094,EXTERNAL://localhost:9092,SASL://kafka-broker-1:9096,\
         SASLEXT://localhost:9097,K8S://host.docker.internal:9095",
        "the DEFAULT render must be byte-for-byte the pre-PROD-01.5 advertisement"
    );

    let n = 3;
    let mut env = vec![(stack::PROJECT_VAR.to_string(), stack::slot_project(n))];
    for p in stack::all_ports() {
        env.push((p.var.to_string(), stack::slot_port(&p, n).to_string()));
    }
    let slot = render(&env);
    assert_eq!(slot["name"], stack::slot_project(n));
    assert_eq!(
        published(&slot, "kafka-broker-1"),
        BTreeSet::from([(39092, 9092), (39095, 9095), (39097, 9097)])
    );
    assert_eq!(
        published(&slot, "minio"),
        BTreeSet::from([(39000, 9000), (39001, 9001)])
    );
    assert_eq!(
        adv(&slot),
        "PLAINTEXT://kafka-broker-1:9094,EXTERNAL://localhost:39092,SASL://kafka-broker-1:9096,\
         SASLEXT://localhost:39097,K8S://host.docker.internal:39095",
        "slot {n} must advertise ITS ports, so a client is never redirected to the default stack"
    );
    // Nothing else moved: the in-network names and the container ports are
    // the same on every slot.
    assert_eq!(
        base["services"]["kafka-broker-1"]["environment"]["KAFKA_LISTENERS"],
        slot["services"]["kafka-broker-1"]["environment"]["KAFKA_LISTENERS"]
    );
}
