//! **PROD-01.5: the compose stack is parameterized, and its readers agree.**
//!
//! Two stacks can run side by side only if every published host port and the
//! project name are parameters, every reader of the stack takes them from ONE
//! list (`e2e/compose/stack-lib.sh`: the scripts source it, the harness
//! compiles it in), and every reader REFUSES an environment that is not one
//! coherent stack. A reader that kept a literal `9092`, or accepted a half-set
//! environment, would reach WHOEVER owns another stack; that is the defect
//! class this file guards.
//!
//! The text and shell rows run in the DEFAULT test set: no Docker, no stack,
//! no network (the shell rows run `bash e2e/compose/stack-env.sh`, bounded).
//! The render row at the bottom needs Docker (`docker compose config` only —
//! no container is started) and sits behind the `e2e` feature.

#[allow(dead_code)]
#[path = "harness/stack.rs"]
mod stack;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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

/// The apache/kafka image's own default KRaft cluster id (its
/// `configureDefaults`), which the DEFAULT stack's broker keeps.
const IMAGE_DEFAULT_CLUSTER_ID: &str = "5L6g3nShT-eMCtK--X86sw";

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

/// The one list, as VARIABLE → DEFAULT.
fn the_list() -> BTreeMap<String, u16> {
    stack::all_ports()
        .into_iter()
        .map(|p| (p.var.to_string(), p.default))
        .collect()
}

/// `bash <args>` in the workspace root with EXACTLY `env` (plus `PATH`),
/// bounded at 30 s: (exit code, stdout, stderr).
fn run_bash(args: &[&str], env: &[(String, String)]) -> (Option<i32>, String, String) {
    let mut c = Command::new("bash");
    c.args(args)
        .current_dir(root())
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        c.env(k, v);
    }
    let mut child = c.spawn().expect("bash runs");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while child.try_wait().expect("try_wait").is_none() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("`bash {args:?}` did not finish in 30 s");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let out = child.wait_with_output().expect("output");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// ---------------------------------------------------------------------------
// 1. The compose file agrees with the one list.
// ---------------------------------------------------------------------------

/// The project name and every port default the compose file spells are the
/// list's, and there is no port variable one side has and the other lacks.
///
/// Mutant: change `${LOGWEIR_E2E_S3_PORT:-9000}` to `:-9002` in the compose
/// file → fails naming `LOGWEIR_E2E_S3_PORT`.
#[test]
fn the_compose_defaults_are_the_one_lists_defaults() {
    let text = read(COMPOSE);
    assert!(
        text.lines()
            .any(|l| l == format!("name: {}", stack::default_project())),
        "{COMPOSE} must keep `name: {}` — COMPOSE_PROJECT_NAME is the override, the file \
         carries the default",
        stack::default_project()
    );
    let found = port_defaults_in(&text);
    let want = the_list();
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
            "{COMPOSE} spells ${{{var}:-{d}}} but {STACK_LIB}'s list says {:?}",
            want.get(var)
        );
    }
    let missing: Vec<_> = want.keys().filter(|v| !found.contains_key(*v)).collect();
    assert!(
        missing.is_empty(),
        "{STACK_LIB} lists port variable(s) the compose file never reads: {missing:?}"
    );
}

/// Every published host port is a parameter: each `ports:` entry of every
/// service reads `"${LOGWEIR_E2E_*_PORT:-N}:<container port>"`, and the list
/// files it under the profile of the service that publishes it.
///
/// Mutant: restore `ports: ["9092:9092", …]` on `kafka-broker-1` → fails
/// naming the service and the entry.
#[test]
fn every_published_host_port_is_a_parameter() {
    let doc = compose_yaml();
    let services = doc["services"].as_mapping().expect("services:");
    let list: BTreeMap<&str, Option<&str>> = stack::all_ports()
        .into_iter()
        .map(|p| (p.var, p.profile))
        .collect();
    let mut seen = 0;
    let mut bad = Vec::new();
    for (name, svc) in services {
        let Some(ports) = svc.get("ports").and_then(|p| p.as_sequence()) else {
            continue;
        };
        let profile = svc
            .get("profiles")
            .and_then(|p| p.as_sequence())
            .and_then(|p| p.first())
            .and_then(|p| p.as_str());
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
            let var = s
                .strip_prefix("${")
                .and_then(|r| r.split(":-").next())
                .unwrap_or("");
            if !ok {
                bad.push(format!(
                    "{}: {s} is not a parameter",
                    name.as_str().unwrap_or("?")
                ));
            } else if list.get(var) != Some(&profile) {
                bad.push(format!(
                    "{}: {var} is published under profile {profile:?} but {STACK_LIB} files it \
                     under {:?}",
                    name.as_str().unwrap_or("?"),
                    list.get(var)
                ));
            }
        }
    }
    assert!(
        seen >= 14,
        "found only {seen} published ports — reading the wrong file?"
    );
    assert!(bad.is_empty(), "{bad:#?}");
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
        // Apache Kafka and Confluent Platform read `KAFKA_ADVERTISED_LISTENERS`;
        // the `redpanda` profile's start script passes `ADVERTISED` to
        // `--advertise-kafka-addr` (PROD-01.2). One `NAME://host:port` list
        // either way.
        let Some(adv) = ADVERTISEMENT_KEYS
            .iter()
            .find_map(|k| env.get(*k).and_then(|v| v.as_str()))
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
    // The Redpanda node's advertisement really is read by this test: its two
    // host-facing names are among the ones checked above only if the service
    // spells them in a key this test knows.
    let redpanda = &services
        .iter()
        .find(|(n, _)| n.as_str() == Some("redpanda"))
        .expect("the `redpanda` service")
        .1["environment"];
    assert!(
        ADVERTISEMENT_KEYS
            .iter()
            .any(|k| redpanda.get(*k).and_then(|v| v.as_str()).is_some()),
        "the `redpanda` service advertises through none of {ADVERTISEMENT_KEYS:?}, so its \
         host-facing names are unchecked"
    );
    let start = read("e2e/compose/profiles/redpanda/start.sh");
    assert!(
        start.contains("--advertise-kafka-addr \"$ADVERTISED\""),
        "profiles/redpanda/start.sh must advertise exactly the compose file's ADVERTISED"
    );
}

/// The environment keys a broker service spells its advertised listeners in.
const ADVERTISEMENT_KEYS: [&str; 2] = ["KAFKA_ADVERTISED_LISTENERS", "ADVERTISED"];

/// **The two non-Apache endpoints are pinned by digest and never in the
/// default set** (PROD-01.2). Every service running a Redpanda or Confluent
/// Platform broker image names it as `repository:tag@sha256:<64 hex>` and
/// carries its own profile, so `just e2e-up` with no profile, which is what
/// CI's e2e job runs beside `auth`, never starts one; and the digest is the
/// one `docs/support-matrix.md` records.
///
/// Mutants: drop the `@sha256:…` from the `redpanda` image, or remove
/// `profiles: [confluent]` from `kafka-cp` → fails naming the service.
#[test]
fn the_other_endpoints_are_pinned_by_digest_and_opt_in() {
    let doc = compose_yaml();
    let matrix = read("docs/support-matrix.md");
    let mut seen = BTreeSet::new();
    for (name, svc) in doc["services"].as_mapping().expect("services:") {
        let name = name.as_str().expect("a service name");
        let image = svc["image"].as_str().unwrap_or("");
        let profile = match image {
            i if i.starts_with("redpandadata/redpanda:") => "redpanda",
            i if i.starts_with("confluentinc/cp-kafka:") => "confluent",
            _ => continue,
        };
        let digest = image
            .split_once("@sha256:")
            .map(|(_, d)| d)
            .unwrap_or_else(|| panic!("{name}: {image} is not pinned by digest"));
        assert!(
            digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "{name}: {image} does not end in a sha256 digest"
        );
        let profiles: Vec<&str> = svc["profiles"]
            .as_sequence()
            .map(|p| p.iter().filter_map(|x| x.as_str()).collect())
            .unwrap_or_default();
        assert_eq!(
            profiles,
            vec![profile],
            "{name}: a {profile} image must be opt-in under profile `{profile}` only"
        );
        assert!(
            matrix.contains(&format!("sha256:{digest}")),
            "{name}: docs/support-matrix.md does not record {image}'s digest"
        );
        seen.insert(profile);
    }
    assert_eq!(
        seen,
        BTreeSet::from(["confluent", "redpanda"]),
        "the scan found both endpoints' services"
    );
    // CI's default e2e job names its profile set in one place, and neither
    // endpoint is in it.
    let ci = read(".github/workflows/ci.yml");
    for line in ci.lines().filter(|l| l.contains("COMPOSE_PROFILES")) {
        assert!(
            !line.contains("redpanda") && !line.contains("confluent"),
            "ci.yml's default profile set must not start another vendor's broker: {line}"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. There is ONE list.
// ---------------------------------------------------------------------------

/// Every spelling of a stack port variable followed by a number (`VAR 9092`,
/// `VAR=19092`, `${VAR:-9092}`) in code lines of `text` — a copy of the list.
fn port_default_copies(rel: &str, text: &str, comment: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim_start().starts_with(comment) {
            continue;
        }
        let mut rest = line;
        while let Some(at) = rest.find("LOGWEIR_E2E_") {
            let tail = &rest[at..];
            let name_len = tail
                .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
                .unwrap_or(tail.len());
            let name = &tail[..name_len];
            let after = tail[name_len..].trim_start_matches([' ', '=', ':', '-']);
            if name.ends_with("_PORT") && after.starts_with(|c: char| c.is_ascii_digit()) {
                out.push(format!("{rel}:{}: {}", i + 1, line.trim()));
                break;
            }
            rest = &tail[name_len.max(1)..];
        }
    }
    out
}

/// **The one list is the only list.** `stack-env.sh` sources `stack-lib.sh`
/// and keeps no table of its own; `stack-lib.sh` derives its per-port values
/// from the table; nothing else a stack reader runs — the scripts, the
/// justfile, the e2e suites — spells a port variable with a number. Only the
/// compose file may (its `${VAR:-N}`, checked against the list above).
///
/// Mutant: put `LOGWEIR_E2E_S3_PORT 9000` back into a PORTS table in
/// `stack-env.sh` → fails naming the line.
#[test]
fn the_one_list_is_the_only_list() {
    let env = read(STACK_ENV);
    assert!(
        env.contains(". \"$here/stack-lib.sh\""),
        "{STACK_ENV} must source {STACK_LIB} for the list"
    );
    let lib = read(STACK_LIB);
    for p in stack::core_ports() {
        let derived = format!("=$(lw_e2e_port {})", p.var);
        assert!(
            lib.lines()
                .any(|l| l.starts_with("LW_E2E_") && l.ends_with(&derived)),
            "{STACK_LIB} must derive {} from the table (want a line ending {derived})",
            p.var
        );
    }
    for var in [
        stack::KAFKA_PORT,
        stack::K8S_PORT,
        stack::SASL_PORT,
        stack::S3_PORT,
    ] {
        assert!(
            the_list().contains_key(var),
            "the harness reads {var}, which the list does not carry"
        );
    }
    let mut copies = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();
    rs_files_recursive(&root().join("e2e/tests"), &mut files);
    for f in &files {
        let rel = f
            .strip_prefix(root())
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "e2e/tests/stack_params.rs" {
            continue;
        }
        copies.extend(port_default_copies(&rel, &read(&rel), "//"));
    }
    let mut shells = vec![
        "justfile".to_string(),
        "scripts/demo.sh".to_string(),
        "scripts/demo-approve.sh".to_string(),
        "scripts/mvp-demo.sh".to_string(),
        "scripts/e2e-seed.sh".to_string(),
        "scripts/k8s-demo.sh".to_string(),
        "scripts/demo-steps.sh".to_string(),
    ];
    for e in std::fs::read_dir(root().join("e2e/compose"))
        .unwrap()
        .flatten()
    {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with(".sh") && n != "stack-lib.sh" {
            shells.push(format!("e2e/compose/{n}"));
        }
    }
    for rel in &shells {
        copies.extend(port_default_copies(rel, &read(rel), "#"));
    }
    assert!(
        copies.is_empty(),
        "a copy of the stack's port list outside {STACK_LIB} (read it from there — source it, \
         or `include_str!` it as e2e/tests/harness/stack.rs does):\n  {}",
        copies.join("\n  ")
    );
}

/// No two slots share a project or a host port, and every slot's port stays
/// below macOS's first ephemeral port (49152), where a listening port could
/// race an outbound connection.
#[test]
fn slots_never_share_a_project_or_a_port() {
    let mut projects = BTreeSet::new();
    let mut ports: BTreeMap<u16, String> = BTreeMap::new();
    for n in 0..=stack::max_slot() {
        assert!(
            projects.insert(stack::slot_project(n)),
            "slot {n} reuses a project name"
        );
        assert_eq!(stack::slot_of(&stack::slot_project(n)), Some(n));
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
    assert_eq!(stack::slot_of("logweir-e2e-s9"), None);
    assert_eq!(stack::slot_of("somebody-elses-project"), None);
}

// ---------------------------------------------------------------------------
// 3. Coherence: the shell and the harness refuse the same environments.
// ---------------------------------------------------------------------------

/// The environment slot `n` is: its project and every port (nothing for 0).
fn slot_env(n: u16) -> Vec<(String, String)> {
    if n == 0 {
        return Vec::new();
    }
    let mut v = vec![(stack::PROJECT_VAR.to_string(), stack::slot_project(n))];
    for p in stack::all_ports() {
        v.push((p.var.to_string(), stack::slot_port(&p, n).to_string()));
    }
    v
}

/// (harness refuses?, shell refuses?, shell's stderr) for one environment.
fn both_readers(env: &[(String, String)]) -> (Option<String>, bool, String) {
    let map: BTreeMap<String, String> = env.iter().cloned().collect();
    let names: Vec<String> = map.keys().cloned().collect();
    let rust = stack::incoherence_in(&|k| map.get(k).cloned(), &names);
    let (rc, _, err) = run_bash(&[STACK_ENV, "--check"], env);
    (rust, rc != Some(0), err)
}

fn assert_refused(what: &str, env: &[(String, String)]) {
    let (rust, shell, err) = both_readers(env);
    assert!(
        rust.is_some(),
        "{what}: the HARNESS accepted {env:?}; stack::incoherence_in must refuse it"
    );
    assert!(
        shell,
        "{what}: the SHELL (`stack-env.sh --check`, what `just e2e-up/e2e-down` run) accepted \
         {env:?}"
    );
    assert!(
        err.contains("not one coherent stack"),
        "{what}: the shell's refusal must say why: {err}"
    );
}

fn assert_accepted(what: &str, env: &[(String, String)]) {
    let (rust, shell, err) = both_readers(env);
    assert_eq!(rust, None, "{what}: the harness refused {env:?}");
    assert!(!shell, "{what}: the shell refused {env:?}: {err}");
}

/// **Every stack variable set ALONE is refused** (review M1): each of the
/// list's ports moved as a slot would move it, or not a number, with the
/// project left at the default; the project alone; a project that is not a
/// stack; a port variable the list does not know. Set alone to its OWN
/// default, a port is slot 0 spelled out, and accepted.
///
/// Mutant: drop the loop over the list in `lw_e2e_check_coherent` (back to
/// the five core ports) → fails on the first profile port.
#[test]
fn every_stack_variable_set_alone_is_refused_by_both_readers() {
    for p in stack::all_ports() {
        let moved = (p.default + stack::stride()).to_string();
        assert_refused(&format!("{} alone", p.var), &[(p.var.to_string(), moved)]);
        assert_refused(
            &format!("{} not a port", p.var),
            &[(p.var.to_string(), "nine".to_string())],
        );
        assert_accepted(
            &format!("{} at its own default", p.var),
            &[(p.var.to_string(), p.default.to_string())],
        );
    }
    assert_refused(
        "a slot's project alone",
        &[(stack::PROJECT_VAR.to_string(), stack::slot_project(1))],
    );
    assert_refused(
        "a project that is not a stack",
        &[(
            stack::PROJECT_VAR.to_string(),
            "somebody-elses-project".to_string(),
        )],
    );
    assert_refused(
        "an unknown port variable",
        &[("LOGWEIR_E2E_KAKFA_PORT".to_string(), "9092".to_string())],
    );
}

/// **A cross-slot mix is refused** (review M1): one slot's project with
/// another slot's ports, a slot with any ONE of its ports missing or taken
/// from another slot, and the default project with a slot's ports.
#[test]
fn a_cross_slot_mix_is_refused_by_both_readers() {
    let one = slot_env(1);
    let mut mixed = one.clone();
    mixed[0].1 = stack::slot_project(2);
    assert_refused("slot 2's project with slot 1's ports", &mixed);
    let mut default_project = one.clone();
    default_project.remove(0);
    assert_refused("the default project with slot 1's ports", &default_project);
    for p in stack::all_ports() {
        let missing: Vec<_> = one.iter().filter(|(k, _)| k != p.var).cloned().collect();
        assert_refused(&format!("slot 1 without {}", p.var), &missing);
        let other: Vec<_> = one
            .iter()
            .map(|(k, v)| {
                if k == p.var {
                    (k.clone(), stack::slot_port(&p, 2).to_string())
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect();
        assert_refused(&format!("slot 1 with slot 2's {}", p.var), &other);
    }
}

/// The positive half: every slot, exactly as `stack-env.sh` prints it, is
/// accepted by both readers — the refusals above are about the MIX, not about
/// slots.
#[test]
fn every_slot_is_coherent_for_both_readers() {
    for n in 0..=stack::max_slot() {
        let (rc, out, err) = run_bash(&[STACK_ENV, "--slot", &n.to_string()], &[]);
        assert_eq!(rc, Some(0), "stack-env.sh --slot {n}: {err}");
        let printed: Vec<(String, String)> = out
            .lines()
            .filter_map(|l| l.strip_prefix("export "))
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(
            printed,
            slot_env(n),
            "stack-env.sh --slot {n} prints slot {n}"
        );
        assert_accepted(&format!("slot {n}"), &printed);
    }
}

// ---------------------------------------------------------------------------
// 4. Switching slots leaves nothing behind (review M2).
// ---------------------------------------------------------------------------

/// **Nothing carries over.** After `--slot 2 --kafka 4.3 --profiles auth` then
/// `--slot 0`, no variable the helper exported is left — so the default stack
/// renders `.env`'s broker line, not 4.3; and moving to slot 1 without
/// `--kafka`/`--profiles` leaves slot 1's ports and nothing of slot 2's line
/// or profiles.
///
/// Mutant: stop printing `unset KAFKA_VERSION` → fails naming KAFKA_VERSION.
#[test]
fn switching_slots_leaves_nothing_behind() {
    let (rc, first, err) = run_bash(
        &[
            STACK_ENV,
            "--slot",
            "2",
            "--kafka",
            "4.3",
            "--profiles",
            "auth",
        ],
        &[],
    );
    assert_eq!(rc, Some(0), "{err}");
    let exported: BTreeSet<String> = first
        .lines()
        .filter_map(|l| l.strip_prefix("export "))
        .filter_map(|kv| kv.split_once('=').map(|(k, _)| k.to_string()))
        .collect();
    for must in [
        "KAFKA_VERSION",
        "KAFKA_IMAGE",
        "COMPOSE_PROFILES",
        stack::PROJECT_VAR,
    ] {
        assert!(
            exported.contains(must),
            "slot 2 --kafka 4.3 --profiles auth exports {must}"
        );
    }
    let (rc, zero, err) = run_bash(&[STACK_ENV, "--slot", "0"], &[]);
    assert_eq!(rc, Some(0), "{err}");
    for name in &exported {
        assert!(
            zero.lines().any(|l| l == format!("unset {name}")),
            "`stack-env.sh --slot 0` does not unset {name}, which --slot 2 exported: it would \
             carry over to the default stack"
        );
    }
    // The same, as a shell sees it after both evals.
    let script = format!(
        "eval \"$(bash {STACK_ENV} --slot 2 --kafka 4.3 --profiles auth)\"; \
         eval \"$(bash {STACK_ENV} --slot 0)\"; env"
    );
    let (rc, env0, err) = run_bash(&["-c", &script], &[]);
    assert_eq!(rc, Some(0), "{err}");
    let left: Vec<&str> = env0
        .lines()
        .filter(|l| exported.iter().any(|n| l.starts_with(&format!("{n}="))))
        .collect();
    assert!(left.is_empty(), "left over after --slot 0: {left:?}");
    let script = format!(
        "eval \"$(bash {STACK_ENV} --slot 2 --kafka 4.3 --profiles auth)\"; \
         eval \"$(bash {STACK_ENV} --slot 1)\"; env"
    );
    let (rc, env1, err) = run_bash(&["-c", &script], &[]);
    assert_eq!(rc, Some(0), "{err}");
    for gone in ["KAFKA_VERSION", "KAFKA_IMAGE", "COMPOSE_PROFILES"] {
        assert!(
            !env1.lines().any(|l| l.starts_with(&format!("{gone}="))),
            "{gone} carried over from slot 2 to slot 1"
        );
    }
    assert!(
        env1.lines()
            .any(|l| l == format!("{}={}", stack::PROJECT_VAR, stack::slot_project(1))),
        "slot 1's project after the switch"
    );
}

// ---------------------------------------------------------------------------
// 5. Per-slot cluster ids (review L3) and pinned broker lines (review L4).
// ---------------------------------------------------------------------------

/// Where every Kafka cluster's id file is loaded from: the stack's own
/// directory under `e2e/compose/slots/`.
const SLOT_DIR: &str = "./slots/${COMPOSE_PROJECT_NAME:-logweir-e2e}/";

/// **Every Kafka cluster on every stack has its own KRaft cluster id, and the
/// default broker keeps the image's** (review L3). Each Kafka node (a service
/// with a `KAFKA_NODE_ID`) loads its cluster's `slots/<project>/<file>.env`
/// and sets no `CLUSTER_ID` in `environment:`, which would override the file:
/// `kafka-broker-1` with `required: false` and NO file for the default
/// project, so the default render is unchanged; the profile clusters with
/// `required: true`, so a project with no ids of its own fails to start
/// rather than sharing another stack's. Every file the stacks need exists, no
/// other file does, and the ids (with the image default) are pairwise
/// distinct valid KRaft ids, so the cluster-id allowlist tells ANY two
/// clusters on ANY two stacks apart.
///
/// Mutants: two slots given one id; a `CLUSTER_ID` back in a profile
/// cluster's `environment:` → fails.
#[test]
fn every_cluster_on_every_stack_has_its_own_id() {
    use base64::Engine as _;
    let mut doc = compose_yaml();
    doc.apply_merge()
        .expect("the compose file's merge keys resolve");
    // id file -> required, over every Kafka node.
    let mut files: BTreeMap<String, bool> = BTreeMap::new();
    for (name, svc) in doc["services"].as_mapping().expect("services") {
        let name = name.as_str().expect("service names are strings");
        let env = &svc["environment"];
        if env.get("KAFKA_NODE_ID").is_none() {
            continue;
        }
        assert!(
            env.get("CLUSTER_ID").is_none(),
            "{name} sets CLUSTER_ID in `environment:`, which overrides its stack's file"
        );
        let ef = svc["env_file"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{name} is a Kafka node that loads no id file"));
        assert_eq!(
            ef.len(),
            1,
            "{name}: exactly one env_file, its cluster's id"
        );
        let path = ef[0]["path"].as_str().expect("env_file path");
        let file = path
            .strip_prefix(SLOT_DIR)
            .unwrap_or_else(|| panic!("{name} loads {path}, not a file in {SLOT_DIR}"));
        let required = ef[0]["required"]
            .as_bool()
            .unwrap_or_else(|| panic!("{name}: `required:` must be explicit"));
        assert_eq!(
            required,
            name != "kafka-broker-1",
            "{name}: only the default broker's id file may be optional"
        );
        if let Some(before) = files.insert(file.to_string(), required) {
            assert_eq!(before, required, "{file} is loaded both ways");
        }
    }
    assert!(
        files.contains_key("kafka-broker-1.env") && files.len() >= 2,
        "the scan found the default broker and the profile clusters: {files:?}"
    );

    let slots = root().join("e2e/compose/slots");
    let mut owner: BTreeMap<String, String> = BTreeMap::from([(
        IMAGE_DEFAULT_CLUSTER_ID.to_string(),
        "the apache/kafka image's default".to_string(),
    )]);
    let mut expected = BTreeSet::new();
    for n in 0..=stack::max_slot() {
        let project = stack::slot_project(n);
        for (file, required) in &files {
            let rel = format!("{project}/{file}");
            if n == 0 && !required {
                assert!(
                    !slots.join(&rel).exists(),
                    "slots/{rel} exists: the DEFAULT broker keeps the image's id, or the \
                     default render changes"
                );
                continue;
            }
            let text = std::fs::read_to_string(slots.join(&rel))
                .unwrap_or_else(|e| panic!("slots/{rel}: {e}"));
            let lines: Vec<&str> = text
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .collect();
            let id = match lines.as_slice() {
                [l] => l
                    .strip_prefix("CLUSTER_ID=")
                    .unwrap_or_else(|| panic!("slots/{rel}: {l:?} is not CLUSTER_ID=…")),
                _ => panic!("slots/{rel} must carry exactly one CLUSTER_ID=… line: {lines:?}"),
            };
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(id)
                .unwrap_or_else(|e| panic!("slots/{rel}: {id} is not base64url: {e}"));
            assert_eq!(
                bytes.len(),
                16,
                "slots/{rel}: {id} is not a 16-byte KRaft id"
            );
            assert!(
                !id.starts_with('-'),
                "slots/{rel}: {id} starts with '-', which Kafka never generates (a CLI \
                 would read it as a flag)"
            );
            if let Some(other) = owner.insert(id.to_string(), format!("slots/{rel}")) {
                panic!("slots/{rel}: {id} is already {other}'s id");
            }
            expected.insert(rel);
        }
    }
    // Nothing else lives there: a stray file is an id no stack loads, or a
    // stack the rule does not know.
    let mut found = BTreeSet::new();
    for dir in std::fs::read_dir(&slots).expect("e2e/compose/slots/") {
        let dir = dir.expect("dir entry").path();
        let dname = dir.file_name().unwrap().to_string_lossy().to_string();
        if dname.starts_with('.') {
            continue;
        }
        if !dir.is_dir() {
            found.insert(dname);
            continue;
        }
        for f in std::fs::read_dir(&dir).expect("a slot directory") {
            let fname = f.expect("entry").file_name().to_string_lossy().to_string();
            if !fname.starts_with('.') {
                found.insert(format!("{dname}/{fname}"));
            }
        }
    }
    assert_eq!(
        found, expected,
        "e2e/compose/slots/ must hold exactly one id file per (stack, cluster)"
    );
}

/// **The broker lines are pinned by digest, and the pins are the recorded
/// ones.** Every `apache/kafka` image in the compose file goes through
/// `KAFKA_IMAGE` (which `--kafka LINE` sets to `apache/kafka:<v>@<digest>`),
/// and each line's digest is the one `docs/support-matrix.md` records for it.
#[test]
fn the_broker_lines_are_pinned_by_digest_and_match_the_support_matrix() {
    let compose = read(COMPOSE);
    let kafka_images: Vec<&str> = compose
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("image:") && l.contains("apache/kafka"))
        .collect();
    assert!(
        kafka_images.len() >= 11,
        "found {} broker images",
        kafka_images.len()
    );
    for l in &kafka_images {
        assert_eq!(
            *l, "image: ${KAFKA_IMAGE:-apache/kafka:${KAFKA_VERSION:-3.7.1}}",
            "every broker image must be overridable by the pinned KAFKA_IMAGE"
        );
    }
    let env = read(STACK_ENV);
    let block = env
        .split("LINES=\"")
        .nth(1)
        .and_then(|r| r.split('"').next())
        .expect("stack-env.sh carries a LINES=\"…\" table");
    let matrix = read("docs/support-matrix.md");
    let mut n = 0;
    for l in block.lines().filter(|l| !l.trim().is_empty()) {
        let w: Vec<&str> = l.split_whitespace().collect();
        let (version, digest) = (w[1], w[2]);
        assert!(digest.starts_with("sha256:") && digest.len() == 71, "{l}");
        let row = matrix
            .lines()
            .find(|r| r.starts_with(&format!("| **{version}** |")))
            .unwrap_or_else(|| {
                panic!("docs/support-matrix.md has no Broker versions row for {version}")
            });
        assert!(
            row.contains(digest),
            "line {version} is pinned to {digest}, but the support matrix records: {row}"
        );
        n += 1;
    }
    assert_eq!(n, 4, "four broker lines");
}

// ---------------------------------------------------------------------------
// 6. Profiles are torn down, and listed.
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
// 7. The class sweep: no reader keeps a literal default address, and every
//    direct compose call checks coherence.
// ---------------------------------------------------------------------------

/// The default stack's host-side addresses, spelled as a reader would
/// hard-code them.
fn default_address_literals() -> Vec<String> {
    let mut v = Vec::new();
    // EVERY published port — the profile ports move with a slot exactly like
    // the core ones (review M1), so a reader spelling `localhost:9130` would
    // reach the default stack's object store from a slot.
    for p in stack::all_ports() {
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

/// Every `.rs` file under `dir`, at ANY depth, sorted. Recursive on purpose: a
/// support module in a subdirectory (`e2e/tests/<suite>_support/…`) is as much
/// a stack reader as a top-level suite, and a flat `read_dir` walked past one.
fn rs_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            rs_files_recursive(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out.sort();
}

/// Scans every `.rs` under each of `dirs` (relative to `base`, recursively)
/// except the paths in `skip`, and returns (files scanned, findings), each
/// finding naming the file relative to `base`.
fn scan_rust_readers(base: &Path, dirs: &[&str], skip: &[&str]) -> (usize, Vec<String>) {
    let mut files = Vec::new();
    for d in dirs {
        rs_files_recursive(&base.join(d), &mut files);
    }
    files.sort();
    let mut scanned = 0;
    let mut found = Vec::new();
    for f in &files {
        let rel = f
            .strip_prefix(base)
            .expect("under the base")
            .to_string_lossy()
            .replace('\\', "/");
        if skip.contains(&rel.as_str()) {
            continue;
        }
        scanned += 1;
        let text = std::fs::read_to_string(f).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        found.extend(literal_addresses(&rel, &text, "//"));
    }
    (scanned, found)
}

/// **The sweep's guard.** No e2e suite or support module (at any depth under
/// `e2e/tests/` or `e2e/src/`), and none of the scripts that address the stack,
/// spells a default-stack host address in CODE: every one goes through
/// `harness/stack.rs` or `stack-lib.sh`, so a slot reaches its own stack and
/// never the default one. (Comments may still name the defaults.)
///
/// Mutant: put `"http://localhost:9000"` back into `pitr_boundary.rs`'s
/// restore spec → fails naming the file and line. A planted twin in a
/// SUBDIRECTORY is `the_sweep_reaches_support_modules_in_subdirectories`.
#[test]
fn no_stack_reader_hard_codes_a_default_host_address() {
    let (mut scanned, mut found) = scan_rust_readers(
        &root(),
        &["e2e/tests", "e2e/src"],
        &["e2e/tests/stack_params.rs"],
    );
    for rel in [
        "scripts/demo.sh",
        "scripts/demo-approve.sh",
        "scripts/mvp-demo.sh",
        "scripts/e2e-seed.sh",
    ] {
        scanned += 1;
        found.extend(literal_addresses(rel, &read(rel), "#"));
    }
    assert!(scanned >= 14, "scanned only {scanned} files");
    assert!(
        found.is_empty(),
        "{} code line(s) hard-code a DEFAULT-stack host address, so on another slot they \
         reach whoever owns the default stack. Use harness::bootstrap()/s3_endpoint()/… or \
         stack-lib.sh's LW_E2E_* instead:\n  {}",
        found.len(),
        found.join("\n  ")
    );
}

/// **The planted twin.** The same scan over a throwaway tree: a support module
/// two levels down that spells `http://localhost:9000` in code must be found,
/// and its twin — the same file reading `s3_endpoint()`, with the default named
/// only in a comment — must not. A non-recursive walk finds neither, and fails
/// the first assertion.
#[test]
fn the_sweep_reaches_support_modules_in_subdirectories() {
    let base = std::env::temp_dir().join(format!(
        "lw-stack-params-twin-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let sub = base.join("e2e/tests/some_suite_support/nested");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(
        sub.join("planted.rs"),
        "pub fn endpoint() -> String {\n    \"http://localhost:9000\".to_string()\n}\n",
    )
    .unwrap();
    std::fs::write(
        sub.join("twin.rs"),
        "// the default stack answers on http://localhost:9000\npub fn endpoint() -> String {\n    s3_endpoint()\n}\n",
    )
    .unwrap();
    let (scanned, found) = scan_rust_readers(&base, &["e2e/tests"], &[]);
    let _ = std::fs::remove_dir_all(&base);
    assert_eq!(
        scanned, 2,
        "the walk did not reach both files two levels down"
    );
    assert_eq!(
        found.len(),
        1,
        "exactly the planted file must be found, not its twin: {found:?}"
    );
    assert!(
        found[0].starts_with("e2e/tests/some_suite_support/nested/planted.rs:2:"),
        "the finding names the planted file and line: {found:?}"
    );
}

/// Whether a trimmed code line opens a function.
fn opens_fn(t: &str) -> bool {
    let t = t
        .strip_prefix("pub(crate) ")
        .or_else(|| t.strip_prefix("pub "))
        .unwrap_or(t);
    t.strip_prefix("async ").unwrap_or(t).starts_with("fn ")
}

/// **Every `docker compose` call checks coherence first, in its own
/// function** (review M1). Each CODE line of an e2e test or support module
/// that names the compose file must sit in a function that, BEFORE that line,
/// calls `ensure_coherent()` or tests `incoherence()` — unless the function
/// renders the file's own defaults with the project variable removed
/// (`env_remove(stack::PROJECT_VAR)`), which reaches no stack. Comment lines
/// count for nothing, so a doc comment naming the check does not pass.
///
/// Mutants: remove the call from `guards.rs::kafka_configs`, or from
/// `record_semantics_support/kafka.rs::compose_broker` while its sibling
/// `try_compose_broker` keeps its own → fails naming the file and line.
#[test]
fn every_direct_compose_call_checks_coherence() {
    let mut files = Vec::new();
    rs_files_recursive(&root().join("e2e/tests"), &mut files);
    let mut unchecked = Vec::new();
    let mut calls = 0;
    for f in &files {
        let rel = f
            .strip_prefix(root())
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "e2e/tests/stack_params.rs" || rel == "e2e/tests/harness/stack.rs" {
            continue;
        }
        let text = read(&rel);
        let lines: Vec<&str> = text.lines().collect();
        let code = |i: usize| !lines[i].trim_start().starts_with("//");
        let fn_at = |i: usize| code(i) && opens_fn(lines[i].trim_start());
        for i in (0..lines.len()).filter(|&i| code(i) && lines[i].contains("docker-compose.yml")) {
            calls += 1;
            let Some(start) = (0..=i).rev().find(|&j| fn_at(j)) else {
                unchecked.push(format!("{rel}:{}: outside any function", i + 1));
                continue;
            };
            let end = (i + 1..lines.len())
                .find(|&j| fn_at(j))
                .unwrap_or(lines.len());
            let checked = (start..i).any(|j| {
                code(j)
                    && (lines[j].contains("ensure_coherent()")
                        || lines[j].contains("incoherence()"))
            });
            let stack_free = (start..end)
                .any(|j| code(j) && lines[j].contains("env_remove(stack::PROJECT_VAR)"));
            if !(checked || stack_free) {
                unchecked.push(format!("{rel}:{}: in `{}`", i + 1, lines[start].trim()));
            }
        }
    }
    assert!(calls >= 8, "found only {calls} compose call(s)");
    assert!(
        unchecked.is_empty(),
        "these `docker compose` calls run without a coherence check first in their function \
         (call stack::ensure_coherent()):\n  {}",
        unchecked.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// 7b. The groups and ACL fixtures (PROD-04.0d).
// ---------------------------------------------------------------------------

/// The shell helpers beside the compose file: every `e2e/compose/*.sh` but the
/// list itself, as (relative path, text).
fn compose_helpers() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(root().join("e2e/compose"))
        .unwrap()
        .flatten()
    {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with(".sh") && n != "stack-lib.sh" {
            let rel = format!("e2e/compose/{n}");
            let text = read(&rel);
            out.push((rel, text));
        }
    }
    out.sort();
    out
}

/// `text`'s lines with backslash continuations joined, each with the 1-based
/// number of its first physical line; comment lines are dropped.
fn shell_code_lines(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut acc = String::new();
    let mut first = 0;
    for (i, line) in text.lines().enumerate() {
        if acc.is_empty() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            first = i + 1;
        }
        match line.strip_suffix('\\') {
            Some(head) => {
                acc.push_str(head);
                acc.push(' ');
            }
            None => {
                acc.push_str(line);
                out.push((first, std::mem::take(&mut acc)));
            }
        }
    }
    out
}

/// **Every single-node broker sizes the share-group state topic for one
/// broker** (PROD-04.0 §3.8). With the defaults — replication factor 3, min
/// ISR 2 — a one-broker cluster never creates `__share_group_state`, so its
/// share groups get members but no share-partition state. A Kafka node whose
/// offsets topic has replication factor 1 is a single-node cluster and must
/// carry both settings at 1; the three-node cluster keeps the defaults its
/// three brokers can meet.
///
/// Mutant: delete the two lines from `kafka-acl` → fails naming `kafka-acl`.
#[test]
fn every_single_node_broker_sizes_the_share_state_topic_for_one_broker() {
    let mut doc = compose_yaml();
    doc.apply_merge()
        .expect("the compose file's merge keys resolve");
    let as_text = |v: &serde_yaml::Value| match v {
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        serde_yaml::Value::String(s) => Some(s.clone()),
        _ => None,
    };
    let mut single = Vec::new();
    let mut wrong = Vec::new();
    for (name, svc) in doc["services"].as_mapping().expect("services") {
        let name = name.as_str().expect("service names are strings");
        let env = &svc["environment"];
        if env.get("KAFKA_NODE_ID").is_none() {
            continue;
        }
        let rf = env
            .get("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR")
            .and_then(as_text)
            .unwrap_or_else(|| panic!("{name}: a Kafka node without an offsets RF"));
        let share = |k: &str| env.get(k).and_then(as_text);
        let (srf, sisr) = (
            share("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR"),
            share("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR"),
        );
        if rf == "1" {
            single.push(name.to_string());
            if srf.as_deref() != Some("1") || sisr.as_deref() != Some("1") {
                wrong.push(format!(
                    "{name}: share-state RF {srf:?}, min ISR {sisr:?} (want 1 and 1)"
                ));
            }
        } else if srf.is_some() || sisr.is_some() {
            wrong.push(format!(
                "{name}: a {rf}-replica cluster sets share-state RF {srf:?} / min ISR {sisr:?}; \
                 its defaults (3 and 2) are the ones its brokers can meet"
            ));
        }
    }
    assert!(
        single.len() >= 4,
        "the scan found the single-node brokers (kafka-broker-1, kafka-auth, kafka-cluster2, \
         kafka-acl): {single:?}"
    );
    assert!(
        wrong.is_empty(),
        "share-group state sizing:\n  {}",
        wrong.join("\n  ")
    );
}

/// **Every helper that reaches a stack checks coherence first.** A
/// `e2e/compose/*.sh` that runs `docker` must source `stack-lib.sh` and call
/// `lw_e2e_check_coherent` on a code line BEFORE its first `docker` line:
/// the shell twin of `every_direct_compose_call_checks_coherence`, for
/// `groups.sh` and `profile-smoke.sh`.
///
/// Mutant: comment out `lw_e2e_check_coherent || exit 1` in `groups.sh` →
/// fails naming `groups.sh`.
#[test]
fn every_compose_helper_checks_coherence_before_docker() {
    let mut checked = Vec::new();
    let mut bad = Vec::new();
    for (rel, text) in compose_helpers() {
        let code = shell_code_lines(&text);
        let Some(&(first_docker, _)) = code
            .iter()
            .find(|(_, l)| l.contains("docker compose") || l.contains("docker run"))
        else {
            continue;
        };
        let sources = code
            .iter()
            .any(|(_, l)| l.trim_start().starts_with(". ") && l.contains("stack-lib.sh"));
        let check = code
            .iter()
            .find(|(_, l)| l.contains("lw_e2e_check_coherent"))
            .map(|&(n, _)| n);
        match check {
            Some(n) if sources && n < first_docker => checked.push(rel),
            _ => bad.push(format!(
                "{rel}: first `docker` on line {first_docker}, sources stack-lib.sh: {sources}, \
                 coherence check on line {check:?}"
            )),
        }
    }
    for helper in ["e2e/compose/groups.sh", "e2e/compose/profile-smoke.sh"] {
        assert!(
            checked.iter().any(|r| r == helper) || bad.iter().any(|b| b.starts_with(helper)),
            "the scan did not reach {helper}"
        );
    }
    assert!(
        bad.is_empty(),
        "these helpers can reach a stack nobody meant:\n  {}",
        bad.join("\n  ")
    );
}

/// **`docker compose exec` never reads the helper's stdin.** It attaches stdin
/// even with `-T`, so inside a `while read … done <<EOF` loop the first call
/// swallows the loop's remaining input — measured on `groups.sh up`, whose
/// wait loop then checked one group of seven and returned 0. Every
/// `docker compose … exec` in the compose helpers and `scripts/` redirects
/// stdin from `/dev/null`.
///
/// Mutant: drop `</dev/null` from `groups.sh`'s `kexec` → fails naming the
/// line.
#[test]
fn compose_exec_in_a_shell_helper_never_reads_stdin() {
    let mut files = compose_helpers();
    for e in std::fs::read_dir(root().join("scripts")).unwrap().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with(".sh") {
            let rel = format!("scripts/{n}");
            let text = read(&rel);
            files.push((rel, text));
        }
    }
    let mut execs = 0;
    let mut bad = Vec::new();
    for (rel, text) in &files {
        for (n, l) in shell_code_lines(text) {
            let Some(at) = l.find("docker compose") else {
                continue;
            };
            if !l[at..].split_whitespace().any(|w| w == "exec") {
                continue;
            }
            execs += 1;
            if !l.contains("</dev/null") && !l.contains("< /dev/null") {
                bad.push(format!("{rel}:{n}: {}", l.trim()));
            }
        }
    }
    assert!(
        execs >= 3,
        "found only {execs} `docker compose … exec` line(s)"
    );
    assert!(
        bad.is_empty(),
        "`docker compose exec` without `</dev/null` (it would read the caller's stdin):\n  {}",
        bad.join("\n  ")
    );
}

/// **A member's stop pattern matches its member and nothing else** (PROD-04.0
/// §3.3: `pkill -f "group pa-classic-live"` inside `sh -c` matched its own
/// shell and killed it before the next pkill ran). `groups.sh`'s `pattern()`
/// is run here, and its regex — `pkill`/`pgrep -f` use extended regular
/// expressions — must match the member's command lines (the `timeout`
/// wrapper's and the JVM's), and must NOT match: a command line that carries
/// the pattern (quoted or not, last or not), a group whose name extends it,
/// or the other members. It is bracketed (`[g]roup …`), as the record asks.
/// And every `pkill`/`pgrep` in the compose helpers goes through it.
///
/// Mutants: `pattern()` printing `group %s` (no bracket, no anchor) → the
/// carrier and neighbour rows fail; dropping only the `$` → the neighbour
/// row fails; dropping only the bracket → the bracket assertion fails.
#[test]
fn a_member_stop_pattern_matches_only_its_member() {
    let groups = read("e2e/compose/groups.sh");
    let def = groups
        .lines()
        .find(|l| l.starts_with("pattern() {"))
        .expect("groups.sh defines pattern() on one line");
    let probe = |group: &str, lines: &[String]| -> (String, Vec<bool>) {
        // One bash: define pattern(), then grep -E each candidate line.
        let mut script = format!("{def}\np=$(pattern {group})\nprintf '%s\\n' \"$p\"\n");
        for l in lines {
            script.push_str(&format!(
                "if printf '%s\\n' '{}' | grep -E -q -- \"$p\"; then echo MATCH; else echo NO; fi\n",
                l.replace('\'', "'\\''")
            ));
        }
        let (code, out, err) = run_bash(&["-c", &script], &[]);
        assert_eq!(code, Some(0), "bash failed: {err}");
        let mut it = out.lines();
        let pat = it.next().expect("pattern() printed a pattern").to_string();
        (pat, it.map(|l| l == "MATCH").collect())
    };
    let g = "pa-classic-live";
    let pat = format!("[g]roup {g}$");
    let member = vec![
        format!(
            "timeout 7200 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server \
             kafka-broker-1:9094 --topic pa-orders --from-beginning --consumer-property \
             group.protocol=classic --group {g}"
        ),
        format!(
            "/opt/java/openjdk/bin/java -Xmx512M -cp /opt/kafka/libs/* \
             org.apache.kafka.tools.consumer.ConsoleConsumer --bootstrap-server \
             kafka-broker-1:9094 --topic pa-orders --group {g}"
        ),
    ];
    let (printed, got) = probe(g, &member);
    assert_eq!(
        got,
        vec![true, true],
        "the pattern {printed:?} must match its member's command lines"
    );
    let never = vec![
        // The carriers: the shell that runs the pkill, in every spelling.
        format!("bash -c pkill -TERM -f '{pat}'"),
        format!("sh -c pkill -f {pat}; pkill -f [g]roup pa-consumer-live$"),
        format!("bash -c pgrep -f '{pat}' >/dev/null"),
        // A group whose name extends this one, and the neighbours.
        format!("timeout 7200 kafka-console-consumer.sh --group {g}2"),
        format!("timeout 7200 kafka-console-consumer.sh --group {g}-b --max-messages 1"),
        "timeout 7200 kafka-console-consumer.sh --group pa-consumer-live".to_string(),
        "timeout 7200 kafka-console-share-consumer.sh --group pa-share-live".to_string(),
        "timeout 90 kafka-console-consumer.sh --max-messages 12 --group pa-classic-empty"
            .to_string(),
    ];
    let (_, got) = probe(g, &never);
    let hits: Vec<&String> = never
        .iter()
        .zip(&got)
        .filter(|(_, m)| **m)
        .map(|(l, _)| l)
        .collect();
    assert!(
        hits.is_empty(),
        "the pattern {printed:?} for {g} also matches:\n  {}",
        hits.iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
    assert_eq!(
        printed, pat,
        "pattern() must print the bracketed, anchored form the record asks for"
    );
    // Every pkill/pgrep in the helpers uses pattern().
    let mut direct = Vec::new();
    for (rel, text) in compose_helpers() {
        for (n, l) in shell_code_lines(&text) {
            if (l.contains("pkill ") || l.contains("pgrep ")) && !l.contains("$(pattern ") {
                direct.push(format!("{rel}:{n}: {}", l.trim()));
            }
        }
    }
    assert!(
        direct.is_empty(),
        "a pkill/pgrep that does not go through groups.sh's pattern():\n  {}",
        direct.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// 8. The render: Docker, but no container (behind `e2e`).
// ---------------------------------------------------------------------------

/// `docker compose config --format json`, with `env` applied on top of an
/// environment with every stack variable REMOVED, bounded by a deadline.
#[cfg(feature = "e2e")]
fn render(env: &[(String, String)]) -> serde_json::Value {
    let mut c = Command::new("docker");
    c.args(["compose", "-f", COMPOSE, "config", "--format", "json"])
        .current_dir(root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for p in stack::all_ports() {
        c.env_remove(p.var);
    }
    for v in [
        stack::PROJECT_VAR,
        "COMPOSE_PROFILES",
        "LOGWEIR_K8S_ADVERTISED_HOST",
        "KAFKA_IMAGE",
        "KAFKA_VERSION",
    ] {
        c.env_remove(v);
    }
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

/// **The defaults are today's, and a slot moves every host port, its
/// advertisement, every cluster's id and (with `--kafka`) its image — and
/// nothing else.** A pure render: nothing is started.
///
/// Mutant: hard-code `EXTERNAL://localhost:9092` in the advertisement → the
/// slot arm fails (the rendered listener still says 9092).
#[cfg(feature = "e2e")]
#[test]
fn a_slot_moves_every_host_port_and_the_default_render_does_not() {
    let base = render(&[]);
    assert_eq!(base["name"], stack::default_project());
    assert_eq!(
        published(&base, "kafka-broker-1"),
        BTreeSet::from([(9092, 9092), (9095, 9095), (9097, 9097)])
    );
    assert_eq!(
        published(&base, "minio"),
        BTreeSet::from([(9000, 9000), (9001, 9001)])
    );
    let broker = |doc: &serde_json::Value| doc["services"]["kafka-broker-1"].clone();
    let adv = |doc: &serde_json::Value| {
        broker(doc)["environment"]["KAFKA_ADVERTISED_LISTENERS"]
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
    assert!(
        broker(&base)["environment"].get("CLUSTER_ID").is_none(),
        "the default broker keeps the image's own cluster id"
    );
    assert_eq!(broker(&base)["image"], "apache/kafka:3.7.1");

    let n = 3;
    let mut env = slot_env(n);
    let line_image = "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";
    env.push(("KAFKA_IMAGE".to_string(), line_image.to_string()));
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
    let id_file = |n: u16, file: &str| {
        read(&format!(
            "e2e/compose/slots/{}/{file}",
            stack::slot_project(n)
        ))
        .lines()
        .find_map(|l| l.strip_prefix("CLUSTER_ID=").map(str::to_string))
        .unwrap()
    };
    assert_eq!(
        broker(&slot)["environment"]["CLUSTER_ID"],
        id_file(n, "kafka-broker-1.env").as_str()
    );
    assert_eq!(broker(&slot)["image"], line_image);
    // Nothing else moved: the in-network names and the container ports are
    // the same on every slot.
    assert_eq!(
        broker(&base)["environment"]["KAFKA_LISTENERS"],
        broker(&slot)["environment"]["KAFKA_LISTENERS"]
    );

    // The profile clusters: slot 0's keep, byte for byte, the ids the compose
    // file spelled inline before they moved to slots/; slot n's are its own.
    let profiles = (
        "COMPOSE_PROFILES".to_string(),
        "auth,cluster3,cluster2".to_string(),
    );
    let ids = |doc: &serde_json::Value| -> BTreeMap<String, String> {
        doc["services"]
            .as_object()
            .unwrap()
            .iter()
            .filter_map(|(name, svc)| {
                svc["environment"]["CLUSTER_ID"]
                    .as_str()
                    .map(|id| (name.clone(), id.to_string()))
            })
            .collect()
    };
    let owned = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(
        ids(&render(std::slice::from_ref(&profiles))),
        owned(&[
            ("kafka-auth", "OofTYol1VE2Jm3oa8rkrCw"),
            ("kafka-c3-1", "7vacnhubVwqdN1WQ_ERT8g"),
            ("kafka-c3-2", "7vacnhubVwqdN1WQ_ERT8g"),
            ("kafka-c3-3", "7vacnhubVwqdN1WQ_ERT8g"),
            ("kafka-cluster2", "R66XfW-OXGqtWwdG7Kc7zg"),
        ]),
        "slot 0's profile clusters must keep the ids they always had"
    );
    let mut slot_env_p = slot_env(n);
    slot_env_p.push(profiles);
    let (c3, auth, c2) = (
        id_file(n, "kafka-c3.env"),
        id_file(n, "kafka-auth.env"),
        id_file(n, "kafka-cluster2.env"),
    );
    assert_eq!(
        ids(&render(&slot_env_p)),
        owned(&[
            ("kafka-broker-1", &id_file(n, "kafka-broker-1.env")),
            ("kafka-auth", &auth),
            ("kafka-c3-1", &c3),
            ("kafka-c3-2", &c3),
            ("kafka-c3-3", &c3),
            ("kafka-cluster2", &c2),
        ]),
        "slot {n}'s clusters must each load slot {n}'s own id"
    );
}
