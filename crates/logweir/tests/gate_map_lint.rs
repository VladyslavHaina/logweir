//! FX-53 §2.1, §2.6: the gate map `ci/gates.toml` is complete enough to skip
//! on.
//!
//! A gate is skipped only when a marker proves it passed on the same input
//! hash, so a file a gate reads but does not declare is a file whose change
//! skips that gate. These rows hold the map to what is known about the reads:
//! every gate has inputs and a definition, every pattern names a tracked file,
//! every crate exists, today's gate ids are all mapped, every e2e binary sits
//! in a shard, and every hidden reader of cicd-structure §2.2/§2.3 is declared
//! by the gate that runs its tests. The weekly traced run (`ci-trace.yml`)
//! catches the reads no static list knows about.
//!
//! The map is read through `scripts/ci-gate.sh describe`, the expansion the
//! hash itself uses; `scripts/test-ci-gate.py` holds its matcher to git's.
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn gate_script(args: &[&str]) -> Value {
    let out = Command::new("bash")
        .arg("scripts/ci-gate.sh")
        .args(args)
        .current_dir(root())
        .output()
        .expect("bash runs scripts/ci-gate.sh");
    assert!(
        out.status.success(),
        "ci-gate.sh {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("ci-gate.sh prints JSON")
}

fn map() -> Value {
    gate_script(&["describe"])
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap_or_else(|| panic!("not a list: {v}"))
        .iter()
        .map(|s| s.as_str().expect("a string").to_string())
        .collect()
}

fn gates(map: &Value) -> &serde_json::Map<String, Value> {
    map["gates"].as_object().expect("gates")
}

fn files(map: &Value, gate: &str) -> BTreeSet<String> {
    strings(&map["gates"][gate]["files"]).into_iter().collect()
}

#[test]
fn every_gate_has_inputs_and_a_definition() {
    let map = map();
    assert!(
        gates(&map).len() >= 20,
        "the map names {} gates",
        gates(&map).len()
    );
    let global: BTreeSet<String> = strings(&map["global"]).into_iter().collect();
    for required in [
        "Cargo.lock",
        "Cargo.toml",
        "rust-toolchain.toml",
        "ci/gates.toml",
        "scripts/ci-gate.sh",
        ".github/actions/**",
        ".github/workflows/ci.yml",
    ] {
        assert!(
            global.contains(required),
            "[global].paths must hold {required}: a change to it changes every gate"
        );
    }
    for (name, gate) in gates(&map) {
        let patterns = &gate["patterns"];
        assert!(
            !strings(&patterns["definition"]).is_empty(),
            "gate {name} has no definition: the scripts and recipes its job runs"
        );
        let own: usize = ["crates", "paths", "readers", "walks", "tests"]
            .iter()
            .map(|k| strings(&patterns[*k]).len())
            .sum();
        assert!(own > 0, "gate {name} declares no inputs of its own");
        assert!(
            !strings(&gate["covers"]).is_empty(),
            "gate {name} covers none of today's gate ids"
        );
    }
}

#[test]
fn every_declared_pattern_matches_a_tracked_file() {
    let map = map();
    let mut stale = Vec::new();
    for (name, gate) in gates(&map) {
        for empty in strings(&gate["empty"]) {
            stale.push(format!("{name}: {empty}"));
        }
    }
    assert!(
        stale.is_empty(),
        "patterns that match no tracked file (a stale map; the hash refuses them, so the \
         gate always runs):\n{}",
        stale.join("\n")
    );
}

#[test]
fn every_crate_named_exists() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .args([
            "metadata",
            "--locked",
            "--offline",
            "--no-deps",
            "--format-version",
            "1",
        ])
        .current_dir(root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: Value = serde_json::from_slice(&out.stdout).unwrap();
    let members: BTreeSet<String> = meta["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();
    let map = map();
    let mut tested = BTreeSet::new();
    for (name, gate) in gates(&map) {
        for krate in strings(&gate["crates"]) {
            assert!(
                krate == "*" || members.contains(&krate),
                "gate {name} names crate {krate}, which is not a workspace package"
            );
        }
        if name.starts_with("unit-") {
            tested.extend(strings(&gate["crates"]));
        }
    }
    // Every package's own tests run in some unit gate (C16 is
    // `cargo test --workspace` today); e2e's no-feature rows run in unit-e2e.
    for member in &members {
        assert!(
            tested.contains(member) || member == "e2e",
            "no unit gate tests {member}"
        );
    }
    assert!(gates(&map).contains_key("unit-e2e"));
}

/// Today's gate ids (cicd-structure §1.1) each sit in some gate, so the parity
/// table is checkable.
#[test]
fn todays_gate_ids_are_each_mapped() {
    let map = map();
    let covered: BTreeSet<String> = gates(&map)
        .values()
        .flat_map(|g| strings(&g["covers"]))
        .collect();
    let mut wanted: Vec<String> = (1..=30).map(|i| format!("C{i}")).collect();
    wanted.extend((1..=5).map(|i| format!("E{i}")));
    wanted.push("K1".into());
    let missing: Vec<&String> = wanted.iter().filter(|id| !covered.contains(*id)).collect();
    assert!(
        missing.is_empty(),
        "today's gates with no gate in ci/gates.toml: {missing:?}"
    );
}

#[test]
fn every_e2e_binary_is_in_an_e2e_gate() {
    let map = map();
    let mut in_gates = BTreeSet::new();
    // A test another shard runs as a row: each row is left out (`except`) of
    // the one shard that runs its binary whole, and each except is some
    // shard's row, exactly once.
    let mut rows: Vec<String> = Vec::new();
    let mut excepted: Vec<String> = Vec::new();
    let mut whole: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, gate) in gates(&map) {
        let tests = strings(&gate["tests"]);
        assert_eq!(
            name.starts_with("e2e-"),
            !tests.is_empty(),
            "gate {name}: only e2e shards name e2e binaries, and every shard names some"
        );
        for t in &tests {
            whole.entry(t.clone()).or_default().push(name.clone());
        }
        for x in strings(&gate["except"]) {
            let binary = x.split("::").next().unwrap().to_string();
            assert!(
                tests.contains(&binary),
                "gate {name} excepts {x}, but does not run {binary}"
            );
            excepted.push(x);
        }
        rows.extend(strings(&gate["rows"]));
        in_gates.extend(tests);
    }
    rows.sort();
    excepted.sort();
    assert_eq!(
        rows, excepted,
        "every row is excepted where its binary runs whole"
    );
    let mut unique = rows.clone();
    unique.dedup();
    assert_eq!(unique, rows, "a row runs in one shard");
    for (binary, shards) in &whole {
        assert_eq!(shards.len(), 1, "{binary} runs whole in {shards:?}");
    }
    let mut binaries = BTreeSet::new();
    for entry in std::fs::read_dir(root().join("e2e/tests")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            binaries.insert(path.file_stem().unwrap().to_string_lossy().into_owned());
        } else if path.join("main.rs").is_file() {
            binaries.insert(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    assert!(
        binaries.len() >= 30,
        "found {} e2e binaries",
        binaries.len()
    );
    assert_eq!(
        in_gates, binaries,
        "the e2e shards' binaries must be exactly e2e/tests/*"
    );
}

/// Every tracked file is classified by some gate. An unclassified file is an
/// input of EVERY gate (the fail-safe for a new path), so this row keeps the
/// fail-safe for paths the map has not met yet.
#[test]
fn every_tracked_file_is_classified() {
    let map = map();
    let unclaimed = strings(&map["unclaimed"]);
    assert!(
        unclaimed.is_empty(),
        "tracked files no gate classifies; each is an input of every gate until one does:\n{}",
        unclaimed.join("\n")
    );
}

/// The hidden readers (cicd-structure §2.2, §2.3): the repository files tests
/// read at run time or compile in, each with the gates whose tests read it.
const HIDDEN_READERS: &[(&str, &[&str])] = &[
    // docs/** (about 25 Rust test binaries)
    (
        "docs/**",
        &[
            "unit-logweir",
            "unit-api",
            "unit-core",
            "unit-weirkeeper",
            "docs",
        ],
    ),
    // e2e's stack_params, in check today
    ("docs/support-matrix.md", &["unit-e2e"]),
    ("docs/to-do/**", &["unit-logweir", "docs"]),
    ("README.md", &["unit-logweir", "unit-api", "docs"]),
    ("CONTRIBUTING.md", &["unit-logweir", "unit-api", "docs"]),
    ("SECURITY.md", &["unit-logweir", "unit-api", "docs"]),
    ("MAINTAINERS.md", &["unit-logweir", "unit-api", "docs"]),
    ("TRADEMARKS.md", &["unit-logweir", "unit-api", "docs"]),
    ("LICENSE", &["unit-logweir", "unit-api", "python"]),
    ("NOTICE", &["unit-logweir", "unit-api", "python"]),
    (
        "THIRD_PARTY_NOTICES.md",
        &["unit-logweir", "unit-api", "python", "supply-chain"],
    ),
    (
        "charts/**",
        &["unit-logweir", "unit-api", "unit-weirkeeper", "generated"],
    ),
    (
        "schemas/**",
        &[
            "unit-logweir",
            "unit-api",
            "unit-core",
            "unit-weirkeeper",
            "generated",
        ],
    ),
    (
        "console/fixtures/**",
        &["unit-api", "unit-logweir", "unit-weirkeeper", "console"],
    ),
    ("console/src/main.tsx", &["unit-logweir"]),
    ("console/scripts/emit-restore-body.mjs", &["unit-logweir"]),
    (
        "e2e/fixtures/**",
        &["unit-logweir", "unit-core", "unit-weirkeeper", "python"],
    ),
    (
        "e2e/fixtures/scorecard-pass.json",
        &["unit-logweir", "unit-core"],
    ),
    ("e2e/compose/**", &["unit-e2e", "unit-logweir"]),
    ("e2e/tests/**", &["unit-e2e", "unit-logweir"]),
    // the #[path] stack.rs and its include_str!
    (
        "e2e/tests/harness/stack.rs",
        &["unit-core", "unit-logweir", "e2e-3"],
    ),
    (
        "e2e/compose/stack-lib.sh",
        &["unit-core", "unit-logweir", "e2e-3"],
    ),
    ("examples/**", &["unit-logweir", "unit-weirkeeper"]),
    (
        "config/**",
        &["unit-logweir", "unit-api", "unit-weirkeeper"],
    ),
    ("dashboards/**", &["unit-logweir"]),
    ("third_party/**", &["unit-logweir", "unit-e2e"]),
    (
        "scripts/**",
        &["unit-logweir", "unit-api", "unit-weirkeeper", "unit-e2e"],
    ),
    (
        ".github/workflows/**",
        &["unit-logweir", "unit-weirkeeper", "unit-e2e"],
    ),
    (
        "justfile",
        &["unit-logweir", "unit-core", "unit-weirkeeper", "unit-e2e"],
    ),
    ("clippy.toml", &["unit-logweir", "lint"]),
    ("deny.toml", &["unit-logweir", "supply-chain"]),
    ("Dockerfile", &["unit-logweir", "unit-e2e"]),
    (
        "Dockerfile.console",
        &["unit-logweir", "unit-e2e", "generated"],
    ),
    ("Dockerfile.weirkeeper", &["unit-logweir", "unit-e2e"]),
    (".dockerignore", &["unit-logweir", "unit-e2e"]),
    ("deploy/**", &["unit-logweir", "docs"]),
    // another crate's source text
    ("crates/weirkeeper/**", &["unit-logweir", "unit-core"]),
    (
        "crates/logweir/**",
        &["unit-weirkeeper", "unit-core", "unit-e2e"],
    ),
    ("crates/logweir-api/**", &["unit-logweir"]),
    ("crates/logweir-kafka/**", &["unit-logweir", "unit-core"]),
    (
        "crates/logweir-core/**",
        &["unit-logweir", "unit-weirkeeper"],
    ),
    ("crates/logweir-engine-oso/**", &["unit-xtask"]),
    // gap 1: the harness runs the verifier
    (
        "docs/verify_scorecard.py",
        &[
            "python", "e2e-1", "e2e-2", "e2e-3", "e2e-4", "e2e-5", "e2e-6", "e2e-7",
        ],
    ),
];

/// Readers by e2e binary: a shard that runs the binary declares the file.
const E2E_READERS: &[(&str, &str)] = &[
    (
        "record_semantics::a_console_plan_asking_for_complete_coverage_verifies_every_record_on_the_stack",
        "console/src/pages/restores/plan.ts",
    ),
    (
        "schema_dependency",
        "console/scripts/emit-schema-dependency.mjs",
    ),
    ("schema_dependency", "console/src/pages/catalog/model.ts"),
    ("stack_params", "docs/support-matrix.md"),
    ("stack_params", "scripts/**"),
    ("check_image", "Dockerfile"),
    ("mvp_demo", "scripts/mvp-demo.sh"),
];

#[test]
fn the_hidden_readers_are_each_declared_by_their_gates() {
    let map = map();
    let mut patterns: Vec<&str> = HIDDEN_READERS.iter().map(|(p, _)| *p).collect();
    patterns.extend(E2E_READERS.iter().map(|(_, p)| *p));
    patterns.extend(["e2e/tests/harness/**", "examples/**"]);
    let mut args = vec!["match"];
    args.extend(patterns.iter().copied());
    let matched = gate_script(&args);
    let named = |p: &str| -> BTreeSet<String> {
        let set: BTreeSet<String> = strings(&matched[p]).into_iter().collect();
        assert!(!set.is_empty(), "{p} names no tracked file");
        set
    };
    let mut gaps: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut check = |gate: &str, pattern: &str| {
        assert!(gates(&map).contains_key(gate), "no gate {gate}");
        let have = files(&map, gate);
        let missing: Vec<String> = named(pattern).difference(&have).cloned().collect();
        if !missing.is_empty() {
            gaps.entry(format!("{gate} must declare {pattern}"))
                .or_default()
                .extend(missing.into_iter().take(3));
        }
    };
    for (pattern, readers) in HIDDEN_READERS {
        for gate in *readers {
            check(gate, pattern);
        }
    }
    for (name, gate) in gates(&map) {
        let tests = strings(&gate["tests"]);
        let rows = strings(&gate["rows"]);
        let except = strings(&gate["except"]);
        if tests.is_empty() && rows.is_empty() {
            continue;
        }
        // Every shard runs the harness, over the examples (harness/mod.rs).
        check(name, "e2e/tests/harness/**");
        check(name, "examples/**");
        // A reader keyed by `<binary>` applies to a shard that runs any of the
        // binary; one keyed by `<binary>::<test>` to the shard that runs that
        // test: as a row, or within its binary unless it is excepted.
        for (unit, pattern) in E2E_READERS {
            let runs = match unit.split_once("::") {
                None => {
                    tests.iter().any(|t| t == unit)
                        || rows.iter().any(|r| r.split("::").next() == Some(*unit))
                }
                Some((binary, _)) => {
                    rows.iter().any(|r| r == unit)
                        || (tests.iter().any(|t| t == binary) && !except.iter().any(|x| x == unit))
                }
            };
            if runs {
                check(name, pattern);
            }
        }
    }
    assert!(
        gaps.is_empty(),
        "hidden readers a gate does not declare (its tests would be skipped when the file \
         changes):\n{}",
        gaps.iter()
            .map(|(k, v)| format!("{k}: e.g. {}", v.join(", ")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// An always gate never skips; the time- and registry-dependent gates are
/// always (FX-53 §2.1: advisories, npm audit).
#[test]
fn the_time_dependent_gates_never_skip() {
    let map = map();
    for (gate, id) in [("supply-chain", "C28"), ("console", "K1")] {
        assert_eq!(
            map["gates"][gate]["always"],
            Value::Bool(true),
            "{gate} must be always = true"
        );
        assert!(strings(&map["gates"][gate]["covers"]).contains(&id.to_string()));
    }
}

/// A gate's proof is its job's `success` in the proving run (the jobs API),
/// so no gate job may turn a failure into success: no `continue-on-error`, on
/// the job or on any step. The gate jobs are ci.yml's (release.yml and
/// ci-nightly.yml only call it), so the row reads every job of ci.yml, which
/// is cicd-w1's file; stage 4 adds "every ci.yml job is a gate".
#[test]
fn no_gate_job_can_pass_on_a_failure() {
    let map = map();
    let provers = strings(&map["provers"]);
    assert!(provers.contains(&".github/workflows/ci.yml".to_string()));
    let ci = ".github/workflows/ci.yml";
    let text = std::fs::read_to_string(root().join(ci)).expect("ci.yml");
    let doc: serde_yaml::Value = serde_yaml::from_str(&text).expect("ci.yml parses");
    let mut found = Vec::new();
    for (id, job) in doc["jobs"].as_mapping().expect("jobs") {
        let id = id.as_str().unwrap_or_default();
        let lenient = |v: &serde_yaml::Value| !v.is_null() && v.as_bool() != Some(false);
        if lenient(&job["continue-on-error"]) {
            found.push(format!("{ci}: job {id}"));
        }
        for (n, step) in job["steps"].as_sequence().into_iter().flatten().enumerate() {
            if lenient(&step["continue-on-error"]) {
                found.push(format!("{ci}: job {id}, step {}", n + 1));
            }
        }
    }
    assert!(
        found.is_empty(),
        "a job whose failure can conclude `success` proves nothing to a marker:\n{}",
        found.join("\n")
    );
}

/// FX-53 §2.6: the nightly runs every gate (`force: true`) and publishes
/// nothing. It calls ci.yml with no secret, only on main, from a schedule or a
/// dispatch; ci.yml's only publishing job runs on a main push alone, and in a
/// called workflow `github.event_name` is the caller's. Its job id `ci` is the
/// `ci / ` prefix the marker lookup requires (scripts/ci-gate.sh, PROVERS).
#[test]
fn the_nightly_forces_every_gate_and_publishes_nothing() {
    let read = |name: &str| -> serde_yaml::Value {
        serde_yaml::from_str(&std::fs::read_to_string(root().join(name)).unwrap()).unwrap()
    };
    let nightly = read(".github/workflows/ci-nightly.yml");
    let triggers: BTreeSet<&str> = nightly["on"]
        .as_mapping()
        .expect("on")
        .keys()
        .map(|k| k.as_str().unwrap())
        .collect();
    assert_eq!(triggers, BTreeSet::from(["schedule", "workflow_dispatch"]));
    let jobs = nightly["jobs"].as_mapping().expect("jobs");
    assert_eq!(jobs.len(), 1, "the nightly is one call of ci.yml");
    let ci = &nightly["jobs"]["ci"];
    assert_eq!(ci["uses"].as_str(), Some("./.github/workflows/ci.yml"));
    assert_eq!(ci["if"].as_str(), Some("github.ref == 'refs/heads/main'"));
    assert!(ci["secrets"].is_null(), "the nightly passes no secret");
    assert_eq!(ci["with"]["force"].as_bool(), Some(true));
    assert_eq!(ci["with"].as_mapping().map(|m| m.len()), Some(1));
    let main = read(".github/workflows/ci.yml");
    for (id, job) in main["jobs"].as_mapping().unwrap() {
        let id = id.as_str().unwrap();
        if id == "publish" {
            let condition = job["if"].as_str().unwrap_or_default();
            assert!(
                condition
                    .starts_with("github.event_name == 'push' && github.ref == 'refs/heads/main'"),
                "ci.yml publish must run on a main push only: {condition}"
            );
        } else {
            assert!(job["secrets"].is_null(), "ci.yml job {id} takes secrets");
        }
    }
}

/// Every workflow a proving run executes is an input of every gate: the
/// PROVERS workflows (scripts/ci-gate.sh) and every workflow they call, so a
/// run proves a hash only if it ran the same workflow text. Otherwise a
/// changed caller could add a job named like a gate's (`tests / check`) that
/// runs `true`, and its run would prove main's real hash.
#[test]
fn every_workflow_a_proving_run_executes_is_a_global_input() {
    let map = map();
    let global: BTreeSet<String> = strings(&map["global"]).into_iter().collect();
    assert!(
        global.contains(".github/actions/**"),
        "the local actions are global inputs"
    );
    let mut pending = strings(&map["provers"]);
    assert!(pending.len() >= 3, "provers: {pending:?}");
    let mut seen = BTreeSet::new();
    while let Some(path) = pending.pop() {
        if !seen.insert(path.clone()) {
            continue;
        }
        assert!(
            global.contains(&path),
            "{path} runs in a proving run but is not in [global].paths"
        );
        let text =
            std::fs::read_to_string(root().join(&path)).unwrap_or_else(|e| panic!("{path}: {e}"));
        let doc: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
        for (_, job) in doc["jobs"].as_mapping().expect("jobs") {
            if let Some(called) = job["uses"].as_str() {
                let called = called
                    .strip_prefix("./")
                    .unwrap_or_else(|| panic!("{path} calls a remote workflow {called}"));
                pending.push(called.to_string());
            }
        }
    }
    assert!(seen.contains(".github/workflows/images.yml"));
    assert!(seen.contains(".github/workflows/release-drill.yml"));
}

/// FX-53 stage 4: EVERY ci.yml JOB IS A GATE OF ci/gates.toml OR ONE OF ITS
/// [not_gates], AND EVERY GATE IS A JOB, wired as the map says:
/// - the gate's `job` is the name of the ci.yml job that runs it (a `check`
///   row's `job`, `e2e N` for a shard, or the job's id);
/// - the gate step (`./.github/actions/gate`, with that gate's name) comes
///   right after the checkout, before anything that could write a tracked
///   file;
/// - every later step is guarded by the gate's `hit` (or, in a row of several
///   gates, by "any of them missed"), except the record steps;
/// - a record step names the gate, runs only after `success()`, only when
///   `GATE_RECORD` (a proving event) and only on a miss.
#[test]
fn every_ci_job_is_a_gate_and_every_gate_a_job() {
    let map = map();
    let declared: BTreeMap<String, String> = gates(&map)
        .iter()
        .map(|(g, v)| (g.clone(), v["job"].as_str().unwrap_or(g).to_string()))
        .collect();
    let not_gates: BTreeSet<String> = strings(&map["not_gates"]).into_iter().collect();
    let text = std::fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    let ci: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
    let record_if = "success() && env.GATE_RECORD == 'true' && ";
    let mut wired: BTreeMap<String, String> = BTreeMap::new();
    let mut plumbing = BTreeSet::new();
    for (id, job) in ci["jobs"].as_mapping().unwrap() {
        let id = id.as_str().unwrap().to_string();
        let steps: Vec<serde_yaml::Value> = job["steps"].as_sequence().cloned().unwrap_or_default();
        let gate_steps: Vec<usize> = steps
            .iter()
            .enumerate()
            .filter(|(_, s)| s["uses"].as_str() == Some("./.github/actions/gate"))
            .map(|(n, _)| n)
            .collect();
        if gate_steps.is_empty() {
            assert!(
                not_gates.contains(&id),
                "ci.yml job `{id}` runs no gate and is not in ci/gates.toml's [not_gates]"
            );
            plumbing.insert(id);
            continue;
        }
        assert!(
            !not_gates.contains(&id),
            "ci.yml job `{id}` runs a gate but is listed in [not_gates]"
        );
        // The gate steps come first, right after the checkout.
        assert!(
            steps[0]["uses"]
                .as_str()
                .is_some_and(|u| u.starts_with("actions/checkout@")),
            "ci.yml job `{id}`: the checkout comes first"
        );
        assert_eq!(
            gate_steps,
            (1..=gate_steps.len()).collect::<Vec<_>>(),
            "ci.yml job `{id}`: the gate steps follow the checkout, before anything else"
        );
        // The (gate, job name) pairs this job runs.
        let rows = job["strategy"]["matrix"]["include"].as_sequence().cloned();
        let mut pairs: Vec<(String, String)> = Vec::new();
        match (id.as_str(), rows) {
            ("e2e", Some(rows)) => {
                for row in rows {
                    let n = serde_yaml::to_string(&row["shard"]).unwrap();
                    let n = n.trim();
                    pairs.push((format!("e2e-{n}"), format!("e2e {n}")));
                }
                assert_eq!(
                    steps[1]["with"]["gate"].as_str(),
                    Some("e2e-${{ matrix.shard }}")
                );
            }
            (_, Some(rows)) => {
                for row in rows {
                    let name = row["job"].as_str().unwrap().to_string();
                    for (n, key) in ["gate1", "gate2", "gate3"].iter().enumerate() {
                        if let Some(g) = row[*key].as_str() {
                            pairs.push((g.to_string(), name.clone()));
                            assert_eq!(
                                steps[1 + n]["with"]["gate"].as_str(),
                                Some(format!("${{{{ matrix.{key} }}}}").as_str())
                            );
                        }
                    }
                }
            }
            (_, None) => {
                assert_eq!(gate_steps.len(), 1, "ci.yml job `{id}`: one gate");
                let g = steps[1]["with"]["gate"].as_str().unwrap().to_string();
                let name = job["name"].as_str().unwrap_or(&id).to_string();
                pairs.push((g, name));
            }
        }
        for (g, name) in pairs {
            assert!(
                wired.insert(g.clone(), name.clone()).is_none(),
                "gate `{g}` is run by two ci.yml jobs"
            );
        }
        // Every later step is guarded; the record steps are the last ones.
        let guard = |s: &serde_yaml::Value| s["if"].as_str().unwrap_or_default().to_string();
        let mut records = 0;
        for (n, step) in steps.iter().enumerate().skip(1 + gate_steps.len()) {
            let cond = guard(step);
            if step["uses"].as_str() == Some("./.github/actions/gate/record") {
                assert!(
                    cond.starts_with(record_if) && cond.contains("outputs.hit != 'true'"),
                    "ci.yml job `{id}`, step {}: a record runs after success(), on a proving \
                     event, on a miss: `{cond}`",
                    n + 1
                );
                assert!(step["with"]["gate"].is_string() && step["with"]["hash"].is_string());
                records += 1;
                continue;
            }
            assert_eq!(records, 0, "ci.yml job `{id}`: the record steps come last");
            assert!(
                cond.contains("outputs.hit != 'true'")
                    || cond.contains("steps.work.outputs.any == 'true'")
                    || (n == 1 + gate_steps.len() && step["id"].as_str() == Some("work")),
                "ci.yml job `{id}`, step {} runs whatever the gate decided: `{cond}`",
                n + 1
            );
        }
        assert_eq!(
            records,
            gate_steps.len(),
            "ci.yml job `{id}`: one record per gate"
        );
    }
    assert_eq!(
        wired, declared,
        "the gates ci.yml runs, by the name of the job that runs each, must be exactly \
         ci/gates.toml's"
    );
    assert_eq!(
        plumbing, not_gates,
        "[not_gates] must be exactly ci.yml's other jobs"
    );
}

/// FX-53: ci.yml's shard lists are the map's. A shard runs the filter its
/// gate gives (`ci-gate.sh filter`), and stack_params' partition guard reads
/// ci.yml's `tests`, so the two lists must be one: each matrix row's `tests`,
/// `rows` and `except` equal its gate's.
#[test]
fn the_shard_lists_in_ci_yml_are_the_maps() {
    let map = map();
    let text = std::fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    let ci: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
    let rows = ci["jobs"]["e2e"]["strategy"]["matrix"]["include"]
        .as_sequence()
        .expect("the e2e matrix");
    let list = |v: &serde_yaml::Value| -> Vec<String> {
        v.as_sequence()
            .map(|s| s.iter().map(|x| x.as_str().unwrap().to_string()).collect())
            .unwrap_or_default()
    };
    let mut seen = BTreeSet::new();
    for row in rows {
        let n = serde_yaml::to_string(&row["shard"]).unwrap();
        let gate = format!("e2e-{}", n.trim());
        let declared = &map["gates"][&gate];
        assert!(
            declared.is_object(),
            "ci.yml shard {gate} is no gate of the map"
        );
        for key in ["tests", "rows", "except"] {
            assert_eq!(
                list(&row[key]),
                strings(&declared[key]),
                "ci.yml shard {gate}'s `{key}` differs from ci/gates.toml's"
            );
        }
        seen.insert(gate);
    }
    let shards: BTreeSet<String> = gates(&map)
        .keys()
        .filter(|g| g.starts_with("e2e-"))
        .cloned()
        .collect();
    assert_eq!(seen, shards, "every e2e gate is a ci.yml shard");
}
