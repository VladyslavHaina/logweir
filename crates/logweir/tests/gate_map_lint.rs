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
        ".github/actions/gate/**",
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
    for (name, gate) in gates(&map) {
        let tests = strings(&gate["tests"]);
        assert_eq!(
            name.starts_with("e2e-"),
            !tests.is_empty(),
            "gate {name}: only e2e shards name e2e binaries, and every shard names some"
        );
        in_gates.extend(tests);
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
        &["unit-core", "unit-logweir", "build-e2e"],
    ),
    (
        "e2e/compose/stack-lib.sh",
        &["unit-core", "unit-logweir", "build-e2e"],
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
            "python", "e2e-1", "e2e-2", "e2e-3", "e2e-4", "e2e-5", "e2e-6", "e2e-7", "e2e-8",
        ],
    ),
];

/// Readers by e2e binary: a shard that runs the binary declares the file.
const E2E_READERS: &[(&str, &str)] = &[
    ("record_semantics", "console/src/pages/restores/plan.ts"),
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
        if tests.is_empty() {
            continue;
        }
        // Every shard runs the harness, over the examples (harness/mod.rs).
        check(name, "e2e/tests/harness/**");
        check(name, "examples/**");
        for (binary, pattern) in E2E_READERS {
            if tests.iter().any(|t| t == binary) {
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
