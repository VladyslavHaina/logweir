//! The crate boundary the `logweir-store` extraction exists to create, asserted
//! at ASSERTION time rather than at link time.
//!
//! `weirkeeper` must be able to hold a read-only archive handle and an evidence
//! handle without linking `logweir-engine-oso` — the crate that carries
//! `subprocess.rs`, `vendored/` and the engine binary path. A cycle back to
//! that crate would fail `cargo build` long before any test ran, which sounds
//! like enough and is not: a build failure is not a NAMED property, nothing
//! records why the build broke, and the moment someone makes the cycle
//! buildable (a `[dev-dependencies]` edge, a feature-gated edge) the boundary
//! is gone with no test to notice. So the property is read out of
//! `cargo metadata`'s DECLARED dependency set, which needs no successful
//! compile.
//!
//! `--no-deps` deliberately: it resolves nothing, downloads nothing and takes
//! no package-cache lock, so this stays a millisecond-scale unit test rather
//! than something that can hang behind a concurrent build (Global Constraint
//! 22's 15 s per-test bound).

use std::collections::BTreeSet;

/// The workspace root: this crate's manifest directory is `crates/logweir-store`.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-store sits two levels under the workspace root")
        .to_path_buf()
}

/// Every dependency `crates/logweir-store/Cargo.toml` DECLARES, of every kind
/// (normal, dev and build alike — a dev edge back to the OSO crate would defeat
/// the extraction just as thoroughly as a normal one).
fn declared_dependencies() -> BTreeSet<String> {
    let out = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        out.status.success(),
        "cargo metadata --no-deps failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata emits JSON");
    let pkg = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .expect("metadata carries packages")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("logweir-store"))
        .expect("logweir-store is a workspace member");
    pkg.get("dependencies")
        .and_then(|d| d.as_array())
        .expect("the package carries a dependency array")
        .iter()
        .filter_map(|d| d.get("name").and_then(|n| n.as_str()))
        .map(|s| s.to_string())
        .collect()
}

/// TWO SEPARATE CLAIMS, deliberately.
///
/// (a) is the exact set, so a future dependency addition — legitimate or not —
/// fails loudly and gets read by a human. (b) is the property this crate
/// exists for, asserted on its own so that relaxing (a) for a legitimate
/// addition can never weaken it. One combined assertion would let a widened
/// set quietly carry the OSO crate back in, which is exactly the mutant.
#[test]
fn logweir_store_depends_on_nothing_oso() {
    let deps = declared_dependencies();

    // (a) the exact declared set.
    let expected: BTreeSet<String> = [
        "chrono",
        "futures",
        "logweir-core",
        "object_store",
        "serde_json",
        "thiserror",
        "tokio",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        deps, expected,
        "logweir-store's declared dependency set changed. `serde_json` is REQUIRED \
         (the untyped `serde_json::Value` reads in `segments_in_manifest` and \
         `manifest_facts`); `tracing` is deliberately ABSENT and cannot be \
         inherited. Adding a dependency here is a reviewable event: change this \
         list in the same commit and say why."
    );

    // (b) the boundary itself, on its own.
    assert!(
        !declares_oso(&deps),
        "logweir-store exists so a component can hold an object-store handle \
         WITHOUT linking the OSO engine wrapper (ADR 0008 §E); it declared {deps:?}"
    );
}

/// The boundary predicate, extracted from the assertion above so that its
/// SECOND disjunct is reachable by a test.
///
/// `logweir-engine-oso` is caught by name; the `kafka-backup` PREFIX arm exists
/// for Global Constraint 2 — `kafka-backup-core` is never a workspace
/// dependency, in any version, behind any feature, and neither is any other
/// `kafka-backup*` crate. Nothing in this tree declares one, so on the real
/// dependency set that arm is dead code, and dead code in an assertion is how
/// a guard quietly stops covering half of what it claims.
fn declares_oso(deps: &BTreeSet<String>) -> bool {
    deps.contains("logweir-engine-oso") || deps.iter().any(|d| d.starts_with("kafka-backup"))
}

/// Task 13 review carry (b): the `kafka-backup` prefix arm, with a case.
#[test]
fn the_boundary_predicate_catches_a_kafka_backup_prefix() {
    let clean: BTreeSet<String> = ["logweir-core", "object_store"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(
        !declares_oso(&clean),
        "a clean set must not trip the predicate"
    );

    for bad in ["logweir-engine-oso", "kafka-backup-core", "kafka-backup"] {
        let mut set = clean.clone();
        set.insert(bad.to_string());
        assert!(
            declares_oso(&set),
            "`{bad}` must trip the boundary predicate (Global Constraint 2 for \
             the `kafka-backup` prefix, ADR 0008 §E for the engine wrapper)"
        );
    }
}
