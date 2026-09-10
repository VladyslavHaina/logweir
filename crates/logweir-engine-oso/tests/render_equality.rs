//! T0-14 — `restore.yaml` is rendered twice (once inside `preflight()` for
//! `kafka-backup validate-restore`, once inside `restore()` for
//! `kafka-backup restore`) and `OsoCliEngine::write` calls `std::fs::write`,
//! which TRUNCATES. Phase 6 therefore physically overwrites, in the workdir,
//! the exact file phase 5 handed to the engine. Until the seam these tests
//! pin existed, nothing hashed either document and nothing compared them: the
//! two were "the same document" only because
//! `crates/logweir/src/drill/mod.rs:576` happened to build `plan` once and
//! hand the same value to both call sites. That is a property of one call
//! site, not a guarantee, and it dissolves the moment phase 5 and phase 6 are
//! split.
//!
//! Ruling **R-E**: a divergence is `EngineError::Operational`, which routes to
//! **exit 1** (operational error, no artifact) through `DrillError::Engine` at
//! `crates/logweir/src/drill/mod.rs:105` — NOT exit 3. Global Constraint 11
//! reserves `3` for "plan refused by a guard, before anything runs", and by
//! phase 6 phases 0-5 have run, including the engine's own `validate-restore`.
//! The exit code itself is asserted from `crates/logweir`
//! (`tests/restore_phase.rs::render_mismatch_is_operational_not_guard`),
//! because `logweir-engine-oso` does not and must not depend on `logweir`.
//! The ADR that would carry R-E is gated on open question O2 and is deferred;
//! `docs/stability.md` is the record.
use logweir_core::engine::{
    BackupSetRef, DataEngine, EngineError, PhaseObserver, RestorePlan, StorageUrl,
    WindowFloorSource,
};
use logweir_engine_oso::engine::OsoCliEngine;
use logweir_engine_oso::render_restore;
use logweir_engine_oso::storage::Store;
use std::path::PathBuf;

/// Copied verbatim from `tests/engine.rs:15` — integration test files do not
/// share modules.
fn unique_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "logweir-engine-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Copied verbatim from `tests/engine.rs:28`.
fn plan() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "b1".into(),
            manifest_key: "b1/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/archive".into(),
        },
        target_bootstrap: vec!["kafka-broker-1:9092".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping: [("orders".to_string(), "drill-20260903-orders".to_string())]
            .into_iter()
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

/// The divergent plan: ONE ordinary field changed — the destination of the
/// existing `topic_mapping` entry. Ruling GR7 binds here as a prohibition:
/// the divergence is never produced by injecting `purge_topics`, `dry_run` or
/// `header_preflight_external` (Global Constraint 4), and no test-only code
/// path exists that could emit one.
fn divergent_plan() -> RestorePlan {
    let mut p = plan();
    p.topic_mapping = [("orders".to_string(), "drill-20260903-orders-2".to_string())]
        .into_iter()
        .collect();
    p
}

/// Copied from the `PhaseObserver` double used by
/// `restore_against_a_clean_engine_succeeds_with_no_warnings`
/// (`tests/engine.rs:167-185`).
#[derive(Default)]
struct RecordingObserver {
    lines: Vec<(String, String)>,
    phases: Vec<(i8, String)>,
}

impl PhaseObserver for RecordingObserver {
    fn phase_started(&mut self, phase: i8, name: &str) {
        self.phases.push((phase, format!("started:{name}")));
    }
    fn phase_finished(&mut self, phase: i8, outcome: &str) {
        self.phases.push((phase, format!("finished:{outcome}")));
    }
    fn engine_line(&mut self, stream: &str, line: &str) {
        self.lines.push((stream.to_string(), line.to_string()));
    }
}

/// `tests/engine.rs:69-78`, with the workdir hoisted out so a test can read
/// the `restore.yaml` the engine wrote. Nothing else differs.
fn engine_at(binary: &str, workdir: PathBuf, store: Store) -> OsoCliEngine {
    OsoCliEngine::new(
        PathBuf::from(binary),
        "v0.21.0-test".into(),
        "sha256:testdigest".into(),
        workdir,
        store,
    )
}

const CLEAN: &str = "../../e2e/fixtures/fake-engine-clean.sh";

#[test]
fn render_equality_digest_is_over_the_written_bytes() {
    let p = plan();
    let (doc, digest) = render_restore::render_and_digest(&p)
        .expect("G-GLOB: this fixture holds no glob metacharacter");
    assert_eq!(
        doc,
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter"),
        "render_and_digest must return the render verbatim"
    );
    assert_eq!(
        digest,
        logweir_core::ids::sha256_prefixed(
            render_restore::render(&p)
                .expect("G-GLOB: this fixture holds no glob metacharacter")
                .as_bytes()
        ),
        "the digest must be over the rendered bytes, not over a re-serialisation of the plan"
    );
    assert!(
        digest.starts_with("sha256:"),
        "digest must be sha256-prefixed"
    );
}

#[test]
fn phase6_accepts_the_identical_document() {
    let p = plan();
    let engine = engine_at(CLEAN, unique_dir("accepts"), Store::in_memory("logweir"));
    let report = engine.preflight(&p).expect("preflight");
    assert_eq!(
        report.rendered_restore_sha256,
        render_restore::render_and_digest(&p)
            .expect("G-GLOB: this fixture holds no glob metacharacter")
            .1,
        "phase 5 must record the digest of the document it wrote"
    );
    let mut obs = RecordingObserver::default();
    let facts = engine
        .restore(&p, &mut obs)
        .expect("restore must accept the identical document");
    assert_eq!(facts.exit_code, 0);
}

#[test]
fn phase6_refuses_when_rendered_restore_differs_from_phase5() {
    let p = plan();
    let mutated = divergent_plan();
    assert_ne!(
        render_restore::render(&mutated).expect("G-GLOB: this fixture holds no glob metacharacter"),
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter"),
        "the fixture must actually diverge, or this test degenerates into a tautology"
    );

    let workdir = unique_dir("refuses");
    let engine = engine_at(CLEAN, workdir.clone(), Store::in_memory("logweir"));
    let report = engine.preflight(&p).expect("preflight");
    let five = report.rendered_restore_sha256.clone();
    let six = render_restore::render_and_digest(&mutated)
        .expect("G-GLOB: this fixture holds no glob metacharacter")
        .1;
    assert_ne!(five, six, "the two digests must differ");

    let mut obs = RecordingObserver::default();
    let err = engine
        .restore(&mutated, &mut obs)
        .expect_err("phase 6 must refuse a divergent render");
    let msg = err.to_string();
    assert!(
        matches!(err, EngineError::Operational(_)),
        "must be Operational, not Unsupported: {msg}"
    );
    assert!(
        msg.contains("rendered restore.yaml diverged between phase 5 and phase 6"),
        "{msg}"
    );
    assert!(
        msg.contains(&five),
        "message must name the phase-5 digest: {msg}"
    );
    assert!(
        msg.contains(&six),
        "message must name the phase-6 digest: {msg}"
    );

    // Mutant M9: the refusal must happen BEFORE `self.write` truncates phase
    // 5's file. The message assertions above all survive a comparison moved
    // after the write; this one does not.
    assert_eq!(
        std::fs::read_to_string(workdir.join("restore.yaml")).unwrap(),
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter"),
        "a refused phase 6 must not have overwritten the document phase 5 validated"
    );
}

#[test]
fn restore_without_preflight_is_refused() {
    let engine = engine_at(
        CLEAN,
        unique_dir("no-preflight"),
        Store::in_memory("logweir"),
    );
    let mut obs = RecordingObserver::default();
    let err = engine
        .restore(&plan(), &mut obs)
        .expect_err("restore() with no phase-5 render to compare against must be refused");
    let msg = err.to_string();
    assert!(
        matches!(err, EngineError::Operational(_)),
        "must be Operational: {msg}"
    );
    assert!(
        msg.contains("restore() called before preflight(): no phase-5 render to compare against"),
        "{msg}"
    );
}
