//! What outlived the laptop demo.
//!
//! `scripts/laptop-demo.sh`, its steps and its transcript were withdrawn with
//! the static install file (release-notes item 76). One check here is not
//! about that walk and stays: the console's own body emitter, which
//! `scripts/helm-demo.sh` still runs. (The parent spec's `kubectl proxy`
//! serving-command row went with that page, release-notes item 75.)

use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root resolves")
}

fn read(rel: &str) -> String {
    let p: PathBuf = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| {
        panic!(
            "{} must exist for this test to mean anything: {e}",
            p.display()
        )
    })
}

// ---------------------------------------------------------------------------
// THE BODY EMITTER
// ---------------------------------------------------------------------------

/// The emitter `scripts/helm-demo.sh` runs (the console's, since PROD-18.1 R8).
const EMITTER: &str = "console/scripts/emit-restore-body.mjs";

/// **The plan bytes are the CONSOLE's, byte for byte.**
///
/// KILLS: replacing the emitter's call to `renderPlanBytes` with a
/// hand-written YAML string. That mutant is invisible everywhere else on the
/// walk (the `Restore` is created, `logweir drill approve` hashes it, the
/// approval checks pass) and surfaces only when the runner cannot parse the
/// document. `console_plan.rs` is the guard that parses the console's goldens
/// into `RestoreSpec`; this is the arm that keeps the demo inside its reach.
#[test]
fn the_body_emitter_renders_the_plan_with_the_consoles_own_renderer() {
    let src = read(EMITTER);

    assert!(
        src.contains("from \"../src/pages/restores/plan.ts\""),
        "{EMITTER} must import from the console's own renderer, \
         console/src/pages/restores/plan.ts, and not a copy of it"
    );
    assert!(
        src.contains("renderPlanBytes(") && src.contains("planHash(") && src.contains("mintNames("),
        "{EMITTER} must CALL renderPlanBytes, planHash and mintNames, not merely import them"
    );
    assert!(
        src.contains("secretRef"),
        "{EMITTER} names the archive credential: without `spec.sourceArchive.secretRef.name` the \
         controller injects no object-store credential into the runner Job (plan erratum E24g)"
    );
    let helm_demo = read("scripts/helm-demo.sh");
    assert!(
        helm_demo.contains(&format!("node --experimental-strip-types {EMITTER}")),
        "scripts/helm-demo.sh runs {EMITTER}"
    );

    // NO YAML LITERAL. Every key below is part of the restore document's
    // grammar and appears only in the console's renderer and the documents it
    // produces; an emitter that wrote the document by hand has to spell them.
    for key in [
        "bootstrap_servers",
        "point_in_time",
        "topic_naming",
        "topic_mapping_prefix",
        "marker_topic",
        "default_replication_factor",
        "window_start",
        "window_end",
        "records_per_partition",
        "rto_seconds",
        "rpo_seconds",
        "pass_rate",
        "path_style",
        "allow_http",
        "webhooks",
    ] {
        assert!(
            !src.contains(key),
            "{EMITTER} contains the plan document's own key `{key}`, so it is writing YAML \
             instead of asking `renderPlanBytes` for it. The bytes the approver signs and the \
             bytes the runner parses are the console's or they are two documents with one hash"
        );
    }
}
