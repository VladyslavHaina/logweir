//! What outlived the laptop demo.
//!
//! `scripts/laptop-demo.sh`, its steps and its transcript were withdrawn with
//! the static install file (release-notes item 76). One check here is not
//! about that walk and stays: the page's own body emitter, which
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

/// **The create body is the PAGE's, byte for byte.**
///
/// KILLS: replacing `emit-restore-body.js`'s call to `renderPlanBytes` with a
/// hand-written YAML string. That mutant is invisible everywhere else on the
/// walk — the `Restore` is created, `logweir drill approve` hashes it, Task
/// 16's five approval checks pass — and surfaces only at step 12, when the
/// runner cannot parse the document. Task 27's
/// `render_plan_bytes_emits_a_document_the_runner_parses` is the guard that
/// exists to catch it three slots earlier; this is the arm that keeps the
/// demo inside that guard's reach.
#[test]
fn the_body_emitter_renders_the_plan_with_the_pages_own_renderer() {
    let src = read("ui/tests/emit-restore-body.js");

    assert!(
        src.contains("from \"../plan.js\""),
        "ui/tests/emit-restore-body.js must import from `../plan.js` — the wizard's own renderer \
         (interface register I19/I20) and not a copy of it"
    );
    for name in ["renderPlanBytes", "planHash", "mintNames"] {
        assert!(
            src.contains(name),
            "ui/tests/emit-restore-body.js must use `{name}` from ../plan.js"
        );
    }
    assert!(
        src.contains("renderPlanBytes(") && src.contains("planHash(") && src.contains("mintNames("),
        "ui/tests/emit-restore-body.js must CALL all three, not merely import them"
    );
    assert!(
        src.contains("restoreBody("),
        "ui/tests/emit-restore-body.js builds the create body with the wizard's own \
         `restoreBody` — a second body builder here is a second thing to keep in step with \
         `ArchiveRef`, and the field it would be easiest to forget is `spec.sourceArchive.\
         secretRef.name` (plan erratum E24g)"
    );

    // NO YAML LITERAL. Every key below is part of the restore document's
    // grammar and appears NOWHERE in this tree except `ui/plan.js`'s renderer
    // and the documents it produces; an emitter that wrote the document by
    // hand has to spell them.
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
            "ui/tests/emit-restore-body.js contains the plan document's own key `{key}`, so it \
             is writing YAML instead of asking `renderPlanBytes` for it. The bytes the approver \
             signs and the bytes the runner parses are the page's or they are two documents \
             with one hash"
        );
    }
}
