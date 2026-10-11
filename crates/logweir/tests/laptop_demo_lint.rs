//! What outlived the laptop demo.
//!
//! `scripts/laptop-demo.sh`, its steps and its transcript were withdrawn with
//! the static install file (release-notes item 73). Two checks here are not
//! about that walk and stay: the page's own body emitter, which
//! `scripts/helm-demo.sh` still runs, and the parent spec's serving command.

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

/// The serving command spec §2's Demo 1 and spec §8 must both spell out.
const FULL_PROXY_COMMAND: &str =
    "kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1";

/// The form critique C L2 is about: it serves the files under the shipped
/// default `--www-prefix='/static/'` and not under `/ui/`, and it names no
/// context (STANDING RULE 12).
const BARE_PROXY_COMMAND: &str = "kubectl proxy --www=./ui";

/// Every run of whitespace collapsed to one space, so a command the spec
/// line-wrapped mid-token (`kubectl\nproxy --www=./ui`) is still one string.
fn collapse(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
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

// ---------------------------------------------------------------------------
// THE PARENT SPEC
// ---------------------------------------------------------------------------

/// **Spec §2's Demo 1 names the whole serving command.**
///
/// As `fdc73a5` wrote it, Demo 1's `kubectl proxy --www=./ui` serves the files
/// under the shipped default `--www-prefix='/static/'` and NOT under `/ui/`,
/// binds every address rather than loopback, and names no context (critique C
/// L2; STANDING RULE 12).
///
/// This is the one acceptance line in the plan that reads a parent-repository
/// file. It resolves the path from `CARGO_MANIFEST_DIR` (Global Constraint
/// 36), so it does not care what the working directory is.
///
/// KILLS: leaving the bare `kubectl proxy --www=./ui` in place.
#[test]
fn the_spec_demo_command_is_the_full_string() {
    let path = root().join("../docs/mvp/03-spec.md");
    // A STANDALONE CHECKOUT HAS NO PARENT. The spec is the parent corpus's file,
    // present only when this repository sits inside it; on GitHub it does not
    // (the first CI run, 2026-09-12, failed here on "No such file or
    // directory"). Absent, the check does not apply and says so — it does NOT
    // pass silently: the line below is the record. The repository's own copies
    // of the serving command are held by `the_ui_recipe_serves_under_ui_prefix`
    // and the transcript tests, which run everywhere.
    if !path.exists() {
        eprintln!(
            "the_spec_demo_command_is_the_full_string: not applicable — {} is not present \
             (standalone checkout; the parent spec is checked only inside the parent corpus)",
            path.display()
        );
        return;
    }
    let spec = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must be readable: {e}", path.display()));

    let two_start = spec
        .find("\n## 2. Personas and the two demos")
        .expect("spec §2 exists");
    let two_end = spec[two_start + 1..]
        .find("\n## 3.")
        .map(|at| two_start + 1 + at)
        .expect("spec §3 follows spec §2");
    let two = collapse(&spec[two_start..two_end]);

    assert!(
        two.contains(FULL_PROXY_COMMAND),
        "spec §2's Demo 1 must name the whole serving command:\n    {FULL_PROXY_COMMAND}\n\n§2, \
         whitespace collapsed:\n{two}"
    );
    assert!(
        !two.contains(BARE_PROXY_COMMAND),
        "spec §2 still carries the bare `{BARE_PROXY_COMMAND}`. As written it serves the UI \
         under the default `--www-prefix='/static/'` and not under `/ui/`, so the demo it \
         describes cannot work; and STANDING RULE 12 admits no `kubectl` without its context.\n\n\
         §2, whitespace collapsed:\n{two}"
    );

    // AND EVERY OTHER PLACE THE SPEC SPELLS THE `./ui` FORM. §8's serving
    // paragraph carries the same command; a fix in §2 alone leaves the
    // document disagreeing with itself about the one command that hands a
    // browser a cluster credential.
    let flat = collapse(&spec);
    let mut at = 0usize;
    while let Some(hit) = flat[at..].find("--www=./ui") {
        let abs = at + hit;
        let prefix = &flat[..abs];
        assert!(
            prefix.ends_with("kubectl --context docker-desktop proxy "),
            "docs/mvp/03-spec.md spells `--www=./ui` after `{}` — every occurrence of the \
             serving command in the spec names the context and the prefix (STANDING RULE 12, \
             critique C L2)",
            &prefix[prefix.len().saturating_sub(60)..]
        );
        at = abs + 1;
    }
}
