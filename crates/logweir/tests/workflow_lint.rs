//! Lints on `.github/workflows/release.yml`. THIS PROVES THE WORKFLOW SAYS THE
//! RIGHT THING, NOT THAT IT DOES — the workflow has never executed on any
//! commit (release.yml:3-8). Task 10 extends this file with the `needs:` and
//! `:latest` lints; keep the helpers reusable.
//!
//! The property the file exists for is one sentence: the bytes
//! `scripts/check-image.sh` interrogated are the bytes that reach `ghcr.io`.
//! Before T0-17 the job built the image twice — a local tag every assertion ran
//! against, and a second, independent `docker/build-push-action` with
//! `push: true` whose bytes nothing had ever looked at — so the published image
//! had passed no check while the job's own comment claimed "only a verified
//! image is pushed". These tests are what stops that returning.
//!
//! NO DOCKER, NO NETWORK, NO `#[ignore]`: every test here reads one file from
//! the working tree and parses it, so they run in the default
//! `cargo test --workspace`. `serde_yaml` is a regular dependency of this crate
//! (`crates/logweir/Cargo.toml`), which is linked into integration-test targets;
//! no dev-dependency is added for this file.

use serde_yaml::Value;

/// The repo-root idiom, copied verbatim from
/// `crates/logweir/tests/extract_engine.rs:7-12`.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn release_yml() -> std::path::PathBuf {
    repo_root().join(".github/workflows/release.yml")
}

/// The workflow as TEXT. Some properties are text properties and must not be
/// asked of the parsed tree: a `docker build` inside a `run:` block is a string
/// the YAML model cannot distinguish from prose.
fn raw() -> String {
    let path = release_yml();
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// The workflow as a parsed document.
fn workflow() -> Value {
    let path = release_yml();
    serde_yaml::from_str(&raw())
        .unwrap_or_else(|e| panic!("{} is not valid YAML: {e}", path.display()))
}

/// The `image:` job's steps, in file order. Panics rather than returning an
/// empty slice when the job or its steps are missing: an absent job is a
/// broken workflow, not a workflow with nothing to lint.
fn image_steps(doc: &Value) -> &[Value] {
    doc["jobs"]["image"]["steps"]
        .as_sequence()
        .map(|s| s.as_slice())
        .expect("release.yml must have a jobs.image job with a steps: list")
}

/// `uses:` of a step, or the empty string.
fn uses(step: &Value) -> &str {
    step["uses"].as_str().unwrap_or("")
}

/// `run:` of a step, or the empty string.
fn run(step: &Value) -> &str {
    step["run"].as_str().unwrap_or("")
}

/// The index of the single step whose `uses:` names `build-push-action`.
fn build_step_index(steps: &[Value]) -> usize {
    let mut found = Vec::new();
    for (i, s) in steps.iter().enumerate() {
        if uses(s).contains("build-push-action") {
            found.push(i);
        }
    }
    assert_eq!(
        1,
        found.len(),
        "expected exactly one build-push-action step in jobs.image; found {:?}",
        found
    );
    found[0]
}

/// The index of the step that calls the shared gate script.
fn check_step_index(steps: &[Value]) -> usize {
    steps
        .iter()
        .position(|s| run(s).contains("scripts/check-image.sh"))
        .expect("no step in jobs.image runs scripts/check-image.sh")
}

/// The index of the step that pushes. Identified by what it DOES (`docker
/// push`), not by its `id:`, so renaming the id cannot make the lint blind.
fn push_step_index(steps: &[Value]) -> usize {
    steps
        .iter()
        .position(|s| run(s).contains("docker push"))
        .expect("no step in jobs.image pushes the image")
}

/// The first whitespace-delimited token after `needle` on the line that holds
/// it — the image reference an argument-taking command was handed.
fn arg_after(text: &str, needle: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(rest) = line.split_once(needle) {
            return rest.1.split_whitespace().next().map(str::to_string);
        }
    }
    None
}

/// Every reference a `docker tag` line in `text` tags FROM.
fn docker_tag_sources(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let toks: Vec<&str> = line.split_whitespace().collect();
        if let Some(i) = toks.iter().position(|t| *t == "tag") {
            if i > 0 && toks[i - 1].ends_with("docker") {
                if let Some(src) = toks.get(i + 1) {
                    out.push(src.trim_matches('"').to_string());
                }
            }
        }
    }
    out
}

// --------------------------------------------------------------------- 1

/// T0-17. One build, or the artifact that was checked is not the artifact that
/// ships. The second assertion is the other half of the same property: the
/// checks must be the shared implementation, because a workflow that reimplements
/// them inline drifts away from the one `just smoke` runs on a laptop.
#[test]
fn workflow_lint_release_builds_the_image_once() {
    let doc = workflow();
    let steps = image_steps(&doc);

    let n = steps
        .iter()
        .filter(|s| uses(s).contains("build-push-action"))
        .count();
    assert_eq!(
        1, n,
        "release.yml must build the release image exactly once; found {n} build-push-action steps"
    );

    let has_check = steps
        .iter()
        .any(|s| run(s).contains("scripts/check-image.sh"));
    assert!(
        has_check,
        "the image assertions must be the shared scripts/check-image.sh, not inline run: steps"
    );
}

// --------------------------------------------------------------------- 2

/// Raw text, not the parsed tree: a `docker build` inside a `run:` block is a
/// string the YAML model will not distinguish from prose.
#[test]
fn workflow_lint_no_docker_build_shellout() {
    let raw = raw();
    assert!(
        !raw.contains("docker build"),
        "a shell `docker build` reintroduces a second, unasserted build"
    );
}

// --------------------------------------------------------------------- 3

/// `push: false` must be PRESENT and literally `false`, not merely absent:
/// absent is a default a future action version is free to change, and this is
/// the one place where "the default is probably fine" published an unchecked
/// artifact. `load: true` is what puts the image in the daemon for the gate to
/// find; without it the build succeeds and `check-image.sh` refuses a reference
/// the daemon does not hold.
#[test]
fn workflow_lint_the_build_step_does_not_push() {
    let doc = workflow();
    let steps = image_steps(&doc);
    let step = &steps[build_step_index(steps)];

    assert_eq!(
        Some(false),
        step["with"]["push"].as_bool(),
        "the asserted build must not push; the push happens after check-image.sh"
    );
    assert_eq!(
        Some(true),
        step["with"]["load"].as_bool(),
        "load: true is what leaves the built image in the local daemon for scripts/check-image.sh to assert"
    );
    assert_eq!(
        Some("logweir:check"),
        step["with"]["tags"].as_str().map(str::trim),
        "the build step must produce exactly the local tag the assert step is handed"
    );
}

// --------------------------------------------------------------------- 4

/// THE TASK'S PROPERTY. The tag that is built, the tag that is asserted and the
/// tag that is pushed from are one string, and they happen in that order.
#[test]
fn workflow_lint_the_asserted_image_is_the_pushed_image() {
    let doc = workflow();
    let steps = image_steps(&doc);

    let b = build_step_index(steps);
    let c = check_step_index(steps);
    let p = push_step_index(steps);

    let built_tag = steps[b]["with"]["tags"]
        .as_str()
        .expect("the build step must name a tag")
        .trim()
        .to_string();
    let checked_tag = arg_after(run(&steps[c]), "scripts/check-image.sh")
        .expect("the assert step must hand check-image.sh an image reference");
    let sources = docker_tag_sources(run(&steps[p]));
    assert!(
        !sources.is_empty(),
        "the push step must `docker tag` the asserted image before pushing it"
    );

    assert!(
        b < c && c < p,
        "order must be build({b}) → assert({c}) → push({p})"
    );
    assert_eq!(built_tag, checked_tag);
    for pushed_from_tag in &sources {
        assert_eq!(built_tag, *pushed_from_tag);
    }
}

// --------------------------------------------------------------------- 5

/// A DIGEST IS THE ONLY IDENTIFIER THAT SURVIVES A MUTABLE TAG (GC7, applied to
/// Logweir's own image rather than only to the engine). It is captured from the
/// PUSH step, because that is the only place a repository digest exists: the
/// `load: true, push: false` build has no registry exporter, so its
/// `outputs.digest` is a local image config digest, not the digest a
/// `docker pull` resolves.
#[test]
fn workflow_lint_pushed_digest_is_captured() {
    let doc = workflow();
    let steps = image_steps(&doc);

    let outputs = doc["jobs"]["image"]["outputs"]
        .as_mapping()
        .expect("jobs.image must declare an outputs: block carrying the pushed digest");
    let v = outputs
        .get(Value::from("digest"))
        .and_then(|v| v.as_str())
        .expect("jobs.image.outputs must have a `digest` key")
        .to_string();
    assert!(
        v.contains("steps.push.outputs.digest"),
        "jobs.image.outputs.digest must come from the PUSH step (the only step with a repository digest); it reads {v:?}"
    );

    let push = &steps[push_step_index(steps)];
    let push_run = run(push);
    assert!(
        push_run.contains(">> \"$GITHUB_OUTPUT\"") && push_run.contains("digest="),
        "the push step must write digest=… to $GITHUB_OUTPUT; its run block is:\n{push_run}"
    );

    let pub_text =
        serde_yaml::to_string(&doc["jobs"]["publish"]).expect("the publish job is serializable");
    assert!(
        pub_text.contains("needs.image.outputs.digest"),
        "a captured digest nobody reads is not a captured digest"
    );
}

// --------------------------------------------------------------------- 6

/// T0-17 handoff, addendum A2. `docs/stability.md` must not carry a limitation
/// saying the release workflow builds the image more than once: THIS commit
/// retires that limitation, and a doc that asserts both shapes is the defect
/// class the whole scorecard argues against. `check-dod.sh` greps that file
/// only for the ASF attribution sentence, so nothing else catches a doc that
/// contradicts itself. Keep this test when Task 10 extends the file: it is a
/// doc lint in a lint file, not a stray.
///
/// SCOPED TO THE CLAIM, NOT TO THE WORD — a deliberate deviation from the
/// addendum's literal whole-file substring ban, recorded in the commit body.
/// The addendum measured `twice` as absent from `docs/stability.md` at
/// `3e448da`; two later commits added it in prose that has nothing to do with
/// the release image ("renders `restore.yaml` twice", "was twice mistaken
/// for"), so a whole-file ban is a permanent false red — the very thing this
/// task's brief refuses elsewhere. A line must therefore carry both a
/// multiplicity token AND a release-build token to fail, and the two phrasings
/// that can only ever mean this claim are banned outright.
#[test]
fn stability_doc_no_longer_claims_the_release_workflow_builds_twice() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/stability.md");
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
    let lower = raw.to_lowercase();

    // Unambiguous: no innocent sentence about this project says these.
    for needle in ["two builds", "second build", "builds twice", "built twice"] {
        assert!(
            !lower.contains(needle),
            "docs/stability.md still contains {needle:?}: the release workflow now builds the \
             image ONCE (release.yml, id: build) and pushes only after scripts/check-image.sh \
             returns 0. That limitation is retired by this commit and its sentence must be \
             DELETED, not appended beside the new one."
        );
    }

    // Ambiguous on its own; a failure needs the release-build context too.
    for line in lower.lines() {
        let multiplicity = ["twice", "two ", "second "]
            .iter()
            .any(|n| line.contains(n));
        let release_build = [
            "release.yml",
            "release workflow",
            "release image",
            "build-push",
        ]
        .iter()
        .any(|n| line.contains(n));
        let builds = line.contains("build");
        assert!(
            !(multiplicity && release_build && builds),
            "docs/stability.md line {line:?} reads as a claim that the release workflow builds \
             the image more than once. It builds it ONCE (release.yml, id: build) and pushes \
             only after scripts/check-image.sh returns 0. Delete the sentence rather than \
             appending a newer, contradictory one."
        );
    }
}
