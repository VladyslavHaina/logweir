//! Lints on `.github/workflows/release.yml`. THIS PROVES THE WORKFLOW SAYS THE
//! RIGHT THING, NOT THAT IT DOES — the workflow has never executed on any
//! commit (release.yml's header): no tag has been pushed to the remote; every
//! assertion here is about the shape of the file.
//!
//! The property the file exists for is one sentence: the bytes
//! `scripts/check-image.sh` interrogated are the bytes that reach `ghcr.io`.
//! Before T0-17 the job built the image twice — a local tag every assertion ran
//! against, and a second, independent `docker/build-push-action` with
//! `push: true` whose bytes nothing had ever looked at — so the published image
//! had passed no check while the job's own comment claimed "only a verified
//! image is pushed". These tests are what stops that returning.
//!
//! **EVERY LINT HERE IS WORKFLOW-SCOPED (Task 30, finding F1).** Until Task 30
//! every helper in this file started at `doc["jobs"]["image"]`, so a SECOND job
//! carrying `docker/build-push-action` with `push: true` against the same
//! registry tag survived all six assertions — while `docs/stability.md` read
//! the guarantee as workflow-wide. `jobs(doc)` now enumerates every job's steps
//! in file order and each test iterates over every job that builds or pushes,
//! and `workflow_lint_exactly_one_job_pushes` bounds the set of pushing jobs at
//! one. Task 30b adds `weirkeeper` to THAT SAME job, deliberately, so the
//! invariant survives the join rather than being weakened by it.
//!
//! **TASK 30b: THREE IMAGES, TWO GATES, AND A PLATFORM DIMENSION.** The
//! controller image is a manifest list of two variants, each compiled on a
//! runner of its own architecture (`Dockerfile.weirkeeper` refuses a cross build
//! by name — plan erratum E19(c) — and STANDING RULE 10 forbids emulating the
//! compile, measured at 33x). Three consequences for this file, each one a place
//! a Task 30 helper had a fixed string where it needed a set:
//!
//! * the gate is no longer one script. `IMAGE_GATES` holds both, and
//!   `asserted_reference` reads the reference each was handed — skipping
//!   `--no-exec`, which sits between the script and its argument.
//! * the local tag is no longer `logweir:check`. Three jobs build three tags,
//!   and the property was never the string: it is that the tag handed to a gate
//!   is the tag the build produced.
//! * "asserted before pushed" is no longer a statement about three step indices.
//!   `workflow_lint_every_image_is_asserted_before_push` (Task 30's F1 rewrite,
//!   renamed here from `..._the_asserted_image_is_the_pushed_image`) states it
//!   over sets, and
//!   `workflow_lint_every_platform_is_asserted_before_the_manifest_push` adds
//!   the dimension the singular form cannot see: a manifest list whose digest is
//!   captured looks asserted even when one variant inside it was never loaded.
//!
//! NO DOCKER, NO NETWORK, NO `#[ignore]`: every test here reads files from the
//! working tree and parses them, so they run in the default
//! `cargo test --workspace`. `serde_yaml` is a regular dependency of this crate
//! (`crates/logweir/Cargo.toml`), which is linked into integration-test targets;
//! no dev-dependency is added for this file.

use std::collections::BTreeSet;

use serde_yaml::Value;

/// The repo-root idiom, copied verbatim from
/// `crates/logweir/tests/extract_engine.rs:7-12`.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// **A workflow by file name.** Task 31 added a SECOND workflow to this file's
/// remit — `kind-demo.yml`, Demo 1 in CI — so the three readers below are
/// parameterised by file name and the three release-scoped names are kept as
/// the thin wrappers every existing test already calls. Extending rather than
/// forking is deliberate: two copies of `is_pushing_step` differing in one
/// subtlety is how the wrong one gets called.
fn workflow_path(file: &str) -> std::path::PathBuf {
    repo_root().join(".github/workflows").join(file)
}

/// A workflow as TEXT. Some properties are text properties and must not be
/// asked of the parsed tree: a `docker build` inside a `run:` block is a string
/// the YAML model cannot distinguish from prose.
fn raw_of(file: &str) -> String {
    let path = workflow_path(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// A workflow as a parsed document.
fn parsed(file: &str) -> Value {
    let path = workflow_path(file);
    serde_yaml::from_str(&raw_of(file))
        .unwrap_or_else(|e| panic!("{} is not valid YAML: {e}", path.display()))
}

/// `kind-demo.yml`: Demo 1 in CI, on a `kind` cluster the workflow creates
/// (spec §16 clause 2). Task 31.
const KIND_DEMO_YML: &str = "kind-demo.yml";

// `release_yml()` — the path of the one workflow this file used to know about
// — is gone: `workflow_path(file)` subsumes it and `cargo clippy -- -D
// warnings` refuses the unused wrapper. Its two callers now read the file name
// directly, which is also the only place in this file that says which workflow
// the release-scoped tests are about.

fn raw() -> String {
    raw_of("release.yml")
}

fn workflow() -> Value {
    parsed("release.yml")
}

/// EVERY job's steps, by job name, in file order (F1). `serde_yaml`'s `Mapping`
/// preserves insertion order, so "file order" is the file's order and a failure
/// message can name jobs in the order a reader sees them.
///
/// A job with no `steps:` (a reusable-workflow `uses:` job, say) contributes an
/// empty slice rather than a panic: it is a job with nothing to lint, which is
/// a different thing from a missing `image:` job.
fn jobs(doc: &Value) -> Vec<(String, &[Value])> {
    let map = doc["jobs"]
        .as_mapping()
        .expect("release.yml must have a jobs: mapping");
    let mut out = Vec::new();
    for (name, body) in map {
        let name = name
            .as_str()
            .expect("every job key in release.yml must be a string")
            .to_string();
        let steps: &[Value] = body["steps"]
            .as_sequence()
            .map(|s| s.as_slice())
            .unwrap_or(&[]);
        out.push((name, steps));
    }
    out
}

/// ONE named job's steps, in file order. Panics rather than returning an empty
/// slice when the job or its steps are missing: a job named by another
/// assertion and then absent is a broken workflow, not a workflow with nothing
/// to lint.
fn job_steps<'a>(doc: &'a Value, name: &str) -> &'a [Value] {
    doc["jobs"][name]["steps"]
        .as_sequence()
        .map(|s| s.as_slice())
        .unwrap_or_else(|| panic!("release.yml must have a jobs.{name} job with a steps: list"))
}

/// `uses:` of a step, or the empty string.
fn uses(step: &Value) -> &str {
    step["uses"].as_str().unwrap_or("")
}

/// `run:` of a step, or the empty string.
fn run(step: &Value) -> &str {
    step["run"].as_str().unwrap_or("")
}

/// `if:` of a step, or the empty string.
fn condition(step: &Value) -> &str {
    step["if"].as_str().unwrap_or("")
}

/// Does this step PUSH? Identified by what it does, never by its `id:` or its
/// `name:`, so renaming either cannot make the lint blind: a
/// `docker/build-push-action` with `push: true`, or a `run:` block that calls
/// `docker push`. YAML's `true` and `'true'` both count — a quoted `with:`
/// value is still a push.
fn is_pushing_step(step: &Value) -> bool {
    let action_pushes = uses(step).contains("build-push-action")
        && (step["with"]["push"].as_bool() == Some(true)
            || step["with"]["push"].as_str().map(str::trim) == Some("true"));
    let block = run(step);
    // `docker manifest push` is Task 30b's spelling for uploading a manifest
    // list, and it does NOT contain the substring `docker push`. Without this
    // arm a step that published the multi-architecture controller image and
    // nothing else would not be a pushing step to any lint in this file:
    // `workflow_lint_exactly_one_job_pushes` would not count it, and
    // `workflow_lint_login_and_push_are_tag_gated` would not require its tag
    // gate. `imagetools create` is the buildx spelling of the same act and is
    // named for the same reason, even though this workflow cannot use it.
    action_pushes
        || block.contains("docker push")
        || block.contains("docker manifest push")
        || block.contains("imagetools create")
}

/// Is this the registry login step?
fn is_login_step(step: &Value) -> bool {
    uses(step).contains("docker/login-action")
}

/// The index of the single step whose `uses:` names `build-push-action`, or
/// `None` when this job has no build step at all. The `Option` sibling exists
/// so a job with no build/check/push step is a skipped job rather than a panic
/// once every test iterates over every job (F1).
fn try_build_step_index(steps: &[Value]) -> Option<usize> {
    steps
        .iter()
        .position(|s| uses(s).contains("build-push-action"))
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
        "expected exactly one build-push-action step in this job; found {:?}",
        found
    );
    found[0]
}

/// THE TWO IMAGE GATES. `scripts/check-image.sh` asserts the runner image;
/// `scripts/check-image-weirkeeper.sh` asserts the controller image — a
/// SEPARATE script (interface I25), because all six of the former's checks are
/// written around the runner's two binaries, its MIT notice and its x86-64 ELF,
/// and five of them fail against the controller image for the right reason.
///
/// Neither name is a substring of the other (`check-image.sh` does not occur
/// inside `check-image-weirkeeper.sh`), so the order of the two arms below
/// cannot matter — which is worth stating, because before Task 30b every helper
/// here looked for one fixed string and a job asserting the OTHER image read as
/// a job asserting nothing.
const IMAGE_GATES: [&str; 2] = [
    "scripts/check-image.sh",
    "scripts/check-image-weirkeeper.sh",
];

/// The first NON-FLAG token after `needle` on the line that holds it. Task 30b's
/// `--no-exec` sits between the script and the reference, and a helper that
/// returned the first token would compare a built tag against `--no-exec` and
/// call that a mismatch.
fn arg_after_skipping_flags(text: &str, needle: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(rest) = line.split_once(needle) {
            return rest
                .1
                .split_whitespace()
                .find(|t| !t.starts_with('-'))
                .map(str::to_string);
        }
    }
    None
}

/// The image reference this step ASSERTS, if it runs either image gate.
fn asserted_reference(step: &Value) -> Option<String> {
    let block = run(step);
    IMAGE_GATES
        .iter()
        .find(|g| block.contains(**g))
        .and_then(|g| arg_after_skipping_flags(block, g))
}

/// `(step index, asserted reference)` for every gate step in a job, in order.
fn asserted_references(steps: &[Value]) -> Vec<(usize, String)> {
    steps
        .iter()
        .enumerate()
        .filter_map(|(i, s)| asserted_reference(s).map(|r| (i, r)))
        .collect()
}

/// The index of the first step that runs either image gate, or `None`.
fn try_check_step_index(steps: &[Value]) -> Option<usize> {
    steps.iter().position(|s| asserted_reference(s).is_some())
}

/// The index of the FIRST step that pushes, or `None`.
fn try_push_step_index(steps: &[Value]) -> Option<usize> {
    steps.iter().position(is_pushing_step)
}

/// Does this step load an image into the job's own daemon? Either half of the
/// two ways bytes get there: a `load: true` build, or `docker load` of a
/// tarball another job produced.
fn is_loading_step(step: &Value) -> bool {
    let action_loads =
        uses(step).contains("build-push-action") && step["with"]["load"].as_bool() == Some(true);
    action_loads || run(step).contains("docker load")
}

/// Does this step build the CONTROLLER image? Identified by the Dockerfile it
/// names, never by the job's name or the step's `id:`, so a rename cannot make
/// the lint blind. The runner image's build step carries no `file:` (it uses the
/// default `Dockerfile`), so it is not one of these.
fn builds_the_controller_image(step: &Value) -> bool {
    uses(step).contains("build-push-action")
        && step["with"]["file"].as_str().map(str::trim) == Some("Dockerfile.weirkeeper")
}

/// The architecture tokens `text` names, out of the two this project publishes.
/// `linux/arm64`, `weirkeeper:check-arm64`, `weirkeeper-arm64.tar` and
/// `ghcr.io/logweir/weirkeeper:-arm64` (a tag whose `${{ … }}` half has been
/// stripped) all yield `arm64`.
///
/// THE SET IS CLOSED AT TWO ON PURPOSE. `linux/s390x` is not a platform this
/// project builds, and a lint that silently discovered new ones would report a
/// typo (`linux/arm46`) as a platform with no assertion rather than as a typo.
fn arch_tokens(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for w in words(text) {
        for arch in ["amd64", "arm64"] {
            if w == arch || w.ends_with(&format!("-{arch}")) {
                out.insert(arch.to_string());
            }
        }
    }
    out
}

/// Every platform a PUSHING step publishes, as architecture tokens. Two sources,
/// because there are two ways to push more than one:
///   * a `build-push-action` with `push: true` names them in `with.platforms`
///     (the shape T30b's brief assumed, and the shape E19(c) makes unrunnable
///     here — but a mutant can still write it, and this is what catches it);
///   * a `run:` block names them in the references it tags and pushes.
///
/// A step that names no architecture token is single-platform: the runner image
/// is pushed by exactly such a step, and
/// `workflow_lint_every_image_is_asserted_before_push` is what covers it.
fn pushed_platforms(step: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if let Some(p) = step["with"]["platforms"].as_str() {
        out.extend(arch_tokens(p));
    }
    let code = strip_shell_comments(&strip_gha_expressions(run(step)));
    out.extend(arch_tokens(&code));
    out
}

/// `needs:` of a job, as a list. A bare string (`needs: image`) and a sequence
/// (`needs: [image]`) are both legal YAML for the same thing.
fn needs_of(doc: &Value, job: &str) -> Vec<String> {
    let v = &doc["jobs"][job]["needs"];
    if let Some(s) = v.as_str() {
        return vec![s.to_string()];
    }
    v.as_sequence()
        .map(|seq| {
            seq.iter()
                .filter_map(|n| n.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// `arg_after` — the first WHITESPACE-DELIMITED token after a needle — lived
// here until Task 30b and is now `arg_after_skipping_flags` above, its only
// caller having grown a flag between the script and the reference
// (`check-image-weirkeeper.sh --no-exec <ref>`). It is deleted rather than kept
// beside its successor: two helpers differing in one subtlety is how the wrong
// one gets called, and `cargo clippy -- -D warnings` refuses the unused one
// anyway.

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

// ------------------------------------------------------------- shell helpers

/// Every `${{ … }}` expression removed. Run FIRST, before the comment strip:
/// `${{ steps.build.outputs.digest }}` is a legal reference to a build step's
/// OUTPUT, not a build command, and Task 30b reads one by design.
fn strip_gha_expressions(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("${{") {
        out.push_str(&rest[..start]);
        match rest[start..].find("}}") {
            Some(end) => rest = &rest[start + end + 2..],
            // Unterminated: nothing after it can be read as code with any
            // confidence, so drop the remainder rather than guess.
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// `#` comments removed, by the shell's own rule: a `#` opens a comment only at
/// the start of a word. `"${entry##*@}"` is therefore not a comment, and
/// `docker push "$ver"  # ship it` is.
fn strip_shell_comments(text: &str) -> String {
    let mut out = Vec::new();
    for line in text.lines() {
        let bytes = line.as_bytes();
        let mut cut = line.len();
        for (i, c) in line.char_indices() {
            if c == '#' && (i == 0 || bytes[i - 1].is_ascii_whitespace()) {
                cut = i;
                break;
            }
        }
        out.push(&line[..cut]);
    }
    out.join("\n")
}

/// Every WORD in `text`: a maximal run of ASCII alphanumerics, `_` and `-`.
/// `.` and `/` are word boundaries, so `steps.build.outputs.digest` yields the
/// word `build` while `docker/build-push-action` yields `build-push-action`.
fn words(text: &str) -> Vec<&str> {
    text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .filter(|w| !w.is_empty())
        .collect()
}

/// `VAR=value` assignments in a `run:` block, in file order, with the value
/// unquoted. Only the plain `name=` form — nothing here needs `export`, arrays
/// or arithmetic, and a helper that pretends to parse shell is worse than one
/// that admits its shape.
fn assignments(block: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in block.lines() {
        let line = line.trim();
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        out.push((name.to_string(), value.trim().trim_matches('"').to_string()));
    }
    out
}

/// `(line index, argument)` for every `docker push <arg>` in a `run:` block.
fn docker_push_arguments(block: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for (i, line) in block.lines().enumerate() {
        let toks: Vec<&str> = line.split_whitespace().collect();
        if let Some(j) = toks.iter().position(|t| *t == "push") {
            if j > 0 && toks[j - 1].ends_with("docker") {
                if let Some(arg) = toks.get(j + 1) {
                    out.push((i, arg.trim_matches('"').to_string()));
                }
            }
        }
    }
    out
}

/// `$ver` / `${ver}` resolved against the block's own assignments; anything
/// else is returned unchanged. One level, which is all this block has.
fn resolve(arg: &str, assigned: &[(String, String)]) -> String {
    let name = arg
        .strip_prefix("${")
        .and_then(|r| r.strip_suffix('}'))
        .or_else(|| arg.strip_prefix('$'));
    match name {
        Some(n) => assigned
            .iter()
            .rev()
            .find(|(k, _)| k == n)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| arg.to_string()),
        None => arg.to_string(),
    }
}

/// Every `ghcr.io/…` reference in `text`, REDUCED TO ITS REPOSITORY PART —
/// everything before the first `:` or `@` after the host. A `${{ … }}`
/// expression inside the reference is kept verbatim (and terminates nothing),
/// so a reference computed from `${{ github.repository }}` is reported as the
/// string it is rather than silently collapsing to `ghcr.io/`.
fn ghcr_repositories(text: &str) -> Vec<String> {
    const HOST: &str = "ghcr.io";
    let mut out = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = text[from..].find(HOST) {
        let start = from + rel;
        let mut reference = String::new();
        let mut rest = &text[start..];
        loop {
            if rest.starts_with("${{") {
                match rest.find("}}") {
                    Some(end) => {
                        reference.push_str(&rest[..end + 2]);
                        rest = &rest[end + 2..];
                        continue;
                    }
                    None => break,
                }
            }
            match rest.chars().next() {
                Some(c)
                    if c.is_ascii_alphanumeric()
                        || c == '.'
                        || c == '_'
                        || c == '-'
                        || c == '/' =>
                {
                    reference.push(c);
                    rest = &rest[c.len_utf8()..];
                }
                _ => break,
            }
        }
        out.push(reference);
        from = start + HOST.len();
    }
    out
}

// --------------------------------------------------------------------- 1

/// T0-17. One build, or the artifact that was checked is not the artifact that
/// ships. The second assertion is the other half of the same property: the
/// checks must be the shared implementation, because a workflow that reimplements
/// them inline drifts away from the one `just smoke` runs on a laptop.
///
/// F1: over EVERY job that builds, not only `jobs.image`.
#[test]
fn workflow_lint_release_builds_the_image_once() {
    let doc = workflow();
    let mut seen = Vec::new();

    for (job, steps) in jobs(&doc) {
        if try_build_step_index(steps).is_none() {
            continue;
        }
        seen.push(job.clone());

        let n = steps
            .iter()
            .filter(|s| uses(s).contains("build-push-action"))
            .count();
        assert_eq!(
            1, n,
            "job `{job}` must build the release image exactly once; found {n} build-push-action steps"
        );

        let has_check = steps.iter().any(|s| asserted_reference(s).is_some());
        assert!(
            has_check,
            "job `{job}` builds an image but runs no assertion: the image assertions must be one \
             of the shared gates ({IMAGE_GATES:?}), not inline run: steps. Task 30b's two native \
             controller-image jobs each build once and assert with \
             scripts/check-image-weirkeeper.sh."
        );
    }

    assert!(
        !seen.is_empty(),
        "release.yml builds no image at all: no job carries a build-push-action step"
    );
}

// --------------------------------------------------------------------- 2

/// Raw text, not the parsed tree: a `docker build` inside a `run:` block is a
/// string the YAML model will not distinguish from prose.
///
/// RETAINED BY TASK 30 AND NOT WIDENED: it is a whole-FILE substring ban, which
/// `workflow_lint_the_push_step_names_no_build_command` is not (that one is
/// scoped to push steps and catches the spellings this one cannot —
/// `docker image build`, `docker buildx build`).
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
///
/// F1: over EVERY job that builds.
#[test]
fn workflow_lint_the_build_step_does_not_push() {
    let doc = workflow();
    let mut seen = 0usize;

    for (job, steps) in jobs(&doc) {
        if try_build_step_index(steps).is_none() {
            continue;
        }
        seen += 1;
        let step = &steps[build_step_index(steps)];

        assert_eq!(
            Some(false),
            step["with"]["push"].as_bool(),
            "job `{job}`: the asserted build must not push; the push happens after check-image.sh"
        );
        assert_eq!(
            Some(true),
            step["with"]["load"].as_bool(),
            "job `{job}`: load: true is what leaves the built image in the local daemon for \
             scripts/check-image.sh to assert"
        );
        // THE BUILT TAG IS THE TAG THIS JOB ASSERTS — not a fixed string.
        // Until Task 30b that literal was `logweir:check`, because the runner
        // image was the only image any job built. There are now three local
        // tags across three jobs (`logweir:check`, `weirkeeper:check-amd64`,
        // `weirkeeper:check-arm64`), and hard-coding one of them would either
        // fail on a correct tree or, widened to "any string", assert nothing.
        // The property was never the string: it is that the tag handed to the
        // gate is the tag the build produced.
        let built = step["with"]["tags"]
            .as_str()
            .map(str::trim)
            .unwrap_or_else(|| panic!("job `{job}`: the build step must name a tag"));
        let asserted = asserted_references(steps);
        assert!(
            asserted.iter().any(|(_, r)| r == built),
            "job `{job}`: the build step produces `{built}`, which no gate step in this job is \
             handed. The assert step's argument and the build step's tag are one string, or the \
             bytes that were checked are not the bytes that ship. This job asserts: {:?}",
            asserted.iter().map(|(_, r)| r).collect::<Vec<_>>()
        );
    }

    assert!(seen > 0, "release.yml has no build step to lint");
}

// --------------------------------------------------------------------- 4

/// THE TASK'S PROPERTY. Every image that reaches a registry was asserted first,
/// in the job that pushes it, from the tag that holds those exact bytes.
///
/// RENAMED BY TASK 30b, from `workflow_lint_the_asserted_image_is_the_pushed_image`
/// to the name that task's brief and acceptance use. The rename is not cosmetic:
/// the old name says "the image", and there are now three of them — the runner
/// image and both controller variants — so the singular was the thing that had
/// gone stale. Nothing outside this file referenced the old name (checked at
/// landing: one occurrence in the tree, the definition).
///
/// RESTRUCTURED, and the restructure is forced by the shape E19(c) requires. The
/// Task 30 form read exactly one build index, one assert index and one push
/// index per job and compared them pairwise. Task 30b's controller-image jobs
/// BUILD AND ASSERT AND DO NOT PUSH, so `push_step_index`'s `expect` would have
/// panicked on a correct tree; and the pushing job now asserts THREE references
/// before pushing, so one `check_step_index` would have read only the first. The
/// property is therefore stated over sets instead of over three integers, which
/// is strictly stronger — it holds for every build and every push in the job
/// rather than for the first of each:
///
///   * every `load: true` build's tag is asserted LATER in the same job;
///   * every reference a push step `docker tag`s FROM was asserted EARLIER in
///     the same job.
///
/// Both halves together are "build → assert → push", and neither alone is.
#[test]
fn workflow_lint_every_image_is_asserted_before_push() {
    let doc = workflow();
    let mut builds_seen = 0usize;
    let mut pushes_seen = 0usize;

    for (job, steps) in jobs(&doc) {
        let asserted = asserted_references(steps);

        for (b, step) in steps.iter().enumerate() {
            if !uses(step).contains("build-push-action") {
                continue;
            }
            if step["with"]["load"].as_bool() != Some(true) {
                continue;
            }
            builds_seen += 1;
            let built = step["with"]["tags"]
                .as_str()
                .unwrap_or_else(|| panic!("job `{job}`: the build step must name a tag"))
                .trim()
                .to_string();
            let at = asserted.iter().find(|(_, r)| *r == built);
            let Some((c, _)) = at else {
                panic!(
                    "job `{job}`: step {b} builds `{built}` into the daemon and no step in this \
                     job hands that reference to an image gate. This job asserts: {:?}",
                    asserted.iter().map(|(_, r)| r).collect::<Vec<_>>()
                );
            };
            assert!(
                b < *c,
                "job `{job}`: `{built}` is asserted at step {c}, BEFORE it is built at step {b}. \
                 The gate would be inspecting whatever the daemon already held under that tag."
            );
        }

        for (p, step) in steps.iter().enumerate() {
            if !is_pushing_step(step) {
                continue;
            }
            pushes_seen += 1;
            let sources = docker_tag_sources(run(step));
            assert!(
                !sources.is_empty(),
                "job `{job}`: step {p} pushes but never `docker tag`s a local reference. \
                 `docker tag` renaming asserted bytes is what makes \"the bytes that were \
                 checked are the bytes that are pushed\" readable at all; a push of something \
                 this job never named is the defect this file exists for."
            );
            for src in &sources {
                let at = asserted.iter().find(|(_, r)| r == src);
                let Some((c, _)) = at else {
                    panic!(
                        "job `{job}`: step {p} pushes from `{src}`, which no gate step in this \
                         job asserted. This job asserts: {:?}",
                        asserted.iter().map(|(_, r)| r).collect::<Vec<_>>()
                    );
                };
                assert!(
                    *c < p,
                    "job `{job}`: `{src}` is pushed at step {p} and asserted at step {c}; the \
                     order must be assert THEN push."
                );
            }
        }
    }

    assert!(
        builds_seen > 0 && pushes_seen > 0,
        "release.yml has {builds_seen} loaded build step(s) and {pushes_seen} push step(s); \
         this lint would be vacuous"
    );
}

// --------------------------------------------------------------------- 5

/// A DIGEST IS THE ONLY IDENTIFIER THAT SURVIVES A MUTABLE TAG (GC7, applied to
/// Logweir's own image rather than only to the engine). It is captured from the
/// PUSH step, because that is the only place a repository digest exists: the
/// `load: true, push: false` build has no registry exporter, so its
/// `outputs.digest` is a local image config digest, not the digest a
/// `docker pull` resolves.
///
/// F1: over EVERY job that pushes, and the `steps.<id>.outputs.digest` the job
/// publishes must name THAT job's push step rather than a fixed `push` id.
#[test]
fn workflow_lint_pushed_digest_is_captured() {
    let doc = workflow();
    let mut seen = 0usize;

    for (job, steps) in jobs(&doc) {
        // EVERY pushing step, not only the first. Task 30b puts two in one job
        // — the runner image and the controller manifest list — and a helper
        // that returned `position(...)` would have left the second one's digest
        // unexamined while the test still reported a pass.
        for push in steps.iter().filter(|s| is_pushing_step(s)) {
            seen += 1;

            let id = push["id"].as_str().unwrap_or_else(|| {
                panic!(
                    "job `{job}`: every push step must carry an `id:` so the job can publish its \
                     digest"
                )
            });

            let outputs = doc["jobs"][job.as_str()]["outputs"]
                .as_mapping()
                .unwrap_or_else(|| {
                    panic!(
                        "job `{job}` pushes but declares no outputs: block carrying the pushed \
                         digest"
                    )
                });
            // The KEY is not fixed — `digest` for the runner image,
            // `weirkeeper_digest` for the controller — but the VALUE has to
            // come from this step, which is the property.
            let needle = format!("steps.{id}.outputs.");
            let key = outputs
                .iter()
                .find(|(_, v)| v.as_str().map(|s| s.contains(&needle)) == Some(true))
                .and_then(|(k, _)| k.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "jobs.{job}.outputs must publish a value read from the PUSH step `{id}` \
                         (the only step where a repository digest exists: a `load: true, \
                         push: false` build has no registry exporter). Its outputs are: {:?}",
                        outputs
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.as_str()))
                            .collect::<Vec<_>>()
                    )
                });

            let push_run = run(push);
            assert!(
                push_run.contains(">> \"$GITHUB_OUTPUT\"") && push_run.contains("digest="),
                "job `{job}`, push step `{id}`: must write digest=… to $GITHUB_OUTPUT; its run \
                 block is:\n{push_run}"
            );

            let read_as = format!("needs.{job}.outputs.{key}");
            assert!(
                raw().contains(&read_as),
                "a captured digest nobody reads is not a captured digest: nothing in release.yml \
                 reads {read_as}"
            );
        }
    }

    assert!(
        seen > 0,
        "release.yml pushes nothing, so no digest is captured"
    );
}

// --------------------------------------------------------------------- 6

/// F1's own test. THE SET OF JOBS THAT PUSH HAS CARDINALITY ONE.
///
/// Every other lint in this file is now workflow-scoped, but scope alone is not
/// the property: two jobs could each build once, assert once and push once and
/// satisfy every assertion above while racing each other to the same registry
/// tag — and only one of them would be the bytes `check-image.sh` looked at for
/// any given pull. One job pushes. Task 30b adds `weirkeeper` to THAT job.
#[test]
fn workflow_lint_exactly_one_job_pushes() {
    let doc = workflow();

    let pushing: Vec<String> = jobs(&doc)
        .into_iter()
        .filter(|(_, steps)| steps.iter().any(is_pushing_step))
        .map(|(name, _)| name)
        .collect();

    assert_eq!(
        1,
        pushing.len(),
        "exactly one job in release.yml may push to the registry (a build-push-action with \
         push: true, or a run: block calling docker push); found {}: {:?}. Task 30b adds the \
         weirkeeper image to the SAME job rather than a second one, so this stays at 1.",
        pushing.len(),
        pushing
    );

    // Re-read that job BY NAME: the one pushing job is also the job that runs
    // the shared assertion, which is what makes "the asserted image is the
    // pushed image" a statement about the whole workflow.
    let steps = job_steps(&doc, &pushing[0]);
    assert!(
        try_check_step_index(steps).is_some(),
        "the pushing job `{}` runs neither of the image gates ({IMAGE_GATES:?})",
        pushing[0]
    );
}

// --------------------------------------------------------------------- 7

/// F2. A RAW BUILD INSIDE THE PUSH STEP IS A SECOND, UNASSERTED BUILD.
///
/// `workflow_lint_no_docker_build_shellout` bans the substring `docker build`
/// over the whole file; `docker image build …` and `docker buildx build …` do
/// not contain it, so `docker image build -t x . ; docker push "$ver"` inside
/// the push step reintroduces exactly what T0-17 removed. This test is the
/// stronger form, scoped to push steps.
///
/// STRIP `${{ … }}` FIRST, THEN `#` COMMENTS, then look for the WORD `build`.
/// The expression strip is not decoration: `${{ steps.build.outputs.digest }}`
/// is a legal reference to a build step's OUTPUT — Task 30b reads one by
/// design — and without the strip this test would fail on it. The word rule is
/// deliberately stricter than "the run: block's first word on any line": the
/// first word of `docker image build` is `docker`, so a first-word rule lets
/// through the exact mutant this test exists for.
#[test]
fn workflow_lint_the_push_step_names_no_build_command() {
    let doc = workflow();
    let mut seen = 0usize;

    for (job, steps) in jobs(&doc) {
        for step in steps.iter().filter(|s| is_pushing_step(s)) {
            let block = run(step);
            if block.is_empty() {
                continue;
            }
            seen += 1;
            let code = strip_shell_comments(&strip_gha_expressions(block));
            let offending: Vec<&str> = code
                .lines()
                .filter(|l| words(l).contains(&"build"))
                .collect();
            assert!(
                offending.is_empty(),
                "job `{job}`: the push step names a build command. `docker tag` renames the bytes \
                 the assert step looked at; anything that BUILDS here publishes bytes nothing \
                 asserted. Offending line(s) (after `${{{{ … }}}}` and `#` comment removal):\n{}",
                offending.join("\n")
            );
        }
    }

    assert!(
        seen > 0,
        "no push step with a run: block was examined; this lint would be vacuous"
    );
}

// --------------------------------------------------------------------- 8

/// F3. LOGIN AND PUSH ARE BOTH GATED ON A TAG REF, IN EVERY JOB.
///
/// `on:` carries `workflow_dispatch` (release.yml:28), so without the gate a
/// dispatch from a branch publishes the runner repository at `:<branch>` AND
/// MOVES `:latest` — from a ref no tag ever named. Asserted over every job
/// (F1's enumerator) so the condition cannot be satisfied in one job while
/// another is unguarded.
#[test]
fn workflow_lint_login_and_push_are_tag_gated() {
    const GATE: &str = "startsWith(github.ref,'refs/tags/')";
    let doc = workflow();
    let (mut logins, mut pushes) = (0usize, 0usize);

    for (job, steps) in jobs(&doc) {
        for (i, step) in steps.iter().enumerate() {
            let login = is_login_step(step);
            let push = is_pushing_step(step);
            if !login && !push {
                continue;
            }
            if login {
                logins += 1;
            }
            if push {
                pushes += 1;
            }
            let kind = if login { "login" } else { "push" };
            let cond: String = condition(step)
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(
                cond.contains(GATE),
                "job `{job}`, step {i} ({kind}, uses={:?}): every registry login and every push \
                 must carry `if: startsWith(github.ref, 'refs/tags/')`; its `if:` reads {:?}. \
                 `on:` includes workflow_dispatch, so an ungated push publishes a branch ref and \
                 moves :latest.",
                uses(step),
                condition(step)
            );
            assert!(
                !cond.contains("||"),
                "job `{job}`, step {i} ({kind}): the tag gate must not be one arm of an `||` — \
                 the other arm is what runs on a branch. Its `if:` reads {:?}",
                condition(step)
            );
        }
    }

    assert!(
        logins > 0 && pushes > 0,
        "release.yml has {logins} login step(s) and {pushes} push step(s); this lint would be vacuous"
    );
}

// --------------------------------------------------------------------- 9

/// F4. `$ver` IS PUSHED BEFORE `:latest`.
///
/// A failed `$ver` push must not leave `:latest` already moved onto bytes no
/// immutable reference names. The two `docker push` line indices inside one
/// `run:` block are the whole assertion; the references are resolved through
/// the block's own assignments so the test reads the shell rather than the
/// variable names.
#[test]
fn workflow_lint_version_tag_is_pushed_before_latest() {
    let doc = workflow();
    let mut seen = 0usize;

    for (job, steps) in jobs(&doc) {
        for step in steps.iter().filter(|s| is_pushing_step(s)) {
            let block = run(step);
            if block.is_empty() {
                continue;
            }
            let assigned = assignments(block);
            let pushes: Vec<(usize, String)> = docker_push_arguments(block)
                .into_iter()
                .map(|(i, arg)| (i, resolve(&arg, &assigned)))
                .collect();
            if pushes.is_empty() {
                continue;
            }
            let latest = pushes.iter().find(|(_, r)| r.ends_with(":latest"));
            let Some((latest_line, latest_ref)) = latest else {
                // A push step that never moves `:latest` cannot get the order
                // wrong. Nothing to assert, and nothing to hide.
                continue;
            };
            seen += 1;
            let version = pushes
                .iter()
                .find(|(_, r)| !r.ends_with(":latest"))
                .unwrap_or_else(|| {
                    panic!(
                        "job `{job}`: the push step moves `{latest_ref}` and pushes no versioned \
                         reference at all; `:latest` must never be the only thing published"
                    )
                });
            assert!(
                version.0 < *latest_line,
                "job `{job}`: the versioned reference `{}` (run: line {}) must be tagged and \
                 pushed BEFORE `{latest_ref}` (run: line {latest_line}). A failed version push \
                 must not leave :latest already moved.",
                version.1,
                version.0
            );
        }
    }

    assert!(
        seen > 0,
        "no push step moves `:latest`; this lint would be vacuous"
    );
}

// --------------------------------------------------------------------- 10

/// H5. WHAT IS PUSHED IS WHAT SHIPS — pushed ⊆ shipped, one way.
///
/// Global Constraint 24 fixes the namespace as the LITERAL
/// `ghcr.io/logweir/<name>` — `logweir` (the runner) and `weirkeeper` (the
/// controller). The literal runner reference is NOT spelled in this file:
/// interface I15 (`crates/weirkeeper/tests/crd_shape.rs`'s
/// `the_runner_image_is_named_once`) allows it exactly one occurrence under
/// `crates/`, in `job.rs`, so that a digest bump cannot update one call site
/// and miss another. This test reads it from `release.yml` and compares it
/// against that one occurrence, which is the stronger arrangement anyway.
/// `ghcr.io/${{ github.repository }}` is `<owner>/<repo>`: it equals the
/// required string only if this repository is literally `logweir/logweir`, and
/// one repository can never produce the TWO names this project publishes.
/// Nothing asserted, before this test, that the reference pushed is the
/// reference an operator is told to pull.
///
/// THE SHIPPED SET IS TWO FILES, AND THE SECOND IS A DEVIATION FROM THIS TASK'S
/// BRIEF, RECORDED RATHER THAN HIDDEN. The brief asks for `logweir.yaml` alone,
/// but the runner image is not a manifest field: `logweir.yaml` carries only
/// `ghcr.io/logweir/weirkeeper` (the controller Deployment), while the runner
/// image an operator actually pulls is the Rust constant
/// `crates/weirkeeper/src/job.rs`'s `RUNNER_IMAGE` — unreachable by kustomize,
/// as `config/overlays/local-images/kustomization.yaml` says in full. Asserting
/// against `logweir.yaml` alone would fail on a correct tree, and widening the
/// reduction to `ghcr.io/logweir` would pass a mutant that pushed
/// `ghcr.io/logweir/anything`. So the shipped set is both files: between them
/// they hold every image reference the shipped control plane uses.
///
/// The assertion runs ONE WAY, at repository-part granularity, so it passes
/// here (one reference), after Task 23 pins both by digest
/// (`…/logweir@sha256:…` reduces to the same repository), and at Task 30b (two
/// references) without the test being edited between them.
#[test]
fn workflow_lint_pushed_references_match_the_shipped_manifest() {
    let doc = workflow();
    let root = repo_root();

    let sources = [
        root.join("logweir.yaml"),
        root.join("crates/weirkeeper/src/job.rs"),
    ];
    let shipped: Vec<(String, String)> = sources
        .iter()
        .map(|p| {
            let body = std::fs::read_to_string(p)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", p.display()));
            (p.display().to_string(), body)
        })
        .collect();

    let mut checked = 0usize;
    for (job, steps) in jobs(&doc) {
        for step in steps.iter().filter(|s| is_pushing_step(s)) {
            let block = run(step);
            if block.is_empty() {
                continue;
            }
            // Comments only: the `${{ … }}` expressions are KEPT, so a
            // reference computed from `${{ github.repository }}` is reported as
            // the string it is.
            let code = strip_shell_comments(block);
            let refs = ghcr_repositories(&code);
            assert!(
                !refs.is_empty(),
                "job `{job}`: the push step names no ghcr.io reference at all"
            );
            for r in refs {
                checked += 1;
                let found = shipped.iter().any(|(_, body)| body.contains(&r));
                assert!(
                    found,
                    "job `{job}` pushes `{r}`, which the shipped control plane never references. \
                     Global Constraint 24 fixes the namespace as the literal \
                     ghcr.io/logweir/<name>, and the shipped \
                     references live in {} and {}. `${{{{ github.repository }}}}` is \
                     <owner>/<repo> and is never one of them.",
                    shipped[0].0, shipped[1].0
                );
            }
        }
    }

    assert!(
        checked > 0,
        "no pushed reference was examined; this lint would be vacuous"
    );
}

// --------------------------------------------------------------------- 11

/// T0-17 handoff, addendum A2. `docs/stability.md` must not carry a limitation
/// saying the release workflow builds the image more than once: THIS commit
/// retires that limitation, and a doc that asserts both shapes is the defect
/// class the whole scorecard argues against. `check-dod.sh` greps that file
/// only for the ASF attribution sentence, so nothing else catches a doc that
/// contradicts itself. Keep this test when Task 30b extends the file: it is a
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

// -------------------------------------------------------------------- 12

/// TASK 30b. EVERY PLATFORM THE WORKFLOW PUBLISHES WAS LOADED AND ASSERTED IN
/// THE PUSHING JOB, BEFORE THE PUSH — AND WAS COMPILED ON A NATIVE RUNNER.
///
/// `workflow_lint_every_image_is_asserted_before_push` above says the *tags* are
/// the same and in the right order. That is not enough once an image is a
/// manifest list: `docker/build-push-action` with
/// `platforms: linux/amd64,linux/arm64` and `load: true` DOES NOT WORK (the
/// docker exporter cannot export a manifest list into the daemon — critique D
/// H4), and even if it did, a gate that asserts by RUNNING the image cannot
/// exercise a foreign variant on an amd64 runner. So the invariant the whole
/// file exists for — the bytes that were checked are the bytes that are pushed —
/// is reachable for a multi-architecture image only if EACH VARIANT is loaded
/// and asserted separately. This test is what makes that a machine check.
///
/// THE NATIVE-JOBS ARM IS THE SECOND HALF, and it is a deviation from this
/// task's brief recorded in the test rather than only in a report. The brief put
/// all three controller build steps in the pushing job on one amd64 runner; on
/// that runner a `linux/arm64` leg can only be produced by emulating a Rust
/// compile, which STANDING RULE 10 forbids and which `Dockerfile.weirkeeper`
/// refuses by name (plan erratum E19(c): `aws-lc-sys` reads the host
/// `/usr/include` and dies in 106 s on a cross build). So each architecture is
/// compiled by its own native job, and this test asserts that those jobs PUSH
/// NOTHING and that the pushing job `needs` every one of them — without which
/// "the manifest list is assembled from bytes two native runners asserted" is a
/// sentence in a comment rather than a property of the file.
#[test]
fn workflow_lint_every_platform_is_asserted_before_the_manifest_push() {
    let doc = workflow();
    let mut multi_arch_pushes = 0usize;

    for (job, steps) in jobs(&doc) {
        let Some(first_push) = try_push_step_index(steps) else {
            continue;
        };
        for (p, step) in steps.iter().enumerate() {
            if !is_pushing_step(step) {
                continue;
            }
            let platforms = pushed_platforms(step);
            if platforms.len() < 2 {
                // Single-platform: the runner image is pushed by exactly such a
                // step (linux/amd64 only — the engine binary has no arm64
                // manifest, Global Constraint 10). There is no per-platform
                // question to ask, and
                // `workflow_lint_every_image_is_asserted_before_push` has
                // already asked the one that matters.
                continue;
            }
            multi_arch_pushes += 1;

            for platform in &platforms {
                let loaded = steps
                    .iter()
                    .enumerate()
                    .find(|(i, s)| {
                        *i < first_push
                            && is_loading_step(s)
                            && arch_tokens(run(s)).contains(platform)
                    })
                    .map(|(i, _)| i);
                assert!(
                    loaded.is_some(),
                    "job `{job}`, push step {p} publishes `linux/{platform}`, and no step before \
                     the first push (step {first_push}) loads that variant into this job's \
                     daemon. A multi-platform build cannot `load:` — the docker exporter refuses \
                     a manifest list — so each variant arrives as a `docker load` of a tarball a \
                     native job produced and asserted."
                );

                let assert_at = asserted_references(steps)
                    .into_iter()
                    .find(|(i, r)| *i < first_push && arch_tokens(r).contains(platform))
                    .map(|(i, _)| i);
                assert!(
                    assert_at.is_some(),
                    "job `{job}`, push step {p} publishes `linux/{platform}`, and no step before \
                     the first push (step {first_push}) hands a `{platform}` reference to an \
                     image gate. An unasserted variant inside a manifest list is exactly the \
                     defect this file exists to prevent — it is invisible to every other lint \
                     here, because the LIST's digest is captured and the list looks asserted."
                );
                // Both indices are already < first_push by construction above;
                // this states the ordering the failure messages promise.
                assert!(
                    loaded.unwrap() < first_push && assert_at.unwrap() < first_push,
                    "job `{job}`: `linux/{platform}` must be loaded and asserted before step \
                     {first_push}"
                );
            }

            // EACH PUBLISHED PLATFORM WAS COMPILED NATIVELY, IN A JOB OF ITS
            // OWN. Without this, a mutant that deleted the arm64 job and added
            // `platforms: linux/arm64` to a load-and-assert step inside THIS
            // job would satisfy everything above — and would be emulating a
            // Rust compile on an amd64 runner.
            let mut native: BTreeSet<String> = BTreeSet::new();
            for (other, other_steps) in jobs(&doc) {
                for s in other_steps
                    .iter()
                    .filter(|s| builds_the_controller_image(s))
                {
                    let built_for = arch_tokens(s["with"]["platforms"].as_str().unwrap_or(""));
                    if built_for.is_empty() {
                        panic!(
                            "job `{other}` builds Dockerfile.weirkeeper without naming a \
                             `platforms:`. A defaulted platform is how a runner's own \
                             architecture silently becomes the shipped one."
                        );
                    }
                    assert!(
                        !other_steps.iter().any(is_pushing_step),
                        "job `{other}` compiles the controller image AND pushes. The compile jobs \
                         hand their bytes on as tarballs and push nothing: one job pushes \
                         (`workflow_lint_exactly_one_job_pushes`), and it is the job that loaded \
                         and asserted both variants."
                    );
                    assert!(
                        needs_of(&doc, &job).contains(&other),
                        "job `{job}` pushes a manifest list but does not `needs:` `{other}`, \
                         which is the job that compiles and asserts one of its variants. \
                         Without the edge the tarball may not exist when the push runs — and the \
                         ordering this test asserts would be an accident of scheduling."
                    );
                    native.extend(built_for);
                }
            }
            assert_eq!(
                platforms, native,
                "job `{job}`, push step {p} publishes {platforms:?} and the workflow compiles the \
                 controller image natively for {native:?}. Every published platform is built by a \
                 job running ON that architecture — `Dockerfile.weirkeeper` refuses a cross build \
                 by name (E19(c)) and STANDING RULE 10 forbids the emulated alternative, measured \
                 at 33x."
            );
        }
    }

    assert_eq!(
        1, multi_arch_pushes,
        "release.yml must push exactly one multi-platform image — the controller manifest list. \
         Found {multi_arch_pushes}; at 0 this lint is vacuous."
    );
}

// -------------------------------------------------------------------- 13

/// TASK 30b. THE PULL-BACK JOB PULLS AND BUILDS NOTHING.
///
/// Global Constraint 37: "publishable" is proven by a pull, not by an apply. A
/// locally built digest is author-only, and it CHANGES ON EVERY BUILD — BuildKit
/// regenerates the provenance attestation even for a cached no-op rebuild — so
/// the only evidence that the shipped digests are pullable is a pull, on a host
/// that did not build the bytes.
///
/// THE TEST IS ABOUT WHAT THE JOB MUST NOT DO. A `pullback:` job that rebuilt
/// the image and compared it would prove the bytes are REPRODUCIBLE, which is a
/// different property and, for an image whose digest moves on every build, a
/// false one. It would also be green on a run where nothing was ever published.
#[test]
fn workflow_lint_the_pullback_job_builds_nothing() {
    let doc = workflow();
    let steps = job_steps(&doc, "pullback");

    for (i, step) in steps.iter().enumerate() {
        assert!(
            !uses(step).contains("build-push-action"),
            "pullback step {i} uses `{}`: this job pulls what was published and compiles nothing",
            uses(step)
        );
        assert!(
            !is_pushing_step(step),
            "pullback step {i} pushes. It pulls, and only pulls — so that \
             `workflow_lint_exactly_one_job_pushes` keeps cardinality 1."
        );
        // `${{ … }}` first, then `#` comments: a comment may legitimately
        // discuss a compile, and `${{ needs.build-weirkeeper-amd64… }}` is a
        // reference to a job's name, not a command.
        let code = strip_shell_comments(&strip_gha_expressions(run(step)));
        for line in code.lines() {
            let toks: Vec<&str> = line.split_whitespace().collect();
            for (j, t) in toks.iter().enumerate() {
                let is_compile = *t == "build" || *t == "buildx";
                if is_compile && j > 0 && toks[j - 1].ends_with("docker") {
                    panic!(
                        "pullback step {i} names a compile command: {line:?}. The job exists to \
                         prove the PUBLISHED bytes can be pulled by a stranger; rebuilding them \
                         proves something else."
                    );
                }
            }
        }
    }

    assert_eq!(
        vec!["image".to_string()],
        needs_of(&doc, "pullback"),
        "pullback must `needs: [image]` — it reads that job's two published digests, and without \
         the edge it would run before anything was pushed"
    );

    // NOT VACUOUS: it must actually pull, both digests, and BOTH PLATFORMS of
    // the manifest list. The two `--platform` pulls are what prove the list
    // carries both variants; one pull would resolve to the runner's own
    // architecture and say nothing about the other.
    let body: String = steps.iter().map(run).collect::<Vec<_>>().join("\n");
    for needle in [
        "docker pull",
        "--platform linux/amd64",
        "--platform linux/arm64",
        "needs.image.outputs.digest",
        "needs.image.outputs.weirkeeper_digest",
        "GITHUB_STEP_SUMMARY",
    ] {
        assert!(
            body.contains(needle),
            "the pullback job must contain {needle:?}: without it the job is not the pull-back \
             Global Constraint 37 asks for. Its run blocks are:\n{body}"
        );
    }
    for gate in IMAGE_GATES {
        assert!(
            body.contains(gate),
            "the pullback job must re-run `{gate}` against what came BACK from the registry — the \
             same assertions, now on bytes that travelled"
        );
    }
}

// -------------------------------------------------------------------- 14

/// TASK 30b. SPEC §16 CLAUSE 6 — THE AUDITOR'S READER SHIPS INSIDE THE RELEASE
/// ARTEFACT, AND THE UI'S OWN TEST HARNESS DOES NOT.
///
/// An auditor's independence is not real if `docs/verify_scorecard.py` lives
/// only in a repository they have to clone: the whole argument for a second,
/// independent reader is that someone who does not trust this project can check
/// a signed scorecard, and "first clone the project" is the one step that
/// argument cannot afford.
///
/// THE EXCLUSION IS ASSERTED AS PRESENT, NOT INFERRED FROM ABSENCE, and that
/// distinction is the test. `ui/tests/` carries fixtures whose data names the
/// compose stack's MinIO endpoint (critique C `:410`); a bundle that happens not
/// to contain them today because nobody copied them is one refactor away from
/// containing them. A stated `--exclude` pattern is a decision; an absence is a
/// coincidence.
#[test]
fn the_release_artefact_ships_the_auditors_reader() {
    let doc = workflow();

    let mut found: Vec<(String, usize, String)> = Vec::new();
    for (job, steps) in jobs(&doc) {
        for (i, step) in steps.iter().enumerate() {
            if run(step).contains("--exclude=") {
                found.push((job.clone(), i, run(step).to_string()));
            }
        }
    }
    assert_eq!(
        1,
        found.len(),
        "exactly one step in release.yml assembles the install bundle (the one naming an \
         `--exclude=` pattern); found {}: {:?}",
        found.len(),
        found.iter().map(|(j, i, _)| (j, i)).collect::<Vec<_>>()
    );
    let (job, index, block) = &found[0];

    // The archive members, one per line, with the shell's line continuation
    // stripped. Parsed rather than substring-matched, because `NOTICE` is a
    // substring of `THIRD_PARTY_NOTICES.md` and a `contains` check would report
    // a bundle carrying only the inventory as carrying both.
    let members: Vec<String> = block
        .lines()
        .map(|l| l.trim().trim_end_matches('\\').trim().to_string())
        .collect();

    for required in [
        "logweir.yaml",
        "THIRD_PARTY_NOTICES.md",
        "LICENSE",
        "NOTICE",
        "docs/verify_scorecard.py",
        "ui",
    ] {
        assert!(
            members.iter().any(|m| m == required),
            "job `{job}` step {index} assembles the release bundle without `{required}` as an \
             archive member of its own. Spec §16 clause 6 names \
             docs/verify_scorecard.py; Global Constraint 15 names the three licence files. The \
             members it does name are: {members:?}"
        );
    }

    let excludes_ui_tests = [
        "--exclude='ui/tests'",
        "--exclude=\"ui/tests\"",
        "--exclude=ui/tests",
    ]
    .iter()
    .any(|pattern| block.contains(pattern));
    assert!(
        excludes_ui_tests,
        "job `{job}` step {index} ships `ui` without an explicit `--exclude` for `ui/tests`. The \
         shipped bundle must not carry its own test harness and fixtures, whose data names the \
         compose stack's MinIO endpoint — and the exclusion is asserted as PRESENT rather than \
         inferred from what happens to be on disk today. Its run block is:\n{block}"
    );

    // THE BUNDLE HAS TO REACH THE RELEASE. A tarball written into a directory
    // the release action never reads is a bundle nobody can download.
    assert!(
        block.contains("artifacts/"),
        "job `{job}` step {index} must write the bundle under `artifacts/`, which is what the \
         release step attaches"
    );
    let release_files = doc["jobs"][job.as_str()]["steps"]
        .as_sequence()
        .expect("the publish job must have steps")
        .iter()
        .filter(|s| uses(s).contains("action-gh-release"))
        .filter_map(|s| s["with"]["files"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert!(
        release_files.iter().any(|f| f.contains("artifacts/")),
        "job `{job}` assembles a bundle under `artifacts/` and its release step attaches \
         {release_files:?}"
    );

    // THE sha256 LISTING OF EVERY SHIPPED FILE UNDER `ui/` REACHES THE NOTES.
    // Spec §8's reason: the page runs with the viewer's own authority, so the
    // bytes are worth pinning somewhere a reader can check without trusting the
    // bundle they arrived in.
    assert!(
        block.contains("sha256sum"),
        "job `{job}` step {index} must compute a sha256 listing of the files it ships under `ui/`"
    );
    assert!(
        raw().contains("steps.bundle.outputs.ui_sha256"),
        "the sha256 listing must be read into the release notes; a listing nobody publishes is \
         not a listing"
    );

    // AND THE TWO CONTAINER REFERENCES, as the literals an operator pulls.
    //
    // THE NEEDLES ARE ASSEMBLED, NOT SPELLED, and that is not style: interface
    // I15 allows the runner image's literal reference EXACTLY ONE occurrence
    // under `crates/` — `crates/weirkeeper/src/job.rs`'s `RUNNER_IMAGE` — so
    // that a digest bump cannot update one call site and miss another, and
    // `crates/weirkeeper/tests/crd_shape.rs::the_runner_image_is_named_once`
    // counts them. Writing the string here would make THIS file the second
    // occurrence and fail that test, which is exactly what happened while this
    // test was being written. The same `format!` idiom is used there, for the
    // same reason.
    let text = raw();
    for name in ["logweir", "weirkeeper"] {
        let literal = format!("{}{name}@", "ghcr.io/logweir/");
        assert!(
            text.contains(&literal),
            "the release notes must name `{literal}<digest>`: a release that publishes images and \
             does not say what to pull has published nothing anybody can use"
        );
    }
}

/// **Task 30b review, finding F1: `publish:` waits for the pull-back.** The
/// release notes assert the digests were pulled back by this run's
/// `pullback:` job; a graph that did not order the two would publish that
/// claim after a failed pull-back. The job carries NO job-level tag gate,
/// because a skipped `needs` dependency skips `publish:` on every branch run
/// — every step carries the gate instead, so a branch run passes through the
/// job with nothing executed, and a tag run's failed pull-back blocks the
/// release.
#[test]
fn workflow_lint_publish_waits_for_the_pullback() {
    let doc = workflow();
    let needs = needs_of(&doc, "publish");
    assert!(
        needs.iter().any(|n| n == "pullback"),
        "publish must `needs` pullback (got {needs:?}): without the edge a failed pull-back \
         still publishes the notes that claim it succeeded"
    );
    let job = &doc["jobs"]["pullback"];
    assert!(
        job.get("if").is_none(),
        "pullback must carry NO job-level `if:`: a job skipped at job level skips every job \
         that `needs` it, and publish now does"
    );
    let steps = job_steps(&doc, "pullback");
    assert!(!steps.is_empty(), "pullback has steps");
    for (i, step) in steps.iter().enumerate() {
        let is_checkout = step
            .get("uses")
            .and_then(Value::as_str)
            .map(|u| u.starts_with("actions/checkout"))
            .unwrap_or(false);
        if is_checkout {
            continue;
        }
        let cond = step.get("if").and_then(Value::as_str).unwrap_or("");
        assert!(
            cond.contains("startsWith(github.ref, 'refs/tags/')"),
            "pullback step {i} must carry the tag gate on the STEP (the job has none): {step:?}"
        );
    }
}

// ===========================================================================
// TASK 31 — `kind-demo.yml`: Demo 1 in CI, on a cluster the workflow creates
// ===========================================================================
//
// Spec §16 clause 2. Every test below reads `.github/workflows/kind-demo.yml`
// through the same three readers `release.yml` uses, and every one of them
// identifies a step by WHAT IT DOES — the command in its `run:`, the action in
// its `uses:` — never by its `id:` and never by its position, so a rename
// cannot make a lint blind. The two places a NAME is load-bearing
// (`workflow_lint_kind_demo_names_the_install_branch` and
// `x_uiwrite_in_ci_is_labelled_mechanical_only`) are exactly the two places
// where the name is the record of what the run means, and each of those tests
// finds its step by behaviour first and then holds the name to account.

/// THE STEPS OF `kind-demo.yml`'s one job, in file order. A panic here rather
/// than an empty slice: a workflow with no steps is a broken workflow, not a
/// workflow with nothing to lint.
fn kind_demo_steps(doc: &Value) -> &[Value] {
    let all = jobs(doc);
    assert_eq!(
        1,
        all.len(),
        "kind-demo.yml has {} jobs. The whole walk is one job on one runner \
         because the compose stack, the kind cluster and the demo all live in one machine's \
         state; a second job would have none of it. Jobs found: {:?}",
        all.len(),
        all.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    let steps = all[0].1;
    assert!(!steps.is_empty(), "kind-demo.yml's job has no steps");
    steps
}

/// `name:` of a step, or the empty string.
fn step_name(step: &Value) -> &str {
    step["name"].as_str().unwrap_or("")
}

/// Does this step INSTALL the control plane? Identified by the command, never
/// by the name — which is the point: the name is what these tests hold to
/// account, so it cannot also be how they find the step.
fn is_install_step(step: &Value) -> bool {
    let block = run(step);
    block.contains("apply --server-side -k") || block.contains("apply --server-side -f")
}

/// **The cluster is created by this workflow and deleted by it, whatever
/// happened — and it is never the laptop's.**
///
/// STANDING RULE 16: `kind` is a CI-only cluster, created and destroyed by the
/// workflow. A `kind` cluster that survives a failed run is a leaked container
/// network and a leaked `kubeconfig` entry on a runner that is about to be
/// recycled, and — much worse on the one authorised local proving run — a
/// cluster nobody deleted.
///
/// The third clause is the one that could not be recovered from later: this
/// workflow must never name the developer's own cluster. Spec §16 clause 2 is
/// about a cluster the workflow CREATED; a step that reached for the laptop's
/// would produce a green run that proved something else entirely.
///
/// KILLS: removing the `kind delete cluster` step; dropping its `if: always()`
/// (a failed demo then leaks the cluster, which is the case the flag exists
/// for); pointing any step at the laptop cluster.
#[test]
fn workflow_lint_kind_demo_creates_and_deletes_its_cluster() {
    let doc = parsed(KIND_DEMO_YML);
    let steps = kind_demo_steps(&doc);

    let create: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| run(s).contains("kind create cluster"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        1,
        create.len(),
        "kind-demo.yml must have exactly one `kind create cluster` step; found {create:?}. \
         Spec §16 clause 2 is about a cluster THE WORKFLOW created"
    );

    let delete: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| run(s).contains("kind delete cluster"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        1,
        delete.len(),
        "kind-demo.yml must have exactly one `kind delete cluster` step; found {delete:?}"
    );
    assert!(
        create[0] < delete[0],
        "the `kind delete cluster` step (index {}) must come after the `kind create cluster` \
         step (index {})",
        delete[0],
        create[0]
    );
    assert_eq!(
        "always()",
        condition(&steps[delete[0]]).trim(),
        "the `kind delete cluster` step must carry `if: always()`; it carries `{}`. Without it \
         a failed demo leaks the cluster — which is the only case the teardown exists for",
        condition(&steps[delete[0]])
    );

    // AND THE WORKFLOW NEVER NAMES THE DEVELOPER'S OWN CLUSTER. Read over the
    // whole of every step — `name:`, `run:`, `uses:`, `env:` and `with:` — by
    // serialising the step back to YAML, so a context hidden in a `with:` value
    // or an environment variable is caught as readily as one on a command line.
    let laptop_context = concat!("docker-", "desktop");
    for (i, step) in steps.iter().enumerate() {
        let text = serde_yaml::to_string(step).expect("a step re-serialises");
        assert!(
            !text.contains(laptop_context),
            "step {i} of kind-demo.yml names the developer's own cluster context. This workflow \
             owns ONE cluster, the `kind` one it created (STANDING RULE 16), and every `kubectl` \
             in it and in `scripts/kind-demo.sh` names `kind-logweir`:\n{text}"
        );
    }
}

/// **Three artefacts, uploaded whatever happened.**
///
/// A demo that failed at step 7 and uploaded nothing has told a reader that it
/// failed and nothing about why; the compose logs, the cluster's own state and
/// the controller's log are the three places the answer is, and all three are
/// deleted seconds later by the teardown.
///
/// KILLS: dropping any of the three uploads; leaving one without `if:
/// always()`, which uploads diagnostics on exactly the runs that need none.
#[test]
fn workflow_lint_kind_demo_uploads_diagnostics_on_failure() {
    let doc = parsed(KIND_DEMO_YML);
    let steps = kind_demo_steps(&doc);

    let uploads: Vec<(usize, &Value)> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| uses(s).contains("upload-artifact"))
        .collect();
    assert!(
        uploads.len() >= 3,
        "kind-demo.yml uploads {} artefact(s). Three are required: the compose logs, the \
         cluster's own state and the controller's log — the three places the answer to a \
         failed demo is, all of them deleted by the teardown moments later",
        uploads.len()
    );
    for (i, step) in &uploads {
        assert_eq!(
            "always()",
            condition(step).trim(),
            "the artefact upload at step {i} (`{}`) must carry `if: always()`; it carries \
             `{}`. Without it the diagnostics are uploaded on exactly the runs that do not \
             need them",
            step_name(step),
            condition(step)
        );
    }

    // AND THE TEARDOWN IS UNCONDITIONAL TOO — both halves of it, because a
    // compose stack left up on a runner is a leak and a compose stack left up
    // on the authorised local proving run breaks `just lint` (Global
    // Constraint 22: `time-unit-suite.sh` refuses while 9092 or 9000 answers).
    let down = steps
        .iter()
        .find(|s| run(s).contains("down -v"))
        .unwrap_or_else(|| panic!("kind-demo.yml must tear the compose stack down"));
    assert_eq!(
        "always()",
        condition(down).trim(),
        "the `docker compose … down -v` step must carry `if: always()`"
    );
    assert!(
        run(down).contains("--profile setup") && run(down).contains("--profile tools"),
        "the teardown must name BOTH profiles: `down` only removes containers for services in \
         the ACTIVE profile set, so a plain `down -v` walks past every setup container an \
         earlier `up` left behind (`justfile`'s `e2e-down`, and its reason):\n{}",
        run(down)
    );
}

/// **The controller image is built for the runner's own architecture, by the
/// one named producer.**
///
/// `just image-weirkeeper` builds `--platform "${LOGWEIR_IMAGE_PLATFORM:-linux/arm64}"`
/// — arm64 by Global Constraint 10, because the development host is arm64 and
/// every local gate inspects a loaded tag. A GitHub runner is amd64, where that
/// build is EMULATED: STANDING RULE 10's forbidden case, measured at 33x in
/// `docs/stability.md`, and it looks like a stall rather than a failure. This
/// workflow sets the variable the recipe reads; Task 23 owns the recipe
/// (STANDING RULE 17 — this task is in chain W and not in chain J).
///
/// The second half is the reason the first half can be a one-line assertion:
/// there is ONE producer of each local tag and ONE place a platform is written.
/// A hand-rolled `docker build` here would be a second, and the platform would
/// then be true in one place and false in another.
///
/// KILLS: dropping `LOGWEIR_IMAGE_PLATFORM` from the step's `env:` (the arm64
/// Rust compile under QEMU then runs on an amd64 runner); open-coding
/// `docker build` or `docker buildx build` anywhere in the file.
#[test]
fn workflow_lint_kind_demo_builds_weirkeeper_for_the_runner_platform() {
    let doc = parsed(KIND_DEMO_YML);
    let steps = kind_demo_steps(&doc);

    let build = steps
        .iter()
        .find(|s| words(run(s)).contains(&"image-weirkeeper"))
        .unwrap_or_else(|| {
            panic!(
                "kind-demo.yml must run `just image-weirkeeper` — the one named producer of the \
                 local controller tag (`justfile`)"
            )
        });
    let platform = build["env"]["LOGWEIR_IMAGE_PLATFORM"]
        .as_str()
        .unwrap_or("");
    assert_eq!(
        "linux/amd64",
        platform.trim(),
        "the `just image-weirkeeper` step must carry `LOGWEIR_IMAGE_PLATFORM: linux/amd64` in \
         its `env:`; it carries `{platform}`. The recipe defaults to linux/arm64 for the \
         development host (Global Constraint 10), and on an amd64 runner that default compiles \
         the whole Rust workspace under QEMU — STANDING RULE 10's forbidden case, 33x in \
         `docs/stability.md`, indistinguishable from a stall"
    );

    // NO SECOND PRODUCER. Text, not tree: a `docker build` inside a `run:`
    // block is a string the YAML model cannot tell from prose, and `${{ … }}`
    // expressions and shell comments are stripped first for the reason Task 30
    // stripped them — `${{ steps.build.outputs.digest }}` is a legal reference
    // to a build step's OUTPUT and a `#` line is not a command.
    let text = strip_shell_comments(&strip_gha_expressions(&raw_of(KIND_DEMO_YML)));
    for (n, line) in text.lines().enumerate() {
        let toks: Vec<&str> = line.split_whitespace().collect();
        for (i, t) in toks.iter().enumerate() {
            if *t != "docker" {
                continue;
            }
            let next = toks.get(i + 1).copied().unwrap_or("");
            assert!(
                next != "build" && next != "buildx",
                "kind-demo.yml line {} open-codes `docker {next}`. The producers of the two \
                 local tags are `just image` and `just image-weirkeeper`, and they are the one \
                 place a platform is written (`justfile`):\n    {line}",
                n + 1
            );
        }
    }
}

/// **The install step says which world it installed from.**
///
/// Global Constraint 37: a locally built or locally loaded image is
/// AUTHOR-ONLY and never satisfies spec §16 clause 1 — the `registry:2`
/// fallback included. A green `kind-demo` therefore means one of two quite
/// different things, and the only durable record of which is the step's own
/// name in the run log.
///
/// The published branch carries an assertion the author-only branch cannot: a
/// `rollout status` on the controller Deployment, which is the one thing
/// X-APPLY cannot prove — a pod PULLED the shipped digest from a registry the
/// author does not control and reached Ready.
///
/// KILLS: renaming either branch to something that does not say what it
/// installed; adding a published-digest install with no `rollout status` after
/// it, which would let a run claim the pull without ever asserting a pod
/// started.
#[test]
fn workflow_lint_kind_demo_names_the_install_branch() {
    const AUTHOR_ONLY: &str = "author-only images; NOT evidence";
    const PUBLISHED: &str = "published digests, pulled by the cluster";

    let doc = parsed(KIND_DEMO_YML);
    let steps = kind_demo_steps(&doc);

    let installs: Vec<(usize, &Value)> = steps
        .iter()
        .enumerate()
        .filter(|(_, s)| is_install_step(s))
        .collect();
    assert!(
        !installs.is_empty(),
        "kind-demo.yml installs nothing: no step runs `kubectl … apply --server-side`"
    );

    let mut saw_author_only = false;
    let mut saw_published = false;
    for (i, step) in &installs {
        let name = step_name(step);
        assert!(
            name.contains(AUTHOR_ONLY) || name.contains(PUBLISHED),
            "the install step at index {i} is named `{name}`, which says neither \
             `{AUTHOR_ONLY}` nor `{PUBLISHED}`. Those two installs prove different things and \
             the run log's only record of which one ran is this name"
        );
        saw_author_only |= name.contains(AUTHOR_ONLY);
        if name.contains(PUBLISHED) {
            saw_published = true;
            // … AND THE ROLLOUT THAT MAKES IT MEAN SOMETHING, in the next step
            // that runs a command.
            let next = steps[i + 1..]
                .iter()
                .find(|s| !run(s).is_empty())
                .unwrap_or_else(|| {
                    panic!(
                        "the published-digest install at index {i} is the last step that runs \
                         anything. It must be followed by a `rollout status`: applying a \
                         manifest that names a published digest proves nothing until a pod has \
                         pulled it and reached Ready"
                    )
                });
            let block = run(next);
            assert!(
                block.contains("rollout status"),
                "the step after the published-digest install runs `{block}` and not a `rollout \
                 status`. That rollout is the assertion X-APPLY cannot make"
            );
            assert!(
                block.contains("--timeout=180s"),
                "the `rollout status` after the published-digest install must bound its wait at \
                 `--timeout=180s`; a rollout that waits for ever reports a stuck pull as a \
                 cancelled job:\n{block}"
            );
        }
    }
    assert!(
        saw_author_only,
        "kind-demo.yml has no `{AUTHOR_ONLY}` install branch. That is the branch that can run \
         today — the images are not published — and the one whose name refuses the clause-1 \
         claim"
    );
    assert!(
        saw_published,
        "kind-demo.yml has no `{PUBLISHED}` install branch. Both branches are written now, so \
         the day a remote exists the workflow needs a variable set and not a rewrite"
    );
}

/// **The CI half of X-UIWRITE is labelled as the half it is.**
///
/// Spec §10's gate is a `create` of a `Restore` FROM THE PAGE, and `curl` is
/// not the page. A runner has no browser, so the mechanical half — the
/// same-origin `POST` through `kubectl proxy`, asserting 201 — is all CI can
/// do. A step named plain `X-UIWRITE` would put a browser assertion in the
/// plan that exists nowhere in it; naming the half AND citing the document
/// where the other half is recorded is what keeps the ledger honest.
///
/// KILLS: naming the CI step plain `X-UIWRITE`; dropping the citation of
/// `e2e/k8s/laptop-demo.md`, which is where the in-browser half lives.
#[test]
fn x_uiwrite_in_ci_is_labelled_mechanical_only() {
    let doc = parsed(KIND_DEMO_YML);
    let steps = kind_demo_steps(&doc);

    // FOUND BY WHAT IT DOES: the step that runs the demo script. The name is
    // what this test holds to account, so it cannot also be the way in.
    let demo = steps
        .iter()
        .find(|s| run(s).contains("scripts/kind-demo.sh"))
        .unwrap_or_else(|| {
            panic!(
                "kind-demo.yml must run `scripts/kind-demo.sh` — the driver that patches \
                 CoreDNS, probes the advertised listener from a pod and then runs the twelve \
                 steps of `scripts/demo-steps.sh`"
            )
        });
    let name = step_name(demo);
    assert!(
        name.contains("mechanical half only"),
        "the step that runs the demo is named `{name}`. It performs X-UIWRITE's MECHANICAL half \
         only — `curl` is not the page (spec §10) — and the name is the only place in a run log \
         that says so"
    );
    assert!(
        name.contains("e2e/k8s/laptop-demo.md"),
        "the step that runs the demo is named `{name}` and cites no document. The half spec §10 \
         actually requires is recorded at `e2e/k8s/laptop-demo.md`, proven by `\"manager\": \
         \"logweir-ui\"` in the created object's managedFields; a name that says \"mechanical \
         only\" without saying where the other half is invites the reader to conclude there \
         isn't one"
    );
}
