//! Lints on `.github/workflows/release.yml`. THIS PROVES THE WORKFLOW SAYS THE
//! RIGHT THING, NOT THAT IT DOES — the workflow has never executed on any
//! commit (release.yml:4-8). Task 30b extends this file with the `weirkeeper`
//! image; keep the helpers reusable.
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
//! NO DOCKER, NO NETWORK, NO `#[ignore]`: every test here reads files from the
//! working tree and parses them, so they run in the default
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
    action_pushes || run(step).contains("docker push")
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

/// The index of the step that calls the shared gate script, or `None`.
fn try_check_step_index(steps: &[Value]) -> Option<usize> {
    steps
        .iter()
        .position(|s| run(s).contains("scripts/check-image.sh"))
}

/// The index of the step that calls the shared gate script.
fn check_step_index(steps: &[Value]) -> usize {
    try_check_step_index(steps).expect("no step in this job runs scripts/check-image.sh")
}

/// The index of the step that pushes, or `None`.
fn try_push_step_index(steps: &[Value]) -> Option<usize> {
    steps.iter().position(is_pushing_step)
}

/// The index of the step that pushes.
fn push_step_index(steps: &[Value]) -> usize {
    try_push_step_index(steps).expect("no step in this job pushes the image")
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

        let has_check = steps
            .iter()
            .any(|s| run(s).contains("scripts/check-image.sh"));
        assert!(
            has_check,
            "job `{job}` builds an image but runs no assertion: the image assertions must be the \
             shared scripts/check-image.sh, not inline run: steps"
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
        assert_eq!(
            Some("logweir:check"),
            step["with"]["tags"].as_str().map(str::trim),
            "job `{job}`: the build step must produce exactly the local tag the assert step is handed"
        );
    }

    assert!(seen > 0, "release.yml has no build step to lint");
}

// --------------------------------------------------------------------- 4

/// THE TASK'S PROPERTY. The tag that is built, the tag that is asserted and the
/// tag that is pushed from are one string, and they happen in that order.
///
/// F1: over EVERY job that builds OR pushes. A job that pushes without building
/// and asserting first is the exact defect this file exists to prevent, so the
/// `expect`s below are the assertion, not an accident of helper choice.
#[test]
fn workflow_lint_the_asserted_image_is_the_pushed_image() {
    let doc = workflow();
    let mut seen = 0usize;

    for (job, steps) in jobs(&doc) {
        if try_build_step_index(steps).is_none() && try_push_step_index(steps).is_none() {
            continue;
        }
        seen += 1;

        let b = build_step_index(steps);
        let c = check_step_index(steps);
        let p = push_step_index(steps);

        let built_tag = steps[b]["with"]["tags"]
            .as_str()
            .unwrap_or_else(|| panic!("job `{job}`: the build step must name a tag"))
            .trim()
            .to_string();
        let checked_tag =
            arg_after(run(&steps[c]), "scripts/check-image.sh").unwrap_or_else(|| {
                panic!("job `{job}`: the assert step must hand check-image.sh an image reference")
            });
        let sources = docker_tag_sources(run(&steps[p]));
        assert!(
            !sources.is_empty(),
            "job `{job}`: the push step must `docker tag` the asserted image before pushing it"
        );

        assert!(
            b < c && c < p,
            "job `{job}`: order must be build({b}) → assert({c}) → push({p})"
        );
        assert_eq!(built_tag, checked_tag, "job `{job}`");
        for pushed_from_tag in &sources {
            assert_eq!(built_tag, *pushed_from_tag, "job `{job}`");
        }
    }

    assert!(seen > 0, "release.yml neither builds nor pushes an image");
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
        let Some(p) = try_push_step_index(steps) else {
            continue;
        };
        seen += 1;

        let push = &steps[p];
        let id = push["id"].as_str().unwrap_or_else(|| {
            panic!(
                "job `{job}`: the push step must carry an `id:` so the job can publish its digest"
            )
        });

        let outputs = doc["jobs"][job.as_str()]["outputs"]
            .as_mapping()
            .unwrap_or_else(|| {
                panic!(
                    "job `{job}` pushes but declares no outputs: block carrying the pushed digest"
                )
            });
        let v = outputs
            .get(Value::from("digest"))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("jobs.{job}.outputs must have a `digest` key"))
            .to_string();
        assert!(
            v.contains(&format!("steps.{id}.outputs.digest")),
            "jobs.{job}.outputs.digest must come from the PUSH step `{id}` (the only step with a \
             repository digest); it reads {v:?}"
        );

        let push_run = run(push);
        assert!(
            push_run.contains(">> \"$GITHUB_OUTPUT\"") && push_run.contains("digest="),
            "job `{job}`: the push step must write digest=… to $GITHUB_OUTPUT; its run block is:\n{push_run}"
        );

        let needle = format!("needs.{job}.outputs.digest");
        assert!(
            raw().contains(&needle),
            "a captured digest nobody reads is not a captured digest: nothing in release.yml reads \
             {needle}"
        );
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
        "the pushing job `{}` runs no scripts/check-image.sh",
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
