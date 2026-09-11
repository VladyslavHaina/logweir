//! **Phase C's exit criterion, and install gate X-UIWRITE, guarded.** Task 28.
//!
//! `scripts/laptop-demo.sh` is the walk spec §1 calls the whole acceptance
//! surface, and `e2e/k8s/laptop-demo.md` is the transcript of the run that
//! proved it. This file is what keeps both honest between runs.
//!
//! # The one thing this file exists for
//!
//! Spec §10's X-UIWRITE reads: under `kubectl proxy --www=`, a `create` of a
//! `Restore` **from the page** returns 201. **`curl` is not the page.** The
//! first draft of this task recorded only the scripted `curl` half, and every
//! assertion a transcript test could naively state was satisfied by it — the
//! section was present, non-empty, carried a verdict and contained `201` — so
//! the mutant "record only the `curl` half" survived, and the one install gate
//! Phase C owns was never actually run anywhere in the plan (Task 31 labels
//! its own CI half "mechanical only" on the grounds that the manual half is
//! this task's).
//!
//! The fix is a string only the page can produce. `ui/api.js`'s `create`
//! appends `?fieldManager=logweir-ui` (interface register I23), so the created
//! object's `metadata.managedFields` names `logweir-ui` — not `kubectl`, and
//! not the unstable browser-derived User-Agent the API server would otherwise
//! have derived, which is nothing a transcript could be checked against. So
//! [`laptop_demo_transcript_is_present`] requires a **separately headed**
//! `in-browser create` sub-section carrying the literal `"manager":
//! "logweir-ui"`, and the mutant fails on BOTH the heading and the literal.
//!
//! # `mod support;` and why this file lives here
//!
//! STANDING RULE 20 — *exit codes are read directly, never through a pipe* —
//! is audited by Task 12's tokeniser, `crates/logweir/tests/support/
//! exit_code_lint.rs` (interface register I29). **This task REUSES it and
//! writes no second one** (controller ruling, critique C M12). `mod support;`
//! reaches a sibling module inside the SAME test target, so this file is a
//! `crates/logweir/tests/` file for that mechanical reason and no other.
//!
//! # Global Constraint 22, and the one process this file starts
//!
//! Nothing here dials a socket, waits on a Kubernetes Job or shells `kubectl`,
//! `docker` or `just`. [`laptop_demo_refuses_a_wrong_context`] runs
//! `bash scripts/laptop-demo.sh` with `$PATH` pointed at
//! `fixtures/stub-kubectl` — a shell stub that answers `config
//! current-context` with `kind-x` and refuses everything else, contacting
//! nothing — so the whole test is a fork, a string compare and an exit code,
//! in milliseconds. It does leave an empty `.demo/laptop/` behind (the script
//! creates its output directory before reading the context); `.demo/` is
//! gitignored.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use support::exit_code_lint;

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
// THE TRANSCRIPT
// ---------------------------------------------------------------------------

/// `## `-level sections of a Markdown document, as `(heading, body)`.
///
/// `### ` is NOT a section here: `starts_with("## ")` needs a space at index
/// 2, which `###` does not have, so a sub-heading stays inside its parent's
/// body — which is exactly what the X-UIWRITE arm below needs.
fn sections(md: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in md.lines() {
        if let Some(rest) = line.strip_prefix("## ") {
            out.push((rest.trim().to_string(), String::new()));
        } else if let Some(last) = out.last_mut() {
            last.1.push_str(line);
            last.1.push('\n');
        }
    }
    out
}

/// The text under a `### ` sub-heading containing `needle`, up to the next
/// `###` (there is no `##` inside a section by construction).
fn subsection<'a>(body: &'a str, needle: &str) -> Option<&'a str> {
    let mut start: Option<usize> = None;
    let mut offset = 0usize;
    for line in body.split_inclusive('\n') {
        let is_sub = line.starts_with("### ");
        if let Some(s) = start {
            if is_sub {
                return Some(&body[s..offset]);
            }
        } else if is_sub && line.to_lowercase().contains(needle) {
            start = Some(offset + line.len());
        }
        offset += line.len();
    }
    start.map(|s| &body[s..])
}

/// Whether `body` carries a fenced block with at least one non-blank line in
/// it. "A non-empty transcript" means output, not a heading and a promise.
fn has_transcript(body: &str) -> bool {
    let mut inside = false;
    for line in body.lines() {
        if line.trim_start().starts_with("```") {
            inside = !inside;
            continue;
        }
        if inside && !line.trim().is_empty() {
            return true;
        }
    }
    false
}

/// **The recorded walk, with both halves of X-UIWRITE.**
///
/// KILLS: recording only the `curl` half of X-UIWRITE (fails on the missing
/// `in-browser create` sub-heading AND on the missing `"manager":
/// "logweir-ui"`); dropping `?fieldManager=logweir-ui` from `api.js`'s
/// `create` (the `managedFields` output then names a browser User-Agent and
/// the literal is absent); deleting any numbered step's transcript or verdict.
#[test]
fn laptop_demo_transcript_is_present() {
    let md = read("e2e/k8s/laptop-demo.md");
    let sections = sections(&md);
    assert!(
        !sections.is_empty(),
        "e2e/k8s/laptop-demo.md has no `## ` sections at all — a transcript that enumerated \
         nothing is not a record"
    );

    // ONE SECTION PER NUMBERED STEP, each with output and a verdict.
    for n in 1..=12u32 {
        let prefix = format!("{n}.");
        let found = sections
            .iter()
            .find(|(h, _)| h.starts_with(&prefix))
            .unwrap_or_else(|| {
                panic!(
                    "e2e/k8s/laptop-demo.md has no `## {prefix} …` section. The walk has twelve \
                     numbered steps and the transcript records each one; headings found: {:?}",
                    sections.iter().map(|(h, _)| h).collect::<Vec<_>>()
                )
            });
        assert!(
            has_transcript(&found.1),
            "step {n} (`## {}`) carries no fenced transcript with output in it. A section that \
             says what was supposed to happen is a plan, not a record",
            found.0
        );
        assert!(
            found.1.contains("Verdict:"),
            "step {n} (`## {}`) states no verdict. Every step of the recorded walk says whether \
             it passed",
            found.0
        );
    }

    // STEP 10 IS X-UIWRITE, AND IT HAS TWO HALVES.
    let (heading, body) = sections
        .iter()
        .find(|(h, _)| h.starts_with("10.") && h.contains("X-UIWRITE"))
        .unwrap_or_else(|| {
            panic!(
                "no `## 10. … X-UIWRITE …` section in e2e/k8s/laptop-demo.md; headings found: {:?}",
                sections.iter().map(|(h, _)| h).collect::<Vec<_>>()
            )
        });

    // (a) the scripted half's status, verbatim.
    assert!(
        body.contains("201"),
        "the X-UIWRITE section (`## {heading}`) does not carry the literal `201` from the \
         scripted `curl` create. That status IS the gate's first half"
    );

    // (b) THE HALF SPEC §10 ACTUALLY ASKS FOR, separately headed.
    let in_browser = subsection(body, "in-browser create").unwrap_or_else(|| {
        panic!(
            "the X-UIWRITE section (`## {heading}`) has no separately headed `in-browser create` \
             sub-section. Spec §10 asks for a `create` of a `Restore` FROM THE PAGE, and `curl` \
             is not the page: a transcript carrying only the scripted half satisfies every other \
             assertion here and still has not run the gate"
        )
    });
    assert!(
        has_transcript(in_browser),
        "the `in-browser create` sub-section of `## {heading}` carries no fenced transcript"
    );
    // AND IT IS THE SECOND PASS'S OUTPUT, NOT THE FIRST PASS'S NOTICE. Under
    // `LOGWEIR_DEMO_NONINTERACTIVE=1` step 10(b) PRINTS the `kubectl … -o
    // jsonpath='{.metadata.managedFields}'` command and the string it will
    // produce, and then says it is skipped — so a transcript that pasted that
    // notice under this heading would carry the literal without anyone ever
    // having created anything from the page. Two clauses close it: the skip
    // notice must be absent, and `"operation": "Update"` — a key only the API
    // server's own `managedFields` entry has — must be present.
    assert!(
        !in_browser.contains("SKIPPED in this pass"),
        "the `in-browser create` sub-section of `## {heading}` carries step 10(b)'s SKIPPED \
         notice. That notice quotes the command and the string it produces, so pasting it here \
         would satisfy a literal check without anybody having created a `Restore` from the page. \
         This sub-section is the SECOND pass's transcript:\n{in_browser}"
    );
    assert!(
        in_browser.contains("\"operation\": \"Update\""),
        "the `in-browser create` sub-section of `## {heading}` carries no `\"operation\": \
         \"Update\"` — the key every `metadata.managedFields` entry has. What is recorded here \
         is the API server's own record of the write, not a sentence about it:\n{in_browser}"
    );
    assert!(
        in_browser.contains("\"manager\": \"logweir-ui\""),
        "the `in-browser create` sub-section of `## {heading}` does not carry the literal \
         `\"manager\": \"logweir-ui\"` out of the created object's `metadata.managedFields`. \
         That string is the evidence the write came from the page — `ui/api.js`'s `create` \
         appends `?fieldManager=logweir-ui` (interface register I23), the scripted half's \
         manager is `logweir-cli`, and without an explicit `?fieldManager=` the API server \
         derives an unstable browser User-Agent nothing can be checked against.\n\nthe \
         sub-section, verbatim:\n{in_browser}"
    );
}

// ---------------------------------------------------------------------------
// THE SCRIPT
// ---------------------------------------------------------------------------

/// **STANDING RULE 20, over the walkthrough.** Task 12's tokeniser (I29),
/// REUSED.
///
/// KILLS: piping step 3's `kubectl apply` into `tee` (or any other guarded
/// line into anything), and dropping the `rc=$?` that follows one.
#[test]
fn laptop_demo_never_pipes_a_load_bearing_exit_code() {
    let script = read("scripts/laptop-demo.sh");

    // THE SELF-CHECK FIRST. A lint over a file it found no guarded lines in
    // passes vacuously, and would keep passing if every `kubectl` were respelt
    // `"$KUBECTL"`. The number is a floor: the walk runs `kubectl` more than
    // thirty times, plus `docker`, `curl`, `just` and `logweir`.
    let guarded = exit_code_lint::logical_lines(&script)
        .into_iter()
        .filter(exit_code_lint::LogicalLine::is_guarded)
        .count();
    assert!(
        guarded >= 30,
        "the tokeniser found only {guarded} guarded line(s) in scripts/laptop-demo.sh — a lint \
         that sees nothing cannot fail. Are the tools still invoked as the bare words `kubectl`, \
         `curl`, `docker`, `just` and `logweir`?"
    );

    exit_code_lint::assert_no_masked_exit_code("scripts/laptop-demo.sh", &script);
}

/// **STANDING RULE 12, on every line that runs `kubectl`.**
///
/// Physical lines, not logical ones: a here-doc body that shows an operator a
/// `kubectl` command is a shipped string too, and STANDING RULE 12 binds
/// "every acceptance line, every script, every shipped document and every
/// pinned string".
///
/// KILLS: dropping `--context docker-desktop` from any `kubectl` line.
#[test]
fn laptop_demo_names_every_kubectl_context() {
    let script = read("scripts/laptop-demo.sh");
    let mut offenders: Vec<String> = Vec::new();
    let mut seen = 0usize;
    for (i, line) in script.lines().enumerate() {
        if line.split_whitespace().next() != Some("kubectl") {
            continue;
        }
        seen += 1;
        if !line.contains("--context docker-desktop") {
            offenders.push(format!("line {}: {}", i + 1, line.trim()));
        }
    }
    assert!(
        seen >= 25,
        "only {seen} line(s) in scripts/laptop-demo.sh begin with `kubectl` — this lint would \
         pass vacuously over a script that spelt the tool some other way"
    );
    assert!(
        offenders.is_empty(),
        "{} `kubectl` line(s) in scripts/laptop-demo.sh do not name the context (STANDING RULE \
         12 — `kubectl proxy` included, and it is the one command in this product that hands a \
         browser a cluster credential):\n{}",
        offenders.len(),
        offenders.join("\n")
    );
}

/// **The published `K8S` listener, and not the pod's own loopback.**
///
/// `host.docker.internal:9095` is the listener Task 7 published for pods
/// (STANDING RULE 15). The runner IS a pod, so a bootstrap address on the
/// host-side harness listener sends it to its own loopback — and the broker's
/// metadata sends it there again on every later connection.
///
/// KILLS: pointing the `KafkaCluster` at the host-side listener.
//
// THE FORBIDDEN ADDRESS IS SPELLED AS A CONCATENATION, and that is
// load-bearing rather than decorative. `crates/logweir/tests/
// no_network_in_unit_tests.rs` greps every `.rs` file under `crates/**` for
// that literal and fails naming the file unless it is in `ALLOWED`; writing
// the string out here would put a test file that constructs no client on a
// dial-token allowlist, for a string it exists to FORBID. The precedent is
// `ui/api.js`'s `SCHEME_SEPARATOR`, built the same way for the same kind of
// reason.
#[test]
fn laptop_demo_uses_the_published_k8s_listener() {
    let script = read("scripts/laptop-demo.sh");
    let forbidden = concat!("local", "host:9092");
    assert!(
        script.contains("host.docker.internal:9095"),
        "scripts/laptop-demo.sh must point the KafkaCluster at the published K8S listener \
         `host.docker.internal:9095` (Task 7, STANDING RULE 15)"
    );
    assert!(
        !script.contains(forbidden),
        "scripts/laptop-demo.sh names the host-side harness listener. A runner pod that \
         bootstraps there reaches its OWN loopback, and step 7 times out waiting for \
         `status.reachable`"
    );
}

/// **No private key ever reaches the page.**
///
/// The wizard's create needs no key at all; the approval is minted on the HOST
/// with the CLI at step 11, and the only things that go into the browser are
/// `approval.json` and `approval.sig` — two public documents.
///
/// KILLS: uploading `approver.pem` through the page in step 11.
#[test]
fn laptop_demo_never_sends_a_key_to_the_page() {
    let script = read("scripts/laptop-demo.sh");
    let lines = exit_code_lint::logical_lines(&script);

    // Step 11 runs on the host, with the CLI, against the approver's own key.
    let approve = lines
        .iter()
        .find(|l| l.first_word == "logweir" && l.code.contains("drill approve"))
        .unwrap_or_else(|| {
            panic!(
                "scripts/laptop-demo.sh runs no `logweir drill approve` line. Step 11 mints the \
                 approval OUT OF BAND, on this host, with the shipped CLI — that is what makes \
                 `self_attested: false` possible and what keeps the key off the page"
            )
        });
    for needed in [
        "--spec",
        "--key",
        "--approver",
        "--ticket",
        "--subject-kind Restore",
        "--out",
    ] {
        assert!(
            approve.code.contains(needed),
            "the `logweir drill approve` line is missing `{needed}`:\n    {}",
            approve.code
        );
    }

    // And nothing `curl` posts is key material.
    for line in lines.iter().filter(|l| l.first_word == "curl") {
        for token in line.code.split_whitespace() {
            let path = token
                .trim_start_matches('@')
                .trim_matches(['"', '\''].as_ref());
            assert!(
                !(path.ends_with(".pem") || path.ends_with(".key")),
                "line {}: a `curl` in scripts/laptop-demo.sh posts `{path}`. Nothing in this \
                 walk sends key material anywhere, and the create the page performs needs no \
                 key at all:\n    {}",
                line.line,
                line.code
            );
        }
    }
}

/// **The refusal, proved — with a `kubectl` that dials nothing.**
///
/// KILLS: removing the step-1 context check. Without it the script proceeds to
/// step 2, the stub refuses `create namespace` with exit 1, `set -e` aborts,
/// and the required stderr string is absent. No socket is dialled and nothing
/// times out.
#[test]
fn laptop_demo_refuses_a_wrong_context() {
    let root = root();
    let bin = tempdir("logweir-t28-stub-kubectl");
    let stub = root.join("crates/logweir/tests/fixtures/stub-kubectl");
    let installed = bin.join("kubectl");
    std::fs::copy(&stub, &installed).expect("the stub kubectl is copied onto $PATH");
    make_executable(&installed);

    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg("scripts/laptop-demo.sh")
        .current_dir(&root)
        .env("PATH", path)
        .env(
            "KUBECONFIG",
            root.join("crates/logweir/tests/fixtures/kubeconfig-kind-x.yaml"),
        )
        .env("LOGWEIR_DEMO_NONINTERACTIVE", "1")
        .output()
        .expect("bash runs scripts/laptop-demo.sh");

    let code = out.status.code();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();

    let _ = std::fs::remove_dir_all(&bin);

    assert_eq!(
        code,
        Some(1),
        "the walkthrough must exit 1 on a context that is not docker-desktop; it exited \
         {code:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    assert!(
        stderr.contains("refusing: current context is kind-x, not docker-desktop"),
        "the refusal string is exact and it goes to stderr. Expected `refusing: current context \
         is kind-x, not docker-desktop`\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}

/// The script is executable and declares bash.
///
/// `bash -n` is not run here — a `#[test]` that shells out belongs in a recipe
/// (Global Constraint 22), and `laptop_demo_refuses_a_wrong_context` already
/// executes the file's first step. This is the cheap half: `just laptop-demo`
/// invokes it as `./scripts/laptop-demo.sh`, and a non-executable file fails
/// with a permission error that says nothing about the demo.
#[test]
fn the_laptop_demo_script_is_an_executable_bash_script() {
    let p = root().join("scripts/laptop-demo.sh");
    let body = read("scripts/laptop-demo.sh");
    assert!(
        body.starts_with("#!/usr/bin/env bash"),
        "the script declares bash; got {:?}",
        body.lines().next()
    );
    assert!(
        is_executable(&p),
        "{} must be executable — `just laptop-demo` runs it as `./scripts/laptop-demo.sh`",
        p.display()
    );
    let stub = root().join("crates/logweir/tests/fixtures/stub-kubectl");
    assert!(
        is_executable(&stub),
        "{} must be executable — the refusal test copies it onto $PATH as `kubectl`",
        stub.display()
    );
}

/// **The `laptop-demo` recipe**, which is two lines and obeys the same rule.
///
/// The extractor stops at the first later unindented, non-blank line, for the
/// reason Task 24's did: reading to the end of the file would lint the NEXT
/// recipe's lines under this one's name. This recipe is appended at the END of
/// the justfile (STANDING RULE 17, chain J slot 21), after `ui`.
#[test]
fn the_laptop_demo_recipe_masks_no_exit_code() {
    let justfile = read("justfile");
    let at = justfile
        .find("\nlaptop-demo:")
        .expect("`just laptop-demo` is a recipe in the justfile");
    let mut recipe = String::new();
    for (i, line) in justfile[at + 1..].lines().enumerate() {
        let indented = line.starts_with(' ') || line.starts_with('\t');
        if i > 0 && !indented && !line.is_empty() {
            break;
        }
        recipe.push_str(line);
        recipe.push('\n');
    }
    exit_code_lint::assert_no_masked_exit_code("justfile (laptop-demo)", &recipe);
    assert!(
        recipe.contains("./scripts/laptop-demo.sh"),
        "the recipe runs the script this file lints; got:\n{recipe}"
    );
    assert!(
        recipe.contains("cargo build -p logweir"),
        "plan erratum E9: the script shells `logweir drill approve` and `logweir drill verify` \
         from target/debug/logweir, and nothing else in the recipe builds it"
    );
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

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn tempdir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("{tag}-{}-{}", std::process::id(), line!()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("a temporary directory");
    p
}

#[cfg(unix)]
fn make_executable(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(p)
        .expect("the copied stub exists")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(p, perms).expect("the copied stub is executable");
}

#[cfg(not(unix))]
fn make_executable(_p: &Path) {}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(p: &Path) -> bool {
    p.exists()
}
