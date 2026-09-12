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

/// **The three shell files of the two demos, with each one's floor for the
/// guarded-line self-check.** Task 31 split Task 28's single script into the
/// twelve steps (`demo-steps.sh`, sourced by both drivers) and two drivers; a
/// lint that kept reading only `laptop-demo.sh` would, after that split, be
/// reading a six-line file and passing vacuously.
///
/// The floors are lower bounds on what the tokeniser must SEE, not counts:
/// `demo-steps.sh` runs `kubectl` forty-odd times plus `docker`, `curl`, `just`
/// and `logweir`; `kind-demo.sh`'s three pre-steps run six `kubectl` and one
/// `docker`; `laptop-demo.sh` is a driver and runs none, which is the point of
/// it being a driver.
const DEMO_SCRIPTS: &[(&str, usize)] = &[
    ("scripts/demo-steps.sh", 30),
    ("scripts/kind-demo.sh", 6),
    ("scripts/laptop-demo.sh", 0),
];

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
    // ALL THREE FILES (Task 31). The twelve steps live in `demo-steps.sh`; the
    // two drivers and `kind-demo.sh`'s three pre-steps run `docker` and
    // `kubectl` of their own, and a pipe there would be exactly as blind.
    for (path, floor) in DEMO_SCRIPTS {
        let script = read(path);

        // THE SELF-CHECK FIRST. A lint over a file it found no guarded lines
        // in passes vacuously, and would keep passing if every `kubectl` were
        // respelt `"$KUBECTL"`. The number is a floor: the walk runs `kubectl`
        // more than thirty times, plus `docker`, `curl`, `just` and `logweir`;
        // the pre-steps of `kind-demo.sh` run six `kubectl` and one `docker`.
        let guarded = exit_code_lint::logical_lines(&script)
            .into_iter()
            .filter(exit_code_lint::LogicalLine::is_guarded)
            .count();
        assert!(
            guarded >= *floor,
            "the tokeniser found only {guarded} guarded line(s) in {path}, and {floor} is the \
             floor — a lint that sees nothing cannot fail. Are the tools still invoked as the \
             bare words `kubectl`, `curl`, `docker`, `just` and `logweir`?"
        );

        exit_code_lint::assert_no_masked_exit_code(path, &script);
    }
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
    // THE SPELLING IS NOW THE VARIABLE, AND THE VARIABLE IS ALWAYS PASSED
    // (Task 31). STANDING RULE 12 is about `kubectl` never being run against
    // an unnamed context, and a parameter that is always present on the
    // command line satisfies it exactly as a literal did — while a literal
    // would have forced a second copy of the twelve steps for the `kind`
    // cluster, which is the defect `the_two_demo_scripts_share_their_steps`
    // exists to prevent. The two arms below are what keeps the variable from
    // being a hole: every `kubectl` passes it, and each driver SETS it to a
    // named cluster.
    let mut offenders: Vec<String> = Vec::new();
    let mut seen = 0usize;
    for (path, _) in DEMO_SCRIPTS {
        let script = read(path);
        for (i, line) in script.lines().enumerate() {
            if line.split_whitespace().next() != Some("kubectl") {
                continue;
            }
            seen += 1;
            if !line.contains(r#"--context "$LOGWEIR_KUBE_CONTEXT""#) {
                offenders.push(format!("{path} line {}: {}", i + 1, line.trim()));
            }
        }
    }
    assert!(
        seen >= 25,
        "only {seen} line(s) across {} begin with `kubectl` — this lint would pass vacuously \
         over a script that spelt the tool some other way",
        DEMO_SCRIPTS
            .iter()
            .map(|(p, _)| *p)
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        offenders.is_empty(),
        "{} `kubectl` line(s) do not name the context (STANDING RULE 12 — `kubectl proxy` \
         included, and it is the one command in this product that hands a browser a cluster \
         credential). Every one reads `kubectl --context \"$LOGWEIR_KUBE_CONTEXT\" …`:\n{}",
        offenders.len(),
        offenders.join("\n")
    );

    // AND EACH DRIVER SETS IT, TO A CLUSTER IT NAMES. A variable that no
    // driver assigned would default silently and STANDING RULE 12 would be
    // satisfied on paper by a `kubectl` aimed at whatever the kubeconfig's
    // current context happened to be.
    for (driver, context) in [
        ("scripts/laptop-demo.sh", "docker-desktop"),
        ("scripts/kind-demo.sh", "kind-logweir"),
    ] {
        let src = read(driver);
        let assignment = format!("LOGWEIR_KUBE_CONTEXT={context}");
        assert!(
            src.contains(&assignment),
            "{driver} must set `{assignment}`. The steps are shared, so the DRIVER is the only \
             place that says which cluster a run is about — and it is what both scripts print \
             as their first line of output"
        );
    }

    // NEITHER DRIVER MAY RUN THE OTHER'S CLUSTER. `kind` is a CI-only cluster
    // (STANDING RULE 16) and the laptop walk is the docker-desktop record;
    // a driver that named both would make a transcript unreadable.
    assert!(
        !read("scripts/kind-demo.sh").contains("docker-desktop"),
        "scripts/kind-demo.sh names the docker-desktop context. It owns exactly one cluster, \
         the `kind` one the workflow created (STANDING RULE 16)"
    );
    assert!(
        !read("scripts/laptop-demo.sh").contains("kind-logweir"),
        "scripts/laptop-demo.sh names the kind cluster. `kind` is CI-only (STANDING RULE 16)"
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
    let forbidden = concat!("local", "host:9092");
    // THE LITERAL, IN BOTH DEMOS (Task 31). `kind-demo.sh` resolves the NAME
    // inside the cluster with a CoreDNS `hosts` block instead of substituting
    // an address, precisely so this string stays what spec §2 says it is.
    for path in ["scripts/demo-steps.sh", "scripts/kind-demo.sh"] {
        let script = read(path);
        assert!(
            script.contains("host.docker.internal:9095"),
            "{path} must point at the published K8S listener `host.docker.internal:9095` \
             (Task 7, STANDING RULE 15). A Kafka client is redirected by the broker's metadata \
             response to the ADVERTISED listener whatever address it bootstrapped at, so an \
             address substituted here would fix the first packet and nothing after it"
        );
        assert!(
            !script.contains(forbidden),
            "{path} names the host-side harness listener. A runner pod that bootstraps there \
             reaches its OWN loopback, and step 7 times out waiting for `status.reachable`"
        );
    }
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
    let script = read("scripts/demo-steps.sh");
    let lines = exit_code_lint::logical_lines(&script);

    // Step 11 runs on the host, with the CLI, against the approver's own key.
    let approve = lines
        .iter()
        .find(|l| l.first_word == "logweir" && l.code.contains("drill approve"))
        .unwrap_or_else(|| {
            panic!(
                "scripts/demo-steps.sh runs no `logweir drill approve` line. Step 11 mints the \
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
                "line {}: a `curl` in scripts/demo-steps.sh posts `{path}`. Nothing in this \
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
    // ALL THREE DECLARE BASH; the two DRIVERS are executable and
    // `demo-steps.sh` is not, because it is sourced and never executed — a
    // mode bit is the cheapest way to say so, and a `./scripts/demo-steps.sh`
    // would run the twelve steps against whatever context happened to be set.
    for path in [
        "scripts/laptop-demo.sh",
        "scripts/kind-demo.sh",
        "scripts/demo-steps.sh",
    ] {
        let body = read(path);
        assert!(
            body.starts_with("#!/usr/bin/env bash"),
            "{path} declares bash; got {:?}",
            body.lines().next()
        );
    }
    for path in ["scripts/laptop-demo.sh", "scripts/kind-demo.sh"] {
        assert!(
            is_executable(&root().join(path)),
            "{path} must be executable — `just laptop-demo` runs it as `./scripts/laptop-demo.sh`, \
             and `.github/workflows/kind-demo.yml` runs the other under `bash`"
        );
    }
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

// ---------------------------------------------------------------------------
// TASK 31 — THE TWO DEMOS SHARE ONE SET OF STEPS, AND THE `kind` DRIVER'S
// THREE PRE-STEPS
// ---------------------------------------------------------------------------

/// **Twelve steps, defined once.**
///
/// Spec §16 clause 2 asks for Demo 1 — *the* Demo 1, the one
/// `e2e/k8s/laptop-demo.md` records — to run in CI. A second copy of the steps
/// in `kind-demo.sh` would satisfy every other assertion in this file while the
/// two walks drifted apart, and the checklist would go on claiming CI runs the
/// walk the transcript proves. So both drivers SOURCE `scripts/demo-steps.sh`
/// and neither defines a step of its own.
///
/// KILLS: copying the twelve steps into `kind-demo.sh` (or back into
/// `laptop-demo.sh`) instead of sourcing them — the step-function definition
/// is what this finds, so a copy that renamed nothing is caught, and a copy
/// that renamed the functions no longer runs the same walk under any name.
#[test]
fn the_two_demo_scripts_share_their_steps() {
    const DRIVERS: [&str; 2] = ["scripts/laptop-demo.sh", "scripts/kind-demo.sh"];
    const STEPS: &str = "scripts/demo-steps.sh";

    // THE STEPS ARE WHERE THEY SAY THEY ARE. A floor, so a `demo-steps.sh`
    // that had been emptied could not make the rest of this test pass
    // vacuously: the walk is twelve numbered steps, step 10 having two halves.
    let steps_src = read(STEPS);
    let defined = step_definitions(&steps_src);
    assert_eq!(
        13,
        defined.len(),
        "{STEPS} defines {} step function(s), and the walk has thirteen — `step_01` … \
         `step_09`, `step_10a`, `step_10b`, `step_11`, `step_12`, which are the twelve numbered \
         steps with step 10 split into its scripted and in-browser halves. Found: {defined:?}",
        defined.len()
    );
    assert!(
        steps_src.contains("demo_run()"),
        "{STEPS} must define `demo_run` — the ONE place the twelve steps are invoked, so there \
         is one ordering of the walk in the tree and not one per driver"
    );

    for driver in DRIVERS {
        let src = read(driver);
        assert!(
            src.contains(". scripts/demo-steps.sh") || src.contains("source scripts/demo-steps.sh"),
            "{driver} must source `scripts/demo-steps.sh`. The two demos run the SAME twelve \
             steps — that is the whole content of spec §16 clause 2's claim that CI runs Demo 1"
        );
        assert!(
            src.contains("demo_run"),
            "{driver} sources the steps and never runs them: it must call `demo_run`"
        );
        let own = step_definitions(&src);
        assert!(
            own.is_empty(),
            "{driver} defines its own step function(s) {own:?}. A driver sets the cluster and \
             sources the steps; a step defined here is a second walk that nothing keeps in step \
             with the recorded one"
        );
    }
}

/// Every `step_NN…() {` (or `step_NN…()  {`) defined at the top level of a
/// shell file, in file order. A definition, never a call: the call sites in
/// `demo_run` are indented and carry no parentheses.
fn step_definitions(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in src.lines() {
        let Some(rest) = line.strip_prefix("step_") else {
            continue;
        };
        let Some(name) = rest.split("()").next() else {
            continue;
        };
        if rest.contains("()") && !name.is_empty() {
            out.push(format!("step_{name}"));
        }
    }
    out
}

/// **A gateway that did not resolve stops the run, and there is no fallback.**
///
/// `kind-demo.sh` reads the kind network's IPAM gateway and gives it to CoreDNS
/// as the address of `host.docker.internal`. A `localhost` fallback would be
/// worse than no fallback: inside a pod `localhost` is the pod, so the demo
/// would proceed, the runner would dial itself, and the failure would surface
/// eleven steps later as a `Backup` that never finished.
///
/// NO CLUSTER AND NO DAEMON. A `docker` shim whose `network inspect` prints an
/// empty string and exits 0 is on `$PATH`; the script gets as far as the
/// assertion and no further. `kubectl` is not stubbed at all, because a correct
/// script never reaches one.
///
/// KILLS: falling back to `localhost` — or to the host-side harness listener,
/// or to any address at all — when the gateway resolution comes back empty; the
/// stubbed run then exits 0, or dies later with a message about something else,
/// where 1 is required here. (The forbidden address is not spelt out in this
/// file: `no_network_in_unit_tests.rs` greps every `.rs` under `crates/**` for
/// it and fails naming the file, which is why
/// `laptop_demo_uses_the_published_k8s_listener` builds it with `concat!`.)
#[test]
fn kind_demo_asserts_a_non_empty_bootstrap_address() {
    // THE SHIM, AND WHY IT IS WRITTEN HERE RATHER THAN CHECKED IN. It exists
    // for one assertion, it is four lines, and a reader of that assertion has
    // to know what `network inspect` answered for the exit code to mean
    // anything. `fixtures/stub-kubectl` is checked in because three tests and
    // a plan rule refer to it by name; this one has no second reader.
    const STUB_DOCKER: &str = "#!/bin/sh\n\
        # A STUB `docker` THAT DIALS NOTHING (Task 31, written by\n\
        # `kind_demo_asserts_a_non_empty_bootstrap_address`). `network inspect`\n\
        # succeeds and prints NOTHING, which is what a kind network that does not\n\
        # exist looks like through `-f '{{(index .IPAM.Config 0).Gateway}}'`.\n\
        case \"$1\" in\n\
        \x20 network) exit 0 ;;\n\
        esac\n\
        echo \"stub-docker: refusing \\`docker $*\\` — this stub answers only \\`network inspect\\`\" >&2\n\
        exit 1\n";

    let root = root();
    let bin = tempdir("logweir-t31-stub-docker");
    let installed = bin.join("docker");
    std::fs::write(&installed, STUB_DOCKER).expect("the stub docker is written onto $PATH");
    make_executable(&installed);

    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg("scripts/kind-demo.sh")
        .current_dir(&root)
        .env("PATH", path)
        .env("LOGWEIR_DEMO_NONINTERACTIVE", "1")
        .output()
        .expect("bash runs scripts/kind-demo.sh");

    let code = out.status.code();
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    let _ = std::fs::remove_dir_all(&bin);

    assert_eq!(
        code,
        Some(1),
        "scripts/kind-demo.sh must exit 1 when the kind network's gateway comes back empty; it \
         exited {code:?}. A run that continued would dial the POD's own loopback and fail \
         eleven steps later with a message about a `Backup`\n--- stdout ---\n{stdout}\n\
         --- stderr ---\n{stderr}"
    );
    assert!(
        stderr.contains("docker network inspect"),
        "the refusal must name what it tried, so a reader knows which command to run by hand. \
         Expected the `docker network inspect` line in stderr\n--- stdout ---\n{stdout}\n\
         --- stderr ---\n{stderr}"
    );
    // AND IT SAYS WHY THERE IS NO FALLBACK, because the next person to read
    // this failure is the person tempted to add one.
    assert!(
        stderr.contains("localhost is the pod"),
        "the refusal must say why it does not fall back\n--- stderr ---\n{stderr}"
    );
    assert!(
        !stdout.contains("1/12 preflight"),
        "the run reached step 1 despite an unresolved gateway\n--- stdout ---\n{stdout}"
    );
}

/// **DNS first, then the probe, then the walk — and the ConfigMap apply is two
/// commands.**
///
/// The order is the whole mechanism. CoreDNS has to answer for
/// `host.docker.internal` before anything dials it; the probe (interface
/// register I14) is what turns "CoreDNS was patched" into "a pod reached the
/// broker"; and only then may step 1 run, because every later step's failure
/// mode looks like something else.
///
/// The second clause is STANDING RULE 20 in the one place it is easiest to
/// break: `kubectl create configmap … --dry-run=client -o yaml | kubectl apply
/// -f -` is the idiom everybody writes, and it reports `kubectl apply`'s status
/// while swallowing the render's. `exit_code_lint` sees the pipe as one logical
/// line beginning with `kubectl`, so this arm states the shape the render must
/// have instead: a file in between.
///
/// KILLS: deleting the CoreDNS patch and passing the computed gateway as
/// `--bootstrap` (H10(a) — the client would be redirected to an unresolvable
/// advertised name, and a lint catches it before a runner minute is spent);
/// moving the probe after the first step; piping the ConfigMap render into
/// `apply`.
#[test]
fn kind_demo_patches_coredns_before_the_first_step() {
    let src = read("scripts/kind-demo.sh");

    // OFFSETS OF COMMANDS, NEVER OF COMMENTS. This file's header quotes the
    // pipe it refuses (`… -o yaml | kubectl apply -f -`) so that the next
    // person to write it knows why not, and a naive `str::find` would then
    // fail the test on the sentence that prevents the defect.
    let at = |needle: &str| -> usize {
        let mut offset = 0usize;
        for line in src.split_inclusive('\n') {
            if !line.trim_start().starts_with('#') && line.contains(needle) {
                return offset;
            }
            offset += line.len();
        }
        panic!("no COMMAND line of scripts/kind-demo.sh contains `{needle}`")
    };

    let gateway = at("docker network inspect");
    let rollout = at("rollout status deployment/coredns --timeout=120s");
    let probe = at("run bootstrap-probe");
    let walk = at("demo_run");

    assert!(
        gateway < rollout,
        "the gateway must be resolved before CoreDNS is told what to answer with"
    );
    assert!(
        rollout < probe,
        "the CoreDNS `rollout status` (offset {rollout}) must precede the `bootstrap-probe` \
         (offset {probe}): a probe run while the old CoreDNS pods are still serving proves \
         nothing about the patch"
    );
    assert!(
        probe < walk,
        "the `bootstrap-probe` (offset {probe}) must precede the first invocation of the \
         sourced steps (offset {walk}). `reachable=true` is step 1's precondition, and a walk \
         that started without it fails at step 7 with a message about a `Backup`"
    );

    // THE HOSTS BLOCK IS THE MECHANISM, AND `fallthrough` IS WHAT KEEPS THE
    // REST OF CLUSTER DNS WORKING.
    assert!(
        src.contains("host.docker.internal") && src.contains("fallthrough"),
        "the CoreDNS patch must add a `hosts` block for `host.docker.internal` carrying \
         `fallthrough`; without it the `hosts` plugin answers NXDOMAIN for every name it does \
         not hold and `kubernetes.default` stops resolving"
    );

    // THE BOOTSTRAP STRING IS NOT SUBSTITUTED. This is the positive form of
    // the mutant "pass the computed gateway as --bootstrap": the probe's
    // address is the literal spec §2 names.
    let probe_line = src[probe..]
        .lines()
        .next()
        .expect("the bootstrap-probe line is a line")
        .trim();
    assert!(
        probe_line.contains("cluster-probe --bootstrap"),
        "the probe must run `cluster-probe --bootstrap …` (interface register I14, Task 15c). \
         `logweir doctor` is not used: it makes `--allowed-clusters` and `--approver-key` \
         mandatory and hard-codes Plaintext:\n    {probe_line}"
    );
    assert!(
        !probe_line.contains("$gw"),
        "the probe passes the resolved gateway ADDRESS as `--bootstrap`. That fixes the first \
         packet and nothing after it: the broker's metadata response redirects the client to \
         the ADVERTISED listener, which STANDING RULE 15 fixes at `host.docker.internal:9095`. \
         The name is what has to resolve, and the CoreDNS block above is what makes it:\n    \
         {probe_line}"
    );

    // TWO COMMANDS, NOT A PIPE (STANDING RULE 20).
    let render = at("--dry-run=client -o yaml");
    let render_line = src[render..]
        .lines()
        .next()
        .expect("the render line is a line")
        .trim();
    assert!(
        render_line.contains('>') && !render_line.contains('|'),
        "the ConfigMap render must REDIRECT to a file and must not be piped into `apply`: a \
         pipe reports `kubectl apply`'s status and swallows the render's (STANDING RULE 20). \
         Found:\n    {render_line}"
    );
    let applied = at("apply -f \"$KIND_OUT/coredns-configmap.yaml\"");
    assert!(
        render < applied,
        "the rendered ConfigMap must be applied from the file the render wrote"
    );
}

/// **The `kind` driver walks through its own first step — by execution, with
/// the real `demo-steps.sh`.**
///
/// This is the test the review's blocker was found in the absence of. Task 31's
/// dry proof ran `kind-demo.sh`'s three pre-steps for real but replaced the
/// twelve steps with tracers in a COPY of `demo-steps.sh`, so the real
/// `step_01` never ran under this driver — and the real `step_01` compared the
/// kubeconfig's current context against the LITERAL `docker-desktop`, which
/// refuses every cluster that is not the laptop's. `kubectl config
/// current-context` prints the kubeconfig's `current-context` FIELD and ignores
/// `--context` (`fixtures/stub-kubectl`'s header says so), so parameterising
/// the flag did not parameterise the comparison: the workflow's demo step would
/// have died at 1/12 on every run with
///
/// ```text
/// refusing: current context is kind-logweir, not docker-desktop
/// ```
///
/// So this runs the REAL `scripts/kind-demo.sh` over the REAL
/// `scripts/demo-steps.sh` — both copied UNCHANGED into a temporary tree, no
/// tracers, nothing rewritten — against stubs that answer the three pre-steps'
/// reads and the probe, and asserts that the walk gets PAST the context check
/// and dies at the next precondition it cannot satisfy (the compose stack,
/// which the stub `docker` refuses). The refusal string is asserted ABSENT.
///
/// KILLS: comparing the current context against the literal `docker-desktop`
/// (or any literal that is not this driver's cluster); the probe moved after
/// step 1; a pre-step whose exit code stopped being read.
///
/// GLOBAL CONSTRAINT 22: every tool the walk reaches for is a stub first on
/// `$PATH` — `kubectl`, `docker`, and the four `command -v` names step 1 wants
/// on a host that may have none of them. Nothing dials, nothing is created, and
/// the temporary tree is the only thing written.
#[test]
fn kind_demo_passes_its_first_step_on_its_own_context() {
    // A `kubectl` THAT DIALS NOTHING and answers exactly what the three
    // pre-steps and step 1's context read ask of it. Every other argument
    // vector is a refusal that names itself — the `fixtures/stub-kubectl`
    // idiom, so a walk that wandered off this path says where it went.
    const STUB_KUBECTL: &str = r#"#!/bin/sh
echo "kubectl $*" >> "$STUB_LOG"
case "$*" in
  *"config current-context"*)
    echo kind-logweir
    exit 0 ;;
  *"get configmap coredns"*)
    printf '%s\n' '.:53 {' '    errors' '    forward . /etc/resolv.conf' '}'
    exit 0 ;;
  *"--dry-run=client -o yaml"*)
    printf '%s\n' 'apiVersion: v1' 'kind: ConfigMap' 'metadata:' '  name: coredns'
    exit 0 ;;
  *"apply -f"*) exit 0 ;;
  *"rollout restart"*) exit 0 ;;
  *"rollout status"*) exit 0 ;;
  *"run bootstrap-probe"*)
    echo cluster-id=stub
    echo reachable=true
    exit 0 ;;
esac
echo "stub-kubectl: refusing \`kubectl $*\` — this stub answers the kind driver's three pre-steps and step 1's context read, and dials nothing." >&2
exit 1
"#;

    // A `docker` that answers ONE IPv4 gateway and refuses the rest, naming
    // what it refused. The refusal is load-bearing twice over: it is what
    // makes `docker compose ps` fail, which is where this walk is expected to
    // stop, and it is what proves the walk got that far.
    const STUB_DOCKER: &str = r#"#!/bin/sh
echo "docker $*" >> "$STUB_LOG"
case "$1" in
  network)
    echo 172.30.0.1
    exit 0 ;;
esac
echo "stub-docker: refusing \`docker $*\` — this stub answers only \`network inspect\` (one IPv4 gateway) and dials nothing." >&2
exit 1
"#;

    let root = root();
    let tmp = tempdir("logweir-t31-kind-first-step");
    let bin = tmp.join("bin");
    let scripts = tmp.join("scripts");
    std::fs::create_dir_all(&bin).expect("a stub bin/ directory");
    std::fs::create_dir_all(&scripts).expect("a scripts/ directory in the temporary tree");

    // THE TWO SCRIPTS, COPIED UNCHANGED. `kind-demo.sh` starts with `cd
    // "$(dirname "$0")/.."`, so the temporary tree is the root of this run and
    // nothing is written into the repository.
    for name in ["kind-demo.sh", "demo-steps.sh"] {
        std::fs::copy(root.join("scripts").join(name), scripts.join(name))
            .unwrap_or_else(|e| panic!("scripts/{name} is copied verbatim: {e}"));
    }

    std::fs::write(bin.join("kubectl"), STUB_KUBECTL).expect("the stub kubectl is written");
    std::fs::write(bin.join("docker"), STUB_DOCKER).expect("the stub docker is written");
    make_executable(&bin.join("kubectl"));
    make_executable(&bin.join("docker"));

    // THE FOUR NAMES STEP 1 ONLY LOOKS FOR. `command -v` is all it does with
    // them, so a stub that refuses if it is ever RUN is both enough and
    // honest, and it makes this test deterministic on a host that has no
    // `just`, no `node` and no `openssl`.
    for tool in ["just", "openssl", "node", "python3"] {
        let p = bin.join(tool);
        std::fs::write(
            &p,
            format!(
                "#!/bin/sh\necho \"{tool} $*\" >> \"$STUB_LOG\"\necho \"stub-{tool}: refusing \\`{tool} $*\\` — this stub exists so \\`command -v\\` finds the tool; step 1 stops at the compose precondition before any of these runs.\" >&2\nexit 1\n"
            ),
        )
        .unwrap_or_else(|e| panic!("the stub {tool} is written: {e}"));
        make_executable(&p);
    }

    let log = tmp.join("argv.log");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg(scripts.join("kind-demo.sh"))
        .current_dir(&tmp)
        .env("PATH", &path)
        .env("STUB_LOG", &log)
        .env("LOGWEIR_PYTHON", bin.join("python3"))
        .env("LOGWEIR_DEMO_NONINTERACTIVE", "1")
        .env_remove("LOGWEIR_KUBE_CONTEXT")
        .env_remove("LOGWEIR_DEMO_ONLY_STEP")
        .env_remove("KUBECONFIG")
        .output()
        .expect("bash runs the copied scripts/kind-demo.sh");

    let code = out.status.code();
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let argv = std::fs::read_to_string(&log).unwrap_or_default();

    let _ = std::fs::remove_dir_all(&tmp);

    let seen = format!("--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}\n--- argv ---\n{argv}");

    // THE DRIVER SAYS WHICH CLUSTER THIS RUN IS ABOUT, FIRST.
    assert!(
        stdout
            .lines()
            .next()
            .is_some_and(|l| l.contains("kind-demo: kubectl context kind-logweir")),
        "the driver's first line of output names its own cluster\n{seen}"
    );

    // THE PRE-STEPS RAN, IN THEIR ORDER, AND THE PROBE CAME LAST.
    let rollout = argv
        .lines()
        .position(|l| l.contains("rollout status deployment/coredns"))
        .unwrap_or_else(|| panic!("the CoreDNS `rollout status` never reached kubectl\n{seen}"));
    let probe = argv
        .lines()
        .position(|l| l.contains("run bootstrap-probe"))
        .unwrap_or_else(|| panic!("the bootstrap probe never reached kubectl\n{seen}"));
    assert!(
        rollout < probe,
        "the probe (argv line {probe}) must follow the CoreDNS rollout (argv line {rollout})\n\
         {seen}"
    );

    // STEP 1 RAN, READ THE CONTEXT, AND ACCEPTED IT.
    assert!(
        stdout.contains("1/12 preflight"),
        "the walk must reach step 1 of the twelve\n{seen}"
    );
    assert!(
        stdout.contains("(kubectl config current-context) -> kind-logweir"),
        "step 1 must print the context it read, and it is this driver's\n{seen}"
    );
    assert!(
        !stdout.contains("refusing: current context")
            && !stderr.contains("refusing: current context"),
        "step 1 refused the cluster this driver exists to run on. The comparison is against \
         `$LOGWEIR_KUBE_CONTEXT`, not a literal: `kubectl config current-context` prints the \
         kubeconfig's `current-context` field and ignores `--context`, so a literal here \
         refuses every cluster but one and `.github/workflows/kind-demo.yml` can never go \
         green\n{seen}"
    );

    // AND IT WALKED ON, TO THE NEXT PRECONDITION IT CANNOT SATISFY HERE.
    assert!(
        stdout.contains("rc=1  (docker compose ps --status running)"),
        "past the context check, step 1's next precondition is the compose stack, and the stub \
         `docker` refuses it — that refusal is the evidence the context check was passed\n{seen}"
    );
    assert!(
        stderr.contains("kind-demo: `docker compose ps` exited 1"),
        "the refusal must be attributed to the driver that ran (`kind-demo:`) and must name the \
         command that failed\n{seen}"
    );
    assert_eq!(
        code,
        Some(1),
        "the walk must exit 1 at the compose precondition; it exited {code:?}\n{seen}"
    );
}
