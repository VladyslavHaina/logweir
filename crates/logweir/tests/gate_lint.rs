//! **The gate-recipe closeout.** Task 32, the last task of the tag-1 plan.
//!
//! Every check this repository has must be reachable by ONE local command or it
//! is not enforced. "Wired into `ci.yml`" is still not the gate here, even now
//! that `ci.yml` HAS executed (green on 2026-09-12): a workflow reports after a
//! push and `just gate` reports before one, `release.yml` has never run at all,
//! and `docs/tag1-checklist.md` clause 4 records that as
//! `blocked: no tag pushed`. `just gate` is the enforcement point, and this
//! file is what keeps it honest.
//!
//! # The enumeration, and why it is a union rather than a single set
//!
//! The tree holds **sixteen** `scripts/check-*.sh`. Fourteen of them run in
//! `just gate` — thirteen named on its own lines and one, `check-chart.sh`,
//! through the `just chart-check` line (Task 35; [`checks_in_gate`] follows a
//! `just <recipe>` line of the gate one level into that recipe's body, and
//! nothing further). The other two — `check-image.sh` and `check-image-weirkeeper.sh`
//! — need a **Docker daemon** and cost a `docker run` per check, and `just gate`
//! must stay runnable on a machine that has neither a daemon nor a cluster. A
//! test that asserted "every `check-*.sh` is invoked by `just gate`" could
//! therefore never pass; asserting it anyway is how a repository ends up with
//! either a gate nobody can run or a test nobody believes.
//!
//! So the property is a **partition**: every `scripts/check-*.sh` is invoked by
//! `just gate` **or** by a recipe named in `docs/gates.md`'s stack/cluster
//! table; the two sets are **disjoint**; and together they are **exhaustive**.
//! A new `scripts/check-new.sh` in neither fails. A script moved from one set
//! to the other without its row moving fails. Adding `check-image.sh` to the
//! gate fails on disjointness — and would also turn `just gate` red on any
//! machine without a daemon.
//!
//! # `mod support;`
//!
//! Two helpers are reused rather than re-derived (controller ruling, Task 12):
//! `support::exit_code_lint` is interface **I29**, the one shell tokeniser for
//! STANDING RULE 20, and this file writes no second one;
//! `support::dial_tokens` is Task 32's array-literal reader, shared with
//! `crates/logweir/tests/notify.rs`.
//!
//! # Why the shell fixtures below spawn no cargo
//!
//! Global Constraint 22 bounds every `#[test]` at 15 s, and
//! `crates/logweir-core/tests/fixture_regen.rs` stays the only test in the
//! workspace that spawns a nested cargo. The `time_unit_suite_*` and `msrv_*`
//! fixtures run `scripts/time-unit-suite.sh`, which shells `cargo` three times
//! — so each fixture builds a **throwaway tree** with a **stub `cargo` (and,
//! for the MSRV rows, a stub `rustup`) first on `$PATH`**. No real cargo runs,
//! no package-cache lock is taken, nothing is compiled, and the assertions are
//! about the harness's own control flow, which is what they were always about.
//! The one fixture that must exceed a budget does it with `sleep`, not with
//! work.

mod support;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use support::dial_tokens;
use support::exit_code_lint::{self, Finding};

// ---------------------------------------------------------------- plumbing

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root resolves")
}

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn justfile() -> String {
    read("justfile")
}

/// A recipe's header line: `name:`, `name: dep`, `name arg="x":`.
fn is_recipe_header(line: &str, name: &str) -> bool {
    if line.starts_with(char::is_whitespace) {
        return false;
    }
    let Some(rest) = line.strip_prefix(name) else {
        return false;
    };
    matches!(rest.chars().next(), Some(':') | Some(' ')) && rest.contains(':')
}

/// The recipes `name` depends on, i.e. the words after the `:` on its header.
fn recipe_deps(just: &str, name: &str) -> Vec<String> {
    let header = just
        .lines()
        .find(|l| is_recipe_header(l, name))
        .unwrap_or_else(|| panic!("the justfile must declare a `{name}` recipe"));
    let after = header
        .split_once(':')
        .expect("a recipe header has a colon")
        .1;
    after
        .split_whitespace()
        .filter(|w| !w.starts_with('#'))
        .map(|w| w.to_string())
        .collect()
}

/// A recipe's body: the indented lines under its header, comments and blanks
/// removed. Comment text is not code, and a gate named only in a comment is not
/// a gate — the defect `cli_verify.rs::the_verifier_parity_script_is_wired_into_lint_and_ci`
/// already guards against elsewhere.
fn recipe_body(just: &str, name: &str) -> Vec<String> {
    let mut lines = just.lines();
    lines
        .by_ref()
        .find(|l| is_recipe_header(l, name))
        .unwrap_or_else(|| panic!("the justfile must declare a `{name}` recipe"));
    lines
        .take_while(|l| l.starts_with(' ') || l.starts_with('\t') || l.trim().is_empty())
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("#!"))
        .map(|l| l.to_string())
        .collect()
}

/// `just gate`'s executed lines, in order.
fn gate_lines() -> Vec<String> {
    recipe_body(&justfile(), "gate")
}

/// The `check-*.sh` the gate runs: those named on its own executed lines, plus
/// those named in the body of any `just <recipe>` line it runs — ONE level, the
/// recipe's own executed lines and nothing it in turn invokes. Task 35 put
/// `check-chart.sh` behind `just chart-check`, beside `just crds-check` and
/// `just schema-check`; before that every check the gate ran was named on a
/// gate line, and a reader of `checks_named_in(gate_lines())` was complete by
/// accident. A `just <recipe>` line IS a gate line — `just` runs the recipe
/// and reads its status — so the scripts that recipe names are the gate's.
fn checks_in_gate() -> BTreeSet<String> {
    let just = justfile();
    let lines = gate_lines();
    let mut out = checks_named_in(&lines.join("\n"));
    for line in &lines {
        let mut words = line.split_whitespace();
        if words.next() != Some("just") {
            continue;
        }
        if let Some(name) = words.next() {
            out.extend(checks_named_in(&recipe_body(&just, name).join("\n")));
        }
    }
    out
}

/// Every `scripts/check-*.sh` that exists in the tree.
fn every_check_script() -> BTreeSet<String> {
    let dir = repo_root().join("scripts");
    let mut out = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("scripts/ is readable") {
        let name = entry.expect("a readable entry").file_name();
        let name = name.to_string_lossy().to_string();
        if name.starts_with("check-") && name.ends_with(".sh") {
            out.insert(name);
        }
    }
    assert!(
        out.len() >= 10,
        "only {} check-*.sh found under scripts/ — this enumeration is reading the wrong \
         directory, and an enumeration over nothing cannot fail",
        out.len()
    );
    out
}

/// The `check-*.sh` scripts named on the executed lines of `text`.
fn checks_named_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for name in every_check_script() {
        if text.contains(&format!("scripts/{name}")) {
            out.insert(name);
        }
    }
    out
}

/// The `check-*.sh` a `just` recipe invokes: its own executed lines, the
/// executed lines of the recipes it depends on, and the bodies of the
/// `scripts/*.sh` those lines run.
///
/// **It does NOT follow `just <other-recipe>` out of a shell script.**
/// `scripts/k8s-demo.sh` runs `just lint` at its step 2, which transitively
/// reaches most of the gate set; following that edge would make "invoked by"
/// mean "could eventually cause to run", under which no two recipes are
/// disjoint and the partition below says nothing. "Invokes" here means the
/// script is named on a line that runs it, which is what a reader of
/// `docs/gates.md`'s last column needs to be true.
fn checks_invoked_by_recipe(just: &str, name: &str) -> BTreeSet<String> {
    let mut text = recipe_body(just, name).join("\n");
    for dep in recipe_deps(just, name) {
        text.push('\n');
        text.push_str(&recipe_body(just, &dep).join("\n"));
    }
    let mut out = checks_named_in(&text);
    // One level into the shell scripts the recipe runs.
    for shell in scripts_named_in(&text) {
        let body = read(&format!("scripts/{shell}"));
        let executed: String = body
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<&str>>()
            .join("\n");
        out.extend(checks_named_in(&executed));
    }
    out
}

/// Every `scripts/<x>.sh` named in `text`.
fn scripts_named_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let dir = repo_root().join("scripts");
    for entry in std::fs::read_dir(&dir).expect("scripts/ is readable") {
        let name = entry.expect("a readable entry").file_name();
        let name = name.to_string_lossy().to_string();
        if name.ends_with(".sh") && text.contains(&format!("scripts/{name}")) {
            out.insert(name);
        }
    }
    out
}

// ------------------------------------------------- docs/gates.md, parsed

/// The heading that opens the stack/cluster table. A literal, so moving the
/// table without moving this constant fails loudly rather than silently
/// yielding an empty set.
const STACK_TABLE_HEADING: &str = "## The stack/cluster table";

/// The recipe names in `docs/gates.md`'s stack/cluster table, in file order.
fn stack_table_recipes() -> Vec<String> {
    let md = read("docs/gates.md");
    let start = md.find(STACK_TABLE_HEADING).unwrap_or_else(|| {
        panic!("docs/gates.md must carry a `{STACK_TABLE_HEADING}` section — it is the other half of the gate enumeration")
    });
    let rest = &md[start + STACK_TABLE_HEADING.len()..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    let mut out = Vec::new();
    for line in rest[..end].lines() {
        let line = line.trim();
        if !line.starts_with('|') {
            continue;
        }
        let first = line
            .trim_matches('|')
            .split('|')
            .next()
            .unwrap_or("")
            .trim();
        if let Some(name) = first
            .strip_prefix("`just ")
            .and_then(|r| r.strip_suffix('`'))
        {
            out.push(name.trim().to_string());
        }
    }
    assert!(
        !out.is_empty(),
        "the stack/cluster table in docs/gates.md names no `just <recipe>` in its first column — \
         a table nothing parses is a table nothing enforces"
    );
    out
}

/// The whole row text for each stack/cluster recipe, so a row can be asked
/// what it says.
fn stack_table_rows() -> Vec<(String, String)> {
    let md = read("docs/gates.md");
    let start = md
        .find(STACK_TABLE_HEADING)
        .expect("the stack/cluster heading");
    let rest = &md[start..];
    let end = rest[STACK_TABLE_HEADING.len()..]
        .find("\n## ")
        .map(|i| i + STACK_TABLE_HEADING.len())
        .unwrap_or(rest.len());
    let mut out = Vec::new();
    for line in rest[..end].lines() {
        let t = line.trim();
        if !t.starts_with('|') {
            continue;
        }
        let first = t.trim_matches('|').split('|').next().unwrap_or("").trim();
        if let Some(name) = first
            .strip_prefix("`just ")
            .and_then(|r| r.strip_suffix('`'))
        {
            out.push((name.trim().to_string(), t.to_string()));
        }
    }
    out
}

// =========================================================== the eight gate tests

/// **H8's enumeration.** Every `scripts/check-*.sh` is invoked by `just gate`
/// OR by a recipe named in `docs/gates.md`'s stack/cluster table; the two sets
/// are disjoint; and together they are exhaustive.
#[test]
fn gate_lint_every_check_is_in_the_gate() {
    let just = justfile();
    let all = every_check_script();

    let in_gate = checks_in_gate();

    let mut in_table: BTreeSet<String> = BTreeSet::new();
    for recipe in stack_table_recipes() {
        in_table.extend(checks_invoked_by_recipe(&just, &recipe));
    }

    // 1. DISJOINT. A script in both is a script whose cost nobody can state:
    //    `just gate` claims to need no daemon and the table claims the opposite.
    let both: Vec<&String> = in_gate.intersection(&in_table).collect();
    assert!(
        both.is_empty(),
        "these check scripts are invoked by BOTH `just gate` and a stack/cluster recipe: {both:?}\n  \
         The two sets must be disjoint. `check-image.sh` and `check-image-weirkeeper.sh` need a \
         Docker daemon and cost a `docker run` per check, which is why they live in `just smoke` / \
         `just smoke-weirkeeper` and in docs/gates.md's table — putting one in the gate makes \
         `just gate` red on every machine without a daemon."
    );

    // 2. EXHAUSTIVE. A new check in neither set is a check nothing runs.
    let covered: BTreeSet<String> = in_gate.union(&in_table).cloned().collect();
    let orphaned: Vec<&String> = all.difference(&covered).collect();
    assert!(
        orphaned.is_empty(),
        "these check scripts are invoked by NEITHER `just gate` NOR any recipe named in \
         docs/gates.md's stack/cluster table: {orphaned:?}\n  \
         A guard nothing runs is documentation. Put it in `just gate` if it needs no daemon, \
         cluster or compose stack; otherwise give it a recipe and a row in that table."
    );

    // 3. NOTHING INVENTED. A name in `covered` that is not a file in the tree
    //    would mean the gate runs a script that does not exist.
    let ghosts: Vec<&String> = covered.difference(&all).collect();
    assert!(
        ghosts.is_empty(),
        "the gate or the table names check scripts that are not in scripts/: {ghosts:?}"
    );

    // 4. The shape, stated so a red is legible.
    assert_eq!(
        in_gate.len() + in_table.len(),
        all.len(),
        "gate {} + table {} != {} scripts in the tree.\n  gate:  {in_gate:?}\n  table: {in_table:?}",
        in_gate.len(),
        in_table.len(),
        all.len()
    );
}

/// **STANDING RULE 20 over the justfile and every file under `scripts/`**,
/// through interface **I29** — Task 12's tokeniser, reused, not re-derived.
///
/// # What is asserted, and what deliberately is not
///
/// [`Finding::PipedStatus`] is asserted over the whole corpus: `a | b` REPLACES
/// `a`'s status with `b`'s, so the exit code the rule is about is gone before
/// anything can read it. That is the defect, and it is a defect everywhere.
///
/// [`Finding::UnreadStatus`] — "a guarded line is not followed by a line reading
/// `$?`" — is **not** asserted over this corpus, and the reason is that in these
/// two places the status IS read, by something the tokeniser cannot see:
///
/// * in a **justfile**, `just` runs each recipe line in its own shell, reads its
///   status and aborts on the first non-zero one. `docker build …` on its own
///   line in `image:` has its exit code read by `just` itself; requiring a
///   `rc=$?` line after it would be requiring a second, weaker reader.
/// * in four shell scripts that predate I29 — `check-image.sh`,
///   `check-image-weirkeeper.sh`, `extract-engine.sh`, `render-install.sh` — the
///   guarded line continues across a `\` into `|| fail …`, which reads the
///   status directly and branches on it. `||` is not a pipe and the tokeniser
///   says so; what it cannot say is that the branch IS the read.
///
/// Asserting the half that does not apply would have produced a red on an
/// unmodified tree, and the governing rule for a gate is that if it is not green
/// against the unmodified tree the gate is wrong, not the tree. The half that is
/// asserted is the one the mutant tests: change a recipe to `cmd | tee log` and
/// this fails at assertion time.
#[test]
fn gate_lint_no_masked_exit_codes() {
    let mut corpus: Vec<(String, String)> = vec![("justfile".to_string(), justfile())];
    for entry in std::fs::read_dir(repo_root().join("scripts")).expect("scripts/ is readable") {
        let entry = entry.expect("a readable entry");
        if !entry.file_type().expect("a readable file type").is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let rel = format!("scripts/{name}");
        corpus.push((rel.clone(), read(&rel)));
    }
    assert!(
        corpus.len() >= 20,
        "only {} files in the corpus — this lint is reading the wrong directory",
        corpus.len()
    );

    // THE SELF-CHECK FIRST. A lint over a corpus it found no guarded lines in
    // passes vacuously, and would keep passing if every tool were spelled
    // `"$DOCKER"`.
    let guarded: usize = corpus
        .iter()
        .map(|(_, text)| {
            exit_code_lint::logical_lines(text)
                .into_iter()
                .filter(exit_code_lint::LogicalLine::is_guarded)
                .count()
        })
        .sum();
    assert!(
        guarded >= 50,
        "the tokeniser found only {guarded} guarded line(s) across the justfile and scripts/ — \
         a lint that sees nothing cannot fail. Are `docker`, `kubectl`, `just`, `curl` and \
         `logweir` still invoked as bare first words?"
    );

    let mut piped: Vec<String> = Vec::new();
    for (label, text) in &corpus {
        for f in exit_code_lint::findings(text) {
            if matches!(f, Finding::PipedStatus { .. }) {
                piped.push(format!("{label}: {f}"));
            }
        }
    }
    assert!(
        piped.is_empty(),
        "{} line(s) send a load-bearing exit code through a pipe (STANDING RULE 20, Global \
         Constraint 11). The pipe's status is reported, not the command's.\n\n{}",
        piped.len(),
        piped.join("\n\n")
    );
}

/// **`ci.yml` mirrors the gate, and says what has actually executed.**
///
/// Both halves matter. The mirror keeps the workflow from quietly covering less
/// than the local gate; the comment keeps `ci.yml` honest about which workflows
/// have run and which have not.
///
/// # Why part 2 reads the checklist in BOTH states
///
/// Until 2026-09-12 this test pinned one sentence — "none — no remote exists" —
/// which was true and which made the test useless the moment it stopped being
/// true: the first green run would have turned it red with nothing to say about
/// what the comment should have become. So it now holds the comment to the
/// checklist in either state, per row:
///
///   * a row that reads `closed` must carry an `actions/runs/<id>` URL, and the
///     comment must name that row's workflow WITH that run id in the same
///     entry, under the executed heading;
///   * a row that reads `blocked: <reason>` must have its workflow named under
///     the never-executed heading, never under the executed one, and its entry
///     there must carry the row's own reason.
///
/// KILLS: (i) a comment that names a run id other than the one the row carries;
/// (ii) row 4 flipped to `closed` with the comment left alone — the row then
/// carries no run URL at all, and `release.yml` is still filed as never run;
/// (iii) a comment that claims `release.yml` executed, by moving it above the
/// never-executed heading.
///
/// The sentence-joining idiom stays: comment markers stripped and the lines
/// joined by a space, because a comment that wraps is still one sentence and a
/// reader of `ci.yml` sees it as one.
#[test]
fn gate_lint_ci_mirrors_the_gate() {
    let ci = read(".github/workflows/ci.yml");
    let executed: String = ci
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");

    // 1. Every check script the gate runs appears on an executed line of ci.yml
    //    — the ones behind a `just <recipe>` gate line included (Task 35).
    let in_gate = checks_in_gate();
    let missing: Vec<&String> = in_gate
        .iter()
        .filter(|s| !executed.contains(&format!("scripts/{s}")))
        .collect();
    assert!(
        missing.is_empty(),
        "these scripts run in `just gate` but appear on no executed line of \
         .github/workflows/ci.yml: {missing:?}\n  ci.yml is the documentation half and it must \
         document the whole set; a comment naming a script does not count."
    );

    // 2. The executed-workflows comment, written from the runs themselves and
    //    naming the same state the checklist does — in EITHER state.
    let checklist = read("docs/tag1-checklist.md");
    // The two rows whose subject is a workflow, and the workflow each is about.
    const ROW_WORKFLOWS: [(&str, &str); 2] = [("| 2 |", "kind-demo.yml"), ("| 4 |", "release.yml")];

    // The sentence is read as a SENTENCE: comment markers stripped and the
    // lines joined by a space, because a comment that wraps is still one
    // sentence and a reader of ci.yml sees it as one.
    let comments: String = ci
        .lines()
        .filter(|l| l.trim_start().starts_with('#'))
        .map(|l| l.trim_start().trim_start_matches('#').trim())
        .collect::<Vec<&str>>()
        .join(" ");

    // The comment has two halves and says which is which. Everything before
    // the never-executed heading is a claim that a workflow RAN; everything
    // after it is a claim that one did not.
    const NEVER: &str = "NEVER EXECUTED";
    let split_at = comments.find(NEVER).unwrap_or_else(|| {
        panic!(
            "ci.yml's comment carries no `{NEVER}` heading. The comment is read as two halves \
             — what has run, and what has not — and without the heading a workflow that never \
             ran cannot be told from one that did."
        )
    });
    let (executed_half, never_half) = comments.split_at(split_at);

    for (prefix, workflow) in ROW_WORKFLOWS {
        let row = checklist
            .lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("docs/tag1-checklist.md must carry a row `{prefix}`"));
        let status = row
            .split('|')
            .nth(3)
            .unwrap_or("")
            .trim()
            .trim_matches('`')
            .trim();

        if status == "closed" {
            // A closed row about a workflow is closed BY A RUN, and says which.
            let ids = run_ids(row);
            assert!(
                !ids.is_empty(),
                "row `{prefix}` of docs/tag1-checklist.md reads `closed` and carries no \
                 `actions/runs/<id>` URL. A clause about a workflow is closed by a run, and \
                 the row is where the run is named.\n{row}"
            );
            let entry = entry_for(executed_half, workflow).unwrap_or_else(|| {
                panic!(
                    "row `{prefix}` reads `closed`, so `{workflow}` ran — and ci.yml's comment \
                     does not name it above the `{NEVER}` heading. The comment and the \
                     checklist may not disagree about what has executed."
                )
            });
            for id in &ids {
                assert!(
                    entry.contains(id.as_str()),
                    "row `{prefix}` names run {id}, and ci.yml's `{workflow}` entry does not. \
                     A comment that names a different run than the row is worse than one that \
                     names none.\n\nthe entry, verbatim:\n{entry}"
                );
            }
        } else if let Some(reason) = status.strip_prefix("blocked: ") {
            assert!(
                entry_for(executed_half, workflow).is_none(),
                "row `{prefix}` reads `{status}`, and ci.yml's comment names `{workflow}` \
                 above the `{NEVER}` heading — i.e. as a workflow that has run. One of the \
                 two is wrong and this test does not guess which."
            );
            let entry = entry_for(never_half, workflow).unwrap_or_else(|| {
                panic!(
                    "row `{prefix}` reads `{status}`, so `{workflow}` has not run — and \
                     ci.yml's comment does not say so under its `{NEVER}` heading."
                )
            });
            assert!(
                entry.contains(reason),
                "row `{prefix}` gives the reason `{reason}`, and ci.yml's `{workflow}` entry \
                 under `{NEVER}` does not carry it. The workflow's absence and the reason for \
                 it are one fact.\n\nthe entry, verbatim:\n{entry}"
            );
        } else {
            panic!(
                "row `{prefix}` of docs/tag1-checklist.md reads `{status}`; this test reads \
                 `closed` and `blocked: <reason>` and nothing else."
            );
        }
    }
}

/// The workflow files this repository ships. Longest first is not needed — no
/// name is a substring of another — but the set is what makes an "entry"
/// bounded.
const WORKFLOW_FILES: [&str; 7] = [
    "ci.yml",
    "no-oso.yml",
    "kind-demo.yml",
    "helm-demo.yml",
    "release.yml",
    "release-drill.yml",
    "engine-matrix.yml",
];

/// The slice of `text` that belongs to `workflow`: from the first mention of
/// its name to the next mention of any OTHER workflow. A comment written as one
/// entry per workflow is read as one entry per workflow, so a run id filed
/// under the wrong name does not count as filed under the right one.
fn entry_for<'a>(text: &'a str, workflow: &str) -> Option<&'a str> {
    let start = text.find(workflow)?;
    let after = start + workflow.len();
    let end = WORKFLOW_FILES
        .iter()
        .filter(|w| **w != workflow)
        .filter_map(|w| text[after..].find(w).map(|at| after + at))
        .min()
        .unwrap_or(text.len());
    Some(&text[start..end])
}

/// Every `actions/runs/<id>` id in `line`, in order. A run URL is how a row
/// about a workflow names the run that closed it.
fn run_ids(line: &str) -> Vec<String> {
    const MARK: &str = "actions/runs/";
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find(MARK) {
        rest = &rest[at + MARK.len()..];
        let id: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if !id.is_empty() {
            out.push(id);
        }
    }
    out
}

/// **The eight stack/cluster recipes are out of the gate, and all eight are in
/// the table.**
///
/// Each needs the compose stack, a cluster or a Docker build, and
/// `scripts/time-unit-suite.sh` REFUSES to run, exit 1, while 9092 or 9000
/// answers — so a gate that started the stack would fail itself. `helm-demo`
/// (Task 35) needs a cluster with the chart installed.
#[test]
fn gate_lint_the_stack_recipes_are_out_of_the_gate() {
    const STACK: [&str; 8] = [
        "e2e",
        "smoke",
        "smoke-weirkeeper",
        "mvp-demo",
        "k8s-demo",
        "laptop-demo",
        "pitr",
        "helm-demo",
    ];
    let just = justfile();
    let gate = gate_lines();

    for name in STACK {
        // It exists. A table row for a recipe nobody wrote is the same defect
        // as a gate line for a script nobody wrote.
        assert!(
            just.lines().any(|l| is_recipe_header(l, name)),
            "docs/gates.md's stack/cluster table names `just {name}`, which the justfile does \
             not declare"
        );
        // The gate does not run it.
        let invoked = gate.iter().any(|l| {
            l.split_whitespace()
                .take(2)
                .eq(["just", name].iter().copied())
        });
        assert!(
            !invoked,
            "`just gate` invokes `just {name}`. That recipe needs the compose stack, a cluster \
             or a Docker build, and ./scripts/time-unit-suite.sh refuses to run, exit 1, while \
             9092 or 9000 answers — so the gate would fail itself."
        );
    }

    // All eight, and only those eight, are in the table.
    let table = stack_table_recipes();
    let want: BTreeSet<String> = STACK.iter().map(|s| s.to_string()).collect();
    let got: BTreeSet<String> = table.iter().cloned().collect();
    assert_eq!(
        got, want,
        "docs/gates.md's stack/cluster table must name exactly these eight recipes"
    );

    // Each row says what it proves — a row that is only a name teaches nobody
    // why the recipe is outside the gate.
    for (name, row) in stack_table_rows() {
        let cells: Vec<&str> = row.trim_matches('|').split('|').map(str::trim).collect();
        assert!(
            cells.len() >= 4,
            "the stack/cluster row for `just {name}` has {} cells; it must carry the recipe, \
             what it proves, what it needs, and which check-*.sh it invokes:\n{row}",
            cells.len()
        );
        assert!(
            cells[1].len() > 20,
            "the `what it proves` cell for `just {name}` is {} characters — say what it proves:\n{row}",
            cells[1].len()
        );
    }
}

/// **`just chart-check` is in the gate, immediately after `just crds-check`,
/// and it is `scripts/check-chart.sh`.** Task 35.
///
/// The position is the ruling's: the chart is derived from `config/` the way
/// the CRDs are derived from the emitter, so its drift check sits beside the
/// CRD drift check. The recipe's body is asserted too — a `chart-check` whose
/// body ran something else would satisfy the enumeration test through
/// [`checks_in_gate`] while running no chart check at all.
///
/// KILLS: removing `just chart-check` from the gate (this AND
/// `gate_lint_every_check_is_in_the_gate`, which then finds `check-chart.sh`
/// orphaned); moving it above `just crds-check`; pointing the recipe at some
/// other script.
#[test]
fn gate_lint_chart_check_follows_crds_check() {
    let gate = gate_lines();
    let at = |line: &str| -> usize {
        gate.iter()
            .position(|l| l == line)
            .unwrap_or_else(|| panic!("`just gate` has no line `{line}`:\n  {gate:#?}"))
    };
    let crds = at("just crds-check");
    let chart = at("just chart-check");
    assert_eq!(
        chart,
        crds + 1,
        "`just chart-check` (line {}) must come IMMEDIATELY after `just crds-check` (line {}): \
         the chart's CRDs are byte copies of config/crd, so the chart drift check follows the \
         CRD drift check",
        chart + 1,
        crds + 1
    );
    let body = recipe_body(&justfile(), "chart-check");
    assert_eq!(
        body,
        vec!["bash scripts/check-chart.sh".to_string()],
        "the `chart-check` recipe runs `bash scripts/check-chart.sh` and nothing else"
    );
    assert!(
        checks_in_gate().contains("check-chart.sh"),
        "check-chart.sh must be counted as in the gate through the `just chart-check` line"
    );
}

/// **I32 is verified here, never rewritten** (Task 1's to close; Task 32 only
/// certifies that it is closed).
#[test]
fn gate_lint_the_engine_allowlists_are_two_constants() {
    let src = read("scripts/check-no-oso.sh");

    assert!(
        src.contains("ENGINE_RUNTIME_ALLOWLIST=\"backup restore validate-restore validation\""),
        "scripts/check-no-oso.sh must carry ENGINE_RUNTIME_ALLOWLIST verbatim — it is the GC3 \
         CONTRACT, the four subcommand tokens, and the failure text quotes it"
    );
    assert!(
        src.contains("ENGINE_ARGV_ALLOWLIST="),
        "scripts/check-no-oso.sh must carry a SEPARATE ENGINE_ARGV_ALLOWLIST. Two constants, not \
         one: the argv allowlist additionally admits `run` and the furniture every invocation \
         carries (`--config`, `--format`, `json`), and merging them would let a contract token \
         be confused with a flag."
    );
    assert!(
        src.contains("ROOT=\"${LOGWEIR_ROOT:-$(cd \"$(dirname \"$0\")/..\" && pwd)}\""),
        "scripts/check-no-oso.sh must resolve its root through LOGWEIR_ROOT, so a test can point \
         it at a temp-workspace overlay"
    );
    assert!(
        src.contains("if [ -z \"${LOGWEIR_ROOT:-}\" ]; then"),
        "the two cargo checks and the linkage grep must be enclosed in `if [ -z \
         \"${{LOGWEIR_ROOT:-}}\" ]` — they take the package-cache lock, and a #[test] that takes \
         it can hang behind a concurrent build (Global Constraint 22)"
    );
}

// ================================================ the derived primitive set

/// A throwaway workspace overlay, so the signer gate can be run against a tree
/// this test controls. Deleted on drop.
struct Overlay {
    path: PathBuf,
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap_or_else(|e| panic!("mkdir {}: {e}", to.display()));
    for entry in std::fs::read_dir(from).unwrap_or_else(|e| panic!("read {}: {e}", from.display()))
    {
        let entry = entry.expect("a readable directory entry");
        let src = entry.path();
        let dst = to.join(entry.file_name());
        let ty = entry.file_type().expect("a readable file type");
        if ty.is_dir() {
            copy_tree(&src, &dst);
        } else if ty.is_file() {
            std::fs::copy(&src, &dst).unwrap_or_else(|e| panic!("copy {}: {e}", src.display()));
        }
    }
}

impl Overlay {
    fn new(tag: &str) -> Overlay {
        let root = repo_root();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "logweir-gate-lint-{tag}-{}-{stamp}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("e2e")).expect("the overlay directory is creatable");
        for f in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
            std::fs::copy(root.join(f), path.join(f)).unwrap_or_else(|e| panic!("copy {f}: {e}"));
        }
        for d in [".cargo", "crates", "xtask"] {
            copy_tree(&root.join(d), &path.join(d));
        }
        std::fs::copy(root.join("e2e/Cargo.toml"), path.join("e2e/Cargo.toml"))
            .expect("copy e2e/Cargo.toml");
        copy_tree(&root.join("e2e/src"), &path.join("e2e/src"));
        copy_tree(&root.join("e2e/tests"), &path.join("e2e/tests"));
        std::fs::create_dir_all(path.join("scripts")).expect("scripts/ is creatable");
        for f in [
            "scripts/check-one-signer.sh",
            "scripts/logweir-evidence-primitives.classify",
        ] {
            std::fs::copy(root.join(f), path.join(f)).unwrap_or_else(|e| panic!("copy {f}: {e}"));
        }
        Overlay { path }
    }

    /// Give `logweir-evidence` a fourth path dependency — a crate nobody has
    /// classified.
    fn add_unclassified_dependency(&self, name: &str) {
        let probe = self.path.join("crates").join(name);
        std::fs::create_dir_all(probe.join("src")).expect("the probe directory is creatable");
        std::fs::write(
            probe.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 publish = false\n\n[dependencies]\n"
            ),
        )
        .expect("the probe manifest is writable");
        std::fs::write(probe.join("src/lib.rs"), "").expect("the probe lib.rs is writable");

        let manifest = self.path.join("crates/logweir-evidence/Cargo.toml");
        let text = std::fs::read_to_string(&manifest).expect("the evidence manifest is readable");
        let patched = text.replacen(
            "[dependencies]",
            &format!("[dependencies]\n{name} = {{ path = \"../{name}\" }}"),
            1,
        );
        assert_ne!(
            text, patched,
            "the evidence manifest must have [dependencies]"
        );
        std::fs::write(&manifest, patched).expect("the evidence manifest is writable");
    }

    fn classify(&self, text: &str) {
        let p = self
            .path
            .join("scripts/logweir-evidence-primitives.classify");
        let mut body = std::fs::read_to_string(&p).expect("the classification is readable");
        body.push('\n');
        body.push_str(text);
        body.push('\n');
        std::fs::write(&p, body).expect("the classification is writable");
    }

    fn run_signer_gate(&self) -> Output {
        Command::new("bash")
            .arg("scripts/check-one-signer.sh")
            .current_dir(&self.path)
            .env("LOGWEIR_ROOT", &self.path)
            .output()
            .expect("the signer gate runs")
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// **The `PRIMITIVES` literal is gone and the set comes from the graph.**
///
/// The overlay gives `logweir-evidence` a fourth, unclassified path dependency.
/// Fail-closed means the gate treats it as a signing primitive and exits **1**
/// naming it — so a third signing crate is a diff a reviewer reads, not a silent
/// pass. Restoring `PRIMITIVES="p256 ed25519-dalek"` makes this test fail at
/// assertion time: the overlay's fourth crate is no longer named and the gate
/// exits 0 where 1 is required.
///
/// `cargo metadata --no-deps` over a path-only overlay performs no resolution
/// and reaches no network.
#[test]
fn one_signer_primitives_come_from_the_graph() {
    // The literal is gone from the source, stated separately so its return is
    // named as itself rather than diagnosed from a rc.
    let src = read("scripts/check-one-signer.sh");
    // CODE, not prose: the header explains why the literal is gone and quotes
    // it to do so, and a substring search over the whole file would read that
    // explanation as the defect it describes.
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        !code.contains("PRIMITIVES=\"p256 ed25519-dalek\""),
        "scripts/check-one-signer.sh has gone back to a fixed PRIMITIVES literal. A third signing \
         primitive added to logweir-evidence would then pass this gate in silence, which is the \
         carried defect Task 32 closed."
    );
    assert!(
        code.contains("PRIMITIVES_CLASSIFY=\"scripts/logweir-evidence-primitives.classify\""),
        "check 2 must derive its primitive set from the graph minus the checked-in classification"
    );

    let ov = Overlay::new("primitives");
    ov.add_unclassified_dependency("zz-probe-primitive");
    let out = ov.run_signer_gate();
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));

    assert_eq!(
        out.status.code(),
        Some(1),
        "the gate must exit 1 on an unclassified direct dependency of logweir-evidence; it exited \
         {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("zz-probe-primitive"),
        "the failure must NAME the unclassified crate — an exit code that does not say which \
         crate costs a bisect:\n{combined}"
    );
}

/// **The classification's own format is load-bearing: an entry without a reason
/// is refused.**
///
/// The mutant: classify the overlay's fourth crate as `non-primitive` and leave
/// the reason out. The gate must still exit 1 — a classification whose reason
/// nobody wrote is a widening nobody reviewed.
#[test]
fn one_signer_refuses_a_classification_without_a_reason() {
    let ov = Overlay::new("noreason");
    ov.add_unclassified_dependency("zz-probe-noreason");
    ov.classify("zz-probe-noreason non-primitive");

    let out = ov.run_signer_gate();
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_eq!(
        out.status.code(),
        Some(1),
        "an entry with no `reason:` line must be refused; the gate exited {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("reason"),
        "the refusal must say the reason line is what is missing:\n{combined}"
    );

    // And with a reason, the same classification is accepted — otherwise this
    // test would pass against a gate that refuses every classification.
    let ov2 = Overlay::new("withreason");
    ov2.add_unclassified_dependency("zz-probe-withreason");
    ov2.classify("zz-probe-withreason non-primitive\nreason: a probe crate; it signs nothing.");
    let out2 = ov2.run_signer_gate();
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert!(
        !combined2.contains("is NOT classified"),
        "a classified crate with a reason must be accepted:\n{combined2}"
    );
}

/// **The parity gate names the pinned toolchain before it builds.**
///
/// A source-reading test: the `RUSTUP_TOOLCHAIN` export precedes the build line,
/// and the build line is `cargo build --release -p logweir`. The execution half
/// is recorded once in the task report rather than run here — a release build
/// exceeds Global Constraint 22's 15 s per-test bound, and
/// `crates/logweir-core/tests/fixture_regen.rs` stays the only test in the
/// workspace that spawns a nested cargo.
#[test]
fn parity_gate_names_the_pinned_toolchain_before_building() {
    let src = read("scripts/check-verifier-parity.sh");

    let build = src
        .find("cargo build --release -p logweir")
        .expect("the parity gate must BUILD the binary it needs rather than refusing");
    let export = src
        .find("export RUSTUP_TOOLCHAIN=")
        .expect("the parity gate must export the pinned toolchain");
    assert!(
        export < build,
        "the RUSTUP_TOOLCHAIN export must precede the cargo build. A cargo reached through \
         rustup's shim with no override in scope resolves rustup's DEFAULT channel and SYNCS IT \
         FROM THE NETWORK, from inside a lint gate (STANDING RULE 7, Global Constraint 17)."
    );
    assert!(
        src[..export].contains("rust-toolchain.toml"),
        "the exported pin must be read from rust-toolchain.toml's `channel`, not invented"
    );
    // The interpreter chain above it is untouched, in its original order.
    let chain = [
        "if [ -n \"${LOGWEIR_PYTHON:-}\" ]; then",
        "elif [ -n \"${LOGWEIR_E2E_PYTHON:-}\" ]; then",
        "elif [ -x \"$ROOT/.e2e/venv/bin/python3\" ]; then",
    ];
    let mut at = 0usize;
    for step in chain {
        let i = src[at..]
            .find(step)
            .unwrap_or_else(|| panic!("the interpreter chain must still read `{step}`"));
        at += i + step.len();
    }
    assert!(
        at < export,
        "the interpreter chain must stay above the build block — it is untouched by this change"
    );
}

/// **The tokeniser, not the slice, is doing the work.**
///
/// Two fixtures over a copy of `no_network_in_unit_tests.rs`'s text: one where a
/// REAL token is moved into a `//` comment inside the array (the tokeniser must
/// not see it, so the agreement fails), and one where a harmless extra token
/// appears in a comment while all six real entries remain (it passes).
///
/// The first fixture is the mutant the old substring search survived.
#[test]
fn the_ureq_token_lists_agree_under_a_comment() {
    let audit = read("crates/logweir/tests/no_network_in_unit_tests.rs");
    let notify = read("crates/logweir/tests/notify.rs");

    // The six, read from notify.rs's own literal rather than restated here — a
    // second copy of the list is the defect this whole pair of tests is about.
    let forbidden = dial_tokens::array_elements(&notify, "const FORBIDDEN");
    assert_eq!(
        forbidden.len(),
        6,
        "notify.rs's FORBIDDEN must still be the six ureq entry points; parsed {forbidden:?}"
    );

    // The unmodified tree agrees.
    let base = dial_tokens::array_elements(&audit, "const DIAL_TOKENS");
    let missing: Vec<&String> = forbidden.iter().filter(|t| !base.contains(*t)).collect();
    assert!(
        missing.is_empty(),
        "the unmodified audit is already narrower than FORBIDDEN: {missing:?}"
    );

    // FIXTURE 1 — a REAL token moved into a `//` comment inside the array.
    //
    // The victim is taken from the parsed set rather than written out here.
    // Two reasons, and both are load-bearing: a literal copy of a dial token in
    // this file would have to be allow-listed in the very audit it is testing
    // (`no_network_in_unit_tests.rs` walks `crates/logweir/tests/`), and a
    // hardcoded victim goes stale the day the list changes while still passing.
    let victim = forbidden
        .iter()
        .next()
        .expect("FORBIDDEN is not empty")
        .clone();
    let quoted = format!("\"{victim}\",");
    assert!(
        audit.contains(&quoted),
        "the audit must still carry {quoted} as an element for this fixture to move it"
    );
    let commented = audit.replacen(&quoted, &format!("// moved here: {quoted}"), 1);
    let seen = dial_tokens::array_elements(&commented, "const DIAL_TOKENS");
    assert!(
        !seen.contains(&victim),
        "a token that appears only inside a `//` comment must NOT be read as an array element — \
         that is exactly what the old substring search could not tell apart, and it is the \
         mutant this test exists to kill. Moved {victim:?}; parsed: {seen:?}"
    );

    // FIXTURE 2 — an extra token in a comment, all six real entries intact.
    let extra = "a-token-that-is-only-prose";
    let decorated = audit.replacen(
        "const DIAL_TOKENS",
        &format!("// see also \"{extra}\" — not an element, just prose\nconst DIAL_TOKENS"),
        1,
    );
    let kept = dial_tokens::array_elements(&decorated, "const DIAL_TOKENS");
    let still_missing: Vec<&String> = forbidden.iter().filter(|t| !kept.contains(*t)).collect();
    assert!(
        still_missing.is_empty(),
        "a comment beside the array must not remove elements from it: {still_missing:?}"
    );
    assert!(
        !kept.contains(extra),
        "a token mentioned only in a comment must not become an element either: {kept:?}"
    );
}

// ============================================ the time-unit-suite fixtures

/// A throwaway tree that `scripts/time-unit-suite.sh` can be run in, with a
/// stub `cargo` (and optionally a stub `rustup`) first on `$PATH` — so the
/// harness's control flow is exercised without compiling anything.
struct SuiteFixture {
    path: PathBuf,
}

impl Drop for SuiteFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl SuiteFixture {
    fn new(tag: &str) -> SuiteFixture {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("logweir-tus-{tag}-{}-{stamp}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        for d in ["scripts", "bin", "tmp"] {
            std::fs::create_dir_all(path.join(d)).expect("the fixture directory is creatable");
        }
        std::fs::copy(
            repo_root().join("scripts/time-unit-suite.sh"),
            path.join("scripts/time-unit-suite.sh"),
        )
        .expect("copy the harness into the fixture");
        std::fs::write(
            path.join("Cargo.toml"),
            "[workspace.package]\nrust-version = \"1.89\"\n",
        )
        .expect("the fixture manifest is writable");
        let fixture = SuiteFixture { path };
        fixture.stub_nc();
        fixture
    }

    /// A stub `nc` that answers "closed" for every port.
    ///
    /// The harness refuses to time the suite while the broker port or the MinIO
    /// port answers on the loopback address (M10), and it asks `nc` — the real one, first on the host's
    /// `PATH`. These fixtures exercise the harness's RED PATHS, not the host:
    /// run under `just e2e` or `ci.yml`'s `e2e` job, where the compose stack is
    /// up by design, the real `nc` made all three red-path fixtures fail on the
    /// refusal instead (the fourth CI run, 2026-09-12). With the stub the
    /// fixtures are stack-agnostic; the refusal stays real for every real run,
    /// because a real run has no fixture `bin/` on its `PATH`.
    fn stub_nc(&self) {
        self.stub("nc", "#!/bin/sh\nexit 1\n");
    }

    fn with_toolchain_pin(self) -> SuiteFixture {
        std::fs::copy(
            repo_root().join("rust-toolchain.toml"),
            self.path.join("rust-toolchain.toml"),
        )
        .expect("copy rust-toolchain.toml into the fixture");
        self
    }

    fn stub(&self, name: &str, body: &str) {
        let p = self.path.join("bin").join(name);
        std::fs::write(&p, body).expect("the stub is writable");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755))
                .expect("the stub is executable");
        }
    }

    /// A stub `mktemp` that honours `$TMPDIR` even with no template.
    ///
    /// **Without this the log-leak check below is vacuous**, and that was
    /// measured rather than assumed: BSD `mktemp` (this is macOS) ignores
    /// `$TMPDIR` unless it is given a template or `-t`, and puts a bare
    /// `mktemp`'s file in `confstr(_CS_DARWIN_USER_TEMP_DIR)` instead — so a
    /// before/after listing of the fixture's own `$TMPDIR` stayed empty whether
    /// the harness removed its log or leaked it. A check that cannot fail is
    /// this build's signature defect; the stub is how this one can.
    fn stub_mktemp(&self) {
        let witness = self.path.join("mktemp-witness");
        self.stub(
            "mktemp",
            &format!(
                "#!/bin/sh\n\
                 d=\"${{TMPDIR:-/tmp}}\"\n\
                 if [ \"${{1:-}}\" = -d ]; then p=\"$d/tmpd.$$\"; mkdir -p \"$p\"; \
                 echo \"$p\" >> {w}; echo \"$p\"; exit 0; fi\n\
                 i=0\n\
                 while :; do p=\"$d/tmp.$$.$i\"; [ -e \"$p\" ] || break; i=$((i+1)); done\n\
                 : > \"$p\"\n\
                 echo \"$p\" >> {w}\n\
                 echo \"$p\"\n",
                w = witness.display()
            ),
        );
    }

    /// Every path the stub `mktemp` handed out, in order.
    fn mktemp_witness(&self) -> Vec<PathBuf> {
        let p = self.path.join("mktemp-witness");
        std::fs::read_to_string(&p)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(PathBuf::from)
            .collect()
    }

    /// A stub `cargo`. `suite_rc` is what `cargo test --workspace` exits with;
    /// `slow` makes it take about two seconds, which is how a fixture exceeds a
    /// one-second budget without doing any work.
    fn stub_cargo(&self, suite_rc: i32, slow: bool) {
        let sleep = if slow { "sleep 2" } else { ":" };
        self.stub(
            "cargo",
            &format!(
                "#!/bin/sh\n\
                 # A stub. No compiler, no package cache, no network.\n\
                 case \"$*\" in\n\
                 \x20 *--no-run*) exit 0 ;;\n\
                 \x20 *build*) exit 0 ;;\n\
                 \x20 *\"test --workspace\"*)\n\
                 \x20   echo \"thread 'probe' panicked at crates/logweir/tests/probe.rs:7:1:\"\n\
                 \x20   echo boom\n\
                 \x20   {sleep}\n\
                 \x20   exit {suite_rc} ;;\n\
                 esac\n\
                 exit 0\n"
            ),
        );
    }

    fn run(&self, env: &[(&str, &str)]) -> Output {
        let path_var = format!(
            "{}:{}",
            self.path.join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut cmd = Command::new("bash");
        cmd.arg("scripts/time-unit-suite.sh")
            .current_dir(&self.path)
            .env("PATH", path_var)
            .env("TMPDIR", self.path.join("tmp"))
            .env_remove("LOGWEIR_TIME_BUDGET_SECS")
            .env_remove("LOGWEIR_UNIT_SUITE_BUDGET_SECS")
            .env_remove("LOGWEIR_UNIT_TEST_BUDGET_SECS");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.output().expect("the harness runs")
    }
}

/// **Every red carries the load average.** This plan runs up to three agents at
/// `CARGO_BUILD_JOBS=4`, and the same tree has measured 20-35 s idle and
/// 105-129 s under two concurrent builds. Without this line a load-induced red
/// and a regression are the same transcript.
#[test]
fn time_unit_suite_prints_load_on_a_red() {
    let fx = SuiteFixture::new("load").with_toolchain_pin();
    fx.stub_cargo(101, false);
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_ne!(
        out.status.code(),
        Some(0),
        "the fixture must be red:\n{combined}"
    );
    assert!(
        combined.contains("load average"),
        "a red run must print the load average:\n{combined}"
    );
}

/// **`LOGWEIR_TIME_BUDGET_SECS` is an alias for both budgets, and the explicit
/// variables win.** The resolved values are printed, with where each came from.
#[test]
fn time_unit_suite_honours_the_time_budget_alias() {
    // The alias alone: both budgets are 1, and a two-second suite is over it.
    let fx = SuiteFixture::new("alias").with_toolchain_pin();
    fx.stub_cargo(0, true);
    let out = fx.run(&[("LOGWEIR_TIME_BUDGET_SECS", "1")]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_eq!(
        out.status.code(),
        Some(1),
        "with the alias at 1s a two-second suite must FAIL; it exited {:?}\n{combined}",
        out.status.code()
    );
    assert!(
        combined.contains("suite budget    1s") && combined.contains("per-test budget 1s"),
        "both resolved budgets must be printed as 1s:\n{combined}"
    );
    assert!(
        combined.contains("LOGWEIR_TIME_BUDGET_SECS (alias)"),
        "the printed budget must name where it came from:\n{combined}"
    );

    // The explicit variable wins over the alias.
    let fx2 = SuiteFixture::new("alias-override").with_toolchain_pin();
    fx2.stub_cargo(0, false);
    let out2 = fx2.run(&[
        ("LOGWEIR_TIME_BUDGET_SECS", "1"),
        ("LOGWEIR_UNIT_SUITE_BUDGET_SECS", "600"),
    ]);
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert!(
        combined2.contains("suite budget    600s (from LOGWEIR_UNIT_SUITE_BUDGET_SECS)"),
        "an explicit LOGWEIR_UNIT_SUITE_BUDGET_SECS must beat the alias:\n{combined2}"
    );
    assert!(
        combined2.contains("per-test budget 1s"),
        "the per-test budget must still come from the alias:\n{combined2}"
    );
}

/// **Item (b): the suite log is removed on EVERY path, including the red one.**
///
/// Source-reading — the trap naming `$suite_log` is installed before the
/// `$SUITE_CMD` line — plus a fixture red run that leaves no file behind in its
/// own `$TMPDIR`.
#[test]
fn time_unit_suite_removes_its_log_on_a_red() {
    let src = read("scripts/time-unit-suite.sh");
    let mktemp = src
        .find("suite_log=\"$(mktemp)\"")
        .expect("the harness must still capture the suite output to a temp file");
    let trap = src
        .find("trap 'rm -f \"$suite_log\"' EXIT")
        .expect("a trap naming $suite_log must be installed for it");
    let run = src
        .find("$SUITE_CMD > \"$suite_log\"")
        .expect("the harness must still run the suite into that file");
    assert!(
        mktemp < trap && trap < run,
        "the $suite_log trap must be installed immediately after the mktemp and BEFORE the suite \
         runs. Inside the `suite_rc -eq 0` branch it covers only the green path, and every red \
         run — the run an agent repeats most — leaks a temp file."
    );

    let fx = SuiteFixture::new("log").with_toolchain_pin();
    fx.stub_cargo(101, false);
    fx.stub_mktemp();
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert_ne!(
        out.status.code(),
        Some(0),
        "the fixture must be red:\n{combined}"
    );

    // THE SELF-CHECK FIRST: the stub must have been the `mktemp` the harness
    // reached, or the survivor check below compares two empty sets for ever.
    let handed_out = fx.mktemp_witness();
    assert!(
        !handed_out.is_empty(),
        "the stub mktemp was never called, so this test proved nothing about the log's \
         lifetime. Is `$PATH` still pointing at the fixture's bin/?\n{combined}"
    );
    let survivors: Vec<&PathBuf> = handed_out.iter().filter(|p| p.exists()).collect();
    assert!(
        survivors.is_empty(),
        "a red run leaked {} of the {} temp file(s) it created: {survivors:?}\n\
         Every red run is a run an agent repeats; a trap that only covers the green path \
         leaks one file per repetition.\n{combined}",
        survivors.len(),
        handed_out.len()
    );
}

/// **Item (b), second half: the suite's output is streamed once.** The verdict
/// prints a REFERENCE into the output it already produced, not a second copy of
/// part of it.
#[test]
fn time_unit_suite_streams_the_suite_once_on_a_red() {
    let src = read("scripts/time-unit-suite.sh");
    // CODE ONLY, for the same reason as above: the comment beside the verdict
    // explains that the log was already streamed by the `cat` line, and a
    // count over prose would read that sentence as a second stream.
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    let streams = code.matches("cat \"$suite_log\"").count();
    assert_eq!(
        streams, 1,
        "the suite log must be streamed exactly once; found {streams} `cat \"$suite_log\"`"
    );
    assert!(
        !code.contains("p \"$suite_log\""),
        "the verdict must not re-print slices of the log with `sed -n …p \"$suite_log\"` — the \
         same bytes appearing twice in one transcript is how a reader ends up debugging the echo"
    );
    assert!(
        !code.contains("tail -n 5 \"$suite_log\""),
        "the verdict must not re-print the tail of the log either"
    );
    assert!(
        code.contains("first panic at suite-log line"),
        "the verdict must print a REFERENCE — `first panic at suite-log line <N>: <site>`"
    );

    // And it actually prints one.
    let fx = SuiteFixture::new("stream").with_toolchain_pin();
    fx.stub_cargo(101, false);
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(
        combined.contains("first panic at suite-log line"),
        "a red run must name where the first panic is in the output it streamed:\n{combined}"
    );
    assert!(
        combined.contains("crates/logweir/tests/probe.rs:7:1"),
        "the reference must name the panic site the suite reported:\n{combined}"
    );
}

/// **Item (c): the MSRV fallback never exports a two-component channel.**
///
/// `Cargo.toml`'s `rust-version` is `"1.89"` — two components — and handing
/// rustup a channel it may not have installed makes rustup SYNC IT FROM THE
/// NETWORK, from inside a lint gate. The fallback resolves the highest
/// INSTALLED `1.89.x` instead, and exits 1 naming the floor when there is none.
#[test]
fn msrv_fallback_never_exports_a_two_component_toolchain() {
    let src = read("scripts/time-unit-suite.sh");
    let fallback_start = src
        .find("MSRV_FLOOR=")
        .expect("the fallback must read the MSRV floor from Cargo.toml");
    let fallback_end = src
        .find("export RUSTUP_TOOLCHAIN=")
        .expect("the harness must export the pin");
    let fallback = &src[fallback_start..fallback_end];
    assert!(
        fallback.contains("rustup toolchain list"),
        "the fallback must select from the toolchains rustup ALREADY HAS:\n{fallback}"
    );
    assert!(
        fallback.contains("\\.[0-9]+"),
        "the fallback must require a third component before exporting anything:\n{fallback}"
    );
    assert!(
        fallback.contains("exit 1"),
        "with no matching toolchain installed the fallback must exit 1 — a lint gate does not \
         install a toolchain:\n{fallback}"
    );
    assert!(
        !fallback.contains("PINNED_TOOLCHAIN=\"$MSRV_FLOOR\""),
        "the fallback must never export the two-component floor unfiltered:\n{fallback}"
    );

    // Fixture A: a 1.89.x is installed, and the highest one is chosen.
    let fx = SuiteFixture::new("msrv-ok");
    fx.stub_cargo(0, false);
    fx.stub(
        "rustup",
        "#!/bin/sh\nif [ \"$1\" = toolchain ] && [ \"$2\" = list ]; then\n\
         printf '%s\\n' '1.89.0-aarch64-apple-darwin (default)' '1.89.3-aarch64-apple-darwin' \
         '1.97.1-aarch64-apple-darwin'\nfi\nexit 0\n",
    );
    let out = fx.run(&[]);
    let combined = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(
        combined.contains("toolchain pinned to 1.89.3"),
        "the fallback must resolve the HIGHEST installed 1.89.x, in full:\n{combined}"
    );

    // Fixture B: none installed — refuse, naming the floor.
    let fx2 = SuiteFixture::new("msrv-missing");
    fx2.stub_cargo(0, false);
    fx2.stub(
        "rustup",
        "#!/bin/sh\nif [ \"$1\" = toolchain ] && [ \"$2\" = list ]; then\n\
         printf '%s\\n' '1.97.1-aarch64-apple-darwin (default)'\nfi\nexit 0\n",
    );
    let out2 = fx2.run(&[]);
    let combined2 = format!("{}{}", text(&out2.stdout), text(&out2.stderr));
    assert_eq!(
        out2.status.code(),
        Some(1),
        "with no 1.89.x installed the harness must REFUSE; it exited {:?}\n{combined2}",
        out2.status.code()
    );
    assert!(
        combined2.contains("1.89"),
        "the refusal must name the floor it could not satisfy:\n{combined2}"
    );
    assert!(
        !combined2.contains("toolchain pinned to 1.89\n"),
        "it must not have exported the two-component floor:\n{combined2}"
    );
}

/// **`docs/gates.md` records a measured number for every gate line.**
///
/// The "move the timing line above `cargo test --workspace`" mutant is caught by
/// the RECORDED FIGURES, not by a guard — `just gate` still exits 0, and what
/// changes is that the timing line absorbs a whole debug compile. That only
/// works if the figures are actually there, which is what this asserts.
#[test]
fn gate_lint_docs_record_the_measured_seconds() {
    let md = read("docs/gates.md");
    let gate = gate_lines();
    // A GATE ROW is one whose first cell is a line NUMBER and whose last cell is
    // a number of seconds. Both halves matter: "contains a digit somewhere"
    // would count the stack/cluster rows (whose last cell names `e2e`) and would
    // keep passing with every measured figure deleted.
    let mut timed = 0usize;
    for row in md.lines().map(str::trim) {
        if !row.starts_with("| ") || row.matches('|').count() < 5 {
            continue;
        }
        let cells: Vec<&str> = row.trim_matches('|').split('|').map(str::trim).collect();
        let is_gate_row = cells.first().is_some_and(|c| c.parse::<usize>().is_ok());
        let has_secs = cells.last().is_some_and(|c| c.parse::<f64>().is_ok());
        if is_gate_row && has_secs {
            timed += 1;
        }
    }
    assert!(
        timed >= gate.len(),
        "docs/gates.md records a measured figure on only {timed} row(s); `just gate` has {} lines \
         and every one of them must carry its measured seconds. Those figures are the only thing \
         that catches a reordering of the timing line.",
        gate.len()
    );
    assert!(
        md.contains("two workspace compilations"),
        "docs/gates.md must state the two-compilation shape beside the measured total"
    );
}

/// THE TIMING LINE'S PLACE IS ASSERTED, NOT INFERRED. The review ran the
/// reorder mutant (M6) for real: with `./scripts/time-unit-suite.sh` moved above
/// `cargo test --workspace` on a cold target, the suite still passed — 64 s
/// inside its window and 115 s of compile OUTSIDE it, because the script builds
/// every test binary before its clock starts. So nothing in the gate's own
/// figures catches a reorder; the budget is a slow, indirect signal for a
/// one-line ordering rule. This is the direct one: the timer sits immediately
/// after the debug workspace test and before the release one, and those two are
/// the only workspace compilations the gate contains.
#[test]
fn gate_lint_the_timing_line_sits_between_the_two_compilations() {
    let gate = gate_lines();
    let at = |line: &str| -> usize {
        gate.iter()
            .position(|l| l == line)
            .unwrap_or_else(|| panic!("`just gate` has no line `{line}`:\n  {gate:#?}"))
    };
    let debug = at("cargo test --workspace");
    let timer = at("./scripts/time-unit-suite.sh");
    let release = at("cargo test --workspace --release");
    assert_eq!(
        timer,
        debug + 1,
        "`./scripts/time-unit-suite.sh` (line {}) must come IMMEDIATELY after `cargo test \
         --workspace` (line {}): run earlier, it pays for a debug compile the gate would have paid \
         for anyway and measures nothing about the order; run later, the release compile sits \
         between the build and the measurement",
        timer + 1,
        debug + 1
    );
    assert!(
        release > timer,
        "`cargo test --workspace --release` (line {}) must come after the timing line (line {})",
        release + 1,
        timer + 1
    );
    let compiles: Vec<&String> = gate
        .iter()
        .filter(|l| l.starts_with("cargo test") || l.starts_with("cargo build"))
        .collect();
    assert_eq!(
        compiles.len(),
        2,
        "the gate compiles the workspace exactly twice, one debug and one release; found {compiles:?}"
    );
}

/// **`ci.yml`'s `e2e` job seeds the stack exactly as `just e2e-up` does.**
///
/// The recipe is the laptop's truth: `up -d --wait` and then the three one-shot
/// seeders. The CI job had two of the three — no `scram-setup` — and every
/// SCRAM test failed on the runner with an authentication error (the third CI
/// run, 2026-09-12). A job that seeds less than the recipe tests a different
/// stack; this holds the job to every executed line of the recipe.
///
/// KILLS: dropping any `docker compose … run --rm <seeder>` line from the job.
#[test]
fn gate_lint_ci_e2e_seeds_like_e2e_up() {
    let recipe = recipe_body(&justfile(), "e2e-up");
    assert!(
        recipe.len() >= 4,
        "`just e2e-up` should be the `up --wait` plus three seeders; found {recipe:?}"
    );
    let ci = read(".github/workflows/ci.yml");
    // The `e2e:` job's executed lines: from its header to the next job header.
    let mut in_job = false;
    let mut job = Vec::new();
    for line in ci.lines() {
        if line == "  e2e:" {
            in_job = true;
            continue;
        }
        if in_job
            && line.starts_with("  ")
            && !line.starts_with("   ")
            && line.trim_end().ends_with(':')
        {
            break;
        }
        if in_job && !line.trim_start().starts_with('#') {
            job.push(line.trim());
        }
    }
    assert!(!job.is_empty(), "ci.yml has no `e2e:` job");
    let missing: Vec<&String> = recipe
        .iter()
        .filter(|l| !job.iter().any(|j| j.ends_with(l.as_str())))
        .collect();
    assert!(
        missing.is_empty(),
        "these lines of `just e2e-up` are not run by ci.yml's `e2e` job: {missing:?}\n  the job \
         must bring the stack up and seed it exactly as the recipe does — a stack seeded less \
         is a different stack, and its SCRAM tests fail with an authentication error"
    );
}
