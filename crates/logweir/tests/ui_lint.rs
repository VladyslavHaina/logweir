//! THE UI's RUST-SIDE LINT. Tasks 26, 27 and 28 extend this file.
//!
//! WHAT THESE TESTS PROVE. That the shipped UI is what the documents say it
//! is: a directory of static files that names no external resource, holds no
//! credential, has no build step, reaches the network through exactly one call
//! site, and builds every identifier it sends with one relative-path builder.
//! Every assertion here reads a checked-in file, or drives
//! `scripts/check-ui-offline.sh` against a temp root, and nothing here starts a
//! server or opens a socket.
//!
//! WHY BOTH A SHELL GATE AND A RUST TEST. `scripts/check-ui-offline.sh` is the
//! gate an adopter and CI run on every `just lint`; the tests below are what
//! prove the gate is armed, that its RED side actually goes red, and that the
//! properties the gate cannot express -- one `fetch(` call site, a module's
//! export list, a serving command identical in three files -- hold too. A gate
//! whose failing arm is never exercised is a gate nobody has seen fail.
//!
//! THE SCOPE RULE, IN ONE PLACE. The shipped assets are every file under `ui/`
//! EXCEPT `*.md` and EXCEPT the `tests/` subtree. A Markdown file is
//! documentation, not an asset the browser loads; a test is data. Everything
//! that is not scoped out is scanned, and there is no exemption inside the
//! scope.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

// ---------------------------------------------------------------- the tokens

/// Rule 1: a line carrying any of these names a resource outside the directory
/// the page was served from. This array is the same list
/// `scripts/check-ui-offline.sh` holds, and the two are meant to be read side
/// by side.
const FORBIDDEN: [&str; 11] = [
    "http:",
    "https:",
    "src=\"//",
    "src='//",
    "href=\"//",
    "href='//",
    "@import url(",
    "rel=\"preconnect\"",
    "rel=\"dns-prefetch\"",
    "//# sourceMappingURL=",
    "@font-face",
];

/// Rule 2: a line carrying any of these puts a credential in the page or
/// stores something in the browser.
const CREDENTIAL: [&str; 5] = [
    "Authorization",
    "Bearer",
    "localStorage.setItem",
    "sessionStorage.setItem",
    "document.cookie",
];

/// The serving command, verbatim, in all three places that carry it.
const SERVING_COMMAND: &str =
    "kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1";

/// The two flags that must never change, and the sentence that says why.
const NEVER_CHANGE_A: &str = "--address=127.0.0.1";
const NEVER_CHANGE_B: &str = "--disable-filter";
const ESCALATION_SENTENCE: &str = "Changing either turns a local page holding your cluster \
                                   authority into a network service holding it.";
const VIEWER_AUTHORITY: &str = "viewer's entire cluster authority";

/// The module's export list, in source order.
///
/// The fifth element carries the `engine-token-ok` escape
/// `check-no-oso.sh`'s check B requires: that token is the name of a
/// JavaScript function in `ui/api.js` which lists a Kubernetes kind, and it
/// has nothing to do with the `kafka-backup` subcommand GC3 denies. The escape
/// sits on the element's own line because that gate matches per physical line
/// -- which is also why this paragraph spells the name without its quotes.
const API_EXPORTS: [&str; 29] = [
    "GROUP",
    "VERSION",
    "WRITABLE_PLURALS",
    "path",
    "list", // engine-token-ok: the JS export name in ui/api.js, never an engine subcommand
    "get",
    "create",
    "patchSuspend",
    "listCluster",
    "apiError",
    // The console half (PLAT-18.1, decision D0 stage 6): the product API's
    // reads, its two writes, its session probe and its problem reader. Every
    // one of them goes through the SAME `request(path(...))` seam the legacy
    // half does, which is what `every_api_path_is_relative` holds. Nothing
    // here is a generic request helper: each names one route.
    "CONSOLE_WRITABLE_PLURALS",
    "session",
    // MCP-5: sign out. `POST /api/v1/session/logout`, a WRITE like every
    // other -- the session's synchroniser token, `application/json`, the
    // exact Origin -- which clears the session cookie server side. It names
    // one route and takes no identifier from a caller.
    "consoleLogout",
    "consoleList",
    "consoleGet",
    "consoleSub",
    "consoleOperation",
    // D3 W12 (PLAT-14.1, PLAT-15.1, PLAT-19.1): the ONE cluster-scoped product
    // read (`GET /api/v1/trust-policies`, bounded by its own frozen plural
    // list in api.js) and the module's SECOND request site -- the operation
    // event stream. `EventSource` is a request, so it lives in api.js like
    // every other one: its identifier is built by `path(...)` on the line of
    // the construction, it carries no header and no token, and closing it
    // closes a connection and never an operation.
    "consoleClusterList",
    "consoleClusterGet",
    "openOperationStream",
    // D2 W13: the SIX named action routes the product API spells as a verb
    // suffix (`destinations/primary:test`) or a sub-collection create
    // (`connections/source/topic-discoveries`) -- neither of which is a plural
    // `consoleCreate` could POST to. It is not a generic POST helper: the
    // action is looked up in a frozen table in `api.js`, so the reachable set
    // is still a list a reviewer reads in one place.
    "consoleAction",
    // PLAT-19.2: ONE read, `GET .../approval-policy` -- the namespace's
    // effective approval policy the submit step and the approvals page render.
    // No body, no query, no write.
    "consoleApprovalPolicy",
    "consoleCreate",
    "consoleSetSuspension",
    // D1 W7: the draft-cadence preview (a safe, namespace-less GET that reads
    // no object) and the product API's ONE replace, the schedule's future
    // policy. See [`the_api_module_offers_no_delete_and_no_put`] for the terms
    // on which the second one is allowed to exist at all.
    "cadencePreview",
    "consoleSchedulePolicy",
    // D3 W12 (PLAT-19.1, D3 section 7.7): the instant the SERVER dated its most
    // recent answer, for the one column that needs an elapsed time -- the keys
    // view's `unknown`. It is a reader over a value this module recorded; it
    // issues no request, and it is deliberately not reachable from a page
    // module (`the_suspend_toggle_is_the_only_update`), which reads it through
    // `ui/operation-watch.js`'s `serverClock`.
    "serverTime",
    // The bound that keeps that instant from becoming a STOPPED clock: an
    // instant carried forward past it is reported as none, and none reads
    // `unknown` (review F6). It is a constant, not a reader.
    "SERVER_TIME_MAX_AGE_MS",
    "problemError",
];

/// Invisible codepoints. The first draft of this plan hid one inside a token so
/// a document could name a scheme without tripping its own rule; that is a hole
/// nobody can see in a diff, and this is what forbids it.
const INVISIBLE: [char; 5] = ['\u{200B}', '\u{200C}', '\u{200D}', '\u{2060}', '\u{FEFF}'];

// --------------------------------------------------------------- the walkers

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn ui_root() -> PathBuf {
    repo_root().join("ui")
}

/// Every file under `dir`, recursively, sorted. Directories are walked in full;
/// nothing is skipped here, so every caller states its own scope.
fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()));
    let mut sorted: Vec<PathBuf> = entries
        .map(|e| e.expect("a readable directory entry").path())
        .collect();
    sorted.sort();
    for path in sorted {
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Every file under `ui/`, including `*.md` and `tests/`.
fn every_ui_file() -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(&ui_root(), &mut out);
    assert!(
        !out.is_empty(),
        "ui/ holds no file at all; a UI lint that enumerated nothing is not a pass"
    );
    out
}

fn is_markdown(path: &Path) -> bool {
    path.extension().map(|e| e == "md").unwrap_or(false)
}

fn is_under_tests(path: &Path) -> bool {
    path.starts_with(ui_root().join("tests"))
}

/// The shipped assets: everything the browser loads. `*.md` and `tests/` are
/// out of scope, and nothing else is.
fn shipped_assets() -> Vec<PathBuf> {
    let assets: Vec<PathBuf> = every_ui_file()
        .into_iter()
        .filter(|p| !is_markdown(p) && !is_under_tests(p))
        .collect();
    assert!(
        !assets.is_empty(),
        "no shipped asset was enumerated under ui/ -- a scan that enumerated nothing is not a pass"
    );
    assets
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

/// `api.js`'s export list AS THE FILE DECLARES IT, in source order.
///
/// READ FROM THE MODULE, NEVER FROM [`API_EXPORTS`]. The constant is what the
/// surface is SUPPOSED to be, and `the_api_module_offers_no_delete_and_no_put`
/// compares the two. Every other test that needs "the identifiers a page could
/// reach for" has to ask the file, because the interesting mutant is precisely
/// the one that ADDS an export -- a generic `patch`, a raw request helper --
/// and an alphabet taken from the constant cannot contain a name the constant
/// does not have. That is a guard whose mutant passes, which STANDING RULE 21
/// calls worse than no guard.
fn api_exports() -> Vec<String> {
    let contents = read(&ui_root().join("api.js"));
    let mut exports: Vec<String> = Vec::new();
    for line in contents.lines() {
        let rest = match line.strip_prefix("export ") {
            Some(rest) => rest,
            None => continue,
        };
        let rest = rest.strip_prefix("async ").unwrap_or(rest);
        let rest = rest
            .strip_prefix("const ")
            .or_else(|| rest.strip_prefix("function "))
            .or_else(|| rest.strip_prefix("let "))
            .unwrap_or_else(|| panic!("an export this test cannot name: {line}"));
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        assert!(!name.is_empty(), "an export with no name: {line}");
        exports.push(name);
    }
    assert!(
        !exports.is_empty(),
        "ui/api.js declares no export at all, so any test using this list as its alphabet \
         would pass having checked nothing"
    );
    exports
}

/// A path as it reads in a failure message: relative to the repository root.
fn shown(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .display()
        .to_string()
}

#[test]
fn chart_readme_ui_harness_binds_readiness_and_cleans_up_on_bash_3_2() {
    let readme = read(&repo_root().join("charts/logweir/README.md"));
    let readiness = readme
        .split("wait_for_ui() {")
        .nth(1)
        .and_then(|rest| rest.split("\n}\n").next())
        .expect("chart README wait_for_ui function");
    assert_eq!(
        readiness.matches("kill -0 \"$port_forward_pid\"").count(),
        2,
        "the exact kubectl child must be checked before and after HTTP"
    );
    for identity in [
        "Forwarding from 127.0.0.1:18132 -> 8001",
        "<title>Logweir</title>",
        "id=\"view-slot\"",
    ] {
        assert!(readiness.contains(identity), "readiness omits {identity}");
    }
    assert!(
        readme.contains("${owned_namespaces[@]+\"${owned_namespaces[@]}\"}"),
        "empty owned_namespaces must be safe under Bash 3.2 nounset"
    );
    assert!(
        readme.contains("image rm \"$image\" >/dev/null 2>&1 || cleanup_status=1")
            && readme.contains("rm -rf \"$wrapper\" || cleanup_status=1")
            && readme.contains("rm -f \"$port_forward_log\" || cleanup_status=1"),
        "cleanup-only failures must make an otherwise successful recipe nonzero"
    );
}

/// Collapses every run of whitespace to one space, so an assertion can name a
/// sentence without depending on where the author wrapped it. The
/// `one_signer_gate.rs::comment_prose` precedent.
fn prose(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Runs the gate. `ui_root`, when given, is exported as `LOGWEIR_UI_ROOT` so
/// the script walks an overlay instead of the real tree.
///
/// The exit status is read from the child process directly -- never from a
/// pipeline and never from `$?` after one.
fn run_gate(overlay: Option<&Path>) -> Output {
    let mut cmd = Command::new("bash");
    cmd.arg("scripts/check-ui-offline.sh")
        .current_dir(repo_root());
    if let Some(r) = overlay {
        cmd.env("LOGWEIR_UI_ROOT", r);
    }
    cmd.output().expect("the gate script can be spawned")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A throwaway UI root, deleted on drop.
struct Overlay {
    path: PathBuf,
}

impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Overlay {
    fn new(tag: &str) -> Overlay {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("logweir-ui-{tag}-{stamp}"));
        std::fs::create_dir_all(&path).expect("the overlay root is created");
        Overlay { path }
    }

    fn write(&self, relative: &str, contents: &str) {
        let target = self.path.join(relative);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).expect("the overlay subdirectory is created");
        }
        std::fs::write(&target, contents).expect("the overlay file is written");
    }
}

/// The body of a `just` recipe: from the line `name:` (or `name <args>:`) to
/// the next line beginning with a lowercase ASCII letter. The
/// `one_signer_gate.rs::just_lint_runs_the_one_signer_gate` pattern.
fn recipe_body(justfile: &str, name: &str) -> String {
    let mut body = String::new();
    let mut inside = false;
    for line in justfile.lines() {
        if !inside
            && line.starts_with(name)
            && (line[name.len()..].starts_with(':') || line[name.len()..].starts_with(' '))
        {
            inside = true;
            continue;
        }
        if inside {
            if line.starts_with(|c: char| c.is_ascii_lowercase()) {
                break;
            }
            body.push_str(line);
            body.push('\n');
        }
    }
    assert!(inside, "the justfile must still declare a `{name}` recipe");
    body
}

/// Every module specifier a line declares: `import ... from "x"`,
/// `export ... from "x"`, a side-effect `import "x"` and a dynamic
/// `import("x")`, in either quote style.
fn specifier_of(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let declares = trimmed.starts_with("import") || trimmed.starts_with("export");
    let after = if declares {
        line.split_once(" from ")
            .map(|p| p.1)
            .or_else(|| trimmed.strip_prefix("import\"").map(|_| &trimmed[6..]))
            .or_else(|| trimmed.strip_prefix("import'").map(|_| &trimmed[6..]))
            .or_else(|| line.split_once("import(").map(|p| p.1))
    } else {
        line.split_once("import(").map(|p| p.1)
    }?;

    let normalised = after.replace('\'', "\"");
    let opened = normalised.split_once('"')?;
    let closed = opened.1.split_once('"')?;
    Some(closed.0.to_string())
}

/// True when `token` occurs in `haystack` with no ASCII alphanumeric or
/// underscore on either side -- a BARE token, whatever quotes surround it.
fn contains_bare_token(haystack: &str, token: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = haystack[from..].find(token) {
        let start = from + offset;
        let end = start + token.len();
        let before_ok = start == 0 || !is_word_byte(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_word_byte(bytes[end]);
        if before_ok && after_ok {
            return Some(start);
        }
        from = start + 1;
    }
    None
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The 1-based line number an offset falls on.
fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].matches('\n').count() + 1
}

// ------------------------------------------------------- 1. the gate is armed

#[test]
fn the_ui_offline_gate_is_in_just_lint() {
    let justfile = std::fs::read_to_string(repo_root().join("justfile")).expect("justfile is read");
    let body = recipe_body(&justfile, "lint");
    assert!(
        body.contains("check-ui-offline.sh"),
        "`just lint` is where the UI's no-external-resource gate runs; dropping the line \
         disarms rules 1 and 2 for every adopter and every CI run without changing a single \
         file under ui/. The `lint` body was:\n{body}"
    );
}

// ------------------------------------------------- 2. no external resource

#[test]
fn no_absolute_url_in_the_shipped_assets() {
    // The name says SHIPPED, not "built", on purpose: there is no bundler in
    // this tree, so the shipped assets ARE the sources and a name promising a
    // build output would send a reviewer looking for one that does not exist.
    let mut findings: Vec<String> = Vec::new();

    for asset in shipped_assets() {
        let contents = read(&asset);
        for (index, line) in contents.lines().enumerate() {
            for token in FORBIDDEN {
                if line.contains(token) {
                    findings.push(format!(
                        "{}:{}: carries {token:?}",
                        shown(&asset),
                        index + 1
                    ));
                }
            }
            if let Some(spec) = specifier_of(line) {
                if !spec.starts_with("./") && !spec.starts_with("../") {
                    findings.push(format!(
                        "{}:{}: module specifier {spec:?} is not relative",
                        shown(&asset),
                        index + 1
                    ));
                }
            }
        }
    }

    assert!(
        findings.is_empty(),
        "the shipped UI names a resource outside its own directory. There is no build step \
         here, so these bytes are what a browser loads, and the page they load into runs with \
         the viewer's cluster authority:\n{}",
        findings.join("\n")
    );

    // The RED arm. The gate must actually fail on the thing it forbids, and it
    // must say WHERE -- a gate that only ever runs green on a clean tree has
    // never been observed to work.
    let overlay = Overlay::new("cdn");
    overlay.write(
        "assets/x.html",
        "<link href=\"https://cdn.example.com/x.css\">\n",
    );
    let out = run_gate(Some(&overlay.path));
    assert_eq!(
        out.status.code(),
        Some(1),
        "the gate must exit 1 on an external stylesheet; stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("assets/x.html:1"),
        "the gate must name file and line; stderr was:\n{stderr}"
    );
}

// --------------------------------------- 3. enumerating nothing is not a pass

#[test]
fn the_scan_enumerated_something() {
    let overlay = Overlay::new("empty");
    let out = run_gate(Some(&overlay.path));
    assert_eq!(
        out.status.code(),
        Some(1),
        "an empty root is a FAILURE, not a green run: it is what a renamed directory looks \
         like from inside the gate. stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("enumerated nothing"),
        "the refusal must say what happened; stderr was:\n{stderr}"
    );

    // And the converse, on the REAL root, which is what makes this test see a
    // renamed directory. Without this arm the rename mutant -- move `ui/`
    // somewhere else and leave the gate's root pointing at the old name --
    // would be caught only by the other tests panicking on a missing path,
    // which reads as a broken test rather than as the gate reporting.
    let real = run_gate(None);
    assert_eq!(
        real.status.code(),
        Some(0),
        "the gate must be green on the unmodified tree; stdout:\n{}\nstderr:\n{}",
        text(&real.stdout),
        text(&real.stderr)
    );
    let stdout = text(&real.stdout);
    assert!(
        !stdout.contains(" 0 file(s)"),
        "the gate reported zero files on the real root, which is a rename it did not follow. \
         stdout was:\n{stdout}"
    );
    assert!(
        stdout.contains("file(s)"),
        "the gate must say how many files it read; a green run over zero files and a green run \
         over six must not look the same. stdout was:\n{stdout}"
    );
}

// ------------------------------------------------------ 4. no hidden bytes

#[test]
fn the_ui_sources_are_ascii_only() {
    let mut findings: Vec<String> = Vec::new();
    for file in every_ui_file() {
        if is_under_tests(&file) {
            continue;
        }
        let contents = read(&file);
        for (index, line) in contents.lines().enumerate() {
            for ch in line.chars() {
                if INVISIBLE.contains(&ch) {
                    findings.push(format!(
                        "{}:{}: invisible codepoint U+{:04X}",
                        shown(&file),
                        index + 1,
                        ch as u32
                    ));
                } else if !ch.is_ascii() {
                    findings.push(format!(
                        "{}:{}: non-ASCII codepoint U+{:04X} ({ch:?})",
                        shown(&file),
                        index + 1,
                        ch as u32
                    ));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "the UI sources carry a codepoint that is not plain ASCII. The load-bearing half of \
         this rule is the invisible set: an earlier draft hid a zero-width space inside a \
         forbidden token so a document could name a scheme without tripping its own rule, and \
         a hole nobody can see in a diff is worse than no rule. The ASCII half is what makes \
         this test's name true, and it is cheap to satisfy.\n{}",
        findings.join("\n")
    );
}

// --------------------------------------------- 5. no directory is listed

#[test]
fn no_ui_subdirectory_lacks_an_index_html() {
    // Go's `http.FileServer` -- which is what `kubectl proxy --www=` is --
    // emits a directory listing for any subdirectory with no `index.html`. An
    // empty index is the whole guard, which is why `ui/pages/index.html` is
    // zero bytes rather than absent.
    let tests_dir = ui_root().join("tests");
    let mut directories: Vec<PathBuf> = vec![ui_root()];
    let mut queue: Vec<PathBuf> = vec![ui_root()];
    while let Some(dir) = queue.pop() {
        for entry in std::fs::read_dir(&dir).expect("a readable directory") {
            let path = entry.expect("a readable entry").path();
            if path.is_dir() && path != tests_dir && !path.starts_with(&tests_dir) {
                directories.push(path.clone());
                queue.push(path);
            }
        }
    }
    directories.sort();

    let missing: Vec<String> = directories
        .iter()
        .filter(|d| !d.join("index.html").is_file())
        .map(|d| shown(d))
        .collect();

    assert!(
        missing.is_empty(),
        "every directory served out of ui/ needs an index.html, or the file server publishes \
         a listing of it. Missing in: {}",
        missing.join(", ")
    );
}

// ------------------------------------------------------- 6. no build step

#[test]
fn no_build_step_exists() {
    let root = ui_root();
    assert!(
        !root.join("package.json").is_file(),
        "ui/package.json exists; there is no build step and no package manifest in this tree"
    );
    assert!(
        !root.join("node_modules").exists(),
        "ui/node_modules exists; nothing here is ever installed or fetched"
    );

    let mut findings: Vec<String> = Vec::new();
    for file in every_ui_file() {
        let name = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.ends_with(".min.js") {
            findings.push(format!("{}: a minified asset", shown(&file)));
        }
        if name.ends_with(".map") {
            findings.push(format!("{}: a source map", shown(&file)));
        }
        if file.components().any(|c| c.as_os_str() == "node_modules") {
            findings.push(format!("{}: inside node_modules", shown(&file)));
        }
        if name.as_str() == "package.json" {
            findings.push(format!("{}: a package manifest", shown(&file)));
        }
        let contents = read(&file);
        if let Some(offset) = contents.find("//# sourceMappingURL=") {
            findings.push(format!(
                "{}:{}: a source-map annotation",
                shown(&file),
                line_of(&contents, offset)
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "the shipped assets are the sources; a build output could differ from what the gates \
         scan, which is the whole reason there is no build step:\n{}",
        findings.join("\n")
    );
}

// -------------------------------------------------- 7. no credential, ever

#[test]
fn no_credential_appears_in_the_page() {
    let mut findings: Vec<String> = Vec::new();
    for file in every_ui_file() {
        let contents = read(&file);
        for (index, line) in contents.lines().enumerate() {
            for token in CREDENTIAL {
                if line.contains(token) {
                    findings.push(format!("{}:{}: carries {token:?}", shown(&file), index + 1));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "the page holds no credential of any kind and stores nothing. The proxy attaches the \
         viewer's own kubeconfig credential server side; a page that carried one would be \
         holding a second copy of an authority it already has and cannot protect:\n{}",
        findings.join("\n")
    );

    // The RED arm.
    let overlay = Overlay::new("credential");
    overlay.write(
        "a.js",
        "const t = load();\nconst init = { headers: {Authorization: \"Bearer \" + t} };\n",
    );
    let out = run_gate(Some(&overlay.path));
    assert_eq!(
        out.status.code(),
        Some(1),
        "the gate must exit 1 on a request header carrying a credential; stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
}

// ------------------------------------------- 8. create only, plus one update

#[test]
fn the_api_module_offers_no_delete_and_no_put() {
    let api = ui_root().join("api.js");
    let contents = read(&api);

    // A DELETE IS STILL UNREACHABLE FROM THIS PAGE, IN EITHER MODE. Logweir
    // holds no delete capability of any kind -- against an archive, against a
    // custom resource, against anything -- and this is the half of that a
    // review of `ui/` can still see.
    if let Some(offset) = contains_bare_token(&contents, "DELETE") {
        panic!(
            "api.js:{} carries the bare token \"DELETE\". The module exports no delete and \
             must not: a delete here would be a write the page's own contract says it \
             cannot make. The token is matched BARE, so neither quote style hides it.",
            line_of(&contents, offset)
        );
    }

    // AND THERE IS EXACTLY ONE REPLACE (D1 W7, PLAT-05.1). `PUT
    // .../schedules/{name}` is the product API's editable future policy: it
    // replaces the whole policy under `expectedGeneration`, and D1 §5.6 makes
    // it the route a guided edit form must use. Until D1 W6 landed it, this
    // test forbade the token outright, and the page said so in prose
    // (`EDIT_IS_A_REPLACE`).
    //
    // WHAT REPLACED THE BAN IS NOT A WEAKER RULE. The token must appear
    // EXACTLY ONCE, and that once must be the value of a module-level
    // `const REPLACE_METHOD`, which is what makes "this module can replace one
    // thing" a sentence a reviewer reads in one place rather than a property
    // they would have to reconstruct from the call sites. A second replace --
    // a `PUT` over a connection, a destination, a Backup -- lands as a second
    // occurrence and fails here, exactly as a delete does above.
    let declaration = "const REPLACE_METHOD = \"PUT\";";
    assert!(
        contents.contains(declaration),
        "api.js must spell its one replace method as `{declaration}`: the module is allowed \
         exactly one replace, the schedule policy route, and naming the method once beside \
         the paragraph that says why is what keeps that reviewable."
    );
    let occurrences = contents.match_indices("PUT").count();
    assert_eq!(
        occurrences, 1,
        "api.js spells the bare token \"PUT\" {occurrences} time(s); exactly one is \
         permitted, and it is the value of `REPLACE_METHOD`. A second replace in this \
         module widens what every page can attempt without any page changing."
    );
    assert!(
        contents.contains("export async function consoleSchedulePolicy("),
        "the one replace is `consoleSchedulePolicy`, the schedule's future policy. If it \
         has been renamed, rename it here too -- this assertion is what stops \
         `REPLACE_METHOD` being reused by some other route."
    );

    assert_eq!(
        api_exports(),
        API_EXPORTS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "api.js's export list is the module's whole surface. Adding to it -- a delete, a \
         generic patch, a raw request helper -- widens what any page can do without any page \
         changing."
    );
}

// ----------------------------------------- 9. one call site, every path built

#[test]
fn every_api_path_is_relative() {
    // (i) and (ii): exactly one network call site in all of ui/, on one line.
    let mut sites: Vec<(PathBuf, usize, String)> = Vec::new();
    for file in every_ui_file() {
        let contents = read(&file);
        for (index, line) in contents.lines().enumerate() {
            if line.contains("fetch(") {
                sites.push((file.clone(), index + 1, line.to_string()));
            }
        }
    }
    assert_eq!(
        sites.len(),
        1,
        "there is exactly one network call site under ui/, and it lives in api.js. Found: {:?}",
        sites
            .iter()
            .map(|(f, l, _)| format!("{}:{l}", shown(f)))
            .collect::<Vec<_>>()
    );
    let (site_file, site_line, site_text) = &sites[0];
    assert_eq!(
        site_file,
        &ui_root().join("api.js"),
        "the one call site must be in api.js, not {}:{site_line}",
        shown(site_file)
    );
    assert_eq!(
        site_text.as_str(),
        "  return fetch(u, init);",
        "the one call site is a one-line private function taking an identifier `path(...)` \
         built. A call site that did anything else to its first argument would put the \
         same-origin property back out of reach of a grep. At {}:{site_line}",
        shown(site_file)
    );

    // (iii): every call to `request(` hands it a `path(...)`.
    let api = read(&ui_root().join("api.js"));
    let mut calls = 0usize;
    let mut from = 0usize;
    while let Some(offset) = api[from..].find("request(") {
        let start = from + offset;
        from = start + "request(".len();

        let preceding = api[..start].trim_end();
        if preceding.ends_with("function") {
            continue; // the function's own definition
        }
        if !is_call_position(&api, start) {
            continue; // a longer identifier that merely ends in `request`
        }
        calls += 1;
        let argument = api[from..].trim_start();
        assert!(
            argument.starts_with("path("),
            "api.js:{}: a request whose first argument is not a `path(...)` call. Every \
             identifier this module sends is built by the one validator that refuses a \
             scheme separator, a leading slash and a parent-directory hop; an argument \
             built any other way is outside that guarantee. The argument read:\n{}",
            line_of(&api, start),
            &argument[..argument.len().min(90)]
        );
    }
    assert!(
        calls >= 5,
        "every exported reader and writer goes through `request(`; only {calls} call sites \
         were found, so this assertion is no longer covering the module"
    );

    // (iv): nothing in api.js can name an origin.
    for token in [
        "location.origin",
        "location.href",
        "new URL(",
        "document.baseURI",
        "://",
    ] {
        assert!(
            !api.contains(token),
            "api.js carries {token:?}. The module builds relative identifiers only, so a \
             reviewer grepping it for an origin or a scheme separator must get an empty \
             result -- and does."
        );
    }
}

// -------------------------------- 9b. the SECOND request site, held the same way

/// **ONE STREAM CONSTRUCTION, IN `api.js`, OVER A `path(...)` IDENTIFIER.**
///
/// `every_api_path_is_relative` counts `fetch(` and nothing else, so when D3
/// W12 added `EventSource` the module grew a second way to reach the network
/// that no gate covered: `ui/README.md` and `api.js`'s own paragraph both
/// assert the identifier is built by `path(...)`, and a page constructing its
/// own stream -- or the argument here becoming a binding -- would have left
/// every gate green (review F8).
///
/// Three arms, the same three the `fetch` site has:
///
///   (i)   exactly one construction of the stream class in all of `ui/`;
///   (ii)  it is in `api.js`;
///   (iii) the whole line is pinned, so its first argument IS a `path(...)`
///         call rather than something built elsewhere and passed in.
///
/// A capability PROBE is not a construction and is deliberately allowed:
/// `ui/operation-watch.js` reads `globalThis.EventSource` to decide whether to
/// stream at all, which reaches no network and creates nothing.
#[test]
fn the_event_stream_is_constructed_once_over_a_built_identifier() {
    const CONSTRUCTIONS: [&str; 2] = ["new EventSource(", "new Source("];
    let mut sites: Vec<(PathBuf, usize, String)> = Vec::new();
    for file in every_ui_file() {
        if is_markdown(&file) || is_under_tests(&file) {
            continue;
        }
        let contents = read(&file);
        for (index, line) in contents.lines().enumerate() {
            if CONSTRUCTIONS.iter().any(|token| line.contains(token)) {
                sites.push((file.clone(), index + 1, line.to_string()));
            }
        }
    }
    assert_eq!(
        sites.len(),
        1,
        "there is exactly one server-sent-event stream constructed under ui/, and it lives in \
         api.js beside the one fetch. Found: {:?}",
        sites
            .iter()
            .map(|(f, l, _)| format!("{}:{l}", shown(f)))
            .collect::<Vec<_>>()
    );
    let (site_file, site_line, site_text) = &sites[0];
    assert_eq!(
        site_file,
        &ui_root().join("api.js"),
        "the one stream construction must be in api.js, not {}:{site_line}",
        shown(site_file)
    );
    assert_eq!(
        site_text.as_str(),
        "  return new Source(path(\"api\", \"v1\", \"namespaces\", ns, \"operations\", kind, name, \"events\"));",
        "the stream's identifier is built by the one validator that refuses a scheme \
         separator, a leading slash and a parent-directory hop, ON THE LINE OF THE \
         CONSTRUCTION -- an argument built anywhere else is outside that guarantee, and the \
         property stops being checkable by reading. At {}:{site_line}",
        shown(site_file)
    );
}

/// True when the `request(` at `start` is a call and not the tail of a longer
/// identifier such as `sendRequest(`.
fn is_call_position(source: &str, start: usize) -> bool {
    if start == 0 {
        return true;
    }
    !is_word_byte(source.as_bytes()[start - 1])
}

// ------------------------------------------ 10. the write carries its manager

#[test]
fn create_carries_the_ui_field_manager() {
    let api = read(&ui_root().join("api.js"));
    let occurrences = api.matches("?fieldManager=logweir-ui").count();
    assert_eq!(
        occurrences, 1,
        "the field manager is written once, as a module-level constant, so every write \
         carries the same one and a second spelling cannot drift away from it"
    );

    let declaration = "const FIELD_MANAGER = \"?fieldManager=logweir-ui\";";
    assert!(
        api.contains(declaration),
        "the literal must be the value of a module-level `const FIELD_MANAGER`; expected the \
         line {declaration:?}"
    );
    let offset = api.find(declaration).expect("the declaration was found");
    let line_start = api[..offset].rfind('\n').map(|i| i + 1).unwrap_or(0);
    assert_eq!(
        &api[line_start..offset],
        "",
        "the declaration is at module level, not indented inside a function; api.js:{}",
        line_of(&api, offset)
    );

    assert!(
        api.contains("+ FIELD_MANAGER"),
        "`create` appends FIELD_MANAGER to the identifier `path(...)` built, and appends \
         nothing else"
    );
}

// ----------------------------------------- 11. the serving command, in three

#[test]
fn the_serving_command_is_documented_identically_in_three_places() {
    let readme = read(&ui_root().join("README.md"));
    assert!(
        readme.contains(SERVING_COMMAND),
        "ui/README.md must carry the serving command verbatim:\n{SERVING_COMMAND}"
    );

    let kubernetes = read(&repo_root().join("docs").join("kubernetes.md"));
    assert!(
        kubernetes.contains(SERVING_COMMAND),
        "docs/kubernetes.md must carry the serving command verbatim:\n{SERVING_COMMAND}"
    );

    let justfile = std::fs::read_to_string(repo_root().join("justfile")).expect("justfile is read");
    let body = recipe_body(&justfile, "ui");
    assert!(
        body.contains(SERVING_COMMAND),
        "the `ui` recipe must run the serving command verbatim -- `--context docker-desktop` \
         included. Without the context, `just ui` proxies whatever context happens to be \
         current, with whatever credential it carries, on the one command in this product \
         that hands a browser a cluster credential. The `ui` body was:\n{body}"
    );

    // A FOURTH PLACE, ONCE IT EXISTS. Task 29's `docs/install.md` reproduces
    // the serving command and the viewer-authority paragraph a fourth time,
    // and four copies of one string drift exactly the way three do. The arm is
    // guarded by the file's existence rather than by a task number: this
    // test's own name counts the three places that are always on disk, and
    // `install.md` is checked whenever it is there. (Renaming the test to say
    // "four" is the next editor's call; it costs a line in a name diff, which
    // is why it is not done here.)
    let install_path = repo_root().join("docs").join("install.md");
    let install = if install_path.is_file() {
        let contents = read(&install_path);
        assert!(
            contents.contains(SERVING_COMMAND),
            "docs/install.md exists and must carry the serving command verbatim -- it is the \
             document an adopter reads FIRST, so a drifted copy there is the copy most people \
             run:\n{SERVING_COMMAND}"
        );
        Some(contents)
    } else {
        // Recorded, not skipped silently: a guard that went quiet when its
        // subject was absent is a guard nobody can tell from a passing one.
        println!(
            "note: docs/install.md is not on disk, so the fourth copy of the serving command \
             was not compared. The three that are always present still must agree."
        );
        None
    };

    // Three copies of one string is a thing that drifts, so the last assertion
    // is that every serving line in all three files is the SAME line, not that
    // each of them looks plausible on its own.
    let mut spellings: BTreeSet<String> = BTreeSet::new();
    for source in [&readme, &kubernetes, &justfile]
        .into_iter()
        .chain(install.iter())
    {
        for line in source.lines() {
            if line.contains("proxy --www=./ui") {
                spellings.insert(line.trim().to_string());
            }
        }
    }
    assert_eq!(
        spellings,
        BTreeSet::from([SERVING_COMMAND.to_string()]),
        "the serving command is spelled more than one way across ui/README.md, \
         docs/kubernetes.md, the `ui` recipe and docs/install.md when it exists. One of \
         those is the one an adopter will copy, and there is no way to tell which."
    );
}

// --------------------------------------------- 12. the cost is in the product

#[test]
fn the_viewer_authority_cost_is_stated() {
    for relative in ["ui/README.md", "docs/kubernetes.md"] {
        let path = repo_root().join(relative);
        let contents = prose(&read(&path));

        assert!(
            contents.contains(VIEWER_AUTHORITY),
            "{relative} must say that the page runs with the viewer's entire cluster \
             authority. The four ClusterRoles bind the USER; under this serving path they \
             bind nothing about the page, and a document that leaves that out is describing \
             a product that does not exist."
        );
        assert!(
            contents.contains(NEVER_CHANGE_A),
            "{relative} must name {NEVER_CHANGE_A}: the proxy binds loopback only"
        );
        assert!(
            contents.contains(NEVER_CHANGE_B),
            "{relative} must name {NEVER_CHANGE_B}: never pass it; the default keeps the \
             cross-site request filter on"
        );
        assert!(
            contents.contains(&prose(ESCALATION_SENTENCE)),
            "{relative} must carry the sentence that says what changing either flag does:\n{}",
            prose(ESCALATION_SENTENCE)
        );
    }
}

// ===========================================================================
// TASK 26 -- the four read-and-create pages, and the behaviour gate.
// ===========================================================================

/// The six `api.js` identifiers a page module may reach for. `create` plus one
/// update is the whole write surface of this product's UI, and these are the
/// only names under `ui/pages/` that come from that module.
///
/// The first element carries the `engine-token-ok` escape `check-no-oso.sh`'s
/// check B requires, for the same reason [`API_EXPORTS`]'s fifth element does:
/// the token names a JavaScript function in `ui/api.js` that lists a Kubernetes
/// kind, and has nothing to do with the `kafka-backup` subcommand GC3 denies.
/// That gate matches per physical line, which is why the escape sits on the
/// element's own line and this paragraph spells the name without its quotes.
const PAGE_API_IDENTIFIERS: [&str; 6] = [
    "list", // engine-token-ok: the JS export name in ui/api.js, never an engine subcommand
    "get",
    "create",
    "patchSuspend",
    "listCluster",
    "apiError",
];

/// The four of [`PAGE_API_IDENTIFIERS`] a page tree must still be using. A page
/// tree that names none of them is reading and writing some other way, which is
/// what this test exists to notice.
///
/// The first element carries the same `engine-token-ok` escape the arrays above
/// do, and for the same reason.
const REQUIRED_PAGE_IDENTIFIERS: [&str; 4] = [
    "list", // engine-token-ok: the JS export name in ui/api.js, never an engine subcommand
    "get",
    "create",
    "patchSuspend",
];

/// The tokens a test file may not carry. The UI analogue of STANDING RULE 18's
/// dial-token audit: `just lint` runs with the compose stack down (Global
/// Constraint 22), and a behaviour suite that opened a socket would be a unit
/// gate that needs the network. Node's five dialling built-ins are listed in
/// BOTH spellings an ES import can use -- the `node:` scheme and the bare
/// name, in either quote style -- because a guard that named one spelling was
/// a guard a file passed by choosing the other (the review of Task 36
/// measured exactly that: a bare `from "http"` left this test green).
const DIAL_TOKENS: [&str; 16] = [
    "fetch(",
    "node:net",
    "node:http",
    "node:https",
    "node:tls",
    "node:dgram",
    "from \"net\"",
    "from \"http\"",
    "from \"https\"",
    "from \"tls\"",
    "from \"dgram\"",
    "from 'net'",
    "from 'http'",
    "from 'https'",
    "from 'tls'",
    "from 'dgram'",
];

/// The ONE file under `ui/tests/` that may dial, BY NAME: the fixture-driven
/// preview server (Task 36), a tool a developer runs to look at the page and
/// the gate never runs. `the_preview_server_binds_loopback_and_refuses_writes`
/// pins what it may do (loopback only; every write refused). The converse arm
/// of `the_ui_behaviour_suite_never_dials` keeps this list from going stale:
/// a file listed here that carries no dial token fails the test.
const DIALLING_TOOLS: [&str; 1] = ["preview-server.js"];

/// Every file under `ui/pages/`.
fn page_modules() -> Vec<PathBuf> {
    let root = ui_root().join("pages");
    assert!(
        root.is_dir(),
        "ui/pages/ holds the four read-and-create page modules; a lint that enumerated \
         nothing is not a pass"
    );
    let mut out = Vec::new();
    walk(&root, &mut out);
    assert!(!out.is_empty(), "ui/pages/ is empty");
    out
}

/// Every file under `ui/tests/`.
fn behaviour_suite_files() -> Vec<PathBuf> {
    let root = ui_root().join("tests");
    assert!(root.is_dir(), "ui/tests/ holds the behaviour suite");
    let mut out = Vec::new();
    walk(&root, &mut out);
    assert!(!out.is_empty(), "ui/tests/ is empty");
    out
}

// ------------------------------- 13. create only, plus one update, per PAGE

#[test]
fn the_suspend_toggle_is_the_only_update() {
    // The whole `api.js` export list is the alphabet; the six above are what a
    // page may use. Anything else from that list appearing under `ui/pages/`
    // means a page reached for a capability -- a raw identifier builder, a
    // generic patch, a widened writable set -- that the page contract does not
    // have. A grep for `patch` cannot express this: it matches
    // `api.patchSuspend(` itself.
    // THE ALPHABET IS `api.js`'s LIVE EXPORT LIST, not the checked-in constant.
    // The mutant this test exists for adds a generic `patch(ns, plural, name,
    // body)` to `api.js` and calls it from `schedules.js`; an alphabet taken
    // from `API_EXPORTS` cannot contain a name `API_EXPORTS` does not have, so
    // that mutant would survive here and be caught only by the neighbouring
    // test -- which is not the same thing as this rule holding.
    let exports = api_exports();
    let mut findings: Vec<String> = Vec::new();
    for file in page_modules() {
        let contents = read(&file);
        for export in &exports {
            if PAGE_API_IDENTIFIERS.contains(&export.as_str()) {
                continue;
            }
            if let Some(offset) = contains_bare_token(&contents, export) {
                findings.push(format!(
                    "{}:{}: names the api.js export {export:?}",
                    shown(&file),
                    line_of(&contents, offset)
                ));
            }
        }
    }
    assert!(
        findings.is_empty(),
        "a page module reached for an api.js export outside the six a page may use \
         ({}). The suspend toggle is the ONE update this UI makes -- a JSON-merge patch \
         touching `spec.suspend` and nothing else -- and a generic writer here would widen \
         what every page can attempt without any page changing:\n{}",
        PAGE_API_IDENTIFIERS.join(", "),
        findings.join("\n")
    );

    // And the converse, so this test sees a page module that stopped using the
    // API at all (which is what a page rewritten to hold its own client looks
    // like from here).
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for file in page_modules() {
        let contents = read(&file);
        for identifier in PAGE_API_IDENTIFIERS {
            if contains_bare_token(&contents, identifier).is_some() {
                seen.insert(identifier);
            }
        }
    }
    // engine-token-ok: the four are ui/api.js JS export names, never engine subcommands
    for required in REQUIRED_PAGE_IDENTIFIERS {
        assert!(
            seen.contains(required),
            "no page module names {required:?} any more. The four pages read with `list` and \
             `get`, create with `create`, and suspend with `patchSuspend`; a page tree that \
             names none of them is reading and writing some other way. Seen: {seen:?}"
        );
    }
}

// ------------------------------------ 14. the behaviour suite never dials

#[test]
fn the_ui_behaviour_suite_never_dials() {
    let mut findings: Vec<String> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for file in behaviour_suite_files() {
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let exempt = DIALLING_TOOLS.contains(&name.as_str());
        let contents = read(&file);
        let mut hits = 0usize;
        for (index, line) in contents.lines().enumerate() {
            for token in DIAL_TOKENS {
                if line.contains(token) {
                    hits += 1;
                    if !exempt {
                        findings.push(format!("{}:{}: carries {token:?}", shown(&file), index + 1));
                    }
                }
            }
        }
        if exempt {
            assert!(
                hits > 0,
                "{} is exempted from the dial-token rule by name but carries no dial token: the \
                 exemption is stale; remove it from DIALLING_TOOLS",
                shown(&file)
            );
            seen.push(name);
        }
    }
    for tool in DIALLING_TOOLS {
        assert!(
            seen.iter().any(|s| s == tool),
            "{tool} is exempted from the dial-token rule by name but is not under ui/tests/"
        );
    }
    assert!(
        findings.is_empty(),
        "a file under ui/tests/ names a dial token. The page modules are pure functions from \
         a JSON object to an HTML string, and the suite asserts on those strings against \
         checked-in fixtures; nothing in it opens a socket. `just lint` runs with the compose \
         stack down (Global Constraint 22), and the per-test budget is 15 s -- a suite that \
         dialled would fail one of those two long after it had stopped being reviewable. The \
         one file that may dial is exempted BY NAME in DIALLING_TOOLS:\n{}",
        findings.join("\n")
    );
}

// ------------------------------------------ 15. the behaviour gate is armed

#[test]
fn the_ui_behaviour_gate_is_in_just_lint() {
    let justfile = std::fs::read_to_string(repo_root().join("justfile")).expect("justfile is read");
    let body = recipe_body(&justfile, "lint");
    assert!(
        body.contains("check-ui-behaviour.sh"),
        "`just lint` is where the UI's behaviour suite runs. Dropping the line makes every \
         page rule in this product -- the two badge rules, the immutable line, the engine \
         sub-report row, the bucket footer, the retention sentence -- unverified on every \
         machine that is not the implementer's, without changing a single file under ui/. \
         The `lint` body was:\n{body}"
    );

    // ORDER MATTERS, because the two UI gates prove different things and the
    // cheap one goes first: `check-ui-offline.sh` reads bytes and needs no
    // toolchain, while this one needs node and a release binary.
    let offline = body
        .find("check-ui-offline.sh")
        .expect("the offline gate is in the lint body");
    let behaviour = body
        .find("check-ui-behaviour.sh")
        .expect("the behaviour gate is in the lint body");
    assert!(
        offline < behaviour,
        "the behaviour gate runs after the offline gate in `lint`. The `lint` body was:\n{body}"
    );
}

// ------------------------------------- 16. the gate refuses an old node

#[test]
fn the_behaviour_gate_refuses_an_old_node() {
    // A temp directory holding a `node` stub that answers `--version` with a
    // version below the floor, prefixed onto PATH. There is no text-check
    // fallback path to test, because there is none: the gate fails.
    let overlay = Overlay::new("oldnode");
    let stub = overlay.path.join("node");
    std::fs::write(
        &stub,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo v18.20.0; exit 0; fi\nexit 0\n",
    )
    .expect("the node stub is written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
            .expect("the node stub is executable");
    }

    let previous = std::env::var("PATH").unwrap_or_default();
    let out = Command::new("bash")
        .arg("scripts/check-ui-behaviour.sh")
        .current_dir(repo_root())
        .env("PATH", format!("{}:{previous}", overlay.path.display()))
        .output()
        .expect("the behaviour gate can be spawned");

    assert_eq!(
        out.status.code(),
        Some(1),
        "a node below v20.0.0 is a REFUSAL, exit 1, and never a warning with a green exit. A \
         gate that degraded here would report green while every page mutant survived, which \
         is the state STANDING RULE 21 calls worse than no guard. stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("is below v20.0.0"),
        "the refusal must say what it found; stderr was:\n{stderr}"
    );
    assert!(
        stderr.contains("never degrades to a text check"),
        "and must say why there is no fallback; stderr was:\n{stderr}"
    );
}

// --------------------------- 17. the offline gate's specifier rule goes RED

#[test]
fn the_offline_gate_refuses_a_bare_module_specifier() {
    // Nothing else exercises the RED side of `check-ui-offline.sh`'s SPECIFIER
    // rule: weakening that rule alone left every other test in this file green,
    // which makes it a guard whose mutant passes (STANDING RULE 21). This is
    // the arm that fails when it is weakened.
    let overlay = Overlay::new("specifier");
    overlay.write("a.js", "import x from \"preact\";\nexport const y = x;\n");
    let out = run_gate(Some(&overlay.path));
    assert_eq!(
        out.status.code(),
        Some(1),
        "a BARE module specifier needs a resolver this page does not have and must not \
         acquire: there is no bundler, no import map and no node_modules in this tree, so \
         the import would fail at load time in the browser -- and, far worse, a specifier \
         that DID resolve would be code arriving from outside the served directory. \
         stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("preact"),
        "the refusal must name the offending specifier; stderr was:\n{stderr}"
    );
    assert!(
        stderr.contains("is not relative"),
        "and must say which rule it broke; stderr was:\n{stderr}"
    );

    // The relative form is accepted, so this test is about the specifier's
    // SHAPE and not about the word `import`.
    let ok = Overlay::new("specifier-ok");
    ok.write(
        "a.js",
        "import { el } from \"./render.js\";\nexport const y = el;\n",
    );
    ok.write("index.html", "<!doctype html>\n");
    let green = run_gate(Some(&ok.path));
    assert_eq!(
        green.status.code(),
        Some(0),
        "a relative specifier is what every module in this tree uses; stdout:\n{}\nstderr:\n{}",
        text(&green.stdout),
        text(&green.stderr)
    );
}

// ===========================================================================
// TASK 27 -- the plan document, the submit region, the download and the
// read-only roster page.
// ===========================================================================

/// The unambiguous reserialisation entry points the wizard's SUBMIT REGION may
/// not carry, plus the plan document's file extension.
///
/// WHY THIS LIST AND NOT A GREP FOR "TRANSFORMATION". Each of these is an
/// entry point that produces a NEW string from the plan bytes, and a new
/// string is a new document with a new sha256 -- which is a document the
/// approver never signed. `.trim(` is deliberately not an entry: this source
/// region also builds the approval route, where it normalises the selected
/// namespace rather than plan bytes. The byte comparison in `pages.spec.js`
/// exercises plan bytes with significant trailing whitespace and is the
/// receiver-aware protection against a trim (or another transformation) of
/// the submitted document.
const RESERIALISERS: [&str; 5] = [
    "JSON.parse",
    "JSON.stringify",
    "structuredClone",
    ".normalize(",
    "yaml",
];

/// The three whose presence anywhere in the wizard is a defect, not just in
/// the submit region: nothing in a page that renders a document and posts it
/// verbatim has any use for them.
const RESERIALISERS_FILE_WIDE: [&str; 3] = ["JSON.parse", "JSON.stringify", "structuredClone"];

fn wizard_source() -> String {
    read(&ui_root().join("pages").join("restore-wizard.js"))
}

/// The text between two marker comments, with both markers asserted present.
/// A mutant that deletes a marker fails here rather than emptying the region
/// and passing.
fn region_of(source: &str, begin: &str, end: &str) -> String {
    let start = source.find(begin).unwrap_or_else(|| {
        panic!("the marker {begin} is gone; the region it delimits cannot be scanned")
    });
    let stop = source.find(end).unwrap_or_else(|| {
        panic!("the marker {end} is gone; the region it delimits cannot be scanned")
    });
    assert!(
        start < stop,
        "{begin} must come before {end}; a region that reads backwards scans nothing"
    );
    source[start + begin.len()..stop].to_string()
}

// -------------------- 18. the plan document the runner parses (arm 1)

#[test]
fn render_plan_bytes_emits_a_document_the_runner_parses() {
    // ARM 1: Rust, no node. It deserialises the checked-in golden into the
    // RUNNER'S OWN TYPE, which is the only assertion that catches an invented
    // shape. `Restore.spec.planBytes` is opaque to the API server and to the
    // controller and it is still not arbitrary: Task 20 writes those bytes
    // VERBATIM into the runner's plan ConfigMap as `data["restore.yaml"]` and
    // the runner parses them with `serde_yaml::from_str::<RestoreSpec>`. An
    // invented shape passes the 201, passes `drill approve` (which hashes
    // whatever bytes it reads), passes all five approval checks (the hash
    // matches) and fails only inside the runner pod, after every gate has
    // reported green. A hash-equality test cannot see it: sha256 of X in
    // JavaScript equals sha256 of X in Rust whatever X is.
    //
    // The grammar is stated ONCE, in docs/mvp/03-spec.md section 6.1's
    // paragraph "The restore plan document has ONE grammar, and it is the
    // runner's restore.yaml (amendment 4)". This test cites it and does not
    // restate it.
    let golden_path = ui_root()
        .join("tests")
        .join("fixtures")
        .join("plan.golden.yaml");
    let golden = read(&golden_path);
    let spec: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&golden).unwrap_or_else(|e| {
        panic!(
            "{} does not deserialise into logweir_core::spec::RestoreSpec: {e}\n\nThe UI's \
             renderPlanBytes emits the runner's document and nothing else. Regenerate the \
             golden with `node ui/tests/emit-plan.js > ui/tests/fixtures/plan.golden.yaml` \
             AFTER fixing the emitter -- regenerating it does not make it correct, which is \
             exactly what this arm exists to say.",
            shown(&golden_path)
        )
    });

    let fields_path = ui_root()
        .join("tests")
        .join("fixtures")
        .join("plan-fields.json");
    let fields: serde_json::Value =
        serde_json::from_str(&read(&fields_path)).expect("plan-fields.json is JSON");

    assert_eq!(
        spec.target.mode.to_string(),
        fields["target"]["mode"]
            .as_str()
            .expect("target.mode is a string"),
        "the deserialised mode is the one the wizard state names. These two strings are the \
         runner's TargetMode and the Restore CRD's own enum, byte for byte (interface I33)."
    );
    let naming = spec
        .target
        .topic_naming
        .as_ref()
        .expect("the golden states target.topic_naming");
    assert_eq!(
        naming.prefix,
        fields["target"]["topicPrefix"]
            .as_str()
            .expect("topicPrefix is a string"),
        "the deserialised prefix is the one the wizard prefilled"
    );
    let point_in_time = spec
        .restore
        .point_in_time
        .expect("the golden states restore.point_in_time");
    let expected: chrono::DateTime<chrono::Utc> = fields["pointInTime"]
        .as_str()
        .expect("pointInTime is a string")
        .parse()
        .expect("pointInTime is an RFC 3339 instant");
    assert_eq!(
        point_in_time, expected,
        "the recovery point round-trips. It is the window's END; its FLOOR is the archive's \
         own earliest covered timestamp, read from the manifest by the runner, and is \
         deliberately not a field of this document."
    );

    // And the emitter that produced it is the one the gate re-runs, so a
    // regenerated golden and the committed one cannot silently diverge.
    let emitter = ui_root().join("tests").join("emit-plan.js");
    assert!(
        emitter.is_file(),
        "{} is the golden's emitter; `scripts/check-ui-behaviour.sh` runs it and `diff -u`s \
         the result against the committed bytes",
        shown(&emitter)
    );
}

// ---- 18b. the catalog-bound plan carries the recovery point binding (PLAT-15.2)

#[test]
fn the_catalog_bound_plan_carries_the_point_binding_the_runner_reads() {
    // ARM 1 of the point golden (arm 2 is `ui/tests/restore-catalog.spec.js`,
    // which byte-compares the emitter's output with the same file, and
    // `scripts/check-ui-behaviour.sh` re-runs the emitter and `diff -u`s it).
    //
    // `RestoreSpec` HAS NO `deny_unknown_fields`, so a golden that spelt the
    // block `recovery_point:` or a key `receiptSha256:` would still parse --
    // into a plan with NO binding, which the runner restores from `backup`
    // alone without the re-verification D3 §5.5 step 6 exists for. So this arm
    // asserts the binding ARRIVES, field by field, with the fixture's values.
    let golden_path = ui_root()
        .join("tests")
        .join("fixtures")
        .join("plan-point.golden.yaml");
    let golden = read(&golden_path);
    let spec: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&golden).unwrap_or_else(|e| {
        panic!(
            "{} does not deserialise into logweir_core::spec::RestoreSpec: {e}. Regenerate with \
             `node ui/tests/emit-plan.js plan-point-fields.json > \
             ui/tests/fixtures/plan-point.golden.yaml` AFTER fixing the emitter.",
            shown(&golden_path)
        )
    });
    let fields_path = ui_root()
        .join("tests")
        .join("fixtures")
        .join("plan-point-fields.json");
    let fields: serde_json::Value =
        serde_json::from_str(&read(&fields_path)).expect("plan-point-fields.json is JSON");
    let point = spec.source.point.as_ref().unwrap_or_else(|| {
        panic!(
            "{} parsed with NO `source.point`: the emitter wrote the binding under a key the \
             runner does not read, and the runner would restore without re-verifying the point",
            shown(&golden_path)
        )
    });
    let want = |key: &str| {
        fields["point"][key]
            .as_str()
            .unwrap_or_else(|| panic!("point.{key} is a string in the fixture"))
            .to_string()
    };
    assert_eq!(point.point_id, want("pointId"), "source.point.point_id");
    assert_eq!(
        point.receipt_key,
        want("receiptKey"),
        "source.point.receipt_key"
    );
    assert_eq!(
        point.receipt_sha256,
        want("receiptSha256"),
        "source.point.receipt_sha256"
    );
    assert_eq!(
        point.manifest_sha256,
        want("manifestSha256"),
        "source.point.manifest_sha256"
    );
    assert_eq!(
        spec.source.backup,
        fields["backupSetRef"].as_str().expect("backupSetRef"),
        "source.backup is the point's backup set, PINNED -- never `latestCompleted`"
    );

    // AND THE BACKUP-BOUND GOLDEN CARRIES NONE: absent is v1's document, byte
    // for byte, and the runner performs no binding check for it.
    let plain: logweir_core::spec::RestoreSpec = serde_yaml::from_str(&read(
        &ui_root()
            .join("tests")
            .join("fixtures")
            .join("plan.golden.yaml"),
    ))
    .expect("plan.golden.yaml parses");
    assert!(
        plain.source.point.is_none(),
        "the Backup-bound golden must not carry a point binding"
    );

    // The gate diffs this golden too.
    let gate = read(&repo_root().join("scripts").join("check-ui-behaviour.sh"));
    assert!(
        gate.contains("plan-point.golden.yaml") && gate.contains("plan-point-fields.json"),
        "scripts/check-ui-behaviour.sh must re-emit and diff the point golden as well"
    );
}

// -------------------- 19. the wizard never reserialises the plan bytes

#[test]
fn the_wizard_never_reserialises_the_plan_bytes() {
    let source = wizard_source();

    // THE THREE THAT ARE NEVER NEEDED ANYWHERE IN THIS FILE.
    for token in RESERIALISERS_FILE_WIDE {
        assert!(
            !source.contains(token),
            "ui/pages/restore-wizard.js names {token:?}. The bytes in the `<pre>` are the bytes \
             `create` sends, and there is no step in between: a page that round-tripped the \
             plan through a parser would post a document whose sha256 is not the one it \
             showed, and `logweir drill approve` would have signed the other one."
        );
    }

    // AND THE SUBMIT REGION CARRIES NONE OF THE FIVE. Namespace normalisation
    // in approvalRoute is intentionally allowed here: this region contains
    // both the submit path and the route hand-off, while the behavioural arm
    // below verifies the value actually assigned to `spec.planBytes`.
    // The plan document's extension appears only in the download handler,
    // which is why the scan is a region and not the whole file -- and why both
    // markers are asserted present above.
    let region = region_of(&source, "// SUBMIT-REGION-BEGIN", "// SUBMIT-REGION-END");
    assert!(
        region.len() > 400,
        "the submit region is {} bytes, which is not a submit path. A mutant that moved the \
         write out of the region would pass a scan over an empty string.",
        region.len()
    );
    assert!(
        region.contains("create("),
        "the submit region is where the write happens; if `create(` is not in it, this scan is \
         guarding a region that does nothing"
    );
    for token in RESERIALISERS {
        assert!(
            !region.contains(token),
            "the submit region of ui/pages/restore-wizard.js names {token:?}. Every one of these \
             produces a NEW string from the plan bytes, and a new string is a new document with \
             a new hash."
        );
    }

    // The behavioural half is the one that can actually hold the property; it
    // must exist, because this scan alone would pass over a page that
    // reserialised with a token nobody thought of.
    let behaviour = read(&ui_root().join("tests").join("pages.spec.js"));
    assert!(
        behaviour.contains("the_wizard_never_reserialises_the_plan_bytes"),
        "the byte-comparison arm lives in ui/tests/pages.spec.js and is what proves the bytes \
         survive; this source scan only forbids the machinery"
    );
}

// -------------------- 20. the download writes no scheme and no cluster write

#[test]
fn the_plan_download_writes_no_scheme_and_no_cluster_write() {
    let source = wizard_source();

    assert!(
        source.contains("new Blob("),
        "the download is a client-side Blob over exactly the bytes in the `<pre>`"
    );
    assert!(
        source.contains("type: \"text/yaml\""),
        "and it carries the plan document's own media type, so a saved file opens as what it is"
    );

    let region = region_of(&source, "// DOWNLOAD-BEGIN", "// DOWNLOAD-END");
    assert!(
        !region.contains("create("),
        "THE DOWNLOAD WRITES NOTHING TO THE CLUSTER. It hands the browser bytes the page \
         already has; an object created by a download handler would be a write an operator \
         did not ask for. The region was:\n{region}"
    );
    assert!(
        !region.contains("fetch("),
        "and it issues no request: `api.js` is the only module in this tree that does"
    );

    // NO URL SCHEME ANYWHERE IN THE FILE. `check-ui-offline.sh` enforces the
    // same rule over the whole directory; this arm names the file the download
    // lives in, so a scheme introduced by a "share this plan" feature fails
    // here with the reason attached.
    for token in [FORBIDDEN[0], FORBIDDEN[1]] {
        assert!(
            !source.contains(token),
            "ui/pages/restore-wizard.js carries {token:?}. Every identifier this page names is \
             relative to the origin that served it; a scheme here is a resource outside the \
             directory the page was served from."
        );
    }
}

// -------------------- 21. the keys page submits nothing (source arm)

#[test]
fn the_keys_page_submits_nothing() {
    let source = read(&ui_root().join("pages").join("keys.js"));
    for token in ["create(", "patchSuspend("] {
        assert!(
            !source.contains(token),
            "ui/pages/keys.js names {token:?}. The TrustRoster is cluster-scoped and \
             admin-only: this page reads it, prints the fingerprint command and surfaces the \
             `kubectl apply` snippet WITHOUT submitting it. `trustrosters` is absent from the \
             frozen writable set in api.js, so the write would throw -- but a page that tried \
             is a page whose contract changed, and that is what this arm notices."
        );
    }
    assert!(
        source.contains("listCluster("),
        "it does still READ the roster; a keys page that read nothing would pass the two \
         assertions above having checked nothing"
    );
    // The behavioural half, with a stub whose writers throw.
    let behaviour = read(&ui_root().join("tests").join("pages.spec.js"));
    assert!(
        behaviour.contains("the_keys_page_submits_nothing"),
        "the stub-api arm lives in ui/tests/pages.spec.js"
    );
}

// -------------------- 22. the prefix default agrees across the two languages

#[test]
fn the_default_prefix_agrees_with_the_rust_one() {
    // THE PAGE PREFILLS A STRING THE RUNNER ALSO COMPUTES, so the two halves
    // are pinned to each other rather than to two independent literals. The
    // page's value reaches `Restore.spec.target.topicNaming.prefix` and the
    // plan document's `target.topic_naming.prefix`; the runner's value is what
    // phase 0 maps every source topic through when a spec states none. A drift
    // between them is a restore whose topics are not the ones the page named.
    let at: chrono::DateTime<chrono::Utc> = "2026-09-07T14:04:59.999Z"
        .parse()
        .expect("the instant parses");
    // The wizard's default point in time is the LAST instant the runner
    // accepts, one millisecond before the covered window's exclusive end
    // (WIZARD-DEFAULT-PIT-EXCLUSIVE); the prefix follows from that instant.
    let rust = logweir_core::spec::default_topic_prefix(at);
    assert_eq!(
        rust, "restore-20260907T140459Z-",
        "default_topic_prefix's own output for that instant"
    );

    let behaviour = read(&ui_root().join("tests").join("pages.spec.js"));
    assert!(
        behaviour.contains(&format!("\"{rust}\"")),
        "ui/tests/pages.spec.js asserts the wizard prefills {rust:?}. If this fails, the node \
         row and this one have drifted and one of the two is now describing a prefix the other \
         half of the product does not produce."
    );

    // The compact form is for KAFKA topic names only -- Kubernetes object
    // names are DNS-1123 and reject the uppercase T and Z, which is why the
    // two MINTED names use a hash suffix and never this.
    assert!(rust.contains('T') && rust.contains('Z'));
}

// -------------------- 23. the secure-context spec stands alone

#[test]
fn the_secure_context_spec_imports_nothing_at_module_scope() {
    // An ES module already imported in a process is NEVER re-evaluated, so
    // `plan.js`'s module-scope refusal can be observed exactly once per
    // process -- and only if nothing has imported it first. A static import
    // anywhere in this file is hoisted above the line that removes
    // `crypto.subtle`, so adding one would leave the test green while
    // asserting nothing, which is the state STANDING RULE 21 calls worse than
    // no guard.
    let spec_path = ui_root().join("tests").join("plan-secure-context.spec.js");
    let source = read(&spec_path);
    for (index, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        assert!(
            !trimmed.starts_with("import ") && !trimmed.starts_with("import{"),
            "{}:{}: a STATIC import. This file may only import dynamically, after it has \
             removed `crypto.subtle` from the global: {line}",
            shown(&spec_path),
            index + 1
        );
    }
    assert!(
        source.contains("await import(\"../plan.js\")")
            || source.contains("import(\"../plan.js\")"),
        "and it must actually import plan.js, dynamically, inside the assertion"
    );
    assert!(
        source.contains("plan_js_refuses_a_non_secure_origin"),
        "the row's name is what `scripts/check-ui-behaviour.sh` reports"
    );
}

// -------------------- 24. the behaviour gate runs only the spec files

/// The runner line `scripts/check-ui-behaviour.sh` passes to node, verbatim.
const RUNNER_GLOB: &str = "'ui/tests/*.spec.js'";

/// The glob it used to pass, and must never pass again.
const RUNNER_GLOB_TOO_WIDE: &str = "'ui/tests/*.js'";

#[test]
fn the_behaviour_gate_runs_only_the_spec_files() {
    // WHY THE SUFFIX IS THE GUARD AND NOT A CONVENTION. `ui/tests/` holds two
    // kinds of file: `*.spec.js` suites, and TOOLS the gate invokes by name --
    // `emit-plan.js`, which prints the plan golden on stdout for the `diff -u`
    // arm. Under `ui/tests/*.js` node executed that tool as a test FILE and
    // counted it as one passing test, so the gate's zero-count refusal could
    // never fire: with every `*.spec.js` deleted the suite still reported
    // `# tests 1` and the gate exited 0. A refusal that cannot reach zero is
    // the state STANDING RULE 21 calls worse than no guard, because the ledger
    // records it as closed -- so the narrow glob is what holds Task 26's
    // refusal, and this arm is what holds the narrow glob.
    let gate_path = repo_root().join("scripts").join("check-ui-behaviour.sh");
    let gate = read(&gate_path);

    assert!(
        gate.contains(RUNNER_GLOB),
        "{} must pass {RUNNER_GLOB} to `node --test`. Anything wider counts a tool in that \
         directory as a test and disarms the zero-count refusal below it.",
        shown(&gate_path)
    );
    assert!(
        !gate.contains(RUNNER_GLOB_TOO_WIDE),
        "{} still names {RUNNER_GLOB_TOO_WIDE}. That glob matches `ui/tests/emit-plan.js`, \
         which is a tool and not a suite; node runs it, reports it as one passing test, and \
         the gate's `# tests 0` refusal is then unreachable by construction.",
        shown(&gate_path)
    );

    // AND THE DIRECTORY HAS TO HOLD SOMETHING THE GLOB MATCHES, or the line
    // above pins a glob over an empty set -- which is the same green-and-empty
    // state from the other side.
    let mut specs: Vec<PathBuf> = Vec::new();
    let mut tools: Vec<PathBuf> = Vec::new();
    for file in behaviour_suite_files() {
        let name = match file.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };
        if name.ends_with(".spec.js") {
            specs.push(file);
        } else if name.ends_with(".js") {
            tools.push(file);
        }
    }
    assert!(
        !specs.is_empty(),
        "ui/tests/ holds no *.spec.js at all, so {RUNNER_GLOB} matches nothing and the gate \
         enumerates nothing"
    );

    // EVERY NON-SUITE `.js` IN THAT DIRECTORY IS REACHED BY NAME. A tool the
    // glob no longer runs and nothing else names is dead -- and a SUITE
    // renamed out of the glob would look exactly like one, which is the way
    // this fix could be undone without touching the line above.
    let specs_text: Vec<String> = specs.iter().map(|p| read(p)).collect();
    let mut orphans: Vec<String> = Vec::new();
    for tool in &tools {
        let name = tool
            .file_name()
            .and_then(|n| n.to_str())
            .expect("the tool's file name is utf-8");
        let named_by_gate = gate.contains(name);
        let named_by_suite = specs_text.iter().any(|s| s.contains(name));
        if !named_by_gate && !named_by_suite {
            orphans.push(shown(tool));
        }
    }
    assert!(
        orphans.is_empty(),
        "these files under ui/tests/ are neither suites the gate's glob runs nor tools \
         anything names. A suite renamed out of {RUNNER_GLOB} disappears from every gate in \
         this repository without a single line of it changing:\n{}",
        orphans.join("\n")
    );
}

// ===========================================================================
// TASK 36 -- the design system, and the fixture-driven preview.
// ===========================================================================

// -------------------- 25. the preview binds loopback and refuses writes

#[test]
fn the_preview_server_binds_loopback_and_refuses_writes() {
    // `ui/tests/preview-server.js` serves the page over checked-in JSON so
    // the design can be looked at with no cluster. It is a development tool
    // and it opens a socket, which is exactly why its two safety properties
    // are pinned from here, over its source, the way every other claim in
    // this file is pinned: it binds the loopback address and nothing else,
    // and it answers every write with a 405 and never stores, forwards or
    // mutates anything.
    let path = ui_root().join("tests").join("preview-server.js");
    let source = read(&path);

    // (i) LOOPBACK, by a module-level constant `listen` is handed by name.
    let declaration = "const HOST = \"127.0.0.1\";";
    assert!(
        source.contains(declaration),
        "{} must bind the loopback literal through a module-level `const HOST`; expected the \
         line {declaration:?}. A preview that listened anywhere else would be a directory \
         listing and a JSON store on the LAN.",
        shown(&path)
    );
    let offset = source.find(declaration).expect("the declaration was found");
    let line_start = source[..offset].rfind('\n').map(|i| i + 1).unwrap_or(0);
    assert_eq!(
        &source[line_start..offset],
        "",
        "the declaration is at module level, not indented inside a function; {}:{}",
        shown(&path),
        line_of(&source, offset)
    );
    assert!(
        source.contains(".listen(PORT, HOST"),
        "and `listen` is handed HOST by name, so the address cannot drift away from the \
         declaration"
    );
    for token in ["0.0.0.0", "[::]", "\"::\""] {
        assert!(
            !source.contains(token),
            "{} carries the wildcard address {token:?}",
            shown(&path)
        );
    }
    assert!(
        contains_bare_token(&source, "127.0.0.1").is_some(),
        "the loopback literal is present as a bare token"
    );

    // (ii) WRITES REFUSED, before anything is looked up, with the code the
    // page renders as the API server's refusal.
    assert!(
        source.contains("const METHOD_NOT_ALLOWED = 405;"),
        "the refusal is a module-level constant"
    );
    assert!(
        contains_bare_token(&source, "405").is_some(),
        "and 405 is the bare token the response carries"
    );
    assert!(
        source.contains("method !== \"GET\" && method !== \"HEAD\""),
        "every method that is not a read is refused"
    );
    for token in [
        "writeFileSync",
        "appendFileSync",
        "createWriteStream",
        "child_process",
    ] {
        assert!(
            !source.contains(token),
            "{} names {token:?}; the preview writes nothing, not even a file, and spawns \
             nothing",
            shown(&path)
        );
    }

    // (iii) A TOOL, NOT A SUITE, AND NOT AN ASSET. The gate's glob cannot
    // match it, the gate never names it, and the shipped-asset scan skips it
    // by the one scope rule this file has.
    assert!(
        !path.to_string_lossy().ends_with(".spec.js"),
        "the preview is not a test file"
    );
    let gate = read(&repo_root().join("scripts").join("check-ui-behaviour.sh"));
    assert!(
        !gate.contains("preview-server"),
        "scripts/check-ui-behaviour.sh must not run the preview: a lint gate never opens a \
         socket (STANDING RULE 7)"
    );
    assert!(is_under_tests(&path), "it lives under ui/tests/");
    assert!(
        !shipped_assets().contains(&path),
        "and is therefore outside the shipped assets and the release bundle"
    );

    // (iv) DOCUMENTED, under its own heading, with the command.
    let readme = read(&ui_root().join("README.md"));
    assert!(
        readme.contains("## Previewing with fixtures"),
        "ui/README.md carries the `Previewing with fixtures` heading"
    );
    assert!(
        readme.contains("node ui/tests/preview-server.js"),
        "and the command that starts it"
    );
}

/// The live console journey and the page agree about the probe panel's
/// control, BY ITS LABEL.
///
/// # The staleness this exists to catch happened
///
/// `scripts/plat12-13-ui-e2e.mjs`'s journey *"a failed probe shows the
/// controller's reason"* asserted `panel.includes("Test connection")` on
/// `#cluster-probe`. `d2-source-check` relabelled that panel's button to
/// `Re-read probe` and moved "Test connection" to the control that really
/// dials — and the assertion KEPT PASSING, because the panel's own sentence
/// still says those words while pointing somewhere else. The lab reported a
/// 20-of-20 run in which one journey had quietly stopped checking that the
/// probe panel offers a control at all (`lab-refresh-4` §10.3).
///
/// A live harness is the wrong place to notice that: it needs a cluster, so it
/// runs rarely, and it asserts on rendered TEXT, which two different sentences
/// can satisfy. This is the cheap half — the page's button label and the
/// journey's expectation are read out of the two sources and compared, in the
/// default `cargo test` suite, so relabelling one without the other is red in
/// seconds rather than a green run that checks less than it says.
#[test]
fn the_live_journey_expects_the_probe_control_the_page_actually_ships() {
    let select = read(&repo_root().join("ui").join("select.js"));
    let harness = read(&repo_root().join("scripts").join("plat12-13-ui-e2e.mjs"));

    // The label the page emits, read out of `renderTestConnection`'s own
    // button rather than out of a constant a test could drift from.
    let at = select
        .find("export function renderTestConnection(")
        .expect("ui/select.js still renders the probe panel's control");
    let body = &select[at..];
    let open = body
        .find("<button type=\\\"submit\\\"")
        .expect("that control is still a submit button");
    let after = &body[open..];
    let start = after.find('>').expect("the button tag closes") + 1;
    let end = after.find("</button>").expect("the button element closes");
    let label = after[start..end].trim().to_string();

    assert!(
        !label.is_empty() && !label.contains('+') && !label.contains("esc("),
        "the probe control's label is no longer a plain literal in \
         `renderTestConnection`, so this guard can no longer read it: `{label}`"
    );

    // The journey's expectation, read out of the one assertion about it.
    let journey = harness
        .find("aFailedProbeShowsTheControllersReason")
        .expect("the journey still exists");
    let scope = &harness[journey..];
    let expected = "check(rereadLabel === \"";
    let e_at = scope.find(expected).unwrap_or_else(|| {
        panic!(
            "the journey no longer compares the probe control's label to an exact string. It \
             asserted `panel.includes(...)` on the panel's TEXT until the label moved under it \
             and the assertion kept passing; reading the BUTTON is what makes it a control \
             assertion, and this guard is what keeps the two in step."
        )
    });
    let rest = &scope[e_at + expected.len()..];
    let wanted = &rest[..rest.find('"').expect("the expected label closes")];

    assert_eq!(
        wanted, label,
        "the live console journey expects the probe panel's button to read `{wanted}` and \
         `ui/select.js` renders `{label}`. One of them was relabelled without the other, which \
         is exactly how that journey came to assert nothing after the `Re-read probe` rename."
    );
}

// ------------------------------------------------ PLAT-18.2: the token layer
//
// THE RULE IS POSITIVE WHERE IT CAN BE (review MEDIUM-1). Tokens are defined
// once, in `ui/style.css`'s `:root` blocks, and every page consumes them. So:
//
//   * a `:root` block may hold `--*` custom properties and `color-scheme`, and
//     nothing else -- no ordinary property, no nested rule;
//   * `:root` exists only in `ui/style.css`; every other `ui/**/*.css` is a
//     rule file like the rest of `style.css`;
//   * outside the token layer a value may carry a number only when it is
//     unitless, a percentage, or one of the non-size units in
//     [`CSS_ALLOWED_UNITS`] (every length unit, known or future, is refused),
//     and no colour spelling at all;
//   * a shipped `.js` or `.html` file may say the word `style` only in running
//     prose, or in the one `href="./style.css"` link -- so no `style`
//     attribute, no `<style>` element, no `.style` / `["style"]` write, however
//     it is spelled or concatenated -- and may carry no presentation attribute
//     (`fill`, `stroke`, `width`, `height`, `color`, `bgcolor`,
//     `stop-color`) and no CSSOM write.

/// The CSS named colours (CSS Color Module Level 4, section 6.1) and the CSS
/// system colours, lower-cased. `transparent` and `currentcolor` name no colour
/// of their own and are not here.
const CSS_NAMED_COLOURS: [&str; 167] = [
    "aliceblue",
    "antiquewhite",
    "aqua",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanchedalmond",
    "blue",
    "blueviolet",
    "brown",
    "burlywood",
    "cadetblue",
    "chartreuse",
    "chocolate",
    "coral",
    "cornflowerblue",
    "cornsilk",
    "crimson",
    "cyan",
    "darkblue",
    "darkcyan",
    "darkgoldenrod",
    "darkgray",
    "darkgreen",
    "darkgrey",
    "darkkhaki",
    "darkmagenta",
    "darkolivegreen",
    "darkorange",
    "darkorchid",
    "darkred",
    "darksalmon",
    "darkseagreen",
    "darkslateblue",
    "darkslategray",
    "darkslategrey",
    "darkturquoise",
    "darkviolet",
    "deeppink",
    "deepskyblue",
    "dimgray",
    "dimgrey",
    "dodgerblue",
    "firebrick",
    "floralwhite",
    "forestgreen",
    "fuchsia",
    "gainsboro",
    "ghostwhite",
    "gold",
    "goldenrod",
    "gray",
    "green",
    "greenyellow",
    "grey",
    "honeydew",
    "hotpink",
    "indianred",
    "indigo",
    "ivory",
    "khaki",
    "lavender",
    "lavenderblush",
    "lawngreen",
    "lemonchiffon",
    "lightblue",
    "lightcoral",
    "lightcyan",
    "lightgoldenrodyellow",
    "lightgray",
    "lightgreen",
    "lightgrey",
    "lightpink",
    "lightsalmon",
    "lightseagreen",
    "lightskyblue",
    "lightslategray",
    "lightslategrey",
    "lightsteelblue",
    "lightyellow",
    "lime",
    "limegreen",
    "linen",
    "magenta",
    "maroon",
    "mediumaquamarine",
    "mediumblue",
    "mediumorchid",
    "mediumpurple",
    "mediumseagreen",
    "mediumslateblue",
    "mediumspringgreen",
    "mediumturquoise",
    "mediumvioletred",
    "midnightblue",
    "mintcream",
    "mistyrose",
    "moccasin",
    "navajowhite",
    "navy",
    "oldlace",
    "olive",
    "olivedrab",
    "orange",
    "orangered",
    "orchid",
    "palegoldenrod",
    "palegreen",
    "paleturquoise",
    "palevioletred",
    "papayawhip",
    "peachpuff",
    "peru",
    "pink",
    "plum",
    "powderblue",
    "purple",
    "rebeccapurple",
    "red",
    "rosybrown",
    "royalblue",
    "saddlebrown",
    "salmon",
    "sandybrown",
    "seagreen",
    "seashell",
    "sienna",
    "silver",
    "skyblue",
    "slateblue",
    "slategray",
    "slategrey",
    "snow",
    "springgreen",
    "steelblue",
    "tan",
    "teal",
    "thistle",
    "tomato",
    "turquoise",
    "violet",
    "wheat",
    "white",
    "whitesmoke",
    "yellow",
    "yellowgreen",
    // CSS Color 4 section 6.2, system colours: a colour the page does not choose.
    "accentcolor",
    "accentcolortext",
    "activetext",
    "buttonborder",
    "buttonface",
    "buttontext",
    "canvas",
    "canvastext",
    "field",
    "fieldtext",
    "graytext",
    "highlight",
    "highlighttext",
    "linktext",
    "mark",
    "marktext",
    "selecteditem",
    "selecteditemtext",
    "visitedtext",
];

/// The colour functions. `var(` is not one: it is how a value READS a token.
const CSS_COLOUR_FUNCTIONS: [&str; 11] = [
    "rgb(",
    "rgba(",
    "hsl(",
    "hsla(",
    "hwb(",
    "lab(",
    "lch(",
    "oklab(",
    "oklch(",
    "color(",
    "color-mix(",
];

/// THE ONLY UNITS A NUMBER OUTSIDE THE TOKEN LAYER MAY CARRY: a grid track's
/// share, an angle, a time. Every length unit -- `px`, `rem`, `lh`, `dvh`,
/// `cqi`, `cap`, and whatever CSS adds next -- is refused because it is not
/// on this list, not because it is on another one.
const CSS_ALLOWED_UNITS: [&str; 7] = ["fr", "deg", "turn", "rad", "grad", "s", "ms"];

/// The properties a `:root` block may set besides `--*` custom properties.
const TOKEN_LAYER_PROPERTIES: [&str; 1] = ["color-scheme"];

/// One declaration or nested rule of a stylesheet.
#[derive(Debug)]
struct CssDeclaration {
    line: usize,
    property: String,
    value: String,
    in_token_layer: bool,
}

/// A stylesheet read into declarations, with the rules nested inside a
/// `:root` block recorded as findings of their own.
#[derive(Debug, Default)]
struct CssRead {
    declarations: Vec<CssDeclaration>,
    root_blocks: usize,
    nested_in_root: Vec<(usize, String)>,
}

/// Reads `css` into declarations. Comments and quoted strings are blanked, so
/// neither can carry, or hide, a literal. A declaration is what ends at `;`
/// or `}`; a prelude is what ends at `{` -- so a media condition or a
/// selector is never read as a value. A block is in the token layer when its
/// OWN prelude is exactly `:root` (possibly under an at-rule); a rule nested
/// inside a `:root` block is NOT token layer and is recorded as a finding.
fn read_css(css: &str) -> CssRead {
    let mut text = String::with_capacity(css.len());
    let chars: Vec<char> = css.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            while i < chars.len()
                && !(chars[i] == '*' && i + 1 < chars.len() && chars[i + 1] == '/')
            {
                text.push(if chars[i] == '\n' { '\n' } else { ' ' });
                i += 1;
            }
            i += 2;
            text.push_str("  ");
            continue;
        }
        text.push(chars[i]);
        i += 1;
    }

    // Each open block: is it a `:root` block?
    let mut stack: Vec<bool> = Vec::new();
    let mut out = CssRead::default();
    let mut current = String::new();
    let mut start_line = 1usize;
    let mut line = 1usize;
    let mut quote: Option<char> = None;
    for c in text.chars() {
        if c == '\n' {
            line += 1;
        }
        if let Some(q) = quote {
            if c == q {
                quote = None;
                current.push(c);
            } else {
                current.push(if c == '\n' { '\n' } else { ' ' });
            }
            continue;
        }
        match c {
            '"' | '\'' => {
                quote = Some(c);
                current.push(c);
            }
            '{' => {
                let prelude = current.trim().to_string();
                if stack.iter().any(|root| *root) {
                    out.nested_in_root.push((start_line, prelude.clone()));
                }
                let is_root = prelude == ":root";
                if is_root {
                    out.root_blocks += 1;
                }
                stack.push(is_root);
                current.clear();
            }
            ';' | '}' => {
                let decl = current.trim();
                if let Some(colon) = decl.find(':') {
                    let property = decl[..colon].trim().to_ascii_lowercase();
                    if !property.is_empty() && !property.contains(char::is_whitespace) {
                        out.declarations.push(CssDeclaration {
                            line: start_line,
                            property,
                            value: decl[colon + 1..].trim().to_string(),
                            in_token_layer: stack.last().copied().unwrap_or(false),
                        });
                    }
                }
                if c == '}' {
                    stack.pop();
                }
                current.clear();
            }
            _ => {
                if current.trim().is_empty() && !c.is_whitespace() {
                    start_line = line;
                }
                current.push(c);
            }
        }
    }
    out
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// Why a value outside the token layer is a literal, or `None`.
fn css_literal_in(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    for f in CSS_COLOUR_FUNCTIONS {
        let mut from = 0;
        while let Some(at) = lower[from..].find(f) {
            let at = from + at;
            let before = lower[..at].chars().last();
            if before.map(|b| !is_ident_char(b)).unwrap_or(true) {
                return Some(format!("a colour function `{f}`"));
            }
            from = at + f.len();
        }
    }
    let chars: Vec<char> = lower.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let prev = if i == 0 { None } else { Some(chars[i - 1]) };
        if c == '#' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_hexdigit() {
                j += 1;
            }
            let n = j - i - 1;
            let ends = j >= chars.len() || !is_ident_char(chars[j]);
            if (3..=8).contains(&n) && ends {
                return Some(format!(
                    "a hex colour `{}`",
                    chars[i..j].iter().collect::<String>()
                ));
            }
        }
        // A number: digits (or `.digits`) not glued to the end of an
        // identifier -- `-0.05em` and `.5rem` are numbers, `h2` is not.
        let digit_start = c.is_ascii_digit()
            || (c == '.'
                && chars
                    .get(i + 1)
                    .map(|d| d.is_ascii_digit())
                    .unwrap_or(false));
        let glued = match prev {
            Some(p) if p.is_ascii_alphanumeric() || p == '_' => true,
            Some('-') => i >= 2 && (chars[i - 2].is_ascii_alphanumeric() || chars[i - 2] == '_'),
            _ => false,
        };
        if digit_start && !glued {
            let mut j = i;
            while j < chars.len() && (chars[j].is_ascii_digit() || chars[j] == '.') {
                j += 1;
            }
            // An exponent is part of the number.
            if j < chars.len()
                && chars[j] == 'e'
                && chars
                    .get(j + 1)
                    .map(|d| d.is_ascii_digit() || *d == '-' || *d == '+')
                    .unwrap_or(false)
            {
                j += 2;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
            }
            let mut k = j;
            while k < chars.len() && chars[k].is_ascii_alphabetic() {
                k += 1;
            }
            let unit: String = chars[j..k].iter().collect();
            if !unit.is_empty() && !CSS_ALLOWED_UNITS.contains(&unit.as_str()) {
                return Some(format!(
                    "a dimension `{}` whose unit is not one a rule may carry",
                    chars[i..k].iter().collect::<String>()
                ));
            }
            i = k.max(i + 1);
            continue;
        }
        if c.is_ascii_alphabetic() && prev.map(|p| !is_ident_char(p)).unwrap_or(true) {
            let mut j = i;
            while j < chars.len() && is_ident_char(chars[j]) {
                j += 1;
            }
            let word: String = chars[i..j].iter().collect();
            if CSS_NAMED_COLOURS.contains(&word.as_str()) {
                return Some(format!("the colour name `{word}`"));
            }
            i = j;
            continue;
        }
        i += 1;
    }
    None
}

/// Every finding in one stylesheet. `tokens_allowed` is true for
/// `ui/style.css` only: the one file where the token layer lives.
fn stylesheet_findings(file: &str, css: &str, tokens_allowed: bool) -> Vec<String> {
    let read = read_css(css);
    let mut findings = Vec::new();
    for (line, prelude) in &read.nested_in_root {
        findings.push(format!(
            "{file}:{line}: the rule `{prelude}` is nested inside a `:root` block; the token \
             layer holds custom properties only"
        ));
    }
    if !tokens_allowed && read.root_blocks > 0 {
        findings.push(format!(
            "{file}: declares a `:root` block; tokens are defined once, in ui/style.css"
        ));
    }
    for d in &read.declarations {
        if d.in_token_layer {
            if !d.property.starts_with("--")
                && !TOKEN_LAYER_PROPERTIES.contains(&d.property.as_str())
            {
                findings.push(format!(
                    "{file}:{}: `{}` is set in a `:root` block, which may hold custom properties \
                     and `color-scheme` only",
                    d.line, d.property
                ));
            }
            continue;
        }
        if d.property.starts_with("--") {
            findings.push(format!(
                "{file}:{}: `{}` declares a token outside the `:root` token layer",
                d.line, d.property
            ));
            continue;
        }
        if let Some(why) = css_literal_in(&d.value) {
            findings.push(format!(
                "{file}:{}: `{}: {}` carries {why}; read it from a token (`var(--cds-...)`) instead",
                d.line, d.property, d.value
            ));
        }
    }
    findings
}

/// Backward-compatible name for the stylesheet check the older rows call.
fn token_layer_findings(file: &str, css: &str) -> Vec<String> {
    stylesheet_findings(file, css, true)
}

/// A script or page's source with ADJACENT STRING LITERALS JOINED -- `"st" +
/// "yle"` becomes `"style"` -- so a concatenation cannot split a word the
/// rules below read. Line breaks inside the join are kept as line breaks so
/// line numbers still point at the right place.
fn joined_literals(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' || c == '`' {
            // Is this a closing quote followed by `+` and an opening quote?
            let mut j = i + 1;
            let mut newlines = 0;
            while j < chars.len() && chars[j].is_whitespace() {
                if chars[j] == '\n' {
                    newlines += 1;
                }
                j += 1;
            }
            if j < chars.len() && chars[j] == '+' {
                let mut k = j + 1;
                while k < chars.len() && chars[k].is_whitespace() {
                    if chars[k] == '\n' {
                        newlines += 1;
                    }
                    k += 1;
                }
                if k < chars.len() && (chars[k] == '"' || chars[k] == '\'' || chars[k] == '`') {
                    for _ in 0..newlines {
                        out.push('\n');
                    }
                    i = k + 1;
                    continue;
                }
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

/// The words `style` may appear in, in a shipped script or page: running
/// prose (a letter, a space, the word, then a space and a lower-case letter,
/// or a comma or full stop and a space) and the one stylesheet link.
fn style_word_allowed(text: &str, at: usize) -> bool {
    let bytes = text.as_bytes();
    let before = |n: usize| if at >= n { Some(bytes[at - n]) } else { None };
    let after = |n: usize| bytes.get(at + 5 + n - 1).copied();
    if text[..at].ends_with("href=\"./") && text[at..].starts_with("style.css\"") {
        return true;
    }
    let prose_before =
        before(1) == Some(b' ') && before(2).map(|b| b.is_ascii_alphabetic()).unwrap_or(false);
    let prose_after = match after(1) {
        Some(b' ') => after(2).map(|b| b.is_ascii_lowercase()).unwrap_or(false),
        Some(b',') | Some(b'.') => after(2) == Some(b' '),
        _ => false,
    };
    prose_before && prose_after
}

/// The presentation attributes a shipped page may not set, as markup
/// (`name="..."`) or through `setAttribute`/`setAttributeNS`: colour and size
/// belong to the stylesheet's classes and tokens. `style` is not listed: the
/// word rule refuses it wherever it is code, `setAttribute("style", ...)`
/// included.
const PRESENTATION_ATTRIBUTES: [&str; 16] = [
    "fill",
    "stroke",
    "width",
    "height",
    "color",
    "bgcolor",
    "stop-color",
    "stroke-width",
    "fill-opacity",
    "stroke-opacity",
    "opacity",
    "font-size",
    "font-family",
    "font-weight",
    "background",
    "border",
];

/// CSSOM entry points that write a style without the word `style` in them.
/// `attributeStyleMap` and `.styleMap` are CSS Typed OM: an element's (or a
/// rule's) style map, written with `.set(`/`.append(`. Naming the map at all
/// is refused, so a write through an alias of it is refused too.
const CSSOM_WRITES: [&str; 7] = [
    "cssText",
    "insertRule",
    "adoptedStyleSheets",
    "CSSStyleSheet",
    ".setProperty(",
    "attributeStyleMap",
    ".styleMap",
];

/// Every `setAttribute("<presentation attribute>", ...)` and
/// `setAttributeNS(ns, "<presentation attribute>", ...)` in a script whose
/// attribute NAME is a literal (after [`joined_literals`], so `"fi" + "ll"` is
/// `"fill"`). The value is not read: a presentation attribute sizes or colours
/// an element outside the token layer whatever it is set to. A name held in a
/// variable is deliberate evasion no textual lint can close.
fn set_attribute_findings(file: &str, text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let line_of_at = |at: usize| text[..at].matches('\n').count() + 1;
    let mut findings = Vec::new();
    let mut from = 0;
    while let Some(found) = lower[from..].find("setattribute") {
        let at = from + found;
        from = at + "setattribute".len();
        let mut i = from;
        let ns = lower[i..].starts_with("ns");
        if ns {
            i += 2;
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if bytes.get(i) != Some(&b'(') {
            continue;
        }
        i += 1;
        if ns {
            // Skip the namespace argument: up to the first comma.
            match lower[i..].find(',') {
                Some(comma) => i += comma + 1,
                None => continue,
            }
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let Some(&quote) = bytes.get(i) else {
            continue;
        };
        if quote != b'"' && quote != b'\'' && quote != b'`' {
            continue;
        }
        let Some(len) = lower[i + 1..].find(quote as char) else {
            continue;
        };
        let name = lower[i + 1..i + 1 + len].trim();
        if PRESENTATION_ATTRIBUTES.contains(&name) {
            let line = line_of_at(at);
            findings.push(format!(
                "{file}:{line}: `setAttribute` of the presentation attribute `{name}`: size and \
                 colour an element by class; {}",
                text.lines().nth(line - 1).unwrap_or("").trim()
            ));
        }
    }
    findings
}

/// Every inline style, style element, CSSOM write and presentation attribute
/// in one shipped `.js` or `.html` file.
fn inline_style_findings(file: &str, source: &str) -> Vec<String> {
    // Comment lines are documentation: they may say anything. (A trailing
    // comment on a code line is scanned, and must be worded accordingly.)
    let code: String = source
        .lines()
        .map(|l| {
            let t = l.trim_start();
            if t.starts_with("//") || t.starts_with('*') || t.starts_with("/*") {
                ""
            } else {
                l
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let text = joined_literals(&code);
    let line_of_at = |at: usize| text[..at].matches('\n').count() + 1;
    let lower = text.to_ascii_lowercase();
    let mut findings = Vec::new();

    let mut from = 0;
    while let Some(found) = lower[from..].find("style") {
        let at = from + found;
        let before = if at == 0 {
            None
        } else {
            lower.as_bytes().get(at - 1).copied()
        };
        let after = lower.as_bytes().get(at + 5).copied();
        let word = before
            .map(|b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'$'))
            .unwrap_or(true)
            && after
                .map(|b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'$'))
                .unwrap_or(true);
        if word && !style_word_allowed(&text, at) {
            let line = line_of_at(at);
            findings.push(format!(
                "{file}:{line}: the word `style` in code (a style attribute, a <style> element or a \
                 style write): {}",
                text.lines().nth(line - 1).unwrap_or("").trim()
            ));
        }
        from = at + 5;
    }

    for attribute in PRESENTATION_ATTRIBUTES {
        let mut from = 0;
        while let Some(found) = lower[from..].find(attribute) {
            let at = from + found;
            from = at + attribute.len();
            let before = if at == 0 {
                None
            } else {
                lower.as_bytes().get(at - 1).copied()
            };
            if !before
                .map(|b| b.is_ascii_whitespace() || b == b'"' || b == b'\'')
                .unwrap_or(true)
            {
                continue;
            }
            let rest = lower[at + attribute.len()..].trim_start();
            let Some(value) = rest.strip_prefix('=') else {
                continue;
            };
            let value = value.trim_start();
            if value.starts_with('"')
                || value.starts_with('\'')
                || value.starts_with('`')
                || value.starts_with("\\\"")
                || value.starts_with("${")
            {
                let line = line_of_at(at);
                findings.push(format!(
                    "{file}:{line}: the presentation attribute `{attribute}=`: size and colour an \
                     element by class; {}",
                    text.lines().nth(line - 1).unwrap_or("").trim()
                ));
            }
        }
    }

    findings.extend(set_attribute_findings(file, &text));

    for token in CSSOM_WRITES {
        if let Some(at) = text.find(token) {
            findings.push(format!(
                "{file}:{}: a CSSOM style write (`{token}`)",
                line_of_at(at)
            ));
        }
    }
    findings
}

#[test]
fn no_literal_colour_or_size_outside_the_token_layer() {
    // PLAT-18.2, the Clarity design standard: tokens are defined once, in
    // `ui/style.css`'s `:root` blocks, and every page consumes them.
    let css_path = ui_root().join("style.css");
    let css = read(&css_path);
    let read_back = read_css(&css);
    let tokens = read_back
        .declarations
        .iter()
        .filter(|d| d.in_token_layer)
        .count();
    let rules = read_back
        .declarations
        .iter()
        .filter(|d| !d.in_token_layer)
        .count();
    assert!(
        tokens >= 150 && rules >= 300 && read_back.root_blocks >= 2,
        "the parser found {tokens} token declaration(s), {rules} rule declaration(s) and {} \
         `:root` block(s) in ui/style.css; a lint that read almost nothing is not a pass",
        read_back.root_blocks
    );

    let mut findings = Vec::new();
    // TWO `:root` BLOCKS: the light layer and the dark one. A third is a
    // second place a token could be defined.
    if read_back.root_blocks != 2 {
        findings.push(format!(
            "ui/style.css: {} `:root` blocks; the token layer is exactly two (light, and dark \
             under prefers-color-scheme)",
            read_back.root_blocks
        ));
    }
    let mut stylesheets = 0;
    let mut pages = 0;
    for asset in shipped_assets() {
        let ext = asset.extension().and_then(|e| e.to_str()).unwrap_or("");
        let body = read(&asset);
        if ext == "css" {
            stylesheets += 1;
            findings.extend(stylesheet_findings(
                &shown(&asset),
                &body,
                asset == css_path,
            ));
        }
        if ext == "js" || ext == "html" {
            pages += 1;
            findings.extend(inline_style_findings(&shown(&asset), &body));
        }
    }
    assert!(
        stylesheets >= 1 && pages >= 20,
        "{stylesheets} stylesheet(s), {pages} script(s)/page(s)"
    );
    assert!(
        findings.is_empty(),
        "literal colours, sizes, style writes or presentation attributes outside the token \
         layer:\n  {}",
        findings.join("\n  ")
    );
}

#[test]
fn the_token_lint_refuses_each_kind_of_literal() {
    // The RED side, one mutant per rule, so the guard above is a guard -- and
    // the twelve escapes the PLAT-18.2 review planted, each by name.
    let flagged = |css: &str| token_layer_findings("mutant.css", css).len();
    let other = |css: &str| stylesheet_findings("other.css", css, false).len();
    assert_eq!(flagged(".a { color: #0072a3; }"), 1, "a hex colour");
    assert_eq!(flagged(".a { color: #fff; }"), 1, "a short hex colour");
    assert_eq!(
        flagged(".a { background: hsl(198, 100%, 34%); }"),
        1,
        "a colour function"
    );
    assert_eq!(
        flagged(".a { border-color: rgba(0, 0, 0, 0.5); }"),
        1,
        "rgba"
    );
    assert_eq!(flagged(".a { color: red; }"), 1, "a named colour");
    assert_eq!(
        flagged(".a { outline-color: Highlight; }"),
        1,
        "a system colour"
    );
    assert_eq!(flagged(".a { margin: 4px 0; }"), 1, "a px length");
    assert_eq!(
        flagged(".a { padding: .5rem; }"),
        1,
        "a bare-decimal rem length"
    );
    assert_eq!(
        flagged(".a { letter-spacing: -0.05em; }"),
        1,
        "a negative em length"
    );
    assert_eq!(
        flagged(".a { border: 1px solid var(--x); }"),
        1,
        "a border width"
    );
    assert_eq!(
        flagged(".a { --lw-x: 1rem; }"),
        1,
        "a token declared outside :root"
    );
    assert_eq!(
        flagged("@media (max-width: 1px) { .a { gap: 2rem; } }"),
        1,
        "a length inside a media block is still a rule, not a token"
    );
    // Review MEDIUM-1, escapes 1-3 (stylesheet).
    assert_eq!(
        flagged(":root { .crumb { color: #ff0000; margin-top: 3px; } }"),
        3,
        "escape 1: a rule nested in :root is not token layer (the nesting, the colour, the size)"
    );
    assert_eq!(
        flagged(":root { background: red; font-size: 13px; }"),
        2,
        "escape 2: an ordinary property set in :root"
    );
    assert_eq!(
        flagged(".crumb { padding-top: 3lh; height: 10dvh; }"),
        2,
        "escape 3: units the old deny-list lacked"
    );
    assert_eq!(
        flagged(".a { inline-size: 3cqi; block-size: 2cap; }"),
        2,
        "container and font units"
    );
    assert_eq!(
        flagged(".a { width: 3zz; }"),
        1,
        "a unit nobody has invented yet"
    );
    assert_eq!(
        other(":root { --x: 1px; }"),
        1,
        "a :root block anywhere but ui/style.css"
    );
    assert_eq!(
        other(".a { color: #fff; }"),
        1,
        "a literal in another stylesheet"
    );
    // And the GREEN side: what the stylesheet is allowed to say.
    assert_eq!(
        flagged(":root { --a: #fff; --b: 4px; color-scheme: light dark; }"),
        0,
        "the token layer"
    );
    assert_eq!(
        flagged("@media (prefers-color-scheme: dark) { :root { --a: hsl(0, 0%, 0%); } }"),
        0,
        "the dark token layer"
    );
    assert_eq!(
        flagged(".a { color: var(--a); margin: 0; width: 100%; }"),
        0,
        "var, zero, %"
    );
    assert_eq!(
        flagged("@media (max-width: 767.98px) { .a { display: block; } }"),
        0,
        "a breakpoint"
    );
    assert_eq!(
        flagged(".a { content: \"#fff 4px red\"; }"),
        0,
        "a quoted string is content"
    );
    assert_eq!(
        flagged(".a { white-space: nowrap; line-height: 1.5; }"),
        0,
        "unitless values"
    );
    assert_eq!(
        flagged(".a { transform: rotate(360deg); z-index: 20; }"),
        0,
        "angles and indexes"
    );
    assert_eq!(
        flagged(".a { transition: color 0.1s ease; grid-row: 1 / span 2; }"),
        0,
        "times, grid lines"
    );
    assert_eq!(
        flagged(".a { grid-template-columns: repeat(6, 1fr); }"),
        0,
        "fr tracks"
    );
    assert_eq!(
        flagged(".a h2 { margin: 0; }"),
        0,
        "a digit inside an identifier is not a number"
    );

    let inline = |js: &str| inline_style_findings("mutant.js", js).len();
    assert_eq!(
        inline("row.style.display = \"none\";"),
        1,
        "a style property write"
    );
    assert_eq!(
        inline("return \"<p style=\\\"color: red\\\">\";"),
        1,
        "a style attribute in a string"
    );
    // Review MEDIUM-1, escapes 4-12 (scripts and pages).
    assert_eq!(
        inline("node.setAttribute(\"style\", \"color: red\");"),
        1,
        "escape 4: setAttribute"
    );
    assert_eq!(
        inline("node.style = \"color: red\";"),
        1,
        "escape 5: assigning .style"
    );
    assert_eq!(
        inline("Object.assign(node.style, {color: \"red\"});"),
        1,
        "escape 6: Object.assign"
    );
    assert_eq!(
        inline("node[\"style\"].color = \"red\";"),
        1,
        "escape 7: a bracketed style"
    );
    assert_eq!(
        inline("const x = `<p style=${q}color: red${q}>`;"),
        1,
        "escape 8: a template"
    );
    assert_eq!(
        inline("const x = \"<p st\" + \"yle=\\\"color: red\\\">\";"),
        1,
        "escape 9: a concatenated word"
    );
    assert_eq!(
        inline(
            "const x = \"<svg><rect fill=\\\"#ff0000\\\" width=\\\"12\\\" height=\\\"12\\\">\";"
        ),
        3,
        "escape 10: SVG presentation attributes"
    );
    assert_eq!(
        inline("document.createElement(\"style\").textContent = \".x{color:#ff0000}\";"),
        1,
        "escape 11: a <style> element built in a script"
    );
    assert_eq!(
        inline("<head>\n<style>.x { color: #ff0000; margin: 3px; }</style>\n</head>"),
        2,
        "escape 12: a <style> element in a page (its open and close tags)"
    );
    assert_eq!(
        inline("node.style.cssText = \"\";"),
        2,
        "cssText, and the word"
    );
    assert_eq!(inline("sheet.insertRule(\".x{}\");"), 1, "insertRule");
    // PLAT-18.2 re-check LOW-R1: variants 14 and 15 of the review, which the
    // lint did not see.
    assert_eq!(
        inline("node.attributeStyleMap.set(\"color\", \"red\");"),
        1,
        "variant 15: a CSS Typed OM write"
    );
    assert_eq!(
        inline("node.attributeStyleMap.append(\"margin\", CSS.px(3));"),
        1,
        "a Typed OM append"
    );
    assert_eq!(
        inline("const m = node.attributeStyleMap;\nm.set(\"width\", CSS.px(12));"),
        1,
        "a Typed OM write through an alias of the map"
    );
    assert_eq!(
        inline("sheet.cssRules[0].styleMap.set(\"color\", \"red\");"),
        1,
        "a rule's Typed OM style map"
    );
    assert_eq!(
        inline("node.setAttribute(\"fill\", \"#ff0000\");"),
        1,
        "variant 14: setAttribute of fill"
    );
    assert_eq!(
        inline("node.setAttribute(\"width\", \"12\");"),
        1,
        "variant 14: setAttribute of width"
    );
    for attribute in PRESENTATION_ATTRIBUTES {
        assert_eq!(
            inline(&format!("node.setAttribute('{attribute}', value);")),
            1,
            "setAttribute of {attribute}, whatever the value"
        );
    }
    assert_eq!(
        inline("node.setAttributeNS(null, \"stroke\", \"#000\");"),
        1,
        "setAttributeNS of stroke"
    );
    assert_eq!(
        inline("node.setAttribute ( `Height` , \"4\");"),
        1,
        "spacing, a template literal and case do not hide the name"
    );
    assert_eq!(
        inline("node.setAttribute(\"fi\" + \"ll\", \"red\");"),
        1,
        "a concatenated attribute name"
    );
    assert_eq!(
        inline("node.setAttribute(\"style\", \"color: red\");"),
        1,
        "setAttribute of style is refused by the word rule, once"
    );
    // The GREEN side.
    assert_eq!(
        inline(
            "node.setAttribute(\"data-label\", x); node.setAttribute(\"aria-sort\", d); \
             node.setAttribute(\"tabindex\", \"0\"); node.setAttribute(key, String(v));"
        ),
        0,
        "setAttribute of a non-presentation attribute, or of a name held in a variable"
    );
    assert_eq!(
        inline("node.setAttribute(\"data-width\", \"12\");"),
        0,
        "a data attribute that ends in a presentation word"
    );
    assert_eq!(
        inline("// a comment may mention .style. freely"),
        0,
        "a comment line"
    );
    assert_eq!(
        inline("\"not the addressing style, not the endpoint\""),
        0,
        "the word in running prose"
    );
    assert_eq!(
        inline("<link rel=\"stylesheet\" href=\"./style.css\">"),
        0,
        "the one stylesheet link"
    );
    assert_eq!(
        inline("const pathStyle = x.path_style; const height = 5;"),
        0,
        "identifiers, a height variable"
    );
}
