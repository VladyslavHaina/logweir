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
const API_EXPORTS: [&str; 10] = [
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

    for token in ["DELETE", "PUT"] {
        if let Some(offset) = contains_bare_token(&contents, token) {
            panic!(
                "api.js:{} carries the bare token {token:?}. The module exports `create` plus \
                 exactly one update; a delete or a replace here would be a write the page's \
                 own contract says it cannot make. The token is matched BARE, so neither \
                 quote style hides it.",
                line_of(&contents, offset)
            );
        }
    }

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

/// The three tokens the wizard's SUBMIT REGION may not carry, plus the plan
/// document's file extension, which is the fourth.
///
/// WHY THIS LIST AND NOT A GREP FOR "TRANSFORMATION". Each of these is an
/// entry point that produces a NEW string from the plan bytes, and a new
/// string is a new document with a new sha256 -- which is a document the
/// approver never signed. JavaScript has no string identity operator, so a
/// behavioural `===` arm holds under a mutant that reserialised to an equal
/// string; the byte comparison in `pages.spec.js` catches the mutants that
/// change the bytes and this scan catches the machinery that could.
const RESERIALISERS: [&str; 6] = [
    "JSON.parse",
    "JSON.stringify",
    "structuredClone",
    ".trim(",
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

    // AND THE SUBMIT REGION CARRIES NONE OF THE SIX. `.trim(` reads a form
    // control outside the region and the plan document's extension appears
    // only in the download handler, which is why the scan is a region and not
    // the whole file -- and why both markers are asserted present above.
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
    let at: chrono::DateTime<chrono::Utc> =
        "2026-09-07T14:05:00Z".parse().expect("the instant parses");
    let rust = logweir_core::spec::default_topic_prefix(at);
    assert_eq!(
        rust, "restore-20260907T140500Z-",
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
