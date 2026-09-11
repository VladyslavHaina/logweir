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

    assert_eq!(
        exports,
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

    // Three copies of one string is a thing that drifts, so the last assertion
    // is that every serving line in all three files is the SAME line, not that
    // each of them looks plausible on its own.
    let mut spellings: BTreeSet<String> = BTreeSet::new();
    for source in [&readme, &kubernetes, &justfile] {
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
         docs/kubernetes.md and the `ui` recipe. One of those is the one an adopter will \
         copy, and there is no way to tell which."
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
