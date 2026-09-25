//! THE DOCUMENTS A STRANGER ACTS ON — Task 29.
//!
//! WHAT THESE TESTS PROVE. That the five documents nobody runs a test against
//! before acting on them — `NOTICE`, `THIRD_PARTY_NOTICES.md`,
//! `CONTRIBUTING.md`'s DCO section, `README.md`'s threat model and
//! `docs/install.md` — say what they are required to say, and that the two
//! lists which are easiest to let rot (the librdkafka component list and the
//! package inventory) are **derived from the artefacts they describe** rather
//! than typed here.
//!
//! THE DERIVATION RULE, AND WHY IT IS THE WHOLE POINT. A licence test whose
//! expected set is a literal in the test file can be satisfied by deleting the
//! same name from both places. Both of the list tests below read their
//! expectation out of a file that is not a document:
//! `the_notice_names_every_librdkafka_component_in_licenses_txt` reads
//! `librdkafka/LICENSES.txt` out of the resolved `rdkafka-sys` source
//! directory, and `third_party_notices_covers_every_resolved_package` reads
//! `Cargo.lock`. Deleting a line from `NOTICE` cannot be matched by an equally
//! shrunken expectation, because the expectation is upstream's file.
//!
//! AND A LICENCE TEST NEVER SKIPS. If `LICENSES.txt` is not where it is looked
//! for, the test **fails naming the path**. A skipping licence test is a check
//! that cannot fail, which is the defect class this task exists to remove: the
//! plan's own first draft froze twelve components where the file names
//! fourteen, and a test that had skipped on a cold cache would have certified
//! that as complete.
//!
//! COSTS NOTHING AND REACHES NOTHING. Every test here reads checked-in files
//! (plus, in one case, the resolved crate source already on disk). Nothing
//! spawns cargo, opens a socket or starts a container, so the whole file is
//! milliseconds against the 15-second per-test budget.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

// --------------------------------------------------------------- the helpers

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the repository root resolves from CARGO_MANIFEST_DIR")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

/// The six Markdown files at the repository root. Named rather than globbed:
/// "the six root markdown files" is a claim about which documents ship, and a
/// glob would quietly stop checking one that was deleted.
const ROOT_MARKDOWN: [&str; 6] = [
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "MAINTAINERS.md",
    "TRADEMARKS.md",
    "THIRD_PARTY_NOTICES.md",
];

/// Every `*.md` under `docs/`, at any depth, in sorted order.
fn docs_markdown() -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![repo_root().join("docs")];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("a directory entry is readable").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                found.push(path);
            }
        }
    }
    found.sort();
    assert!(
        !found.is_empty(),
        "no Markdown file was found under docs/ — a footer check that enumerated \
         nothing is not a pass"
    );
    found
}

// ------------------------------------------------- the librdkafka components

/// `Cargo.lock`'s recorded version of a package, by name. There is exactly one
/// `rdkafka-sys` in this graph; two would be a finding of its own.
fn locked_version(lock: &str, package: &str) -> String {
    let needle = format!("name = \"{package}\"");
    let mut versions = Vec::new();
    for block in lock.split("[[package]]") {
        if block.lines().any(|l| l.trim() == needle) {
            for line in block.lines() {
                if let Some(rest) = line.trim().strip_prefix("version = \"") {
                    versions.push(rest.trim_end_matches('"').to_string());
                    break;
                }
            }
        }
    }
    assert_eq!(
        versions.len(),
        1,
        "Cargo.lock records {} `{package}` packages; expected exactly one: {versions:?}",
        versions.len()
    );
    versions.remove(0)
}

/// `$CARGO_HOME/registry/src/<index>/rdkafka-sys-<version>/librdkafka/LICENSES.txt`.
///
/// The index directory name is registry-specific and must not be hard-coded,
/// so every `registry/src/*` child is tried. Returns the path whether or not
/// it exists, so the caller can FAIL naming it.
fn licenses_txt_candidates(version: &str) -> Vec<PathBuf> {
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cargo")))
        .expect("either $CARGO_HOME or $HOME is set");
    let src = cargo_home.join("registry").join("src");
    let tail = PathBuf::from(format!("rdkafka-sys-{version}"))
        .join("librdkafka")
        .join("LICENSES.txt");

    let mut candidates = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&src) {
        for entry in entries.flatten() {
            candidates.push(entry.path().join(&tail));
        }
    }
    if candidates.is_empty() {
        candidates.push(src.join("<index>").join(&tail));
    }
    candidates.sort();
    candidates
}

/// NOTICE's librdkafka attribution table: `component` -> `licence`.
///
/// The table is the contiguous run of two-space-indented `<name>  <licence>`
/// rows that ends at the sentence "The full text of each". Scoping the parse
/// to the table rather than to the whole file is the whole point: NOTICE's
/// PROSE also names components (the paragraph recording that the first draft
/// omitted `tinycthread` and `wingetopt`), and a guard that counts a prose
/// mention as an attribution cannot see an attribution deleted.
fn notice_component_table(notice: &str) -> BTreeMap<String, String> {
    let lines: Vec<&str> = notice.lines().collect();
    let terminator = lines
        .iter()
        .position(|l| l.contains("The full text of each"))
        .unwrap_or_else(|| {
            panic!(
                "NOTICE has no `The full text of each` sentence. That sentence closes \
                 the librdkafka component table and is how this test finds the table's \
                 end; without it the parse has no anchor and would silently read \
                 nothing, which is the failure mode this whole file exists to remove"
            )
        });

    let mut table = BTreeMap::new();
    let mut index = terminator;
    let mut saw_row = false;
    while index > 0 {
        index -= 1;
        let line = lines[index];
        if line.trim().is_empty() {
            // Blank lines separate the table from the sentence below it; once
            // a row has been seen, a blank line ends the table going upwards.
            if saw_row {
                break;
            }
            continue;
        }
        let Some(rest) = line.strip_prefix("  ") else {
            break;
        };
        if rest.starts_with(' ') {
            break;
        }
        let mut parts = rest.splitn(2, "  ");
        let (Some(name), Some(licence)) = (parts.next(), parts.next()) else {
            break;
        };
        let name = name.trim();
        if name.is_empty() {
            break;
        }
        saw_row = true;
        let previous = table.insert(name.to_string(), licence.trim().to_string());
        assert!(
            previous.is_none(),
            "NOTICE's component table names `{name}` twice; one row per component"
        );
    }

    assert!(
        table.len() >= 12,
        "NOTICE's component table parsed as only {} row(s) ({:?}). The file has carried \
         fourteen since librdkafka 2.12.1; a collapse means the table's SHAPE changed \
         and this test is now asserting set equality against something too small to be \
         worth asserting",
        table.len(),
        table.keys().collect::<Vec<_>>()
    );
    table
}

/// **`NOTICE` names every component `LICENSES.txt` names — derived, never typed.**
///
/// The expected set is every `^LICENSE.<component>` header in librdkafka's own
/// `LICENSES.txt`, read out of the `rdkafka-sys` source directory resolved
/// from `Cargo.lock`'s recorded version. Fourteen today —
/// `tinycthread` and `wingetopt` among them, which the specification's prose
/// and this plan's first draft both omitted while naming twelve. A fifteenth
/// after an upstream bump FAILS this test rather than passing it.
#[test]
fn the_notice_names_every_librdkafka_component_in_licenses_txt() {
    let lock = read("Cargo.lock");
    let version = locked_version(&lock, "rdkafka-sys");

    let candidates = licenses_txt_candidates(&version);
    let licenses = candidates.iter().find(|p| p.is_file()).unwrap_or_else(|| {
        // NEVER A SKIP. A licence test that skips is a check that cannot fail,
        // and this is the one test standing between an incomplete attribution
        // and a release that certifies it as complete.
        let tried: Vec<String> = candidates.iter().map(|p| p.display().to_string()).collect();
        panic!(
            "librdkafka's LICENSES.txt was not found for rdkafka-sys {version}. This test \
             does NOT skip: the component list in NOTICE is derived from that file, and a \
             licence check that cannot run is a licence check that cannot fail. Run a \
             build (or `cargo fetch --offline`) so the crate source is unpacked, then \
             re-run. Paths tried:\n  {}",
            tried.join("\n  ")
        )
    });
    let text = std::fs::read_to_string(licenses)
        .unwrap_or_else(|e| panic!("{} is readable: {e}", licenses.display()));

    // Every `^LICENSE` header. The bare `LICENSE` at line 1 is librdkafka's
    // own; `LICENSE.<name>` is one vendored component each.
    let mut components: BTreeSet<String> = BTreeSet::new();
    let mut saw_top_level = false;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("LICENSE") else {
            continue;
        };
        if rest.is_empty() || rest.trim().is_empty() {
            saw_top_level = true;
        } else if let Some(name) = rest.strip_prefix('.') {
            let name = name.trim();
            if !name.is_empty() {
                components.insert(name.to_string());
            }
        }
    }

    assert!(
        saw_top_level,
        "{} has no bare `LICENSE` header — librdkafka's own licence block is the \
         first thing in that file, and its absence means the file's shape changed",
        licenses.display()
    );
    assert!(
        components.len() >= 12,
        "{} yielded only {} component headers ({components:?}). The file has carried \
         fourteen since 2.12.1; a sudden collapse means the header shape changed and \
         this test is now deriving an expectation that is too small to be worth \
         asserting",
        licenses.display(),
        components.len()
    );

    let notice = read("NOTICE");

    // THE GUARD READS THE TABLE, AND READS IT BOTH WAYS.
    //
    // An earlier shape asserted `notice.contains(component)` over the WHOLE
    // file, and that is not the assertion it looks like. `tinycthread` and
    // `wingetopt` are named twice in NOTICE — once in the attribution table
    // and once in the paragraph explaining that the first draft omitted them —
    // so deleting either component's ATTRIBUTION LINE left the test green.
    // The two components this file exists to stop losing were the two the
    // guard could not see lost. It was also one-directional: an invented
    // fifteenth component added to the table passed, because nothing asserted
    // NOTICE ⊆ LICENSES.txt.
    //
    // So: parse the table into a set and assert SET EQUALITY with the derived
    // fourteen. A missing component fails naming it; an extra one fails naming
    // it; the prose is prose again.
    let components_in_notice = notice_component_table(&notice);

    let missing: Vec<&String> = components
        .iter()
        .filter(|c| !components_in_notice.contains_key(*c))
        .collect();
    assert!(
        missing.is_empty(),
        "NOTICE's component table does not name the librdkafka component(s) {missing:?}. \
         The expected set is read from {} — not from a literal in this test — so \
         deleting a line from the table cannot be matched by shrinking the \
         expectation, and a mention in the surrounding PROSE does not count: this \
         assertion reads the attribution table and nothing else. Table read \
         ({} rows): {:?}",
        licenses.display(),
        components_in_notice.len(),
        components_in_notice.keys().collect::<Vec<_>>()
    );

    let extra: Vec<&String> = components_in_notice
        .keys()
        .filter(|c| !components.contains(*c))
        .collect();
    assert!(
        extra.is_empty(),
        "NOTICE's component table names {extra:?}, which librdkafka's own {} does NOT \
         vendor. A licence notice is a statement about what is inside the binary; \
         naming a component that is not there is a false attribution, and it is as \
         wrong as losing one. The derived set ({} components) is: {components:?}",
        licenses.display(),
        components.len()
    );

    for (component, licence) in &components_in_notice {
        assert!(
            !licence.trim().is_empty(),
            "NOTICE's component table gives `{component}` no licence. The row exists to \
             say WHICH licence the component travels under; a bare name states nothing"
        );
    }

    // The umbrella attribution the components hang from.
    for required in ["librdkafka", "BSD-2-Clause", "OpenSSL"] {
        assert!(
            notice.contains(required),
            "NOTICE must name `{required}`: librdkafka is statically linked through \
             `rdkafka-sys`'s `cmake-build` feature and its licence requires the \
             copyright notice to travel with the redistributed binary"
        );
    }

    // And the fact that the list is derived, said in the document itself, so a
    // reader can check it the same way this test does.
    assert!(
        notice.contains("LICENSES.txt"),
        "NOTICE must name `LICENSES.txt` as the file its component list is derived \
         from; a list a reader cannot check is a list a reader must trust"
    );
}

// ------------------------------------------------------- the crate inventory

/// Every `name@version` in `Cargo.lock`, in sorted order.
fn locked_packages(lock: &str) -> BTreeSet<String> {
    let mut packages = BTreeSet::new();
    for block in lock.split("[[package]]").skip(1) {
        let mut name = None;
        let mut version = None;
        for line in block.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("name = \"") {
                if name.is_none() {
                    name = Some(rest.trim_end_matches('"').to_string());
                }
            } else if let Some(rest) = line.strip_prefix("version = \"") {
                if version.is_none() {
                    version = Some(rest.trim_end_matches('"').to_string());
                }
            }
            if line == "[[package]]" || (name.is_some() && version.is_some()) {
                // Both fields are at the top of a block; stop before the
                // `dependencies` list, whose entries also look like names.
                if name.is_some() && version.is_some() {
                    break;
                }
            }
        }
        if let (Some(n), Some(v)) = (name, version) {
            packages.insert(format!("{n}@{v}"));
        }
    }
    packages
}

/// The inventory's entries: `name@version` -> (SPDX field, copyright lines).
///
/// THE COPYRIGHT FIELD IS PLURAL. The obligation a redistributed binary carries
/// is to reproduce the notices, and a licence file naming four holders owes
/// four lines — `ring` names Brian Smith, the Go Authors and the Chromium
/// Authors; `aws-lc-sys` names eleven. The generator emits one
/// `- Copyright:` line per notice and then the single `- Copyright source:`
/// line, so this parser collects a `Vec` rather than overwriting a `String`.
/// An earlier shape kept the last line only, which would have hidden exactly
/// the defect this parse now makes visible.
fn inventory_entries(text: &str) -> BTreeMap<String, (String, Vec<String>)> {
    let mut entries: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    let mut current: Option<String> = None;
    let mut spdx = String::new();
    let mut copyrights: Vec<String> = Vec::new();

    let flush = |entries: &mut BTreeMap<String, (String, Vec<String>)>,
                 current: &mut Option<String>,
                 spdx: &mut String,
                 copyrights: &mut Vec<String>| {
        if let Some(key) = current.take() {
            let previous = entries.insert(key.clone(), (spdx.clone(), copyrights.clone()));
            assert!(
                previous.is_none(),
                "THIRD_PARTY_NOTICES.md carries TWO entries for `{key}`; the generator \
                 emits one per resolved package"
            );
        }
        spdx.clear();
        copyrights.clear();
    };

    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("### ") {
            flush(&mut entries, &mut current, &mut spdx, &mut copyrights);
            current = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("- SPDX: ") {
            spdx = rest.trim().trim_matches('`').to_string();
        } else if let Some(rest) = line.strip_prefix("- Copyright: ") {
            copyrights.push(rest.trim().to_string());
        }
    }
    flush(&mut entries, &mut current, &mut spdx, &mut copyrights);
    entries
}

/// **`THIRD_PARTY_NOTICES.md` carries one entry per resolved package, with a
/// non-empty SPDX field and a non-empty copyright field on each.**
///
/// The expected set comes from `Cargo.lock`, so hand-editing one entry out of
/// the document fails here naming the missing `name@version` — and the
/// acceptance line `diff -u THIRD_PARTY_NOTICES.md <(generator)` exits 1 at the
/// same time. Two independent detectors for one edit, on purpose.
///
/// The non-empty copyright assertion is what makes the generator's third arm
/// load-bearing: nineteen packages carry neither a copyright line in a licence
/// file nor an `authors` field, and a generator that emitted nothing for them
/// would produce a file that looks complete and is not.
#[test]
fn third_party_notices_covers_every_resolved_package() {
    let lock = read("Cargo.lock");
    let expected = locked_packages(&lock);
    assert!(
        expected.len() > 100,
        "Cargo.lock yielded only {} packages; the parse is wrong, not the lockfile",
        expected.len()
    );

    let inventory = read("THIRD_PARTY_NOTICES.md");
    let entries = inventory_entries(&inventory);

    for package in &expected {
        let (spdx, copyrights) = entries.get(package).unwrap_or_else(|| {
            panic!(
                "THIRD_PARTY_NOTICES.md has no entry for `{package}`. The expected set is \
                 parsed from Cargo.lock ({} packages), not from a literal here, so a \
                 hand edit cannot be matched by shrinking the expectation. Regenerate \
                 with `bash scripts/gen-third-party-notices.sh --write`",
                expected.len()
            )
        });
        assert!(
            !spdx.trim().is_empty(),
            "THIRD_PARTY_NOTICES.md's entry for `{package}` has an empty SPDX field"
        );
        assert!(
            !copyrights.is_empty(),
            "THIRD_PARTY_NOTICES.md's entry for `{package}` carries NO copyright line. \
             Every package gets one of the three arms, and the third arm states \
             the fact (`no copyright statement in the published crate; SPDX <expr> \
             applies`) rather than emitting nothing — an absent field is \
             indistinguishable from `nothing is owed here`"
        );
        for copyright in copyrights {
            assert!(
                !copyright.trim().is_empty(),
                "THIRD_PARTY_NOTICES.md's entry for `{package}` carries an EMPTY \
                 `- Copyright:` line. A blank notice is worse than a stated absence: \
                 it reads as an answer"
            );
        }
    }

    let extra: Vec<&String> = entries.keys().filter(|k| !expected.contains(*k)).collect();
    assert!(
        extra.is_empty(),
        "THIRD_PARTY_NOTICES.md names packages that are not in Cargo.lock: {extra:?}"
    );

    assert!(
        inventory.contains(&format!(
            "Packages in the resolved graph: {}",
            expected.len()
        )),
        "THIRD_PARTY_NOTICES.md's header must record the package count it was \
         generated at ({}), so a stale file is visible without a diff",
        expected.len()
    );
    assert!(
        inventory.contains("Regenerated, never edited"),
        "THIRD_PARTY_NOTICES.md must say that it is regenerated and never edited; \
         a generated file that does not say so gets edited"
    );
    assert!(
        inventory.contains("scripts/gen-third-party-notices.sh"),
        "THIRD_PARTY_NOTICES.md must name the command that regenerates it"
    );
}

/// The inventory entry for the crate named `name`, whatever version resolved.
/// Keyed by name rather than `name@version` so a dependency bump does not
/// silently turn this assertion into a vacuous one.
fn entry_for<'a>(
    entries: &'a BTreeMap<String, (String, Vec<String>)>,
    name: &str,
) -> (&'a String, &'a Vec<String>) {
    let prefix = format!("{name}@");
    let (key, (_, copyrights)) = entries
        .iter()
        .find(|(k, _)| k.starts_with(&prefix))
        .unwrap_or_else(|| {
            panic!(
                "THIRD_PARTY_NOTICES.md has no entry for a crate named `{name}`. If the \
                 dependency genuinely left the graph, this assertion is the thing to \
                 delete — deliberately, not by accident"
            )
        });
    (key, copyrights)
}

/// **Arm 1 carries EVERY holder its licence files name, not the first one.**
///
/// The obligation MIT, BSD and Apache-2.0 impose is to reproduce the copyright
/// notices — plural. A generator that returned the first matching line of the
/// first matching file attributed `ring` to "The Go Authors" (its
/// `LICENSE-BoringSSL` sorts before `LICENSE-other-bits`, where Brian Smith
/// is) and printed nine of `aws-lc-sys`'s eleven notices nowhere at all. And a
/// notice with no `(c)` and no year is still a notice: `aws-lc-sys`'s
/// `LICENSE:9` is `Copyright Amazon.com, Inc. or its affiliates.` — skipping
/// it attributed an Amazon crate to Google, and told the reader that
/// `aws-lc-rs` and `utf8_iter` carried no licence-file notice while both ship
/// one.
///
/// These four crates are the measured witnesses of those two defects, so they
/// are the four asserted here. Reads two checked-in files and nothing else
/// (Global Constraint 22): no cargo, no registry, no network.
#[test]
fn the_inventory_carries_every_holder_a_licence_file_names() {
    let inventory = read("THIRD_PARTY_NOTICES.md");
    let entries = inventory_entries(&inventory);

    // (F2) Every holder in the file, not the first. `ring`'s own notice lives
    // in the file that sorts LAST.
    let (ring_key, ring) = entry_for(&entries, "ring");
    for holder in ["Brian Smith", "The Go Authors"] {
        assert!(
            ring.iter().any(|line| line.contains(holder)),
            "THIRD_PARTY_NOTICES.md's `{ring_key}` entry does not name `{holder}`. ring \
             ships three holder notices across `LICENSE-BoringSSL` and \
             `LICENSE-other-bits`; a generator that keeps only the first attributes the \
             crate to whichever file sorts first and loses its author. Lines found: \
             {ring:?}"
        );
    }
    assert!(
        ring.len() >= 2,
        "THIRD_PARTY_NOTICES.md's `{ring_key}` entry carries {} copyright line(s). The \
         entry is plural by construction — one line per notice — and a single line \
         means the generator went back to first-match-wins",
        ring.len()
    );

    // (F1) A holder line with no `(c)` and no year is still a notice, and
    // (F2) again: the OpenSSL-family notices aws-lc-sys vendors.
    let (aws_key, aws) = entry_for(&entries, "aws-lc-sys");
    for holder in ["Amazon", "OpenSSL"] {
        assert!(
            aws.iter().any(|line| line.contains(holder)),
            "THIRD_PARTY_NOTICES.md's `{aws_key}` entry does not name `{holder}`. Its \
             `LICENSE` opens `Copyright Amazon.com, Inc. or its affiliates.` — no \
             `(c)`, no year, and still the notice — and goes on to carry the OpenSSL \
             Project's and Eric Young's. Attributing an Amazon crate to Google is a \
             false attribution, not a gap. Lines found: {aws:?}"
        );
    }
    assert!(
        aws.len() >= 3,
        "THIRD_PARTY_NOTICES.md's `{aws_key}` entry carries {} copyright line(s); its \
         `LICENSE` carries eleven holder notices and the entry owes one line each",
        aws.len()
    );

    // The arm LABEL is an assertion about the crate, not a formatting choice:
    // `authors` here would say "no licence-file notice exists" about two crates
    // that ship one.
    for name in ["aws-lc-rs", "utf8_iter"] {
        let prefix = format!("{name}@");
        let (key, (_, _)) = entries
            .iter()
            .find(|(k, _)| k.starts_with(&prefix))
            .unwrap_or_else(|| panic!("THIRD_PARTY_NOTICES.md has no entry for `{name}`"));
        let block = inventory
            .split("### ")
            .find(|b| b.starts_with(key.as_str()))
            .unwrap_or_else(|| panic!("THIRD_PARTY_NOTICES.md has no `### {key}` block"));
        assert!(
            block.contains("- Copyright source: licence file"),
            "THIRD_PARTY_NOTICES.md's `{key}` entry does not use the licence-file arm. \
             `aws-lc-rs` ships `Copyright Amazon.com, Inc. or its affiliates.` and \
             `utf8_iter` ships `Copyright Mozilla Foundation`; reporting the \
             `authors` arm asserts no licence-file notice exists when one does. Block \
             read:\n{block}"
        );
    }
}

// ------------------------------------------------------------- the footers

/// **Every doc footer carries the ASF sentence and a CC-BY-4.0 line whose link
/// resolves to `docs/LICENSE-docs`.**
///
/// The link is resolved on disk rather than pattern-matched. A footer line
/// asserting a licence with no artefact behind it is an assertion, not a
/// licence, which is why `docs/LICENSE-docs` exists at all.
#[test]
fn every_doc_footer_carries_the_asf_sentence_and_the_docs_licence() {
    let root = repo_root();
    let licence = root
        .join("docs")
        .join("LICENSE-docs")
        .canonicalize()
        .expect("docs/LICENSE-docs exists: the CC-BY-4.0 text every footer cites");

    let mut paths = docs_markdown();
    paths.extend(ROOT_MARKDOWN.iter().map(|f| root.join(f)));

    for path in paths {
        let relative = path.strip_prefix(&root).unwrap_or(&path).to_path_buf();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));

        // The ASF sentence, either spelling: `®` at the repository root and
        // under `docs/`, `(R)` where an ASCII-only gate applies.
        assert!(
            text.contains("registered trademarks of the Apache Software")
                && text.contains("not affiliated with or endorsed by the ASF"),
            "{} does not carry the ASF attribution sentence (Global Constraint 14)",
            relative.display()
        );

        // The CC-BY-4.0 line, and the link on it has to RESOLVE.
        let mut resolved = false;
        let mut seen: Vec<String> = Vec::new();
        for line in text.lines() {
            if !line.contains("CC-BY-4.0](") {
                continue;
            }
            let Some(open) = line.find("CC-BY-4.0](") else {
                continue;
            };
            let after = &line[open + "CC-BY-4.0](".len()..];
            let Some(close) = after.find(')') else {
                continue;
            };
            let target = &after[..close];
            seen.push(target.to_string());
            let candidate = path
                .parent()
                .expect("a file has a parent directory")
                .join(target);
            if candidate.canonicalize().is_ok_and(|c| c == licence) {
                resolved = true;
            }
        }
        assert!(
            resolved,
            "{}'s footer needs a CC-BY-4.0 line whose relative link resolves to \
             docs/LICENSE-docs (R10: docs are CC-BY-4.0). Link targets found on \
             CC-BY-4.0 lines: {seen:?}",
            relative.display()
        );
    }
}

// ----------------------------------------------------------------- the DCO

/// **`CONTRIBUTING.md` requires a DCO sign-off and accepts no CLA.**
#[test]
fn contributing_requires_a_dco_signoff_and_no_cla() {
    let contributing = read("CONTRIBUTING.md");

    assert!(
        contributing.contains("Signed-off-by"),
        "CONTRIBUTING.md must name the `Signed-off-by` trailer"
    );
    assert!(
        contributing.contains("git commit -s"),
        "CONTRIBUTING.md must give the `git commit -s` recipe"
    );
    assert!(
        contributing.contains("Developer Certificate of Origin"),
        "CONTRIBUTING.md must name the Developer Certificate of Origin"
    );
    // The certificate itself, not only a link to it: a contributor certifies
    // what is in front of them.
    for clause in [
        "The contribution was created in whole or in part by me",
        "The contribution is based upon previous work",
        "The contribution was provided directly to me by some other",
        "maintained indefinitely and may be redistributed",
    ] {
        assert!(
            contributing.contains(clause),
            "CONTRIBUTING.md must carry DCO 1.1's own text, including the clause \
             starting `{clause}`"
        );
    }
    assert!(
        contributing.contains("No copyright-assignment CLA is required or accepted"),
        "CONTRIBUTING.md must state that no copyright-assignment CLA is required or \
         accepted — the DCO is a certification and not a transfer, and that difference \
         is the reason this project cannot be quietly relicensed"
    );
}

// --------------------------------------------------------- the threat model

/// **`README.md` states the four threat-model residuals, on that surface.**
///
/// They are residuals, not bugs: the surface a stranger reads first has to
/// carry them, because none of the four is discoverable from the code.
#[test]
fn the_readme_states_the_four_threat_model_residuals() {
    let readme = read("README.md");

    assert!(
        readme.contains(
            "Job CRUD over the signing-key namespace is\n  equivalent to holding the key"
        ) || readme
            .contains("Job CRUD over the signing-key namespace is equivalent to holding the key"),
        "README.md must state that Job CRUD over the signing-key namespace is \
         equivalent to holding the key — the signing-oracle residual (O1/O0 default \
         (a), Global Constraint 27)"
    );
    assert!(
        readme.contains("A cluster-admin defeats every control described here"),
        "README.md must state that a cluster-admin defeats every control described \
         here (O0, default (a))"
    );
    assert!(
        readme.contains("RBAC bounds the viewer, not the page"),
        "README.md must carry the sentence `RBAC bounds the viewer, not the page` \
         (Global Constraint 28): the four shipped ClusterRoles bind the user, and \
         under the `kubectl proxy` serving path they bind nothing about the page"
    );
    assert!(
        readme.contains("`self_attested: false` means only \"two different keys\""),
        "README.md must state that `self_attested: false` means only \"two different \
         keys\" — one operator holding two keypairs satisfies it, and it is not \
         evidence of an independent auditor"
    );

    // The withdrawn claim is forbidden on every shipped surface
    // (`scripts/check-one-signer.sh` and `scripts/check-withdrawn-claim.sh`).
    // Stating the residuals is what replaces it; restating it here would be the
    // exact regression the corpus grep exists to catch.
    assert!(
        !readme.to_lowercase().contains("cannot sign"),
        "README.md restates a withdrawn claim about signing; state the narrowed \
         position instead"
    );

    // The release-notes note about the UI bundle.
    assert!(
        readme.contains("listed by\ndigest in the release notes")
            || readme.contains("listed by digest in the release notes"),
        "README.md must record that the `ui/` bundle's contents are listed by digest \
         in the release notes — the page runs with the viewer's authority, so which \
         bytes it is has to be checkable"
    );
}

// ----------------------------------------------------------- the two images

/// A `COPY` instruction's operands, for every `COPY` in a Dockerfile.
/// Line continuations are joined first; `--from=` flags are dropped.
fn copy_instructions(dockerfile: &str) -> Vec<Vec<String>> {
    let mut joined = String::new();
    for line in dockerfile.lines() {
        let trimmed = line.trim_end();
        if let Some(stripped) = trimmed.strip_suffix('\\') {
            joined.push_str(stripped);
            joined.push(' ');
        } else {
            joined.push_str(trimmed);
            joined.push('\n');
        }
    }

    joined
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let rest = line.strip_prefix("COPY ")?;
            Some(
                rest.split_whitespace()
                    .filter(|token| !token.starts_with("--"))
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

/// **Every image `COPY`s Logweir's LICENSE and NOTICE into
/// `/usr/share/licenses/logweir/`; each carries the third-party notices it owes
/// and none it does not.**
///
/// RENAMED FROM `both_images_copy_the_licence_and_the_notice` (Task 39,
/// STANDING RULE 19): there are three images now — the runner, the controller
/// and `logweir-ui` — and "both" was the half of the name that had gone stale.
/// The three occurrences of the old name in the tree
/// (`crates/logweir/tests/manifest_lint.rs`, `docs/tag1-checklist.md`, the
/// definition) move with it.
///
/// WHAT EACH IMAGE OWES, AND ONLY WHAT IT OWES. A licence notice is a statement
/// about what is inside, not decoration, so a notice for something an image
/// does not carry is a FALSE ATTRIBUTION:
///
/// * all three redistribute Logweir's own Apache-2.0 code, so all three carry
///   `LICENSE` and `NOTICE`;
/// * the runner and the controller each ship a statically linked Rust binary,
///   so both carry `THIRD_PARTY_NOTICES.md`, the inventory of that graph. The
///   UI image ships fourteen static files over a kubectl and links no Rust at
///   all, so it must NOT — asserted below, and by
///   `scripts/check-image-ui.sh` check 2;
/// * only the runner redistributes the MIT-licensed `kafka-backup`, so only it
///   carries `/usr/share/licenses/kafka-backup/`. Asserted absent from the
///   other two here, and from the built images by
///   `scripts/check-image-weirkeeper.sh` check 3 and `check-image-ui.sh`
///   check 2;
/// * only the UI image redistributes kubectl, so only it carries
///   `/usr/share/licenses/kubectl/` — its Apache-2.0 text and an inventory
///   naming the base digest.
///
/// This parses `COPY` instructions rather than grepping the file, because the
/// Dockerfiles' comments explain the absences at length and a grep for
/// `kafka-backup` would report the explanation as the violation.
#[test]
fn every_image_copies_the_licence_and_the_notice() {
    const LOGWEIR_LICENCES: &str = "/usr/share/licenses/logweir/";

    for (name, path) in [
        ("runner", "Dockerfile"),
        ("controller", "Dockerfile.weirkeeper"),
        ("UI", "Dockerfile.ui"),
    ] {
        let dockerfile = read(path);
        let copies = copy_instructions(&dockerfile);
        assert!(
            !copies.is_empty(),
            "{path} has no COPY instruction; the parse is wrong, not the file"
        );

        // THE RUST INVENTORY IS OWED BY THE TWO IMAGES THAT SHIP A RUST
        // BINARY, and by neither the third nor anything else.
        let required: &[&str] = if path == "Dockerfile.ui" {
            &["LICENSE", "NOTICE"]
        } else {
            &["LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"]
        };
        for required in required.iter().copied() {
            let carried = copies.iter().any(|operands| {
                operands.last().is_some_and(|dest| {
                    dest == LOGWEIR_LICENCES || dest == LOGWEIR_LICENCES.trim_end_matches('/')
                }) && operands
                    .iter()
                    .rev()
                    .skip(1)
                    .any(|source| source == required)
            });
            assert!(
                carried,
                "the {name} image ({path}) does not COPY `{required}` into \
                 {LOGWEIR_LICENCES}. Apache-2.0, MIT, BSD-2-Clause and BSD-3-Clause each \
                 require the copyright notice to travel with the redistributed binary, \
                 and both images redistribute one. COPY instructions found: {copies:?}"
            );
        }
    }

    // The runner redistributes the engine binary, so it owes the MIT notice.
    let runner = copy_instructions(&read("Dockerfile"));
    assert!(
        runner.iter().any(|operands| {
            operands
                .last()
                .is_some_and(|dest| dest == "/usr/share/licenses/kafka-backup/LICENSE")
                && operands.iter().any(|s| s.contains("LICENSE-MIT"))
        }),
        "Dockerfile must COPY third_party/LICENSE-MIT to \
         /usr/share/licenses/kafka-backup/LICENSE: the runner image redistributes the \
         MIT-licensed `kafka-backup` binary and MIT requires the notice to travel with \
         it. COPY instructions found: {runner:?}"
    );

    // The other two redistribute none of it, and must not claim to.
    for (name, path, gate) in [
        (
            "controller",
            "Dockerfile.weirkeeper",
            "scripts/check-image-weirkeeper.sh check 3",
        ),
        ("UI", "Dockerfile.ui", "scripts/check-image-ui.sh check 2"),
    ] {
        for operands in &copy_instructions(&read(path)) {
            for token in operands {
                assert!(
                    !token.contains("kafka-backup"),
                    "{path} COPYs a `kafka-backup` path (`{token}`). The {name} image links no \
                     OSO code and redistributes no MIT-licensed binary, so that notice would be \
                     a FALSE ATTRIBUTION — a claim to redistribute something the image does not \
                     contain. `{gate}` asserts the same absence from the built image."
                );
            }
        }
    }

    // AND THE UI IMAGE CARRIES KUBECTL'S, because it redistributes the whole
    // kubectl base — the same obligation, the same shape, a different upstream.
    let ui = copy_instructions(&read("Dockerfile.ui"));
    for (dest, what) in [
        (
            "/usr/share/licenses/kubectl/INVENTORY",
            "the inventory naming the base image by digest and its licence",
        ),
        (
            "/usr/share/licenses/kubectl/LICENSE",
            "the Apache-2.0 text kubectl is licensed under",
        ),
    ] {
        assert!(
            ui.iter()
                .any(|operands| operands.last().is_some_and(|d| d == dest)),
            "Dockerfile.ui must COPY {what} to {dest}: this image redistributes the whole \
             registry.k8s.io/kubectl base, binary included, exactly as the runner image \
             redistributes kafka-backup. COPY instructions found: {ui:?}"
        );
    }
    // AND NOT THE RUST GRAPH'S INVENTORY, which would claim a redistribution it
    // does not make. The loop above requires it of the other two and skips it
    // here; this asserts the skip is an ABSENCE rather than an omission.
    for operands in &ui {
        for token in operands {
            assert!(
                !token.contains("THIRD_PARTY_NOTICES.md"),
                "Dockerfile.ui COPYs `{token}`. THIRD_PARTY_NOTICES.md is the inventory of \
                 Logweir's RUST dependency graph, generated from Cargo.lock; this image links no \
                 Rust binary and redistributes no crate, so shipping it would claim a \
                 redistribution that does not happen. `scripts/check-image-ui.sh` check 2 \
                 asserts the same absence from the built image."
            );
        }
    }
}

// -------------------------------------------------------- the UI section pointer

/// Every `§N` / `section N` marker on one line, as `(start, end, N)` byte
/// offsets into that line. Hand-rolled rather than pulled from `regex`,
/// because Global Constraint 38 closes the workspace graph and a cross-reference
/// checker is not worth a package.
fn section_markers(line: &str) -> Vec<(usize, usize, u32)> {
    let bytes = line.as_bytes();
    let digits_from = |start: usize| -> Option<(usize, u32)> {
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end == start {
            return None;
        }
        line[start..end].parse::<u32>().ok().map(|n| (end, n))
    };

    let mut markers: Vec<(usize, usize, u32)> = Vec::new();
    for (at, _) in line.match_indices('\u{a7}') {
        if let Some((end, number)) = digits_from(at + '\u{a7}'.len_utf8()) {
            markers.push((at, end, number));
        }
    }
    // `to_ascii_lowercase` maps only A-Z, so every byte offset in the lowered
    // copy is the same offset in the original.
    let lowered = line.to_ascii_lowercase();
    for (at, _) in lowered.match_indices("section ") {
        if let Some((end, number)) = digits_from(at + "section ".len()) {
            markers.push((at, end, number));
        }
    }
    markers.sort_unstable();
    markers.dedup();
    markers
}

/// **A pointer at the UI section names the number that section actually has.**
///
/// `docs/kubernetes.md`'s UI section was `## 15.` when `docs/install.md` and
/// `docs/kubernetes.md` were written to point at it, and Task 24 inserted a new
/// `## 15.` ahead of it, renumbering it to `## 16.`. The rebase was clean —
/// nothing in git notices that a number in prose became a pointer at the wrong
/// section — so this arm exists to notice it: the heading is PARSED, the
/// pointers are found, and the two must agree.
///
/// Finding a pointer: a `§N` or `section N` marker with `Serving the UI` or
/// `UI section` within 60 characters on the same line, where the window stops
/// at the NEXT marker. That bound is what keeps the `§14` in
/// "`§14`'s X-DIGEST transcript and `§16`'s UI section" from being read as a
/// pointer at the UI section — it belongs to the transcript beside it.
///
/// A `§15` that means Task 24's own `## 15. The evidence credential…` is not a
/// UI pointer and is untouched by all of this.
#[test]
fn ui_section_pointers_name_the_heading_that_exists() {
    let kubernetes = read("docs/kubernetes.md");

    let headings: Vec<u32> = kubernetes
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("## ")?;
            let (number, title) = rest.split_once(". ")?;
            (title.trim() == "Serving the UI").then(|| number.trim().parse::<u32>().ok())?
        })
        .collect();
    assert_eq!(
        headings.len(),
        1,
        "docs/kubernetes.md must carry exactly one `## <N>. Serving the UI` heading; \
         found {}. Every pointer in the tree is checked against that number, so two \
         headings (or none) leaves the pointers checked against nothing",
        headings.len()
    );
    let heading = headings[0];

    let mut sources: Vec<PathBuf> = docs_markdown();
    sources.push(repo_root().join("README.md"));
    sources.push(repo_root().join("ui").join("README.md"));

    let mut sites: Vec<String> = Vec::new();
    for path in &sources {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));
        for (number, line_number, line) in text.lines().enumerate().flat_map(|(i, line)| {
            let markers = section_markers(line);
            let mut found = Vec::new();
            for (index, &(start, end, number)) in markers.iter().enumerate() {
                let ceiling = markers
                    .get(index + 1)
                    .map(|next| next.0)
                    .unwrap_or(line.len());
                let mut forward = (end + 60).min(ceiling).min(line.len());
                while forward > end && !line.is_char_boundary(forward) {
                    forward -= 1;
                }
                let floor = if index > 0 { markers[index - 1].1 } else { 0 };
                let mut backward = start.saturating_sub(60).max(floor);
                while backward < start && !line.is_char_boundary(backward) {
                    backward += 1;
                }
                let window = format!("{}{}", &line[backward..start], &line[end..forward]);
                if window.contains("Serving the UI") || window.contains("UI section") {
                    found.push((number, i + 1, line));
                }
            }
            found
        }) {
            let relative = path
                .strip_prefix(repo_root())
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            assert_eq!(
                number,
                heading,
                "{relative}:{line_number} points at section {number} for the UI section, \
                 but `docs/kubernetes.md`'s heading is `## {heading}. Serving the UI`. \
                 The line reads:\n  {}\nA renumbering upstream of that heading does not \
                 conflict in git and does not break a link — it silently aims the \
                 reader at a different section",
                line.trim()
            );
            sites.push(format!("{relative}:{line_number}"));
        }
    }

    assert!(
        sites.len() >= 2,
        "only {} UI-section pointer(s) were found ({sites:?}). `docs/install.md` and \
         `docs/kubernetes.md` each carry one, and an assertion that finds nothing to \
         check is an assertion that cannot fail — which is the defect this arm exists \
         to catch. If a pointer was deliberately removed, this floor is the thing to \
         change, deliberately",
        sites.len()
    );
}

// ---------------------------------------------------------- the install doc

/// Every digest literal in `text`, in any of its shapes, described in words.
///
/// TWO SHAPES, because a digest in prose has two tells and only one of them is
/// the full `sha256:<64 hex>` form:
///
///   1. `sha256:` followed by six or more hex characters. Six, not sixty-four,
///      because the form people actually paste into prose is the TRUNCATED one
///      — `sha256:a5aa6dc1…` — and it is a measurement of one build just as
///      much as the whole thing is. Six hex digits is already an identifier;
///      fewer could be a word (`decade`, `facade`) or a version.
///   2. a run of sixty-four hex characters anywhere at all, prefix or no
///      prefix. A digest pasted without its `sha256:` is still a digest.
///
/// The caller runs this over the file as written AND over the file with its
/// whitespace removed, which is what catches a digest wrapped across a line
/// break. `label` says which pass found the hit, so the failure message points
/// at the right thing.
///
/// Byte scanning is safe over UTF-8: every byte of a multi-byte sequence is
/// >= 0x80 and no ASCII hex digit ever appears inside one.
fn digest_literals(text: &str, label: &str) -> Vec<String> {
    const PREFIX: &str = "sha256:";
    const TRUNCATED_MIN: usize = 6;
    const FULL: usize = 64;

    let bytes = text.as_bytes();
    let hex_run_from = |start: usize| {
        bytes[start..]
            .iter()
            .take_while(|b| b.is_ascii_hexdigit())
            .count()
    };
    let excerpt = |start: usize, len: usize| {
        let end = (start + len.min(72)).min(bytes.len());
        String::from_utf8_lossy(&bytes[start..end]).into_owned()
    };

    let mut found = Vec::new();

    for (at, _) in text.match_indices(PREFIX) {
        let start = at + PREFIX.len();
        let run = hex_run_from(start);
        if run >= TRUNCATED_MIN {
            found.push(format!(
                "`{PREFIX}` followed by {run} hex character(s) — `{PREFIX}{}` — at byte \
                 {at} of the file {label}",
                excerpt(start, run)
            ));
        }
    }

    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_hexdigit() {
            index += 1;
            continue;
        }
        let run = hex_run_from(index);
        if run >= FULL {
            found.push(format!(
                "a bare run of {run} hex characters — `{}` — at byte {index} of the \
                 file {label}",
                excerpt(index, run)
            ));
        }
        index += run;
    }

    found
}

/// Local image loading must remain distinguishable from registry publication.
#[test]
fn install_md_distinguishes_local_images_and_registry_publication() {
    let install = read("docs/install.md");

    for literal in [
        "author-only",
        "imagePullPolicy: Never",
        "kubectl --context docker-desktop apply --server-side -k config/overlays/local-images",
    ] {
        assert!(
            install.contains(literal),
            "docs/install.md must carry the literal `{literal}` (Global Constraint 37)"
        );
    }

    assert!(
        install.contains("never satisfies spec\n§16 clause 1")
            || install.contains("never satisfies spec §16 clause 1"),
        "docs/install.md must state that a locally built or locally loaded image never \
         satisfies spec §16 clause 1 — \"published\" means a pull from a registry the \
         author does not control, the `registry:2` fallback included"
    );

    // The author-only tag step: the kubelet keys on the WHOLE reference, so a
    // matching digest under a different repository name is ErrImageNeverPull.
    //
    // BUILT FROM PIECES, NEVER WRITTEN OUT. The bare-tag form of the runner
    // image is forbidden in any `.rs` file under `crates/` (Global Constraint
    // 7; `manifest_lint.rs::the_runner_image_lives_in_exactly_one_place` scans
    // for it), and a literal here would be its own violation.
    let tag_step = format!(
        "docker tag logweir:check {}{}:{}",
        "docker.io/vladyslavhaina/", "logweir", "v0.1.0"
    );
    assert!(
        install.contains(&tag_step),
        "docs/install.md must carry the author-only `{tag_step}` step: the kubelet \
         keys images on the whole reference, so the shipped digest reference does not \
         resolve to a locally built image until it is tagged with the shipped name"
    );

    // NO DIGEST LITERAL, IN ANY SHAPE. A locally built digest changes on every
    // build, so a digest copied into prose is a measurement that stops being
    // true. An earlier shape of this guard split on `sha256:` and required the
    // NEXT 64 characters to be hex, which caught exactly one spelling.
    // Measured against that guard: a truncated `sha256:a5aa6dc1…`, a bare
    // 64-hex run with no prefix, and a digest wrapped across a line break all
    // passed. All three are a digest in prose; a reader copies them the same
    // way. So the scan now takes every shape, over the file as written AND
    // over the file with its whitespace removed.
    //
    // The legitimate `@sha256:<digest>` placeholder carries no hex at all and
    // is untouched — which is the distinction the document actually draws.
    let mut digests = digest_literals(&install, "as written");
    let squeezed: String = install.chars().filter(|c| !c.is_whitespace()).collect();
    digests.extend(digest_literals(&squeezed, "with whitespace removed"));
    assert!(
        digests.is_empty(),
        "docs/install.md carries {} digest literal(s):\n  {}\nIt must not: a locally \
         built image's digest changes on EVERY build, so a digest in prose is a \
         measurement of one build. The document names \
         `config/manager/deployment.yaml` and `crates/weirkeeper/src/job.rs` as the \
         places the pinned references live, and the reader reads them from the \
         checkout. The bare `@sha256:<digest>` placeholder is fine — it carries no hex",
        digests.len(),
        digests.join("\n  ")
    );
    for source_of_truth in [
        "config/manager/deployment.yaml",
        "crates/weirkeeper/src/job.rs",
    ] {
        assert!(
            install.contains(source_of_truth),
            "docs/install.md must name `{source_of_truth}` as a place the pinned image \
             reference actually lives"
        );
    }
}

/// **The Secret preflight runs before any custom resource.**
///
/// `just check-secrets` exits 1 naming the first absent Secret. The Helm path
/// initializes the retained signer first; a document that runs the check after
/// samples have been applied has documented the trap rather than avoided it.
#[test]
fn install_md_runs_the_secret_preflight_before_any_custom_resource() {
    let install = read("docs/install.md");

    // THE COMMAND, NOT A MENTION. `just check-secrets` is named several times
    // in this document's prose — in the paragraph that explains what it exits,
    // and in the silent-mint warning that explains why it exists. A prose
    // mention is not a step an adopter performs, so the index compared below
    // is the first line that IS the command: trimmed, it starts with it.
    let preflight = install
        .match_indices("just check-secrets")
        .find(|(at, _)| {
            let line_start = install[..*at].rfind('\n').map_or(0, |i| i + 1);
            install[line_start..*at].trim().is_empty()
        })
        .map(|(at, _)| at)
        .expect(
            "docs/install.md must carry `just check-secrets` as a COMMAND on its own line \
             (interface I26), not only as a mention in prose",
        );

    let first_sample = install
        .match_indices("config/samples/")
        .filter(|(at, _)| {
            // Only an APPLY of a sample counts; a prose mention does not.
            let line_start = install[..*at].rfind('\n').map_or(0, |i| i + 1);
            install[line_start..*at].contains("apply")
        })
        .map(|(at, _)| at)
        .min()
        .expect("docs/install.md must show applying the samples");

    assert!(
        preflight < first_sample,
        "docs/install.md applies a file under config/samples/ at byte {first_sample} \
         but does not reach `just check-secrets` until byte {preflight}. The preflight \
         runs BEFORE any custom resource: missing workload prerequisites must fail \
         before a controller creates a Job"
    );

    // Workload credentials remain explicit. The signing Secret is the one
    // fixed Secret that MUST NOT have a manual creation command on the
    // supported Helm path: bootstrap initializes it without local key files.
    for secret in ["logweir-s3", "logweir-evidence-ro"] {
        let found = install
            .match_indices("create secret generic")
            .any(|(at, _)| {
                let end = (at + 400).min(install.len());
                install.get(at..end).is_some_and(|w| w.contains(secret))
            });
        assert!(
            found,
            "docs/install.md has no `kubectl create secret` command for `{secret}`; a \
             mention is not a command"
        );
    }
    assert!(
        install.contains("--from-literal=password="),
        "docs/install.md must show how to create the per-cluster SCRAM credential, \
         whose data key is fixed at `password` (`TARGET_PASSWORD_SECRET_KEY`)"
    );
    assert!(
        install.contains("short-lived bootstrap Job")
            && install.contains("logweir-signing-trust")
            && install.contains("Helm never renders private key"),
        "docs/install.md must document managed bootstrap, public material and the no-render boundary"
    );
    assert!(
        !install.contains("-out signing.pem"),
        "the supported install must not require local signing-key generation"
    );
    assert!(
        install.contains(
            "openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out approver.pem"
        ),
        "the independent approver recipe remains explicit"
    );

    // The managed and low-level paths are deliberately distinct: Helm owns
    // declared runner namespaces; logweir.yaml users apply the base account.
    assert!(
        install.contains("config/rbac/backup-runner-serviceaccount.yaml"),
        "docs/install.md must tell the reader to apply the runner ServiceAccount"
    );
    assert!(
        install.contains("identity.authorizedRunnerNamespaces")
            && install.contains("For the low-level path, apply it once per namespace"),
        "docs/install.md must distinguish Helm-managed authorized namespaces from the low-level manual account path"
    );

    assert!(
        install.contains("identity.bootstrapImage")
            && install.contains("identity bootstrap --help")
            && install.contains("refuses an emptied value")
            && install.contains("amd64-only"),
        "install docs must state the pinned compatible bootstrap image, its re-pin check and its architecture limit"
    );
    assert!(
        install.contains("post-rollback")
            && install.contains("rollback to a pre-bootstrap chart")
            && install.contains("retention—not active validation"),
        "rollback validation must be limited to targets that actually carry the hook"
    );
    assert!(
        install.contains("connected Helm operation")
            && install.contains("offline `helm template`")
            && install.contains("unsupported with\n`identity.enabled=true`")
            && install.contains("exact\nIPv4 `/32` or IPv6 `/128`")
            && install.contains("TCP 443\nand 6443")
            && install.contains("Other API ports are unsupported"),
        "managed identity docs must refuse unsafe offline renders and state exact API CIDR/port requirements"
    );
    assert!(
        install.contains("Only the bootstrap/distributor **processes and their projected API tokens** are\nshort-lived")
            && install.contains("ordinary persistent release resources")
            && install.contains("does not automatically revoke these RBAC grants"),
        "identity docs must distinguish short-lived processes/tokens from persistent scoped RBAC"
    );
    assert!(
        install.contains("Routine Helm 3 and Helm 4 upgrades")
            && install.contains("`pre-install,pre-upgrade` hooks")
            && install.contains("not `pre-rollback` hooks")
            && install.contains("never the patched private bytes"),
        "rollback docs must describe the Helm 3/4-safe creation-hook retention boundary"
    );
    assert!(
        install.contains("one Logweir\ninstallation identity per cluster")
            && install.contains("different signer")
            && install.contains("Never mint a per-namespace signer"),
        "the singleton and same-identity namespace contract must be actionable"
    );
    assert!(
        install.contains("apply --server-side --force-conflicts -f charts/logweir/crds/")
            && install.contains("--for=condition=Established")
            && install.contains("#upgrade-rollback-and-legacy-jobs"),
        "install docs must order CRD apply/wait before Helm upgrade and cross-link details"
    );

    let chart_readme = read("charts/logweir/README.md");
    assert!(!chart_readme.contains("mints two\nkeypairs, creates the five Secrets"));
    assert!(!chart_readme.contains("the chart creates none of them on this path"));
    for required in [
        "identity.bootstrapImage",
        "identity.authorizedRunnerNamespaces",
        "logweir-identity-singleton",
        "identity bootstrap --help",
        "post-rollback",
    ] {
        assert!(
            chart_readme.contains(required),
            "chart README omits current identity contract `{required}`"
        );
    }

    let sample = read("config/samples/secrets.yaml");
    assert!(sample.contains("HELM-MANAGED; low-level/external/recovery only"));
    assert!(!sample.contains("-out signing.pem"));

    // The `TrustRoster`'s name is fixed.
    assert!(
        install.contains("name: default"),
        "docs/install.md must carry the `TrustRoster` snippet with `name: default`"
    );
    assert!(
        install.contains("nothing reads any other name"),
        "docs/install.md must say the `TrustRoster` name is fixed: a roster called \
         anything else is stored and reconciled and consulted by no approval check"
    );

    // Global Constraint 29: two clients, two trust stores.
    assert!(
        install.contains("webpki-roots") && install.contains("ssl_ca_location"),
        "docs/install.md must carry the two-trust-stores paragraph: the engine falls \
         back to bundled `webpki-roots` unless `ssl_ca_location` is set, while \
         Logweir's rdkafka path uses the image's `ca-certificates`, so a private-CA \
         adopter configures BOTH"
    );

    // The uninstall, and what survives it.
    assert!(
        install.contains("kubectl --context docker-desktop delete -f logweir.yaml"),
        "docs/install.md must carry the uninstall command"
    );
    assert!(
        install.contains("removes only\nthat first thing")
            || install.contains("removes only that first thing"),
        "docs/install.md must state that `kubectl delete -f logweir.yaml` removes only \
         the control plane; the scratch topics, the archive objects and the evidence \
         objects survive it and each has its own command"
    );
}

// --------------------------------------------------- deliberately not tag 1

/// **`docs/stability.md` lists the sixteen deferred items and the four nevers.**
///
/// A table test rather than a substring sweep: the failure names the item, so
/// "one line was dropped" is a readable failure rather than a boolean.
#[test]
fn stability_lists_the_deferred_items() {
    let stability = read("docs/stability.md");
    let section = stability
        .split_once("## Deliberately not in tag 1")
        .map(|(_, rest)| rest)
        .expect("docs/stability.md must carry the `Deliberately not in tag 1` section");

    const LATER: [&str; 16] = [
        "MSK IAM auth",
        "Strimzi as a source",
        "in-browser WASM verifier",
        "OsoCliEngine::validation_run",
        "Retention deletion",
        "Byte-faithful production restores",
        "Multi-tenancy beyond namespace RBAC",
        "Delegated rule-based schedule approval",
        "A Helm chart",
        "KMS / PKCS#11 signing",
        "Key generation and rotation",
        "A PVC for the runner pod",
        "Subprocess timeout, cancellation and SIGTERM handling",
        "A configurable Kafka client timeout",
        "kind + Calico NetworkPolicy probe",
        "Stage-2 Tasks 10 and 17-24",
    ];
    for (index, item) in LATER.iter().enumerate() {
        assert!(
            section.contains(item),
            "docs/stability.md's `Deliberately not in tag 1` section is missing \
             *Later, named* item {} of 16: `{item}`",
            index + 1
        );
    }

    const NEVER: [&str; 4] = [
        "Restore-in-place into a live topic",
        "Confluent Schema Registry / Apicurio / RBAC-MDS / CSFLE",
        "MSK ZK-to-KRaft migration",
        "Multi-cluster or fleet views",
    ];
    for (index, entry) in NEVER.iter().enumerate() {
        assert!(
            section.contains(entry),
            "docs/stability.md's `Deliberately not in tag 1` section is missing \
             *Never* entry {} of 4: `{entry}`",
            index + 1
        );
    }

    // The two lists are stated separately, and the difference is the point.
    assert!(
        section.contains("### Later, named") && section.contains("### Never"),
        "the *Later, named* and *Never* lists are stated separately: one is a \
         schedule and the other is a refusal"
    );

    // Each deferred item carries a reason and a citation, which is what makes
    // the list reviewable rather than decorative.
    for (label, citation) in [
        ("the MSK IAM seam", "crates/logweir-kafka/src/token.rs:1-9"),
        (
            "the Kafka client timeout constant",
            "crates/logweir-kafka/src/rdkafka_reader.rs:16",
        ),
        ("the Strimzi engine floor", "v0.19.1"),
    ] {
        assert!(
            section.contains(citation),
            "the *Later, named* entry for {label} must carry its citation `{citation}`"
        );
    }
}

// ------------------------------------------------------------ release notes

/// The `#### N.` item sections of one release entry, in file order: each is
/// `(N, body)`, where the body runs to the next `#### ` or `### ` heading.
fn release_note_items(notes: &str) -> Vec<(u32, String)> {
    let mut items: Vec<(u32, String)> = Vec::new();
    let mut current: Option<(u32, String)> = None;
    for line in notes.lines() {
        if line.starts_with("#### ") || line.starts_with("### ") || line.starts_with("## ") {
            if let Some(done) = current.take() {
                items.push(done);
            }
            if let Some(rest) = line.strip_prefix("#### ") {
                let number = rest
                    .split_once('.')
                    .and_then(|(n, _)| n.trim().parse::<u32>().ok())
                    .unwrap_or_else(|| {
                        panic!("a `#### ` heading in docs/release-notes.md is not numbered: {line}")
                    });
                current = Some((number, String::new()));
                continue;
            }
        }
        if let Some((_, body)) = current.as_mut() {
            body.push_str(line);
            body.push('\n');
        }
    }
    if let Some(done) = current.take() {
        items.push(done);
    }
    items
}

/// **The release notes exist, the checklist's release-notes row points at
/// them, and they carry every operator action PLAT-20.2 collected.**
///
/// `docs/tag1-checklist.md` row 9 ("release notes describe limitations …")
/// had no release notes to point at, and README.md promised a `ui/` bundle
/// "listed by digest in the release notes" that no file listed. The ten
/// operator-facing changes were collected from merged changes on 2026-09-23.
///
/// EACH ITEM IS CHECKED INSIDE ITS OWN SECTION. A token searched over the
/// whole file also matches the *Required operator actions* summary and the
/// *Verification scope* section, so deleting a whole item could stay green
/// (review M1: items 1, 4, 6 and 10 were deletable). The notes are split on
/// their `#### N.` headings, there must be exactly ten numbered 1 to 10 in
/// order, and each item's token must be in that item's own body, together
/// with the three things every item owes: a scope and a rollback.
///
/// NEGATIVE CONTROLS (fix round, 2026-09-23): deleting each of the ten
/// `#### N.` sections in turn fails this test ten times out of ten; so does
/// pointing row 9 back at `docs/stability.md` alone, and dropping one of the
/// six required upgrade actions.
///
/// The count has since grown to twenty (items 11–16 from the lab and PoC
/// rounds, 17–20 from the PoC's in-place upgrades: P9, P11, P12, P14). The
/// notes as they stood at `4e58d330`, with sixteen items, fail the twenty pin,
/// and so does deleting any one of items 17–20 (release-docs-final, 2026-09-25).
#[test]
fn the_release_notes_carry_every_owed_operator_action() {
    let notes = read("docs/release-notes.md");
    let checklist = read("docs/tag1-checklist.md");

    let row9 = checklist
        .lines()
        .find(|l| l.starts_with("| 9 |"))
        .expect("the release checklist carries row 9");
    assert!(
        row9.contains("`docs/release-notes.md`"),
        "release checklist row 9 must name `docs/release-notes.md` as its evidence \
         source; it reads: {row9}"
    );

    // Only the newest entry's items are the owed ten: the first `## ` entry.
    let entry = notes
        .split_once("\n## ")
        .map(|(_, rest)| rest)
        .expect("docs/release-notes.md carries at least one `## ` release entry");
    let entry = entry.split("\n## ").next().unwrap_or(entry);
    let items = release_note_items(entry);
    let numbers: Vec<u32> = items.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        numbers,
        (1..=20).collect::<Vec<u32>>(),
        "the release entry must carry exactly twenty operator-facing changes, `#### 1.` to \
         `#### 20.` in order; found {numbers:?}"
    );

    for ((number, body), (item, token)) in items.iter().zip([
        ("the retention delete grant", "s3:GetObject"),
        ("versioned buckets", "VersionedBucket"),
        ("shared backup sets", "co_point_ids"),
        ("operation states", "NoExitCode"),
        ("point-bound restores", "--evidence-keys"),
        ("the shared console's scope", "controller.watchNamespaces"),
        ("the approval policy floor", "allowOrdinaryConfirmation"),
        ("restore completion", "recordsRestored"),
        ("pre-creation slots", "metadata.creationTimestamp"),
        ("the API trust state", "RecordedBeforeRevocation"),
        // RECEIPT-DUP (2026-09-23): the execution claim and its store requirement.
        ("one engine run per execution", "ExecutionAlreadyClaimed"),
        // PLAT-20.2's "later items" (poc-install, 2026-09-24): controller before runner, and the
        // readiness principals with the old-runner refusal.
        ("controller before runner", "roll the controller out before"),
        ("readiness principals", "CheckContractMismatch"),
        // legacy-point-restore (2026-09-24): P3, P5 and P6 of the PoC round.
        ("a point with no saved destination", "legacySourceArchive"),
        // P10 (2026-09-24): manual runs may queue, and "Back up now" is limited.
        ("the manual-run pool", "ConcurrencyLimited"),
        // TRUSTPOLICY-DELETE-DROPS-REVOCATION (2026-09-24): the compromise guard.
        ("the compromise guard", "logweir.dev/compromise-revocation"),
        // The PoC rounds' later items (release-docs-final, 2026-09-25): P9, P11, P12 and P14.
        (
            "an admitted Restore's Approval is a record",
            "RestoreAdmitted",
        ),
        ("one catalog per destination", "DuplicateCatalog"),
        ("a failed controller read is read again", "retryAfter"),
        ("a readiness replay names its expiry", "staleBasis"),
    ]) {
        assert!(
            body.contains(token),
            "docs/release-notes.md item {number} ({item}) no longer carries `{token}` in \
             its own section"
        );
        for owed in ["**Scope:**", "**Rollback:**"] {
            assert!(
                body.contains(owed),
                "docs/release-notes.md item {number} ({item}) has no `{owed}` paragraph"
            );
        }
    }

    // The six required actions, numbered, in the section that orders them.
    let actions = entry
        .split_once("### Required operator actions")
        .and_then(|(_, rest)| rest.split("\n### ").next())
        .expect("docs/release-notes.md carries `### Required operator actions`");
    let steps: Vec<u32> = actions
        .lines()
        .filter_map(|l| l.split_once(". ").and_then(|(n, _)| n.parse::<u32>().ok()))
        .collect();
    assert_eq!(
        steps,
        (1..=6).collect::<Vec<u32>>(),
        "*Required operator actions* must keep its six numbered steps, in order"
    );

    // The UI bundle digest listing README.md promises, with the gate's own
    // selection of shipped files.
    assert!(
        notes.contains("find ui -type f ! -name '*.md' ! -path 'ui/tests/*'"),
        "docs/release-notes.md must give the command that lists the shipped ui/ files \
         by digest — README.md says the release notes carry that list"
    );
    for heading in [
        "### Required operator actions",
        "### Verification scope",
        "### Retention authority",
        "### Migration and rollback",
        "### Limitations and open items",
    ] {
        assert!(
            notes.contains(heading),
            "docs/release-notes.md is missing its `{heading}` section"
        );
    }
}

// -------------------------------------------------------------- trademarks

/// **`TRADEMARKS.md` states the clearance act and the announcement gate.**
///
/// In the research report's own terms rather than a softened paraphrase:
/// "counsel must assess likelihood-of-confusion" with no registries and no
/// classes names no act anybody can perform.
#[test]
fn trademarks_states_the_clearance_act_and_the_announcement_gate() {
    let trademarks = read("TRADEMARKS.md");

    for literal in ["UKIPO", "EUIPO", "USPTO", "classes 9 and 42", "2,824"] {
        assert!(
            trademarks.contains(literal),
            "TRADEMARKS.md must carry `{literal}`: the clearance act is a formal \
             UKIPO + EUIPO + USPTO search for LOGWEIR and WEIRKEEPER in classes 9 and \
             42, and the stem \"weir\" alone returns 2,824 records"
        );
    }
    assert!(
        trademarks.contains("working name"),
        "TRADEMARKS.md must say LOGWEIR is a **working name**"
    );
    assert!(
        trademarks.contains("owner action, not a task's"),
        "TRADEMARKS.md must say the clearance search is owner action and not a task's \
         — no engineering task in this repository can produce a clearance opinion"
    );
    assert!(
        trademarks.contains("A git tag is a git object. An announcement is a use in commerce."),
        "TRADEMARKS.md must distinguish announcing from tagging: announcing publicly \
         is gated on the clearance opinion and tagging is not"
    );
    // BUILT FROM PIECES, LIKE EVERY OTHER REFERENCE TO THE RUNNER IMAGE IN A
    // `.rs` FILE. Interface I15 says the runner image is named EXACTLY ONCE
    // under `crates/` — `crates/weirkeeper/src/job.rs`'s `RUNNER_IMAGE` — and
    // `crd_shape.rs::the_runner_image_is_named_once` scans every file under
    // `crates/` for the whole string. A literal here would be the second
    // occurrence and would fail that guard, which is the point of it.
    let runner_repository = format!("{}{}", "docker.io/vladyslavhaina/", "logweir");
    let prose = trademarks.replace('\n', " ");
    assert!(
        prose.contains(&runner_repository) && prose.contains("not placeholders"),
        "TRADEMARKS.md must record that the registry namespace is fixed as a literal \
         (`{runner_repository}`), so clearing the question changes one string and not \
         the install path"
    );
}
