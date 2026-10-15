//! THE DOCUMENTS A STRANGER ACTS ON.
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
//! for, the test **fails naming the path**: a skipping licence test is a check
//! that cannot fail.
//!
//! COSTS NOTHING AND REACHES NOTHING. Every test here reads checked-in files
//! (plus, in one case, the resolved crate source already on disk). Nothing
//! spawns cargo, opens a socket or starts a container, so the whole file is
//! milliseconds against the 15-second per-test budget.

mod support;
use support::{read, repo_root};

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

// --------------------------------------------------------------- the helpers

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
/// from `Cargo.lock`'s recorded version: fourteen today, `tinycthread` and
/// `wingetopt` among them. A fifteenth after an upstream bump FAILS this test
/// rather than passing it.
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

    // THE GUARD READS THE TABLE, AND READS IT BOTH WAYS. `tinycthread` and
    // `wingetopt` are also named in NOTICE's prose, so a whole-file
    // `contains` cannot see an attribution line deleted, and a one-way check
    // cannot see an invented component added. So: parse the table into a set
    // and assert SET EQUALITY with the derived fourteen.
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
/// notices, plural, and a notice with no `(c)` and no year is still a notice
/// (`aws-lc-sys`'s `LICENSE:9` is `Copyright Amazon.com, Inc. or its
/// affiliates.`). `ring`'s own notice is in the file that sorts LAST, and
/// `aws-lc-sys` names eleven holders. These four crates are the measured
/// witnesses, so they are the four asserted here. Reads two checked-in files
/// and nothing else (Global Constraint 22).
#[test]
fn the_inventory_carries_every_holder_a_licence_file_names() {
    let inventory = read("THIRD_PARTY_NOTICES.md");
    let entries = inventory_entries(&inventory);

    // Every holder in the file, not the first. `ring`'s own notice lives
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

    // A holder line with no `(c)` and no year is still a notice, and every
    // holder again: the OpenSSL-family notices aws-lc-sys vendors.
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
fn the_readme_states_its_threat_model_residuals() {
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

    // The release-notes note about the console's bytes.
    assert!(
        readme.contains("list the console image by digest")
            && readme.contains("`scripts/check-image-api.sh` holds the image's `/ui`"),
        "README.md must record that the release notes list the console image by digest and \
         that the image's /ui is held to the tree's build, so which bytes the page is has to \
         be checkable"
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
/// The runner and the controller are read here; the console image's three
/// licence files are `manifest_lint.rs`'s and `scripts/check-image-api.sh`'s.
///
/// WHAT EACH IMAGE OWES, AND ONLY WHAT IT OWES. A licence notice is a statement
/// about what is inside, not decoration, so a notice for something an image
/// does not carry is a FALSE ATTRIBUTION:
///
/// * both redistribute Logweir's own Apache-2.0 code, so both carry `LICENSE`
///   and `NOTICE`;
/// * both ship a statically linked Rust binary, so both carry
///   `THIRD_PARTY_NOTICES.md`, the inventory of that graph;
/// * only the runner redistributes the MIT-licensed `kafka-backup`, so only it
///   carries `/usr/share/licenses/kafka-backup/`. Asserted absent from the
///   controller here, and from the built image by
///   `scripts/check-image-weirkeeper.sh` check 3.
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
    ] {
        let dockerfile = read(path);
        let copies = copy_instructions(&dockerfile);
        assert!(
            !copies.is_empty(),
            "{path} has no COPY instruction; the parse is wrong, not the file"
        );

        // THE RUST INVENTORY IS OWED BY BOTH: each ships a Rust binary.
        for required in ["LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"] {
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

    // The controller redistributes none of it, and must not claim to.
    for operands in &copy_instructions(&read("Dockerfile.weirkeeper")) {
        for token in operands {
            assert!(
                !token.contains("kafka-backup"),
                "Dockerfile.weirkeeper COPYs a `kafka-backup` path (`{token}`). The controller \
                 image links no OSO code and redistributes no MIT-licensed binary, so that \
                 notice would be a FALSE ATTRIBUTION — a claim to redistribute something the \
                 image does not contain. `scripts/check-image-weirkeeper.sh` check 3 asserts \
                 the same absence from the built image."
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
/// A number in prose is a pointer git never checks, so the heading is PARSED,
/// the pointers are found, and the two must agree. The guide's headings are
/// unnumbered; the old numbers live in its *Section numbers before v1* table,
/// because code, messages and other documents still cite them, and the UI
/// section's number is the one that table's `Serving the UI` row gives.
///
/// Finding a pointer: a `§N` or `section N` marker with `Serving the UI` or
/// `UI section` within 60 characters on the same line, where the window stops
/// at the NEXT marker. That bound is what keeps the `§14` in
/// "`§14`'s X-DIGEST transcript and `§16`'s UI section" from being read as a
/// pointer at the UI section — it belongs to the transcript beside it.
#[test]
fn ui_section_pointers_name_the_heading_that_exists() {
    let kubernetes = read("docs/kubernetes.md");

    let headings: Vec<u32> = kubernetes
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("| \u{a7}")?;
            let (number, rest) = rest.split_once(" | ")?;
            let (title, _) = rest.split_once(" | ")?;
            (title.trim() == "Serving the UI").then(|| number.trim().parse::<u32>().ok())?
        })
        .collect();
    assert_eq!(
        headings.len(),
        1,
        "docs/kubernetes.md must carry exactly one `| \u{a7}<N> | Serving the UI |` row in \
         its *Section numbers before v1* table; found {}. Every pointer in the tree is \
         checked against that number, so two rows (or none) leaves the pointers checked \
         against nothing",
        headings.len()
    );
    let heading = headings[0];

    let mut sources: Vec<PathBuf> = docs_markdown();
    sources.push(repo_root().join("README.md"));
    sources.push(repo_root().join("console").join("README.md"));

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
                 but `docs/kubernetes.md` maps that section as \u{a7}{heading}. \
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
        "-f charts/logweir/examples/author-only.values.yaml",
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
    // true. A truncated `sha256:a5aa6dc1…`, a bare 64-hex run and a digest
    // wrapped across a line break are all a digest in prose, so the scan
    // takes every shape, over the file as written AND with its whitespace
    // removed.
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
         `charts/logweir/values.yaml` and `crates/weirkeeper/src/job.rs` as the \
         places the image references live, and the reader reads them from the \
         checkout. The bare `@sha256:<digest>` placeholder is fine — it carries no hex",
        digests.len(),
        digests.join("\n  ")
    );
    for source_of_truth in ["charts/logweir/values.yaml", "crates/weirkeeper/src/job.rs"] {
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
    // declared runner namespaces; identity-off installs apply the base account.
    assert!(
        install.contains("config/samples/backup-runner-serviceaccount.yaml"),
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
            && install.contains("is an index of linux/amd64 and\nlinux/arm64"),
        "install docs must state the pinned compatible bootstrap image, its re-pin check and its \
         architectures (PROD-14.1: an index of both)"
    );
    assert!(
        install.contains("post-rollback")
            && install.contains("rollback to a pre-bootstrap chart")
            && install.contains("retention—not active validation"),
        "rollback validation must be limited to targets that actually carry the hook"
    );
    assert!(
        install.contains("connected Helm operation")
            && install.contains("is **refused by name**")
            && install.contains("`identity.renderedOffline: true`")
            && install.contains("never\nfor output you apply")
            && install.contains("exact\nIPv4 `/32` or IPv6 `/128`")
            && install.contains("TCP 443\nand 6443")
            && install.contains("Other API ports are unsupported"),
        "managed identity docs must say an offline render is refused, name the offline answer \
         (PROD-14.1) and state exact API CIDR/port requirements"
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
            && install.contains("(kubernetes.md#upgrade)"),
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
        install.contains("helm uninstall logweir -n logweir-system")
            && install.contains("kubectl --context docker-desktop delete -f charts/logweir/crds/"),
        "docs/install.md must carry the uninstall commands"
    );
    assert!(
        install.contains("removes only\nthat first thing")
            || install.contains("removes only that first thing"),
        "docs/install.md must state that uninstalling removes only \
         the control plane; the scratch topics, the archive objects and the evidence \
         objects survive it and each has its own command"
    );
}

// ------------------------------------------------- ADR 0008, Amendment D

/// **Amendment D names the engine's consumer-group subcommands (PROD-04.0c).**
/// Rule 8 lands an ADR 0008 amendment only with this lint. The amendment and
/// `scripts/check-no-oso.sh`'s secondary scan must name the same six tokens,
/// so neither can drop one without the other.
#[test]
fn amendment_d_denies_the_engines_group_subcommands_by_name() {
    let adr = read("docs/architecture.md");
    let amendment_d = adr
        .split_once("## Amendment D — engine command allowlist")
        .and_then(|(_, rest)| rest.split("\n## ").next())
        .expect("docs/architecture.md carries ADR 0008's Amendment D");
    let flat: String = amendment_d.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains("**Amendment (PROD-04.0c; after OD-3"),
        "Amendment D carries PROD-04.0c's amendment and names OD-3"
    );
    let gate = read("scripts/check-no-oso.sh");
    let secondary = gate
        .lines()
        .find(|l| l.contains("hits=$(grep -rnE"))
        .expect("check-no-oso.sh's secondary token scan");
    for token in [
        "snapshot-groups", // engine-token-ok: names the amendment's token, not an invocation
        "offset-reset",    // engine-token-ok: names the amendment's token, not an invocation
        "offset-reset-bulk", // engine-token-ok: names the amendment's token, not an invocation
        "offset-rollback", // engine-token-ok: names the amendment's token, not an invocation
        "three-phase-restore", // engine-token-ok: names the amendment's token, not an invocation
        "show-offset-mapping", // engine-token-ok: names the amendment's token, not an invocation
    ] {
        assert!(
            flat.contains(&format!("`{token}`")),
            "Amendment D must deny `{token}` by name"
        );
        assert!(
            secondary.contains(&format!("|{token}|")) || secondary.contains(&format!("|{token})")),
            "check-no-oso.sh's secondary scan must name `{token}`: {secondary}"
        );
    }
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

    // OD-2 narrowed Never #1 to a LIVE topic. Its row names the decision,
    // keeps the live-topic refusal, and names the one path it no longer
    // covers, so an edit that drops the narrowing, its citation, or the
    // refusal fails here.
    let never_1 = section
        .lines()
        .find(|l| l.starts_with("| 1 | **Restore-in-place into a live topic**"))
        .expect("docs/stability.md's Never #1 row");
    for needle in [
        "Narrowed by the owner's decision [OD-2]",
        "to exactly that: a LIVE topic",
        "into a topic that does NOT exist is not this entry",
        "behind its own approval subject (`originalName`)",
        "A name that exists, in any mode, and an identity mapping in a scratch drill stay refused",
    ] {
        assert!(
            never_1.contains(needle),
            "docs/stability.md's Never #1 row must carry OD-2's narrowing: missing `{needle}`"
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
/// them, and they carry every operator action.**
///
/// EACH ITEM IS CHECKED INSIDE ITS OWN SECTION: a token searched over the
/// whole file also matches the *Required operator actions* summary and the
/// *Verification scope* section, so a deleted item could stay green. The notes
/// are split on their `#### N.` headings, numbered 1 to 27 in order, and each
/// item's token must be in that item's own body, with a scope and a rollback.
/// Every line that lists the product-expansion items by number (the entry's
/// opening paragraph, the paragraph under the count heading and the upgrade
/// path in *Migration and rollback*) names each of them. `v0.2.0-rc.1`
/// shipped items 1–27; later items live in the entry after the tag, which
/// `the_entry_after_the_rc1_record_carries_its_own_items` checks.
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

    // The entry these items belong to: the one headed `main` after `v0.1.5`,
    // which since the tag holds the `v0.2.0-rc.1` record and the twenty-seven
    // items that candidate shipped. Everything after the tag is in the entry
    // above it, checked by `the_entry_after_the_rc1_record_carries_its_own_items`.
    let entry = release_entry(&notes, "Unreleased — `main` after `v0.1.5`");
    let items = release_note_items(entry);
    let numbers: Vec<u32> = items.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        numbers,
        (1..=27).collect::<Vec<u32>>(),
        "the release entry must carry exactly twenty-seven operator-facing changes, `#### 1.` \
         to `#### 27.` in order; found {numbers:?}"
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
        // The execution claim and its store requirement.
        ("one engine run per execution", "ExecutionAlreadyClaimed"),
        // Controller before runner, and the readiness principals with the
        // old-runner refusal.
        ("controller before runner", "roll the controller out before"),
        ("readiness principals", "CheckContractMismatch"),
        // Restoring a point with no saved destination.
        ("a point with no saved destination", "legacySourceArchive"),
        // Manual runs may queue, and "Back up now" is limited.
        ("the manual-run pool", "ConcurrencyLimited"),
        // The compromise guard.
        ("the compromise guard", "logweir.dev/compromise-revocation"),
        // The PoC rounds' later items.
        (
            "an admitted Restore's Approval is a record",
            "RestoreAdmitted",
        ),
        ("one catalog per destination", "DuplicateCatalog"),
        ("a failed controller read is read again", "retryAfter"),
        ("a readiness replay names its expiry", "staleBasis"),
        // runnerResources applied or refused, and the
        // pre-upgrade inventory of the objects whose behaviour changes.
        (
            "runnerResources applied or refused",
            "select(.spec.runnerResources // .spec.bounds.runnerResources)",
        ),
        // The replication factor a console restore asks
        // for, and the topic subset a resumed draft lost.
        (
            "a console restore's replication factor and topic subset",
            "*Discover topics*",
        ),
        // Two inert policy values withdrawn; the notes say so.
        (
            "two inert policy values withdrawn",
            "WITHDRAWN VALUES ARE SET AND IGNORED",
        ),
        // A newTopic restore's scorecard names the source
        // settings it did not reconstruct, never "intended".
        (
            "not-reconstructed source settings",
            "topic_parity.not_reconstructed",
        ),
        // The sign-in limit per client behind the
        // trusted ingress, and the audit note that says which counter.
        ("the sign-in limit per client", "loginRateKey"),
        // Every Job-owning controller reports a refused
        // pod; the operator's read of which namespaces refuse one today.
        (
            "a refused Job pod is reported by every kind",
            "--field-selector reason=FailedCreate",
        ),
        // A point-in-time restore of a LogAppendTime topic
        // is refused unless its plan selects by producer time.
        (
            "a LogAppendTime point in time refused or labelled",
            "PointInTimeByProducerTime",
        ),
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

    // ITEM 22 OWES MORE THAN ONE THING, each in its own
    // section: what an operator sees after the upgrade, what a larger factor
    // costs on the target, and the read that audits a Restore made from a
    // resumed draft.
    let (_, fx5) = items
        .iter()
        .find(|(n, _)| *n == 22)
        .expect("docs/release-notes.md carries item 22");
    for owed in [
        "What an operator sees after the console image is upgraded",
        "takes three times the storage it took there",
        "-o jsonpath='{.spec.planBytes}'",
    ] {
        assert!(
            fx5.contains(owed),
            "docs/release-notes.md item 22 (FX-5) no longer carries `{owed}` in its own section"
        );
    }

    // EVERY LINE THAT LISTS THE ITEMS AFTER `fdb48cd8` NAMES EACH OF THEM.
    // Items 1–20 are the first collection; every later one landed after
    // `fdb48cd8`, and three places say which: the entry's opening paragraph,
    // the paragraph under the count heading, and the upgrade path. A number
    // is matched as a whole word, outside backticks, so `fdb48cd8` names
    // nothing.
    let squeezed = entry.split_whitespace().collect::<Vec<_>>().join(" ");
    let sentence_with = |marker: &str| -> &str {
        let at = squeezed
            .find(marker)
            .unwrap_or_else(|| panic!("docs/release-notes.md no longer says `{marker}`"));
        let start = squeezed[..at].rfind(". ").map_or(0, |i| i + 2);
        let end = squeezed[at..]
            .find(". ")
            .map_or(squeezed.len(), |i| at + i + 1);
        &squeezed[start..end]
    };
    let heading_paragraph = {
        let at = entry
            .find("operator-facing changes\n")
            .expect("docs/release-notes.md carries `### The … operator-facing changes`");
        let body = &entry[at..];
        let body = &body[..body.find("\n#### ").unwrap_or(body.len())];
        body.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    let names = |text: &str, n: u32| -> bool {
        let outside: String = text
            .split('`')
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, s)| s)
            .collect::<Vec<_>>()
            .join(" ");
        outside
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token == n.to_string())
    };
    let last = numbers.last().copied().unwrap_or(0);
    for (place, text) in [
        (
            "the entry's opening paragraph",
            sentence_with("land after `fdb48cd8`"),
        ),
        (
            "the paragraph under the count heading",
            heading_paragraph.as_str(),
        ),
        (
            "the upgrade path in *Migration and rollback*",
            sentence_with("from `fdb48cd8` crosses items"),
        ),
    ] {
        for n in 21..=last {
            assert!(
                names(text, n),
                "docs/release-notes.md item {n} landed after `fdb48cd8`, and {place} does not \
                 name it: {text}"
            );
        }
    }

    // ITEM 27 OWES the inventory an operator runs before the runner rolls --
    // which source topics are LogAppendTime, by override or broker default,
    // printed by name -- and the opt-in that restores them knowingly.
    let (_, fx8) = items
        .iter()
        .find(|(n, _)| *n == 27)
        .expect("docs/release-notes.md carries item 27");
    for owed in [
        // The inventory prints topic NAMES, and `--all` covers
        // a broker default too.
        "/^ *message.timestamp.type=LogAppendTime/ {print t}",
        "restore.time_basis: producerTime",
        "RehearsalSchedule",
    ] {
        assert!(
            fx8.contains(owed),
            "docs/release-notes.md item 27 (FX-8) no longer carries `{owed}` in its own section"
        );
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

/// The body of the release entry whose `## ` heading is `heading`, up to the
/// next `## ` heading.
fn release_entry<'a>(notes: &'a str, heading: &str) -> &'a str {
    let at = notes
        .find(&format!("\n## {heading}\n"))
        .unwrap_or_else(|| panic!("docs/release-notes.md has no `## {heading}` entry"));
    let body = &notes[at + 1..];
    let body = body.split_once('\n').map_or(body, |(_, rest)| rest);
    body.split("\n## ").next().unwrap_or(body)
}

/// **The changes after `v0.2.0-rc.1` have their own entry.** The tag froze the
/// entry below it at twenty-seven items with its candidate record; an item
/// that lands on `main` afterwards is not in the rc.1 binaries, so listing it
/// there would claim it shipped. The newest entry is headed `main` after
/// `v0.2.0-rc.1`, it is the FIRST `## ` entry, its items continue the
/// numbering from 28 without a gap, each in its own section with its token, a
/// scope and a rollback, and its opening paragraph and its upgrade sentence
/// name every one of them.
#[test]
fn the_entry_after_the_rc1_record_carries_its_own_items() {
    let notes = read("docs/release-notes.md");
    let heading = "Unreleased — `main` after `v0.2.0-rc.1`";
    let first = notes
        .split_once("\n## ")
        .and_then(|(_, rest)| rest.split_once('\n'))
        .map(|(h, _)| h)
        .expect("docs/release-notes.md carries a `## ` entry");
    assert_eq!(first, heading, "the newest entry must come first");
    let entry = release_entry(&notes, heading);
    let items = release_note_items(entry);
    let numbers: Vec<u32> = items.iter().map(|(n, _)| *n).collect();
    let rc1_last = release_note_items(release_entry(&notes, "Unreleased — `main` after `v0.1.5`"))
        .last()
        .map(|(n, _)| *n)
        .expect("the rc.1 entry carries items");
    assert_eq!(
        numbers,
        (rc1_last + 1..=rc1_last + items.len() as u32).collect::<Vec<u32>>(),
        "the entry after v0.2.0-rc.1 must number its items on from {}: {numbers:?}",
        rc1_last + 1
    );
    let tokens = [
        // The engine pin moved to 0.23.3; a
        // standalone install changes its engine identity with it.
        ("the engine pin is 0.23.3", "LOGWEIR_ENGINE_VERSION"),
        // No approver key by default, and the marker
        // an upgrade never reaches.
        ("no approver key by default", "logweir.dev/approval-default"),
        // Complete coverage, and what every 1.4.0
        // scorecard says its verdict covered.
        ("complete coverage", "coverage: complete"),
        // Every scheduled point is offered once the
        // catalog is synced by the new runner; the operator re-syncs it.
        (
            "scheduled points offered after a re-sync",
            "spec.syncRequest",
        ),
        // Every receipt records each topic's
        // configuration model, and the console defaults from it.
        ("the topic configuration model", "topic_configuration"),
        // Three new auth modes, and the credential
        // binding every credentialed connection's Secret must carry.
        (
            "a connection presents only its own credential",
            "logweir-binding",
        ),
        // A point-bound restore restores its point's own
        // set or is refused; a standalone plan naming `latestCompleted` is.
        (
            "a bound restore restores its point's set",
            "PointBindingSetMismatch",
        ),
        // Logweir's build of the engine, both
        // platforms, signed; and the build argument that rolls the engine back.
        (
            "the engine is built from the vendored source",
            "ENGINE_SOURCE=oso",
        ),
        // An early-stopped restore is never signed pass,
        // and a sampled scorecard names the topics its cap left unsampled.
        (
            "an early-stopped restore is never signed pass",
            "sample.unsampled_topics",
        ),
        // One crate may hold `unsafe` code, and
        // rustc forbids it everywhere else through the workspace lint table.
        ("one crate holds all unsafe code", "[workspace.lints.rust]"),
        // Every other credential reference is bound, and
        // the one-Secret-one-object upgrade tool binds the existing ones.
        (
            "every other credential reference is bound",
            "scripts/bind-credential.py",
        ),
        // Phase 0 uses a topic it created only once the
        // cluster serves it.
        (
            "a created topic is used only once served",
            "until every partition of each topic it created has a",
        ),
        // A connection that sends nothing meets the
        // console's header deadline, and the console no longer serves HTTP/2,
        // so an ingress configured for an `h2c` backend must go back to 1.1.
        (
            "a silent connection meets the console's header deadline",
            "h2c",
        ),
        // An unrecorded replication factor is not
        // assessed, never matching; the engine's first patch, build 2.
        (
            "a replication factor the archive does not record",
            "replication_factor (notRecorded)",
        ),
        // A plan may select a window start (partition
        // subsets were refused until OD-9; item 51), and the scorecard signs
        // it.
        ("a restore can select a window start", "source.selection"),
        // Item 43: complete coverage requested
        // through the CRDs, the API and the console, and a standing scope
        // that signs it.
        (
            "complete coverage through the CRDs, the API and the console",
            "spec.coverage: complete",
        ),
        // A client that stops reading an answer, or
        // stops sending a body, meets the console's stall deadline; the
        // constant names the bound an operator can find in the source.
        (
            "a stopped client meets the console's stall deadline",
            "IO_STALL_TIMEOUT",
        ),
        // Item 45: a status is written only when its
        // content moves; the field whose leftover spun the PoC's schedules.
        (
            "a controller no longer rewrites an unchanged status",
            "status.lastSlot.backupRef",
        ),
        // Item 46: topics whose records need a schema
        // registry are flagged from the archived bytes, and the console says
        // the registry was not captured.
        (
            "schema-dependent topics are flagged",
            "Registry not captured: applications may not read these",
        ),
        // Item 47: a sign-in whose identity provider
        // stalls its body is answered at the provider deadline, under the
        // audit code an operator alerts on.
        (
            "a stalled identity provider meets the provider deadline",
            "provider_timeout",
        ),
        // Item 48: a destination's Test access compares
        // every grant's binding on a blocking row, and is never READY for a
        // destination a backup would refuse.
        (
            "Test access compares every grant's binding",
            "destination.credentialBound",
        ),
        // Item 49: every receipt records each
        // topic's ID before and after the engine, and a recreated topic is a
        // new generation.
        ("each topic's ID before and after the engine", "generations"),
        // Item 50: a backup records the committed
        // positions of the groups it names, one outcome each, never offset 0
        // for absence.
        (
            "consumer position evidence for selected groups",
            "consumer_positions",
        ),
        // Item 51: a partition subset,
        // and the scorecard's first MAJOR it signs.
        (
            "a partition subset signs scorecard format 2.0.0",
            "logweir-drill-scorecard-2.0.0.json",
        ),
        // Item 52: every object-store read has a size cap;
        // the controller reads evidence under the relay's cap, which sets the
        // receipt's topic limit an operator must know.
        (
            "every object-store read has a size cap",
            "about **250–300 topics**",
        ),
        // Item 53: one peer outside the trusted proxy
        // holds at most its share of the console's connections, and a request
        // body must keep up the window's floor; the constant names the cap an
        // operator can find in the source.
        (
            "one peer's share of the console's connections",
            "MAX_CONNECTIONS_PER_PEER",
        ),
        // Item 54: a probe Job Kubernetes is collecting
        // is not a crash; the field that says it is being collected.
        (
            "a probe Job being collected does not clear reachable",
            "metadata.deletionTimestamp",
        ),
        // Item 55: a sign-in state is
        // single-use on each replica, and a replay there is refused under the
        // audit code an operator alerts on, before any token request.
        (
            "a sign-in state is single-use on each replica",
            "login_state_replayed",
        ),
        // Item 56: a restore under the original topic
        // names, into an absent topic, behind its own approval subject; the
        // flag an approver mints it with.
        (
            "a restore under the original topic names",
            "--approval-subject original-name",
        ),
        // Item 57: a restore preflight of a catalog point
        // makes the runner's own comparisons, reads only that point's own
        // receipt, and names the refusal an operator will meet.
        (
            "a catalog restore's preflight judges the archive as the runner will",
            "ManifestSuperseded",
        ),
        // Item 58: a RetentionPolicy's status says what
        // the per-run ceiling held back and never counts it as kept; the
        // field an operator reads the held-back count from.
        (
            "a retention status says what the per-run ceiling held back",
            "truncatedByCap",
        ),
        // Item 59: the shared console shows what the
        // product API publishes; the sentence a cell carries for a value the
        // API does not publish, which an operator will read on the Restore
        // detail.
        (
            "the shared console shows what the product API publishes",
            "not published by the product API",
        ),
        // Item 60: a readiness check asks the
        // endpoint what it can do, and the runner image moves with the
        // controller because every Backup and Restore plan lists the rows.
        (
            "capability rows, and the compatibility contract",
            "capabilityChecks",
        ),
        // Item 61: a guard-refused Restore or Backup says
        // why in its status, off the one stdout line the runner adds for it.
        (
            "a guard-refused run says why in its status",
            "refusal-detail=",
        ),
        // Item 62: every engine document turns
        // the engine's metrics server off; the key it renders.
        (
            "the engine's metrics port stays closed",
            "metrics: {enabled: false}",
        ),
        // Item 63: a restore started from a Backup is
        // bound to its point; the review row that says what the run reads.
        (
            "a restore started from a Backup is bound to its point",
            "recorded by the point",
        ),
        // Item 64: the status lists are cut at
        // the CRD's bound with their counts, and an `Enforce` run refuses a
        // partial or expired catalog view by name.
        (
            "a retention policy over 500 points keeps enforcing",
            "ViewIncomplete",
        ),
        // Item 65: one backup names at most 1,000
        // topics; the name a selection over it is refused under, at every
        // entry, before the engine runs.
        (
            "one backup names at most 1,000 topics",
            "BackupSelectionTooLarge",
        ),
        // Item 66: two-person approval in the
        // console; the policy setting the chart renders for it.
        (
            "two-person approval in the console",
            "approverSignature: Console",
        ),
        // Item 67: an archive holding transaction
        // markers is refused unless the plan allows them; the plan field.
        (
            "an archive holding transaction markers is refused",
            "restore.allow_transaction_markers: true",
        ),
        // Item 68: a backup of a source whose
        // records all expired is a signed empty capture; the reason an
        // operator reads on the `Backup`.
        (
            "a backup of an emptied source is a named empty capture",
            "EmptyCapture",
        ),
        // Item 69: a failed restore removes or names the
        // scratch topic it created; the line the controller reads it from.
        (
            "a failed restore removes or names the scratch topic it created",
            "teardown-failed=",
        ),
        // Item 70: the console's original-name box takes
        // effect when ticked; the box's own label.
        (
            "the original-name box takes effect when ticked",
            "Restore under the original topic names",
        ),
        // Item 71: a catalog's window holds the newest
        // points; the sentence a partial view's `Synced` message carries.
        (
            "a catalog's window holds the newest points",
            "It is a partial view",
        ),
        // Item 72: CI's default broker
        // line is 4.3.1 and its e2e job runs in shards; the variable that
        // still starts the legacy line.
        (
            "CI tests on Kafka 4.3.1, in four shards",
            "KAFKA_VERSION=3.7.1",
        ),
        // Item 73: scheduled rehearsals and
        // standing authorizations are withdrawn; the reason both refusals
        // carry.
        (
            "scheduled rehearsals and standing authorizations are withdrawn",
            "StandingAuthorizationRefused",
        ),
        // Item 74: parts v1 does not use are
        // withdrawn; the owner decision that withdrew them.
        ("four unused parts are withdrawn for v1", "OD-12"),
        // Item 75: the v1 cut's withdrawals; the
        // route an API client may still call.
        (
            "surfaces withdrawn for the v1 cut",
            "destinations:from-legacy",
        ),
        // Item 76: the chart is the only install;
        // the selector that removes a bundle install's control plane.
        (
            "the Helm chart is the only install",
            "-l app.kubernetes.io/name=logweir",
        ),
        // Item 77: an unparseable plan is refused
        // before its approval is read.
        ("an unparseable plan refused early", "PlanUnparseable"),
        // Item 78: a schedule's `spec.retention`
        // stops reporting; the condition a schedule that still sets it carries.
        (
            "a schedule's spec.retention stops reporting",
            "RetentionReportWithdrawn",
        ),
        // Item 79: every refused retention run is named on
        // its policy's status; the line the worker prints for it.
        (
            "a refused retention run says why",
            "retention-refusal=<code>",
        ),
        // Item 80: a setting the existing target does not
        // report counts as differing; the signed line it lands in.
        (
            "an unreported target setting counts as differing",
            "`differing config: [...]`",
        ),
        // Item 81: the hand-run cells
        // get repeatable rows; the weekly workflow that re-runs them.
        (
            "the matrix's hand-run cells get rows and a weekly job",
            "compat-weekly.yml",
        ),
        // Item 82: engine build 3; the patch that
        // makes it.
        (
            "the engine is build 3",
            "0003-restore-checkpoint-interval-and-hash.patch",
        ),
        // PROD-14.1 (item 83): a render that cannot ask the cluster is
        // refused; the value that answers it.
        ("an offline render is refused", "`identity.renderedOffline`"),
        // PROD-14.1 (item 84): the multi-platform identity hook; its pin.
        (
            "the identity hook runs natively on arm64",
            "sha256:8c5a76556236b3ff22d8618c1e2f141816eb60f6b1a476519b3b618efde348f6",
        ),
        // PROD-14.1 (item 85): the demo archive kept through an uninstall; the value.
        (
            "the demo archive can outlive helm uninstall",
            "`minio.persistence.keep: true`",
        ),
        // Item 86: the API document's version step for the next candidate.
        ("the API document is 1.0.0-alpha.3", "`1.0.0-alpha.3`"),
        // Item 87 (D-R1): a refusal stays named until the next run; the
        // record that keeps it.
        (
            "a refused run stays named until the next run",
            "`status.lastEnforcement.refusalCode`",
        ),
        // Item 88: the visual pass's console fixes; the member the Restore view gains.
        ("the visual pass's console fixes", "`integrityResult`"),
        // PROD-14.1 (item 89): the visual pass's console fixes; where Create
        // now goes.
        (
            "the visual pass's console fixes",
            "`#/operations?...&kind=restore`",
        ),
        (
            "a dropped connection no longer fails an evidence write",
            "error sending request`",
        ),
        // PROD-11.1a (item 91): safe clones; the finalizer that holds them.
        (
            "a scratch restore kept for a declared time",
            "`logweir.dev/clone-cleanup`",
        ),
        // PROD-05.2 (item 92): the target topic configuration; the plan block.
        (
            "a restore rebuilds its topics' configuration",
            "`target.topic_configuration: {transition: afterVerification | none, owners?}`",
        ),
        // PROD-05.1a (item 93): Strimzi owners from the controller; the value.
        (
            "backups record Strimzi owners",
            "`strimzi.readKafkaTopics: true`",
        ),
        // PROD-01.1b (item 94): out-of-order timestamps; the refusal.
        (
            "a record out of timestamp order is never lost",
            "`refusal-reason=PointInTimeSelectionIncomplete`",
        ),
        // PROD-04.2 (item 95): positions follow a restore; the new kind's record.
        (
            "consumer positions follow a restore",
            "`logweir/switchovers/<run_id>.record.json`",
        ),
        // FX-43 (item 96): sealed runner lines; the flag every runner Job takes.
        (
            "every runner line a controller acts on is sealed",
            "`--runner-line-token <hex>`",
        ),
        // Item 97: the control-evidence page removed; where the limits are.
        (
            "the control-evidence mapping page is removed",
            "verify-a-scorecard.md#what-the-scorecard-does-not-claim",
        ),
        // Item 98 (theme A's D1, D4): the evidence read grant; the runner flag
        // that names it.
        (
            "the switchover and a clone's cleanup read with the read grant",
            "`--evidence-read-grant`",
        ),
        // Item 99 (D3): a refused create leaves nothing; the answer.
        (
            "a create refused for a taken approval name",
            "`409 state_conflict`",
        ),
        // Item 100 (L1): the readers agree on a key file; the script version.
        (
            "both verifiers read a public key file the same way",
            "`verify_scorecard.py` 1.34.0",
        ),
        // Item 101 (D6): an older runner's pod rows; the row that says it ran.
        ("an older runner's pod rows say the pod ran", "`runner.pod`"),
        // PROD-18.1 (item 102): the rebuilt console and its addresses.
        ("the console is rebuilt", "`/ui/#/<namespace>/<page>`"),
        // FX-30 (item 103): a capped segment is not compared, never a pass.
        (
            "a complete verification reads under caps",
            "`covered: false`",
        ),
        // FX-38 (item 104): a cut Backup listing is Unknown, by name.
        ("a partial read says partial", "`BackupHistoryTooLarge`"),
        // FX-36 (item 105): one answer for a receipt not held.
        (
            "a bound restore reads only its own receipt",
            "`PointBindingMismatch`",
        ),
        // FX-12 (item 106): one instant per reconcile pass.
        ("evidence judged at the pass's instant", "`verifiedAt`"),
        // FX-45 (item 107): a missing READ named before the engine.
        ("a missing READ is named", "`TopicAuthorizationFailed`"),
        // FX-46 (item 108): the probe's third line.
        (
            "the probe asks every advertised broker",
            "`brokers-answered=<answered>/<listed>`",
        ),
        // DUI-1, DUI-2 (item 109): a stale edit's answer.
        (
            "the default access test and the precondition first",
            "`412 precondition_failed`",
        ),
        // DUI-5, DUI-25 (item 110): the sealed line that says why a run failed.
        ("a failed run says why", "`failure-detail=`"),
        // DUI-16, DUI-17 (item 111): the refusal the wizard no longer builds.
        (
            "the wizard asks who owns the topics",
            "`TopicOwnerNotChecked`",
        ),
        // DUI-21, DUI-27 (item 112): the signed requester details.
        (
            "a requester named by a signed identifier",
            "`logweir.dev/requester-display`",
        ),
        // DUI-26 (item 113): the refusal a long name caused.
        ("long Kafka names shown whole", "`PlanDisagreesWithMapping`"),
        // DUI-4 (item 114): the session warning's action.
        (
            "the console warns before a session ends",
            "Sign in again in a new tab",
        ),
        // DUI-7 to DUI-31 (item 115): layout and wording; a reason in words.
        ("layout and wording", "`DuplicateCatalog`"),
        // Item 116: RUSTSEC-2026-0346 to -0349; the version that fixes them.
        ("Logweir's binaries carry rustls 0.23.46", "rustls 0.23.46"),
        // Item 117: engine build 4; the patch that makes it.
        (
            "the engine is build 4",
            "0001-lockfile-rustls-h2-spin.patch",
        ),
        // DUI-38 (item 118): the state a restore's verdict reads while owed.
        (
            "a restore page follows its verdict",
            "`verification.state: pending`",
        ),
        // DUI-39, DUI-36 (item 119): what a long sampled name used to read.
        ("readiness checks name their topics", "`[redacted]`"),
        // DUI-43 (item 120): the reason a switchover's approval is consumed.
        (
            "a switchover's approval stays its record",
            "`SwitchoverAdmitted`",
        ),
        // DUI-42 (item 121): the refusal the restore page no longer leads to.
        (
            "no switchover where nothing switches",
            "`NoPositionsMapping`",
        ),
        // DUI-41.7 (item 122): the navigation that reads a page.
        ("a replaced sign-in reads a page", "`Accept: text/html`"),
        // DUI-22.3 to DUI-44 (item 123): a refused run is counted.
        ("the console counts refused runs", "\"1 refused\""),
        // Review M1 (item 124): the refusal a bundle that is not the admission's gets.
        (
            "a switchover runs only on its own bundle",
            "`ApprovalBundleConflict`",
        ),
        // DUI-45 (item 125): the refusal page's own stylesheet.
        ("the newer of two sign-ins finishes", "`/ui/sign-in.css`"),
        // DUI-46, DUI-47 (item 126): a pending verdict's instant, in words.
        ("a switchover record links by mode", "Last attempt at"),
        // DUI-48 (item 127): a complete restore's counts, in its own words.
        ("first probes and complete counts", "Compared and matching"),
    ];
    assert_eq!(
        items.len(),
        tokens.len(),
        "every item of the entry after v0.2.0-rc.1 needs its token row here: {numbers:?}"
    );
    for ((number, body), (item, token)) in items.iter().zip(tokens) {
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
    let squeezed = entry.split_whitespace().collect::<Vec<_>>().join(" ");
    let opening = squeezed
        .split(". ")
        .find(|s| s.contains("collects what lands on `main` after that publication"))
        .expect("the entry says what it collects");
    let upgrade = squeezed
        .split(". ")
        .find(|s| s.contains("An upgrade from `v0.2.0-rc.1`"))
        .expect("the entry gives the upgrade from v0.2.0-rc.1");
    // A number is matched as a whole token, outside backticks, so a digest
    // or a commit names nothing (the rule the rc.1 entry's check uses).
    let names = |text: &str, n: u32| -> bool {
        text.split('`')
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .flat_map(|(_, s)| s.split(|c: char| !c.is_ascii_alphanumeric()))
            .any(|token| token == n.to_string())
    };
    for &n in &numbers {
        for (place, text) in [
            ("its opening paragraph", opening),
            ("its upgrade sentence", upgrade),
        ] {
            assert!(
                names(text, n),
                "docs/release-notes.md item {n} is in the entry after v0.2.0-rc.1, and {place} \
                 does not name it: {text}"
            );
        }
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

// ------------------------------------- the grant tables and the version read

/// Every `*.rs` under `crates/*/src`, as `(repository-relative path, the
/// file's non-comment lines above its first `#[cfg(test)]`)`.
fn production_sources() -> Vec<(String, String)> {
    let root = repo_root();
    let mut files = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        for entry in
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()))
        {
            let path = entry.expect("a directory entry is readable").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_dir() {
                if name != "target" && name != "tests" {
                    stack.push(path);
                }
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
        .into_iter()
        .filter_map(|path| {
            let relative = path
                .strip_prefix(&root)
                .expect("under the repository")
                .to_string_lossy()
                .replace('\\', "/");
            if !relative.contains("/src/") {
                return None;
            }
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));
            let production = body.split("#[cfg(test)]").next().unwrap_or_default();
            let code = production
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            Some((relative, code))
        })
        .collect()
}

/// The row of the Markdown table under `header` whose first cell is `role`.
fn table_row<'a>(document: &'a str, file: &str, header: &str, role: &str) -> &'a str {
    let start = document
        .find(header)
        .unwrap_or_else(|| panic!("{file} has no table headed `{header}`"));
    document[start..]
        .lines()
        .take_while(|line| line.starts_with('|'))
        .find(|line| {
            line.split('|')
                .nth(1)
                .is_some_and(|cell| cell.trim() == role)
        })
        .unwrap_or_else(|| panic!("{file}: the table headed `{header}` has no `{role}` row"))
}

/// **The documented grants and the code's reads cannot drift.**
///
/// A read BY VERSION (`GET ?versionId=`) is authorised by AWS S3 as
/// `s3:GetObjectVersion`, a different action from `s3:GetObject`. So every
/// source file that asks a store for one version of an object is named here
/// with the ROLE whose grant it reads with, and that role's row in both
/// tables must name the action. The check fails in each direction a drift can
/// take:
///
/// * a NEW reader of a version (a file that USES the read, under either of
///   its names and however the use is spelled — [`uses_get_version`] — and is
///   not in [`READERS`]) — say which role's grant it uses, and put the action
///   in that role's rows;
/// * a row that LOSES the action while its reader still makes the read;
/// * a reader that no longer makes the read (a stale entry here, and a grant
///   the docs may then stop asking for);
/// * the store no longer reading by version at all.
///
/// **And the receipt a point-bound restore reads.** The
/// same two `archiveRead` readers fetch the plan's bound receipt, at a key
/// under [`logweir::catalog::record::RECEIPTS_PREFIX`], with the archive
/// grant, which the measured minimum (`<prefix>/*` alone) does not cover. So
/// the `archiveRead` row of both tables must also name `s3:GetObject` on that
/// namespace, spelled from the constant the code derives the key with: a row
/// that loses it fails here, and so does a namespace the code moved.
#[test]
fn the_documented_grants_name_the_version_read_of_every_role_that_makes_one() {
    /// `(reader, its row in docs/kubernetes.md, its row in docs/install.md)`.
    const READERS: [(&str, &str, &str); 3] = [
        (
            "crates/logweir/src/check/kinds/catalog_sync.rs",
            "`catalogSync` reader",
            "`RecoveryCatalog` sync",
        ),
        (
            "crates/logweir/src/check/kinds/restore.rs",
            "`archiveRead`",
            "`archiveRead`",
        ),
        (
            "crates/logweir/src/drill/binding.rs",
            "`archiveRead`",
            "`archiveRead`",
        ),
    ];
    /// Where the read is IMPLEMENTED and delegated; neither is a role.
    const IMPLEMENTATION: [&str; 2] = [
        "crates/logweir-store/src/lib.rs",
        "crates/logweir/src/check/store.rs",
    ];
    const ACTION: &str = "s3:GetObjectVersion";

    let sources = production_sources();
    assert!(
        sources.len() > 100,
        "the walk found {} source files; a scan that read nothing proves nothing",
        sources.len()
    );
    let store = sources
        .iter()
        .find(|(path, _)| path == IMPLEMENTATION[0])
        .map(|(_, code)| code.as_str())
        .expect("the store crate is scanned");
    // Every read has a cap, and the store's read by version is
    // `Store::get_version_capped`.
    assert!(
        store.contains("pub fn get_version_capped(")
            && store.contains("version: Some(version.to_string())"),
        "the store no longer reads an object by version; if no reader needs {ACTION} any more, \
         take it out of the grant tables and out of this test"
    );

    let callers = version_readers(&sources, &IMPLEMENTATION);
    let named: BTreeSet<&str> = READERS.iter().map(|(path, ..)| *path).collect();
    assert_eq!(
        callers, named,
        "the files that read an object BY VERSION are not the ones this test maps to a role. A \
         new reader needs {ACTION} on its role's grant: add it to READERS with the role, and \
         name the action in that role's rows of docs/kubernetes.md and docs/install.md. A \
         reader that stopped making the read leaves a grant the docs may stop asking for."
    );

    let kubernetes = read("docs/kubernetes.md");
    let install = read("docs/install.md");
    for (reader, kubernetes_role, install_role) in READERS {
        for (file, document, header, role) in [
            (
                "docs/kubernetes.md",
                &kubernetes,
                "| Role | Minimal actions, each at the resource scope shown |",
                kubernetes_role,
            ),
            (
                "docs/install.md",
                &install,
                "| Role | Actions | Resources |",
                install_role,
            ),
        ] {
            let row = table_row(document, file, header, role);
            assert!(
                row.contains(ACTION),
                "{file}: the {role} row does not name {ACTION}, and {reader} reads a manifest by \
                 version with that role's grant (a 403 there is `Unreadable`, exit 1, or \
                 `archive.backupSet AccessDenied`): {row}"
            );
        }
    }
    // What the tables rest on is said beside them: measured where it was
    // measured, and labelled where it was not.
    assert!(
        kubernetes.contains("**The read of a pinned version.**")
            && kubernetes
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .contains("MinIO serves it under `s3:GetObject` (measured 2026-10-09)"),
        "docs/kubernetes.md's grants section must keep the paragraph that says which store was \
         measured"
    );

    // The bound receipt's read: the `archiveRead` readers still read the
    // plan's receipt key, and the role's row grants the namespace it is in.
    let receipt_namespace = format!("<bucket>/{}*", logweir::catalog::record::RECEIPTS_PREFIX);
    assert_eq!(
        receipt_namespace, "<bucket>/logweir/backups/*",
        "the receipt namespace moved: both grant tables name it and must move with it"
    );
    for (reader, kubernetes_role, install_role) in READERS {
        if kubernetes_role != "`archiveRead`" {
            continue;
        }
        let code = sources
            .iter()
            .find(|(path, _)| path == reader)
            .map(|(_, code)| code.as_str())
            .unwrap_or_else(|| panic!("{reader} is scanned"));
        assert!(
            code.contains("receipt_key"),
            "{reader} no longer reads a plan's bound receipt; if no `archiveRead` reader does, \
             take the grant on {receipt_namespace} out of the rows and out of this test"
        );
        for (file, document, header, role) in [
            (
                "docs/kubernetes.md",
                &kubernetes,
                "| Role | Minimal actions, each at the resource scope shown |",
                kubernetes_role,
            ),
            (
                "docs/install.md",
                &install,
                "| Role | Actions | Resources |",
                install_role,
            ),
        ] {
            let row = table_row(document, file, header, role);
            assert!(
                row.contains(&receipt_namespace),
                "{file}: the {role} row does not grant `s3:GetObject` on {receipt_namespace}, and \
                 {reader} reads a point-bound plan's receipt there with that role's grant (a 403 \
                 is `archive.backupSet AccessDenied`, or exit 1 at the runner): {row}"
            );
        }
    }
}

/// The files among `sources` that read an object BY VERSION, other than the
/// ones in `implementation`, where the read is defined and delegated.
fn version_readers<'a>(
    sources: &'a [(String, String)],
    implementation: &[&str],
) -> BTreeSet<&'a str> {
    sources
        .iter()
        .filter(|(path, code)| !implementation.contains(&path.as_str()) && uses_get_version(code))
        .map(|(path, _)| path.as_str())
        .collect()
}

/// The two names the read by version has:
/// the store's own method, `Store::get_version_capped`, which the runner's
/// binding calls, and the check Jobs' seam over it,
/// `ObjectAccess::get_version`, which takes the same cap. Neither is part of
/// the other as a whole word, so each is looked for by itself.
const VERSION_READS: [&str; 2] = ["get_version_capped", "get_version"];

/// Whether `code` USES the store's read by version, under either of
/// [`VERSION_READS`] and however the use is spelled: a method call
/// (`access.get_version(…)`, `archive.get_version_capped(…)`, with or without
/// the receiver on the same line), a path call
/// (`Store::get_version_capped(&store, …)`,
/// `ObjectAccess::get_version(access, …)`, `<dyn ObjectAccess>::get_version`)
/// or the function taken as a value (`.map(Store::get_version_capped)`). The
/// name as a whole word is a use; only its own definition
/// (`fn get_version_capped`, `fn get_version`) and a longer identifier that
/// merely contains it are not.
fn uses_get_version(code: &str) -> bool {
    let part_of_a_name = |c: char| c.is_ascii_alphanumeric() || c == '_';
    VERSION_READS.iter().any(|name| {
        code.match_indices(name).any(|(at, _)| {
            let before = &code[..at];
            let after = &code[at + name.len()..];
            !before.chars().next_back().is_some_and(part_of_a_name)
                && !after.chars().next().is_some_and(part_of_a_name)
                && !before.trim_end().ends_with("fn")
        })
    })
}

/// [`uses_get_version`] sees every spelling of a use and no definition, so
/// the lint above cannot be passed by writing the same read another way
/// (`Store::get_version(&store, …)`), **and it sees the read under both its
/// names**: the store's `get_version_capped`, which the runner's binding
/// calls, and the check Jobs' `ObjectAccess::get_version`.
#[test]
fn a_read_by_version_is_seen_however_the_call_is_spelled() {
    for used in [
        // The check Jobs' seam, `ObjectAccess::get_version`.
        "let bytes = access.get_version(key, version, caps::MANIFEST)?;",
        "access\n    .get_version(&manifest_key, version, caps::MANIFEST)",
        "ObjectAccess::get_version(access, key, version, caps::MANIFEST)",
        "<dyn ObjectAccess>::get_version(access.as_ref(), key, version, caps::MANIFEST)",
        "pin::judge(pinned, current, digest, |v| access.get_version(key, v, caps::MANIFEST))",
        "versions.iter().map(|v| access.get_version (key, v, cap))",
        // The store's own read, `Store::get_version_capped`.
        "let (bytes, _) = archive.get_version_capped(key, version, caps::MANIFEST)?;",
        "archive\n    .get_version_capped(&manifest_key, version, caps::MANIFEST)",
        "Store::get_version_capped(&store, key, version, caps::MANIFEST)",
        "logweir_store::Store::get_version_capped(self, key, version, max_bytes).map(|(b, _)| b)",
        "let read = Store::get_version_capped;",
        "versions.iter().map(|v| store.get_version_capped (key, v, cap))",
    ] {
        assert!(uses_get_version(used), "a use went unseen: {used}");
    }
    for not_a_use in [
        "pub fn get_version_capped(\n        &self,\n        key: &str,\n        version: &str,",
        "    fn get_version(&self, key: &str, version: &str, max_bytes: u64) -> Result<Vec<u8>, StoreError>;",
        "fn   get_version(",
        "fn   get_version_capped(",
        "let v = get_versioned(key);",
        "let v = try_get_version_id(key);",
        "let v = get_version_capped_twice(key);",
        "let v = forget_version_capped(key);",
        // The reads of the CURRENT version are not reads by version.
        "store.get_capped(key, caps::MANIFEST)",
        "access.get_with_version(key, caps::MANIFEST)",
        "access.get(key, caps::SIGNED_DOCUMENT)",
        "",
    ] {
        assert!(!uses_get_version(not_a_use), "taken for a use: {not_a_use}");
    }

    // And the lint's own walk finds a path-form reader, leaves out a file
    // that only defines the read, and leaves out the implementation.
    let source = |path: &str, code: &str| (path.to_string(), code.to_string());
    let sources = [
        source(
            "crates/a/src/path_form.rs",
            "let b = Store::get_version_capped(&store, key, v, caps::MANIFEST)?;",
        ),
        source(
            "crates/a/src/trait_path_form.rs",
            "let b = ObjectAccess::get_version(access, key, v, caps::MANIFEST)?;",
        ),
        source(
            "crates/a/src/method_form.rs",
            "let b = access.get_version(key, v, caps::MANIFEST)?;",
        ),
        source(
            "crates/a/src/store_method_form.rs",
            "let (b, _) = archive.get_version_capped(key, v, caps::MANIFEST)?;",
        ),
        source(
            "crates/a/src/defines.rs",
            "fn get_version(&self, key: &str, v: &str, max_bytes: u64);",
        ),
        source(
            "crates/a/src/reads_current.rs",
            "let b = store.get_capped(key, caps::MANIFEST)?;",
        ),
        source(
            "crates/a/src/store.rs",
            "Store::get_version_capped(self, key, v, max_bytes)",
        ),
    ];
    assert_eq!(
        version_readers(&sources, &["crates/a/src/store.rs"]),
        BTreeSet::from([
            "crates/a/src/method_form.rs",
            "crates/a/src/path_form.rs",
            "crates/a/src/store_method_form.rs",
            "crates/a/src/trait_path_form.rs",
        ])
    );
}

/// **The three documents say which runner images stop under a controller
/// that passes `--line-token`, what that looks like, the chart keys that
/// exist, and the rollback order that is safe.** Each is held against the
/// chart's own values file: a key the chart does not have, or the rollback
/// order that PRODUCES the failure (the runner image first), fails here.
#[test]
fn the_line_token_skew_is_documented_with_real_chart_keys_and_the_safe_rollback_order() {
    let values = read("charts/logweir/values.yaml");
    for key in ["controllerImage:", "runnerImage:"] {
        assert!(
            values.lines().any(|l| l.starts_with(key)),
            "charts/logweir/values.yaml has no top-level `{key}`; the docs name it"
        );
    }
    assert!(
        !values.lines().any(|l| l.starts_with("image:")),
        "the chart has no top-level `image` key, and the docs must not name one"
    );
    // FX-34's release-notes item keeps what that release did; the guide and
    // the stability page now carry FX-43's contract: the pair is named.
    let fx34: &[&str] = &[
        // The keys an operator sets, as the chart spells them.
        "`controllerimage`",
        "`runnerimage`",
        // Which images: not "older than this release".
        "every runner image published before this change",
        // What it looked like, on the object and in the pod log.
        "`exitcode: 1`",
        "`exitreason: operational`",
        "error: unexpected argument '--line-token' found",
        // The single safe step of a two-step rollback.
        "or the controller first",
        // Why a tagged release's runner already needed the roll.
        "item 35",
    ];
    let fx43: &[&str] = &[
        "`runnerimage`",
        "every runner image published before",
        "runnerolderthancontroller",
        "error: unexpected argument '--runner-line-token' found",
        "or the controller first",
    ];
    for (doc, owed_list) in [
        ("docs/kubernetes.md", fx43),
        ("docs/stability.md", fx43),
        ("docs/release-notes.md", fx34),
    ] {
        // One line, lower case, emphasis marks aside: the three documents
        // wrap and stress the same sentences differently.
        let text = read(doc)
            .replace('*', "")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();
        for owed in owed_list {
            assert!(
                text.contains(owed),
                "{doc} no longer says `{owed}` about a runner image that does not know \
                 the controller's line-token flag"
            );
        }
        for wrong in [
            // The order that leaves this controller over an older runner.
            "or the runner image first",
            // A key the chart does not have.
            "`image` and `runnerimage`",
            // The reason that no longer exists (item 28's stamp).
            "which item 28 already orders",
        ] {
            assert!(
                !text.contains(wrong),
                "{doc} says `{wrong}` again; see FX-34's review, MED-1, MED-2 and H1"
            );
        }
    }
}
