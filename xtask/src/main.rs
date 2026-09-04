//! cargo xtask sync-upstream --tag v0.21.0 --upstream /path/to/kafka-backup
//!
//! For each vendored file, extract the `SOURCE:` header, read the named
//! upstream file, extract every `pub struct` / `pub enum` block named in the
//! vendored file, and compare the FIELD NAME SETS. A field added upstream is a
//! warning (serde(default) + the catch-all absorb it); a field REMOVED or
//! RETYPED upstream is an error, because our reader may be depending on it.
//!
//! An item that cannot be located at all, or that resolves to an empty
//! field/variant set, is ALSO an error -- never agreement. A rename or typo in
//! either the vendored file or upstream must not silently produce "zero
//! fields to disagree about", which a naive checker would report as a clean
//! pass. See `resolve` below.
use std::collections::BTreeSet;

/// Extracts the field names (structs) or variant names (enums) declared by
/// `pub struct <item> { .. }` or `pub enum <item> { .. }` in `src`.
/// Returns `None` when `item` cannot be located at all.
fn field_names(src: &str, item: &str) -> Option<BTreeSet<String>> {
    let (start, is_enum) = match src.find(&format!("pub struct {item} ")) {
        Some(s) => (s, false),
        None => (src.find(&format!("pub enum {item} "))?, true),
    };
    let body = &src[start..];
    let open = body.find('{')?;
    let mut depth = 0usize;
    let mut end = open;
    for (i, c) in body[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = open + i;
                    break;
                }
            }
            _ => {}
        }
    }
    let inner = &body[open + 1..end];
    if is_enum {
        // A variant is the leading identifier of a non-comment, non-attribute
        // line, whether it is `Full,`, `Partial(..)` or `Full { .. }`.
        Some(
            inner
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with("//") && !l.starts_with('#'))
                .filter_map(|l| l.split(['(', '{', ',']).next())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    } else {
        Some(
            inner
                .lines()
                .filter_map(|l| l.trim().strip_prefix("pub "))
                .filter_map(|l| l.split(':').next())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }
}

/// Resolves `item`'s field/variant set on one side (`side` is "ours" or
/// "upstream", `path` is the file it lives in). A missing item or an empty
/// set is a hard DRIFT, not agreement: a rename or typo in either file must
/// never be read as "nothing to disagree about".
fn resolve(
    src: &str,
    item: &str,
    side: &str,
    path: &str,
    tag: &str,
    failed: &mut bool,
) -> Option<BTreeSet<String>> {
    match field_names(src, item) {
        None => {
            println!(
                "DRIFT  {tag} {item}: not found in {side} ({path}) -- a rename or typo must never read as agreement"
            );
            *failed = true;
            None
        }
        Some(set) if set.is_empty() => {
            println!(
                "DRIFT  {tag} {item}: found in {side} ({path}) but declares zero fields/variants -- treating as drift, not agreement"
            );
            *failed = true;
            None
        }
        Some(set) => Some(set),
    }
}

/// Reads `path`, or prints a diagnostic and exits with a code distinct from
/// both success (0) and drift (1) so CI can tell "could not check" from
/// "drifted". A missing or unreadable upstream checkout is the ORDINARY case
/// for a contributor who hasn't cloned it, not an exceptional one -- it must
/// never be mistaken for a passing run.
fn read_or_diagnose(path: &str, advice: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("cannot read {path}: {e} -- {advice}");
        std::process::exit(2);
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let tag = args
        .iter()
        .position(|a| a == "--tag")
        .map(|i| args[i + 1].clone())
        .expect("--tag");
    let up = args
        .iter()
        .position(|a| a == "--upstream")
        .map(|i| args[i + 1].clone())
        .expect("--upstream");
    let mut failed = false;
    for (vend, upstream_rel, items) in [
        (
            "crates/logweir-engine-oso/src/vendored/manifest.rs",
            "crates/kafka-backup-core/src/manifest.rs",
            vec![
                "BackupManifest",
                "TopicBackup",
                "PartitionBackup",
                "SegmentMetadata",
                "OffsetGap",
                "PrunedRange",
                "DryRunReport",
                "DryRunTopicReport",
            ],
        ),
        (
            "crates/logweir-engine-oso/src/vendored/preflight.rs",
            "crates/kafka-backup-core/src/restore/preflight.rs",
            vec![
                "HeaderPreflightReport",
                "PartitionHeaderCoverage",
                "PartitionCoverageState",
            ],
        ),
    ] {
        let ours = read_or_diagnose(
            vend,
            "the vendored file should exist in-tree; run xtask from the repository root",
        );
        let upstream_path = format!("{up}/{upstream_rel}");
        let theirs = read_or_diagnose(
            &upstream_path,
            "clone kafka-backup at the pinned tag and pass its path via --upstream",
        );
        for item in items {
            let a = resolve(&ours, item, "ours", vend, &tag, &mut failed);
            let b = resolve(&theirs, item, "upstream", &upstream_path, &tag, &mut failed);
            let Some((a, b)) = a.zip(b) else {
                continue;
            };
            for gone in b.difference(&a) {
                println!("note   {tag} {item}: upstream has `{gone}`, we do not (added upstream)");
            }
            for extra in a.difference(&b) {
                // `extra` is the flatten catch-all every vendored struct carries so an
                // upstream addition degrades instead of failing the parse (spec §7.2);
                // `Unknown` is `PartitionCoverageState`'s hand-written catch-all variant
                // for the same reason (spec §11). Neither exists upstream by design, so
                // without this skip every clean run would report them as drift.
                if extra == "extra" || extra == "Unknown" {
                    continue;
                }
                println!("DRIFT  {tag} {item}: we read `{extra}`, upstream no longer declares it");
                failed = true;
            }
        }
    }
    if failed {
        eprintln!(
            "vendored structs have drifted from {tag}. Update them, bump the pin, \
                   and re-run the engine-matrix job before releasing."
        );
        std::process::exit(1);
    }
    println!("vendored structs agree with {tag}");
}
