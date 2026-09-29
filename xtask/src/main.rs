//! cargo xtask sync-upstream --tag v0.21.0 --upstream /path/to/kafka-backup
//!
//! The drift gate for the serde shapes vendored under
//! `crates/logweir-engine-oso/src/vendored/`. `CHECKS` pairs each vendored file
//! with every upstream file its shapes were ported from, and each vendored
//! item with the upstream item it mirrors. A PAIR, because a vendored shape
//! need not keep upstream's name: the consumer-groups snapshot's writer
//! declares its structs inside a function, as `Snapshot` and `GroupEntry`.
//! For every pair the gate compares:
//!
//! - NAMES (struct fields; enum variants). A name upstream declares and we do
//!   not is a note: `serde(default)` and the flattened catch-all absorb it. A
//!   name we read that upstream no longer declares is DRIFT.
//! - TYPES, of every field both sides declare (FX-1). A field RETYPED upstream
//!   is how a vendored shape stops reading real bytes while every name still
//!   matches: the consumer-groups snapshot's `offsets` was a list here and a
//!   map upstream, and `DryRunTopicReport.partitions` was once a count here and
//!   a sequence upstream. A name-only comparison passed both. The rules are in
//!   `compatible`; an incompatible pair is DRIFT unless `DIVERGENCES` declares
//!   exactly that pair, with its reason, and a declaration no comparison uses
//!   is DRIFT too, so the list cannot go stale.
//!
//! An item that cannot be located at all, or that resolves to an empty
//! field/variant set, is ALSO an error -- never agreement. A rename or typo in
//! either the vendored file or upstream must not silently produce "zero fields
//! to disagree about", which a naive checker would report as a clean pass.
//!
//! What it does not compare: serde attributes (a `rename` upstream would pass
//! it) and the bodies of enum variants.
//!
//! The unit tests at the bottom run the gate against the pinned source tarball
//! (`third_party/kafka-backup-v*.tar.gz`), so `cargo test --workspace` runs it
//! on every CI build, and they fail on a file under `vendored/` that `CHECKS`
//! does not cover.
use std::collections::BTreeSet;
use std::path::Path;

/// Where the vendored files live, relative to the repository root.
const VENDORED_DIR: &str = "crates/logweir-engine-oso/src/vendored";

/// One vendored file against one upstream file.
struct Check {
    /// File name under `VENDORED_DIR`.
    vendored: &'static str,
    /// Path relative to the root of an upstream checkout.
    upstream: &'static str,
    /// `(ours, upstream's)` item names.
    items: &'static [(&'static str, &'static str)],
}

const CHECKS: &[Check] = &[
    Check {
        vendored: "manifest.rs",
        upstream: "crates/kafka-backup-core/src/manifest.rs",
        items: &[
            ("BackupManifest", "BackupManifest"),
            ("TopicBackup", "TopicBackup"),
            ("PartitionBackup", "PartitionBackup"),
            ("SegmentMetadata", "SegmentMetadata"),
            ("OffsetGap", "OffsetGap"),
            ("PrunedRange", "PrunedRange"),
            ("DryRunReport", "DryRunReport"),
            ("DryRunTopicReport", "DryRunTopicReport"),
        ],
    },
    Check {
        vendored: "preflight.rs",
        upstream: "crates/kafka-backup-core/src/restore/preflight.rs",
        items: &[
            ("HeaderPreflightReport", "HeaderPreflightReport"),
            ("PartitionHeaderCoverage", "PartitionHeaderCoverage"),
            ("PartitionCoverageState", "PartitionCoverageState"),
        ],
    },
    // FX-1: the consumer-groups snapshot, against BOTH of its writers -- the
    // backup engine's `snapshot_consumer_groups` (fn-local structs) and the
    // `snapshot-groups` subcommand -- and against upstream's own reader.
    Check {
        vendored: "consumer_groups.rs",
        upstream: "crates/kafka-backup-core/src/backup/engine.rs",
        items: &[
            ("ConsumerGroupsSnapshot", "Snapshot"),
            ("ConsumerGroupEntry", "GroupEntry"),
        ],
    },
    Check {
        vendored: "consumer_groups.rs",
        upstream: "crates/kafka-backup-cli/src/commands/snapshot_groups.rs",
        items: &[
            ("ConsumerGroupsSnapshot", "ConsumerGroupsSnapshot"),
            ("ConsumerGroupEntry", "GroupEntry"),
        ],
    },
    Check {
        vendored: "consumer_groups.rs",
        upstream: "crates/kafka-backup-core/src/restore/engine.rs",
        items: &[
            ("ConsumerGroupsSnapshot", "AutoConsumerGroupSnapshot"),
            ("ConsumerGroupEntry", "AutoConsumerGroupSnapshotGroup"),
        ],
    },
];

/// A vendored field whose type deliberately differs from upstream's in a way
/// `compatible` does not accept, and why. `ours`/`upstream` are the types as
/// `render` prints them.
struct Divergence {
    vendored: &'static str,
    item: &'static str,
    field: &'static str,
    ours: &'static str,
    upstream: &'static str,
    #[allow(dead_code)] // read by people: the reason is the point of the entry
    why: &'static str,
}

const DIVERGENCES: &[Divergence] = &[
    Divergence {
        vendored: "manifest.rs",
        item: "OffsetGap",
        field: "reason",
        ours: "String",
        upstream: "OffsetGapReason",
        why: "a string-serialising enum (rename_all = snake_case) read as its string, so a \
              reason this build does not know never fails the parse",
    },
    Divergence {
        vendored: "manifest.rs",
        item: "PrunedRange",
        field: "reason",
        ours: "String",
        upstream: "PruneReason",
        why: "a string-serialising enum (rename_all = snake_case) read as its string, so a \
              reason this build does not know never fails the parse",
    },
];

/// Everything one run found.
#[derive(Debug, Default)]
struct Report {
    /// `DRIFT …` and `note …` lines, in order.
    lines: Vec<String>,
    drift: bool,
    /// `(vendored file, our item, upstream file, upstream item)` for every
    /// pair both sides resolved and the gate compared.
    compared: Vec<(String, String, String, String)>,
}

impl Report {
    fn drift(&mut self, line: String) {
        self.lines.push(line);
        self.drift = true;
    }
}

// ---------------------------------------------------------------------------
// Reading declarations out of Rust source.
// ---------------------------------------------------------------------------

/// If a string, raw string (`r"…"`, `r#"…"#`, `br#"…"#`) or char literal
/// starts at `i`, the index just past it. A lifetime or a label (`'a`) is not a
/// literal. Upstream's sources hold raw strings (JSON in test modules); a
/// scanner that took their inner quotes for string ends would lose track of
/// what is code and leave a doc comment inside a struct body.
fn literal_end(s: &str, i: usize) -> Option<usize> {
    let b = s.as_bytes();
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    match b[i] {
        b'"' => {
            let mut j = i + 1;
            while j < b.len() && b[j] != b'"' {
                if b[j] == b'\\' {
                    j += 1;
                }
                j += 1;
            }
            Some((j + 1).min(b.len()))
        }
        b'r' => {
            let starts_token = match i.checked_sub(1).map(|p| b[p]) {
                None => true,
                Some(b'b') => i < 2 || !ident(b[i - 2]),
                Some(p) => !ident(p),
            };
            if !starts_token {
                return None;
            }
            let mut j = i + 1;
            while j < b.len() && b[j] == b'#' {
                j += 1;
            }
            let hashes = j - i - 1;
            if b.get(j) != Some(&b'"') {
                return None; // `r#ident`, or an identifier starting with `r`
            }
            j += 1;
            while j < b.len() {
                if b[j] == b'"'
                    && b.len() >= j + 1 + hashes
                    && b[j + 1..j + 1 + hashes].iter().all(|&c| c == b'#')
                {
                    return Some(j + 1 + hashes);
                }
                j += 1;
            }
            Some(b.len())
        }
        b'\'' => {
            if b.get(i + 1) == Some(&b'\\') {
                let mut j = i + 3;
                while j < b.len() && b[j] != b'\'' {
                    j += 1;
                }
                return Some((j + 1).min(b.len()));
            }
            let c = s[i + 1..].chars().next()?;
            (b.get(i + 1 + c.len_utf8()) == Some(&b'\'')).then_some(i + 2 + c.len_utf8())
        }
        _ => None,
    }
}

/// `src` with every `//` and `/* */` comment removed (newlines kept), so that
/// neither a declaration nor a brace inside a comment is ever read.
fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = literal_end(src, i) {
            // Copied whole: `//` inside a literal is not a comment.
            out.push_str(&src[i..end]);
            i = end;
            continue;
        }
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    if b[i] == b'\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
                i = (i + 2).min(b.len());
            }
            _ => {
                let c = src[i..].chars().next().expect("a char at a char boundary");
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    out
}

/// `line` without a leading visibility (`pub`, `pub(crate)`, `pub(super)`,
/// `pub(in path)`).
fn strip_visibility(line: &str) -> &str {
    let l = line.trim_start();
    let Some(rest) = l.strip_prefix("pub") else {
        return l;
    };
    let rest = rest.trim_start();
    if let Some(inner) = rest.strip_prefix('(') {
        return match inner.find(')') {
            Some(close) => inner[close + 1..].trim_start(),
            None => l,
        };
    }
    if rest.len() < l.len() - 3 {
        // `pub` followed by whitespace, not an identifier starting with "pub".
        return rest;
    }
    l
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Struct,
    Enum,
}

/// The kind and the brace-delimited body of `struct <item>` or `enum <item>`,
/// declared at the start of a line of `src` (comments already stripped), at
/// any visibility and at any nesting -- the snapshot writer's structs are
/// declared inside a function. `None` when there is no such declaration.
fn find_item<'a>(src: &'a str, item: &str) -> Option<(Kind, &'a str)> {
    let mut offset = 0;
    for line in src.split_inclusive('\n') {
        let decl = strip_visibility(line);
        for (keyword, kind) in [("struct ", Kind::Struct), ("enum ", Kind::Enum)] {
            let Some(rest) = decl.strip_prefix(keyword) else {
                continue;
            };
            let rest = rest.trim_start();
            let Some(after) = rest.strip_prefix(item) else {
                continue;
            };
            if after.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                continue; // `struct SnapshotRange` is not `struct Snapshot`
            }
            let start = offset + (line.len() - decl.len());
            return brace_body(&src[start..]).map(|body| (kind, body));
        }
        offset += line.len();
    }
    None
}

/// The text between the first `{` of `s` and its matching `}`, skipping
/// literals. `None` for a tuple or unit struct (a `;` or `(` before the `{`).
fn brace_body(s: &str) -> Option<&str> {
    let open = s.find('{')?;
    if s[..open].contains([';', '(']) {
        return None;
    }
    let b = s.as_bytes();
    let (mut depth, mut i) = (0usize, open);
    while i < b.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        match b[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[open + 1..i]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// `body` with every `#[…]` attribute removed, skipping literals.
fn strip_attributes(body: &str) -> String {
    let b = body.as_bytes();
    let mut out = String::with_capacity(body.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'#' && b.get(i + 1) == Some(&b'[') {
            let mut depth = 0usize;
            while i < b.len() {
                if let Some(end) = literal_end(body, i) {
                    i = end;
                    continue;
                }
                match b[i] {
                    b'[' => depth += 1,
                    b']' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            continue;
        }
        let c = body[i..].chars().next().expect("a char at a char boundary");
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// `s` split on `sep` where no `<>`, `()`, `[]` or `{}` is open.
fn split_top_level(s: &str, sep: char) -> Vec<&str> {
    let (mut parts, mut depth, mut start) = (Vec::new(), 0i32, 0);
    for (i, c) in s.char_indices() {
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth -= 1,
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// The members of an item's body: `(field, Some(type))` for a struct,
/// `(variant, None)` for an enum.
type Members = Vec<(String, Option<String>)>;

fn members(kind: Kind, body: &str) -> Members {
    let body = strip_attributes(body);
    split_top_level(&body, ',')
        .into_iter()
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .filter_map(|m| {
            let m = strip_visibility(m);
            let name: String = m
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                return None;
            }
            match kind {
                Kind::Enum => Some((name, None)),
                Kind::Struct => {
                    let ty = m[name.len()..].trim_start().strip_prefix(':')?.trim();
                    Some((name, Some(ty.to_string())))
                }
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Types.
// ---------------------------------------------------------------------------

/// A field type, reduced to what decides whether two serde shapes read the
/// same JSON: the last path segment and the generic arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ty {
    Named(String, Vec<Ty>),
    Tuple(Vec<Ty>),
    /// Anything else (references, arrays, trait objects), compared as text.
    Other(String),
}

fn parse_ty(s: &str) -> Ty {
    let s = s.trim();
    let compact = || s.split_whitespace().collect::<String>();
    if let Some(inner) = s.strip_prefix('(').and_then(|r| r.strip_suffix(')')) {
        let elems: Vec<Ty> = split_top_level(inner, ',')
            .into_iter()
            .filter(|e| !e.trim().is_empty())
            .map(parse_ty)
            .collect();
        return Ty::Tuple(elems);
    }
    let (path, args) = match s.find('<') {
        Some(lt) => match s.strip_suffix('>') {
            Some(inner) => (&s[..lt], Some(&inner[lt + 1..])),
            None => return Ty::Other(compact()),
        },
        None => (s, None),
    };
    let name = path.rsplit("::").next().unwrap_or(path).trim();
    if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ty::Other(compact());
    }
    let args = args
        .map(|a| split_top_level(a, ',').into_iter().map(parse_ty).collect())
        .unwrap_or_default();
    Ty::Named(name.to_string(), args)
}

/// How `ty` prints in a diagnostic and in `DIVERGENCES`.
fn render(ty: &Ty) -> String {
    let list = |v: &[Ty]| v.iter().map(render).collect::<Vec<_>>().join(", ");
    match ty {
        Ty::Named(n, a) if a.is_empty() => n.clone(),
        Ty::Named(n, a) => format!("{n}<{}>", list(a)),
        Ty::Tuple(v) => format!("({})", list(v)),
        Ty::Other(s) => s.clone(),
    }
}

/// Two names serde reads from the same JSON.
fn canon(name: &str) -> &str {
    match name {
        "HashMap" | "BTreeMap" => "Map",
        "HashSet" | "BTreeSet" => "Set",
        other => other,
    }
}

/// Whether a vendored field of type `ours` reads everything upstream writes
/// into a field of type `theirs`. `rename` maps a vendored item name to the
/// upstream name it is paired with.
///
/// - Paths are ignored: `serde_json::Value` is `Value`.
/// - `HashMap` and `BTreeMap` are the same JSON object (likewise the sets).
/// - Our `Value` reads anything: an opaque field is opaque by design.
/// - Our `Option<T>` reads an upstream `T`: tolerating the absence of a field
///   upstream always writes is a widening. The reverse is DRIFT -- upstream
///   may omit or null what we require.
fn compatible(ours: &Ty, theirs: &Ty, rename: &dyn Fn(&str) -> String) -> bool {
    let all = |a: &[Ty], b: &[Ty]| {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| compatible(x, y, rename))
    };
    match (ours, theirs) {
        (Ty::Named(n, a), _) if n == "Value" && a.is_empty() => true,
        (Ty::Named(n, a), t)
            if n == "Option" && a.len() == 1 && !matches!(t, Ty::Named(m, _) if m == "Option") =>
        {
            compatible(&a[0], t, rename)
        }
        (Ty::Named(n, a), Ty::Named(m, b)) => canon(&rename(n)) == canon(m) && all(a, b),
        (Ty::Tuple(a), Ty::Tuple(b)) => all(a, b),
        (Ty::Other(a), Ty::Other(b)) => a == b,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// The gate.
// ---------------------------------------------------------------------------

/// Resolves `item`'s members on one side (`side` is "ours" or "upstream",
/// `path` is the file it lives in). A missing item or an empty set is a hard
/// DRIFT, not agreement: a rename or typo in either file must never be read as
/// "nothing to disagree about".
fn resolve(
    src: &str,
    item: &str,
    side: &str,
    path: &str,
    tag: &str,
    report: &mut Report,
) -> Option<(Kind, Members)> {
    match find_item(src, item) {
        None => {
            report.drift(format!(
                "DRIFT  {tag} {item}: not found in {side} ({path}) -- a rename or typo must never read as agreement"
            ));
            None
        }
        Some((kind, body)) => {
            let m = members(kind, body);
            if m.is_empty() {
                report.drift(format!(
                    "DRIFT  {tag} {item}: found in {side} ({path}) but declares zero fields/variants -- treating as drift, not agreement"
                ));
                return None;
            }
            Some((kind, m))
        }
    }
}

/// Compares one vendored file's text with one upstream file's text, for the
/// item pairs of `check`. `ours`/`theirs` are the raw sources.
fn compare(
    check: &Check,
    ours: &str,
    theirs: &str,
    tag: &str,
    divergences: &[Divergence],
    used: &mut BTreeSet<usize>,
    report: &mut Report,
) {
    let (ours, theirs) = (strip_comments(ours), strip_comments(theirs));
    let rename = |n: &str| {
        check
            .items
            .iter()
            .find(|(o, _)| *o == n)
            .map(|(_, u)| u.to_string())
            .unwrap_or_else(|| n.to_string())
    };
    for &(our_item, their_item) in check.items {
        let label = if our_item == their_item {
            our_item.to_string()
        } else {
            format!("{our_item} (upstream {their_item})")
        };
        let a = resolve(&ours, our_item, "ours", check.vendored, tag, report);
        let b = resolve(&theirs, their_item, "upstream", check.upstream, tag, report);
        let (Some((ka, a)), Some((kb, b))) = (a, b) else {
            continue;
        };
        if ka != kb {
            report.drift(format!(
                "DRIFT  {tag} {label}: ours is a {ka:?}, upstream ({}) a {kb:?}",
                check.upstream
            ));
            continue;
        }
        report.compared.push((
            check.vendored.to_string(),
            our_item.to_string(),
            check.upstream.to_string(),
            their_item.to_string(),
        ));
        let names =
            |m: &Members| -> BTreeSet<String> { m.iter().map(|(n, _)| n.clone()).collect() };
        let (na, nb) = (names(&a), names(&b));
        for gone in nb.difference(&na) {
            report.lines.push(format!(
                "note   {tag} {label}: upstream ({}) has `{gone}`, we do not (added upstream)",
                check.upstream
            ));
        }
        for extra in na.difference(&nb) {
            // `extra` is the flatten catch-all every vendored struct carries so an
            // upstream addition degrades instead of failing the parse (spec §7.2);
            // `Unknown` is `PartitionCoverageState`'s hand-written catch-all variant
            // for the same reason (spec §11). Neither exists upstream by design, so
            // without this skip every clean run would report them as drift.
            if extra == "extra" || extra == "Unknown" {
                continue;
            }
            report.drift(format!(
                "DRIFT  {tag} {label}: we read `{extra}`, upstream ({}) no longer declares it",
                check.upstream
            ));
        }
        for (field, ty_a) in &a {
            let Some(ty_a) = ty_a else { continue };
            let Some((_, Some(ty_b))) = b.iter().find(|(n, _)| n == field) else {
                continue;
            };
            let (pa, pb) = (parse_ty(ty_a), parse_ty(ty_b));
            if compatible(&pa, &pb, &rename) {
                continue;
            }
            let (ra, rb) = (render(&pa), render(&pb));
            let declared = divergences.iter().position(|d| {
                d.vendored == check.vendored
                    && d.item == our_item
                    && d.field == field
                    && d.ours == ra
                    && d.upstream == rb
            });
            match declared {
                Some(i) => {
                    used.insert(i);
                }
                None => report.drift(format!(
                    "DRIFT  {tag} {label}.{field}: we read `{ra}`, upstream ({}) declares `{rb}` -- \
                     a retyped field reads no real bytes however well its name matches",
                    check.upstream
                )),
            }
        }
    }
}

/// Runs every check: `root` is the repository root, `upstream` an upstream
/// checkout. `Err` names a file that could not be read, which is "could not
/// check", never "agrees".
fn run(
    root: &Path,
    upstream: &Path,
    tag: &str,
    checks: &[Check],
    divergences: &[Divergence],
) -> Result<Report, String> {
    let read = |p: &Path, advice: &str| {
        std::fs::read_to_string(p)
            .map_err(|e| format!("cannot read {}: {e} -- {advice}", p.display()))
    };
    let mut report = Report::default();
    let mut used = BTreeSet::new();
    for check in checks {
        let ours = read(
            &root.join(VENDORED_DIR).join(check.vendored),
            "the vendored file should exist in-tree; run xtask from the repository root",
        )?;
        let theirs = read(
            &upstream.join(check.upstream),
            "extract kafka-backup at the pinned tag and pass its path via --upstream",
        )?;
        compare(
            check,
            &ours,
            &theirs,
            tag,
            divergences,
            &mut used,
            &mut report,
        );
    }
    for (i, d) in divergences.iter().enumerate() {
        if !used.contains(&i) && checks.iter().any(|c| c.vendored == d.vendored) {
            report.drift(format!(
                "DRIFT  {tag} {}.{}: the declared divergence `{}` vs upstream `{}` no longer \
                 describes the code -- remove or correct it",
                d.item, d.field, d.ours, d.upstream
            ));
        }
    }
    Ok(report)
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
    // A missing or unreadable upstream checkout is the ORDINARY case for a
    // contributor who hasn't extracted it, not an exceptional one: exit 2, a
    // code distinct from both success (0) and drift (1), so CI can tell "could
    // not check" from "drifted" and neither is mistaken for a pass.
    let report =
        run(Path::new("."), Path::new(&up), &tag, CHECKS, DIVERGENCES).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
    for line in &report.lines {
        println!("{line}");
    }
    if report.drift {
        eprintln!(
            "vendored structs have drifted from {tag}. Update them, bump the pin, \
                   and re-run the engine-matrix job before releasing."
        );
        std::process::exit(1);
    }
    println!(
        "vendored structs agree with {tag} ({} item pairs, names and types)",
        report.compared.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .canonicalize()
            .expect("the repository root resolves")
    }

    /// The pinned source tarball: exactly one `third_party/kafka-backup-v*.tar.gz`.
    fn pinned_tarball() -> (PathBuf, String) {
        let dir = repo_root().join("third_party");
        let found: Vec<(PathBuf, String)> = std::fs::read_dir(&dir)
            .expect("third_party/ lists")
            .filter_map(|e| {
                let p = e.ok()?.path();
                let name = p.file_name()?.to_str()?.to_string();
                let tag = name
                    .strip_prefix("kafka-backup-")?
                    .strip_suffix(".tar.gz")?;
                Some((p.clone(), tag.to_string()))
            })
            .collect();
        assert_eq!(
            found.len(),
            1,
            "exactly one pinned source tarball: {found:?}"
        );
        found.into_iter().next().unwrap()
    }

    /// The upstream files `CHECKS` names, extracted ONCE per test process from
    /// the pinned tarball with `tar` (bounded: killed after 60 s). Returns the
    /// checkout root and the tag.
    fn upstream_checkout() -> &'static (PathBuf, String) {
        static CHECKOUT: std::sync::OnceLock<(PathBuf, String)> = std::sync::OnceLock::new();
        CHECKOUT.get_or_init(|| {
            let (tarball, tag) = pinned_tarball();
            let top = format!("kafka-backup-{}", tag.trim_start_matches('v'));
            let dest = std::env::temp_dir().join(format!(
                "logweir-xtask-upstream-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dest).unwrap();
            let members: BTreeSet<String> = CHECKS
                .iter()
                .map(|c| format!("{top}/{}", c.upstream))
                .collect();
            let mut child = Command::new("tar")
                .arg("-xzf")
                .arg(&tarball)
                .arg("-C")
                .arg(&dest)
                .args(&members)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("tar starts");
            let deadline = Instant::now() + Duration::from_secs(60);
            let status = loop {
                if let Some(s) = child.try_wait().expect("tar is waitable") {
                    break s;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "tar did not finish extracting {} in 60 s",
                        tarball.display()
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            assert!(
                status.success(),
                "tar -xzf {} failed: {status}",
                tarball.display()
            );
            (dest.join(top), tag)
        })
    }

    fn upstream_src(rel: &str) -> String {
        let (root, _) = upstream_checkout();
        std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
    }

    fn vendored_src(file: &str) -> String {
        std::fs::read_to_string(repo_root().join(VENDORED_DIR).join(file))
            .unwrap_or_else(|e| panic!("{file}: {e}"))
    }

    /// The checks of one vendored file, run against the real upstream sources
    /// with `ours` standing in for the file's text.
    fn run_file_with(file: &str, ours: &str, divergences: &[Divergence]) -> Report {
        let (_, tag) = upstream_checkout();
        let mut report = Report::default();
        let mut used = BTreeSet::new();
        for check in CHECKS.iter().filter(|c| c.vendored == file) {
            let theirs = upstream_src(check.upstream);
            compare(
                check,
                ours,
                &theirs,
                tag,
                divergences,
                &mut used,
                &mut report,
            );
        }
        report
    }

    /// `text` with `from` replaced by `to`, asserting `from` was there: a mutant
    /// whose target moved must fail loudly, never pass by mutating nothing.
    fn mutate(text: &str, from: &str, to: &str) -> String {
        assert!(text.contains(from), "the mutation target `{from}` is gone");
        text.replacen(from, to, 1)
    }

    fn drift_lines(r: &Report) -> Vec<&str> {
        r.lines
            .iter()
            .map(String::as_str)
            .filter(|l| l.starts_with("DRIFT"))
            .collect()
    }

    /// **The gate itself, at the pin.** Every vendored shape agrees with the
    /// tarball's, names and types, and every pair in `CHECKS` was really
    /// compared (a pair that resolved on neither side would be DRIFT, so a
    /// green here compared all of them).
    #[test]
    fn the_vendored_shapes_agree_with_the_pinned_engine_source() {
        let (up, tag) = upstream_checkout();
        let r = run(&repo_root(), up, tag, CHECKS, DIVERGENCES).expect("every file reads");
        assert!(!r.drift, "drift at the pin:\n{}", r.lines.join("\n"));
        let pairs: usize = CHECKS.iter().map(|c| c.items.len()).sum();
        assert_eq!(r.compared.len(), pairs, "{:?}", r.compared);
        for writer in [
            "crates/kafka-backup-core/src/backup/engine.rs",
            "crates/kafka-backup-cli/src/commands/snapshot_groups.rs",
        ] {
            assert!(
                r.compared
                    .iter()
                    .any(|(v, _, u, _)| v == "consumer_groups.rs" && u == writer),
                "consumer_groups.rs was not compared with its writer {writer}"
            );
        }
    }

    /// **The gate cannot skip a file.** Every `.rs` under `vendored/` but
    /// `mod.rs` is covered by `CHECKS`; FX-1's `consumer_groups.rs` was not.
    #[test]
    fn every_vendored_file_is_gated() {
        let dir = repo_root().join(VENDORED_DIR);
        let files: BTreeSet<String> = std::fs::read_dir(&dir)
            .expect("vendored/ lists")
            .filter_map(|e| e.ok()?.file_name().to_str().map(str::to_string))
            .filter(|n| n.ends_with(".rs") && n != "mod.rs")
            .collect();
        assert!(files.contains("consumer_groups.rs"), "{files:?}");
        let gated: BTreeSet<String> = CHECKS.iter().map(|c| c.vendored.to_string()).collect();
        let ungated: Vec<&String> = files.difference(&gated).collect();
        assert!(
            ungated.is_empty(),
            "vendored files the drift gate never checks: {ungated:?}"
        );
    }

    /// **FX-1 would have been caught.** The shape that stood in
    /// `consumer_groups.rs` before FX-1, verbatim, against the real writers:
    /// the invented field names and the list-typed `offsets` are all DRIFT.
    #[test]
    fn the_shape_consumer_groups_rs_had_before_fx1_is_drift() {
        const BEFORE: &str = r#"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupsSnapshot {
    #[serde(default)]
    pub backup_id: String,
    #[serde(default)]
    pub captured_at: i64,
    #[serde(default)]
    pub groups: Vec<ConsumerGroupEntry>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupEntry {
    #[serde(default)]
    pub group_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub offsets: Vec<Value>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
"#;
        let r = run_file_with("consumer_groups.rs", BEFORE, DIVERGENCES);
        let d = drift_lines(&r).join("\n");
        for needle in [
            "`captured_at`",
            "`backup_id`",
            "`state`",
            ".offsets: we read `Vec<Value>`",
        ] {
            assert!(d.contains(needle), "no DRIFT naming {needle}:\n{d}");
        }
    }

    /// Mutant: the field back under its OLD name. Names catch it.
    #[test]
    fn a_snapshot_field_under_the_old_name_is_drift() {
        let m = mutate(
            &vendored_src("consumer_groups.rs"),
            "pub snapshot_time: Option<i64>,",
            "pub captured_at: Option<i64>,",
        );
        let d = drift_lines(&run_file_with("consumer_groups.rs", &m, DIVERGENCES)).join("\n");
        assert!(d.contains("we read `captured_at`"), "{d}");
    }

    /// Mutant: `offsets` as a flat list again. Only the TYPE comparison sees it:
    /// every name still matches.
    #[test]
    fn offsets_retyped_as_a_flat_list_is_drift() {
        let m = mutate(
            &vendored_src("consumer_groups.rs"),
            "pub offsets: BTreeMap<String, BTreeMap<String, i64>>,",
            "pub offsets: Vec<Value>,",
        );
        let r = run_file_with("consumer_groups.rs", &m, DIVERGENCES);
        let d = drift_lines(&r);
        assert_eq!(d.len(), 3, "one per upstream declaration: {d:?}");
        assert!(
            d.iter()
                .all(|l| l.contains(".offsets: we read `Vec<Value>`")),
            "{d:?}"
        );
    }

    /// Mutant: a per-group field the writer never wrote (the invented `state`).
    #[test]
    fn a_field_the_writer_never_wrote_is_drift() {
        let m = mutate(
            &vendored_src("consumer_groups.rs"),
            "pub group_id: String,",
            "pub group_id: String,\n    pub state: String,",
        );
        let d = drift_lines(&run_file_with("consumer_groups.rs", &m, DIVERGENCES)).join("\n");
        assert!(d.contains("we read `state`"), "{d}");
    }

    /// The same class of defect in `manifest.rs`, found before FX-1:
    /// `DryRunTopicReport.partitions` read as a count where upstream writes a
    /// sequence. The name-only gate passed it; this one does not.
    #[test]
    fn the_dry_run_partitions_count_that_once_stood_in_manifest_rs_is_drift() {
        let m = mutate(
            &vendored_src("manifest.rs"),
            "pub partitions: Vec<Value>,",
            "pub partitions: i32,",
        );
        let d = drift_lines(&run_file_with("manifest.rs", &m, DIVERGENCES)).join("\n");
        assert!(
            d.contains("DryRunTopicReport.partitions: we read `i32`"),
            "{d}"
        );
    }

    /// A declared divergence must still describe the code; one that no
    /// comparison uses is DRIFT, and without its declaration the real
    /// difference is DRIFT too.
    #[test]
    fn divergences_are_exact_and_cannot_go_stale() {
        let (up, tag) = upstream_checkout();
        let stale = [Divergence {
            vendored: "manifest.rs",
            item: "SegmentMetadata",
            field: "key",
            ours: "Vec<u8>",
            upstream: "String",
            why: "a test entry that describes nothing",
        }];
        let r = run(&repo_root(), up, tag, CHECKS, &stale).unwrap();
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("SegmentMetadata.key: the declared divergence"),
            "{d}"
        );
        assert!(d.contains("OffsetGap.reason: we read `String`"), "{d}");
        assert!(d.contains("PrunedRange.reason: we read `String`"), "{d}");
    }

    /// An item that is not there on either side is DRIFT, never agreement.
    #[test]
    fn a_missing_item_is_drift_not_agreement() {
        let m = mutate(
            &vendored_src("consumer_groups.rs"),
            "pub struct ConsumerGroupEntry {",
            "pub struct ConsumerGroupEntryRenamed {",
        );
        let d = drift_lines(&run_file_with("consumer_groups.rs", &m, DIVERGENCES)).join("\n");
        assert!(
            d.contains("ConsumerGroupEntry: not found in ours (consumer_groups.rs)"),
            "{d}"
        );
    }

    #[test]
    fn declarations_are_found_at_any_visibility_and_nesting_and_never_in_comments() {
        let src = strip_comments(
            "// struct Snapshot { fake: u8 }\n\
             /* pub struct Snapshot { fake: u8 } */\n\
             fn f() {\n        struct SnapshotRange { a: u8 }\n        \
             #[derive(serde::Serialize)]\n        struct Snapshot {\n            \
             /// topic -> partition\n            snapshot_time: i64,\n            \
             groups: Vec<GroupEntry>,\n        }\n}\n\
             pub(crate) struct Other { pub(crate) x: std::collections::HashMap<String, (i64, i64)> }\n",
        );
        let (kind, body) = find_item(&src, "Snapshot").expect("the fn-local struct");
        assert_eq!(kind, Kind::Struct);
        assert_eq!(
            members(kind, body),
            vec![
                ("snapshot_time".to_string(), Some("i64".to_string())),
                ("groups".to_string(), Some("Vec<GroupEntry>".to_string())),
            ]
        );
        let (kind, body) = find_item(&src, "Other").expect("a pub(crate) struct");
        assert_eq!(
            members(kind, body),
            vec![(
                "x".to_string(),
                Some("std::collections::HashMap<String, (i64, i64)>".to_string())
            )]
        );
        assert!(find_item(&src, "Snap").is_none(), "a prefix is not a name");
    }

    #[test]
    fn type_rules() {
        let id = |n: &str| n.to_string();
        let ok = |a: &str, b: &str| compatible(&parse_ty(a), &parse_ty(b), &id);
        assert!(
            ok("Option<i64>", "i64"),
            "our Option reads a value upstream always writes"
        );
        assert!(
            !ok("i64", "Option<i64>"),
            "upstream may omit what we require"
        );
        assert!(ok(
            "BTreeMap<String, BTreeMap<String, i64>>",
            "std::collections::HashMap<String, std::collections::HashMap<String, i64>>"
        ));
        assert!(!ok("Vec<Value>", "HashMap<String, HashMap<String, i64>>"));
        assert!(!ok("Vec<String>", "Vec<i64>"));
        assert!(
            ok("Option<Value>", "Option<SnapshotCheck>"),
            "opaque by design"
        );
        assert!(ok("Option<(i64, i64)>", "Option<(i64, i64)>"));
        assert!(!ok("Option<(i64, i64)>", "Option<(i64, i64, i64)>"));
        assert!(ok(
            "Option<super::preflight::HeaderPreflightReport>",
            "Option<crate::restore::preflight::HeaderPreflightReport>"
        ));
        let rename = |n: &str| {
            if n == "ConsumerGroupEntry" {
                "GroupEntry".to_string()
            } else {
                n.to_string()
            }
        };
        assert!(compatible(
            &parse_ty("Vec<ConsumerGroupEntry>"),
            &parse_ty("Vec<GroupEntry>"),
            &rename
        ));
        assert!(
            !ok("Vec<ConsumerGroupEntry>", "Vec<GroupEntry>"),
            "only through the pair"
        );
        assert_eq!(render(&parse_ty("Vec<serde_json::Value>")), "Vec<Value>");
    }
}
