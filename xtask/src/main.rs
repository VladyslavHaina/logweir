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
//! - SERDE WIRE ATTRIBUTES (FX-1 fix round, L2), on the item and on every
//!   field or variant both sides declare: the items in `WIRE` -- `rename`,
//!   `rename_all`, `alias`, `flatten`, `tag`/`content`/`untagged`,
//!   `transparent`, `from`/`into`, the `with` and `skip` families. A
//!   `#[serde(rename = "time")]` upstream changes the key while every Rust name
//!   still matches, so unequal sets are DRIFT unless `DIVERGENCES` declares
//!   them.
//! - PRESENCE: a field upstream may leave out (`skip_serializing_if`,
//!   `skip_serializing`, `skip`) must be one we can do without -- `default` on
//!   it or on the item, an `Option`, or never read.
//!
//! An item that cannot be located at all, that resolves to an empty
//! field/variant set, or that is declared MORE THAN ONCE in one file (L3) is
//! ALSO an error -- never agreement. A rename or typo in either the vendored
//! file or upstream must not silently produce "zero fields to disagree about",
//! which a naive checker would report as a clean pass; and with two
//! declarations of one name the gate cannot know which one to compare.
//!
//! What it does not compare: the payloads of enum variants; attributes that
//! do not change which JSON is read (`default`, `deny_unknown_fields`,
//! `bound`, …) outside the presence rule; the semantics behind an attribute's
//! text -- two spellings that happen to name the same keys (`rename_all` on the
//! item against a `rename` on each field) are reported as drift, which errs on
//! the safe side; and anything a macro generates.
//!
//! It also compares vendored LISTS (`LIST_CHECKS`, FX-4): a `&str` array that
//! must name exactly the string literals of one upstream function, for data
//! the engine never serialises -- the topic-configuration allowlist. A key only
//! one side names is DRIFT; the same keys in another order are a note.
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
    // FX-23: the restore's offset-mapping report, two fields of each struct
    // and no catch-all on purpose (`offset_report.rs`'s header): the fields
    // upstream adds or keeps beyond them are notes, a field we read that
    // upstream drops or retypes is DRIFT.
    Check {
        vendored: "offset_report.rs",
        upstream: "crates/kafka-backup-core/src/manifest.rs",
        items: &[
            ("OffsetMappingReport", "OffsetMapping"),
            ("OffsetMappingEntry", "OffsetMappingEntry"),
        ],
    },
];

/// A vendored LIST, not a serde shape (FX-4): a `&str` array constant in a
/// vendored file that must name exactly the string literals of one upstream
/// function's body. The engine's topic-configuration allowlist is data the
/// engine never serialises, so the shape comparison above cannot see it; a key
/// upstream adds or drops changes what the engine captures, and so what FX-4's
/// `captured` claims.
struct ListCheck {
    /// File name under `VENDORED_DIR`.
    vendored: &'static str,
    /// The array constant in it.
    constant: &'static str,
    /// Path relative to the root of an upstream checkout.
    upstream: &'static str,
    /// The upstream function whose body lists the same strings.
    function: &'static str,
}

const LIST_CHECKS: &[ListCheck] = &[ListCheck {
    vendored: "topic_config.rs",
    constant: "RECOVERY_TOPIC_CONFIG_KEYS",
    upstream: "crates/kafka-backup-core/src/backup/engine.rs",
    function: "is_recovery_topic_config",
}];

/// The contents of every string literal in `src`, in order (escapes kept as
/// written: the lists compared hold plain configuration keys).
fn string_literals(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < src.len() {
        if src.as_bytes()[i] == b'"' {
            if let Some(end) = literal_end(src, i) {
                out.push(src[i + 1..end.saturating_sub(1)].to_string());
                i = end;
                continue;
            }
        }
        i += src[i..].chars().next().map_or(1, char::len_utf8);
    }
    out
}

/// The text between the brackets of `const <name>: … = [ … ];` in
/// comment-stripped `src`, or `None`.
fn const_array_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let at = src.find(&format!("const {name}:"))?;
    let open = at + src[at..].find("= [")? + 3;
    let close = open + src[open..].find("];")?;
    Some(&src[open..close])
}

/// The body of `fn <name>(` in comment-stripped `src`, braces balanced with
/// string literals skipped, or `None`.
fn fn_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let at = src.find(&format!("fn {name}("))?;
    let open = at + src[at..].find('{')?;
    let (b, mut depth, mut i) = (src.as_bytes(), 0usize, open);
    while i < b.len() {
        if let Some(end) = literal_end(src, i) {
            i = end;
            continue;
        }
        match b[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[open + 1..i]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// One [`ListCheck`]: the SET of strings must be equal (a key only one side
/// names is DRIFT, either way); the same set in another order is a note. A
/// side that resolves to no list at all is DRIFT, never agreement.
fn compare_list(check: &ListCheck, ours: &str, theirs: &str, tag: &str, report: &mut Report) {
    let (ours, theirs) = (strip_comments(ours), strip_comments(theirs));
    let label = format!("{}::{}", check.vendored, check.constant);
    let Some(mine) = const_array_body(&ours, check.constant).map(string_literals) else {
        report.drift(format!(
            "DRIFT  {tag} {label}: no `const {}: … = [ … ];` in {}",
            check.constant, check.vendored
        ));
        return;
    };
    let Some(upstream) = fn_body(&theirs, check.function).map(string_literals) else {
        report.drift(format!(
            "DRIFT  {tag} {label}: upstream {} has no `fn {}` -- renamed or moved, and \
             nothing was compared",
            check.upstream, check.function
        ));
        return;
    };
    if mine.is_empty() || upstream.is_empty() {
        report.drift(format!(
            "DRIFT  {tag} {label}: resolved to an EMPTY list (ours {}, upstream {}), which is \
             not agreement",
            mine.len(),
            upstream.len()
        ));
        return;
    }
    let (a, b): (BTreeSet<&String>, BTreeSet<&String>) =
        (mine.iter().collect(), upstream.iter().collect());
    for gone in b.difference(&a) {
        report.drift(format!(
            "DRIFT  {tag} {label}: upstream `{}` keeps `{gone}`, we do not -- the engine \
             captures a key our coverage filter would call a difference",
            check.function
        ));
    }
    for extra in a.difference(&b) {
        report.drift(format!(
            "DRIFT  {tag} {label}: we keep `{extra}`, upstream `{}` does not -- `captured` \
             would claim a key the engine never writes",
            check.function
        ));
    }
    if a == b && mine != upstream {
        report.lines.push(format!(
            "note   {tag} {label}: the same {} keys as upstream `{}`, in another order",
            mine.len(),
            check.function
        ));
    }
    report.compared.push((
        check.vendored.to_string(),
        check.constant.to_string(),
        check.upstream.to_string(),
        check.function.to_string(),
    ));
}

/// What a declared divergence is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum On {
    /// A field's type, as `render` prints it.
    Type,
    /// The WIRE serde items of a field, or of the item itself when `field` is
    /// empty, as `render_wire` prints them.
    Serde,
}

/// A vendored item or field that deliberately differs from upstream in a way
/// the rules do not accept, and why. A declaration no comparison uses is
/// itself DRIFT, so the list cannot go stale.
struct Divergence {
    vendored: &'static str,
    item: &'static str,
    /// Empty for the item itself (only with `On::Serde`).
    field: &'static str,
    on: On,
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
        on: On::Type,
        ours: "String",
        upstream: "OffsetGapReason",
        why: "a string-serialising enum (rename_all = snake_case) read as its string, so a \
              reason this build does not know never fails the parse",
    },
    Divergence {
        vendored: "manifest.rs",
        item: "PrunedRange",
        field: "reason",
        on: On::Type,
        ours: "String",
        upstream: "PruneReason",
        why: "a string-serialising enum (rename_all = snake_case) read as its string, so a \
              reason this build does not know never fails the parse",
    },
    Divergence {
        vendored: "preflight.rs",
        item: "PartitionCoverageState",
        field: "",
        on: On::Serde,
        ours: "from=\"String\", into=\"String\"",
        upstream: "rename_all=\"snake_case\"",
        why: "ours reads and writes the state through its string and spells every snake_case \
              name by hand (`PartitionCoverageState::as_str`), so a state this build does not \
              know degrades to `Unknown` instead of failing the parse (spec §11); the variant \
              names are still compared",
    },
    Divergence {
        vendored: "consumer_groups.rs",
        item: "ConsumerGroupEntry",
        field: "offsets",
        on: On::Serde,
        ours: "deserialize_with=\"offsets_without_repeated_keys\"",
        upstream: "none",
        why: "reads the same JSON object, refusing a repeated topic or partition key instead \
              of keeping the last value (FX-1 fix round, L1)",
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

/// One declaration of an item: its kind, the attributes written above it
/// (`#[derive(…)]`, `#[serde(…)]`, …, as source text) and its brace-delimited
/// body.
struct Found<'a> {
    kind: Kind,
    attrs: Vec<String>,
    body: &'a str,
}

/// The index just past the `#[…]` (or `#![…]`) attribute starting at `at`,
/// skipping literals.
fn attribute_end(src: &str, at: usize) -> Option<usize> {
    let b = src.as_bytes();
    let open = at + src[at..].find('[')?;
    let (mut depth, mut i) = (0usize, open);
    while i < b.len() {
        if let Some(end) = literal_end(src, i) {
            i = end;
            continue;
        }
        match b[i] {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// EVERY declaration of `struct <item>` or `enum <item>` in `src` (comments
/// already stripped) that begins a line, at any visibility and at any nesting
/// -- the snapshot writer's structs are declared inside a function -- with the
/// attributes written above it. All of them, not the first: a second
/// declaration of the same name makes the pairing ambiguous, and `resolve`
/// refuses that (L3) rather than silently comparing whichever came first.
fn find_items<'a>(src: &'a str, item: &str) -> Vec<Found<'a>> {
    let mut found = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut i = 0;
    while i < src.len() {
        let rest = &src[i..];
        let at = i + (rest.len() - rest.trim_start_matches([' ', '\t']).len());
        let from_at = &src[at..];
        if from_at.starts_with("#[") || from_at.starts_with("#![") {
            if let Some(end) = attribute_end(src, at) {
                pending.push(src[at..end].to_string());
                i = end;
                continue;
            }
        }
        let line_end = from_at.find('\n').map(|n| at + n + 1).unwrap_or(src.len());
        let line = &src[at..line_end];
        if line.trim().is_empty() {
            // A blank line between an item's attributes and the item is legal.
            i = line_end;
            continue;
        }
        let decl = strip_visibility(line);
        for (keyword, kind) in [("struct ", Kind::Struct), ("enum ", Kind::Enum)] {
            let Some(r) = decl.strip_prefix(keyword) else {
                continue;
            };
            let Some(after) = r.trim_start().strip_prefix(item) else {
                continue;
            };
            if after.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                continue; // `struct SnapshotRange` is not `struct Snapshot`
            }
            let start = at + (line.len() - decl.len());
            if let Some(body) = brace_body(&src[start..]) {
                found.push(Found {
                    kind,
                    attrs: pending.clone(),
                    body,
                });
            }
        }
        // Attributes belong to the next item only.
        pending.clear();
        i = line_end;
    }
    found
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

/// `s` split on `sep` where no `<>`, `()`, `[]` or `{}` is open and outside
/// every literal: `rename = "a,b"` is one serde item, not two.
fn split_top_level(s: &str, sep: char) -> Vec<&str> {
    let (mut parts, mut depth, mut start, mut i) = (Vec::new(), 0i32, 0, 0);
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        let c = s[i..].chars().next().expect("a char at a char boundary");
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth -= 1,
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
        i += c.len_utf8();
    }
    parts.push(&s[start..]);
    parts
}

/// One member of an item's body: a struct field (`ty` is its type) or an enum
/// variant (`ty` is `None`), with the attributes written on it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    name: String,
    ty: Option<String>,
    attrs: Vec<String>,
}

fn members(kind: Kind, body: &str) -> Vec<Member> {
    split_top_level(body, ',')
        .into_iter()
        .filter_map(|piece| {
            let mut rest = piece.trim_start();
            let mut attrs = Vec::new();
            while rest.starts_with("#[") {
                let end = attribute_end(rest, 0)?;
                attrs.push(rest[..end].to_string());
                rest = rest[end..].trim_start();
            }
            let m = strip_visibility(rest);
            let name: String = m
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                return None;
            }
            let ty = match kind {
                Kind::Enum => None,
                Kind::Struct => Some(
                    m[name.len()..]
                        .trim_start()
                        .strip_prefix(':')?
                        .trim()
                        .to_string(),
                ),
            };
            Some(Member { name, ty, attrs })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// serde attributes.
// ---------------------------------------------------------------------------

/// The serde attribute items that decide WHICH JSON a field or an item reads
/// or writes: its names (`rename`, `rename_all`, `alias`), its nesting and
/// representation (`flatten`, `tag`, `content`, `untagged`, `transparent`,
/// `from`/`into`, the `with` family) and whether it is on the wire at all (the
/// `skip` family). `default` and `skip_serializing_if` are not here: they
/// change whether a key may be ABSENT, which `compare`'s presence rule checks.
const WIRE: &[&str] = &[
    "rename",
    "rename_all",
    "rename_all_fields",
    "alias",
    "flatten",
    "tag",
    "content",
    "untagged",
    "transparent",
    "remote",
    "from",
    "try_from",
    "into",
    "with",
    "serialize_with",
    "deserialize_with",
    "skip",
    "skip_serializing",
    "skip_deserializing",
];

/// Every item of every `#[serde(…)]` attribute in `attrs`, whitespace removed
/// outside string literals: `rename = "a"` becomes `rename="a"`.
fn serde_items(attrs: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for a in attrs {
        let inner = a
            .trim()
            .trim_start_matches("#[")
            .trim_start_matches("#![")
            .trim_end_matches(']')
            .trim();
        let Some(args) = inner
            .strip_prefix("serde")
            .map(str::trim_start)
            .and_then(|r| r.strip_prefix('('))
            .and_then(|r| r.strip_suffix(')'))
        else {
            continue;
        };
        for item in split_top_level(args, ',') {
            let mut norm = String::new();
            let mut i = 0;
            while i < item.len() {
                if let Some(end) = literal_end(item, i) {
                    norm.push_str(&item[i..end]);
                    i = end;
                    continue;
                }
                let c = item[i..].chars().next().expect("a char at a char boundary");
                if !c.is_whitespace() {
                    norm.push(c);
                }
                i += c.len_utf8();
            }
            if !norm.is_empty() {
                out.push(norm);
            }
        }
    }
    out
}

/// The leading identifier of a serde item: `rename` of `rename="a"`.
fn item_key(item: &str) -> &str {
    let end = item
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(item.len());
    &item[..end]
}

/// The WIRE items of `attrs`, sorted.
fn wire(attrs: &[String]) -> BTreeSet<String> {
    serde_items(attrs)
        .into_iter()
        .filter(|i| WIRE.contains(&item_key(i)))
        .collect()
}

fn has_serde(attrs: &[String], keys: &[&str]) -> bool {
    serde_items(attrs)
        .iter()
        .any(|i| keys.contains(&item_key(i)))
}

/// How a set of wire items prints in a diagnostic and in `DIVERGENCES`.
fn render_wire(set: &BTreeSet<String>) -> String {
    if set.is_empty() {
        "none".to_string()
    } else {
        set.iter().cloned().collect::<Vec<_>>().join(", ")
    }
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

/// Resolves `item` on one side (`side` is "ours" or "upstream", `path` is the
/// file it lives in): its kind, the attributes above it and its members. A
/// missing item, an empty set, and an item declared MORE THAN ONCE are hard
/// DRIFT, never agreement: a rename or typo in either file must never be read
/// as "nothing to disagree about", and with two declarations of one name the
/// gate cannot know which one the vendored shape mirrors (L3).
fn resolve(
    src: &str,
    item: &str,
    side: &str,
    path: &str,
    tag: &str,
    report: &mut Report,
) -> Option<(Kind, Vec<String>, Vec<Member>)> {
    let found = find_items(src, item);
    match found.as_slice() {
        [] => {
            report.drift(format!(
                "DRIFT  {tag} {item}: not found in {side} ({path}) -- a rename or typo must never read as agreement"
            ));
            None
        }
        [one] => {
            let m = members(one.kind, one.body);
            if m.is_empty() {
                report.drift(format!(
                    "DRIFT  {tag} {item}: found in {side} ({path}) but declares zero fields/variants -- treating as drift, not agreement"
                ));
                return None;
            }
            Some((one.kind, one.attrs.clone(), m))
        }
        many => {
            report.drift(format!(
                "DRIFT  {tag} {item}: declared {} times in {side} ({path}) -- the gate cannot tell \
                 which one the vendored shape mirrors; pair it by an unambiguous name",
                many.len()
            ));
            None
        }
    }
}

/// Looks up a declared divergence, marking it used. `None` when there is none.
fn declared(
    divergences: &[Divergence],
    used: &mut BTreeSet<usize>,
    want: (&str, &str, &str, On, &str, &str),
) -> Option<usize> {
    let (vendored, item, field, on, ours, upstream) = want;
    let i = divergences.iter().position(|d| {
        d.vendored == vendored
            && d.item == item
            && d.field == field
            && d.on == on
            && d.ours == ours
            && d.upstream == upstream
    })?;
    used.insert(i);
    Some(i)
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
        let (Some((ka, attrs_a, a)), Some((kb, attrs_b, b))) = (a, b) else {
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

        // The item's own wire attributes: `rename_all`, `tag`, `untagged`, …
        let (wa, wb) = (wire(&attrs_a), wire(&attrs_b));
        if wa != wb {
            let (ra, rb) = (render_wire(&wa), render_wire(&wb));
            let want = (
                check.vendored,
                our_item,
                "",
                On::Serde,
                ra.as_str(),
                rb.as_str(),
            );
            if declared(divergences, used, want).is_none() {
                report.drift(format!(
                    "DRIFT  {tag} {label}: our serde attributes are `{ra}`, upstream ({}) has \
                     `{rb}` -- a renamed or re-shaped wire reads no real bytes however well \
                     the Rust names match",
                    check.upstream
                ));
            }
        }

        let names =
            |m: &[Member]| -> BTreeSet<String> { m.iter().map(|x| x.name.clone()).collect() };
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
        let ours_defaults_all = has_serde(&attrs_a, &["default"]);
        for ma in &a {
            let Some(mb) = b.iter().find(|x| x.name == ma.name) else {
                continue;
            };
            let field = ma.name.as_str();

            // TYPES.
            if let (Some(ty_a), Some(ty_b)) = (&ma.ty, &mb.ty) {
                let (pa, pb) = (parse_ty(ty_a), parse_ty(ty_b));
                if !compatible(&pa, &pb, &rename) {
                    let (ra, rb) = (render(&pa), render(&pb));
                    let want = (
                        check.vendored,
                        our_item,
                        field,
                        On::Type,
                        ra.as_str(),
                        rb.as_str(),
                    );
                    if declared(divergences, used, want).is_none() {
                        report.drift(format!(
                            "DRIFT  {tag} {label}.{field}: we read `{ra}`, upstream ({}) declares \
                             `{rb}` -- a retyped field reads no real bytes however well its name \
                             matches",
                            check.upstream
                        ));
                    }
                }
            }

            // WIRE ATTRIBUTES of the field or variant (L2): a `rename` or an
            // `alias` upstream changes the key while every Rust name matches.
            let (wa, wb) = (wire(&ma.attrs), wire(&mb.attrs));
            if wa != wb {
                let (ra, rb) = (render_wire(&wa), render_wire(&wb));
                let want = (
                    check.vendored,
                    our_item,
                    field,
                    On::Serde,
                    ra.as_str(),
                    rb.as_str(),
                );
                if declared(divergences, used, want).is_none() {
                    report.drift(format!(
                        "DRIFT  {tag} {label}.{field}: our serde attributes are `{ra}`, upstream \
                         ({}) has `{rb}` -- a renamed key reads no real bytes however well the \
                         Rust name matches",
                        check.upstream
                    ));
                }
            }

            // PRESENCE: a field upstream may leave out must be one we can do
            // without -- `default` on it or on the item, an `Option`, or never
            // read at all.
            let upstream_may_omit = has_serde(
                &mb.attrs,
                &["skip_serializing_if", "skip_serializing", "skip"],
            );
            let ours_tolerates_absence = ours_defaults_all
                || has_serde(&ma.attrs, &["default", "skip", "skip_deserializing"])
                || matches!(ma.ty.as_deref().map(parse_ty), Some(Ty::Named(n, _)) if n == "Option");
            if upstream_may_omit && !ours_tolerates_absence {
                report.drift(format!(
                    "DRIFT  {tag} {label}.{field}: upstream ({}) may leave it out (`{}`), and we \
                     require it -- give it `#[serde(default)]` or make it an `Option`",
                    check.upstream,
                    render_wire(&serde_items(&mb.attrs).into_iter().collect())
                ));
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
    for check in LIST_CHECKS {
        let ours = read(
            &root.join(VENDORED_DIR).join(check.vendored),
            "the vendored file should exist in-tree; run xtask from the repository root",
        )?;
        let theirs = read(
            &upstream.join(check.upstream),
            "extract kafka-backup at the pinned tag and pass its path via --upstream",
        )?;
        compare_list(check, &ours, &theirs, tag, &mut report);
    }
    for (i, d) in divergences.iter().enumerate() {
        if !used.contains(&i) && checks.iter().any(|c| c.vendored == d.vendored) {
            let what = if d.field.is_empty() {
                d.item.to_string()
            } else {
                format!("{}.{}", d.item, d.field)
            };
            report.drift(format!(
                "DRIFT  {tag} {what}: the declared {:?} divergence `{}` vs upstream `{}` no \
                 longer describes the code -- remove or correct it",
                d.on, d.ours, d.upstream
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
        "vendored structs and lists agree with {tag} ({} item pairs: names, types and serde \
         wire attributes; {} list(s): their keys)",
        report.compared.len() - LIST_CHECKS.len(),
        LIST_CHECKS.len()
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

    /// The upstream files `CHECKS` names, extracted from the pinned tarball
    /// with `tar` (bounded: killed after 60 s) into a directory of this
    /// checkout's own, which is removed when the checkout is dropped -- a
    /// test run leaves nothing behind in the temp directory.
    struct Checkout {
        dir: PathBuf,
        /// `kafka-backup-<version>` inside `dir`: the upstream checkout root.
        root: PathBuf,
        tag: String,
    }

    impl Checkout {
        fn extract() -> Checkout {
            static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let (tarball, tag) = pinned_tarball();
            let top = format!("kafka-backup-{}", tag.trim_start_matches('v'));
            let dir = std::env::temp_dir().join(format!(
                "logweir-xtask-upstream-{}-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            // Constructed before `tar` runs, so a failed extraction is cleaned up too.
            let checkout = Checkout {
                root: dir.join(&top),
                dir,
                tag,
            };
            let members: BTreeSet<String> = CHECKS
                .iter()
                .map(|c| c.upstream)
                .chain(LIST_CHECKS.iter().map(|c| c.upstream))
                .map(|u| format!("{top}/{u}"))
                .collect();
            let mut child = Command::new("tar")
                .arg("-xzf")
                .arg(&tarball)
                .arg("-C")
                .arg(&checkout.dir)
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
            checkout
        }

        fn src(&self, rel: &str) -> String {
            std::fs::read_to_string(self.root.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
        }
    }

    impl Drop for Checkout {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn vendored_src(file: &str) -> String {
        std::fs::read_to_string(repo_root().join(VENDORED_DIR).join(file))
            .unwrap_or_else(|e| panic!("{file}: {e}"))
    }

    /// The checks of one vendored file, run against the real upstream sources
    /// with `ours` standing in for the file's text.
    fn run_file_with(file: &str, ours: &str, divergences: &[Divergence]) -> Report {
        run_file_with_upstream(file, ours, divergences, None)
    }

    /// `run_file_with`, with ONE upstream file's text replaced by
    /// `mutate(<its text>, from, to)`: `(upstream path, from, to)`.
    fn run_file_with_upstream(
        file: &str,
        ours: &str,
        divergences: &[Divergence],
        upstream_mutation: Option<(&str, &str, &str)>,
    ) -> Report {
        let up = Checkout::extract();
        let mut report = Report::default();
        let mut used = BTreeSet::new();
        for check in CHECKS.iter().filter(|c| c.vendored == file) {
            let mut theirs = up.src(check.upstream);
            if let Some((path, from, to)) = upstream_mutation {
                if path == check.upstream {
                    theirs = mutate(&theirs, from, to);
                }
            }
            compare(
                check,
                ours,
                &theirs,
                &up.tag,
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
        let up = Checkout::extract();
        let r =
            run(&repo_root(), &up.root, &up.tag, CHECKS, DIVERGENCES).expect("every file reads");
        assert!(!r.drift, "drift at the pin:\n{}", r.lines.join("\n"));
        let pairs: usize = CHECKS.iter().map(|c| c.items.len()).sum();
        assert_eq!(
            r.compared.len(),
            pairs + LIST_CHECKS.len(),
            "{:?}",
            r.compared
        );
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

    /// **FX-4's allowlist, at the pin, and its three drift shapes.** The
    /// vendored `RECOVERY_TOPIC_CONFIG_KEYS` names exactly the keys of the
    /// pinned engine's `is_recovery_topic_config`. A key the engine adds, a key
    /// we keep that it dropped, and a function that moved are each DRIFT --
    /// never a silent agreement; the same keys in another order are a note.
    #[test]
    fn the_topic_config_allowlist_agrees_with_the_pinned_engine_and_drift_is_caught() {
        let up = Checkout::extract();
        let check = &LIST_CHECKS[0];
        let theirs = up.src(check.upstream);
        let ours = vendored_src(check.vendored);
        let at_pin = |ours: &str, theirs: &str| {
            let mut r = Report::default();
            compare_list(check, ours, theirs, &up.tag, &mut r);
            r
        };
        let r = at_pin(&ours, &theirs);
        assert!(!r.drift, "drift at the pin:\n{}", r.lines.join("\n"));
        assert_eq!(r.compared.len(), 1, "{:?}", r.compared);
        assert!(r.lines.is_empty(), "{:?}", r.lines);

        let added = mutate(
            &theirs,
            "| \"unclean.leader.election.enable\"",
            "| \"unclean.leader.election.enable\"\n            | \"local.retention.ms\"",
        );
        let d = drift_lines(&at_pin(&ours, &added)).join("\n");
        assert!(d.contains("keeps `local.retention.ms`, we do not"), "{d}");

        let dropped = mutate(&ours, "    \"segment.ms\",\n", "");
        let d = drift_lines(&at_pin(&dropped, &theirs)).join("\n");
        assert!(d.contains("keeps `segment.ms`, we do not"), "{d}");

        let extra = mutate(
            &ours,
            "    \"segment.ms\",\n",
            "    \"segment.ms\",\n    \"x.y\",\n",
        );
        let d = drift_lines(&at_pin(&extra, &theirs)).join("\n");
        assert!(d.contains("we keep `x.y`"), "{d}");

        let moved = mutate(
            &theirs,
            "fn is_recovery_topic_config(",
            "fn is_restorable_config(",
        );
        let d = drift_lines(&at_pin(&ours, &moved)).join("\n");
        assert!(d.contains("has no `fn is_recovery_topic_config`"), "{d}");

        let reordered = mutate(
            &ours,
            "    \"cleanup.policy\",\n    \"compression.type\",\n",
            "    \"compression.type\",\n    \"cleanup.policy\",\n",
        );
        let r = at_pin(&reordered, &theirs);
        assert!(!r.drift, "{:?}", r.lines);
        assert!(
            r.lines.iter().any(|l| l.contains("in another order")),
            "{:?}",
            r.lines
        );
    }

    /// **The gate cannot skip a file.** Every `.rs` under `vendored/` but
    /// `mod.rs` is covered by `CHECKS` or `LIST_CHECKS`; FX-1's
    /// `consumer_groups.rs` was not, and FX-4's `topic_config.rs` was not
    /// until its allowlist got a gate of its own.
    #[test]
    fn every_vendored_file_is_gated() {
        let dir = repo_root().join(VENDORED_DIR);
        let files: BTreeSet<String> = std::fs::read_dir(&dir)
            .expect("vendored/ lists")
            .filter_map(|e| e.ok()?.file_name().to_str().map(str::to_string))
            .filter(|n| n.ends_with(".rs") && n != "mod.rs")
            .collect();
        assert!(files.contains("consumer_groups.rs"), "{files:?}");
        let gated: BTreeSet<String> = CHECKS
            .iter()
            .map(|c| c.vendored.to_string())
            .chain(LIST_CHECKS.iter().map(|c| c.vendored.to_string()))
            .collect();
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
        let up = Checkout::extract();
        let stale = [Divergence {
            vendored: "manifest.rs",
            item: "SegmentMetadata",
            field: "key",
            on: On::Type,
            ours: "Vec<u8>",
            upstream: "String",
            why: "a test entry that describes nothing",
        }];
        let r = run(&repo_root(), &up.root, &up.tag, CHECKS, &stale).unwrap();
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("SegmentMetadata.key: the declared Type divergence"),
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
        let named = |m: Vec<Member>| -> Vec<(String, Option<String>)> {
            m.into_iter().map(|x| (x.name, x.ty)).collect()
        };
        let found = find_items(&src, "Snapshot");
        assert_eq!(found.len(), 1, "the fn-local struct, once");
        assert_eq!(found[0].kind, Kind::Struct);
        assert_eq!(
            found[0].attrs,
            vec!["#[derive(serde::Serialize)]".to_string()]
        );
        assert_eq!(
            named(members(found[0].kind, found[0].body)),
            vec![
                ("snapshot_time".to_string(), Some("i64".to_string())),
                ("groups".to_string(), Some("Vec<GroupEntry>".to_string())),
            ]
        );
        let found = find_items(&src, "Other");
        assert_eq!(found.len(), 1, "a pub(crate) struct");
        assert!(found[0].attrs.is_empty(), "{:?}", found[0].attrs);
        assert_eq!(
            named(members(found[0].kind, found[0].body)),
            vec![(
                "x".to_string(),
                Some("std::collections::HashMap<String, (i64, i64)>".to_string())
            )]
        );
        assert!(
            find_items(&src, "Snap").is_empty(),
            "a prefix is not a name"
        );
    }

    /// Attributes are read where serde reads them: on the item (across lines,
    /// through a blank line) and on each field, with literals intact; and they
    /// attach to the NEXT item only.
    #[test]
    fn serde_attributes_are_read_on_items_and_fields() {
        let src = strip_comments(
            "#[derive(Debug)]\n\
             #[serde(\n    rename_all = \"camelCase\",\n    tag = \"t\"\n)]\n\n\
             pub struct A {\n\
                 #[serde(default, rename = \"x,y\")]\n    pub a: i64,\n\
                 #[serde(alias = \"old\")] #[serde(skip_serializing_if = \"Option::is_none\")]\n    b: Option<i64>,\n\
             }\n\
             struct B { c: u8 }\n",
        );
        let a = &find_items(&src, "A")[0];
        assert_eq!(
            render_wire(&wire(&a.attrs)),
            "rename_all=\"camelCase\", tag=\"t\""
        );
        let m = members(a.kind, a.body);
        assert_eq!(render_wire(&wire(&m[0].attrs)), "rename=\"x,y\"");
        assert!(has_serde(&m[0].attrs, &["default"]));
        assert_eq!(render_wire(&wire(&m[1].attrs)), "alias=\"old\"");
        assert!(has_serde(&m[1].attrs, &["skip_serializing_if"]));
        let b = &find_items(&src, "B")[0];
        assert!(
            b.attrs.is_empty(),
            "A's attributes leaked onto B: {:?}",
            b.attrs
        );
    }

    /// L3: a name declared twice in one file is DRIFT, never "the first one".
    #[test]
    fn an_item_declared_twice_is_drift_not_the_first_match() {
        let r = run_file_with_upstream(
            "consumer_groups.rs",
            &vendored_src("consumer_groups.rs"),
            DIVERGENCES,
            Some((
                "crates/kafka-backup-core/src/backup/engine.rs",
                "async fn snapshot_consumer_groups(&self) -> Result<()> {",
                "async fn snapshot_consumer_groups(&self) -> Result<()> {\n        \
                 struct GroupEntry {\n            group_id: String,\n        }",
            )),
        );
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("GroupEntry: declared 2 times in upstream (crates/kafka-backup-core/src/backup/engine.rs)"),
            "{d}"
        );
    }

    /// L2: a wire rename upstream, on a field and on the item, is DRIFT --
    /// the two plants the review found passing (D10, D11).
    #[test]
    fn an_upstream_serde_rename_is_drift() {
        let engine_rs = "crates/kafka-backup-core/src/backup/engine.rs";
        let ours = vendored_src("consumer_groups.rs");
        let r = run_file_with_upstream(
            "consumer_groups.rs",
            &ours,
            DIVERGENCES,
            Some((
                engine_rs,
                "            snapshot_time: i64,\n",
                "            #[serde(rename = \"time\")]\n            snapshot_time: i64,\n",
            )),
        );
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("ConsumerGroupsSnapshot (upstream Snapshot).snapshot_time: our serde attributes are `none`, upstream (crates/kafka-backup-core/src/backup/engine.rs) has `rename=\"time\"`"),
            "{d}"
        );
        let r = run_file_with_upstream(
            "consumer_groups.rs",
            &ours,
            DIVERGENCES,
            Some((
                engine_rs,
                "        #[derive(serde::Serialize)]\n        struct GroupEntry {",
                "        #[derive(serde::Serialize)]\n        #[serde(rename_all = \"camelCase\")]\n        struct GroupEntry {",
            )),
        );
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("ConsumerGroupEntry (upstream GroupEntry): our serde attributes are `none`, upstream (crates/kafka-backup-core/src/backup/engine.rs) has `rename_all=\"camelCase\"`"),
            "{d}"
        );
    }

    /// L2, our side: an `alias` we add changes which key we read, so it is
    /// DRIFT unless declared.
    #[test]
    fn our_serde_alias_is_drift_unless_declared() {
        let m = mutate(
            &vendored_src("consumer_groups.rs"),
            "    #[serde(default)]\n    pub snapshot_time: Option<i64>,",
            "    #[serde(default, alias = \"captured_at\")]\n    pub snapshot_time: Option<i64>,",
        );
        let d = drift_lines(&run_file_with("consumer_groups.rs", &m, DIVERGENCES)).join("\n");
        assert!(
            d.contains(".snapshot_time: our serde attributes are `alias=\"captured_at\"`"),
            "{d}"
        );
    }

    /// Presence: a field upstream may leave out must be one we can do without.
    #[test]
    fn a_field_upstream_may_omit_and_we_require_is_drift() {
        let r = run_file_with_upstream(
            "consumer_groups.rs",
            &vendored_src("consumer_groups.rs"),
            DIVERGENCES,
            Some((
                "crates/kafka-backup-cli/src/commands/snapshot_groups.rs",
                "struct GroupEntry {\n    group_id: String,",
                "struct GroupEntry {\n    #[serde(skip_serializing_if = \"String::is_empty\")]\n    group_id: String,",
            )),
        );
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("ConsumerGroupEntry (upstream GroupEntry).group_id: upstream (crates/kafka-backup-cli/src/commands/snapshot_groups.rs) may leave it out"),
            "{d}"
        );
    }

    /// A serde divergence that no longer describes the code is DRIFT, and the
    /// real difference it covered is DRIFT without it.
    #[test]
    fn serde_divergences_are_exact_and_cannot_go_stale() {
        let up = Checkout::extract();
        let without: Vec<Divergence> = DIVERGENCES
            .iter()
            .filter(|d| d.on == On::Type)
            .map(|d| Divergence { ..*d })
            .collect();
        let r = run(&repo_root(), &up.root, &up.tag, CHECKS, &without).unwrap();
        let d = drift_lines(&r).join("\n");
        assert!(d.contains("PartitionCoverageState: our serde attributes are `from=\"String\", into=\"String\"`"), "{d}");
        assert!(d.contains("ConsumerGroupEntry (upstream GroupEntry).offsets: our serde attributes are `deserialize_with="), "{d}");
        let stale = [Divergence {
            vendored: "consumer_groups.rs",
            item: "ConsumerGroupsSnapshot",
            field: "groups",
            on: On::Serde,
            ours: "flatten",
            upstream: "none",
            why: "a test entry that describes nothing",
        }];
        let r = run(&repo_root(), &up.root, &up.tag, CHECKS, &stale).unwrap();
        let d = drift_lines(&r).join("\n");
        assert!(
            d.contains("ConsumerGroupsSnapshot.groups: the declared Serde divergence"),
            "{d}"
        );
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
