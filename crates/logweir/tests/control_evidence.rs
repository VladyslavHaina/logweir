//! THE CONTROL-EVIDENCE MAPPING — PROD-08.4.
//!
//! `docs/control-evidence.md` tells auditors which fields of Logweir's evidence
//! support which backup and restore-testing clause (DORA, its RTS, ISO/IEC
//! 27001, SOC 2, NIS2's implementing regulation, HIPAA), and which gaps the
//! evidence leaves. Such a page rots in several directions, and the guards here
//! close one each:
//!
//! 1. **FIELD EXISTENCE.** Every field the page cites must exist in the document
//!    the citation names. The field sets are DERIVED, never typed here: the
//!    newest version of each of the three JSON schemas under `schemas/`
//!    (chosen by numeric semver), the generated `RehearsalSchedule` and
//!    `Restore` CRD schemas, and — for the two documents with no checked-in
//!    schema — the Rust type that reads them, by round-tripping their
//!    documented example through it (the put receipt's example in
//!    `docs/verify-a-scorecard.md`, and the standing-authorization fixture
//!    every standing test reads). An object keyed by topic name is entered
//!    through the `<topic>` segment (`config_coverage.<topic>.coverage`), and
//!    only through it. A field that is renamed or removed fails here until the
//!    page is updated, which turns the row's handoff, "refreshed as fields
//!    change", into a check. Every field-shaped token in the prose must
//!    resolve too, except the Kafka configuration keys the page names.
//! 2. **THE DEFINITION NAMES THE FIELD.** Each citation links the page that
//!    defines its document's fields, and that page (or the linked section of
//!    it) must name every field cited under it.
//! 3. **NO COMPLIANCE CLAIM.** The page never calls Logweir, an installation or
//!    an organisation compliant, certified or audit-ready, and never says a field
//!    satisfies, meets or fulfils a requirement — in any inflection: claim
//!    stems, claim phrases and claim verbs next to a regulatory object are all
//!    refused, after removing the few official quotations and the page's own
//!    "no compliance claim" sentence. The scan is wrap-insensitive, because a
//!    phrase broken across two source lines renders as one.
//! 4. **NO SUPPORTED ROW SAYS MORE THAN ITS CONDITION.** A row claiming a
//!    byte-level comparison names `byte-fingerprint` and cites
//!    `integrity.level`; a row saying every test leaves a signed record says
//!    "that reaches a result"; a row saying configuration was compared cites
//!    the `not_assessed` list that bounds it; and no row calls a restore an
//!    exact or complete copy, or the approver a human.
//!
//! Around them, the structure the row's acceptance names: every supported
//! statement cites a field, and links the page that defines that document's
//! fields; every "does not show" item links to a section under
//! the page's gap heading; every anchor the page links resolves (`just links`
//! checks files, not fragments); each of the six texts keeps its section; and
//! the verification guide and the README link the page. A docs-wide sweep keeps
//! the unambiguous claim words off every shipped operator page.
//!
//! NEGATIVE CONTROLS. Every check is a pure function over text, run against the
//! real page (green) and against mutated copies (red): a made-up field, a real
//! field cited under the wrong document, a map entered without `<topic>`, a
//! supported row that cites nothing, a gap item with no link, a dangling
//! anchor, a definition that does not name its field, the review's planted
//! claim sentences, and the review's overreaching rows. A check that cannot
//! fail is not a check. POSITIVE CONTROLS: the page's own permitted phrasings
//! pass the claim scan, so its stems cannot over-ban silently.
//!
//! COSTS NOTHING AND REACHES NOTHING: it reads checked-in files only.

use logweir::drill::phase8_score::PutReceipt;
use logweir_core::execution_contract::StandingAuthorization;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const PAGE: &str = "docs/control-evidence.md";

/// The header of a table of supported statements, exactly.
const SUPPORTS_HEADER: [&str; 2] = ["What the evidence supports", "Fields"];

/// The label above each list of what a clause's evidence does not show.
const DOES_NOT_SHOW: &str = "**What it does not show**";

/// The heading whose `### ` sections are the gaps the lists link to.
const GAPS_HEADING: &str = "## What the evidence does not show";

/// The six texts PROD-08.4 maps, by the start of their `## ` heading. A text
/// that loses its section, or its section's table, fails the structure test.
const TEXTS: [&str; 6] = [
    "## DORA",
    "## The DORA RTS",
    "## ISO/IEC 27001",
    "## SOC 2",
    "## NIS2",
    "## HIPAA",
];

/// How a cited path names "every entry of an object keyed by topic name".
const MAP_KEY: &str = "<topic>";

/// How the derived field sets spell the value of any key of such an object.
/// Never written on the page: a citation must say what the keys are.
const MAP_VALUE: &str = "<>";

/// Word stems no sentence of the page may contain, in any inflection. Matched
/// as substrings of the lower-cased, whitespace-collapsed page, after the
/// [`PERMITTED`] passages are removed, so `DORA-compliant`, `complied`,
/// `certification`, `is satisfied by` and `fulfilment` are all caught. The
/// page states its limits with "provides evidence for", "supports a control
/// owner's …" and "does not show …" instead.
const CLAIM_STEMS: [&str; 8] = [
    "compli",   // complies, complied, compliance, compliant, non-compliant
    "comply",   // comply, complying
    "certif",   // certified, certifies, certify, certification, certificate
    "satisf",   // satisfies, satisfy, satisfied, satisfaction, satisfactory
    "fulfil",   // fulfil, fulfils, fulfill, fulfills, fulfilled, fulfilment
    "conform",  // conforms, conformity, conformance, conforming
    "adhere",   // adheres, adhered, adherence
    "accredit", // accredited, accreditation
];

/// Claim phrases that are not a stem: readiness labels, "in accordance with"
/// and an audit outcome.
const CLAIM_PHRASES: [&str; 10] = [
    "-ready",
    "audit ready",
    "regulator ready",
    "examination ready",
    "in accordance with",
    "in line with",
    "audit-proof",
    "pass an audit",
    "passes an audit",
    "pass the audit",
];

/// Verbs that, within three words of a regulatory object, state a verdict:
/// "meets the RTS", "addresses Article 12(7)", "demonstrates the
/// requirements". The passive participles are also checked against the five
/// words BEFORE them ("Article 25(5) is met").
const CLAIM_VERBS: [&str; 14] = [
    "meet",
    "meets",
    "met",
    "meeting",
    "address",
    "addresses",
    "addressed",
    "addressing",
    "demonstrate",
    "demonstrates",
    "demonstrated",
    "achieve",
    "achieves",
    "achieved",
];

/// The participles [`CLAIM_VERBS`] also checks backwards.
const PASSIVE_VERBS: [&str; 4] = ["met", "addressed", "demonstrated", "achieved"];

/// Regulatory objects matched as whole words.
const CLAIM_OBJECT_WORDS: [&str; 11] = [
    "dora", "rts", "nis2", "nis", "hipaa", "iso", "iec", "soc", "gdpr", "art", "a1",
];

/// Regulatory objects matched as word prefixes ("requirement" and
/// "requirements", "regulation" and "regulatory").
const CLAIM_OBJECT_PREFIXES: [&str; 16] = [
    "article",
    "clause",
    "control",
    "criteri",
    "requirement",
    "obligation",
    "standard",
    "regulat",
    "directive",
    "mandate",
    "annex",
    "paragraph",
    "specification",
    "safeguard",
    "auditor",
    "expectation",
];

/// Passages the claim scan removes before it looks: the page's own statement
/// of what it does not do, and the official text it quotes. Each is matched
/// lower-cased and whitespace-collapsed, exactly; a claim that merely shares
/// a word with one of them is still scanned.
const PERMITTED: [&str; 2] = [
    "this page makes no compliance claim",
    "necessary to meet the obligations set out in article 12",
];

/// The subset of claim words that has no honest technical use, swept across
/// every shipped operator page and not only the mapping. `docs/to-do/` is out
/// of the sweep: the trackers quote competitors' marketing in order to refuse
/// it. The stems above stay page-only: operator pages say "is satisfied by"
/// of a condition and "certificate" of TLS.
const SWEPT: [&str; 5] = [
    "compliant",
    "compliance-ready",
    "audit-ready",
    "audit ready",
    "regulator-ready",
];

/// Statements of a byte-level comparison: a supported row using one names
/// `byte-fingerprint` and cites `integrity.level`, because a `consume-only`
/// check compares no record contents.
const BYTE_LEVEL: [&str; 3] = ["byte for byte", "byte-for-byte", "exactly as archived"];

/// What no supported row may say: a restore is never shown to be a complete
/// or exact copy, every record is never checked, and a key is not a person.
const OVERREACH: [&str; 6] = [
    "exact cop",
    "complete cop",
    "every record",
    "all records",
    "a human",
    "guarantee",
];

/// Kafka configuration keys the page names in prose. They are dotted like
/// field paths and are not fields of any Logweir document; every OTHER
/// field-shaped token in the prose must resolve.
const KAFKA_CONFIG_KEYS: [&str; 4] = [
    "cleanup.policy",
    "message.timestamp.type",
    "min.insync.replicas",
    "retention.ms",
];

/// Inline-code tokens ending in these are file names, not field paths.
const FILE_SUFFIXES: [&str; 10] = [
    ".json", ".sig", ".md", ".pem", ".yaml", ".yml", ".py", ".rs", ".sh", ".txt",
];

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

fn read_json(relative: &str) -> Value {
    serde_json::from_str(&read(relative)).unwrap_or_else(|e| panic!("{relative} is JSON: {e}"))
}

fn join(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

/// The lines of `text` that are outside fenced code blocks.
fn prose_lines(text: &str) -> Vec<&str> {
    let mut inside = false;
    let mut out = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            inside = !inside;
            continue;
        }
        if !inside {
            out.push(line);
        }
    }
    out
}

/// The contents of every inline code span on one line.
fn code_spans(line: &str) -> Vec<&str> {
    line.split('`').skip(1).step_by(2).collect()
}

/// Every Markdown link target on one line: the `x` of each `](x)`.
fn link_targets(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(start) = rest.find("](") {
        let after = &rest[start + 2..];
        let Some(end) = after.find(')') else { break };
        out.push(&after[..end]);
        rest = &after[end..];
    }
    out
}

/// The cells of one Markdown table row, split on `|` outside code spans.
fn table_cells(line: &str) -> Vec<String> {
    let trimmed = line.trim();
    let inner = trimmed.strip_prefix('|').unwrap_or(trimmed);
    let inner = inner.strip_suffix('|').unwrap_or(inner);
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut in_code = false;
    for c in inner.chars() {
        match c {
            '`' => {
                in_code = !in_code;
                cell.push(c);
            }
            '|' if !in_code => cells.push(std::mem::take(&mut cell).trim().to_string()),
            _ => cell.push(c),
        }
    }
    cells.push(cell.trim().to_string());
    cells
}

/// GitHub's heading anchor: lower-cased, every character that is not a letter,
/// a digit, a space, `-` or `_` dropped, and each space turned into `-`.
fn slug(heading: &str) -> String {
    heading
        .trim()
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
        .map(|c| if c == ' ' { '-' } else { c })
        .collect()
}

/// A heading line's level and text, or nothing.
fn heading(line: &str) -> Option<(usize, &str)> {
    let hashes = line.chars().take_while(|c| *c == '#').count();
    if (1..=6).contains(&hashes) && line[hashes..].starts_with(' ') {
        Some((hashes, &line[hashes..]))
    } else {
        None
    }
}

/// Every heading anchor of a Markdown text, with GitHub's `-1`, `-2` suffixes
/// for repeated headings.
fn heading_slugs(text: &str) -> BTreeSet<String> {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut out = BTreeSet::new();
    for line in prose_lines(text) {
        let Some((_, title)) = heading(line) else {
            continue;
        };
        let base = slug(title);
        let n = seen.entry(base.clone()).or_insert(0);
        out.insert(if *n == 0 {
            base.clone()
        } else {
            format!("{base}-{n}")
        });
        *n += 1;
    }
    out
}

/// The section of a Markdown text under the heading whose anchor is `anchor`:
/// that heading and every line up to the next heading of the same or a higher
/// level. Empty when no heading has that anchor.
fn section_of(text: &str, anchor: &str) -> String {
    let mut out = String::new();
    let mut level: Option<usize> = None;
    let mut fenced = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
        }
        let here = if fenced { None } else { heading(line) };
        match (level, here) {
            (None, Some((n, title))) if slug(title) == anchor => level = Some(n),
            (Some(open), Some((n, _))) if n <= open => break,
            (None, _) => continue,
            _ => {}
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether `text` holds `word` as a whole identifier, not inside a longer one.
fn names_identifier(text: &str, word: &str) -> bool {
    text.match_indices(word).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + word.len()..].chars().next();
        !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
    })
}

/// Lower-cased, every whitespace run collapsed to one space, and a hyphenated
/// compound broken at a line end (`audit-` / `ready`) rejoined.
fn normalise(text: &str) -> String {
    let lower = text.to_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut in_space = false;
    for c in lower.chars() {
        if c.is_whitespace() {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out.replace("- ", "-")
}

/// The words of a normalised text, split on every character that is not a
/// letter or a digit.
fn words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect()
}

// -------------------------------------------------------------- the documents

/// The documents a citation may name: the name is the link text of a
/// citation in a `Fields` cell, `[Name](definition): `path`, …`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Doc {
    Scorecard,
    PutReceipt,
    BackupReceipt,
    CatalogPoint,
    StandingAuthorization,
    RehearsalSchedule,
    Restore,
}

impl Doc {
    const ALL: [Doc; 7] = [
        Doc::Scorecard,
        Doc::PutReceipt,
        Doc::BackupReceipt,
        Doc::CatalogPoint,
        Doc::StandingAuthorization,
        Doc::RehearsalSchedule,
        Doc::Restore,
    ];

    fn name(self) -> &'static str {
        match self {
            Doc::Scorecard => "Scorecard",
            Doc::PutReceipt => "Put receipt",
            Doc::BackupReceipt => "Backup receipt",
            Doc::CatalogPoint => "Catalog point",
            Doc::StandingAuthorization => "Standing authorization",
            Doc::RehearsalSchedule => "RehearsalSchedule",
            Doc::Restore => "Restore",
        }
    }

    /// The page that defines the document's fields, as the mapping links it
    /// from `docs/`. Every citation links exactly this, so each supported
    /// statement names the definition behind its field, and
    /// `every_cited_field_is_named_on_its_definition` holds that page to
    /// naming it. The two Kubernetes kinds link their generated CRD schema,
    /// which describes every field; the operator guide describes the kind.
    fn definition(self) -> &'static str {
        match self {
            Doc::Scorecard => "formats/drill-scorecard.md",
            Doc::PutReceipt => "verify-a-scorecard.md#the-storage-receipt-a-second-signed-document",
            Doc::BackupReceipt => "formats/backup-receipt.md",
            Doc::CatalogPoint => "formats/catalog-point.md",
            Doc::StandingAuthorization => {
                "stability.md#the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature"
            }
            Doc::RehearsalSchedule => "../config/crd/rehearsalschedules.yaml",
            Doc::Restore => "../config/crd/restores.yaml",
        }
    }

    fn from_name(name: &str) -> Option<Doc> {
        Doc::ALL.into_iter().find(|d| d.name() == name)
    }
}

/// The text of a definition link from `docs/`: the whole file, or the
/// section its anchor names.
fn definition_text(link: &str) -> String {
    let (file, anchor) = match link.split_once('#') {
        Some((file, anchor)) => (file, Some(anchor)),
        None => (link, None),
    };
    let text = read(&format!("docs/{file}"));
    match anchor {
        Some(anchor) => section_of(&text, anchor),
        None => text,
    }
}

/// The identifier a definition must name for `path`: its last segment that
/// is not a map key, without `[]`.
fn leaf(path: &str) -> &str {
    path.split('.')
        .rev()
        .map(|segment| segment.strip_suffix("[]").unwrap_or(segment))
        .find(|segment| !segment.starts_with('<'))
        .unwrap_or(path)
}

/// `path` with every [`MAP_KEY`] segment spelled as the derived sets spell a
/// map value. A literal [`MAP_VALUE`] on the page is refused before this.
fn canonical(path: &str) -> String {
    path.split('.')
        .map(|segment| {
            if segment == MAP_KEY {
                MAP_VALUE
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Every field path of every document, derived from its checked-in definition.
/// A path is the field's JSON path; an array element is `name[]`, as in
/// `phases[].notes`, and a map value is [`MAP_VALUE`].
struct Fields(BTreeMap<Doc, BTreeSet<String>>);

impl Fields {
    fn load() -> Fields {
        let mut sets = BTreeMap::new();
        sets.insert(
            Doc::Scorecard,
            json_schema_paths(&read_json(&newest_schema("drill-scorecard"))),
        );
        sets.insert(
            Doc::BackupReceipt,
            json_schema_paths(&read_json(&newest_schema("backup-receipt"))),
        );
        sets.insert(
            Doc::CatalogPoint,
            json_schema_paths(&read_json(&newest_schema("catalog-point"))),
        );
        sets.insert(
            Doc::RehearsalSchedule,
            crd_paths(&read("config/crd/rehearsalschedules.yaml")),
        );
        sets.insert(Doc::Restore, crd_paths(&read("config/crd/restores.yaml")));
        sets.insert(
            Doc::PutReceipt,
            put_receipt_paths(&read("docs/verify-a-scorecard.md")),
        );
        sets.insert(
            Doc::StandingAuthorization,
            standing_authorization_paths(&read(
                "crates/logweir-core/tests/fixtures/standing-authorization.json",
            )),
        );
        for (doc, set) in &sets {
            assert!(
                set.len() >= 6,
                "the {} field list derived only {} paths ({set:?}); a derivation that \
                 enumerates (almost) nothing would let every citation fail open or shut \
                 for the wrong reason",
                doc.name(),
                set.len()
            );
        }
        Fields(sets)
    }

    /// Whether `path`, as the page spells it, is a field of `doc`.
    fn has(&self, doc: Doc, path: &str) -> bool {
        self.0
            .get(&doc)
            .is_some_and(|set| spelled_as_cited(path) && set.contains(&canonical(path)))
    }

    fn any_has(&self, path: &str) -> bool {
        Doc::ALL.into_iter().any(|doc| self.has(doc, path))
    }

    /// Whether a prose token names a field: a whole path of some document,
    /// or, for a token of one segment, the last segment of one
    /// (`self_attested` for `approval.self_attested`). A dotted token must be
    /// a whole path: `config_coverage.coverage` is not the catalog point's
    /// `topics[].config_coverage.coverage`.
    fn resolves_in_prose(&self, token: &str) -> bool {
        if !spelled_as_cited(token) {
            return false;
        }
        let token = canonical(token);
        let tail = format!(".{token}");
        let one_segment = !token.contains('.');
        self.0
            .values()
            .flatten()
            .any(|path| *path == token || (one_segment && path.ends_with(&tail)))
    }
}

/// A path the page may write: no segment is the derived sets' own map spelling.
fn spelled_as_cited(path: &str) -> bool {
    path.split('.')
        .all(|segment| segment.strip_suffix("[]").unwrap_or(segment) != MAP_VALUE)
}

/// `MAJOR.MINOR.PATCH`, numerically, or nothing.
fn parse_semver(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|p| p.parse::<u64>().ok());
    let triple = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(triple)
}

/// The highest semver among `logweir-<stem>-<semver>.json` file names.
fn newest_version_file<'a>(
    stem: &str,
    names: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let prefix = format!("logweir-{stem}-");
    names
        .into_iter()
        .filter_map(|name| {
            let version = name.strip_prefix(&prefix)?.strip_suffix(".json")?;
            Some((parse_semver(version)?, name))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, name)| name)
}

/// The newest checked-in schema of a document, as a path from the repository
/// root. A MINOR bump only adds optional fields, so every field an older 1.x
/// schema declares is in the newest one; a MAJOR bump that drops a field the
/// page cites fails the field check, which is the point. Reading the newest,
/// not a pinned version, is what lets the page cite a field the day its
/// version lands.
fn newest_schema(stem: &str) -> String {
    let dir = repo_root().join("schemas");
    let names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()))
        .map(|entry| {
            entry
                .expect("a directory entry is readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let newest = newest_version_file(stem, names.iter().map(String::as_str))
        .unwrap_or_else(|| panic!("schemas/ holds no logweir-{stem}-<semver>.json"));
    format!("schemas/{newest}")
}

/// The field paths a JSON schema (draft-07, `definitions` + `$ref`) declares.
fn json_schema_paths(schema: &Value) -> BTreeSet<String> {
    let defs = schema.get("definitions").cloned().unwrap_or(Value::Null);
    let mut out = BTreeSet::new();
    walk_schema(schema, "", &defs, &mut out, 0);
    out
}

fn walk_schema(node: &Value, prefix: &str, defs: &Value, out: &mut BTreeSet<String>, depth: usize) {
    // A self-referential definition would otherwise recurse forever.
    if depth > 32 {
        return;
    }
    let Some(obj) = node.as_object() else { return };
    if let Some(reference) = obj.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or(reference);
        if let Some(target) = defs.get(name) {
            walk_schema(target, prefix, defs, out, depth + 1);
        }
    }
    for combinator in ["allOf", "anyOf", "oneOf"] {
        for sub in obj
            .get(combinator)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            walk_schema(sub, prefix, defs, out, depth + 1);
        }
    }
    if let Some(properties) = obj.get("properties").and_then(Value::as_object) {
        for (key, sub) in properties {
            let path = join(prefix, key);
            out.insert(path.clone());
            walk_schema(sub, &path, defs, out, depth + 1);
        }
    }
    if let Some(items) = obj.get("items") {
        walk_schema(items, &format!("{prefix}[]"), defs, out, depth + 1);
    }
    // An object keyed by data (the receipt's `config_coverage` and `records`,
    // keyed by topic name): its values are one path, entered through the map.
    // `additionalProperties: false` is not a map.
    if let Some(values) = obj.get("additionalProperties").filter(|v| v.is_object()) {
        let path = join(prefix, MAP_VALUE);
        out.insert(path.clone());
        walk_schema(values, &path, defs, out, depth + 1);
    }
}

/// The field paths of a CRD's OpenAPI schema, which `just crds` generates from
/// the Rust type and CI drift-checks.
fn crd_paths(yaml: &str) -> BTreeSet<String> {
    let crd: Value = serde_yaml::from_str(yaml).expect("a checked-in CRD parses");
    let schema = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"];
    assert!(
        schema.is_object(),
        "the CRD carries spec.versions[0].schema.openAPIV3Schema"
    );
    let mut out = BTreeSet::new();
    walk_schema(schema, "", &Value::Null, &mut out, 0);
    out
}

/// The field paths of a concrete JSON value. An array is entered through its
/// first element.
fn value_paths(value: &Value) -> BTreeSet<String> {
    fn walk(value: &Value, prefix: &str, out: &mut BTreeSet<String>) {
        match value {
            Value::Object(map) => {
                for (key, sub) in map {
                    let path = join(prefix, key);
                    out.insert(path.clone());
                    walk(sub, &path, out);
                }
            }
            Value::Array(items) => {
                if let Some(first) = items.first() {
                    walk(first, &format!("{prefix}[]"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(value, "", &mut out);
    out
}

/// Every fenced ```json block of a Markdown text that parses as JSON.
fn json_blocks(text: &str) -> Vec<Value> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let fence = line.trim_start();
        match current.as_mut() {
            None if fence.starts_with("```json") => current = Some(String::new()),
            Some(body) if fence.starts_with("```") => {
                if let Ok(value) = serde_json::from_str(body) {
                    blocks.push(value);
                }
                current = None;
            }
            Some(body) => {
                body.push_str(line);
                body.push('\n');
            }
            None => {}
        }
    }
    blocks
}

/// The put receipt has no checked-in schema: its fields are the Rust type's,
/// read by round-tripping the example `docs/verify-a-scorecard.md` documents it
/// with. The example must therefore parse as the type, which keeps it honest
/// as a side effect.
fn put_receipt_paths(guide: &str) -> BTreeSet<String> {
    let example = json_blocks(guide)
        .into_iter()
        .find(|v| v.get("scorecard_sha256").is_some())
        .expect("docs/verify-a-scorecard.md documents the put receipt with a JSON example");
    let receipt: PutReceipt = serde_json::from_value(example)
        .expect("the guide's put-receipt example parses as the PutReceipt the runner signs");
    value_paths(&serde_json::to_value(&receipt).expect("a PutReceipt serializes"))
}

/// The standing authorization has no checked-in schema either: its fields are
/// the Rust type's, read by round-tripping the fixture every standing test
/// reads. Fields the fixture carried and the type does not are dropped.
fn standing_authorization_paths(fixture: &str) -> BTreeSet<String> {
    let document: StandingAuthorization = serde_json::from_str(fixture)
        .expect("the standing-authorization fixture parses as the type");
    value_paths(&serde_json::to_value(&document).expect("a StandingAuthorization serializes"))
}

// -------------------------------------------------------------- the page check

/// What a survey of the page found, and what is wrong with it.
#[derive(Default)]
struct Survey {
    tables: usize,
    rows: usize,
    citations: Vec<(Doc, String)>,
    gap_lists: usize,
    gap_items: usize,
    problems: Vec<String>,
}

/// Is this inline-code token shaped like a field path (`a.b`, `a_b`,
/// `a[].b`, `a.<topic>.b`), rather than a value, a command or a file name?
fn field_shaped(token: &str) -> bool {
    if !token.starts_with(|c: char| c.is_ascii_lowercase()) {
        return false;
    }
    if !token.contains('.') && !token.contains('_') {
        return false;
    }
    if FILE_SUFFIXES.iter().any(|suffix| token.ends_with(suffix)) {
        return false;
    }
    token.split('.').all(|segment| {
        let segment = segment.strip_suffix("[]").unwrap_or(segment);
        if let Some(key) = segment.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
            return key.chars().all(|c| c.is_ascii_lowercase() || c == '_');
        }
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

/// One citation group, `[Name](definition): paths`, as its three parts.
fn split_citation(group: &str) -> Option<(&str, &str, &str)> {
    let rest = group.strip_prefix('[')?;
    let (name, rest) = rest.split_once("](")?;
    let (definition, rest) = rest.split_once(')')?;
    let paths = rest.strip_prefix(':')?;
    Some((name, definition, paths))
}

/// Parse one `Fields` cell: groups separated by `;`, each
/// `[Name](definition): `path`, …`.
fn parse_fields_cell(cell: &str, statement: &str, survey: &mut Survey) -> usize {
    let mut cited = 0;
    for group in cell.split(';').map(str::trim).filter(|g| !g.is_empty()) {
        let Some((name, definition, paths)) = split_citation(group) else {
            survey.problems.push(format!(
                "a citation must read `[Document](definition): `path`, …`, linking the page that \
                 defines the document's fields; found `{group}` (row: {statement})"
            ));
            continue;
        };
        let Some(doc) = Doc::from_name(name.trim()) else {
            survey.problems.push(format!(
                "a citation names an unknown document `{}` (row: {statement}); the documents are {:?}",
                name.trim(),
                Doc::ALL.map(Doc::name)
            ));
            continue;
        };
        if definition != doc.definition() {
            survey.problems.push(format!(
                "a {} citation must link its definition `{}`, not `{definition}` (row: {statement})",
                doc.name(),
                doc.definition()
            ));
        }
        let spans = code_spans(paths);
        if spans.is_empty() {
            survey.problems.push(format!(
                "`{}` is followed by no field (row: {statement})",
                doc.name()
            ));
        }
        for path in spans {
            cited += 1;
            survey.citations.push((doc, path.to_string()));
        }
    }
    cited
}

/// Every supported-statement row of the page, as (statement, fields cell).
fn supported_rows(page: &str) -> Vec<(String, String)> {
    let lines = prose_lines(page);
    let mut rows = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].starts_with('|') && table_cells(lines[i]) == SUPPORTS_HEADER {
            i += 2;
            while i < lines.len() && lines[i].starts_with('|') {
                let mut cells = table_cells(lines[i]).into_iter();
                let statement = cells.next().unwrap_or_default();
                let fields = cells.next().unwrap_or_default();
                rows.push((statement, fields));
                i += 1;
            }
            continue;
        }
        i += 1;
    }
    rows
}

/// Walk the page: every supported-statement table, every "does not show" list
/// and every field-shaped token in its prose.
fn survey(page: &str, fields: &Fields) -> Survey {
    let mut survey = Survey::default();
    let lines = prose_lines(page);

    // The gap sections every "does not show" item must point into.
    let gap_slugs: BTreeSet<String> = {
        let mut slugs = BTreeSet::new();
        let mut inside = false;
        for line in &lines {
            if line.starts_with("## ") {
                inside = line.trim() == GAPS_HEADING;
            } else if inside {
                if let Some(heading) = line.strip_prefix("### ") {
                    slugs.insert(slug(heading));
                }
            }
        }
        slugs
    };
    if gap_slugs.is_empty() {
        survey.problems.push(format!(
            "the page has no `### ` gap sections under `{GAPS_HEADING}`"
        ));
    }

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        // A supported-statement table.
        if line.starts_with('|') && table_cells(line) == SUPPORTS_HEADER {
            survey.tables += 1;
            i += 2; // the header and its separator row
            let mut rows_here = 0;
            while i < lines.len() && lines[i].starts_with('|') {
                let cells = table_cells(lines[i]);
                rows_here += 1;
                survey.rows += 1;
                if cells.len() != 2 {
                    survey.problems.push(format!(
                        "a supported-statement row has {} cells: {}",
                        cells.len(),
                        lines[i]
                    ));
                } else if parse_fields_cell(&cells[1], &cells[0], &mut survey) == 0 {
                    survey.problems.push(format!(
                        "a supported statement cites no field: {}",
                        cells[0]
                    ));
                }
                i += 1;
            }
            if rows_here == 0 {
                survey
                    .problems
                    .push("a supported-statement table has no rows".to_string());
            }
            continue;
        }
        // A "does not show" list.
        if line.trim() == DOES_NOT_SHOW {
            survey.gap_lists += 1;
            i += 1;
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            let mut items: Vec<String> = Vec::new();
            while i < lines.len() {
                let item_line = lines[i];
                if let Some(rest) = item_line.strip_prefix("- ") {
                    items.push(rest.to_string());
                } else if item_line.starts_with("  ") && !items.is_empty() {
                    let last = items.last_mut().expect("an item exists");
                    last.push(' ');
                    last.push_str(item_line.trim());
                } else {
                    break;
                }
                i += 1;
            }
            if items.is_empty() {
                survey
                    .problems
                    .push(format!("a `{DOES_NOT_SHOW}` label has no list under it"));
            }
            for item in items {
                survey.gap_items += 1;
                let points_into_gaps = link_targets(&item)
                    .iter()
                    .filter_map(|t| t.strip_prefix('#'))
                    .any(|anchor| gap_slugs.contains(anchor));
                if !points_into_gaps {
                    survey.problems.push(format!(
                        "a \"does not show\" item links to no gap section under `{GAPS_HEADING}`: {item}"
                    ));
                }
            }
            continue;
        }
        i += 1;
    }

    // Every citation resolves in the document it names.
    for (doc, path) in &survey.citations {
        if !fields.has(*doc, path) {
            survey.problems.push(format!(
                "{}: `{path}` is not a field of that document",
                doc.name()
            ));
        }
    }

    // Every field-shaped token in the prose resolves in SOME document, as a
    // whole path or as the tail of one. Only the Kafka configuration keys the
    // page names (`cleanup.policy`) and file names are exempt; a made-up path
    // (`retention_lock.mode`) and a typo in a real block (`integrity.resutl`)
    // are not.
    for line in &lines {
        for token in code_spans(line) {
            if field_shaped(token)
                && !KAFKA_CONFIG_KEYS.contains(&token)
                && !fields.resolves_in_prose(token)
            {
                survey.problems.push(format!(
                    "the prose cites `{token}`, which no cited document has"
                ));
            }
        }
    }
    survey
}

/// Every cited field that the definition its citation links does not name.
/// `definition_of` returns the definition's text for a document, so a test can
/// hand in a definition that lost a field.
fn unnamed_citations(
    citations: &[(Doc, String)],
    definition_of: impl Fn(Doc) -> String,
) -> Vec<String> {
    let mut texts: BTreeMap<Doc, String> = BTreeMap::new();
    let mut out = Vec::new();
    for (doc, path) in citations {
        let text = texts.entry(*doc).or_insert_with(|| definition_of(*doc));
        let name = leaf(path);
        if !names_identifier(text, name) {
            out.push(format!(
                "{}: `{path}` is cited, and its definition `{}` never names `{name}`",
                doc.name(),
                doc.definition()
            ));
        }
    }
    out
}

/// Every anchor the page links: in-page ones against its own headings, and
/// relative `file.md#anchor` ones against that file's headings.
fn anchor_problems(page: &str, page_dir: &Path) -> Vec<String> {
    let own = heading_slugs(page);
    let mut problems = Vec::new();
    for line in prose_lines(page) {
        for target in link_targets(line) {
            if target.starts_with("http://") || target.starts_with("https://") {
                continue;
            }
            let Some((file, anchor)) = target.split_once('#') else {
                continue;
            };
            if file.is_empty() {
                if !own.contains(anchor) {
                    problems.push(format!(
                        "the page links #{anchor}, which no heading of the page has"
                    ));
                }
                continue;
            }
            let path = page_dir.join(file);
            match std::fs::read_to_string(&path) {
                Ok(text) => {
                    if !heading_slugs(&text).contains(anchor) {
                        problems.push(format!(
                            "the page links {file}#{anchor}, which no heading of {file} has"
                        ));
                    }
                }
                Err(e) => {
                    problems.push(format!("the page links {file}, which cannot be read: {e}"))
                }
            }
        }
    }
    problems
}

/// A text with its inline code spans and link targets removed: field paths
/// and URLs are not prose a claim verb can act on.
fn without_code_and_targets(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for (n, piece) in text.split('`').enumerate() {
        if n % 2 == 0 {
            out.push_str(piece);
            out.push(' ');
        }
    }
    let mut cleaned = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(start) = rest.find("](") {
        cleaned.push_str(&rest[..=start]);
        let after = &rest[start + 2..];
        match after.find(')') {
            Some(end) => rest = &after[end + 1..],
            None => {
                rest = after;
                break;
            }
        }
    }
    cleaned.push_str(rest);
    cleaned
}

fn is_claim_object(word: &str) -> bool {
    CLAIM_OBJECT_WORDS.contains(&word) || CLAIM_OBJECT_PREFIXES.iter().any(|p| word.starts_with(p))
}

/// Every claim the text makes: a claim stem, a claim phrase, or a claim verb
/// within three words of a regulatory object (five before a passive
/// participle), in one sentence. The [`PERMITTED`] passages are removed first.
fn claim_findings(text: &str) -> Vec<String> {
    let mut normalised = normalise(text);
    for permitted in PERMITTED {
        normalised = normalised.replace(permitted, " ");
    }
    let mut found: Vec<String> = Vec::new();
    for stem in CLAIM_STEMS {
        if normalised.contains(stem) {
            found.push(format!("the stem `{stem}`"));
        }
    }
    for phrase in CLAIM_PHRASES {
        if normalised.contains(phrase) {
            found.push(format!("the phrase `{phrase}`"));
        }
    }
    let prose = without_code_and_targets(&normalised);
    for sentence in prose.split(['.', ';', ':', '!', '?']) {
        let w = words(sentence);
        for (i, verb) in w.iter().enumerate() {
            if !CLAIM_VERBS.contains(verb) {
                continue;
            }
            let mut after = w.iter().skip(i + 1).take(3);
            let before = w[i.saturating_sub(5)..i].iter();
            let object = if PASSIVE_VERBS.contains(verb) {
                after.chain(before).find(|o| is_claim_object(o))
            } else {
                after.find(|o| is_claim_object(o))
            };
            if let Some(object) = object {
                found.push(format!("`{verb}` next to `{object}`"));
            }
        }
    }
    found
}

/// Every supported row that says more than its condition allows.
fn overreach_problems(page: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for (statement, fields) in supported_rows(page) {
        let text = normalise(&statement);
        if BYTE_LEVEL.iter().any(|p| text.contains(p))
            && !(statement.contains("`byte-fingerprint`") && fields.contains("`integrity.level`"))
        {
            problems.push(format!(
                "a row claims a byte-level comparison without naming `byte-fingerprint` and \
                 citing `integrity.level` (a `consume-only` check compares no contents): \
                 {statement}"
            ));
        }
        let w = words(&text);
        let every = w.iter().any(|x| matches!(*x, "each" | "every" | "all"));
        let signed = w.contains(&"signed");
        let a_test = w
            .iter()
            .any(|x| x.starts_with("test") || x.starts_with("rehears") || x.starts_with("drill"));
        if every && signed && a_test && !w.iter().any(|x| x.starts_with("reach")) {
            problems.push(format!(
                "a row says every test is signed without \"that reaches a result\" (exit 1, 3 \
                 and 4 sign nothing): {statement}"
            ));
        }
        if w.contains(&"configuration")
            && w.iter().any(|x| x.starts_with("compar"))
            && !fields.contains("not_assessed`")
        {
            problems.push(format!(
                "a row says configuration was compared without citing the `not_assessed` list \
                 that says for which topics it was not (FX-4): {statement}"
            ));
        }
        for phrase in OVERREACH {
            if text.contains(phrase) {
                problems.push(format!("a row says `{phrase}`: {statement}"));
            }
        }
    }
    problems
}

fn phrases_in<'a>(text: &str, phrases: &[&'a str]) -> Vec<&'a str> {
    let normalised = normalise(text);
    phrases
        .iter()
        .copied()
        .filter(|p| normalised.contains(p))
        .collect()
}

fn links_to(text: &str, target: &str) -> bool {
    text.lines()
        .flat_map(link_targets)
        .any(|t| t.split('#').next() == Some(target))
}

/// The page with `extra` inserted as a new first row of its first table of
/// supported statements.
fn with_extra_row(page: &str, extra: &str) -> String {
    let mut out = String::new();
    let mut lines = page.lines();
    let mut done = false;
    while let Some(line) = lines.next() {
        out.push_str(line);
        out.push('\n');
        if !done && line.starts_with('|') && table_cells(line) == SUPPORTS_HEADER {
            let separator = lines
                .next()
                .expect("a table header is followed by its separator");
            out.push_str(separator);
            out.push('\n');
            out.push_str(extra);
            out.push('\n');
            done = true;
        }
    }
    assert!(
        done,
        "the page has a table of supported statements to mutate"
    );
    out
}

// --------------------------------------------------------------- the tests

/// **Every field the page cites exists in the document it names**, every
/// supported statement cites one, every "does not show" item points at a gap,
/// and each of the six texts keeps its section with both parts.
#[test]
fn every_field_the_control_evidence_page_cites_exists() {
    let fields = Fields::load();
    let page = read(PAGE);
    let survey = survey(&page, &fields);

    assert!(
        survey.problems.is_empty(),
        "{PAGE} has {} problem(s):\n  {}",
        survey.problems.len(),
        survey.problems.join("\n  ")
    );

    // A check that enumerated nothing is not a pass.
    assert!(
        survey.tables >= TEXTS.len()
            && survey.rows >= 2 * TEXTS.len()
            && survey.citations.len() >= 50,
        "the survey found {} tables, {} rows and {} citations: too few for a page that maps six \
         texts, so the parser no longer reads the page",
        survey.tables,
        survey.rows,
        survey.citations.len()
    );
    assert_eq!(
        survey.gap_lists, survey.tables,
        "every clause section has both a table of what the evidence supports and a list of \
         what it does not show"
    );
    assert!(
        survey.gap_items >= survey.gap_lists,
        "every list of what the evidence does not show has at least one item"
    );
    for doc in Doc::ALL {
        assert!(
            survey.citations.iter().any(|(d, _)| *d == doc),
            "{PAGE} cites no field of the {} — either the document left the mapping, or the \
             citation syntax changed under the parser",
            doc.name()
        );
    }

    // Each of the six texts keeps its own section, with both parts.
    let sections: Vec<&str> = page.split("\n## ").skip(1).collect();
    for text in TEXTS {
        let heading = text.trim_start_matches("## ");
        let section = sections
            .iter()
            .find(|s| s.starts_with(heading))
            .unwrap_or_else(|| panic!("{PAGE} lost its section `{text}`"));
        let section = format!("## {section}");
        let local = survey_section(&section, &fields);
        assert!(
            local.tables >= 1 && local.gap_lists >= 1,
            "the section `{text}` must keep a table of supported statements and a list of what \
             the evidence does not show"
        );
    }
}

fn survey_section(section: &str, fields: &Fields) -> Survey {
    // Gap links resolve against the whole page's gap list, so only the counts
    // matter here.
    let mut local = survey(section, fields);
    local.problems.clear();
    local
}

/// **Negative control: a made-up field, and a real field under the wrong
/// document, are refused** — by the field sets directly, and by the page check
/// when one is written into the page or its prose.
#[test]
fn a_made_up_or_misattributed_field_is_refused() {
    let fields = Fields::load();

    // The field sets are not vacuous: real fields are found where they live...
    for (doc, real) in [
        (Doc::Scorecard, "integrity.records_sampled_matching"),
        (Doc::Scorecard, "phases[].notes"),
        (Doc::Scorecard, "topic_parity.not_assessed"),
        (Doc::BackupReceipt, "covered.from_ms"),
        (Doc::BackupReceipt, "config_coverage"),
        (Doc::CatalogPoint, "topics[].records"),
        (Doc::CatalogPoint, "topics[].config_coverage.coverage"),
        (Doc::PutReceipt, "create_only_enforced"),
        (Doc::StandingAuthorization, "scope.templateDigest"),
        (Doc::RehearsalSchedule, "status.lastSucceeded.rtoSeconds"),
        (Doc::Restore, "status.exitCode"),
    ] {
        assert!(
            fields.has(doc, real),
            "{}: `{real}` must resolve",
            doc.name()
        );
    }
    // ...made-up ones are not, in any document...
    for (doc, made_up) in [
        (Doc::Scorecard, "integrity.exhaustively_checked"),
        (Doc::BackupReceipt, "covered.complete"),
        (Doc::CatalogPoint, "capture.verified_at"),
        (Doc::PutReceipt, "object_lock_mode"),
        (Doc::StandingAuthorization, "scope.maxRecords"),
        (
            Doc::RehearsalSchedule,
            "status.lastSucceeded.allRecordsVerified",
        ),
        (Doc::Restore, "status.signedFailure"),
    ] {
        assert!(
            !fields.has(doc, made_up),
            "{}: made-up `{made_up}` must not resolve",
            doc.name()
        );
        assert!(
            !fields.any_has(made_up),
            "made-up `{made_up}` must resolve in no document"
        );
    }
    // ...and a real field is refused under a document that does not carry it.
    for (doc, elsewhere) in [
        (Doc::Scorecard, "covered.from_ms"),
        (Doc::BackupReceipt, "integrity.level"),
        (Doc::CatalogPoint, "exit_code"),
        (Doc::PutReceipt, "outcome"),
        (Doc::StandingAuthorization, "spec.schedule"),
        (Doc::RehearsalSchedule, "scope.templateDigest"),
        (Doc::Restore, "status.lastFailed.reason"),
    ] {
        assert!(fields.any_has(elsewhere), "`{elsewhere}` is real somewhere");
        assert!(
            !fields.has(doc, elsewhere),
            "`{elsewhere}` must not resolve as a {} field",
            doc.name()
        );
    }

    // The page check refuses each, written into a copy of the real page.
    let page = read(PAGE);
    for (row, expected) in [
        (
            "| A made-up statement. | [Scorecard](formats/drill-scorecard.md): `integrity.exhaustively_checked` |",
            "Scorecard: `integrity.exhaustively_checked` is not a field",
        ),
        (
            "| A misattributed statement. | [Scorecard](formats/drill-scorecard.md): `covered.from_ms` |",
            "Scorecard: `covered.from_ms` is not a field",
        ),
        (
            "| A statement from an unknown document. | [Ledger](formats/ledger.md): `entries` |",
            "unknown document `Ledger`",
        ),
        ("| An uncited statement. |  |", "cites no field"),
        (
            "| A statement with a name and no field. | [Scorecard](formats/drill-scorecard.md): none |",
            "`Scorecard` is followed by no field",
        ),
        (
            "| A statement linking the wrong definition. | [Scorecard](formats/backup-receipt.md): `run_id` |",
            "a Scorecard citation must link its definition `formats/drill-scorecard.md`",
        ),
        (
            "| A statement that names no definition. | Scorecard: `run_id` |",
            "a citation must read `[Document](definition)",
        ),
        (
            "| A schedule cited through the operator guide. | [RehearsalSchedule](kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization): `spec.schedule` |",
            "a RehearsalSchedule citation must link its definition `../config/crd/rehearsalschedules.yaml`",
        ),
    ] {
        let mutated = with_extra_row(&page, row);
        let problems = survey(&mutated, &fields).problems;
        assert!(
            problems.iter().any(|p| p.contains(expected)),
            "the page check must refuse the row {row}; it reported {problems:?}"
        );
    }

    // And a made-up dotted field in the prose, outside any table — one whose
    // first segment is a real block, and one whose first segment is not
    // (the review's mutant ME, which the first version of this check missed).
    for (sentence, token) in [
        (
            "The scorecard's `measured.rto_guaranteed_seconds` field.",
            "`measured.rto_guaranteed_seconds`",
        ),
        (
            "The put receipt records `retention_lock.mode` for every object.",
            "`retention_lock.mode`",
        ),
        (
            "The receipt records `config_coverage.<topic>.exhaustive` per topic.",
            "`config_coverage.<topic>.exhaustive`",
        ),
    ] {
        let mutated = page.replacen(
            "### Key custody\n",
            &format!("### Key custody\n\n{sentence}\n"),
            1,
        );
        assert_ne!(mutated, page, "the mutation found its anchor heading");
        let problems = survey(&mutated, &fields).problems;
        assert!(
            problems.iter().any(|p| p.contains(token)),
            "the prose check must refuse {token} outside the tables; it reported {problems:?}"
        );
    }
    // A Kafka configuration key in the prose is not a field and passes, but
    // only the ones the page names: a made-up key is refused like any path.
    let mutated = page.replacen(
        "### Key custody\n",
        "### Key custody\n\nThe source's `min.insync.replicas` and `cleanup.policy`, and its `segment.madeup.bytes`.\n",
        1,
    );
    let problems = survey(&mutated, &fields).problems;
    assert!(
        problems
            .iter()
            .any(|p| p.contains("`segment.madeup.bytes`"))
            && !problems.iter().any(|p| p.contains("`min.insync.replicas`")),
        "only the listed Kafka keys are exempt from the prose check; it reported {problems:?}"
    );
}

/// **An object keyed by topic name is cited through `<topic>`, and only
/// through it** (FX-4's `config_coverage`). The derived sets enter such a map
/// once, as one value path, so a literal key, a missing map segment, array
/// syntax on a map, a made-up field under the value, a `<topic>` on a field
/// that is not a map, and an unknown placeholder are all refused.
#[test]
fn a_map_value_is_cited_through_the_topic_segment_only() {
    let fields = Fields::load();
    for path in [
        "config_coverage.<topic>",
        "config_coverage.<topic>.coverage",
        "config_coverage.<topic>.reason",
        "config_coverage.<topic>.timestamp_type.value",
        "config_coverage.<topic>.timestamp_type.source",
        "records.<topic>",
    ] {
        assert!(
            fields.has(Doc::BackupReceipt, path),
            "Backup receipt: `{path}` must resolve"
        );
    }
    for path in [
        "config_coverage.orders.coverage",
        "config_coverage.coverage",
        "config_coverage[].coverage",
        "config_coverage.<topic>.exhaustive",
        "config_coverage.<partition>.coverage",
        "config_coverage.<>.coverage",
        "source.<topic>.cluster_id",
        "covered.<topic>",
    ] {
        assert!(
            !fields.has(Doc::BackupReceipt, path),
            "Backup receipt: `{path}` must be refused"
        );
        assert!(
            !fields.resolves_in_prose(path),
            "`{path}` must not resolve in the prose either"
        );
    }
    // The catalog point's copy is an array of topics, entered with `[]`.
    assert!(fields.has(Doc::CatalogPoint, "topics[].config_coverage.coverage"));
    assert!(!fields.has(Doc::CatalogPoint, "topics.<topic>.config_coverage"));

    // Written into the page, a literal topic name is refused by the survey.
    let page = read(PAGE);
    let mutated = with_extra_row(
        &page,
        "| A statement citing one topic's entry. | [Backup receipt](formats/backup-receipt.md): `config_coverage.orders.coverage` |",
    );
    let problems = survey(&mutated, &fields).problems;
    assert!(
        problems
            .iter()
            .any(|p| p.contains("Backup receipt: `config_coverage.orders.coverage` is not a field")),
        "the page check must refuse a literal map key; it reported {problems:?}"
    );
}

/// **The newest schema is chosen by numeric semver**: `1.10.0` beats `1.2.0`
/// (a lexicographic pick would not), and another document's file or a malformed
/// version is never chosen. This selection feeds the field check, so it gets
/// its own negative control.
#[test]
fn the_newest_schema_is_chosen_by_numeric_semver() {
    let names = [
        "logweir-drill-scorecard-1.0.0.json",
        "logweir-drill-scorecard-1.2.0.json",
        "logweir-drill-scorecard-1.10.0.json",
        "logweir-drill-scorecard-2.0.json",
        "logweir-backup-receipt-9.9.9.json",
        "logweir-api-v1.openapi.json",
    ];
    assert_eq!(
        newest_version_file("drill-scorecard", names),
        Some("logweir-drill-scorecard-1.10.0.json")
    );
    assert_eq!(newest_version_file("catalog-point", names), None);
    for stem in ["drill-scorecard", "backup-receipt", "catalog-point"] {
        let path = newest_schema(stem);
        assert!(
            repo_root().join(&path).is_file(),
            "the newest {stem} schema `{path}` is a file"
        );
    }
}

/// **Every cited field is named on the definition its citation links**, so a
/// reader who follows the link finds the field (review finding L3: two
/// documents linked pages that never named the fields cited under them).
#[test]
fn every_cited_field_is_named_on_its_definition() {
    let fields = Fields::load();
    let page = read(PAGE);
    let citations = survey(&page, &fields).citations;
    assert!(citations.len() >= 50, "the survey found the citations");
    let problems = unnamed_citations(&citations, |doc| definition_text(doc.definition()));
    assert!(problems.is_empty(), "{PAGE}:\n  {}", problems.join("\n  "));

    // Negative controls. The standing authorization's section before its
    // scope was documented, which elided the scope:
    let elided = "### The standing rehearsal authorization is SIGNED\n\n\"subjectRef\": {\"uid\": \"…\"},\n\
                  \"scope\": { …D3 §4.3's RehearsalScope, camelCase… },\n\"issuedAt\": \"…\", \"expiresAt\": \"…\"\n";
    let standing = [
        (Doc::StandingAuthorization, "scope.modes".to_string()),
        (Doc::StandingAuthorization, "subjectRef.uid".to_string()),
    ];
    let problems = unnamed_citations(&standing, |_| elided.to_string());
    assert_eq!(
        problems.len(),
        1,
        "`scope.modes` is unnamed in the elided section and `subjectRef.uid` is named; got \
         {problems:?}"
    );
    assert!(problems[0].contains("`modes`"), "{problems:?}");

    // The operator guide's §7g, which describes the schedule and never names
    // the objective's field:
    let guide_7g = definition_text(
        "kubernetes.md#7g-a-rehearsalschedule-proves-recovery-on-a-cron-under-one-signed-authorization",
    );
    assert!(guide_7g.len() > 1000, "the §7g section was found");
    let schedule = [(
        Doc::RehearsalSchedule,
        "spec.objectives.rtoSeconds".to_string(),
    )];
    assert_eq!(
        unnamed_citations(&schedule, |_| guide_7g.clone()).len(),
        1,
        "§7g does not name `rtoSeconds`, so a citation linking it must be refused"
    );
    assert!(
        unnamed_citations(&schedule, |doc| definition_text(doc.definition())).is_empty(),
        "the CRD schema names `rtoSeconds`"
    );
    // A name inside a longer identifier is not a name.
    assert!(!names_identifier("`last_phase_completed`", "phase"));
    assert!(names_identifier("`phases[].notes`", "notes"));
}

/// **Every anchor the page links resolves**, in the page and in the docs it
/// points into. `just links` checks only that the files exist.
#[test]
fn every_anchor_the_control_evidence_page_links_resolves() {
    let root = repo_root();
    let page = read(PAGE);
    let problems = anchor_problems(&page, &root.join("docs"));
    assert!(problems.is_empty(), "{PAGE}:\n  {}", problems.join("\n  "));

    // Negative controls: a dangling in-page anchor and a dangling cross-file one.
    for (from, to) in [
        ("(#key-custody)", "(#key-custodian)"),
        (
            "kubernetes.md#154-the-residual-stated-plainly",
            "kubernetes.md#154-the-residual",
        ),
    ] {
        let mutated = page.replacen(from, to, 1);
        assert_ne!(mutated, page, "the mutation found `{from}`");
        assert!(
            !anchor_problems(&mutated, &root.join("docs")).is_empty(),
            "a link to `{to}` must be refused"
        );
    }
}

/// **Every "does not show" item links to a gap section** (negative control: an
/// unlinked item is refused).
#[test]
fn an_unlinked_gap_item_is_refused() {
    let fields = Fields::load();
    let page = read(PAGE);
    let mutated = page.replacen(
        &format!("{DOES_NOT_SHOW}\n\n"),
        &format!("{DOES_NOT_SHOW}\n\n- Something the evidence does not show, with no gap named.\n"),
        1,
    );
    assert_ne!(mutated, page, "the mutation found a `{DOES_NOT_SHOW}` list");
    let problems = survey(&mutated, &fields).problems;
    assert!(
        problems
            .iter()
            .any(|p| p.contains("links to no gap section")),
        "an item that names no gap must be refused; the check reported {problems:?}"
    );

    // A link to a heading that is not a gap does not count either.
    let mutated = page.replacen(
        &format!("{DOES_NOT_SHOW}\n\n"),
        &format!("{DOES_NOT_SHOW}\n\n- See [the sources](#sources).\n"),
        1,
    );
    let problems = survey(&mutated, &fields).problems;
    assert!(
        problems
            .iter()
            .any(|p| p.contains("links to no gap section")),
        "an item linking outside the gap sections must be refused; the check reported {problems:?}"
    );
}

/// **No sentence on the page claims compliance.**
#[test]
fn the_control_evidence_page_makes_no_compliance_claim() {
    let page = read(PAGE);
    let found = claim_findings(&page);
    assert!(
        found.is_empty(),
        "{PAGE} must not use {found:?}: the page states what the evidence supports and does \
         not show, and leaves the verdict to the organisation, its auditor and its regulator"
    );
}

/// The claim sentences every scan must catch: the review's planted mutants
/// MA-MD, the fix brief's forms and their inflections, and every phrase the
/// first version of the guard banned.
const CLAIM_SENTENCES: [&str; 40] = [
    // The review's planted sentences, verbatim.
    "Logweir complies with DORA Article 12(7).",
    "A passing rehearsal meets the RTS.",
    "Article 25(5) is satisfied by the signed scorecard.",
    "These fields are sufficient evidence for an ISO/IEC 27001 certification audit and keep an entity in compliance with NIS2.",
    // The fix brief's forms, and inflections.
    "The installation is in compliance with the regulation.",
    "The backup conforms to ISO/IEC 27001 control 8.13.",
    "The scorecard fulfils the requirement of Article 25(5).",
    "The scorecard fulfills the requirement of Article 25(5).",
    "Signed scorecards fulfil point 4.2.6.",
    "Point 4.2.6 is fulfilled by the scorecards.",
    "A rehearsal lets the entity comply with Article 11(6).",
    "The test complied with the RTS.",
    "Each rehearsal is complying with the clause.",
    "Logweir is certified for SOC 2.",
    "Logweir certifies the restore.",
    "The page certifies nothing, and the scorecard is a certificate of recovery.",
    "Logweir satisfies Article 25.",
    "The evidence satisfied the auditor.",
    "The control is satisfactory.",
    "Logweir's evidence shows conformity with HIPAA.",
    "Logweir adheres to the RTS.",
    "The scorecard is accredited evidence.",
    "A rehearsal meets DORA's testing requirement.",
    "The rehearsal met the requirements of Article 40.",
    "Article 25(5) is met by the signed scorecard.",
    "Logweir addresses Article 12(7).",
    "The control is addressed by a rehearsal.",
    "The evidence demonstrates the requirements of 164.308.",
    "Rehearsals achieve DORA's objectives.",
    "Tests are run in accordance with Article 25.",
    "The evidence is in line with the RTS.",
    "This evidence will pass an audit.",
    // The first guard's phrases, in sentences.
    "With this release Logweir is DORA-compliant.",
    "The evidence is compliance-ready.",
    "The scorecard meets the requirement.",
    "The scorecards meet the requirement.",
    "Logweir is audit-ready.",
    "Logweir is audit ready.",
    "Logweir is regulator-ready.",
    "Logweir guarantees compliance and ensures compliance.",
];

/// **Negative control: every claim form is caught**, written at the end of the
/// real page as it is, wrapped across two source lines at every space, and
/// upper-cased.
#[test]
fn every_claim_form_is_caught_even_when_wrapped() {
    let page = read(PAGE);
    assert!(claim_findings(&page).is_empty(), "the page itself passes");
    for sentence in CLAIM_SENTENCES {
        for variant in [
            sentence.to_string(),
            sentence.replace(' ', "\n"),
            sentence.to_uppercase(),
        ] {
            let mutated = format!("{page}\n{variant}\n");
            assert!(
                !claim_findings(&mutated).is_empty(),
                "the scan must catch {variant:?}"
            );
        }
    }
    // A hyphenated claim broken at the line end is rejoined.
    assert!(!claim_findings(&format!("{page}\nLogweir is audit-\nready.\n")).is_empty());
}

/// **Positive control: the page's own phrasings pass**, so the stems and the
/// verb-object rule cannot over-ban silently.
#[test]
fn the_permitted_phrasings_pass_the_claim_scan() {
    for permitted in [
        "This page makes no compliance claim.",
        "It does not say that Logweir answers any clause.",
        "The evidence provides evidence for a control owner's statement.",
        "It can support a control owner's statement that a restore was tested and what the test found.",
        "It does not show that every record was checked.",
        "The objectives the approved plan set for the test, and whether the test met them, are recorded.",
        "The RTO and pass-rate objectives the plan set for this test, and whether the test met them, are recorded.",
        "The entity tests recovery plan procedures supporting system recovery to meet its objectives.",
        "switchovers between the primary ICT infrastructure and the redundant capacity, backups and \
         redundant facilities necessary to meet the obligations set out in Article 12",
        "Any identified deficiencies resulting from that testing shall be analysed, addressed, and \
         reported to the management body.",
        "Article 40(3) asks these entities, too, to document the results and to analyse, address \
         and report deficiencies to the management body.",
        "using the criteria does not require an assessment of whether each one is addressed.",
        "Such time objectives shall ensure that, in extreme scenarios, the agreed service levels are met.",
        "`objectives.met` reads `null`, and the requirement is the control owner's to state.",
    ] {
        let found = claim_findings(permitted);
        assert!(
            found.is_empty(),
            "a permitted phrasing must pass the claim scan: {permitted:?} gave {found:?}"
        );
    }
    // The permitted passages are removed exactly, not by a shared word: a
    // claim built around one of them is still caught.
    assert!(
        !claim_findings("This page makes no compliance claim, and Logweir is compliant.")
            .is_empty()
    );
}

/// **No supported row says more than its condition** — the review's M1, M2,
/// M3 and overreach findings, as a guard over every row.
#[test]
fn no_supported_statement_says_more_than_its_condition() {
    let page = read(PAGE);
    let problems = overreach_problems(&page);
    assert!(problems.is_empty(), "{PAGE}:\n  {}", problems.join("\n  "));
    assert!(
        supported_rows(&page).len() >= 2 * TEXTS.len(),
        "the guard read the supported rows"
    );

    // Negative controls: the rows the review found, verbatim from the
    // reviewed tip, each refused.
    for (row, expected) in [
        (
            "| Restored records were reconciled byte for byte with the archive, on a sample whose size and result are recorded. | [Scorecard](formats/drill-scorecard.md): `integrity.level`, `integrity.result` |",
            "byte-level comparison",
        ),
        (
            "| A sample of the restored records was compared byte for byte where the level reads `byte-fingerprint`. | [Scorecard](formats/drill-scorecard.md): `integrity.result` |",
            "byte-level comparison",
        ),
        (
            "| The restored sample was compared with the archive, and the test records how much of it read back exactly as archived. | [Scorecard](formats/drill-scorecard.md): `integrity.level` |",
            "byte-level comparison",
        ),
        (
            "| The result of each restore test is documented in a signed record, which also says why a test did not pass. | [Scorecard](formats/drill-scorecard.md): `outcome` |",
            "every test is signed",
        ),
        (
            "| 4.2.6: each recovery test leaves a signed record of its result, and of why it did not pass. | [Scorecard](formats/drill-scorecard.md): `outcome` |",
            "every test is signed",
        ),
        (
            "| Periodic testing: restore tests recur on a schedule, and each one leaves a dated, signed result. | [Scorecard](formats/drill-scorecard.md): `outcome` |",
            "every test is signed",
        ),
        (
            "| Every record of the archive was verified, and the restore is a complete, exact copy. | [Scorecard](formats/drill-scorecard.md): `integrity.result` |",
            "`every record`",
        ),
        (
            "| A rehearsal's plan was proven to fall inside a scope a human signed for that one schedule. | [Scorecard](formats/drill-scorecard.md): `approval.key_id` |",
            "`a human`",
        ),
        (
            "| The target's topic configuration was compared with the configuration the archive recorded. | [Scorecard](formats/drill-scorecard.md): `topic_parity.unexpected_divergence`, `topic_parity.intentionally_deviated` |",
            "configuration was compared without citing the `not_assessed` list",
        ),
    ] {
        let problems = overreach_problems(&with_extra_row(&page, row));
        assert!(
            problems.iter().any(|p| p.contains(expected)),
            "the guard must refuse {row}; it reported {problems:?}"
        );
    }
}

/// **The verification guide and the README link the page** (the row's
/// acceptance names the guide).
#[test]
fn the_verification_guide_and_the_readme_link_the_page() {
    let guide = read("docs/verify-a-scorecard.md");
    let readme = read("README.md");
    assert!(
        links_to(&guide, "control-evidence.md"),
        "docs/verify-a-scorecard.md must link control-evidence.md"
    );
    assert!(
        links_to(&readme, "docs/control-evidence.md"),
        "README.md's documentation table must link docs/control-evidence.md"
    );
    // Negative control: the predicate fails once the link is gone.
    let unlinked = guide.replace("](control-evidence.md", "](verify-a-scorecard.md");
    assert!(
        !links_to(&unlinked, "control-evidence.md"),
        "the link predicate must fail on a guide that no longer links the page"
    );
}

/// The shipped operator pages: README, `docs/` outside the trackers, and the
/// chart and console guides.
fn shipped_pages() -> Vec<(String, String)> {
    let root = repo_root();
    let mut found = Vec::new();
    let mut stack = vec![root.join("docs")];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("a directory entry is readable").path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "to-do") {
                    stack.push(path);
                }
            } else if path.extension().is_some_and(|e| e == "md") {
                found.push(path);
            }
        }
    }
    for extra in ["README.md", "charts/logweir/README.md", "ui/README.md"] {
        found.push(root.join(extra));
    }
    found.sort();
    found
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&root)
                .unwrap_or(&path)
                .display()
                .to_string();
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));
            (relative, text)
        })
        .collect()
}

/// **The class sweep: no shipped operator page uses the unambiguous claim
/// words**, so a compliance claim cannot move to the page next door.
#[test]
fn no_shipped_page_uses_a_compliance_claim_word() {
    let pages = shipped_pages();
    assert!(
        pages.len() >= 20 && pages.iter().any(|(p, _)| p == PAGE),
        "the sweep enumerated {} pages; it must cover docs/ (with {PAGE}) and the guides",
        pages.len()
    );
    let mut offenders = Vec::new();
    for (path, text) in &pages {
        let found = phrases_in(text, &SWEPT);
        if !found.is_empty() {
            offenders.push(format!("{path}: {found:?}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "shipped pages must not claim compliance (PROD-08.4):\n  {}",
        offenders.join("\n  ")
    );

    // Negative control: the same scan over a page that gained the word.
    let (path, text) = &pages[0];
    let mutated = format!("{text}\nThis install is DORA-compliant.\n");
    assert!(
        phrases_in(&mutated, &SWEPT).contains(&"compliant"),
        "the sweep must catch a claim word added to {path}"
    );
}
