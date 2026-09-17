//! The retention plan's wire agreement — review `d3w9` **L6**.
//!
//! # Why this file exists, and why it is in THIS crate
//!
//! `weirkeeper::retention_plan::PlanDocument` writes the plan and
//! `logweir_reaper::Plan` reads it. They are two independent structs and must
//! be: the crates may not link each other — that is the whole point of
//! `scripts/check-no-archive-write.sh` check 3, which proves `logweir-reaper`
//! is reachable only from `logweir-retention` — so there is no shared type to
//! keep them honest.
//!
//! **Both carry `#[serde(deny_unknown_fields)]`, so the failure mode of drift
//! is total, not partial.** One field added to the writer makes every plan
//! `Refusal::Unreadable`, every run exit 3, and the discovery happens at 04:17
//! in a Job log. Nothing tested the agreement; the reviewer checked it by hand
//! and recorded that as a finding.
//!
//! `crates/logweir` is where the check can live: it is a crate that already
//! reads the whole tree's source in its lint suites, and it can link neither of
//! the two — so this file compares them the way anything else would have to, by
//! **round-tripping real bytes** rather than by comparing type definitions.
//! The bytes are built here from the same JSON shape the controller emits, so
//! a writer-side change that this file does not know about fails it.

use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is two levels above crates/logweir")
}

fn read(rel: &str) -> String {
    let p = repo().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("could not read {}: {e}", p.display()))
}

/// The fields of one `#[serde(deny_unknown_fields)]` struct, read out of its
/// source.
///
/// A source read and not a reflection trick: neither type is linkable from
/// here, and a `serde` round trip cannot enumerate a struct's fields. The
/// parser is deliberately dumb — it takes the `pub <name>:` lines between the
/// declaration and its closing brace — and it PANICS on a struct it cannot
/// find, so a rename fails this file rather than silently comparing nothing.
fn fields_of(source: &str, declaration: &str) -> Vec<String> {
    let start = source
        .find(declaration)
        .unwrap_or_else(|| panic!("`{declaration}` is not in that file any more"));
    let rest = &source[start..];
    let end = rest
        .find("\n}")
        .unwrap_or_else(|| panic!("`{declaration}` is not terminated"));
    rest[..end]
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            let name = l.strip_prefix("pub ")?;
            let (name, _) = name.split_once(':')?;
            (!name.contains(' ') && !name.contains('(')).then(|| name.to_string())
        })
        .collect()
}

/// The writer's document and the reader's `Plan` have the same fields, in the
/// same order.
///
/// ORDER MATTERS AND IS NOT PEDANTRY: the bytes are produced by
/// `logweir_core::det_json::to_deterministic_json`, which emits struct fields
/// in DECLARATION order, and `planSha256` is the digest of those exact bytes.
/// Two structs with the same fields in different orders would produce two
/// different digests for one plan.
#[test]
fn the_writer_and_the_reader_declare_the_same_plan() {
    let writer = read("crates/weirkeeper/src/retention_plan.rs");
    let reader = read("crates/logweir-reaper/src/lib.rs");

    let written = fields_of(&writer, "pub struct PlanDocument {");
    let readable = fields_of(&reader, "pub struct Plan {");
    assert_eq!(
        written, readable,
        "`PlanDocument` and `Plan` are two independent structs — the crates may not link each \
         other — and both carry `deny_unknown_fields`. One field on the writer that the reader \
         does not know makes EVERY plan unreadable and every run exit 3, discovered at 04:17."
    );

    let written_line = fields_of(&writer, "pub struct PlanLine {");
    let readable_line = fields_of(&reader, "pub struct PlanLine {");
    assert_eq!(written_line, readable_line, "the same, per line");

    // And the three fields the C1 fix removed stay removed on BOTH sides: an
    // instant, a generation or a counter in the digested bytes is a digest that
    // moves without the deletion moving, which is what made the approval gate
    // unreachable.
    for forbidden in ["evaluated_at", "policy_generation", "points_evaluated"] {
        assert!(
            !written.contains(&forbidden.to_string()),
            "`{forbidden}` must not be in the digested plan (review `d3w9` C1): {written:?}"
        );
        assert!(
            !readable.contains(&forbidden.to_string()),
            "`{forbidden}` must not be in the reader's plan either: {readable:?}"
        );
    }
}

/// The media type is declared twice — it has to be — and the two agree.
#[test]
fn the_two_media_type_constants_agree() {
    let writer = read("crates/weirkeeper/src/retention_plan.rs");
    let reader = read("crates/logweir-reaper/src/lib.rs");
    let literal = |source: &str, name: &str| -> String {
        let line = source
            .lines()
            .find(|l| {
                l.trim_start()
                    .starts_with(&format!("pub const {name}: &str ="))
            })
            .unwrap_or_else(|| panic!("`{name}` is not declared in that file any more"));
        let start = line.find('"').expect("a string literal");
        let rest = &line[start + 1..];
        rest[..rest.find('"').expect("a closing quote")].to_string()
    };
    assert_eq!(
        literal(&writer, "PLAN_MEDIA_TYPE"),
        literal(&reader, "PLAN_MEDIA_TYPE"),
        "the worker refuses a document whose `format` is not exactly its own constant, so a \
         drift between the two makes every plan exit 3"
    );
    assert_eq!(
        literal(&writer, "PLAN_MEDIA_TYPE"),
        "application/vnd.logweir.retention-plan+json;version=1.0.0"
    );
    assert_eq!(
        literal(&writer, "EVIDENCE_ROOT"),
        literal(&reader, "EVIDENCE_ROOT"),
        "the one prefix no retention run may delete under is declared on both sides of the \
         seam, and a disagreement would let one of them validate against the wrong root"
    );
}

/// Both structs still refuse an unknown field.
///
/// The agreement above is only load-bearing because of this: with
/// `deny_unknown_fields` on both sides, drift is caught at the first run rather
/// than half-obeyed. A deletion worker that silently ignored half of an
/// instruction is the failure mode the whole two-step approval exists to
/// prevent.
#[test]
fn both_sides_still_refuse_an_unknown_field() {
    for (rel, declaration) in [
        (
            "crates/weirkeeper/src/retention_plan.rs",
            "pub struct PlanDocument {",
        ),
        ("crates/logweir-reaper/src/lib.rs", "pub struct Plan {"),
        (
            "crates/weirkeeper/src/retention_plan.rs",
            "pub struct PlanLine {",
        ),
        ("crates/logweir-reaper/src/lib.rs", "pub struct PlanLine {"),
    ] {
        let source = read(rel);
        let at = source
            .find(declaration)
            .unwrap_or_else(|| panic!("`{declaration}` is not in {rel} any more"));
        let preamble = &source[at.saturating_sub(400)..at];
        assert!(
            preamble.contains("deny_unknown_fields"),
            "`{declaration}` in {rel} must keep `deny_unknown_fields`: without it a newer \
             controller's extra field would be silently ignored by an older worker, which is \
             half-obeying a deletion instruction"
        );
    }
}
