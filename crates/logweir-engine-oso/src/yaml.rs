//! The workspace's SINGLE YAML scalar escaper.
//!
//! It was `render_restore::yaml_scalar` — one of the two renderers owning the
//! function the other one imported. That is a private detail turned into a
//! dependency: `render_validation.rs` read `use crate::render_restore::{…,
//! yaml_scalar}`, so the escaper's home was decided by which renderer happened
//! to be written first, and a third renderer (`render_backup`) arriving with
//! its own copy would have been the path of least resistance. Both halves of
//! the hazard below are the same hazard in all three documents, so there is
//! one function and one place to fix it.
//!
//! `tests/render_backup.rs::there_is_exactly_one_yaml_escaper` reads the three
//! renderer sources and asserts none of them defines `fn yaml_scalar`.
//! Task 3 adds `yaml_scalar_checked` HERE, beside this one, and swaps the call
//! sites over.

/// Escapes an arbitrary string for safe embedding as a YAML double-quoted
/// scalar, and is used at EVERY interpolation site in both renderers
/// (`backup_id`, bootstrap servers, both sides of `topic_mapping`, every
/// storage field, `checkpoint_state`, `run_id` and `triggered_by`).
///
/// Global Constraint 4 says the three forbidden keys are never emitted "at
/// any value" — a value that, written raw, would ITSELF contain a physical
/// line whose pre-colon token is one of those keys is still an emission.
/// Concretely: `render_validation::render(&plan, "r", Some("x\ndry_run:
/// true"))` written without this function would produce a physical line
/// `dry_run: true` inside `restore.yaml`/`validation.yaml`, because a raw `\n`
/// in an interpolated `&str` is a real newline byte once it lands in the
/// rendered `String`. A `"` would truncate a naively hand-quoted scalar early
/// (see the old `format!("backup_id: \"{}\"\n\n", …)`, which quoted but never
/// escaped); a `\` would be reinterpreted as the start of a YAML escape by
/// whatever eventually reads the file. This function closes all three: it
/// ALWAYS emits a double-quoted scalar (never a bare/plain one) and escapes
/// backslash, double-quote, and every C0 control character (`\n`, `\r`, `\t`
/// by their short names; everything else below 0x20 as `\xHH`).
///
/// Deliberately NOT an attempt at full YAML plain-scalar-safety detection
/// (leading indicators, ambiguous ": ", reserved words, …) with quoting only
/// where "needed": a scalar that is unconditionally quoted is unconditionally
/// safe, and the resulting document is easier for the human reviewer this
/// task exists for to audit than one where quoting is conditional on the
/// input's shape.
///
/// What this function does NOT and CANNOT defend against: upstream's
/// `expand_env_vars` [U/kafka-backup/crates/kafka-backup-cli/src/commands/
/// config.rs:6-35] scans the RAW file text for a literal `${VAR}` and
/// substitutes it BEFORE the file is parsed as YAML at all — a textual
/// preprocessing pass with no awareness of quoting. A value containing a
/// literal `${...}` is expanded by the engine's own preprocessing regardless
/// of how this function escapes it, and the substituted text (drawn from
/// whichever environment variable is named, in the engine's OWN process
/// environment) lands in the file un-requoted. That hazard sits a layer
/// below YAML syntax entirely, so no YAML-scalar escaper — this one or any
/// other — can close it; it is a distinct problem from the one this function
/// solves.
pub(crate) fn yaml_scalar(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
