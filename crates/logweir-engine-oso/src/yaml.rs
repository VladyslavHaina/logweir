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
//! renderer sources and asserts none of them defines `fn yaml_scalar(`.
//! Task 3 added `yaml_scalar_checked` HERE, beside this one, and swapped every
//! call site over; `tests/expansion.rs::every_renderer_calls_the_checked_
//! escaper` reads the same three sources and asserts NO call site calls the
//! unchecked `yaml_scalar(` any more. The unchecked function is still the one
//! escaper — `yaml_scalar_checked` is a pre-check in front of it, not a second
//! escaper — which is why the "exactly one" count is over `fn yaml_scalar(`
//! and not over the name's every prefix.

use crate::render_backup::RenderError;

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

/// The ONE `${…}` sequence a rendered document may carry for the SOURCE
/// cluster's SASL password, emitted as a RAW LITERAL and never through the
/// escaper (`yaml_scalar_checked` refuses `${` in any input by construction).
///
/// This is the deliberate exception to G-EXP, and the reason it is an
/// exception: the password must NOT be in the bytes Logweir hashes. It reaches
/// the engine through the engine's own textual expansion of its own process
/// environment, so the rendered document names the variable and the projected
/// Secret supplies the value — which is also why the value itself has to be
/// safe to substitute into pre-parse text, and why
/// `logweir_core::guard::credential_is_renderable` exists (interface **I11**).
/// The SASL block that emits it is Task 6's (interface **I1**); this task
/// lands the name and the sweep that permits exactly these two.
pub const PLACEHOLDER_SOURCE_PASSWORD: &str = "${LOGWEIR_SOURCE_PASSWORD}";
/// The target cluster's twin of `PLACEHOLDER_SOURCE_PASSWORD`.
pub const PLACEHOLDER_TARGET_PASSWORD: &str = "${LOGWEIR_TARGET_PASSWORD}";

/// Guard **G-EXP**, first leg: `yaml_scalar` plus a pre-check that REFUSES any
/// input containing `${`.
///
/// The hazard is the one `yaml_scalar`'s own doc comment names and then says
/// it cannot close. Upstream runs `expand_env_vars` over the WHOLE config file
/// as raw text before it is parsed as YAML at all
/// [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], wired into every
/// command Logweir drives (`backup.rs:11`, `restore.rs:12`,
/// `validate_restore.rs:8`), and an UNSET variable is replaced with the EMPTY
/// STRING behind nothing but a `tracing::warn!`. Two consequences, and only
/// the second one needs an attacker:
///
/// 1. A correctness hazard with no attacker at all: a topic legitimately named
///    `orders${X}` becomes `orders`, silently, and the drill restores a
///    different topic than the approved plan named.
/// 2. The document Logweir hashes is a TEMPLATE; the document the engine
///    executes is its expansion. `plan_hash`
///    (`crates/logweir/src/drill/phase1_approval.rs:87`) therefore covers
///    bytes the engine never executes, and anything able to set an environment
///    variable on the runner pod can change topics, prefixes, bootstrap
///    servers or the storage prefix AFTER the hash check passed.
///
/// Escaping cannot help, because the expansion happens a layer below YAML
/// syntax: `"orders${X}"` is expanded inside its own quotes. So the only
/// closure is a REFUSAL, and it lives here — beside the escaper, at every
/// interpolation site — rather than as a post-render scan alone, so the
/// refusal names the input value the operator has to go and fix.
pub fn yaml_scalar_checked(value: &str) -> Result<String, RenderError> {
    if value.contains("${") {
        return Err(RenderError::DollarBrace(value.to_string()));
    }
    Ok(yaml_scalar(value))
}

/// Guard **G-EXP**, second leg: the post-render sweep. The only `${…}`
/// sequences permitted ANYWHERE in a rendered document are
/// `PLACEHOLDER_SOURCE_PASSWORD` and `PLACEHOLDER_TARGET_PASSWORD`.
///
/// Called by all three renderers' `render_and_digest`, BEFORE the digest is
/// taken, so a digest is only ever computed over a document that passed. A
/// sweep after the hash would publish the hash of a template and then refuse —
/// the digest would already be in a caller's hand.
///
/// # Why a second leg at all, when every interpolation site is checked
///
/// `yaml_scalar_checked` guards the VALUES. This guards the DOCUMENT, which is
/// the thing the engine expands: a `${` that arrives from a literal in a
/// renderer's own format string, from a field rendered without the escaper (an
/// integer today, a string after somebody's refactor), or from a future
/// storage arm, is invisible to the per-value check and visible here. Two legs
/// because the two failure modes are independent, not because one is
/// insufficient.
///
/// The scan reads each `${` to its matching `}`, or to the end of the physical
/// line when there is none — an unterminated `${` is reported as what it is
/// rather than swallowing the rest of the file, and it is refused just the
/// same, because upstream's own scanner is the authority on how far it reads
/// and this one may not claim to know.
pub fn assert_no_unnamed_dollar_brace(doc: &str) -> Result<(), RenderError> {
    let bytes = doc.as_bytes();
    let mut i = 0usize;
    // `$` (0x24) and `{` (0x7b) are ASCII, and no UTF-8 continuation byte is
    // below 0x80, so a byte-wise search cannot land inside a multi-byte
    // character and every index below is a char boundary.
    while i + 1 < bytes.len() {
        if bytes[i] != b'$' || bytes[i + 1] != b'{' {
            i += 1;
            continue;
        }
        let mut j = i + 2;
        let mut closed = None;
        while j < bytes.len() {
            match bytes[j] {
                b'}' => {
                    closed = Some(j + 1);
                    break;
                }
                b'\n' => break,
                _ => j += 1,
            }
        }
        let stop = closed.unwrap_or(j);
        let found = &doc[i..stop];
        if found != PLACEHOLDER_SOURCE_PASSWORD && found != PLACEHOLDER_TARGET_PASSWORD {
            return Err(RenderError::UnnamedPlaceholder(found.to_string()));
        }
        i = stop;
    }
    Ok(())
}

/// **G-EXP's ordering half.** Applies `yaml_scalar_checked`'s pre-check to a
/// list of entries EARLY, discarding the escaped output, so a value carrying
/// `${` is refused as an EXPANSION and not as something else.
///
/// # Why this exists rather than "the per-value check will catch it anyway"
///
/// It will, but with the wrong reason. `${` contains `{` and `}`, which are
/// two of `logweir_core::guard::GLOB_METACHARACTERS`, and both renderers run
/// **G-GLOB** over their topic lists FIRST — before a single byte is pushed,
/// deliberately (see `render_backup::render`). So `orders${X}` was reported as
/// a glob metacharacter, which sends an operator to the wrong fix: they did
/// not write a pattern, they wrote an interpolation, and the answer is not to
/// escape a brace but to stop the engine expanding the name out from under the
/// plan hash. G-EXP is therefore checked ahead of G-GLOB wherever both apply.
///
/// The escaped string is thrown away on purpose: this must be the SAME
/// predicate as the interpolation sites', not a second copy of
/// `contains("${")` that can drift from it, so it calls the one function and
/// keeps only its verdict.
pub(crate) fn reject_dollar_brace(entries: &[String]) -> Result<(), RenderError> {
    for entry in entries {
        yaml_scalar_checked(entry)?;
    }
    Ok(())
}
