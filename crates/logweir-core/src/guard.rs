use std::collections::BTreeMap;

pub const FORBIDDEN_KEYS: [&str; 3] = ["purge_topics", "dry_run", "header_preflight_external"];

#[derive(Debug, thiserror::Error)]
#[error("plan refused by the admission guard: {0}")]
pub struct GuardRefusal(pub String);

/// Walks any YAML/JSON document and returns the dotted path of every forbidden
/// key found, AT ANY VALUE and AT ANY DEPTH. Matching on the key rather than
/// the value is the point: `purge_topics: false` is refused too, because the
/// key's presence means someone believes they are configuring purging.
///
/// IT FAILS CLOSED. A document this function cannot parse is a document it did
/// not scan, and it returns `Err` rather than an empty list — a scanner that
/// answers "I found nothing" over text it never read is indistinguishable from
/// a scanner that read the text and found nothing clean, which is precisely
/// the shape this whole guard exists to prevent. The predecessor returned
/// `vec![]` on a parse error.
///
/// This is a BACKSTOP, and its scope is worth stating exactly, because its
/// name invites over-reading: today it is applied to the adopter's SPEC TEXT
/// (`phase0_admit::run`) and to rendered documents in the renderer's own
/// tests. Global Constraint 4 / GR7 — the three keys never being emitted on
/// any argv or in any rendered YAML — is enforced STRUCTURALLY, at
/// `logweir-engine-oso`'s renderers, which name none of the three keys and
/// escape every interpolated value through `yaml_scalar` so a value cannot
/// forge a key line. That structural property is the guarantee; this function
/// is a second pair of eyes over the input a human wrote.
pub fn scan_forbidden_keys(doc_text: &str) -> Result<Vec<String>, GuardRefusal> {
    let v: serde_yaml::Value = serde_yaml::from_str(doc_text).map_err(|e| {
        GuardRefusal(format!(
            "the forbidden-key scan could not parse this document, so it did not scan it: \
             {e}. A guard that reports `no forbidden keys` over text it never read is \
             worse than no guard."
        ))
    })?;
    let mut found = Vec::new();
    walk(&v, String::new(), &mut found);
    found.sort();
    found.dedup();
    Ok(found)
}

fn walk(v: &serde_yaml::Value, path: String, out: &mut Vec<String>) {
    if let serde_yaml::Value::Mapping(m) = v {
        for (k, val) in m {
            let name = k.as_str().unwrap_or_default().to_string();
            let p = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}.{name}")
            };
            if FORBIDDEN_KEYS.contains(&name.as_str()) {
                out.push(p.clone());
            }
            walk(val, p, out);
        }
    } else if let serde_yaml::Value::Sequence(s) = v {
        for (i, item) in s.iter().enumerate() {
            walk(item, format!("{path}[{i}]"), out);
        }
    }
}

/// The six characters upstream's topic selector treats as GLOB PATTERN
/// SYNTAX rather than as part of a name.
///
/// `TopicSelection.include`/`exclude` "supports glob patterns"
/// [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], and both
/// renderers put operator-chosen topic names straight into
/// `topics.include`. So a topic legitimately NAMED `orders*` or `events[1]`
/// is handed to the engine as a PATTERN: one named entry silently widens to a
/// set, and GC18(c)'s "mandatory named-topic allowlist with no wildcard" rail
/// would be satisfied only by nobody having typed one. `*`/`?` are the
/// classic pair; `[`/`]` open and close a character class; `{`/`}` open and
/// close a brace alternation. All six are refused, and the CLOSING halves are
/// refused as well as the opening ones — a scanner that accepted `a]b`
/// because the class was never opened would be reasoning about the glob
/// dialect's grammar instead of about whether the string is a plain name.
pub const GLOB_METACHARACTERS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

/// GC18(c) rail 1, and guard **G-GLOB**: no topic include entry may contain a
/// glob metacharacter. Shared by `render_backup::render` (over
/// `BackupPlan::topics`) and `render_restore::render` (over BOTH sides of
/// `topic_mapping` — a mapped target name is an include entry too), so the
/// two renderers can never disagree about what counts as a plain name.
///
/// It lives HERE, in the pure core, for the same reason `redact_url` does:
/// two copies of a predicate is how the two copies come to disagree.
///
/// Returns the FIRST offending entry verbatim, not a formatted message: the
/// caller owns the wording (`render_backup::RenderError::GlobMetacharacter`
/// carries the whole explanation), and an `Err(String)` that is already prose
/// cannot be re-wrapped without either nesting two sentences or discarding
/// the entry the operator has to go and fix.
///
/// **It does not, and must not, quote or escape anything.** Quoting is a YAML
/// concern and `logweir_engine_oso::yaml::yaml_scalar` already does it
/// unconditionally; globbing is the ENGINE's concern, one layer further out.
/// A quoted `"orders*"` is still a glob to the engine's selector, which is
/// exactly why this predicate exists beside the escaper rather than inside it.
pub fn reject_glob_metacharacters(entries: &[String]) -> Result<(), String> {
    for entry in entries {
        if entry.chars().any(|c| GLOB_METACHARACTERS.contains(&c)) {
            return Err(entry.clone());
        }
    }
    Ok(())
}

/// Every selected topic must have a mapping entry whose target DIFFERS from
/// its source, or the restore would write over the topic it came from.
pub fn check_topic_mapping_coverage(
    topics: &[String],
    mapping: &BTreeMap<String, String>,
) -> Result<(), GuardRefusal> {
    for t in topics {
        match mapping.get(t) {
            None => {
                return Err(GuardRefusal(format!(
                    "selected topic `{t}` has no topic_mapping entry"
                )))
            }
            Some(dst) if dst == t => {
                return Err(GuardRefusal(format!(
                    "topic_mapping maps `{t}` onto itself; the target must differ from the source"
                )))
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GR7 / Global Constraint 4: the phase-0 guard is the backstop for a
    /// forbidden key that reaches a rendered restore document by any route.
    /// It is exercised against a constructed document, because no Logweir code
    /// path may exist that is capable of emitting the key — global ruling GR7
    /// deleted the `LOGWEIR_TEST_INJECT_DRY_RUN` hook Task 21c's brief
    /// proposed, and Global Constraint 4 admits no test or debug exception.
    ///
    /// The expected value is the DOTTED PATH `target.dry_run`, not the bare
    /// key name the addendum's snippet wrote: `scan_forbidden_keys` reports
    /// where it found the key, which is the only form an operator can act on.
    #[test]
    fn a_dry_run_key_in_a_rendered_restore_document_is_caught_by_the_key_scan() {
        let rendered = "\
target:
  bootstrap_servers: kafka-broker-1:9094
  create_topics: true
  dry_run: true
";
        assert_eq!(
            scan_forbidden_keys(rendered).unwrap(),
            vec!["target.dry_run".to_string()]
        );
    }

    #[test]
    fn a_dry_run_key_at_value_false_is_caught_identically() {
        let rendered = "target:\n  dry_run: false\n";
        assert_eq!(
            scan_forbidden_keys(rendered).unwrap(),
            vec!["target.dry_run".to_string()]
        );
    }

    /// The shared half of **G-GLOB**, pinned at its own home. Both renderers'
    /// arms are pinned separately (`tests/render_backup.rs` and
    /// `tests/render.rs` in `logweir-engine-oso`); this asserts the predicate
    /// itself refuses ALL SIX metacharacters, including the closing halves,
    /// and returns the offending entry rather than a message.
    #[test]
    fn the_glob_predicate_refuses_every_metacharacter_and_names_the_entry() {
        for bad in ["orders*", "orders?", "events[1]", "a]b", "x{1}", "y}z"] {
            assert_eq!(
                reject_glob_metacharacters(&[bad.to_string()]),
                Err(bad.to_string()),
                "`{bad}` must be refused as a glob pattern"
            );
        }
        for good in ["orders", "payments", "orders.v2", "a-b_c"] {
            assert_eq!(
                reject_glob_metacharacters(&[good.to_string()]),
                Ok(()),
                "`{good}` is a plain topic name and must be accepted"
            );
        }
        // The FIRST offender is the one reported, and a clean prefix does not
        // shadow a later offender.
        assert_eq!(
            reject_glob_metacharacters(&["orders".into(), "pay*".into(), "z?".into()]),
            Err("pay*".to_string())
        );
        assert_eq!(reject_glob_metacharacters(&[]), Ok(()));
    }
}
