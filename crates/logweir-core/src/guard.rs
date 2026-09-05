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
}
