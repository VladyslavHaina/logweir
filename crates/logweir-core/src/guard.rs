use std::collections::BTreeMap;

pub const FORBIDDEN_KEYS: [&str; 3] = ["purge_topics", "dry_run", "header_preflight_external"];

#[derive(Debug, thiserror::Error)]
#[error("plan refused by the admission guard: {0}")]
pub struct GuardRefusal(pub String);

/// Walks any YAML/JSON document and returns the dotted path of every forbidden
/// key found, AT ANY VALUE and AT ANY DEPTH. Matching on the key rather than
/// the value is the point: `purge_topics: false` is refused too, because the
/// key's presence means someone believes they are configuring purging.
pub fn scan_forbidden_keys(doc_text: &str) -> Vec<String> {
    let v: serde_yaml::Value = match serde_yaml::from_str(doc_text) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    let mut found = Vec::new();
    walk(&v, String::new(), &mut found);
    found.sort();
    found.dedup();
    found
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
