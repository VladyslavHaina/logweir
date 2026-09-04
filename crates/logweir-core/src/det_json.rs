use serde::Serialize;

/// Deterministic JSON: two-space pretty print; struct fields in DECLARATION
/// order; Rust `BTreeMap` fields in key order; an embedded `serde_json::Value`
/// map in the order it was READ, because `serde_json/preserve_order` is enabled
/// (Task 1). RFC3339 `Z` timestamps, no NaN/Inf.
/// Mirrors OSO's own `to_deterministic_json` intent so an auditor comparing
/// the two documents sees the same shape (spec §6 C1).
#[derive(Debug, thiserror::Error)]
pub enum DetJsonError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A float that JSON cannot represent. `serde_json` serialises NaN and ±Inf
    /// as a bare `null`, which in a SIGNED document is indistinguishable from
    /// "not measured" — so the boundary refuses it instead of signing a lie.
    #[error("non-finite number at {0}")]
    NonFinite(String),
}

pub fn to_deterministic_json<T: Serialize>(value: &T) -> Result<Vec<u8>, DetJsonError> {
    // The `to_value` round-trip preserves ordering because `serde_json` is built
    // with `preserve_order` (Task 1), so a Map keeps insertion order and struct
    // fields keep declaration order.
    let v = serde_json::to_value(value)?;
    reject_non_finite(&v, "")?;
    let mut buf = Vec::new();
    let fmt = serde_json::ser::PrettyFormatter::with_indent(b"  ");
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, fmt);
    v.serialize(&mut ser)?;
    buf.push(b'\n');
    Ok(buf)
}

/// Walks the tree once. A `Number` that is neither an integer nor a FINITE f64
/// is refused, with the JSON pointer of where it was found.
fn reject_non_finite(v: &serde_json::Value, at: &str) -> Result<(), DetJsonError> {
    match v {
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() || n.as_f64().map(f64::is_finite).unwrap_or(false) {
                Ok(())
            } else {
                Err(DetJsonError::NonFinite(at.to_string()))
            }
        }
        serde_json::Value::Array(a) => a
            .iter()
            .enumerate()
            .try_for_each(|(i, x)| reject_non_finite(x, &format!("{at}/{i}"))),
        serde_json::Value::Object(o) => o
            .iter()
            .try_for_each(|(k, x)| reject_non_finite(x, &format!("{at}/{k}"))),
        _ => Ok(()),
    }
}
