//! FX-33: `ReceiptFacts::fold` reads what the `serde_json::Value` walk read.
//!
//! The controller used to parse a receipt into a `Value` and read five things
//! from it. The fold reads the same five as the bytes stream past. A verdict
//! hangs on each (the signing time decides a key's validity window, the
//! `backup_id` binds a relayed receipt to its run), so the fold is held to
//! the walk it replaces, body by body.

use chrono::{DateTime, Utc};
use logweir_core::receipt_facts::{Field, ReceiptFacts, MAX_RECORDS_ENTRIES};
use logweir_core::trust::{read_claimed_signing_time, ClaimAbsence};
use serde_json::Value;

const RECEIPT_TYPE: &str = "application/vnd.logweir.backup-receipt+json;version=1.0.0";

/// What the `Value` walk answered, VERBATIM in its decisions: the oracle.
/// `None` for bytes that are not JSON.
#[derive(Debug, PartialEq)]
struct Walked {
    backup_id: Option<String>,
    covered: Option<(i64, i64)>,
    records: Option<i64>,
    capture: Option<(DateTime<Utc>, DateTime<Utc>)>,
    signing_time: Result<DateTime<Utc>, ClaimAbsence>,
}

/// `weirkeeper::controllers::backup::{covered_from_receipt,
/// records_from_receipt, capture_from_receipt}`, the relayed receipt's
/// `backup_id` read, and `trust::read_claimed_signing_time`, as they stood.
fn value_walk(bytes: &[u8]) -> Option<Walked> {
    let doc: Value = serde_json::from_slice(bytes).ok()?;
    let covered = (|| {
        let covered = doc.get("covered")?;
        Some((
            covered.get("from_ms")?.as_i64()?,
            covered.get("to_ms")?.as_i64()?,
        ))
    })();
    let records = (|| {
        let records = doc.get("records")?.as_object()?;
        let mut total: i64 = 0;
        for value in records.values() {
            total = total.checked_add(i64::try_from(value.as_u64()?).ok()?)?;
        }
        Some(total)
    })();
    let capture = (|| {
        let at = |key: &str| -> Option<DateTime<Utc>> {
            doc.get(key)?.as_str()?.parse::<DateTime<Utc>>().ok()
        };
        Some((at("started_at")?, at("finished_at")?))
    })();
    Some(Walked {
        backup_id: doc
            .get("backup_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        covered,
        records,
        capture,
        signing_time: read_claimed_signing_time(RECEIPT_TYPE, &doc),
    })
}

fn folded(bytes: &[u8]) -> Option<Walked> {
    let facts = ReceiptFacts::fold(bytes)?;
    Some(Walked {
        backup_id: facts.backup_id.as_str().map(str::to_string),
        covered: facts.covered,
        records: facts.records,
        capture: facts.capture(),
        signing_time: facts.signing_time(),
    })
}

fn nested(depth: usize) -> String {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
}

/// Every body the two readers are compared over. Each names what it is for.
fn corpus() -> Vec<(&'static str, Vec<u8>)> {
    let s = |text: &str| text.as_bytes().to_vec();
    let mut bodies: Vec<(&'static str, Vec<u8>)> = vec![
        (
            "a whole receipt's five fields",
            s(
                r#"{"format_version":"1.6.0","backup_id":"set-a","started_at":"2026-10-09T03:00:00Z","finished_at":"2026-10-09T03:04:00Z","records":{"a":1,"b":41},"covered":{"from_ms":10,"to_ms":20}}"#,
            ),
        ),
        ("an empty object", s("{}")),
        ("an array", s(r#"[{"backup_id":"x"}]"#)),
        ("a string", s(r#""backup_id""#)),
        ("a number", s("7")),
        ("null", s("null")),
        ("true", s("true")),
        ("not JSON", s("{")),
        ("empty bytes", Vec::new()),
        ("trailing bytes", s(r#"{"backup_id":"a"} x"#)),
        ("trailing whitespace only", s("{\"backup_id\":\"a\"}\n  \n")),
        ("two documents", s(r#"{"backup_id":"a"}{"backup_id":"b"}"#)),
        // -- repeated keys: the last wins, at every level
        (
            "backup_id twice",
            s(r#"{"backup_id":"first","backup_id":"last"}"#),
        ),
        (
            "backup_id then a non-string",
            s(r#"{"backup_id":"first","backup_id":7}"#),
        ),
        (
            "covered twice, the second not an object",
            s(r#"{"covered":{"from_ms":1,"to_ms":2},"covered":5}"#),
        ),
        (
            "covered twice, the second whole",
            s(r#"{"covered":5,"covered":{"from_ms":1,"to_ms":2}}"#),
        ),
        (
            "from_ms twice inside covered, the second a string",
            s(r#"{"covered":{"from_ms":1,"to_ms":2,"from_ms":"x"}}"#),
        ),
        (
            "from_ms twice inside covered, the second a number",
            s(r#"{"covered":{"from_ms":"x","to_ms":2,"from_ms":9}}"#),
        ),
        (
            "records twice, the last counts",
            s(r#"{"records":{"a":1},"records":{"a":5,"b":6}}"#),
        ),
        (
            "a topic twice inside records REPLACES, never adds",
            s(r#"{"records":{"a":1,"b":2,"a":10}}"#),
        ),
        (
            "a topic twice, the later not a count",
            s(r#"{"records":{"a":1,"a":"x"}}"#),
        ),
        (
            "a topic twice, the earlier not a count",
            s(r#"{"records":{"a":"x","a":3}}"#),
        ),
        // -- covered's numbers, as `as_i64` reads them
        ("covered half", s(r#"{"covered":{"from_ms":1}}"#)),
        (
            "covered a float",
            s(r#"{"covered":{"from_ms":1.0,"to_ms":2}}"#),
        ),
        (
            "covered negative",
            s(r#"{"covered":{"from_ms":-5,"to_ms":-1}}"#),
        ),
        (
            "covered at i64's ends",
            s(r#"{"covered":{"from_ms":-9223372036854775808,"to_ms":9223372036854775807}}"#),
        ),
        (
            "covered one past i64",
            s(r#"{"covered":{"from_ms":0,"to_ms":9223372036854775808}}"#),
        ),
        ("covered an array", s(r#"{"covered":[1,2]}"#)),
        ("covered null", s(r#"{"covered":null}"#)),
        (
            "covered with an exponent",
            s(r#"{"covered":{"from_ms":1e3,"to_ms":2}}"#),
        ),
        // -- records' numbers, as `as_u64` then `i64::try_from` reads them
        ("records empty", s(r#"{"records":{}}"#)),
        ("records an array", s(r#"{"records":[1,2]}"#)),
        ("records a negative count", s(r#"{"records":{"a":-1}}"#)),
        ("records a float", s(r#"{"records":{"a":1.5}}"#)),
        ("records a string", s(r#"{"records":{"a":"1"}}"#)),
        ("records a null", s(r#"{"records":{"a":null}}"#)),
        (
            "records one count past i64",
            s(r#"{"records":{"a":9223372036854775808}}"#),
        ),
        (
            "records summing past i64",
            s(r#"{"records":{"a":9223372036854775807,"b":1}}"#),
        ),
        (
            "records summing to i64's end",
            s(r#"{"records":{"a":9223372036854775806,"b":1}}"#),
        ),
        (
            "records at u64's end",
            s(r#"{"records":{"a":18446744073709551615}}"#),
        ),
        (
            "records with a nested object as a count",
            s(r#"{"records":{"a":{"b":1}}}"#),
        ),
        // -- the two instants
        (
            "finished_at not a string",
            s(r#"{"started_at":"2026-10-09T03:00:00Z","finished_at":7}"#),
        ),
        (
            "finished_at not an instant",
            s(r#"{"started_at":"2026-10-09T03:00:00Z","finished_at":"yesterday"}"#),
        ),
        (
            "finished_at with an offset",
            s(
                r#"{"started_at":"2026-10-09T03:00:00+02:00","finished_at":"2026-10-09T05:04:00.5+02:00"}"#,
            ),
        ),
        (
            "finished_at with a space, which FromStr reads and RFC 3339 does not",
            s(r#"{"started_at":"2026-10-09 03:00:00Z","finished_at":"2026-10-09 03:04:00Z"}"#),
        ),
        (
            "started_at absent",
            s(r#"{"finished_at":"2026-10-09T03:04:00Z"}"#),
        ),
        ("finished_at null", s(r#"{"finished_at":null}"#)),
        // -- keys are compared after their escapes are read
        (
            "an escaped key is the key",
            s(r#"{"backup_id":"escaped","finished_at":"2026-10-09T03:04:00Z"}"#),
        ),
        ("an escaped string value", s(r#"{"backup_id":"set-a\n"}"#)),
        // -- what `IgnoredAny` would have let through in a value nobody reads
        (
            "a lone surrogate in an ignored string",
            s(r#"{"backup_id":"a","triggered_by":"\ud800"}"#),
        ),
        (
            "a lone surrogate in an ignored key",
            s(r#"{"backup_id":"a","\ud800":1}"#),
        ),
        (
            "a number out of range in an ignored value",
            s(r#"{"backup_id":"a","exit_code":1e999}"#),
        ),
        (
            "a bad escape in an ignored value",
            s(r#"{"backup_id":"a","x":"\q"}"#),
        ),
        (
            "a control character in an ignored string",
            b"{\"backup_id\":\"a\",\"x\":\"\x01\"}".to_vec(),
        ),
        (
            "a duplicate in an ignored object",
            s(r#"{"backup_id":"a","source":{"topics":["t"],"topics":7}}"#),
        ),
    ];
    bodies.push((
        "invalid UTF-8 in an ignored string",
        b"{\"backup_id\":\"a\",\"triggered_by\":\"\xff\xfe\"}".to_vec(),
    ));
    bodies.push((
        "invalid UTF-8 in a field the fold reads",
        b"{\"backup_id\":\"\xff\"}".to_vec(),
    ));
    bodies.push((
        "nesting inside the parser's depth, in an ignored value",
        format!(r#"{{"backup_id":"a","x":{}}}"#, nested(100)).into_bytes(),
    ));
    bodies.push((
        "nesting past the parser's depth, in an ignored value",
        format!(r#"{{"backup_id":"a","x":{}}}"#, nested(200)).into_bytes(),
    ));
    bodies.push((
        "nesting past the parser's depth, inside records",
        format!(r#"{{"records":{{"a":{}}}}}"#, nested(200)).into_bytes(),
    ));
    bodies
}

/// **The fold equals the `Value` walk**, over every body: the same five
/// answers, and "not JSON" for exactly the same bytes.
///
/// KILLS: skipping with `IgnoredAny` (the surrogate, UTF-8, range and depth
/// bodies), the first occurrence of a repeated key winning, a repeated topic
/// adding to the sum, `covered` read half, a count read as `i64` and not
/// `u64`, trailing bytes accepted.
#[test]
fn the_fold_reads_what_the_value_walk_read() {
    let bodies = corpus();
    assert!(bodies.len() >= 55, "the corpus shrank: {}", bodies.len());
    let mut refused = 0;
    for (name, bytes) in &bodies {
        let walked = value_walk(bytes);
        assert_eq!(
            folded(bytes),
            walked,
            "{name}: the fold and the Value walk disagree over {:?}",
            String::from_utf8_lossy(bytes)
        );
        refused += usize::from(walked.is_none());
    }
    // NEGATIVE CONTROLS on the corpus itself: it holds bodies both readers
    // refuse, and bodies in which each fact is read.
    assert!(
        refused >= 10,
        "only {refused} bodies are refused as not JSON"
    );
    let first = value_walk(&bodies[0].1).expect("the first body is a receipt");
    assert_eq!(first.backup_id.as_deref(), Some("set-a"));
    assert_eq!(first.covered, Some((10, 20)));
    assert_eq!(first.records, Some(42));
    assert!(first.capture.is_some() && first.signing_time.is_ok());
    let replaced = value_walk(br#"{"records":{"a":1,"b":2,"a":10}}"#).unwrap();
    assert_eq!(replaced.records, Some(12), "the oracle itself: last wins");
}

/// The three states of a field, and what each means to the signing time: no
/// key is `FieldAbsent`, a value that is not an instant is `Unparseable`.
#[test]
fn a_field_is_absent_text_or_something_else() {
    let facts = ReceiptFacts::fold(br#"{"backup_id":7,"finished_at":"2026-10-09T03:04:00Z"}"#)
        .expect("JSON");
    assert_eq!(facts.backup_id, Field::Other);
    assert_eq!(facts.started_at, Field::Absent);
    assert_eq!(
        facts.finished_at,
        Field::Text("2026-10-09T03:04:00Z".to_string())
    );
    assert_eq!(
        facts.signing_time(),
        Ok("2026-10-09T03:04:00Z".parse().unwrap())
    );
    assert_eq!(
        ReceiptFacts::fold(b"{}").unwrap().signing_time(),
        Err(ClaimAbsence::FieldAbsent)
    );
    assert_eq!(
        ReceiptFacts::fold(br#"{"finished_at":[]}"#)
            .unwrap()
            .signing_time(),
        Err(ClaimAbsence::Unparseable)
    );
}

fn records_of(entries: usize) -> Vec<u8> {
    let mut body = String::from(r#"{"backup_id":"set-a","records":{"#);
    for i in 0..entries {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(r#""t{i}":1"#));
    }
    body.push_str("}}");
    body.into_bytes()
}

/// **The one stated difference from the `Value` walk**: a `records` object
/// naming more distinct topics than the fold holds keys for yields NO sum,
/// never a wrong one; at the bound it still sums, and the other facts are
/// read either way.
#[test]
fn records_over_the_entry_bound_yield_no_sum_and_nothing_else_is_lost() {
    let at = ReceiptFacts::fold(&records_of(MAX_RECORDS_ENTRIES)).expect("JSON");
    assert_eq!(
        at.records,
        Some(i64::try_from(MAX_RECORDS_ENTRIES).unwrap()),
        "NEGATIVE CONTROL: at the bound the sum is read"
    );
    let over = ReceiptFacts::fold(&records_of(MAX_RECORDS_ENTRIES + 1)).expect("JSON");
    assert_eq!(over.records, None, "one topic past the bound: no sum");
    assert_eq!(over.backup_id.as_str(), Some("set-a"));
    assert_eq!(
        value_walk(&records_of(MAX_RECORDS_ENTRIES + 1))
            .unwrap()
            .records,
        Some(i64::try_from(MAX_RECORDS_ENTRIES).unwrap() + 1),
        "the walk this replaces summed it: the difference is stated, and it is the safer side"
    );
    // A topic REPEATED past the bound is not a new topic: it replaces.
    let mut repeated = String::from(r#"{"records":{"#);
    for i in 0..MAX_RECORDS_ENTRIES {
        repeated.push_str(&format!(r#""t{i}":1,"#));
    }
    repeated.push_str(r#""t0":5}}"#);
    assert_eq!(
        ReceiptFacts::fold(repeated.as_bytes()).unwrap().records,
        Some(i64::try_from(MAX_RECORDS_ENTRIES).unwrap() + 4)
    );
    // And a body still invalid AFTER the bound is still not JSON.
    let mut broken = records_of(MAX_RECORDS_ENTRIES + 5);
    broken.truncate(broken.len() - 1);
    assert_eq!(ReceiptFacts::fold(&broken), None);
}
