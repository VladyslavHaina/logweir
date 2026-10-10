//! **PROD-15.1: the one line a runner prints about a stopped creation step**,
//! `target-topics-appeared=<json>`, written by the runner
//! (`logweir::drill::phase0_admit::CreationStop`) and read by the controller
//! (`weirkeeper::controllers::restore`). ONE definition for both sides, pure,
//! so the writer and the reader cannot disagree about the keys, the bounds or
//! what a name is.
//!
//! # The three lists
//!
//! | list | what the run knows | what the surfaces say |
//! |---|---|---|
//! | `appeared` | a mapped target name someone ELSE created after phase 0 proved it absent: the pre-create listing showed it, or `CreateTopics` answered `TOPIC_ALREADY_EXISTS` | created by someone else; the restore wrote nothing into it |
//! | `left` | a topic THIS execution created: its own `CreateTopics` call answered `Ok` for that name, exactly once | [`crate::guard::LEFT_TOPIC_SENTENCE`] |
//! | `unconfirmed` | a name this execution ASKED for and got NO DEFINITE ANSWER about (the whole call failed, the name got no answer or two, or an error other than "already exists"), which the cluster LISTED when the run looked again | [`crate::guard::UNCONFIRMED_TOPIC_SENTENCE`], or, when the run could not list the cluster, [`crate::guard::UNCONFIRMED_UNLISTED_TOPIC_SENTENCE`] |
//!
//! A name is in at most one list. `left` claims ownership and `appeared`
//! says someone else made the topic, so a name the run cannot account for is
//! never put in either.
//!
//! # Bounds
//!
//! Each list carries at most [`MAX_NAMES`] names a broker accepts, and a
//! COUNT of how many the run named, so a reader can say "and N more" when
//! the bound cut the list. The whole value is at most [`MAX_VALUE_BYTES`];
//! a reader refuses a longer one BEFORE parsing it.

use serde_json::{json, Value};

/// The stdout key of the stopped creation step's one structured line.
pub const LINE_PREFIX: &str = "target-topics-appeared=";

/// The most names one list carries, on the line and on every surface that
/// copies it (`Restore.status.targetTopicsAppeared`, the product API's view).
pub const MAX_NAMES: usize = 100;

/// The most a count may claim. A restore maps far fewer topics; the bound
/// keeps a hostile line from putting an arbitrary integer on an object.
pub const MAX_COUNT: usize = 1_000_000;

/// The longest value a genuine line carries: three lists of [`MAX_NAMES`]
/// names of [`crate::guard::MAX_TOPIC_NAME_CHARS`] bytes, each quoted and
/// comma-separated, plus the keys, the three counts and the flag. A reader
/// refuses a longer value before parsing it (review 2, L7): the pod-log read
/// that hands it over is not bounded by line length.
pub const MAX_VALUE_BYTES: usize = 3 * MAX_NAMES * (crate::guard::MAX_TOPIC_NAME_CHARS + 3) + 512;

/// One list of the line: at most [`MAX_NAMES`] names, and how many the run
/// named in all.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NameList {
    /// The first [`MAX_NAMES`] names, each one a broker accepts.
    pub names: Vec<String>,
    /// How many names the run named: at least `names.len()`.
    pub count: usize,
}

impl NameList {
    /// The bounded list of `all`: its first [`MAX_NAMES`] Kafka-legal names,
    /// and `all.len()` as the count.
    #[must_use]
    pub fn of(all: &[String]) -> Self {
        NameList {
            names: all
                .iter()
                .filter(|n| crate::guard::topic_name_is_kafka_legal(n))
                .take(MAX_NAMES)
                .cloned()
                .collect(),
            count: all.len().min(MAX_COUNT),
        }
    }

    /// How many names the bound cut: `count - names.len()`.
    #[must_use]
    pub fn more(&self) -> usize {
        self.count.saturating_sub(self.names.len())
    }

    /// Whether the run named nothing here.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty() && self.count == 0
    }

    /// Only the names `keep` admits, with the count held to `at_most` (the
    /// number of names that COULD be here) and never below what is kept. A
    /// controller uses it to hold a list to its own Restore's mapped targets.
    #[must_use]
    pub fn held_to(&self, keep: impl Fn(&str) -> bool, at_most: usize) -> Self {
        let names: Vec<String> = self.names.iter().filter(|n| keep(n)).cloned().collect();
        let dropped = self.names.len() - names.len();
        let count = self
            .count
            .saturating_sub(dropped)
            .min(at_most)
            .max(names.len());
        NameList { names, count }
    }

    fn parse(doc: &serde_json::Map<String, Value>, key: &str) -> Self {
        let names: Vec<String> = doc
            .get(key)
            .and_then(Value::as_array)
            .map(|names| {
                names
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|n| crate::guard::topic_name_is_kafka_legal(n))
                    .take(MAX_NAMES)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        // A count is believed only as an integer no smaller than the list
        // beside it and no larger than the bound; anything else is the
        // list's own length. An older runner writes none.
        let count = doc
            .get(&format!("{key}Count"))
            .and_then(Value::as_u64)
            .and_then(|c| usize::try_from(c).ok())
            .filter(|c| *c >= names.len() && *c <= MAX_COUNT)
            .unwrap_or(names.len());
        NameList { names, count }
    }
}

/// The three lists of a stopped creation step (the module doc says what each
/// one claims).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreationStopLists {
    /// Mapped target names someone else created after phase 0.
    pub appeared: NameList,
    /// Topics this execution created and LEFT, empty.
    pub left: NameList,
    /// Names this execution asked for and cannot account for.
    pub unconfirmed: NameList,
    /// Whether the run LISTED the cluster after the stop and saw every
    /// `unconfirmed` name. `false`: it could not list the cluster, so each
    /// name MAY exist. Meaningful only when `unconfirmed` is not empty.
    pub unconfirmed_seen: bool,
}

impl CreationStopLists {
    /// Whether the run named nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.appeared.is_empty() && self.left.is_empty() && self.unconfirmed.is_empty()
    }

    /// The line's value. `appeared` and `left` come first, as every runner
    /// since the first wrote them, so a controller that predates the third
    /// list and the counts reads what it always read and ignores the rest.
    /// `unconfirmedSeen` is written only beside an `unconfirmed` name.
    #[must_use]
    pub fn line_value(&self) -> String {
        let mut doc = serde_json::Map::new();
        doc.insert("appeared".into(), json!(self.appeared.names));
        doc.insert("left".into(), json!(self.left.names));
        doc.insert("unconfirmed".into(), json!(self.unconfirmed.names));
        doc.insert("appearedCount".into(), json!(self.appeared.count));
        doc.insert("leftCount".into(), json!(self.left.count));
        doc.insert("unconfirmedCount".into(), json!(self.unconfirmed.count));
        if !self.unconfirmed.is_empty() {
            doc.insert("unconfirmedSeen".into(), json!(self.unconfirmed_seen));
        }
        Value::Object(doc).to_string()
    }

    /// The whole line: [`LINE_PREFIX`] and [`Self::line_value`].
    #[must_use]
    pub fn line(&self) -> String {
        format!("{LINE_PREFIX}{}", self.line_value())
    }

    /// What a line's VALUE says, FILTERED: `None` for a value longer than
    /// [`MAX_VALUE_BYTES`] (refused before it is parsed) or one that is not a
    /// JSON object; otherwise only the three lists, only names a broker
    /// accepts, at most [`MAX_NAMES`] each, with counts as [`NameList`]
    /// believes them. Every other key is dropped, so a noisy or hostile line
    /// cannot put an arbitrary string on an object.
    ///
    /// `unconfirmedSeen` is read as `true` only when the line says exactly
    /// `true`: anything else, an absent key included, is "could not look",
    /// the weaker claim.
    #[must_use]
    pub fn parse_value(value: &str) -> Option<Self> {
        if value.len() > MAX_VALUE_BYTES {
            return None;
        }
        let doc: Value = serde_json::from_str(value).ok()?;
        let doc = doc.as_object()?;
        Some(CreationStopLists {
            appeared: NameList::parse(doc, "appeared"),
            left: NameList::parse(doc, "left"),
            unconfirmed: NameList::parse(doc, "unconfirmed"),
            unconfirmed_seen: doc.get("unconfirmedSeen").and_then(Value::as_bool) == Some(true),
        })
    }

    /// The sentence for this stop's `unconfirmed` names: "exists now" when
    /// the run saw them, "may exist now" when it could not look.
    #[must_use]
    pub fn unconfirmed_sentence(&self) -> &'static str {
        if self.unconfirmed_seen {
            crate::guard::UNCONFIRMED_TOPIC_SENTENCE
        } else {
            crate::guard::UNCONFIRMED_UNLISTED_TOPIC_SENTENCE
        }
    }
}

/// ` and N more`, or nothing: what follows a list the bound cut.
#[must_use]
pub fn and_more(more: usize) -> String {
    if more == 0 {
        String::new()
    } else {
        format!(" and {more} more")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("t{i:03}")).collect()
    }

    /// The writer's line is what the reader reads: the three lists, their
    /// counts and the flag survive the round trip, and the first two keys are
    /// the ones every earlier runner wrote. KILLS: a writer and a reader that
    /// disagree about a key; a reader that drops the third list or a count.
    #[test]
    fn the_line_round_trips_the_three_lists_their_counts_and_the_flag() {
        let lists = CreationStopLists {
            appeared: NameList::of(&["payments".into()]),
            left: NameList::of(&["orders".into()]),
            unconfirmed: NameList::of(&["ledger".into(), "audit".into()]),
            unconfirmed_seen: true,
        };
        let value = lists.line_value();
        assert_eq!(
            value,
            r#"{"appeared":["payments"],"left":["orders"],"unconfirmed":["ledger","audit"],"appearedCount":1,"leftCount":1,"unconfirmedCount":2,"unconfirmedSeen":true}"#
        );
        assert_eq!(lists.line(), format!("target-topics-appeared={value}"));
        assert_eq!(CreationStopLists::parse_value(&value), Some(lists.clone()));
        assert_eq!(
            lists.unconfirmed_sentence(),
            crate::guard::UNCONFIRMED_TOPIC_SENTENCE
        );

        // Nothing unconfirmed: no flag is written, and none is read.
        let plain = CreationStopLists {
            appeared: NameList::of(&["payments".into()]),
            ..CreationStopLists::default()
        };
        assert_eq!(
            plain.line_value(),
            r#"{"appeared":["payments"],"left":[],"unconfirmed":[],"appearedCount":1,"leftCount":0,"unconfirmedCount":0}"#
        );
        assert_eq!(
            CreationStopLists::parse_value(&plain.line_value()),
            Some(plain)
        );

        // The run could not look: the flag is `false`, and the sentence is
        // the weaker one.
        let unlisted = CreationStopLists {
            unconfirmed: NameList::of(&["ledger".into()]),
            unconfirmed_seen: false,
            ..CreationStopLists::default()
        };
        assert!(unlisted
            .line_value()
            .ends_with(r#""unconfirmedSeen":false}"#));
        let read = CreationStopLists::parse_value(&unlisted.line_value()).unwrap();
        assert!(!read.unconfirmed_seen);
        assert_eq!(
            read.unconfirmed_sentence(),
            crate::guard::UNCONFIRMED_UNLISTED_TOPIC_SENTENCE
        );
        assert!(CreationStopLists::default().is_empty());
        assert!(!unlisted.is_empty());
    }

    /// **The count bound, at the writer and at the reader** (review 2, L3
    /// and M2 (c)): a list of 150 names is cut to 100 and says 50 more; a
    /// line carrying 5,000 names is read as 100. KILLS: an unbounded list (a
    /// status patch over the CRD's `maxItems` would never land); a cut that
    /// says nothing.
    #[test]
    fn a_list_is_cut_at_the_bound_and_says_how_many_more() {
        let list = NameList::of(&names(150));
        assert_eq!(list.names.len(), MAX_NAMES);
        assert_eq!(list.count, 150);
        assert_eq!(list.more(), 50);
        assert_eq!(and_more(list.more()), " and 50 more");
        assert_eq!(and_more(0), "");
        let lists = CreationStopLists {
            left: list,
            ..CreationStopLists::default()
        };
        let read = CreationStopLists::parse_value(&lists.line_value()).unwrap();
        assert_eq!(read.left.names.len(), MAX_NAMES);
        assert_eq!(read.left.more(), 50);

        // A line that carries more than the bound is cut by the READER too.
        let many =
            json!({"appeared": names(1000), "left": names(1000), "unconfirmed": names(1000)})
                .to_string();
        assert!(many.len() <= MAX_VALUE_BYTES);
        let read = CreationStopLists::parse_value(&many).unwrap();
        for list in [&read.appeared, &read.left, &read.unconfirmed] {
            assert_eq!(list.names.len(), MAX_NAMES);
            // No count on the line: the list's own length, never a guess.
            assert_eq!(list.count, MAX_NAMES);
        }
        // The review's 5,000 names per list are longer than any genuine
        // line, and are not read at all.
        let huge = json!({"appeared": names(5000), "left": names(5000)}).to_string();
        assert!(huge.len() > MAX_VALUE_BYTES);
        assert_eq!(CreationStopLists::parse_value(&huge), None);
    }

    /// What the reader believes of a hostile or older line. KILLS: a name a
    /// broker would not accept reaching an object (`.` and `..` included,
    /// review 2 L12); a key other than the three lists; a count smaller than
    /// its list, absurd, negative or not a number; `unconfirmedSeen` read as
    /// seen from anything but `true`.
    #[test]
    fn the_reader_keeps_only_legal_names_the_three_lists_and_sane_counts() {
        let long = "a".repeat(250);
        let ok249 = "b".repeat(249);
        let value = json!({
            "appeared": ["<script>alert(1)</script>", "a b", "x\ty", "caf\u{e9}", long, 7, null,
                         ["n"], {"k": "v"}, "", ok249.clone()],
            "left": ["`rm -rf`", "ok.name_1-2", "..", "."],
            "unconfirmed": ["line\nbreak", "ledger"],
            "removed": ["orders"],
            "note": "<img src=x onerror=alert(1)>",
            "leftInstruction": "delete everything",
            "appearedCount": 0,
            "leftCount": 2_000_000,
            "unconfirmedCount": "many",
            "unconfirmedSeen": "true",
        })
        .to_string();
        let read = CreationStopLists::parse_value(&value).expect("an object");
        assert_eq!(read.appeared.names, vec![ok249]);
        assert_eq!(
            read.appeared.count, 1,
            "a count below its list is the list's length"
        );
        assert_eq!(read.left.names, vec!["ok.name_1-2".to_string()]);
        assert_eq!(
            read.left.count, 1,
            "a count over the bound is the list's length"
        );
        assert_eq!(read.unconfirmed.names, vec!["ledger".to_string()]);
        assert_eq!(read.unconfirmed.count, 1);
        assert!(!read.unconfirmed_seen, "only `true` is seen");

        // THE WRITER filters the same way (mutant R2-06): a name a broker
        // would not accept never reaches the line, and the count still says
        // how many names the run had.
        let written = NameList::of(&[
            "orders".into(),
            "pay ments".into(),
            "..".into(),
            "x\ny".into(),
        ]);
        assert_eq!(written.names, vec!["orders".to_string()]);
        assert_eq!(written.count, 4);
        let line = CreationStopLists {
            left: written,
            ..CreationStopLists::default()
        }
        .line_value();
        assert!(
            line.contains(r#""left":["orders"]"#) && !line.contains("pay ments"),
            "{line}"
        );

        // An older runner's line: two lists, no counts, no third list.
        let older = CreationStopLists::parse_value(r#"{"appeared":[],"left":["orders"]}"#).unwrap();
        assert_eq!(older.left, NameList::of(&["orders".into()]));
        assert!(older.unconfirmed.is_empty());
        // A sane count is believed.
        let counted =
            CreationStopLists::parse_value(r#"{"left":["orders"],"leftCount":150}"#).unwrap();
        assert_eq!(counted.left.more(), 149);
        // Not an object, or a wrong shape inside one.
        for not_an_object in ["[\"orders\"]", "\"orders\"", "7", "null", "{", "orders", ""] {
            assert_eq!(
                CreationStopLists::parse_value(not_an_object),
                None,
                "{not_an_object}"
            );
        }
        let odd =
            CreationStopLists::parse_value(r#"{"appeared":"orders","left":{"a":1}}"#).unwrap();
        assert!(odd.is_empty());
    }

    /// **An over-long value is refused BEFORE it is parsed** (review 2, L7):
    /// the pod-log read is not bounded by line length, and parsing a
    /// megabyte line into a JSON tree to keep 100 names of it is work a
    /// hostile log should not be able to ask for. The bound admits the
    /// largest genuine line. KILLS: parsing first and cutting after.
    #[test]
    fn a_value_longer_than_the_bound_is_refused_before_parsing() {
        // The largest genuine line: three full lists of the longest names.
        let longest: Vec<String> = (0..MAX_NAMES)
            .map(|i| {
                format!(
                    "{i:03}{}",
                    "n".repeat(crate::guard::MAX_TOPIC_NAME_CHARS - 3)
                )
            })
            .collect();
        let full = CreationStopLists {
            appeared: NameList {
                names: longest.clone(),
                count: MAX_COUNT,
            },
            left: NameList {
                names: longest.clone(),
                count: MAX_COUNT,
            },
            unconfirmed: NameList {
                names: longest,
                count: MAX_COUNT,
            },
            unconfirmed_seen: false,
        };
        let value = full.line_value();
        assert!(value.len() <= MAX_VALUE_BYTES, "{} bytes", value.len());
        assert_eq!(CreationStopLists::parse_value(&value), Some(full));

        // One byte over, as valid JSON: refused, although it would parse.
        let padded = format!(
            "{{\"left\":[\"orders\"],\"pad\":\"{}\"}}",
            "x".repeat(MAX_VALUE_BYTES)
        );
        assert!(padded.len() > MAX_VALUE_BYTES);
        assert!(serde_json::from_str::<Value>(&padded).is_ok());
        assert_eq!(CreationStopLists::parse_value(&padded), None);
        // And a value exactly at the bound is still read.
        let pad = MAX_VALUE_BYTES - "{\"left\":[\"orders\"],\"pad\":\"\"}".len();
        let at_bound = format!("{{\"left\":[\"orders\"],\"pad\":\"{}\"}}", "x".repeat(pad));
        assert_eq!(at_bound.len(), MAX_VALUE_BYTES);
        assert_eq!(
            CreationStopLists::parse_value(&at_bound).map(|l| l.left.names),
            Some(vec!["orders".to_string()])
        );
    }

    /// A controller holds each list to its own Restore's mapped targets
    /// (review 2, M1): a name outside them is dropped, and the count can
    /// never claim more names than the plan maps. KILLS: a count that still
    /// counts the dropped names; a count above the plan's own size.
    #[test]
    fn a_list_held_to_a_plan_drops_foreign_names_and_bounds_its_count() {
        let mapped = ["orders", "payments"];
        let keep = |n: &str| mapped.contains(&n);
        let list = NameList {
            names: vec!["orders".into(), "payments-prod".into(), "ledger".into()],
            count: 3,
        };
        let held = list.held_to(keep, mapped.len());
        assert_eq!(held.names, vec!["orders".to_string()]);
        assert_eq!(held.count, 1);
        // A claimed count beyond the plan is the plan's size at most.
        let claimed = NameList {
            names: vec!["orders".into()],
            count: 5000,
        };
        assert_eq!(claimed.held_to(keep, mapped.len()).count, 2);
        // Nothing kept: nothing claimed.
        let foreign = NameList {
            names: vec!["ledger".into()],
            count: 1,
        };
        let held = foreign.held_to(keep, mapped.len());
        assert!(held.is_empty(), "{held:?}");
    }
}
