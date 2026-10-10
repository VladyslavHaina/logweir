//! **FX-34.** A guard refusal's reason code and sentence, as ONE
//! machine-readable line a controller can read off a pod log.
//!
//! # Why this line exists
//!
//! Interface I9's `refusal-reason=<TerminalState>` names a CLOSED state, and
//! most refusals have none: a plan that names `restore.partitions` is a plain
//! `GuardRefused`. The sentence that says why was only on the human line, in
//! a pod log that goes with the pod, so a `Restore` or a `Backup` said "the
//! runner exited 3 (guard-refused)" and nothing else (PoC batch 5, F-1).
//!
//! # The line
//!
//! ```text
//! refusal-detail={"code":"<ReasonCode>","message":"<sentence>"}
//! ```
//!
//! One JSON object with exactly those two string members, on one line, read
//! by its key name like every other line a controller takes off a log
//! (erratum E4). The runner prints it at exit 3 only, immediately BEFORE
//! `refusal-reason=`, so I9's "the final stdout line" still holds.
//!
//! # A pod log is untrusted text, on both sides
//!
//! The sentence interpolates what the plan, the broker and the archive said:
//! a topic name, a cluster id, a parse error. In shared mode one controller
//! reads the logs of every namespace and its status feeds the console. So
//! this module is the ONE place a `refusal-detail=` value becomes a
//! [`RefusalDetail`], and the type has no other constructor:
//!
//! * the code is ASCII letters and digits, at most
//!   [`REASON_CODE_MAX_BYTES`] bytes, starting with a letter
//!   ([`is_reason_code`]); anything else and the whole line is refused;
//! * the sentence is reduced to printable text ([`clean_message`]): every
//!   control character, line break, bidi override and other code point
//!   outside the small allow-list is replaced, credential shapes are removed
//!   by the same rules every relayed message passes
//!   ([`crate::check_contract::redaction_rules`]), and the result is cut to
//!   [`REASON_MESSAGE_MAX_BYTES`] bytes on a character boundary with a
//!   visible marker;
//! * a line over [`REFUSAL_DETAIL_LINE_MAX_BYTES`] cannot have come from
//!   [`RefusalDetail::to_line`], so it is not read at all.
//!
//! The runner cleans before it prints and the reader cleans again. The second
//! pass is what a controller relies on; the first keeps an honest runner's
//! line inside the bounds the reader enforces.

use serde::{Deserialize, Serialize};

/// The key the line is read by.
pub const REFUSAL_DETAIL_PREFIX: &str = "refusal-detail=";

/// The longest reason code, in bytes. The same bound
/// `weirkeeper::check::refused_plan_field` puts on the one other identifier it
/// takes off a pod log.
pub const REASON_CODE_MAX_BYTES: usize = 64;

/// The longest sentence kept, in bytes, truncation marker included.
///
/// 512, the number every relayed message is already capped at
/// ([`crate::check_contract::MESSAGE_MAX_CHARS`]), counted in BYTES here
/// because a status is stored and sent as bytes: 512 characters of four-byte
/// code points would be 2 KiB. The refusal PoC batch 5 met is 366 bytes and
/// the longest fixed sentences this build writes are about 460 (the glob and
/// forbidden-key refusals), so a sentence is cut only when what it
/// interpolates is long: a list of topics, a parse error.
pub const REASON_MESSAGE_MAX_BYTES: usize = 512;

/// The longest `refusal-detail=` line, prefix included, in bytes.
///
/// [`RefusalDetail::to_line`] cannot exceed it: a 64-byte code, a 512-byte
/// sentence whose every byte is a `"` or a `\` and doubles in JSON, and 39
/// bytes of prefix and punctuation come to 1127. It is well under the 16 KiB
/// at which CRI splits a container log line, so the line always arrives
/// whole. A reader ignores a longer line instead of parsing it.
pub const REFUSAL_DETAIL_LINE_MAX_BYTES: usize = 2048;

/// What a cut sentence ends with.
pub const TRUNCATION_MARKER: char = '…';

/// What a run of code points outside the allow-list becomes.
pub const REPLACEMENT: char = '\u{FFFD}';

/// The code of a refusal whose sentence opens with none: I9's default state.
pub const DEFAULT_REASON_CODE: &str = crate::guard::TERMINAL_STATE_GUARD_REFUSED;

/// The typographic punctuation the runner's own sentences use, kept beside
/// printable ASCII. Every other non-ASCII code point is replaced.
const KEPT_PUNCTUATION: [char; 5] = ['§', '–', '—', '…', '→'];

/// Whether `code` is a reason code: ASCII letters and digits only, a letter
/// first, at most [`REASON_CODE_MAX_BYTES`] bytes.
///
/// A hand-written predicate, as [`crate::guard::topic_name_is_kafka_legal`]
/// is and for its reason: no regex dependency, and no `.` or anchor to get
/// wrong. Every accepted character is one byte, so the byte bound is the
/// character bound.
#[must_use]
pub fn is_reason_code(code: &str) -> bool {
    let bytes = code.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && bytes.len() <= REASON_CODE_MAX_BYTES
        && bytes.iter().all(u8::is_ascii_alphanumeric)
}

/// A refusal sentence as text that is safe to store and to show.
///
/// Three passes, in this order:
///
/// 1. **Characters** ([`printable`]). Printable ASCII and
///    [`KEPT_PUNCTUATION`] are kept. A run of whitespace or control
///    characters (a newline, a tab, an ANSI escape's `ESC`, a C1 control, a
///    line or paragraph separator) becomes ONE space. A run of anything else
///    (a bidi override, a zero-width or other formatting character, a
///    private-use or unassigned code point, a letter of another script, the
///    `U+FFFD` an invalid byte decodes to) becomes ONE [`REPLACEMENT`]. An
///    allow-list, so a code point nobody thought of is replaced and never
///    passed through.
/// 2. **Credential shapes.** A URL loses its query string and its fragment
///    ([`without_url_queries`]: a presigned URL's signature is in its query),
///    and then [`crate::check_contract::redaction_rules`] apply: URL
///    userinfo, secret key/value forms, PEM blocks, AWS access key ids, S3
///    error bodies and unkeyed base64 or hex runs of 40 characters or more.
///    BEFORE the cut, so a secret is never left half-shown at the boundary,
///    where the rules would no longer recognise it.
/// 3. **Length.** At most [`REASON_MESSAGE_MAX_BYTES`] bytes, cut on a
///    character boundary; a cut sentence ends with [`TRUNCATION_MARKER`].
///
/// The runner calls it and the reader calls it again. A second pass can only
/// replace, remove or cut, so the reader's result is never wider than the
/// runner's; for a sentence the runner already cleaned it is the same text.
#[must_use]
pub fn clean_message(raw: &str) -> String {
    let redacted = crate::check_contract::apply_rules(
        &without_url_queries(&printable(raw)),
        crate::check_contract::redaction_rules(),
    );
    // The rules write ASCII only, so the second character pass does one
    // thing: it folds a double space a removed value left behind.
    truncate(&printable(&redacted))
}

/// `text` with the query string and the fragment removed from every
/// space-delimited word that holds a URL.
///
/// The redaction rules remove a URL's userinfo and leave its query alone,
/// and a presigned URL carries its signature there. Everything from the
/// first `?` or `#` after the scheme goes, as the controller's own
/// diagnostics already do for a kubelet's or a webhook's prose.
fn without_url_queries(text: &str) -> String {
    text.split(' ')
        .map(|word| match word.find("://") {
            Some(at) => {
                let (scheme, rest) = word.split_at(at + 3);
                let kept = rest.split(['?', '#']).next().unwrap_or_default();
                format!("{scheme}{kept}")
            }
            None => word.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Pass 1 of [`clean_message`]: the allow-list, with runs folded and the
/// ends trimmed.
fn printable(raw: &str) -> String {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Pending {
        Nothing,
        Space,
        Replacement,
    }
    let mut out = String::with_capacity(raw.len());
    let mut pending = Pending::Nothing;
    for c in raw.chars() {
        if c.is_ascii_graphic() || KEPT_PUNCTUATION.contains(&c) {
            match pending {
                Pending::Space if !out.is_empty() => out.push(' '),
                Pending::Replacement => out.push(REPLACEMENT),
                _ => {}
            }
            pending = Pending::Nothing;
            out.push(c);
        } else if c.is_whitespace() || c.is_control() {
            // A replacement already pending is written before the space, so
            // `a<RLO> b` reads `a<U+FFFD> b` and not `a b`.
            if pending == Pending::Replacement {
                out.push(REPLACEMENT);
            }
            pending = Pending::Space;
        } else {
            if pending == Pending::Space && !out.is_empty() {
                out.push(' ');
            }
            pending = Pending::Replacement;
        }
    }
    if pending == Pending::Replacement {
        out.push(REPLACEMENT);
    }
    out
}

/// `text` in at most [`REASON_MESSAGE_MAX_BYTES`] bytes, cut on a character
/// boundary, ending with [`TRUNCATION_MARKER`] when it was cut.
fn truncate(text: &str) -> String {
    if text.len() <= REASON_MESSAGE_MAX_BYTES {
        return text.to_string();
    }
    let mut end = REASON_MESSAGE_MAX_BYTES - TRUNCATION_MARKER.len_utf8();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = text[..end].trim_end().to_string();
    out.push(TRUNCATION_MARKER);
    out
}

/// The reason code a refusal sentence opens with, and the rest of it.
///
/// A guard's message MAY open with its name. I9's terminal states and most
/// named reasons write `<Code>: ` (`StorageRegionInvalid: …`,
/// `PlainWithoutTls: …`); the recovery-point and standing-authorization
/// refusals write `<Code>. ` (`PointBindingMismatch. The plan is bound …`).
/// Both are read. The code is a CamelCase word: an ASCII upper-case letter,
/// then two or more ASCII letters and digits, at most
/// [`REASON_CODE_MAX_BYTES`] in all, and the separator is part of the match.
/// A sentence that opens with anything else carries
/// [`DEFAULT_REASON_CODE`] and is kept whole.
///
/// Either way `"{code}: {rest}"` is the runner's own sentence (with `: `
/// where it wrote `. `), or that sentence behind `GuardRefused: `, so a word
/// mistaken for a code changes nothing a reader sees.
#[must_use]
pub fn split_reason_code(message: &str) -> (&str, &str) {
    let word_end = message
        .bytes()
        .position(|b| !b.is_ascii_alphanumeric())
        .unwrap_or(message.len());
    let (head, tail) = message.split_at(word_end);
    let camel = head.len() >= 3 && head.as_bytes()[0].is_ascii_uppercase() && is_reason_code(head);
    if camel {
        for separator in [": ", ". "] {
            if let Some(rest) = tail.strip_prefix(separator) {
                return (head, rest);
            }
        }
    }
    (DEFAULT_REASON_CODE, message)
}

/// The two members of the line's JSON object, and nothing else.
///
/// `deny_unknown_fields`, and serde refuses a repeated member of a struct, so
/// a line with a third member or with two `message`s is not this document.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    code: String,
    message: String,
}

/// A refusal's reason code and sentence, **validated and cleaned**.
///
/// The fields are private and the two constructors below are the only ones,
/// so a value of this type has always passed [`is_reason_code`] and
/// [`clean_message`]. What reaches a status is built from one of these and
/// never from a raw line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalDetail {
    code: String,
    message: String,
}

impl RefusalDetail {
    /// The reason code: [`is_reason_code`] holds.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// The sentence: [`clean_message`]'s output, never empty.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// **The runner's side.** From a guard refusal's own message (the
    /// `GuardRefusal`'s text, not an error enum's wrapped `Display`).
    ///
    /// `None` when nothing printable is left: a refusal that says nothing
    /// prints no line, and the reader then reports exactly what it reported
    /// before this line existed.
    #[must_use]
    pub fn from_refusal_message(message: &str) -> Option<Self> {
        let (code, rest) = split_reason_code(message);
        Self::checked(code, rest)
    }

    /// **The reader's side.** From one log line, prefix included.
    ///
    /// `None` for a line that does not open with [`REFUSAL_DETAIL_PREFIX`],
    /// that is longer than [`REFUSAL_DETAIL_LINE_MAX_BYTES`], whose value is
    /// not a JSON object with exactly the string members `code` and
    /// `message`, whose code is not a reason code, or whose sentence has
    /// nothing printable in it. The sentence is cleaned whatever the runner
    /// did to it.
    #[must_use]
    pub fn from_line(line: &str) -> Option<Self> {
        if line.len() > REFUSAL_DETAIL_LINE_MAX_BYTES {
            return None;
        }
        let value = line.strip_prefix(REFUSAL_DETAIL_PREFIX)?;
        // An OBJECT. serde reads a struct from a JSON array too, by position,
        // and `["A","m"]` is not this document.
        if !value.starts_with('{') {
            return None;
        }
        let wire: Wire = serde_json::from_str(value).ok()?;
        Self::checked(&wire.code, &wire.message)
    }

    fn checked(code: &str, message: &str) -> Option<Self> {
        if !is_reason_code(code) {
            return None;
        }
        let message = clean_message(message);
        if message.is_empty() {
            return None;
        }
        Some(Self {
            code: code.to_string(),
            message,
        })
    }

    /// The line, prefix included and with no newline. At most
    /// [`REFUSAL_DETAIL_LINE_MAX_BYTES`] bytes.
    #[must_use]
    pub fn to_line(&self) -> String {
        let wire = Wire {
            code: self.code.clone(),
            message: self.message.clone(),
        };
        // Two `String` members cannot fail to serialise; the fallback keeps
        // this function total without an `expect`.
        let value = serde_json::to_string(&wire).unwrap_or_default();
        format!("{REFUSAL_DETAIL_PREFIX}{value}")
    }
}

/// `<code>: <sentence>`: the runner's own sentence, or that sentence behind
/// `GuardRefused: ` when it opened with no code.
impl std::fmt::Display for RefusalDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// The `refusal-detail=` line for a guard refusal's message, or `None` when
/// the message has nothing printable in it. Pure, for the reason
/// [`crate::guard::refusal_reason_line`] is: this crate does no I/O, and the
/// binary prints what this returns.
#[must_use]
pub fn refusal_detail_line(message: &str) -> Option<String> {
    RefusalDetail::from_refusal_message(message).map(|d| d.to_line())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sentence PoC batch 5's K3 runner printed (`k3-runner.log`).
    const K3: &str = "PartitionSubsetsAwaitOwnerDecision: restore.partitions names a partition \
        subset of orders; a restore of a partition subset is refused until the owner decides \
        how its scorecard is versioned (OD-9), because a verifier that predates it would read \
        the narrowed restore as a full one. Remove restore.partitions to restore every \
        partition (a window start, restore.point_in_time: \"<start>/<end>\", is accepted)";

    /// `bytes` bytes of five-letter words. Words, because one long run of
    /// letters is a credential shape and would be removed whole.
    fn prose(bytes: usize) -> String {
        let mut text = "lorem ".repeat(bytes / 6 + 1);
        text.truncate(bytes);
        if text.ends_with(' ') {
            text.pop();
            text.push('x');
        }
        text
    }

    /// What every cleaned sentence satisfies, whatever went in.
    fn assert_clean(text: &str) {
        assert!(
            text.len() <= REASON_MESSAGE_MAX_BYTES,
            "{} bytes",
            text.len()
        );
        for c in text.chars() {
            assert!(
                c == ' '
                    || c.is_ascii_graphic()
                    || KEPT_PUNCTUATION.contains(&c)
                    || c == REPLACEMENT,
                "U+{:04X} survived in {text:?}",
                c as u32
            );
        }
        assert!(!text.contains("  "), "a folded run: {text:?}");
        assert_eq!(text.trim(), text);
    }

    #[test]
    fn the_real_refusal_round_trips_whole() {
        let line = refusal_detail_line(K3).expect("a sentence prints a line");
        assert!(line.starts_with(REFUSAL_DETAIL_PREFIX));
        assert!(!line.contains('\n'), "one line: {line}");
        let read = RefusalDetail::from_line(&line).expect("the reader accepts the runner's line");
        assert_eq!(read.code(), "PartitionSubsetsAwaitOwnerDecision");
        assert!(read
            .message()
            .starts_with("restore.partitions names a partition subset"));
        assert!(
            read.message().ends_with("\"<start>/<end>\", is accepted)"),
            "the remedy at the end survives: {}",
            read.message()
        );
        assert_eq!(
            read.to_string(),
            K3,
            "code and sentence are the runner's own words"
        );
        assert_eq!(
            read.message().len(),
            366,
            "the figure the bound's note quotes"
        );
    }

    #[test]
    fn a_sentence_with_no_code_carries_the_default_state() {
        let plain = "target cluster id abc is not in allowedClusterIds";
        let d = RefusalDetail::from_refusal_message(plain).expect("a line");
        assert_eq!(d.code(), DEFAULT_REASON_CODE);
        assert_eq!(d.message(), plain);
        // Words before a colon that are not ONE CamelCase word are prose.
        for prose in [
            "plan_hash mismatch: the approval names a",
            "incomplete Restore execution contract: X is missing",
            "ab: too short to be a code",
            "lowercase: is not a code",
            "Has-Dash: is not a code",
            "NoSpaceAfterColon:x",
        ] {
            let d = RefusalDetail::from_refusal_message(prose).expect("a line");
            assert_eq!(d.code(), DEFAULT_REASON_CODE, "{prose}");
            assert_eq!(d.message(), prose);
        }
    }

    #[test]
    fn a_code_that_ends_in_a_full_stop_is_read_too() {
        // The recovery-point refusals' own spelling (`drill/binding.rs`).
        let d = RefusalDetail::from_refusal_message(
            "PointBindingMismatch. The plan is bound to recovery point lwp1-abc; no data \
             operation was started.",
        )
        .expect("a line");
        assert_eq!(d.code(), "PointBindingMismatch");
        assert_eq!(
            d.message(),
            "The plan is bound to recovery point lwp1-abc; no data operation was started."
        );
        // A sentence whose first word merely ends a sentence is still prose
        // when it is not a CamelCase word of three or more characters.
        for prose in [
            "No. That is not a code",
            "it. is lower case",
            "Has Space. Not a code",
        ] {
            let d = RefusalDetail::from_refusal_message(prose).expect("a line");
            assert_eq!(d.code(), DEFAULT_REASON_CODE, "{prose}");
            assert_eq!(d.message(), prose);
        }
        // Only at the very start, and only one word.
        let d = RefusalDetail::from_refusal_message("the plan: PointUntrusted. x").expect("a line");
        assert_eq!(d.code(), DEFAULT_REASON_CODE);
    }

    #[test]
    fn a_refusal_that_says_nothing_prints_no_line() {
        assert_eq!(refusal_detail_line(""), None);
        assert_eq!(refusal_detail_line(" \n\t "), None);
        assert_eq!(refusal_detail_line("StorageRegionInvalid: \u{0007}"), None);
    }

    #[test]
    fn the_reason_code_pattern_is_strict() {
        for good in [
            "GuardRefused",
            "A",
            "a1",
            "PlainWithoutTls",
            &"A".repeat(64),
        ] {
            assert!(is_reason_code(good), "{good}");
        }
        for bad in [
            "",
            "1Abc",
            "Has Space",
            "Has-Dash",
            "Has_Underscore",
            "Dot.Ted",
            "Colon:",
            "<script>",
            "Ünïcode",
            "Tab\t",
            "New\nLine",
            "Nul\u{0}",
            "Bidi\u{202E}",
            &"A".repeat(65),
        ] {
            assert!(!is_reason_code(bad), "{bad:?}");
        }
    }

    fn line(code: &str, message: &str) -> String {
        format!(
            "{REFUSAL_DETAIL_PREFIX}{}",
            serde_json::json!({ "code": code, "message": message })
        )
    }

    #[test]
    fn a_line_with_a_bad_code_is_refused_whole() {
        for bad in [
            "",
            "Has Space",
            "<script>alert(1)</script>",
            "A\u{202E}B",
            &"A".repeat(65),
        ] {
            assert_eq!(
                RefusalDetail::from_line(&line(bad, "a sentence")),
                None,
                "{bad:?}"
            );
        }
        assert!(RefusalDetail::from_line(&line("GuardRefused", "a sentence")).is_some());
    }

    #[test]
    fn only_the_exact_document_is_read() {
        let p = REFUSAL_DETAIL_PREFIX;
        for bad in [
            // not this key, or not at the start of the line
            r#"refusal-reason={"code":"A","message":"m"}"#.to_string(),
            format!(r#" {p}{{"code":"A","message":"m"}}"#),
            format!(r#"x {p}{{"code":"A","message":"m"}}"#),
            // not an object, a member missing, a member of the wrong type
            format!("{p}GuardRefused: a sentence"),
            format!(r#"{p}"a string""#),
            format!(r#"{p}["A","m"]"#),
            format!(r#"{p} {{"code":"A","message":"m"}}"#),
            format!(r#"{p}{{"code":"A"}}"#),
            format!(r#"{p}{{"message":"m"}}"#),
            format!(r#"{p}{{"code":1,"message":"m"}}"#),
            format!(r#"{p}{{"code":"A","message":["m"]}}"#),
            format!(r#"{p}{{"code":"A","message":null}}"#),
            // a third member, a repeated member, trailing text
            format!(r#"{p}{{"code":"A","message":"m","remedy":"r"}}"#),
            format!(r#"{p}{{"code":"A","message":"m","message":"n"}}"#),
            format!(r#"{p}{{"code":"A","code":"B","message":"m"}}"#),
            format!(r#"{p}{{"code":"A","message":"m"}} and more"#),
            format!(r#"{p}{{"code":"A","message":"m"}}{{"code":"B","message":"n"}}"#),
            // cut short, or empty
            format!(r#"{p}{{"code":"A","message":"m"#),
            p.to_string(),
        ] {
            assert_eq!(RefusalDetail::from_line(&bad), None, "{bad}");
        }
        let good = format!(r#"{p}{{"code":"A","message":"m"}}"#);
        assert!(RefusalDetail::from_line(&good).is_some());
        // Member order is not part of the document.
        let swapped = format!(r#"{p}{{"message":"m","code":"A"}}"#);
        assert_eq!(
            RefusalDetail::from_line(&swapped),
            RefusalDetail::from_line(&good)
        );
    }

    #[test]
    fn an_overlong_line_is_not_read() {
        // One mebibyte on one line: not parsed, whatever it holds.
        let huge = line("GuardRefused", &prose(1024 * 1024));
        assert!(huge.len() > 1024 * 1024);
        assert_eq!(RefusalDetail::from_line(&huge), None);
        // The bound is on the LINE: at it the line is read (and its sentence
        // cut), one byte over it is not.
        let overhead = line("GuardRefused", "").len();
        let at = line(
            "GuardRefused",
            &prose(REFUSAL_DETAIL_LINE_MAX_BYTES - overhead),
        );
        assert_eq!(at.len(), REFUSAL_DETAIL_LINE_MAX_BYTES);
        let read = RefusalDetail::from_line(&at).expect("a line at the bound is read");
        assert!(read.message().ends_with(TRUNCATION_MARKER));
        assert_clean(read.message());
        let over = line(
            "GuardRefused",
            &prose(REFUSAL_DETAIL_LINE_MAX_BYTES - overhead + 1),
        );
        assert_eq!(over.len(), REFUSAL_DETAIL_LINE_MAX_BYTES + 1);
        assert_eq!(RefusalDetail::from_line(&over), None);
    }

    #[test]
    fn the_sentence_is_cut_on_a_character_boundary_with_a_marker() {
        let exact = prose(REASON_MESSAGE_MAX_BYTES);
        assert_eq!(clean_message(&exact), exact, "at the bound nothing is cut");
        let over = prose(REASON_MESSAGE_MAX_BYTES + 1);
        let cut = clean_message(&over);
        assert!(cut.ends_with(TRUNCATION_MARKER), "{cut}");
        assert!(cut.len() <= REASON_MESSAGE_MAX_BYTES, "{}", cut.len());
        assert!(
            cut.len() >= REASON_MESSAGE_MAX_BYTES - 1,
            "nearly all of the budget is used: {}",
            cut.len()
        );
        assert!(over.starts_with(cut.trim_end_matches(TRUNCATION_MARKER)));
        // A three-byte character straddling the cut is dropped whole, never
        // split: every offset of the boundary is tried.
        for pad in 0..4 {
            let text = format!("{}{}", "a".repeat(pad), "—".repeat(400));
            let cut = clean_message(&text);
            assert!(cut.ends_with(TRUNCATION_MARKER), "pad {pad}");
            assert!(
                cut.len() > REASON_MESSAGE_MAX_BYTES - 3,
                "pad {pad}: {}",
                cut.len()
            );
            assert_clean(&cut);
        }
    }

    #[test]
    fn control_characters_and_line_breaks_become_one_space() {
        assert_eq!(clean_message("a\nb"), "a b");
        assert_eq!(clean_message("a\r\n\t  b"), "a b");
        assert_eq!(clean_message("a\u{0000}b\u{0007}c\u{007F}d"), "a b c d");
        // C1 controls, NEL, the line and paragraph separators, NBSP.
        assert_eq!(
            clean_message("a\u{0085}b\u{009B}c\u{2028}d\u{2029}e\u{00A0}f"),
            "a b c d e f"
        );
        // An ANSI escape: ESC goes, and what is left is inert text.
        assert_eq!(clean_message("\u{001B}[31mred\u{001B}[0m"), "[31mred [0m");
        assert_eq!(
            clean_message("  leading and trailing \n"),
            "leading and trailing"
        );
    }

    #[test]
    fn bidi_and_other_non_printing_code_points_are_replaced() {
        let r = REPLACEMENT;
        // RLO, LRO, PDF, the embeddings, the isolates, the marks.
        for c in [
            '\u{202E}', '\u{202D}', '\u{202C}', '\u{202A}', '\u{202B}', '\u{2066}', '\u{2067}',
            '\u{2068}', '\u{2069}', '\u{200E}', '\u{200F}', '\u{061C}',
        ] {
            assert_eq!(
                clean_message(&format!("a{c}b")),
                format!("a{r}b"),
                "U+{:04X}",
                c as u32
            );
        }
        // Zero-width and other formatting, tag characters, a private-use code
        // point, a noncharacter, two Hangul fillers, the BOM, a soft hyphen,
        // the Mongolian vowel separator, the blank Braille pattern.
        for c in [
            '\u{200B}',
            '\u{200C}',
            '\u{200D}',
            '\u{2060}',
            '\u{FEFF}',
            '\u{00AD}',
            '\u{E0001}',
            '\u{E0041}',
            '\u{E000}',
            '\u{FFFE}',
            '\u{3164}',
            '\u{115F}',
            '\u{180E}',
            '\u{2800}',
        ] {
            assert_eq!(
                clean_message(&format!("a{c}b")),
                format!("a{r}b"),
                "U+{:04X}",
                c as u32
            );
        }
        // Letters of other scripts too: an allow-list, so right-to-left text
        // cannot reorder its neighbours and a look-alike letter cannot pass.
        assert_eq!(clean_message("a\u{05D0}\u{05D1}b"), format!("a{r}b"));
        assert_eq!(clean_message("p\u{0430}ssword"), format!("p{r}ssword"));
        // A run is one replacement, and a replacement survives a second pass.
        assert_eq!(
            clean_message("a\u{202E}\u{202D}\u{200B}b"),
            format!("a{r}b")
        );
        assert_eq!(clean_message(&format!("a{r}{r}b")), format!("a{r}b"));
        assert_eq!(clean_message("a\u{202E} b"), format!("a{r} b"));
        assert_eq!(clean_message("a \u{202E}b"), format!("a {r}b"));
        assert_eq!(clean_message("\u{202E}"), r.to_string());
    }

    #[test]
    fn the_runners_own_punctuation_and_markup_characters_are_kept_as_text() {
        let text = "D3 §4.3(e) — a `topic` → b… <script>alert(1)</script> &lt; \"q\" 'q' \\ 1–2";
        assert_eq!(clean_message(text), text);
    }

    #[test]
    fn credential_shapes_are_removed_before_the_cut() {
        let url = clean_message("the endpoint http://user:hunter2@minio:9000/bucket refused");
        assert_eq!(url, "the endpoint http://minio:9000/bucket refused");
        let kv = clean_message("sasl.password=hunter2 was sent");
        assert!(!kv.contains("hunter2"), "{kv}");
        // A presigned URL: the signature is in the query, and the query goes.
        // So does a fragment; the path and the words around it stay.
        let presigned = clean_message(
            "GET https://bucket.s3.amazonaws.com/logweir/x.json?X-Amz-Signature=0123abcd&\
             X-Amz-Credential=a%2F20261009 failed (see http://docs.example/page#section) twice",
        );
        assert_eq!(
            presigned,
            "GET https://bucket.s3.amazonaws.com/logweir/x.json failed (see \
             http://docs.example/page twice"
        );
        // A word that only NAMES a scheme is left as it is.
        assert_eq!(
            clean_message("a plain http:// endpoint, or an https:// one?"),
            "a plain http:// endpoint, or an https:// one?"
        );
        // 40 characters of the base64 alphabet, assembled so that no source
        // line carries a secret-shaped literal.
        let key = format!("{}{}", "wJalrXUtnFEMI/K7MDENG/", "bPxRfiCYEXAMPLEKEY");
        assert_eq!(key.len(), 40);
        let run = clean_message(&format!("the key {key} was refused"));
        assert_eq!(run, "the key [redacted] was refused");
        // The cut comes AFTER the rules: a secret straddling byte 512 is
        // removed whole, never left as a prefix the rules no longer match.
        let straddle = format!("{} {key} tail", prose(REASON_MESSAGE_MAX_BYTES - 25));
        assert!(straddle.len() > REASON_MESSAGE_MAX_BYTES);
        let cut = clean_message(&straddle);
        assert!(!cut.contains(&key[..8]), "{cut}");
        assert!(cut.ends_with("[redacted] tail"), "{cut}");
        // A value the rules removed leaves one space behind, not two.
        assert_clean(&clean_message(
            "before -----BEGIN X----- abc -----END X----- after",
        ));
    }

    #[test]
    fn whatever_goes_in_what_comes_out_is_bounded_printable_text() {
        let mut inputs = vec![
            K3.to_string(),
            String::new(),
            "a\u{202E}b\nc\u{0000}d".to_string(),
            "sasl.password=hunter2 and http://u:p@h/x".to_string(),
            prose(4000),
            "—".repeat(600),
            "\u{202E}x".repeat(600),
            format!("{}\u{202E}", prose(600)),
            "\u{001B}[2J".repeat(300),
            "<script>".repeat(200),
            String::from_utf8_lossy(&[0xFF; 900]).into_owned(),
        ];
        // Every code point in the first three planes, in slices.
        let all: String = (0u32..0x3_0000).filter_map(char::from_u32).collect();
        inputs.extend(
            all.chars()
                .collect::<Vec<_>>()
                .chunks(97)
                .map(|c| c.iter().collect::<String>()),
        );
        for text in inputs {
            let once = clean_message(&text);
            assert_clean(&once);
            // The reader's second pass keeps every property and shows no more.
            let twice = clean_message(&once);
            assert_clean(&twice);
            assert!(twice.len() <= once.len() + "[redacted]".len());
        }
    }

    #[test]
    fn an_honest_sentence_is_the_same_after_the_readers_pass() {
        for text in [
            K3.to_string(),
            "target cluster id abc is not in allowedClusterIds".to_string(),
            prose(4000),
            "D3 §4.3(e) needs the signed document — all of it".to_string(),
        ] {
            let once = clean_message(&text);
            assert_eq!(clean_message(&once), once);
        }
    }

    #[test]
    fn a_hostile_sentence_is_read_cleaned_and_never_raw() {
        let hostile = "restore.partitions\u{001B}[2J names\n\u{202E}<script>alert(1)</script>";
        let read =
            RefusalDetail::from_line(&line("GuardRefused", hostile)).expect("cleaned, not refused");
        assert_eq!(
            read.message(),
            format!("restore.partitions [2J names {REPLACEMENT}<script>alert(1)</script>")
        );
        assert_clean(read.message());
        // Lossily decoded invalid UTF-8 is one replacement, not a failure.
        let lossy = String::from_utf8_lossy(b"bad \xFF\xFE bytes").into_owned();
        let read = RefusalDetail::from_line(&line("GuardRefused", &lossy)).expect("a line");
        assert_eq!(read.message(), format!("bad {REPLACEMENT} bytes"));
    }

    #[test]
    fn the_runners_line_never_exceeds_the_bound_the_reader_enforces() {
        // The worst case for JSON: every byte of the sentence doubles.
        for filler in ["\"", "\\", "ab ", "—", "\u{202E}x", "\n"] {
            let message = format!("{}: x{}", "A".repeat(64), filler.repeat(4000));
            let line = refusal_detail_line(&message).expect("a line");
            assert!(
                line.len() <= REFUSAL_DETAIL_LINE_MAX_BYTES,
                "{filler:?}: {} bytes",
                line.len()
            );
            assert!(line.len() <= 1127, "the figure the bound's note quotes");
            assert!(!line.contains('\n') && !line.contains('\r'));
            let read = RefusalDetail::from_line(&line).expect("the reader accepts it");
            assert_eq!(read.to_line(), line, "and reading it changes nothing");
        }
    }
}
