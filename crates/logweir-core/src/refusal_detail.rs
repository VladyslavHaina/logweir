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
//! One JSON object with exactly those two string members, on one line. The
//! runner prints it at exit 3 only, ALWAYS, immediately BEFORE
//! `refusal-reason=`, so I9's "the final stdout line" still holds and the two
//! lines are the last two the runner writes. A controller honours it at that
//! position and nowhere else (`weirkeeper::refusal::runner_reason`).
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
//!   ([`is_reason_code`]), AND a member of the closed set of codes the
//!   refusing runner can print ([`RefusingRun::reason_codes`], one set per
//!   kind of run); anything else and the whole line is refused;
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
//!
//! # A line in a pod log can be forged by whoever wrote the plan
//!
//! The runner prints an error's text raw on stderr, some errors repeat a plan
//! value, and a YAML scalar may hold a line break: a plan can start a line of
//! its own choosing in the pod log (PROD-15.1's review). So a well-formed
//! line is not thereby the runner's. Two things here answer that and the
//! third is the reader's: the code is one of a CLOSED set per kind, the two
//! refusal lines must AGREE ([`RefusalDetail::agrees_with_state`]), and the
//! reader takes the line only from the position the runner prints it at.

use serde::{Deserialize, Serialize};

/// The key the line is read by.
pub const REFUSAL_DETAIL_PREFIX: &str = "refusal-detail=";

/// The longest reason code, in bytes. The same bound
/// `weirkeeper::check::refused_plan_field` puts on the one other identifier it
/// takes off a pod log.
pub const REASON_CODE_MAX_BYTES: usize = 64;

/// The longest sentence kept, in bytes, truncation marker included.
///
/// # Why 760
///
/// It keeps a refused run's WHOLE condition message inside 1024 bytes, which
/// is `status.progress.message`'s `maxLength` and the bound the product API
/// serves a condition's message under: the 123 bytes the message always had,
/// the 48-byte label, a 64-byte code, `: ` and 760 come to 997, with 27 to
/// spare for the fixed text. A sentence that took the message past 1024
/// would be cut a second time, with no marker, on its way to the console.
///
/// A smaller one cuts remedies. They are at the END of a sentence ("Delete
/// them, or restore under a prefix nothing has used yet …"), and the fixed
/// text of the longest sentences this build writes is 489 bytes (a restore
/// whose target topics already exist), 453 and 435 before any name is
/// interpolated: at the 512 every other relayed message is capped at
/// ([`crate::check_contract::MESSAGE_MAX_CHARS`]) the commonest real refusal
/// would lose its last clause with one topic in it. At 760 it keeps about
/// eight. The refusal PoC batch 5 met is 402 bytes, its opening word included.
///
/// BYTES, not characters, because a status is stored and sent as bytes.
pub const REASON_MESSAGE_MAX_BYTES: usize = 760;

/// The longest `refusal-detail=` line, prefix included, in bytes.
///
/// [`RefusalDetail::to_line`] cannot exceed it: a code of at most 64 bytes
/// (the longest in a closed set is 30), a 760-byte sentence whose every byte
/// is a `"` or a `\` and doubles in JSON, and 39 bytes of prefix and
/// punctuation come to at most 1623. It is well under the 16 KiB
/// at which CRI splits a container log line, so the line always arrives
/// whole. A reader ignores a longer line instead of parsing it.
pub const REFUSAL_DETAIL_LINE_MAX_BYTES: usize = 2048;

/// What a cut sentence ends with.
pub const TRUNCATION_MARKER: char = '…';

/// What a run of code points outside the allow-list becomes.
pub const REPLACEMENT: char = '\u{FFFD}';

/// The code of a refusal whose sentence opens with none: I9's default state.
pub const DEFAULT_REASON_CODE: &str = crate::guard::TERMINAL_STATE_GUARD_REFUSED;

/// What the line carries for a refusal that has nothing printable in it. The
/// runner prints a detail line at EVERY exit 3, so that the position a reader
/// trusts is never left for another line to occupy.
pub const NO_SENTENCE: &str = "the refusal carried no sentence";

/// Which runner refused. Each kind of run has its OWN closed set of reason
/// codes: a `Backup`'s log cannot put a restore-only code on a `Backup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusingRun {
    /// `logweir restore run` (and its alias `drill run`): a `Restore`'s Job.
    Restore,
    /// `logweir backup run`: a `Backup`'s Job.
    Backup,
}

/// Every reason code `logweir restore run` can print: the code a guard
/// refusal's sentence may open with on that path, and the default.
///
/// The list is the exit-3 sweep's (FX-34's report carries the table, one row
/// per place a refusal is built). Four of the codes are constants of the
/// `logweir` crate, which this crate cannot name; `crates/logweir/tests/
/// refusal_detail.rs` holds each literal here to its constant.
pub const RESTORE_REASON_CODES: [&str; 13] = [
    crate::guard::TERMINAL_STATE_GUARD_REFUSED,
    crate::guard::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    crate::guard::TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
    crate::guard::TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME,
    crate::guard::TERMINAL_STATE_PLAIN_WITHOUT_TLS,
    crate::guard::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    crate::guard::STORAGE_REGION_INVALID,
    crate::execution_contract::AUTHORIZATION_INVALID,
    crate::execution_contract::AUTHORIZATION_EXPIRED,
    // `logweir::drill::binding`'s four.
    "PointBindingMismatch",
    "PointBindingSetMismatch",
    "PointUntrusted",
    "RehearsalScopeViolation",
];

/// Every reason code `logweir backup run` can print. See
/// [`RESTORE_REASON_CODES`]; the last one is
/// `logweir_engine_oso::storage::WORKLOAD_IDENTITY_NOT_INJECTED`.
pub const BACKUP_REASON_CODES: [&str; 9] = [
    crate::guard::TERMINAL_STATE_GUARD_REFUSED,
    crate::guard::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    crate::guard::TERMINAL_STATE_PLAIN_WITHOUT_TLS,
    crate::guard::TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    crate::guard::STORAGE_REGION_INVALID,
    crate::consumer_positions::SELECTION_TOO_LARGE,
    crate::consumer_positions::SELECTION_ID_INVALID,
    crate::consumer_positions::SELECTION_REPEATED,
    "WorkloadIdentityNotInjected",
];

impl RefusingRun {
    /// The closed set of reason codes this kind of run can print.
    #[must_use]
    pub const fn reason_codes(self) -> &'static [&'static str] {
        match self {
            Self::Restore => &RESTORE_REASON_CODES,
            Self::Backup => &BACKUP_REASON_CODES,
        }
    }

    /// The member of this kind's closed set that equals `code`, if any. The
    /// answer is the set's own `&'static str`, never the caller's text.
    #[must_use]
    pub fn reason_code(self, code: &str) -> Option<&'static str> {
        self.reason_codes().iter().copied().find(|c| *c == code)
    }
}

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
/// Both are read, and the separator is part of the match.
///
/// **Only a member of `run`'s closed set is a code.** A sentence that opens
/// with any other word, however code-shaped, carries [`DEFAULT_REASON_CODE`]
/// and is kept WHOLE, opening word included. So the runner cannot print a
/// code outside the set whatever a sentence says, and a sentence's first word
/// (which may be a name the plan chose) never becomes a code by its shape.
///
/// Either way `"{code}: {rest}"` is the runner's own sentence (with `: `
/// where it wrote `. `), or that sentence behind `GuardRefused: `, so nothing
/// a reader sees is lost.
#[must_use]
pub fn split_reason_code(run: RefusingRun, message: &str) -> (&'static str, &str) {
    let word_end = message
        .bytes()
        .position(|b| !b.is_ascii_alphanumeric())
        .unwrap_or(message.len());
    let (head, tail) = message.split_at(word_end);
    if let Some(code) = run.reason_code(head) {
        for separator in [": ", ". "] {
            if let Some(rest) = tail.strip_prefix(separator) {
                return (code, rest);
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
/// so a value of this type has always passed [`is_reason_code`], is a member
/// of its run's closed set, and has been through [`clean_message`]. What
/// reaches a status is built from one of these and never from a raw line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalDetail {
    code: &'static str,
    message: String,
}

impl RefusalDetail {
    /// The reason code: a member of the refusing run's closed set
    /// ([`RefusingRun::reason_codes`]), so [`is_reason_code`] holds too.
    #[must_use]
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// The sentence: [`clean_message`]'s output, never empty.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// **The runner's side.** From a guard refusal's own message (the
    /// `GuardRefusal`'s text, not an error enum's wrapped `Display`).
    ///
    /// ALWAYS a value. A refusal with nothing printable in it carries
    /// [`NO_SENTENCE`]: the runner prints a detail line at every exit 3, so
    /// the one position a reader trusts is always the runner's own line.
    #[must_use]
    pub fn from_refusal_message(run: RefusingRun, message: &str) -> Self {
        let (code, rest) = split_reason_code(run, message);
        let cleaned = clean_message(rest);
        Self {
            code,
            message: if cleaned.is_empty() {
                NO_SENTENCE.to_string()
            } else {
                cleaned
            },
        }
    }

    /// **The reader's side.** From one log line, prefix included, of a run of
    /// kind `run`.
    ///
    /// `None` for a line that does not open with [`REFUSAL_DETAIL_PREFIX`],
    /// that is longer than [`REFUSAL_DETAIL_LINE_MAX_BYTES`], whose value is
    /// not a JSON object with exactly the string members `code` and
    /// `message`, whose code is not a reason code OF THAT KIND OF RUN
    /// ([`RefusingRun::reason_codes`]: a well-formed word outside the closed
    /// set is not a code), or whose sentence has nothing printable in it. The
    /// sentence is cleaned whatever the runner did to it.
    #[must_use]
    pub fn from_line(run: RefusingRun, line: &str) -> Option<Self> {
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
        // The pattern first, then the set: the set implies the pattern, and
        // the pattern is what bounds the comparison's input.
        if !is_reason_code(&wire.code) {
            return None;
        }
        let code = run.reason_code(&wire.code)?;
        let message = clean_message(&wire.message);
        if message.is_empty() {
            return None;
        }
        Some(Self { code, message })
    }

    /// Whether the state a `refusal-reason=` line names is the one THIS
    /// detail's runner would have printed beside it.
    ///
    /// The runner derives both lines from one refusal: the state is the
    /// detail's code when that code is one of I9's terminal states and the
    /// sentence named it with `: `, and [`DEFAULT_REASON_CODE`] otherwise. So
    /// a genuine pair's state is the code or the default, never a third
    /// word, and a detail line beside a state it could not have been printed
    /// with is not the runner's.
    #[must_use]
    pub fn agrees_with_state(&self, state: &str) -> bool {
        state == DEFAULT_REASON_CODE || state == self.code
    }

    /// The line, prefix included and with no newline. At most
    /// [`REFUSAL_DETAIL_LINE_MAX_BYTES`] bytes.
    #[must_use]
    pub fn to_line(&self) -> String {
        let wire = Wire {
            code: self.code.to_string(),
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

/// The `refusal-detail=` line `run` prints for a guard refusal's message.
/// Always a line ([`RefusalDetail::from_refusal_message`]). Pure, for the
/// reason [`crate::guard::refusal_reason_line`] is: this crate does no I/O,
/// and the binary prints what this returns.
#[must_use]
pub fn refusal_detail_line(run: RefusingRun, message: &str) -> String {
    RefusalDetail::from_refusal_message(run, message).to_line()
}

#[cfg(test)]
mod tests {
    use super::*;
    use RefusingRun::{Backup, Restore};

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
        // K3's opening word was a reason code when PoC batch 5 ran and is in
        // no closed set today (PROD-11.1b retired the refusal). So it is what
        // any word outside the set is: part of the sentence, kept whole,
        // behind the default code.
        let line = refusal_detail_line(Restore, K3);
        assert!(line.starts_with(REFUSAL_DETAIL_PREFIX));
        assert!(!line.contains('\n'), "one line: {line}");
        let read =
            RefusalDetail::from_line(Restore, &line).expect("the reader accepts the runner's line");
        assert_eq!(read.code(), DEFAULT_REASON_CODE);
        assert_eq!(read.message(), K3, "the sentence is the runner's own words");
        assert!(
            read.message().ends_with("\"<start>/<end>\", is accepted)"),
            "the remedy at the end survives: {}",
            read.message()
        );
        assert_eq!(read.to_string(), format!("GuardRefused: {K3}"));
        assert_eq!(
            read.message().len(),
            402,
            "the figure the bound's note quotes"
        );
        // A sentence that opens with a member of the set gives it as the
        // code, and `<code>: <sentence>` is the runner's own text again.
        let named = "PointInTimeByProducerTime: topic `lat` records LogAppendTime; state \
                     restore.time_basis: producerTime";
        let read = RefusalDetail::from_line(Restore, &refusal_detail_line(Restore, named))
            .expect("a line");
        assert_eq!(read.code(), "PointInTimeByProducerTime");
        assert_eq!(read.to_string(), named);
    }

    #[test]
    fn a_sentence_with_no_code_carries_the_default_state() {
        let plain = "target cluster id abc is not in allowedClusterIds";
        let d = RefusalDetail::from_refusal_message(Restore, plain);
        assert_eq!(d.code(), DEFAULT_REASON_CODE);
        assert_eq!(d.message(), plain);
        // Words before a colon that are not a member of the set are prose,
        // however code-shaped: a retired code, a made-up one, a name a plan
        // chose, and a code of the OTHER kind of run.
        for prose in [
            "plan_hash mismatch: the approval names a",
            "incomplete Restore execution contract: X is missing",
            "ab: too short to be a code",
            "lowercase: is not a code",
            "Has-Dash: is not a code",
            "NoSpaceAfterColon:x",
            "PartitionSubsetsAwaitOwnerDecision: a retired code",
            "Succeeded: a word the plan chose",
            "ConsumerGroupIdInvalid: a backup's code, in a restore's sentence",
            "WorkloadIdentityNotInjected: another",
            "GuardRefusedX: a member with a letter added",
            "guardrefused: a member in another case",
        ] {
            let d = RefusalDetail::from_refusal_message(Restore, prose);
            assert_eq!(d.code(), DEFAULT_REASON_CODE, "{prose}");
            assert_eq!(d.message(), prose, "kept whole, opening word included");
        }
        // And the other way round: a restore's code is prose to a backup.
        for prose in [
            "PointUntrusted. the receipt is not signed",
            "TargetTopicConfigRefused: cleanup.policy is compact",
            "AuthorizationInvalid. x",
        ] {
            let d = RefusalDetail::from_refusal_message(Backup, prose);
            assert_eq!(d.code(), DEFAULT_REASON_CODE, "{prose}");
            assert_eq!(d.message(), prose);
        }
        // NEGATIVE CONTROL: each kind's own members ARE promoted, with either
        // separator, and the sentence loses exactly the word and separator.
        for (run, codes) in [
            (Restore, &RESTORE_REASON_CODES[..]),
            (Backup, &BACKUP_REASON_CODES[..]),
        ] {
            for code in codes {
                for separator in [": ", ". "] {
                    let d = RefusalDetail::from_refusal_message(
                        run,
                        &format!("{code}{separator}a sentence"),
                    );
                    assert_eq!(d.code(), *code, "{run:?} {code}{separator}");
                    assert_eq!(d.message(), "a sentence");
                }
            }
        }
    }

    #[test]
    fn a_code_that_ends_in_a_full_stop_is_read_too() {
        // The recovery-point refusals' own spelling (`drill/binding.rs`).
        let d = RefusalDetail::from_refusal_message(
            Restore,
            "PointBindingMismatch. The plan is bound to recovery point lwp1-abc; no data \
             operation was started.",
        );
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
            let d = RefusalDetail::from_refusal_message(Restore, prose);
            assert_eq!(d.code(), DEFAULT_REASON_CODE, "{prose}");
            assert_eq!(d.message(), prose);
        }
        // Only at the very start, and only one word.
        let d = RefusalDetail::from_refusal_message(Restore, "the plan: PointUntrusted. x");
        assert_eq!(d.code(), DEFAULT_REASON_CODE);
    }

    #[test]
    fn a_refusal_that_says_nothing_still_prints_a_line() {
        // The runner prints a detail line at EVERY exit 3: the position a
        // reader trusts must never be left for another line to occupy.
        for (run, message, code) in [
            (Restore, "", DEFAULT_REASON_CODE),
            (Backup, " \n\t ", DEFAULT_REASON_CODE),
            (
                Restore,
                "StorageRegionInvalid: \u{0007}",
                "StorageRegionInvalid",
            ),
        ] {
            let line = refusal_detail_line(run, message);
            let read = RefusalDetail::from_line(run, &line).expect("the reader accepts it");
            assert_eq!(read.code(), code, "{message:?}");
            assert_eq!(read.message(), NO_SENTENCE, "{message:?}");
        }
        // NEGATIVE CONTROL: a sentence is never replaced by that text.
        assert_eq!(
            RefusalDetail::from_refusal_message(Restore, "x").message(),
            "x"
        );
    }

    #[test]
    fn the_closed_sets_are_the_sweeps_and_each_kind_has_its_own() {
        for (run, codes) in [
            (Restore, &RESTORE_REASON_CODES[..]),
            (Backup, &BACKUP_REASON_CODES[..]),
        ] {
            assert_eq!(run.reason_codes(), codes);
            let unique: std::collections::BTreeSet<&str> = codes.iter().copied().collect();
            assert_eq!(unique.len(), codes.len(), "{run:?}: a code listed twice");
            for code in codes {
                assert!(is_reason_code(code), "{code}");
                assert!(code.len() <= 30, "the line bound's note quotes 30: {code}");
                assert_eq!(run.reason_code(code), Some(*code));
            }
            assert!(
                codes.contains(&DEFAULT_REASON_CODE),
                "{run:?} has the default"
            );
            // Every state `refusal-reason=` can name is a code of a restore;
            // a backup has the four it can reach.
        }
        for state in crate::guard::TERMINAL_STATES {
            assert!(RESTORE_REASON_CODES.contains(&state), "{state}");
        }
        let restore: std::collections::BTreeSet<&str> = RESTORE_REASON_CODES.into_iter().collect();
        let backup: std::collections::BTreeSet<&str> = BACKUP_REASON_CODES.into_iter().collect();
        assert_eq!(
            restore.intersection(&backup).copied().collect::<Vec<_>>(),
            vec![
                "CredentialBindingMismatch",
                "CredentialNotRenderable",
                "GuardRefused",
                "PlainWithoutTls",
                "StorageRegionInvalid",
            ],
            "what both kinds of run can print"
        );
        assert_eq!(
            restore.union(&backup).count(),
            17,
            "the exit-3 sweep's count of distinct codes"
        );
        // PER KIND: a code of one is not a code of the other.
        for only_restore in restore.difference(&backup) {
            assert_eq!(Backup.reason_code(only_restore), None, "{only_restore}");
        }
        for only_backup in backup.difference(&restore) {
            assert_eq!(Restore.reason_code(only_backup), None, "{only_backup}");
        }
        // And membership is exact: no prefix, no other case, no padding.
        for near in [
            "GuardRefuse",
            "GuardRefusedX",
            "guardrefused",
            " GuardRefused",
            "GuardRefused ",
        ] {
            assert_eq!(Restore.reason_code(near), None, "{near:?}");
            assert_eq!(Backup.reason_code(near), None, "{near:?}");
        }
    }

    #[test]
    fn a_detail_and_a_state_agree_or_the_pair_is_not_the_runners() {
        // What the runner prints beside each detail: the state
        // `guard::terminal_state` derives from the SAME message.
        for message in [
            "TargetTopicConfigRefused: cleanup.policy is `compact`",
            "PointBindingMismatch. The plan is bound to another point",
            "CredentialNotRenderable. named with a full stop, so the state line says GuardRefused",
            "restore.partitions.orders names partition 99",
            "",
        ] {
            let detail = RefusalDetail::from_refusal_message(Restore, message);
            let state = crate::guard::terminal_state(message);
            assert!(
                detail.agrees_with_state(state),
                "{message:?}: code {} beside state {state}",
                detail.code()
            );
        }
        // A state that is neither the code nor the default was not printed
        // beside this detail.
        let detail = RefusalDetail::from_refusal_message(Restore, "PointUntrusted. x");
        assert!(detail.agrees_with_state("GuardRefused"));
        assert!(detail.agrees_with_state("PointUntrusted"));
        for other in ["TargetTopicConfigRefused", "Succeeded", "", "guardrefused"] {
            assert!(!detail.agrees_with_state(other), "{other:?}");
        }
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
                RefusalDetail::from_line(Restore, &line(bad, "a sentence")),
                None,
                "{bad:?}"
            );
        }
        // PATTERN-SHAPED IS NOT ENOUGH: a well-formed word outside the closed
        // set is not a code, and neither is the other kind's.
        for unknown in [
            "A",
            "Succeeded",
            "PartitionSubsetsAwaitOwnerDecision",
            "GuardRefusedUnknownReason",
            "ConsumerGroupIdInvalid",
            "WorkloadIdentityNotInjected",
        ] {
            assert!(
                is_reason_code(unknown),
                "the control: {unknown} IS pattern-shaped"
            );
            assert_eq!(
                RefusalDetail::from_line(Restore, &line(unknown, "a sentence")),
                None,
                "{unknown}"
            );
        }
        for restore_only in [
            "PointUntrusted",
            "TargetTopicConfigRefused",
            "AuthorizationExpired",
        ] {
            assert_eq!(
                RefusalDetail::from_line(Backup, &line(restore_only, "a sentence")),
                None,
                "{restore_only}"
            );
            assert!(RefusalDetail::from_line(Restore, &line(restore_only, "a sentence")).is_some());
        }
        // NEGATIVE CONTROL: every member of each set is read, for its kind.
        for (run, codes) in [
            (Restore, &RESTORE_REASON_CODES[..]),
            (Backup, &BACKUP_REASON_CODES[..]),
        ] {
            for code in codes {
                let read = RefusalDetail::from_line(run, &line(code, "a sentence"))
                    .unwrap_or_else(|| panic!("{run:?} reads its own code {code}"));
                assert_eq!(read.code(), *code);
            }
        }
    }

    #[test]
    fn only_the_exact_document_is_read() {
        let p = REFUSAL_DETAIL_PREFIX;
        for bad in [
            // not this key, or not at the start of the line
            r#"refusal-reason={"code":"GuardRefused","message":"m"}"#.to_string(),
            format!(r#" {p}{{"code":"GuardRefused","message":"m"}}"#),
            format!(r#"x {p}{{"code":"GuardRefused","message":"m"}}"#),
            // not an object, a member missing, a member of the wrong type
            format!("{p}GuardRefused: a sentence"),
            format!(r#"{p}"a string""#),
            format!(r#"{p}["GuardRefused","m"]"#),
            format!(r#"{p} {{"code":"GuardRefused","message":"m"}}"#),
            format!(r#"{p}{{"code":"GuardRefused"}}"#),
            format!(r#"{p}{{"message":"m"}}"#),
            format!(r#"{p}{{"code":1,"message":"m"}}"#),
            format!(r#"{p}{{"code":"GuardRefused","message":["m"]}}"#),
            format!(r#"{p}{{"code":"GuardRefused","message":null}}"#),
            // a third member, a repeated member, trailing text
            format!(r#"{p}{{"code":"GuardRefused","message":"m","remedy":"r"}}"#),
            format!(r#"{p}{{"code":"GuardRefused","message":"m","message":"n"}}"#),
            format!(r#"{p}{{"code":"GuardRefused","code":"PointUntrusted","message":"m"}}"#),
            format!(r#"{p}{{"code":"GuardRefused","message":"m"}} and more"#),
            format!(
                r#"{p}{{"code":"GuardRefused","message":"m"}}{{"code":"PointUntrusted","message":"n"}}"#
            ),
            // cut short, or empty
            format!(r#"{p}{{"code":"GuardRefused","message":"m"#),
            p.to_string(),
        ] {
            assert_eq!(RefusalDetail::from_line(Restore, &bad), None, "{bad}");
        }
        let good = format!(r#"{p}{{"code":"GuardRefused","message":"m"}}"#);
        assert!(RefusalDetail::from_line(Restore, &good).is_some());
        // Member order is not part of the document.
        let swapped = format!(r#"{p}{{"message":"m","code":"GuardRefused"}}"#);
        assert_eq!(
            RefusalDetail::from_line(Restore, &swapped),
            RefusalDetail::from_line(Restore, &good)
        );
    }

    #[test]
    fn an_overlong_line_is_not_read() {
        // One mebibyte on one line: not parsed, whatever it holds.
        let huge = line("GuardRefused", &prose(1024 * 1024));
        assert!(huge.len() > 1024 * 1024);
        assert_eq!(RefusalDetail::from_line(Restore, &huge), None);
        // The bound is on the LINE: at it the line is read (and its sentence
        // cut), one byte over it is not.
        let overhead = line("GuardRefused", "").len();
        let at = line(
            "GuardRefused",
            &prose(REFUSAL_DETAIL_LINE_MAX_BYTES - overhead),
        );
        assert_eq!(at.len(), REFUSAL_DETAIL_LINE_MAX_BYTES);
        let read = RefusalDetail::from_line(Restore, &at).expect("a line at the bound is read");
        assert!(read.message().ends_with(TRUNCATION_MARKER));
        assert_clean(read.message());
        let over = line(
            "GuardRefused",
            &prose(REFUSAL_DETAIL_LINE_MAX_BYTES - overhead + 1),
        );
        assert_eq!(over.len(), REFUSAL_DETAIL_LINE_MAX_BYTES + 1);
        assert_eq!(RefusalDetail::from_line(Restore, &over), None);
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
            let text = format!("{}{}", "a".repeat(pad), "—".repeat(600));
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
        // The cut comes AFTER the rules: a secret straddling the bound is
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
        let read = RefusalDetail::from_line(Restore, &line("GuardRefused", hostile))
            .expect("cleaned, not refused");
        assert_eq!(
            read.message(),
            format!("restore.partitions [2J names {REPLACEMENT}<script>alert(1)</script>")
        );
        assert_clean(read.message());
        // Lossily decoded invalid UTF-8 is one replacement, not a failure.
        let lossy = String::from_utf8_lossy(b"bad \xFF\xFE bytes").into_owned();
        let read =
            RefusalDetail::from_line(Restore, &line("GuardRefused", &lossy)).expect("a line");
        assert_eq!(read.message(), format!("bad {REPLACEMENT} bytes"));
    }

    #[test]
    fn the_runners_line_never_exceeds_the_bound_the_reader_enforces() {
        // The worst case for JSON: every byte of the sentence doubles. Under
        // the longest code of each set, and under a 64-byte word that is NOT
        // a code and so stays in the sentence.
        let longest = |codes: &[&'static str]| {
            codes
                .iter()
                .copied()
                .max_by_key(|c| c.len())
                .expect("a set is not empty")
        };
        let openings = [
            (Restore, longest(&RESTORE_REASON_CODES).to_string()),
            (Backup, longest(&BACKUP_REASON_CODES).to_string()),
            (Restore, "A".repeat(64)),
        ];
        for (run, opening) in openings {
            for filler in ["\"", "\\", "ab ", "—", "\u{202E}x", "\n"] {
                let message = format!("{opening}: x{}", filler.repeat(4000));
                let line = refusal_detail_line(run, &message);
                assert!(
                    line.len() <= REFUSAL_DETAIL_LINE_MAX_BYTES,
                    "{filler:?}: {} bytes",
                    line.len()
                );
                assert!(line.len() <= 1623, "the figure the bound's note quotes");
                assert!(!line.contains('\n') && !line.contains('\r'));
                let read = RefusalDetail::from_line(run, &line).expect("the reader accepts it");
                assert_eq!(read.to_line(), line, "and reading it changes nothing");
                assert!(run.reason_codes().contains(&read.code()));
            }
        }
    }
}
