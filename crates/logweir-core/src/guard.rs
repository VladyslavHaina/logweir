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

/// The longest name a Kafka broker accepts for a topic.
///
/// 249 and not 255: the broker reserves the remainder for the `-<partition>`
/// suffix it appends when it names the topic's log directory on disk.
pub const MAX_TOPIC_NAME_CHARS: usize = 249;

/// Whether `name` is a name a Kafka broker would accept —
/// `^[a-zA-Z0-9._-]{1,249}$`, the pattern
/// `weirkeeper::crds::selection::TOPIC_NAME_PATTERN` puts on every CRD field
/// that holds one.
///
/// # Why this is a second rail beside [`reject_glob_metacharacters`]
///
/// The two answer different questions. The glob rail asks "would the engine's
/// selector read this as a PATTERN", and its six characters are the ones that
/// silently widen one named entry into a set. This asks "is this a name at
/// all" — and it is the rail that matters for a list produced by something
/// other than a human typing into a CRD field. A drifted, older or hostile
/// runner can relay `orders eu`, a 400-character name, or a name carrying a
/// control character; every one of those passes the glob rail, freezes into an
/// immutable plan, and then fails opaquely inside the engine instead of being
/// refused by name.
///
/// Written as a character predicate and not a regex on purpose: this crate has
/// no regex dependency, the grammar is four ASCII classes, and a hand-written
/// predicate cannot be defeated by a `.` that matches a newline or by a
/// multiline anchor.
#[must_use]
pub fn topic_name_is_kafka_legal(name: &str) -> bool {
    // ASCII-only by construction: every accepted character is one byte, so a
    // count of characters and a count of bytes are the same number and the
    // broker's own limit applies either way.
    !name.is_empty()
        && name.len() <= MAX_TOPIC_NAME_CHARS
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

/// The list rail: returns the FIRST entry that is not a Kafka-legal topic
/// name, verbatim.
///
/// The same shape as [`reject_glob_metacharacters`] and for the same reason —
/// the caller owns the wording, and the entry an operator has to go and fix is
/// what has to come back. Callers that put the entry into a status message
/// bound and redact it there; this function neither truncates nor quotes.
pub fn reject_non_kafka_topic_names(entries: &[String]) -> Result<(), String> {
    for entry in entries {
        if !topic_name_is_kafka_legal(entry) {
            return Err(entry.clone());
        }
    }
    Ok(())
}

/// The five characters a projected password may not contain, and interface
/// **I11**'s producer half.
///
/// # Why escaping cannot be the answer here
///
/// The password is NOT in the bytes Logweir renders. `crate::spec::AuthSpec`
/// carries no secret and `crate::engine::AuthRender` says so in its own doc
/// comment: the rendered document names `${LOGWEIR_SOURCE_PASSWORD}` and the
/// engine substitutes the value out of its OWN process environment, as raw
/// text, BEFORE the document is parsed as YAML
/// [U:crates/kafka-backup-cli/src/commands/config.rs:1-34]. So there is no
/// interpolation site to escape at: by the time the value meets the document,
/// every escaper in this workspace has already run. The value itself has to be
/// safe to substitute into pre-parse text, and this is the predicate that says
/// so.
///
/// A `\n` in the value ends the physical line and whatever follows it becomes
/// a NEW YAML key at whatever indentation it carries — `sasl_password:
/// "hunter2\ndry_run: true"` is two keys, not one. `\r` does the same on a
/// CRLF reader. `"` closes the double-quoted scalar the placeholder sits
/// inside, and `'` closes a single-quoted one, so either can end the scalar
/// early and leave the rest of the value as document structure. `$` is
/// refused because upstream's expansion pass is not idempotent-safe: a value
/// containing `${OTHER}` is itself scanned for expansion on some readings of
/// that pass, and a password that can name an environment variable is a
/// password that can read one.
///
/// # Where it is called, and where it is DELIBERATELY NOT called
///
/// **The runner** calls it, on the projected value, at the moment it reads
/// `LOGWEIR_SOURCE_PASSWORD`/`LOGWEIR_TARGET_PASSWORD`
/// (`crates/logweir/src/drill/mod.rs::check_projected_credentials`). The
/// CONTROLLER does not: `weirkeeper` holds no `get` on Secrets anywhere
/// (spec §9), so it never sees the projected value and has nothing to
/// validate. A predicate the controller re-used would be a check performed on
/// a value the controller does not have — see spec §7 amendment 4, "the runner
/// refuses, not the controller", and critique B H11.
pub const UNRENDERABLE_CREDENTIAL_CHARACTERS: [char; 5] = ['\n', '\r', '"', '\'', '$'];

/// The one reason string every `CredentialRefusal` carries. It names the
/// MECHANISM, never the value.
const CREDENTIAL_REFUSAL_REASON: &str = "a projected password is substituted into the config \
     text before it is parsed, so a newline or a quote in the value can introduce a new YAML key";

/// A refused credential: the offending character and the mechanism.
///
/// **It never carries the value, and its `Display` is a pure function of
/// `character`.** That is a stronger property than "does not print the
/// secret": two different unrenderable passwords with the same first
/// offending character produce byte-identical messages, so the message cannot
/// carry a fragment of either one, and `tests/guard.rs` asserts exactly that
/// rather than asserting the absence of a list of substrings somebody has to
/// keep complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRefusal {
    pub character: char,
    pub reason: &'static str,
}

impl CredentialRefusal {
    /// The character CLASS, as prose. A refusal that printed the character
    /// itself would be printing one byte of the secret, and for `\n` and `\r`
    /// it would print something invisible in a log line.
    fn class(&self) -> &'static str {
        match self.character {
            '\n' => "a newline",
            '\r' => "a carriage return",
            '"' => "a double quote",
            '\'' => "a single quote",
            '$' => "a dollar sign",
            // Unreachable while `UNRENDERABLE_CREDENTIAL_CHARACTERS` is the
            // only source of this type, and deliberately value-free if the
            // list ever grows without this match growing with it.
            _ => "a character this build cannot render",
        }
    }
}

impl std::fmt::Display for CredentialRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the projected credential contains {}, which is not renderable; {}",
            self.class(),
            self.reason
        )
    }
}

impl std::error::Error for CredentialRefusal {}

/// Interface **I11**: a password that will be textually substituted into a
/// pre-parse YAML template. See `UNRENDERABLE_CREDENTIAL_CHARACTERS` for the
/// full argument.
///
/// Returns the FIRST offending character in scan order, so a value that opens
/// with `"` is reported as a double quote even when it also carries a
/// newline — the operator fixes the value, and the first thing wrong with it
/// is the thing to name.
pub fn credential_is_renderable(secret: &str) -> Result<(), CredentialRefusal> {
    for ch in secret.chars() {
        if UNRENDERABLE_CREDENTIAL_CHARACTERS.contains(&ch) {
            return Err(CredentialRefusal {
                character: ch,
                reason: CREDENTIAL_REFUSAL_REASON,
            });
        }
    }
    Ok(())
}

/// [I9] The runner's machine-readable refusal discriminator, and the default.
///
/// A guard refusal that names no more specific state is this one, which is why
/// `terminal_state` returns it rather than an `Option`: a controller reading
/// `refusal-reason=` must always get a state, and "the message did not open
/// with a state name" is not a different fact about the run — it is a plain
/// guard refusal.
pub const TERMINAL_STATE_GUARD_REFUSED: &str = "GuardRefused";
/// [I9] Spec §3.2's terminal state for a projected credential that cannot be
/// substituted into the config text (`credential_is_renderable`). Its call
/// site is Task 6's; the runner's own read
/// (`crates/logweir/src/drill/mod.rs::check_projected_credentials`) already
/// emits it.
pub const TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE: &str = "CredentialNotRenderable";
/// [I9] Spec §3.2's terminal state for a target topic whose configuration the
/// drill refuses to write to. Its call site is **Task 8**'s; the constant
/// lives here so the three states are one list and a controller's mapping can
/// be written against it before the third producer lands.
pub const TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED: &str = "TargetTopicConfigRefused";
/// [I9] Every tag-1 terminal state a guard refusal can name. **Task 20**
/// (Phase B) is the only consumer.
pub const TERMINAL_STATES: [&str; 3] = [
    TERMINAL_STATE_GUARD_REFUSED,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
];

/// [I9] A refusal message MAY open with `<State>: `, naming one of
/// `TERMINAL_STATES`. Returns that state, or `TERMINAL_STATE_GUARD_REFUSED`
/// when the message opens with none of them.
///
/// **It matches a PREFIX and never a substring**, and the separator is part of
/// the prefix. A message that merely MENTIONS a state name is not that state:
/// `"the projected password is not CredentialNotRenderable-safe"` is a plain
/// guard refusal, and a `contains` match would classify it as the credential
/// state and tell a controller that a Secret is malformed when nothing about a
/// Secret was observed. Requiring `": "` also stops
/// `"CredentialNotRenderableXyz: …"` — a state name that is a prefix of a
/// longer word — from matching.
pub fn terminal_state(message: &str) -> &'static str {
    for state in TERMINAL_STATES {
        if let Some(rest) = message.strip_prefix(state) {
            if rest.starts_with(": ") {
                return state;
            }
        }
    }
    TERMINAL_STATE_GUARD_REFUSED
}

/// [I9] `refusal-reason=<state>`.
///
/// Pure, and here rather than in the binary, because `logweir-core` does no
/// I/O (`crate` doc, `lib.rs:1`): this crate produces the LINE and
/// `crates/logweir/src/exit.rs::print_refusal_reason` prints it. Splitting it
/// that way is what lets the line's format be unit-tested without a process,
/// and what stops a `println!` appearing in the pure layer.
pub fn refusal_reason_line(message: &str) -> String {
    format!("refusal-reason={}", terminal_state(message))
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

    /// **A NAME THAT IS NOT A KAFKA NAME IS REFUSED BEFORE IT IS FROZEN** —
    /// D1 §3.3's "Kafka-legal names `^[a-zA-Z0-9._-]{1,249}$` are re-validated
    /// before freeze".
    ///
    /// The rail exists for the list that does NOT come from a CRD field: a
    /// dynamic selection takes its names from a runner's stdout, and the glob
    /// rail does not cover a space, a slash, a control character or a
    /// 400-character name.
    ///
    /// KILLS: accepting a name with a space, a colon, a slash or a newline;
    /// accepting an empty name; using a byte-count bound that admits 250
    /// characters, or one that rejects exactly 249.
    #[test]
    fn the_kafka_name_predicate_admits_exactly_the_broker_grammar() {
        for good in [
            "orders",
            "orders.v2",
            "a-b_c",
            "A1",
            "_",
            "-",
            ".",
            &"x".repeat(MAX_TOPIC_NAME_CHARS),
        ] {
            assert!(
                topic_name_is_kafka_legal(good),
                "`{good}` is a Kafka-legal topic name"
            );
        }
        for bad in [
            "",
            "orders eu",
            "orders/eu",
            "orders:eu",
            "orders\n",
            "orders\u{7f}",
            "ordërs",
            "orders*",
            &"x".repeat(MAX_TOPIC_NAME_CHARS + 1),
        ] {
            assert!(
                !topic_name_is_kafka_legal(bad),
                "`{}` is not a Kafka-legal topic name",
                bad.escape_debug()
            );
        }

        // The list rail reports the FIRST offender verbatim, and a clean
        // prefix does not shadow a later one.
        assert_eq!(
            reject_non_kafka_topic_names(&["orders".into(), "pay ments".into(), "z\n".into()]),
            Err("pay ments".to_string())
        );
        assert_eq!(
            reject_non_kafka_topic_names(&["orders".into(), "payments".into()]),
            Ok(())
        );
        assert_eq!(reject_non_kafka_topic_names(&[]), Ok(()));

        // The two rails are INDEPENDENT: neither is a superset of the other.
        assert!(reject_glob_metacharacters(&["orders eu".to_string()]).is_ok());
        assert!(reject_non_kafka_topic_names(&["orders*".to_string()]).is_err());
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
