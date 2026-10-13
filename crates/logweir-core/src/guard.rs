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
/// not scan, and it returns `Err` rather than an empty list: "I found nothing"
/// over text it never read would be indistinguishable from a clean scan.
///
/// A BACKSTOP over the adopter's SPEC TEXT (`phase0_admit::run`) and over
/// rendered documents in the renderer's tests. Global Constraint 4 / GR7 (the
/// three keys never emitted on any argv or in any rendered YAML) is enforced
/// STRUCTURALLY by `logweir-engine-oso`'s renderers, which name none of the
/// keys and escape every interpolated value through `yaml_scalar`.
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

/// The plan's `notifications` block is withdrawn: a run posts nothing to a
/// webhook, Slack or PagerDuty. An absent or empty block parses and passes; a
/// block that names a sink is refused by name, never silently ignored, so a
/// plan written for the old behaviour does not run believing it will page.
///
/// # Errors
///
/// [`GuardRefusal`] naming `notifications` when any sink is set.
pub fn reject_withdrawn_notifications(
    notifications: &crate::spec::Notifications,
) -> Result<(), GuardRefusal> {
    if notifications.is_empty() {
        return Ok(());
    }
    Err(GuardRefusal(
        "notifications is withdrawn: a run no longer posts to a webhook, Slack or PagerDuty, \
         so a plan that names a sink (notifications.webhooks, slack_webhook, \
         pagerduty_routing_key or pagerduty_endpoint) is refused. Remove the sinks (an empty \
         `notifications: {webhooks: []}` is accepted) and approve the plan again"
            .to_string(),
    ))
}

/// The six characters upstream's topic selector treats as GLOB PATTERN SYNTAX
/// rather than as part of a name.
///
/// `TopicSelection.include`/`exclude` "supports glob patterns"
/// [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], and both
/// renderers put operator-chosen names into `topics.include`, so a topic NAMED
/// `orders*` or `events[1]` would silently widen to a set. `*`/`?`, `[`/`]` (a
/// character class) and `{`/`}` (a brace alternation) are all refused, closing
/// halves included: the question is whether the string is a plain name, not the
/// glob dialect's grammar.
pub const GLOB_METACHARACTERS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

/// GC18(c) rail 1, and guard **G-GLOB**: no topic include entry may contain a
/// glob metacharacter. Shared by `render_backup::render` (over
/// `BackupPlan::topics`) and `render_restore::render` (over BOTH sides of
/// `topic_mapping`), so the two renderers cannot disagree.
///
/// Returns the FIRST offending entry verbatim; the caller owns the wording
/// (`render_backup::RenderError::GlobMetacharacter`). It does not quote or
/// escape: quoting is YAML's concern (`yaml_scalar`), and a quoted `"orders*"`
/// is still a glob to the engine's selector.
pub fn reject_glob_metacharacters(entries: &[String]) -> Result<(), String> {
    for entry in entries {
        if entry.chars().any(|c| GLOB_METACHARACTERS.contains(&c)) {
            return Err(entry.clone());
        }
    }
    Ok(())
}

/// **C15 at the spec layer**: a spec whose storage is an
/// `http://` endpoint with `allow_http: false` is refused before anything
/// runs, with exit 3, like destination rule R3. The renderers refuse the same
/// combination again for every engine document
/// (`logweir_engine_oso::render_restore::render_storage_block`), but a
/// renderer refusal is reached after phase 0 and is exit 1.
///
/// The message names the field and never the endpoint's value: an endpoint
/// string is operator input that could carry userinfo.
pub fn reject_plaintext_endpoint_without_allow_http(
    field: &str,
    storage: &crate::engine::StorageUrl,
) -> Result<(), GuardRefusal> {
    if storage.plaintext_endpoint_without_allow_http() {
        return Err(GuardRefusal(format!(
            "{field}.endpoint is a plain http:// endpoint but {field}.allow_http is false. \
             The pinned engine (kafka-backup 0.22.0 and later) derives plaintext transport \
             from an http:// endpoint whatever allow_http says, so it would dial the archive \
             in the clear although the spec asked for no plaintext. Set allow_http: true to \
             state plaintext explicitly, or use an https:// endpoint."
        )));
    }
    Ok(())
}

/// The named reason a storage location whose S3 `region` is not a region name
/// is refused. It names the message, not a terminal state: the
/// `refusal-reason=` line stays `GuardRefused`, as `PointBindingSetMismatch`
/// does.
pub const STORAGE_REGION_INVALID: &str = "StorageRegionInvalid";

/// Refuses a spec storage block whose S3 `region` does not match
/// [`crate::engine::S3_REGION_PATTERN`], before any client exists: without an
/// endpoint the region is part of the host (`s3.<region>.amazonaws.com`), so
/// `x@attacker.example/` would send the credential's signed requests elsewhere.
/// The location binding covers the region too; this is the second, independent
/// rule.
///
/// The message names the field and never the value: a region crafted as a
/// userinfo could carry a credential.
pub fn reject_invalid_storage_region(
    field: &str,
    storage: &crate::engine::StorageUrl,
) -> Result<(), GuardRefusal> {
    if storage.has_invalid_region() {
        return Err(GuardRefusal(format!(
            "{STORAGE_REGION_INVALID}: {field}.region is not an S3 region name: it must match \
             {}. Without an endpoint the region is part of the host the request is sent to, \
             so any other spelling could carry the archive credential's signed requests to \
             another host; the value is not repeated here. No client was built.",
            crate::engine::S3_REGION_PATTERN
        )));
    }
    Ok(())
}

/// **SASL/PLAIN only over TLS.** Refuses a spec's auth block whose
/// mode is `plain` without `tls: true` — the message opens with
/// `PlainWithoutTls: `, so the runner's `refusal-reason=` line names it
/// ([`TERMINAL_STATE_PLAIN_WITHOUT_TLS`]) — and `mtls` without `tls: true`
/// (a plain guard refusal). Called by backup phase −1 for `source.auth` and by
/// drill/restore phase 0 for `target.auth`, both before any client exists, so
/// a PLAIN password is never sent anywhere by a refused plan. `AuthConfig::
/// from_spec` and the engine renderer refuse the same shapes as backstops.
///
/// The message names the field and the mode, never a username or a password.
pub fn reject_auth_without_required_tls(
    field: &str,
    auth: &crate::spec::AuthSpec,
) -> Result<(), GuardRefusal> {
    match auth.transport_refusal() {
        // The refusal text OPENS with the state for `plain`, which is what
        // `terminal_state` matches on; the field is appended after it.
        Some(text) => Err(GuardRefusal(format!("{text} ({field}.mode, {field}.tls)"))),
        None => Ok(()),
    }
}

/// The longest name a Kafka broker accepts for a topic.
///
/// 249 and not 255: the broker reserves the remainder for the `-<partition>`
/// suffix it appends when it names the topic's log directory on disk.
pub const MAX_TOPIC_NAME_CHARS: usize = 249;

/// Whether `name` is a name a Kafka broker would accept —
/// `^[a-zA-Z0-9._-]{1,249}$`, the pattern
/// `weirkeeper::crds::selection::TOPIC_NAME_PATTERN` puts on every CRD field
/// that holds one — and not `.` or `..`, which that pattern admits and Kafka
/// refuses (`Topic.validate`: "Topic name cannot be \".\" or \"..\"").
///
/// # A second rail beside [`reject_glob_metacharacters`]
///
/// The glob rail asks whether the engine would read a name as a PATTERN; this
/// asks whether it is a name at all, which matters for a list a runner relays
/// rather than a human types: `orders eu`, a 400-character name or a control
/// character pass the glob rail and would fail opaquely inside the engine. A
/// character predicate, not a regex: no regex dependency, and no `.` or
/// multiline anchor to get wrong.
#[must_use]
pub fn topic_name_is_kafka_legal(name: &str) -> bool {
    // ASCII-only by construction: every accepted character is one byte, so a
    // count of characters and a count of bytes are the same number and the
    // broker's own limit applies either way.
    !name.is_empty()
        && name.len() <= MAX_TOPIC_NAME_CHARS
        && name != "."
        && name != ".."
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
/// # Why escaping cannot be the answer
///
/// The password is NOT in the bytes Logweir renders: the document names
/// `${LOGWEIR_SOURCE_PASSWORD}` and the engine substitutes the value from its
/// OWN environment, as raw text, BEFORE parsing the YAML
/// [U:crates/kafka-backup-cli/src/commands/config.rs:1-34]. So the value itself
/// must be safe in pre-parse text. `\n` (and `\r`) would start a NEW YAML key
/// (`"hunter2\ndry_run: true"` is two keys); `"` and `'` close the quoted
/// scalar the placeholder sits in; and `$` could name another environment
/// variable on some readings of the expansion pass.
///
/// # Where it is called
///
/// **The runner** calls it on the projected value when it reads
/// `LOGWEIR_SOURCE_PASSWORD`/`LOGWEIR_TARGET_PASSWORD`
/// (`crates/logweir/src/drill/mod.rs::check_projected_credentials`). The
/// CONTROLLER does not: `weirkeeper` holds no `get` on Secrets, so it never
/// sees the value.
pub const UNRENDERABLE_CREDENTIAL_CHARACTERS: [char; 5] = ['\n', '\r', '"', '\'', '$'];

/// The one reason string every `CredentialRefusal` carries. It names the
/// MECHANISM, never the value.
const CREDENTIAL_REFUSAL_REASON: &str = "a projected password is substituted into the config \
     text before it is parsed, so a newline or a quote in the value can introduce a new YAML key";

/// A refused credential: the offending character and the mechanism.
///
/// **It never carries the value, and its `Display` is a pure function of
/// `character`**, so two passwords with the same first offending character
/// produce byte-identical messages, which cannot carry a fragment of either
/// (`tests/guard.rs` asserts exactly that).
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
/// [I9] The terminal state for a projected credential that cannot be
/// substituted into the config text (`credential_is_renderable`, read by
/// `crates/logweir/src/drill/mod.rs::check_projected_credentials`).
pub const TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE: &str = "CredentialNotRenderable";
/// [I9] The terminal state for a target topic whose configuration the drill
/// refuses to write to.
pub const TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED: &str = "TargetTopicConfigRefused";
/// [I9] A time selection (a stated `restore.point_in_time`, or a
/// `sample.window_end` that cuts the archive) over a source topic whose
/// recorded timestamp type is `LogAppendTime`, in a plan that does not state
/// `restore.time_basis: producerTime`. The archive holds producer time only,
/// so the selection would read the producers' clocks instead of the broker's.
/// Its call site is `crate::time_basis::decide`; refused after the archive is
/// described and before any target topic is created.
pub const TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME: &str = "PointInTimeByProducerTime";
/// [I9] A connection whose auth mode is `plain` (SASL/PLAIN)
/// without `tls: true`. PLAIN sends the password itself, so the plan is
/// refused before any client exists rather than dialled in the clear. The
/// message is [`crate::connection::PlainWithoutTls`]'s, which opens with this
/// state; its call sites are the runner's guards over a spec's auth
/// (`crate::spec::AuthSpec::transport_refusal`).
pub const TERMINAL_STATE_PLAIN_WITHOUT_TLS: &str = crate::connection::PLAIN_WITHOUT_TLS;
/// [I9] A projected credential Secret whose `logweir-binding` is absent or
/// names another connection or endpoint
/// (`crate::connection::CredentialBindingRefusal`). Refused before any client
/// exists, so a credential is never presented to brokers it was not entered
/// for.
pub const TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH: &str =
    crate::connection::CREDENTIAL_BINDING_MISMATCH;
/// [I9] The restored partitions' archive holds records shaped
/// like transaction control records, and the plan does not state
/// `restore.allow_transaction_markers: true`. Its call site is
/// `crate::transaction_markers::refusal`; refused before phase 2.
pub const TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE: &str = "TransactionMarkersInArchive";
/// [I9] The restore window selects archived records in a segment the engine
/// skips: its first and last record timestamps do not overlap the window, and
/// its records are out of timestamp order. Its call site is
/// `crate::record_times::refusal`; refused before phase 2.
pub const TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE: &str =
    "PointInTimeSelectionIncomplete";
/// [I9] Every terminal state a guard refusal can name.
pub const TERMINAL_STATES: [&str; 8] = [
    TERMINAL_STATE_GUARD_REFUSED,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
    TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
    TERMINAL_STATE_POINT_IN_TIME_BY_PRODUCER_TIME,
    TERMINAL_STATE_PLAIN_WITHOUT_TLS,
    TERMINAL_STATE_CREDENTIAL_BINDING_MISMATCH,
    TERMINAL_STATE_TRANSACTION_MARKERS_IN_ARCHIVE,
    TERMINAL_STATE_POINT_IN_TIME_SELECTION_INCOMPLETE,
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

/// [I9] `refusal-reason=<state>`. Pure: this crate produces the LINE and
/// `crates/logweir/src/exit.rs::print_refusal` prints it, so the format is
/// unit-tested without a process.
pub fn refusal_reason_line(message: &str) -> String {
    format!("refusal-reason={}", terminal_state(message))
}

/// A backup run whose execution was already claimed by an
/// earlier run: exit 1, no engine run, nothing signed. Retryable, because a
/// retry is a NEW execution id.
pub const TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED: &str = "ExecutionAlreadyClaimed";
/// A backup run whose execution claim the evidence store could
/// not prove exclusive (put refused, conditional create unsupported or
/// ignored): exit 4, no engine run, nothing signed. Not retryable.
pub const TERMINAL_STATE_EXECUTION_CLAIM_UNPROVEN: &str = "ExecutionClaimUnproven";

/// A restore whose creation step lost a race: a mapped target name phase 0
/// proved absent existed when the run came to create it. Exit 1 (phases 0–5
/// ran); the runner names what appeared and what it created and LEFT on its
/// `target-topics-appeared=` line. Nothing is deleted.
pub const TERMINAL_STATE_TARGET_TOPIC_APPEARED: &str = crate::original_name::TARGET_TOPIC_APPEARED;

/// A restore whose creation step stopped for a reason other than a race and
/// LEFT topics on the cluster: ones this execution created (a broker refused
/// another name, answered short, or did not serve a created topic in time), or
/// ones it asked for, got no definite answer about, and cannot account for.
/// Exit 1; each is named on `target-topics-appeared=` (`left`, `unconfirmed`)
/// and left in place.
pub const TERMINAL_STATE_CREATED_TOPICS_LEFT: &str = "CreatedTopicsLeft";

/// What EVERY surface says about a topic a stopped creation step created and
/// left — the runner's message, the Restore's status, the product API and the
/// console. One sentence, in one place: it is the whole instruction the
/// operator gets, because Logweir itself never deletes such a topic (Kafka
/// has no conditional delete, so a record a producer wrote between an "it is
/// empty" read and the delete would be lost under a production name).
pub const LEFT_TOPIC_SENTENCE: &str =
    "created by this restore and left empty; remove it yourself once you have checked nothing \
     writes to it";

/// What EVERY surface says about a topic in the THIRD list of a stopped
/// creation step, `unconfirmed`: a name this restore
/// asked the cluster to create, for which it got NO DEFINITE ANSWER (the whole
/// `CreateTopics` call failed, the name got no answer, or it got an error
/// that is not "already exists"), and which the cluster LISTED when the run
/// looked again. The run cannot say whose it is: its own request may have
/// been applied, or someone else may have created it. So this sentence never
/// claims ownership ([`LEFT_TOPIC_SENTENCE`] does) and never says someone
/// else made it (the `appeared` list's sentence does).
pub const UNCONFIRMED_TOPIC_SENTENCE: &str =
    "exists now; this restore asked the cluster to create it and got no definite answer, so it \
     may be this restore's or someone else's: check what it holds and who writes to it before \
     you remove it";

/// [`UNCONFIRMED_TOPIC_SENTENCE`]'s twin for a run that COULD NOT LIST the
/// cluster after the stop: every name it asked for without a definite answer
/// is named, and the sentence says it may exist, never that it does.
pub const UNCONFIRMED_UNLISTED_TOPIC_SENTENCE: &str =
    "may exist now; this restore asked the cluster to create it, got no definite answer, and \
     could not list the cluster afterwards: look for it, and check what it holds and who writes \
     to it before you remove it";

/// The stdout line a runner prints, LAST, when a non-refusal failure (exit 1
/// or 4) has a state more specific than its code — the exit-1/4 twin of I9's
/// `refusal-reason=`, and read by the controller the same way: by prefix, off
/// the log's bounded tail.
pub const FAILURE_REASON_PREFIX: &str = "failure-reason=";

/// Which runner's log a `failure-reason=` line is read from. **The closed
/// set is PER KIND**: one list for both reconcilers would let a Backup's log
/// lift a restore-only state onto a `Backup`, and a Restore's log a
/// backup-only one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReasonKind {
    /// `logweir backup run`: RECEIPT-DUP's two execution-claim states.
    Backup,
    /// `logweir restore run`: the stopped creation step's two states.
    Restore,
}

/// Every state a BACKUP's `failure-reason=` may carry, WITH the one exit code
/// it may accompany. A CLOSED list in both directions: a controller lifts a
/// value into `status.exitReason` only when the pair is on its kind's list,
/// so a noisy or newer runner cannot put an arbitrary string on the object,
/// and a claimed execution can never be reported under the exit code of an
/// unproven one.
pub const BACKUP_FAILURE_REASONS: [(&str, i32); 2] = [
    (TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED, 1),
    (TERMINAL_STATE_EXECUTION_CLAIM_UNPROVEN, 4),
];

/// Every state a RESTORE's `failure-reason=` may carry, each beside exit 1:
/// the stopped creation step's two. Closed the same way.
pub const RESTORE_FAILURE_REASONS: [(&str, i32); 2] = [
    (TERMINAL_STATE_TARGET_TOPIC_APPEARED, 1),
    (TERMINAL_STATE_CREATED_TOPICS_LEFT, 1),
];

impl FailureReasonKind {
    /// This kind's closed list of `(state, exit code)` pairs.
    #[must_use]
    pub fn reasons(self) -> &'static [(&'static str, i32)] {
        match self {
            FailureReasonKind::Backup => &BACKUP_FAILURE_REASONS,
            FailureReasonKind::Restore => &RESTORE_FAILURE_REASONS,
        }
    }
}

/// `failure-reason=<state>`. Pure, for the reason `refusal_reason_line` is.
pub fn failure_reason_line(state: &str) -> String {
    format!("{FAILURE_REASON_PREFIX}{state}")
}

/// The state a `failure-reason=` VALUE names, if the value is on `kind`'s
/// closed list for THIS exit code; `None` otherwise — a state of the other
/// kind included.
pub fn failure_reason_for_exit(
    kind: FailureReasonKind,
    exit_code: i32,
    value: &str,
) -> Option<&'static str> {
    kind.reasons()
        .iter()
        .find(|(state, code)| *code == exit_code && *state == value)
        .map(|(state, _)| *state)
}

/// Every selected topic must have a mapping entry whose target DIFFERS from
/// its source, or the restore would write over the topic it came from.
///
/// The ordinary rule — [`check_topic_mapping`] with no original-name opt-in.
pub fn check_topic_mapping_coverage(
    topics: &[String],
    mapping: &BTreeMap<String, String>,
) -> Result<(), GuardRefusal> {
    check_topic_mapping(topics, mapping, false)
}

/// Every selected topic must have a mapping entry, and:
///
/// * `original_name: false` (every restore but one) — the target DIFFERS from
///   its source, or the restore would write over the topic it came from;
/// * `original_name: true` (a plan that passed
///   [`crate::original_name::refuse_shape`]) — the target IS its source, for
///   EVERY topic. An original-name restore maps nothing partway: a mapping
///   that renames one topic and keeps another's name is two plans, and the
///   conditions the runner proves next (absence, the cluster, the owners) are
///   proved for the original names.
///
/// The identity mapping is never allowed by this function alone: the caller
/// passes `true` only for a plan whose shape opted in, and the absence and
/// cluster conditions are proved after it, before anything is written.
pub fn check_topic_mapping(
    topics: &[String],
    mapping: &BTreeMap<String, String>,
    original_name: bool,
) -> Result<(), GuardRefusal> {
    for t in topics {
        match mapping.get(t) {
            None => {
                return Err(GuardRefusal(format!(
                    "selected topic `{t}` has no topic_mapping entry"
                )))
            }
            Some(dst) if dst == t && !original_name => {
                return Err(GuardRefusal(format!(
                    "topic_mapping maps `{t}` onto itself; the target must differ from the source \
                     (a restore under the original topic names states \
                     target.topic_naming.original_name in newTopic mode)"
                )))
            }
            Some(dst) if dst != t && original_name => {
                return Err(GuardRefusal(format!(
                    "topic_mapping maps `{t}` to `{dst}` in an original-name restore; every \
                     topic of an original-name restore keeps its own name"
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
    /// Exercised against a constructed document, because no Logweir code path
    /// may be able to emit the key (no test or debug exception).
    ///
    /// The expected value is the DOTTED PATH `target.dry_run`: where the key
    /// was found is the only form an operator can act on.
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

    /// **A NAME THAT IS NOT A KAFKA NAME IS REFUSED BEFORE IT IS FROZEN.**
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
            // Dots are legal; only the two directory names are not.
            "...",
            ".a",
            "a..",
            &"x".repeat(MAX_TOPIC_NAME_CHARS),
        ] {
            assert!(
                topic_name_is_kafka_legal(good),
                "`{good}` is a Kafka-legal topic name"
            );
        }
        for bad in [
            "",
            // Kafka refuses exactly these two, which
            // the character grammar alone admits.
            ".",
            "..",
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
