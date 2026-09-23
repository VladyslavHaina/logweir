use logweir_core::guard::{
    check_topic_mapping_coverage, credential_is_renderable, refusal_reason_line,
    scan_forbidden_keys, terminal_state, CredentialRefusal, TERMINAL_STATES,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_GUARD_REFUSED,
    TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED, UNRENDERABLE_CREDENTIAL_CHARACTERS,
};
use std::collections::BTreeMap;

/// The guard is on the KEY, not the value: `purge_topics: false` is refused
/// exactly like `true` (spec §9.3 phase 0, GT-9).
#[test]
fn purge_topics_is_refused_at_either_value() {
    for v in ["true", "false"] {
        let doc = format!("restore:\n  purge_topics: {v}\n");
        assert_eq!(
            scan_forbidden_keys(&doc).unwrap(),
            vec!["restore.purge_topics".to_string()],
            "value {v}"
        );
    }
}

#[test]
fn dry_run_is_refused_at_either_value() {
    for v in ["true", "false"] {
        let doc = format!("restore:\n  dry_run: {v}\n");
        assert_eq!(
            scan_forbidden_keys(&doc).unwrap(),
            vec!["restore.dry_run".to_string()]
        );
    }
}

#[test]
fn header_preflight_external_is_refused_but_header_preflight_is_not() {
    assert_eq!(
        scan_forbidden_keys("restore:\n  header_preflight_external: true\n").unwrap(),
        vec!["restore.header_preflight_external".to_string()]
    );
    assert!(scan_forbidden_keys("restore:\n  header_preflight: full\n")
        .unwrap()
        .is_empty());
}

#[test]
fn a_forbidden_key_at_any_nesting_depth_is_found() {
    let doc = "a:\n  b:\n    c:\n      purge_topics: true\n";
    assert_eq!(
        scan_forbidden_keys(doc).unwrap(),
        vec!["a.b.c.purge_topics".to_string()]
    );
}

#[test]
fn a_clean_document_passes() {
    assert!(
        scan_forbidden_keys("restore:\n  create_topics: true\n  header_preflight: full\n")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn every_selected_topic_must_have_a_mapping_whose_target_differs() {
    let mut m = BTreeMap::new();
    m.insert("orders".to_string(), "drill-orders".to_string());
    assert!(check_topic_mapping_coverage(&["orders".into()], &m).is_ok());
    assert!(check_topic_mapping_coverage(&["orders".into(), "payments".into()], &m).is_err());

    let mut same = BTreeMap::new();
    same.insert("orders".to_string(), "orders".to_string());
    assert!(
        check_topic_mapping_coverage(&["orders".into()], &same).is_err(),
        "a mapping onto the same name would restore over the source topic"
    );
}

/// THE SCANNER FAILS CLOSED. A document it cannot parse is a document it did
/// not scan, and it must say so rather than answering "I found nothing" — the
/// predecessor returned an empty list, which is indistinguishable from a clean
/// document to every caller. This is the same "reported a pass having examined
/// nothing" shape the integrity chokepoint exists to prevent, in the guard
/// layer.
#[test]
fn a_document_the_scanner_cannot_parse_is_refused_never_reported_clean() {
    let err = scan_forbidden_keys("restore:\n  a: [unclosed\n\t\tbad: : :\n")
        .expect_err("unparseable YAML must not answer `no forbidden keys`");
    let msg = err.to_string();
    assert!(
        msg.contains("did not scan it"),
        "the refusal must say the document was not scanned, not merely that it was bad: {msg}"
    );
}

// --------------------------------------------------------------- I11 / I9
// The credential that cannot break out, and the refusal-reason contract.

/// Interface **I11**. The projected password is NOT in the bytes Logweir
/// renders: the document names `${LOGWEIR_SOURCE_PASSWORD}` and the engine
/// substitutes the value out of its own environment as RAW TEXT, before the
/// document is parsed [U:crates/kafka-backup-cli/src/commands/config.rs:1-34].
/// So there is no interpolation site to escape at — every escaper in this
/// workspace has already run by then — and the value itself has to be safe to
/// substitute into pre-parse text.
///
/// The named payload is the break-out: `"` closes the double-quoted scalar the
/// placeholder sits inside, the `\n` ends the physical line, and
/// `bootstrap_servers:` becomes a NEW YAML KEY at column 1. That is a password
/// choosing where the restore writes.
#[test]
fn credential_is_renderable_refuses_a_yaml_break_out() {
    // The brief's value, verbatim: a double quote, a newline, then a key line.
    const BREAK_OUT: &str = "\"\n bootstrap_servers:";
    let refusal = credential_is_renderable(BREAK_OUT)
        .expect_err("a value that closes the scalar and opens a key must be refused");
    assert_eq!(
        refusal.character, '"',
        "the FIRST offender in scan order is the one named"
    );
    assert!(
        refusal.reason.contains("before it is parsed"),
        "the reason must name the mechanism: {}",
        refusal.reason
    );

    // Each refused character on its own, so a deletion from the set is
    // attributable to one row rather than to "the set".
    for (ch, what) in [
        ('\n', "a newline ends the physical line and opens a new key"),
        ('\r', "a carriage return does the same on a CRLF reader"),
        ('"', "a double quote closes the double-quoted scalar early"),
        ('\'', "a single quote closes a single-quoted scalar early"),
        (
            '$',
            "a dollar sign lets the password name an environment variable",
        ),
    ] {
        let secret = format!("hunter2{ch}tail");
        let refusal = credential_is_renderable(&secret).expect_err(what);
        assert_eq!(refusal.character, ch, "{what}");
    }

    // ACCEPTED. An ordinary strong password, and a 64-byte random-looking
    // ASCII one — the guard must not be a de-facto complexity policy that
    // rejects passwords an adopter's own generator produces.
    assert_eq!(credential_is_renderable("hunter2-Aa1!"), Ok(()));
    const SIXTY_FOUR: &str = "aZ3-kQ9_tW1.pL7~mB5+xR2:hN8@vC4=fJ6*yD0?gS2%eT9^uK5&iO1#wP3!qX7|zM";
    let sixty_four: String = SIXTY_FOUR.chars().take(64).collect();
    assert_eq!(sixty_four.len(), 64, "the fixture must be 64 bytes");
    assert_eq!(
        credential_is_renderable(&sixty_four),
        Ok(()),
        "a 64-byte random-looking ASCII password must be accepted: {sixty_four}"
    );
    // The empty value is renderable — "the operator projected nothing" is not
    // this predicate's business, and refusing it here would report a missing
    // Secret as a malformed one.
    assert_eq!(credential_is_renderable(""), Ok(()));

    // THE REFUSAL LEAKS NOTHING. Asserted two ways.
    //
    // (a) The `Display` is a PURE FUNCTION of the character class: two
    // completely different unrenderable values whose first offender is the
    // same character produce byte-identical messages. That is strictly
    // stronger than checking a list of substrings, because a message that
    // cannot depend on the value cannot carry a fragment of it.
    let other = credential_is_renderable("\"totally-different-secret")
        .expect_err("also refused on the double quote");
    assert_eq!(
        refusal.to_string(),
        other.to_string(),
        "the message must not vary with the value"
    );
    // (b) And directly: the message contains neither the value nor any 4-byte
    // window of it. (The brief says "any substring longer than one
    // character"; at two characters that is unsatisfiable for this payload by
    // ANY English sentence — `quote` alone contains `ot` from
    // `bootstrap` — and the same brief requires the message to name the
    // character class. Four is the threshold that admits no coincidence: no
    // 4-gram of this payload is an English fragment. Recorded as a deviation
    // in the task report, with this reason.)
    let msg = refusal.to_string();
    assert!(!msg.contains(BREAK_OUT), "{msg}");
    let bytes: Vec<char> = BREAK_OUT.chars().collect();
    for w in bytes.windows(4) {
        let window: String = w.iter().collect();
        assert!(
            !msg.contains(&window),
            "the refusal must not carry `{window}` from the value: {msg}"
        );
    }
    // It names the class, so an operator knows what to change.
    assert!(msg.contains("a double quote"), "{msg}");

    // The set is exactly five characters, and the list is what the predicate
    // reads — a sixth added to the constant without a row above would show up
    // here as a length mismatch.
    assert_eq!(UNRENDERABLE_CREDENTIAL_CHARACTERS.len(), 5);
    for ch in UNRENDERABLE_CREDENTIAL_CHARACTERS {
        assert!(
            credential_is_renderable(&ch.to_string()).is_err(),
            "`{ch:?}` is in the refused set and must be refused"
        );
    }
    // A `CredentialRefusal` can be built and compared by value, which is what
    // a caller mapping it to a terminal state needs.
    assert_eq!(
        refusal,
        CredentialRefusal {
            character: '"',
            reason: refusal.reason
        }
    );
}

/// [I9] The refusal discriminator a controller reads. It matches a PREFIX,
/// including the `": "` separator, and never a substring.
///
/// The second row is the whole point. A `contains` match would classify a
/// message that merely MENTIONS `CredentialNotRenderable` as that state, and
/// tell a controller a Secret is malformed when nothing about a Secret was
/// observed — sending an operator to rotate a credential over a marker topic
/// that does not exist.
#[test]
fn terminal_state_matches_a_prefix_and_never_a_substring() {
    assert_eq!(
        terminal_state("CredentialNotRenderable: the projected password contains a newline"),
        "CredentialNotRenderable"
    );
    assert_eq!(
        terminal_state("the projected password is not CredentialNotRenderable-safe"),
        "GuardRefused",
        "a MENTION is not a state; only an opening `<State>: ` is"
    );
    assert_eq!(
        terminal_state("marker topic `logweir-marker` does not exist"),
        "GuardRefused"
    );
    assert_eq!(
        refusal_reason_line("TargetTopicConfigRefused: cleanup.policy is `compact`"),
        "refusal-reason=TargetTopicConfigRefused"
    );

    // Every state round-trips through its own prefix, so the three constants
    // and the matcher cannot drift.
    for state in TERMINAL_STATES {
        assert_eq!(
            terminal_state(&format!("{state}: something happened")),
            state
        );
        assert_eq!(
            refusal_reason_line(&format!("{state}: something happened")),
            format!("refusal-reason={state}")
        );
    }
    // A state name that is a PREFIX of a longer word does not match: the
    // separator is part of the prefix.
    assert_eq!(
        terminal_state("CredentialNotRenderableXyz: nope"),
        "GuardRefused"
    );
    // A bare state name with no separator is not a state either.
    assert_eq!(terminal_state("CredentialNotRenderable"), "GuardRefused");
    // The empty message is a plain guard refusal, never a missing answer:
    // `refusal-reason=` always names a state (see the constant's doc).
    assert_eq!(terminal_state(""), "GuardRefused");
    assert_eq!(refusal_reason_line(""), "refusal-reason=GuardRefused");

    // The three names are exactly spec §3.2's, spelled once.
    assert_eq!(TERMINAL_STATE_GUARD_REFUSED, "GuardRefused");
    assert_eq!(
        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
        "CredentialNotRenderable"
    );
    assert_eq!(
        TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
        "TargetTopicConfigRefused"
    );
    assert_eq!(TERMINAL_STATES.len(), 3);
    // The line carries no whitespace and no quoting: a controller reads it as
    // the final stdout line and splits on `=`.
    let line = refusal_reason_line("CredentialNotRenderable: x");
    assert_eq!(line, "refusal-reason=CredentialNotRenderable");
    assert!(!line.contains(' '), "{line}");
    assert!(!line.contains('\n'), "{line}");
}

/// RECEIPT-DUP's `failure-reason=` vocabulary is closed in BOTH directions: a
/// state is lifted only beside the one exit code it belongs to.
#[test]
fn a_failure_reason_is_lifted_only_beside_its_own_exit_code() {
    use logweir_core::guard::{
        failure_reason_for_exit, failure_reason_line, TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED,
        TERMINAL_STATE_EXECUTION_CLAIM_UNPROVEN,
    };
    assert_eq!(
        failure_reason_line(TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED),
        "failure-reason=ExecutionAlreadyClaimed"
    );
    assert_eq!(
        failure_reason_for_exit(1, "ExecutionAlreadyClaimed"),
        Some(TERMINAL_STATE_EXECUTION_ALREADY_CLAIMED)
    );
    assert_eq!(
        failure_reason_for_exit(4, "ExecutionClaimUnproven"),
        Some(TERMINAL_STATE_EXECUTION_CLAIM_UNPROVEN)
    );
    // The wrong code for a known state, an unknown state, and exits that
    // never carry one.
    assert_eq!(failure_reason_for_exit(4, "ExecutionAlreadyClaimed"), None);
    assert_eq!(failure_reason_for_exit(1, "ExecutionClaimUnproven"), None);
    assert_eq!(failure_reason_for_exit(1, "Anything"), None);
    assert_eq!(failure_reason_for_exit(0, "ExecutionAlreadyClaimed"), None);
    assert_eq!(failure_reason_for_exit(3, "ExecutionClaimUnproven"), None);
}
