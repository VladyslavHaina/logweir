//! What is left of the drill's notification rows once the post itself was
//! withdrawn: a plan's `notifications` block still parses (it sits inside
//! signed plan bytes) and is refused by name when it names a sink, so its
//! sink URLs still reach a `{:?}` and a refusal path, and must stay redacted
//! there; and the crate's one remaining HTTP client, the identity bootstrap's
//! Kubernetes agent, still has to be the bounded one.
mod support;

use logweir_core::spec::redact_url;
use support::dial_tokens;

/// The exact shape of the finding: a Slack incoming webhook. Everything that
/// authorises a post lives after the host.
#[test]
fn a_slack_webhook_url_keeps_only_its_host() {
    let secret = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX";
    let shown = redact_url(secret);
    assert_eq!(shown, "https://hooks.slack.com/…");
    assert!(
        !shown.contains("T00000000")
            && !shown.contains("B00000000")
            && !shown.contains("XXXXXXXXXXXXXXXXXXXXXXXX"),
        "the credential survived redaction: {shown}"
    );
}

/// A secret can live in the query string just as easily as in the path — a
/// signed webhook puts it there. Keeping "everything up to the `?`" would have
/// looked like a redaction and leaked half of them.
#[test]
fn a_secret_in_the_query_or_fragment_is_dropped_with_the_path() {
    assert_eq!(
        redact_url("https://example.test/hook?token=s3cr3t#frag"),
        "https://example.test/…"
    );
}

/// `user:password@host` userinfo is dropped, host kept.
#[test]
fn userinfo_credentials_never_survive() {
    let shown = redact_url("https://alice:hunter2@example.test/hook");
    assert_eq!(shown, "https://example.test/…");
    assert!(!shown.contains("hunter2"), "{shown}");
}

/// A string that does not parse as a URL is REPORTED as unparseable, never
/// echoed. "It did not look like a URL to me" is not a reason to print a
/// secret — an operator who pasted a bare token into `webhooks:` would
/// otherwise have it logged in full.
#[test]
fn an_unparseable_sink_is_named_not_echoed() {
    for junk in ["xoxb-not-a-url-but-a-token", "", "https://", "://x"] {
        let shown = redact_url(junk);
        assert_eq!(shown, "<unparseable url>", "input {junk:?}");
    }
}

/// `Notifications` holds three secrets and `DrillSpec` derives `Debug`, so a
/// derived impl here would put all three into any `{:?}` of the spec.
#[test]
fn debugging_a_notifications_block_prints_no_secret() {
    let n = logweir_core::spec::Notifications {
        webhooks: vec!["https://example.test/hook?token=s3cr3t".into()],
        slack_webhook: Some(
            "https://hooks.slack.com/services/T0/B0/XXXXXXXXXXXXXXXXXXXXXXXX".into(),
        ),
        pagerduty_routing_key: Some("R000000000000000000000000000000".into()),
        // Task 14, fix round 1 (F1). Not a credential, but free-form adopter
        // input: reduced to `scheme://host/…` like every other URL that
        // reaches a display surface.
        pagerduty_endpoint: Some("https://events.eu.pagerduty.com/v2/enqueue".into()),
    };
    let shown = format!("{n:?}");
    for secret in [
        "s3cr3t",
        "XXXXXXXXXXXXXXXXXXXXXXXX",
        "R000000000000000000000000000000",
    ] {
        assert!(
            !shown.contains(secret),
            "{secret} reached a Debug rendering: {shown}"
        );
    }
    // Presence is still reported: "none configured" and "one configured and it
    // failed" are different findings.
    assert!(shown.contains("1 configured"), "{shown}");
    assert!(shown.contains("***"), "{shown}");
    // Task 14, the other direction, as corrected by fix round 1 (F1): the
    // endpoint is still IDENTIFIABLE — the host is what says which PagerDuty
    // region the events went to, and an operator who cannot read that cannot
    // diagnose the misdirection the field exists to fix — but it is no longer
    // VERBATIM. The two are not the same requirement, and the first version of
    // this test asserted the stronger one.
    // The leak assertion first: a verbatim URL contains the host as well, so
    // asserting the redacted form first makes a raw-endpoint mutant report a
    // missing host rather than the path that got out.
    assert!(
        !shown.contains("/v2/enqueue"),
        "the endpoint's PATH reached a Debug rendering — a token in a pasted \
         URL lives exactly there: {shown}"
    );
    assert!(
        shown.contains("https://events.eu.pagerduty.com/…"),
        "the endpoint must stay identifiable by host, or the Debug line is \
         useless for the very misdirection the field exists to fix: {shown}"
    );
}

/// The ureq entry points that carry the crate's DEFAULT configuration — no
/// overall or read timeout — and are therefore forbidden in production code.
///
/// The workspace-wide audit in `crates/logweir/tests/no_network_in_unit_tests.rs`
/// forbids the same six as part of its `DIAL_TOKENS`, and
/// `the_two_ureq_token_lists_agree` below asserts that rather than asserting it
/// in a comment. This file is allow-listed in that audit precisely so it can
/// spell them out.
const FORBIDDEN: [&str; 6] = [
    "ureq::post(",
    "ureq::get(",
    "ureq::request(",
    "ureq::Agent::new(",
    "Agent::new(",
    "ureq::agent(",
];

/// FIX ROUND 2 (re-review R1). The last round's audit carried a comment saying
/// "The two lists now match." They did not — bare `Agent::new(` was in this
/// file's list and missing from the workspace one, and a probe using it walked
/// through both gates into `crates/logweir/tests/`, which the crate-local test
/// does not walk.
///
/// A comment cannot hold that property, so this test does. It reads the audit's
/// source and requires every token here to be an ELEMENT of its `DIAL_TOKENS`
/// literal: the workspace-wide audit may be BROADER than this one, never
/// narrower.
///
/// **IT TOKENISES; IT DOES NOT SLICE** (Task 32, stage-2 carried item (a)).
/// The first version took `text[start..end]` and asked
/// `list.contains(&format!("\"{t}\""))` — a substring search, which cannot
/// distinguish an array element from a mention inside a `//` comment in the
/// same literal. A token moved from the array into that comment would have kept
/// this test green while the gate stopped covering it. `support::dial_tokens`
/// strips comments first and reads the remaining double-quoted literals as a
/// SET, so the property is now held by a parser;
/// `crates/logweir/tests/gate_lint.rs::the_ureq_token_lists_agree_under_a_comment`
/// is the test that proves the parser, and not the slice, is doing the work.
#[test]
fn the_two_ureq_token_lists_agree() {
    let audit =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no_network_in_unit_tests.rs");
    let text =
        std::fs::read_to_string(&audit).unwrap_or_else(|e| panic!("read {}: {e}", audit.display()));
    let elements = dial_tokens::array_elements(&text, "const DIAL_TOKENS");

    let missing: Vec<&str> = FORBIDDEN
        .iter()
        .filter(|t| !elements.contains(**t))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "the workspace-wide audit's DIAL_TOKENS is NARROWER than this file's FORBIDDEN \
         list, so a default-suite test outside crates/logweir/src/ can use these and be \
         caught by neither gate: {missing:?}\n  add them to {}\n  (the audit's parsed \
         elements were {elements:?} — a token that appears only in a comment there is \
         NOT an element)",
        audit.display()
    );
}

/// THE MUTANT KILLER, structural rather than behavioural: no ureq entry point
/// with ureq's default configuration anywhere under `crates/logweir/src/`.
/// (Re-scoped when the drill's notification post was withdrawn: the crate's
/// one remaining agent is the identity bootstrap's.)
///
/// Two families are forbidden:
///
/// * **Agentless request builders** — `ureq::post(`, `ureq::get(`,
///   `ureq::request(`. Each constructs a throwaway agent with ureq's defaults.
/// * **Default-configured agents** — `ureq::Agent::new(`, `Agent::new(`,
///   `ureq::agent(`. Same defect, spelled differently: the reviewer's mutant
///   `agent.post(&url)` -> `ureq::Agent::new().post(&url)` restored the
///   unbounded wait and survived the first version of this test.
///
/// And `AgentBuilder::new()` must occur EXACTLY ONCE under
/// `crates/logweir/src/`: inside `identity.rs`'s `kubernetes_agent`, the
/// identity bootstrap's Kubernetes API client, which must bound connect, read
/// and write. A builder anywhere else is a second, unreviewed timeout policy.
///
/// This is the discipline `crates/logweir/tests/engine_resolution.rs`'s
/// `the_engine_is_resolved_in_exactly_one_module` already uses in this crate:
/// when the property is "there is exactly one way to do this", assert over the
/// source rather than hope a behavioural test covers the next way someone adds.
#[test]
fn every_notification_post_goes_through_the_bounded_agent() {
    const BUILDER: &str = "AgentBuilder::new()";
    // One builder per reviewed timeout policy, confined to the function named
    // beside its file. Its bounds are asserted below because no behavioural
    // row exercises them.
    const SANCTIONED: [(&str, &str); 1] = [("identity.rs", "fn kubernetes_agent(")];

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut builders: Vec<String> = Vec::new();
    let mut sources = std::collections::BTreeMap::new();
    let mut visited = 0usize;
    let mut stack = vec![src.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                visited += 1;
                let t = std::fs::read_to_string(&p).unwrap();
                for needle in FORBIDDEN {
                    if t.contains(needle) {
                        offenders.push(format!("{}: {needle}", p.display()));
                    }
                }
                let rel = p
                    .strip_prefix(&src)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                for _ in t.matches(BUILDER) {
                    builders.push(rel.clone());
                }
                sources.insert(rel, t);
            }
        }
    }
    // The walk must actually have walked: an empty file list makes the
    // assertions below vacuously true forever.
    assert!(
        visited >= 20,
        "the walk visited only {visited} .rs files under {} — it is not looking \
         at the crate it claims to be checking",
        src.display()
    );
    assert!(
        offenders.is_empty(),
        "these ureq entry points carry ureq's DEFAULT configuration, which has no \
         overall or read timeout (2.12.1: \"requests may block forever on reads by \
         default\") — a server that accepts and never replies then hangs the run. \
         Build the agent in `kubernetes_agent`.\n  {}",
        offenders.join("\n  ")
    );
    let mut found = builders.clone();
    found.sort();
    let expected: Vec<String> = SANCTIONED
        .iter()
        .map(|(file, _)| file.to_string())
        .collect();
    assert_eq!(
        found,
        expected,
        "`{BUILDER}` must appear EXACTLY ONCE under {}, in `kubernetes_agent`, the \
         identity bootstrap's Kubernetes API client, and nowhere else. Found: {builders:?}",
        src.display()
    );
    for (file, function) in SANCTIONED {
        let body = fn_body(&sources[file], function)
            .unwrap_or_else(|| panic!("{file} no longer defines `{function}`"));
        assert!(
            body.contains(BUILDER),
            "{file}'s `{BUILDER}` moved out of `{function}`, the one place its policy is reviewed"
        );
    }
    let kubernetes = fn_body(&sources["identity.rs"], "fn kubernetes_agent(").unwrap();
    for bound in [".timeout_connect(", ".timeout_read(", ".timeout_write("] {
        assert!(
            kubernetes.contains(bound),
            "`kubernetes_agent` must call `{bound}`: without it a Kubernetes API server that \
             accepts and never replies hangs the identity bootstrap"
        );
    }
}

/// The brace-counted body of the `fn` at the first occurrence of `signature`.
/// Each sanctioned signature occurs once in its file, and neither body carries
/// a brace in a string or a comment.
fn fn_body<'a>(src: &'a str, signature: &str) -> Option<&'a str> {
    let start = src.find(signature)?;
    let open = start + src[start..].find('{')?;
    let mut depth = 0usize;
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[open..=open + i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// **Acceptance check 12.** Both new fields are provably ADDITIVE: the
/// checked-in example spec, which has neither, still parses.
///
/// It asserts the example's own shape first, because a future edit that added
/// `name:` to it would turn this into a test of nothing.
#[test]
fn a_spec_without_a_name_or_endpoint_still_parses() {
    let text = include_str!("../../../examples/drill.yaml");
    assert!(
        !text.contains("\nname:") && !text.contains("\n  pagerduty_endpoint:"),
        "the example gained one of the fields this test exists to prove optional"
    );

    let spec: logweir_core::spec::DrillSpec =
        serde_yaml::from_str(text).expect("an existing spec with neither new field must parse");
    assert!(spec.name.is_none());
    assert!(spec.notifications.pagerduty_endpoint.is_none());

    // And a spec that DOES carry them round-trips.
    let named = format!("name: nightly\n{text}");
    let spec: logweir_core::spec::DrillSpec = serde_yaml::from_str(&named).unwrap();
    assert_eq!(spec.name.as_deref(), Some("nightly"));
}
