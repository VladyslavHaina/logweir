//! Notification sinks are CREDENTIALS. This file pins that no configured sink
//! reaches a log line, an error message or a `{:?}` intact.
//!
//! The finding this file answers: `phase7_verify::notify` logged the sink URL
//! verbatim at INFO on the success path — `url = %url` — and again on the
//! error path, to the JSON subscriber intended for a log aggregator. A
//! `https://hooks.slack.com/services/T…/B…/…` URL is a bearer credential:
//! whoever holds it can post as that integration, with no other secret. The
//! asymmetry was the tell — `pagerduty_routing_key`, four lines below in the
//! same function, was deliberately never logged.
mod support;

use logweir::drill::phase7_verify::redact_url;
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
        // reaches a display surface. `the_pagerduty_endpoint_is_redacted_\
        // wherever_it_is_displayed` below drives the two shapes that carry a
        // secret; this one keeps the ordinary case honest.
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

/// THE CALL SITE, not just the helper. Every test above exercises
/// `redact_url` directly, and a mutant that reverts `notify`'s own log line to
/// `url = %url` would survive all of them. This one installs a subscriber over
/// a shared buffer, drives the real `notify` at a sink that cannot answer, and
/// asserts the bytes that reached the log.
///
/// The sink is `http://127.0.0.1:1/...` — loopback, port 1, which nothing
/// listens on — so the request fails immediately and no packet leaves the
/// machine. Global Constraint 17 forbids external calls; a refused connection
/// to loopback is not one.
#[test]
fn the_notify_log_lines_carry_no_sink_credential() {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    let captured = Arc::new(Mutex::new(Vec::new()));
    ensure_global_subscriber();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(Buf(captured.clone()))
        .with_max_level(tracing::Level::TRACE)
        .finish();

    // Port 1 on loopback: the connection is refused immediately, so `notify`
    // takes its error arm — the arm that used to log BOTH `url = %url` and a
    // `ureq::Error` whose own Display embeds the URL.
    let secret_path = "T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX";
    let n = logweir_core::spec::Notifications {
        webhooks: vec![format!("http://127.0.0.1:1/services/{secret_path}")],
        slack_webhook: Some(format!("http://127.0.0.1:1/slack/{secret_path}")),
        pagerduty_routing_key: None,
        pagerduty_endpoint: None,
    };
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_str(include_str!("../../../e2e/fixtures/scorecard-pass.json")).unwrap();

    tracing::subscriber::with_default(subscriber, || {
        logweir::drill::phase7_verify::notify(&n, None, &sc);
    });

    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert!(
        !log.is_empty(),
        "the test proves nothing if `notify` logged nothing at all"
    );
    assert!(
        !log.contains(secret_path),
        "a sink credential reached the log: {log}"
    );
    assert!(
        !log.contains("/services/") && !log.contains("/slack/"),
        "the URL PATH must not reach the log at all — the secret can live \
         anywhere in it: {log}"
    );
    assert!(
        log.contains("http://127.0.0.1:1/…"),
        "the sink must still be identifiable, or the log line is useless: {log}"
    );
}

/// The checked-in format example, loaded the same way
/// `the_notify_log_lines_carry_no_sink_credential` above loads it.
fn scorecard_pass() -> logweir_core::scorecard::Scorecard {
    serde_json::from_str(include_str!("../../../e2e/fixtures/scorecard-pass.json")).unwrap()
}

// ------------------------------------------------------------------- T0-3
// The notification body is the ONLY surface that reaches a human away from a
// terminal — it is posted to every webhook and Slack sink and embedded in
// PagerDuty's `custom_details`. It carried no `redactions`, so a redacted
// scorecard notified as if whole. These pin the third of the three display
// surfaces; `notify_body` is public so the shape can be asserted without a
// network, exactly as `redact_url` is.

/// Guarantee: a non-empty `redactions[]` reaches the on-call reader, by path.
/// A mutant that drops the key — the surface going quiet again — fails here at
/// assertion time.
#[test]
fn the_notification_body_names_every_redacted_path() {
    let mut sc = scorecard_pass();
    sc.redactions = vec![
        logweir_core::scorecard::Redaction {
            path: "/measured/rpo_seconds".into(),
            reason: "customer policy".into(),
            present: false,
        },
        logweir_core::scorecard::Redaction {
            path: "/target/cluster_id".into(),
            reason: "customer policy".into(),
            present: false,
        },
    ];
    let body = logweir::drill::phase7_verify::notify_body(&sc);
    let paths = body
        .get("redactions")
        .expect("the notification body must carry `redactions`")
        .as_array()
        .expect("`redactions` is an array");
    assert_eq!(
        paths.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>(),
        vec!["/measured/rpo_seconds", "/target/cluster_id"],
        "every removed path must reach the sink: {body}"
    );
    // The `reason` strings do NOT travel. This body is pasted verbatim into
    // Slack and PagerDuty, and on a redacted document — one no Logweir writer
    // produced — `reason` is text that arrived with the document.
    assert!(
        !body.to_string().contains("customer policy"),
        "document-controlled free text must not be posted to a sink: {body}"
    );
}

/// The control, and the reason the key is always present: a sink that has to
/// tell "absent" from "none" cannot branch on this field at all.
#[test]
fn the_notification_body_carries_an_empty_redactions_for_a_whole_document() {
    let sc = scorecard_pass();
    assert!(
        sc.redactions.is_empty(),
        "the fixture is the whole document"
    );
    let body = logweir::drill::phase7_verify::notify_body(&sc);
    assert_eq!(
        body.get("redactions").and_then(|v| v.as_array()),
        Some(&vec![]),
        "`redactions` must be present and empty, never absent: {body}"
    );
}

// ------------------------------------------------------- Task 5b: the bound
// A notification sink is the fourth thing this process dials, and it was the
// one with no timeout of any kind. `notify` sent every POST with
// `ureq::post(&url).send_json(&body)` — ureq 2.12.1's bare builder, whose
// agent documents "requests may block forever on reads by default". A sink
// that ACCEPTS the connection and never replies therefore hung the drill,
// indefinitely, AFTER the scorecard was signed and uploaded.
//
// The reason it looked fine: every existing test posts to a CLOSED port, and
// a closed port is refused by the kernel in microseconds. Being fast because
// the peer says no is not the same as being bounded.

/// A listener that accepts connections and never writes a byte — the failure
/// mode a refused port cannot reproduce, and the one with no bound.
///
/// Bound to `127.0.0.1:0`, so the kernel picks the port and nothing leaves the
/// loopback interface (GC17). Accepted sockets are pushed into a `Vec` and
/// never dropped: dropping one closes it and hands the client an EOF, which is
/// a completed request, not a hang.
///
/// The thread accepts FOREVER and is deliberately detached — it is never
/// joined and there is no teardown to reach. Fix round 1, F2: the first
/// version of this helper broke out of the accept loop after 8 connections
/// and then slept, and a comment claimed it "holds the sockets open a moment
/// past the caller's bound". With the single sink every test actually uses it
/// blocked in `accept()` on connection two and never reached either line, so
/// nothing bounded anything. The bound now lives in `within_deadline` below,
/// where the test can see it; this thread's only job is to be a peer that
/// never answers. libtest exits the process without joining detached threads,
/// so an eternal accept loop costs nothing.
fn black_hole() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback bind");
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in l.incoming() {
            match s {
                Ok(s) => held.push(s),
                Err(_) => break,
            }
        }
    });
    format!("http://{addr}/hook")
}

/// Run `f` on its own thread and refuse to wait longer than `deadline`.
///
/// **Fix round 1, F2 — this is the point of the whole helper.** Every test
/// below drives a sink that never replies. If the bound under test is deleted,
/// the call inside `f` never returns; without this the TEST BINARY hangs until
/// the harness's 5-minute limit, cargo never prints its summary, and what a
/// human or CI observes is a hang rather than a failure. That is precisely the
/// failure mode this whole task exists to remove, and the first version of
/// these tests re-created it inside the fix for it.
///
/// So the deadline is the test's own, independent of the bound being tested:
/// the worker thread is abandoned (it is stuck by construction and cannot be
/// joined), the assertion fires here, and the test goes RED in `deadline`.
fn within_deadline<T: Send + 'static>(
    deadline: std::time::Duration,
    what: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> (T, std::time::Duration) {
    let (tx, rx) = std::sync::mpsc::channel();
    let t0 = std::time::Instant::now();
    std::thread::spawn(move || {
        let out = f();
        // A send error just means the receiver already gave up and failed the
        // test; there is nobody left to tell.
        let _ = tx.send(out);
    });
    match rx.recv_timeout(deadline) {
        Ok(v) => (v, t0.elapsed()),
        Err(_) => panic!(
            "{what}: still running after {deadline:?}. A sink that accepts the \
             connection and never replies must be GIVEN UP ON, not waited out. \
             This is the unbounded-`ureq` defect: the POST has no timeout, so it \
             would block forever and the test binary would hang instead of \
             failing. Check `notify_agent()` still configures both bounds."
        ),
    }
}

/// THE BOUND, wired. A sink that accepts and never replies must make the POST
/// fail, rather than blocking forever.
///
/// The agent is injected at 300 ms rather than driving the production
/// `NOTIFY_TIMEOUT` of 10 s, because a unit test that waits out a production
/// timeout is the same defect this task exists to remove. What this proves is
/// that the bound is WIRED — that `notify_with` gives up and returns — and the
/// wiring is bound-independent. That the PRODUCTION agent carries the
/// production numbers is proved separately and directly by
/// `the_production_notify_agent_carries_its_configured_bounds`, and end to end
/// by the `e2e`-gated twin below.
#[test]
fn a_sink_that_never_replies_fails_within_the_bound() {
    let url = black_hole();
    let bound = std::time::Duration::from_millis(300);
    let agent = logweir::drill::phase7_verify::notify_agent_with(bound, bound);

    let n = logweir_core::spec::Notifications {
        webhooks: vec![url],
        slack_webhook: None,
        pagerduty_routing_key: None,
        pagerduty_endpoint: None,
    };
    let sc = scorecard_pass();

    let (_, took) = within_deadline(
        std::time::Duration::from_secs(3),
        "a_sink_that_never_replies_fails_within_the_bound",
        move || logweir::drill::phase7_verify::notify_with(&agent, &n, None, &sc),
    );

    // Generous by 10x: the assertion is "bounded", not "bounded to the
    // millisecond", and a loaded runner must not make it flake.
    assert!(
        took < std::time::Duration::from_secs(3),
        "the POST took {took:?} against a {bound:?} bound"
    );
}

/// **Fix round 1, F1(a) — the mutant this task shipped without.** Replacing
/// `notify_agent()`'s body with `ureq::AgentBuilder::new().build()` restores
/// exactly the unbounded state §7 of the report describes FINDING, and every
/// notification test passed anyway: the constants were pinned, and the wiring
/// from `notify` to *an* agent was pinned, but the wiring of the constants
/// into the PRODUCTION agent was pinned nowhere.
///
/// `ureq` 2.x exposes no getter for an `Agent`'s configured timeouts — but
/// `Agent` derives `Debug` and holds an `Arc<AgentConfig>` that does too, so
/// the configuration is directly observable. This asserts on the real
/// `notify_agent()`, in microseconds, and the expected substrings are built
/// FROM the constants, so changing a constant moves the assertion with it.
///
/// The negative control is what makes that safe. ureq's `Debug` rendering is
/// not a stable API; if it ever stops emitting these fields the positive
/// assertions fail loudly rather than passing vacuously, and the control below
/// pins that an UNBOUNDED agent renders differently from a bounded one — which
/// is the entire discriminating power being relied on. (Measured against ureq
/// 2.12.1: a bare agent renders `timeout_connect: Some(30s)` and
/// `timeout: None`. Note that the unbounded defect was never a missing CONNECT
/// timeout — ureq defaults that to 30 s — it was the missing OVERALL/read
/// bound, which is exactly the never-replies case.)
#[test]
fn the_production_notify_agent_carries_its_configured_bounds() {
    use logweir::drill::phase7_verify::{notify_agent, NOTIFY_CONNECT_TIMEOUT, NOTIFY_TIMEOUT};

    // The numbers themselves, where a reviewer reads them.
    assert_eq!(
        NOTIFY_CONNECT_TIMEOUT,
        std::time::Duration::from_secs(5),
        "the documented connect bound"
    );
    assert_eq!(
        NOTIFY_TIMEOUT,
        std::time::Duration::from_secs(10),
        "the documented overall bound"
    );

    let want_connect = format!("timeout_connect: Some({NOTIFY_CONNECT_TIMEOUT:?})");
    let want_overall = format!("timeout: Some({NOTIFY_TIMEOUT:?})");

    let shown = format!("{:?}", notify_agent());
    assert!(
        shown.contains(&want_connect),
        "the PRODUCTION agent does not carry {NOTIFY_CONNECT_TIMEOUT:?} as its connect \
         bound. Expected to find `{want_connect}` in:\n{shown}"
    );
    assert!(
        shown.contains(&want_overall),
        "the PRODUCTION agent does not carry {NOTIFY_TIMEOUT:?} as its overall bound — a \
         webhook that accepts and never replies would hang the drill forever, after the \
         scorecard is signed and uploaded. Expected `{want_overall}` in:\n{shown}"
    );

    // THE NEGATIVE CONTROL: an unbounded agent must render differently, or the
    // two assertions above prove nothing.
    let unbounded = format!("{:?}", ureq::AgentBuilder::new().build());
    assert!(
        !unbounded.contains(&want_overall),
        "an agent built with no timeouts renders the same overall bound as the \
         production one, so this test cannot tell them apart. ureq's Debug format \
         has changed and this test needs rewriting:\n{unbounded}"
    );

    // Ordering and sanity, kept from the original test.
    assert!(
        !NOTIFY_CONNECT_TIMEOUT.is_zero() && !NOTIFY_TIMEOUT.is_zero(),
        "a zero timeout is not a bound, it is an immediate failure"
    );
    assert!(
        NOTIFY_CONNECT_TIMEOUT <= NOTIFY_TIMEOUT,
        "a connect budget larger than the overall budget cannot be reached: \
         {NOTIFY_CONNECT_TIMEOUT:?} > {NOTIFY_TIMEOUT:?}"
    );
}

/// **Fix round 1, F1(b)** — the same property end to end, with NOTHING
/// injected: the real `notify()`, the real `notify_agent()`, the real
/// constants, against a sink that accepts and never replies. It costs the
/// production bound (~10 s), which is why it is behind `--features e2e` and
/// not in the default suite.
///
/// It needs no broker, no bucket and no compose stack — only a loopback
/// listener — but the `e2e` feature is this repository's marker for "this test
/// is allowed to be slow", and 10 s is four times over the 5 s per-test budget
/// `scripts/time-unit-suite.sh` enforces on the default suite.
#[cfg(feature = "e2e")]
#[test]
fn the_production_agent_gives_up_on_a_sink_that_never_replies() {
    use logweir::drill::phase7_verify::NOTIFY_TIMEOUT;
    let url = black_hole();
    let n = logweir_core::spec::Notifications {
        webhooks: vec![url],
        slack_webhook: None,
        pagerduty_routing_key: None,
        pagerduty_endpoint: None,
    };
    let sc = scorecard_pass();

    // The deadline is the production bound plus a wide margin, so a slow
    // machine cannot flake it while an UNBOUNDED agent still fails here
    // rather than hanging the binary (F2).
    let (_, took) = within_deadline(
        NOTIFY_TIMEOUT + std::time::Duration::from_secs(20),
        "the_production_agent_gives_up_on_a_sink_that_never_replies",
        move || logweir::drill::phase7_verify::notify(&n, None, &sc),
    );
    assert!(
        took < NOTIFY_TIMEOUT + std::time::Duration::from_secs(10),
        "the production agent must give up at about {NOTIFY_TIMEOUT:?}; it took {took:?}"
    );
    // And it must actually have SPENT the bound rather than failing instantly
    // for some unrelated reason — a listener that refused the connection, or a
    // zero timeout, would return in milliseconds and prove nothing about the
    // never-replies case.
    assert!(
        took > std::time::Duration::from_secs(1),
        "returned in {took:?}, far under the {NOTIFY_TIMEOUT:?} bound — the sink was \
         not actually accepting-and-never-replying, so this proved nothing"
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

/// THE MUTANT KILLER, structural rather than behavioural. Every behavioural
/// test above that exercises a timeout supplies its own agent, so reverting
/// `notify` to an agentless builder would pass all of them.
///
/// Two families are forbidden, and fix round 1 (F5/F6) added the second after
/// the reviewer walked straight through the first:
///
/// * **Agentless request builders** — `ureq::post(`, `ureq::get(`,
///   `ureq::request(`. Each constructs a throwaway agent with ureq's defaults.
/// * **Default-configured agents** — `ureq::Agent::new(`, `Agent::new(`,
///   `ureq::agent(`. Same defect, spelled differently: the reviewer's mutant
///   `agent.post(&url)` -> `ureq::Agent::new().post(&url)` restored the
///   unbounded wait and survived the first version of this test.
///
/// And `AgentBuilder::new()` must occur EXACTLY ONCE per reviewed timeout
/// policy under `crates/logweir/src/`: inside `notify_agent_with`, the one
/// place the two notification constants meet ureq, and inside `identity.rs`'s
/// `kubernetes_agent`, the identity bootstrap's Kubernetes API client, which
/// must bound connect, read and write. A builder anywhere else is a second,
/// unreviewed timeout policy, which is how the first one went missing.
///
/// This is the discipline `crates/logweir/tests/engine_resolution.rs`'s
/// `the_engine_is_resolved_in_exactly_one_module` already uses in this crate:
/// when the property is "there is exactly one way to do this", assert over the
/// source rather than hope a behavioural test covers the next way someone adds.
#[test]
fn every_notification_post_goes_through_the_bounded_agent() {
    const BUILDER: &str = "AgentBuilder::new()";
    // One builder per reviewed timeout policy, each confined to the function
    // named beside its file. The Kubernetes API client is not a notification
    // sink, so it is its own policy; its bounds are asserted below because no
    // behavioural row in this file exercises them.
    //
    // `notify.rs`, not `drill/phase7_verify.rs`: D3 §3.4 moved the whole
    // notification half to `crates/logweir/src/notify.rs` so `logweir notify
    // deliver` can reach it without reaching into a drill phase, and
    // `phase7_verify` re-exports every public item. THIS TABLE IS THE ONE ROW
    // OF THIS FILE THE MOVE HAD TO CHANGE, because it names the file the
    // builder lives in rather than the path the builder is reached by — which
    // is the point of it: a re-export cannot satisfy "exactly one reviewed
    // timeout policy", only the definition site can.
    // The order is the SORTED one, because `found` below is sorted before the
    // comparison; `drill/phase7_verify.rs` happened to sort ahead of
    // `identity.rs` and `notify.rs` does not.
    const SANCTIONED: [(&str, &str); 2] = [
        ("identity.rs", "fn kubernetes_agent("),
        ("notify.rs", "fn notify_agent_with("),
    ];

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
         default\") — a sink that accepts and never replies then hangs the drill. \
         Every notification POST must go through `notify_agent()`.\n  {}",
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
        "`{BUILDER}` must appear EXACTLY ONCE in each reviewed timeout policy under {} — \
         `notify_agent_with` for notifications, `kubernetes_agent` for the identity \
         bootstrap's Kubernetes API client — and nowhere else. Found: {builders:?}",
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

// ------------------------------------------------------------------ T0-15
// The PagerDuty route stops silencing itself.
//
// Three defects, one property. The route fired from exactly ONE place —
// phase 8, after the scorecard was already signed and uploaded — so exits 1,
// 3 and 4, the three codes that mean a human is needed and no artifact
// exists, notified nobody. When it did fire, `dedup_key` was
// `logweir-drill-{cluster_id}`, so two drill specs against one cluster shared
// one PagerDuty incident and a passing drill's `resolve` closed the failing
// drill's open page. And the enqueue URL was a US-region string literal, so an
// EU-service-region adopter's events went to a region that does not hold their
// account and the non-2xx was swallowed.
//
// The property, and the one to try to falsify: **for every exit code, the
// configured PagerDuty route either receives an event whose `event_action` and
// `dedup_key` correctly describe THIS drill, or refuses in a named, logged
// way — and no drill's event can ever resolve another drill's incident.**
//
// Every test below drives the `EventSink` seam and opens no socket (GC17).

/// A routing key that is obviously fake and obviously a credential, so an
/// assertion that it did not leak reads as one.
const TEST_ROUTING_KEY: &str = "R0FAKEFAKEFAKEFAKEFAKEFAKEFAKE0";

/// The `EventSink` a test uses instead of a network: it records what would
/// have been posted, and to where.
#[derive(Default)]
struct RecordingSink {
    posts: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
}

impl logweir::drill::phase7_verify::EventSink for RecordingSink {
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        self.posts
            .lock()
            .unwrap()
            .push((url.to_string(), body.clone()));
        Ok(())
    }
}

impl RecordingSink {
    fn posts(&self) -> Vec<(String, serde_json::Value)> {
        self.posts.lock().unwrap().clone()
    }

    /// Only the PagerDuty enqueues — a `routing_key` is the thing that makes an
    /// Events v2 body one, and no webhook body carries one.
    fn pagerduty(&self) -> Vec<(String, serde_json::Value)> {
        self.posts()
            .into_iter()
            .filter(|(_, b)| b.get("routing_key").is_some())
            .collect()
    }
}

/// An `EventSink` whose POST fails the way a down or misdirected PagerDuty
/// does. The error it returns is already redacted, as `EventSink::post`'s
/// contract requires.
#[derive(Default)]
struct FailingSink {
    attempts: std::sync::Mutex<Vec<String>>,
}

impl logweir::drill::phase7_verify::EventSink for FailingSink {
    fn post(&self, url: &str, _body: &serde_json::Value) -> Result<(), String> {
        self.attempts.lock().unwrap().push(url.to_string());
        Err("status code 502".into())
    }
}

fn pagerduty_only(endpoint: Option<&str>) -> logweir_core::spec::Notifications {
    logweir_core::spec::Notifications {
        webhooks: Vec::new(),
        slack_webhook: None,
        pagerduty_routing_key: Some(TEST_ROUTING_KEY.into()),
        pagerduty_endpoint: endpoint.map(str::to_owned),
    }
}

/// **M2 and M7.** Two drill specs pointed at ONE cluster must produce two
/// incidents, whether or not either spec has a `name`.
///
/// This is ruling R-D's whole content. The old key was
/// `logweir-drill-{cluster_id}`: a nightly smoke drill and a weekly full drill
/// against the same scratch cluster shared one PagerDuty incident, and because
/// a pass sends `event_action: resolve`, the nightly one passing at 03:00
/// silently closed the weekly one's open page. Nobody was paged again until
/// somebody noticed by hand.
///
/// The unnamed pair is the half R-D's own wording leaves open: R-D says "the
/// drill spec's own stable name", and at the base commit `DrillSpec` had no
/// `name` field at all. This task creates it, and because it must be optional
/// for every existing spec to keep parsing, an unnamed spec still needs a
/// distinct key — hence the `plan_hash` prefix, a sha256 over the approved
/// plan bytes, distinct per spec and stable across re-runs of one spec.
#[test]
fn two_specs_one_cluster_have_distinct_dedup_keys() {
    use logweir::drill::phase7_verify::dedup_key;

    let mut a = scorecard_pass();
    let mut b = scorecard_pass();
    a.target.cluster_id = "c1".into();
    b.target.cluster_id = "c1".into();
    a.approval.plan_hash =
        "sha256:aaaaaaaaaaaa1111111111111111111111111111111111111111111111111111".into();
    b.approval.plan_hash =
        "sha256:bbbbbbbbbbbb2222222222222222222222222222222222222222222222222222".into();

    let k1 = dedup_key(Some("nightly"), &a);
    let k2 = dedup_key(Some("weekly"), &b);
    assert_ne!(
        k1, k2,
        "two drill specs against one cluster share a PagerDuty incident and each resolves \
         the other's"
    );

    let k3 = dedup_key(None, &a);
    let k4 = dedup_key(None, &b);
    assert_ne!(
        k3, k4,
        "two UNNAMED drill specs against one cluster share a PagerDuty incident and each \
         resolves the other's — the optional `name` cannot be the only thing that \
         disambiguates, or every spec written before it existed keeps the defect"
    );

    // The cluster is still in the key: an incident has to name the thing it is
    // about, and the identity half is an addition, not a replacement.
    for k in [&k1, &k2, &k3, &k4] {
        assert!(
            k.contains("c1"),
            "the target cluster left the dedup key: {k}"
        );
    }

    // STABLE, which is the other half of what a dedup key is for. The run id
    // is distinct per run and on every log line (Tasks 11/12), so a key built
    // from it would never dedup at all — every re-run of one failing drill
    // would open a brand-new incident.
    let mut a_rerun = a.clone();
    a_rerun.run_id = "01ZZZZZZZZZZZZZZZZZZZZZZZZ".into();
    assert_eq!(
        dedup_key(Some("nightly"), &a_rerun),
        k1,
        "the dedup key changed between two runs of the SAME spec — a key that is not \
         stable never dedups, and every re-run opens a new incident"
    );
    assert_eq!(dedup_key(None, &a_rerun), k3, "same, for an unnamed spec");
}

/// **Fix round 1, finding F3.** An unnamed spec whose `plan_hash` cannot
/// supply an identity must NOT fall back to the shape R-D exists to abolish.
///
/// With an empty `plan_hash`, `dedup_key(None, &sc)` returned
/// `logweir-drill--c1`: one incident per CLUSTER, which is exactly the
/// pre-fix key with a stray hyphen — a nightly drill passing at 03:00 resolves
/// the weekly drill's open page again. It is unreachable today (phase 1
/// recomputes the hash and refuses on mismatch before any scorecard exists),
/// which is precisely why nothing would have noticed it coming back: the only
/// symptom is a page that stops arriving.
///
/// Mutant: delete the `if h.len() < PLAN_HASH_IDENT_LEN` fallback. This test
/// fails at assertion time on the first case.
#[test]
fn an_unusable_plan_hash_never_degenerates_to_the_per_cluster_key() {
    use logweir::drill::phase7_verify::dedup_key;

    // A hash long enough to identify the spec is untouched by the guard — the
    // control, without which this test could pass on a function that always
    // returns the sentinel.
    let mut good = scorecard_pass();
    good.target.cluster_id = "c1".into();
    good.approval.plan_hash =
        "sha256:aaaaaaaaaaaa1111111111111111111111111111111111111111111111111111".into();
    assert_eq!(
        dedup_key(None, &good),
        "logweir-drill-aaaaaaaaaaaa-c1",
        "a real plan hash must still identify the spec"
    );

    // Every way the hash can fail to identify anything: absent, prefix only,
    // and shorter than the identity it is supposed to supply.
    for hash in ["", "sha256:", "sha256:abc", "abc"] {
        let mut sc = scorecard_pass();
        sc.target.cluster_id = "c1".into();
        sc.approval.plan_hash = hash.into();
        let k = dedup_key(None, &sc);

        assert_eq!(
            k, "logweir-drill-unnamed-c1",
            "plan_hash {hash:?} must fall back to the same sentinel `failure_dedup_key` \
             uses, not to a truncation of nothing"
        );
        assert_ne!(
            k, "logweir-drill--c1",
            "plan_hash {hash:?} produced the pre-fix key with a stray hyphen: one \
             PagerDuty incident per cluster, every drill resolving every other drill's \
             page — the defect R-D closes"
        );
        // Said the other way, without naming the shape: whatever identity is
        // used, the key must not be a function of the cluster alone.
        let mut other_cluster = sc.clone();
        other_cluster.target.cluster_id = "c2".into();
        assert!(
            k.len() > format!("logweir-drill--{}", sc.target.cluster_id).len(),
            "the identity segment is empty: {k}"
        );
        assert_ne!(k, dedup_key(None, &other_cluster), "{k}");
    }
}

/// **M5 and M6.** `resolve` on a pass, `trigger` on anything else — and the
/// two share a key, so the pass actually closes the page the failure opened
/// **for that same drill** and for no other.
#[test]
fn notify_resolves_only_on_pass() {
    use logweir::drill::phase7_verify::{dedup_key, notify_with_sink};

    let n = pagerduty_only(None);

    let mut pass = scorecard_pass();
    pass.target.cluster_id = "c1".into();
    assert_eq!(
        pass.outcome,
        logweir_core::outcome::Outcome::Pass,
        "the fixture must be a pass, or this test proves nothing"
    );

    let mut fail = pass.clone();
    fail.outcome = logweir_core::outcome::Outcome::FailObjective;

    let pass_sink = RecordingSink::default();
    notify_with_sink(&n, Some("nightly"), &pass, &pass_sink);
    let fail_sink = RecordingSink::default();
    notify_with_sink(&n, Some("nightly"), &fail, &fail_sink);

    let p = pass_sink.pagerduty();
    let f = fail_sink.pagerduty();
    assert_eq!(p.len(), 1, "a pass must still enqueue its resolve: {p:?}");
    assert_eq!(f.len(), 1, "a non-pass must trigger: {f:?}");
    assert_eq!(p[0].1["event_action"], "resolve");
    assert_eq!(f[0].1["event_action"], "trigger");

    // The SAME drill: the resolve has to close the trigger it belongs to.
    assert_eq!(
        p[0].1["dedup_key"], f[0].1["dedup_key"],
        "a pass must resolve the incident ITS OWN failing run opened, so the two keys \
         are equal for one spec against one cluster"
    );
    assert_eq!(p[0].1["dedup_key"], dedup_key(Some("nightly"), &pass));

    // And a DIFFERENT drill against the same cluster must not be touched.
    let other_sink = RecordingSink::default();
    notify_with_sink(&n, Some("weekly"), &pass, &other_sink);
    assert_ne!(
        other_sink.pagerduty()[0].1["dedup_key"],
        f[0].1["dedup_key"],
        "a passing `weekly` drill resolved the `nightly` drill's open incident"
    );
}

/// **M1 and M3-adjacent.** The three codes that mean a human is needed —
/// exit 1 (operational, no artifact), exit 3 (a guard refused the plan) and
/// exit 4 (the result is unattested and nothing was uploaded) — each page,
/// exactly once, with a key that describes the failure and never resolves.
#[test]
fn notify_triggers_on_operational_failure() {
    use logweir::drill::phase7_verify::{
        dedup_key, failure_dedup_key, notify_failure_with, PAGERDUTY_US_ENDPOINT,
    };
    use logweir::exit::ExitCode;

    let n = pagerduty_only(None);
    let mut sc = scorecard_pass();
    sc.target.cluster_id = "c1".into();

    for code in [
        ExitCode::Operational,
        ExitCode::GuardRefused,
        ExitCode::SigningOrLock,
    ] {
        let sink = RecordingSink::default();
        notify_failure_with(
            &n,
            Some("nightly"),
            "01TESTRUNID0000000000000",
            code,
            "broker unreachable",
            &sink,
        );

        let posts = sink.pagerduty();
        assert_eq!(
            posts.len(),
            1,
            "exit {} produced {} PagerDuty events, not 1 — this is the code that means a \
             human is needed and there is no artifact to read instead",
            code as u8,
            posts.len()
        );
        let (url, body) = &posts[0];
        assert_eq!(url, PAGERDUTY_US_ENDPOINT);
        assert_eq!(
            body["event_action"], "trigger",
            "a path with no scorecard has no result to resolve"
        );
        assert_eq!(body["dedup_key"], failure_dedup_key(Some("nightly")));
        assert_ne!(
            body["dedup_key"],
            serde_json::json!(dedup_key(Some("nightly"), &sc)),
            "the operational-failure key must be DISTINCT from the drill-result key for \
             the same spec: 'logweir could not run this drill' and 'this drill did not \
             pass' are different facts, and a later passing run's resolve must not close \
             an operational incident nobody has looked at"
        );
        assert!(
            body["payload"]["summary"]
                .as_str()
                .unwrap()
                .contains("01TESTRUNID0000000000000"),
            "the run id is the only handle an on-call reader has on the log stream: {body}"
        );
        assert!(
            body["payload"]["summary"]
                .as_str()
                .unwrap()
                .contains("broker unreachable"),
            "the failure message never reached the page: {body}"
        );

        // The routing key is a bearer credential. It belongs in exactly one
        // field, and nowhere else in a body that is pasted into an incident
        // timeline many people can read.
        assert_eq!(body["routing_key"], TEST_ROUTING_KEY);
        let mut stripped = body.clone();
        stripped["routing_key"] = serde_json::Value::Null;
        assert!(
            !stripped.to_string().contains(TEST_ROUTING_KEY),
            "the routing key appears in the body somewhere other than `routing_key`: {body}"
        );

        // GC11, in the only direction this function can break it: it returns
        // nothing, so it cannot carry an exit code out. The severity is the
        // one thing that varies, and a guard refusal — nothing ran, nothing is
        // at risk — is deliberately not a critical.
        let want_severity = if code == ExitCode::GuardRefused {
            "warning"
        } else {
            "critical"
        };
        assert_eq!(body["payload"]["severity"], want_severity);
    }

    // Opt-in, exactly as the scorecard route is: no routing key, no route.
    let quiet = logweir_core::spec::Notifications::default();
    let sink = RecordingSink::default();
    notify_failure_with(
        &quiet,
        Some("nightly"),
        "01TESTRUNID0000000000000",
        ExitCode::Operational,
        "broker unreachable",
        &sink,
    );
    assert!(
        sink.posts().is_empty(),
        "a spec that configured no PagerDuty route was paged anyway"
    );

    // It sends nothing to `webhooks` / `slack_webhook`: those carry
    // `notify_body(sc)`, and there is no scorecard here. A scorecard-shaped
    // body with no scorecard behind it is how a dashboard starts reporting
    // drills that never ran.
    let with_webhooks = logweir_core::spec::Notifications {
        webhooks: vec!["https://example.test/hook".into()],
        slack_webhook: Some("https://hooks.slack.com/services/T0/B0/X".into()),
        pagerduty_routing_key: Some(TEST_ROUTING_KEY.into()),
        pagerduty_endpoint: None,
    };
    let sink = RecordingSink::default();
    notify_failure_with(
        &with_webhooks,
        Some("nightly"),
        "01TESTRUNID0000000000000",
        ExitCode::Operational,
        "broker unreachable",
        &sink,
    );
    assert_eq!(
        sink.posts().len(),
        1,
        "the failure page must go to PagerDuty ONLY; the webhook and Slack routes carry a \
         scorecard summary and there is no scorecard on this path: {:?}",
        sink.posts()
    );
}

/// **M3.** The enqueue URL is the spec's, not a US-region literal.
#[test]
fn pagerduty_endpoint_is_configurable() {
    use logweir::drill::phase7_verify::{
        notify_with_sink, pagerduty_endpoint, PAGERDUTY_US_ENDPOINT,
    };

    assert_eq!(
        pagerduty_endpoint(&logweir_core::spec::Notifications::default()),
        Ok(PAGERDUTY_US_ENDPOINT.to_string()),
        "an adopter who configured nothing must keep the behaviour they had"
    );

    const EU: &str = "https://events.eu.pagerduty.com/v2/enqueue";
    let n = pagerduty_only(Some(EU));
    assert_eq!(pagerduty_endpoint(&n), Ok(EU.to_string()));

    // The resolver being right is not enough: the POST has to USE it. The
    // defect was a string literal inside the request, which any test of the
    // resolver alone would survive.
    let sink = RecordingSink::default();
    notify_with_sink(&n, Some("nightly"), &scorecard_pass(), &sink);
    let posts = sink.pagerduty();
    assert_eq!(posts.len(), 1, "{posts:?}");
    assert_eq!(
        posts[0].0, EU,
        "the event went to the US region an EU-service-region account does not hold, \
         where it gets a non-2xx that is swallowed into a warning"
    );

    // And the default still reaches the US endpoint.
    let sink = RecordingSink::default();
    notify_with_sink(
        &pagerduty_only(None),
        Some("nightly"),
        &scorecard_pass(),
        &sink,
    );
    assert_eq!(sink.pagerduty()[0].0, PAGERDUTY_US_ENDPOINT);
}

/// **M4.** A non-HTTPS endpoint is refused BEFORE any request, because the
/// routing key travels in the request body and plaintext would put a bearer
/// credential on the wire.
#[test]
fn pagerduty_endpoint_refuses_plain_http() {
    use logweir::drill::phase7_verify::{notify_with_sink, pagerduty_endpoint};

    let n = pagerduty_only(Some("http://evil.test/enqueue"));
    let err = pagerduty_endpoint(&n).expect_err("a plaintext endpoint must be refused");

    assert!(
        err.contains("`http`"),
        "the refusal must name the scheme, or an operator cannot tell what to fix: {err}"
    );
    assert!(
        !err.contains("/enqueue") && !err.contains("evil.test"),
        "the refusal echoed the configured URL; an adopter can put anything in it and \
         this string reaches a log: {err}"
    );

    let sink = RecordingSink::default();
    notify_with_sink(&n, Some("nightly"), &scorecard_pass(), &sink);
    assert!(
        sink.posts().is_empty(),
        "a refused endpoint was posted to anyway: {:?}",
        sink.posts()
    );
}

/// A SILENCED ALERT IS VISIBLE AS ONE.
///
/// Refusing the endpoint is only half of "refuses in a named, logged way". A
/// route that quietly declines is indistinguishable, from a log, from a drill
/// nobody configured an alert for — which is precisely the failure mode this
/// task exists to close. So the refusal emits `PAGERDUTY_SILENCED` at WARN,
/// carrying the `dedup_key` (which incident did not open), the run id and the
/// reason — and carrying NO credential.
#[test]
fn a_silenced_pagerduty_alert_says_so_on_the_log() {
    use logweir::drill::phase7_verify::{
        failure_dedup_key, notify_failure_with, PAGERDUTY_SILENCED,
    };
    use logweir::exit::ExitCode;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    let captured = Arc::new(Mutex::new(Vec::new()));
    ensure_global_subscriber();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(Buf(captured.clone()))
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let n = pagerduty_only(Some("http://evil.test/enqueue"));
    let sink = RecordingSink::default();
    tracing::subscriber::with_default(subscriber, || {
        notify_failure_with(
            &n,
            Some("nightly"),
            "01TESTRUNID0000000000000",
            ExitCode::Operational,
            "broker unreachable",
            &sink,
        );
    });

    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert!(
        sink.posts().is_empty(),
        "the endpoint was refused, so nothing may have been posted"
    );
    assert!(
        log.contains(PAGERDUTY_SILENCED),
        "an alert was silenced and the log does not say so — from a log stream this is \
         indistinguishable from a drill with no PagerDuty route at all: {log}"
    );
    assert!(
        log.contains(&failure_dedup_key(Some("nightly"))),
        "the silenced line must name WHICH incident did not open: {log}"
    );
    assert!(
        log.contains("01TESTRUNID0000000000000"),
        "the silenced line must carry the run id, like every other line: {log}"
    );
    assert!(
        log.contains("\"level\":\"WARN\""),
        "a page that did not go out is not an INFO: {log}"
    );
    assert!(
        !log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line: {log}"
    );

    // THE OTHER WAY AN ALERT IS SILENCED: the endpoint was accepted, the POST
    // was made, and it failed. GC11 keeps that a swallowed warning — a down
    // PagerDuty must never change a drill's exit code — which is exactly why
    // the warning has to be findable. This arm and the refusal arm above emit
    // the SAME message on purpose: an operator asking "why did no page
    // arrive?" greps one string, not two.
    let captured = Arc::new(Mutex::new(Vec::new()));
    ensure_global_subscriber();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(Buf(captured.clone()))
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let n = pagerduty_only(Some("https://events.eu.pagerduty.com/v2/enqueue"));
    let failing = FailingSink::default();
    tracing::subscriber::with_default(subscriber, || {
        notify_failure_with(
            &n,
            Some("nightly"),
            "01TESTRUNID0000000000000",
            ExitCode::Operational,
            "broker unreachable",
            &failing,
        );
    });

    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert_eq!(
        failing.attempts.lock().unwrap().len(),
        1,
        "the endpoint was https, so the POST must have been attempted"
    );
    assert!(
        log.contains(PAGERDUTY_SILENCED),
        "the enqueue FAILED and the log does not say the alert was not delivered — the \
         failure is swallowed by design (GC11), so the log line is the only trace it \
         leaves: {log}"
    );
    assert!(
        log.contains(&failure_dedup_key(Some("nightly"))),
        "the silenced line must name WHICH incident did not open: {log}"
    );
    assert!(
        log.contains("\"level\":\"WARN\""),
        "a page that did not go out is not an INFO: {log}"
    );
    assert!(
        log.contains("status code 502"),
        "the already-redacted failure kind is the whole diagnostic payload: {log}"
    );
    // Fix round 1, F1. Identifiable, not verbatim: the tail is dropped even
    // here, where the URL is a perfectly ordinary one, because a redactor that
    // is applied case by case is not a redactor. This one is asserted FIRST —
    // a verbatim URL contains the host too, so with the region assertion first
    // the raw-endpoint mutant complained that the host was missing.
    assert!(
        !log.contains("/v2/enqueue"),
        "the endpoint's path reached the WARN line: {log}"
    );
    assert!(
        log.contains("https://events.eu.pagerduty.com/…"),
        "WHICH region was posted to is the thing an operator debugging a missing page \
         needs most, and the host carries it: {log}"
    );
    assert!(
        !log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line: {log}"
    );
}

// ------------------------------------------------- Fix round 1, finding F1
// THE ENDPOINT IS FREE-FORM ADOPTER INPUT.
//
// `notifications.pagerduty_endpoint` is not a credential, and Task 14 drew the
// wrong conclusion from that: it logged the value verbatim on the
// `PAGERDUTY_SILENCED` WARN and printed it verbatim from `Notifications`'
// `Debug`, arguing that "redacting it would reproduce the defect the field
// exists to fix". The redactor this codebase already owns refutes that.
// `redact_url` keeps `scheme://host/…` — the region, which IS the diagnostic —
// and drops the userinfo, path and query, which is where a token lives in a URL
// somebody pasted out of a runbook.
//
// The same file argues, one screen away and about the same event, that "a
// per-sink exemption is how the next credential reaches a log". These two tests
// close the exemption Task 14 opened, on both display surfaces and for both
// shapes a secret takes in a URL.

/// Runs `f` with a JSON `tracing` subscriber installed on this thread and hands
/// back what it wrote, so a test can assert on the bytes an operator would
/// actually have in their aggregator.
///
/// Fix round 1. The two tests above spell this writer out inline; a third and
/// fourth copy is where the copies start to disagree. Those two are left as
/// they were reviewed — rewriting a passing mutant-killer to save twenty lines
/// is not a trade worth making — but nothing new duplicates it.
/// Install one permissive global subscriber, once per test binary, before any
/// thread-local capture below.
///
/// Why this exists (root cause of a 1-in-6 red on the merged tree, 2026-09-09):
/// tracing caches each callsite's `Interest` process-wide, and a callsite's
/// FIRST registration computes it from `DISPATCHERS.rebuilder()`. With no
/// global default installed, the registry's `has_just_one` flag is true
/// whenever exactly one scoped dispatcher is live — i.e. while ONE test here
/// holds a `with_default` capture — and in that state the rebuilder consults
/// the *registering thread's* current dispatcher (`Rebuilder::JustOne` →
/// `dispatcher::get_default`, tracing-core 0.1.36 `callsite.rs:562-567`).
/// An uncaptured test (one that drives `notify_failure_with` with no
/// subscriber) hitting the success-arm INFO callsite for the first time from
/// a thread with no default therefore caches `Interest::never()` for it
/// (`NoSubscriber` → never; `callsite.rs:508`), and the capturing test's
/// entire INFO log comes back empty until the next `Dispatch::new` rebuilds
/// the cache. With a global default that enables every level, the
/// registering thread always resolves to a subscriber that says `always`,
/// and a live capture makes the registry hold two dispatchers, which routes
/// rebuilds through the full list. The thread-local captures below still
/// override it on their own thread; every other thread's events go to a
/// sink. Same mechanism and same cure as `teardown.rs`.
fn ensure_global_subscriber() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        // Another helper in this binary may already have installed one; the
        // guarantee we need is "some permissive global exists", not "ours".
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

fn capture_json_logs<T>(f: impl FnOnce() -> T) -> (T, String) {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    let captured = Arc::new(Mutex::new(Vec::new()));
    ensure_global_subscriber();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(Buf(captured.clone()))
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let out = tracing::subscriber::with_default(subscriber, f);
    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    (out, log)
}

/// Drive one endpoint through every surface it can be displayed on and hand
/// back what each one showed: the failed-POST WARN, the successful-enqueue
/// INFO, the `Debug` rendering — and the URL that was actually POSTed to.
///
/// That last one is the control, and it is the reason this returns four things
/// rather than three. Redaction that reaches the REQUEST is not a redaction,
/// it is a bug: the events would go to `https://host/…`, which is not an
/// endpoint. Every test below asserts the posted URL is the adopter's, intact.
fn endpoint_surfaces(endpoint: &str) -> (String, String, String, String) {
    use logweir::drill::phase7_verify::notify_failure_with;
    use logweir::exit::ExitCode;

    let n = pagerduty_only(Some(endpoint));

    let failing = FailingSink::default();
    let ((), warn_log) = capture_json_logs(|| {
        notify_failure_with(
            &n,
            Some("nightly"),
            "01TESTRUNID0000000000000",
            ExitCode::Operational,
            "broker unreachable",
            &failing,
        );
    });

    let recording = RecordingSink::default();
    let ((), info_log) = capture_json_logs(|| {
        notify_failure_with(
            &n,
            Some("nightly"),
            "01TESTRUNID0000000000000",
            ExitCode::Operational,
            "broker unreachable",
            &recording,
        );
    });
    let posted = recording
        .pagerduty()
        .first()
        .map(|(u, _)| u.clone())
        .expect("an https endpoint must have been posted to");

    let debug = format!("{n:?}");
    (warn_log, info_log, debug, posted)
}

/// **F1, shape one: a token in the query string.** A signed or tokenised
/// enqueue URL is the ordinary way this happens — the adopter pastes the whole
/// thing into the spec, and every WARN line thereafter carries the token.
///
/// Mutant: restore `endpoint = %url` on the WARN (and on the INFO). This test
/// and its `user:pass@` twin below both fail here, at assertion time.
#[test]
fn an_endpoint_carrying_a_query_token_is_redacted_on_every_surface() {
    use logweir::drill::phase7_verify::PAGERDUTY_SILENCED;

    const ENDPOINT: &str = "https://events.eu.pagerduty.com/v2/enqueue?token=s3cr3t";
    let (warn_log, info_log, debug, posted) = endpoint_surfaces(ENDPOINT);

    // The control first: what is displayed changed, what is REQUESTED did not.
    assert_eq!(
        posted, ENDPOINT,
        "the redaction reached the request itself — the events would be enqueued to a \
         URL the adopter never configured"
    );

    assert!(
        warn_log.contains(PAGERDUTY_SILENCED),
        "the test proves nothing unless the silenced WARN was actually emitted: {warn_log}"
    );
    for (surface, shown) in [
        ("the WARN line", &warn_log),
        ("the INFO line", &info_log),
        ("the Debug rendering", &debug),
    ] {
        // THE LEAK ASSERTIONS COME FIRST, on purpose. The mutant this test
        // exists to kill is "log the endpoint verbatim", and a verbatim URL
        // still CONTAINS the host — so with the positive assertion first, the
        // raw-endpoint mutant failed with "lost the host", which is the one
        // thing that had not happened. Ordered this way, the message a future
        // engineer reads names the secret that got out.
        assert!(
            !shown.contains("s3cr3t"),
            "a token in the endpoint's query string reached {surface}: {shown}"
        );
        assert!(
            !shown.contains("token="),
            "the endpoint's query string reached {surface} — the secret is IN it: {shown}"
        );
        assert!(
            !shown.contains("/v2/enqueue"),
            "the endpoint's path reached {surface}: {shown}"
        );
        // And the other direction: redacting the host too would leave the
        // operator unable to tell WHICH region the events went to, which is
        // the whole reason the field is displayed at all.
        assert!(
            shown.contains("https://events.eu.pagerduty.com/…"),
            "{surface} lost the host, which is the region and the whole diagnostic: {shown}"
        );
    }
    assert!(
        !warn_log.contains(TEST_ROUTING_KEY) && !info_log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line"
    );
}

/// **F1, shape two: `user:password@` userinfo.** The other place a URL hides a
/// credential, and the one `redact_url` was written for — an endpoint behind a
/// proxy that takes basic auth is the realistic way it arrives here.
///
/// Same mutant, same failure: this is the twin of the test above.
#[test]
fn an_endpoint_carrying_userinfo_is_redacted_on_every_surface() {
    use logweir::drill::phase7_verify::PAGERDUTY_SILENCED;

    const ENDPOINT: &str = "https://pdbot:hunter2@events.eu.pagerduty.com/v2/enqueue";
    let (warn_log, info_log, debug, posted) = endpoint_surfaces(ENDPOINT);

    assert_eq!(
        posted, ENDPOINT,
        "the redaction reached the request itself — the events would be enqueued to a \
         URL the adopter never configured"
    );

    assert!(
        warn_log.contains(PAGERDUTY_SILENCED),
        "the test proves nothing unless the silenced WARN was actually emitted: {warn_log}"
    );
    for (surface, shown) in [
        ("the WARN line", &warn_log),
        ("the INFO line", &info_log),
        ("the Debug rendering", &debug),
    ] {
        // Leak assertions first — see the twin above for why the order is
        // load-bearing on the mutant's failure message.
        assert!(
            !shown.contains("hunter2"),
            "a password in the endpoint's userinfo reached {surface}: {shown}"
        );
        assert!(
            !shown.contains("pdbot"),
            "the userinfo's user half reached {surface} — half a credential is still \
             half a credential, and it names the account: {shown}"
        );
        assert!(
            !shown.contains("/v2/enqueue"),
            "the endpoint's path reached {surface}: {shown}"
        );
        assert!(
            shown.contains("https://events.eu.pagerduty.com/…"),
            "{surface} lost the host, which is the region and the whole diagnostic: {shown}"
        );
    }
    assert!(
        !warn_log.contains(TEST_ROUTING_KEY) && !info_log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line"
    );
}

// ------------------------------------------------- Fix round 1, finding F2
// THE PRODUCTION SINK NEVER RAN.
//
// Every PagerDuty test above drives `RecordingSink` or `FailingSink`, and the
// one pre-existing test that touches a real socket
// (`the_notify_log_lines_carry_no_sink_credential`) sets
// `pagerduty_routing_key: None`, so it skips the branch. `UreqSink`'s
// PagerDuty path — the endpoint resolution, the POST, the `PAGERDUTY_SILENCED`
// WARN, the routing key's non-disclosure — was therefore pinned only on a
// double, and a refactor of `UreqSink` could have reintroduced a silent
// silence with the whole suite green.
//
// This closes it on the real transport, against loopback only (GC17): one
// event a listener accepts, one connection the kernel refuses.

/// A loopback listener that ACCEPTS, reads the whole request, and answers
/// `202 Accepted` — PagerDuty's own success status for an enqueue.
///
/// It is the opposite of `black_hole` above, and both are needed: a peer that
/// never answers proves the bound, a peer that answers proves the POST.
/// Returns the URL to post to and the bytes it received, so a test can assert
/// what actually went over the wire rather than what a double was told.
///
/// `127.0.0.1:0` — the kernel picks the port, nothing leaves the loopback
/// interface. The accept loop is detached and eternal for the same reason
/// `black_hole`'s is; the read carries its own socket timeout so a client that
/// sends a partial request cannot wedge the thread.
fn accepting_listener() -> (String, std::sync::Arc<std::sync::Mutex<Vec<u8>>>) {
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    /// The request is complete once the head has arrived and the body is as
    /// long as the head said it would be. `send_json` always sets
    /// `Content-Length`; a request without one is treated as complete at the
    /// head, which is enough for the assertions and cannot hang.
    fn complete(acc: &[u8]) -> bool {
        let Some(head_end) = acc.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        let head = String::from_utf8_lossy(&acc[..head_end]).to_ascii_lowercase();
        let len = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        acc.len() >= head_end + 4 + len
    }

    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback bind");
    let addr = l.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { break };
            let _ = s.set_read_timeout(Some(std::time::Duration::from_millis(1500)));
            let mut acc = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                match s.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        acc.extend_from_slice(&buf[..n]);
                        if complete(&acc) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            // Recorded BEFORE the reply, so that by the time the client's
            // `post` returns the bytes are already readable by the test.
            sink.lock().unwrap().extend_from_slice(&acc);
            let _ = s.write_all(
                b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            let _ = s.flush();
        }
    });
    (format!("http://{addr}/v2/enqueue"), seen)
}

/// **F2.** The production `UreqSink`, on a real socket, on both arms.
///
/// Arm one, ACCEPTED: the real sink posts a real Events v2 body to a listener
/// that answers `202`, and `post` reports success. The listener's own bytes
/// then say what an operator's PagerDuty account would have received — and
/// that the routing key travelled in the BODY, which is the reason it is
/// allowed nowhere near a log line.
///
/// Arm two, REFUSED: the real sink inside the real `notify_failure_with`, at a
/// loopback port nothing listens on. The connection is refused, no page opens,
/// and the WARN is the only trace the failure leaves — GC11 swallows the error
/// itself, so a WARN that went missing here would be a page that vanished with
/// nothing at all to grep for. The exit code the caller decided is untouched:
/// `notify_failure_with` returns `()` and cannot return one.
///
/// Both arms run under their own deadline rather than the sink's. The
/// production bound is 5 s to connect and 10 s overall, and a test that WAITS
/// OUT a production timeout to discover it exists is the defect Task 5b
/// removed; more to the point, a regression in the bound must make this test
/// go RED in seconds instead of hanging the binary until libtest gives up.
#[test]
fn the_production_sink_enqueues_on_a_real_socket_and_says_silenced_when_refused() {
    use logweir::drill::phase7_verify::{
        failure_dedup_key, notify_failure_with, EventSink, UreqSink, PAGERDUTY_SILENCED,
    };
    use logweir::exit::ExitCode;
    use std::time::Duration;

    const RUN_ID: &str = "01TESTRUNID0000000000000";
    // Four seconds: comfortably over a loopback round trip, comfortably under
    // both the production bound and the 15 s per-test budget
    // `scripts/time-unit-suite.sh` enforces, with two arms to pay for.
    const DEADLINE: Duration = Duration::from_secs(4);

    // ---------------------------------------------------------- accepted
    let (url, seen) = accepting_listener();
    let posted_to = url.clone();
    let ev = serde_json::json!({
        "routing_key": TEST_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": failure_dedup_key(Some("nightly")),
        "payload": { "summary": "logweir drill test", "source": "nightly",
                     "severity": "critical" },
    });
    let (result, took) = within_deadline(
        DEADLINE,
        "the production sink posting to a listener that answers 202",
        move || UreqSink::new().post(&posted_to, &ev),
    );
    assert_eq!(
        result,
        Ok(()),
        "a listener that answers 202 must be a successful enqueue — this is the only \
         place in the suite where the production sink's POST actually executes"
    );
    let wire = String::from_utf8_lossy(&seen.lock().unwrap().clone()).into_owned();
    assert!(
        wire.starts_with("POST /v2/enqueue "),
        "the sink must POST to the path it was given: {wire:?}"
    );
    assert!(
        wire.contains(TEST_ROUTING_KEY),
        "the routing key must travel in the request BODY — that is the whole reason it \
         is never allowed on a log line: {wire:?}"
    );

    // ---------------------------------------------------------- refused
    // Port 1 on loopback: nothing listens, the kernel refuses immediately, and
    // no packet leaves the machine (GC17).
    let n = pagerduty_only(Some("https://127.0.0.1:1/v2/enqueue"));
    let code = ExitCode::Operational;
    let (((), log), refused_took) = within_deadline(
        DEADLINE,
        "the production sink giving up on a refused endpoint",
        move || {
            capture_json_logs(|| {
                notify_failure_with(
                    &n,
                    Some("nightly"),
                    RUN_ID,
                    code,
                    "broker unreachable",
                    &UreqSink::new(),
                )
            })
        },
    );
    assert!(
        log.contains(PAGERDUTY_SILENCED),
        "the real sink could not deliver and the log does not say so — from a log \
         stream that is indistinguishable from a drill with no PagerDuty route: {log}"
    );
    assert!(
        log.contains("\"level\":\"WARN\""),
        "a page that did not go out is not an INFO: {log}"
    );
    assert!(
        log.contains(&failure_dedup_key(Some("nightly"))),
        "the silenced line must name WHICH incident did not open: {log}"
    );
    assert!(
        log.contains(RUN_ID),
        "the silenced line must carry the run id, like every other line: {log}"
    );
    assert!(
        !log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line from the PRODUCTION sink — the branch no \
         other test in this file executes: {log}"
    );
    // GC11, at this seam: a notification that failed cannot move an exit code,
    // because `notify_failure_with` has no way to return one.
    assert_eq!(
        code,
        ExitCode::Operational,
        "the exit code the drill decided must survive a silenced page"
    );

    // The whole test, both arms, well inside the per-test budget.
    assert!(
        took + refused_took < Duration::from_secs(10),
        "the real-socket arms took {took:?} + {refused_took:?}; the per-test budget is 15 s"
    );
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

// ------------------------------------------------------------- D3 §3.4 W4
//
// The move. `crates/logweir/src/notify.rs` is the definition now and
// `drill::phase7_verify` is the alias, and the rows above this line are how we
// know nothing else changed: every one of them reaches the code through the
// OLD path and every one of them still passes.
//
// What is left to prove is the thing those rows cannot see — that the alias is
// EXHAUSTIVE. A re-export list that quietly dropped an item would leave the
// old path compiling for everything the existing tests happen to name and
// broken for the one call site none of them do, and a `pub use` that named
// something else entirely would leave two implementations with one behaving
// like the other only until someone edited it.

/// **Every public item of the moved block is reachable under BOTH paths, and
/// the two paths are the SAME item.**
///
/// The equality assertions are what make this more than a compile check:
/// `phase7_verify::PAGERDUTY_US_ENDPOINT` being *a* string is not the property,
/// `phase7_verify::PAGERDUTY_US_ENDPOINT` being `notify::PAGERDUTY_US_ENDPOINT`
/// is. Function items are compared by calling both and comparing the answers —
/// the pure ones on the same inputs, the agent one on its two constants — for
/// the same reason.
///
/// KILLS: a re-export list that drops an item; a `pub use` pointed at a
/// look-alike; a second copy of a constant left behind in `phase7_verify.rs`.
#[test]
fn the_old_path_is_an_alias_for_the_new_module_and_not_a_copy() {
    use logweir::drill::phase7_verify as old;
    use logweir::notify as new;

    assert_eq!(old::PAGERDUTY_US_ENDPOINT, new::PAGERDUTY_US_ENDPOINT);
    assert_eq!(old::PAGERDUTY_SILENCED, new::PAGERDUTY_SILENCED);
    assert_eq!(old::NOTIFY_CONNECT_TIMEOUT, new::NOTIFY_CONNECT_TIMEOUT);
    assert_eq!(old::NOTIFY_TIMEOUT, new::NOTIFY_TIMEOUT);

    // The pure functions, on inputs whose answers are already pinned above.
    assert_eq!(
        old::failure_dedup_key(Some("nightly")),
        new::failure_dedup_key(Some("nightly"))
    );
    assert_eq!(
        old::redact_url("https://hooks.slack.com/services/T/B/zz"),
        new::redact_url("https://hooks.slack.com/services/T/B/zz")
    );
    let sc = scorecard_pass();
    assert_eq!(old::dedup_key(None, &sc), new::dedup_key(None, &sc));
    assert_eq!(old::notify_body(&sc), new::notify_body(&sc));

    let n = pagerduty_only(Some("https://events.eu.pagerduty.com/v2/enqueue"));
    assert_eq!(old::pagerduty_endpoint(&n), new::pagerduty_endpoint(&n));

    // `EventSink` is ONE trait, not two: a `RecordingSink` written against the
    // old path is accepted where the new path's trait object is wanted, which
    // a `pub use` gives and a duplicate definition does not. This line is the
    // whole assertion; it is a type check and it cannot be made at runtime.
    let sink = RecordingSink::default();
    let _: &dyn new::EventSink = &sink;
    let _: &dyn old::EventSink = &sink;

    // And `UreqSink` reaches both, carrying the same bounds.
    let _: new::UreqSink = old::UreqSink::new();
}

/// **The protection half is NOT reachable through the drill path.**
///
/// The move was one-directional on purpose. `drill::phase7_verify` re-exports
/// what USED to live in it so nothing breaks; it does not become a second door
/// onto PLAT-14.2's event document, because a drill phase that can hand out a
/// `ProtectionEvent` is a drill phase that looks like it has an opinion about
/// protection policies, and it has none.
///
/// Asserted structurally — the re-export list is read — because there is no
/// way to write "this does not compile" as a test.
#[test]
fn the_drill_path_re_exports_the_drill_half_and_nothing_more() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/drill/phase7_verify.rs");
    let text = std::fs::read_to_string(&src).unwrap();
    let start = text
        .find("pub use crate::notify::{")
        .expect("phase7_verify re-exports the moved block from crate::notify");
    let end = start + text[start..].find("};").expect("the re-export list closes");
    let list = &text[start..end];

    for item in [
        "dedup_key",
        "failure_dedup_key",
        "notify_agent",
        "notify_body",
        "notify_failure",
        "notify_with_sink",
        "pagerduty_endpoint",
        "redact_url",
        "EventSink",
        "UreqSink",
        "NOTIFY_TIMEOUT",
        "PAGERDUTY_SILENCED",
        "PAGERDUTY_US_ENDPOINT",
    ] {
        assert!(
            list.contains(item),
            "`{item}` was in the moved block and must stay reachable at the old path; \
             the list read:\n{list}"
        );
    }
    for protection in [
        "ProtectionEvent",
        "deliver_with",
        "AlertKind",
        "VerificationScope",
        "protection_dedup_key",
        "SinkRoutes",
        "DeliverArgs",
    ] {
        assert!(
            !list.contains(protection),
            "`{protection}` is PLAT-14.2's, not the drill's: a drill phase that can hand \
             out a protection event looks like it has an opinion about protection \
             policies, and it has none. The list read:\n{list}"
        );
    }
    // The block really moved: `phase7_verify.rs` no longer DEFINES any of it.
    for defined in [
        "pub fn notify_body(",
        "pub fn notify_agent_with(",
        "pub trait EventSink",
        "pub struct UreqSink",
        "fn enqueue_pagerduty(",
    ] {
        assert!(
            !text.contains(defined),
            "`{defined}` is still defined in phase7_verify.rs — the move left a second \
             implementation behind, and two copies are how the two come to disagree"
        );
    }
}

/// **A protection dedup key can never resolve a drill incident, and the other
/// way round.**
///
/// The three key families now live in one module, which is exactly when they
/// become able to collide. T0-15's defect was one incident shared by two
/// drills, where a passing nightly drill's `resolve` closed the weekly full
/// drill's open page; a protection `resolve` closing a drill's page would be
/// the same defect with a third family added to it. The prefixes are what keep
/// them apart, so the prefixes are asserted here rather than left to be
/// noticed.
#[test]
fn the_three_dedup_key_families_cannot_collide() {
    use logweir::notify::{
        protection_dedup_key, recovery_completed_dedup_key, AlertKind, PROTECTION_DEDUP_PREFIX,
    };

    let sc = scorecard_pass();
    let drill = logweir::drill::phase7_verify::dedup_key(Some("nightly"), &sc);
    let preflight = logweir::drill::phase7_verify::failure_dedup_key(Some("nightly"));
    let protection = protection_dedup_key("uid-1", AlertKind::Staleness);
    let recovery = recovery_completed_dedup_key("restore-1");

    let keys = [&drill, &preflight, &protection, &recovery];
    let distinct: std::collections::BTreeSet<&String> = keys.iter().copied().collect();
    assert_eq!(distinct.len(), 4, "four families, four keys: {keys:?}");

    for k in [&protection, &recovery] {
        assert!(k.starts_with(PROTECTION_DEDUP_PREFIX), "{k}");
        assert!(
            !k.starts_with("logweir-drill-"),
            "a protection resolve must not be able to close a drill's page: {k}"
        );
    }
    for k in [&drill, &preflight] {
        assert!(k.starts_with("logweir-drill-"), "{k}");
        assert!(
            !k.starts_with(PROTECTION_DEDUP_PREFIX),
            "a drill resolve must not be able to close a protection page: {k}"
        );
    }
}
