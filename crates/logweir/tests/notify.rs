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
use logweir::drill::phase7_verify::redact_url;

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
    };
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_str(include_str!("../../../e2e/fixtures/scorecard-pass.json")).unwrap();

    tracing::subscriber::with_default(subscriber, || {
        logweir::drill::phase7_verify::notify(&n, &sc);
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
    };
    let sc = scorecard_pass();

    let (_, took) = within_deadline(
        std::time::Duration::from_secs(3),
        "a_sink_that_never_replies_fails_within_the_bound",
        move || logweir::drill::phase7_verify::notify_with(&agent, &n, &sc),
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
    };
    let sc = scorecard_pass();

    // The deadline is the production bound plus a wide margin, so a slow
    // machine cannot flake it while an UNBOUNDED agent still fails here
    // rather than hanging the binary (F2).
    let (_, took) = within_deadline(
        NOTIFY_TIMEOUT + std::time::Duration::from_secs(20),
        "the_production_agent_gives_up_on_a_sink_that_never_replies",
        move || logweir::drill::phase7_verify::notify(&n, &sc),
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
/// source and requires every token here to appear in its `DIAL_TOKENS` literal:
/// the workspace-wide audit may be BROADER than this one, never narrower.
#[test]
fn the_two_ureq_token_lists_agree() {
    let audit =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/no_network_in_unit_tests.rs");
    let text =
        std::fs::read_to_string(&audit).unwrap_or_else(|e| panic!("read {}: {e}", audit.display()));
    let start = text
        .find("const DIAL_TOKENS")
        .expect("the audit must still declare DIAL_TOKENS");
    let end = start
        + text[start..]
            .find("];")
            .expect("DIAL_TOKENS must be a closed array literal");
    let list = &text[start..end];

    let missing: Vec<&str> = FORBIDDEN
        .iter()
        .filter(|t| !list.contains(&format!("\"{t}\"")))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "the workspace-wide audit's DIAL_TOKENS is NARROWER than this file's FORBIDDEN \
         list, so a default-suite test outside crates/logweir/src/ can use these and be \
         caught by neither gate: {missing:?}\n  add them to {}",
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
/// And `AgentBuilder::new()` must occur EXACTLY ONCE under
/// `crates/logweir/src/` — inside `notify_agent_with`, the one place the two
/// constants meet ureq. A second builder anywhere is a second, unreviewed
/// timeout policy, which is how the first one went missing.
///
/// This is the discipline `crates/logweir/tests/engine_resolution.rs`'s
/// `the_engine_is_resolved_in_exactly_one_module` already uses in this crate:
/// when the property is "there is exactly one way to do this", assert over the
/// source rather than hope a behavioural test covers the next way someone adds.
#[test]
fn every_notification_post_goes_through_the_bounded_agent() {
    const BUILDER: &str = "AgentBuilder::new()";

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut builders = Vec::new();
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
                for _ in t.matches(BUILDER) {
                    builders.push(p.display().to_string());
                }
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
    assert_eq!(
        builders.len(),
        1,
        "`{BUILDER}` must appear EXACTLY ONCE under {} — in `notify_agent_with`, the \
         single place the two timeout constants meet ureq. Found {}: {:?}",
        src.display(),
        builders.len(),
        builders
    );
}
