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
        // Task 14. NOT a secret and NOT redacted — the assertions below say so
        // in both directions.
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
    // Task 14, the other direction: the endpoint is deliberately shown IN
    // FULL. It is a routing destination, not a credential, and an operator who
    // cannot read which PagerDuty region their events went to cannot diagnose
    // the misdirection the field exists to fix. Redacting it here would
    // reproduce the defect.
    assert!(
        shown.contains("https://events.eu.pagerduty.com/v2/enqueue"),
        "the endpoint is not a secret and must be readable off a Debug line: {shown}"
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
    assert!(
        log.contains("https://events.eu.pagerduty.com/v2/enqueue"),
        "the endpoint is not a secret, and WHICH region was posted to is the thing an \
         operator debugging a missing page needs most: {log}"
    );
    assert!(
        !log.contains(TEST_ROUTING_KEY),
        "the routing key reached a log line: {log}"
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
