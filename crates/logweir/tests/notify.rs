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
/// It is bound to `127.0.0.1:0`, so the kernel picks the port and nothing
/// leaves the loopback interface (GC17). The accepted sockets are held in the
/// thread rather than dropped, because dropping one closes it and hands the
/// client an EOF — which is a completed request, not a hang.
fn black_hole() -> (String, std::thread::JoinHandle<()>) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback bind");
    let addr = l.local_addr().unwrap();
    let h = std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in l.incoming() {
            match s {
                Ok(s) => held.push(s),
                Err(_) => break,
            }
            if held.len() >= 8 {
                break;
            }
        }
        // Hold the sockets open a moment past the caller's bound so the
        // client times out rather than being handed an EOF on teardown.
        std::thread::sleep(std::time::Duration::from_secs(2));
    });
    (format!("http://{addr}/hook"), h)
}

/// THE BOUND. A sink that accepts and never replies must make the POST FAIL,
/// within the agent's timeout, rather than blocking forever.
///
/// The agent is injected at 300 ms rather than driving the production
/// `NOTIFY_TIMEOUT` of 10 s, for one reason: a unit test that waits out a
/// production timeout is the same defect this whole task exists to remove.
/// What is being proved here is that the bound is WIRED — that `notify_with`
/// gives up and returns — and the wiring is bound-independent. That the
/// production numbers are the ones actually used is pinned by
/// `the_production_notify_agent_is_bounded` and
/// `every_notification_post_goes_through_the_bounded_agent` below; the three
/// together leave no gap for a mutant.
#[test]
fn a_sink_that_never_replies_fails_within_the_bound() {
    let (url, h) = black_hole();
    let bound = std::time::Duration::from_millis(300);
    let agent = logweir::drill::phase7_verify::notify_agent_with(bound, bound);

    let n = logweir_core::spec::Notifications {
        webhooks: vec![url],
        slack_webhook: None,
        pagerduty_routing_key: None,
    };
    let sc = scorecard_pass();

    let t0 = std::time::Instant::now();
    logweir::drill::phase7_verify::notify_with(&agent, &n, &sc);
    let took = t0.elapsed();

    // Generous by 10x: the assertion is "bounded", not "bounded to the
    // millisecond", and a loaded CI runner must not make it flake. Without a
    // timeout this never returns at all, so any finite number here is the
    // whole finding.
    assert!(
        took < std::time::Duration::from_secs(3),
        "a sink that accepts and never replies must be given up on, not waited \
         out forever; the POST took {took:?} against a {bound:?} bound"
    );
    drop(h);
}

/// The production agent's numbers, asserted where a reviewer reads them.
/// A mutant that builds the agent with no timeout at all — the state this
/// task found — cannot leave both constants bounded and non-zero.
#[test]
fn the_production_notify_agent_is_bounded() {
    use logweir::drill::phase7_verify::{NOTIFY_CONNECT_TIMEOUT, NOTIFY_TIMEOUT};
    assert!(
        !NOTIFY_CONNECT_TIMEOUT.is_zero() && !NOTIFY_TIMEOUT.is_zero(),
        "a zero timeout is not a bound, it is an immediate failure"
    );
    assert!(
        NOTIFY_TIMEOUT <= std::time::Duration::from_secs(30),
        "the overall bound is what an operator waits through after the scorecard \
         is already signed and uploaded: {NOTIFY_TIMEOUT:?}"
    );
    assert!(
        NOTIFY_CONNECT_TIMEOUT <= NOTIFY_TIMEOUT,
        "a connect budget larger than the overall budget cannot be reached: \
         {NOTIFY_CONNECT_TIMEOUT:?} > {NOTIFY_TIMEOUT:?}"
    );
    // Construction must not panic — `AgentBuilder::build` is the only place
    // the two constants meet ureq.
    let _ = logweir::drill::phase7_verify::notify_agent();
}

/// THE MUTANT KILLER, structural rather than behavioural. Every test above
/// that exercises a timeout supplies its own agent, so reverting `notify` to
/// `ureq::post(&url)` — restoring the unbounded wait — would pass all of
/// them. `ureq::post`/`ureq::get`/`ureq::request` are ureq's AGENTLESS
/// builders: each one constructs a throwaway agent with ureq's defaults,
/// which is to say with no timeout.
///
/// This is the same discipline `crates/logweir/tests/engine_resolution.rs`'s
/// `the_engine_is_resolved_in_exactly_one_module` already uses in this crate:
/// when the property is "there is exactly one way to do this", assert over
/// the source rather than hope a behavioural test happens to cover the second
/// way someone adds.
#[test]
fn every_notification_post_goes_through_the_bounded_agent() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
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
                for needle in ["ureq::post(", "ureq::get(", "ureq::request("] {
                    if t.contains(needle) {
                        offenders.push(format!("{}: {needle}", p.display()));
                    }
                }
            }
        }
    }
    // The walk must actually have walked: an empty file list makes the
    // assertion below vacuously true forever.
    assert!(
        visited >= 20,
        "the walk visited only {visited} .rs files under {} — it is not looking \
         at the crate it claims to be checking",
        src.display()
    );
    assert!(
        offenders.is_empty(),
        "agentless ureq request builders carry NO timeout (ureq 2.12.1: \"requests \
         may block forever on reads by default\"). Every notification POST must go \
         through `notify_agent()`.\n  {}",
        offenders.join("\n  ")
    );
}
