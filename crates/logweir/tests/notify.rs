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
