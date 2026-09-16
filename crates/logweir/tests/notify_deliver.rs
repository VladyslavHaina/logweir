//! PLAT-14.2 / D3 §3.4 — the protection event document and
//! `logweir notify deliver`.
//!
//! `crates/logweir/tests/notify.rs` next door owns the DRILL's notification
//! path and its structural guards; this file owns the protection one. They are
//! separate binaries on purpose: the drill rows are a fix history with a
//! narrative, and appending a second product's contract to them would make
//! both harder to read and neither easier to trust.
//!
//! # The properties, and the mutants they exist to kill
//!
//! | property | mutant it kills |
//! |---|---|
//! | `verification_scope` has three values | `complete` becomes parseable, or a body claims an exhaustive check |
//! | a failed sink is a non-zero exit | `deliver` reports `ok` for a sink that refused, or returns 0 anyway |
//! | nothing prints a routing key or an unredacted URL | a credential reaches stdout, stderr or a log line |
//! | a refused document posts NOTHING | a malformed event is delivered as far as it parsed |
//! | every configured sink is attempted | the first failure returns early and the working channel is skipped |
//!
//! # No socket except loopback, and every child is bounded
//!
//! Global Constraint 17: nothing here reaches a public host. The contract is
//! driven through the `EventSink` seam with a recording double, and the ONE
//! transport test binds `127.0.0.1:0` and lets the kernel pick the port. Every
//! child process runs under [`bounded_output`], because a runner that hangs
//! blocks the whole suite — the failure mode WORKER-RULES records from
//! 2026-09-15.

use logweir::notify::{
    deliver_with, exhaustive_claim_offences, insecure_sink_refusal, pagerduty_event, parse_event,
    protection_dedup_key, read_event, recovery_completed_dedup_key, sanitize_exhaustive_claims,
    scope_offences, slack_body, slack_text, webhook_body, Alert, AlertAction, AlertKind,
    DeliveryOutcome, EventSink, Health, LastAvailablePoint, PolicyAlertKind, PolicyRef,
    ProtectionEvent, SinkRoutes, VerificationScope, ALLOW_INSECURE_SINKS_ENV, CLAIM_REDACTED,
    EXHAUSTIVE_CLAIMS, MAX_EVENT_BYTES, NOTIFY_RESULT_LINE, PAGERDUTY_ENDPOINT_ENV,
    PAGERDUTY_SILENCED, PROTECTION_DEDUP_PREFIX, PROTECTION_EVENT_FORMAT_VERSION, ROUTING_KEY_ENV,
    SAMPLE_DISCLAIMER, SLACK_WEBHOOK_URL_ENV, WEBHOOK_URL_ENV,
};

// ----------------------------------------------------------------- fixtures

/// A routing key that is obviously fake and obviously a credential, so an
/// assertion that it did not leak reads as one. Same spelling as
/// `notify.rs`'s, deliberately: one grep finds every non-disclosure assertion
/// in the crate.
const TEST_ROUTING_KEY: &str = "R0FAKEFAKEFAKEFAKEFAKEFAKEFAKE0";

/// A Slack-shaped webhook whose PATH is the credential — the exact shape
/// `redact_url` exists for.
const TEST_SLACK_URL: &str = "https://hooks.slack.example/services/T00SECRET/B00SECRET/zzTOKENzz";

/// A generic webhook whose QUERY is the credential.
const TEST_WEBHOOK_URL: &str = "https://sink.example/hooks/protection?token=WEBHOOKSECRET";

/// The policy UID every fixture is about.
const POLICY_UID: &str = "11111111-2222-3333-4444-555555555555";

/// The Restore UID a `RecoveryCompleted` fixture keys on.
const RESTORE_UID: &str = "99999999-8888-7777-6666-555555555555";

fn event_of(kind: AlertKind, action: AlertAction, health: Health) -> ProtectionEvent {
    ProtectionEvent {
        format_version: PROTECTION_EVENT_FORMAT_VERSION.to_string(),
        event_id: "sha256:0123456789abcdef".to_string(),
        policy: PolicyRef {
            namespace: "team-a".to_string(),
            name: "orders-prod".to_string(),
            uid: POLICY_UID.to_string(),
        },
        alert: Alert {
            key: match kind.policy_keyed() {
                Some(k) => protection_dedup_key(POLICY_UID, k),
                // The fifth kind keys on a Restore UID and the builder that
                // takes a policy UID cannot be reached for it — see
                // `the_wrong_key_builder_does_not_compile_for_a_recovery`.
                None => recovery_completed_dedup_key(RESTORE_UID),
            },
            kind,
            action,
            transition: 3,
        },
        health,
        summary: "orders-prod: newest available recovery point is 31h old (objective 26h)"
            .to_string(),
        last_available_point: Some(LastAvailablePoint {
            point_id: "lwp1-0123456789".to_string(),
            recovery_point_at: "2026-09-15T01:00:00Z".parse().unwrap(),
            age_seconds: 111_600,
            evidence: "Valid".to_string(),
        }),
        consecutive_failed_runs: 2,
        missed_slots: 1,
        verification_scope: VerificationScope::Sampled,
        details_route: "#/protection?ns=team-a&name=orders-prod".to_string(),
        generated_at: "2026-09-16T08:00:00Z".parse().unwrap(),
    }
}

fn stale_event() -> ProtectionEvent {
    event_of(AlertKind::Staleness, AlertAction::Trigger, Health::Stale)
}

/// The worked example **read out of the published format document**, so the
/// page an operator reads and the Rust type are pinned to each other.
///
/// It was a hand-typed copy inside this file, and the report claimed it pinned
/// the decision's example. It pinned the copy: the copy and
/// `docs/formats/protection-event.md` had already drifted in `event_id`, and a
/// field the document renamed would have passed here in silence. Now the
/// fenced ```json block in that page IS the fixture, so a rename that the page
/// does not carry fails on the next line, and a page whose example stops
/// parsing fails too.
///
/// D3 §3.4's own block is not readable the same way — its `event_id` is the
/// prose placeholder `<sha256 of policyUID|alertKey|transition>` and its
/// `uid` is `...`, so it is an illustration rather than a document. The format
/// page is where the example became real, and that is the one to pin.
fn decision_example_json() -> String {
    let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/formats/protection-event.md");
    let text = std::fs::read_to_string(&doc)
        .unwrap_or_else(|e| panic!("{} is readable: {e}", doc.display()));
    let open = text
        .find("```json\n")
        .expect("docs/formats/protection-event.md carries a fenced ```json worked example");
    let body = &text[open + "```json\n".len()..];
    let close = body.find("```").expect("the fenced block closes");
    let json = body[..close].to_string();
    // The fixture must really be a protection event and not, say, the first
    // JSON block of some later section: an unchecked extraction that silently
    // returned the wrong block would make every row below a test of nothing.
    assert!(
        json.contains("\"format_version\"") && json.contains("\"verification_scope\""),
        "the first ```json block in the format doc is not the worked event:\n{json}"
    );
    json
}

/// The `EventSink` a test uses instead of a network.
#[derive(Default)]
struct RecordingSink {
    posts: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    /// URLs whose POST is reported as a failure. Matched by exact string.
    fail: Vec<String>,
}

impl RecordingSink {
    fn failing(urls: &[&str]) -> Self {
        Self {
            posts: std::sync::Mutex::new(Vec::new()),
            fail: urls.iter().map(|u| (*u).to_string()).collect(),
        }
    }

    fn posts(&self) -> Vec<(String, serde_json::Value)> {
        self.posts.lock().unwrap().clone()
    }
}

impl EventSink for RecordingSink {
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        self.posts
            .lock()
            .unwrap()
            .push((url.to_string(), body.clone()));
        if self.fail.iter().any(|u| u == url) {
            // Already redacted, exactly as the trait's contract requires.
            return Err("status code 500".to_string());
        }
        Ok(())
    }
}

/// Routes built from a map, never from the process environment:
/// `std::env::set_var` is process-global and libtest is threaded, so a test
/// that set a routing key would set it for every test running at that instant.
fn routes(pairs: &[(&str, &str)]) -> SinkRoutes {
    let owned: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    SinkRoutes::from_lookup(move |k| {
        owned
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
    })
}

fn all_three() -> SinkRoutes {
    routes(&[
        (ROUTING_KEY_ENV, TEST_ROUTING_KEY),
        (WEBHOOK_URL_ENV, TEST_WEBHOOK_URL),
        (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
    ])
}

/// The five alert kinds, the five health states and the three verification
/// scopes, as tables the cross-product rows walk.
const EVERY_KIND: [AlertKind; 5] = [
    AlertKind::BackupFailure,
    AlertKind::Staleness,
    AlertKind::ArchiveUnavailable,
    AlertKind::RehearsalFailure,
    AlertKind::RecoveryCompleted,
];
const EVERY_HEALTH: [Health; 5] = [
    Health::Healthy,
    Health::AtRisk,
    Health::Stale,
    Health::Unprotected,
    Health::Unknown,
];
const EVERY_POLICY_KIND: [PolicyAlertKind; 4] = [
    PolicyAlertKind::BackupFailure,
    PolicyAlertKind::Staleness,
    PolicyAlertKind::ArchiveUnavailable,
    PolicyAlertKind::RehearsalFailure,
];
const EVERY_SCOPE: [VerificationScope; 3] = [
    VerificationScope::Sampled,
    VerificationScope::Degraded,
    VerificationScope::None,
];

/// The whole of stdout plus every diagnostic, as one string.
///
/// **Not the whole of a pod log** — see [`deliver_capturing_logs`], which adds
/// the `tracing` half. Rows that are not about disclosure use this one.
fn everything(o: &DeliveryOutcome) -> String {
    format!("{}\n{}", o.stdout, o.diagnostics.join("\n"))
}

/// Install one permissive global subscriber, once per test binary, before any
/// thread-local capture below.
///
/// The same cure `crates/logweir/tests/notify.rs::ensure_global_subscriber`
/// documents at length: tracing caches each callsite's `Interest`
/// process-wide, and with no global default a callsite first registered from
/// an uncaptured thread caches `Interest::never()` — so a capturing test's log
/// comes back empty, intermittently, depending on test order. A permissive
/// global makes the registering thread always resolve to a subscriber that
/// says `always`.
fn ensure_global_subscriber() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Deliver with a JSON `tracing` subscriber installed on this thread, and hand
/// back the outcome **and every surface a pod log would carry**: stdout, the
/// stderr diagnostics, and the tracing events.
///
/// # This helper is the fix for a mutant that survived
///
/// `deliver_with` writes five `tracing` events, and a pod log is stdout and
/// stderr merged — `GET …/pods/{pod}/log` has no stream selector — so the
/// tracing line **is** the surface a log aggregator reads. The disclosure row
/// folded only `stdout` and `diagnostics`, so a mutant that leaked the routing
/// key as a `tracing` FIELD (rather than into a diagnostic string) left all 56
/// rows green while the shipped binary printed the key in full on stderr.
///
/// Folding the captured JSON in closes it: any leak through any of the three
/// channels now fails the same assertion.
fn deliver_capturing_logs(
    ev: &ProtectionEvent,
    routes: &SinkRoutes,
    sink: &dyn EventSink,
) -> (DeliveryOutcome, String) {
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
    let out = tracing::subscriber::with_default(subscriber, || deliver_with(ev, routes, sink));
    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    let folded = format!("{}\n{}\n{log}", out.stdout, out.diagnostics.join("\n"));
    (out, folded)
}

// ------------------------------------------------------ the event document

/// **`verification_scope` has exactly three values, and `complete` is not one.**
///
/// The tracker's migration note for PLAT-14.2/14.3 is "do not label a
/// signature as exhaustive data verification", and D3 §3.4 spells the
/// consequence: `sampled`, `degraded` or `none`, **never `complete`**. This
/// is the row of the decision that must not be implementable around, so it is
/// enforced by the TYPE — a three-variant enum — and this table is what proves
/// the type is the one doing the work.
///
/// KILLS: a fourth variant; `#[serde(other)]`; a `String` field with a
/// runtime check somebody can forget to call.
#[test]
fn verification_scope_parses_three_values_and_refuses_complete() {
    for (value, want) in [
        ("sampled", Some(VerificationScope::Sampled)),
        ("degraded", Some(VerificationScope::Degraded)),
        ("none", Some(VerificationScope::None)),
        ("complete", None),
        ("Complete", None),
        ("COMPLETE", None),
        ("exhaustive", None),
        ("full", None),
        ("", None),
    ] {
        let text = decision_example_json().replace(r#""verification_scope":"sampled""#, &{
            format!(r#""verification_scope":"{value}""#)
        });
        let got = parse_event(text.as_bytes());
        match want {
            Some(scope) => assert_eq!(
                got.as_ref().map(|e| e.verification_scope),
                Ok(scope),
                "`{value}` is one of D3 §3.4's three scopes: {got:?}"
            ),
            None => {
                let e = got.expect_err(&format!(
                    "`verification_scope: {value}` MUST NOT parse — Logweir verifies a \
                     SAMPLE, and this value reaches an incident responder deciding \
                     whether an archive can be trusted"
                ));
                assert!(
                    e.contains("sampled") && e.contains("degraded") && e.contains("none"),
                    "the refusal must name the three legal values so an operator can \
                     fix the producer; it said: {e}"
                );
            }
        }
    }
}

/// **D3 §3.4's worked example parses, field for field.**
///
/// The decision document shows one JSON object. If the Rust type and that
/// object disagree — a renamed field, a number where a string is, an optional
/// that is required — the protection controller (W6) writes a document this
/// subcommand refuses, and the first time anybody finds out is when a page
/// does not arrive.
#[test]
fn the_decision_documents_worked_example_parses() {
    let ev = parse_event(decision_example_json().as_bytes()).expect("D3 §3.4's example parses");
    assert_eq!(ev.policy.namespace, "team-a");
    assert_eq!(ev.policy.name, "orders-prod");
    assert_eq!(ev.policy.uid, POLICY_UID);
    assert_eq!(ev.alert.kind, AlertKind::Staleness);
    assert_eq!(ev.alert.action, AlertAction::Trigger);
    assert_eq!(ev.alert.transition, 3);
    assert_eq!(ev.health, Health::Stale);
    assert_eq!(ev.consecutive_failed_runs, 2);
    assert_eq!(ev.missed_slots, 1);
    assert_eq!(ev.verification_scope, VerificationScope::Sampled);
    assert_eq!(ev.details_route, "#/protection?ns=team-a&name=orders-prod");
    let p = ev
        .last_available_point
        .as_ref()
        .expect("the example carries a point");
    assert_eq!(p.age_seconds, 111_600);
    assert_eq!(p.evidence, "Valid");
}

/// **An unknown field is REFUSED, by name, and nothing is posted.**
///
/// A deliberate departure from `docs/stability.md`'s general format policy
/// (ignore unknown fields on a matching major), and the reason is on the type:
/// that policy is about signed archival documents read years later, this is a
/// control message between two components of one release, and the thing a
/// lenient reader silently drops is an alert detail an on-call responder then
/// never learns.
///
/// KILLS: dropping `#[serde(deny_unknown_fields)]`.
#[test]
fn an_unknown_field_is_refused_by_name() {
    let text = decision_example_json().replace(
        r#""health":"Stale""#,
        r#""health":"Stale","severity_override":"info""#,
    );
    let e = parse_event(text.as_bytes()).expect_err("an unknown field is refused");
    assert!(
        e.contains("severity_override"),
        "the refusal must NAME the field, or the producer cannot find it: {e}"
    );
}

/// **`format_version`'s major is checked, and a future major is refused FOR
/// BEING a future major.**
///
/// The order matters and is asserted: a `2.0.0` document is refused with a
/// message about the major, not about whatever fields major 2 added. "unknown
/// field `foo`" sends a reader looking for a typo; "major 2, this build reads
/// major 1" sends them to the upgrade they actually need.
///
/// KILLS: parsing the struct first and reporting serde's error; dropping the
/// major check entirely.
#[test]
fn the_format_version_major_is_checked_before_the_shape() {
    for (value, ok) in [
        ("1.0.0", true),
        ("1.4.0", true),
        ("1.0.9", true),
        ("2.0.0", false),
        ("0.9.0", false),
        ("banana", false),
    ] {
        let text = decision_example_json().replace(
            r#""format_version":"1.0.0""#,
            &format!(r#""format_version":"{value}""#),
        );
        let got = parse_event(text.as_bytes());
        assert_eq!(got.is_ok(), ok, "format_version `{value}`: {got:?}");
        if !ok && value != "banana" {
            let e = got.unwrap_err();
            assert!(
                e.contains("major") && e.contains(value),
                "a wrong major must name both the document's version and this build's \
                 major: {e}"
            );
        }
    }
    // And an absent one is refused rather than assumed to be 1.
    let text = decision_example_json().replace(r#""format_version":"1.0.0","#, "");
    let e = parse_event(text.as_bytes()).expect_err("no format_version is refused");
    assert!(e.contains("format_version"), "{e}");
}

/// **`last_available_point` is optional, and its absence is the fact.**
///
/// A `health: Unprotected` policy has no available point. Requiring the field
/// would make the one event that matters most unpublishable; inventing one
/// would print a recovery point that does not exist into an incident.
#[test]
fn an_unprotected_policy_has_no_last_available_point() {
    // The field is removed by re-serializing rather than by splicing text: the
    // fixture is now the published document's own block and its whitespace is
    // the page's, not this file's.
    let mut v: serde_json::Value = serde_json::from_str(&decision_example_json()).unwrap();
    let obj = v.as_object_mut().unwrap();
    obj.insert("health".into(), serde_json::json!("Unprotected"));
    assert!(
        obj.remove("last_available_point").is_some(),
        "the published example carries the field this row removes"
    );
    let text = v.to_string();
    let ev = parse_event(text.as_bytes()).expect("an Unprotected event needs no point");
    assert_eq!(ev.health, Health::Unprotected);
    assert!(ev.last_available_point.is_none());
    assert!(
        slack_text(&ev).contains("no available recovery point"),
        "the absence has to be SAID, not left blank: {}",
        slack_text(&ev)
    );
}

// ------------------------------------------------ the exhaustive-claim gate

/// **No prose any sink receives claims exhaustive verification — over the
/// whole cross-product of kind, health and scope.**
///
/// 5 × 5 × 3 = 75 events, each scanned on both prose surfaces this module
/// produces: the event's own `summary` and the Slack line `slack_text`
/// composes from it. The `RecoveryCompleted` rows are the ones that matter
/// twice: they carry the literal word "Completed" in `alert.kind` and in
/// `alert.key`, and they must PASS.
///
/// KILLS: composing prose that says "fully verified"; widening the gate to the
/// bare word and making `RecoveryCompleted` unpublishable.
#[test]
fn no_prose_any_sink_receives_claims_exhaustive_verification() {
    let mut checked = 0usize;
    for kind in EVERY_KIND {
        for health in EVERY_HEALTH {
            for scope in EVERY_SCOPE {
                let mut ev = event_of(kind, AlertAction::Trigger, health);
                ev.verification_scope = scope;
                for (name, prose) in [("summary", ev.summary.clone()), ("slack", slack_text(&ev))] {
                    checked += 1;
                    assert!(
                        exhaustive_claim_offences(&prose).is_empty(),
                        "{name} prose for {kind:?}/{health:?}/{scope:?} claims an \
                         exhaustive check: {:?}\n{prose}",
                        exhaustive_claim_offences(&prose)
                    );
                }
                // And the composed bodies carry a legal scope, which is the
                // half that still refuses to post.
                for body in [
                    webhook_body(&ev),
                    slack_body(&ev),
                    pagerduty_event(&ev, TEST_ROUTING_KEY),
                ] {
                    assert!(scope_offences(&body).is_empty(), "{body}");
                }
            }
        }
    }
    assert_eq!(
        checked,
        5 * 5 * 3 * 2,
        "the table must actually have walked"
    );
}

/// **THE REVIEWER'S REPRODUCTION. A policy named `exhaustive-backups`
/// delivers.**
///
/// `exhaustive` is a legal DNS-1123 label, so it is a legal
/// `ProtectionPolicy` name — and the first version of the claim gate scanned
/// the whole serialized body, which carries `policy.name`,
/// `policy.namespace`, `alert.key`, `details_route` and `point_id`. Every body
/// for that policy therefore contained the banned token, every sink refused,
/// and after the controller's three attempts (D3 §3.4.4: 60/300/900 s) that
/// policy's `Stale` and `Unprotected` pages reached **nobody, permanently and
/// silently**.
///
/// That is the outcome `dedup_key_advice`'s own rationale rejects in so many
/// words — a gate that converts a naming choice into a total loss of alerting
/// is strictly worse than the thing it catches — and the argument had not been
/// applied here.
///
/// KILLS: re-widening the scan to the serialized body, to `alert.key`, to
/// `details_route`, or to any other identifier field.
#[test]
fn a_policy_whose_name_contains_a_banned_word_still_delivers() {
    let mut ev = stale_event();
    ev.policy.name = "exhaustive-backups".to_string();
    ev.policy.namespace = "exhaustive-verification-team".to_string();
    ev.alert.key = "logweir-protection-exhaustive-uid-Staleness".to_string();
    ev.details_route =
        "#/protection?ns=exhaustive-verification-team&name=exhaustive-backups".to_string();
    if let Some(p) = ev.last_available_point.as_mut() {
        p.point_id = "lwp1-exhaustive-0001".to_string();
    }

    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:ok\n\
         notify-result=webhook:ok\n\
         notify-result=slack:ok\n",
        "an operator's NAMING CHOICE must never silence their own alerts; \
         diagnostics were {:?}",
        out.diagnostics
    );
    assert_eq!(out.code as u8, 0);
    assert_eq!(sink.posts().len(), 3);
    // The identifiers travel intact — sanitizing an identifier would be a
    // second defect, since `alert.key` IS PagerDuty's incident identity.
    let pd = sink
        .posts()
        .into_iter()
        .find(|(u, _)| u.contains("pagerduty"))
        .unwrap();
    assert_eq!(
        pd.1["dedup_key"],
        "logweir-protection-exhaustive-uid-Staleness"
    );
    assert_eq!(
        pd.1["payload"]["source"],
        "exhaustive-verification-team/exhaustive-backups"
    );
}

/// **The scan's subject is prose, and only prose.** The unit-level half of the
/// row above: every identifier shape asserted clean one at a time, so a
/// regression names which field was re-admitted.
#[test]
fn an_identifier_is_never_scanned_as_prose() {
    for identifier in [
        "exhaustive-backups",
        "exhaustive-verification-team",
        "logweir-protection-exhaustive-uid-Staleness",
        "#/protection?ns=team-a&name=exhaustive",
        "lwp1-exhaustive-0001",
        "sha256:exhaustivelyhashed",
    ] {
        let mut ev = stale_event();
        ev.policy.name = identifier.to_string();
        ev.policy.namespace = identifier.to_string();
        ev.alert.key = identifier.to_string();
        ev.details_route = identifier.to_string();
        ev.event_id = identifier.to_string();
        let out = deliver_with(&ev, &all_three(), &RecordingSink::default());
        assert_eq!(
            out.code as u8, 0,
            "`{identifier}` in an identity field must not stop delivery: {:?}",
            out.diagnostics
        );
        assert_eq!(out.stdout.matches(":ok").count(), 3, "{identifier}");
    }
}

/// **The disclaimer this module writes is a CONSTANT and carries no claim.**
///
/// The prose scan's subject is the controller's `summary`; the prose *this*
/// module authors needs a different guard, one an operator's naming choice
/// cannot trip and that cannot be edited without changing the source. This is
/// it.
///
/// KILLS: rewriting `SAMPLE_DISCLAIMER` as "not an exhaustive comparison" —
/// the wording the first draft used, which a truncating channel clips into
/// the claim itself.
#[test]
fn the_disclaimer_logweir_writes_carries_no_claim() {
    assert!(
        exhaustive_claim_offences(SAMPLE_DISCLAIMER).is_empty(),
        "`{SAMPLE_DISCLAIMER}` claims what Logweir does not do: {:?}",
        exhaustive_claim_offences(SAMPLE_DISCLAIMER)
    );
    assert!(
        slack_text(&stale_event()).contains(SAMPLE_DISCLAIMER),
        "the disclaimer has to actually reach the Slack line"
    );
    // The list itself has teeth: every phrase in it is found in itself.
    for claim in EXHAUSTIVE_CLAIMS {
        assert_eq!(
            exhaustive_claim_offences(&format!("orders-prod: {claim} yesterday")),
            vec![claim],
            "`{claim}` is in the list and must be found"
        );
    }
}

/// **A claim in the controller's `summary` is EDITED OUT and the alert is
/// still delivered.**
///
/// `summary` is the one field this subcommand does not compose — it arrives
/// from the controller and reaches a PagerDuty incident title verbatim — so it
/// is the field most likely to carry a sentence somebody wrote in a hurry.
///
/// It used to SUPPRESS the post. That was the wrong half of the trade, and the
/// same one `dedup_key_advice` already rejects: the alert underneath a badly
/// worded summary is still real and still needs a human, so dropping the page
/// punishes the responder for the controller's wording. Sanitizing costs them
/// one marker and keeps the page; the removal is reported on a named line so
/// the producer gets fixed.
///
/// KILLS: forwarding the claim unedited; suppressing the alert; sanitizing
/// only the body that happens to be composed first.
#[test]
fn a_claim_in_the_controllers_summary_is_edited_out_and_the_alert_still_goes() {
    let mut ev = stale_event();
    ev.summary = "orders-prod: verification is complete and every record was verified".to_string();

    let offences = exhaustive_claim_offences(&ev.summary);
    assert!(
        offences.contains(&"verification is complete")
            && offences.contains(&"every record was verified"),
        "both claims must be named: {offences:?}"
    );

    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:ok\n\
         notify-result=webhook:ok\n\
         notify-result=slack:ok\n",
        "the page still goes: {:?}",
        out.diagnostics
    );
    assert_eq!(out.code as u8, 0);
    assert!(
        everything(&out).contains("exhaustive"),
        "…and the edit is on a line an operator can grep: {}",
        everything(&out)
    );

    // EVERY body is composed from the sanitized event — a sink that has already
    // received the unedited sentence cannot be un-notified.
    assert_eq!(sink.posts().len(), 3);
    for (url, body) in sink.posts() {
        let text = body.to_string().to_lowercase();
        for claim in ["verification is complete", "every record was verified"] {
            assert!(!text.contains(claim), "`{claim}` reached {url}:\n{body}");
        }
        assert!(
            text.contains(&CLAIM_REDACTED.to_lowercase()),
            "the edit must be VISIBLE in the body, not a silent deletion:\n{body}"
        );
    }
}

/// **The sanitizer is pure, case-insensitive, and leaves the rest of the
/// sentence alone.**
#[test]
fn sanitizing_removes_the_claim_and_nothing_else() {
    for (input, want_removed) in [
        ("orders-prod is fine", vec![]),
        ("orders-prod: FULLY VERIFIED", vec!["fully verified"]),
        (
            "orders-prod: Verification Is Complete; exhaustive too",
            vec!["exhaustive", "verification is complete"],
        ),
    ] {
        let (clean, removed) = sanitize_exhaustive_claims(input);
        let mut got: Vec<&str> = removed.clone();
        got.sort_unstable();
        let mut want = want_removed.clone();
        want.sort_unstable();
        assert_eq!(got, want, "{input}");
        assert!(
            exhaustive_claim_offences(&clean).is_empty(),
            "sanitizing must leave nothing behind: `{clean}`"
        );
        if want_removed.is_empty() {
            assert_eq!(clean, input, "an innocent sentence is returned untouched");
        } else {
            assert!(
                clean.contains(CLAIM_REDACTED),
                "the edit is marked, never silent: `{clean}`"
            );
            assert!(
                clean.starts_with("orders-prod"),
                "the surrounding prose survives: `{clean}`"
            );
        }
    }
}

/// **`RecoveryCompleted` is publishable.** The narrow half of the gate above,
/// as its own row, because the day someone widens the phrase list to the bare
/// word this is the test that says what it cost.
#[test]
fn a_recovery_completed_event_is_not_mistaken_for_a_verification_claim() {
    let ev = event_of(
        AlertKind::RecoveryCompleted,
        AlertAction::Resolve,
        Health::Healthy,
    );
    assert!(ev.alert.key.contains("RecoveryCompleted"));
    for prose in [ev.summary.clone(), slack_text(&ev)] {
        assert!(
            exhaustive_claim_offences(&prose).is_empty(),
            "a restore that COMPLETED is a fact about a restore, not a claim about an \
             archive: {:?}",
            exhaustive_claim_offences(&prose)
        );
    }
    // And it really delivers, key and all.
    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(out.code as u8, 0, "{:?}", out.diagnostics);
    assert_eq!(sink.posts().len(), 2, "webhook and Slack, never PagerDuty");
}

// --------------------------------------------------------- the dedup keys

/// **D3 §3.3's key shape, exactly: `logweir-protection-<policyUID>-<kind>`.**
///
/// Exposed for W6 so the controller's ledger and the runner's report are one
/// implementation. The rows also assert the property the SHAPE exists for:
/// two policies never share a key, and a protection key can never be a drill
/// key — `dedup_key`/`failure_dedup_key` spell `logweir-drill-`, and a
/// protection `resolve` that closed a drill's open page would be the T0-15
/// defect arriving from a new direction.
#[test]
fn the_protection_dedup_key_is_the_policy_uid_and_the_kind() {
    assert_eq!(
        protection_dedup_key(POLICY_UID, PolicyAlertKind::Staleness),
        format!("logweir-protection-{POLICY_UID}-Staleness")
    );
    for kind in EVERY_POLICY_KIND {
        let a = protection_dedup_key("uid-a", kind);
        let b = protection_dedup_key("uid-b", kind);
        assert_ne!(a, b, "two policies must never share an incident");
        assert!(a.starts_with(PROTECTION_DEDUP_PREFIX));
        assert!(
            !a.starts_with("logweir-drill-"),
            "a protection resolve must not be able to close a drill's page: {a}"
        );
        assert!(a.ends_with(kind.as_str()));
    }
    // One kind per key: a policy's Staleness page is not its BackupFailure page,
    // and the fifth kind's key comes from the other builder entirely.
    let mut keys: std::collections::BTreeSet<String> = EVERY_POLICY_KIND
        .iter()
        .map(|k| protection_dedup_key(POLICY_UID, *k))
        .collect();
    keys.insert(recovery_completed_dedup_key(RESTORE_UID));
    assert_eq!(keys.len(), 5);
}

/// **THE WRONG BUILDER DOES NOT COMPILE.**
///
/// `protection_dedup_key` used to take an `AlertKind` and cheerfully answer
/// `logweir-protection-<policyUID>-RecoveryCompleted` — a WELL-FORMED key for
/// the wrong object, which is the worst kind of wrong: W6 would have keyed
/// every recovery of a policy onto one incident, so the second restore of the
/// day overwrites the first one's message and a responder reads about the
/// wrong restore. `dedup_key_advice` returned `None` early for that kind, so
/// nothing would even have warned.
///
/// The narrowing is now a TYPE. This row is the runtime half of it: the
/// narrowing is total for the four, and empty for the fifth. The compile-time
/// half cannot be written as a test — `protection_dedup_key(uid,
/// AlertKind::RecoveryCompleted)` is a type error — so it is asserted here
/// that the only bridge between the enums refuses exactly that one kind.
#[test]
fn the_wrong_key_builder_does_not_compile_for_a_recovery() {
    for kind in EVERY_KIND {
        match kind.policy_keyed() {
            Some(narrowed) => {
                assert_ne!(kind, AlertKind::RecoveryCompleted);
                assert_eq!(narrowed.as_str(), kind.as_str(), "one wire spelling");
                assert_eq!(AlertKind::from(narrowed), kind, "the bridge round-trips");
            }
            None => assert_eq!(
                kind,
                AlertKind::RecoveryCompleted,
                "`{kind:?}` is policy-keyed and must narrow"
            ),
        }
    }
    assert!(AlertKind::RecoveryCompleted.policy_keyed().is_none());
}

/// **A UID that identifies nothing falls back rather than degenerating.**
///
/// A blank UID would collapse the key to `logweir-protection--Staleness`: one
/// incident per KIND across every policy in the cluster, so one team's resolve
/// closes another team's open page. That is `dedup_key`'s `PLAN_HASH_IDENT_LEN`
/// defect (fix round 1, finding F3) arriving by a different route, and it is
/// SILENT — the page simply stops being about what you think it is about.
///
/// KILLS: `format!("logweir-protection-{uid}-{kind}")` with no guard.
#[test]
fn a_blank_uid_cannot_degenerate_into_a_shared_key() {
    for blank in ["", " ", "\t", "\n  "] {
        let k = protection_dedup_key(blank, PolicyAlertKind::Staleness);
        assert_eq!(k, "logweir-protection-unknown-Staleness");
        assert!(!k.contains("--"), "the degenerate shape is the defect: {k}");
        assert_eq!(
            recovery_completed_dedup_key(blank),
            "logweir-protection-unknown-RecoveryCompleted"
        );
    }
    // A UID with surrounding whitespace is the same identity as the trimmed
    // one — otherwise a projected field with a trailing newline opens a
    // SECOND incident for the same policy.
    assert_eq!(
        protection_dedup_key("  abc\n", PolicyAlertKind::Staleness),
        protection_dedup_key("abc", PolicyAlertKind::Staleness)
    );
}

/// **`RecoveryCompleted` keys on the RESTORE UID, not on `(policy, kind)`.**
///
/// D3 §3.3. A policy's points can be restored many times, and each of those is
/// its own completed recovery with its own topic names and counts; keying on
/// the policy would make the second restore of the day overwrite the first
/// one's message in the sink that dedups, and a responder would be reading
/// about the wrong restore.
#[test]
fn recovery_completed_keys_on_the_restore_uid() {
    let a = recovery_completed_dedup_key("restore-aaa");
    let b = recovery_completed_dedup_key("restore-bbb");
    assert_eq!(a, "logweir-protection-restore-aaa-RecoveryCompleted");
    assert_ne!(a, b, "two restores are two recoveries");
    // The shape is the family's — same prefix, same kind tail — so the key is
    // recognisably a protection key and cannot collide with a drill one.
    assert!(a.starts_with(PROTECTION_DEDUP_PREFIX) && a.ends_with("RecoveryCompleted"));
    // What differs is the identity, and `protection_dedup_key` can no longer
    // be handed a `RecoveryCompleted` to get it wrong with — see
    // `the_wrong_key_builder_does_not_compile_for_a_recovery`.
    assert_ne!(
        a,
        recovery_completed_dedup_key(POLICY_UID),
        "a recovery is keyed on the RESTORE, so two restores of one policy's points \
         are two messages and not one overwriting the other"
    );
}

// ------------------------------------------------------ the sink selection

/// **A delivery with NO configured sink is `none:unconfigured` and exit 1 —
/// never a bare, silent success.**
///
/// The review's F3. "Nothing was configured" and "every sink accepted" used to
/// be the SAME machine-readable answer: empty stdout, exit 0. A `secretKeyRef`
/// that was rotated, renamed or left blank projects `Ok("")`, the emptiness
/// filter correctly reports it as unconfigured — and W6 then recorded
/// `delivery.state=Delivered` for an alert that reached nobody. D3 §3.4.2's
/// "exits 0 only when every configured sink accepted" was vacuously satisfied.
///
/// Both shapes are driven — nothing set at all, and all three set to a blank
/// value — because they are one finding with two causes and an operator fixes
/// them in different places.
///
/// KILLS: returning `ExitCode::Ok` with empty stdout for a delivery that
/// reached nobody; printing `none:failed`, which would be indistinguishable
/// from a sink that refused.
#[test]
fn a_delivery_with_no_sink_says_so_and_does_not_report_success() {
    for (arm, r) in [
        ("nothing is set", routes(&[])),
        (
            "every variable is present and blank",
            routes(&[
                (ROUTING_KEY_ENV, ""),
                (WEBHOOK_URL_ENV, ""),
                (SLACK_WEBHOOK_URL_ENV, "   "),
            ]),
        ),
    ] {
        let sink = RecordingSink::default();
        let out = deliver_with(&stale_event(), &r, &sink);
        assert_eq!(
            out.stdout, "notify-result=none:unconfigured\n",
            "{arm}: the one line that says the alert reached nobody"
        );
        assert_eq!(
            out.code as u8, 1,
            "{arm}: a delivery Job that delivered nothing did not deliver"
        );
        assert!(sink.posts().is_empty(), "{arm}");
        assert!(
            everything(&out).contains("BLANK"),
            "{arm}: the diagnostic must point at the projected Secret keys, which is \
             where the operator fixes it:\n{}",
            everything(&out)
        );
    }
}

/// **`none` can never be mistaken for a sink.** The line is machine-readable
/// precisely because the pseudo-sink name collides with nothing.
#[test]
fn the_unconfigured_line_is_distinguishable_from_every_sink_result() {
    let out = deliver_with(&stale_event(), &routes(&[]), &RecordingSink::default());
    for real in ["pagerduty", "webhook", "slack"] {
        assert!(
            !out.stdout.contains(&format!("notify-result={real}")),
            "`none` must not look like `{real}`: {}",
            out.stdout
        );
    }
    assert!(!out.stdout.contains(":ok") && !out.stdout.contains(":failed"));
}

/// **A sink is configured exactly when its variable holds a NON-BLANK value.**
///
/// `std::env::var` returns `Ok("")` — not `Err(NotPresent)` — for a Kubernetes
/// `env:` entry with an empty `value:`, and a `secretKeyRef` to a present-but-
/// blank key projects the same thing. A routing key of `""` is not a routing
/// key: treating it as configured posts an event PagerDuty refuses, reports
/// `pagerduty:failed`, and burns all three of the controller's attempts on a
/// route nobody configured.
///
/// KILLS: `std::env::var(k).ok()` with no emptiness filter.
#[test]
fn a_blank_variable_is_not_a_configured_sink() {
    for blank in ["", " ", "\n"] {
        let r = routes(&[
            (ROUTING_KEY_ENV, blank),
            (WEBHOOK_URL_ENV, blank),
            (SLACK_WEBHOOK_URL_ENV, blank),
        ]);
        assert!(
            r.configured_for(AlertKind::Staleness).is_empty(),
            "`{blank:?}` is not a credential"
        );
        let out = deliver_with(&stale_event(), &r, &RecordingSink::default());
        assert_eq!(
            out.stdout, "notify-result=none:unconfigured\n",
            "a blank credential is not a credential, and the outcome says so"
        );
        assert_eq!(out.code as u8, 1);
    }
}

/// **The three sinks, in the fixed contract order, one line each.**
///
/// The order is `pagerduty`, `webhook`, `slack` and it does not vary. A
/// controller reads by key name (spec §7 amendment 4), so the order is not
/// what makes the parse work — it is what makes the contract one thing rather
/// than six spellings of it, which is why this compares the WHOLE string
/// instead of three `contains` calls.
#[test]
fn every_configured_sink_gets_one_line_in_the_contract_order() {
    let ev = stale_event();
    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:ok\n\
         notify-result=webhook:ok\n\
         notify-result=slack:ok\n"
    );
    assert_eq!(out.code as u8, 0);
    let posted: Vec<String> = sink.posts().into_iter().map(|(u, _)| u).collect();
    assert_eq!(
        posted,
        vec![
            "https://events.pagerduty.com/v2/enqueue".to_string(),
            TEST_WEBHOOK_URL.to_string(),
            TEST_SLACK_URL.to_string(),
        ],
        "one POST per sink, in the same order, and the PagerDuty default is the US \
         region constant rather than a second literal"
    );
    // Each line is a whole line: a reader that splits the tail on '\n' and
    // takes the last non-empty entry must get a complete contract line.
    for line in out.stdout.lines() {
        assert!(line.starts_with(NOTIFY_RESULT_LINE), "{line}");
        assert!(line.ends_with(":ok"), "{line}");
    }
}

/// **`RecoveryCompleted` never reaches PagerDuty, even with a routing key
/// set.**
///
/// D3 §3.3 makes it webhook/Slack only: it is informational, it auto-resolves
/// immediately, and a PagerDuty incident under it would be opened and closed
/// in the same breath — a page for something that went RIGHT, at 03:00, with
/// nothing for the responder to do. A configured routing key is therefore not
/// a configured SINK for this kind, and prints no line.
///
/// KILLS: routing every kind to every configured sink.
#[test]
fn a_recovery_completed_event_is_webhook_and_slack_only() {
    let ev = event_of(
        AlertKind::RecoveryCompleted,
        AlertAction::Resolve,
        Health::Healthy,
    );
    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(
        out.stdout, "notify-result=webhook:ok\nnotify-result=slack:ok\n",
        "no `pagerduty` line at all: the route is not a sink for this kind"
    );
    assert_eq!(out.code as u8, 0);
    assert!(
        !sink
            .posts()
            .iter()
            .any(|(u, _)| u.contains("pagerduty.com")),
        "nothing may reach PagerDuty: {:?}",
        sink.posts()
    );

    // And with ONLY the routing key set, there is no sink at all for this kind
    // — which is `none:unconfigured` and exit 1, not a silent success. An
    // operator who configured a page for their recoveries and got nothing must
    // be able to see that from the Job, not from prose.
    let only_pd = routes(&[(ROUTING_KEY_ENV, TEST_ROUTING_KEY)]);
    let out = deliver_with(&ev, &only_pd, &RecordingSink::default());
    assert_eq!(out.stdout, "notify-result=none:unconfigured\n");
    assert_eq!(out.code as u8, 1);
    assert!(
        everything(&out).contains("webhook/Slack only"),
        "an operator who set only a routing key must be told why nothing was sent: {}",
        everything(&out)
    );
}

// ----------------------------------------------------- the PagerDuty event

/// **`event_action` is `alert.action` and `dedup_key` is `alert.key` — neither
/// is invented here.**
///
/// That is what makes the controller's alert ledger and PagerDuty's incident
/// the same object: `trigger` on Open and `resolve` on Resolved under one
/// stable key. A `resolve` that carried a different key would leave the
/// incident open forever, which is the failure the whole dedup contract
/// exists to prevent.
#[test]
fn the_pagerduty_envelope_carries_the_documents_action_and_key() {
    for (action, wire) in [
        (AlertAction::Trigger, "trigger"),
        (AlertAction::Resolve, "resolve"),
    ] {
        let ev = event_of(AlertKind::Staleness, action, Health::Stale);
        let body = pagerduty_event(&ev, TEST_ROUTING_KEY);
        assert_eq!(body["event_action"], wire);
        assert_eq!(body["dedup_key"], ev.alert.key);
        assert_eq!(body["routing_key"], TEST_ROUTING_KEY);
        assert_eq!(body["payload"]["source"], "team-a/orders-prod");
        assert_eq!(body["payload"]["summary"], ev.summary);
        assert_eq!(
            body["payload"]["custom_details"]["verification_scope"], "sampled",
            "the scope travels into the incident: it is what a responder reads before \
             deciding whether the archive can be trusted"
        );
    }
}

/// **Severity is a function of HEALTH, and it is a table rather than a
/// comment.**
///
/// Coarse on purpose: a severity scale nobody can predict is a severity scale
/// nobody routes on. `Unprotected` (there is nothing to recover from) and
/// `Stale` (the newest point is outside the objective) page as `critical`;
/// everything else is a `warning`.
#[test]
fn severity_is_critical_only_when_the_archive_cannot_meet_the_objective() {
    for (health, want) in [
        (Health::Healthy, "warning"),
        (Health::AtRisk, "warning"),
        (Health::Unknown, "warning"),
        (Health::Stale, "critical"),
        (Health::Unprotected, "critical"),
    ] {
        let ev = event_of(AlertKind::Staleness, AlertAction::Trigger, health);
        assert_eq!(
            pagerduty_event(&ev, TEST_ROUTING_KEY)["payload"]["severity"],
            want,
            "{health:?}"
        );
    }
}

/// **A non-https PagerDuty endpoint is refused before a request, says
/// `PAGERDUTY_SILENCED`, and does not stop the other two sinks.**
///
/// The routing key travels in the request BODY, so a plaintext endpoint would
/// put a bearer credential on the wire. The refusal reuses
/// `pagerduty_endpoint` rather than re-deciding it, so the drill path and this
/// one cannot disagree about what an acceptable endpoint is.
///
/// KILLS: an early return that skips webhook and Slack; a second, laxer
/// endpoint check.
#[test]
fn a_plaintext_pagerduty_endpoint_is_silenced_and_the_other_sinks_still_go() {
    let r = routes(&[
        (ROUTING_KEY_ENV, TEST_ROUTING_KEY),
        (
            PAGERDUTY_ENDPOINT_ENV,
            "http://events.pagerduty.example/v2/enqueue",
        ),
        (WEBHOOK_URL_ENV, TEST_WEBHOOK_URL),
        (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
    ]);
    let sink = RecordingSink::default();
    let out = deliver_with(&stale_event(), &r, &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:failed\n\
         notify-result=webhook:ok\n\
         notify-result=slack:ok\n",
        "a responder reachable by TWO of three channels must be reached"
    );
    assert_eq!(out.code as u8, 1);
    assert!(
        everything(&out).contains(PAGERDUTY_SILENCED),
        "the one line an operator greps for when a page did not arrive: {}",
        everything(&out)
    );
    assert!(
        !sink.posts().iter().any(|(u, _)| u.starts_with("http://")),
        "nothing may be posted to a plaintext endpoint: {:?}",
        sink.posts()
    );
}

/// **A non-default endpoint is honoured, so an EU-region adopter can be
/// paged.**
#[test]
fn a_configured_service_region_is_where_the_event_goes() {
    let r = routes(&[
        (ROUTING_KEY_ENV, TEST_ROUTING_KEY),
        (
            PAGERDUTY_ENDPOINT_ENV,
            "https://events.eu.pagerduty.com/v2/enqueue",
        ),
    ]);
    let sink = RecordingSink::default();
    let out = deliver_with(&stale_event(), &r, &sink);
    assert_eq!(out.stdout, "notify-result=pagerduty:ok\n");
    assert_eq!(
        sink.posts()[0].0,
        "https://events.eu.pagerduty.com/v2/enqueue",
        "an adopter whose account is on the EU region must not enqueue into the US one"
    );
}

// ------------------------------------------------------- failure behaviour

/// **A sink that refuses is `failed`, the exit is non-zero, and every OTHER
/// sink is still attempted.**
///
/// The tracker's row is "notification transport failure"; D3 §3.4 point 4 has
/// the controller read the exit code AND the `notify-result=` lines. Both have
/// to be true at once, and the early-return mutant breaks only the second.
///
/// KILLS: returning `ExitCode::Ok` when a sink failed; `?`-ing out of the
/// loop on the first failure.
#[test]
fn a_failed_sink_is_reported_and_does_not_stop_the_others() {
    let sink = RecordingSink::failing(&[TEST_WEBHOOK_URL]);
    let out = deliver_with(&stale_event(), &all_three(), &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:ok\n\
         notify-result=webhook:failed\n\
         notify-result=slack:ok\n"
    );
    assert_eq!(
        out.code as u8, 1,
        "exit 0 only when EVERY configured sink accepted"
    );
    assert_eq!(sink.posts().len(), 3, "all three were attempted");
}

/// **Every sink failing is still one line per sink, and still exit 1.**
#[test]
fn all_sinks_failing_is_one_failed_line_each() {
    let sink = RecordingSink::failing(&[
        "https://events.pagerduty.com/v2/enqueue",
        TEST_WEBHOOK_URL,
        TEST_SLACK_URL,
    ]);
    let out = deliver_with(&stale_event(), &all_three(), &sink);
    assert_eq!(
        out.stdout,
        "notify-result=pagerduty:failed\n\
         notify-result=webhook:failed\n\
         notify-result=slack:failed\n"
    );
    assert_eq!(out.code as u8, 1);
}

/// **A mismatched dedup key WARNS and still delivers.**
///
/// It was written as a refusal first. A wrong key is a real defect — it
/// collapses incidents, or leaves one open forever — but the two failure modes
/// are not symmetric: a mis-keyed page still reaches a human who can act on
/// the incident, and a refused one reaches nobody at all. Refusing would turn
/// a dedup bug into a total loss of alerting.
///
/// KILLS: refusing the document; dropping the check so a controller bug is
/// silent.
#[test]
fn a_mismatched_dedup_key_is_a_warning_not_a_refusal() {
    let mut ev = stale_event();
    ev.alert.key = "logweir-protection-SOMEONE-ELSES-UID-Staleness".to_string();
    let sink = RecordingSink::default();
    let out = deliver_with(&ev, &all_three(), &sink);
    assert_eq!(out.code as u8, 0, "the page still goes");
    assert_eq!(sink.posts().len(), 3);
    assert!(
        everything(&out).contains("is not the `(policy, kind)` key"),
        "…and the defect is on a line an operator can grep: {}",
        everything(&out)
    );
    // The key that was GIVEN is the key that is posted: the controller owns
    // its ledger, and a runner that silently corrected the key would resolve
    // an incident the controller never opened.
    let pd = sink
        .posts()
        .into_iter()
        .find(|(u, _)| u.contains("pagerduty"))
        .unwrap();
    assert_eq!(pd.1["dedup_key"], ev.alert.key);
}

/// **A key from another family is named too.** `logweir-drill-…` on a
/// protection event would let a protection `resolve` close a drill's open
/// page — T0-15's defect, from a new direction.
#[test]
fn a_key_from_another_family_is_named() {
    let mut ev = stale_event();
    ev.alert.key = "logweir-drill-nightly-cluster-a".to_string();
    let out = deliver_with(&ev, &all_three(), &RecordingSink::default());
    assert!(
        everything(&out).contains(PROTECTION_DEDUP_PREFIX),
        "{}",
        everything(&out)
    );
}

/// **A `http://` webhook or Slack URL is refused before a request, unless the
/// operator set the documented override.**
///
/// A webhook URL is a bearer credential in its own right — a signed webhook
/// puts the token in the query, a Slack incoming webhook puts it in the path —
/// and the whole protection event travels beside it. `http://` puts both on
/// the wire in cleartext with no refusal and no warning, which is the review's
/// F7.
///
/// PagerDuty's endpoint has been https-only since fix round 1, one layer up,
/// through `pagerduty_endpoint`; this is the same rule for the other two, and
/// the refusal names the SCHEME only because the rest of the URL is where the
/// token lives and this string reaches a log.
///
/// KILLS: dropping the check; honouring the override by default; naming the
/// whole URL in the refusal.
#[test]
fn a_plaintext_webhook_is_refused_unless_the_override_is_set() {
    let insecure = "http://sink.example/hooks?token=WEBHOOKSECRET";

    let strict = routes(&[
        (WEBHOOK_URL_ENV, insecure),
        (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
    ]);
    let sink = RecordingSink::default();
    let out = deliver_with(&stale_event(), &strict, &sink);
    assert_eq!(
        out.stdout, "notify-result=webhook:failed\nnotify-result=slack:ok\n",
        "the https sink still goes: a responder reachable by one channel must be"
    );
    assert_eq!(out.code as u8, 1);
    assert!(
        !sink.posts().iter().any(|(u, _)| u.starts_with("http://")),
        "nothing may be posted in cleartext: {:?}",
        sink.posts()
    );
    let surfaces = everything(&out);
    assert!(
        surfaces.contains("refusing scheme `http`") && surfaces.contains(ALLOW_INSECURE_SINKS_ENV),
        "the refusal must name the scheme AND the way out: {surfaces}"
    );
    assert!(
        !surfaces.contains("WEBHOOKSECRET") && !surfaces.contains("token="),
        "…and must not echo the token it is protecting: {surfaces}"
    );

    // The override, which is what the loopback rows below rely on.
    let permissive = routes(&[
        (WEBHOOK_URL_ENV, insecure),
        (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
        (ALLOW_INSECURE_SINKS_ENV, "1"),
    ]);
    let sink = RecordingSink::default();
    let out = deliver_with(&stale_event(), &permissive, &sink);
    assert_eq!(
        out.stdout,
        "notify-result=webhook:ok\nnotify-result=slack:ok\n"
    );
    assert_eq!(out.code as u8, 0);
    assert_eq!(sink.posts().len(), 2);
}

/// **The scheme rule and its override, as a pure table.**
///
/// The override is an explicit affirmative only: `0`, `false` and a blank
/// value all mean "no", so an operator who switches it off does not discover
/// it was still on.
#[test]
fn the_scheme_rule_is_https_or_an_explicit_override() {
    for (url, allow, ok) in [
        ("https://sink.example/h", false, true),
        ("https://sink.example/h", true, true),
        ("http://sink.example/h", false, false),
        ("http://sink.example/h", true, true),
        ("http://127.0.0.1:8080/h", false, false),
        ("ftp://sink.example/h", false, false),
        ("sink.example/h", false, false),
    ] {
        assert_eq!(
            insecure_sink_refusal(url, allow).is_ok(),
            ok,
            "{url} with allow_insecure={allow}"
        );
    }
    // Loopback is NOT special-cased. A special case for `127.0.0.1` reads as
    // safe and is not: it would also have to decide about `localhost`, about
    // an IPv6 loopback, and about a pod-network address that merely looks
    // local. One greppable variable in a Job spec is a decision an operator
    // makes on purpose; a hostname rule is one they inherit.
    assert!(insecure_sink_refusal("http://127.0.0.1:9/h", false).is_err());

    for (value, want) in [
        ("1", true),
        ("true", true),
        ("TRUE", true),
        ("yes", true),
        ("0", false),
        ("false", false),
        ("", false),
        ("maybe", false),
    ] {
        assert_eq!(
            routes(&[(ALLOW_INSECURE_SINKS_ENV, value)]).allow_insecure_sinks,
            want,
            "{ALLOW_INSECURE_SINKS_ENV}={value:?}"
        );
    }
}

// ------------------------------------------------------------- disclosure

/// **NOTHING this subcommand writes carries a routing key or an unredacted
/// sink URL.**
///
/// Three credentials, three shapes: a routing key that travels in a request
/// body, a Slack URL whose PATH is the credential, and a webhook URL whose
/// QUERY is. Every one of them is checked against the whole of stdout and
/// every diagnostic, on the success arm AND the failure arm — a credential
/// that is safe on one and logged on the other is still logged, which is fix
/// round 1's finding F1.
///
/// KILLS: `error = %e` with a `ureq::Error` whose `Display` embeds the URL;
/// logging the endpoint verbatim; printing the routing key in a diagnostic.
#[test]
fn no_surface_carries_a_routing_key_or_an_unredacted_sink_url() {
    // A claim in `summary`, so the claim-refusal branch composes a diagnostic
    // too — every branch that writes a sentence has to be walked, not just the
    // two obvious ones. A leak on the arm nobody enumerated is still a leak,
    // and that is exactly how the first version of this row let a mutant
    // through: it drove only the default routes, so the SILENCED-endpoint
    // branch — the one that has the routing key in scope — was never reached.
    let mut claiming = stale_event();
    claiming.summary = "orders-prod: fully verified".to_string();

    for (arm, event, routes, sink) in [
        (
            "every sink accepts",
            stale_event(),
            all_three(),
            RecordingSink::default(),
        ),
        (
            "every sink refuses",
            stale_event(),
            all_three(),
            RecordingSink::failing(&[
                "https://events.pagerduty.com/v2/enqueue",
                TEST_WEBHOOK_URL,
                TEST_SLACK_URL,
            ]),
        ),
        (
            "the PagerDuty endpoint is refused before a request",
            stale_event(),
            routes(&[
                (ROUTING_KEY_ENV, TEST_ROUTING_KEY),
                (
                    PAGERDUTY_ENDPOINT_ENV,
                    "http://events.pagerduty.example/v2/enqueue",
                ),
                (WEBHOOK_URL_ENV, TEST_WEBHOOK_URL),
                (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
            ]),
            RecordingSink::default(),
        ),
        (
            "the summary is sanitized for claiming an exhaustive check",
            claiming,
            all_three(),
            RecordingSink::default(),
        ),
        (
            "a plaintext webhook is refused for its scheme",
            stale_event(),
            routes(&[
                (ROUTING_KEY_ENV, TEST_ROUTING_KEY),
                (
                    WEBHOOK_URL_ENV,
                    "http://sink.example/hooks?token=WEBHOOKSECRET",
                ),
                (SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
            ]),
            RecordingSink::default(),
        ),
        (
            "nothing is configured at all",
            stale_event(),
            routes(&[(ROUTING_KEY_ENV, "")]),
            RecordingSink::default(),
        ),
    ] {
        // THE TRACING HALF IS FOLDED IN. A pod log is stdout and stderr
        // merged, so a `tracing` field is a surface a log aggregator reads —
        // and a mutant that leaked the routing key as a FIELD rather than into
        // a diagnostic string survived every row of both suites until this
        // helper existed.
        let (out, surfaces) = deliver_capturing_logs(&event, &routes, &sink);
        let surfaces = format!("{arm}\n{surfaces}");
        assert!(
            surfaces.contains("logweir::notify"),
            "{arm}: no tracing event was captured, so the tracing half of this \
             assertion is vacuous:\n{surfaces}"
        );
        for secret in [
            TEST_ROUTING_KEY,
            "T00SECRET",
            "B00SECRET",
            "zzTOKENzz",
            "WEBHOOKSECRET",
            "token=",
            "/services/",
        ] {
            assert!(
                !surfaces.contains(secret),
                "{arm}: `{secret}` reached a surface a human or a log aggregator \
                 reads:\n{surfaces}"
            );
        }
        // …while the sink is still IDENTIFIABLE, which is the whole diagnostic
        // value of naming it at all. Not asserted on the arm that HAS no sink:
        // "nothing was configured" is a diagnostic about the Job's projection,
        // not about a sink, and demanding a host in it would be demanding a
        // fact that does not exist.
        if !out.diagnostics.is_empty() && !out.stdout.contains("none:unconfigured") {
            assert!(
                surfaces.contains("hooks.slack.example")
                    || surfaces.contains("sink.example")
                    || surfaces.contains("pagerduty"),
                "{arm}: a failure an operator cannot attribute to a sink is not a \
                 diagnostic:\n{surfaces}"
            );
        }
    }
}

/// **STRUCTURAL: no `tracing` call in `notify.rs` names a credential-bearing
/// field, redacted or not.**
///
/// The behavioural rows above catch a leak that a test happens to drive. This
/// one catches the SHAPE, which is what a reviewer scanning a diff needs: the
/// four `SinkRoutes` fields that can hold a credential — and the raw `url`
/// binding they are copied into — must never appear inside a `tracing!`
/// invocation. A URL reaches a log line only as `redact_url(…)` or as the
/// `shown` binding that holds its result.
///
/// This is the discipline `every_notification_post_goes_through_the_bounded_agent`
/// already uses next door: when the property is "there is exactly one way to
/// do this", assert over the source rather than hope a behavioural test covers
/// the next way someone adds.
///
/// KILLS: `routing_key = %routes.pagerduty_routing_key…`, `endpoint = %url`,
/// `webhook = %routes.webhook_url…` — the reviewer's surviving mutant and
/// every sibling of it.
#[test]
fn no_tracing_call_names_a_credential_bearing_binding() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/notify.rs");
    let text = std::fs::read_to_string(&src).unwrap();

    // Every `tracing::<level>!( … );` invocation, by brace-free paren match —
    // these are all single-call macros with no nested parens beyond `%foo(…)`.
    let mut calls = Vec::new();
    let mut rest = text.as_str();
    while let Some(at) = rest.find("tracing::") {
        let from = &rest[at..];
        let Some(open) = from.find('(') else { break };
        let mut depth = 0usize;
        let mut end = None;
        for (i, c) in from[open..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else { break };
        calls.push(from[..=end].to_string());
        rest = &from[end..];
    }
    assert!(
        calls.len() >= 10,
        "the scan found only {} tracing calls in {} — it is not looking at the file \
         it claims to be checking",
        calls.len(),
        src.display()
    );

    const FORBIDDEN: [&str; 6] = [
        "pagerduty_routing_key",
        "webhook_url",
        "slack_webhook_url",
        "pagerduty_endpoint",
        "%url",
        "?url",
    ];
    let mut offenders = Vec::new();
    for call in &calls {
        for needle in FORBIDDEN {
            if call.contains(needle) {
                offenders.push(format!("`{needle}` in: {}", call.replace('\n', " ")));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a `tracing` field is a surface a log aggregator reads — a pod log is stdout \
         and stderr merged, with no stream selector. A credential-bearing binding may \
         reach one only through `redact_url`, and a routing key not at all.\n  {}",
        offenders.join("\n  ")
    );
}

/// **`SinkRoutes`' `Debug` discloses presence and nothing else.**
///
/// Hand-written for the reason `logweir_core::spec::Notifications`' is: this
/// struct sits one `{:?}` away from an error message at all times, and there
/// is no `{:?}` site today — this exists so that ADDING one is not a
/// disclosure.
///
/// KILLS: `#[derive(Debug)]`.
#[test]
fn debugging_the_routes_prints_no_secret() {
    let text = format!("{:?}", all_three());
    for secret in [TEST_ROUTING_KEY, "T00SECRET", "zzTOKENzz", "WEBHOOKSECRET"] {
        assert!(
            !text.contains(secret),
            "{secret} leaked through Debug: {text}"
        );
    }
    assert!(
        text.contains("***"),
        "presence is still reported: `no sink configured` and `a sink configured and \
         failed` are different findings: {text}"
    );
    assert!(
        text.contains("hooks.slack.example"),
        "the HOST survives, because that is the whole diagnostic: {text}"
    );
}

// ------------------------------------------------------------ the file read

/// **A file that is not a readable protection event is refused, by name, and
/// nothing is posted.**
#[test]
fn the_event_file_is_read_defensively() {
    let dir = tempfile::tempdir().unwrap();

    let missing = dir.path().join("absent.json");
    let e = read_event(&missing).expect_err("a missing file is refused");
    assert!(e.contains("absent.json"), "{e}");

    let a_dir = dir.path().join("not-a-file");
    std::fs::create_dir(&a_dir).unwrap();
    let e = read_event(&a_dir).expect_err("a directory is refused");
    assert!(e.contains("not a regular file"), "{e}");

    let garbage = dir.path().join("garbage.json");
    std::fs::write(&garbage, b"not json at all").unwrap();
    let e = read_event(&garbage).expect_err("non-JSON is refused");
    assert!(e.contains("not JSON"), "{e}");

    let good = dir.path().join("event.json");
    std::fs::write(&good, decision_example_json()).unwrap();
    read_event(&good).expect("the worked example round-trips through the file read");
}

/// **A file larger than a ConfigMap can hold is refused before it is read
/// into memory.**
///
/// The document arrives as a ConfigMap key and the API server caps a ConfigMap
/// at 1 MiB, so a larger file is not a protection event by construction. The
/// bound is what turns a wrong `--event` path — a log, a core file, a mounted
/// archive — into a named refusal in milliseconds instead of a gigabyte read
/// inside a Job with `activeDeadlineSeconds: 120`.
#[test]
fn an_oversized_event_file_is_refused_before_it_is_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.json");
    // One byte over, and the content is valid JSON, so the only thing that can
    // refuse it is the bound.
    let padding = "x".repeat(usize::try_from(MAX_EVENT_BYTES).unwrap());
    std::fs::write(&big, format!(r#"{{"pad":"{padding}"}}"#)).unwrap();
    let e = read_event(&big).expect_err("an oversized file is refused");
    assert!(
        e.contains("ConfigMap") && e.contains(&MAX_EVENT_BYTES.to_string()),
        "the refusal must say what the bound is and why: {e}"
    );
}

// --------------------------------------------------- the shipped process

/// Run the shipped binary with a DEADLINE, killing it if it overruns.
///
/// Every subprocess a test spawns must have a timeout: a hung child blocks the
/// whole worker, which is the failure WORKER-RULES records from 2026-09-15.
/// `Command::output()` and `Child::wait()` have no bound of their own, so the
/// wait is a `try_wait` poll against a deadline — and the child is **killed and
/// then reaped** on the timeout arm, because a `kill` without a `wait` leaves a
/// zombie for as long as the test binary lives.
///
/// The output is read AFTER the child has exited, which is safe only because
/// the outputs here are a few hundred bytes: a child that filled the pipe
/// buffer would block on its own write and never reach the exit this polls for.
/// A larger-output child would need the reads on their own threads.
fn bounded_output(cmd: &mut std::process::Command, deadline: std::time::Duration) -> BoundedRun {
    let mut child = cmd
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the shipped binary is spawnable");
    let started = std::time::Instant::now();
    let code = loop {
        match child.try_wait().expect("try_wait on the child") {
            Some(status) => break status.code(),
            None => {
                if started.elapsed() > deadline {
                    let _ = child.kill();
                    // REAPED, not just killed.
                    let _ = child.wait();
                    panic!(
                        "`logweir notify deliver` did not exit within {deadline:?} — a \
                         runner that hangs inside a Job with \
                         `activeDeadlineSeconds: 120` is the unbounded-sink defect \
                         Task 5b removed"
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    };
    let (mut out, mut err) = child
        .stdout
        .take()
        .zip(child.stderr.take())
        .expect("piped stdout and stderr");
    let mut stdout = String::new();
    let mut stderr = String::new();
    use std::io::Read;
    let _ = out.read_to_string(&mut stdout);
    let _ = err.read_to_string(&mut stderr);
    BoundedRun {
        code,
        stdout,
        stderr,
    }
}

struct BoundedRun {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// A loopback listener that accepts, reads the whole request and answers a
/// chosen status. `127.0.0.1:0` — the kernel picks the port and nothing leaves
/// the loopback interface (GC17). Returns the URL and the bytes it received.
///
/// The same arrangement as `notify.rs`'s `accepting_listener`; it is not
/// shared because the two test binaries do not share a support module and a
/// `mod support;` entry for twenty lines of listener would be its own kind of
/// coupling.
fn listener(status: &'static str) -> (String, std::sync::Arc<std::sync::Mutex<Vec<u8>>>) {
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

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
            sink.lock().unwrap().extend_from_slice(&acc);
            let _ = s.write_all(
                format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            );
            let _ = s.flush();
        }
    });
    (format!("http://{addr}/hook"), seen)
}

fn write_event(dir: &std::path::Path, text: &str) -> std::path::PathBuf {
    let p = dir.join("event.json");
    std::fs::write(&p, text).unwrap();
    p
}

fn bin() -> std::process::Command {
    std::process::Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// **The shipped process, on a real socket, on both arms — and the exit codes
/// W6 branches on.**
///
/// This is the only test here that opens a socket, and it opens it on
/// loopback. The `EventSink` seam pins the contract; a seam cannot pin that
/// `main.rs` dispatches the subcommand, that the contract lines really reach
/// stdout, that the diagnostics really reach stderr, or that the process
/// really exits 0 and 1.
///
/// The env is set PER CHILD (`Command::env`), never with `std::env::set_var`:
/// that is process-global and libtest is threaded.
///
/// KILLS: a `notify-result=` line written to stderr; the subcommand left
/// undispatched; exit 0 on a sink that answered 500.
#[test]
fn the_shipped_subcommand_delivers_on_loopback_and_reports_both_arms() {
    let dir = tempfile::tempdir().unwrap();
    let event = write_event(dir.path(), &decision_example_json());
    let deadline = std::time::Duration::from_secs(30);

    // Arm one: a listener that accepts.
    let (ok_url, seen) = listener("202 Accepted");
    let run = bounded_output(
        bin()
            .args(["notify", "deliver", "--event"])
            .arg(&event)
            .env_remove(ROUTING_KEY_ENV)
            .env_remove(SLACK_WEBHOOK_URL_ENV)
            .env(ALLOW_INSECURE_SINKS_ENV, "1")
            .env(WEBHOOK_URL_ENV, &ok_url),
        deadline,
    );
    assert_eq!(run.code, Some(0), "stderr:\n{}", run.stderr);
    assert_eq!(
        run.stdout, "notify-result=webhook:ok\n",
        "the contract line is on STDOUT and is the whole of it"
    );
    let wire = String::from_utf8_lossy(&seen.lock().unwrap().clone()).to_string();
    assert!(
        wire.contains("\"verification_scope\":\"sampled\""),
        "the document really went over the wire: {wire}"
    );
    assert!(
        !wire.to_lowercase().contains("exhaustive"),
        "and it claims no exhaustive check: {wire}"
    );

    // Arm two: a listener that refuses. Same process, same document.
    let (bad_url, _) = listener("500 Internal Server Error");
    let run = bounded_output(
        bin()
            .args(["notify", "deliver", "--event"])
            .arg(&event)
            .env_remove(ROUTING_KEY_ENV)
            .env_remove(SLACK_WEBHOOK_URL_ENV)
            .env(ALLOW_INSECURE_SINKS_ENV, "1")
            .env(WEBHOOK_URL_ENV, &bad_url),
        deadline,
    );
    assert_eq!(
        run.code,
        Some(1),
        "a sink that did not accept is a non-zero exit; stdout:\n{}\nstderr:\n{}",
        run.stdout,
        run.stderr
    );
    assert_eq!(run.stdout, "notify-result=webhook:failed\n");
    assert!(
        !run.stderr.contains(&bad_url) && !run.stdout.contains(&bad_url),
        "the sink URL must be redacted to scheme://host on every surface:\n{}\n{}",
        run.stdout,
        run.stderr
    );
}

/// **A refused event document exits 3 and prints NO `notify-result=` line.**
///
/// That is what makes exit 3 and exit 1 distinguishable without parsing prose:
/// a refusal posted nothing, so there is nothing to report per sink. D3 §3.4
/// point 4 has the controller read both the exit code and the lines, and this
/// is the row that makes the pair unambiguous.
///
/// KILLS: mapping a malformed document to exit 1; printing a synthetic
/// `failed` line for a document that was never delivered.
#[test]
fn a_refused_event_document_exits_three_with_no_contract_line() {
    let dir = tempfile::tempdir().unwrap();
    let deadline = std::time::Duration::from_secs(30);
    let (url, seen) = listener("202 Accepted");

    for (name, text) in [
        ("missing format_version", r#"{"event_id":"x"}"#.to_string()),
        (
            "future major",
            decision_example_json()
                .replace(r#""format_version":"1.0.0""#, r#""format_version":"2.0.0""#),
        ),
        (
            "verification_scope complete",
            decision_example_json().replace(
                r#""verification_scope":"sampled""#,
                r#""verification_scope":"complete""#,
            ),
        ),
        (
            "unknown field",
            decision_example_json().replace(r#""health":"Stale""#, r#""health":"Stale","extra":1"#),
        ),
        ("not json", "}{".to_string()),
    ] {
        let event = write_event(dir.path(), &text);
        let run = bounded_output(
            bin()
                .args(["notify", "deliver", "--event"])
                .arg(&event)
                .env_remove(ROUTING_KEY_ENV)
                .env_remove(SLACK_WEBHOOK_URL_ENV)
                .env(ALLOW_INSECURE_SINKS_ENV, "1")
                .env(WEBHOOK_URL_ENV, &url),
            deadline,
        );
        assert_eq!(
            run.code,
            Some(3),
            "{name}: a document refused before anything ran is exit 3; stderr:\n{}",
            run.stderr
        );
        assert!(
            !run.stdout.contains(NOTIFY_RESULT_LINE),
            "{name}: a refusal reports no sink, because it reached none:\n{}",
            run.stdout
        );
    }
    assert!(
        seen.lock().unwrap().is_empty(),
        "NOT ONE BYTE may reach a sink for a document that was refused"
    );

    // …and a missing file is the same refusal.
    let run = bounded_output(
        bin()
            .args(["notify", "deliver", "--event"])
            .arg(dir.path().join("does-not-exist.json")),
        deadline,
    );
    assert_eq!(run.code, Some(3));
    assert!(!run.stdout.contains(NOTIFY_RESULT_LINE));
}

/// **This subcommand never exits 2 and never exits 4.**
///
/// Global Constraint 11 reserves 2 for "a drill result that is not a pass — a
/// scorecard IS written and signed" and 4 for "signing or lock-proof failed".
/// `notify deliver` signs nothing and writes no artifact, so either code would
/// make a delivery failure indistinguishable from a drill result to every
/// reader of the exit contract — including the controller that maps an exit
/// code to a condition reason (`weirkeeper::conditions::reason_for_exit`).
///
/// KILLS: reusing `ExitCode::DrillNotPass` for "a sink refused" because the
/// name of the enum variant was not read.
#[test]
fn no_delivery_outcome_is_ever_reported_as_a_drill_result() {
    let dir = tempfile::tempdir().unwrap();
    let deadline = std::time::Duration::from_secs(30);
    let good = write_event(dir.path(), &decision_example_json());
    let (ok_url, _) = listener("202 Accepted");
    let (bad_url, _) = listener("503 Service Unavailable");
    let bad_doc = dir.path().join("bad.json");
    std::fs::write(&bad_doc, "not json").unwrap();

    for (name, event, url, want) in [
        ("accepted", good.clone(), ok_url, 0),
        ("refused by the sink", good, bad_url.clone(), 1),
        ("refused document", bad_doc, bad_url, 3),
    ] {
        let run = bounded_output(
            bin()
                .args(["notify", "deliver", "--event"])
                .arg(&event)
                .env_remove(ROUTING_KEY_ENV)
                .env_remove(SLACK_WEBHOOK_URL_ENV)
                .env(ALLOW_INSECURE_SINKS_ENV, "1")
                .env(WEBHOOK_URL_ENV, &url),
            deadline,
        );
        assert_eq!(run.code, Some(want), "{name}: stderr:\n{}", run.stderr);
        assert!(
            !matches!(run.code, Some(2) | Some(4)),
            "{name}: 2 and 4 belong to a signed scorecard and to signing"
        );
    }
}

/// **THE SHIPPED PROCESS, WITH A ROUTING KEY ACTUALLY SET, PRINTS IT NOWHERE.**
///
/// The review's F2, second half. Every other process row does
/// `env_remove(ROUTING_KEY_ENV)`, so until this one existed **no test ran the
/// shipped binary with a routing key present at all** — the mutated binary
/// printed `routing_key=…` on stderr in full and the suite stayed green.
///
/// The arms are chosen to be the ones that write the most: a refused PagerDuty
/// region (the `PAGERDUTY_SILENCED` branch, which is the branch that has the
/// key in scope), a sink that answers 500, and a delivery that succeeds. Both
/// streams are captured and folded, because a pod log has no stream selector.
///
/// `RUST_LOG=trace` is set so the child's own subscriber emits every event
/// this could leak through; its default is `warn`, which would have made the
/// INFO success line invisible to the assertion.
///
/// KILLS: a routing key in a `tracing` field; an unredacted endpoint on
/// stderr; a `ureq::Error` whose `Display` embeds the URL.
#[test]
fn the_shipped_process_never_prints_the_routing_key_on_either_stream() {
    let dir = tempfile::tempdir().unwrap();
    let event = write_event(dir.path(), &decision_example_json());
    let deadline = std::time::Duration::from_secs(30);
    let (ok_url, _) = listener("202 Accepted");
    let (bad_url, _) = listener("500 Internal Server Error");

    for (arm, endpoint, webhook) in [
        (
            "a refused PagerDuty region",
            "http://events.pagerduty.example/v2/enqueue",
            ok_url.clone(),
        ),
        (
            "a sink that answers 500",
            "https://events.pagerduty.example/v2/enqueue",
            bad_url.clone(),
        ),
        (
            "a sink that accepts",
            "https://events.pagerduty.example/v2/enqueue",
            ok_url.clone(),
        ),
    ] {
        let run = bounded_output(
            bin()
                .args(["notify", "deliver", "--event"])
                .arg(&event)
                .env("RUST_LOG", "trace")
                .env(ROUTING_KEY_ENV, TEST_ROUTING_KEY)
                .env(PAGERDUTY_ENDPOINT_ENV, endpoint)
                .env(ALLOW_INSECURE_SINKS_ENV, "1")
                .env(WEBHOOK_URL_ENV, &webhook)
                .env(SLACK_WEBHOOK_URL_ENV, TEST_SLACK_URL),
            deadline,
        );
        // BOTH STREAMS, folded — that is the point of the row.
        let surfaces = format!("{arm}\n{}\n{}", run.stdout, run.stderr);
        assert!(
            run.stderr.contains("logweir::notify"),
            "{arm}: the child emitted no tracing event, so this assertion is \
             vacuous:\n{surfaces}"
        );
        for secret in [TEST_ROUTING_KEY, "T00SECRET", "zzTOKENzz"] {
            assert!(
                !surfaces.contains(secret),
                "{arm}: `{secret}` reached a stream a pod log carries:\n{surfaces}"
            );
        }
        assert!(
            run.stdout.contains(NOTIFY_RESULT_LINE),
            "{arm}: the contract lines are still on stdout:\n{surfaces}"
        );
    }
}

/// **`--event` is required, and a missing one is a usage error (exit 1), not a
/// delivery result.**/// **`--event` is required, and a missing one is a usage error (exit 1), not a
/// delivery result.**
///
/// The same property `cli_exit_codes.rs` asserts for every other subcommand:
/// clap hardcodes 2 for a usage error and `main.rs` maps it to 1, because "the
/// drill ran and did not pass" must never be the report for a command line
/// that did not parse.
#[test]
fn a_usage_error_is_not_a_delivery_result() {
    let deadline = std::time::Duration::from_secs(30);
    for args in [
        vec!["notify", "deliver"],
        vec!["notify", "deliver", "--nope"],
        vec!["notify", "frobnicate"],
    ] {
        let run = bounded_output(bin().args(&args), deadline);
        assert_eq!(run.code, Some(1), "{args:?}: stderr:\n{}", run.stderr);
        assert!(!run.stdout.contains(NOTIFY_RESULT_LINE), "{args:?}");
    }
    // `--help` still exits 0, so a release smoke test can call it.
    let run = bounded_output(bin().args(["notify", "deliver", "--help"]), deadline);
    assert_eq!(run.code, Some(0));
    assert!(run.stdout.contains("notify-result="));
}
