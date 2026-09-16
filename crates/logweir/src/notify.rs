//! Outbound notification: the sinks, their bounds, their redaction, their
//! dedup keys — and `logweir notify deliver`, the subcommand that posts one
//! protection event.
//!
//! # Why this module exists at this path
//!
//! Everything from [`redact_url`] down to [`notify_with_sink`] was
//! `crate::drill::phase7_verify`'s second half and **moved here verbatim**
//! (D3 §3.4). It moved for one reason: PLAT-14.2's protection controller may
//! not deliver its own alerts. It holds no Secret verb (`config/rbac/role.yaml`)
//! and is given no HTTP egress, so the alert has to be posted by something that
//! already has a reviewed timeout policy, a reviewed redactor and a reviewed
//! PagerDuty route — which is this code, in the runner image. A subcommand that
//! reached into `drill::phase7_verify` for a sink trait would be saying the
//! drill owns notification; it does not, and never did.
//!
//! The old path still works. `drill::phase7_verify` re-exports every public
//! item below, so every call site and every structural guard in
//! `crates/logweir/tests/notify.rs` keeps the path it already uses. The one
//! guard that had to change is `every_notification_post_goes_through_the_bounded_agent`,
//! whose sanctioned-file table names the file the one bounded-agent builder
//! lives in: it now names `notify.rs`, because that is where `notify_agent_with`
//! is. (The builder's own spelling is deliberately not repeated in this header —
//! that guard counts literal occurrences under `crates/logweir/src/`, and a
//! module comment naming it would read as a second timeout policy.)
//!
//! # The two halves
//!
//! * **The drill half** — [`notify`], [`notify_failure`], [`dedup_key`],
//!   [`failure_dedup_key`]. Unchanged, and still driven from
//!   `drill::mod.rs`'s two call sites.
//! * **The protection half** — [`ProtectionEvent`] and [`deliver`], new in
//!   PLAT-14.2. A protection event is READ from a file, not computed here: the
//!   controller decides what the alert says, the ConfigMap carries the bytes,
//!   and this subcommand is the thing with the credentials. That split is the
//!   whole point — see [`deliver`] for the env/exit/stdout contract a
//!   controller depends on.
//!
//! # What never reaches a log, on either half
//!
//! The PagerDuty routing key travels in a request BODY and is never logged,
//! `Debug`-printed or echoed. Every sink URL that reaches a display surface
//! goes through [`redact_url`] first, on the success arm and the failure arm
//! alike — a credential that is safe on one and logged on the other is still
//! logged. `crates/logweir/tests/notify_deliver.rs` asserts both over the real
//! process's stdout, stderr and JSON log lines.

use std::time::Duration;

use crate::exit::ExitCode;
use logweir_core::scorecard::Scorecard;
use logweir_core::spec::Notifications;

/// A webhook URL reduced to the part that identifies the SINK, never the part
/// that authorises posting to it.
///
/// The implementation moved to `logweir_core::spec::redact_url` in fix round 1
/// (finding F1) and is re-exported here so that every existing caller and
/// every test keeps the path it already uses. It had to move because the
/// second site that has to redact — `Notifications`' hand-written `Debug` —
/// lives in `logweir-core`, which cannot depend on this crate; a second copy
/// of a redactor is how the two copies come to disagree. The function is pure
/// string arithmetic and reads no clock, no file and no socket, so it costs
/// the pure layer nothing (GC1; `scripts/check-pure-core.sh` passes).
pub use logweir_core::spec::redact_url;

/// What a failed notification may say about itself, with the URL taken out.
///
/// `ureq::Error::Status(code, response)` displays as `<url>: status code <n>`
/// and `Transport` embeds the URL too, so neither may be printed. The status
/// code is the whole diagnostic payload and carries no secret; a transport
/// failure is reported by kind only.
fn redact_ureq_error(e: &ureq::Error) -> String {
    match e {
        ureq::Error::Status(code, _) => format!("status code {code}"),
        ureq::Error::Transport(_) => "transport error".into(),
    }
}

/// The JSON summary posted to every sink and embedded in PagerDuty's
/// `custom_details`.
///
/// Public, and separate from `notify`, so its SHAPE can be asserted without a
/// network — `redact_url` next door is public for exactly the same reason, and
/// `crates/logweir/tests/notify.rs` drives both directly.
///
/// This is **the only surface that reaches a human away from a terminal**, so
/// what it omits is what an on-call reader never learns. T0-3: it carried no
/// `redactions`, so a redacted scorecard notified as if whole.
///
/// `redactions` is always present — an empty array for the whole document
/// every v0.1 run writes — so a sink can branch on it without having to tell
/// "absent" from "none". The PATHS travel and the `reason` strings do NOT: a
/// `reason` is free text arriving with a document that, by construction, no
/// Logweir writer produced, and this body is pasted verbatim into Slack and
/// PagerDuty. The path is the whole actionable signal.
pub fn notify_body(sc: &Scorecard) -> serde_json::Value {
    serde_json::json!({
        "run_id": sc.run_id,
        "outcome": sc.outcome,
        "rto_excluding_preflight_seconds": sc.measured.rto_excluding_preflight_seconds,
        "rpo_seconds": sc.measured.rpo_seconds,
        "integrity": { "level": sc.integrity.level, "result": sc.integrity.result },
        "self_attested": sc.approval.self_attested,
        "redactions": sc.redactions.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
    })
}

/// How long one notification POST may take before it is written off.
///
/// Task 5b. There was NO timeout here at all: every sink was posted with
/// ureq 2.12.1's free-function request builder, which constructs a throwaway
/// agent carrying that crate's defaults — no connect, read or overall timeout,
/// in its own words *"requests may block forever on reads by default"*. (The
/// spelling is not repeated here: `crates/logweir/tests/notify.rs` fails if
/// those builders appear anywhere under `crates/logweir/src/`.) A webhook
/// endpoint that ACCEPTS the
/// connection and then never replies hung this function, and therefore the
/// drill, forever — after the scorecard was signed and uploaded. The drill had
/// already succeeded and produced its evidence; the process just never
/// returned to say so, and the operator watching it had no way to tell a
/// hung notification from a hung restore.
///
/// A refused connection was never the risk: the kernel answers a closed port
/// with an RST in microseconds, which is why the existing tests were fast and
/// why this went unnoticed. A firewall that DROPs instead of REJECTing, or a
/// sink that is merely wedged, is the case with no bound.
///
/// The numbers. `timeout_connect` bounds the TCP handshake; `timeout` bounds
/// the whole call, handshake included, and takes precedence over the
/// per-socket read and write timeouts (ureq's own contract), so the two
/// together are the complete bound: **no sink can cost more than
/// `NOTIFY_TIMEOUT`, and the worst case for a spec with `w` webhooks, a Slack
/// sink and a PagerDuty key is `(w + 2) * NOTIFY_TIMEOUT`.** Ten seconds is
/// long enough that a merely slow sink is still notified — losing a page
/// because PagerDuty took four seconds would be a worse failure than the one
/// being fixed — and short enough that a wedged one is written off promptly.
pub const NOTIFY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The overall bound on one notification POST. See `NOTIFY_CONNECT_TIMEOUT`.
pub const NOTIFY_TIMEOUT: Duration = Duration::from_secs(10);

/// The agent every notification POST goes through. Public so a test can build
/// one with a short bound and drive `notify_with` against a listener that
/// accepts and never replies — the failure mode with no bound — without
/// spending the production bound to do it.
pub fn notify_agent_with(connect: Duration, overall: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(connect)
        .timeout(overall)
        .build()
}

/// The production agent: `NOTIFY_CONNECT_TIMEOUT` and `NOTIFY_TIMEOUT`.
pub fn notify_agent() -> ureq::Agent {
    notify_agent_with(NOTIFY_CONNECT_TIMEOUT, NOTIFY_TIMEOUT)
}

/// PagerDuty's Events v2 enqueue endpoint for the **US** service region — the
/// default when `notifications.pagerduty_endpoint` is absent.
///
/// This was a bare string literal inside the POST. An adopter whose PagerDuty
/// account lives on the EU service region therefore enqueued into a region
/// that does not hold their account and never saw a page; the non-2xx was
/// swallowed into a warning. The literal now exists exactly once, here, and
/// `pagerduty_endpoint` below is the only thing that decides what is posted to.
pub const PAGERDUTY_US_ENDPOINT: &str = "https://events.pagerduty.com/v2/enqueue";

/// The one line an operator greps for when a page did not arrive.
///
/// Emitted whenever a configured PagerDuty route produced NO incident — the
/// endpoint was refused before a request, or the request itself failed. A
/// silenced alert that logs nothing is indistinguishable from a drill nobody
/// configured an alert for, which is the whole defect this task closes: the
/// route must either deliver an event that describes *this* drill or say, by
/// name, that it did not.
pub const PAGERDUTY_SILENCED: &str = "pagerduty alert NOT delivered";

/// The seam every outbound notification POST goes through.
///
/// It exists so a test can observe the exact payload — `event_action`,
/// `dedup_key`, the endpoint URL — without opening a socket, which GC17
/// requires: no test in this repository may reach `events.pagerduty.com` or
/// any other public host. The production implementation is `UreqSink`, which
/// posts through `notify_agent()` and therefore keeps Task 5b's bound; the
/// structural test `every_notification_post_goes_through_the_bounded_agent`
/// still forbids a bare `ureq::post` anywhere under `crates/logweir/src/`.
pub trait EventSink {
    /// `Err` carries an **already-redacted** description: no URL, no
    /// credential, no `ureq::Error` `Display` (which embeds the request URL).
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String>;
}

/// The production `EventSink`: one bounded `ureq::Agent`, reused for every
/// sink in a run so the timeout policy is decided in exactly one place.
pub struct UreqSink {
    agent: ureq::Agent,
}

impl UreqSink {
    /// The production sink, carrying `notify_agent()`'s bounds.
    pub fn new() -> Self {
        Self {
            agent: notify_agent(),
        }
    }

    /// A sink over a caller-supplied agent, so a test can inject a short bound
    /// instead of waiting out the production one.
    pub fn with_agent(agent: ureq::Agent) -> Self {
        Self { agent }
    }
}

impl Default for UreqSink {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSink for UreqSink {
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        self.agent
            .post(url)
            .send_json(body)
            .map(|_| ())
            // Redacted HERE, at the boundary, so no caller can reach a
            // `ureq::Error` whose `Display` embeds the request URL.
            .map_err(|e| redact_ureq_error(&e))
    }
}

/// Which endpoint this spec's PagerDuty events go to, or why none will be sent.
///
/// `Ok(url)` is the endpoint to POST to; `Err(reason)` means the route is
/// REFUSED before any request is made and the caller must log
/// `PAGERDUTY_SILENCED`.
///
/// Only `https://` is accepted. The routing key travels in the request body,
/// so a plaintext endpoint would put a bearer credential on the wire, and a
/// misconfigured scheme is a configuration error a human must fix rather than
/// something to attempt and let fail. The refusal names the SCHEME and nothing
/// else: the rest of the URL may carry whatever the adopter typed, and this
/// string reaches a log.
pub fn pagerduty_endpoint(n: &Notifications) -> Result<String, String> {
    match &n.pagerduty_endpoint {
        None => Ok(PAGERDUTY_US_ENDPOINT.to_string()),
        Some(u) if u.starts_with("https://") => Ok(u.clone()),
        Some(u) => {
            let scheme = match u.split_once("://") {
                Some((s, _)) if !s.is_empty() => s,
                _ => "<no scheme>",
            };
            Err(format!(
                "notifications.pagerduty_endpoint must be an https:// URL; \
                 refusing scheme `{scheme}`"
            ))
        }
    }
}

/// R-D's interim dedup key for a drill that produced a scorecard.
///
/// PagerDuty's Events v2 API keys an incident by `dedup_key`. This was
/// `format!("logweir-drill-{}", sc.target.cluster_id)`, so every drill spec
/// pointed at one cluster shared ONE incident — and because a passing drill
/// sends `event_action: resolve`, a nightly smoke drill passing at 03:00
/// silently closed the weekly full drill's open page. Two different outcomes
/// must produce two different incidents.
///
/// The identity is the spec's own `name` when it has one. When it does not,
/// the fallback is the first 12 hex characters of the approval's `plan_hash`
/// — a sha256 over the approved plan bytes, so it is distinct per spec and
/// stable across re-runs of the same spec, which is exactly what a dedup key
/// has to be. The run id would be distinct but NOT stable, and a key
/// containing it never dedups at all.
///
/// **`PLAN_HASH_IDENT_LEN` is a guard, not a length calculation** (fix round 1,
/// finding F3). A `plan_hash` too short to supply 12 characters — an empty one
/// above all — collapsed this to `logweir-drill--{cluster_id}`, which is the
/// PRE-FIX SHAPE with a stray hyphen: one incident per cluster, every drill
/// resolving every other drill's page, the exact defect R-D closes. It is not
/// reachable today, because `phase1_approval::verify` recomputes
/// `sha256(spec_text)` and refuses before any scorecard exists, so the field is
/// always a real digest by the time this runs. But it was SILENT, and a
/// degenerate dedup key does not announce itself: the page simply stops
/// arriving. So a hash that cannot identify anything falls back to `"unnamed"`,
/// the same sentinel `failure_dedup_key` uses for a spec with no name — a key
/// that is honest about having no identity, and structurally incapable of being
/// the old one.
///
/// Ruling **R-D**. The durable fix is an artifact-side `drill_id` (backlog
/// T1-8), which §12 assigns to decision **O16** with default *not funded*; the
/// residual is recorded in `docs/stability.md`.
pub fn dedup_key(spec_name: Option<&str>, sc: &Scorecard) -> String {
    /// How much of the `plan_hash` identifies the spec. 12 hex characters is
    /// 48 bits — collision-free across any plausible number of drill specs.
    const PLAN_HASH_IDENT_LEN: usize = 12;

    let ident = match spec_name {
        Some(n) => n.to_string(),
        None => {
            let h: String = sc
                .approval
                .plan_hash
                .trim_start_matches("sha256:")
                .chars()
                .take(PLAN_HASH_IDENT_LEN)
                .collect();
            if h.len() < PLAN_HASH_IDENT_LEN {
                "unnamed".to_string()
            } else {
                h
            }
        }
    };
    format!("logweir-drill-{ident}-{}", sc.target.cluster_id)
}

/// The dedup key for a drill that produced NO scorecard — exits 1, 3 and 4.
///
/// There is no cluster id to key on: `TargetSpec` does not carry one, and
/// `Scorecard::target.cluster_id` is discovered at runtime in phase 0, which
/// on these paths may never have run. So the key is the spec name alone.
///
/// It is DELIBERATELY distinct from `dedup_key`'s, and that is a property, not
/// an accident: "logweir could not run this drill" and "this drill ran and did
/// not pass" are different facts about different things, and a later passing
/// run's `resolve` must not automatically close an operational-failure
/// incident that nobody has looked at. An unnamed spec collapses to one
/// incident per Logweir install — the second half of the residual recorded in
/// `docs/stability.md`, and the reason to give a spec a `name`.
pub fn failure_dedup_key(spec_name: Option<&str>) -> String {
    format!("logweir-drill-{}-preflight", spec_name.unwrap_or("unnamed"))
}

/// Resolve the endpoint, POST, and make a silenced alert visible either way.
///
/// GC11: every outcome here is logged and swallowed. A refused endpoint, a
/// down PagerDuty and a timeout all leave the exit code exactly as the drill
/// decided it.
fn enqueue_pagerduty(
    n: &Notifications,
    ev: &serde_json::Value,
    dedup_key: &str,
    run_id: &str,
    sink: &dyn EventSink,
) {
    let url = match pagerduty_endpoint(n) {
        Ok(u) => u,
        Err(reason) => {
            tracing::warn!(target: "logweir::notify", run_id = %run_id,
                           dedup_key = %dedup_key, reason = %reason,
                           silenced = true, "{PAGERDUTY_SILENCED}");
            return;
        }
    };
    // THE POST GOES TO `url`; ONLY THE LOG SEES `shown`. Fix round 1, F1: this
    // logged the endpoint verbatim, on the argument that it is not a secret.
    // It is not — but it is free-form adopter input, and `redact_url` keeps
    // `scheme://host/…`, which is 100 % of the stated diagnostic (WHICH region
    // the events went to) while dropping the userinfo, path and query, which
    // is where a token in a pasted URL lives. The failure arm and the success
    // arm are redacted identically: a credential that is safe on one and
    // logged on the other is still logged. The routing key travels in the BODY
    // and is never logged at all.
    let shown = redact_url(&url);
    if let Err(e) = sink.post(&url, ev) {
        tracing::warn!(target: "logweir::notify", run_id = %run_id,
                       dedup_key = %dedup_key, endpoint = %shown, error = %e,
                       silenced = true, "{PAGERDUTY_SILENCED}");
    } else {
        tracing::info!(target: "logweir::notify", run_id = %run_id,
                       dedup_key = %dedup_key, endpoint = %shown,
                       "pagerduty event enqueued");
    }
}

/// The exit-1 / exit-3 / exit-4 page: a drill that produced no scorecard.
///
/// The PagerDuty route used to fire from exactly one place — phase 8, AFTER
/// the scorecard was signed and uploaded — so it was loud when the drill had
/// already succeeded at producing evidence and SILENT in the three situations
/// that need a human: an operational failure with no artifact (1), a plan a
/// guard refused (3), and a result nobody could attest (4).
///
/// It sends nothing to `webhooks` / `slack_webhook` on purpose. Those are the
/// scorecard-summary route: their body is `notify_body(sc)` and there is no
/// scorecard here. Inventing a scorecard-shaped body without a scorecard is
/// how a dashboard starts reporting drills that never ran.
pub fn notify_failure(
    n: &Notifications,
    spec_name: Option<&str>,
    run_id: &str,
    code: ExitCode,
    message: &str,
) {
    notify_failure_with(n, spec_name, run_id, code, message, &UreqSink::new())
}

/// `notify_failure` with the sink injected — see `EventSink`.
pub fn notify_failure_with(
    n: &Notifications,
    spec_name: Option<&str>,
    run_id: &str,
    code: ExitCode,
    message: &str,
    sink: &dyn EventSink,
) {
    // Opt-in, exactly as `notify_with_sink` is: no routing key, no route.
    let Some(key) = &n.pagerduty_routing_key else {
        return;
    };
    let dedup = failure_dedup_key(spec_name);
    let severity = match code {
        // A guard refusing a plan BEFORE anything ran is a configuration
        // finding, not an outage: nothing was touched and nothing is at risk.
        ExitCode::GuardRefused => "warning",
        // 1 and 4 both mean the drill's evidence does not exist. Anything else
        // reaching here would be a bug, and "critical" is the safe default for
        // a case nobody anticipated.
        _ => "critical",
    };
    let ev = serde_json::json!({
        "routing_key": key,
        // NEVER `resolve`. This path has no result to clear.
        "event_action": "trigger",
        "dedup_key": dedup,
        "payload": {
            "summary": format!(
                "logweir drill {run_id}: no scorecard (exit {}) — {message}",
                code as u8
            ),
            "source": spec_name.unwrap_or("unnamed"),
            "severity": severity,
            "custom_details": {
                "run_id": run_id,
                "exit_code": code as u8 as i64,
                "error": message,
            },
        },
    });
    enqueue_pagerduty(n, &ev, &dedup, run_id, sink);
}

/// POSTs one JSON summary per configured sink. EVERY transport failure is
/// logged and swallowed: the drill result is a measurement, and a webhook being
/// down must never change it. A TIMEOUT is one of those transport failures and
/// is swallowed identically — it arrives as `ureq::Error::Transport`, is
/// logged as `transport error`, and changes neither the scorecard nor the exit
/// code. The exit contract (GC11) is untouched: by the time this runs the
/// scorecard is signed and uploaded, and a sink that would not answer is not
/// a drill result. `ureq` is blocking on purpose — it adds no async runtime to
/// `crates/logweir`.
///
/// NO SINK URL REACHES A LOG LINE INTACT — see `redact_url`. That includes the
/// error arm: `ureq::Error`'s own `Display` embeds the request URL, so
/// `error = %e` leaked the same credential a second time, by a route a reader
/// of the `url = %url` field alone would not have noticed.
///
/// The body it posts is `notify_body` next door, which is where the shape —
/// and what it deliberately omits — is documented and tested.
pub fn notify(n: &Notifications, spec_name: Option<&str>, sc: &Scorecard) {
    notify_with(&notify_agent(), n, spec_name, sc)
}

/// `notify` with the agent injected, so the timeout bound is a parameter of
/// the test rather than a property the test has to wait out. Every POST in
/// this module goes through the passed agent — `crates/logweir/tests/notify.rs`
/// asserts structurally that no bare `ureq::post`/`ureq::get` survives
/// anywhere in `crates/logweir/src/`, because a revert to one would restore
/// the unbounded wait and pass every behavioural test that supplies its own
/// agent.
pub fn notify_with(
    agent: &ureq::Agent,
    n: &Notifications,
    spec_name: Option<&str>,
    sc: &Scorecard,
) {
    notify_with_sink(n, spec_name, sc, &UreqSink::with_agent(agent.clone()))
}

/// `notify` with the SINK injected rather than the agent, so a test can read
/// the exact `event_action`, `dedup_key` and endpoint URL off the recorded
/// payload without opening a socket (GC17).
///
/// This is the brief's `notify_with(n, spec_name, sc, sink)` under a different
/// name: `notify_with` was already taken by Task 5b's agent-injected variant
/// above, whose behavioural tests must keep working unchanged.
pub fn notify_with_sink(
    n: &Notifications,
    spec_name: Option<&str>,
    sc: &Scorecard,
    sink: &dyn EventSink,
) {
    let body = notify_body(sc);
    let mut sinks: Vec<String> = n.webhooks.clone();
    if let Some(u) = &n.slack_webhook {
        sinks.push(u.clone());
    }
    for url in sinks {
        let shown = redact_url(&url);
        match sink.post(&url, &body) {
            Ok(()) => tracing::info!(target: "logweir::notify", sink = %shown, "notified"),
            // `error = %e` is NOT logged: `ureq::Error`'s `Display` embeds the
            // request URL, credential and all. What an operator needs from a
            // failed notification is which sink and what kind of failure, and
            // both survive `redact_url` + the variant name — `EventSink::post`
            // hands back an already-redacted description for that reason.
            Err(e) => tracing::warn!(target: "logweir::notify", sink = %shown,
                                     error = %e,
                                     "notification failed; continuing"),
        }
    }
    if let Some(key) = &n.pagerduty_routing_key {
        let dedup = dedup_key(spec_name, sc);
        let ev = serde_json::json!({
            "routing_key": key,
            "event_action": if sc.outcome == logweir_core::outcome::Outcome::Pass
                            { "resolve" } else { "trigger" },
            "dedup_key": dedup,
            "payload": { "summary": format!("logweir drill {}: {:?}", sc.run_id, sc.outcome),
                         "source": sc.target.cluster_id, "severity": "warning",
                         "custom_details": body },
        });
        enqueue_pagerduty(n, &ev, &dedup, &sc.run_id, sink);
    }
}
