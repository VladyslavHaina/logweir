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

// ------------------------------------------------------------- PLAT-14.2
//
// The protection event, and `logweir notify deliver`.
//
// Everything above this line is the DRILL's notification path and is
// unchanged. Everything below is D3 §3.3/§3.4's: a document the protection
// controller writes, a subcommand that posts it, and the two pure dedup-key
// builders the controller will key its alert ledger on (W6).
//
// The split is the whole design. The controller decides WHAT the alert says —
// it is the only thing that can, because it holds the Backup/catalog/schedule
// facts — and it may not deliver: `config/rbac/role.yaml` gives it no Secret
// verb, so it cannot read a routing key, and it is given no HTTP egress. This
// subcommand decides NOTHING about the alert. It reads the document, refuses
// it if it is not one, and posts it with the credentials projected into its
// own Job. Neither half can do the other's work, and that is what keeps a
// notification failure from ever being able to rewrite a backup result.

/// The event document's media type. D3 §3.4.
///
/// **Unsigned, and that is stated rather than implied**: a notification is not
/// evidence and is never rendered as one. The signed formats in this product
/// are the scorecard, the put-receipt and the teardown attestation; this is a
/// control message between two components of one release.
pub const PROTECTION_EVENT_MEDIA_TYPE: &str =
    "application/vnd.logweir.protection-event+json;version=1.0.0";

/// The `format_version` this build writes.
pub const PROTECTION_EVENT_FORMAT_VERSION: &str = "1.0.0";

/// The `format_version` MAJOR this build reads. A document whose major is
/// anything else is refused, named, and nothing is posted.
pub const PROTECTION_EVENT_FORMAT_MAJOR: u64 = 1;

/// The largest `--event` file that will be read.
///
/// The document arrives as a ConfigMap key, and a ConfigMap is capped at 1 MiB
/// by the API server, so a larger file is not a protection event by
/// construction. The bound exists so that a wrong `--event` path — a log, a
/// core file, a mounted archive — is a named refusal in milliseconds instead
/// of a runner that reads a gigabyte into memory inside a Job with
/// `activeDeadlineSeconds: 120`.
pub const MAX_EVENT_BYTES: u64 = 1024 * 1024;

/// The PagerDuty Events v2 routing key. Present ⇒ the `pagerduty` sink is
/// configured. **Projected from a Secret, never from an argv**, for the reason
/// `probe::SOURCE_PASSWORD_ENV` states: an argv is visible in every process
/// listing on the host and lands in the Job spec anyone with pod read can see.
pub const ROUTING_KEY_ENV: &str = "PAGERDUTY_ROUTING_KEY";

/// The generic JSON webhook. Present ⇒ the `webhook` sink is configured.
/// A bearer credential in its own right — a signed webhook puts the token in
/// the query — so it is projected from a Secret and [`redact_url`]'d on every
/// line that names it.
pub const WEBHOOK_URL_ENV: &str = "NOTIFY_WEBHOOK_URL";

/// The Slack incoming webhook. Present ⇒ the `slack` sink is configured.
/// `https://hooks.slack.com/services/T…/B…/…` **is** a bearer credential:
/// whoever holds it can post as the integration.
pub const SLACK_WEBHOOK_URL_ENV: &str = "NOTIFY_SLACK_WEBHOOK_URL";

/// The PagerDuty service region, when it is not the US default.
///
/// NOT a sink of its own and NOT a credential — it is the free-form adopter
/// input `Notifications::pagerduty_endpoint` already is, and it is routed
/// through the same [`pagerduty_endpoint`] so the https-only refusal is
/// decided in exactly one place. An EU-region adopter who cannot set this
/// enqueues into a region that does not hold their account and never sees a
/// page; that defect is already recorded on [`PAGERDUTY_US_ENDPOINT`], and a
/// subcommand with no way to say "EU" would reintroduce it.
pub const PAGERDUTY_ENDPOINT_ENV: &str = "PAGERDUTY_ENDPOINT";

/// The prefix of `logweir notify deliver`'s per-sink stdout contract line.
///
/// One line per CONFIGURED sink, `notify-result=<sink>:<ok|failed>`, and they
/// are the LAST thing the process writes. A pod log has no stream selector —
/// `GET …/pods/{pod}/log` interleaves stdout and stderr with no marker saying
/// which byte came from which (spec §7 amendment 4) — so a controller reads
/// these BY KEY NAME from a bounded tail, never by position, exactly as it
/// already reads `cluster-id=` and `refusal-reason=`.
pub const NOTIFY_RESULT_LINE: &str = "notify-result=";

/// The `pagerduty` sink's name in a [`NOTIFY_RESULT_LINE`].
pub const SINK_PAGERDUTY: &str = "pagerduty";

/// The `webhook` sink's name in a [`NOTIFY_RESULT_LINE`].
pub const SINK_WEBHOOK: &str = "webhook";

/// The `slack` sink's name in a [`NOTIFY_RESULT_LINE`].
pub const SINK_SLACK: &str = "slack";

/// The sink accepted: a 2xx.
pub const RESULT_OK: &str = "ok";

/// The sink did not accept, for any reason at all — refused endpoint, non-2xx,
/// transport error, timeout.
pub const RESULT_FAILED: &str = "failed";

/// D3 §3.3's alert vocabulary. Five kinds and no sixth.
///
/// The serde spelling is the CamelCase wire value the controller writes into
/// `alert.kind` and the tail of `alert.key`; a `#[serde(deny_unknown_fields)]`
/// document with a sixth kind does not parse, which is the point — an alert
/// nobody defined is not something to post and hope.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum AlertKind {
    /// `consecutiveFailedRuns >= maxConsecutiveFailedRuns`.
    BackupFailure,
    /// `health == Stale`.
    Staleness,
    /// The newest otherwise-available point is `Missing`/`Unreadable`/
    /// `Untrusted` in the catalog.
    ArchiveUnavailable,
    /// The last rehearsal failed, or the last success is older than
    /// `maxRehearsalAgeSeconds`.
    RehearsalFailure,
    /// A Restore matching this policy's points reached a terminal state.
    /// Informational, auto-resolved immediately, and **never paged** — see
    /// [`AlertKind::pages`].
    RecoveryCompleted,
}

impl AlertKind {
    /// The wire spelling, which is also the tail of the dedup key.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AlertKind::BackupFailure => "BackupFailure",
            AlertKind::Staleness => "Staleness",
            AlertKind::ArchiveUnavailable => "ArchiveUnavailable",
            AlertKind::RehearsalFailure => "RehearsalFailure",
            AlertKind::RecoveryCompleted => "RecoveryCompleted",
        }
    }

    /// Whether this kind may reach PagerDuty at all.
    ///
    /// `RecoveryCompleted` is **webhook/Slack only** (D3 §3.3): it is
    /// informational, it auto-resolves immediately, and it keys on a Restore
    /// UID rather than on `(policy, kind)`, so a PagerDuty incident opened
    /// under it would be opened and closed in the same breath — a page for
    /// something that went RIGHT, at 03:00, with nothing for the responder to
    /// do. A configured routing key is therefore NOT a configured sink for
    /// this kind, and no `notify-result=pagerduty:` line is printed for it.
    #[must_use]
    pub fn pages(self) -> bool {
        !matches!(self, AlertKind::RecoveryCompleted)
    }
}

/// What PagerDuty is being asked to do, from D3 §3.3: `trigger` on Open,
/// `resolve` on Resolved, under one stable `dedup_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertAction {
    /// The condition opened, or re-notified.
    Trigger,
    /// The condition cleared.
    Resolve,
}

impl AlertAction {
    /// PagerDuty Events v2's own spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AlertAction::Trigger => "trigger",
            AlertAction::Resolve => "resolve",
        }
    }
}

/// D3 §3.2's five protection-health states. `Protected` mirrors `Healthy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Health {
    /// An available point inside the objective, failures below threshold.
    Healthy,
    /// Inside the objective, but failures at threshold, a suspended or
    /// NotReady schedule, or a missed slot.
    AtRisk,
    /// The newest available point is older than the objective.
    Stale,
    /// There is no available point at all.
    Unprotected,
    /// Evaluation was impossible — a stale or unreadable catalog, a missing
    /// source or destination, a stale own status. **Never `Healthy`.**
    Unknown,
}

impl Health {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Health::Healthy => "Healthy",
            Health::AtRisk => "AtRisk",
            Health::Stale => "Stale",
            Health::Unprotected => "Unprotected",
            Health::Unknown => "Unknown",
        }
    }
}

/// How much of the archive behind this alert was actually checked.
///
/// **THERE IS NO `Complete` VARIANT, AND THERE MUST NEVER BE ONE** (D3 §3.4,
/// and the tracker's "do not label a signature as exhaustive data
/// verification"). This is the single most load-bearing enum in the module:
/// the value travels into a PagerDuty incident and a Slack channel, where it
/// is read by someone deciding whether an archive can be trusted in an
/// incident. Logweir verifies a SAMPLE. A document claiming otherwise would be
/// the product's one unrecoverable lie, and `deny_unknown_fields` plus three
/// variants is what makes `"verification_scope": "complete"` a PARSE FAILURE
/// rather than a value someone has to notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerificationScope {
    /// A sampled per-record comparison ran.
    Sampled,
    /// `integrityLevel: consume-only` — records were read back but not
    /// compared byte-for-byte.
    Degraded,
    /// No record check ran at all.
    None,
}

impl VerificationScope {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            VerificationScope::Sampled => "sampled",
            VerificationScope::Degraded => "degraded",
            VerificationScope::None => "none",
        }
    }
}

/// Which `ProtectionPolicy` this event is about.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRef {
    /// The policy's namespace.
    pub namespace: String,
    /// The policy's name.
    pub name: String,
    /// The policy's UID — the half of the dedup key that survives a rename
    /// and does not survive a delete-and-recreate, which is exactly the
    /// identity an incident should have.
    pub uid: String,
}

/// The alert itself: its stable key, its kind, what PagerDuty is asked to do,
/// and how many times this `(policy, kind)` has changed state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alert {
    /// `logweir-protection-<policyUID>-<kind>`, or the Restore-UID form for
    /// `RecoveryCompleted`. **The controller is the authority on this value**
    /// — it is the thing that tracks transitions — so this subcommand posts
    /// what it is given and never recomputes it. [`protection_dedup_key`] is
    /// the builder the controller uses, exposed here so there is one
    /// implementation rather than two; [`dedup_key_advice`] reports a
    /// mismatch without refusing, because a mis-keyed page still reaches a
    /// human and a refused one does not.
    pub key: String,
    /// Which of D3 §3.3's five conditions this is.
    pub kind: AlertKind,
    /// `trigger` or `resolve`.
    pub action: AlertAction,
    /// How many times this key has changed state. Part of the ConfigMap and
    /// Job names the controller derives, so a duplicate reconcile is a 409
    /// rather than a second page.
    pub transition: u64,
}

/// The newest available recovery point at the moment the event was generated.
///
/// **Optional, and its absence is the fact**: a `health: Unprotected` policy
/// has no available point, and a `last_available_point` invented for it would
/// be a recovery point that does not exist, printed into an incident. An
/// absent field parses to `None` and renders as "no available recovery point".
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LastAvailablePoint {
    /// The catalog's stable point identity (`lwp1-…`).
    pub point_id: String,
    /// D3 §3.2's freshness instant: the CAPTURE START of the point, not its
    /// `finishedAt` and not the newest record instant.
    pub recovery_point_at: chrono::DateTime<chrono::Utc>,
    /// `generated_at - recovery_point_at`, in seconds, as the controller
    /// measured it. Carried rather than recomputed here: a delivery Job that
    /// recomputed the age would report the age at DELIVERY time, which drifts
    /// from the age the health decision was made on by however long the Job
    /// waited to be scheduled.
    pub age_seconds: i64,
    /// The evidence verdict for that point — `Valid`, `ValidHistorical`,
    /// `Unverified`, … as the catalog recorded it.
    pub evidence: String,
}

/// D3 §3.4's protection event, exactly.
///
/// # Reading rules
///
/// 1. **`format_version` is semver and its major must be
///    [`PROTECTION_EVENT_FORMAT_MAJOR`].** Anything else is refused by name.
/// 2. **Unknown fields are REFUSED, not ignored** — and this is a deliberate
///    departure from `docs/stability.md`'s general format policy, which has a
///    reader ignore unknown fields on a matching major. That policy is about
///    SIGNED, ARCHIVAL documents read years later by something that was not
///    there when they were written. This is neither: it is a control message
///    passed between a controller and a Job whose image the same chart pins,
///    and the thing a lenient reader would silently drop is an alert detail an
///    on-call responder then never learns. The cost is stated on the same page:
///    a minor bump that adds a field needs the runner image upgraded with the
///    controller, which the chart already does.
/// 3. **`verification_scope` has three values and `complete` is not one.**
///    See [`VerificationScope`].
/// 4. **`last_available_point` is optional**; absent means there is none.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionEvent {
    /// Semver; major [`PROTECTION_EVENT_FORMAT_MAJOR`].
    pub format_version: String,
    /// `sha256` of `policyUID|alertKey|transition`, as the controller computed
    /// it. Opaque here: this subcommand identifies nothing by it, it is
    /// carried so a sink can correlate a POST with the ConfigMap the
    /// controller wrote.
    pub event_id: String,
    /// The policy this is about.
    pub policy: PolicyRef,
    /// The alert.
    pub alert: Alert,
    /// D3 §3.2's health at the moment of the transition.
    pub health: Health,
    /// One sentence, controller-authored, for a human. It reaches a PagerDuty
    /// incident title and a Slack message verbatim.
    pub summary: String,
    /// The newest available point, or `None`.
    #[serde(default)]
    pub last_available_point: Option<LastAvailablePoint>,
    /// Consecutive failed SLOTS — a retry chain counts once (D3 §3.2).
    pub consecutive_failed_runs: u64,
    /// Missed schedule slots.
    pub missed_slots: u64,
    /// How much was actually checked. Never `complete`.
    pub verification_scope: VerificationScope,
    /// The UI route an incident responder should open. A fragment route
    /// (`#/protection?ns=…&name=…`), not an absolute URL: the controller does
    /// not know the installation's external hostname, and inventing one would
    /// put a link in an incident that goes nowhere.
    pub details_route: String,
    /// When the controller generated the event.
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

impl ProtectionEvent {
    /// The PagerDuty severity this event carries.
    ///
    /// Two values, decided by HEALTH and not by kind: `Unprotected` (there is
    /// nothing to recover from) and `Stale` (the newest point is outside the
    /// objective) are `critical`; everything else is `warning`. The mapping is
    /// deliberately coarse — a severity scale nobody can predict is a severity
    /// scale nobody routes on — and it is a table test rather than a comment.
    #[must_use]
    pub fn severity(&self) -> &'static str {
        match self.health {
            Health::Unprotected | Health::Stale => "critical",
            Health::Healthy | Health::AtRisk | Health::Unknown => "warning",
        }
    }

    /// `<namespace>/<name>` — what PagerDuty calls the `source`. The UID is
    /// deliberately not here: `source` is read by a human in an incident list,
    /// and it names an object they can `kubectl get`.
    #[must_use]
    pub fn source(&self) -> String {
        format!("{}/{}", self.policy.namespace, self.policy.name)
    }
}

/// Parse a protection event from bytes, strictly.
///
/// `Err` is an already-safe, already-redacted sentence naming what was wrong:
/// it reaches stderr and a `tracing` line, and the document it is about is
/// adopter-shaped input. The two refusals it can report are the two reading
/// rules on [`ProtectionEvent`] — a major this build does not read, and a
/// shape it does not recognise.
///
/// # Errors
///
/// A document that is not JSON, whose `format_version` is missing, unparseable
/// or of another major, or whose shape carries an unknown field, a bad enum
/// value or a missing required one.
pub fn parse_event(bytes: &[u8]) -> Result<ProtectionEvent, String> {
    // The major is read FIRST, off a loose parse, so that a document from a
    // future major is refused for BEING a future major rather than for the
    // unknown fields that major added. "unknown field `foo`" would send a
    // reader looking for a typo; "format_version 2.0.0, this build reads
    // major 1" sends them to the upgrade they actually need.
    let loose: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| format!("the event document is not JSON: {e}"))?;
    let raw = loose
        .get("format_version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            format!(
                "the event document has no string `format_version`; \
                 this build reads `{PROTECTION_EVENT_MEDIA_TYPE}`"
            )
        })?;
    let major: u64 = raw
        .split('.')
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|_| format!("`format_version` `{raw}` is not semver"))?;
    if major != PROTECTION_EVENT_FORMAT_MAJOR {
        return Err(format!(
            "`format_version` `{raw}` is major {major}; this build reads major \
             {PROTECTION_EVENT_FORMAT_MAJOR} only, and refuses a shape it has never \
             seen rather than guessing at it"
        ));
    }
    serde_json::from_slice(bytes).map_err(|e| format!("the event document is malformed: {e}"))
}

/// D3 §3.3's dedup key for the four policy-keyed kinds:
/// `logweir-protection-<policyUID>-<kind>`.
///
/// **Pure, and exposed for W6**: the protection controller keys its alert
/// ledger on this and the runner reports the same string back, so there is one
/// implementation and not two that drift. One open alert per `(policy, kind)`
/// is the whole dedup contract — PagerDuty gets `trigger` on Open and
/// `resolve` on Resolved under this exact string, and a key that changed
/// between the two would leave an incident open forever.
///
/// # The empty UID falls back, and that is a guard
///
/// A blank `policy_uid` would collapse this to `logweir-protection--Staleness`
/// — one incident per KIND across every policy in the cluster, so one team's
/// resolve closes another team's open page. That is precisely the defect
/// [`dedup_key`]'s `PLAN_HASH_IDENT_LEN` guard exists for, arriving by a
/// different route, and it is SILENT: the page simply stops being about what
/// you think it is about. A UID that identifies nothing therefore becomes
/// `unknown`, a key that is honest about having no identity and structurally
/// incapable of being the degenerate one.
#[must_use]
pub fn protection_dedup_key(policy_uid: &str, kind: AlertKind) -> String {
    format!(
        "{PROTECTION_DEDUP_PREFIX}{}-{}",
        ident_or_unknown(policy_uid),
        kind.as_str()
    )
}

/// D3 §3.3's `RecoveryCompleted` key, which is keyed on the **Restore UID**
/// and not on `(policy, kind)`.
///
/// It has to be: a policy's points can be restored many times, and every one
/// of those is its own completed recovery with its own topic names and its own
/// counts. Keying on `(policy, RecoveryCompleted)` would make the second
/// restore of the day overwrite the first one's message in the sink that
/// dedups, and a responder reading it would be reading about the wrong
/// restore. The empty-UID fallback is [`protection_dedup_key`]'s, for the same
/// reason.
#[must_use]
pub fn recovery_completed_dedup_key(restore_uid: &str) -> String {
    format!(
        "{PROTECTION_DEDUP_PREFIX}{}-{}",
        ident_or_unknown(restore_uid),
        AlertKind::RecoveryCompleted.as_str()
    )
}

/// The prefix every protection dedup key carries, and the thing that keeps a
/// protection incident from ever resolving a DRILL incident: `dedup_key` and
/// `failure_dedup_key` above both spell `logweir-drill-`.
pub const PROTECTION_DEDUP_PREFIX: &str = "logweir-protection-";

/// The sentinel a UID that identifies nothing collapses to. Shared by both key
/// builders so there is one spelling of "no identity".
pub const UNKNOWN_IDENT: &str = "unknown";

/// A UID, or [`UNKNOWN_IDENT`] when it is blank. Whitespace-only counts as
/// blank: a projected field that exists and holds a space is not an identity.
fn ident_or_unknown(uid: &str) -> &str {
    let t = uid.trim();
    if t.is_empty() {
        UNKNOWN_IDENT
    } else {
        t
    }
}

/// Whether `alert.key` is the key the `(policy, kind)` rule would have built,
/// as ADVICE rather than as a refusal.
///
/// `None` means "consistent, or not checkable". `Some(sentence)` is a WARN
/// line, and delivery continues.
///
/// # Why this does not refuse
///
/// It was written as a refusal first. A wrong dedup key is a real defect — it
/// collapses incidents, or leaves one open forever — and this repository
/// prefers loud. But the two failure modes are not symmetric here: a
/// mis-keyed page still reaches a human who can act on the incident, and a
/// refused one reaches nobody at all. Refusing would convert a dedup bug into
/// a total loss of alerting, which is strictly worse than the thing it
/// catches. So it warns, by name, on a line an operator can grep, and the
/// controller stays the authority on its own ledger's key.
///
/// `RecoveryCompleted` keys on a Restore UID the event does not carry, so only
/// its PREFIX is checkable; that is stated rather than silently skipped.
#[must_use]
pub fn dedup_key_advice(ev: &ProtectionEvent) -> Option<String> {
    if !ev.alert.key.starts_with(PROTECTION_DEDUP_PREFIX) {
        return Some(format!(
            "alert.key `{}` does not start with `{PROTECTION_DEDUP_PREFIX}`; a protection \
             alert that shares a prefix with another family can resolve that family's \
             incident",
            ev.alert.key
        ));
    }
    if ev.alert.kind == AlertKind::RecoveryCompleted {
        // Keyed on the Restore UID (D3 §3.3), which is not a field of this
        // document. The prefix above is all there is to check, and saying so
        // is better than a check that silently covers four kinds of five.
        return None;
    }
    let expected = protection_dedup_key(&ev.policy.uid, ev.alert.kind);
    (ev.alert.key != expected).then(|| {
        format!(
            "alert.key `{}` is not the `(policy, kind)` key for this event (`{expected}`); \
             delivering anyway — the controller owns its ledger's key, and a mis-keyed page \
             still reaches a human where a refused one does not",
            ev.alert.key
        )
    })
}

/// Phrases that would make a notification body claim exhaustive verification,
/// matched case-insensitively over the SERIALIZED body of every POST.
///
/// # Why a fixed list and not a bare word
///
/// `RecoveryCompleted` is one of D3 §3.3's five alert kinds, and its name is
/// in `alert.kind` and in the tail of `alert.key` on every recovery event. A
/// gate on the bare word `complete` would fire on all of them — a permanent
/// false red, which is how a gate stops meaning anything (the same narrowing
/// `scripts/check-unverified-labels.sh` had to make, and for the same reason).
/// What is forbidden is the CLAIM: `exhaustive` in any of its forms, which has
/// no honest use in a Logweir notification at all, and the fixed phrases that
/// spell "we checked everything". A restore that COMPLETED is a fact about a
/// restore; verification that is complete is a claim about an archive, and
/// Logweir checks a sample.
///
/// # `exhaustive` is forbidden EVEN IN A DENIAL, and that is the subtle half
///
/// The first draft of [`slack_text`] wrote D3 §3.5's own wording — "a sampled
/// check, not an exhaustive comparison" — and this list caught it, which is
/// how the rule below was arrived at rather than assumed. A denial and a claim
/// differ by one word, and these channels TRUNCATE: PagerDuty clips an
/// incident summary, Slack collapses a long message behind "show more", and a
/// notification body is pasted into a ticket by hand. A sentence that survives
/// clipping as "…an exhaustive comparison" is the product's one unrecoverable
/// lie, delivered by its own disclaimer. So the disclaimers this module writes
/// say what Logweir DID — a sample was checked, never the whole archive — and
/// never reach for the word they are denying. `ui/render.js` owns the
/// completion-panel prose D3 §3.5 specifies and is a different surface with a
/// different truncation story; this rule binds the notification bodies.
///
/// A new paraphrase found in review is added here, in the same commit as the
/// surface it was found on.
pub const EXHAUSTIVE_CLAIMS: [&str; 10] = [
    "exhaustive",
    "complete verification",
    "verification is complete",
    "verification complete",
    "completely verified",
    "fully verified",
    "verified in full",
    "verified completely",
    "every record was verified",
    "all records were verified",
];

/// Every forbidden verification claim found in a body that is about to be
/// POSTed, plus a `verification_scope` that is not one of the three.
///
/// PURE, and it takes the SERIALIZED body rather than the typed event, because
/// what reaches a Slack channel is the bytes: a claim that arrives through
/// `summary`, through `details_route`, through a PagerDuty `payload.summary`
/// this module composes, or through a field a later version adds, is caught by
/// the same pass. `crates/logweir/tests/notify_deliver.rs` drives it over the
/// full cross-product of kind × health × scope, and the `RecoveryCompleted`
/// row is the one that proves the list is narrow enough to be usable.
#[must_use]
pub fn exhaustive_claim_offences(body: &serde_json::Value) -> Vec<String> {
    let text = body.to_string().to_lowercase();
    let mut found: Vec<String> = EXHAUSTIVE_CLAIMS
        .iter()
        .filter(|c| text.contains(**c))
        .map(|c| (*c).to_string())
        .collect();
    // The `verification_scope` value itself, wherever it sits in the body —
    // top level for the webhook, inside `payload.custom_details` for
    // PagerDuty. A three-variant enum makes `complete` unparseable, so this
    // can only fire on a body this module composed wrongly; it is the second
    // lock on the one claim the product must never make.
    for scope in find_all(body, "verification_scope") {
        if let Some(s) = scope.as_str() {
            if !matches!(s, "sampled" | "degraded" | "none") {
                found.push(format!("verification_scope={s}"));
            }
        } else {
            found.push("verification_scope is not a string".to_string());
        }
    }
    found
}

/// Every value stored under `key`, at any depth. Used only by
/// [`exhaustive_claim_offences`]; a body is a handful of fields deep.
fn find_all<'a>(v: &'a serde_json::Value, key: &str) -> Vec<&'a serde_json::Value> {
    let mut out = Vec::new();
    match v {
        serde_json::Value::Object(m) => {
            for (k, child) in m {
                if k == key {
                    out.push(child);
                }
                out.extend(find_all(child, key));
            }
        }
        serde_json::Value::Array(a) => {
            for child in a {
                out.extend(find_all(child, key));
            }
        }
        _ => {}
    }
    out
}

/// The generic webhook's body: the event document, re-serialized.
///
/// RE-SERIALIZED from the parsed type, never passed through as the bytes that
/// arrived. `deny_unknown_fields` means nothing unrecognised can survive the
/// parse, so what goes on the wire is exactly the shape this build understands
/// — a document with an extra field is refused rather than forwarded to a
/// sink that might render it.
#[must_use]
pub fn webhook_body(ev: &ProtectionEvent) -> serde_json::Value {
    serde_json::json!({
        "media_type": PROTECTION_EVENT_MEDIA_TYPE,
        "event": ev,
    })
}

/// The Slack incoming webhook's body: `{"text": …}` and nothing else.
///
/// **Not the raw event document.** A Slack incoming webhook rejects a JSON
/// payload with no `text`, `blocks` or `attachments` — it answers
/// `invalid_payload` — so posting the document verbatim would make every
/// Slack delivery a `notify-result=slack:failed`, three controller retries and
/// a `NotificationsDelivered=False` for a channel that is working perfectly.
/// (The drill path above posts `notify_body(sc)` raw to `slack_webhook` and
/// has this defect today; fixing it there changes shipped behaviour and belongs
/// to its own task, so it is recorded rather than smuggled into this one.)
#[must_use]
pub fn slack_body(ev: &ProtectionEvent) -> serde_json::Value {
    serde_json::json!({ "text": slack_text(ev) })
}

/// The two lines Slack renders. Pure, so the wording is a test and not a hope.
///
/// Line 1 is the controller's own `summary`. Line 2 is the machine facts a
/// responder needs before they open anything: what kind of alert, whether it
/// opened or cleared, the health, **the verification scope spelled out in
/// words that cannot be mistaken for a whole-archive check**, and the route.
///
/// The disclaimer deliberately avoids the word [`EXHAUSTIVE_CLAIMS`] forbids,
/// rather than writing "not an exhaustive comparison" — see that constant for
/// why a negated claim is not safe in a channel that truncates.
#[must_use]
pub fn slack_text(ev: &ProtectionEvent) -> String {
    let point = match &ev.last_available_point {
        Some(p) => format!(
            "{} ({}s old, evidence {})",
            p.point_id, p.age_seconds, p.evidence
        ),
        None => "no available recovery point".to_string(),
    };
    format!(
        "{}\n{} {} · health {} · {} · failed slots {} · missed slots {} · \
         verification scope: {} (a SAMPLE was checked, never the whole archive) · {}",
        ev.summary,
        ev.alert.kind.as_str(),
        ev.alert.action.as_str(),
        ev.health.as_str(),
        point,
        ev.consecutive_failed_runs,
        ev.missed_slots,
        ev.verification_scope.as_str(),
        ev.details_route,
    )
}

/// The PagerDuty Events v2 envelope for one protection event.
///
/// `event_action` is `alert.action` and `dedup_key` is `alert.key` — both
/// taken from the document, neither invented here, which is what makes the
/// controller's ledger and PagerDuty's incident the same thing. The routing
/// key travels in the BODY, as PagerDuty's API requires, and is never logged.
#[must_use]
pub fn pagerduty_event(ev: &ProtectionEvent, routing_key: &str) -> serde_json::Value {
    serde_json::json!({
        "routing_key": routing_key,
        "event_action": ev.alert.action.as_str(),
        "dedup_key": ev.alert.key,
        "payload": {
            "summary": ev.summary,
            "source": ev.source(),
            "severity": ev.severity(),
            "custom_details": ev,
        },
    })
}

/// The sinks this delivery is configured for, read from the environment.
///
/// **A sink is configured exactly when its variable holds a non-blank value.**
/// `std::env::var` returns `Ok("")` — not `Err(NotPresent)` — for a Kubernetes
/// `env:` entry with an empty `value:`, and a `secretKeyRef` to a key that is
/// present and blank projects the same thing. A routing key of `""` is not a
/// routing key: treating it as configured would post an event PagerDuty
/// refuses, report `pagerduty:failed`, and burn the controller's three
/// attempts on a route nobody configured. The same trap is recorded at
/// `weirkeeper/src/retention.rs:99` and `weirkeeper/src/job.rs:98`.
#[derive(Default, Clone)]
pub struct SinkRoutes {
    /// [`ROUTING_KEY_ENV`], when non-blank. **Never logged, never
    /// `Debug`-printed** — see this struct's hand-written `Debug`.
    pub pagerduty_routing_key: Option<String>,
    /// [`PAGERDUTY_ENDPOINT_ENV`], when non-blank. Not a sink; the region.
    pub pagerduty_endpoint: Option<String>,
    /// [`WEBHOOK_URL_ENV`], when non-blank.
    pub webhook_url: Option<String>,
    /// [`SLACK_WEBHOOK_URL_ENV`], when non-blank.
    pub slack_webhook_url: Option<String>,
}

/// HAND-WRITTEN for the reason `logweir_core::spec::Notifications`' is: three
/// of these four fields are bearer credentials, and this struct sits one
/// `{:?}` away from an error message at all times. Presence is still reported,
/// because "no sink configured" and "a sink configured and failed" are
/// different findings an operator must be able to tell apart from a log.
impl std::fmt::Debug for SinkRoutes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRoutes")
            .field(
                "pagerduty_routing_key",
                &self.pagerduty_routing_key.as_ref().map(|_| "***"),
            )
            .field(
                "pagerduty_endpoint",
                &self.pagerduty_endpoint.as_deref().map(redact_url),
            )
            .field("webhook_url", &self.webhook_url.as_deref().map(redact_url))
            .field(
                "slack_webhook_url",
                &self.slack_webhook_url.as_deref().map(redact_url),
            )
            .finish()
    }
}

impl SinkRoutes {
    /// Read the four variables from the process environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Read them through a caller-supplied lookup, so the whole contract is
    /// testable without mutating process-global state.
    ///
    /// `std::env::set_var` is process-global and the test harness is threaded,
    /// so a test that set a routing key would be setting it for every other
    /// test running at that instant. This seam is why
    /// `crates/logweir/tests/notify_deliver.rs` has no `set_var` in it.
    #[must_use]
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let read = |k: &str| {
            get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Self {
            pagerduty_routing_key: read(ROUTING_KEY_ENV),
            pagerduty_endpoint: read(PAGERDUTY_ENDPOINT_ENV),
            webhook_url: read(WEBHOOK_URL_ENV),
            slack_webhook_url: read(SLACK_WEBHOOK_URL_ENV),
        }
    }

    /// Which sinks are configured FOR THIS EVENT, in the fixed contract order.
    ///
    /// The order is `pagerduty`, `webhook`, `slack` and it does not vary. A
    /// controller reads the lines by key name (spec §7 amendment 4), so the
    /// order is not what makes the parse work — it is what makes the contract
    /// one thing rather than six spellings of it, and what lets a test compare
    /// the whole stdout string instead of three `contains` calls.
    ///
    /// `RecoveryCompleted` drops `pagerduty` even when the routing key is set:
    /// D3 §3.3 makes that kind webhook/Slack only, so the route is not a
    /// configured sink for it and prints no line. See [`AlertKind::pages`].
    #[must_use]
    pub fn configured_for(&self, kind: AlertKind) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.pagerduty_routing_key.is_some() && kind.pages() {
            out.push(SINK_PAGERDUTY);
        }
        if self.webhook_url.is_some() {
            out.push(SINK_WEBHOOK);
        }
        if self.slack_webhook_url.is_some() {
            out.push(SINK_SLACK);
        }
        out
    }
}

/// What one `logweir notify deliver` produced: the exact stdout bytes, the
/// stderr diagnostics, and the exit code.
///
/// Same arrangement as [`crate::probe::ProbeOutcome`] and for the same reason:
/// the whole contract is a value a test can compare, rather than a `println!`
/// nobody can observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryOutcome {
    /// The `notify-result=` lines, in contract order, each newline-terminated.
    /// EMPTY when no sink was configured for this event.
    pub stdout: String,
    /// Lines for stderr, in the order they were produced. Already redacted.
    pub diagnostics: Vec<String>,
    /// [`ExitCode::Ok`] when every configured sink accepted (including when
    /// none was configured), [`ExitCode::Operational`] when at least one did
    /// not.
    pub code: ExitCode,
}

/// Post one protection event to every configured sink, through the
/// [`EventSink`] seam.
///
/// **THE WHOLE SUBCOMMAND EXCEPT THE FILE READ AND THE SOCKET.** It takes the
/// parsed event, the routes and a sink, and returns the bytes and the code —
/// so `crates/logweir/tests/notify_deliver.rs` drives every branch of the
/// contract with a recording sink and opens nothing (Global Constraint 17).
///
/// Every sink is attempted. A PagerDuty route that is refused before a request
/// does not stop the webhook; a webhook that times out does not stop Slack.
/// A responder who can be reached by ONE of three channels must be, and an
/// early return is how the one channel that was working gets skipped.
#[must_use]
pub fn deliver_with(
    ev: &ProtectionEvent,
    routes: &SinkRoutes,
    sink: &dyn EventSink,
) -> DeliveryOutcome {
    let mut stdout = String::new();
    let mut diagnostics: Vec<String> = Vec::new();
    let mut all_ok = true;

    if let Some(advice) = dedup_key_advice(ev) {
        diagnostics.push(advice);
    }

    let mut record = |name: &str, ok: bool| {
        stdout.push_str(NOTIFY_RESULT_LINE);
        stdout.push_str(name);
        stdout.push(':');
        stdout.push_str(if ok { RESULT_OK } else { RESULT_FAILED });
        stdout.push('\n');
    };

    for name in routes.configured_for(ev.alert.kind) {
        let (url, body) = match name {
            SINK_PAGERDUTY => {
                // `pagerduty_endpoint` is reused rather than re-implemented,
                // so the https-only refusal is decided in exactly one place
                // for the drill path and this one alike.
                let n = Notifications {
                    pagerduty_endpoint: routes.pagerduty_endpoint.clone(),
                    ..Notifications::default()
                };
                match pagerduty_endpoint(&n) {
                    Ok(u) => {
                        let key = routes.pagerduty_routing_key.as_deref().unwrap_or_default();
                        (u, pagerduty_event(ev, key))
                    }
                    Err(reason) => {
                        // A refused endpoint is a SILENCED alert and says so
                        // by the same name the drill path uses, so one grep
                        // finds both.
                        diagnostics.push(format!(
                            "{PAGERDUTY_SILENCED}: dedup_key={} reason={reason}",
                            ev.alert.key
                        ));
                        tracing::warn!(target: "logweir::notify",
                                       event_id = %ev.event_id,
                                       dedup_key = %ev.alert.key, reason = %reason,
                                       silenced = true, "{PAGERDUTY_SILENCED}");
                        all_ok = false;
                        record(name, false);
                        continue;
                    }
                }
            }
            SINK_WEBHOOK => (
                routes.webhook_url.clone().unwrap_or_default(),
                webhook_body(ev),
            ),
            SINK_SLACK => (
                routes.slack_webhook_url.clone().unwrap_or_default(),
                slack_body(ev),
            ),
            // `configured_for` returns the three constants and nothing else;
            // the arm exists so adding a fourth sink is a visible gap rather
            // than a silent skip.
            other => {
                diagnostics.push(format!("no body is defined for sink `{other}`"));
                all_ok = false;
                record(other, false);
                continue;
            }
        };

        // THE LAST GATE BEFORE THE WIRE. `verification_scope` cannot be
        // `complete` — the enum has three variants — but the bytes are checked
        // anyway, because this is the one claim the product must never make
        // and the composition above is code that can be edited. An offending
        // body is NOT posted.
        let offences = exhaustive_claim_offences(&body);
        if !offences.is_empty() {
            let shown = redact_url(&url);
            diagnostics.push(format!(
                "refusing to post to {shown}: the body would claim exhaustive \
                 verification ({offences:?}); Logweir verifies a SAMPLE"
            ));
            tracing::error!(target: "logweir::notify", sink = %shown,
                            offences = ?offences,
                            "refusing to post a body that claims exhaustive verification");
            all_ok = false;
            record(name, false);
            continue;
        }

        let shown = redact_url(&url);
        match sink.post(&url, &body) {
            Ok(()) => {
                tracing::info!(target: "logweir::notify", sink = %name,
                               endpoint = %shown, dedup_key = %ev.alert.key,
                               "protection event delivered");
                record(name, true);
            }
            Err(e) => {
                // `e` is ALREADY redacted: `EventSink::post`'s contract is that
                // it never hands back a `ureq::Error` whose `Display` embeds
                // the request URL, credential and all.
                diagnostics.push(format!("{name}: post to {shown} failed: {e}"));
                if name == SINK_PAGERDUTY {
                    tracing::warn!(target: "logweir::notify", sink = %name,
                                   endpoint = %shown, dedup_key = %ev.alert.key,
                                   error = %e, silenced = true, "{PAGERDUTY_SILENCED}");
                } else {
                    tracing::warn!(target: "logweir::notify", sink = %name,
                                   endpoint = %shown, dedup_key = %ev.alert.key,
                                   error = %e, "protection event NOT delivered");
                }
                all_ok = false;
                record(name, false);
            }
        }
    }

    if stdout.is_empty() {
        let why = if ev.alert.kind.pages() {
            format!(
                "no sink is configured: set {ROUTING_KEY_ENV}, {WEBHOOK_URL_ENV} or \
                 {SLACK_WEBHOOK_URL_ENV}"
            )
        } else {
            format!(
                "no sink is configured for a {} alert: it is webhook/Slack only \
                 (D3 §3.3), so {ROUTING_KEY_ENV} alone routes nothing — set \
                 {WEBHOOK_URL_ENV} or {SLACK_WEBHOOK_URL_ENV}",
                ev.alert.kind.as_str()
            )
        };
        diagnostics.push(why.clone());
        tracing::warn!(target: "logweir::notify", event_id = %ev.event_id,
                       dedup_key = %ev.alert.key, "{why}");
    }

    DeliveryOutcome {
        stdout,
        diagnostics,
        code: if all_ok {
            ExitCode::Ok
        } else {
            ExitCode::Operational
        },
    }
}

/// `logweir notify deliver --event <path>`.
pub struct DeliverArgs {
    /// The protection event document. In the shipped Job this is the
    /// controller's immutable ConfigMap projected at `/event/event.json`.
    pub event: std::path::PathBuf,
}

/// Read the event document, bounded.
///
/// # Errors
///
/// A path that is not a readable regular file, a file larger than
/// [`MAX_EVENT_BYTES`], or bytes that are not a protection event. The message
/// names the path — a path is not a secret, and "which file" is the whole
/// diagnostic when a projection did not land.
pub fn read_event(path: &std::path::Path) -> Result<ProtectionEvent, String> {
    let meta = std::fs::metadata(path).map_err(|e| format!("--event {}: {e}", path.display()))?;
    if !meta.is_file() {
        return Err(format!("--event {} is not a regular file", path.display()));
    }
    if meta.len() > MAX_EVENT_BYTES {
        return Err(format!(
            "--event {} is {} bytes; a protection event arrives as a ConfigMap key and \
             cannot exceed {MAX_EVENT_BYTES}",
            path.display(),
            meta.len()
        ));
    }
    let bytes = std::fs::read(path).map_err(|e| format!("--event {}: {e}", path.display()))?;
    parse_event(&bytes).map_err(|e| format!("--event {}: {e}", path.display()))
}

/// Write a [`DeliveryOutcome`] to its two streams.
///
/// **The diagnostics go FIRST and the contract lines LAST.** A pod log merges
/// stdout and stderr (erratum E4) and a controller scans a bounded tail;
/// putting the diagnostics ahead of the contract keeps every
/// `notify-result=` line inside that tail however long the diagnostics are.
/// This is `probe::write_outcome`'s arrangement, deliberately identical.
///
/// # Errors
///
/// Whatever the writers return. [`run`] discards it: a closed stdout is not a
/// reason to change an exit code that has already been decided.
pub fn write_outcome<O: std::io::Write, E: std::io::Write>(
    out: &mut O,
    err: &mut E,
    o: &DeliveryOutcome,
) -> std::io::Result<()> {
    for d in &o.diagnostics {
        writeln!(err, "notify deliver: {d}")?;
    }
    err.flush()?;
    out.write_all(o.stdout.as_bytes())?;
    out.flush()
}

/// The shipped subcommand.
///
/// # The contract, in full — this is what W6 depends on
///
/// **Environment.** A sink is configured exactly when its variable holds a
/// non-blank value; an empty `value:` or a blank `secretKeyRef` is NOT
/// configured (see [`SinkRoutes`]).
///
/// | variable | sink |
/// |---|---|
/// | [`ROUTING_KEY_ENV`] | `pagerduty` — `trigger`/`resolve` by `alert.action`, `dedup_key` = `alert.key` |
/// | [`WEBHOOK_URL_ENV`] | `webhook` — one POST of the event document |
/// | [`SLACK_WEBHOOK_URL_ENV`] | `slack` — one POST of `{"text": …}` |
/// | [`PAGERDUTY_ENDPOINT_ENV`] | not a sink; the PagerDuty service region, `https://` only |
///
/// **Stdout.** One `notify-result=<sink>:<ok|failed>` line per configured
/// sink, in the order `pagerduty`, `webhook`, `slack`, as the FINAL lines the
/// process writes. No configured sink ⇒ no lines.
///
/// **Exit codes.**
///
/// * **0** — every configured sink accepted, or none was configured.
/// * **1** — at least one configured sink did not accept. The
///   `notify-result=…:failed` line says which. (`ExitCode::Operational`:
///   nothing about an archive is being reported, and no artifact is written.)
/// * **3** — the event document is missing, unreadable, too large, of another
///   `format_version` major, or malformed. **Nothing was posted**, which is
///   exactly `ExitCode::GuardRefused`'s meaning: refused before anything ran.
///   Distinguishable from 1 without parsing prose, because a refusal prints no
///   `notify-result=` line at all.
///
/// **2 and 4 are never returned.** 2 means a signed scorecard exists that is
/// not a pass and 4 means signing or lock-proof failed; this subcommand signs
/// nothing and writes no artifact, and returning either would make a delivery
/// failure indistinguishable from a drill result to every reader of Global
/// Constraint 11. A test asserts it over the real process.
///
/// **Timeouts.** [`NOTIFY_CONNECT_TIMEOUT`] and [`NOTIFY_TIMEOUT`], through
/// [`notify_agent`] — the same bound the drill path carries, so the worst case
/// for three sinks is `3 * NOTIFY_TIMEOUT`, comfortably inside the Job's
/// `activeDeadlineSeconds: 120`.
#[must_use]
pub fn run(args: &DeliverArgs) -> ExitCode {
    // Reused rather than copied: the whole point of the two-stream split is
    // that the tracing subscriber writes to STDERR while stdout carries the
    // contract, and a second subscriber installer is a second place for that
    // to be got wrong.
    crate::probe::install_diagnostics();

    let ev = match read_event(&args.event) {
        Ok(e) => e,
        Err(reason) => {
            // Refused before anything was posted, so no `notify-result=` line
            // is printed and the exit code is the refusal one.
            let _ = writeln!(
                &mut std::io::stderr().lock() as &mut dyn std::io::Write,
                "notify deliver: {reason}"
            );
            tracing::error!(target: "logweir::notify", reason = %reason,
                            "refusing the event document; nothing was posted");
            return ExitCode::GuardRefused;
        }
    };

    let routes = SinkRoutes::from_env();
    let outcome = deliver_with(&ev, &routes, &UreqSink::new());
    let _ = write_outcome(
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
        &outcome,
    );
    outcome.code
}
