use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrillSpec {
    /// This drill's own stable identity, per ruling **R-D**.
    ///
    /// It exists so two drill specs pointed at ONE cluster do not share a
    /// PagerDuty incident: the dedup key is built from this plus the target
    /// cluster id (`crate::drill::phase7_verify::dedup_key` in the `logweir`
    /// crate). Before it existed the key was `logweir-drill-{cluster_id}`, so
    /// a passing nightly drill RESOLVED a failing weekly drill's open page.
    ///
    /// Optional, so every spec written before this field existed still parses;
    /// when it is absent the key falls back to a prefix of the approval's
    /// `plan_hash`, which is distinct per spec and stable across re-runs.
    ///
    /// It is NOT `drill_id`. An artifact-side identity that travels ON the
    /// scorecard is backlog T1-8, assigned to decision O16 with default *not
    /// funded*; this field is the interim, spec-side half and is deliberately
    /// never written to a scorecard (that would be a GC12 format change).
    #[serde(default)]
    pub name: Option<String>,
    pub source: SourceSpec,
    pub target: TargetSpec,
    pub sample: SampleSpec,
    pub objectives: ObjectivesSpec,
    /// The evidence sink. A DIFFERENT bucket and principal from `source` by
    /// default (spec §7.1); the guard warns loudly when they are equal.
    pub evidence: crate::engine::StorageUrl,
    /// Deliberately a free-form map so an adopter CAN try to pass an engine
    /// key — and be refused by name. Silently ignoring it would hide the guard.
    #[serde(default)]
    pub engine_overrides: BTreeMap<String, serde_yaml::Value>,
    /// Spec §13. Absent means "notify nobody"; it is never an error.
    #[serde(default)]
    pub notifications: Notifications,
}

/// Spec §13's notification shape. v0.1 POSTs one JSON summary per sink and
/// treats every transport failure as a logged warning — a drill result that is
/// already signed and uploaded must not be downgraded because a webhook was
/// down.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Notifications {
    #[serde(default)]
    pub webhooks: Vec<String>,
    #[serde(default)]
    pub slack_webhook: Option<String>,
    #[serde(default)]
    pub pagerduty_routing_key: Option<String>,
    /// PagerDuty's Events v2 enqueue endpoint. `None` means the US default,
    /// `crate::…::PAGERDUTY_US_ENDPOINT` in the `logweir` crate.
    ///
    /// This existed as a string LITERAL in the POST, so an adopter whose
    /// PagerDuty account is on the EU service region posted every event into a
    /// region that does not hold their account, got a non-2xx, and the failure
    /// was swallowed into a warning. The EU value is
    /// `https://events.eu.pagerduty.com/v2/enqueue`.
    ///
    /// NOT A SECRET — but FREE-FORM ADOPTER INPUT, which is not the same
    /// thing, and fix round 1 (finding F1) corrects the conclusion that was
    /// drawn from it here. This field was logged and `Debug`-printed in full
    /// on the argument that "redacting it would reproduce the defect the field
    /// exists to fix". `redact_url` refutes that: it keeps `scheme://host/…`,
    /// which IS the whole diagnostic — the region — and drops the userinfo,
    /// path and query, which are exactly where an adopter who pastes a signed
    /// or tokenised URL puts a credential. A `?token=…` or a `user:pass@` in
    /// this field reached a WARN line and any `{:?}` of the spec. That was a
    /// per-sink exemption from the rule the three fields above obey, and a
    /// per-sink exemption is how the next credential reaches a log.
    ///
    /// Only `https://` is accepted; anything else is refused before a request
    /// is made, because the routing key travels in the request BODY and
    /// plaintext HTTP would put a bearer credential on the wire.
    #[serde(default)]
    pub pagerduty_endpoint: Option<String>,
}

/// HAND-WRITTEN, not derived, and for the same reason
/// `logweir_kafka::reader::AuthConfig` writes its own: the three SINK fields
/// are secrets (`pagerduty_endpoint`, added later, is not — see its doc
/// comment). A Slack incoming-webhook URL is a bearer credential — whoever
/// holds it can post as the integration — and so is a PagerDuty routing key.
/// `DrillSpec` derives `Debug`, so a derived impl here would put all three
/// into any `{:?}` of the spec, and this struct sits one field away from an
/// error message or a log line at all times. There is no `{:?}` site today;
/// this exists so that adding one is not a disclosure.
///
/// Presence is still reported, because "no notification was configured" and
/// "a notification was configured and failed" are different findings an
/// operator has to be able to tell apart from a log.
impl std::fmt::Debug for Notifications {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifications")
            .field(
                "webhooks",
                &format_args!("{} configured", self.webhooks.len()),
            )
            .field("slack_webhook", &self.slack_webhook.as_ref().map(|_| "***"))
            .field(
                "pagerduty_routing_key",
                &self.pagerduty_routing_key.as_ref().map(|_| "***"),
            )
            // REDACTED TO `scheme://host/…`, like every sink URL that reaches
            // a display surface (fix round 1, F1). The host is the whole
            // diagnostic — it is what says US region or EU region — and the
            // tail that is dropped is the only part an adopter could have put
            // a credential in. `None` still renders as `None`: "no endpoint
            // configured" and "an endpoint configured and misdirected" are
            // different findings.
            .field(
                "pagerduty_endpoint",
                &self.pagerduty_endpoint.as_deref().map(redact_url),
            )
            .finish()
    }
}

/// A webhook URL reduced to the part that identifies the SINK, never the part
/// that authorises posting to it.
///
/// `notifications.slack_webhook` is a `https://hooks.slack.com/services/T…/B…/…`
/// URL, and that URL **is** a bearer credential: whoever holds it can post as
/// the integration, with no other secret. It was logged verbatim at INFO on
/// the success path and again on the error path, to the JSON subscriber
/// intended for a log aggregator — while `pagerduty_routing_key`, four lines
/// below, was deliberately never logged. The asymmetry was the tell.
///
/// `notifications.pagerduty_endpoint` is not a credential but IS free-form
/// adopter input, and the same reasoning reaches the same place: the region is
/// the diagnostic, the tail is where a token would be. Fix round 1, F1.
///
/// Only scheme and host survive. The path, the query, the fragment and any
/// `user:password@` userinfo are all dropped, because the secret can live in
/// any of them (Slack puts it in the path; a signed webhook puts it in the
/// query). An operator can still tell WHICH sink a line is about — that is the
/// whole diagnostic value of the field — without the line being enough to use
/// it. A URL that will not parse is reported as `<unparseable url>` rather
/// than echoed, because "it did not look like a URL to me" is not a reason to
/// print a secret.
///
/// It lives HERE, in the pure core, rather than beside its first caller in
/// `crates/logweir`, because `Notifications`' hand-written `Debug` above is a
/// second site that has to redact and this crate cannot depend on that one.
/// Fix round 1 (F1) moved it; `logweir::drill::phase7_verify::redact_url`
/// re-exports it, so every caller keeps its path. Two copies of a redactor is
/// how the two copies come to disagree about what a credential looks like.
///
/// It reads no clock, no file and no socket — GC1 is untouched.
///
/// Pinned by `crates/logweir/tests/notify.rs`.
pub fn redact_url(url: &str) -> String {
    // Hand-parsed rather than pulled in as a dependency: the pure core
    // carries no URL crate, and the rule is "keep the prefix up to the first
    // '/' after the scheme, minus any userinfo", which is four lines.
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<unparseable url>".into();
    };
    if scheme.is_empty() || rest.is_empty() {
        return "<unparseable url>".into();
    }
    // Everything before the first `/`, `?` or `#` is the authority.
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .expect("split always yields at least one element");
    // `user:password@host` — the credential half is dropped, the host kept.
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    if host.is_empty() {
        return "<unparseable url>".into();
    }
    format!("{scheme}://{host}/…")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpec {
    pub storage: crate::engine::StorageUrl,
    /// "latestCompleted" or a pinned backup id.
    #[serde(default = "latest")]
    pub backup: String,
    pub topics: Vec<String>,
}
fn latest() -> String {
    "latestCompleted".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetSpec {
    pub bootstrap_servers: Vec<String>,
    /// The v0.1 segregation proof. Must EXIST on the target.
    #[serde(default = "marker")]
    pub marker_topic: String,
    pub topic_mapping_prefix: String,
    #[serde(default = "rf1")]
    pub default_replication_factor: i16,
    /// "delete" (default) or "keep".
    #[serde(default = "delete")]
    pub teardown: String,
}
fn marker() -> String {
    "logweir.scratch".into()
}
fn rf1() -> i16 {
    1
}
fn delete() -> String {
    "delete".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleSpec {
    pub window_start: chrono::DateTime<chrono::Utc>,
    pub window_end: chrono::DateTime<chrono::Utc>,
    #[serde(default = "n25")]
    pub records_per_partition: usize,
    /// Which records in the window this drill reconciles. A CLOSED SET, not a
    /// free-form string: an unsupported spelling is unspellable rather than
    /// detected somewhere downstream. Rotating across runs is the adopter's
    /// job; the scorecard always records which was used.
    ///
    /// Defaults to `head` — the behaviour v0.1 actually implements end to end.
    /// It defaulted to `random` until Task 21c ran a drill against a real
    /// archive: see `Anchor`'s own doc comment for what that cost.
    #[serde(default)]
    pub anchor: Anchor,
    #[serde(default)]
    pub max_partitions: Option<u32>,
}
fn n25() -> usize {
    25
}

/// WHICH records in the sampled window a drill reconciles.
///
/// # Why this is an enum, and why `Head` is the default
///
/// It was a free-form `String` defaulting to `"random"` until Task 21c. Two
/// consequences, both measured against a live cluster and a real archive:
///
/// 1. **Every adopter who omitted the field got `random`**, and phase 4 honours
///    the anchor when choosing WHICH archive records to fingerprint while
///    `phase7_verify::verdict_for_selection` reads the target's FIRST
///    `records_per_partition` records (`consume_range(mapped, partition, 0,
///    count)`, because a drill's destination is a freshly created scratch
///    topic). The two therefore sample different records. Measured on 338
///    records per partition with 25 sampled: they overlapped in 2 offsets, and
///    a byte-for-byte correct restore scored `records_sampled_matching:
///    12 / 150`, `pass_rate_measured: 0.08`, `outcome: fail-integrity`. The
///    small overlap was arithmetic coincidence, not partial success.
/// 2. **Any unsupported spelling degraded silently to head-like behaviour**,
///    because nothing between the spec and `select_sample` constrained the
///    string.
///
/// The enum removes (2) outright — an unsupported value now fails to parse —
/// and `Head` as the default removes (1) for everyone who does not ask for
/// something else. `Tail` and `Random` remain SPELLABLE and are REFUSED by
/// `phase0_admit::run` with exit 3, before anything runs, naming the
/// limitation. They are refused for DIFFERENT reasons, and the distinction
/// matters to whoever lifts the refusal:
///
/// - **`Tail` is UNSOUND, not unbounded.** Reading the last `count` records of
///   the target is perfectly bounded — phase 6 already reads the target's end
///   offsets, so `consume_range(topic, partition, hi - count, count)` needs no
///   new machinery. What it returns is the last `count` records OF THE RESTORED
///   WINDOW, and phase 4's `Tail` selects the last `count` archive records IN
///   THE SAMPLED WINDOW. Those coincide only if the restore wrote every
///   in-window record contiguously with none dropped or filtered — which is
///   exactly the property the drill exists to test, so it may not be assumed. A
///   drill that assumed it would reconcile the wrong records precisely when the
///   restore was faulty.
/// - **`Random` is both.** Its offsets are spread across the window, so
///   reaching them means either seeking the target by original offset (unsound
///   for the same reason plus one more — a scratch topic's offsets are
///   window-relative, which is why `phase7_verify::compare` reads
///   `x-original-offset` instead of trusting positions) or consuming the whole
///   span between the lowest and highest sampled offsets, which on a
///   million-record partition reads the entire partition and is the opposite of
///   what sampling is for.
///
/// Refusing is the honest answer until phase 7 gains a read strategy that can
/// honour them. Note also that both lanes share `verdict_for_selection`'s one
/// `consumed` vector — `records_restored` and the consume-only lane's "at least
/// `claimed` records read back" obligation are computed from it — so changing
/// the read strategy is a change to both, not to the fingerprint lane alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Anchor {
    /// The first `records_per_partition` records in the window, by offset.
    /// The only anchor v0.1 implements end to end.
    #[default]
    Head,
    /// The last `records_per_partition` records in the window, by offset.
    Tail,
    /// A deterministic, evenly spaced sample across the window.
    Random,
}

impl Anchor {
    /// The wire spelling, which is also what reaches `sample.anchor` in the
    /// signed scorecard. That field stays a `String` in `logweir_core::
    /// scorecard`: Global Constraint 12 freezes `format_version` at 1.0.0, and
    /// narrowing a published field's type is not an optional addition.
    pub fn as_str(self) -> &'static str {
        match self {
            Anchor::Head => "head",
            Anchor::Tail => "tail",
            Anchor::Random => "random",
        }
    }
}

impl std::fmt::Display for Anchor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectivesSpec {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub pass_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowedClusters {
    /// Supplied as a SEPARATE file argument, never read from the drill spec,
    /// so an edited spec cannot widen its own allowlist (spec §9.3 phase 0).
    pub allowed_cluster_ids: Vec<String>,
    /// Refused as a target even if it appears above.
    #[serde(default)]
    pub source_cluster_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDoc {
    pub approver: String,
    pub ticket: String,
    /// sha256 over the canonical bytes of the drill spec.
    pub plan_hash: String,
    pub approved_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(yaml: &str) -> Result<SampleSpec, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// THE DEFAULT IS `head`. It was `random` — an anchor phase 7 cannot
    /// reconcile — so every adopter who omitted the field got a drill that
    /// reported a healthy backup as broken. A default must never name
    /// behaviour the product does not have.
    #[test]
    fn an_omitted_anchor_defaults_to_head_not_random() {
        let s = sample("window_start: 2026-08-29T00:00:00Z\nwindow_end: 2026-08-30T00:00:00Z\n")
            .unwrap();
        assert_eq!(s.anchor, Anchor::Head);
        assert_eq!(Anchor::default(), Anchor::Head);
    }

    #[test]
    fn each_supported_anchor_round_trips_through_its_wire_spelling() {
        for (text, want) in [
            ("head", Anchor::Head),
            ("tail", Anchor::Tail),
            ("random", Anchor::Random),
        ] {
            let s = sample(&format!(
                "window_start: 2026-08-29T00:00:00Z\nwindow_end: 2026-08-30T00:00:00Z\nanchor: {text}\n"
            ))
            .unwrap();
            assert_eq!(s.anchor, want);
            assert_eq!(s.anchor.as_str(), text);
            assert_eq!(serde_yaml::to_string(&s.anchor).unwrap().trim(), text);
        }
    }

    /// UNSPELLABLE, not detected. `anchor` was a free-form `String`, so any
    /// unsupported value reached `select_sample` and — before Task 21c added a
    /// catch-all there — degraded silently to head-like behaviour. The closed
    /// enum makes the invalid state unrepresentable, which is the same
    /// structural move that closed phase 7's false pass.
    #[test]
    fn an_unsupported_anchor_spelling_does_not_parse_at_all() {
        let err = sample(
            "window_start: 2026-08-29T00:00:00Z\nwindow_end: 2026-08-30T00:00:00Z\nanchor: sideways\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("sideways"), "{err}");
        // Case matters too: the wire spelling is lowercase and only lowercase.
        assert!(sample(
            "window_start: 2026-08-29T00:00:00Z\nwindow_end: 2026-08-30T00:00:00Z\nanchor: Head\n"
        )
        .is_err());
    }
}
