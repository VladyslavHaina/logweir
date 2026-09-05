use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrillSpec {
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Notifications {
    #[serde(default)]
    pub webhooks: Vec<String>,
    #[serde(default)]
    pub slack_webhook: Option<String>,
    #[serde(default)]
    pub pagerduty_routing_key: Option<String>,
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
/// limitation: they select archive offsets phase 7's leading-range read cannot
/// reach, and making it reach them means either seeking the target by original
/// offset (unsound — a scratch topic's offsets are window-relative, which is
/// exactly why `phase7_verify::compare` reads `x-original-offset` instead of
/// trusting positions) or consuming the whole span between the lowest and
/// highest sampled offsets (unbounded — on a million-record partition a
/// `random` sample would read the entire partition, which is the opposite of
/// what sampling is for). Refusing is the honest answer until phase 7 gains a
/// read strategy that can honour them.
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
