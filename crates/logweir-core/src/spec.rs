use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// **Interface I20's canonical name for this document.** `RestoreSpec` is what
/// all new code and every producer of `Restore.spec.planBytes` uses;
/// `DrillSpec` is the same type under its tag-0 name, kept valid so every
/// checked-in reference keeps compiling.
///
/// An ALIAS and not a new type, deliberately: spec §6.1 amendment 4 says "the
/// restore plan document has ONE grammar, and it is the runner's
/// `restore.yaml`", and two types would be two grammars the moment one of them
/// grew a field. `crates/logweir/tests/restore_mode.rs::
/// restore_spec_is_the_canonical_name` asserts `serde_yaml::from_str` accepts
/// the same bytes under both names, so neither can drift from the other.
///
/// It is NOT `RestoreSpecBlock`. That is the nested `{point_in_time}` block
/// INSIDE this document (Task 9), reached as `RestoreSpec::restore`, and it is
/// not a synonym for the document.
pub type RestoreSpec = DrillSpec;

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
    /// The RESTORE's own window, as distinct from the SAMPLE's.
    ///
    /// Optional and defaulted so every spec written before this block existed
    /// still parses, and so an absent block means exactly what it did before:
    /// `time_window.1` is `sample.window_end`. See `RestoreSpecBlock`.
    #[serde(default)]
    pub restore: RestoreSpecBlock,
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

/// Spec §3.2 `Restore.spec`'s restore block — the recovery POINT, which is the
/// only half of the window a spec may state.
///
/// # There is deliberately no `window_start` here, and there never will be
///
/// Spec §6.1 H7: the window is a closed interval and the spec binds its
/// **start** to the archive, not to a field. `RestorePlan.time_window.0` is the
/// archive set's earliest covered timestamp as recorded in the manifest, and
/// `crate::engine::WindowFloorSource` is how the plan says so. A spec-supplied
/// floor is what guard **G-WIN** exists to refuse: a restore that inherits a
/// later start silently loses everything before it, while phase 7 reconciles
/// only the *sampled* records and the scorecard says pass.
///
/// `point_in_time` is the window's END when it is present, and
/// `sample.window_end` when it is absent — which preserves every existing
/// drill's behaviour for the end of the window.
///
/// # It is INCLUSIVE, and its receipt twin is not
///
/// The window is a closed interval: the engine filters `timestamp >= start &&
/// timestamp <= end`, so a record whose timestamp equals `point_in_time`
/// exactly is restored. The `BackupReceipt`'s `covered.to_ms` is the opposite
/// convention — the newest segment's end plus one millisecond, i.e. the first
/// instant the archive does NOT cover (`logweir::backup::phase_run`). Copying
/// a `covered.to_ms` in here is harmless; assuming `point_in_time` is
/// exclusive silently drops the boundary record. Recorded in
/// `docs/stability.md`.
///
/// A `point_in_time` at or before the archive set's earliest covered
/// timestamp is refused, exit 3, naming both integers: the window would hold
/// no instant and the restore would produce nothing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestoreSpecBlock {
    #[serde(default)]
    pub point_in_time: Option<DateTime<Utc>>,
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
    /// How Logweir's own client AND the engine authenticate to the restore
    /// TARGET (Task 6, interface **I1**). `#[serde(default)]` so every
    /// checked-in spec that predates this field keeps parsing as `Plaintext`.
    ///
    /// It is HERE and deliberately NOT on `SourceSpec` (critique A F15):
    /// `SourceSpec` is the ARCHIVE's location — `{storage, backup, topics}`,
    /// no `bootstrap_servers` and no broker — so an auth block on it would
    /// ship a field nothing reads. `BackupSourceSpec`, which does name a
    /// broker, carries its own.
    #[serde(default)]
    pub auth: AuthSpec,
    /// **Interface I33.** WHICH of the two restores this is (spec §6.1;
    /// §3.1's `Drill` = `Restore` with `target.mode: scratch`).
    ///
    /// `#[serde(default)]` — `Scratch` — so every checked-in spec that
    /// predates this field keeps its exact behaviour, which is what makes
    /// `logweir drill run` and `logweir restore run` the same command over the
    /// same documents.
    #[serde(default)]
    pub mode: TargetMode,
    /// How the target topics are NAMED in `newTopic` mode. Absent means
    /// `default_topic_prefix(<the recovery point>)`.
    ///
    /// Absent is also the only legal state in `Scratch` mode, where the name
    /// comes from `topic_mapping_prefix`; it is not read there and it is not
    /// refused there either, because a field an adopter left behind after
    /// switching modes is not a plan this build has to reject.
    #[serde(default)]
    pub topic_naming: Option<TopicNaming>,
    /// The v0.1 segregation proof. Must EXIST on the target.
    ///
    /// **`Scratch` mode only.** `TargetMode::NewTopic` skips this check —
    /// see `TargetMode`'s own doc comment for why the marker proves nothing
    /// about a restore into a brand-new topic on a real cluster.
    #[serde(default = "marker")]
    pub marker_topic: String,
    /// The `Scratch`-mode target-topic prefix, and **required on purpose**.
    ///
    /// It is deliberately NOT `#[serde(default)]`: an empty prefix maps every
    /// source topic onto ITSELF, which on a scratch cluster is a restore into
    /// the topic the archive was taken from. Today a spec that omits it is a
    /// serde error before any phase runs, and that is the right refusal. A
    /// `newTopic` spec therefore still carries it (see
    /// `examples/restore.yaml`), where it is unread — `topic_naming.prefix`,
    /// or `default_topic_prefix`, is the name source in that mode
    /// (`target_topic_prefix`).
    pub topic_mapping_prefix: String,
    #[serde(default = "rf1")]
    pub default_replication_factor: i16,
    /// "delete" (default) or "keep". **`Scratch` mode only**: phase 9 tears
    /// nothing down in `newTopic` mode at any value of this field (Global
    /// Constraint 19).
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

/// **Interface I33, the Rust half.** The two restores tag 1 performs.
///
/// The serde names are `scratch` and `newTopic`, in that order, and they are
/// the `Restore` CRD's `target.mode` enum byte for byte
/// (`crates/weirkeeper/config/crd/restores.yaml`, pinned by
/// `crates/weirkeeper/tests/crd_shape.rs::
/// restore_target_mode_accepts_only_scratch_or_new_topic` and compared against
/// THIS type by `the_crd_mode_enum_and_target_mode_agree` in the same file). A
/// `kubectl apply` that succeeds and a `restore run` that then refuses to parse
/// the same string is the failure that agreement closes.
///
/// # What the two modes actually differ in
///
/// `Scratch` is v0.1's behaviour, unchanged: the marker topic must exist and
/// be healthy, the target cluster id must be in `allowedClusterIds` and must
/// not equal the source, and phase 9 deletes the topics the run created.
/// Those three are the SCRATCH SEGREGATION PROOF — they exist to establish
/// that a drill is writing to a throwaway cluster.
///
/// `NewTopic` skips all three, and the reason is that each of them is
/// meaningless for the thing tag 1's flagship actually does. A restore into a
/// new topic on a real cluster is non-destructive BY CONSTRUCTION: it only
/// ever writes a topic that did not exist, which phase 0 proves by refusing
/// the plan outright if any mapped target topic is already there. A marker
/// topic on a production cluster would be a lie about that cluster's purpose;
/// an allowlist of scratch clusters cannot contain the cluster an operator is
/// recovering INTO; and tearing the restored topic down would delete the
/// recovery.
///
/// `Default` is `Scratch`, so `#[serde(default)]` on `TargetSpec::mode` makes
/// every spec written before this field existed mean exactly what it meant.
///
/// `JsonSchema` is derived because `crate::scorecard::TargetInfo::mode` carries
/// this type into the SIGNED document and therefore into
/// `schemas/logweir-drill-scorecard-1.0.0.json` (global ruling GR3). It is the
/// only spec type that does: nothing else in this module reaches a schema.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum TargetMode {
    #[default]
    Scratch,
    NewTopic,
}

impl TargetMode {
    /// `true` for the default. Named here rather than written as a closure at
    /// the `skip_serializing_if` site so the scorecard's "absent means
    /// scratch" rule has one owner, beside the `Default` impl it agrees with.
    pub fn is_scratch(&self) -> bool {
        matches!(self, TargetMode::Scratch)
    }
}

impl std::fmt::Display for TargetMode {
    /// The WIRE spelling, so a refusal message and a CRD field cannot disagree
    /// about what mode a run was in.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TargetMode::Scratch => "scratch",
            TargetMode::NewTopic => "newTopic",
        })
    }
}

/// `target.topicNaming` — how `newTopic` mode names the topics it creates.
///
/// One field today. It is a BLOCK rather than a bare `target.topic_prefix`
/// because the CRD already declares it as one
/// (`target.topicNaming.prefix`, and `status.newTopics` is built from it), and
/// a Rust shape that flattened it would make the two documents disagree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicNaming {
    pub prefix: String,
}

/// The `newTopic` default prefix: `restore-<YYYYmmddTHHMMSSZ>-`.
///
/// So `orders` at a recovery point of `2026-09-07T14:05:00Z` becomes
/// `restore-20260907T140500Z-orders`, which says on the broker's own topic
/// list what the topic is and what instant it was recovered to.
///
/// **The compact `YYYYmmddTHHMMSSZ` form is for KAFKA TOPIC NAMES ONLY.**
/// Kafka permits `[a-zA-Z0-9._-]`, so it is legal there. Kubernetes object
/// names are DNS-1123 and permit neither the uppercase `T`/`Z` nor a leading
/// digit after a dot; those names use the form Task 18 defines and never this
/// one.
///
/// A PURE function of its argument — no clock, no environment (Global
/// Constraint 1) — so the mapped names a plan carries are a function of the
/// approved bytes and of nothing else.
pub fn default_topic_prefix(point_in_time: DateTime<Utc>) -> String {
    format!("restore-{}-", point_in_time.format("%Y%m%dT%H%M%SZ"))
}

/// The prefix phase 0 actually maps every source topic through, for THIS
/// spec's mode. The ONE place the rule lives, so a renderer, a refusal message
/// and the signed scorecard cannot each derive it differently.
///
/// * `Scratch` → `target.topic_mapping_prefix`, exactly as v0.1.
/// * `NewTopic` → `target.topic_naming.prefix` when the spec states one, else
///   `default_topic_prefix` of THE RECOVERY POINT.
///
/// **"The recovery point" is `restore.point_in_time` when the spec states one
/// and `sample.window_end` otherwise** — the same pairing
/// `logweir::drill::build_plan_with_floor` makes for `time_window.1` and
/// `phase0_admit::target_topic_preflight` makes for the broker's timestamp
/// bound. Reading `point_in_time` alone would leave the default prefix
/// unconstructible for a `newTopic` spec that names no explicit recovery
/// point, and reading `sample.window_end` alone would name an instant this
/// restore never asks the engine for.
pub fn target_topic_prefix(spec: &DrillSpec) -> String {
    match spec.target.mode {
        TargetMode::Scratch => spec.target.topic_mapping_prefix.clone(),
        TargetMode::NewTopic => match &spec.target.topic_naming {
            Some(n) => n.prefix.clone(),
            None => {
                default_topic_prefix(spec.restore.point_in_time.unwrap_or(spec.sample.window_end))
            }
        },
    }
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

/// How a cluster's client authenticates. Task 2 landed the SHAPE; Task 6
/// landed the three methods below that wire it to a client and to a rendered
/// document (interface **I1**).
///
/// It landed in Task 2 rather than in Task 6 because `BackupSourceSpec`
/// below is declared with an `auth` field, so a Task-2 implementer reading
/// only Task 2 could not compile the type it was told to produce.
///
/// **No password field, at any variant.** A SCRAM secret reaches the engine
/// through the engine's own `${VAR}` expansion of its config file
/// [U/kafka-backup/crates/kafka-backup-cli/src/commands/config.rs:6-35] and
/// Logweir's own client through its environment — never through a spec file
/// an adopter commits, and never through a rendered document. `tls` is
/// separate from the mechanism because SASL/SCRAM over PLAINTEXT and over SSL
/// are two different `security.protocol` values for one mechanism, and an
/// adopter with a private CA has to configure BOTH trust stores (Global
/// Constraint 29).
/// The default is `Plaintext`, expressed as `#[derive(Default)]` +
/// `#[default]` rather than as the hand-written `impl Default for AuthSpec`
/// the plan's interface block writes. The two are the same value —
/// `AuthSpec::default() == AuthSpec::Plaintext` either way, which is what a
/// consumer of this interface can observe — and `just lint`'s
/// `clippy::derivable_impls` (under `-D warnings`) rejects the hand-written
/// form. `Anchor`, forty lines up, is the same shape written the same way.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum AuthSpec {
    #[default]
    Plaintext,
    ScramSha512 {
        username: String,
        #[serde(default)]
        tls: bool,
    },
}

impl AuthSpec {
    /// The render-side twin, for `BackupPlan::source_auth` and
    /// `RestorePlan::target_auth`. It maps and never refuses: the mode a spec
    /// names is carried faithfully into the plan, so a plan that asked for
    /// SCRAM can never be rendered unauthenticated on the operator's behalf.
    ///
    /// **It carries no password, at any variant** — see `AuthRender`'s own doc
    /// comment. The secret reaches the engine through the engine's own
    /// `${VAR}` expansion of its config text and reaches Logweir's client
    /// through `AuthConfig::from_spec`; neither path passes through a plan.
    pub fn to_render(&self) -> crate::engine::AuthRender {
        match self {
            AuthSpec::Plaintext => crate::engine::AuthRender::Plaintext,
            AuthSpec::ScramSha512 { username, tls } => crate::engine::AuthRender::ScramSha512 {
                username: username.clone(),
                tls: *tls,
            },
        }
    }

    /// The mode as the two documents spell it: `"plaintext"` or
    /// `"scramSha512"`.
    ///
    /// **These are the serde tag values of this enum, and they are the
    /// `KafkaCluster` CRD's `auth.mode` enum, byte for byte and in that
    /// order** (Task 15b, interface **I33**). Three surfaces have to agree —
    /// the YAML an adopter writes, the CRD an adopter applies, and the signed
    /// documents a reader parses — so the strings are asserted equal by
    /// `crates/weirkeeper/tests/crd_shape.rs::the_crd_auth_mode_enum_and_auth_spec_agree`
    /// and by
    /// `crates/logweir/tests/auth_binding.rs::the_scorecard_auth_block_and_auth_spec_agree`
    /// rather than kept in step by three comments.
    ///
    /// `&'static str` on purpose: a closed set of two literals cannot be
    /// handed a value computed at run time.
    pub fn mode_str(&self) -> &'static str {
        match self {
            AuthSpec::Plaintext => "plaintext",
            AuthSpec::ScramSha512 { .. } => "scramSha512",
        }
    }

    /// The SASL principal, when there is one.
    ///
    /// **This is what `planBytes` binds — guard G-ID.** The Secret named by a
    /// `secretRef` is mutable and covered by no hash, so binding only the
    /// address would let anyone with `update` on that Secret change WHICH
    /// PRINCIPAL Logweir authenticates as, after approval, with no plan-hash
    /// change and no new signature; on SASL/SCRAM the credential *is* the
    /// authorisation. Logweir cannot OBSERVE the principal the broker
    /// authenticated — Kafka exposes no such call and neither client offers
    /// one — so the binding is structural, not observational: `sasl_username`
    /// is rendered from the plan and never from a cluster object read at run
    /// time.
    ///
    /// `None` under `Plaintext`, which is not the same as an empty username.
    pub fn username(&self) -> Option<&str> {
        match self {
            AuthSpec::Plaintext => None,
            AuthSpec::ScramSha512 { username, .. } => Some(username),
        }
    }
}

/// The adopter-facing shape of a `--from-cluster` backup. `render_backup`
/// consumes `crate::engine::BackupPlan`, not this: a spec is what a human
/// wrote and a plan is what the guards have already accepted, and collapsing
/// the two is how an unvalidated topic list reaches a rendered document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSpec {
    pub source: BackupSourceSpec,
    pub storage: crate::engine::StorageUrl,
    pub backup_id: String,
    #[serde(default)]
    pub backup: BackupSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSourceSpec {
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub auth: AuthSpec,
    /// NAMED topics, never patterns — GC18(c) rail 1, enforced at render time
    /// by `crate::guard::reject_glob_metacharacters` (**G-GLOB**). Required,
    /// with no default: an omitted list is the one shape that would mean
    /// "everything" to the engine, and a mandatory allowlist whose absence
    /// means "all topics" is not an allowlist.
    pub topics: Vec<String>,
}

/// The `backup:` block's tunables. The two keys the rendered document pins
/// unconditionally — `continuous: false` and `include_offset_headers: true` —
/// are deliberately NOT fields: they are invariants of what a Logweir-driven
/// backup is, not settings (see `render_backup::render`'s comments for what
/// each one costs if flipped). `strip_offset_headers` is the other end of the
/// same invariant and is pinned in the RESTORE document only: it is a field of
/// the engine's `RestoreOptions` and of nothing else, so a backup config
/// naming it is dropped as an unknown key (Task 4 review, F-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSettings {
    #[serde(default = "zstd")]
    pub compression: String,
    #[serde(default = "seg_max_records")]
    pub segment_max_records: u64,
    #[serde(default = "seg_max_bytes")]
    pub segment_max_bytes: u64,
    #[serde(default = "max_concurrent_partitions")]
    pub max_concurrent_partitions: u32,
}

impl Default for BackupSettings {
    fn default() -> Self {
        Self {
            compression: zstd(),
            segment_max_records: seg_max_records(),
            segment_max_bytes: seg_max_bytes(),
            max_concurrent_partitions: max_concurrent_partitions(),
        }
    }
}

fn zstd() -> String {
    "zstd".into()
}
/// The harness's own checked-in values (`e2e/compose/config/backup-drill.yaml:37-39`),
/// which are what the drill archive was produced with.
fn seg_max_records() -> u64 {
    1000
}
fn seg_max_bytes() -> u64 {
    10_485_760
}
fn max_concurrent_partitions() -> u32 {
    3
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

/// The wire spelling `subject_kind` takes when the approval does not say.
///
/// `Restore`, and this is a COMPATIBILITY default, not a policy one: every
/// approval minted before Task 22 carries no `subject_kind` at all, and tag
/// 1's only approved subject is a restore (spec §5 contains no `approv`; §16
/// puts the second kind in tag 2). A pre-existing `approval.json` therefore
/// keeps verifying unchanged on the runner path. The CONTROLLER makes the
/// opposite call on purpose: `weirkeeper::controllers::approval`'s document
/// defaults the field to `""`, so an absent field is refused by check 8 when
/// the referent is not a `Restore` — fail-closed where a cluster object is
/// being authorised, backwards-compatible where a file is being read.
pub const SUBJECT_KIND_RESTORE: &str = "Restore";

fn default_subject_kind() -> String {
    SUBJECT_KIND_RESTORE.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDoc {
    pub approver: String,
    pub ticket: String,
    /// sha256 over the canonical bytes of the drill spec.
    pub plan_hash: String,
    pub approved_at: chrono::DateTime<chrono::Utc>,
    /// Which kind of subject this approval authorises — `Restore` or
    /// `Backup`, the wire spellings
    /// `weirkeeper::crds::approval::SubjectKind` serialises to.
    ///
    /// **INSIDE THE SIGNED BYTES, and that is the whole value of the field.**
    /// `logweir drill approve` serialises this struct and signs the resulting
    /// payload, so the kind an approval binds cannot be changed without
    /// invalidating the signature. The controller's check 8 compares it
    /// against the referent's actual kind; if the field lived in the DSSE
    /// sidecar instead, check 8 would be comparing a value anyone with
    /// `patch` on the Secret could rewrite.
    ///
    /// Defaults to [`SUBJECT_KIND_RESTORE`] on READ so approvals minted before
    /// the field existed still parse; `serde` writes it on every mint, so a
    /// document this workspace produces always names its kind.
    #[serde(default = "default_subject_kind")]
    pub subject_kind: String,
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
