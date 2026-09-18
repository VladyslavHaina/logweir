//! The six kinds of `logweir.dev/v1alpha1`, the CEL immutability seals, and
//! the deterministic emitter the drift gate diffs against.
//!
//! SIX KINDS, AND `Drill` IS NOT ONE OF THEM. A drill is a [`Restore`] whose
//! `spec.target.mode` is `scratch` (spec §3.1; Global Constraint 34). A
//! `Restore` only ever writes a *new* topic, so restoring into production is
//! non-destructive by construction, and the drill's only distinguishing
//! behaviour is a scratch target plus phase-9 teardown — a field, not a
//! lifecycle. Two kinds would duplicate the whole exit-code-to-condition
//! table for one boolean. `MetadataSnapshot` stays reserved and unbuilt;
//! `Switchover` is tag 2 and appears nowhere in this crate, not even in the
//! [`approval::SubjectKind`] enum.
//!
//! THE GROUP IS OURS AND ONLY OURS. [`GROUP`] is `logweir.dev` (Global
//! Constraints 5 and 14): `weirkeeper` reads no vendor custom resource and
//! creates none. `crates/weirkeeper/tests/crd_shape.rs`'s
//! `no_vendor_crd_group_is_named_anywhere` reads this directory, the examples
//! directory and `config/crd/` and asserts the vendor group strings appear in
//! none of them, so the property is a test and not an intention.
//!
//! WHY THE CRD YAML IS CHECKED IN AND DIFFED. Same reason the scorecard schema
//! is (`.github/workflows/ci.yml`'s schema arm): a CRD change is a FORMAT
//! change, and a format change that appears as a diff in a pull request is one
//! a reviewer sees. [`render_all`] is the single renderer; the
//! `emit_crds` example writes its output to files, the drift arm diffs those
//! files, and `the_checked_in_crds_are_what_the_emitter_renders` compares them
//! in-process so the gate also holds in `cargo test` with no subprocess.
//!
//! WHY `seal_spec` EXISTS. `kube`'s `CustomResource` derive emits no
//! `x-kubernetes-validations`, so the immutability rules are injected into the
//! generated [`CustomResourceDefinition`] after the fact. kube 0.99 does ship
//! a separate `CELSchema` derive with a `#[cel_validate]` attribute; it is
//! deliberately not used here, because the rules this task lands are attached
//! to `.spec` as a whole (see [`backup_schedule::SOURCE_REF_IMMUTABLE_RULE`] for why
//! that placement is load-bearing) and one helper that all six kinds pass
//! through is what makes `every_spec_is_sealed_and_only_suspend_is_mutable`
//! checkable at one place.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, CustomResourceDefinitionVersion, JSONSchemaProps,
    JSONSchemaPropsOrArray, ValidationRule,
};
use kube::CustomResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod approval;
pub mod backup;
pub mod backup_destination;
pub mod backup_schedule;
pub mod kafka_cluster;
pub mod preflight;
pub mod protection_policy;
pub mod recovery_catalog;
pub mod rehearsal_schedule;
pub mod restore;
pub mod retention_policy;
pub mod selection;
pub mod topic_discovery;
pub mod trust_policy;
pub mod trust_roster;

/// The API group. Global Constraint 14: Logweir owns `logweir.dev`, now, not
/// deferred — and never a vendor's group.
pub const GROUP: &str = "logweir.dev";

/// The one served, stored version. `v1alpha1` for tag 1.
pub const VERSION: &str = "v1alpha1";

/// Every kind, in the order [`render_all`] emits them.
///
/// THE LIST IS THE DECISION RECORD'S, NOT A CONVENIENCE. ADR 0008 Amendment A
/// fixes the kind list and requires a recorded architectural decision for each
/// addition: the first six are Amendment A's, `BackupDestination`,
/// `TopicDiscovery` and `Preflight` are **Amendment F**'s, and `TrustPolicy`,
/// `ProtectionPolicy`, `RehearsalSchedule`, `RecoveryCatalog` and
/// `RetentionPolicy` are **Amendment G**'s. `TrustRoster` is NOT removed: it
/// stays served and reconciled, deprecated in its own description, so a
/// cluster that has one keeps working while `TrustPolicy` is adopted.
/// `the_kind_list_is_exactly_the_adr` asserts the emitted set against this
/// list and additionally asserts that no kind is named `Drill`,
/// `RestoreDrill`, `Switchover` or `MetadataSnapshot`, and that no kind
/// contains `Kafka` other than `KafkaCluster`.
pub const KINDS: [&str; 14] = [
    "KafkaCluster",
    "BackupSchedule",
    "Backup",
    "Restore",
    "Approval",
    "TrustRoster",
    "BackupDestination",
    "TopicDiscovery",
    "Preflight",
    "TrustPolicy",
    "ProtectionPolicy",
    "RehearsalSchedule",
    "RecoveryCatalog",
    "RetentionPolicy",
];

/// The CRD spelling of a Kubernetes `metav1.Time`: an RFC 3339 string, which
/// is what `type: string, format: date-time` means in a structural schema.
///
/// WHY NOT `k8s_openapi::apimachinery::pkg::apis::meta::v1::Time`. That type's
/// `JsonSchema` implementation sits behind `k8s-openapi`'s optional `schemars`
/// feature, and this crate's `k8s-openapi` feature list is fixed at exactly
/// `["v1_29"]` by Global Constraint 25 and asserted by
/// `tests/linkage.rs::kube_is_declared_without_default_features`. Taking a
/// second feature to reach a `JsonSchema` impl would reopen a landed decision
/// for a schema that is byte-identical either way: `metav1.Time` renders as
/// `{type: string, format: date-time}` and so does
/// `chrono::DateTime<Utc>` under `schemars`'s `chrono` feature, which this
/// workspace already enables (`Cargo.toml`'s `schemars = { version = "0.8",
/// features = ["chrono"] }`). The wire value is an RFC 3339 timestamp on both
/// sides of that choice.
pub type Time = chrono::DateTime<chrono::Utc>;

/// A reference to another Logweir object in the SAME namespace.
///
/// Namespace-local by construction: there is no `namespace` field, because a
/// cross-namespace reference is a privilege-escalation surface (the referrer's
/// RBAC does not cover the referent's namespace) and Global Constraint 30 puts
/// one controller in one cluster with no fleet.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LocalRef {
    /// The referenced object's `metadata.name`, in this namespace.
    pub name: String,
}

/// Where an archive lives, and the credential that reaches it.
///
/// `url` is an object-store URL (`s3://…`, `gs://…`, `az://…`, `http://…` —
/// Global Constraint 9's feature set). It is not a bucket name plus a prefix,
/// because the CLI half already takes one URL
/// (`logweir-core`'s `BackupSpec::storage`) and two spellings of one location
/// is how a controller and a CLI come to disagree about which archive they
/// read.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveRef {
    /// The object-store URL of the archive root.
    pub url: String,
    /// A Secret in this namespace carrying the object-store credential. Its
    /// keys are read by the Job, never by the controller's own archive
    /// handle, which is read-only (Global Constraint 6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalRef>,
}

/// A `metav1.Condition`: the standard six fields with their standard
/// meanings.
// Declared here rather than taken from
// `k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition`, which has the
// same `JsonSchema`-behind-a-feature problem `Time` documents and the same
// resolution. The fields below are `metav1.Condition`'s, so a client reading
// `.status.conditions` sees the shape it expects. Kept out of the doc comment
// because `schemars` publishes doc comments as `description` in the shipped
// CRD, and this belongs in the code and not in the wire format.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// The condition type, in CamelCase — `Ready`, `Reachable`, `Loaded`,
    /// `Verified`, `Expired`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// `True`, `False` or `Unknown`.
    pub status: String,
    /// The `.metadata.generation` this condition was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// When the condition last changed from one status to another.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// A short, machine-readable CamelCase reason for the current status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// A human-readable message. Never carries key material or a credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// What `weirkeeper` recorded when it verified a piece of signed evidence.
///
/// The controller reads the evidence object with its own read-only credential,
/// resolves the signing key from `TrustRoster.spec.signingKeys[].spkiPem`
/// (interface **I17**), performs the DSSE checks through `logweir-verify` and
/// writes this block. Spec §8's green-badge rule reads `result` here together
/// with a second field on the owning kind — `Backup.status.exitCode == 0`, or
/// `Restore.status.outcome == pass` — never `result` alone.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceVerification {
    /// `Valid`, `Invalid` or `NotAttempted`. An empty
    /// `TrustRoster.spec.signingKeys` is `NotAttempted` naming itself, never a
    /// silent `Invalid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// The `keyId` of the `TrustRoster` signing key whose public key verified
    /// the signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_key_id: Option<String>,
    /// The DSSE `payloadType` that was verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_type: Option<String>,
    /// When the controller performed the verification.
    ///
    /// **WHEN THIS VERDICT WAS REACHED, not when it was last re-confirmed.** A
    /// reconciler's own status patch is what wakes it (plan erratum
    /// **E11(d)**), so a field carrying a fresh clock read on every pass would
    /// make every pass a write and every write a wake-up. A change in
    /// `result`, `matchedKeyId`, `payloadType` or `detail` is a new verdict and
    /// takes a new instant; anything else keeps the stored one, and the second
    /// status patch is then a no-op that is never sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<Time>,
    /// Why the verdict is what it is: the verification error's own message on
    /// `Invalid`, and the reason nothing was attempted on `NotAttempted` — no
    /// evidence credential, an unreadable object, or a `TrustRoster` carrying
    /// no signing key material. Absent on `Valid`: there is nothing to explain
    /// about an answer that came out yes.
    ///
    /// NEVER KEY MATERIAL AND NEVER A CREDENTIAL. It is an error's `Display`
    /// and a fixed sentence, both of which name objects and key IDs only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The signing time the verdict was reached AGAINST: the document's own
    /// latest pre-signature timestamp (`logweir_core::trust::claimed_signing_time`).
    ///
    /// **ATTACKER-CONTROLLED FOR A COMPROMISED KEY**, which is why it is
    /// recorded rather than trusted. For a retired or expired key it is what
    /// distinguishes "signed while valid" from "signed afterwards"; for a key
    /// revoked as `KeyCompromise` it is deliberately NOT accepted as evidence
    /// of when the document was signed, and only a controller-written
    /// observation ([`EvidenceVerification::verified_at`] from an earlier
    /// reconcile) counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_at: Option<Time>,
    /// Which trust policy said so, and on what basis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<TrustBasis>,
}

/// Why a verification verdict is what it is, in trust terms.
///
/// # Why `Untrusted` is a fourth result and not `Invalid`
///
/// `Invalid` means the bytes do not match the signature. `Untrusted` means
/// they do, and the key that made them is one this installation will not
/// accept — revoked, unknown, or holding the wrong usage. Collapsing them
/// would tell an operator their archive is corrupt when it is not, and would
/// hide the one fact that matters: WHOSE key it was.
///
/// Every old reader treats anything that is not `Valid` as unverified
/// (`ui/pages/backups.js`'s `validVerification`), so the new value fails
/// closed on every surface that predates it.
///
/// # THAT SENTENCE IS A CONSTRAINT ON WRITERS, NOT A DESCRIPTION
///
/// `TRUST-UPGRADE-SIGNEDAT`, review finding **F1**. Three surfaces read
/// `result` and nothing else — `ui/pages/backups.js`'s `validVerification`,
/// `logweir_api::status`'s projection, and the `SIGNED` printer column on
/// `Backup` and `Restore` — so `result` is the ONLY field a new state may use
/// to fail closed on them. A verdict that meant "not verified" while leaving
/// `Valid` on `result` put a green *"verified by weirkeeper at … against key
/// …"* badge on the console, whatever it wrote beside it.
///
/// [`Self::basis`] `Unverified` is therefore always written with
/// `result: NotAttempted` and never with `Valid`. The basis refines an
/// already-safe result; it never rescues an unsafe one.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TrustBasis {
    /// `Current`, `Historical`, `RecordedBeforeRevocation`, `Unverified` or
    /// `None`.
    ///
    /// **`Historical` is a pass, not a downgrade**: the key was valid when it
    /// signed and has since been retired, which is what key rotation is
    /// supposed to look like. The console renders it as "verified against
    /// retired key `<id>` (signed before retirement)" and never as a warning.
    ///
    /// **`Unverified` is not a verdict at all**: the status was written by a
    /// controller that predates `signedAt`, so the key's validity window has
    /// not been compared to anything yet and the controller owes this object
    /// one bounded re-read of its own receipt. It is written only beside
    /// `result: NotAttempted`, it is never green, and it disappears as soon as
    /// the read supplies a real `signedAt`. See `docs/kubernetes.md` §15.2c.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    /// `Active`, `Retired`, `Expired`, `Revoked` or `Unknown` — the signing
    /// key's state at the moment the verdict was reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_state: Option<String>,
    /// Which `TrustPolicy` answered, with the revision that answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyRef>,
    /// **The controller will not re-read this run's document before this
    /// instant** — the bounded backoff behind `docs/kubernetes.md` §15.2c.
    ///
    /// A terminal object reconciles every `REQUEUE_SECS`, so "one bounded
    /// re-read" needed something on the object to be bounded BY: without it a
    /// document the archive cannot answer for costs one `Store::get` every 15
    /// seconds, for ever, for a verdict that cannot change. Written only when
    /// an attempt learned nothing — the archive did not answer, or there was
    /// no reader — and cleared the moment a read succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<Time>,
    /// `absent` once the bounded re-read COMPLETED and the document itself
    /// carried no signing time.
    ///
    /// **THE PERMANENT SETTLE, AND IT IS A FACT ABOUT THE DOCUMENT.** A block
    /// with no `signedAt` and no `basis` that compared one is re-read, because
    /// nothing on the status says whether the absence is this installation's
    /// or the document's. Once a read has answered, it IS the document's, and
    /// asking the archive again can only get the same answer — so this records
    /// the answer instead of the question. It is never written for a read that
    /// failed, and never beside a `signedAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_time_read: Option<String>,
}

/// A cluster-scoped `TrustPolicy`, with the revision a verdict used.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRef {
    /// Its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Its UID — a same-named replacement is a different policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// Its `metadata.generation`. A generation change is what makes the
    /// controller re-run the verdict over STORED fields, with no storage read
    /// and no change to `phase`, `exitCode` or `outcome`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
}

/// What a Job-backed run is doing right now, in user-facing terms.
///
/// # Additive, and absent on anything an older controller reconciled
///
/// This block is written by the controller and read by the console. It is not
/// a second source of truth about the OUTCOME — `phase`, `exitCode` and
/// `outcome` keep that job — it is the answer to "what is happening and why is
/// it taking so long", which those three could never give while a pod sat in
/// `ImagePullBackOff`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunProgress {
    /// `Admission`, `Queued`, `Preparing`, `Running`, `Verifying` or
    /// `Finished`.
    pub stage: String,
    /// A CamelCase reason, from the same closed vocabulary the `RunnerReady`
    /// condition uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// A sanitized, bounded explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 1024))]
    pub message: Option<String>,
    /// When `stage` or `reason` last CHANGED. Not a heartbeat: a reconciler's
    /// own status patch is what wakes it (erratum E11(d)), so a field carrying
    /// a fresh clock read on every pass would make every pass a write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// When the run was last observed, in active stages only, rewritten at
    /// most once every 60 s for the same reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_time: Option<Time>,
    /// Facts about the one runner pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner: Option<RunnerFacts>,
    /// The runner's own phase, when it reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_phase: Option<RunnerPhase>,
    /// What went wrong on the way, newest `lastSeen` first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 8))]
    pub diagnostics: Option<Vec<Diagnostic>>,
}

/// Facts about the runner pod, all optional because every one of them can be
/// unavailable while the pod is still legitimately starting.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerFacts {
    /// The Job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    /// The pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    /// `Pending`, `Running`, `Succeeded`, `Failed` or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_phase: Option<String>,
    /// Whether a node has been chosen. `false` with no diagnostic is the
    /// "nothing is wrong yet, it is just queued" case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<bool>,
    /// `Waiting`, `Running` or `Terminated`, for the `runner` container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_state: Option<String>,
    /// The kubelet's own waiting reason, verbatim and bounded —
    /// `ImagePullBackOff`, `CreateContainerConfigError`. Verbatim because a
    /// translated kubelet reason is a reason nobody can search for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub waiting_reason: Option<String>,
    /// When the runner container started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
}

/// The runner's own progress, from its `progress-phase=` key lines.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerPhase {
    /// The phase number, `-1`..`9`. A backup reports `-1` with a step name,
    /// because it has steps and not the drill's numbered phases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = -1, max = 9))]
    pub number: Option<i32>,
    /// The phase or step name, from the runner's own closed list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 32))]
    pub name: Option<String>,
}

/// One thing that went wrong, with a count rather than a repetition.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    /// A CamelCase code from the closed vocabulary.
    pub code: String,
    /// `Warning` or `Error`.
    pub severity: String,
    /// A sanitized, bounded explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 512))]
    pub message: Option<String>,
    /// The object it is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<DiagnosticObject>,
    /// When it was first seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<Time>,
    /// When it was last seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<Time>,
    /// How many times — capped, because a status is not a counter store and an
    /// unbounded integer here is an unbounded write rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 1000000))]
    pub count: Option<i64>,
}

/// The `Pod` or `Job` a diagnostic is about.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticObject {
    /// `Pod` or `Job`.
    pub kind: String,
    /// Its name.
    pub name: String,
}

/// The CEL rule that seals a whole `.spec`.
pub const SPEC_IMMUTABLE_RULE: &str = "self == oldSelf";

/// The message the API server returns when [`SPEC_IMMUTABLE_RULE`] refuses an
/// update.
pub const SPEC_IMMUTABLE_MESSAGE: &str = "spec is immutable; create a new object instead";

/// One CEL rule and the message that travels with it.
///
/// A PAIR, AND NOT TWO PARALLEL LISTS. Before this type there were exactly two
/// rules in this crate and [`seal_spec`] looked the message up from the rule
/// text; with fourteen rules on five `.spec`s a lookup is a table that can go
/// out of step with itself, and a message that belongs to a different rule is
/// the worst kind of admission error — legible, confident and wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpecRule {
    /// The CEL expression, exactly as the API server compiles it.
    pub rule: &'static str,
    /// What the API server returns when it refuses.
    pub message: &'static str,
}

impl SpecRule {
    /// Pair a rule with its message.
    #[must_use]
    pub const fn new(rule: &'static str, message: &'static str) -> Self {
        Self { rule, message }
    }
}

/// The seal every kind with a fully immutable `.spec` carries.
pub const WHOLE_SPEC_SEAL: [SpecRule; 1] =
    [SpecRule::new(SPEC_IMMUTABLE_RULE, SPEC_IMMUTABLE_MESSAGE)];

/// Attach `rules` to `.spec` of every version of `crd`, replacing whatever was
/// there.
///
/// `rules` is [`WHOLE_SPEC_SEAL`] for the kinds whose whole `.spec` is sealed,
/// [`backup_schedule::SPEC_RULES`] for the kind whose policy is editable, and
/// a kind-specific list for the ones whose `.spec` carries several rules
/// (`backup_destination::SPEC_RULES` is four).
///
/// THE RULES GO ON `.spec`, NOT ON `.spec`'s FIELDS. A per-field transition
/// rule is evaluated only when `oldSelf` exists for that field, so an optional
/// field could be ADDED after creation (absent → present) and a per-field
/// `self == oldSelf` would never fire. `optionalOldSelf` closes that and is
/// 1.30+, above the 1.29 floor Global Constraint 25 fixes. An object-level
/// rule is evaluated on every update, which is what makes the
/// `has(self.x) == has(oldSelf.x)` half of
/// [`backup_schedule::SOURCE_REF_IMMUTABLE_RULE`] able to refuse the absent →
/// present transition at all. The one exception is a rule attached to a REQUIRED
/// sub-object — `spec.request` on the two check kinds — which is evaluated on
/// every update for the same reason; [`attach_transition_rule`] is how that is
/// spelled, and it says so at its own call site.
///
/// PANICS, DELIBERATELY, rather than skipping. A `CustomResourceDefinition`
/// with no `.spec` property is a programming error in this module, and an
/// emitter that quietly wrote an UNSEALED CRD would produce a file that passes
/// the drift gate and seals nothing. The only caller is [`render_all`].
pub fn seal_spec(crd: &mut CustomResourceDefinition, rules: &[SpecRule]) {
    let name = crd.metadata.name.clone().unwrap_or_default();
    assert!(
        !crd.spec.versions.is_empty(),
        "{name}: a CRD with no versions cannot be sealed"
    );
    for version in crd.spec.versions.iter_mut() {
        let schema = version
            .schema
            .as_mut()
            .unwrap_or_else(|| panic!("{name}: version {} carries no schema", version.name));
        let root = schema
            .open_api_v3_schema
            .as_mut()
            .unwrap_or_else(|| panic!("{name}: version {} carries no root schema", version.name));
        let properties = root.properties.as_mut().unwrap_or_else(|| {
            panic!(
                "{name}: version {}'s root schema has no properties",
                version.name
            )
        });
        let spec = properties.get_mut("spec").unwrap_or_else(|| {
            panic!(
                "{name}: version {} has no `spec` property to seal",
                version.name
            )
        });
        // AN EMPTY LIST IS A DECISION, NOT AN OVERSIGHT, and exactly one kind
        // makes it: `ProtectionPolicy`'s spec is evaluation policy, never an
        // execution input, so editing it cannot change a recorded run and
        // there is nothing to seal. `exactly_one_kind_declares_no_spec_rule`
        // names it, so a second unsealed kind is a red test.
        spec.x_kubernetes_validations = (!rules.is_empty()).then(|| {
            rules
                .iter()
                .map(|r| ValidationRule {
                    rule: r.rule.to_string(),
                    message: Some(r.message.to_string()),
                    ..Default::default()
                })
                .collect()
        });
    }
}

/// Attach one rule to the schema ROOT of every version of `crd`.
///
/// THE ROOT IS THE ONE PLACE A RULE MAY READ `self.metadata.name`. The API
/// server exposes exactly `metadata.name` and `metadata.generateName` there
/// and nothing else, which is why a name-length budget
/// ([`backup_destination::R0_NAME_RULE`]) cannot be expressed anywhere below
/// it.
///
/// PANICS on a CRD with no versions, for the reason [`seal_spec`] does.
pub fn attach_root_rule(crd: &mut CustomResourceDefinition, rule: &str, message: &str) {
    let name = crd.metadata.name.clone().unwrap_or_default();
    assert!(
        !crd.spec.versions.is_empty(),
        "{name}: a CRD with no versions carries no root schema"
    );
    for version in crd.spec.versions.iter_mut() {
        let root = version
            .schema
            .as_mut()
            .and_then(|s| s.open_api_v3_schema.as_mut())
            .unwrap_or_else(|| panic!("{name}: version {} carries no root schema", version.name));
        root.x_kubernetes_validations
            .get_or_insert_with(Vec::new)
            .push(ValidationRule {
                rule: rule.to_string(),
                message: Some(message.to_string()),
                ..Default::default()
            });
    }
}

/// Attach one NON-TRANSITION validation rule below `.spec`.
///
/// For cross-field rules such as the saved-connection contract's
/// ([`kafka_cluster::CONNECTION_RULES`]), the destination's R5–R9
/// ([`backup_destination::NESTED_RULES`]) and the check kinds' P3–P9
/// ([`preflight::NESTED_RULES`]). None of them names `oldSelf`, so the
/// per-field placement that [`seal_spec`] warns about for IMMUTABILITY is the
/// right one here: a rule on `spec.access.evidenceRead` is evaluated exactly
/// when that object exists, which is exactly when it has something to say.
///
/// PANICS on a path the schema does not have, for the reason [`seal_spec`]
/// does: an emitter that silently skipped a rule would render a CRD that
/// passes the drift gate and validates nothing.
pub fn attach_rule(crd: &mut CustomResourceDefinition, path: &[&str], rule: &str, message: &str) {
    assert!(
        !rule.contains("oldSelf"),
        "attach_rule is for non-transition rules; `{rule}` names oldSelf. A transition rule \
         below `.spec` is only sound on a REQUIRED sub-object — use attach_transition_rule, \
         which says so and checks it."
    );
    attach_at(crd, path, rule, message);
}

/// Attach one TRANSITION rule to a REQUIRED sub-object of `.spec`.
///
/// # Why this is sound where a per-field transition rule is not
///
/// [`seal_spec`]'s warning is about OPTIONAL fields: a transition rule is
/// evaluated only when `oldSelf` has the field, so an optional field can be
/// added after creation and the rule never fires. A REQUIRED sub-object is
/// present in every stored object by construction, so `oldSelf` always has it
/// and the rule is evaluated on every update — exactly as an object-level rule
/// is.
///
/// That is the whole reason `TopicDiscovery` and `Preflight` split their specs
/// into a required `spec.request` plus `spec.cancelRequested`: one rule on the
/// required sub-object seals everything inside it, including fields that are
/// themselves optional, and the one field an operator may change sits outside
/// it with a monotonic rule of its own.
///
/// CALLERS MUST KEEP THE SUB-OBJECT REQUIRED.
/// `a_sealed_request_is_a_required_property` reads the emitted schema back and
/// asserts that every path this function is used on appears in its parent's
/// `required` list, so making `request` optional is a red test rather than a
/// seal that silently stops sealing.
///
/// PANICS on a path the schema does not have, like [`attach_rule`].
pub fn attach_transition_rule(
    crd: &mut CustomResourceDefinition,
    path: &[&str],
    rule: &str,
    message: &str,
) {
    assert!(
        !path.is_empty(),
        "a transition rule with an empty path belongs on `.spec` itself, through seal_spec"
    );
    attach_at(crd, path, rule, message);
}

/// Declare a list under `.spec` an ASSOCIATIVE LIST keyed by `keys`.
///
/// # Why this is not decoration
///
/// `x-kubernetes-list-type: map` makes the API server itself refuse two
/// entries with the same key, for free and at no CEL cost. The alternative —
/// `self.keys.all(k, self.keys.filter(n, n.keyId == k.keyId).size() == 1)` —
/// is a quadratic walk over a 64-entry list inside the per-expression cost
/// budget, to enforce something the server already knows how to enforce. It
/// also gives server-side apply a merge key, so two administrators editing
/// different entries of one [`trust_policy::TrustPolicy`] do not clobber each
/// other.
///
/// PANICS on a path that is not a list, for the reason [`seal_spec`] does.
pub fn mark_list_map(crd: &mut CustomResourceDefinition, path: &[&str], keys: &[&str]) {
    let name = crd.metadata.name.clone().unwrap_or_default();
    for version in crd.spec.versions.iter_mut() {
        let node = node_at(&name, version, path);
        assert!(
            node.items.is_some(),
            "{name}: `spec.{}` is not a list, so it cannot be an associative list",
            path.join(".")
        );
        node.x_kubernetes_list_type = Some("map".to_string());
        node.x_kubernetes_list_map_keys = Some(keys.iter().map(|k| (*k).to_string()).collect());
    }
}

/// The shared walk both attach helpers use.
fn attach_at(crd: &mut CustomResourceDefinition, path: &[&str], rule: &str, message: &str) {
    let name = crd.metadata.name.clone().unwrap_or_default();
    for version in crd.spec.versions.iter_mut() {
        node_at(&name, version, path)
            .x_kubernetes_validations
            .get_or_insert_with(Vec::new)
            .push(ValidationRule {
                rule: rule.to_string(),
                message: Some(message.to_string()),
                ..Default::default()
            });
    }
}

/// The schema node at `spec.<path>`, where a path segment of `"[]"` steps into
/// a list's `items`.
///
/// `"[]"` IS THE SPELLING `crd_shape.rs` ALREADY USES when it walks the
/// emitted YAML back out, so a rule's path reads the same in the emitter and
/// in the test that checks the emitter.
fn node_at<'a>(
    name: &str,
    version: &'a mut CustomResourceDefinitionVersion,
    path: &[&str],
) -> &'a mut JSONSchemaProps {
    let mut node = version
        .schema
        .as_mut()
        .and_then(|s| s.open_api_v3_schema.as_mut())
        .and_then(|root| root.properties.as_mut())
        .and_then(|props| props.get_mut("spec"))
        .unwrap_or_else(|| panic!("{name}: version {} has no `spec` schema", version.name));
    for key in path {
        node = if *key == "[]" {
            node.items
                .as_mut()
                .and_then(|items| match items {
                    JSONSchemaPropsOrArray::Schema(s) => Some(&mut **s),
                    JSONSchemaPropsOrArray::Schemas(_) => None,
                })
                .unwrap_or_else(|| {
                    panic!(
                        "{name}: version {} has no `spec.{}` list items",
                        version.name,
                        path.join(".")
                    )
                })
        } else {
            node.properties
                .as_mut()
                .and_then(|props| props.get_mut(*key))
                .unwrap_or_else(|| {
                    panic!(
                        "{name}: version {} has no `spec.{}` schema",
                        version.name,
                        path.join(".")
                    )
                })
        };
    }
    node
}

/// One rendered CRD document.
#[derive(Clone, Debug)]
pub struct Rendered {
    /// The kind, as it appears in [`KINDS`].
    pub kind: &'static str,
    /// The file this document is checked in as, under `config/crd/`.
    pub file_name: &'static str,
    /// The YAML text, header comment included.
    pub yaml: String,
}

/// The header every rendered file carries, so nobody hand-edits one.
const HEADER: &str = "\
# GENERATED FILE — do not edit by hand.
#
# Rendered by `cargo run -p weirkeeper --example emit_crds -- --out config/crd`
# (`just crds`) from `crates/weirkeeper/src/crds/`. The CI `crd drift` arm
# re-renders into a temporary directory and `diff -u`s these files against it,
# so a hand edit here is a red build rather than a silent divergence — the same
# arrangement the scorecard schema has.
#
# Minimum Kubernetes: 1.29 (CEL validation rules GA). See docs/kubernetes.md.
";

/// The schema keys whose value is a JSON NUMBER that `schemars` models as an
/// `f64`.
const NUMERIC_BOUND_KEYS: [&str; 5] = [
    "maximum",
    "minimum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "multipleOf",
];

/// Write an integral numeric bound as an integer: `maximum: 600.0` becomes
/// `maximum: 600`.
///
/// # Why this exists, and why it is not cosmetic
///
/// `JSONSchemaProps::maximum` is an `f64`, so `serde_yaml` writes `600.0`.
/// `kubectl kustomize` — the one renderer of `logweir.yaml` — round-trips the
/// same document through its own YAML library and writes `600`. The two
/// installs Logweir ships would then carry BYTE-DIFFERENT CRDs for the same
/// kind, and `chart_lint_default_render_agrees_with_the_install_file` fails on
/// it. Normalising here means both renderers see the same text, so neither has
/// anything left to normalise.
///
/// JSON has one number type, so `600` and `600.0` are the same value to the
/// API server; this changes the spelling and never the schema. A bound with a
/// real fraction is left exactly as it is.
fn integral_bounds_as_integers(yaml: &str) -> String {
    let mut out = String::with_capacity(yaml.len());
    // BLOCK SCALARS ARE SKIPPED WHOLE (review finding F7). The emitted CRDs
    // carry block-scalar `description`s — thirteen in `backups.yaml` alone —
    // and their content is PROSE, written by whoever wrote a doc comment. A
    // line-based rewrite that did not track them would silently edit a
    // sentence that happened to read `maximum: 600.0` at the start of a line.
    // Cosmetic and deterministic, but a text rewrite reaching into text it
    // does not own is exactly the kind of thing no gate would catch.
    let mut block: Option<usize> = None;
    for line in yaml.lines() {
        let indent = line.len() - line.trim_start().len();
        if let Some(open) = block {
            // A block scalar runs until a line indented no further than the
            // key that opened it. Blank lines belong to it either way.
            if line.trim().is_empty() || indent > open {
                out.push_str(line);
                out.push('\n');
                continue;
            }
            block = None;
        }
        let trimmed = line.trim_start();
        // `key: |`, `key: |-`, `key: >`, `key: >2-` — serde_yaml's block
        // scalar openers, all of which end the line at the indicator.
        if let Some((_, rest)) = trimmed.split_once(": ") {
            let indicator = rest.trim_end();
            if matches!(indicator.chars().next(), Some('|' | '>'))
                && indicator
                    .chars()
                    .skip(1)
                    .all(|c| c.is_ascii_digit() || c == '-' || c == '+')
            {
                block = Some(indent);
                out.push_str(line);
                out.push('\n');
                continue;
            }
        }
        let rewritten = NUMERIC_BOUND_KEYS.iter().find_map(|key| {
            let rest = trimmed.strip_prefix(key)?.strip_prefix(": ")?;
            let digits = rest.strip_suffix(".0")?;
            // `-` is the only sign a bound can carry, and everything else must
            // be a digit: this refuses `1.0e3`, a quoted string and anything
            // that is not a plain decimal.
            let body = digits.strip_prefix('-').unwrap_or(digits);
            (!body.is_empty() && body.bytes().all(|b| b.is_ascii_digit()))
                .then(|| format!("{}{key}: {digits}", &line[..line.len() - trimmed.len()]))
        });
        out.push_str(rewritten.as_deref().unwrap_or(line));
        out.push('\n');
    }
    out
}

/// Render every CRD, sealed, in [`KINDS`] order.
///
/// DETERMINISTIC BY CONSTRUCTION, which is what makes the drift gate a gate.
/// `serde_yaml` writes struct fields in declaration order and
/// `JSONSchemaProps::properties` is a `BTreeMap`, so key order is a function
/// of the types and not of a hash seed; the kind order is [`KINDS`]'s.
pub fn render_all() -> Vec<Rendered> {
    let mut out = Vec::with_capacity(KINDS.len());
    let mut push = |kind: &'static str,
                    file_name: &'static str,
                    mut crd: CustomResourceDefinition,
                    spec_rules: &[SpecRule]| {
        seal_spec(&mut crd, spec_rules);
        let body = integral_bounds_as_integers(
            &serde_yaml::to_string(&crd).expect("a CustomResourceDefinition serialises"),
        );
        out.push(Rendered {
            kind,
            file_name,
            yaml: format!("{HEADER}{body}"),
        });
    };

    push(
        "KafkaCluster",
        "kafkaclusters.yaml",
        {
            let mut crd = kafka_cluster::KafkaCluster::crd();
            for (path, rule, message) in kafka_cluster::CONNECTION_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &WHOLE_SPEC_SEAL,
    );
    push(
        "BackupSchedule",
        "backupschedules.yaml",
        {
            let mut crd = backup_schedule::BackupSchedule::crd();
            // D1 §5.2 R3, THE RETRY NAME BUDGET. The schema root is the one
            // node a rule may read `self.metadata.name` from, which is why a
            // name-length budget cannot live on `.spec` beside R1 and R2.
            attach_root_rule(
                &mut crd,
                backup_schedule::RETRY_NAME_BUDGET_RULE,
                backup_schedule::RETRY_NAME_BUDGET_MESSAGE,
            );
            crd
        },
        &backup_schedule::SPEC_RULES,
    );
    push(
        "Backup",
        "backups.yaml",
        backup::Backup::crd(),
        &backup::SPEC_RULES,
    );
    push(
        "Restore",
        "restores.yaml",
        restore::Restore::crd(),
        &restore::SPEC_RULES,
    );
    push(
        "Approval",
        "approvals.yaml",
        approval::Approval::crd(),
        &WHOLE_SPEC_SEAL,
    );
    push(
        "TrustRoster",
        "trustrosters.yaml",
        trust_roster::TrustRoster::crd(),
        &WHOLE_SPEC_SEAL,
    );
    push(
        "BackupDestination",
        "backupdestinations.yaml",
        {
            let mut crd = backup_destination::BackupDestination::crd();
            attach_root_rule(
                &mut crd,
                backup_destination::R0_NAME_RULE,
                backup_destination::R0_NAME_MESSAGE,
            );
            for (path, rule, message) in backup_destination::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &backup_destination::SPEC_RULES,
    );
    push(
        "TopicDiscovery",
        "topicdiscoveries.yaml",
        {
            let mut crd = topic_discovery::TopicDiscovery::crd();
            let (path, rule, message) = topic_discovery::REQUEST_RULE;
            attach_transition_rule(&mut crd, path, rule, message);
            crd
        },
        &topic_discovery::SPEC_RULES,
    );
    push(
        "Preflight",
        "preflights.yaml",
        {
            let mut crd = preflight::Preflight::crd();
            let (path, rule, message) = preflight::REQUEST_RULE;
            attach_transition_rule(&mut crd, path, rule, message);
            for (path, rule, message) in preflight::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &preflight::SPEC_RULES,
    );
    push(
        "TrustPolicy",
        "trustpolicies.yaml",
        {
            let mut crd = trust_policy::TrustPolicy::crd();
            let (path, keys) = trust_policy::KEYS_LIST_MAP;
            mark_list_map(&mut crd, path, keys);
            for (path, rule, message) in trust_policy::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            for (path, rule, message) in trust_policy::KEY_TRANSITION_RULES {
                attach_transition_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &trust_policy::SPEC_RULES,
    );
    push(
        "ProtectionPolicy",
        "protectionpolicies.yaml",
        {
            let mut crd = protection_policy::ProtectionPolicy::crd();
            for (path, rule, message) in protection_policy::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &protection_policy::SPEC_RULES,
    );
    push(
        "RehearsalSchedule",
        "rehearsalschedules.yaml",
        {
            let mut crd = rehearsal_schedule::RehearsalSchedule::crd();
            for (path, rule, message) in rehearsal_schedule::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &rehearsal_schedule::SPEC_RULES,
    );
    push(
        "RecoveryCatalog",
        "recoverycatalogs.yaml",
        recovery_catalog::RecoveryCatalog::crd(),
        &recovery_catalog::SPEC_RULES,
    );
    push(
        "RetentionPolicy",
        "retentionpolicies.yaml",
        {
            let mut crd = retention_policy::RetentionPolicy::crd();
            for (path, rule, message) in retention_policy::NESTED_RULES {
                attach_rule(&mut crd, path, rule, message);
            }
            crd
        },
        &retention_policy::SPEC_RULES,
    );

    out
}
