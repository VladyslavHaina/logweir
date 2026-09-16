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
//! to `.spec` as a whole (see [`backup_schedule::SUSPEND_ONLY_RULE`] for why
//! that placement is load-bearing) and one helper that all six kinds pass
//! through is what makes `every_spec_is_sealed_and_only_suspend_is_mutable`
//! checkable at one place.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, ValidationRule,
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
pub mod restore;
pub mod topic_discovery;
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
/// addition: the first six are Amendment A's, and `BackupDestination`,
/// `TopicDiscovery` and `Preflight` are **Amendment F**'s.
/// `the_kind_list_is_exactly_the_adr` asserts the emitted set against this
/// list and additionally asserts that no kind is named `Drill`,
/// `RestoreDrill`, `Switchover` or `MetadataSnapshot`, and that no kind
/// contains `Kafka` other than `KafkaCluster`.
pub const KINDS: [&str; 9] = [
    "KafkaCluster",
    "BackupSchedule",
    "Backup",
    "Restore",
    "Approval",
    "TrustRoster",
    "BackupDestination",
    "TopicDiscovery",
    "Preflight",
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
/// [`backup_schedule::SUSPEND_ONLY_RULE`]'s pair for the one kind with a
/// mutable field, and a kind-specific list for the ones whose `.spec` carries
/// several rules (`backup_destination::SPEC_RULES` is four).
///
/// THE RULES GO ON `.spec`, NOT ON `.spec`'s FIELDS. A per-field transition
/// rule is evaluated only when `oldSelf` exists for that field, so an optional
/// field could be ADDED after creation (absent → present) and a per-field
/// `self == oldSelf` would never fire. `optionalOldSelf` closes that and is
/// 1.30+, above the 1.29 floor Global Constraint 25 fixes. An object-level
/// rule is evaluated on every update, which is what makes the
/// `has(self.x) == has(oldSelf.x)` half of
/// [`backup_schedule::SUSPEND_ONLY_RULE`] able to refuse the absent → present
/// transition at all. The one exception is a rule attached to a REQUIRED
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
        !rules.is_empty(),
        "{name}: a kind with no rule on `.spec` is a kind whose spec is not sealed at all"
    );
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
        spec.x_kubernetes_validations = Some(
            rules
                .iter()
                .map(|r| ValidationRule {
                    rule: r.rule.to_string(),
                    message: Some(r.message.to_string()),
                    ..Default::default()
                })
                .collect(),
        );
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

/// The shared walk both attach helpers use.
fn attach_at(crd: &mut CustomResourceDefinition, path: &[&str], rule: &str, message: &str) {
    let name = crd.metadata.name.clone().unwrap_or_default();
    for version in crd.spec.versions.iter_mut() {
        let mut node = version
            .schema
            .as_mut()
            .and_then(|s| s.open_api_v3_schema.as_mut())
            .and_then(|root| root.properties.as_mut())
            .and_then(|props| props.get_mut("spec"))
            .unwrap_or_else(|| panic!("{name}: version {} has no `spec` schema", version.name));
        for key in path {
            node = node
                .properties
                .as_mut()
                .and_then(|props| props.get_mut(*key))
                .unwrap_or_else(|| {
                    panic!(
                        "{name}: version {} has no `spec.{}` schema",
                        version.name,
                        path.join(".")
                    )
                });
        }
        node.x_kubernetes_validations
            .get_or_insert_with(Vec::new)
            .push(ValidationRule {
                rule: rule.to_string(),
                message: Some(message.to_string()),
                ..Default::default()
            });
    }
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
    for line in yaml.lines() {
        let trimmed = line.trim_start();
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
        backup_schedule::BackupSchedule::crd(),
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

    out
}
