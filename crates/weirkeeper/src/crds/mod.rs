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
pub mod backup_schedule;
pub mod kafka_cluster;
pub mod restore;
pub mod trust_roster;

/// The API group. Global Constraint 14: Logweir owns `logweir.dev`, now, not
/// deferred — and never a vendor's group.
pub const GROUP: &str = "logweir.dev";

/// The one served, stored version. `v1alpha1` for tag 1.
pub const VERSION: &str = "v1alpha1";

/// The six kinds, in the order [`render_all`] emits them.
///
/// EXACTLY SIX. `the_kind_list_is_exactly_six` asserts the emitted set against
/// this list and additionally asserts that no kind is named `Drill`,
/// `RestoreDrill`, `Switchover` or `MetadataSnapshot`, and that no kind
/// contains `Kafka` other than `KafkaCluster`.
pub const KINDS: [&str; 6] = [
    "KafkaCluster",
    "BackupSchedule",
    "Backup",
    "Restore",
    "Approval",
    "TrustRoster",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<Time>,
}

/// The CEL rule that seals a whole `.spec`.
pub const SPEC_IMMUTABLE_RULE: &str = "self == oldSelf";

/// The message the API server returns when [`SPEC_IMMUTABLE_RULE`] refuses an
/// update.
pub const SPEC_IMMUTABLE_MESSAGE: &str = "spec is immutable; create a new object instead";

/// The message paired with an object-level rule this crate does not otherwise
/// know about. Reached only if a later task passes `seal_spec` a rule of its
/// own without pairing a message with it, which is a mistake worth a
/// legible-but-generic message rather than a panic at emit time.
pub const SPEC_PARTIALLY_IMMUTABLE_MESSAGE: &str =
    "this spec field is immutable; create a new object instead";

/// Inject the CEL immutability rule onto `.spec` of every version of `crd`.
///
/// `object_rule` is `None` for the five kinds whose whole `.spec` is sealed,
/// and `Some(rule)` for [`backup_schedule::SUSPEND_ONLY_RULE`] — the one kind
/// with a mutable field. The message travels with the rule rather than as a
/// second parameter because exactly two rules exist in this crate and each has
/// exactly one message; [`message_for`] is that pairing, and
/// `every_spec_is_sealed_and_only_suspend_is_mutable` reads both halves back
/// out of the checked-in YAML.
///
/// THE RULE GOES ON `.spec`, NOT ON `.spec`'s FIELDS. A per-field transition
/// rule is evaluated only when `oldSelf` exists for that field, so an optional
/// field could be ADDED after creation (absent → present) and a per-field
/// `self == oldSelf` would never fire. `optionalOldSelf` closes that and is
/// 1.30+, above the 1.29 floor Global Constraint 25 fixes. An object-level
/// rule is evaluated on every update, which is what makes the
/// `has(self.x) == has(oldSelf.x)` half of
/// [`backup_schedule::SUSPEND_ONLY_RULE`] able to refuse the absent → present
/// transition at all.
///
/// PANICS, DELIBERATELY, rather than skipping. A `CustomResourceDefinition`
/// with no `.spec` property is a programming error in this module, and an
/// emitter that quietly wrote an UNSEALED CRD would produce a file that
/// passes the drift gate and seals nothing. The only caller is
/// [`render_all`].
pub fn seal_spec(crd: &mut CustomResourceDefinition, object_rule: Option<&str>) {
    let rule = object_rule.unwrap_or(SPEC_IMMUTABLE_RULE).to_string();
    let message = message_for(object_rule).to_string();
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
        spec.x_kubernetes_validations = Some(vec![ValidationRule {
            rule: rule.clone(),
            message: Some(message.clone()),
            ..Default::default()
        }]);
    }
}

/// The message that belongs to `object_rule`.
fn message_for(object_rule: Option<&str>) -> &'static str {
    match object_rule {
        None => SPEC_IMMUTABLE_MESSAGE,
        Some(rule) if rule == backup_schedule::SUSPEND_ONLY_RULE => {
            backup_schedule::SUSPEND_ONLY_MESSAGE
        }
        Some(_) => SPEC_PARTIALLY_IMMUTABLE_MESSAGE,
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

/// Render all six CRDs, sealed, in [`KINDS`] order.
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
                    object_rule: Option<&str>| {
        seal_spec(&mut crd, object_rule);
        let body = serde_yaml::to_string(&crd).expect("a CustomResourceDefinition serialises");
        out.push(Rendered {
            kind,
            file_name,
            yaml: format!("{HEADER}{body}"),
        });
    };

    push(
        "KafkaCluster",
        "kafkaclusters.yaml",
        kafka_cluster::KafkaCluster::crd(),
        None,
    );
    push(
        "BackupSchedule",
        "backupschedules.yaml",
        backup_schedule::BackupSchedule::crd(),
        Some(backup_schedule::SUSPEND_ONLY_RULE),
    );
    push("Backup", "backups.yaml", backup::Backup::crd(), None);
    push("Restore", "restores.yaml", restore::Restore::crd(), None);
    push(
        "Approval",
        "approvals.yaml",
        approval::Approval::crd(),
        None,
    );
    push(
        "TrustRoster",
        "trustrosters.yaml",
        trust_roster::TrustRoster::crd(),
        None,
    );

    out
}
