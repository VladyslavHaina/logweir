//! The five plan kinds, and the seam they are driven through.
//!
//! # `Wiring` is why none of this needs a socket to be tested
//!
//! Every kind below takes a `&dyn Wiring`. [`Live`] is the shipped
//! implementation — it dials through [`crate::check::kafka::dial`] and builds
//! handles through [`crate::check::store`] — and a test supplies a fake whose
//! probe and object access are in memory. So the RULES (which check gets which
//! code, which state, which remedy, which expiry, and what reaches a frame)
//! are covered in the default `cargo test` suite, and the only thing that is
//! not is the dial itself, which `crates/logweir/tests/check_cli.rs`'s
//! e2e-gated rows cover against the compose stack.
//!
//! # Which rows a runner emits
//!
//! Exactly the D2 §6.3 rows whose authority contains **J**, minus the two
//! `clusterIdentity` rows whose verdict needs Kubernetes state. See
//! [`crate::check::catalogue`] for the full reasoning; the short version is
//! that a result carrying two answers under one id is worse than a result
//! carrying one answer from the side that can actually establish it.

pub mod access;
pub mod evidence;
pub mod inventory;
pub mod readiness;
pub mod restore;

use std::time::Duration;

use chrono::{DateTime, Utc};
use logweir_core::check_contract::{CheckCode, CheckId, CheckOutcome, CheckRequest, CheckState};
use logweir_core::check_contract::{ConnectionPlan, DestinationPlan};
use logweir_core::destination::DestinationRole;
use logweir_kafka::inventory::{CheckFailure, InventoryProbe};

use super::catalogue;
use super::store::{ObjectAccess, StoreFailure};
use super::{Deadline, Emission, Loaded};

/// Everything a check kind needs from outside itself.
///
/// The three constructors return BOXED trait objects rather than concrete
/// types so a fake can be a different type per call — a readiness check builds
/// an archive handle and an evidence handle for the same destination, and they
/// have to be able to fail differently.
pub trait Wiring {
    /// Build a broker probe for this connection.
    ///
    /// # Errors
    /// [`CheckFailure`] in the closed Kafka vocabulary.
    fn broker(
        &self,
        plan: &ConnectionPlan,
        budget: Duration,
    ) -> Result<Box<dyn InventoryProbe>, CheckFailure>;

    /// Build a READ-ONLY object handle for one role of this destination.
    ///
    /// # Errors
    /// [`StoreFailure`].
    fn objects(
        &self,
        plan: &DestinationPlan,
        role: DestinationRole,
        budget: Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure>;

    /// Build the ONE writable handle a check may hold — the evidence root, for
    /// the create-only marker.
    ///
    /// # Errors
    /// [`StoreFailure`].
    fn evidence_writer(
        &self,
        plan: &DestinationPlan,
        budget: Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure>;

    /// Load the projected signing key and return its PUBLIC key id.
    ///
    /// # Errors
    /// The refusal text, which never contains key material:
    /// `ValidatedSigner::load` names the path and the parse failure.
    fn signer_key_id(&self, path: &str) -> Result<String, String>;

    /// Read a projected file — the restore preflight's verbatim `plan.yaml`.
    ///
    /// # Errors
    /// The io error.
    fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>>;

    /// The observation clock.
    fn now(&self) -> DateTime<Utc>;
}

/// The shipped wiring.
pub struct Live;

impl Wiring for Live {
    fn broker(
        &self,
        plan: &ConnectionPlan,
        budget: Duration,
    ) -> Result<Box<dyn InventoryProbe>, CheckFailure> {
        Ok(Box::new(super::kafka::dial(plan, budget)?))
    }

    fn objects(
        &self,
        plan: &DestinationPlan,
        role: DestinationRole,
        budget: Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        Ok(Box::new(super::store::open_read(plan, role, budget)?))
    }

    fn evidence_writer(
        &self,
        plan: &DestinationPlan,
        budget: Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        Ok(Box::new(super::store::open_evidence_write(plan, budget)?))
    }

    fn signer_key_id(&self, path: &str) -> Result<String, String> {
        // The SAME readiness probe the execution path runs
        // (`crate::signer::ValidatedSigner::load`): parse the PEM, sign a
        // probe with it, and verify that signature with the derived public
        // half. Parsing establishes format, not capability, and D2 §6.3's
        // `SigningKeyInvalid` is about capability.
        let signer = crate::signer::ValidatedSigner::load(
            std::path::Path::new(path),
            SIGNER_PROBE_PAYLOAD_TYPE,
            SIGNER_PROBE,
            "No check result is affected: the check reports the signer as not ready and \
             nothing was signed.",
        )?;
        Ok(signer.verifying_key().key_id())
    }

    fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// The payload type the signer readiness probe signs under.
///
/// A CHECK-SPECIFIC type, not a scorecard's or a receipt's: a probe signature
/// is never written anywhere, and giving it an evidence payload type would put
/// a document type on the wire that no verifier expects.
pub const SIGNER_PROBE_PAYLOAD_TYPE: &str = "application/vnd.logweir.check-signer-probe+octet";

/// The bytes the signer readiness probe signs. Fixed, so the probe is a
/// function of the key alone.
pub const SIGNER_PROBE: &[u8] = b"logweir check signer readiness probe";

/// Whether a code is an "I could not tell" answer rather than a verdict.
///
/// D2 §6.3's Unknown column, for the codes this runner can produce. Kept as a
/// list rather than a default so that a NEW code is `notReady` only when
/// somebody decided it should be: silently defaulting an unclassifiable
/// failure to `notReady` would report an outage as a misconfiguration.
#[must_use]
pub fn is_unknown_code(code: CheckCode) -> bool {
    matches!(
        code,
        CheckCode::MetadataTimeout
            | CheckCode::Timeout
            | CheckCode::BlockedByPrerequisite
            | CheckCode::TopicVisibilityUnknown
            | CheckCode::MappedTopicVisibilityUnknown
            | CheckCode::TopicCreateValidationUnsupported
            | CheckCode::BrokerConfigsNotReadable
            | CheckCode::SegmentListTooLarge
            | CheckCode::WriteNotProbed
            | CheckCode::EvidenceReadNotConfigured
            | CheckCode::ClusterIdentityNotObserved
            | CheckCode::SignerKeyIdNotObserved
    )
}

/// The state a code implies.
#[must_use]
pub fn state_for(code: CheckCode) -> CheckState {
    if is_unknown_code(code) {
        CheckState::Unknown
    } else {
        CheckState::NotReady
    }
}

/// The operator-facing remedy for one code — PLAT-03.1's acceptance
/// ("the UI names each failed prerequisite and **its remedy**").
///
/// A TABLE, not prose at each site, so a code cannot reach a UI with no remedy
/// text; `every_code_the_runner_emits_has_a_remedy` walks the runner's own
/// emission sites and asserts it.
#[must_use]
pub fn remedy_for(code: CheckCode) -> &'static str {
    match code {
        // -- Kafka -------------------------------------------------------
        CheckCode::BrokerUnreachable => {
            "Check that the bootstrap addresses resolve from this namespace and that a \
             NetworkPolicy or mesh policy allows egress to the broker port."
        }
        CheckCode::AuthenticationFailed => {
            "Check the SASL mechanism, the username and the projected password key on the \
             connection's Secret; rotate the credential if it was changed on the broker."
        }
        CheckCode::TlsHandshakeFailed => {
            "The broker refused the TLS handshake. Check that the listener really is TLS and \
             that the client and broker share a protocol version and cipher."
        }
        CheckCode::TlsTrustFailed => {
            "The broker's certificate did not verify. Project the issuing CA as the \
             connection's trust bundle, and check the certificate's SANs cover the bootstrap \
             host names."
        }
        CheckCode::MetadataTimeout => {
            "The broker did not answer a metadata request within the check's budget. Raise the \
             check timeout, or investigate broker load."
        }
        CheckCode::ClusterAuthorizationFailed => {
            "The principal has no cluster-level DESCRIBE. Grant it on the broker."
        }
        CheckCode::TopicAuthorizationFailed | CheckCode::TopicNotAuthorized => {
            "The principal cannot DESCRIBE this topic. Grant DESCRIBE on it, or remove it from \
             the selection."
        }
        CheckCode::UnknownTopicOrPartition | CheckCode::TopicNotFound => {
            "The topic is not on the cluster. Correct the name, or create the topic before the \
             run."
        }
        CheckCode::TopicVisibilityUnknown => {
            "Kafka hides topics this principal cannot describe, so the check could not tell \
             whether the topic exists. Grant DESCRIBE to get a definite answer."
        }
        // -- object store ------------------------------------------------
        CheckCode::AccessDenied => {
            "The principal is authenticated and not authorized. Grant the missing S3 actions \
             for this role on the bucket and prefix."
        }
        CheckCode::InvalidCredentials => {
            "The access key, the secret or the session token is wrong or expired. Rotate the \
             destination's credential Secret."
        }
        CheckCode::BucketNotFound => {
            "The bucket does not exist at this endpoint. Check the bucket name, the endpoint \
             and the region."
        }
        CheckCode::ObjectNotFound => "The object is not there. Check the prefix and the backup id.",
        CheckCode::EndpointUnreachable => {
            "The endpoint did not accept a connection. Check the URL and port, and that egress \
             to it is allowed from this namespace."
        }
        CheckCode::RegionMismatch => {
            "The bucket is in another region. Set the destination's region to the bucket's."
        }
        CheckCode::Timeout => {
            "The object store did not answer within the check's budget. Raise the check \
             timeout, or investigate the endpoint."
        }
        CheckCode::WorkloadIdentityNotInjected => {
            "The destination asks for workload identity and none was injected. Annotate the \
             runner ServiceAccount, or switch the destination to static credentials."
        }
        CheckCode::CaBundleNotFound => {
            "The destination's trust bundle ConfigMap is not projected into the check pod. \
             Check that it exists in this namespace."
        }
        CheckCode::StoreErrorUnclassified => {
            "The object store refused in a way this build does not classify. Check the \
             controller log for the check Job's own stderr."
        }
        // -- signer ------------------------------------------------------
        CheckCode::SigningKeyMissing => {
            "No signing key is mounted. Check the signing Secret's name and key, and the \
             execution context's ServiceAccount."
        }
        CheckCode::SigningKeyUnreadable => {
            "The mounted signing key could not be read. Check the Secret key's file mode and \
             that it holds one PEM document."
        }
        CheckCode::SigningKeyInvalid => {
            "The mounted signing key is not a usable P-256 or Ed25519 PKCS#8 PEM private key. \
             Replace it."
        }
        // -- restore preflight -------------------------------------------
        CheckCode::PlanHashMismatch => {
            "The mounted plan bytes are not the ones this check was pinned to. Re-create the \
             preflight; a plan ConfigMap cannot be edited in place."
        }
        CheckCode::PlanUnparseable => {
            "The mounted plan is not a Logweir restore spec. Re-create the preflight from the \
             draft."
        }
        CheckCode::BackupSetNotFound => {
            "No manifest at this key. Check the recovery point's backup id and the \
             destination's prefix."
        }
        CheckCode::ManifestUnreadable => {
            "The object at the manifest key is not a backup manifest. Check that the prefix \
             points at the archive root this backup set was written to."
        }
        CheckCode::PointInTimeBeforeCoverage => {
            "The requested point in time is older than anything this backup set covers. Pick a \
             later point, or another recovery point."
        }
        CheckCode::PointInTimeAfterCoverage => {
            "The requested point in time is newer than anything this backup set covers. Pick an \
             earlier point, or take a newer backup."
        }
        CheckCode::TopicNotInBackupSet => {
            "A selected topic is not in this backup set. Remove it from the selection, or pick \
             a recovery point that holds it."
        }
        CheckCode::SegmentMissing => {
            "Segments the manifest names are not in the archive. Do not restore from this set \
             until the objects are recovered; the restore would silently produce less data."
        }
        CheckCode::SegmentListTooLarge => {
            "The backup set holds more keys than a preflight will list. The segments were not \
             checked; execution still verifies each one it reads."
        }
        CheckCode::MappedTopicExists => {
            "A mapped target topic already exists. Choose a topic prefix nothing has used, or \
             delete the topics on the target before restoring."
        }
        CheckCode::MappedTopicVisibilityUnknown => {
            "The principal cannot DESCRIBE a mapped target name, so the check could not tell \
             whether it exists. Grant DESCRIBE on the target."
        }
        CheckCode::TopicCreateNotAuthorized => {
            "The principal cannot CREATE topics on the target. Grant CREATE on the topic \
             prefix, or on the cluster."
        }
        CheckCode::TopicConfigRejected => {
            "The target broker refused the topic configuration Logweir pins. Check the \
             broker's topic config policy."
        }
        CheckCode::ReplicationFactorExceedsBrokers => {
            "The requested replication factor is higher than the target's broker count. Lower \
             it, or add brokers."
        }
        CheckCode::TopicCreateValidationUnsupported => {
            "The target did not answer a validate-only CreateTopics. Collisions were checked by \
             metadata alone."
        }
        CheckCode::TimestampBoundExceeded => {
            "The target broker bounds how far in the past a record timestamp may be, and this \
             plan's window end is outside it. Raise \
             log.message.timestamp.before.max.ms on the target, or restore a more recent \
             window."
        }
        CheckCode::BrokerConfigsNotReadable => {
            "The principal cannot DESCRIBE broker configs, so the timestamp bound was not \
             checked. Grant DESCRIBE on the cluster."
        }
        CheckCode::MarkerTopicMissing => {
            "The scratch target's marker topic does not exist. Create it, or point the restore \
             at a cluster that carries one."
        }
        CheckCode::MarkerTopicErrored => {
            "The scratch target's marker topic reported a metadata error. Investigate the \
             target before restoring into it."
        }
        // -- framework ---------------------------------------------------
        CheckCode::BlockedByPrerequisite => {
            "A prerequisite of this check did not pass; fix the cause reported above it."
        }
        CheckCode::ClusterIdentityNotObserved => {
            "The broker named no cluster id, so its identity could not be compared. Check that \
             the connection reaches the cluster it names."
        }
        CheckCode::SignerKeyIdNotObserved => {
            "No signing key id was observed, so the roster check could not run. Fix the signer \
             prerequisite reported beside it."
        }
        CheckCode::WriteNotProbed => {
            "This destination has no create-only write probe configured, so the grant is \
             verified only when a run executes."
        }
        CheckCode::EvidenceReadNotConfigured => {
            "This destination configures no evidence-read grant, so evidence reads were not \
             checked."
        }
        CheckCode::ReadVerifiedOnlyAtExecution
        | CheckCode::ArchivePrefixWriteVerifiedOnlyAtExecution
        | CheckCode::LogAppendTimeOverrideVerifiedOnlyAtExecution => {
            "This permission can only be verified by the run itself; the run's own guards \
             remain authoritative."
        }
        _ => "",
    }
}

/// One outcome from a Kafka failure.
#[must_use]
pub fn from_broker_failure(id: CheckId, f: &CheckFailure, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(id, state_for(f.code), f.code, now)
        // `CheckFailure::new` has already redacted and capped this; passing it
        // through `with_message` redacts it again, which is idempotent and is
        // what keeps the chokepoint claim true for every path.
        .with_message(&f.message)
        .with_remedy(remedy_for(f.code))
}

/// One outcome from an object-store failure.
#[must_use]
pub fn from_store_failure(id: CheckId, f: &StoreFailure, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(id, state_for(f.code), f.code, now)
        .with_message(&f.message)
        .with_remedy(remedy_for(f.code))
}

/// A ready outcome.
#[must_use]
pub fn ready(id: CheckId, code: CheckCode, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(id, CheckState::Ready, code, now)
}

/// An execution-only row: always `unknown`, always excluded from aggregation.
#[must_use]
pub fn execution_only(id: CheckId, code: CheckCode, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(id, CheckState::Unknown, code, now).with_remedy(remedy_for(code))
}

/// An outcome for a store failure whose code was already classified and whose
/// message this module composed.
///
/// The message NEVER carries the backend's own text — D2 §4.2: "Raw errors are
/// never printed, only codes plus a redacted message." Every caller passes a
/// sentence built from the operation, the destination and the key.
#[must_use]
pub fn catalogue_outcome_for_store(
    id: CheckId,
    code: CheckCode,
    message: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    catalogue::outcome(id, state_for(code), code, now)
        .with_message(&format!("{message}: {code}"))
        .with_remedy(remedy_for(code))
}

/// `runner.contract` — Ready, always, and that is the point.
///
/// Reaching the code that builds a result IS the proof that this image
/// implements the contract version the plan names: the plan was parsed with
/// `deny_unknown_fields` at exactly [`CHECK_CONTRACT_VERSION`], and a plan
/// this build did not understand was refused in step 4 with exit 3. The
/// NEGATIVE answer, `RunnerContractUnsupported`, cannot be produced here at
/// all — an image without the `check` subcommand exits before `main` dispatches
/// — and D2 §4.3 has the controller derive it from that exit instead.
#[must_use]
pub fn runner_contract(now: DateTime<Utc>) -> CheckOutcome {
    ready(CheckId::RunnerContract, CheckCode::ContractSupported, now).with_message(&format!(
        "this runner implements check contract version {}",
        logweir_core::check_contract::CHECK_CONTRACT_VERSION
    ))
}

/// Dispatch one verified plan to its kind.
#[must_use]
pub fn run_kind(loaded: &Loaded, deadline: Deadline) -> Emission {
    run_kind_with(loaded, deadline, &Live)
}

/// The same, over a supplied wiring — the seam every unit row drives.
#[must_use]
pub fn run_kind_with(loaded: &Loaded, deadline: Deadline, wiring: &dyn Wiring) -> Emission {
    match &loaded.plan.request {
        CheckRequest::TopicInventory(r) => inventory::run(r, wiring, deadline),
        CheckRequest::OperationReadiness(r) => readiness::run(r, wiring, deadline),
        CheckRequest::RestorePreflight(r) => restore::run(r, wiring, deadline),
        CheckRequest::DestinationAccess(r) => access::run(r, wiring, deadline),
        CheckRequest::EvidenceFetch(r) => evidence::run(r, wiring, deadline),
    }
}
