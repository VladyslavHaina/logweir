//! Execution contract **v2**'s two pre-data-plane bindings: the recovery
//! point a plan is bound to (decision D3 §5.5) and the standing rehearsal
//! authorization's signed scope (D3 §4.3).
//!
//! # Not a phase, and that is the point
//!
//! Every check here runs BEFORE `drill::context` — before the rdkafka client
//! is constructed, therefore before a single bootstrap connection is opened,
//! and before any object under `logweir/` is written. The only I/O either
//! check performs is a read of the archive the plan already names, through a
//! read-only handle that `Store::put_create_only` refuses to write through at
//! all. That ordering is what lets these refusals be exit 3 under Global
//! Constraint 11 ("refused by a guard, BEFORE anything runs") rather than a
//! discovery made after a restore had begun.
//!
//! # Why the runner repeats what the controller already proved
//!
//! D3 §4.3 says the standing authorization is "checked twice", and §5.5 says
//! the runner re-verifies the point "before any data-plane work". Both are the
//! same argument PLAT-01.2 makes for the bundle digests: the controller's
//! check is a statement about objects in the API server, and the runner's is a
//! statement about the bytes in this pod. A substitution between the two —
//! projected volume replaced, ConfigMap rewritten, archive object swapped — is
//! invisible to the first check and fatal to the second.

use crate::drill::DrillError;
use logweir_core::execution_contract::{self as wire, PointBinding};
use logweir_core::guard::GuardRefusal;
use logweir_core::spec::{AllowedClusters, DrillSpec};
use logweir_engine_oso::storage::{Store, StoreError};
use logweir_evidence::{
    keys::VerifyingKey, verify::verify_detached, Error as EvidenceError, Sidecar,
};

/// The prose a point-binding refusal opens with.
///
/// `logweir_core::guard::TERMINAL_STATES` is a closed three-element list owned
/// elsewhere, and D3 §5.5 names `PointBindingMismatch` as a state a controller
/// should be able to read off `refusal-reason=`. Until that list grows (it is
/// the status worker's to extend, not this one's) the refusal classifies as
/// the general `GuardRefused` and carries this token in the message, so an
/// operator and a log search can still find it by name.
pub const POINT_BINDING_MISMATCH: &str = "PointBindingMismatch";
/// The same arrangement for D3 §4.3's scope refusal.
pub const REHEARSAL_SCOPE_VIOLATION: &str = "RehearsalScopeViolation";

fn refuse(message: String) -> DrillError {
    DrillError::Guard(GuardRefusal(message))
}

/// Re-verify the recovery point the plan is bound to, against the archive.
///
/// `Ok(None)` when the plan carries no `source.point` — a v1-shaped plan, and
/// the only shape a pre-catalog archive can be restored from. `Ok(Some(id))`
/// names the point this run proved, for the log line.
///
/// # The three answers, and why they are not one code
///
/// * **missing / unreadable** — exit 1. The archive did not answer. That may
///   be a rotated credential, a denied prefix or a bucket that is briefly
///   unavailable, and Global Constraint 11 reserves exit 3 for a refusal of
///   the PLAN. Recording a transient storage failure as "the plan was refused"
///   would tell an operator to change an approved document to fix an outage.
/// * **digest mismatch** — exit 3, the tampered-bundle case exactly: the bytes
///   the archive holds are not the bytes the approver signed a binding to.
///   Never retryable, never a warning.
/// * **agreement** — the run continues, having touched no broker.
pub fn verify_point_binding(
    plan: &DrillSpec,
    archive: &Store,
) -> Result<Option<String>, DrillError> {
    let Some(point) = plan.source.point.as_ref() else {
        return Ok(None);
    };
    check_point_shape(point)?;

    // **The key is BUCKET-ABSOLUTE and is not qualified against the store's
    // prefix.** Every `logweir/` key in this workspace is: `put_create_only`
    // and `get` operate in the fully-qualified space (`Store::qualify`'s own
    // doc comment says so — the prefix is a LISTING root), the backup runner
    // puts the receipt at `logweir/backups/<id>/<run>.receipt.json` with no
    // qualification, and the catalog record carries that same string. Passing
    // it through `qualify` here would produce `<prefix>/logweir/backups/…` and
    // report every present point as missing on any destination whose archive
    // sits under a prefix.
    let qualified = point.receipt_key.clone();
    let receipt_bytes = match archive.get(&qualified) {
        Ok((bytes, _)) => bytes,
        Err(StoreError::NotFound(_)) => {
            return Err(DrillError::Operational(format!(
                "the plan is bound to recovery point {} but its receipt {qualified} is not in \
                 the archive; no data operation was started",
                point.point_id
            )))
        }
        Err(error) => {
            return Err(DrillError::Operational(format!(
                "the plan is bound to recovery point {} and its receipt {qualified} could not be \
                 read: {error}; no data operation was started",
                point.point_id
            )))
        }
    };

    // **The digest first, before the bytes are parsed or believed.** Everything
    // read out of the receipt below is trustworthy only because these bytes are
    // the bytes the approval covers: the plan is signed, the plan names this
    // digest, and this line proves the archive's object hashes to it. Parsing
    // first and comparing afterwards would act on unverified bytes.
    let actual = logweir_core::ids::sha256_prefixed(&receipt_bytes);
    if actual != point.receipt_sha256 {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. The plan is bound to recovery point {} whose receipt must \
             hash to {}, but {qualified} hashes to {actual}. The archive does not hold the point \
             this plan was approved for; no data operation was started.",
            point.point_id, point.receipt_sha256
        )));
    }
    // The identity is content-derived (D3 §5.1), so it is RE-DERIVED here and
    // never taken from the document. A point id that had to be believed would
    // be a label anyone could relabel.
    let derived = crate::catalog::record::point_id(&receipt_bytes);
    if derived != point.point_id {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. The receipt at {qualified} derives recovery point id \
             {derived}, but the plan is bound to {}; no data operation was started.",
            point.point_id
        )));
    }
    let receipt: logweir_core::backup_receipt::BackupReceipt =
        serde_json::from_slice(&receipt_bytes).map_err(|error| {
            // The digest already matched, so these are the approved bytes and
            // the fault is in the PLAN, not in the archive: a binding was
            // approved to an object that is not a backup receipt. Exit 3.
            refuse(format!(
                "{POINT_BINDING_MISMATCH}. The bytes bound as recovery point {} are not a backup \
                 receipt: {error}; no data operation was started.",
                point.point_id
            ))
        })?;
    if receipt.archive.manifest_sha256 != point.manifest_sha256 {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {}'s receipt attests manifest digest {}, \
             but the plan is bound to {}; no data operation was started.",
            point.point_id, receipt.archive.manifest_sha256, point.manifest_sha256
        )));
    }

    // **And the manifest the receipt names, read back.** The three checks
    // above prove the plan and the receipt agree; this one proves the ARCHIVE
    // does. Without it a point whose receipt is intact and whose manifest was
    // replaced or deleted passes every digest comparison and fails at phase 4,
    // after the broker has been contacted — which is the whole class of
    // failure this binding exists to move earlier.
    // `receipt.archive.manifest_key` is likewise the key the backup runner
    // read the manifest back through (`backup::phase_run::run` calls
    // `store.get(&set.manifest_key)` on the value `list_manifests` returned),
    // so it is already in the same space.
    let manifest_key = receipt.archive.manifest_key.clone();
    let manifest_bytes = match archive.get(&manifest_key) {
        Ok((bytes, _)) => bytes,
        Err(StoreError::NotFound(_)) => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s manifest {manifest_key} is not in the archive; no data \
                 operation was started",
                point.point_id
            )))
        }
        Err(error) => {
            return Err(DrillError::Operational(format!(
                "recovery point {}'s manifest {manifest_key} could not be read: {error}; no data \
                 operation was started",
                point.point_id
            )))
        }
    };
    let manifest_digest = logweir_core::ids::sha256_prefixed(&manifest_bytes);
    if manifest_digest != point.manifest_sha256 {
        return Err(refuse(format!(
            "{POINT_BINDING_MISMATCH}. Recovery point {}'s manifest {manifest_key} hashes to \
             {manifest_digest}, not the bound {}; no data operation was started.",
            point.point_id, point.manifest_sha256
        )));
    }
    Ok(Some(point.point_id.clone()))
}

/// Refuse a binding that is malformed on its face, before the archive is
/// touched at all.
///
/// A plan naming an empty receipt key would otherwise `get("")` against a real
/// bucket, and a digest that is not `sha256:<64 hex>` can never match anything
/// this build computes — so both are answered without a round trip, and the
/// message says which field.
fn check_point_shape(point: &PointBinding) -> Result<(), DrillError> {
    let mut faults = Vec::new();
    if !point
        .point_id
        .starts_with(crate::catalog::record::POINT_ID_PREFIX)
        || point.point_id.len() != crate::catalog::record::POINT_ID_PREFIX.len() + 32
        || !point.point_id[crate::catalog::record::POINT_ID_PREFIX.len()..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        faults.push(format!(
            "`source.point.point_id` is {:?}, not `{}` plus 32 lowercase hex characters",
            point.point_id,
            crate::catalog::record::POINT_ID_PREFIX
        ));
    }
    if point.receipt_key.trim().is_empty() {
        faults.push("`source.point.receipt_key` is empty".to_string());
    }
    for (field, value) in [
        ("receipt_sha256", &point.receipt_sha256),
        ("manifest_sha256", &point.manifest_sha256),
    ] {
        let hex = value.strip_prefix("sha256:").unwrap_or_default();
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            faults.push(format!(
                "`source.point.{field}` is {value:?}, not `sha256:` plus 64 hex characters"
            ));
        }
    }
    if faults.is_empty() {
        return Ok(());
    }
    Err(refuse(format!(
        "{POINT_BINDING_MISMATCH}. The plan's recovery point binding is malformed: {}; no data \
         operation was started.",
        faults.join("; ")
    )))
}

/// What [`verify_standing_authorization`] PROVED, returned so the caller does
/// not have to re-parse bytes whose signature has already been checked.
///
/// **The caller needs both halves, and re-deriving either would be a second
/// source of truth.** `crates/logweir/src/drill/mod.rs` mints the run's
/// `Approved` from this: the key id is the identity the signature actually
/// verified under (never the keyring's first entry, never a caller-supplied
/// flag), and the document carries the subject the human signed for — which is
/// what binds `--triggered-by`'s schedule segment to SIGNED bytes rather than
/// to an environment variable the controller sets.
#[derive(Clone, Debug)]
pub struct VerifiedStandingAuthorization {
    /// The `key_id` of the trusted key the DSSE signature verified under, and
    /// whose usage was then judged.
    pub key_id: String,
    /// The signed document, parsed only after the signature over these exact
    /// bytes verified.
    pub document: wire::StandingAuthorization,
}

/// **Verify the SIGNED standing rehearsal authorization, then prove the plan
/// falls inside the scope it carries** — D3 §4.3(d) and (e), the runner's half
/// of "checked twice".
///
/// # Why a signature and not a digest
///
/// The first shape of this check parsed a bare `RehearsalScope` out of bytes
/// pinned by `LOGWEIR_EXECUTION_SCOPE_SHA256`. That digest is set by the same
/// controller that mounts the volume, so against the adversary §4.3's own
/// first sentence names — "a controller that could mint its own
/// authorization" — the check proved nothing: mint a scope that fits the plan,
/// pin its digest, mount it, pass. §4.3(e) says the bundle carries "the
/// authorization document, ITS SIGNATURES and THE TRUSTED PUBLIC KEYS, the
/// scope and the rendered plan", and this function is why that list is what it
/// is.
///
/// # The order, and why each step is where it is
///
/// 1. the keyring, so there is something to anchor in;
/// 2. the sidecar signature over the ENVELOPE BYTES, under a pinned key —
///    before the envelope is parsed, for the same reason
///    [`verify_point_binding`] checks its digest before parsing: everything
///    read out of the document is trustworthy only because these exact bytes
///    were signed;
/// 3. the signing key's USAGE, judged on the key that actually verified. A key
///    carrying only `EvidenceSigning` is refused even though its signature is
///    perfectly good — that is D3 §7.3's key-usage separation, and it is a
///    different fault from a forgery, so it is named differently;
/// 4. the document's own admissibility — kind, subject, the UID binding to
///    THIS schedule, and the validity window
///    (`logweir_core::execution_contract::admit_standing_authorization`, with
///    `now` passed in from the caller because Global Constraint 1 puts the
///    clock in this crate);
/// 5. only then, `plan ∈ scope`.
///
/// # Verification only
///
/// No signing primitive is constructed here and no key material is minted:
/// `verify_detached` and `VerifyingKey::from_pem_str` are the same two calls
/// `phase1_approval::verify_bytes` already makes, so `check-one-signer.sh`'s
/// picture of which crates reach the SIGNING half is unchanged.
///
/// # The clock, honestly
///
/// A Job's node clock is not a trusted time source, and `docs/stability.md`
/// says so. The runner's expiry check is a SECOND line behind the controller's
/// each-slot check (§4.3(c)), not a replacement for it — but it is still worth
/// having: a bundle replayed weeks later against a skew-free node is refused
/// here and nowhere else.
pub fn verify_standing_authorization(
    plan: &DrillSpec,
    allowed: &AllowedClusters,
    document: &[u8],
    sidecar_bytes: &[u8],
    keyring_bytes: &[u8],
    schedule_uid: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<VerifiedStandingAuthorization, DrillError> {
    let keyring: wire::AuthorizationKeyring =
        serde_json::from_slice(keyring_bytes).map_err(|error| {
            // Structural corruption of a mounted member says nothing about
            // whether anyone tried to forge anything — the routing
            // `phase1_approval::verify_bytes` gives a sidecar that will not
            // parse.
            DrillError::Operational(format!(
                "the mounted authorization keyring does not parse: {error}"
            ))
        })?;
    if keyring.keys.is_empty() {
        return Err(refuse(format!(
            "{}. The bundle presents no trusted public key, so a signature over the standing \
             authorization could anchor in nothing; no data operation was started.",
            wire::AUTHORIZATION_INVALID
        )));
    }
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes).map_err(|error| {
        DrillError::Operational(format!(
            "the standing authorization DSSE sidecar does not parse: {error}"
        ))
    })?;

    // EVERY key is tried, and the one that VERIFIED is the one whose usage is
    // judged. Filtering by usage first would turn "a key that may not
    // authorise signed this" — a real and reportable fault — into the
    // indistinguishable "nothing verified".
    let mut verified: Option<&wire::AuthorizationKey> = None;
    let mut unusable: Vec<String> = Vec::new();
    for key in &keyring.keys {
        let parsed = match VerifyingKey::from_pem_str(&key.public_key_pem) {
            Ok(parsed) => parsed,
            Err(error) => {
                unusable.push(format!("{} ({error})", key.key_id));
                continue;
            }
        };
        match verify_detached(
            &parsed,
            wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION,
            document,
            &sidecar,
        ) {
            Ok(_) => {
                verified = Some(key);
                break;
            }
            // The sidecar itself is broken — truncated base64, a DER blob of
            // the wrong length, no signature at all. Operational, per
            // `EvidenceError::Malformed`'s own doc comment.
            Err(EvidenceError::Malformed(message)) => {
                return Err(DrillError::Operational(format!(
                    "standing authorization sidecar signature data is malformed: {message}"
                )))
            }
            // This key did not sign it. Try the next one.
            Err(EvidenceError::Verify(_)) => {}
            Err(EvidenceError::Key(message)) => {
                unusable.push(format!("{} ({message})", key.key_id))
            }
        }
    }

    let Some(key) = verified else {
        return Err(refuse(format!(
            "{}. The standing authorization's signature does not verify under any of the {} \
             trusted public keys the bundle pins{}. The document is not the one a human signed, \
             or it was signed by a key this installation does not trust; no data operation was \
             started.",
            wire::AUTHORIZATION_INVALID,
            keyring.keys.len(),
            if unusable.is_empty() {
                String::new()
            } else {
                format!(" (unusable: {})", unusable.join(", "))
            }
        )));
    };
    // **A GOOD SIGNATURE UNDER THE WRONG KIND OF KEY.** Named as a usage
    // mismatch and never as a signature failure: an operator told "bad
    // signature" about a genuinely signed document goes looking at the wrong
    // thing, which is exactly what `trust::UntrustReason::KeyUsageMismatch`
    // exists to prevent.
    if !key.may_authorize() {
        return Err(refuse(format!(
            "{}. {}: the standing authorization verifies under key {}, whose usages are [{}]. A \
             rehearsal in the current format may be authorised only by a key carrying {}; \
             ConsoleConfirmation requires PLAT-19.2's immutable policy-mode binding, and the \
             installation's own evidence-signing identity never authorises (D3 §7.3); no data \
             operation was started.",
            wire::AUTHORIZATION_INVALID,
            logweir_core::trust::UntrustReason::KeyUsageMismatch.as_str(),
            key.key_id,
            key.usages
                .iter()
                .map(|u| u.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            logweir_core::trust::KeyUsage::GovernedApproval.as_str(),
        )));
    }

    // Only NOW are the bytes read as a document: the signature covers them.
    let doc: wire::StandingAuthorization = serde_json::from_slice(document).map_err(|error| {
        // The signature verified, so these ARE the approved bytes and the
        // fault is in what was signed — the same argument
        // `verify_point_binding` makes for a receipt that will not parse after
        // its digest matched.
        refuse(format!(
            "{}. The signed bytes are not a standing rehearsal authorization: {error}; no data \
             operation was started.",
            wire::AUTHORIZATION_INVALID
        ))
    })?;
    if let Err(refusal) = wire::admit_standing_authorization(&doc, schedule_uid, now) {
        return Err(refuse(format!("{refusal}; no data operation was started.")));
    }

    let facts = wire::plan_scope_facts(plan, allowed);
    if let Err(refusal) = wire::plan_within_scope(&facts, &doc.scope) {
        return Err(refuse(format!(
            "{REHEARSAL_SCOPE_VIOLATION}. {refusal}. The standing authorization key {} signed \
             for RehearsalSchedule {} ({}) does not cover this plan; no data operation was \
             started.",
            key.key_id, doc.subject_ref.name, doc.subject_ref.uid
        )));
    }
    Ok(VerifiedStandingAuthorization {
        key_id: key.key_id.clone(),
        document: doc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::backup_receipt::{
        BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
    };
    use std::collections::BTreeMap;

    fn receipt(manifest_key: &str, manifest_sha256: &str) -> BackupReceipt {
        BackupReceipt {
            format_version: "1.0.0".into(),
            run_id: "run-1".into(),
            backup_id: "nightly-7".into(),
            requested_at: chrono::Utc::now(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            exit_code: 0,
            triggered_by: "manual".into(),
            source: ReceiptSource {
                cluster_id: "SOURCE00000000000000000".into(),
                bootstrap_servers: vec!["source:9092".into()],
                auth: ReceiptAuth {
                    mode: "plaintext".into(),
                    username: None,
                },
                topics: vec!["orders".into()],
            },
            engine: ReceiptEngine {
                id: "oso".into(),
                version: "1".into(),
                digest: "sha256:ee".into(),
            },
            archive: ReceiptArchive {
                manifest_key: manifest_key.into(),
                manifest_sha256: manifest_sha256.into(),
                prefix: "logweir/backups/nightly-7/".into(),
            },
            records: BTreeMap::from([("orders".to_string(), 3u64)]),
            covered: ReceiptCovered {
                from_ms: 1,
                to_ms: 2,
            },
        }
    }

    const RECEIPT_KEY: &str = "logweir/backups/nightly-7/run-1.receipt.json";
    /// **A manifest under `logweir/` is a fixture concession, not a claim
    /// about where a real archive keeps one.** `Store::in_memory` is the
    /// socket-free double this repository's gate requires
    /// (`tests/no_network_in_unit_tests.rs`), and `put_create_only` asserts
    /// Global Constraint 6's root on every key it accepts — so an in-memory
    /// archive cannot hold anything outside it. Nothing in
    /// [`verify_point_binding`] reads the key's shape: it reads whatever
    /// `receipt.archive.manifest_key` names, through the same `qualify`, and
    /// `a_receipt_key_is_qualified_against_the_stores_prefix` pins the one
    /// behaviour the location actually affects.
    const MANIFEST_KEY: &str = "logweir/backups/nightly-7/run-1.manifest.json";

    struct Archive {
        store: Store,
        binding: PointBinding,
    }

    /// One internally consistent recovery point in a socket-free store.
    fn archive_with_a_point(prefix: &str, key_in_plan: &str) -> Archive {
        let store = Store::in_memory(prefix);
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        store
            .put_create_only(RECEIPT_KEY, &receipt_bytes)
            .expect("the receipt is written");
        store
            .put_create_only(MANIFEST_KEY, &manifest)
            .expect("the manifest is written");
        Archive {
            store,
            binding: PointBinding {
                point_id: crate::catalog::record::point_id(&receipt_bytes),
                receipt_key: key_in_plan.into(),
                receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
                manifest_sha256,
            },
        }
    }

    fn archive() -> Archive {
        archive_with_a_point("", RECEIPT_KEY)
    }

    const PLAN_YAML: &str = r#"
source:
  storage: {backend: filesystem, path: /tmp/logweir-binding-fixture}
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: ["target:9092"]
  mode: scratch
  topic_mapping_prefix: "rehearsal-3f2a91c7-"
  marker_topic: logweir.scratch
sample:
  window_start: 2026-01-01T00:00:00Z
  window_end: 2026-01-02T00:00:00Z
  records_per_partition: 25
  max_partitions: 200
objectives: {rto_seconds: 1800, pass_rate: 1.0}
evidence: {backend: filesystem, path: /tmp/logweir-binding-fixture-evidence}
"#;

    fn plan_with(point: Option<PointBinding>) -> DrillSpec {
        let mut plan: DrillSpec = serde_yaml::from_str(PLAN_YAML).expect("the fixture parses");
        plan.source.point = point;
        plan
    }

    #[test]
    fn a_plan_with_no_point_binding_is_the_v1_shape_and_checks_nothing() {
        let a = archive();
        assert_eq!(
            verify_point_binding(&plan_with(None), &a.store).expect("no binding, no check"),
            None
        );
    }

    #[test]
    fn a_point_the_archive_holds_is_proven_and_names_itself() {
        let a = archive();
        let id = a.binding.point_id.clone();
        assert_eq!(
            verify_point_binding(&plan_with(Some(a.binding)), &a.store)
                .expect("the point verifies"),
            Some(id)
        );
    }

    /// A destination whose handle carries a prefix still resolves the SAME
    /// bucket-absolute key. The mutant this kills is a `Store::qualify` added
    /// "for safety": it would rewrite `logweir/backups/…` to
    /// `logweir/logweir/backups/…` and report every present point as missing.
    #[test]
    fn a_receipt_key_is_bucket_absolute_and_is_not_requalified() {
        let a = archive_with_a_point("logweir/", RECEIPT_KEY);
        assert!(
            verify_point_binding(&plan_with(Some(a.binding)), &a.store).is_ok(),
            "a prefixed destination handle must resolve a bucket-absolute receipt key"
        );
    }

    /// THE MUTANT: tolerate a receipt digest that differs. This is the
    /// tampered-bundle case moved to the archive, and it must be exit 3.
    #[test]
    fn a_receipt_digest_that_differs_is_a_refusal_not_a_warning() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.receipt_sha256 = format!("sha256:{}", "0".repeat(64));
        let error = verify_point_binding(&plan_with(Some(binding)), &a.store)
            .expect_err("a tampered point is refused");
        assert!(
            matches!(error, DrillError::Guard(_)),
            "a digest mismatch must be exit 3, got {error}"
        );
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains(POINT_BINDING_MISMATCH),
            "{error}"
        );
    }

    #[test]
    fn a_point_id_that_does_not_derive_from_the_receipt_is_refused() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.point_id = format!("lwp1-{}", "a".repeat(32));
        let error = verify_point_binding(&plan_with(Some(binding)), &a.store)
            .expect_err("a relabelled point is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("derives recovery point id"),
            "{error}"
        );
    }

    #[test]
    fn a_manifest_digest_the_receipt_contradicts_is_refused() {
        let a = archive();
        let mut binding = a.binding.clone();
        binding.manifest_sha256 = format!("sha256:{}", "1".repeat(64));
        let error = verify_point_binding(&plan_with(Some(binding)), &a.store)
            .expect_err("a contradicted manifest digest is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("attests manifest digest"),
            "{error}"
        );
    }

    /// A receipt that is intact while the manifest it names is gone: every
    /// document agrees with every other, and the archive does not.
    #[test]
    fn a_manifest_the_archive_does_not_hold_is_refused() {
        let store = Store::in_memory("");
        let manifest = br#"{"topics":[]}"#.to_vec();
        let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest);
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &manifest_sha256)).expect("serialises");
        store
            .put_create_only(RECEIPT_KEY, &receipt_bytes)
            .expect("the receipt is written");
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            manifest_sha256,
        };
        let error = verify_point_binding(&plan_with(Some(binding)), &store)
            .expect_err("a point whose manifest is gone is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::Operational);
        assert!(
            error.to_string().contains("is not in the archive"),
            "{error}"
        );
    }

    /// **F2's MUTANT: tolerate a manifest whose bytes changed under an intact
    /// receipt.**
    ///
    /// The review planted `if false && manifest_digest != point.manifest_sha256`
    /// and the ENTIRE suite passed: the `NotFound` half of this step was
    /// guarded and the DIGEST half was not. This row kills it — every document
    /// agrees with every other document, and the archive holds something else.
    ///
    /// Seeded by ATTESTING bytes A and STORING bytes B, because `Store` is
    /// create-only by construction (Global Constraint 6) and a test cannot
    /// overwrite a key it already wrote. The effect is identical to a manifest
    /// replaced in place.
    #[test]
    fn a_manifest_whose_bytes_changed_under_an_intact_receipt_is_refused() {
        let store = Store::in_memory("");
        let attested = br#"{"topics":[]}"#.to_vec();
        let attested_sha256 = logweir_core::ids::sha256_prefixed(&attested);
        let substituted = br#"{"topics":["swapped"]}"#.to_vec();
        assert_ne!(
            logweir_core::ids::sha256_prefixed(&substituted),
            attested_sha256
        );
        let receipt_bytes =
            serde_json::to_vec(&receipt(MANIFEST_KEY, &attested_sha256)).expect("serialises");
        store
            .put_create_only(RECEIPT_KEY, &receipt_bytes)
            .expect("the receipt is written");
        store
            .put_create_only(MANIFEST_KEY, &substituted)
            .expect("the substituted manifest is written");
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&receipt_bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&receipt_bytes),
            // The plan and the receipt agree, and both are wrong about the
            // bucket.
            manifest_sha256: attested_sha256.clone(),
        };
        let error = verify_point_binding(&plan_with(Some(binding)), &store)
            .expect_err("a substituted manifest is refused");
        assert_eq!(
            error.exit_code(),
            crate::exit::ExitCode::GuardRefused,
            "the archive does not hold what the approver bound to: {error}"
        );
        let rendered = error.to_string();
        assert!(rendered.contains("hashes to"), "{rendered}");
        assert!(rendered.contains(POINT_BINDING_MISMATCH), "{rendered}");
        assert!(rendered.contains(&attested_sha256), "{rendered}");
    }

    /// A missing point is exit 1, NOT exit 3: the archive did not answer, and
    /// telling an operator to change an approved plan would be wrong.
    #[test]
    fn a_missing_receipt_is_operational_and_not_a_plan_refusal() {
        let store = Store::in_memory("");
        let binding = PointBinding {
            point_id: format!("lwp1-{}", "c".repeat(32)),
            receipt_key: "logweir/backups/gone/run-1.receipt.json".into(),
            receipt_sha256: format!("sha256:{}", "d".repeat(64)),
            manifest_sha256: format!("sha256:{}", "e".repeat(64)),
        };
        let error = verify_point_binding(&plan_with(Some(binding)), &store)
            .expect_err("a missing point is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::Operational);
        assert!(
            error.to_string().contains("is not in the archive"),
            "{error}"
        );
    }

    #[test]
    fn a_malformed_binding_is_refused_before_the_archive_is_touched() {
        let store = Store::in_memory("");
        let binding = PointBinding {
            point_id: "not-a-point".into(),
            receipt_key: "  ".into(),
            receipt_sha256: "nope".into(),
            manifest_sha256: "sha256:short".into(),
        };
        let error = verify_point_binding(&plan_with(Some(binding)), &store)
            .expect_err("a malformed binding is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        for field in [
            "point_id",
            "receipt_key",
            "receipt_sha256",
            "manifest_sha256",
        ] {
            assert!(rendered.contains(field), "{field} unnamed in: {rendered}");
        }
    }

    /// The bytes are the approved ones (the digest matched) and they are not a
    /// receipt: the fault is in the PLAN, so it is exit 3 and not exit 1.
    #[test]
    fn bytes_that_are_not_a_receipt_are_a_plan_refusal() {
        let store = Store::in_memory("");
        let bytes = b"{\"not\":\"a receipt\"}".to_vec();
        store.put_create_only(RECEIPT_KEY, &bytes).expect("written");
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_sha256: format!("sha256:{}", "f".repeat(64)),
        };
        let error = verify_point_binding(&plan_with(Some(binding)), &store)
            .expect_err("non-receipt bytes are refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("are not a backup \nreceipt")
                || error.to_string().contains("are not a backup receipt"),
            "{error}"
        );
    }

    // ---- the SIGNED standing authorization (D3 §4.3(e)) -------------------

    use logweir_core::trust::KeyUsage;
    use logweir_evidence::keys::SigningKey;
    use logweir_evidence::sign::sign_detached;

    fn scope_value(prefix: &str, cluster: &str) -> serde_json::Value {
        serde_json::json!({
            "templateDigest": "sha256:aa",
            "targetClusterId": cluster,
            "topicPrefix": prefix,
            "topics": ["orders"],
            "maxPartitions": 200,
            "recordsPerPartition": 25,
            "deadlineSeconds": 3600,
            "modes": ["scratch"],
        })
    }

    fn document(uid: &str, prefix: &str, cluster: &str, issued_days_ago: i64) -> Vec<u8> {
        let issued = chrono::Utc::now() - chrono::Duration::days(issued_days_ago);
        serde_json::to_vec(&serde_json::json!({
            "formatVersion": "1.0.0",
            "kind": "StandingRehearsalAuthorization",
            "subjectRef": {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "RehearsalSchedule",
                "namespace": "team-a",
                "name": "weekly-orders",
                "uid": uid,
            },
            "scope": scope_value(prefix, cluster),
            "issuedAt": issued.to_rfc3339(),
            "expiresAt": (issued + chrono::Duration::days(30)).to_rfc3339(),
        }))
        .expect("the document serialises")
    }

    /// The three byte-streams D3 §4.3(e) names, as they arrive in the bundle.
    struct Signed {
        document: Vec<u8>,
        sidecar: Vec<u8>,
        keys: Vec<u8>,
    }

    fn keyring(entries: Vec<(&SigningKey, Vec<KeyUsage>)>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "formatVersion": "1.0.0",
            "keys": entries
                .iter()
                .map(|(key, usages)| {
                    serde_json::json!({
                        "keyId": key.key_id(),
                        "publicKeyPem": key
                            .verifying_key()
                            .to_public_key_pem()
                            .expect("pem"),
                        "usages": usages,
                    })
                })
                .collect::<Vec<_>>(),
        }))
        .expect("the keyring serialises")
    }

    /// Signed by a `GovernedApproval` key, for this schedule, covering the
    /// fixture plan.
    ///
    /// The key is generated IN THE TEST. `crates/logweir` is already one of the
    /// three crates `scripts/check-one-signer.sh` permits to name the signing
    /// API, and this is test-only material that exists so the runner's
    /// VERIFICATION path has something real to verify — no production code in
    /// this module signs anything.
    fn signed_with(document: Vec<u8>, usages: Vec<KeyUsage>) -> Signed {
        let approver = SigningKey::generate_ed25519();
        let sidecar = serde_json::to_vec(
            &sign_detached(
                &approver,
                wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION,
                &document,
            )
            .expect("sign"),
        )
        .expect("the sidecar serialises");
        Signed {
            document,
            sidecar,
            keys: keyring(vec![(&approver, usages)]),
        }
    }

    fn signed_authorization() -> Signed {
        signed_with(
            document("uid-1", "rehearsal-3f2a91c7-", "TARGET00000000000000000", 1),
            vec![KeyUsage::GovernedApproval],
        )
    }

    fn allowed(ids: &[&str]) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: ids.iter().map(|s| (*s).to_string()).collect(),
            source_cluster_id: None,
        }
    }

    fn verify(signed: &Signed, uid: Option<&str>) -> Result<(), DrillError> {
        verify_standing_authorization_full(signed, uid).map(|_| ())
    }

    fn verify_standing_authorization_full(
        signed: &Signed,
        uid: Option<&str>,
    ) -> Result<VerifiedStandingAuthorization, DrillError> {
        verify_standing_authorization(
            &plan_with(None),
            &allowed(&["TARGET00000000000000000"]),
            &signed.document,
            &signed.sidecar,
            &signed.keys,
            uid,
            chrono::Utc::now(),
        )
    }

    #[test]
    fn a_validly_signed_authorization_for_this_schedule_and_plan_is_admitted() {
        verify(&signed_authorization(), Some("uid-1"))
            .expect("a signed, current, in-scope authorization is admitted");
    }

    /// **THE MUTANT F1 EXISTS FOR: skip the signature check.**
    ///
    /// This is the adversary D3 §4.3 names in its own first sentence — "a
    /// controller that could mint its own authorization". A controller can
    /// write any bytes it likes and sign them with any key IT holds; what it
    /// does not hold is an approver key. So: a well-formed, current,
    /// plan-fitting document, genuinely signed, by a key the bundle's keyring
    /// does not pin. Before the signature check landed, this passed.
    #[test]
    fn a_document_minted_and_signed_by_a_key_the_bundle_does_not_pin_is_refused() {
        let minted = document("uid-1", "rehearsal-3f2a91c7-", "TARGET00000000000000000", 1);
        let controller = SigningKey::generate_ed25519();
        let approver = SigningKey::generate_ed25519();
        let forged = Signed {
            sidecar: serde_json::to_vec(
                &sign_detached(
                    &controller,
                    wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION,
                    &minted,
                )
                .expect("sign"),
            )
            .expect("serialises"),
            document: minted,
            // The keyring the controller cannot change without the contract
            // digest failing: it pins the human approver, not itself.
            keys: keyring(vec![(&approver, vec![KeyUsage::GovernedApproval])]),
        };
        let error = verify(&forged, Some("uid-1")).expect_err("a minted document is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains(wire::AUTHORIZATION_INVALID),
            "{error}"
        );
        assert!(
            error.to_string().contains("does not verify under any"),
            "{error}"
        );
    }

    /// One byte of the scope changed after signing.
    #[test]
    fn a_tampered_envelope_is_refused() {
        let signed = signed_authorization();
        let tampered = Signed {
            document: String::from_utf8(signed.document.clone())
                .expect("utf-8")
                .replace("rehearsal-3f2a91c7-", "rehearsal-deadbeef-")
                .into_bytes(),
            sidecar: signed.sidecar.clone(),
            keys: signed.keys.clone(),
        };
        let error = verify(&tampered, Some("uid-1")).expect_err("a tampered envelope is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains("does not verify under any"),
            "{error}"
        );
    }

    /// A perfectly good signature, made by a key the bundle does not pin.
    #[test]
    fn a_signature_under_an_unpinned_key_is_refused() {
        let signed = signed_authorization();
        let stranger = SigningKey::generate_ed25519();
        let swapped = Signed {
            document: signed.document.clone(),
            sidecar: signed.sidecar.clone(),
            keys: keyring(vec![(&stranger, vec![KeyUsage::GovernedApproval])]),
        };
        let error = verify(&swapped, Some("uid-1")).expect_err("an unpinned signer is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(error.to_string().contains("trusted public keys"), "{error}");
    }

    /// **Key-usage separation (D3 §7.3).** The signature is genuine and the key
    /// is pinned — and it is the installation's EVIDENCE key, which must never
    /// be able to authorise its own rehearsals. Named as a usage mismatch,
    /// never as a signature failure.
    #[test]
    fn a_signature_under_a_wrong_usage_key_is_refused_by_usage_and_not_by_signature() {
        for usage in [KeyUsage::ConsoleConfirmation, KeyUsage::EvidenceSigning] {
            let signed = signed_with(
                document("uid-1", "rehearsal-3f2a91c7-", "TARGET00000000000000000", 1),
                vec![usage],
            );
            let error = verify(&signed, Some("uid-1"))
                .expect_err("a usage not bound by the current format is refused");
            assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
            let rendered = error.to_string();
            assert!(rendered.contains("KeyUsageMismatch"), "{rendered}");
            assert!(rendered.contains(usage.as_str()), "{rendered}");
            assert!(
                !rendered.contains("does not verify"),
                "a genuinely signed document must not be reported as a bad signature: {rendered}"
            );
        }
    }

    /// A keyring with TWO keys, where the one that signed is not the first.
    ///
    /// Every other fixture here pins exactly one key, which makes "the usage is
    /// judged on the key that VERIFIED" unobservable: with a single entry, the
    /// verifying key and `keys[0]` are the same object and a confused deputy
    /// reading `keys[0]` behaves identically. These two rows are the ones that
    /// tell them apart.
    fn signed_by_second_of_two(
        first_usages: Vec<KeyUsage>,
        signer_usages: Vec<KeyUsage>,
    ) -> Signed {
        let document = document("uid-1", "rehearsal-3f2a91c7-", "TARGET00000000000000000", 1);
        let bystander = SigningKey::generate_ed25519();
        let signer = SigningKey::generate_ed25519();
        let sidecar = serde_json::to_vec(
            &sign_detached(
                &signer,
                wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION,
                &document,
            )
            .expect("sign"),
        )
        .expect("the sidecar serialises");
        Signed {
            document,
            sidecar,
            // ORDER MATTERS: the bystander is first, the signer second.
            keys: keyring(vec![(&bystander, first_usages), (&signer, signer_usages)]),
        }
    }

    /// **N1, half one.** `keys[0]` may not authorise; the key that actually
    /// signed may. The run is ADMITTED, because the usage that matters is the
    /// verifying key's.
    ///
    /// A confused deputy judging `keys[0]` refuses this — a perfectly good
    /// rehearsal blocked by a key that had nothing to do with it, which is the
    /// availability half of the same bug.
    #[test]
    fn the_usage_judged_is_the_verifying_keys_and_not_the_first_in_the_keyring() {
        let signed = signed_by_second_of_two(
            vec![KeyUsage::EvidenceSigning],
            vec![KeyUsage::GovernedApproval],
        );
        verify(&signed, Some("uid-1")).expect(
            "the key that SIGNED carries GovernedApproval; a wrong-usage bystander earlier in \
             the keyring is not what authorises anything",
        );
    }

    /// **N1, half two, and it is the one with teeth.** `keys[0]` may authorise
    /// and the key that actually signed may not.
    ///
    /// A confused deputy judging `keys[0]` ADMITS this: a document signed by
    /// the installation's own evidence key is accepted because some other,
    /// unrelated key in the keyring is allowed to approve rehearsals. That is
    /// precisely the key-usage separation D3 §7.3 exists to enforce, defeated
    /// by list order.
    #[test]
    fn a_wrong_usage_signer_is_refused_even_when_another_keyring_entry_may_authorize() {
        let signed = signed_by_second_of_two(
            vec![KeyUsage::GovernedApproval],
            vec![KeyUsage::EvidenceSigning],
        );
        let error = verify(&signed, Some("uid-1"))
            .expect_err("the key that signed may not authorise, whatever else the keyring holds");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        assert!(rendered.contains("KeyUsageMismatch"), "{rendered}");
        assert!(
            rendered.contains("EvidenceSigning"),
            "the refusal names the usages of the key that SIGNED, not another entry's: {rendered}"
        );
        assert!(
            !rendered.contains("does not verify"),
            "the signature is genuine; this is a usage fault: {rendered}"
        );
    }

    #[test]
    fn an_empty_keyring_anchors_in_nothing_and_is_refused() {
        let signed = signed_authorization();
        let empty = Signed {
            document: signed.document.clone(),
            sidecar: signed.sidecar.clone(),
            keys: serde_json::to_vec(&serde_json::json!({"formatVersion":"1.0.0","keys":[]}))
                .expect("serialises"),
        };
        let error = verify(&empty, Some("uid-1")).expect_err("an empty keyring is refused");
        assert!(
            error.to_string().contains("no trusted public key"),
            "{error}"
        );
    }

    /// A sidecar carrying no signature at all is STRUCTURAL corruption, and
    /// `EvidenceError::Malformed`'s own doc comment says so: evidence that the
    /// file is broken, not that anyone forged anything. Exit 1, the same
    /// routing `phase1_approval::verify_bytes` gives the approval's.
    #[test]
    fn a_sidecar_carrying_no_signature_is_operational() {
        let signed = signed_authorization();
        let empty = Signed {
            document: signed.document.clone(),
            sidecar: serde_json::to_vec(&serde_json::json!({"signatures": []}))
                .expect("serialises"),
            keys: signed.keys.clone(),
        };
        let error = verify(&empty, Some("uid-1")).expect_err("an empty sidecar is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::Operational);
    }

    #[test]
    fn an_expired_authorization_is_refused_by_name() {
        let signed = signed_with(
            // Issued 60 days ago with a 30-day life.
            document(
                "uid-1",
                "rehearsal-3f2a91c7-",
                "TARGET00000000000000000",
                60,
            ),
            vec![KeyUsage::GovernedApproval],
        );
        let error =
            verify(&signed, Some("uid-1")).expect_err("an expired authorization is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error.to_string().contains(wire::AUTHORIZATION_EXPIRED),
            "{error}"
        );
    }

    /// The UID is now a VERIFIED binding: the environment says which schedule
    /// this run claims to be, the SIGNED document says which schedule the human
    /// authorised, and they must agree.
    #[test]
    fn an_authorization_signed_for_another_schedule_is_refused() {
        let signed = signed_with(
            document(
                "uid-other",
                "rehearsal-3f2a91c7-",
                "TARGET00000000000000000",
                1,
            ),
            vec![KeyUsage::GovernedApproval],
        );
        let error = verify(&signed, Some("uid-1")).expect_err("a foreign subject is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(error.to_string().contains("uid-other"), "{error}");
    }

    /// The scope check still runs — on the scope the SIGNATURE covers, not on
    /// one anybody could substitute.
    #[test]
    fn a_signed_authorization_whose_scope_excludes_the_plan_is_refused() {
        let signed = signed_with(
            document("uid-1", "rehearsal-deadbeef-", "TARGET00000000000000000", 1),
            vec![KeyUsage::GovernedApproval],
        );
        let error =
            verify(&signed, Some("uid-1")).expect_err("a plan outside the scope is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        assert!(rendered.contains(REHEARSAL_SCOPE_VIOLATION), "{rendered}");
        assert!(rendered.contains("rehearsal-deadbeef-"), "{rendered}");
        assert!(rendered.contains("weekly-orders"), "{rendered}");
    }

    #[test]
    fn a_signed_authorization_naming_another_cluster_is_refused() {
        let signed = signed_with(
            document("uid-1", "rehearsal-3f2a91c7-", "OTHER000000000000000000", 1),
            vec![KeyUsage::GovernedApproval],
        );
        let error =
            verify(&signed, Some("uid-1")).expect_err("a foreign target cluster is refused");
        assert!(
            error.to_string().contains("OTHER000000000000000000"),
            "{error}"
        );
    }

    #[test]
    fn an_unparseable_keyring_or_sidecar_is_operational_not_a_plan_refusal() {
        let signed = signed_authorization();
        let bad_keys = Signed {
            document: signed.document.clone(),
            sidecar: signed.sidecar.clone(),
            keys: b"not json".to_vec(),
        };
        assert_eq!(
            verify(&bad_keys, Some("uid-1"))
                .expect_err("an unparseable keyring is refused")
                .exit_code(),
            crate::exit::ExitCode::Operational
        );
        let bad_sidecar = Signed {
            document: signed.document.clone(),
            sidecar: b"not json".to_vec(),
            keys: signed.keys.clone(),
        };
        assert_eq!(
            verify(&bad_sidecar, Some("uid-1"))
                .expect_err("an unparseable sidecar is refused")
                .exit_code(),
            crate::exit::ExitCode::Operational
        );
    }

    /// Signed bytes that verify and are not the document this build reads.
    #[test]
    fn signed_bytes_that_are_not_an_authorization_are_a_plan_refusal() {
        let signed = signed_with(
            br#"{"not":"an authorization"}"#.to_vec(),
            vec![KeyUsage::GovernedApproval],
        );
        let error = verify(&signed, Some("uid-1")).expect_err("non-document bytes are refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(
            error
                .to_string()
                .contains("not a standing rehearsal authorization"),
            "{error}"
        );
    }
}
