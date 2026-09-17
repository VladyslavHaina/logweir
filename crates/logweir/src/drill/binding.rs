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
use logweir_core::rehearsal_scope::RehearsalScope;
use logweir_core::spec::{AllowedClusters, DrillSpec};
use logweir_engine_oso::storage::{Store, StoreError};

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

/// Prove the mounted plan falls inside the signed standing rehearsal scope
/// (D3 §4.3(d)), the runner's half of the two checks.
///
/// `scope_bytes` are the digest-verified bundle member;
/// `validate_execution_contract` has already established they are the bytes
/// the controller pinned, so this function's only job is to read them and
/// apply the predicate.
///
/// Pure apart from its arguments: no clock, no network, no store. The scope's
/// `issuedAt`/`expiresAt` live on the enclosing authorization document and are
/// the controller's each-slot check (§4.3(c)) — a runner has no trusted clock
/// to re-decide an expiry with, and pretending otherwise would put a Job's
/// node time in the authorization path.
pub fn verify_standing_scope(
    plan: &DrillSpec,
    allowed: &AllowedClusters,
    scope_bytes: &[u8],
    schedule_uid: Option<&str>,
) -> Result<(), DrillError> {
    let scope: RehearsalScope = serde_json::from_slice(scope_bytes).map_err(|error| {
        refuse(format!(
            "{REHEARSAL_SCOPE_VIOLATION}. The mounted standing rehearsal scope does not parse: \
             {error}; no data operation was started."
        ))
    })?;
    let facts = wire::plan_scope_facts(plan, allowed);
    if let Err(refusal) = wire::plan_within_scope(&facts, &scope) {
        return Err(refuse(format!(
            "{REHEARSAL_SCOPE_VIOLATION}. {refusal}. The standing authorization for \
             RehearsalSchedule {} does not cover this plan; no data operation was started.",
            schedule_uid.unwrap_or("<unnamed>")
        )));
    }
    Ok(())
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

    fn scope_json(prefix: &str, cluster: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "templateDigest": "sha256:aa",
            "targetClusterId": cluster,
            "topicPrefix": prefix,
            "topics": ["orders"],
            "maxPartitions": 200,
            "recordsPerPartition": 25,
            "deadlineSeconds": 3600,
            "modes": ["scratch"],
        }))
        .expect("the scope serialises")
    }

    fn allowed(ids: &[&str]) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: ids.iter().map(|s| (*s).to_string()).collect(),
            source_cluster_id: None,
        }
    }

    #[test]
    fn a_plan_inside_the_signed_scope_passes_the_runners_half() {
        verify_standing_scope(
            &plan_with(None),
            &allowed(&["TARGET00000000000000000"]),
            &scope_json("rehearsal-3f2a91c7-", "TARGET00000000000000000"),
            Some("uid-1"),
        )
        .expect("the plan is inside the signed scope");
    }

    /// THE MUTANT: skip the scope check. A plan whose prefix is not the signed
    /// one must not run, and the refusal must name the schedule and the
    /// mismatch.
    #[test]
    fn a_plan_outside_the_signed_prefix_is_refused_before_any_client() {
        let error = verify_standing_scope(
            &plan_with(None),
            &allowed(&["TARGET00000000000000000"]),
            &scope_json("rehearsal-deadbeef-", "TARGET00000000000000000"),
            Some("uid-1"),
        )
        .expect_err("a plan outside the scope is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        let rendered = error.to_string();
        assert!(rendered.contains(REHEARSAL_SCOPE_VIOLATION), "{rendered}");
        assert!(rendered.contains("rehearsal-deadbeef-"), "{rendered}");
        assert!(rendered.contains("uid-1"), "{rendered}");
    }

    #[test]
    fn a_scope_naming_another_cluster_is_refused() {
        let error = verify_standing_scope(
            &plan_with(None),
            &allowed(&["TARGET00000000000000000"]),
            &scope_json("rehearsal-3f2a91c7-", "OTHER000000000000000000"),
            Some("uid-1"),
        )
        .expect_err("a foreign target cluster is refused");
        assert!(
            error.to_string().contains("OTHER000000000000000000"),
            "{error}"
        );
    }

    #[test]
    fn an_unparseable_scope_is_refused_rather_than_ignored() {
        let error = verify_standing_scope(
            &plan_with(None),
            &allowed(&["TARGET00000000000000000"]),
            b"not json",
            None,
        )
        .expect_err("an unparseable scope is refused");
        assert_eq!(error.exit_code(), crate::exit::ExitCode::GuardRefused);
        assert!(error.to_string().contains("does not parse"), "{error}");
    }
}
