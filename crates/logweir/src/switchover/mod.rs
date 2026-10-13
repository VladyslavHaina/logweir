//! **`logweir switchover`** (PROD-04.2; design
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §3): `plan` and
//! `rollback-plan` build the plan an approver signs, offline; `run` is the
//! `Switchover` Job's entry point.
//!
//! # `run`, in order (§3.4)
//!
//! 1. The plan parses and is bounded; the restore plan the Job mounts is the
//!    one the switchover plan binds (`restore.plan_hash`).
//! 2. The approval: its signature under the approver key, the switchover
//!    plan's hash, and the subject kind `Switchover` (an approval for the
//!    `Restore` cannot authorise it: `SubjectKindMismatch`).
//! 3. The restore's evidence: the scorecard's digest is the plan's and its
//!    signature verifies under the mounted evidence keyring; it is a passing
//!    complete verification (`VerificationNotComplete`, naming the field); it
//!    binds a positions mapping (`NoPositionsMapping`) whose digest is the
//!    plan's (`MappingDigestMismatch`) and which holds against it (arms
//!    PMD-1 to PMD-7).
//! 4. Every plan entry is the mapping's (`PlanDisagreesWithMapping`), or, for
//!    a rollback plan, the signed record's.
//! 5. The target: its cluster id is the scorecard's (`TargetClusterChanged`);
//!    every planned topic's ID is the one recorded at creation
//!    (`target.creation`; `TargetGenerationChanged`, `TargetIdentityUnknown`).
//! 6. Each group, independently ([`apply::apply_group`]).
//! 7. A signed switchover record, and the lines the controller lifts.
//!
//! A refusal at 1 to 5 touches no group: every group is `untouched`, the
//! record (when the evidence store could be opened) says why, and the run
//! exits 3. Nothing here ever commits a position outside step 6.

pub mod apply;
pub mod target;

use crate::exit::ExitCode;
use logweir_core::positions_mapping::{self as pm, MappingDocument};
use logweir_core::scorecard::Scorecard;
use logweir_core::switchover::{
    self as so, GroupCounts, GroupRecord, RecordRef, RestoreRef, SwitchoverPlan, SwitchoverRecord,
    TopicChecked,
};
use logweir_engine_oso::storage::{caps, Store, StoreError};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The stdout line one group's outcome is printed on.
pub const GROUP_LINE: &str = "switchover-group=";
/// The stdout line the counts are printed on, second to last.
pub const COUNTS_LINE: &str = "switchover-counts=";
/// The stdout line the record key is printed on.
pub const RECORD_LINE: &str = "switchover-record-key=";
/// The stdout line the run's outcome is printed on.
pub const OUTCOME_LINE: &str = "switchover-outcome=";

/// `logweir switchover run`'s inputs.
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// The approved switchover plan.
    pub plan: PathBuf,
    /// The restore's plan (`Restore.spec.planBytes`): its target and evidence
    /// store.
    pub restore_plan: PathBuf,
    /// The approval document; its DSSE sidecar beside it (`.sig`).
    pub approval: PathBuf,
    pub approver_key: PathBuf,
    /// The evidence-signing keyring the scorecard (and a reversed record) is
    /// verified under.
    pub evidence_keys: PathBuf,
    /// The key the switchover record is signed with.
    pub signing_key: PathBuf,
    /// The restore's scorecard key in the evidence store
    /// (`Restore.status.evidence.scorecardKey`).
    pub scorecard_key: String,
    /// The `Switchover` object, for the record.
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// D2 §3.5's store contract, as `restore run` takes it.
    pub store_contract_version: Option<String>,
}

fn refusal(code: &str, message: impl std::fmt::Display) -> String {
    format!("{code}: {message}")
}

/// One whole-run refusal: the word and the sentence.
#[derive(Debug)]
struct Refused(String);

/// The run's verified inputs, once steps 1 to 4 held.
struct Verified {
    plan: SwitchoverPlan,
    plan_hash: String,
    restore_spec: logweir_core::spec::DrillSpec,
    scorecard: Scorecard,
    /// `target.creation.topics[]`'s `(name, topic_id)` (PROD-11.1a's block,
    /// read from the signed bytes).
    created_ids: BTreeMap<String, Option<String>>,
    approval: so::ApprovalSummary,
}

/// The evidence store the restore wrote to.
fn evidence_store(spec: &logweir_core::spec::DrillSpec, contract: bool) -> Result<Store, String> {
    if contract {
        let archive = crate::backup::store_contract::archive_options().map_err(|e| e.0)?;
        let evidence =
            crate::backup::store_contract::evidence_options(&archive).map_err(|e| e.0)?;
        Store::from_url_with(&spec.evidence, &evidence).map_err(|e| e.to_string())
    } else {
        Store::from_url(&spec.evidence).map_err(|e| e.to_string())
    }
}

/// The document at `key` and its sidecar, verified under `keyring` for
/// `payload_type` and judged for `EvidenceSigning` by the installation's
/// trust.
fn verified_evidence(
    store: &Store,
    key: &str,
    sidecar_key: &str,
    payload_type: &str,
    keyring: &logweir_core::execution_contract::EvidenceKeyring,
) -> Result<Vec<u8>, String> {
    use logweir_core::trust::{decide, EvidenceClaim, IndependentObservation, KeyUsage};
    let read = |k: &str, cap: u64| match store.get_capped(k, cap) {
        Ok((bytes, _)) => Ok(bytes),
        Err(StoreError::NotFound(_)) => Err(format!("{k} is not in the evidence store")),
        Err(e) => Err(format!("{k} could not be read: {e}")),
    };
    let bytes = read(key, caps::SIGNED_DOCUMENT)?;
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&read(sidecar_key, caps::SIDECAR)?)
            .map_err(|e| format!("{sidecar_key} is not a DSSE sidecar: {e}"))?;
    let key_found = keyring.keys.iter().find(|k| {
        logweir_evidence::keys::VerifyingKey::from_pem_str(&k.public_key_pem)
            .ok()
            .is_some_and(|v| {
                logweir_evidence::verify::verify_detached(&v, payload_type, &bytes, &sidecar)
                    .is_ok()
            })
    });
    let Some(k) = key_found else {
        return Err(format!(
            "{key}'s signature does not verify under any of the {} evidence-signing keys this \
             installation trusts",
            keyring.keys.len()
        ));
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let verdict = decide(
        Some(&k.trust),
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::from_document(payload_type, &json),
        &IndependentObservation::none(),
        chrono::Utc::now(),
    );
    if !verdict.may_render_green() {
        return Err(format!(
            "{key} verifies under key {}, and this installation's trust refuses that key for it",
            k.trust.key_id
        ));
    }
    Ok(bytes)
}

/// `target.creation.topics[]` from the scorecard's signed bytes: PROD-11.1a's
/// block (design §5), read through its interface. Absent: no topic has a
/// recorded identity.
#[must_use]
pub fn created_topic_ids(scorecard_bytes: &[u8]) -> BTreeMap<String, Option<String>> {
    let json: serde_json::Value = serde_json::from_slice(scorecard_bytes).unwrap_or_default();
    json.pointer("/target/creation/topics")
        .and_then(serde_json::Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .filter_map(|t| {
                    let name = t.get("name")?.as_str()?.to_string();
                    let id = t
                        .get("topic_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    Some((name, id))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Steps 2's approval check: the v1 document's signature under the approver
/// key, its `plan_hash` over the switchover plan's bytes, and its subject
/// kind.
fn verify_approval(
    plan_bytes: &[u8],
    approval: &Path,
    approver_key: &Path,
) -> Result<so::ApprovalSummary, Refused> {
    let read = |p: &Path| {
        std::fs::read(p).map_err(|e| {
            Refused(refusal(
                "ApprovalUnreadable",
                format!("{}: {e}", p.display()),
            ))
        })
    };
    let bytes = read(approval)?;
    let sidecar_bytes = read(&approval.with_extension("sig"))?;
    let key_bytes = read(approver_key)?;
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&sidecar_bytes).map_err(|_| {
            Refused(refusal(
                "ApprovalUnreadable",
                "the approval sidecar does not parse",
            ))
        })?;
    let key = std::str::from_utf8(&key_bytes)
        .ok()
        .and_then(|pem| logweir_evidence::keys::VerifyingKey::from_pem_str(pem).ok())
        .ok_or_else(|| {
            Refused(refusal(
                "ApprovalUnreadable",
                "the approver key is not a PEM public key",
            ))
        })?;
    logweir_evidence::verify::verify_detached(
        &key,
        crate::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
        &sidecar,
    )
    .map_err(|e| {
        Refused(refusal(
            "ApprovalRefused",
            format!("the approval signature does not verify: {e}"),
        ))
    })?;
    let doc: logweir_core::spec::ApprovalDoc = serde_json::from_slice(&bytes).map_err(|e| {
        Refused(refusal(
            "ApprovalUnreadable",
            format!("not an approval document: {e}"),
        ))
    })?;
    if doc.subject_kind != logweir_core::spec::SUBJECT_KIND_SWITCHOVER {
        return Err(Refused(refusal(
            "SubjectKindMismatch",
            format!(
                "the approval authorises a {}, and this is a Switchover; an approval binds one \
                 kind of subject",
                doc.subject_kind
            ),
        )));
    }
    let want = logweir_core::ids::sha256_prefixed(plan_bytes);
    if doc.plan_hash != want {
        return Err(Refused(refusal(
            "PlanHashMismatch",
            format!(
                "the approval names plan {}, the switchover plan hashes to {want}",
                doc.plan_hash
            ),
        )));
    }
    Ok(so::ApprovalSummary {
        mode: "v1Approval".into(),
        approver: doc.approver,
        requester: None,
        key_id: key.key_id(),
    })
}

/// Steps 1 to 4: everything that is read before the target is dialled.
fn verify_inputs(
    args: &RunArgs,
    store: &Store,
    keyring: &logweir_core::execution_contract::EvidenceKeyring,
    plan_bytes: &[u8],
    restore_text: &str,
) -> Result<Verified, Refused> {
    let plan = SwitchoverPlan::parse(plan_bytes).map_err(Refused)?;
    let restore_hash = logweir_core::ids::sha256_prefixed(restore_text.as_bytes());
    if restore_hash != plan.restore.plan_hash {
        return Err(Refused(refusal(
            so::PLAN_INVALID,
            format!(
                "the restore plan mounted for this run hashes to {restore_hash}, and the \
                 switchover plan binds {}",
                plan.restore.plan_hash
            ),
        )));
    }
    let restore_spec: logweir_core::spec::DrillSpec =
        serde_yaml::from_str(restore_text).map_err(|e| {
            Refused(refusal(
                so::PLAN_INVALID,
                format!("the restore plan does not parse: {e}"),
            ))
        })?;
    let approval = verify_approval(plan_bytes, &args.approval, &args.approver_key)?;

    // 3. The restore's evidence.
    let sidecar_key = args
        .scorecard_key
        .strip_suffix(".json")
        .map(|stem| format!("{stem}.sig"))
        .unwrap_or_else(|| format!("{}.sig", args.scorecard_key));
    let sc_bytes = verified_evidence(
        store,
        &args.scorecard_key,
        &sidecar_key,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        keyring,
    )
    .map_err(|e| Refused(refusal(so::MAPPING_DIGEST_MISMATCH, e)))?;
    let sc_digest = logweir_core::ids::sha256_prefixed(&sc_bytes);
    if sc_digest != plan.restore.scorecard_sha256 {
        return Err(Refused(refusal(
            so::MAPPING_DIGEST_MISMATCH,
            format!(
                "the restore's scorecard hashes to {sc_digest}, and the plan binds {}",
                plan.restore.scorecard_sha256
            ),
        )));
    }
    let scorecard: Scorecard = serde_json::from_slice(&sc_bytes).map_err(|e| {
        Refused(refusal(
            so::MAPPING_DIGEST_MISMATCH,
            format!("the scorecard does not parse: {e}"),
        ))
    })?;
    scorecard
        .validate_invariants()
        .map_err(|e| Refused(refusal(so::MAPPING_DIGEST_MISMATCH, e)))?;
    if scorecard.approval.plan_hash != plan.restore.plan_hash {
        return Err(Refused(refusal(
            so::MAPPING_DIGEST_MISMATCH,
            "the scorecard is not the restore plan's: its approval names another plan hash",
        )));
    }
    if let Some(why) = so::verification_refusal(&scorecard) {
        return Err(Refused(why));
    }
    let v = scorecard.integrity.verification.as_ref();
    let block = v.and_then(|v| v.positions_mapping.as_ref());
    let Some(document) = block.and_then(|b| b.document.as_ref()) else {
        return Err(Refused(refusal(
            so::NO_POSITIONS_MAPPING,
            match block.and_then(|b| b.omitted.as_deref()) {
                Some(o) => format!("the restore wrote no positions mapping ({o})"),
                None => "the restore's scorecard binds no positions mapping: its point captured \
                         no consumer group, or the restore was not bound to a point"
                    .to_string(),
            },
        )));
    };
    if document.sha256 != plan.restore.mapping_sha256 {
        return Err(Refused(refusal(
            so::MAPPING_DIGEST_MISMATCH,
            format!(
                "the scorecard binds mapping {}, and the plan binds {}",
                document.sha256, plan.restore.mapping_sha256
            ),
        )));
    }
    let mapping_bytes = store
        .get_capped(&document.key, pm::MAX_DOCUMENT_BYTES)
        .map(|(b, _)| b)
        .map_err(|e| {
            Refused(refusal(
                so::NO_POSITIONS_MAPPING,
                format!("{}: {e}", document.key),
            ))
        })?;
    let mapping: MappingDocument = serde_json::from_slice(&mapping_bytes).map_err(|e| {
        Refused(refusal(
            so::MAPPING_DIGEST_MISMATCH,
            format!("the mapping does not parse: {e}"),
        ))
    })?;
    let verified: pm::VerifiedPartitions = v
        .and_then(|v| v.complete.as_ref())
        .map(|c| {
            c.partitions
                .iter()
                .map(|p| {
                    (
                        (p.topic.clone(), i64::from(p.partition)),
                        p.target_topic.clone(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if let Some(b) = block {
        pm::validate_document(b, &scorecard.run_id, &verified, &mapping_bytes, &mapping)
            .map_err(|e| Refused(refusal(so::MAPPING_DIGEST_MISMATCH, e)))?;
    }

    // 4. The plan against the mapping, or a rollback plan against its record.
    match &plan.rollback_of {
        None => so::check_against_mapping(&plan, &mapping)
            .map_err(|e| Refused(refusal(so::PLAN_DISAGREES_WITH_MAPPING, e)))?,
        Some(r) => {
            let sidecar = r
                .key
                .strip_suffix(".record.json")
                .map(|s| format!("{s}.record.sig"))
                .unwrap_or_default();
            let bytes = verified_evidence(
                store,
                &r.key,
                &sidecar,
                logweir_evidence::PAYLOAD_TYPE_SWITCHOVER_RECORD,
                keyring,
            )
            .map_err(|e| Refused(refusal(so::PLAN_DISAGREES_WITH_MAPPING, e)))?;
            if logweir_core::ids::sha256_prefixed(&bytes) != r.sha256 {
                return Err(Refused(refusal(
                    so::PLAN_DISAGREES_WITH_MAPPING,
                    "the record the rollback plan names is not the bytes it binds",
                )));
            }
            let record: SwitchoverRecord = serde_json::from_slice(&bytes).map_err(|e| {
                Refused(refusal(
                    so::PLAN_DISAGREES_WITH_MAPPING,
                    format!("the record does not parse: {e}"),
                ))
            })?;
            if record.restore_ref != plan.restore {
                return Err(Refused(refusal(
                    so::PLAN_DISAGREES_WITH_MAPPING,
                    "the record the rollback plan names is about another restore",
                )));
            }
            so::check_against_record(&plan, &record)
                .map_err(|e| Refused(refusal(so::PLAN_DISAGREES_WITH_MAPPING, e)))?;
        }
    }
    Ok(Verified {
        plan_hash: logweir_core::ids::sha256_prefixed(plan_bytes),
        plan,
        restore_spec,
        created_ids: created_topic_ids(&sc_bytes),
        scorecard,
        approval,
    })
}

/// What step 5 read: the cluster id and each planned topic's identities.
type TargetRead = (String, Vec<TopicChecked>);

/// Step 5: the target's cluster and every planned topic's generation.
fn check_target(
    target: &dyn target::SwitchoverTarget,
    v: &Verified,
) -> Result<TargetRead, (Refused, String, Vec<TopicChecked>)> {
    let cluster = target.cluster_id().map_err(|e| {
        (
            Refused(refusal(
                so::TARGET_CLUSTER_CHANGED,
                format!("the target's cluster id could not be read: {e}"),
            )),
            String::new(),
            Vec::new(),
        )
    })?;
    if cluster != v.scorecard.target.cluster_id {
        return Err((
            Refused(refusal(
                so::TARGET_CLUSTER_CHANGED,
                format!(
                    "the target answers cluster id {cluster}, and the restore wrote to {}",
                    v.scorecard.target.cluster_id
                ),
            )),
            cluster,
            Vec::new(),
        ));
    }
    let topics: Vec<String> = v
        .plan
        .groups
        .iter()
        .flat_map(|g| g.topics.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let current = target.topic_ids(&topics).unwrap_or_default();
    let checked: Vec<TopicChecked> = topics
        .iter()
        .map(|t| TopicChecked {
            name: t.clone(),
            created_id: v.created_ids.get(t).cloned().flatten(),
            current_id: current.get(t).and_then(|r| r.clone().ok()),
        })
        .collect();
    for c in &checked {
        let (Some(created), Some(now)) = (&c.created_id, current.get(&c.name)) else {
            return Err((
                Refused(refusal(
                    so::TARGET_IDENTITY_UNKNOWN,
                    format!(
                        "{}: the restore recorded no topic ID at creation (target.creation), so \
                         a recreated topic could not be told from the restored one",
                        c.name
                    ),
                )),
                cluster,
                checked,
            ));
        };
        match now {
            Ok(id) if id == created => {}
            Ok(id) => {
                return Err((
                    Refused(refusal(
                        so::TARGET_GENERATION_CHANGED,
                        format!(
                            "{} was created as {created} and is {id} now: it was deleted and \
                             recreated since the restore",
                            c.name
                        ),
                    )),
                    cluster,
                    checked,
                ))
            }
            Err(why) if why == "NotFound" => {
                return Err((
                    Refused(refusal(
                        so::TARGET_GENERATION_CHANGED,
                        format!("{} no longer exists on the target", c.name),
                    )),
                    cluster,
                    checked,
                ))
            }
            Err(why) => {
                return Err((
                    Refused(refusal(
                        so::TARGET_IDENTITY_UNKNOWN,
                        format!("{}: its topic ID could not be read now ({why})", c.name),
                    )),
                    cluster,
                    checked,
                ))
            }
        }
    }
    Ok((cluster, checked))
}

/// The target reader for the restore plan's target, with the projected
/// credentials `restore run` uses.
fn target_reader(
    spec: &logweir_core::spec::DrillSpec,
) -> Result<logweir_kafka::rdkafka_reader::RdKafkaReader, String> {
    use crate::drill::{naming_the_password_var, validated_password, TARGET_PASSWORD_VAR};
    let ca = crate::tls_ca::projected_ca_file(crate::tls_ca::TARGET_TLS_CA_FILE_VAR)?;
    let cert = crate::tls_ca::projected_client_certificate(crate::tls_ca::Side::Target)?;
    let password = validated_password(TARGET_PASSWORD_VAR).map_err(|e| e.0)?;
    let auth = logweir_kafka::reader::AuthConfig::from_spec(&spec.target.auth, password)
        .map_err(|e| naming_the_password_var(e, TARGET_PASSWORD_VAR).to_string())?
        .with_tls_ca_file(ca)
        .map_err(|e| e.to_string())?
        .with_client_certificate(cert)
        .map_err(|e| e.to_string())?;
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(&spec.target.bootstrap_servers, auth)
        .map_err(|e| e.to_string())
}

/// The record's untouched groups for a whole-run refusal.
fn untouched(plan: Option<&SwitchoverPlan>) -> Vec<GroupRecord> {
    plan.map(|p| {
        p.groups
            .iter()
            .map(|g| GroupRecord {
                source: g.source.clone(),
                target: g.target.clone(),
                classification: "not classified".into(),
                outcome: so::UNTOUCHED.into(),
                reason: None,
                created: false,
                committed: false,
                partitions: Vec::new(),
            })
            .collect()
    })
    .unwrap_or_default()
}

/// The line one group's outcome is printed on: compact JSON the controller
/// reads into `status.groups[]`.
#[must_use]
pub fn group_line(g: &GroupRecord) -> String {
    let partitions: Vec<serde_json::Value> = g
        .partitions
        .iter()
        .map(|p| {
            serde_json::json!({
                "topic": p.topic, "partition": p.partition, "prior": p.prior,
                "planned": p.planned, "applied": p.applied, "readback": p.readback,
            })
        })
        .collect();
    let v = serde_json::json!({
        "source": g.source, "target": g.target, "outcome": g.outcome, "reason": g.reason,
        "created": g.created, "committed": g.committed, "partitions": partitions,
    });
    format!("{GROUP_LINE}{v}")
}

/// **`logweir switchover run`**: the Job's entry point. Exit 0 every group
/// applied or already applied; 2 a result short of that (some groups refused
/// or failed); 3 a whole-run refusal before any group was touched; 1 the
/// run could not do its job; 4 the record could not be signed or written.
pub fn run(args: &RunArgs) -> ExitCode {
    let started_at = chrono::Utc::now();
    let run_id = crate::ids::new_run_id();
    println!("switchover-run-id={run_id}");
    let fail = |code: ExitCode, why: String| {
        eprintln!("{why}");
        println!("{OUTCOME_LINE}{}", so::RUN_FAILED);
        println!(
            "failure-reason={}",
            why.split(':').next().unwrap_or("Operational")
        );
        code
    };
    let plan_bytes = match std::fs::read(&args.plan) {
        Ok(b) => b,
        Err(e) => {
            return fail(
                ExitCode::Operational,
                format!("Operational: {}: {e}", args.plan.display()),
            )
        }
    };
    let restore_text = match std::fs::read_to_string(&args.restore_plan) {
        Ok(t) => t,
        Err(e) => {
            return fail(
                ExitCode::Operational,
                format!("Operational: {}: {e}", args.restore_plan.display()),
            )
        }
    };
    let signer = match crate::signer::ValidatedSigner::load(
        &args.signing_key,
        logweir_evidence::PAYLOAD_TYPE_SWITCHOVER_RECORD,
        b"logweir switchover signing readiness probe v1",
        "No position was committed",
    ) {
        Ok(s) => s,
        Err(e) => return fail(ExitCode::SigningOrLock, format!("SigningKeyNotReady: {e}")),
    };
    let keyring: logweir_core::execution_contract::EvidenceKeyring =
        match std::fs::read(&args.evidence_keys)
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()))
        {
            Ok(k) => k,
            Err(e) => {
                return fail(
                    ExitCode::Operational,
                    format!("Operational: the evidence keyring: {e}"),
                )
            }
        };
    // The evidence store is the restore plan's; a plan that does not parse is
    // refused below, before it is needed for anything but the record.
    let spec_for_store: Option<logweir_core::spec::DrillSpec> =
        serde_yaml::from_str(&restore_text).ok();
    let store = match spec_for_store
        .as_ref()
        .map(|s| evidence_store(s, args.store_contract_version.is_some()))
    {
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            return fail(
                ExitCode::Operational,
                format!("Operational: the evidence store: {e}"),
            )
        }
        None => {
            eprintln!("{}: the restore plan does not parse", so::PLAN_INVALID);
            println!("{OUTCOME_LINE}{}", so::RUN_REFUSED);
            println!("refusal-reason={}", so::PLAN_INVALID);
            return ExitCode::GuardRefused;
        }
    };
    let parsed_plan = SwitchoverPlan::parse(&plan_bytes).ok();
    let mut record = SwitchoverRecord {
        format_version: so::RECORD_FORMAT_VERSION.into(),
        run_id: run_id.clone(),
        switchover: so::ObjectRef {
            namespace: args.namespace.clone(),
            name: args.name.clone(),
            uid: args.uid.clone(),
        },
        plan_hash: logweir_core::ids::sha256_prefixed(&plan_bytes),
        restore_ref: parsed_plan.as_ref().map_or_else(
            || RestoreRef {
                name: String::new(),
                uid: String::new(),
                plan_hash: String::new(),
                scorecard_sha256: String::new(),
                mapping_sha256: String::new(),
            },
            |p| p.restore.clone(),
        ),
        rollback_of: parsed_plan.as_ref().and_then(|p| p.rollback_of.clone()),
        approval: so::ApprovalSummary::default(),
        target_cluster_id: String::new(),
        topics: Vec::new(),
        started_at,
        finished_at: started_at,
        outcome: so::RUN_REFUSED.into(),
        reason: None,
        counts: GroupCounts::default(),
        groups: untouched(parsed_plan.as_ref()),
    };

    let outcome: Result<(), Refused> = (|| {
        let v = verify_inputs(args, &store, &keyring, &plan_bytes, &restore_text)?;
        record.approval = v.approval.clone();
        let reader = target_reader(&v.restore_spec).map_err(|e| {
            Refused(refusal(
                so::TARGET_CLUSTER_CHANGED,
                format!("the target could not be dialled: {e}"),
            ))
        })?;
        let (cluster, checked) = match check_target(&reader, &v) {
            Ok(x) => x,
            Err((r, cluster, checked)) => {
                record.target_cluster_id = cluster;
                record.topics = checked;
                return Err(r);
            }
        };
        record.target_cluster_id = cluster;
        record.topics = checked;
        let topics: Vec<String> = record.topics.iter().map(|t| t.name.clone()).collect();
        let marks = target::SwitchoverTarget::marks(&reader, &topics);
        record.groups = v
            .plan
            .groups
            .iter()
            .map(|g| apply::apply_group(&reader, g, &marks))
            .collect();
        let _ = v.plan_hash;
        Ok(())
    })();
    let refused = outcome.as_ref().err().map(|r| r.0.clone());
    record.reason = refused.clone();
    record.counts = GroupCounts::of(&record.groups);
    record.outcome = if refused.is_some() {
        so::RUN_REFUSED.into()
    } else {
        record.counts.outcome().into()
    };
    record.finished_at = chrono::Utc::now();

    // 7. The signed record, then the lines.
    let key = so::record_key(&run_id);
    let written = record.to_bytes().and_then(|bytes| {
        let sidecar = signer.sign(logweir_evidence::PAYLOAD_TYPE_SWITCHOVER_RECORD, &bytes)?;
        let sig = serde_json::to_vec(&sidecar).map_err(|e| e.to_string())?;
        store
            .put_create_only(&key, &bytes)
            .map_err(|e| e.to_string())?;
        store
            .put_create_only(&so::record_sidecar_key(&run_id), &sig)
            .map_err(|e| e.to_string())?;
        Ok(())
    });
    for g in &record.groups {
        println!("{}", group_line(g));
    }
    if let Err(e) = &written {
        eprintln!("the switchover record could not be signed or written: {e}");
    } else {
        println!("{RECORD_LINE}{key}");
    }
    println!("{OUTCOME_LINE}{}", record.outcome);
    println!("{COUNTS_LINE}{}", record.counts.render());
    if let Some(why) = &refused {
        eprintln!("refused: {why}");
        println!(
            "refusal-reason={}",
            why.split(':').next().unwrap_or(so::PLAN_INVALID)
        );
        return ExitCode::GuardRefused;
    }
    if written.is_err() {
        println!("failure-reason=RecordNotWritten");
        return ExitCode::SigningOrLock;
    }
    if record.outcome == so::SUCCEEDED {
        ExitCode::Ok
    } else {
        ExitCode::DrillNotPass
    }
}

/// `logweir switchover plan`'s inputs.
#[derive(Debug, Clone)]
pub struct PlanArgs {
    /// The restore's signed scorecard (local copy).
    pub scorecard: PathBuf,
    /// The positions-mapping document it binds (local copy).
    pub mapping: PathBuf,
    pub restore_name: String,
    pub restore_uid: String,
    /// `source` or `source=target`, repeatable; none: every mapped group.
    pub groups: Vec<String>,
    pub out: PathBuf,
}

/// **`logweir switchover plan`**: a forward plan from a restore's scorecard
/// and mapping, checked against each other (arms PMD-1 to PMD-7). Prints the
/// notes (unavailable entries, approximate ones, groups active at capture).
///
/// # Errors
///
/// The inputs do not agree, or the plan cannot be built.
pub fn plan(args: &PlanArgs) -> Result<String, String> {
    let sc_bytes =
        std::fs::read(&args.scorecard).map_err(|e| format!("{}: {e}", args.scorecard.display()))?;
    let map_bytes =
        std::fs::read(&args.mapping).map_err(|e| format!("{}: {e}", args.mapping.display()))?;
    let sc: Scorecard =
        serde_json::from_slice(&sc_bytes).map_err(|e| format!("not a scorecard: {e}"))?;
    if let Some(why) = so::verification_refusal(&sc) {
        return Err(why);
    }
    let v = sc.integrity.verification.as_ref();
    let block = v
        .and_then(|v| v.positions_mapping.as_ref())
        .ok_or_else(|| {
            format!(
                "{}: the scorecard binds no positions mapping",
                so::NO_POSITIONS_MAPPING
            )
        })?;
    let mapping: MappingDocument =
        serde_json::from_slice(&map_bytes).map_err(|e| format!("not a positions mapping: {e}"))?;
    let verified: pm::VerifiedPartitions = v
        .and_then(|v| v.complete.as_ref())
        .map(|c| {
            c.partitions
                .iter()
                .map(|p| {
                    (
                        (p.topic.clone(), i64::from(p.partition)),
                        p.target_topic.clone(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    pm::validate_document(block, &sc.run_id, &verified, &map_bytes, &mapping)
        .map_err(|e| format!("{}: {e}", so::MAPPING_DIGEST_MISMATCH))?;
    let chosen: Vec<so::GroupChoice> = args
        .groups
        .iter()
        .map(|g| match g.split_once('=') {
            Some((s, t)) => so::GroupChoice {
                source: s.to_string(),
                target: t.to_string(),
            },
            None => so::GroupChoice {
                source: g.clone(),
                target: g.clone(),
            },
        })
        .collect();
    let restore = RestoreRef {
        name: args.restore_name.clone(),
        uid: args.restore_uid.clone(),
        plan_hash: sc.approval.plan_hash.clone(),
        scorecard_sha256: logweir_core::ids::sha256_prefixed(&sc_bytes),
        mapping_sha256: logweir_core::ids::sha256_prefixed(&map_bytes),
    };
    let (plan, notes) = so::plan_from_mapping(&mapping, restore, &chosen)?;
    write_plan(&plan, &args.out)?;
    Ok(summary(&plan, &notes, &args.out))
}

fn write_plan(plan: &SwitchoverPlan, out: &Path) -> Result<(), String> {
    let yaml = serde_yaml::to_string(plan).map_err(|e| e.to_string())?;
    std::fs::write(out, yaml).map_err(|e| format!("{}: {e}", out.display()))
}

fn summary(plan: &SwitchoverPlan, notes: &[String], out: &Path) -> String {
    let mut s = format!(
        "wrote {} ({} group(s), {} position(s))\n",
        out.display(),
        plan.groups.len(),
        plan.positions()
    );
    for n in notes {
        s.push_str(&format!("  note: {n}\n"));
    }
    s
}

/// `logweir switchover rollback-plan`'s inputs.
#[derive(Debug, Clone)]
pub struct RollbackPlanArgs {
    /// The switchover record (local copy).
    pub record: PathBuf,
    pub out: PathBuf,
}

/// **`logweir switchover rollback-plan`**: `offsets := prior`, `expected :=
/// applied` from a record; the priors that were absent are listed, not
/// planned.
///
/// # Errors
///
/// The record does not parse, or applied nothing with a prior.
pub fn rollback_plan(args: &RollbackPlanArgs) -> Result<String, String> {
    let bytes =
        std::fs::read(&args.record).map_err(|e| format!("{}: {e}", args.record.display()))?;
    let record: SwitchoverRecord =
        serde_json::from_slice(&bytes).map_err(|e| format!("not a switchover record: {e}"))?;
    let rref = RecordRef {
        key: so::record_key(&record.run_id),
        sha256: logweir_core::ids::sha256_prefixed(&bytes),
    };
    let (plan, notes) = so::rollback_plan(&record, rref)?;
    write_plan(&plan, &args.out)?;
    Ok(summary(&plan, &notes, &args.out))
}
