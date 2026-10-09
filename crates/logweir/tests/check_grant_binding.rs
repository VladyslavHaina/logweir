//! FX-20c: `destination.credentialBound` — a destination's *Test access* (and
//! every readiness check that names a destination) compares the binding of
//! EVERY Secret-backed grant it lists, `archiveWrite` included, with no
//! request, and is never READY for a destination a run would refuse.
//!
//! The defect (PoC batch 4, FX-20 F6): `fx20-thief`, whose only grant named
//! `primary`'s `archiveWrite` Secret beside a sentinel endpoint, tested READY
//! — `destination.credentialProjected: Projected`, and
//! `destination.archivePrefixWritable` is execution-only, so nothing compared
//! the binding — while its Backup was refused `CredentialBindingMismatch`.
//!
//! Every row reads the environment through `Wiring::env`, overridden here
//! with a map, so no test mutates the process environment and they may run
//! in parallel.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use logweir::check::kinds::{self, Wiring};
use logweir::check::store::{ObjectAccess, StoreFailure};
use logweir::check::{Deadline, Loaded};
use logweir_core::check_contract::{
    aggregate, CheckCode, CheckId, CheckOperation, CheckOutcome, CheckPlan, CheckRequest,
    CheckResult, CheckState, ConnectionPlan, CredentialMode, DestinationAccessRequest,
    DestinationPlan, Gating, GrantBindingRef, GrantRef, OperationReadinessRequest, OverallState,
    RestorePreflightRequest, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use logweir_core::credential_binding::grant_binding_env;
use logweir_core::destination::{
    Addressing, DestinationLocation, DestinationRole, StorageProvider, TransportSecurity,
};
use logweir_engine_oso::storage::{PutOutcome, StoreError};
use logweir_kafka::inventory::{CheckFailure, InventoryProbe};

const THIEF_BINDING: &str = "v1:thief-uid:sha256:7e1f";
const VICTIM_BINDING: &str = "v1:victim-uid:sha256:91c4";
const VICTIM_SECRET: &str = "lwd-primary-archive-write";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 9, 13, 1, 35).unwrap()
}

fn destination(name: &str, grants: &[(DestinationRole, &str)]) -> DestinationPlan {
    DestinationPlan {
        name: name.to_string(),
        uid: format!("{name}-uid"),
        location: DestinationLocation {
            provider: StorageProvider::S3,
            bucket: "kafka-backups".to_string(),
            prefix: "poc".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://fx20-sentinel.example:9000".to_string()),
            addressing: Addressing::PathStyle,
            transport: TransportSecurity::InsecureHttp,
        },
        location_digest: "sha256:aa".to_string(),
        ca_file: None,
        credentials: CredentialMode::Static,
        grant_bindings: grants
            .iter()
            .map(|(role, secret)| GrantBindingRef {
                role: *role,
                secret_name: (*secret).to_string(),
            })
            .collect(),
    }
}

/// An empty object store that records every operation.
#[derive(Clone, Default)]
struct Empty {
    calls: Arc<Mutex<Vec<String>>>,
}

impl ObjectAccess for Empty {
    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        self.calls.lock().unwrap().push(format!("get {key}"));
        Err(StoreError::NotFound(key.to_string()))
    }
    fn list_page(
        &self,
        prefix: &str,
        _start_after: Option<&str>,
        _max: usize,
    ) -> Result<Vec<String>, StoreError> {
        self.calls.lock().unwrap().push(format!("list {prefix}"));
        Ok(Vec::new())
    }
    fn put_create_only(&self, key: &str, _bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        self.calls.lock().unwrap().push(format!("put {key}"));
        Ok(PutOutcome {
            version_id: None,
            create_only_enforced: true,
        })
    }
    fn qualify(&self, relative_key: &str) -> String {
        relative_key.to_string()
    }
}

/// A wiring whose environment is a map and whose every handle is [`Empty`].
#[derive(Default)]
struct Fake {
    env: BTreeMap<String, String>,
    store: Empty,
    handles: Arc<Mutex<Vec<String>>>,
}

impl Fake {
    /// The pod a controller renders for `grants`: each role's EXPECTED binding
    /// is the destination's, and its PROJECTED binding is whatever the named
    /// Secret carries (`None`: the Secret has no `logweir-binding`).
    fn with(grants: &[(DestinationRole, Option<&str>)], expected: &str) -> Self {
        let mut env = BTreeMap::new();
        for (role, projected) in grants {
            let (p, e) = grant_binding_env(*role);
            env.insert(e.to_string(), expected.to_string());
            if let Some(value) = projected {
                env.insert(p.to_string(), (*value).to_string());
            }
        }
        Self {
            env,
            ..Self::default()
        }
    }

    fn handles(&self) -> Vec<String> {
        self.handles.lock().unwrap().clone()
    }
}

impl Wiring for Fake {
    fn broker(
        &self,
        _plan: &ConnectionPlan,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn InventoryProbe>, CheckFailure> {
        Err(CheckFailure::new(
            CheckCode::BrokerUnreachable,
            "no broker in this test",
        ))
    }
    fn objects(
        &self,
        _plan: &DestinationPlan,
        role: DestinationRole,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        self.handles.lock().unwrap().push(role.as_str().to_string());
        Ok(Box::new(self.store.clone()))
    }
    fn evidence_writer(
        &self,
        _plan: &DestinationPlan,
        _grant: Option<&GrantRef>,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        self.handles
            .lock()
            .unwrap()
            .push("evidenceWriter".to_string());
        Ok(Box::new(self.store.clone()))
    }
    fn evidence_reader(
        &self,
        _plan: &DestinationPlan,
        _grant: Option<&GrantRef>,
        _budget: std::time::Duration,
    ) -> Result<Box<dyn ObjectAccess>, StoreFailure> {
        self.handles
            .lock()
            .unwrap()
            .push("evidenceReader".to_string());
        Ok(Box::new(self.store.clone()))
    }
    fn signer_key_id(&self, _path: &str) -> Result<String, String> {
        Ok("0".repeat(64))
    }
    fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            path.to_string(),
        ))
    }
    fn now(&self) -> DateTime<Utc> {
        now()
    }
    fn env(&self, name: &str) -> Option<String> {
        self.env.get(name).cloned()
    }
}

fn run(request: CheckRequest, wiring: &Fake) -> CheckResult {
    let loaded = Loaded {
        plan: CheckPlan {
            contract: CHECK_PLAN_CONTRACT.to_string(),
            contract_version: CHECK_CONTRACT_VERSION,
            subject_uid: "pf-uid".to_string(),
            timeout_seconds: 120,
            policy_digest: None,
            request,
        },
        plan_sha256: "sha256:00".to_string(),
        subject_uid: "pf-uid".to_string(),
    };
    loaded
        .plan
        .validate()
        .expect("the plan is one a controller renders");
    kinds::run_kind_with(&loaded, Deadline::new(120), wiring).result
}

fn access(dest: DestinationPlan, roles: Vec<DestinationRole>) -> CheckRequest {
    CheckRequest::DestinationAccess(DestinationAccessRequest {
        destination: dest,
        roles,
        write_probe: false,
        evidence_write: None,
        evidence_read: None,
    })
}

fn row(result: &CheckResult, id: CheckId) -> Option<CheckOutcome> {
    result.checks.iter().find(|c| c.id == id).cloned()
}

fn bound_row(result: &CheckResult) -> CheckOutcome {
    row(result, CheckId::DestinationCredentialBound)
        .unwrap_or_else(|| panic!("no destination.credentialBound row: {:?}", result.checks))
}

/// The four values a row must never carry.
fn assert_no_binding_value(row: &CheckOutcome) {
    let text = format!("{} {} {:?}", row.message, row.remedy, row.facts);
    for value in [THIEF_BINDING, VICTIM_BINDING, "victim-uid", "thief-uid"] {
        assert!(
            !text.contains(value),
            "a binding value reached the row: {text}"
        );
    }
}

/// **The F6 thief.** A destination whose ONLY grant is another
/// destination's `archiveWrite` Secret is `notReady`,
/// `CredentialBindingMismatch` on `archiveWrite`, by name — and the check
/// opened no store to find out. CONTROL: the same destination with its own
/// bound Secret is READY.
///
/// KILLS (mutants M1–M3 in the report): skipping the `archiveWrite` grant;
/// not emitting the row (the verdict is READY on `Projected` alone — the
/// runner's half of it: nothing else here is blocking and not ready); a
/// message or fact that drops the grant's name.
#[test]
fn fx20c_a_thief_with_only_a_foreign_archive_write_secret_is_not_ready() {
    let thief = destination(
        "fx20-thief",
        &[(DestinationRole::ArchiveWrite, VICTIM_SECRET)],
    );
    let wiring = Fake::with(
        &[(DestinationRole::ArchiveWrite, Some(VICTIM_BINDING))],
        THIEF_BINDING,
    );
    let result = run(
        access(thief.clone(), vec![DestinationRole::ArchiveWrite]),
        &wiring,
    );
    let bound = bound_row(&result);
    assert_eq!(bound.state, CheckState::NotReady, "{bound:?}");
    assert_eq!(bound.gating, Gating::Blocking);
    assert_eq!(bound.code, CheckCode::CredentialBindingMismatch);
    assert!(
        bound.message.starts_with(
            "`archiveWrite` of destination `fx20-thief` (Secret \
             `lwd-primary-archive-write`: bound to another object or endpoint)"
        ),
        "the refusal names the grant, its destination and its Secret first: {}",
        bound.message
    );
    assert_eq!(
        bound.facts.get("archiveWrite").map(String::as_str),
        Some("CredentialBindingMismatch")
    );
    assert!(bound.remedy.contains("logweir-binding"), "{}", bound.remedy);
    assert_eq!(
        bound.scope.as_ref().map(|s| s.name.as_str()),
        Some("fx20-thief")
    );
    assert_no_binding_value(&bound);
    // The archive-write row itself is still execution-only and never green.
    let write = row(&result, CheckId::DestinationArchivePrefixWritable).expect("the role row");
    assert_eq!(write.gating, Gating::ExecutionOnly);
    assert_eq!(aggregate(&result.checks), OverallState::NotReady);
    // NOTHING WAS DIALLED: no store handle of any kind was built.
    assert!(wiring.handles().is_empty(), "{:?}", wiring.handles());

    // CONTROL: its own Secret, bound to it — READY, and still nothing dialled.
    let own = destination(
        "fx20-thief",
        &[(
            DestinationRole::ArchiveWrite,
            "lwd-fx20-thief-archive-write",
        )],
    );
    let wiring = Fake::with(
        &[(DestinationRole::ArchiveWrite, Some(THIEF_BINDING))],
        THIEF_BINDING,
    );
    let result = run(access(own, vec![DestinationRole::ArchiveWrite]), &wiring);
    let bound = bound_row(&result);
    assert_eq!(bound.state, CheckState::Ready, "{bound:?}");
    assert_eq!(bound.code, CheckCode::CredentialBound);
    assert_eq!(
        bound.facts.get("archiveWrite").map(String::as_str),
        Some("bound")
    );
    assert_no_binding_value(&bound);
    assert_eq!(aggregate(&result.checks), OverallState::Ready);
    assert!(wiring.handles().is_empty());
}

/// **One foreign grant among bound ones is named, and only it.** The probe
/// rows of the bound grants still run and pass; the binding row names
/// `archiveWrite` and not the others, and its facts say which is which.
#[test]
fn fx20c_one_foreign_grant_among_bound_ones_is_the_one_named() {
    let dest = destination(
        "primary",
        &[
            (DestinationRole::ArchiveWrite, VICTIM_SECRET),
            (DestinationRole::ArchiveRead, "lwd-primary-archive-read"),
            (DestinationRole::EvidenceRead, "lwd-primary-evidence-read"),
        ],
    );
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveWrite, Some(VICTIM_BINDING)),
            (DestinationRole::ArchiveRead, Some(THIEF_BINDING)),
            (
                DestinationRole::EvidenceRead,
                // Several bindings, one of them its own: bound.
                Some("v1:other:sha256:01, v1:thief-uid:sha256:7e1f"),
            ),
        ],
        THIEF_BINDING,
    );
    let result = run(
        access(
            dest,
            vec![
                DestinationRole::ArchiveRead,
                DestinationRole::ArchiveWrite,
                DestinationRole::EvidenceRead,
            ],
        ),
        &wiring,
    );
    let bound = bound_row(&result);
    assert_eq!(bound.state, CheckState::NotReady, "{bound:?}");
    assert!(
        bound.message.contains("`archiveWrite`"),
        "{}",
        bound.message
    );
    assert!(
        !bound.message.contains("`archiveRead`"),
        "{}",
        bound.message
    );
    assert!(
        !bound.message.contains("`evidenceRead`"),
        "{}",
        bound.message
    );
    assert!(bound.message.contains(" is not bound to its destination"));
    let facts: BTreeMap<&str, &str> = bound
        .facts
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        facts,
        [
            ("archiveRead", "bound"),
            ("archiveWrite", "CredentialBindingMismatch"),
            ("evidenceRead", "bound"),
        ]
        .into_iter()
        .collect()
    );
    // The bound grants' own rows still answered.
    assert_eq!(
        row(&result, CheckId::DestinationArchiveListable)
            .expect("listing")
            .state,
        CheckState::Ready
    );
    assert_eq!(aggregate(&result.checks), OverallState::NotReady);
}

/// **Absent, foreign and an expectation the pod lost are three refusals,**
/// and two refused grants are both named.
#[test]
fn fx20c_absent_foreign_and_unexpected_are_all_refused_and_all_named() {
    let dest = destination(
        "primary",
        &[
            (DestinationRole::ArchiveWrite, "aw"),
            (DestinationRole::EvidenceWrite, "ew"),
        ],
    );
    // archiveWrite: the Secret has no binding; evidenceWrite: foreign.
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveWrite, None),
            (DestinationRole::EvidenceWrite, Some(VICTIM_BINDING)),
        ],
        THIEF_BINDING,
    );
    let result = run(
        access(dest.clone(), vec![DestinationRole::ArchiveWrite]),
        &wiring,
    );
    let bound = bound_row(&result);
    assert!(
        bound
            .message
            .contains("`archiveWrite` of destination `primary` (Secret `aw`: no binding)")
            && bound.message.contains(
                "`evidenceWrite` of destination `primary` (Secret `ew`: bound to another \
                 object or endpoint)"
            )
            && bound
                .message
                .contains(" are not bound to their destination"),
        "{}",
        bound.message
    );
    assert_no_binding_value(&bound);

    // The pod carries the Secret's binding and NO expectation: refused, never
    // read as "a hand-run process".
    let (projected, _) = grant_binding_env(DestinationRole::ArchiveWrite);
    let mut lost = Fake::default();
    lost.env
        .insert(projected.to_string(), THIEF_BINDING.to_string());
    let result = run(
        access(
            destination("primary", &[(DestinationRole::ArchiveWrite, "aw")]),
            vec![DestinationRole::ArchiveWrite],
        ),
        &lost,
    );
    assert_eq!(bound_row(&result).state, CheckState::NotReady);
}

/// A plan that lists no grant (a workload-identity destination, or a plan an
/// older controller rendered) emits no binding row: `compares_grant_bindings`
/// is false and the controller expects none.
#[test]
fn fx20c_no_listed_grant_is_no_row() {
    let request = access(
        destination("primary", &[]),
        vec![DestinationRole::ArchiveWrite],
    );
    assert!(!request.compares_grant_bindings());
    let result = run(request, &Fake::default());
    assert!(row(&result, CheckId::DestinationCredentialBound).is_none());
}

/// **The class sweep: a Backup's readiness check.** A separated
/// `evidenceWrite` Secret is never probed unless the destination opted in to
/// the marker, so a foreign one passed the Backup preflight while the backup
/// Job refused it. The binding row names it. CONTROL: bound, READY on that
/// row.
#[test]
fn fx20c_a_backup_readiness_check_names_a_foreign_evidence_write_secret() {
    let dest = destination(
        "primary",
        &[
            (DestinationRole::ArchiveWrite, "lwd-primary-archive-write"),
            (DestinationRole::EvidenceWrite, "lwd-other-evidence-write"),
        ],
    );
    let readiness = |dest: DestinationPlan| {
        CheckRequest::OperationReadiness(Box::new(OperationReadinessRequest {
            operation: CheckOperation::Backup,
            connection: ConnectionPlan {
                bootstrap_servers: vec!["broker.example:9092".to_string()],
                auth_mode: "plaintext".to_string(),
                username: None,
                password_env: None,
                tls: Some(false),
                ca_file: None,
                client_cert_file: None,
                client_key_file: None,
                principal: "User:ANONYMOUS".to_string(),
            },
            destination: Some(dest),
            roles: vec![DestinationRole::ArchiveRead, DestinationRole::EvidenceWrite],
            topics: vec!["orders".to_string()],
            signer_path: None,
            write_probe: false,
            evidence_write: None,
            evidence_read: None,
            skip_checks: Vec::new(),
        }))
    };
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveWrite, Some(THIEF_BINDING)),
            (DestinationRole::EvidenceWrite, Some(VICTIM_BINDING)),
        ],
        THIEF_BINDING,
    );
    let result = run(readiness(dest.clone()), &wiring);
    let bound = bound_row(&result);
    assert_eq!(bound.state, CheckState::NotReady, "{bound:?}");
    assert!(bound
        .message
        .starts_with("`evidenceWrite` of destination `primary`"));
    // The marker row is execution-only here: before FX-20c nothing else
    // would have said so.
    assert_eq!(
        row(&result, CheckId::DestinationEvidenceWritable)
            .expect("marker row")
            .code,
        CheckCode::WriteNotProbed
    );
    // CONTROL.
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveWrite, Some(THIEF_BINDING)),
            (DestinationRole::EvidenceWrite, Some(THIEF_BINDING)),
        ],
        THIEF_BINDING,
    );
    let result = run(readiness(dest), &wiring);
    assert_eq!(bound_row(&result).state, CheckState::Ready);
}

/// **The class sweep: a restore (rehearsal) preflight.** The evidence
/// destination's `evidenceWrite` grant is execution-only there (a restore
/// preflight carries no write probe), and the source's `archiveRead` grant is
/// compared even when no archive row runs. The row comes FIRST — it is not a
/// claim about the plan bytes — so a plan that does not parse still reports
/// it. CONTROL: both bound.
#[test]
fn fx20c_a_restore_preflight_names_each_destinations_grant() {
    let restore = || {
        CheckRequest::RestorePreflight(Box::new(RestorePreflightRequest {
            plan_file: "/check/plan.yaml".to_string(),
            plan_sha256: format!("sha256:{}", "0".repeat(64)),
            target: ConnectionPlan {
                bootstrap_servers: vec!["target.example:9092".to_string()],
                auth_mode: "plaintext".to_string(),
                username: None,
                password_env: None,
                tls: Some(false),
                ca_file: None,
                client_cert_file: None,
                client_key_file: None,
                principal: "User:ANONYMOUS".to_string(),
            },
            source_destination: destination(
                "source",
                &[(DestinationRole::ArchiveRead, "lwd-source-archive-read")],
            ),
            evidence_destination: Some(destination(
                "evidence",
                &[(
                    DestinationRole::EvidenceWrite,
                    "lwd-evidence-evidence-write",
                )],
            )),
            backup_id: "bk-1".to_string(),
            manifest_key: "bk-1/manifest.json".to_string(),
            checks: Vec::new(),
            skip_checks: Vec::new(),
        }))
    };
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveRead, Some(THIEF_BINDING)),
            (DestinationRole::EvidenceWrite, Some(VICTIM_BINDING)),
        ],
        THIEF_BINDING,
    );
    let result = run(restore(), &wiring);
    let bound = bound_row(&result);
    assert_eq!(bound.state, CheckState::NotReady, "{bound:?}");
    assert!(
        bound.message.starts_with(
            "`evidenceWrite` of destination `evidence` (Secret \
                 `lwd-evidence-evidence-write`"
        ),
        "{}",
        bound.message
    );
    assert_eq!(
        bound.facts.get("archiveRead").map(String::as_str),
        Some("bound")
    );
    // The plan bytes did not parse (no plan is mounted here); the binding row
    // was reported anyway.
    assert_eq!(
        row(&result, CheckId::PlanParse).expect("plan.parse").state,
        CheckState::NotReady
    );
    // CONTROL.
    let wiring = Fake::with(
        &[
            (DestinationRole::ArchiveRead, Some(THIEF_BINDING)),
            (DestinationRole::EvidenceWrite, Some(THIEF_BINDING)),
        ],
        THIEF_BINDING,
    );
    assert_eq!(bound_row(&run(restore(), &wiring)).state, CheckState::Ready);
}

/// The row is in the runner's catalogue as BLOCKING with the default expiry,
/// so a relayed `notReady` holds the verdict back and a stale `ready` expires.
#[test]
fn fx20c_the_binding_row_is_blocking_and_expires() {
    let (gating, expiry) = logweir::check::catalogue::RUNNER_ROWS
        .iter()
        .find(|(id, _, _)| *id == CheckId::DestinationCredentialBound)
        .map(|(_, g, e)| (*g, *e))
        .expect("the row is catalogued");
    assert_eq!(gating, Gating::Blocking);
    assert_eq!(expiry, Some(logweir::check::catalogue::EXPIRY_DEFAULT));
}

/// **The console fixture is this row.** `ui/tests/fixtures/console/
/// preflight-binding-mismatch.json` is what the product API sends for the F6
/// thief and what the console renders; its `destination.credentialBound`
/// entry is THIS runner's row for that destination, as the controller folds
/// its facts into the message (`entry_of`: `<message> [<k>=<v>; …]`), with
/// this runner's remedy and the destination's scope.
#[test]
fn fx20c_the_console_fixture_is_the_runners_row() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../ui/tests/fixtures/console/preflight-binding-mismatch.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("the fixture")).expect("json");
    let entry = fixture["item"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .find(|c| c["id"] == "destination.credentialBound")
        .expect("the fixture carries the binding row")
        .clone();
    let thief = destination(
        "fx20-thief",
        &[(DestinationRole::ArchiveWrite, VICTIM_SECRET)],
    );
    let wiring = Fake::with(
        &[(DestinationRole::ArchiveWrite, Some(VICTIM_BINDING))],
        THIEF_BINDING,
    );
    let bound = bound_row(&run(
        access(thief, vec![DestinationRole::ArchiveWrite]),
        &wiring,
    ));
    let folded = format!(
        "{} [{}]",
        bound.message,
        bound
            .facts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    );
    assert_eq!(entry["message"].as_str(), Some(folded.as_str()));
    assert_eq!(entry["remedy"].as_str(), Some(bound.remedy.as_str()));
    assert_eq!(entry["code"].as_str(), Some(bound.code.as_str()));
    assert_eq!(entry["state"], "notReady");
    assert_eq!(entry["gating"], "blocking");
    assert_eq!(entry["scope"]["name"], "fx20-thief");
}
