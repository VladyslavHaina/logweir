//! PLAT-15.2 — a restore readiness check about a CATALOG point (D3 §5.5
//! step 5).
//!
//! The controller re-reads the catalog row when the check runs and reports
//! `recoveryPoint.state` from it. Every row below has a negative control: the
//! same fixture with the one fact that decides the answer changed, which must
//! flip the answer. A row whose control stayed green would prove nothing.

use chrono::{DateTime, Duration, TimeZone, Utc};
use k8s_openapi::api::core::v1::ConfigMap;
use logweir_core::check_contract::{CheckCode, CheckId, CheckOutcome, CheckState};
use serde_json::{json, Value};

use weirkeeper::catalog_view::ControllerRefusals;
use weirkeeper::controllers::preflight::{
    catalog_point_facts, catalog_point_row, CatalogPointFacts, CatalogPointRead, PlanFacts,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::recovery_catalog::RecoveryCatalog;

const POINT: &str = "lwp1-0123456789abcdef0123456789abcdef";
const OTHER_POINT: &str = "lwp1-ffffffffffffffffffffffffffffffff";
const SET: &str = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260922-140000";
const RECEIPT_KEY: &str =
    "logweir/backups/3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260922-140000/01JB7Z00000000000000000000.receipt.json";

fn receipt_sha() -> String {
    format!("sha256:{}", "a1".repeat(32))
}

fn manifest_sha() -> String {
    format!("sha256:{}", "b2".repeat(32))
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 22, 15, 0, 0).unwrap()
}

fn entry(point_id: &str, selectable: bool) -> Value {
    json!({
        "pointId": point_id,
        "backupId": SET,
        "runId": "01JB7Z00000000000000000000",
        "recoveryPointAtMs": 1_758_549_600_000_i64,
        "coveredFromMs": 1_758_546_000_000_i64,
        "coveredToMs": 1_758_549_600_000_i64,
        "locations": [{"locationId": "s3://lw-p152/archive", "availability": "Available"}],
        "receiptKey": RECEIPT_KEY,
        "receiptSha256": receipt_sha(),
        "manifestKey": format!("{SET}/manifest.json"),
        "manifestSha256": manifest_sha(),
        "availability": "Available",
        "verification": if selectable { "Verified" } else { "UntrustedSigner" },
        "signerKeyId": "c".repeat(64),
        "selectable": selectable,
    })
}

/// One page: `(name, ConfigMap, recorded digest)` over the given entries.
fn page(name: &str, entries: &[Value], immutable: bool) -> (String, ConfigMap, String) {
    let lines: Vec<String> = entries.iter().map(Value::to_string).collect();
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let digest = weirkeeper::catalog_view::page_digest(&refs);
    let mut body = String::new();
    for line in &lines {
        body.push_str(line);
        body.push('\n');
    }
    let map: ConfigMap = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": "lw-p152"},
        "immutable": immutable,
        "data": {(weirkeeper::catalog_view::PAGE_DATA_KEY): body},
    }))
    .expect("a ConfigMap");
    (name.to_string(), map, format!("sha256:{digest}"))
}

fn catalog(pages: &[(String, ConfigMap, String)], expires: DateTime<Utc>) -> RecoveryCatalog {
    let rows: Vec<Value> = pages
        .iter()
        .enumerate()
        .map(|(i, (name, _, digest))| {
            json!({"configMapName": name, "index": i, "count": 1, "sha256": digest})
        })
        .collect();
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "RecoveryCatalog",
        "metadata": {"name": "archive", "namespace": "lw-p152", "uid": "cat-uid-1"},
        "spec": {
            "destinationRef": {"name": "primary"},
            "sync": {"intervalSeconds": 0, "mode": "Full", "maxObjectsPerRun": 100000,
                     "deepCheck": "ManifestDigest", "viewLimit": 2000}
        },
        "status": {
            "syncedAt": (now() - Duration::minutes(5)).to_rfc3339(),
            "viewExpiresAt": expires.to_rfc3339(),
            "pages": rows,
        }
    }))
    .expect("a RecoveryCatalog")
}

fn backup(name: &str, verdict: Option<&str>, receipt: Option<&str>, set: &str) -> Backup {
    let mut evidence = json!({});
    if let Some(v) = verdict {
        evidence["verification"] = json!({"result": v});
    }
    if let Some(r) = receipt {
        evidence["receiptSha256"] = json!(r);
    }
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": "lw-p152", "uid": format!("uid-{name}")},
        "spec": {
            "sourceRef": {"name": "source"},
            "archive": {"url": "logweir-destination://primary"},
            "topics": ["orders"],
            "triggeredBy": "manual",
            "deadlineSeconds": 3600,
        },
        "status": {"phase": "Succeeded", "backupId": set, "evidence": evidence},
    }))
    .expect("a Backup")
}

fn plan(point: Option<(&str, &str, String, String)>, set: &str) -> PlanFacts {
    let mut yaml = format!(
        "source:\n  storage:\n    backend: \"s3\"\n    bucket: \"lw-p152\"\n    prefix: \"archive\"\n  \
         backup: \"{set}\"\n  topics:\n    - \"orders\"\n"
    );
    if let Some((id, key, receipt, manifest)) = point {
        yaml.push_str(&format!(
            "  point:\n    point_id: \"{id}\"\n    receipt_key: \"{key}\"\n    receipt_sha256: \
             \"{receipt}\"\n    manifest_sha256: \"{manifest}\"\n"
        ));
    }
    yaml.push_str(
        "target:\n  bootstrap_servers:\n    - \"kafka:9092\"\n  mode: \"newTopic\"\n  topic_naming:\n    \
         prefix: \"restored-\"\n  topic_mapping_prefix: \"restored-\"\n  marker_topic: \
         \"logweir.scratch\"\n  default_replication_factor: 1\n  teardown: \"delete\"\n\
         restore:\n  point_in_time: \"2026-09-22T13:59:59.999Z\"\nsample:\n  window_start: \
         \"2026-09-22T13:00:00Z\"\n  window_end: \"2026-09-22T13:59:59.999Z\"\n  \
         records_per_partition: 25\n  anchor: \"head\"\nobjectives: {}\nevidence:\n  backend: \
         \"s3\"\n  bucket: \"lw-p152-evidence\"\n  prefix: \"logweir/\"\nnotifications:\n  webhooks: []\n",
    );
    let facts = PlanFacts::of(yaml.as_bytes(), None);
    assert!(
        facts.parsed.is_ok(),
        "the fixture plan parses: {:?}",
        facts.parsed.err()
    );
    facts
}

fn bound_plan() -> PlanFacts {
    plan(
        Some((POINT, RECEIPT_KEY, receipt_sha(), manifest_sha())),
        SET,
    )
}

struct Fixture {
    catalog: Option<RecoveryCatalog>,
    pages: Vec<(String, Option<ConfigMap>)>,
    backups: Vec<Backup>,
    complete: bool,
    plan: Option<PlanFacts>,
}

impl Fixture {
    fn healthy() -> Self {
        let p = page(
            "archive-g1-p0",
            &[entry(OTHER_POINT, true), entry(POINT, true)],
            true,
        );
        let cat = catalog(std::slice::from_ref(&p), now() + Duration::hours(1));
        Self {
            catalog: Some(cat),
            pages: vec![(p.0, Some(p.1))],
            backups: Vec::new(),
            complete: true,
            plan: Some(bound_plan()),
        }
    }

    fn facts(&self) -> CatalogPointFacts {
        let refusals = ControllerRefusals::from_backups(&self.backups);
        catalog_point_facts(&CatalogPointRead {
            catalog_name: "archive",
            point_id: POINT,
            catalog: self.catalog.as_ref(),
            pages: &self.pages,
            refusals: &refusals,
            backups_complete: self.complete,
            plan: self.plan.as_ref(),
            now: now(),
        })
    }

    fn row(&self) -> CheckOutcome {
        catalog_point_row(&self.facts(), now()).expect("a requested catalog point has a row")
    }
}

fn assert_row(row: &CheckOutcome, state: CheckState, code: CheckCode, why: &str) {
    assert_eq!(
        row.id,
        CheckId::RecoveryPointState,
        "{why}: the row is recoveryPoint.state"
    );
    assert_eq!(row.state, state, "{why}: state; message {:?}", row.message);
    assert_eq!(row.code, code, "{why}: code; message {:?}", row.message);
}

#[test]
fn a_selectable_row_bound_into_the_plan_is_ready() {
    let row = Fixture::healthy().row();
    assert_row(
        &row,
        CheckState::Ready,
        CheckCode::CatalogPointSelectable,
        "healthy",
    );
    let scope = row.scope.as_ref().expect("scoped to the catalog");
    assert_eq!(scope.kind, "RecoveryCatalog");
    assert_eq!(scope.name, "archive");
    assert_eq!(scope.uid.as_deref(), Some("cat-uid-1"));
}

#[test]
fn not_requested_reports_no_row() {
    assert!(catalog_point_row(&CatalogPointFacts::NotRequested, now()).is_none());
}

#[test]
fn a_missing_catalog_is_not_found() {
    let mut f = Fixture::healthy();
    f.catalog = None;
    assert_row(
        &f.row(),
        CheckState::NotReady,
        CheckCode::RecoveryPointNotFound,
        "no catalog",
    );
}

#[test]
fn a_point_the_view_does_not_list_is_not_found_and_a_listed_one_is_the_control() {
    let mut f = Fixture::healthy();
    let p = page("archive-g1-p0", &[entry(OTHER_POINT, true)], true);
    f.catalog = Some(catalog(
        std::slice::from_ref(&p),
        now() + Duration::hours(1),
    ));
    f.pages = vec![(p.0, Some(p.1))];
    assert_row(
        &f.row(),
        CheckState::NotReady,
        CheckCode::RecoveryPointNotFound,
        "unlisted",
    );
    // CONTROL: the healthy fixture lists it.
    assert_eq!(Fixture::healthy().row().state, CheckState::Ready);
}

#[test]
fn an_expired_view_has_no_answer() {
    let mut f = Fixture::healthy();
    let p = page("archive-g1-p0", &[entry(POINT, true)], true);
    f.catalog = Some(catalog(
        std::slice::from_ref(&p),
        now() - Duration::seconds(1),
    ));
    f.pages = vec![(p.0, Some(p.1))];
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "expired",
    );
}

#[test]
fn a_page_that_is_gone_mutable_or_altered_is_never_read_as_a_row() {
    // GONE.
    let mut f = Fixture::healthy();
    f.pages[0].1 = None;
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "gone",
    );

    // MUTABLE.
    let mut f = Fixture::healthy();
    let p = page("archive-g1-p0", &[entry(POINT, true)], false);
    f.catalog = Some(catalog(
        &[(p.0.clone(), p.1.clone(), p.2.clone())],
        now() + Duration::hours(1),
    ));
    f.pages = vec![(p.0, Some(p.1))];
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "mutable",
    );

    // ALTERED: the page's bytes are not the ones the status recorded. The row
    // it carries says `selectable: true`, and must not be believed.
    let mut f = Fixture::healthy();
    let honest = page("archive-g1-p0", &[entry(POINT, false)], true);
    let forged = page("archive-g1-p0", &[entry(POINT, true)], true);
    f.catalog = Some(catalog(
        std::slice::from_ref(&honest),
        now() + Duration::hours(1),
    ));
    f.pages = vec![(forged.0, Some(forged.1))];
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "altered",
    );
}

#[test]
fn a_row_the_catalog_does_not_mark_selectable_is_not_ready() {
    let mut f = Fixture::healthy();
    let p = page("archive-g1-p0", &[entry(POINT, false)], true);
    f.catalog = Some(catalog(
        std::slice::from_ref(&p),
        now() + Duration::hours(1),
    ));
    f.pages = vec![(p.0, Some(p.1))];
    let row = f.row();
    assert_row(
        &row,
        CheckState::NotReady,
        CheckCode::CatalogPointNotSelectable,
        "unselectable",
    );
    let message = row.message.clone();
    assert!(
        message.contains("UntrustedSigner"),
        "both axes are named: {message}"
    );
}

#[test]
fn a_reached_backup_refusal_of_the_same_receipt_outranks_a_selectable_row() {
    for verdict in ["Invalid", "Untrusted", "SomethingNew"] {
        let mut f = Fixture::healthy();
        f.backups = vec![backup("run-1", Some(verdict), Some(&receipt_sha()), SET)];
        let row = f.row();
        assert_row(
            &row,
            CheckState::NotReady,
            CheckCode::CatalogPointRefusedByController,
            verdict,
        );
        assert!(row.message.clone().contains(verdict));
    }
    // CONTROLS: a verdict the controller did NOT reach, or a pass, leaves the
    // catalog in charge -- and a refusal of a DIFFERENT receipt is not this one.
    // `Pending` is the evidence-fetch Job still reading: not a verdict, and the
    // shared rule (`catalog_view::is_reached_refusal`) defers on it.
    for verdict in [None, Some("NotAttempted"), Some("Pending"), Some("Valid")] {
        let mut f = Fixture::healthy();
        f.backups = vec![backup("run-1", verdict, Some(&receipt_sha()), SET)];
        assert_row(
            &f.row(),
            CheckState::Ready,
            CheckCode::CatalogPointSelectable,
            "deferring",
        );
    }
    let mut f = Fixture::healthy();
    let other = format!("sha256:{}", "d4".repeat(32));
    f.backups = vec![backup("run-2", Some("Invalid"), Some(&other), SET)];
    assert_row(
        &f.row(),
        CheckState::Ready,
        CheckCode::CatalogPointSelectable,
        "other receipt",
    );
}

#[test]
fn a_digestless_backup_of_the_set_is_joined_on_the_set_id() {
    let mut f = Fixture::healthy();
    f.backups = vec![backup("legacy-run", Some("Invalid"), None, SET)];
    assert_row(
        &f.row(),
        CheckState::NotReady,
        CheckCode::CatalogPointRefusedByController,
        "digest-less refusal of the same set",
    );
    // CONTROL: the same refusal of another set.
    let mut f = Fixture::healthy();
    f.backups = vec![backup("legacy-run", Some("Invalid"), None, "another-set")];
    assert_row(
        &f.row(),
        CheckState::Ready,
        CheckCode::CatalogPointSelectable,
        "another set",
    );
}

#[test]
fn an_incomplete_backup_listing_cannot_rule_out_a_refusal() {
    let mut f = Fixture::healthy();
    f.complete = false;
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "incomplete Backup list",
    );
    // BUT A REFUSAL IT DID FIND IS AN ANSWER.
    f.backups = vec![backup("run-1", Some("Invalid"), Some(&receipt_sha()), SET)];
    assert_row(
        &f.row(),
        CheckState::NotReady,
        CheckCode::CatalogPointRefusedByController,
        "found refusal",
    );
}

#[test]
fn a_plan_not_bound_to_the_row_is_a_binding_mismatch() {
    let cases: Vec<(&str, PlanFacts)> = vec![
        ("no source.point", plan(None, SET)),
        (
            "another point id",
            plan(
                Some((OTHER_POINT, RECEIPT_KEY, receipt_sha(), manifest_sha())),
                SET,
            ),
        ),
        (
            "another receipt key",
            plan(
                Some((
                    POINT,
                    "logweir/backups/x/y.receipt.json",
                    receipt_sha(),
                    manifest_sha(),
                )),
                SET,
            ),
        ),
        (
            "another receipt digest",
            plan(
                Some((
                    POINT,
                    RECEIPT_KEY,
                    format!("sha256:{}", "e5".repeat(32)),
                    manifest_sha(),
                )),
                SET,
            ),
        ),
        (
            "another manifest digest",
            plan(
                Some((
                    POINT,
                    RECEIPT_KEY,
                    receipt_sha(),
                    format!("sha256:{}", "f6".repeat(32)),
                )),
                SET,
            ),
        ),
        (
            "another backup set",
            plan(
                Some((POINT, RECEIPT_KEY, receipt_sha(), manifest_sha())),
                "latestCompleted",
            ),
        ),
    ];
    for (why, plan) in cases {
        let mut f = Fixture::healthy();
        f.plan = Some(plan);
        assert_row(
            &f.row(),
            CheckState::NotReady,
            CheckCode::CatalogPointBindingMismatch,
            why,
        );
    }
    let mut f = Fixture::healthy();
    f.plan = Some(PlanFacts::of(b"source: [not a plan", None));
    let row = f.row();
    assert_row(
        &row,
        CheckState::NotReady,
        CheckCode::CatalogPointBindingMismatch,
        "an unparseable plan",
    );
    // Said as what it is: a plan that did not parse, not one that parsed and
    // carries no binding -- the two have different repairs.
    assert!(
        row.message.contains("did not parse"),
        "the message names the parse failure: {}",
        row.message
    );
}

#[test]
fn a_refused_verdict_that_names_no_point_makes_the_join_incomplete() {
    // A refusal with neither a receipt digest nor a set id cannot be tied to a
    // row -- so it cannot be ruled out as THIS row's, and the answer is unknown.
    let mut orphan = backup("orphan", Some("Invalid"), None, SET);
    orphan.status.as_mut().expect("status").backup_id = None;
    let mut f = Fixture::healthy();
    f.backups = vec![orphan];
    assert_row(
        &f.row(),
        CheckState::Unknown,
        CheckCode::CatalogPointViewUnavailable,
        "an unattributed refusal",
    );
    // CONTROL: the same run with its set id is attributed to another set and
    // the row is ready.
    let mut f = Fixture::healthy();
    f.backups = vec![backup("orphan", Some("Invalid"), None, "another-set")];
    assert_row(
        &f.row(),
        CheckState::Ready,
        CheckCode::CatalogPointSelectable,
        "attributed",
    );
}

/// The shipped CRD admits the reference, pins the point id's shape, and holds
/// rule P10 -- read from the generated file an installation applies, so a
/// struct that lost its `regex` or a rule table that lost P10 is red here
/// before `just crds-check` has to notice the drift.
#[test]
fn the_shipped_crd_carries_the_catalog_point_reference_and_rule_p10() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/crd/preflights.yaml");
    let text = std::fs::read_to_string(&path).expect("config/crd/preflights.yaml");
    let crd: serde_yaml::Value = serde_yaml::from_str(&text).expect("the CRD parses");
    let restore = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["request"]["properties"]["restore"];
    let reference = &restore["properties"]["catalogPointRef"];
    assert_eq!(
        reference["properties"]["pointId"]["pattern"].as_str(),
        Some(weirkeeper::crds::preflight::POINT_ID_PATTERN),
        "catalogPointRef.pointId is pinned to lwp1- plus 32 lowercase hex"
    );
    let required: Vec<&str> = reference["required"]
        .as_sequence()
        .expect("required")
        .iter()
        .filter_map(serde_yaml::Value::as_str)
        .collect();
    assert_eq!(required, vec!["catalogRef", "pointId"]);
    let rules: Vec<&str> = restore["x-kubernetes-validations"]
        .as_sequence()
        .expect("the restore block's rules")
        .iter()
        .filter_map(|r| r["rule"].as_str())
        .collect();
    assert!(
        rules.contains(&weirkeeper::crds::preflight::P10_ONE_RECOVERY_POINT_RULE),
        "rule P10 is on the restore block: {rules:?}"
    );
}
