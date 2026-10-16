//! **PROD-03.1 / 03.2, the pure half, row by row**, over a fake registry that
//! answers the REST calls the way the prototype measured them
//! (`docs/to-do/decisions/OD-23-core.md` §1.1): Confluent and Redpanda keep IDs
//! in `IMPORT` mode, Karapace answers `PUT /mode` with 405, and Apicurio's
//! compatibility API accepts the mode and assigns its own IDs.

use logweir_core::schema_registry::{
    capture, check_scorecard_block, classify, closure, import, Class, ImportRefusal, ImportReport,
    ImportedVersion, NeededIds, ReceiptSchemaRegistry, Rest, ScorecardSchemas, Snapshot,
    SnapshotVersion, CANNOT_KEEP_IDS, IMPORTED_LISTED, REASON_INCOMPLETE, REASON_TOO_LARGE,
    REASON_UNAUTHORIZED, REASON_UNREACHABLE, SCHEMA_CONFLICT,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Confluent / Redpanda: IMPORT mode, IDs kept.
    KeepsIds,
    /// Karapace 6.2.3: `PUT /mode` is 405.
    NoImportMode,
    /// Apicurio's compatibility API: the mode is accepted and `GET /mode`
    /// does not answer IMPORT.
    ModeNotReported,
    /// A registry that accepts IMPORT and still assigns its own IDs.
    AssignsIds,
}

#[derive(Clone)]
struct Fake {
    kind: Kind,
    /// subject -> version -> (id, deleted)
    subjects: BTreeMap<String, BTreeMap<u32, (u32, bool)>>,
    /// id -> (type, schema, references)
    schemas: BTreeMap<u32, (String, String, Value)>,
    modes: BTreeMap<String, String>,
    next_id: u32,
    status_for_everything: Option<u16>,
    writes: Vec<String>,
    /// Fail every request after this many (a registry that goes away).
    budget: Option<usize>,
    global_mode: String,
    refuse_mode_delete: bool,
    /// Answer 500 to the registration of this ID.
    fail_register: Option<u32>,
    /// `(method, exact route without query, status, body)` answered once
    /// instead.
    forced: Vec<(String, String, u16, Value)>,
}

impl Fake {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            subjects: BTreeMap::new(),
            schemas: BTreeMap::new(),
            modes: BTreeMap::new(),
            next_id: 1,
            status_for_everything: None,
            writes: Vec::new(),
            budget: None,
            global_mode: "READWRITE".into(),
            refuse_mode_delete: false,
            fail_register: None,
            forced: Vec::new(),
        }
    }

    fn put(&mut self, v: &SnapshotVersion) {
        self.schemas.insert(
            v.id,
            (
                v.schema_type.clone(),
                v.schema.clone(),
                serde_json::to_value(&v.references).unwrap(),
            ),
        );
        self.subjects
            .entry(v.subject.clone())
            .or_default()
            .insert(v.version, (v.id, v.deleted));
        self.next_id = self.next_id.max(v.id + 1);
    }

    fn version_body(&self, subject: &str, version: u32) -> Value {
        let (id, _) = self.subjects[subject][&version];
        let (t, schema, refs) = &self.schemas[&id];
        let mut body = json!({"subject": subject, "version": version, "id": id, "schema": schema, "references": refs});
        if t != "AVRO" {
            body["schemaType"] = json!(t);
        }
        body
    }

    fn same(&self, id: u32, body: &Value) -> bool {
        let (t, schema, refs) = &self.schemas[&id];
        body["schema"].as_str() == Some(schema.as_str())
            && body["schemaType"].as_str().unwrap_or("AVRO") == t
            && body.get("references").cloned().unwrap_or(json!([])) == *refs
    }
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

impl Rest for Fake {
    fn call(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String> {
        if let Some(b) = self.budget.as_mut() {
            if *b == 0 {
                return Err("connection refused".into());
            }
            *b -= 1;
        }
        if let Some(status) = self.status_for_everything {
            return Ok((status, Value::Null));
        }
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        // Answered once: the registry then behaves as before.
        if let Some(i) = self
            .forced
            .iter()
            .position(|(m, r, _, _)| m == method && r == route)
        {
            let (_, _, status, body) = self.forced.remove(i);
            return Ok((status, body));
        }
        let deleted = query.contains("deleted=true");
        let parts: Vec<&str> = route.trim_start_matches('/').split('/').collect();
        let not_found = Ok((404, json!({"error_code": 40401})));
        let no_version = Ok((404, json!({"error_code": 40402})));
        match (method, parts.as_slice()) {
            ("GET", ["subjects"]) => Ok((
                200,
                json!(self
                    .subjects
                    .iter()
                    .filter(|(_, vs)| deleted || vs.values().any(|(_, d)| !d))
                    .map(|(s, _)| s.clone())
                    .collect::<Vec<_>>()),
            )),
            ("GET", ["subjects", s, "versions"]) => {
                let s = decode(s);
                match self.subjects.get(&s) {
                    None => not_found,
                    Some(vs) => Ok((
                        200,
                        json!(vs
                            .iter()
                            .filter(|(_, (_, d))| deleted || !d)
                            .map(|(v, _)| *v)
                            .collect::<Vec<_>>()),
                    )),
                }
            }
            ("GET", ["subjects", s, "versions", v]) => {
                let s = decode(s);
                let v: u32 = v.parse().unwrap();
                match self.subjects.get(&s).map(|vs| vs.get(&v)) {
                    Some(Some((_, d))) if deleted || !d => Ok((200, self.version_body(&s, v))),
                    Some(_) => no_version,
                    None => not_found,
                }
            }
            ("GET", ["schemas", "ids", id]) => {
                let id: u32 = id.parse().unwrap();
                match self.schemas.get(&id) {
                    Some((_, schema, _)) => Ok((200, json!({"schema": schema}))),
                    None => Ok((404, json!({"error_code": 40403}))),
                }
            }
            ("POST", ["subjects", s]) => {
                let s = decode(s);
                let body = body.unwrap();
                let found = self.subjects.get(&s).and_then(|vs| {
                    vs.iter()
                        .find(|(_, (id, _))| self.same(*id, body))
                        .map(|(v, (id, _))| (*v, *id))
                });
                match found {
                    Some((v, id)) => Ok((200, json!({"subject": s, "version": v, "id": id}))),
                    None => not_found,
                }
            }
            ("PUT", ["mode", s]) => {
                if self.kind == Kind::NoImportMode {
                    return Ok((405, json!({"message": "HTTP 405 Method Not Allowed"})));
                }
                let mode = body.unwrap()["mode"].as_str().unwrap().to_string();
                self.writes.push(format!("mode {} {mode}", decode(s)));
                if self.kind == Kind::ModeNotReported {
                    return Ok((204, Value::Null));
                }
                self.modes.insert(decode(s), mode.clone());
                Ok((200, json!({"mode": mode})))
            }
            ("GET", ["mode"]) => Ok((200, json!({"mode": self.global_mode}))),
            ("GET", ["mode", s]) => match self.modes.get(&decode(s)) {
                Some(mode) => Ok((200, json!({ "mode": mode }))),
                None => Ok((404, json!({"error_code": 40409}))),
            },
            ("DELETE", ["mode", s]) => {
                if self.refuse_mode_delete {
                    return Ok((500, Value::Null));
                }
                self.writes.push(format!("mode {} deleted", decode(s)));
                self.modes.remove(&decode(s));
                Ok((200, json!({"mode": "READWRITE"})))
            }
            ("POST", ["subjects", s, "versions"]) => {
                let s = decode(s);
                let body = body.unwrap().clone();
                if self.fail_register == body.get("id").and_then(Value::as_u64).map(|i| i as u32) {
                    return Ok((500, Value::Null));
                }
                let import = self.modes.get(&s).map(String::as_str) == Some("IMPORT");
                let id = match (import && self.kind == Kind::KeepsIds, body.get("id")) {
                    (true, Some(id)) => {
                        let id = id.as_u64().unwrap() as u32;
                        if self.schemas.contains_key(&id) && !self.same(id, &body) {
                            return Ok((422, json!({"message": "id taken"})));
                        }
                        id
                    }
                    _ => {
                        let id = self.next_id;
                        self.next_id += 1;
                        id
                    }
                };
                let version = body
                    .get("version")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32)
                    .unwrap_or_else(|| {
                        self.subjects
                            .get(&s)
                            .map_or(1, |vs| vs.keys().max().unwrap() + 1)
                    });
                self.schemas.insert(
                    id,
                    (
                        body["schemaType"].as_str().unwrap_or("AVRO").into(),
                        body["schema"].as_str().unwrap().into(),
                        body.get("references").cloned().unwrap_or(json!([])),
                    ),
                );
                self.subjects
                    .entry(s.clone())
                    .or_default()
                    .insert(version, (id, false));
                self.writes.push(format!("register {s}@{version} id {id}"));
                Ok((200, json!({"id": id})))
            }
            _ => Ok((404, Value::Null)),
        }
    }
}

fn v(
    subject: &str,
    version: u32,
    id: u32,
    schema: &str,
    refs: &[(&str, u32)],
    deleted: bool,
) -> SnapshotVersion {
    SnapshotVersion {
        subject: subject.into(),
        version,
        id,
        schema_type: if subject.ends_with(".proto") || subject == "events-value" {
            "PROTOBUF".into()
        } else {
            "AVRO".into()
        },
        schema: schema.into(),
        references: refs
            .iter()
            .map(|(s, v)| logweir_core::schema_registry::SchemaReference {
                name: (*s).into(),
                subject: (*s).into(),
                version: *v,
            })
            .collect(),
        deleted,
    }
}

/// The prototype's source: IDs from 4, a shared reference, two versions of
/// one subject (the second soft-deleted), a Protobuf import, and a subject
/// named by RecordNameStrategy.
fn source_versions() -> Vec<SnapshotVersion> {
    vec![
        v(
            "com.ex.Order",
            1,
            11,
            r#"{"type":"record","name":"Order2"}"#,
            &[],
            false,
        ),
        v("common.proto", 1, 8, "message Money {}", &[], false),
        v(
            "customer",
            1,
            4,
            r#"{"type":"record","name":"Customer"}"#,
            &[],
            false,
        ),
        v(
            "events-value",
            1,
            9,
            "import \"common.proto\"; message Paid {}",
            &[("common.proto", 1)],
            false,
        ),
        v(
            "orders-value",
            1,
            5,
            r#"{"type":"record","name":"Order"}"#,
            &[("customer", 1)],
            false,
        ),
        v(
            "orders-value",
            2,
            7,
            r#"{"type":"record","name":"Order","v":2}"#,
            &[("customer", 1)],
            true,
        ),
    ]
}

fn snapshot() -> Snapshot {
    Snapshot {
        format: 1,
        captured_at: "2026-10-16T00:00:00Z".parse().unwrap(),
        registry_url: "http://registry:8081".into(),
        versions: source_versions(),
    }
}

fn source() -> Fake {
    let mut f = Fake::new(Kind::KeepsIds);
    for v in source_versions() {
        f.put(&v);
    }
    f
}

#[test]
fn capture_reads_every_version_soft_deleted_included() {
    let mut src = source();
    let got = capture(&mut src, &|| false).unwrap();
    assert_eq!(got, source_versions());
    // KILLS: a capture that reads only live versions.
    assert!(got.iter().any(|v| v.deleted && v.id == 7));
}

#[test]
fn capture_failures_are_named_never_a_failed_backup() {
    let mut src = source();
    src.status_for_everything = Some(401);
    assert_eq!(
        capture(&mut src, &|| false).unwrap_err().reason(),
        REASON_UNAUTHORIZED
    );
    src.status_for_everything = Some(503);
    assert_eq!(
        capture(&mut src, &|| false).unwrap_err().reason(),
        REASON_UNREACHABLE
    );
    let mut gone = source();
    gone.budget = Some(0);
    assert_eq!(
        capture(&mut gone, &|| false).unwrap_err().reason(),
        REASON_UNREACHABLE
    );
    let mut late = source();
    assert_eq!(
        capture(&mut late, &|| true).unwrap_err().reason(),
        REASON_INCOMPLETE
    );
    // Too many versions.
    let mut big = Fake::new(Kind::KeepsIds);
    for i in 0..=logweir_core::schema_registry::MAX_SNAPSHOT_VERSIONS as u32 {
        big.subjects
            .entry("s".into())
            .or_default()
            .insert(i + 1, (1, false));
    }
    big.schemas
        .insert(1, ("AVRO".into(), "\"string\"".into(), json!([])));
    assert_eq!(
        capture(&mut big, &|| false).unwrap_err().reason(),
        REASON_TOO_LARGE
    );
}

#[test]
fn snapshot_parse_holds_the_digest() {
    let snap = snapshot();
    let bytes = snap.to_bytes().unwrap();
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    assert_eq!(Snapshot::parse(&bytes, &sha).unwrap(), snap);
    let mut tampered = bytes.clone();
    tampered[10] ^= 1;
    assert!(Snapshot::parse(&tampered, &sha)
        .unwrap_err()
        .starts_with("SchemaSnapshotInvalid"));
}

#[test]
fn unresolved_references_are_listed() {
    let mut snap = snapshot();
    snap.versions.retain(|v| v.subject != "customer");
    assert_eq!(snap.unresolved_references(), vec!["customer@1".to_string()]);
    // KILLS: a summary that ignores references (the row would read complete).
    let block =
        ReceiptSchemaRegistry::captured(&snap, "k".into(), "sha256:x".into(), &BTreeMap::new());
    assert!(!block.complete());
    assert_eq!(
        block.incomplete_reason().as_deref(),
        Some("unresolvedReferences")
    );
}

#[test]
fn the_closure_is_whole_subjects_references_first_with_no_naming_strategy() {
    let snap = snapshot();
    // ID 7 is orders-value v2: the whole subject comes, and customer first.
    let order: Vec<(String, u32)> = closure(&snap, &BTreeSet::from([7]))
        .iter()
        .map(|v| (v.subject.clone(), v.version))
        .collect();
    assert_eq!(
        order,
        vec![
            ("customer".into(), 1),
            ("orders-value".into(), 1),
            ("orders-value".into(), 2)
        ]
    );
    // SR-5: a subject named by RecordNameStrategy is found by its ID.
    let named: Vec<String> = closure(&snap, &BTreeSet::from([11]))
        .iter()
        .map(|v| v.subject.clone())
        .collect();
    assert_eq!(named, vec!["com.ex.Order".to_string()]);
    // The Protobuf import comes before its importer.
    let proto: Vec<String> = closure(&snap, &BTreeSet::from([9]))
        .iter()
        .map(|v| v.subject.clone())
        .collect();
    assert_eq!(
        proto,
        vec!["common.proto".to_string(), "events-value".to_string()]
    );
}

fn run(target: &mut Fake, needed: &[u32]) -> Result<ImportReport, ImportRefusal> {
    let snap = snapshot();
    let entries = closure(&snap, &needed.iter().copied().collect());
    let classes = classify(target, &entries)?;
    import(target, &entries, &classes)
}

#[test]
fn an_empty_target_gets_every_id_kept_and_a_second_run_writes_nothing() {
    let mut target = Fake::new(Kind::KeepsIds);
    let report = run(&mut target, &[5, 9, 10, 11]).unwrap();
    assert_eq!(report.imported.len(), 6);
    assert_eq!(report.already_present, 0);
    for v in source_versions() {
        assert_eq!(
            target.subjects[&v.subject][&v.version].0, v.id,
            "{}",
            v.subject
        );
        // Imported versions stay live, soft deletes are not replayed.
        assert!(!target.subjects[&v.subject][&v.version].1);
        // Each subject inherited its mode, so it ends with none of its own.
        assert_eq!(target.modes.get(&v.subject), None, "{}", v.subject);
    }
    // SR-9: the idempotence half.
    let writes = target.writes.len();
    let again = run(&mut target, &[5, 9, 10, 11]).unwrap();
    assert!(again.imported.is_empty());
    assert_eq!(again.already_present, 6);
    assert_eq!(target.writes.len(), writes, "a second run writes nothing");
}

#[test]
fn the_source_registry_itself_is_all_present() {
    let mut src = source();
    let report = run(&mut src, &[5, 9, 11]).unwrap();
    assert!(report.imported.is_empty());
    assert!(src.writes.is_empty());
}

#[test]
fn a_conflicting_id_refuses_before_any_write() {
    let mut target = Fake::new(Kind::KeepsIds);
    target.put(&v(
        "other",
        1,
        5,
        r#"{"type":"record","name":"Other"}"#,
        &[],
        false,
    ));
    let err = run(&mut target, &[5]).unwrap_err();
    assert_eq!(err.reason(), SCHEMA_CONFLICT);
    assert!(err.to_string().contains("orders-value"), "{err}");
    assert!(
        target.writes.is_empty(),
        "nothing written: {:?}",
        target.writes
    );
    // A subject-version held under another ID is a conflict too.
    let mut target = Fake::new(Kind::KeepsIds);
    target.put(&v(
        "customer",
        1,
        40,
        r#"{"type":"record","name":"Customer"}"#,
        &[],
        false,
    ));
    assert_eq!(
        run(&mut target, &[4]).unwrap_err().reason(),
        SCHEMA_CONFLICT
    );
    // And a subject holding a version the snapshot lacks.
    let mut target = Fake::new(Kind::KeepsIds);
    target.put(&v("customer", 2, 41, r#""string""#, &[], false));
    assert_eq!(
        run(&mut target, &[4]).unwrap_err().reason(),
        SCHEMA_CONFLICT
    );
    assert!(target.writes.is_empty());
}

#[test]
fn registries_that_cannot_keep_ids_are_refused_with_nothing_registered() {
    for kind in [Kind::NoImportMode, Kind::ModeNotReported, Kind::AssignsIds] {
        let mut target = Fake::new(kind);
        let err = run(&mut target, &[4]).unwrap_err();
        assert_eq!(err.reason(), CANNOT_KEEP_IDS);
        if kind != Kind::AssignsIds {
            assert!(
                !target.writes.iter().any(|w| w.starts_with("register")),
                "nothing registered: {:?}",
                target.writes
            );
        }
        // Never left in IMPORT mode: the subject inherited, so it has none.
        assert!(target.modes.is_empty(), "{:?}", target.modes);
    }
}

#[test]
fn a_partly_imported_closure_is_continued_not_refused() {
    // SR-8: the first subject is already there (an import stopped after it).
    let mut target = Fake::new(Kind::KeepsIds);
    target.put(&source_versions()[2]); // customer@1, id 4
    let report = run(&mut target, &[5]).unwrap();
    assert_eq!(report.already_present, 1);
    assert_eq!(
        report.imported,
        vec![
            ImportedVersion {
                subject: "orders-value".into(),
                version: 1,
                id: 5
            },
            ImportedVersion {
                subject: "orders-value".into(),
                version: 2,
                id: 7
            },
        ]
    );
}

#[test]
fn needed_ids_never_include_what_the_snapshot_lacks() {
    let frame = |id: u32| {
        let mut b = vec![0u8];
        b.extend_from_slice(&id.to_be_bytes());
        b.push(2);
        b
    };
    let mut scan = NeededIds::new(snapshot().ids());
    for _ in 0..9 {
        scan.observe("t", false, Some(&frame(5)));
    }
    scan.observe("t", false, Some(&frame(99)));
    scan.observe("t", false, None);
    // A key side where nothing resolves: its framed-looking IDs are not named.
    for i in 0..20 {
        scan.observe("t", true, Some(&frame(1000 + i)));
    }
    assert_eq!(scan.needed(), &BTreeSet::from([5]));
    assert_eq!(scan.not_in_snapshot(), vec![99]);
    let _ = Class::Import;
}

#[test]
fn scorecard_arms_refuse_each_contradiction() {
    let report = ImportReport {
        imported: (0..300)
            .map(|i| ImportedVersion {
                subject: format!("s{i:03}"),
                version: 1,
                id: i + 1,
            })
            .collect(),
        already_present: 2,
    };
    let ok = ScorecardSchemas::imported(
        &logweir_core::ids::sha256_prefixed(b"x"),
        "http://t",
        &report,
        vec![],
    );
    assert_eq!(ok.imported.len(), IMPORTED_LISTED);
    assert_eq!(ok.imported_count, 300);
    check_scorecard_block(&ok, true).unwrap();
    assert!(check_scorecard_block(&ok, false).is_err());
    let mut bad = ok.clone();
    bad.imported.pop();
    assert!(check_scorecard_block(&bad, true).is_err());
    let not = ScorecardSchemas::not_imported("noTargetRegistry");
    check_scorecard_block(&not, true).unwrap();
    let mut bad = not.clone();
    bad.already_present = 1;
    assert!(check_scorecard_block(&bad, true).is_err());
    assert!(check_scorecard_block(&ScorecardSchemas::not_imported("other"), true).is_err());
}

#[test]
fn a_read_only_subject_or_registry_is_refused_and_untouched() {
    for (own, global) in [(Some("READONLY"), "READWRITE"), (None, "READONLY_OVERRIDE")] {
        let mut target = Fake::new(Kind::KeepsIds);
        target.global_mode = global.into();
        if let Some(mode) = own {
            target.modes.insert("customer".into(), mode.into());
        }
        let err = run(&mut target, &[4]).unwrap_err();
        assert_eq!(
            err.reason(),
            logweir_core::schema_registry::READ_ONLY,
            "{err}"
        );
        assert!(target.writes.is_empty(), "untouched: {:?}", target.writes);
        assert_eq!(target.modes.get("customer").map(String::as_str), own);
    }
}

#[test]
fn each_subject_ends_with_the_mode_it_had() {
    // An explicit READWRITE stays an explicit READWRITE; an inherited mode
    // stays inherited (no subject-level mode).
    let mut target = Fake::new(Kind::KeepsIds);
    target.modes.insert("customer".into(), "READWRITE".into());
    run(&mut target, &[5]).unwrap();
    assert_eq!(
        target.modes.get("customer").map(String::as_str),
        Some("READWRITE")
    );
    assert_eq!(target.modes.get("orders-value"), None);
    // KILLS: an import that writes READWRITE on every subject.
    assert!(target
        .writes
        .contains(&"mode orders-value deleted".to_string()));
}

#[test]
fn a_failure_mid_import_puts_the_prior_mode_back() {
    let mut target = Fake::new(Kind::KeepsIds);
    target
        .modes
        .insert("orders-value".into(), "READWRITE".into());
    target.fail_register = Some(7); // orders-value v2
    let err = run(&mut target, &[5]).unwrap_err();
    assert_eq!(
        err.reason(),
        logweir_core::schema_registry::UNREACHABLE,
        "{err}"
    );
    assert_eq!(
        target.modes.get("orders-value").map(String::as_str),
        Some("READWRITE")
    );
    assert_eq!(target.modes.get("customer"), None);
}

#[test]
fn a_mode_that_cannot_be_put_back_is_said() {
    let mut target = Fake::new(Kind::KeepsIds);
    target.refuse_mode_delete = true;
    let err = run(&mut target, &[4]).unwrap_err();
    assert_eq!(
        err.reason(),
        logweir_core::schema_registry::MODE_NOT_RESTORED
    );
    assert!(err.to_string().contains("customer"), "{err}");
}

/// The fail-closed rule: only the registry's own "not there" answers are read
/// as "not there"; any other status or an odd body refuses before any write.
#[test]
fn every_unknown_answer_refuses_before_any_write() {
    let cases: Vec<(&str, &str, u16, Value)> = vec![
        // The registry's mode: a 500 is unreachable, a 404 or a body with no
        // mode cannot keep IDs.
        ("GET", "/mode", 500, Value::Null),
        ("GET", "/mode", 404, json!({"error_code": 40401})),
        ("GET", "/mode", 200, json!({})),
        // A subject's own mode: only 40409 / 40401 mean "inherits".
        (
            "GET",
            "/mode/customer",
            404,
            json!({"message": "not found"}),
        ),
        ("GET", "/mode/customer", 403, Value::Null),
        ("GET", "/mode/customer", 200, json!({"m": 1})),
        // The held versions: only 40401 means "holds none".
        ("GET", "/subjects/customer/versions", 500, Value::Null),
        ("GET", "/subjects/customer/versions", 404, json!({})),
        (
            "GET",
            "/subjects/customer/versions",
            200,
            json!({"not": "a list"}),
        ),
        // An ID's lookup: only 40403 means "not held".
        ("GET", "/schemas/ids/4", 404, json!({"error_code": 40401})),
        ("GET", "/schemas/ids/4", 302, Value::Null),
        // A version's lookup: only 40401 / 40402 mean "not held".
        ("GET", "/subjects/customer/versions/1", 404, json!({})),
    ];
    for (method, route, status, body) in cases {
        let mut target = Fake::new(Kind::KeepsIds);
        target
            .forced
            .push((method.into(), route.into(), status, body.clone()));
        let err = run(&mut target, &[4]).expect_err(&format!("{method} {route} {status} {body}"));
        assert!(
            matches!(
                err.reason(),
                "SchemaConflict" | "SchemaRegistryCannotKeepIds" | "SchemaRegistryUnreachable"
            ),
            "{method} {route} {status}: {err}"
        );
        assert!(
            !target.writes.iter().any(|w| w.starts_with("register")),
            "{method} {route} {status}: nothing registered: {:?}",
            target.writes
        );
    }
    // CONTROLS: the legitimate "not there" answers proceed.
    let mut target = Fake::new(Kind::KeepsIds);
    target.forced.push((
        "GET".into(),
        "/mode/customer".into(),
        404,
        json!({"error_code": 40401}),
    ));
    run(&mut target, &[4]).expect("a subject that does not exist yet inherits");
}

/// A referenced subject comes WHOLE: every version, not only the one the
/// reference names. KILLS: a closure that follows references version by
/// version only.
#[test]
fn a_referenced_subject_is_imported_whole() {
    let mut snap = snapshot();
    snap.versions.push(v(
        "customer",
        2,
        12,
        r#"{"type":"record","name":"Customer","v":2}"#,
        &[],
        false,
    ));
    let got: Vec<(String, u32)> = closure(&snap, &BTreeSet::from([5]))
        .iter()
        .map(|v| (v.subject.clone(), v.version))
        .collect();
    assert!(got.contains(&("customer".to_string(), 2)), "{got:?}");
}
