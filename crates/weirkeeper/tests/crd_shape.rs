//! The properties of every kind, read off the CHECKED-IN CRD YAML.
//!
//! WHY THESE READ FILES RATHER THAN CALL CODE. What ships is `config/crd/*.yaml`
//! — that is what `kubectl apply` consumes, what the UI's forms are written
//! against, and what Tasks 16–24 read. A test over the Rust types would pass
//! while the checked-in YAML said something else, which is exactly the failure
//! the drift gate exists to remove. So the assertions are made against the
//! files, and `the_checked_in_crds_are_what_the_emitter_renders` is what ties
//! the files back to the types.
//!
//! NONE OF THESE DIALS, WAITS ON A JOB, OR RUNS `kubectl`. Every test here
//! parses a checked-in YAML file or reads a source file; the CEL evaluation in
//! `an_absent_optional_field_cannot_be_added_on_update` is a table against the
//! rule string, so it runs in `cargo test` with no API server (Global
//! Constraint 22's 15 s per-test bound, Standing Rule 22).
//!
//! THIS FILE IS HOSTED HERE AND NOT OWNED HERE, IN PART. Two late-binding
//! agreement tests are appended by later slots and are NOT written, stubbed or
//! anticipated by Task 15b: `the_crd_auth_mode_enum_and_auth_spec_agree`
//! (Task 6, slot 7) and `the_crd_mode_enum_and_target_mode_agree` (Task 9b,
//! slot 10). The section at the bottom of this file is where they go, and
//! [`enum_values`] is the helper they need — the CRD side of both agreements
//! is already asserted here, so those tests only have to compare the Rust side
//! against it.

use std::path::{Path, PathBuf};

use serde_yaml::Value;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The workspace root: this crate's manifest directory is
/// `crates/weirkeeper`.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

/// The directory the checked-in CRDs live in.
fn crd_dir() -> PathBuf {
    repo_root().join("config/crd")
}

/// Every file, in the order [`weirkeeper::crds::KINDS`] names their kinds.
const FILES: [&str; 9] = [
    "kafkaclusters.yaml",
    "backupschedules.yaml",
    "backups.yaml",
    "restores.yaml",
    "approvals.yaml",
    "trustrosters.yaml",
    "backupdestinations.yaml",
    "topicdiscoveries.yaml",
    "preflights.yaml",
];

/// One checked-in CRD, parsed.
fn crd(file: &str) -> Value {
    let path = crd_dir().join(file);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e} — run `just crds`", path.display()));
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{} is not YAML: {e}", path.display()))
}

/// `v[key]`, or a panic naming the path that was missing.
fn at<'a>(v: &'a Value, path: &[&str]) -> &'a Value {
    let mut cur = v;
    for (i, key) in path.iter().enumerate() {
        cur = match cur.get(*key) {
            Some(next) => next,
            None => panic!("no `{}` at `{}`", key, path[..i].join(".")),
        };
    }
    cur
}

/// The sole version's `openAPIV3Schema`.
fn root_schema(crd: &Value) -> &Value {
    let versions = at(crd, &["spec", "versions"])
        .as_sequence()
        .expect("spec.versions is a list");
    assert_eq!(
        versions.len(),
        1,
        "tag 1 serves exactly one version; a second version is a conversion-webhook \
         decision this plan does not take"
    );
    at(&versions[0], &["schema", "openAPIV3Schema"])
}

/// The schema node for `.spec`.
fn spec_schema(crd: &Value) -> &Value {
    at(root_schema(crd), &["properties", "spec"])
}

/// The schema node for `.status`.
fn status_schema(crd: &Value) -> &Value {
    at(root_schema(crd), &["properties", "status"])
}

/// The `enum` values at a schema node, as strings, IN FILE ORDER.
///
/// The helper the two late-binding agreement tests need: both compare a Rust
/// type's serde spellings against the CRD's enum, and the CRD half is this.
fn enum_values(node: &Value) -> Vec<String> {
    node.get("enum")
        .unwrap_or_else(|| panic!("this schema node declares no `enum`: {node:?}"))
        .as_sequence()
        .expect("`enum` is a list")
        .iter()
        .map(|v| {
            v.as_str()
                .expect("every enum value is a string")
                .to_string()
        })
        .collect()
}

/// `required`, as strings, sorted.
///
/// SORTED, AND WHY. `schemars` builds `required` from a `BTreeSet`, so the
/// emitted order is lexicographic and not declaration order. The property the
/// plan states — "`.spec.required` is exactly these four" — is a property of
/// the SET, so the comparison is made against a sorted expectation. Asserting
/// declaration order would be asserting a `schemars` implementation detail,
/// and a fifth required field or a dropped one still fails this either way.
fn required(node: &Value) -> Vec<String> {
    let mut out: Vec<String> = node
        .get("required")
        .map(|r| {
            r.as_sequence()
                .expect("`required` is a list")
                .iter()
                .map(|v| v.as_str().expect("a required name is a string").to_string())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Every file under `dir`, recursively, as `(path, text)`. Non-UTF-8 files are
/// skipped, and there are none in this tree.
fn files_under(dir: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries =
            std::fs::read_dir(&d).unwrap_or_else(|e| panic!("read_dir {}: {e}", d.display()));
        for entry in entries {
            let entry = entry.expect("a directory entry");
            let path = entry.path();
            let meta = entry.metadata().expect("entry metadata");
            if meta.is_dir() {
                // `target/` never appears under `crates/` in this workspace,
                // but a stray one would make this walk enormous rather than
                // wrong, so it is skipped by name.
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if meta.is_file() {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    out.push((path, text));
                }
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The body of a `just` recipe, by name.
///
/// The `just_lint_runs_the_one_signer_gate` pattern
/// (`crates/logweir/tests/one_signer_gate.rs`): collect the lines after the
/// recipe header until the next line that starts at column 0 with a lowercase
/// letter, and assert membership only — never the recipe's length, its line
/// numbers, or the absence of other lines.
fn just_recipe(justfile: &str, name: &str) -> Option<String> {
    let mut body = String::new();
    let mut inside = false;
    for line in justfile.lines() {
        if !inside {
            let header = line.split_once(':').map(|(lhs, _)| lhs.trim_end());
            if header == Some(name) && !line.starts_with(char::is_whitespace) {
                inside = true;
            }
            continue;
        }
        if line.starts_with(|c: char| c.is_ascii_lowercase()) {
            break;
        }
        body.push_str(line);
        body.push('\n');
    }
    inside.then_some(body)
}

// ---------------------------------------------------------------------------
// The group, the version, and the kind list
// ---------------------------------------------------------------------------

/// Global Constraint 14: Logweir owns `logweir.dev/v1alpha1`, now, not
/// deferred.
#[test]
fn the_group_is_logweir_dev_v1alpha1() {
    for file in FILES {
        let doc = crd(file);
        assert_eq!(
            at(&doc, &["spec", "group"]).as_str(),
            Some("logweir.dev"),
            "{file}: the group is `logweir.dev` and never a vendor's"
        );
        let versions = at(&doc, &["spec", "versions"])
            .as_sequence()
            .expect("spec.versions is a list");
        assert_eq!(
            versions[0].get("name").and_then(Value::as_str),
            Some("v1alpha1"),
            "{file}: versions[0].name is `v1alpha1`"
        );
        assert_eq!(
            at(&doc, &["apiVersion"]).as_str(),
            Some("apiextensions.k8s.io/v1"),
            "{file}: a CRD this plan ships is an apiextensions/v1 CRD — `v1beta1` \
             was removed in Kubernetes 1.22, well below the 1.29 floor"
        );
    }
}

/// Exactly the kinds ADR 0008 records, and no others.
///
/// Global Constraint 34 amended the roadmap's list to Amendment A's six,
/// `RestoreDrill` retired for `Restore` and `MetadataSnapshot` merely
/// reserved; **Amendment F** adds `BackupDestination`, `TopicDiscovery` and
/// `Preflight`. The count below is the ADR's, written down, so a kind that
/// arrives without an amendment is a red test rather than a new CRD file.
#[test]
fn the_kind_list_is_exactly_the_adr() {
    // READ EVERY FILE IN `config/crd/`, NOT THE SIX THIS TEST NAMES. A list
    // built from `FILES` could not see an UNLISTED kind at all — the emitted
    // set would be compared against itself and the forbidden-name loop below
    // would have nothing to look at. Reading the directory is what makes a
    // `Drill` CRD someone added and emitted fail on its own name.
    //
    // ONE EXACT FILENAME IS SKIPPED, AND IT IS NOT A PATTERN (Task 21).
    // `config/crd/kustomization.yaml` is the kustomize base that lists the
    // CRDs by name, and it is a `kustomize.config.k8s.io/v1beta1 Kustomization`
    // rather than a `CustomResourceDefinition`. It has to live in this
    // directory: kustomize only recognises a base by a file of that exact name
    // inside it, and `config/overlays/local-images` can name a sibling
    // DIRECTORY but not a bare file from outside its own root. Skipping it by
    // its literal name leaves the property this test asserts untouched — a
    // seventh CRD file still fails here, on its own name — and adding a second
    // skip would be a visible diff on this line.
    const NOT_A_CRD: [&str; 1] = ["kustomization.yaml"];
    let is_crd_file = |p: &PathBuf| -> bool {
        p.extension().is_some_and(|x| x == "yaml")
            && !NOT_A_CRD.contains(&p.file_name().and_then(|n| n.to_str()).unwrap_or_default())
    };
    let dir = crd_dir();
    let mut yaml_files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("config/crd is readable")
        .map(|e| e.expect("an entry").path())
        .filter(is_crd_file)
        .collect();
    yaml_files.sort();
    let mut kinds: Vec<String> = yaml_files
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p).expect("a CRD file is readable");
            let doc: Value = serde_yaml::from_str(&text)
                .unwrap_or_else(|e| panic!("{} is not YAML: {e}", p.display()));
            at(&doc, &["spec", "names", "kind"])
                .as_str()
                .expect("names.kind is a string")
                .to_string()
        })
        .collect();
    kinds.sort();
    assert_eq!(
        kinds.len(),
        weirkeeper::crds::KINDS.len(),
        "config/crd holds {} kinds, not the {} ADR 0008 records: {kinds:?}",
        kinds.len(),
        weirkeeper::crds::KINDS.len()
    );
    assert_eq!(
        weirkeeper::crds::KINDS.len(),
        9,
        "ADR 0008 records nine kinds — Amendment A's six plus Amendment F's \
         BackupDestination, TopicDiscovery and Preflight. A tenth needs its own \
         amendment in docs/architecture.md, and this line is where that decision \
         becomes a diff."
    );
    let mut expected: Vec<String> = weirkeeper::crds::KINDS
        .iter()
        .map(|k| k.to_string())
        .collect();
    expected.sort();
    assert_eq!(
        kinds, expected,
        "the emitted kind set must be exactly `weirkeeper::crds::KINDS`, which is ADR 0008 \
         Amendment A's six plus Amendment F's three"
    );

    // The four names that must NOT be kinds, each for its own reason: a drill
    // is a `Restore` with `spec.target.mode: scratch` (`Drill`,
    // `RestoreDrill`), `Switchover` is tag 2, and `MetadataSnapshot` is
    // reserved and unbuilt.
    for forbidden in ["Drill", "RestoreDrill", "Switchover", "MetadataSnapshot"] {
        assert!(
            !kinds.iter().any(|k| k == forbidden),
            "`{forbidden}` is not a kind of this group; the emitted set was {kinds:?}"
        );
    }
    // And no second Kafka-named kind: `KafkaCluster` is the only one, so a
    // `KafkaBackup` or `KafkaRestore` cannot creep in beside it.
    let kafka_named: Vec<&String> = kinds.iter().filter(|k| k.contains("Kafka")).collect();
    assert_eq!(
        kafka_named,
        vec!["KafkaCluster"],
        "the only kind containing `Kafka` is `KafkaCluster`; got {kafka_named:?}"
    );

    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .expect("config/crd is readable")
        .map(|e| {
            e.expect("an entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .filter(|n| !NOT_A_CRD.contains(&n.as_str()))
        .collect();
    on_disk.sort();
    let mut want: Vec<String> = FILES.iter().map(|f| f.to_string()).collect();
    want.sort();
    assert_eq!(
        on_disk, want,
        "config/crd holds exactly the CRD files `FILES` names and no more (the one kustomize \
         base named in NOT_A_CRD aside) — a stray file here is a kind someone applied"
    );

    // AND THE KUSTOMIZE BASE LISTS EVERY ONE OF THEM. `kustomize` has no
    // directory wildcard, so a file that exists here but is missing from
    // `resources:` never reaches `logweir.yaml` — the install would create a
    // controller for a kind the cluster cannot store.
    let kustomization = std::fs::read_to_string(dir.join("kustomization.yaml"))
        .expect("config/crd/kustomization.yaml is readable");
    let listed: Vec<String> = kustomization
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- "))
        .map(str::to_string)
        .collect();
    let mut listed_sorted = listed.clone();
    listed_sorted.sort();
    assert_eq!(
        listed_sorted, want,
        "config/crd/kustomization.yaml must list every CRD file by name; kustomize has no \
         directory wildcard, so an unlisted file is a kind `logweir.yaml` never installs"
    );
}

/// Global Constraints 5 and 14: `weirkeeper` reads no vendor custom resource
/// and creates none.
///
/// A SOURCE-AND-YAML READING TEST, over the three directories where a vendor
/// group would have to appear to matter: the controller's own source, the
/// emitter, and the shipped CRDs. This file is deliberately NOT in that set —
/// a guard that scanned itself would report itself, which is the same hazard
/// `scripts/check-dod.sh`'s header records.
#[test]
fn no_vendor_crd_group_is_named_anywhere() {
    let root = repo_root();
    let scanned = [
        root.join("crates/weirkeeper/src"),
        root.join("crates/weirkeeper/examples"),
        root.join("config/crd"),
    ];
    let needles = ["kafka.oso.sh", "kafkabackup.com", "osodevops/"];
    let mut hits = Vec::new();
    let mut scanned_files = 0usize;
    for dir in &scanned {
        for (path, text) in files_under(dir) {
            scanned_files += 1;
            for needle in needles {
                if text.contains(needle) {
                    hits.push(format!("{} names `{needle}`", path.display()));
                }
            }
        }
    }
    assert!(
        scanned_files >= 9,
        "this test scanned only {scanned_files} files, which means it is looking in the \
         wrong place and could not fail: expected at least the seven crds modules, the \
         emitter and the six CRDs"
    );
    assert!(
        hits.is_empty(),
        "a vendor API group or registry namespace is named where Logweir's own control \
         plane and its own CRDs live (Global Constraints 5 and 14):\n{}",
        hits.join("\n")
    );
}

// ---------------------------------------------------------------------------
// The CRD ENVELOPE — printer columns, the status subresource, the scope
// ---------------------------------------------------------------------------
//
// FIX ROUND 1, FINDINGS 1, 2 AND 3. Every test above this section reads the
// CHECKED-IN files, which is right for the field schemas and WRONG for the
// three properties below. Both medium findings were found the same way: mutate
// the EMITTER, then run `just crds`. That moves the checked-in bytes with the
// mutant, so every file-reading test and all six CI `diff -u`s stay green, and
// a dropped printer column or a dropped `subresources.status` ships silently.
//
// So these three read `render_all()` — the renderer's own output, independent
// of `config/crd/` — and compare it against a LITERAL copied from
// `task-15b-brief.md`'s Produces table. The two halves are complementary and
// both are needed: the drift gate
// (`the_checked_in_crds_are_what_the_emitter_renders`) catches a hand edit to
// `config/crd/` with no re-render, and these catch an emitter edit WITH one.

/// Every kind as the RENDERER produces it, parsed: `(kind, file_name, doc)`.
///
/// `render_all` is the single renderer the `emit_crds` example and the drift
/// test both go through, so this is exactly the document that would be written
/// to `config/crd/` by the next `just crds` — which is the point: a test that
/// re-read the files after that command could not see the change.
fn rendered() -> Vec<(&'static str, &'static str, Value)> {
    weirkeeper::crds::render_all()
        .into_iter()
        .map(|r| {
            let doc: Value = serde_yaml::from_str(&r.yaml)
                .unwrap_or_else(|e| panic!("the rendered {} parses as YAML: {e}", r.kind));
            (r.kind, r.file_name, doc)
        })
        .collect()
}

/// The sole served version of a rendered CRD.
///
/// One version, asserted rather than indexed blindly: `versions[0]` on a
/// two-version CRD would silently check half of it, and a second version is a
/// conversion-webhook decision this tag has not taken.
fn sole_version<'a>(kind: &str, doc: &'a Value) -> &'a Value {
    let versions = at(doc, &["spec", "versions"])
        .as_sequence()
        .expect("spec.versions is a list");
    assert_eq!(
        versions.len(),
        1,
        "{kind}: `v1alpha1` is the one served, stored version of this group at tag 1; a \
         second version is a conversion decision, not a rendering detail"
    );
    &versions[0]
}

/// One printer column, as `kubectl get` reads it: `(NAME, jsonPath, type)`.
type Column = (&'static str, &'static str, &'static str);

/// The brief's Produces table, per kind, IN ORDER.
///
/// A LITERAL, AND DELIBERATELY NOT DERIVED FROM ANYTHING. The column NAMES and
/// their ORDER are copied verbatim from `task-15b-brief.md`'s Produces table
/// (`KafkaCluster` ROLE/REACHABLE/CLUSTER-ID/AGE, `BackupSchedule`
/// SCHEDULE/SUSPEND/LAST/NEXT/READY/AGE, `Backup` PHASE/EXIT/RECORDS/SIGNED/AGE,
/// `Restore` MODE/PHASE/EXIT/REASON/OUTCOME/INTEGRITY/RTO/SIGNED/AGE, `Approval`
/// SUBJECT/VERIFIED/APPROVER/KEY-ID/AGE, `TrustRoster` KEYS/LOADED/EXPIRED/AGE);
/// the `jsonPath` and `type` beside each name are the declarations those names
/// are required to keep, so a column that survives a RENAME of the field it
/// reads fails here too.
///
/// Reading these back out of `config/crd/` — or out of the derive — would make
/// the expectation a copy of the thing under test, which is exactly the hole
/// mutant MA walked through: it dropped `REASON` from `Restore`, ran
/// `just crds`, and 16 of 16 tests passed with all six CI diffs green.
/// `kubectl get restore` is the operator's whole view of a run and Task 26's UI
/// reads these columns, so the table is an interface and not decoration.
///
/// `Restore`'s `REASON` READS `.status.reason`, NOT `.status.exitReason`, since
/// Task 20 fix round 1 (review finding M2). `exitReason` is written from an
/// exit code, and a refusal the CONTROLLER makes before any `POST` has no code
/// — so all four admission refusals and `NameTooLong` printed the identical
/// `operational` in this column, measured live on two objects. `status.reason`
/// is the condition's own reason promoted to a scalar; the field's doc comment
/// in `crds/restore.rs` carries the measurement.
/// **`Backup`'s table is unchanged and was never affected: it has no `REASON`
/// column at all** (PHASE/EXIT/RECORDS/SIGNED/AGE), so there was nothing
/// reading `.status.exitReason` on that kind to repoint.
const PRINTER_COLUMNS: [(&str, &[Column]); 9] = [
    (
        "KafkaCluster",
        &[
            ("ROLE", ".spec.role", "string"),
            ("REACHABLE", ".status.reachable", "string"),
            ("CLUSTER-ID", ".status.clusterId", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "BackupSchedule",
        &[
            ("SCHEDULE", ".spec.schedule", "string"),
            ("SUSPEND", ".spec.suspend", "string"),
            ("LAST", ".status.lastFireTime", "date"),
            ("NEXT", ".status.nextFireTime", "date"),
            (
                "READY",
                ".status.conditions[?(@.type==\"Ready\")].status",
                "string",
            ),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "Backup",
        &[
            ("PHASE", ".status.phase", "string"),
            ("EXIT", ".status.exitCode", "integer"),
            ("RECORDS", ".status.records", "integer"),
            ("SIGNED", ".status.evidence.verification.result", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "Restore",
        &[
            ("MODE", ".spec.target.mode", "string"),
            ("PHASE", ".status.phase", "string"),
            ("EXIT", ".status.exitCode", "integer"),
            ("REASON", ".status.reason", "string"),
            ("OUTCOME", ".status.outcome", "string"),
            ("INTEGRITY", ".status.integrity.result", "string"),
            ("RTO", ".status.measured.rtoSeconds", "integer"),
            ("SIGNED", ".status.evidence.verification.result", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "Approval",
        &[
            ("SUBJECT", ".spec.subjectRef.name", "string"),
            ("VERIFIED", ".status.verified", "string"),
            ("APPROVER", ".status.approver", "string"),
            ("KEY-ID", ".status.matchedKeyId", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "TrustRoster",
        &[
            ("KEYS", ".spec.approverKeys[*].keyId", "string"),
            ("LOADED", ".status.loaded", "string"),
            ("EXPIRED", ".status.expiredKeyIds[*]", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "BackupDestination",
        &[
            ("BUCKET", ".spec.storage.bucket", "string"),
            ("ENDPOINT", ".spec.storage.endpoint", "string"),
            ("TRANSPORT", ".spec.transport.security", "string"),
            (
                "VALID",
                ".status.conditions[?(@.type==\"Valid\")].status",
                "string",
            ),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "TopicDiscovery",
        &[
            ("CONNECTION", ".spec.request.connectionRef.name", "string"),
            ("PHASE", ".status.phase", "string"),
            ("VISIBILITY", ".status.result.visibility.state", "string"),
            ("TOPICS", ".status.result.counts.returned", "integer"),
            ("OBSERVED", ".status.observedAt", "date"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "Preflight",
        &[
            ("OPERATION", ".spec.request.operation", "string"),
            ("PHASE", ".status.phase", "string"),
            ("RESULT", ".status.result.state", "string"),
            ("EXPIRES", ".status.result.expiresAt", "date"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
];

/// The brief's Scope column: five workload kinds Namespaced, the roster
/// Cluster.
///
/// A LITERAL for the same reason [`PRINTER_COLUMNS`] is. `TrustRoster` is
/// Cluster-scoped because `allowedClusterIds` must not sit where a namespace
/// tenant can widen its own allowlist; the other five are Namespaced because
/// [`weirkeeper::crds::LocalRef`] carries no namespace and a cross-namespace
/// reference is a privilege-escalation surface. A kind that quietly became
/// Cluster-scoped would move its objects out of every namespaced RBAC rule
/// Task 21 writes.
const SCOPES: [(&str, &str); 9] = [
    ("KafkaCluster", "Namespaced"),
    ("BackupSchedule", "Namespaced"),
    ("Backup", "Namespaced"),
    ("Restore", "Namespaced"),
    ("Approval", "Namespaced"),
    ("TrustRoster", "Cluster"),
    ("BackupDestination", "Namespaced"),
    ("TopicDiscovery", "Namespaced"),
    ("Preflight", "Namespaced"),
];

/// FIX ROUND 1, FINDING 1: every kind's printer columns are exactly the
/// brief's table — name, `jsonPath` and `type`, in order.
#[test]
fn the_printer_columns_are_exactly_the_briefs_table() {
    let docs = rendered();
    assert_eq!(
        docs.len(),
        PRINTER_COLUMNS.len(),
        "the Produces tables fix printer columns for every kind; the renderer \
         produced {}",
        docs.len()
    );

    for (kind, _file, doc) in &docs {
        let want: Vec<(String, String, String)> = PRINTER_COLUMNS
            .iter()
            .find(|(k, _)| k == kind)
            .unwrap_or_else(|| {
                panic!(
                    "the brief's Produces table fixes the printer columns of every kind this \
                     group ships, and `{kind}` is not in it"
                )
            })
            .1
            .iter()
            .map(|(n, p, t)| (n.to_string(), p.to_string(), t.to_string()))
            .collect();

        // An ABSENT block is the empty list and not a panic, so the count
        // assertion below is what reports it: `additionalPrinterColumns`
        // deleted wholesale is the same defect as one column dropped, only
        // larger.
        let empty: Vec<Value> = Vec::new();
        let cols: &Vec<Value> = sole_version(kind, doc)
            .get("additionalPrinterColumns")
            .and_then(Value::as_sequence)
            .unwrap_or(&empty);

        let got: Vec<(String, String, String)> = cols
            .iter()
            .map(|c| {
                let f = |key: &str| {
                    c.get(key)
                        .and_then(Value::as_str)
                        .unwrap_or_else(|| {
                            panic!("{kind}: a printer column declares `{key}`; got {c:?}")
                        })
                        .to_string()
                };
                (f("name"), f("jsonPath"), f("type"))
            })
            .collect();

        // COUNT FIRST. A dropped or an added column is the likeliest drift and
        // its own sentence reads better than a diff of two nine-element lists.
        assert_eq!(
            got.len(),
            want.len(),
            "{kind}: the brief's Produces table fixes {} printer columns and the renderer \
             produced {}. `kubectl get` is the operator's whole view of a run and Task 26's \
             UI reads these columns — a column is an interface, not decoration.\n  \
             wanted: {:?}\n  got:    {:?}",
            want.len(),
            got.len(),
            want.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>(),
            got.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>()
        );

        assert_eq!(
            got, want,
            "{kind}: the printer columns must be the brief's table exactly — name, jsonPath \
             and type, IN ORDER"
        );
    }
}

/// FIX ROUND 1, FINDING 2: every kind declares `subresources.status`, and
/// losing it is otherwise silent.
///
/// MEASURED CONSEQUENCE, not a style rule. With `subresources` dropped from
/// `Restore` and the CRD applied to a live 1.34 API server,
/// `kubectl --context docker-desktop patch restore <name> --subresource=status`
/// returns
/// `Error from server (NotFound)` while `get restore <name>` still shows the
/// object. Tasks 16-20 write every `.status` through that subresource
/// (kube-rs `patch_status` / `replace_status`), so the loss lands as a runtime
/// failure in a later slot with no gate pointing at the cause — and the CRD
/// still applies, so nothing upstream of the controller complains either.
///
/// `status: {}` is the whole declaration: an EMPTY mapping is what enables the
/// subresource, and a non-empty one would be a shape this tag has not chosen.
#[test]
fn every_kind_declares_the_status_subresource() {
    let docs = rendered();
    assert_eq!(
        docs.len(),
        weirkeeper::crds::KINDS.len(),
        "every kind carries a status subresource; the renderer produced {}",
        docs.len()
    );

    let mut wrong: Vec<String> = Vec::new();
    for (kind, _file, doc) in &docs {
        let status = sole_version(kind, doc)
            .get("subresources")
            .and_then(|s| s.get("status"));
        match status {
            Some(Value::Mapping(m)) if m.is_empty() => {}
            Some(other) => wrong.push(format!("{kind}: subresources.status is {other:?}")),
            None => wrong.push(format!("{kind}: subresources.status is ABSENT")),
        }
    }
    assert!(
        wrong.is_empty(),
        "every kind of this group declares `subresources.status: {{}}`, because Tasks 16-20 \
         write every `.status` through it. Without it a `patch --subresource=status` \
         returns NotFound on an object that still exists, and the CRD applies cleanly — so \
         nothing but this test would report it:\n{}",
        wrong.join("\n")
    );
}

/// FIX ROUND 1, FINDING 3: the scope of EVERY kind, not only the roster's.
///
/// `the_roster_carries_key_material_for_both_lists` asserts
/// `TrustRoster.scope == Cluster` and nothing asserted the others, so a kind
/// that silently became Cluster-scoped passed every gate — and would then sit
/// outside every namespaced RBAC rule Task 21 writes. A kind that became
/// Cluster-scoped the other way round is worse: `BackupDestination` names
/// namespace-local Secrets, so a cluster-scoped one would be a reference out
/// of its own namespace, which [`weirkeeper::crds::LocalRef`] exists to make
/// impossible.
#[test]
fn only_the_roster_is_cluster_scoped() {
    let got: Vec<(&str, String)> = rendered()
        .iter()
        .map(|(kind, _file, doc)| {
            (
                *kind,
                at(doc, &["spec", "scope"])
                    .as_str()
                    .unwrap_or_else(|| panic!("{kind}: spec.scope is a string"))
                    .to_string(),
            )
        })
        .collect();
    let want: Vec<(&str, String)> = SCOPES.iter().map(|(k, s)| (*k, s.to_string())).collect();
    assert_eq!(
        got, want,
        "the Scope column: every workload kind is Namespaced and `TrustRoster` alone is \
         Cluster-scoped, so `allowedClusterIds` does not sit where a namespace tenant can \
         widen its own allowlist and no workload kind escapes a namespaced RBAC rule"
    );
}

// ---------------------------------------------------------------------------
// The CEL seals
// ---------------------------------------------------------------------------

/// One CEL rule as the API server sees it: the schema path it is attached to,
/// its text and its message.
#[derive(Debug, Clone)]
struct Attached {
    path: Vec<String>,
    rule: String,
    message: String,
}

/// Every `x-kubernetes-validations` rule at or under `.spec`, with its path.
fn attached_rules(crd: &Value) -> Vec<Attached> {
    let mut out = Vec::new();
    collect_rules(spec_schema(crd), &mut vec!["spec".to_string()], &mut out);
    out
}

fn collect_rules(node: &Value, path: &mut Vec<String>, out: &mut Vec<Attached>) {
    if let Some(rules) = node.get("x-kubernetes-validations") {
        for entry in rules
            .as_sequence()
            .expect("x-kubernetes-validations is a list")
        {
            out.push(Attached {
                path: path.clone(),
                rule: entry
                    .get("rule")
                    .and_then(Value::as_str)
                    .expect("a validation carries a rule")
                    .to_string(),
                message: entry
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            });
        }
    }
    if let Some(props) = node.get("properties").and_then(Value::as_mapping) {
        for (k, v) in props {
            path.push(k.as_str().expect("a property name is a string").to_string());
            collect_rules(v, path, out);
            path.pop();
        }
    }
    if let Some(items) = node.get("items") {
        path.push("[]".to_string());
        collect_rules(items, path, out);
        path.pop();
    }
}

/// The `.spec` rules the emitter injects, per file, taken from the emitter's
/// own constants.
///
/// READ FROM THE CONSTANTS, COMPARED AGAINST THE FILE. The property is "the
/// checked-in YAML carries exactly the rules the emitter injects, in order",
/// which is only worth asserting if the two sides come from different places:
/// the left is `crds::*::SPEC_RULES`, the right is the bytes `kubectl apply`
/// will read.
fn expected_spec_rules(file: &str) -> Vec<(String, String)> {
    use weirkeeper::crds;
    let rules: &[crds::SpecRule] = match file {
        "kafkaclusters.yaml" | "approvals.yaml" | "trustrosters.yaml" => &crds::WHOLE_SPEC_SEAL,
        "backupschedules.yaml" => &crds::backup_schedule::SPEC_RULES,
        "backups.yaml" => &crds::backup::SPEC_RULES,
        "restores.yaml" => &crds::restore::SPEC_RULES,
        "backupdestinations.yaml" => &crds::backup_destination::SPEC_RULES,
        "topicdiscoveries.yaml" => &crds::topic_discovery::SPEC_RULES,
        "preflights.yaml" => &crds::preflight::SPEC_RULES,
        other => panic!("no `.spec` rule list is declared for {other} — add one beside its kind"),
    };
    rules
        .iter()
        .map(|r| (r.rule.to_string(), r.message.to_string()))
        .collect()
}

/// Every rule attached BELOW `.spec`, per file, as `(path, rule, message)`.
///
/// A LITERAL TABLE, and deliberately not derived from the emitter's loops. It
/// is the placement that matters — a cross-field rule one node too high is
/// evaluated when it has nothing to say, and one node too low is never
/// evaluated at all — so the expectation names the path.
fn expected_nested_rules(file: &str) -> Vec<(Vec<String>, String, String)> {
    use weirkeeper::crds;
    let mut out: Vec<(Vec<String>, String, String)> = Vec::new();
    let mut push = |path: &[&str], rule: &str, message: &str| {
        let mut full = vec!["spec".to_string()];
        full.extend(path.iter().map(|p| (*p).to_string()));
        out.push((full, rule.to_string(), message.to_string()));
    };
    match file {
        "kafkaclusters.yaml" => {
            for (path, rule, message) in crds::kafka_cluster::CONNECTION_RULES {
                push(path, rule, message);
            }
        }
        "backupdestinations.yaml" => {
            for (path, rule, message) in crds::backup_destination::NESTED_RULES {
                push(path, rule, message);
            }
        }
        "topicdiscoveries.yaml" => {
            let (path, rule, message) = crds::topic_discovery::REQUEST_RULE;
            push(path, rule, message);
        }
        "preflights.yaml" => {
            let (path, rule, message) = crds::preflight::REQUEST_RULE;
            push(path, rule, message);
            for (path, rule, message) in crds::preflight::NESTED_RULES {
                push(path, rule, message);
            }
        }
        _ => {}
    }
    out
}

/// Every `.spec` carries exactly the rules its kind declares, in order, and
/// nothing is attached to `spec.suspend`.
///
/// # What changed when the group grew past six kinds
///
/// The original form of this test asserted ONE rule per `.spec` and one
/// author of deeper rules. Both were true and neither was the property. A
/// kind's `.spec` may now carry several rules — `BackupDestination` carries
/// four, two of them transition rules — because a seal and a cross-field
/// validation are different questions and a validation rule has to run on
/// CREATE, where a transition rule is skipped. So the assertion is now
/// equality against the kind's own declared list, which fails on a dropped
/// rule, an added one, a reordered one and a message that travelled with the
/// wrong rule.
#[test]
fn every_spec_carries_exactly_its_declared_rules() {
    for file in FILES {
        let doc = crd(file);
        let rules = attached_rules(&doc);

        let on_spec: Vec<(String, String)> = rules
            .iter()
            .filter(|r| r.path == ["spec"])
            .map(|r| (r.rule.clone(), r.message.clone()))
            .collect();
        assert_eq!(
            on_spec,
            expected_spec_rules(file),
            "{file}: the checked-in `.spec` rules must be exactly the ones the emitter \
             injects, in order, each with its own message"
        );
        assert!(
            !on_spec.is_empty(),
            "{file}: a kind with no rule on `.spec` is a kind whose spec is not sealed"
        );

        // SORTED, AND WHY. `JSONSchemaProps::properties` is a `BTreeMap`, so
        // the rendered file walks the schema alphabetically and not in
        // injection order. The property asserted is the SET of (path, rule,
        // message) triples; a dropped rule, an added one, a rule at the wrong
        // path and a mispaired message all still fail.
        let mut deeper: Vec<(Vec<String>, String, String)> = rules
            .iter()
            .filter(|r| r.path != ["spec"])
            .map(|r| (r.path.clone(), r.rule.clone(), r.message.clone()))
            .collect();
        deeper.sort();
        let mut want = expected_nested_rules(file);
        want.sort();
        assert_eq!(
            deeper, want,
            "{file}: the rules below `.spec` must be exactly the ones the emitter injects, \
             AT EXACTLY THOSE PATHS"
        );
    }

    // Stated separately, because it is the mutant's target: NOTHING is
    // attached to `spec.suspend`. A rule there would be evaluated on the one
    // field that has to change.
    let schedule = crd("backupschedules.yaml");
    let on_suspend: Vec<Attached> = attached_rules(&schedule)
        .into_iter()
        .filter(|r| r.path == ["spec", "suspend"])
        .collect();
    assert!(
        on_suspend.is_empty(),
        "no CEL rule may be attached to `spec.suspend` — it is the one mutable field, and the \
         only `.spec` write the controller performs in tag 1. Got {on_suspend:?}"
    );

    // And the schedule seal still names every field but `suspend`, including
    // the one W6b added.
    let rule = &attached_rules(&schedule)
        .into_iter()
        .find(|r| r.path == ["spec"])
        .expect("the schedule seals its spec")
        .rule;
    for named in [
        "schedule",
        "sourceRef",
        "topics",
        "archive",
        "destinationRef",
        "concurrencyPolicy",
        "retention",
    ] {
        assert!(
            rule.contains(&format!("has(self.{named}) == has(oldSelf.{named})")),
            "backupschedules.yaml: `{named}` needs its `has(self.x) == has(oldSelf.x)` half, \
             which is what refuses the absent -> present transition; rule was:\n{rule}"
        );
        assert!(
            rule.contains(&format!("self.{named} == oldSelf.{named}")),
            "backupschedules.yaml: `{named}` must be compared against oldSelf; rule was:\n{rule}"
        );
    }
    assert!(
        !rule.contains("suspend"),
        "backupschedules.yaml: `suspend` is the one mutable field and must not appear in the \
         seal; rule was:\n{rule}"
    );
}

/// A transition rule below `.spec` is sound ONLY on a required sub-object, and
/// every one this group ships sits on one.
///
/// # The hole this closes
///
/// `seal_spec`'s module note is about optional fields: a transition rule is
/// evaluated only when `oldSelf` HAS the field, so a rule on an optional one
/// never fires on the absent → present transition and seals nothing. The two
/// check kinds deliberately put `self == oldSelf` on `spec.request` instead of
/// on `.spec`, which is sound **because `request` is required** — and would
/// silently stop being sound the moment somebody made it optional. That edit
/// is a one-word diff in `crds/topic_discovery.rs`; this test is what turns it
/// red.
#[test]
fn a_transition_rule_below_spec_sits_on_a_required_property() {
    for file in FILES {
        let doc = crd(file);
        for r in attached_rules(&doc) {
            if r.path == ["spec"] || !r.rule.contains("oldSelf") {
                continue;
            }
            // Walk to the rule's PARENT and read its `required` list.
            let mut node = spec_schema(&doc);
            let last = r.path.last().expect("a non-empty path");
            for key in &r.path[1..r.path.len() - 1] {
                node = at(node, &["properties", key.as_str()]);
            }
            assert!(
                required(node).contains(last),
                "{file}: the transition rule at {:?} sits on `{last}`, which its parent does \
                 NOT list as required. A transition rule on an OPTIONAL property is not \
                 evaluated when the stored object lacks it, so this rule would seal nothing \
                 on exactly the update it exists to refuse. Either make `{last}` required or \
                 move the rule to `.spec`.",
                r.path
            );
        }
    }
}

// --------------------------------------------------- the CEL evaluator
//
// A tiny recursive-descent evaluator over the fragment of CEL these rules use:
// `&&`, `||`, `!`, parentheses, `==`, `has(path)` and dotted paths rooted at
// `self` / `oldSelf`. It PANICS on anything it does not recognise rather than
// returning a default, because an evaluator that silently answers `true` for
// an expression it did not understand is a test that cannot fail.
//
// `&&` binds tighter than `||`, as in CEL.

/// A value in the fragment: a boolean, or a resolved path that may be absent.
///
/// A STRING AND AN INTEGER LITERAL ARE `Field(Some(...))`, not variants of
/// their own, so `==` between a literal and a path is the same comparison the
/// API server makes and needs no per-type arm.
#[derive(Debug, Clone, PartialEq)]
enum Cel {
    Bool(bool),
    /// `None` is "the field is not present", which is what `has()` reports on.
    Field(Option<Value>),
}

impl Cel {
    fn truth(&self, src: &str) -> bool {
        match self {
            Cel::Bool(b) => *b,
            Cel::Field(Some(Value::Bool(b))) => *b,
            Cel::Field(_) => panic!("a path is not a boolean in `{src}`"),
        }
    }
}

struct Cursor<'a> {
    toks: Vec<String>,
    i: usize,
    src: &'a str,
    new: &'a Value,
    old: &'a Value,
    /// Inside the UNTAKEN branch of a ternary.
    ///
    /// CEL EVALUATES ONE BRANCH, AND SO MUST THIS. The sentinel rule's `else`
    /// arm reads `self.archive.url.startsWith(...)`, and its `then` arm reads
    /// `self.destinationRef.name`; evaluating both would make one of them
    /// touch a field that is absent by construction and turn a correct rule
    /// into a panic. While this is set the parser still CONSUMES the branch —
    /// so a malformed expression in it is still caught — but every operation
    /// that would inspect a value answers a placeholder instead.
    skip: bool,
}

/// A string-literal token, marked so it cannot collide with an identifier.
const STR: char = '\u{1}';

fn tokenize(rule: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes: Vec<char> = rule.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '(' || c == ')' || c == '.' || c == '?' || c == ':' || c == '+' {
            out.push(c.to_string());
            i += 1;
        } else if c == '\'' || c == '"' {
            // A CEL string literal. `\\` and `\'` are the only escapes these
            // rules use; anything else is taken verbatim, and an unterminated
            // literal is a panic rather than a silent truncation.
            let quote = c;
            let mut lit = String::new();
            i += 1;
            loop {
                assert!(
                    i < bytes.len(),
                    "unterminated string literal in rule: {rule}"
                );
                let ch = bytes[i];
                if ch == '\\' {
                    assert!(i + 1 < bytes.len(), "trailing backslash in rule: {rule}");
                    lit.push(bytes[i + 1]);
                    i += 2;
                    continue;
                }
                if ch == quote {
                    i += 1;
                    break;
                }
                lit.push(ch);
                i += 1;
            }
            out.push(format!("{STR}{lit}"));
        } else if c == '&' || c == '|' {
            assert!(
                i + 1 < bytes.len() && bytes[i + 1] == c,
                "unexpected single `{c}` in rule: {rule}"
            );
            out.push(format!("{c}{c}"));
            i += 2;
        } else if c == '=' {
            assert!(
                i + 1 < bytes.len() && bytes[i + 1] == '=',
                "unexpected single `=` in rule: {rule}"
            );
            out.push("==".to_string());
            i += 2;
        } else if c == '!' {
            if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                out.push("!=".to_string());
                i += 2;
            } else {
                out.push("!".to_string());
                i += 1;
            }
        } else if c == '<' || c == '>' {
            if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                out.push(format!("{c}="));
                i += 2;
            } else {
                out.push(c.to_string());
                i += 1;
            }
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_alphanumeric() || bytes[i] == '_') {
                i += 1;
            }
            out.push(bytes[start..i].iter().collect());
        } else {
            panic!("unexpected character `{c}` in rule: {rule}");
        }
    }
    out
}

impl<'a> Cursor<'a> {
    fn peek(&self) -> Option<&str> {
        self.toks.get(self.i).map(String::as_str)
    }
    fn next(&mut self) -> String {
        let t = self
            .toks
            .get(self.i)
            .unwrap_or_else(|| panic!("rule ended early: {}", self.src))
            .clone();
        self.i += 1;
        t
    }
    fn expect(&mut self, want: &str) {
        let got = self.next();
        assert_eq!(got, want, "expected `{want}` in rule: {}", self.src);
    }
    /// `v.truth()`, or `false` inside an untaken branch.
    fn truth(&self, v: &Cel) -> bool {
        if self.skip {
            return false;
        }
        v.truth(self.src)
    }
    /// Parse `f` with the untaken-branch flag forced on, and discard its value.
    fn skipped<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        let was = self.skip;
        self.skip = true;
        let v = f(self);
        self.skip = was;
        v
    }

    /// The whole fragment: a conditional, which is CEL's lowest precedence.
    fn expr(&mut self) -> Cel {
        let cond = self.or_expr();
        if self.peek() != Some("?") {
            return cond;
        }
        self.next();
        let taken = self.truth(&cond);
        let then = if taken && !self.skip {
            self.expr()
        } else {
            self.skipped(Self::expr)
        };
        self.expect(":");
        let otherwise = if !taken && !self.skip {
            self.expr()
        } else {
            self.skipped(Self::expr)
        };
        if taken {
            then
        } else {
            otherwise
        }
    }

    fn or_expr(&mut self) -> Cel {
        let mut acc = self.and_expr();
        while self.peek() == Some("||") {
            self.next();
            let rhs = self.and_expr();
            acc = Cel::Bool(self.truth(&acc) || self.truth(&rhs));
        }
        acc
    }
    fn and_expr(&mut self) -> Cel {
        let mut acc = self.unary();
        while self.peek() == Some("&&") {
            self.next();
            let rhs = self.unary();
            acc = Cel::Bool(self.truth(&acc) && self.truth(&rhs));
        }
        acc
    }
    fn unary(&mut self) -> Cel {
        if self.peek() == Some("!") {
            self.next();
            let v = self.unary();
            return Cel::Bool(!self.truth(&v));
        }
        let lhs = self.additive();
        match self.peek() {
            Some("==") => {
                self.next();
                let rhs = self.additive();
                Cel::Bool(lhs == rhs)
            }
            Some("!=") => {
                self.next();
                let rhs = self.additive();
                Cel::Bool(lhs != rhs)
            }
            Some("<=") => {
                self.next();
                let rhs = self.additive();
                Cel::Bool(self.number(&lhs) <= self.number(&rhs))
            }
            _ => lhs,
        }
    }
    /// String concatenation — the one `+` these rules use.
    fn additive(&mut self) -> Cel {
        let mut acc = self.primary();
        while self.peek() == Some("+") {
            self.next();
            let rhs = self.primary();
            if self.skip {
                continue;
            }
            let joined = format!("{}{}", self.string(&acc), self.string(&rhs));
            acc = Cel::Field(Some(Value::String(joined)));
        }
        acc
    }
    fn primary(&mut self) -> Cel {
        match self.peek() {
            Some("(") => {
                self.next();
                let v = self.expr();
                self.expect(")");
                v
            }
            Some("has") => {
                self.next();
                self.expect("(");
                let v = self.path();
                self.expect(")");
                match v {
                    Cel::Field(f) => Cel::Bool(f.is_some()),
                    Cel::Bool(_) => panic!("has() takes a path, in rule: {}", self.src),
                }
            }
            Some("size") => {
                self.next();
                self.expect("(");
                let v = self.expr();
                self.expect(")");
                if self.skip {
                    return Cel::Field(Some(Value::Number(0.into())));
                }
                let n = match v {
                    Cel::Field(Some(Value::String(s))) => s.chars().count(),
                    Cel::Field(Some(Value::Sequence(q))) => q.len(),
                    Cel::Field(Some(Value::Mapping(m))) => m.len(),
                    other => panic!(
                        "size() takes a string, list or map; got {other:?} in `{}`",
                        self.src
                    ),
                };
                Cel::Field(Some(Value::Number((n as u64).into())))
            }
            Some(t) if t.starts_with(STR) => {
                let lit = self.next();
                Cel::Field(Some(Value::String(lit[STR.len_utf8()..].to_string())))
            }
            Some(t) if t.chars().all(|c| c.is_ascii_digit()) => {
                let lit = self.next();
                let n: u64 = lit.parse().expect("an integer literal");
                Cel::Field(Some(Value::Number(n.into())))
            }
            Some(_) => self.path(),
            None => panic!("rule ended early: {}", self.src),
        }
    }
    fn path(&mut self) -> Cel {
        let root = self.next();
        let mut cur = match root.as_str() {
            "self" => Some(self.new.clone()),
            "oldSelf" => Some(self.old.clone()),
            other => panic!("unknown root `{other}` in rule: {}", self.src),
        };
        loop {
            if self.peek() != Some(".") {
                break;
            }
            self.next();
            let field = self.next();
            if self.peek() == Some("(") {
                // A method call. `startsWith` is the only one this fragment
                // evaluates; `matches` is regex and is deliberately NOT
                // evaluated here (see `REGEX_ONLY_RULES`), because a second
                // regex engine in a test would be a second answer to the
                // question the API server already answers.
                self.next();
                let arg = self.expr();
                self.expect(")");
                assert_eq!(
                    field, "startsWith",
                    "unsupported CEL method `{field}` in rule: {}",
                    self.src
                );
                if self.skip {
                    return Cel::Bool(false);
                }
                let recv = self.string(&Cel::Field(cur.clone()));
                let needle = self.string(&arg);
                return Cel::Bool(recv.starts_with(&needle));
            }
            cur = cur.and_then(|v| v.get(field.as_str()).cloned());
            // An explicit YAML `null` is "absent" for `has()`, which is what
            // the API server does with a null-valued optional property.
            if matches!(cur, Some(Value::Null)) {
                cur = None;
            }
        }
        Cel::Field(cur)
    }

    fn string(&self, v: &Cel) -> String {
        match v {
            Cel::Field(Some(Value::String(s))) => s.clone(),
            other => panic!("expected a string, got {other:?} in rule: {}", self.src),
        }
    }
    fn number(&self, v: &Cel) -> i64 {
        match v {
            Cel::Field(Some(Value::Number(n))) => n.as_i64().expect("an integer"),
            other => panic!("expected a number, got {other:?} in rule: {}", self.src),
        }
    }
}

/// Evaluate `rule` with `self` = `new` and `oldSelf` = `old`.
fn eval(rule: &str, new: &Value, old: &Value) -> bool {
    let mut c = Cursor {
        toks: tokenize(rule),
        i: 0,
        src: rule,
        new,
        old,
        skip: false,
    };
    let v = c.expr();
    assert_eq!(
        c.i,
        c.toks.len(),
        "the rule was not fully consumed, so this evaluation means nothing: {rule}"
    );
    v.truth(rule)
}

/// The rules this evaluator deliberately does not evaluate: the regex-only
/// ones.
///
/// EVALUATING THEM HERE WOULD BE A SECOND ANSWER. `matches()` is RE2 inside
/// the API server; a Rust `regex` here would agree most of the time and
/// disagree exactly where a subtle pattern matters. These are asserted by TEXT
/// in `the_destination_rules_are_the_decisions_text` and exercised for real by
/// the live CEL probe recorded in the task report.
fn is_regex_only(rule: &str) -> bool {
    rule.contains(".matches(")
}

/// Evaluate every rule the CHECKED-IN CRD attaches, under the API server's
/// transition semantics, and AND the results.
///
/// THE SEMANTICS ARE THE POINT, and they are what kills the "put the rule on
/// each field instead" mutant. A transition rule — one that mentions `oldSelf`
/// — is evaluated only when `oldSelf` EXISTS at the rule's path. An
/// object-level rule on `.spec` therefore runs on every update; a per-field
/// rule on `.spec.retention` does not run at all when the old object had no
/// `retention`, and its clause contributes a vacuous `true`.
fn eval_attached(crd: &Value, new_spec: &Value, old_spec: &Value) -> bool {
    let rules = attached_rules(crd);
    assert!(
        !rules.is_empty(),
        "this CRD attaches no CEL rule at all, so there is nothing to evaluate"
    );
    let mut verdict = true;
    for r in &rules {
        // `r.path[0]` is always "spec"; navigate the rest.
        let sub = |root: &Value| -> Option<Value> {
            let mut cur = Some(root.clone());
            for key in &r.path[1..] {
                cur = cur.and_then(|v| v.get(key.as_str()).cloned());
                if matches!(cur, Some(Value::Null)) {
                    cur = None;
                }
            }
            cur
        };
        if is_regex_only(&r.rule) {
            continue;
        }
        let (Some(new_at), Some(old_at)) = (sub(new_spec), sub(old_spec)) else {
            // Not evaluated: `oldSelf` (or `self`) has no value at this path.
            continue;
        };
        verdict = verdict && eval(&r.rule, &new_at, &old_at);
    }
    verdict
}

fn yaml(text: &str) -> Value {
    serde_yaml::from_str(text).expect("the fixture is YAML")
}

/// The hole a per-field rule leaves open, closed: an OPTIONAL field cannot be
/// added on update.
///
/// A table over `(oldSelf, self, expected)` against the rule text the CRD
/// actually carries. Case 1 is the named one — `retention` absent, then
/// present — and cases 8 and 9 are the same hole one level down, inside
/// `retention` and inside `archive`.
#[test]
fn an_absent_optional_field_cannot_be_added_on_update() {
    let doc = crd("backupschedules.yaml");

    let base = "\
schedule: '0 3 * * *'
sourceRef:
  name: prod
topics:
- orders
archive:
  url: s3://bucket/archive
suspend: false
";
    let with_retention = format!("{base}retention:\n  keepLast: 3\n");
    let with_forbid = format!("{base}concurrencyPolicy: Forbid\n");

    let cases: Vec<(&str, Value, Value, bool)> = vec![
        (
            "an absent optional field is ADDED on update",
            yaml(base),
            yaml(&with_retention),
            false,
        ),
        (
            "a present optional field is REMOVED on update",
            yaml(&with_retention),
            yaml(base),
            false,
        ),
        (
            "nothing changes, no retention",
            yaml(base),
            yaml(base),
            true,
        ),
        (
            "nothing changes, with retention",
            yaml(&with_retention),
            yaml(&with_retention),
            true,
        ),
        (
            "only suspend flips",
            yaml(base),
            yaml(&base.replace("suspend: false", "suspend: true")),
            true,
        ),
        (
            "the cron expression changes",
            yaml(base),
            yaml(&base.replace("'0 3 * * *'", "'0 4 * * *'")),
            false,
        ),
        (
            "the topic list changes",
            yaml(base),
            yaml(&base.replace("- orders", "- orders\n- payments")),
            false,
        ),
        (
            "the concurrency policy changes",
            yaml(&with_forbid),
            yaml(&with_forbid.replace("Forbid", "Allow")),
            false,
        ),
        (
            "a nested optional inside retention is added",
            yaml(&with_retention),
            yaml(&format!("{base}retention:\n  keepLast: 3\n  keepDays: 7\n")),
            false,
        ),
        (
            "a nested optional inside archive is added",
            yaml(base),
            yaml(&base.replace(
                "  url: s3://bucket/archive",
                "  url: s3://bucket/archive\n  secretRef:\n    name: creds",
            )),
            false,
        ),
    ];

    for (name, old, new, expected) in cases {
        let got = eval_attached(&doc, &new, &old);
        assert_eq!(
            got, expected,
            "case `{name}`: the checked-in BackupSchedule seal evaluated to {got}, expected \
             {expected}.\nold: {old:?}\nnew: {new:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// The field shapes later tasks read by name
// ---------------------------------------------------------------------------

/// Interface **I17**: `signingKeys[]` carries key material, exactly as
/// `approverKeys[]` does.
///
/// A `signingKeyIds: [string]` shape is a CRD SCHEMA ERROR, not a degraded
/// mode: Task 24's `verify_evidence` resolves a runner's signing key from this
/// roster and calls `verify_detached`, and an id alone gives it nothing to
/// verify against — every verification would return `NotAttempted`,
/// `status.evidence.verification.result` could never be `Valid`, and Phase B's
/// exit criterion would be unreachable.
#[test]
fn the_roster_carries_key_material_for_both_lists() {
    let doc = crd("trustrosters.yaml");
    let spec = spec_schema(&doc);

    assert!(
        spec.get("properties")
            .and_then(|p| p.get("signingKeyIds"))
            .is_none(),
        "`signingKeyIds` is the unbuildable shape this field replaced; it must not exist"
    );

    for list in ["approverKeys", "signingKeys"] {
        let node = at(spec, &["properties", list]);
        assert_eq!(
            node.get("type").and_then(Value::as_str),
            Some("array"),
            "{list} is a list"
        );
        let items = at(node, &["items"]);
        assert_eq!(
            required(items),
            vec!["keyId".to_string(), "spkiPem".to_string()],
            "{list}[] must REQUIRE both `keyId` and `spkiPem`: an id with no key material \
             makes every verification NotAttempted"
        );
        let props = items
            .get("properties")
            .and_then(Value::as_mapping)
            .expect("the item schema has properties");
        let mut names: Vec<&str> = props
            .keys()
            .map(|k| k.as_str().expect("a property name"))
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["keyId", "notAfter", "spkiPem", "subject"],
            "{list}[] carries exactly the four fields of a roster key entry"
        );
        assert_eq!(
            at(items, &["properties", "spkiPem", "type"]).as_str(),
            Some("string"),
            "{list}[].spkiPem is the PEM text"
        );
    }

    // Declared here so no consumer derives expiry from a clock the controller
    // does not share with it (critique C L4).
    let status = status_schema(&doc);
    let expired = at(status, &["properties", "expiredKeyIds"]);
    assert_eq!(expired.get("type").and_then(Value::as_str), Some("array"));
    assert_eq!(
        at(expired, &["items", "type"]).as_str(),
        Some("string"),
        "status.expiredKeyIds is a list of key ids"
    );

    assert_eq!(
        at(&doc, &["spec", "scope"]).as_str(),
        Some("Cluster"),
        "the TrustRoster is the one CLUSTER-scoped kind: `allowedClusterIds` must not sit \
         where a namespace tenant can widen its own allowlist"
    );
}

/// Interface **I18**, and critique C H3: four required spec fields, and the
/// documents are the verbatim UTF-8 text.
#[test]
fn approval_spec_has_four_fields_and_no_base64() {
    let doc = crd("approvals.yaml");
    let spec = spec_schema(&doc);

    // `required` is compared as a SET — see `required()`'s note on why the
    // emitted order is lexicographic.
    assert_eq!(
        required(spec),
        vec![
            "approvalBytes".to_string(),
            "planHash".to_string(),
            "sidecarBytes".to_string(),
            "subjectRef".to_string(),
        ],
        "`Approval.spec` requires exactly subjectRef, planHash, approvalBytes and \
         sidecarBytes: a create form that posts only the two documents posts an object the \
         schema rejects"
    );
    let props = spec
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("spec has properties");
    assert_eq!(
        props.len(),
        4,
        "`Approval.spec` has FOUR fields and no fifth; got {:?}",
        props.keys().collect::<Vec<_>>()
    );

    for field in ["approvalBytes", "sidecarBytes"] {
        let node = at(spec, &["properties", field]);
        assert_eq!(
            node.get("type").and_then(Value::as_str),
            Some("string"),
            "{field} is a string"
        );
        assert!(
            node.get("format").is_none(),
            "{field} must declare NO `format` — `format: byte` is base64, which inserts an \
             encoding step between the approver's file and the hashed bytes. Got {:?}",
            node.get("format")
        );
        let description = node
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{field} carries a description"));
        for word in ["verbatim", "never base64"] {
            assert!(
                description.contains(word),
                "{field}'s description must say `{word}` — Tasks 20, 22 and 27 all need the \
                 answer, and Task 16's `evaluate(approval_bytes: &[u8], …)` signature \
                 sidesteps it. Description was:\n{description}"
            );
        }
    }
}

/// A Verified status carries the exact Kubernetes identity the controller
/// actually read. All five fields are required so an older or hand-written
/// partial status cannot authorize a same-name or cross-namespace Restore.
#[test]
fn approval_status_subject_provenance_is_complete() {
    let doc = crd("approvals.yaml");
    let subject = at(status_schema(&doc), &["properties", "verifiedSubjectRef"]);
    assert_eq!(
        required(subject),
        vec![
            "apiVersion".to_string(),
            "kind".to_string(),
            "name".to_string(),
            "namespace".to_string(),
            "uid".to_string(),
        ]
    );
    assert_eq!(
        enum_values(at(subject, &["properties", "kind"])),
        vec!["Restore", "Backup"]
    );
}

/// The subject kind enum is exactly `["Restore","Backup"]`. `Switchover` is
/// tag 2 and an `Approval` cannot name one.
#[test]
fn the_subject_kind_enum_has_no_switchover() {
    let doc = crd("approvals.yaml");
    let node = at(
        spec_schema(&doc),
        &["properties", "subjectRef", "properties", "kind"],
    );
    assert_eq!(
        enum_values(node),
        vec!["Restore", "Backup"],
        "the subject kind enum is exactly [\"Restore\",\"Backup\"] — the subject kind is part \
         of the bytes the approval binds, so an `Approval` whose planHash matches a `Restore` \
         is never accepted for a `Switchover`"
    );
}

/// `target.mode`'s enum, byte for byte. Task 9b's late-binding test compares
/// the Rust `TargetMode` against exactly this.
#[test]
fn restore_target_mode_accepts_only_scratch_or_new_topic() {
    let doc = crd("restores.yaml");
    let node = at(
        spec_schema(&doc),
        &["properties", "target", "properties", "mode"],
    );
    assert_eq!(
        enum_values(node),
        vec!["scratch", "newTopic"],
        "`target.mode` is exactly [\"scratch\",\"newTopic\"], in that order — the spellings \
         later Rust types must match (interface I33)"
    );
    // `scratch` is what a drill IS, which is why there is no `Drill` kind.
    let naming = at(
        spec_schema(&doc),
        &["properties", "target", "properties", "topicNaming"],
    );
    assert_eq!(
        at(naming, &["properties", "prefix", "type"]).as_str(),
        Some("string"),
        "`target.topicNaming.prefix` is what `status.newTopics` is built from"
    );
}

/// The auth mode enum, byte for byte. Task 6's late-binding test compares the
/// Rust `AuthSpec` against exactly this.
#[test]
fn kafka_cluster_auth_mode_accepts_only_plaintext_or_scram_sha512() {
    let doc = crd("kafkaclusters.yaml");
    let node = at(
        spec_schema(&doc),
        &["properties", "auth", "properties", "mode"],
    );
    assert_eq!(
        enum_values(node),
        vec!["plaintext", "scramSha512"],
        "`auth.mode` is exactly [\"plaintext\",\"scramSha512\"], in that order (interface I1)"
    );
    let auth = at(spec_schema(&doc), &["properties", "auth"]);
    let mut names: Vec<&str> = auth
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("auth has properties")
        .keys()
        .map(|k| k.as_str().expect("a property name"))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["mode", "secretRef", "tls", "tlsCa", "username"],
        "the auth block is {{mode, username, secretRef, tls, tlsCa}} and carries NO password field: \
         PLAT-07.1 added the CA REFERENCE and the credential key NAME, never a value"
    );
    let secret_ref = at(auth, &["properties", "secretRef", "properties"]);
    let mut secret_ref_names: Vec<&str> = secret_ref
        .as_mapping()
        .expect("secretRef has properties")
        .keys()
        .map(|k| k.as_str().expect("a property name"))
        .collect();
    secret_ref_names.sort();
    assert_eq!(
        secret_ref_names,
        vec!["name", "passwordKey"],
        "the credential reference is a Secret NAME and a data KEY, and has no namespace field: a \
         cross-namespace reference is a privilege-escalation surface"
    );
    for source in ["configMapKeyRef", "secretKeyRef"] {
        let node = at(
            auth,
            &["properties", "tlsCa", "properties", source, "properties"],
        );
        let mut keys: Vec<&str> = node
            .as_mapping()
            .expect("a CA source has properties")
            .keys()
            .map(|k| k.as_str().expect("a property name"))
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["key", "name"],
            "a CA source is {{name, key}} in THIS namespace — no namespace field"
        );
    }
}

/// Interface **I22**: `windowCovered` is two epoch-millisecond integers.
#[test]
fn window_covered_is_epoch_milliseconds() {
    let doc = crd("backups.yaml");
    let node = at(status_schema(&doc), &["properties", "windowCovered"]);
    let props = node
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("windowCovered has properties");
    let mut names: Vec<&str> = props.keys().map(|k| k.as_str().expect("a name")).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["fromMs", "toMs"],
        "`status.windowCovered` has exactly `fromMs` and `toMs`"
    );
    for field in ["fromMs", "toMs"] {
        let f = at(node, &["properties", field]);
        assert_eq!(
            f.get("type").and_then(Value::as_str),
            Some("integer"),
            "{field} is an integer — epoch milliseconds, the same shape as \
             `BackupReceipt.covered`"
        );
        assert_eq!(
            f.get("format").and_then(Value::as_str),
            Some("int64"),
            "{field} is int64"
        );
        assert_ne!(
            f.get("format").and_then(Value::as_str),
            Some("date-time"),
            "{field} is NOT an RFC 3339 string: the receipt these two fields mirror carries \
             integers, and Phase C's wizard would print the wrong default"
        );
    }
}

/// Interface **I34**: `Restore.status.objectives{rtoSeconds, rpoSeconds,
/// passRate, met}` and `Restore.status.integrity.partialReason`.
///
/// Declared here, produced by Task 20, read by Task 26's UI — by these exact
/// names. Spec §8 requires the UI to render both, and no other status field
/// carries them.
#[test]
fn restore_status_declares_the_objectives_block_and_the_partial_reason() {
    let doc = crd("restores.yaml");
    let status = status_schema(&doc);

    let objectives = at(status, &["properties", "objectives"]);
    let mut names: Vec<&str> = objectives
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("objectives has properties")
        .keys()
        .map(|k| k.as_str().expect("a name"))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["met", "passRate", "rpoSeconds", "rtoSeconds"],
        "`status.objectives` is exactly {{rtoSeconds, rpoSeconds, passRate, met}} — camelCase, \
         lifted from the scorecard's own `objectives` block"
    );
    assert_eq!(
        at(objectives, &["properties", "met", "type"]).as_str(),
        Some("boolean"),
        "`met` is the aggregate verdict; absent means unmeasurable, not satisfied"
    );
    assert_eq!(
        at(objectives, &["properties", "passRate", "type"]).as_str(),
        Some("number"),
        "`passRate` is a rate, not a count"
    );

    let integrity = at(status, &["properties", "integrity"]);
    let mut inames: Vec<&str> = integrity
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("integrity has properties")
        .keys()
        .map(|k| k.as_str().expect("a name"))
        .collect();
    inames.sort();
    assert_eq!(
        inames,
        vec!["level", "partialReason", "result"],
        "`status.integrity` carries `partialReason` beside `level` and `result` — a `partial` \
         with no reason is a badge an auditor cannot act on"
    );

    // `measured` is what the run achieved; `objectives` is what was asked
    // for. Both, or the UI cannot show the gap.
    let measured = at(status, &["properties", "measured"]);
    let mut mnames: Vec<&str> = measured
        .get("properties")
        .and_then(Value::as_mapping)
        .expect("measured has properties")
        .keys()
        .map(|k| k.as_str().expect("a name"))
        .collect();
    mnames.sort();
    assert_eq!(mnames, vec!["rpoSeconds", "rtoSeconds"]);

    // Critique C M4: both offset-report keys are declared here so no consumer
    // has to guess.
    let evidence = at(status, &["properties", "evidence"]);
    for field in [
        "scorecardKey",
        "scorecardSha256",
        "sidecarKey",
        "offsetReportKey",
        "offsetReportSha256",
        "verification",
    ] {
        assert!(
            evidence
                .get("properties")
                .and_then(|p| p.get(field))
                .is_some(),
            "`status.evidence.{field}` is declared here"
        );
    }
}

// ---------------------------------------------------------------------------
// ADR 0008 Amendment F: BackupDestination, TopicDiscovery, Preflight
// ---------------------------------------------------------------------------

/// `BackupDestination.spec` is the decision's table: the required set, the
/// enum spellings, the defaults, and the two immutables.
#[test]
fn the_destination_spec_is_the_decisions_table() {
    let doc = crd("backupdestinations.yaml");
    let spec = spec_schema(&doc);

    assert_eq!(
        required(spec),
        vec!["access", "storage", "transport"],
        "a destination is a LOCATION, a TRANSPORT and its GRANTS; `description` and \
         `readiness` are the two a user may leave out"
    );

    let storage = at(spec, &["properties", "storage"]);
    assert_eq!(
        required(storage),
        vec!["addressing", "bucket", "provider"],
        "`addressing` has NO DEFAULT on purpose: path-style versus virtual-hosted is a \
         property of the endpoint the operator is pointing at, and a default would make one \
         of the two silently wrong"
    );
    assert_eq!(
        enum_values(at(storage, &["properties", "provider"])),
        vec!["S3"],
        "one provider; a second is a reviewable event with its own validation rules"
    );
    assert_eq!(
        enum_values(at(storage, &["properties", "addressing"])),
        vec!["PathStyle", "VirtualHosted"],
        "the addressing spellings are the contract `logweir_core::destination::Addressing` \
         serialises to"
    );
    assert_eq!(
        at(storage, &["properties", "bucket", "pattern"]).as_str(),
        Some(weirkeeper::crds::backup_destination::BUCKET_PATTERN),
        "the bucket pattern is the one the emitter declares"
    );
    assert_eq!(
        at(storage, &["properties", "prefix", "maxLength"]).as_u64(),
        Some(512),
        "the prefix cap matches `logweir_core::destination::PREFIX_MAX_LEN`, so the API's 422 \
         and the API server's rejection agree"
    );
    assert_eq!(
        at(storage, &["properties", "endpoint", "maxLength"]).as_u64(),
        Some(2048),
        "the endpoint cap matches `logweir_core::destination::ENDPOINT_MAX_LEN`"
    );

    let transport = at(spec, &["properties", "transport"]);
    assert_eq!(
        required(transport),
        vec!["security"],
        "`security` is required and `caBundle` is not: trust material is optional, a \
         transport choice never is"
    );
    assert_eq!(
        enum_values(at(transport, &["properties", "security"])),
        vec!["TLS", "InsecureHTTP"],
        "the transport spellings are the contract \
         `logweir_core::destination::TransportSecurity` serialises to — and the ONLY place \
         plaintext HTTP is ever chosen"
    );
    assert_eq!(
        at(
            transport,
            &["properties", "caBundle", "properties", "key", "default"]
        )
        .as_str(),
        Some("ca.crt"),
        "the CA key default is `ca.crt`"
    );

    let access = at(spec, &["properties", "access"]);
    assert_eq!(
        required(access),
        vec!["archiveWrite"],
        "a destination nothing may write to is not a backup destination; the other three \
         grants have documented absent-field behaviour"
    );
    for role in ["archiveWrite", "archiveRead", "evidenceWrite"] {
        assert_eq!(
            enum_values(at(access, &["properties", role, "properties", "mode"])),
            vec!["SecretKeys", "WorkloadIdentity"],
            "{role}: two modes"
        );
    }
    assert_eq!(
        enum_values(at(
            access,
            &["properties", "evidenceRead", "properties", "mode"]
        )),
        vec![
            "SecretKeys",
            "WorkloadIdentity",
            "ControllerIdentity",
            "ArchiveReadGrant"
        ],
        "evidenceRead has two more modes, and neither of them takes a reference"
    );
    let secret = at(
        access,
        &["properties", "archiveWrite", "properties", "secret"],
    );
    assert_eq!(
        at(secret, &["properties", "accessKeyIdKey", "default"]).as_str(),
        Some("access-key-id"),
        "the key defaults are today's ARCHIVE_ACCESS_KEY / ARCHIVE_SECRET_KEY, so a \
         destination written with no key names projects what the existing Jobs project"
    );
    assert_eq!(
        at(secret, &["properties", "secretAccessKeyKey", "default"]).as_str(),
        Some("secret-access-key")
    );
    assert_eq!(
        required(secret),
        vec!["name"],
        "only the Secret NAME is required; the keys have defaults"
    );
    assert!(
        secret
            .get("properties")
            .and_then(|p| p.get("sessionTokenKey"))
            .is_some_and(|k| k.get("default").is_none()),
        "`sessionTokenKey` has NO default: a session token nobody configured is a token that \
         does not exist, and defaulting it would make every Secret look temporary"
    );

    assert_eq!(
        enum_values(at(
            spec,
            &["properties", "readiness", "properties", "writeProbe"]
        )),
        vec!["Disabled", "CreateOnlyMarker"],
        "the write probe is opt-in"
    );
    assert_eq!(
        at(
            spec,
            &[
                "properties",
                "readiness",
                "properties",
                "writeProbe",
                "default"
            ]
        )
        .as_str(),
        Some("Disabled"),
        "absent `readiness.writeProbe` means Disabled (D2 §11.1), and the schema says so \
         rather than leaving it to a controller"
    );
}

/// **NO CREDENTIAL VALUE, ANYWHERE, IN ANY OF THE THREE NEW SPECS.**
///
/// A SOURCE-SHAPED PROPERTY ASSERTED OVER THE SHIPPED SCHEMA, because the
/// shipped schema is what an API server will store. A destination holds
/// REFERENCES: Secret names and key names, which are public. A field named
/// `password`, `secret` with a string type, `accessKey`, `token` or
/// `credential` carrying a scalar would be a place an operator could paste a
/// key and a place `kubectl get -o yaml` would then print it.
#[test]
fn no_new_kind_declares_a_field_that_could_hold_a_credential() {
    const FORBIDDEN: [&str; 7] = [
        "password",
        "accesskey",
        "accesskeyid",
        "secretaccesskey",
        "sessiontoken",
        "credential",
        "privatekey",
    ];
    let mut found: Vec<String> = Vec::new();
    for file in [
        "backupdestinations.yaml",
        "topicdiscoveries.yaml",
        "preflights.yaml",
    ] {
        let doc = crd(file);
        let mut stack = vec![(vec!["spec".to_string()], spec_schema(&doc).clone())];
        while let Some((path, node)) = stack.pop() {
            if let Some(props) = node.get("properties").and_then(Value::as_mapping) {
                for (k, v) in props {
                    let name = k.as_str().expect("a property name").to_string();
                    let ty = v.get("type").and_then(Value::as_str).unwrap_or("");
                    if ty == "string" && FORBIDDEN.contains(&name.to_lowercase().as_str()) {
                        found.push(format!("{file}: {}.{name}", path.join(".")));
                    }
                    let mut next = path.clone();
                    next.push(name);
                    stack.push((next, v.clone()));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "these kinds carry references, never values. A scalar field with one of these names \
         is a place a credential can be pasted and a place `kubectl get -o yaml` prints it \
         back:\n{}",
        found.join("\n")
    );
}

/// The destination's rules are the decision's text, byte for byte — including
/// the two the in-process evaluator deliberately does not evaluate.
#[test]
fn the_destination_rules_are_the_decisions_text() {
    use weirkeeper::crds::backup_destination as d;
    let doc = crd("backupdestinations.yaml");
    let rules = attached_rules(&doc);
    let texts: Vec<&str> = rules.iter().map(|r| r.rule.as_str()).collect();

    for (id, rule) in [
        ("R1", d::R1_STORAGE_IMMUTABLE_RULE),
        ("R2", d::R2_TRANSPORT_IMMUTABLE_RULE),
        ("R3", d::R3_TRANSPORT_SCHEME_RULE),
        ("R4", d::R4_CA_REQUIRES_TLS_RULE),
        ("R5", d::R5_ENDPOINT_RULE),
        ("R6", d::R6_PREFIX_RULE),
        ("R7", d::R7_GRANT_SHAPE_RULE),
        ("R8", d::R8_EVIDENCE_READ_SHAPE_RULE),
        ("R9", d::R9_ARCHIVE_READ_GRANT_RULE),
    ] {
        assert!(
            texts.contains(&rule),
            "{id} is missing from the checked-in BackupDestination CRD. Rules are what the \
             API server enforces; a rule that is only in the decision document is a rule \
             nobody runs.\nwanted: {rule}\ngot: {texts:#?}"
        );
    }

    // R0 sits on the ROOT, which is the one node a rule may read
    // `self.metadata.name` from — `attached_rules` walks `.spec` and downward,
    // so it is read separately here.
    let root_rules = root_schema(&doc)
        .get("x-kubernetes-validations")
        .and_then(Value::as_sequence)
        .map(|v| {
            v.iter()
                .filter_map(|e| e.get("rule").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert_eq!(
        root_rules,
        vec![d::R0_NAME_RULE],
        "R0 is a name-length budget and `self.metadata.name` is readable ONLY at the schema \
         root; anywhere else it would not compile"
    );

    // R3 NEVER MENTIONS ADDRESSING, in either direction. This is defect G5 —
    // the UI turning path-style addressing into plaintext HTTP — written into
    // the schema so it cannot come back through the API server either.
    assert!(
        !d::R3_TRANSPORT_SCHEME_RULE.contains("addressing"),
        "R3 couples the endpoint SCHEME to the transport and nothing else. Naming \
         `addressing` here would make bucket addressing a transport decision, which is \
         exactly the defect this rule exists to close"
    );
    for rule in &texts {
        assert!(
            !(rule.contains("addressing") && rule.contains("security")),
            "no rule may derive transport security from addressing, or the reverse: `{rule}`"
        );
    }

    // The two regex-only rules are the ones the in-process evaluator skips,
    // and that set is asserted rather than assumed.
    let skipped: Vec<&&str> = texts.iter().filter(|r| is_regex_only(r)).collect();
    assert_eq!(
        skipped,
        vec![&d::R5_ENDPOINT_RULE, &d::R6_PREFIX_RULE],
        "only R5 and R6 use `matches()`; a third regex rule needs its own live-probe line in \
         the task report, because this file does not evaluate regexes"
    );
}

/// The sentinel is a pair, and half of it is refused.
///
/// A TABLE OVER THE RULE THE CRD ACTUALLY CARRIES, evaluated with the API
/// server's semantics. The rule is a VALIDATION rule (no `oldSelf`), so unlike
/// the seal beside it, it runs on CREATE — which is the only moment that
/// matters, because `Backup.spec` is immutable afterwards.
#[test]
fn the_destination_sentinel_binds_the_ref_to_the_url() {
    let rule = weirkeeper::crds::backup::DESTINATION_SENTINEL_RULE;
    let spec = |url: &str, dest: Option<&str>, secret: bool| -> Value {
        let mut text = format!("archive:\n  url: {url}\n");
        if secret {
            text.push_str("  secretRef:\n    name: creds\n");
        }
        if let Some(d) = dest {
            text.push_str(&format!("destinationRef:\n  name: {d}\n"));
        }
        yaml(&text)
    };

    let cases: Vec<(&str, Value, bool)> = vec![
        (
            "no ref, an ordinary url",
            spec("s3://bucket/archive", None, true),
            true,
        ),
        (
            "a ref and the matching sentinel",
            spec("logweir-destination://primary", Some("primary"), false),
            true,
        ),
        (
            "a ref and a sentinel naming a DIFFERENT destination",
            spec("logweir-destination://other", Some("primary"), false),
            false,
        ),
        (
            "a ref and an ordinary url — the run would write somewhere else",
            spec("s3://bucket/archive", Some("primary"), false),
            false,
        ),
        (
            "a ref, the right sentinel, but a credential reference beside it",
            spec("logweir-destination://primary", Some("primary"), true),
            false,
        ),
        (
            "the reserved scheme with NO ref at all",
            spec("logweir-destination://primary", None, false),
            false,
        ),
    ];
    for (name, value, expected) in cases {
        let got = eval(rule, &value, &value);
        assert_eq!(
            got, expected,
            "case `{name}`: the sentinel rule evaluated to {got}, expected {expected}.\n\
             rule: {rule}\nspec: {value:?}"
        );
    }

    // And the same text is on the schedule, which is what makes a schedule's
    // children inherit a consistent pair.
    assert_eq!(
        weirkeeper::crds::backup_schedule::DESTINATION_SENTINEL_RULE,
        rule,
        "a Backup and the BackupSchedule that creates it must agree about the sentinel, or \
         a child would be refused by a rule its parent passed"
    );
}

/// A `Restore` takes both destinations or neither, and its sentinel binds the
/// SOURCE archive.
#[test]
fn a_restore_takes_both_destinations_or_neither() {
    use weirkeeper::crds::restore as r;
    let spec = |src: Option<&str>, ev: Option<&str>, url: &str| -> Value {
        let mut text = format!("sourceArchive:\n  url: {url}\n");
        if let Some(s) = src {
            text.push_str(&format!("sourceDestinationRef:\n  name: {s}\n"));
        }
        if let Some(e) = ev {
            text.push_str(&format!("evidenceDestinationRef:\n  name: {e}\n"));
        }
        yaml(&text)
    };
    let cases: Vec<(&str, Value, bool)> = vec![
        ("neither", spec(None, None, "s3://b/a"), true),
        (
            "both",
            spec(
                Some("primary"),
                Some("evidence"),
                "logweir-destination://primary",
            ),
            true,
        ),
        (
            "source only — evidence would land wherever the inline archive pointed",
            spec(Some("primary"), None, "logweir-destination://primary"),
            false,
        ),
        (
            "evidence only",
            spec(None, Some("evidence"), "s3://b/a"),
            false,
        ),
    ];
    for (name, value, expected) in cases {
        assert_eq!(
            eval(r::DESTINATIONS_TOGETHER_RULE, &value, &value),
            expected,
            "case `{name}`"
        );
    }

    let mismatched = spec(
        Some("primary"),
        Some("evidence"),
        "logweir-destination://other",
    );
    assert!(
        !eval(r::DESTINATION_SENTINEL_RULE, &mismatched, &mismatched),
        "a sourceArchive sentinel naming a destination other than sourceDestinationRef must \
         be refused"
    );
}

/// The check kinds seal `spec.request` and let `cancelRequested` move one way.
#[test]
fn the_check_kinds_seal_the_request_and_cancel_moves_forward_only() {
    for (file, kind) in [
        ("topicdiscoveries.yaml", "TopicDiscovery"),
        ("preflights.yaml", "Preflight"),
    ] {
        let doc = crd(file);
        let spec = spec_schema(&doc);
        assert_eq!(
            required(spec),
            vec!["request"],
            "{kind}: `request` is REQUIRED — that is what makes a transition rule on it fire \
             on every update — and `cancelRequested` is not, because it has a default"
        );
        assert_eq!(
            at(spec, &["properties", "cancelRequested", "default"]).as_bool(),
            Some(false),
            "{kind}: absent `cancelRequested` means false"
        );

        let request_rule = attached_rules(&doc)
            .into_iter()
            .find(|r| r.path == ["spec", "request"] && r.rule.contains("oldSelf"))
            .unwrap_or_else(|| panic!("{kind}: `spec.request` carries no transition rule"));
        assert_eq!(
            request_rule.rule, "self == oldSelf",
            "{kind}: the request is sealed whole; a per-field seal would leave every optional \
             field inside it addable after creation"
        );

        let cancel_rule = attached_rules(&doc)
            .into_iter()
            .find(|r| r.path == ["spec"])
            .unwrap_or_else(|| panic!("{kind}: `.spec` carries no rule"));
        let with = |c: bool| yaml(&format!("cancelRequested: {c}\n"));
        for (name, old, new, expected) in [
            ("false stays false", with(false), with(false), true),
            ("false becomes true", with(false), with(true), true),
            ("true stays true", with(true), with(true), true),
            ("true falls back to false", with(true), with(false), false),
        ] {
            assert_eq!(
                eval(&cancel_rule.rule, &new, &old),
                expected,
                "{kind} case `{name}`: an uncancel would ask a reconciler to resurrect work \
                 it has provably stopped"
            );
        }
    }
}

/// `Preflight.spec.request` admits exactly the block its `operation` names.
#[test]
fn a_preflight_carries_only_the_block_its_operation_names() {
    use weirkeeper::crds::preflight as pf;
    let doc = crd("preflights.yaml");
    let request = at(spec_schema(&doc), &["properties", "request"]);
    assert_eq!(
        enum_values(at(request, &["properties", "operation"])),
        vec!["Backup", "Restore", "DestinationAccess"],
        "three operations, and P3 ties the block to the value"
    );
    assert_eq!(
        required(request),
        vec!["operation"],
        "only `operation` is required; which block must be present is P3's job, not the \
         structural schema's, because the schema cannot express `exactly one of`"
    );

    let req = |op: &str, block: &str| yaml(&format!("operation: {op}\n{block}:\n  x: 1\n"));
    for (name, value, expected) in [
        ("Backup with backup", req("Backup", "backup"), true),
        ("Backup with restore", req("Backup", "restore"), false),
        ("Restore with restore", req("Restore", "restore"), true),
        (
            "DestinationAccess with destinationAccess",
            req("DestinationAccess", "destinationAccess"),
            true,
        ),
        (
            "DestinationAccess with backup",
            req("DestinationAccess", "backup"),
            false,
        ),
        (
            "Backup with no block at all",
            yaml("operation: Backup\n"),
            false,
        ),
    ] {
        assert_eq!(
            eval(pf::P3_OPERATION_BLOCK_RULE, &value, &value),
            expected,
            "case `{name}`"
        );
    }

    // Two blocks at once is refused too, which is the half a naive
    // `has(the right one)` rule would miss.
    let two = yaml("operation: Backup\nbackup:\n  x: 1\nrestore:\n  y: 2\n");
    assert!(
        !eval(pf::P3_OPERATION_BLOCK_RULE, &two, &two),
        "P3 must refuse a second block beside the matching one"
    );
}

// ---------------------------------------------------------------------------
// The runner image, the recipe, and the drift gate
// ---------------------------------------------------------------------------

/// Interface **I15**: the runner image is named in exactly one place under
/// `crates/`.
///
/// THE NEEDLE IS BUILT FROM TWO PIECES ON PURPOSE. This file lives under
/// `crates/`, so a literal here would be a second occurrence and this test
/// would fail on itself. Splitting the string keeps the source bytes clean
/// while the needle at run time is the whole path.
///
/// Task 23 tightens this to the digest form; until then the tag is what is
/// checked in, and `job.rs`'s own comment records why (Global Constraint 37 —
/// nothing has been published, so no published digest exists to pin).
#[test]
fn the_runner_image_is_named_once() {
    let needle = format!("{}{}", "docker.io/vladyslavhaina/", "logweir");
    let root = repo_root();
    let expected = root.join("crates/weirkeeper/src/job.rs");

    let mut total = 0usize;
    let mut where_ = Vec::new();
    for (path, text) in files_under(&root.join("crates")) {
        let n = text.matches(needle.as_str()).count();
        if n > 0 {
            total += n;
            where_.push(format!("{} ({n}x)", path.display()));
        }
    }
    assert_eq!(
        total, 1,
        "the runner image must be named EXACTLY ONCE under `crates/` — a second occurrence is \
         how a digest bump updates one call site and misses another. Found: {where_:?}"
    );
    assert_eq!(
        where_,
        vec![format!("{} (1x)", expected.display())],
        "the one occurrence is `crates/weirkeeper/src/job.rs`'s RUNNER_IMAGE"
    );
    assert!(
        weirkeeper::job::RUNNER_IMAGE.starts_with(&needle),
        "RUNNER_IMAGE is under the literal docker.io/vladyslavhaina namespace (Global Constraint 24), \
         never a `<org>` placeholder: a shipped logweir.yaml carrying one is not applyable. \
         Got {}",
        weirkeeper::job::RUNNER_IMAGE
    );
}

/// Interface **I15**'s runtime half: `main` reads
/// `job::RUNNER_IMAGE_ENV` **once**, and through the predicate — Task 33.
///
/// THREE PROPERTIES, AND EACH IS A DEFECT THAT HAS HAPPENED IN THIS TREE.
///
/// 1. **One read.** A second `std::env::var` of the same variable is a second
///    decision, and the two can disagree — the `Backup` reconciler and the
///    probe reconciler would then create Jobs naming different images in the
///    same cluster. The value is read here and threaded, exactly as the
///    archive handle is (interface I13).
/// 2. **Through `job::configured_runner_image`.** A decision behind `fn main`
///    is reachable from no test at all, which is precisely how plan erratum
///    **E19(e)**'s wrong ERROR line on the empty archive URL survived to Task
///    24. The predicate takes the `Result` so a test can hand it `Ok("")`.
/// 3. **The variable's NAME is spelt in `job.rs` and nowhere else under
///    `crates/weirkeeper/src`.** The same argument
///    [`the_runner_image_is_named_once`] makes about the reference: a second
///    spelling is how a rename updates one site and misses another.
#[test]
fn main_reads_the_runner_image_override_once() {
    let root = repo_root();
    let main_rs = std::fs::read_to_string(root.join("crates/weirkeeper/src/main.rs"))
        .expect("crates/weirkeeper/src/main.rs is read");
    // Whitespace-free, so the assertion is about the CALL and not about how
    // rustfmt chose to break the line.
    let dense: String = main_rs.chars().filter(|c| !c.is_whitespace()).collect();

    let reads = dense
        .matches("std::env::var(weirkeeper::job::RUNNER_IMAGE_ENV")
        .count();
    assert_eq!(
        reads, 1,
        "`main` must read the runner-image variable EXACTLY ONCE; found {reads} reads. Two reads \
         are two decisions, and the reconcilers they feed would then create Jobs naming \
         different images in one cluster."
    );
    assert!(
        dense.contains("configured_runner_image(std::env::var(weirkeeper::job::RUNNER_IMAGE_ENV"),
        "the read must be the ARGUMENT of `job::configured_runner_image`: the predicate is the \
         whole of the decision (empty is unset — plan erratum E19(e)) and `main` supplies only \
         the read, because a decision behind `fn main` is reachable from no test at all"
    );
    assert_eq!(
        dense.matches("configured_runner_image(").count(),
        1,
        "and the predicate is called once"
    );

    // The one `info!` line, naming the image AND where it came from.
    assert!(
        main_rs.contains("\"the runner image every Job this controller creates will name\""),
        "`main` must log, once, which runner image this process will use — a controller that \
         silently used a different image from the one the install file names is the failure this \
         variable exists to fix, not to create"
    );
    assert!(
        main_rs.contains("\"shipped pin\"") && main_rs.contains("RUNNER_IMAGE_ENV"),
        "and that line must say WHERE the image came from: the shipped pin, or the environment \
         variable named by `job::RUNNER_IMAGE_ENV`"
    );

    // The variable's name is spelt in `job.rs` and nowhere else in the crate's
    // sources. Split, so this file is not itself an occurrence.
    let needle = format!("{}{}", "LOGWEIR_RUNNER_", "IMAGE");
    let expected = root.join("crates/weirkeeper/src/job.rs");
    let mut where_ = Vec::new();
    for (path, text) in files_under(&root.join("crates/weirkeeper/src")) {
        let n = text.matches(needle.as_str()).count();
        if n > 0 {
            where_.push(format!("{} ({n}x)", path.display()));
        }
    }
    assert_eq!(
        where_,
        vec![format!("{} (1x)", expected.display())],
        "the variable's name belongs to `job::RUNNER_IMAGE_ENV` and is spelt there once; every \
         other site names the constant"
    );
    assert_eq!(
        weirkeeper::job::RUNNER_IMAGE_ENV,
        "LOGWEIR_RUNNER_IMAGE",
        "and this is the spelling `scripts/demo-steps.sh`, `docs/kubernetes.md` §14 and the \
         Deployment's `set env` all use"
    );
}

/// Task 37's half of interface **I15**'s runtime shape: `main` reads
/// `job::RUNNER_PULL_POLICY_ENV` **once**, through the predicate, logs it once,
/// and **refuses to start** when the predicate refuses.
///
/// FOUR PROPERTIES, AND THE FOURTH IS THE ONE THIS VARIABLE ADDS.
///
/// 1. **One read.** A second `std::env::var` of the same variable is a second
///    decision, and the two can disagree — the `Backup` reconciler and the
///    probe reconciler would then create Jobs under different pull policies in
///    one cluster. Exactly the argument
///    [`main_reads_the_runner_image_override_once`] makes for the image.
/// 2. **Through `job::configured_runner_pull_policy`.** A decision behind
///    `fn main` is reachable from no test at all — which is how plan erratum
///    **E19(e)**'s wrong ERROR line survived to Task 24.
/// 3. **One `info!`, naming the policy AND its source.** A controller silently
///    running under a policy the operator did not set is the failure this
///    variable exists to fix, not to create.
/// 4. **A refusal EXITS.** `imagePullPolicy` is a closed set the API server
///    validates at Job CREATE, so a controller that started under `always`
///    would turn every `Backup` and every `Restore` into a rejected Job with
///    nothing but an API error per object to say why. The `Err` arm must
///    `return ExitCode::FAILURE`, not log and continue.
#[test]
fn main_reads_the_runner_pull_policy_once() {
    let root = repo_root();
    let main_rs = std::fs::read_to_string(root.join("crates/weirkeeper/src/main.rs"))
        .expect("crates/weirkeeper/src/main.rs is read");
    // Whitespace-free, so the assertion is about the CALL and not about how
    // rustfmt chose to break the line.
    let dense: String = main_rs.chars().filter(|c| !c.is_whitespace()).collect();

    let reads = dense
        .matches("std::env::var(weirkeeper::job::RUNNER_PULL_POLICY_ENV")
        .count();
    assert_eq!(
        reads, 1,
        "`main` must read the runner pull-policy variable EXACTLY ONCE; found {reads} reads. Two \
         reads are two decisions, and the reconcilers they feed would then create Jobs under \
         different policies in one cluster."
    );
    assert!(
        dense.contains(
            "configured_runner_pull_policy(std::env::var(weirkeeper::job::RUNNER_PULL_POLICY_ENV"
        ),
        "the read must be the ARGUMENT of `job::configured_runner_pull_policy`: the predicate is \
         the whole of the decision (empty is unset — plan erratum E19(e); a non-policy is a \
         refusal) and `main` supplies only the read"
    );
    assert_eq!(
        dense.matches("configured_runner_pull_policy(").count(),
        1,
        "and the predicate is called once"
    );

    // The one `info!` line, naming the policy AND where it came from.
    assert!(
        main_rs.contains("\"the runner pull policy every Job this controller creates will carry\""),
        "`main` must log, once, which pull policy this process will use — a controller silently \
         running under a policy the operator did not set is the failure this variable exists to \
         fix, not to create"
    );
    assert!(
        main_rs.contains("\"shipped constant\"") && main_rs.contains("RUNNER_PULL_POLICY_ENV"),
        "and that line must say WHERE the policy came from: the shipped constant, or the \
         environment variable named by `job::RUNNER_PULL_POLICY_ENV`"
    );

    // A refusal EXITS. The `Err` arm returns a failure code rather than
    // logging and carrying on under a policy the API server will reject.
    let err_arm = dense
        .find("Err(message)=>{")
        .expect("`main`'s pull-policy match carries an `Err(message)` arm");
    let tail = &dense[err_arm..err_arm + 200.min(dense.len() - err_arm)];
    assert!(
        tail.contains("returnExitCode::FAILURE"),
        "the `Err` arm must REFUSE TO START (`return ExitCode::FAILURE`), because a Job with an \
         invalid pull policy is rejected by the API server at every Backup instead: {tail}"
    );
    assert!(
        tail.contains("error!("),
        "and it must say so on the way out, at ERROR: {tail}"
    );

    // The variable's name is spelt in `job.rs` and nowhere else in the crate's
    // sources. Split, so this file is not itself an occurrence.
    let needle = format!("{}{}", "LOGWEIR_RUNNER_", "PULL_POLICY");
    let expected = root.join("crates/weirkeeper/src/job.rs");
    let mut where_ = Vec::new();
    for (path, text) in files_under(&root.join("crates/weirkeeper/src")) {
        let n = text.matches(needle.as_str()).count();
        if n > 0 {
            where_.push(format!("{} ({n}x)", path.display()));
        }
    }
    assert_eq!(
        where_,
        vec![format!("{} (1x)", expected.display())],
        "the variable's name belongs to `job::RUNNER_PULL_POLICY_ENV` and is spelt there once; \
         every other site names the constant"
    );
    assert_eq!(
        weirkeeper::job::RUNNER_PULL_POLICY_ENV,
        "LOGWEIR_RUNNER_PULL_POLICY",
        "and this is the spelling `charts/logweir/templates/deployment.yaml` renders"
    );
}

/// `just crds` regenerates the checked-in CRDs.
///
/// Membership only — not the recipe's length, not its line numbers, not the
/// absence of other recipes — so a later task appending to the justfile cannot
/// turn this red (the `just_lint_runs_the_one_signer_gate` pattern).
#[test]
fn the_crds_recipe_is_in_the_justfile() {
    let justfile =
        std::fs::read_to_string(repo_root().join("justfile")).expect("the justfile is read");
    let body = just_recipe(&justfile, "crds").expect(
        "the justfile must declare a `crds` recipe — it is how the checked-in CRDs are \
                 regenerated, and the drift gate has nothing to point at without it",
    );
    assert!(
        body.contains("emit_crds"),
        "the `crds` recipe must invoke the `emit_crds` example. Body was:\n{body}"
    );
    assert!(
        body.contains("config/crd"),
        "the `crds` recipe must write into `config/crd`, which is what CI diffs. Body \
         was:\n{body}"
    );
    // The schema line is untouched, and stays a sibling rather than a
    // dependency: the two gates are independent formats.
    assert!(
        just_recipe(&justfile, "schema").is_some_and(|b| b.contains("emit_schema")),
        "the `schema` recipe stays as it was — this task appends beside it"
    );
}

/// The third drift arm exists in CI, beside the schema arm.
#[test]
fn the_ci_workflow_carries_the_crd_drift_arm() {
    let ci = std::fs::read_to_string(repo_root().join(".github/workflows/ci.yml"))
        .expect("ci.yml is read");
    assert!(ci.lines().any(
        |line| !line.trim_start().starts_with('#') && line.contains("bash scripts/ci-check.sh")
    ));
    let checks = std::fs::read_to_string(repo_root().join("scripts/ci-check.sh"))
        .expect("shared checks are read");
    assert!(checks.lines().any(|line| line == "just crds-check"));
    assert!(checks.lines().any(|line| line == "just schema-check"));
    // The following in-process test compares every CRD to the actual emitter;
    // no workflow needs a duplicate list of the filenames.
}

/// THE DRIFT GATE, LOCALLY AND IN-PROCESS: the checked-in files are exactly
/// what the renderer produces.
///
/// The CI arm re-renders with `cargo run` and `diff -u`s the files, which is
/// the shipped gate. This test asserts the same property with no subprocess
/// and no shell, so a hand edit to `config/crd/` fails `cargo test` on a
/// laptop too — `ci.yml` mirrors `just gate` (green since 2026-09-12), and a
/// gate that lives only there runs only on a push; this one runs everywhere.
#[test]
fn the_checked_in_crds_are_what_the_emitter_renders() {
    let rendered = weirkeeper::crds::render_all();
    assert_eq!(
        rendered.len(),
        FILES.len(),
        "the emitter renders one document per kind; got {}",
        rendered.len()
    );
    let order: Vec<&str> = rendered.iter().map(|r| r.file_name).collect();
    assert_eq!(
        order,
        FILES.to_vec(),
        "the emitter's order is deterministic and is the kind order"
    );
    for r in &rendered {
        let path = crd_dir().join(r.file_name);
        let on_disk = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        // FIX ROUND 1, FINDING 5: REPORT THE FIRST DIFFERING LINE, NOT TWO
        // BLOBS. An `assert_eq!` of two ~10 KB YAML documents prints ~20 KB of
        // near-identical text and hides the one line that moved, which is the
        // whole information the failure carries. CI's arm is a `diff -u` and
        // reads well; the local half of the same gate now does too.
        if on_disk != r.yaml {
            let (line, want, got) = first_difference(&on_disk, &r.yaml);
            panic!(
                "{} has drifted from the emitter. Run `just crds` — never hand-edit a \
                 generated CRD.\n  first difference at line {line}:\n    checked in: \
                 {want}\n    rendered:   {got}",
                path.display()
            );
        }
    }
}

/// The first line at which two documents differ, as
/// `(1-based line number, the left line, the right line)`.
///
/// `<end of file>` stands in for a document that ran out of lines, and a pair
/// of them means the two differ only in trailing bytes that `lines()` does not
/// yield — a missing or a doubled final newline.
fn first_difference(left: &str, right: &str) -> (usize, String, String) {
    let eof = || "<end of file>".to_string();
    let mut l = left.lines();
    let mut r = right.lines();
    let mut n = 0usize;
    loop {
        n += 1;
        let (a, b) = (l.next(), r.next());
        if a.is_none() && b.is_none() {
            return (n, eof(), eof());
        }
        let (a, b) = (
            a.map(str::to_string).unwrap_or_else(eof),
            b.map(str::to_string).unwrap_or_else(eof),
        );
        if a != b {
            return (n, a, b);
        }
    }
}

// ===========================================================================
// LATE-BINDING AGREEMENT TESTS — hosted here, NOT owned here
// ===========================================================================
//
// Task 15b writes neither of the two tests below and consumes nothing from
// either task (critique B H21(b), interface I33). The `auth` block and
// `target.mode` are declared in this repository as CRD FIELD SHAPES only; the
// Rust `AuthSpec` (Task 6, interface I1) and `TargetMode` / `TopicNaming` /
// `WindowFloorSource` (Task 9b, interface I33) land in later slots and MUST
// match what is already here.
//
//   - `the_crd_auth_mode_enum_and_auth_spec_agree`  — Task 6,  slot 7
//   - `the_crd_mode_enum_and_target_mode_agree`     — Task 9b, slot 10
//
// Both assert BYTE equality, so the spellings this file already pins —
// `["plaintext", "scramSha512"]` and `["scratch", "newTopic"]`, in those
// orders, asserted by
// `kafka_cluster_auth_mode_accepts_only_plaintext_or_scram_sha512` and
// `restore_target_mode_accepts_only_scratch_or_new_topic` — are the contract.
// `enum_values` is the helper each of them needs for the CRD half.

/// **I33, Task 6's half.** `AuthSpec`'s serde tag values are exactly the
/// `KafkaCluster` CRD's `auth.mode` enum, in that order.
///
/// Three surfaces name this mode and all three have to agree: the YAML an
/// adopter writes into a `DrillSpec`/`BackupSpec` (`AuthSpec`, serde), the
/// custom resource an adopter applies (this CRD's enum), and the signed
/// documents a reader parses (`ReceiptAuth.mode`, and Task 5b's
/// `AuthSummary.mode`). A `kubectl apply` that succeeds and a `drill run` that
/// then refuses to parse the same string is the failure this closes — and it
/// was not hypothetical: until Task 5b's fix round the receipt's own field doc
/// described the value as `"scram-sha-512"`, a spelling nothing emitted.
///
/// The order is asserted too, not just the set: `enum_values` returns the
/// declaration order, the CRD's enum is what `kubectl explain` prints in that
/// order, and an adopter reading the two lists side by side should not have to
/// wonder whether they are the same list.
///
/// The mutant this exists for: spell `AuthSpec::mode_str()` as
/// `"scram-sha-512"` and this fails at assertion time, together with
/// `crates/logweir/tests/auth_binding.rs::
/// the_scorecard_auth_block_and_auth_spec_agree`. That is the whole point of a
/// same-slot late binding having a test rather than a comment.
#[test]
fn the_crd_auth_mode_enum_and_auth_spec_agree() {
    let doc = crd("kafkaclusters.yaml");
    let crd_enum = enum_values(at(
        spec_schema(&doc),
        &["properties", "auth", "properties", "mode"],
    ));

    // The Rust half: the SERDE TAG of each variant, read out of serde itself
    // rather than retyped, so this cannot pass against a type whose wire
    // spelling has drifted from its variant name.
    let rust_tags: Vec<String> = [
        logweir_core::spec::AuthSpec::Plaintext,
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        },
    ]
    .iter()
    .map(|a| {
        serde_json::to_value(a)
            .expect("AuthSpec serialises")
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .expect("`#[serde(tag = \"mode\")]`")
            .to_string()
    })
    .collect();

    assert_eq!(
        rust_tags, crd_enum,
        "`AuthSpec`'s serde tags and the CRD's `auth.mode` enum must be the same two strings in \
         the same order (interface I33)"
    );
    assert_eq!(crd_enum, vec!["plaintext", "scramSha512"]);

    // And `mode_str()` — the accessor the two documents are filled from — is
    // the same string again, so a document cannot carry a third spelling.
    assert_eq!(
        logweir_core::spec::AuthSpec::Plaintext.mode_str(),
        crd_enum[0]
    );
    assert_eq!(
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: true
        }
        .mode_str(),
        crd_enum[1]
    );

    // NO THIRD MODE. The CRD's own doc comment says `mtls`, `gssapi`,
    // `oauthbearer` and `scramSha256` are not in tag 1; the Rust enum must not
    // have quietly grown one either.
    assert_eq!(rust_tags.len(), 2, "{rust_tags:?}");
}

/// **I33, Task 9b's half.** `TargetMode`'s serde names are exactly the
/// `Restore` CRD's `target.mode` enum, in that order.
///
/// Two surfaces name this mode and both have to agree: the custom resource an
/// adopter applies (this CRD's enum, pinned independently by
/// `restore_target_mode_accepts_only_scratch_or_new_topic`) and the YAML an
/// adopter writes into a `RestoreSpec` (`TargetMode`, serde). Task 20 renders
/// `Restore.spec.planBytes` from the former into the latter, so a spelling
/// that differs between them is a `kubectl apply` that succeeds followed by a
/// `restore run` that cannot parse its own plan.
///
/// **BY VALUE, out of serde itself, not retyped.** Each variant is serialised
/// and the resulting string read back, so this cannot pass against a type
/// whose wire spelling has drifted from its variant name. The ORDER is
/// asserted too, not just the set: `enum_values` returns the CRD's declaration
/// order, which is the order `kubectl explain` prints, and an adopter reading
/// the two lists side by side should not have to wonder whether they are the
/// same list.
///
/// The mutant this exists for: spell the CRD-facing mode `new-topic` instead of
/// `newTopic` — drop `#[serde(rename_all = "camelCase")]` from `TargetMode`, or
/// rename the variant. Either fails HERE, at assertion time.
#[test]
fn the_crd_mode_enum_and_target_mode_agree() {
    let doc = crd("restores.yaml");
    let crd_enum = enum_values(at(
        spec_schema(&doc),
        &["properties", "target", "properties", "mode"],
    ));

    // The Rust half: each variant's SERDE representation, read out of serde.
    // `TargetMode` is a unit enum, so `to_value` is the bare string.
    let rust_names: Vec<String> = [
        logweir_core::spec::TargetMode::Scratch,
        logweir_core::spec::TargetMode::NewTopic,
    ]
    .iter()
    .map(|m| {
        serde_json::to_value(m)
            .expect("TargetMode serialises")
            .as_str()
            .expect("a unit enum serialises to a string")
            .to_string()
    })
    .collect();

    assert_eq!(
        rust_names, crd_enum,
        "`TargetMode`'s serde names and the CRD's `target.mode` enum must be the same two \
         strings in the same order (interface I33)"
    );
    assert_eq!(crd_enum, vec!["scratch", "newTopic"]);

    // And `Display` — which is what a refusal message interpolates — is the
    // same string again, so a run cannot report a third spelling of its own
    // mode.
    assert_eq!(
        logweir_core::spec::TargetMode::Scratch.to_string(),
        crd_enum[0]
    );
    assert_eq!(
        logweir_core::spec::TargetMode::NewTopic.to_string(),
        crd_enum[1]
    );

    // NO THIRD MODE, and `scratch` is the DEFAULT: `#[serde(default)]` on
    // `TargetSpec::mode` is what makes every spec written before this field
    // existed mean exactly what it meant.
    assert_eq!(rust_names.len(), 2, "{rust_names:?}");
    assert_eq!(
        logweir_core::spec::TargetMode::default(),
        logweir_core::spec::TargetMode::Scratch
    );

    // `target.topicNaming.prefix` is the CRD's name for the block
    // `RestoreSpec`'s `target.topic_naming` carries — camelCase on the custom
    // resource, snake_case in the plan document, exactly like every other
    // field of this grammar. The TYPE is what both sides have to agree on.
    let naming = at(
        spec_schema(&doc),
        &["properties", "target", "properties", "topicNaming"],
    );
    assert_eq!(
        at(naming, &["properties", "prefix", "type"]).as_str(),
        Some("string")
    );
    let round_tripped: logweir_core::spec::TopicNaming =
        serde_yaml::from_str("prefix: \"restore-20260907T140500Z-\"")
            .expect("TopicNaming accepts a bare `prefix` string, like the CRD's block");
    assert_eq!(round_tripped.prefix, "restore-20260907T140500Z-");
}
