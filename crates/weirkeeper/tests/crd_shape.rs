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
const FILES: [&str; 14] = [
    "kafkaclusters.yaml",
    "backupschedules.yaml",
    "backups.yaml",
    "restores.yaml",
    "approvals.yaml",
    "trustrosters.yaml",
    "backupdestinations.yaml",
    "topicdiscoveries.yaml",
    "preflights.yaml",
    "trustpolicies.yaml",
    "protectionpolicies.yaml",
    "rehearsalschedules.yaml",
    "recoverycatalogs.yaml",
    "retentionpolicies.yaml",
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
        14,
        "ADR 0008 records fourteen kinds — Amendment A's six, Amendment F's \
         BackupDestination, TopicDiscovery and Preflight, and Amendment G's TrustPolicy, \
         ProtectionPolicy, RehearsalSchedule, RecoveryCatalog and RetentionPolicy. A \
         fifteenth needs its own amendment in docs/architecture.md, and this line is where \
         that decision becomes a diff."
    );

    // AND NO KIND IS REMOVED. `TrustRoster` stays served and reconciled,
    // deprecated in its description, so a cluster that has one keeps working
    // while `TrustPolicy` is adopted. Deleting a CRD deletes its objects, and
    // a roster's objects are the trust anchor of every archive in the cluster.
    assert!(
        kinds.iter().any(|k| k == "TrustRoster"),
        "TrustRoster is DEPRECATED, not removed: Amendment G replaces it with TrustPolicy \
         and keeps it served, because deleting the CRD would delete the trust anchor"
    );
    let roster = crd("trustrosters.yaml");
    let description = at(root_schema(&roster), &["description"])
        .as_str()
        .unwrap_or_default();
    assert!(
        description.starts_with("DEPRECATED"),
        "the shipped TrustRoster CRD must say it is deprecated in the description \
         `kubectl explain` prints; got: {description:?}"
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
const PRINTER_COLUMNS: [(&str, &[Column]); 14] = [
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
    (
        "TrustPolicy",
        &[
            ("DEFAULT", ".spec.default", "boolean"),
            ("KEYS", ".status.keyCount", "integer"),
            ("LOADED", ".status.loaded", "string"),
            ("BOUND", ".status.boundNamespaces[*]", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "ProtectionPolicy",
        &[
            ("SOURCE", ".spec.protects.sourceRef.name", "string"),
            ("HEALTH", ".status.health", "string"),
            (
                "POINT-AGE",
                ".status.lastAvailablePoint.ageSeconds",
                "integer",
            ),
            ("BASIS", ".status.availabilityBasis", "string"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "RehearsalSchedule",
        &[
            ("SCHEDULE", ".spec.schedule", "string"),
            ("SUSPEND", ".spec.suspend", "boolean"),
            ("TARGET", ".spec.target.clusterRef.name", "string"),
            ("LAST-OK", ".status.lastSucceeded.at", "date"),
            ("NEXT", ".status.nextFireTime", "date"),
            (
                "AUTHORIZED",
                ".status.conditions[?(@.type==\"Authorized\")].status",
                "string",
            ),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "RecoveryCatalog",
        &[
            ("DESTINATION", ".spec.destinationRef.name", "string"),
            ("POINTS", ".status.counts.total", "integer"),
            ("AVAILABLE", ".status.counts.available", "integer"),
            ("SYNCED", ".status.syncedAt", "date"),
            ("TRUNCATED", ".status.truncated", "boolean"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
    (
        "RetentionPolicy",
        &[
            ("DESTINATION", ".spec.destinationRef.name", "string"),
            ("MODE", ".spec.mode", "string"),
            ("ENFORCEMENT", ".status.enforcement", "string"),
            (
                "CANDIDATES",
                ".status.lastEvaluation.candidateCount",
                "integer",
            ),
            ("EVALUATED", ".status.lastEvaluation.at", "date"),
            ("AGE", ".metadata.creationTimestamp", "date"),
        ],
    ),
];

/// The brief's Scope column: five workload kinds Namespaced, the roster
/// Cluster.
///
/// A LITERAL for the same reason [`PRINTER_COLUMNS`] is. `TrustRoster` and its
/// replacement `TrustPolicy` are Cluster-scoped because the allowed target
/// cluster ids must not sit where a namespace tenant can widen their own
/// allowlist — "a roster whose name the subject supplies is a roster the
/// subject can choose"; every other kind is Namespaced because
/// [`weirkeeper::crds::LocalRef`] carries no namespace and a cross-namespace
/// reference is a privilege-escalation surface. A kind that quietly became
/// Cluster-scoped would move its objects out of every namespaced RBAC rule
/// Task 21 writes.
const SCOPES: [(&str, &str); 14] = [
    ("KafkaCluster", "Namespaced"),
    ("BackupSchedule", "Namespaced"),
    ("Backup", "Namespaced"),
    ("Restore", "Namespaced"),
    ("Approval", "Namespaced"),
    ("TrustRoster", "Cluster"),
    ("BackupDestination", "Namespaced"),
    ("TopicDiscovery", "Namespaced"),
    ("Preflight", "Namespaced"),
    ("TrustPolicy", "Cluster"),
    ("ProtectionPolicy", "Namespaced"),
    ("RehearsalSchedule", "Namespaced"),
    ("RecoveryCatalog", "Namespaced"),
    ("RetentionPolicy", "Namespaced"),
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
fn only_the_trust_kinds_are_cluster_scoped() {
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
        "the Scope column: every workload kind is Namespaced and only the two trust kinds \
         are Cluster-scoped, so the allowed target cluster ids do not sit where a namespace \
         tenant can widen them and no workload kind escapes a namespaced RBAC rule"
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
        "trustpolicies.yaml" => &crds::trust_policy::SPEC_RULES,
        "protectionpolicies.yaml" => &crds::protection_policy::SPEC_RULES,
        "rehearsalschedules.yaml" => &crds::rehearsal_schedule::SPEC_RULES,
        "recoverycatalogs.yaml" => &crds::recovery_catalog::SPEC_RULES,
        "retentionpolicies.yaml" => &crds::retention_policy::SPEC_RULES,
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
        "trustpolicies.yaml" => {
            for (path, rule, message) in crds::trust_policy::NESTED_RULES {
                push(path, rule, message);
            }
            for (path, rule, message) in crds::trust_policy::KEY_TRANSITION_RULES {
                push(path, rule, message);
            }
        }
        "protectionpolicies.yaml" => {
            for (path, rule, message) in crds::protection_policy::NESTED_RULES {
                push(path, rule, message);
            }
        }
        "rehearsalschedules.yaml" => {
            for (path, rule, message) in crds::rehearsal_schedule::NESTED_RULES {
                push(path, rule, message);
            }
        }
        "retentionpolicies.yaml" => {
            for (path, rule, message) in crds::retention_policy::NESTED_RULES {
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
        assert_eq!(
            on_spec.is_empty(),
            file == "protectionpolicies.yaml",
            "{file}: exactly one kind ships with no `.spec` rule, and it is ProtectionPolicy \
             — evaluation policy that is never an execution input, so there is no recorded \
             result an edit could rewrite. Every other kind seals something."
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

    // Stated separately, because it is the mutant's target: the BackupSchedule
    // seal names `sourceRef` AND NOTHING ELSE, and no rule is attached to any
    // editable field. PLAT-05.1 inverted this kind's mutability — a rule
    // attached below `.spec` here would be evaluated on a field an operator is
    // now told they may change.
    let schedule = crd("backupschedules.yaml");
    let below_spec: Vec<Attached> = attached_rules(&schedule)
        .into_iter()
        .filter(|r| r.path.len() > 1)
        .collect();
    assert!(
        below_spec.is_empty(),
        "no CEL rule may be attached below `BackupSchedule.spec`: every field but `sourceRef` \
         is editable policy, and `sourceRef`'s seal is the object-level R1. Got {below_spec:?}"
    );

    let rule = &attached_rules(&schedule)
        .into_iter()
        .find(|r| r.path == ["spec"] && r.rule.contains("oldSelf"))
        .expect("the schedule seals sourceRef")
        .rule;
    assert!(
        rule.contains("has(self.sourceRef) == has(oldSelf.sourceRef)"),
        "backupschedules.yaml: `sourceRef` needs its `has(self.x) == has(oldSelf.x)` half, \
         which is what refuses the absent -> present transition; rule was:\n{rule}"
    );
    assert!(
        rule.contains("self.sourceRef == oldSelf.sourceRef"),
        "backupschedules.yaml: `sourceRef` must be compared against oldSelf; rule was:\n{rule}"
    );
    // EVERY OTHER FIELD IS EDITABLE, AND THE SEAL MUST NOT NAME IT. This is
    // the mutant that matters now: re-adding a clause for `schedule`, `topics`,
    // `archive`, `timeZone`, `retry` or any other field would silently restore
    // the pre-PLAT-05.1 behaviour, and the operator role's `patch` would then
    // be a grant nobody can use.
    let editable: Vec<String> = spec_schema(&schedule)["properties"]
        .as_mapping()
        .expect("BackupSchedule.spec has properties")
        .keys()
        .filter_map(|k| k.as_str())
        .filter(|k| *k != "sourceRef")
        .map(str::to_string)
        .collect();
    assert!(
        editable.len() >= 12,
        "BackupSchedule.spec should carry at least twelve editable fields after PLAT-04.2 and \
         PLAT-05.1; got {editable:?}"
    );
    for field in &editable {
        assert!(
            !rule.contains(&format!("self.{field} == oldSelf.{field}")),
            "backupschedules.yaml: `{field}` is editable policy (D1 §5.1) and must not appear \
             in the seal; rule was:\n{rule}"
        );
    }
}

/// Every CEL rule the three decisions name is PRESENT in the shipped CRD, by
/// its own constant and by its message text in the file's bytes.
///
/// # The hole this closes, which was reproduced twice
///
/// `every_spec_carries_exactly_its_declared_rules` compares the shipped YAML
/// against each kind's `SPEC_RULES` array — so when a rule is deleted from that
/// array, BOTH SIDES MOVE TOGETHER and the comparison still holds. The
/// semantic tests beside it read the rule CONSTANT and evaluate its text, which
/// says what the rule means and nothing about whether it ships.
///
/// Review finding F1 planted two mutants through that hole. Deleting
/// `restore::EXACTLY_ONE_AUTHORIZATION_RULE` from `restore::SPEC_RULES` and
/// running `just crds` left all forty `crd_shape` tests green with
/// `config/crd/restores.yaml` no longer carrying the rule — and that rule is
/// the ONLY thing making an unauthorised `Restore` unrepresentable, in the same
/// change that made `approvalRef` optional. Deleting
/// `backup::DESTINATION_SENTINEL_RULE` survived the whole 424-test suite.
///
/// So this table is a LITERAL, deliberately not derived from any array the
/// emitter reads. Each row names a kind, the constant, and the message; the
/// assertions are made against the parsed schema AND against the file's raw
/// bytes, so neither a dropped array entry nor a schema-walk that stopped
/// looking can hide a missing rule.
const NAMED_RULES: &[(&str, &str, &str)] = &[
    (
        "backups.yaml",
        weirkeeper::crds::SPEC_IMMUTABLE_RULE,
        weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
    ),
    (
        "backups.yaml",
        weirkeeper::crds::backup::DESTINATION_SENTINEL_RULE,
        weirkeeper::crds::backup::DESTINATION_SENTINEL_MESSAGE,
    ),
    (
        "backups.yaml",
        weirkeeper::crds::backup::SELECTION_SHAPE_RULE,
        weirkeeper::crds::backup::SELECTION_SHAPE_MESSAGE,
    ),
    (
        "backupschedules.yaml",
        weirkeeper::crds::backup_schedule::SOURCE_REF_IMMUTABLE_RULE,
        weirkeeper::crds::backup_schedule::SOURCE_REF_IMMUTABLE_MESSAGE,
    ),
    (
        "backupschedules.yaml",
        weirkeeper::crds::backup_schedule::SELECTION_SHAPE_RULE,
        weirkeeper::crds::backup_schedule::SELECTION_SHAPE_MESSAGE,
    ),
    (
        "backupschedules.yaml",
        weirkeeper::crds::backup_schedule::DESTINATION_SENTINEL_RULE,
        weirkeeper::crds::backup_schedule::DESTINATION_SENTINEL_MESSAGE,
    ),
    (
        "restores.yaml",
        weirkeeper::crds::SPEC_IMMUTABLE_RULE,
        weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
    ),
    (
        "restores.yaml",
        weirkeeper::crds::restore::DESTINATIONS_TOGETHER_RULE,
        weirkeeper::crds::restore::DESTINATIONS_TOGETHER_MESSAGE,
    ),
    (
        "restores.yaml",
        weirkeeper::crds::restore::DESTINATION_SENTINEL_RULE,
        weirkeeper::crds::restore::DESTINATION_SENTINEL_MESSAGE,
    ),
    (
        "restores.yaml",
        weirkeeper::crds::restore::EXACTLY_ONE_AUTHORIZATION_RULE,
        weirkeeper::crds::restore::EXACTLY_ONE_AUTHORIZATION_MESSAGE,
    ),
    (
        "kafkaclusters.yaml",
        weirkeeper::crds::SPEC_IMMUTABLE_RULE,
        weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
    ),
    (
        "approvals.yaml",
        weirkeeper::crds::SPEC_IMMUTABLE_RULE,
        weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
    ),
    (
        "trustrosters.yaml",
        weirkeeper::crds::SPEC_IMMUTABLE_RULE,
        weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
    ),
    // D2 §3.2, R0-R9. R0 sits on the schema ROOT and is checked beside the
    // others below, because that is the one node a rule may read
    // `self.metadata.name` from.
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R1_STORAGE_IMMUTABLE_RULE,
        weirkeeper::crds::backup_destination::R1_STORAGE_IMMUTABLE_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R2_TRANSPORT_IMMUTABLE_RULE,
        weirkeeper::crds::backup_destination::R2_TRANSPORT_IMMUTABLE_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R3_TRANSPORT_SCHEME_RULE,
        weirkeeper::crds::backup_destination::R3_TRANSPORT_SCHEME_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R4_CA_REQUIRES_TLS_RULE,
        weirkeeper::crds::backup_destination::R4_CA_REQUIRES_TLS_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R5_ENDPOINT_RULE,
        weirkeeper::crds::backup_destination::R5_ENDPOINT_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R6_PREFIX_RULE,
        weirkeeper::crds::backup_destination::R6_PREFIX_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R7_GRANT_SHAPE_RULE,
        weirkeeper::crds::backup_destination::R7_GRANT_SHAPE_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R8_EVIDENCE_READ_SHAPE_RULE,
        weirkeeper::crds::backup_destination::R8_EVIDENCE_READ_SHAPE_MESSAGE,
    ),
    (
        "backupdestinations.yaml",
        weirkeeper::crds::backup_destination::R9_ARCHIVE_READ_GRANT_RULE,
        weirkeeper::crds::backup_destination::R9_ARCHIVE_READ_GRANT_MESSAGE,
    ),
    // D2 §5.1 T1/T2 and §6.2 P1-P9.
    (
        "topicdiscoveries.yaml",
        weirkeeper::crds::topic_discovery::T1_REQUEST_IMMUTABLE_RULE,
        weirkeeper::crds::topic_discovery::T1_REQUEST_IMMUTABLE_MESSAGE,
    ),
    (
        "topicdiscoveries.yaml",
        weirkeeper::crds::topic_discovery::CANCEL_MONOTONIC_RULE,
        weirkeeper::crds::topic_discovery::CANCEL_MONOTONIC_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P1_REQUEST_IMMUTABLE_RULE,
        weirkeeper::crds::preflight::P1_REQUEST_IMMUTABLE_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P2_CANCEL_MONOTONIC_RULE,
        weirkeeper::crds::preflight::P2_CANCEL_MONOTONIC_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P3_OPERATION_BLOCK_RULE,
        weirkeeper::crds::preflight::P3_OPERATION_BLOCK_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P4_BACKUP_TARGET_RULE,
        weirkeeper::crds::preflight::P4_BACKUP_TARGET_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P5_RESTORE_SUBJECT_RULE,
        weirkeeper::crds::preflight::P5_RESTORE_SUBJECT_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P6_DRAFT_FIELDS_RULE,
        weirkeeper::crds::preflight::P6_DRAFT_FIELDS_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P7_RESTORE_DESTINATIONS_RULE,
        weirkeeper::crds::preflight::P7_RESTORE_DESTINATIONS_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P8_RESTORE_SOURCE_RULE,
        weirkeeper::crds::preflight::P8_RESTORE_SOURCE_MESSAGE,
    ),
    (
        "preflights.yaml",
        weirkeeper::crds::preflight::P9_PLAN_HASH_RULE,
        weirkeeper::crds::preflight::P9_PLAN_HASH_MESSAGE,
    ),
    // D3 §7.1 G1-G7.
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G1_KEYS_ARE_APPEND_ONLY_RULE,
        weirkeeper::crds::trust_policy::G1_KEYS_ARE_APPEND_ONLY_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G2_NOT_AFTER_ONLY_SHORTENS_RULE,
        weirkeeper::crds::trust_policy::G2_NOT_AFTER_ONLY_SHORTENS_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G3_STATE_IS_MONOTONIC_RULE,
        weirkeeper::crds::trust_policy::G3_STATE_IS_MONOTONIC_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G4_REVOCATION_IS_WRITE_ONCE_RULE,
        weirkeeper::crds::trust_policy::G4_REVOCATION_IS_WRITE_ONCE_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G5_LIFECYCLE_FIELDS_RULE,
        weirkeeper::crds::trust_policy::G5_LIFECYCLE_FIELDS_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G6_VALIDITY_ORDER_RULE,
        weirkeeper::crds::trust_policy::G6_VALIDITY_ORDER_MESSAGE,
    ),
    // PLAT-19.1 / D3 §7.3, added at the W1 review round. The window to add it
    // closes the first time a policy object is applied: G7 makes `usages`
    // immutable and G1 makes `spec.keys` append-only, so a dual-usage key
    // created before the rule can never be narrowed or removed — and adding
    // the rule afterwards makes that object un-updatable, so its keys could
    // never be retired or revoked either.
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G8_ONE_USAGE_PER_KEY_RULE,
        weirkeeper::crds::trust_policy::G8_ONE_USAGE_PER_KEY_MESSAGE,
    ),
    (
        "trustpolicies.yaml",
        weirkeeper::crds::trust_policy::G7_KEY_MATERIAL_IS_IMMUTABLE_RULE,
        weirkeeper::crds::trust_policy::G7_KEY_MATERIAL_IS_IMMUTABLE_MESSAGE,
    ),
    // D3 §3.1, §4.1, §5.3, §6.2.
    (
        "protectionpolicies.yaml",
        weirkeeper::crds::protection_policy::H1_DESTINATION_XOR_RULE,
        weirkeeper::crds::protection_policy::H1_DESTINATION_XOR_MESSAGE,
    ),
    (
        "protectionpolicies.yaml",
        weirkeeper::crds::protection_policy::H2_ROUTE_HAS_A_CHANNEL_RULE,
        weirkeeper::crds::protection_policy::H2_ROUTE_HAS_A_CHANNEL_MESSAGE,
    ),
    (
        "rehearsalschedules.yaml",
        weirkeeper::crds::rehearsal_schedule::SUSPEND_ONLY_RULE,
        weirkeeper::crds::rehearsal_schedule::SUSPEND_ONLY_MESSAGE,
    ),
    (
        "rehearsalschedules.yaml",
        weirkeeper::crds::rehearsal_schedule::I2_REQUIRE_VERIFIED_EVIDENCE_RULE,
        weirkeeper::crds::rehearsal_schedule::I2_REQUIRE_VERIFIED_EVIDENCE_MESSAGE,
    ),
    (
        "rehearsalschedules.yaml",
        weirkeeper::crds::rehearsal_schedule::I3_POINT_SOURCE_RULE,
        weirkeeper::crds::rehearsal_schedule::I3_POINT_SOURCE_MESSAGE,
    ),
    (
        "recoverycatalogs.yaml",
        weirkeeper::crds::recovery_catalog::SYNC_REQUEST_ONLY_RULE,
        weirkeeper::crds::recovery_catalog::SYNC_REQUEST_ONLY_MESSAGE,
    ),
    (
        "recoverycatalogs.yaml",
        weirkeeper::crds::recovery_catalog::J2_DESTINATION_XOR_RULE,
        weirkeeper::crds::recovery_catalog::J2_DESTINATION_XOR_MESSAGE,
    ),
    (
        "retentionpolicies.yaml",
        weirkeeper::crds::retention_policy::IMMUTABLE_TARGET_RULE,
        weirkeeper::crds::retention_policy::IMMUTABLE_TARGET_MESSAGE,
    ),
    (
        "retentionpolicies.yaml",
        weirkeeper::crds::retention_policy::K2_ENFORCEMENT_IFF_ENFORCE_RULE,
        weirkeeper::crds::retention_policy::K2_ENFORCEMENT_IFF_ENFORCE_MESSAGE,
    ),
    (
        "retentionpolicies.yaml",
        weirkeeper::crds::retention_policy::K3_EXTERNAL_IFF_EXTERNAL_RULE,
        weirkeeper::crds::retention_policy::K3_EXTERNAL_IFF_EXTERNAL_MESSAGE,
    ),
    (
        "retentionpolicies.yaml",
        weirkeeper::crds::retention_policy::K4_SCOPE_IS_NOT_EVIDENCE_RULE,
        weirkeeper::crds::retention_policy::K4_SCOPE_IS_NOT_EVIDENCE_MESSAGE,
    ),
];

/// Every rule in [`NAMED_RULES`] is in the shipped CRD, with its own message.
#[test]
fn every_named_cel_rule_ships_in_its_crd() {
    // Enough rows that a truncated table is itself a failure: forty-eight
    // named rules across twelve rule-bearing kinds.
    assert!(
        NAMED_RULES.len() >= 48,
        "the named-rule table has shrunk to {} rows; a rule removed from this table is a rule \
         nothing pins",
        NAMED_RULES.len()
    );

    for (file, rule, message) in NAMED_RULES {
        // 1. The PARSED schema carries it, somewhere at or under `.spec`.
        let doc = crd(file);
        let attached = attached_rules(&doc);
        let found = attached
            .iter()
            .find(|r| r.rule == *rule)
            .unwrap_or_else(|| {
                panic!(
                    "{file} does not carry this rule anywhere at or under `.spec`:\n  {rule}\n\
                     It is named by a decision and by a constant in `crds/`, so its absence from \
                     the shipped CRD means the API server enforces nothing. The rules present \
                     are:\n{}",
                    attached
                        .iter()
                        .map(|r| format!("  {:?} {}", r.path, r.rule))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            });
        assert_eq!(
            &found.message, message,
            "{file}: the rule ships with the wrong message. A legible message paired with the \
             wrong rule is the worst kind of admission error — confident and wrong.\n  rule: \
             {rule}"
        );

        // 2. And the FILE'S BYTES carry the message. `attached_rules` walks a
        //    structure this test also relies on; reading the text as well means
        //    a walk that stopped looking cannot hide a missing rule either.
        let text = std::fs::read_to_string(crd_dir().join(file))
            .unwrap_or_else(|e| panic!("read {file}: {e}"));
        assert!(
            text.contains(*message),
            "{file}'s bytes do not contain this rule's message, so `kubectl apply -f` would \
             install a CRD that does not enforce it:\n  {message}"
        );
    }

    // R0 is the one rule that does not sit at or under `.spec`: a name-length
    // budget may only read `self.metadata.name`, and that is readable only at
    // the schema root.
    let dest = crd("backupdestinations.yaml");
    let root = root_schema(&dest)
        .get("x-kubernetes-validations")
        .and_then(Value::as_sequence)
        .map(|v| {
            v.iter()
                .filter_map(|e| e.get("rule").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(
        root.contains(&weirkeeper::crds::backup_destination::R0_NAME_RULE),
        "R0 must be on the BackupDestination schema ROOT; got {root:?}"
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
            // Walk to the rule's PARENT.
            let mut node = spec_schema(&doc);
            let last = r.path.last().expect("a non-empty path");
            for key in &r.path[1..r.path.len() - 1] {
                node = if key == "[]" {
                    at(node, &["items"])
                } else {
                    at(node, &["properties", key.as_str()])
                };
            }
            if last == "[]" {
                // A LIST ITEM IS THE SECOND SOUND PLACE, and only when the
                // list is an ASSOCIATIVE list. The API server correlates
                // `oldSelf` with the entry that has the same key, so the rule
                // is evaluated once per existing item and simply not evaluated
                // for a newly added one — which is what makes
                // `TrustPolicy`'s per-key immutability append-only rather than
                // a bar on adding keys. On a PLAIN list the correlation is by
                // INDEX, so inserting an entry at the front would compare
                // every later item against its neighbour's old value, and the
                // rule would mean something nobody wrote.
                assert_eq!(
                    node.get("x-kubernetes-list-type").and_then(Value::as_str),
                    Some("map"),
                    "{file}: the transition rule at {:?} sits on the items of a list that is \
                     not `x-kubernetes-list-type: map`. Without a merge key the API server \
                     correlates oldSelf BY INDEX, and an insertion would compare each item \
                     against a different entry's previous value.",
                    r.path
                );
                continue;
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
    /// The comprehension variables bound by `all()` / `exists()`, innermost
    /// last. A stack, because `TrustPolicy`'s append-only rule nests one
    /// inside the other.
    vars: Vec<(String, Value)>,
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
        } else if c == '(' || c == ')' || c == '.' || c == '?' || c == ':' || c == '+' || c == ',' {
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
                Cel::Bool(self.ordered(&lhs) <= self.ordered(&rhs))
            }
            Some("<") => {
                self.next();
                let rhs = self.additive();
                Cel::Bool(self.ordered(&lhs) < self.ordered(&rhs))
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
            // A boolean LITERAL is a `Field`, not a `Bool`, so that
            // `self.x == true` compares equal to a schema boolean. `truth()`
            // reads either one, and `Cel::Bool` stays what an OPERATOR
            // produced.
            Some("true") => {
                self.next();
                Cel::Field(Some(Value::Bool(true)))
            }
            Some("false") => {
                self.next();
                Cel::Field(Some(Value::Bool(false)))
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
            other => match self.vars.iter().rev().find(|(n, _)| n == other) {
                Some((_, v)) => Some(v.clone()),
                // Inside an untaken branch nothing is bound, and that is not a
                // mistake — the branch is being consumed, not evaluated.
                None if self.skip => None,
                None => panic!("unknown root `{other}` in rule: {}", self.src),
            },
        };
        loop {
            if self.peek() != Some(".") {
                break;
            }
            self.next();
            let field = self.next();
            if self.peek() == Some("(") {
                self.next();
                // The two comprehension macros, which is how every "every
                // existing entry is still there" rule in this group is
                // written. They bind a variable and re-evaluate their body
                // once per element, so the body is PARSED once and EVALUATED
                // n times — the cursor is rewound to the body's first token
                // for each element.
                if field == "all" || field == "exists" {
                    let var = self.next();
                    self.expect(",");
                    let body = self.i;
                    let items: Vec<Value> = match &cur {
                        Some(Value::Sequence(seq)) => seq.clone(),
                        None => Vec::new(),
                        Some(other) => {
                            panic!("{field}() takes a list; got {other:?} in `{}`", self.src)
                        }
                    };
                    let mut acc = field == "all";
                    if self.skip || items.is_empty() {
                        // An empty list makes `all` vacuously true and
                        // `exists` false, which is what CEL does; the body is
                        // still consumed so the cursor ends where the parser
                        // expects it.
                        self.skipped(|c| {
                            c.expr();
                        });
                    } else {
                        for item in &items {
                            self.i = body;
                            self.vars.push((var.clone(), item.clone()));
                            let v = self.expr();
                            let truth = self.truth(&v);
                            self.vars.pop();
                            if field == "all" {
                                acc = acc && truth;
                            } else {
                                acc = acc || truth;
                            }
                        }
                    }
                    self.expect(")");
                    return Cel::Bool(!self.skip && acc);
                }
                // An ordinary method. `startsWith` is the only one this
                // fragment evaluates; `matches` is regex and is deliberately
                // NOT evaluated here (see `is_regex_only`), because a second
                // regex engine in a test would be a second answer to the
                // question the API server already answers.
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
    /// An orderable rendering of a value, for `<` and `<=`.
    ///
    /// A NUMBER OR AN RFC 3339 INSTANT, AND NOTHING ELSE. The API server maps
    /// `type: string, format: date-time` to CEL's `timestamp` and compares
    /// those properly; this evaluator compares their TEXT, which agrees for
    /// every value this group's schemas can hold — `date-time` in a structural
    /// schema is RFC 3339, the fields are written by controllers that emit
    /// `Z`, and equal-length `Z`-normalised timestamps order lexicographically
    /// exactly as instants do. A value that is neither is a panic rather than
    /// a guess, because a comparison this evaluator got quietly wrong would be
    /// a green test for a rule that does not hold.
    fn ordered(&self, v: &Cel) -> String {
        match v {
            Cel::Field(Some(Value::Number(n))) => {
                format!("{:020}", n.as_i64().expect("an integer"))
            }
            Cel::Field(Some(Value::String(s)))
                if s.len() == 20 && s.ends_with('Z') && s.contains('T') =>
            {
                s.clone()
            }
            other => panic!(
                "expected a number or an RFC 3339 `Z` instant, got {other:?} in rule: {}",
                self.src
            ),
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
        vars: Vec::new(),
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

/// D1 §5.2's rules R1, R2 and R3 accept and refuse exactly the table the
/// decision states.
///
/// # What replaced what, and why the property is different now
///
/// This test used to be `an_absent_optional_field_cannot_be_added_on_update`,
/// and it asserted the opposite of what the product now promises: that adding
/// an optional field to a stored `BackupSchedule` was refused. PLAT-05.1 makes
/// **every field but `sourceRef` editable**, so those rows are now the
/// behaviour rather than the defect, and asserting them would pin a seal the
/// decision deliberately removed.
///
/// The property that survives is narrower and sharper: R1 refuses a `sourceRef`
/// change in BOTH directions (changed value, and the absent → present
/// transition a per-field rule would miss), R2 refuses the two-answers shape on
/// create as well as on update, and R3 refuses a retrying schedule whose name
/// cannot hold a `-r<N>` suffix. Everything else is accepted, and each accepted
/// row is a mutant target: re-adding a clause for `schedule`, `topics` or
/// `archive` turns that row red.
///
/// The evaluator is the same fragment the rest of this file uses; R3 is
/// evaluated against the schema ROOT, which is the one node a rule may read
/// `self.metadata.name` from.
#[test]
fn crd_rules_r1_r2_r3_accept_and_refuse_the_table() {
    use weirkeeper::crds::backup_schedule as bs;

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
    let dynamic = "\
schedule: '0 3 * * *'
sourceRef:
  name: prod
topics: []
allUserTopics:
  incompleteDiscovery: Refuse
archive:
  url: s3://bucket/archive
suspend: false
";

    // (name, oldSelf, self, accepted)
    let cases: Vec<(&str, Value, Value, bool)> = vec![
        // --- R1: the one seal ------------------------------------------
        (
            "R1 refuses a changed sourceRef",
            yaml(base),
            yaml(&base.replace("name: prod", "name: staging")),
            false,
        ),
        (
            "R1 accepts an unchanged sourceRef",
            yaml(base),
            yaml(base),
            true,
        ),
        // --- every other field is EDITABLE (D1 §5.1) --------------------
        (
            "the cron expression changes",
            yaml(base),
            yaml(&base.replace("'0 3 * * *'", "'0 4 * * *'")),
            true,
        ),
        (
            "the topic list changes",
            yaml(base),
            yaml(&base.replace("- orders", "- orders\n- payments")),
            true,
        ),
        (
            "the archive URL changes",
            yaml(base),
            yaml(&base.replace("s3://bucket/archive", "s3://bucket/archive-2")),
            true,
        ),
        (
            "suspend flips",
            yaml(base),
            yaml(&base.replace("suspend: false", "suspend: true")),
            true,
        ),
        (
            "an absent optional field is ADDED on update",
            yaml(base),
            yaml(&with_retention),
            true,
        ),
        (
            "a present optional field is REMOVED on update",
            yaml(&with_retention),
            yaml(base),
            true,
        ),
        (
            "the concurrency policy changes",
            yaml(&with_forbid),
            yaml(&with_forbid.replace("Forbid", "Allow")),
            true,
        ),
        (
            "a time zone is added",
            yaml(base),
            yaml(&format!("{base}timeZone: Europe/Berlin\n")),
            true,
        ),
        (
            "a retry block is added",
            yaml(base),
            yaml(&format!("{base}retry:\n  maxRetries: 2\n")),
            true,
        ),
        (
            "a catch-up policy is added",
            yaml(base),
            yaml(&format!("{base}catchUpPolicy: Latest\n")),
            true,
        ),
        (
            "a nested optional inside retention is added",
            yaml(&with_retention),
            yaml(&format!("{base}retention:\n  keepLast: 3\n  keepDays: 7\n")),
            true,
        ),
        // --- R2: the two-answers shape ----------------------------------
        (
            "R2 accepts a dynamic selection with an empty topics list",
            yaml(dynamic),
            yaml(dynamic),
            true,
        ),
        (
            "R2 refuses allUserTopics beside a non-empty topics list",
            yaml(base),
            yaml(&format!(
                "{base}allUserTopics:\n  incompleteDiscovery: Refuse\n"
            )),
            false,
        ),
        (
            "R2 refuses it on CREATE too (self == oldSelf)",
            yaml(&format!(
                "{base}allUserTopics:\n  incompleteDiscovery: Refuse\n"
            )),
            yaml(&format!(
                "{base}allUserTopics:\n  incompleteDiscovery: Refuse\n"
            )),
            false,
        ),
    ];

    for (name, old, new, expected) in cases {
        let got = eval_attached(&doc, &new, &old);
        assert_eq!(
            got, expected,
            "case `{name}`: the checked-in BackupSchedule `.spec` rules evaluated to {got}, \
             expected {expected}.\nold: {old:?}\nnew: {new:?}"
        );
    }

    // --- R3: the retry name budget, on the schema ROOT -------------------
    //
    // The root is the one node a rule may read `self.metadata.name` from, so
    // `eval_attached` (which walks `.spec`) cannot reach it and the rows are
    // evaluated directly against the rule the CRD actually ships.
    let root = root_schema(&doc)
        .get("x-kubernetes-validations")
        .and_then(Value::as_sequence)
        .expect("BackupSchedule carries a root rule")
        .iter()
        .filter_map(|e| e.get("rule").and_then(Value::as_str))
        .find(|r| *r == bs::RETRY_NAME_BUDGET_RULE)
        .expect("the shipped root rule is R3");

    let object = |name: &str, retry: Option<&str>| -> Value {
        let spec = match retry {
            Some(block) => format!("spec:\n  retry:\n{block}"),
            None => "spec: {}\n".to_string(),
        };
        yaml(&format!("metadata:\n  name: {name}\n{spec}"))
    };
    let twenty_nine = "n".repeat(29);
    let thirty = "n".repeat(30);
    let r3: Vec<(&str, Value, bool)> = vec![
        (
            "no retry block: any name is accepted",
            object(&thirty, None),
            true,
        ),
        (
            "maxRetries 0 on a 30-character name is accepted",
            object(&thirty, Some("    maxRetries: 0\n")),
            true,
        ),
        (
            "maxRetries 1 on a 30-character name is refused",
            object(&thirty, Some("    maxRetries: 1\n")),
            false,
        ),
        (
            "maxRetries 1 on a 29-character name is accepted",
            object(&twenty_nine, Some("    maxRetries: 1\n")),
            true,
        ),
        (
            "maxRetries 3 on a short name is accepted",
            object("nightly", Some("    maxRetries: 3\n")),
            true,
        ),
    ];
    for (name, object, expected) in r3 {
        let got = eval(root, &object, &object);
        assert_eq!(
            got, expected,
            "R3 case `{name}` evaluated to {got}, expected {expected}. A retry Backup is named \
             `logweir-backup-<schedule>-<slot>-r<N>`, three characters longer than attempt 0, \
             so 29 is the budget when retries are on.\nobject: {object:?}"
        );
    }

    // The budget in the rule is the budget the name functions enforce. Two
    // numbers that must agree, read from the two places they live.
    assert_eq!(
        weirkeeper::slot::max_schedule_name_len(true),
        29,
        "R3's literal 29 and `slot::max_schedule_name_len(true)` are the same budget"
    );
    assert_eq!(weirkeeper::slot::max_schedule_name_len(false), 32);
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
        vec!["Restore", "Backup", "RehearsalSchedule"]
    );
}

/// The subject kind enum is exactly the three kinds an `Approval` may be
/// about. `Switchover` is tag 2 and an `Approval` cannot name one.
///
/// `RehearsalSchedule` joined it with ADR 0008 Amendment G, for a standing
/// rehearsal authorization. It is additive: no existing spelling changed, and
/// the kind is still part of the bytes the approval binds, so an approval that
/// matches a `Restore`'s plan hash is not accepted for a schedule.
#[test]
fn the_subject_kind_enum_has_no_switchover() {
    let doc = crd("approvals.yaml");
    let node = at(
        spec_schema(&doc),
        &["properties", "subjectRef", "properties", "kind"],
    );
    assert_eq!(
        enum_values(node),
        vec!["Restore", "Backup", "RehearsalSchedule"],
        "the subject kind enum is exactly these three — the subject kind is part of the bytes \
         the approval binds, so an `Approval` whose planHash matches a `Restore` is never \
         accepted for a `Switchover`"
    );
    assert!(
        !enum_values(node).iter().any(|v| v == "Switchover"),
        "`Switchover` is tag 2 and appears in no enum of this group"
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
    const FORBIDDEN: [&str; 12] = [
        "password",
        "accesskey",
        "accesskeyid",
        "secretaccesskey",
        "sessiontoken",
        "credential",
        "privatekey",
        // Review finding F5 widened both lists. These four are the shapes the
        // D3 kinds could plausibly have grown: a routing key, a bearer token,
        // an API key, and a Slack webhook URL — which is a bearer token with a
        // hostname on the front.
        "token",
        "bearertoken",
        "apikey",
        "routingkey",
        "webhookurl",
    ];
    // `url` ALONE IS NOT ON THE LIST, and the omission is deliberate: an
    // `ArchiveRef.url` is a LOCATION, and the credential that reaches it is the
    // `secretRef` beside it. `webhookUrl` is on the list because a Slack
    // incoming-webhook URL is a bearer token with a hostname on the front —
    // the one URL in this group that is a secret.
    let mut found: Vec<String> = Vec::new();
    // EVERY NEW KIND, spec AND status. The narrow three-kind form was finding
    // F5: a `smtp.password` added to `ProtectionPolicy.spec.notifications` —
    // the one kind in the group that carries delivery channels — would have
    // shipped unguarded under a test whose name claims it covers "no new
    // kind".
    for file in [
        "backupdestinations.yaml",
        "topicdiscoveries.yaml",
        "preflights.yaml",
        "trustpolicies.yaml",
        "protectionpolicies.yaml",
        "rehearsalschedules.yaml",
        "recoverycatalogs.yaml",
        "retentionpolicies.yaml",
    ] {
        let doc = crd(file);
        let mut stack = vec![
            (vec!["spec".to_string()], spec_schema(&doc).clone()),
            (vec!["status".to_string()], status_schema(&doc).clone()),
        ];
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

    // THE SCAN IS NOT VACUOUS. `spkiPem` is a string field on `TrustPolicy`
    // whose whole purpose is to carry key material, so the walk really does
    // reach the deepest list-item properties of the widest kind. Without this
    // line a walk that stopped at the first level would pass silently.
    let policy = crd("trustpolicies.yaml");
    assert!(
        at(
            spec_schema(&policy),
            &["properties", "keys", "items", "properties", "spkiPem"]
        )
        .get("type")
        .and_then(Value::as_str)
            == Some("string"),
        "the walk must reach list-item properties, and this is the deepest string in the group"
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
// ADR 0008 Amendment G: the five operational recovery kinds
// ---------------------------------------------------------------------------

/// A `TrustPolicy` key list is append-only and its lifecycle is one-way.
///
/// # Two rule shapes, because one of them could not be installed
///
/// G1 — "every keyId that existed still exists" — is object-level, because a
/// REMOVED key has no `self` to attach a rule to. Everything else is a
/// TRANSITION RULE ON ONE ITEM of an associative list, which the API server
/// correlates by `keyId`.
///
/// That split is not a preference. The first shape put every comparison inside
/// the quadratic walk and a live API server refused the whole CRD for
/// exceeding the CEL cost budget by more than 100x; the measurement is quoted
/// at `trust_policy::G1_KEYS_ARE_APPEND_ONLY_RULE`. This test therefore
/// evaluates each rule with the operands the API server would give it: the
/// whole `.spec` for G1, and one key entry for the rest.
#[test]
fn a_trust_policy_key_is_append_only_and_its_state_is_one_way() {
    use weirkeeper::crds::trust_policy as tp;

    let key = |id: &str, pem: &str, state: &str, not_after: &str, extra: &str| {
        format!(
            "keyId: {id}\nspkiPem: {pem}\nalgorithm: p256\nusages: [EvidenceSigning]\n\
             principal:\n  id: install:one\nnotBefore: '2026-01-01T00:00:00Z'\n\
             notAfter: '{not_after}'\nstate: {state}\n{extra}"
        )
    };
    let entry = |id: &str, pem: &str, state: &str, not_after: &str, extra: &str| {
        yaml(&key(id, pem, state, not_after, extra))
    };
    let spec = |entries: &[Value]| {
        let mut m = serde_yaml::Mapping::new();
        m.insert(
            Value::String("keys".into()),
            Value::Sequence(entries.to_vec()),
        );
        Value::Mapping(m)
    };

    let active = entry("aa", "PEM-A", "Active", "2027-01-01T00:00:00Z", "");
    let old_spec = spec(std::slice::from_ref(&active));

    // ---- G1, object-level: a key may be ADDED but never REMOVED ----------
    let second = entry("bb", "PEM-B", "Active", "2028-01-01T00:00:00Z", "");
    assert!(
        eval(
            tp::G1_KEYS_ARE_APPEND_ONLY_RULE,
            &spec(&[active.clone(), second.clone()]),
            &old_spec
        ),
        "appending a key is the routine edit"
    );
    assert!(
        eval(tp::G1_KEYS_ARE_APPEND_ONLY_RULE, &old_spec, &old_spec),
        "and an unchanged list is accepted"
    );
    assert!(
        !eval(
            tp::G1_KEYS_ARE_APPEND_ONLY_RULE,
            &spec(std::slice::from_ref(&second)),
            &old_spec
        ),
        "REMOVING the only key must be refused: a receipt signed in March must still verify in \
         December, and a policy that could drop the key would make every archive it signed \
         unverifiable in one `kubectl apply`"
    );

    // ---- the per-key transition rules, one entry at a time ---------------
    let case = |id: &str, rule: &str, new: &Value, old: &Value, expected: bool| {
        assert_eq!(
            eval(rule, new, old),
            expected,
            "case `{id}`:\n{rule}\nold: {old:?}\nnew: {new:?}"
        );
    };

    case(
        "material unchanged",
        tp::G7_KEY_MATERIAL_IS_IMMUTABLE_RULE,
        &active,
        &active,
        true,
    );
    case(
        "public material swapped under an existing keyId",
        tp::G7_KEY_MATERIAL_IS_IMMUTABLE_RULE,
        &entry("aa", "PEM-EVIL", "Active", "2027-01-01T00:00:00Z", ""),
        &active,
        false,
    );
    case(
        "the usage set widened in place",
        tp::G7_KEY_MATERIAL_IS_IMMUTABLE_RULE,
        &yaml(
            &key("aa", "PEM-A", "Active", "2027-01-01T00:00:00Z", "").replace(
                "usages: [EvidenceSigning]",
                "usages: [EvidenceSigning, GovernedApproval]",
            ),
        ),
        &active,
        false,
    );
    case(
        "notAfter brought forward",
        tp::G2_NOT_AFTER_ONLY_SHORTENS_RULE,
        &entry("aa", "PEM-A", "Active", "2026-06-01T00:00:00Z", ""),
        &active,
        true,
    );
    case(
        "notAfter EXTENDED",
        tp::G2_NOT_AFTER_ONLY_SHORTENS_RULE,
        &entry("aa", "PEM-A", "Active", "2030-01-01T00:00:00Z", ""),
        &active,
        false,
    );

    let retired = entry(
        "aa",
        "PEM-A",
        "Retired",
        "2027-01-01T00:00:00Z",
        "retiredAt: '2026-05-01T00:00:00Z'\n",
    );
    let revoked = entry(
        "aa",
        "PEM-A",
        "Revoked",
        "2027-01-01T00:00:00Z",
        "retiredAt: '2026-05-01T00:00:00Z'\nrevokedAt: '2026-06-01T00:00:00Z'\n\
         revocationEffectiveFrom: '2026-06-01T00:00:00Z'\nrevocationReason: KeyCompromise\n",
    );
    case(
        "Active -> Retired",
        tp::G3_STATE_IS_MONOTONIC_RULE,
        &retired,
        &active,
        true,
    );
    case(
        "Retired -> Revoked",
        tp::G3_STATE_IS_MONOTONIC_RULE,
        &revoked,
        &retired,
        true,
    );
    case(
        "Retired -> Active: a retirement that can be undone proves nothing",
        tp::G3_STATE_IS_MONOTONIC_RULE,
        &active,
        &retired,
        false,
    );
    case(
        "Revoked -> Retired: un-revoking a compromised key is the edit an attacker most wants",
        tp::G3_STATE_IS_MONOTONIC_RULE,
        &retired,
        &revoked,
        false,
    );

    let moved = entry(
        "aa",
        "PEM-A",
        "Revoked",
        "2027-01-01T00:00:00Z",
        "retiredAt: '2026-05-01T00:00:00Z'\nrevokedAt: '2026-06-01T00:00:00Z'\n\
         revocationEffectiveFrom: '2026-12-01T00:00:00Z'\nrevocationReason: KeyCompromise\n",
    );
    case(
        "revocationEffectiveFrom moved later, PAST an attacker's signature",
        tp::G4_REVOCATION_IS_WRITE_ONCE_RULE,
        &moved,
        &revoked,
        false,
    );
    case(
        "the instants unchanged",
        tp::G4_REVOCATION_IS_WRITE_ONCE_RULE,
        &revoked,
        &revoked,
        true,
    );
    case(
        "a key that was never revoked may have the instants written now",
        tp::G4_REVOCATION_IS_WRITE_ONCE_RULE,
        &revoked,
        &retired,
        true,
    );

    // ---- and the lifecycle fields a state requires ----------------------
    case(
        "Revoked with no instants",
        tp::G5_LIFECYCLE_FIELDS_RULE,
        &entry(
            "aa",
            "PEM-A",
            "Revoked",
            "2027-01-01T00:00:00Z",
            "retiredAt: '2026-05-01T00:00:00Z'\n",
        ),
        &active,
        false,
    );
    case(
        "Retired with no retiredAt",
        tp::G5_LIFECYCLE_FIELDS_RULE,
        &entry("aa", "PEM-A", "Retired", "2027-01-01T00:00:00Z", ""),
        &active,
        false,
    );
    case(
        "validity backwards",
        tp::G6_VALIDITY_ORDER_RULE,
        &yaml(
            "notBefore: '2027-01-01T00:00:00Z'\nnotAfter: '2026-01-01T00:00:00Z'\nstate: Active\n",
        ),
        &active,
        false,
    );

    // And the key list is an associative list, so the API server itself
    // refuses a duplicate keyId — no quadratic CEL self-join for it — and the
    // per-item transition rules are correlated by that key rather than by
    // index.
    let doc = crd("trustpolicies.yaml");
    let keys = at(spec_schema(&doc), &["properties", "keys"]);
    assert_eq!(
        keys.get("x-kubernetes-list-type").and_then(Value::as_str),
        Some("map"),
        "spec.keys is an associative list keyed by keyId"
    );
    assert_eq!(
        keys.get("x-kubernetes-list-map-keys")
            .and_then(Value::as_sequence)
            .map(|v| v.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec!["keyId"]),
        "the merge key is keyId"
    );

    // THE COST BOUND IS PART OF THE RULE. The CEL estimator reads `maxLength`
    // and nothing else; without it the quadratic G1 walk is priced against the
    // largest string a request could carry and the API server refuses to
    // install the CRD at all.
    assert_eq!(
        at(keys, &["items", "properties", "keyId", "maxLength"]).as_u64(),
        Some(64),
        "keyId must carry a maxLength, not merely a pattern: it is what makes G1's estimated \
         cost finite, and a live API server refused this CRD without it"
    );
    assert_eq!(
        at(keys, &["items", "properties", "spkiPem", "maxLength"]).as_u64(),
        Some(4096),
    );
    assert_eq!(
        keys.get("maxItems").and_then(Value::as_u64),
        Some(64),
        "and the list itself is bounded, for the same reason"
    );
}

/// `TrustPolicy` is cluster-scoped and carries public material only.
#[test]
fn a_trust_policy_carries_public_material_and_is_cluster_scoped() {
    let doc = crd("trustpolicies.yaml");
    assert_eq!(
        at(&doc, &["spec", "scope"]).as_str(),
        Some("Cluster"),
        "a namespace tenant must not be able to name or edit the trust that authorises it"
    );
    let entry = at(
        spec_schema(&doc),
        &["properties", "keys", "items", "properties"],
    );
    assert!(
        entry.get("spkiPem").is_some(),
        "a key entry carries its PUBLIC key material: an id alone gives verification nothing \
         to verify against, which is the defect `the_roster_carries_key_material_for_both_lists` \
         records for the roster"
    );
    for forbidden in ["privateKeyPem", "privateKey", "secretRef", "keyMaterial"] {
        assert!(
            entry.get(forbidden).is_none(),
            "`{forbidden}` must not exist on a TrustPolicy key: `logweir trust export` writes \
             this object verbatim, and every field here is world-readable to anyone with \
             cluster read"
        );
    }
    assert_eq!(
        enum_values(at(entry, &["state"])),
        vec!["Active", "Retired", "Revoked"],
        "three states, and the order is the direction G3 allows"
    );
    assert_eq!(
        enum_values(at(entry, &["usages", "items"])),
        vec!["EvidenceSigning", "GovernedApproval", "ConsoleConfirmation"],
        "the usage split is what keeps `signs evidence` and `approves restores` separate \
         grants, which P17 requires for PLAT-19.2"
    );
}

/// `RecoveryCatalog` seals everything but its sync trigger, and
/// `RetentionPolicy` seals the three fields that decide WHERE deletion could
/// happen.
#[test]
fn the_catalog_and_the_retention_policy_seal_what_they_must() {
    use weirkeeper::crds::{recovery_catalog as rc, retention_policy as rp};

    let catalog = |dest: &str, interval: i32, token: &str| {
        yaml(&format!(
            "destinationRef:\n  name: {dest}\nsync:\n  intervalSeconds: {interval}\n\
             syncRequest: {token}\n"
        ))
    };
    let base = catalog("primary", 3600, "t1");
    assert!(
        eval(
            rc::SYNC_REQUEST_ONLY_RULE,
            &catalog("primary", 3600, "t2"),
            &base
        ),
        "changing only syncRequest is the one permitted edit"
    );
    assert!(
        !eval(
            rc::SYNC_REQUEST_ONLY_RULE,
            &catalog("other", 3600, "t1"),
            &base
        ),
        "re-pointing a catalog at a different destination must be refused"
    );
    assert!(
        !eval(
            rc::SYNC_REQUEST_ONLY_RULE,
            &catalog("primary", 300, "t1"),
            &base
        ),
        "the sync settings are sealed too"
    );

    let policy = |dest: &str, prefix: &str, keep_last: i32| {
        yaml(&format!(
            "destinationRef:\n  name: {dest}\ncatalogRef:\n  name: primary\n\
             scope:\n  prefix: {prefix}\nrules:\n  keepLast: {keep_last}\nmode: Report\n"
        ))
    };
    let old = policy("primary", "kafka-backups/team-a", 30);
    assert!(
        eval(
            rp::IMMUTABLE_TARGET_RULE,
            &policy("primary", "kafka-backups/team-a", 10),
            &old
        ),
        "tightening the rules is the routine edit"
    );
    assert!(
        !eval(
            rp::IMMUTABLE_TARGET_RULE,
            &policy("primary", "kafka-backups/team-b", 30),
            &old
        ),
        "the scope prefix is immutable: an approved plan names point ids, and a movable scope \
         would apply that plan to a different prefix"
    );
    assert!(
        !eval(
            rp::IMMUTABLE_TARGET_RULE,
            &policy("other", "kafka-backups/team-a", 30),
            &old
        ),
        "the destination is immutable for the same reason"
    );

    // And `mode` and its block travel together, in both directions.
    let with = |mode: &str, block: &str| yaml(&format!("mode: {mode}\n{block}"));
    for (name, value, expected) in [
        ("Report with no block", with("Report", ""), true),
        (
            "Enforce with its block",
            with("Enforce", "enforcement:\n  schedule: '17 4 * * *'\n"),
            true,
        ),
        ("Enforce with NO block", with("Enforce", ""), false),
        (
            "Report carrying an enforcement block",
            with("Report", "enforcement:\n  schedule: '17 4 * * *'\n"),
            false,
        ),
    ] {
        assert_eq!(
            eval(rp::K2_ENFORCEMENT_IFF_ENFORCE_RULE, &value, &value),
            expected,
            "case `{name}`: a delete-capable credential configured under a mode that never \
             deletes is a credential mounted for nothing"
        );
    }
    assert_eq!(
        enum_values(at(
            spec_schema(&crd("retentionpolicies.yaml")),
            &["properties", "mode"]
        )),
        vec!["Report", "Enforce", "ExternalLifecycle"],
    );
    assert_eq!(
        at(
            spec_schema(&crd("retentionpolicies.yaml")),
            &["properties", "mode", "default"]
        )
        .as_str(),
        Some("Report"),
        "the DEFAULT is the mode that deletes nothing. Amendment H makes the tag-1 statement \
         version-scoped, not withdrawn: it is unqualified wherever mode is not Enforce, and \
         an object that says nothing about mode is one of those."
    );
}

/// `RehearsalSchedule` seals every field but `suspend`, and the seal names
/// each of them.
#[test]
fn a_rehearsal_schedule_seals_everything_but_suspend() {
    let doc = crd("rehearsalschedules.yaml");
    let rule = &attached_rules(&doc)
        .into_iter()
        .find(|r| r.path == ["spec"] && r.rule.contains("oldSelf"))
        .expect("the rehearsal schedule seals its spec")
        .rule;
    let sealed: Vec<&str> = at(spec_schema(&doc), &["properties"])
        .as_mapping()
        .expect("the spec has properties")
        .keys()
        .filter_map(|k| k.as_str())
        .filter(|k| *k != "suspend")
        .collect();
    assert!(
        sealed.len() >= 7,
        "expected the full field set; got {sealed:?}"
    );
    for field in &sealed {
        assert!(
            rule.contains(&format!("has(self.{field}) == has(oldSelf.{field})")),
            "the seal must close the absent -> present transition for `{field}`; the standing \
             authorization binds a digest of this spec, so a field that could be ADDED after \
             approval would authorize work nobody approved.\nrule: {rule}"
        );
    }
    assert!(
        !rule.contains("suspend"),
        "`suspend` is the one mutable field and must not appear in the seal"
    );

    // And v1's two single-value enums say what the decision says with CEL.
    let spec = spec_schema(&doc);
    assert_eq!(
        enum_values(at(
            spec,
            &["properties", "point", "properties", "selection"]
        )),
        vec!["NewestAvailable"],
    );
    assert_eq!(
        enum_values(at(
            spec,
            &["properties", "bounds", "properties", "concurrencyPolicy"]
        )),
        vec!["Forbid"],
        "two concurrent rehearsals would race over the same mapped topic names"
    );
    assert_eq!(
        at(
            spec,
            &[
                "properties",
                "target",
                "properties",
                "topicPrefix",
                "pattern"
            ]
        )
        .as_str(),
        Some(weirkeeper::crds::rehearsal_schedule::TOPIC_PREFIX_PATTERN),
        "the SPEC field carries the pre-rendering grammar"
    );
    assert_eq!(
        at(
            spec,
            &[
                "properties",
                "target",
                "properties",
                "topicPrefix",
                "minLength"
            ]
        )
        .as_u64(),
        Some(10),
        "ten is `rehearsal-` itself, D3 §4.1's own example"
    );

    // THE TWO GRAMMARS ARE DIFFERENT, AND THAT IS THE POINT. The decision
    // states `^rehearsal-[a-z0-9-]*-$` "after rendering": the controller
    // appends `<uid8>-`, and it is the RENDERED value that has to end in a
    // hyphen. Putting the rendered pattern on the spec field refused
    // `rehearsal-` — measured live on 2026-09-16, because RE2 needs one more
    // character before `-$` once the leading literal is consumed. This walks
    // the same rendering the controller performs and checks the result,
    // instead of trusting that the two patterns relate.
    let rendered_ok = |prefix: &str| {
        let rendered = format!("{prefix}3f2a91c7-");
        rendered.starts_with("rehearsal-")
            && rendered.ends_with('-')
            && rendered[10..]
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    };
    for prefix in ["rehearsal-", "rehearsal-eu-", "rehearsal-team-a-"] {
        assert!(
            rendered_ok(prefix),
            "`{prefix}` must render into something matching {}",
            weirkeeper::crds::rehearsal_schedule::RENDERED_TOPIC_PREFIX_PATTERN
        );
    }
    assert_ne!(
        weirkeeper::crds::rehearsal_schedule::RENDERED_TOPIC_PREFIX_PATTERN,
        weirkeeper::crds::rehearsal_schedule::TOPIC_PREFIX_PATTERN,
        "the two grammars are deliberately different; collapsing them is exactly what the live \
         probe caught"
    );

    // And v1 refuses a rehearsal that would accept unverified evidence — which
    // would prove the archive is READABLE and nothing about whether it is
    // trustworthy.
    let rs = weirkeeper::crds::rehearsal_schedule::I2_REQUIRE_VERIFIED_EVIDENCE_RULE;
    for (name, value, expected) in [
        (
            "requireVerifiedEvidence true",
            yaml("point:\n  requireVerifiedEvidence: true\n"),
            true,
        ),
        (
            "requireVerifiedEvidence false",
            yaml("point:\n  requireVerifiedEvidence: false\n"),
            false,
        ),
    ] {
        assert_eq!(eval(rs, &value, &value), expected, "case `{name}`");
    }
}

/// A `Restore` is never unauthorized, and the additive status blocks are all
/// optional.
#[test]
fn a_restore_carries_exactly_one_authorization_and_additive_status_is_optional() {
    use weirkeeper::crds::restore as r;
    let doc = crd("restores.yaml");
    let spec = spec_schema(&doc);

    assert!(
        !required(spec).contains(&"approvalRef".to_string()),
        "approvalRef became optional so a standing authorization can take its place"
    );
    assert!(
        !required(spec).contains(&"authorization".to_string()),
        "and the standing authorization is optional too — the CEL rule is what makes exactly \
         one of them required"
    );
    for (name, value, expected) in [
        (
            "a per-run approval",
            yaml("approvalRef:\n  name: a1\n"),
            true,
        ),
        (
            "a standing authorization",
            yaml("authorization:\n  kind: Standing\n  approvalRef:\n    name: a1\n  rehearsalScheduleRef:\n    name: weekly\n"),
            true,
        ),
        ("NEITHER — an unauthorized restore", yaml("deadlineSeconds: 60\n"), false),
        (
            "BOTH — two authorities that could disagree",
            yaml("approvalRef:\n  name: a1\nauthorization:\n  kind: Standing\n  approvalRef:\n    name: a2\n  rehearsalScheduleRef:\n    name: weekly\n"),
            false,
        ),
    ] {
        assert_eq!(
            eval(r::EXACTLY_ONE_AUTHORIZATION_RULE, &value, &value),
            expected,
            "case `{name}`"
        );
    }

    // Every additive status block is optional, on both kinds. An older
    // controller writes none of them, and a status schema that required one
    // would make every such object invalid on upgrade.
    for (file, blocks) in [
        ("backups.yaml", &["progress", "capture"][..]),
        ("restores.yaml", &["progress", "completion", "teardown"][..]),
    ] {
        let doc = crd(file);
        let status = status_schema(&doc);
        let req = required(status);
        for block in blocks {
            assert!(
                at(status, &["properties", block]).is_mapping(),
                "{file}: status.{block} must exist"
            );
            assert!(
                !req.contains(&(*block).to_string()),
                "{file}: status.{block} must be OPTIONAL — every object an older controller \
                 reconciled has none of it, and requiring it would invalidate them all on \
                 upgrade"
            );
        }
    }

    // The trust block on the shared verification type, likewise additive.
    let backup = crd("backups.yaml");
    let verification = at(
        status_schema(&backup),
        &[
            "properties",
            "evidence",
            "properties",
            "verification",
            "properties",
        ],
    );
    for field in ["signedAt", "trust"] {
        assert!(
            verification.get(field).is_some(),
            "status.evidence.verification.{field} is what tells `signed while the key was \
             valid` from `signed afterwards`"
        );
    }
    assert!(
        at(verification, &["trust", "properties"])
            .get("basis")
            .is_some(),
        "and the basis is what makes `Historical` a pass rather than a downgrade"
    );
}

// ---------------------------------------------------------------------------
// D1 W3a: the run contract on `Backup`
// ---------------------------------------------------------------------------

/// `Backup.spec` carries the trigger, the schedule revision and the selection,
/// and **every one of them is optional or unchanged**.
#[test]
fn the_backup_run_contract_is_additive_to_the_last_field() {
    let doc = crd("backups.yaml");
    let spec = spec_schema(&doc);

    assert_eq!(
        required(spec),
        vec![
            "archive".to_string(),
            "deadlineSeconds".to_string(),
            "sourceRef".to_string(),
            "topics".to_string(),
            "triggeredBy".to_string(),
        ],
        "THE REQUIRED SET DID NOT MOVE. Every field D1 W3a adds is optional, because every \
         stored `Backup` in every cluster has none of them and a new required field would make \
         all of them invalid on upgrade. `topics` in particular STAYS required: a dynamic run \
         carries `topics: []`, so an older controller still deserializes it and its runner \
         refuses on the empty list — where an absent `topics` would be a reflector decode \
         error across the whole kind."
    );

    let trigger = at(spec, &["properties", "trigger"]);
    assert_eq!(
        enum_values(at(trigger, &["properties", "kind"])),
        vec!["Scheduled", "CatchUp", "Retry", "Manual"],
        "four trigger kinds; `triggeredBy` keeps its own two-value vocabulary, which is what \
         the signed receipt carries"
    );
    assert_eq!(
        required(trigger),
        vec!["kind"],
        "only `kind` is required inside the trigger: `attempt` defaults to 0 and the other two \
         are absent for everything but a retry"
    );
    assert_eq!(
        at(trigger, &["properties", "attempt", "default"]).as_u64(),
        Some(0),
    );
    assert_eq!(
        at(trigger, &["properties", "attempt", "maximum"]).as_u64(),
        Some(3),
        "at most three retries, which is also why the retry suffix is three characters and the \
         schedule-name budget is 29"
    );

    let schedule_ref = at(spec, &["properties", "scheduleRef"]);
    assert_eq!(
        required(schedule_ref),
        vec!["name"],
        "`scheduleRef` grew `uid`, `generation` and `runPolicySha256` and every one of them is \
         optional — a `Backup` created before they existed carries only the name and must keep \
         resolving exactly as it did"
    );
    for field in ["name", "uid", "generation", "runPolicySha256"] {
        assert!(
            at(schedule_ref, &["properties"]).get(field).is_some(),
            "scheduleRef.{field} must exist"
        );
    }
}

/// The two selection shapes, and the field with no default.
#[test]
fn the_selection_shape_is_declared_and_incomplete_discovery_has_no_default() {
    let doc = crd("backups.yaml");
    let spec = spec_schema(&doc);
    let dynamic = at(spec, &["properties", "allUserTopics"]);

    assert_eq!(
        required(dynamic),
        vec!["incompleteDiscovery"],
        "`incompleteDiscovery` is REQUIRED: Kafka omits topics a principal cannot describe, so \
         no discovery can prove whole-cluster visibility, and the user must choose what happens \
         then"
    );
    let mode = at(dynamic, &["properties", "incompleteDiscovery"]);
    assert_eq!(enum_values(mode), vec!["Refuse", "BackUpVisibleTopics"]);
    assert!(
        mode.get("default").is_none(),
        "AND IT HAS NO DEFAULT, deliberately. Defaulting to `Refuse` makes dynamic mode \
         unusable out of the box; defaulting to `BackUpVisibleTopics` silently weakens the \
         promise the mode's own name makes. An operator who has not thought about it must not \
         be able to ship either answer by omission."
    );

    // AND ADMISSION REFUSES THE THIRD SHAPE. Review finding F2: without this
    // rule a `Backup` naming two topics AND `allUserTopics` was accepted live,
    // and today's controller ignores `allUserTopics` — so the operator who
    // asked for whole-cluster coverage would have got a two-topic run and no
    // signal. `crate::policy::validate_topic_selection` says the same thing in
    // the reconciler, for objects admitted by an older CRD revision.
    let shape = weirkeeper::crds::backup::SELECTION_SHAPE_RULE;
    for (name, value, expected) in [
        (
            "a named allowlist, no dynamic block",
            yaml("topics: [orders, payments]\n"),
            true,
        ),
        (
            "topics: [] with a dynamic block",
            yaml("topics: []\nallUserTopics:\n  incompleteDiscovery: Refuse\n"),
            true,
        ),
        (
            "BOTH: two answers to one question",
            yaml("topics: [orders]\nallUserTopics:\n  incompleteDiscovery: Refuse\n"),
            false,
        ),
        (
            "topics: [] alone — accepted here, and refused by the controller and the runner",
            yaml("topics: []\n"),
            true,
        ),
    ] {
        assert_eq!(
            eval(shape, &value, &value),
            expected,
            "case `{name}` against {shape}"
        );
    }

    // The exclusions are NAMES AND LITERAL PREFIXES. Guard G-GLOB is a pattern
    // on both, so `orders*` is not writable in an exclusion any more than in
    // an allowlist.
    let exclude = at(dynamic, &["properties", "exclude", "properties"]);
    for (field, max) in [("topics", 1000_u64), ("prefixes", 32)] {
        let node = at(exclude, &[field]);
        assert_eq!(
            node.get("maxItems").and_then(Value::as_u64),
            Some(max),
            "exclude.{field} is bounded — an unbounded list in a spec is an unbounded CEL cost \
             and an unbounded object"
        );
        assert_eq!(
            at(node, &["items", "pattern"]).as_str(),
            Some(weirkeeper::crds::selection::TOPIC_NAME_PATTERN),
            "exclude.{field} items carry the Kafka name grammar, which contains no glob \
             metacharacter"
        );
    }
    assert_eq!(
        at(spec, &["properties", "topics", "items", "pattern"]).as_str(),
        Some(weirkeeper::crds::selection::TOPIC_NAME_PATTERN),
        "and the allowlist itself, which is where guard G-GLOB started"
    );

    // `status.selection` records what the run may CLAIM, and carries no names.
    let status = at(status_schema(&doc), &["properties", "selection"]);
    assert_eq!(
        enum_values(at(status, &["properties", "coverage"])),
        vec![
            "NamedTopics",
            "AllUserTopicsAttested",
            "VisibleUserTopicsOnly"
        ],
        "three coverage labels, and only the middle one may ever be rendered as `all topics`"
    );
    let properties = at(status, &["properties"])
        .as_mapping()
        .expect("selection has properties");
    for key in properties.keys().filter_map(Value::as_str) {
        assert!(
            !key.ends_with("Topics") || key == "allUserTopics",
            "status.selection carries COUNTS and a digest, never names: a resolved list is \
             unbounded and a status is not a store. Found `{key}`"
        );
    }
    assert!(
        at(status, &["properties"])
            .get("resolvedTopicCount")
            .is_some()
            && at(status, &["properties"]).get("discoverySha256").is_some(),
        "the count and the digest are what make a frozen selection checkable without storing it"
    );
}

/// The `Backup.spec` seal still covers every field, including the new ones.
///
/// THE MUTANT THIS KILLS is adding a field to `BackupSpec` and forgetting that
/// `self == oldSelf` is what makes a run's inputs the run. It cannot be
/// forgotten here — the seal is object-level and covers the whole spec — so
/// this asserts the seal is still the WHOLE-spec one and not a narrowed
/// enumeration somebody wrote while adding `trigger`.
#[test]
fn the_backup_spec_is_still_sealed_whole() {
    let doc = crd("backups.yaml");
    let seals: Vec<String> = attached_rules(&doc)
        .into_iter()
        .filter(|r| r.path == ["spec"] && r.rule.contains("oldSelf"))
        .map(|r| r.rule)
        .collect();
    assert_eq!(
        seals,
        vec![weirkeeper::crds::SPEC_IMMUTABLE_RULE.to_string()],
        "a `Backup`'s spec is its run's inputs, and they are sealed WHOLE — never as an \
         enumeration that a new field could be left out of"
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
