//! The properties of the six kinds, read off the CHECKED-IN CRD YAML.
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

/// The directory the six checked-in CRDs live in.
fn crd_dir() -> PathBuf {
    repo_root().join("config/crd")
}

/// The six files, in the order [`weirkeeper::crds::KINDS`] names their kinds.
const FILES: [&str; 6] = [
    "kafkaclusters.yaml",
    "backupschedules.yaml",
    "backups.yaml",
    "restores.yaml",
    "approvals.yaml",
    "trustrosters.yaml",
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

/// Exactly six kinds. Global Constraint 34 amends the roadmap's kind list to
/// these, `RestoreDrill` retired for `Restore` and `MetadataSnapshot` merely
/// reserved.
#[test]
fn the_kind_list_is_exactly_six() {
    // READ EVERY FILE IN `config/crd/`, NOT THE SIX THIS TEST NAMES. A list
    // built from `FILES` could not see a SEVENTH kind at all — the emitted set
    // would be compared against itself and the forbidden-name loop below would
    // have nothing to look at. Reading the directory is what makes a `Drill`
    // CRD someone added and emitted fail on its own name.
    let dir = crd_dir();
    let mut yaml_files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("config/crd is readable")
        .map(|e| e.expect("an entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
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
        6,
        "config/crd holds {} kinds, not six: {kinds:?}",
        kinds.len()
    );
    let mut expected: Vec<String> = weirkeeper::crds::KINDS
        .iter()
        .map(|k| k.to_string())
        .collect();
    expected.sort();
    assert_eq!(
        kinds, expected,
        "the emitted kind set must be exactly {{KafkaCluster, BackupSchedule, Backup, Restore, \
         Approval, TrustRoster}}"
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
        .collect();
    on_disk.sort();
    let mut want: Vec<String> = FILES.iter().map(|f| f.to_string()).collect();
    want.sort();
    assert_eq!(
        on_disk, want,
        "config/crd holds exactly the six files and no seventh — a stray file here is a \
         seventh kind someone applied"
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

/// Five kinds seal the whole `.spec`; `BackupSchedule` seals everything but
/// `suspend`, with ONE object-level rule; and nothing is attached to
/// `spec.suspend`.
#[test]
fn every_spec_is_sealed_and_only_suspend_is_mutable() {
    for file in FILES {
        let doc = crd(file);
        let rules = attached_rules(&doc);
        let on_spec: Vec<&Attached> = rules.iter().filter(|r| r.path == ["spec"]).collect();
        assert_eq!(
            on_spec.len(),
            1,
            "{file}: exactly one CEL rule is attached to `.spec`; got {rules:?}"
        );
        let deeper: Vec<&Attached> = rules.iter().filter(|r| r.path != ["spec"]).collect();
        assert!(
            deeper.is_empty(),
            "{file}: no CEL rule is attached BELOW `.spec` — a per-field transition rule is \
             evaluated only when `oldSelf` has that field, so it does not seal an optional \
             one on the 1.29 floor (Global Constraint 25). Got {deeper:?}"
        );

        let rule = &on_spec[0].rule;
        if file == "backupschedules.yaml" {
            // The object-level rule names every field except `suspend`.
            for named in ["schedule", "sourceRef", "topics", "archive", "retention"] {
                assert!(
                    rule.contains(&format!("self.{named}")),
                    "{file}: the object-level rule must name `{named}`; rule was:\n{rule}"
                );
                assert!(
                    rule.contains(&format!("oldSelf.{named}")),
                    "{file}: the object-level rule must compare `{named}` against oldSelf; \
                     rule was:\n{rule}"
                );
                // And close the absent -> present transition for it.
                assert!(
                    rule.contains(&format!("has(self.{named}) == has(oldSelf.{named})")),
                    "{file}: `{named}` needs its `has(self.x) == has(oldSelf.x)` half, which \
                     is what refuses the absent -> present transition; rule was:\n{rule}"
                );
            }
            assert!(
                !rule.contains("suspend"),
                "{file}: `suspend` is the one mutable field and must not appear in the seal; \
                 rule was:\n{rule}"
            );
            assert_eq!(
                rule,
                weirkeeper::crds::backup_schedule::SUSPEND_ONLY_RULE,
                "{file}: the checked-in rule must be the constant the emitter injects"
            );
            assert_eq!(
                on_spec[0].message,
                weirkeeper::crds::backup_schedule::SUSPEND_ONLY_MESSAGE,
                "{file}: the message travels with the rule"
            );
        } else {
            assert_eq!(
                rule,
                weirkeeper::crds::SPEC_IMMUTABLE_RULE,
                "{file}: the whole `.spec` is sealed with `self == oldSelf`"
            );
            assert_eq!(
                on_spec[0].message,
                weirkeeper::crds::SPEC_IMMUTABLE_MESSAGE,
                "{file}: the message travels with the rule"
            );
        }
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
}

fn tokenize(rule: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes: Vec<char> = rule.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '(' || c == ')' || c == '.' {
            out.push(c.to_string());
            i += 1;
        } else if c == '&' || c == '|' || c == '=' {
            assert!(
                i + 1 < bytes.len() && bytes[i + 1] == c,
                "unexpected single `{c}` in rule: {rule}"
            );
            out.push(format!("{c}{c}"));
            i += 2;
        } else if c == '!' {
            out.push("!".to_string());
            i += 1;
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

    fn or_expr(&mut self) -> Cel {
        let mut acc = self.and_expr();
        while self.peek() == Some("||") {
            self.next();
            let rhs = self.and_expr();
            acc = Cel::Bool(acc.truth(self.src) || rhs.truth(self.src));
        }
        acc
    }
    fn and_expr(&mut self) -> Cel {
        let mut acc = self.unary();
        while self.peek() == Some("&&") {
            self.next();
            let rhs = self.unary();
            acc = Cel::Bool(acc.truth(self.src) && rhs.truth(self.src));
        }
        acc
    }
    fn unary(&mut self) -> Cel {
        if self.peek() == Some("!") {
            self.next();
            let v = self.unary();
            return Cel::Bool(!v.truth(self.src));
        }
        let lhs = self.primary();
        if self.peek() == Some("==") {
            self.next();
            let rhs = self.primary();
            return Cel::Bool(lhs == rhs);
        }
        lhs
    }
    fn primary(&mut self) -> Cel {
        match self.peek() {
            Some("(") => {
                self.next();
                let v = self.or_expr();
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
        while self.peek() == Some(".") {
            self.next();
            let field = self.next();
            cur = cur.and_then(|v| v.get(field.as_str()).cloned());
            // An explicit YAML `null` is "absent" for `has()`, which is what
            // the API server does with a null-valued optional property.
            if matches!(cur, Some(Value::Null)) {
                cur = None;
            }
        }
        Cel::Field(cur)
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
    };
    let v = c.or_expr();
    assert_eq!(
        c.i,
        c.toks.len(),
        "the rule was not fully consumed, so this evaluation means nothing: {rule}"
    );
    v.truth(rule)
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
        vec!["mode", "secretRef", "tls", "username"],
        "the auth block is {{mode, username, secretRef, tls}} and carries NO password field"
    );
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
/// no remote exists, so no digest exists to pin).
#[test]
fn the_runner_image_is_named_once() {
    let needle = format!("{}{}", "ghcr.io/logweir/", "logweir");
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
        "RUNNER_IMAGE is under the literal ghcr.io/logweir namespace (Global Constraint 24), \
         never a `<org>` placeholder: a shipped logweir.yaml carrying one is not applyable. \
         Got {}",
        weirkeeper::job::RUNNER_IMAGE
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
    assert!(
        ci.contains("emit_crds"),
        "ci.yml must re-render the CRDs: a CRD change is a format change, and a format change \
         that appears as a diff is one a reviewer sees"
    );
    for file in FILES {
        assert!(
            ci.contains(file),
            "ci.yml's drift arm must diff `{file}` — an arm that diffs five of six files is a \
             gate the sixth kind walks through"
        );
    }
    assert!(
        ci.contains("cargo run -p logweir-core --example emit_schema"),
        "the scorecard schema arm stays as it was"
    );
}

/// THE DRIFT GATE, LOCALLY AND IN-PROCESS: the checked-in files are exactly
/// what the renderer produces.
///
/// The CI arm re-renders with `cargo run` and `diff -u`s the files, which is
/// the shipped gate. This test asserts the same property with no subprocess
/// and no shell, so a hand edit to `config/crd/` fails `cargo test` on a
/// laptop too — `ci.yml` has never executed on any commit, so a gate that
/// lives only there is documentation.
#[test]
fn the_checked_in_crds_are_what_the_emitter_renders() {
    let rendered = weirkeeper::crds::render_all();
    assert_eq!(
        rendered.len(),
        6,
        "the emitter renders six documents; got {}",
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
        assert_eq!(
            on_disk,
            r.yaml,
            "{} has drifted from the emitter. Run `just crds` — never hand-edit a generated \
             CRD.",
            path.display()
        );
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
