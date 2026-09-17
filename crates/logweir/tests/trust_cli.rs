//! `logweir trust export` and `logweir trust migrate-roster` (PLAT-19.1,
//! decision D3 §7.1 and §7.5).
//!
//! # Two levels, deliberately
//!
//! The transforms are asserted against the LIBRARY, because the property is a
//! mapping from one document to another and a subprocess adds nothing to it.
//! The argv surface is asserted against the COMPILED BINARY, because the
//! failure this repository has actually shipped is a subcommand that exists in
//! `cli.rs` and is unreachable from `main` (Task 22's carried obligation: the
//! shipped binary silently had no `drill show`).
//!
//! # No key material here is private
//!
//! The PEMs below are **public** keys. The one private-looking string in this
//! file is a PEM HEADER with no body at all —
//! [`mutant_export_refuses_an_object_carrying_private_material`] needs
//! something that trips the refusal, and a real private key would be a real
//! private key in a repository.
//!
//! # Nothing dials anything
//!
//! Both commands read one object on stdin or from a file and write one
//! document on stdout. `crates/logweir/tests/no_network_in_unit_tests.rs`
//! keeps that true for the crate; these tests would hang rather than pass if
//! it stopped being.

use std::io::Write as _;
use std::process::{Command, Stdio};

use logweir::trust::{
    export, migrate_roster, refuse_private_material, ExportArgs, Input, MigrateArgs,
    PRIVATE_PEM_MARKERS,
};

/// An Ed25519 **public** key, SubjectPublicKeyInfo PEM.
const KEY_A_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(KEY_A_PEM's SPKI DER)`, lowercase hex.
const KEY_A_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
/// A second Ed25519 **public** key.
const KEY_B_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAKTrTSpPTt1d9M1kMim3Imkt2s1OjRm48GfqVh+fjvyk=\n-----END PUBLIC KEY-----\n";
/// `sha256(KEY_B_PEM's SPKI DER)`, lowercase hex.
const KEY_B_ID: &str = "067bf4d360d3c0658620a75a225d4d3e2e038cdb12cdf38ef6611241b9d380d2";

/// The YAML document after the header comment and its `---` separator.
///
/// NOT `split("---\n")`: a PEM's own `-----BEGIN PUBLIC KEY-----` line
/// contains that substring, so a naive split cuts the document in half and
/// every assertion below it becomes vacuous. The separator is a LINE that is
/// exactly `---`.
fn yaml_text(out: &str) -> String {
    let mut lines = out.lines();
    let mut body = String::new();
    for line in lines.by_ref() {
        if line == "---" {
            break;
        }
        assert!(
            line.starts_with('#'),
            "everything before the separator is a review comment; got `{line}`"
        );
    }
    for line in lines {
        body.push_str(line);
        body.push('\n');
    }
    body
}

/// The same document, parsed.
fn yaml_body(out: &str) -> serde_yaml::Value {
    serde_yaml::from_str(&yaml_text(out)).expect("the output parses as YAML")
}

/// A `TrustRoster` as `kubectl get -o json` prints it, with `status` and the
/// server-side metadata a real object carries.
fn roster_json() -> String {
    let a = KEY_A_PEM.replace('\n', "\\n");
    let b = KEY_B_PEM.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustRoster",
             "metadata":{{"name":"default","uid":"roster-uid","resourceVersion":"4711",
                          "managedFields":[{{"manager":"kubectl"}}]}},
             "spec":{{"approverKeys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{a}","subject":"ops@example.com","notAfter":"2027-01-01T00:00:00Z"}}],
                      "signingKeys":[{{"keyId":"{KEY_B_ID}","spkiPem":"{b}"}}],
                      "allowedClusterIds":["scratch-cluster-id"]}},
             "status":{{"loaded":true,"expiredKeyIds":[]}}}}"#
    )
}

/// A `TrustPolicy` as `kubectl get -o json` prints it.
fn policy_json() -> String {
    let a = KEY_A_PEM.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicy",
             "metadata":{{"name":"org-default","uid":"policy-uid","resourceVersion":"99",
                          "generation":3,"managedFields":[{{"manager":"kubectl"}}]}},
             "spec":{{"default":true,"namespaces":["team-a"],
                      "allowedTargetClusterIds":["scratch-cluster-id"],
                      "keys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{a}","algorithm":"ed25519",
                                "usages":["EvidenceSigning"],
                                "principal":{{"id":"install:{KEY_A_ID}","display":"prod signer"}},
                                "notBefore":"2026-01-01T00:00:00Z",
                                "notAfter":"2027-06-01T00:00:00Z","state":"Retired",
                                "retiredAt":"2026-05-01T00:00:00Z"}}]}},
             "status":{{"loaded":true,"observedGeneration":3,
                        "evaluatedAt":"2026-09-16T12:00:00Z",
                        "keys":[{{"keyId":"{KEY_A_ID}","effectiveState":"Retired"}}]}}}}"#
    )
}

/// Write `body` to a temp file and hand back its path.
fn temp(body: &str, name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("logweir-trust-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a temp directory");
    let path = dir.join(name);
    std::fs::write(&path, body).expect("a writable temp file");
    path
}

// ---------------------------------------------------------------------------
// migrate-roster — D3 §7.5
// ---------------------------------------------------------------------------

/// The translation, field by field, as a reviewable document.
#[test]
fn migrate_roster_translates_every_field_the_decision_names() {
    let path = temp(&roster_json(), "migrate-roster.json");
    let out = migrate_roster(&MigrateArgs {
        name: "org-default".to_string(),
        default: true,
        namespaces: vec![],
        input: Input::File(path),
    })
    .expect("the roster translates");

    let document = yaml_body(&out);
    assert_eq!(document["kind"].as_str(), Some("TrustPolicy"));
    assert_eq!(
        document["apiVersion"].as_str(),
        Some("logweir.dev/v1alpha1")
    );
    assert_eq!(document["metadata"]["name"].as_str(), Some("org-default"));
    assert_eq!(document["spec"]["default"].as_bool(), Some(true));
    assert_eq!(
        document["spec"]["allowedTargetClusterIds"][0].as_str(),
        Some("scratch-cluster-id"),
        "allowedClusterIds becomes allowedTargetClusterIds"
    );

    let keys = document["spec"]["keys"].as_sequence().expect("keys");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0]["keyId"].as_str(), Some(KEY_A_ID));
    assert_eq!(keys[0]["usages"][0].as_str(), Some("GovernedApproval"));
    assert_eq!(keys[0]["state"].as_str(), Some("Active"));
    assert_eq!(keys[0]["notAfter"].as_str(), Some("2027-01-01T00:00:00Z"));
    assert_eq!(
        keys[0]["principal"]["id"].as_str(),
        Some(format!("legacy:{KEY_A_ID}").as_str())
    );
    assert_eq!(
        keys[0]["principal"]["display"].as_str(),
        Some("ops@example.com"),
        "the roster's `subject` is a display name, not an identity"
    );
    assert_eq!(keys[1]["keyId"].as_str(), Some(KEY_B_ID));
    assert_eq!(keys[1]["usages"][0].as_str(), Some("EvidenceSigning"));
    assert_eq!(
        keys[1]["notAfter"].as_str(),
        Some("9999-12-31T23:59:59Z"),
        "a roster entry with no notAfter does not expire, which is how every consumer reads it"
    );

    for key in keys {
        assert_eq!(
            key["algorithm"].as_str(),
            Some("ed25519"),
            "the algorithm is discovered from the PEM's own SubjectPublicKeyInfo"
        );
        assert_eq!(key["notBefore"].as_str(), Some("1970-01-01T00:00:00Z"));
        assert!(
            key.get("retiredAt").is_none()
                && key.get("revokedAt").is_none()
                && key.get("revocationEffectiveFrom").is_none(),
            "the roster records no lifecycle event, and these three fields are write-once on the \
             CRD: inventing one would assert something nobody recorded and could never take back"
        );
        assert!(
            !key["usages"]
                .as_sequence()
                .expect("usages")
                .iter()
                .any(|u| u.as_str() == Some("ConsoleConfirmation")),
            "D3 §7.3: no ConsoleConfirmation key is ever synthesised"
        );
    }
}

/// **IDEMPOTENT.** Two runs over the same roster produce byte-identical
/// output — no clock is read and no name is generated.
#[test]
fn migrate_roster_is_idempotent() {
    let path = temp(&roster_json(), "idempotent.json");
    let args = || MigrateArgs {
        name: "org-default".to_string(),
        default: true,
        namespaces: vec!["team-a".to_string()],
        input: Input::File(path.clone()),
    };
    let first = migrate_roster(&args()).expect("the first run");
    let second = migrate_roster(&args()).expect("the second run");
    assert_eq!(
        first, second,
        "a clock read or a generated name here would make `kubectl diff` show a change on every \
         re-run of a documented migration command"
    );
}

/// A key on **both** roster lists becomes ONE entry with BOTH usages —
/// `spec.keys` is an associative list keyed by `keyId` and the API server
/// refuses a duplicate.
#[test]
fn migrate_roster_merges_a_key_that_is_on_both_lists() {
    let pem = KEY_A_PEM.replace('\n', "\\n");
    let roster = format!(
        r#"{{"kind":"TrustRoster","metadata":{{"name":"default"}},
             "spec":{{"approverKeys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{pem}"}}],
                      "signingKeys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{pem}"}}],
                      "allowedClusterIds":[]}}}}"#
    );
    let path = temp(&roster, "merged.json");
    let out = migrate_roster(&MigrateArgs {
        name: "org-default".to_string(),
        default: false,
        namespaces: vec!["team-a".to_string()],
        input: Input::File(path),
    })
    .expect("the roster translates");
    let document = yaml_body(&out);
    let keys = document["spec"]["keys"].as_sequence().expect("keys");
    assert_eq!(keys.len(), 1);
    let usages: Vec<&str> = keys[0]["usages"]
        .as_sequence()
        .expect("usages")
        .iter()
        .filter_map(serde_yaml::Value::as_str)
        .collect();
    assert_eq!(usages, vec!["GovernedApproval", "EvidenceSigning"]);
}

/// The output carries a header a reviewer reads before applying it.
#[test]
fn migrate_roster_output_is_reviewable() {
    let path = temp(&roster_json(), "reviewable.json");
    let out = migrate_roster(&MigrateArgs {
        name: "org-default".to_string(),
        default: false,
        namespaces: vec!["team-a".to_string()],
        input: Input::File(path),
    })
    .expect("the roster translates");
    assert!(out.starts_with("# Generated by `logweir trust migrate-roster`"));
    assert!(out.contains("REVIEW THIS FILE BEFORE APPLYING IT"));
    assert!(
        out.contains("The roster is NOT deleted"),
        "D3 §7.5's rollback path is that the old controller still reads TrustRoster/default, \
         present and unchanged"
    );
}

/// A `TrustPolicy` handed to `migrate-roster` is refused, not mangled.
#[test]
fn migrate_roster_refuses_the_wrong_kind() {
    let path = temp(&policy_json(), "wrong-kind.json");
    let err = migrate_roster(&MigrateArgs {
        name: "x".to_string(),
        default: false,
        namespaces: vec![],
        input: Input::File(path),
    })
    .expect_err("a TrustPolicy is not a TrustRoster");
    assert!(err.to_string().contains("TrustRoster"), "{err}");
}

// ---------------------------------------------------------------------------
// export — D3 §7.1
// ---------------------------------------------------------------------------

/// The export carries the spec and nothing the server owns, and re-applies.
#[test]
fn export_writes_the_spec_and_nothing_the_server_owns() {
    let path = temp(&policy_json(), "export.json");
    let out = export(&ExportArgs {
        policy: "org-default".to_string(),
        input: Input::File(path),
    })
    .expect("the policy exports");

    // THE DOCUMENT, NOT THE HEADER. The review header NAMES the four fields it
    // says are absent, so a grep over the whole output would be satisfied by
    // the comment that promises the property rather than by the property.
    let body = yaml_text(&out);
    assert!(
        !body.contains("resourceVersion")
            && !body.contains("managedFields")
            && !body.contains("policy-uid")
            && !body.contains("observedGeneration")
            && !body.contains("status:"),
        "status, managedFields, resourceVersion and uid are absent so the file re-applies \
         cleanly onto any cluster:\n{body}"
    );

    let document = yaml_body(&out);
    assert_eq!(document["kind"].as_str(), Some("TrustPolicy"));
    assert_eq!(document["metadata"]["name"].as_str(), Some("org-default"));
    assert_eq!(document["spec"]["default"].as_bool(), Some(true));
    assert_eq!(document["spec"]["namespaces"][0].as_str(), Some("team-a"));
    let key = &document["spec"]["keys"][0];
    assert_eq!(key["keyId"].as_str(), Some(KEY_A_ID));
    assert_eq!(key["spkiPem"].as_str(), Some(KEY_A_PEM));
    assert_eq!(
        key["state"].as_str(),
        Some("Retired"),
        "the lifecycle a key already has is public material and is carried"
    );
    assert_eq!(key["retiredAt"].as_str(), Some("2026-05-01T00:00:00Z"));
    assert_eq!(key["principal"]["display"].as_str(), Some("prod signer"));
}

/// `--policy` naming a different object is a refusal, not a rename.
#[test]
fn export_refuses_to_rename_the_object_it_was_handed() {
    let path = temp(&policy_json(), "rename.json");
    let err = export(&ExportArgs {
        policy: "team-b".to_string(),
        input: Input::File(path),
    })
    .expect_err("the name must match");
    assert!(err.to_string().contains("org-default"), "{err}");
}

/// The export is a REBUILD: a field this build has never heard of is not
/// forwarded.
#[test]
fn export_does_not_forward_a_field_this_build_does_not_know() {
    let injected = policy_json().replace(
        r#""state":"Retired""#,
        r#""state":"Retired","somethingNobodyReviewed":"forwarded""#,
    );
    let path = temp(&injected, "unknown-field.json");
    let out = export(&ExportArgs {
        policy: "org-default".to_string(),
        input: Input::File(path),
    })
    .expect("the policy exports");
    assert!(
        !out.contains("somethingNobodyReviewed"),
        "a filtered copy forwards whatever it has not been taught to remove; a rebuild cannot. \
         Got:\n{out}"
    );
}

/// **MUTANT 4 — private material in export.**
///
/// The planted mutation is dropping [`refuse_private_material`] and letting the
/// rebuild forward `spkiPem` verbatim. The object below carries a private-key
/// PEM header where a public key belongs — the exact hand-edit the check
/// exists for — and this test asserts both halves: the command REFUSES, and
/// the refusal message does not quote the material.
#[test]
fn mutant_export_refuses_an_object_carrying_private_material() {
    for marker in PRIVATE_PEM_MARKERS {
        let secret = format!("-----{marker}-----\\nQk9HVVM=\\n-----END PRIVATE KEY-----\\n");
        let poisoned = policy_json().replace(&KEY_A_PEM.replace('\n', "\\n"), &secret);
        let path = temp(&poisoned, "poisoned.json");
        let err = export(&ExportArgs {
            policy: "org-default".to_string(),
            input: Input::File(path),
        })
        .expect_err("an object carrying a private key is refused");
        let message = err.to_string();
        assert!(message.contains(marker), "{message}");
        assert!(
            message.contains("REFUSING to write anything"),
            "nothing is written — a partial file an operator commits is the disclosure this \
             check exists to prevent: {message}"
        );
        assert!(
            !message.contains("Qk9HVVM="),
            "the refusal must never quote the material: an error that echoed the key would put \
             it in the terminal scrollback, the shell history and any CI log capturing stderr"
        );
    }
    // And the same guard is what `migrate-roster` runs, so a roster carrying a
    // private key never reaches the output either.
    assert!(refuse_private_material(KEY_A_PEM).is_ok());
    assert!(refuse_private_material("-----BEGIN EC PRIVATE KEY-----").is_err());
}

/// Four spellings, because a check that knew only one would pass the others.
#[test]
fn the_private_marker_list_covers_every_pem_spelling() {
    assert_eq!(PRIVATE_PEM_MARKERS.len(), 4);
    for marker in ["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"] {
        assert!(
            PRIVATE_PEM_MARKERS.iter().any(|m| m.contains(marker)),
            "PKCS#8, PKCS#1, SEC1 and the encrypted form all say `private` differently; \
             `{marker}` is not covered"
        );
    }
}

// ---------------------------------------------------------------------------
// The argv surface, against the COMPILED BINARY
// ---------------------------------------------------------------------------

/// The binary under test.
fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

/// Run the binary with `stdin_body` on standard input.
fn run(args: &[&str], stdin_body: &str) -> (Option<i32>, String, String) {
    let mut child = bin()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the binary starts");
    child
        .stdin
        .as_mut()
        .expect("a piped stdin")
        .write_all(stdin_body.as_bytes())
        .expect("stdin accepts the object");
    // `wait_with_output` closes stdin and reads both pipes to EOF, so the child
    // cannot block on a full pipe and this cannot hang (WORKER-RULES: every
    // subprocess a test spawns must be bounded).
    let out = child.wait_with_output().expect("the binary exits");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// **The shipped binary reaches both leaves.** A subcommand that exists in
/// `cli.rs` and is unreachable from `main` is a defect this repository has
/// shipped before.
#[test]
fn the_compiled_binary_reaches_both_trust_leaves() {
    let (code, stdout, stderr) = run(
        &[
            "trust",
            "migrate-roster",
            "--stdin",
            "--name",
            "org-default",
            "--default",
        ],
        &roster_json(),
    );
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(stdout.contains("kind: TrustPolicy"), "{stdout}");
    assert!(
        stderr.contains("trust-migrate-name=org-default"),
        "the summary goes to STDERR so `> trustpolicy.yaml` captures the document and nothing \
         else: {stderr}"
    );

    let (code, stdout, stderr) = run(
        &["trust", "export", "--policy", "org-default", "--stdin"],
        &policy_json(),
    );
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(stdout.contains("kind: TrustPolicy"), "{stdout}");
    assert!(stderr.contains("trust-export-policy=org-default"));
}

/// The document on stdout is a document and nothing else — it parses as YAML
/// with no summary line in it.
#[test]
fn the_document_on_stdout_is_only_the_document() {
    let (code, stdout, _) = run(
        &["trust", "export", "--policy", "org-default", "--stdin"],
        &policy_json(),
    );
    assert_eq!(code, Some(0));
    let parsed = yaml_body(&stdout);
    assert_eq!(parsed["kind"].as_str(), Some("TrustPolicy"));
    assert!(!stdout.contains("trust-export-policy="));
}

/// A refusal exits 1 — `Operational`, never a drill result. Nothing ran and
/// no scorecard exists.
#[test]
fn a_refusal_exits_operational_and_writes_no_document() {
    let (code, stdout, stderr) = run(
        &["trust", "export", "--policy", "org-default", "--stdin"],
        "this is not a Kubernetes object",
    );
    assert_eq!(
        code,
        Some(1),
        "Global Constraint 11 reserves 2 for a drill result that is not a pass; a malformed \
         input is operational. stderr: {stderr}"
    );
    assert!(stdout.is_empty(), "nothing is written: {stdout}");
}

/// Exactly one of `--stdin` and `--from` is required, and clap enforces it —
/// so neither can be silently ignored.
#[test]
fn exactly_one_input_flag_is_required() {
    let (code, _, stderr) = run(&["trust", "export", "--policy", "org-default"], "");
    assert_eq!(
        code,
        Some(1),
        "a usage error is operational, never a drill result"
    );
    assert!(
        stderr.contains("--stdin") || stderr.contains("--from"),
        "{stderr}"
    );

    let path = temp(&policy_json(), "conflict.json");
    let (code, _, stderr) = run(
        &[
            "trust",
            "export",
            "--policy",
            "org-default",
            "--stdin",
            "--from",
            path.to_str().expect("a UTF-8 temp path"),
        ],
        "",
    );
    assert_eq!(code, Some(1));
    assert!(
        stderr.contains("cannot be used with"),
        "clap refuses both at once rather than letting one win silently: {stderr}"
    );
}

/// `--help` on the group still exits 0, so the release smoke test can read it.
#[test]
fn trust_help_exits_ok() {
    let out = bin()
        .args(["trust", "--help"])
        .output()
        .expect("the binary");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("export"));
    assert!(text.contains("migrate-roster"));
}
