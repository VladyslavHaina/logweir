//! The chart's rendered policy `ConfigMap`, parsed by the code that will parse
//! it in a cluster — D2 §4.4, W11.
//!
//! # Why this file exists, and why it is HERE
//!
//! `charts/logweir/templates/policy.yaml` renders a JSON document and
//! `weirkeeper::check::policy::parse` consumes it. Between them sits
//! `serde(deny_unknown_fields)` and a `validate()` with ten range rules, and
//! the failure mode when they disagree is the worst kind: the install
//! SUCCEEDS, the controller STARTS, and every policy read fails closed —
//! `Policy::fail_closed()`, no attestations, no evidence allowlist — reporting
//! itself only as one advisory `configuration.policy notReady PolicyUnreadable`
//! row on a `Preflight` nobody is necessarily looking at. So
//! `attestedComplete` silently becomes unreachable and every check runs on
//! compiled-in defaults, and nothing anywhere is red.
//!
//! `crates/logweir/tests/chart_lint.rs` asserts the document's SHAPE — it is
//! the crate that owns the chart gates — but `logweir` declares no
//! `weirkeeper` edge, so it cannot call the parser. This file is the other
//! half, and it is one integration test rather than a dependency edge: the
//! rendered bytes in the tree, through the real `parse`, in the crate that
//! owns it.
//!
//! **It reads files and calls a pure function.** No Kubernetes, no network, no
//! subprocess.

use std::collections::BTreeMap;
use std::path::PathBuf;

use weirkeeper::check::policy::{self, Policy, POLICY_KEY};

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root is above crates/weirkeeper")
}

/// Every `policy.json` in one rendered chart file, by ConfigMap name.
fn rendered_policies(name: &str) -> BTreeMap<String, String> {
    let path = repo().join(format!("charts/logweir/rendered/{name}.yaml"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut out = BTreeMap::new();
    for doc in serde_yaml::Deserializer::from_str(&text) {
        let value = match serde_yaml::Value::deserialize(doc) {
            Ok(v) => v,
            Err(e) => panic!("{name}.yaml holds a document that is not YAML: {e}"),
        };
        if value["kind"].as_str() != Some("ConfigMap") {
            continue;
        }
        let Some(raw) = value["data"][POLICY_KEY].as_str() else {
            continue;
        };
        let cm = value["metadata"]["name"]
            .as_str()
            .expect("a ConfigMap has a name")
            .to_string();
        out.insert(cm, raw.to_string());
    }
    out
}

use serde::Deserialize as _;

/// **The default render's policy parses, validates, and is the compiled-in
/// defaults.**
///
/// The second half matters as much as the first: an install that changes no
/// value must behave exactly as an install that renders no policy at all, or
/// "the chart now renders a policy" would be a silent behaviour change on
/// every upgrade.
///
/// MUTANT: change any number in `values.yaml` without meaning to — say
/// `keepPerConnection: 4` — and this test names the field.
#[test]
fn the_default_render_parses_and_equals_the_compiled_in_defaults() {
    let policies = rendered_policies("default");
    let raw = policies
        .get("weirkeeper-policy")
        .unwrap_or_else(|| panic!("the default render carries no weirkeeper-policy: {policies:?}"));
    let parsed = policy::parse(raw.as_bytes()).unwrap_or_else(|e| {
        panic!(
            "the chart's own policy document is REFUSED by the parser \
             that reads it in a cluster: {e}\n{raw}"
        )
    });
    assert_eq!(
        parsed,
        Policy::defaults(),
        "the chart's default policy must be the controller's compiled-in defaults, or \
         rendering one changes behaviour on every upgrade that did not ask for it"
    );
}

/// **A configured render parses too, and its attestation is a real one.**
///
/// `charts/logweir/examples/admission-policy.values.yaml` carries the
/// administrator attestation and the evidence allowlist, which are the two
/// collections `Policy::fail_closed()` clears — so they are exactly what a
/// parse failure would silently remove.
#[test]
fn a_configured_render_parses_and_keeps_its_attestation_and_allowlist() {
    let policies = rendered_policies("admission-policy");
    let raw = policies
        .get("weirkeeper-policy")
        .expect("the configured render carries weirkeeper-policy");
    let parsed = policy::parse(raw.as_bytes())
        .unwrap_or_else(|e| panic!("a configured policy document is refused: {e}\n{raw}"));

    assert_eq!(parsed.discovery.visibility_attestations.len(), 1);
    let att = &parsed.discovery.visibility_attestations[0];
    assert_eq!(att.id, "att-orders-prod");
    assert_eq!(att.namespace, "team-a");
    assert_eq!(att.kafka_cluster, "source");
    assert_eq!(att.principal, "User:backup");
    assert!(
        !att.cluster_id.trim().is_empty(),
        "a blank clusterId matches nothing while LOOKING like an attestation"
    );
    assert!(att.expires_at > att.attested_at);
    assert_eq!(parsed.evidence.controller_identity_locations.len(), 1);
    assert_eq!(
        parsed.evidence.controller_identity_locations[0].bucket,
        "lw-b"
    );

    // AND `fail_closed` REALLY WOULD REMOVE THEM — which is the cost this test
    // exists to keep somebody from paying by accident.
    let closed = parsed.clone().closed();
    assert!(closed.discovery.visibility_attestations.is_empty());
    assert!(closed.evidence.controller_identity_locations.is_empty());
}

/// **Every rendered chart file's policy document parses.** The walk, so a new
/// example cannot add a render nobody checked.
#[test]
fn every_rendered_policy_document_parses() {
    let dir = repo().join("charts/logweir/rendered");
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&dir).expect("the rendered directory") {
        let path = entry.expect("a directory entry").path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        for (name, raw) in rendered_policies(stem) {
            policy::parse(raw.as_bytes()).unwrap_or_else(|e| {
                panic!("{stem}.yaml's {name} is refused by check::policy::parse: {e}")
            });
            checked += 1;
        }
    }
    assert!(
        checked >= 7,
        "only {checked} rendered policy document(s) were parsed; every rendered file carries \
         one, so this walk has gone quiet"
    );
}

/// **A document with an unknown key is REFUSED, and that is why the key set is
/// asserted exactly elsewhere.** The negative control for the three tests
/// above: without it they could pass over a parser that ignored what it did not
/// recognise.
#[test]
fn an_unknown_key_is_refused_which_is_what_makes_the_shape_assertions_matter() {
    let raw = rendered_policies("default")
        .remove("weirkeeper-policy")
        .expect("the default render carries a policy");
    let mut doc: serde_json::Value = serde_json::from_str(&raw).expect("it is JSON");
    doc["discovery"]["keepPerConection"] = serde_json::json!(5); // one letter short
    let refused = policy::parse(doc.to_string().as_bytes());
    assert!(
        refused.is_err(),
        "a misspelled key must be REFUSED; if it were ignored, a chart that renamed a field \
         would silently drop the value and nothing would say so"
    );
    // And a value out of range, which `validate()` and not `serde` catches.
    let mut doc: serde_json::Value = serde_json::from_str(&raw).expect("it is JSON");
    doc["discovery"]["keepPerConnection"] = serde_json::json!(0);
    assert!(
        policy::parse(doc.to_string().as_bytes()).is_err(),
        "keeping none would delete a discovery the moment it finished"
    );
}

/// **The chart's schema bounds are the parser's own rules** — review finding
/// **F1**, fix round 1.
///
/// # The defect this closes
///
/// `values.schema.json` bounded `hardMaxTopics` at `maximum: 200000` while
/// [`logweir_core::check_contract::MAX_TOPICS_CEILING`] — which
/// `Policy::validate` enforces — is **50 000**. So
/// `helm install --set checks.discovery.hardMaxTopics=100000` SUCCEEDED,
/// rendered `weirkeeper-policy`, and every policy read afterwards was
/// `PolicyLoad::Unreadable` → `Policy::fail_closed()`: every
/// `visibilityAttestations` entry and every `controllerIdentityLocations`
/// entry the administrator configured silently discarded, `attestedComplete`
/// unreachable, retention back to the compiled-in defaults — with one advisory
/// row on a `Preflight` as the only signal. Both operator docs tell the reader
/// to validate a hand-written policy *against this schema*, so the documented
/// remedy did not catch it either.
///
/// # What is asserted, and why a literal is not enough
///
/// The bound is compared with the CONSTANT, not with `50000`: a schema that
/// merely happens to agree today is a schema that drifts the next time the
/// ceiling moves. The same for the preflight timeout's `1..=600`.
///
/// MUTANT: set `"maximum": 200000` back on `hardMaxTopics`, or change
/// `MAX_TOPICS_CEILING`, and this fails naming both numbers.
#[test]
fn the_schema_bounds_are_the_parsers_own_rules() {
    let schema: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo().join("charts/logweir/values.schema.json"))
            .expect("the chart schema is readable"),
    )
    .expect("the chart schema is JSON");
    let discovery = &schema["properties"]["checks"]["properties"]["discovery"]["properties"];
    assert_eq!(
        discovery["hardMaxTopics"]["maximum"].as_u64(),
        Some(u64::from(logweir_core::check_contract::MAX_TOPICS_CEILING)),
        "values.schema.json's `hardMaxTopics` bound must EQUAL \
         `check_contract::MAX_TOPICS_CEILING` ({}), which is what `Policy::validate` \
         enforces. A looser schema lets `helm install` succeed on a document the \
         controller then refuses — and a refused policy fails CLOSED and silently.",
        logweir_core::check_contract::MAX_TOPICS_CEILING
    );
    // FX-10: the two withdrawn keys are ALLOWED (so an older values file still
    // installs), OPTIONAL, UNBOUNDED (a value nothing reads must not be able to
    // fail an install) and say so.
    let checks_schema = &schema["properties"]["checks"]["properties"];
    for (block, key) in [
        ("discovery", "defaultMaxTopics"),
        ("preflight", "defaultTimeoutSeconds"),
    ] {
        let node = &checks_schema[block]["properties"][key];
        assert_eq!(
            node["type"], "integer",
            "`checks.{block}.{key}` is still typed"
        );
        assert!(
            node.get("minimum").is_none() && node.get("maximum").is_none(),
            "`checks.{block}.{key}` is withdrawn and bounds nothing: {node}"
        );
        assert!(
            node["description"]
                .as_str()
                .is_some_and(|d| d.starts_with("WITHDRAWN (FX-10")),
            "`checks.{block}.{key}` says it is withdrawn: {node}"
        );
        assert!(
            !checks_schema[block]["required"]
                .as_array()
                .expect("a required list")
                .iter()
                .any(|r| r == key),
            "`checks.{block}.{key}` must not be required"
        );
    }
    // Every field `validate()` bounds at >= 1 is bounded at >= 1 here too.
    let checks = &schema["properties"]["checks"]["properties"];
    for field in [
        "maxActivePerNamespace",
        "maxActiveTotal",
        "maxActiveDiscoveriesPerConnection",
        "maxEvidenceFetchActivePerNamespace",
    ] {
        assert_eq!(
            checks[field]["minimum"].as_u64(),
            Some(1),
            "`checks.{field}` must be bounded at >= 1, as `Policy::validate` bounds it"
        );
    }
    for field in ["keepPerConnection", "hardMaxTopics"] {
        assert_eq!(
            discovery[field]["minimum"].as_u64(),
            Some(1),
            "`checks.discovery.{field}` must be bounded at >= 1"
        );
    }
    // P10: the manual-run pool ceilings — `validate()` refuses a zero, so the
    // schema must, and both are required (a half block is a refused document).
    let runs = &schema["properties"]["runs"];
    for field in [
        "maxManualBackupsActivePerNamespace",
        "maxManualRestoresActivePerNamespace",
    ] {
        assert_eq!(
            runs["properties"][field]["minimum"].as_u64(),
            Some(1),
            "`runs.{field}` must be bounded at >= 1, as `Policy::validate` bounds it"
        );
        assert!(
            runs["required"]
                .as_array()
                .is_some_and(|r| r.iter().any(|f| f == field)),
            "`runs.{field}` is required: `RunsPolicy` has no per-field default"
        );
        let mut zero: serde_json::Value = serde_json::json!({
            "version": 1,
            "runs": {
                "maxManualBackupsActivePerNamespace": 4,
                "maxManualRestoresActivePerNamespace": 2
            }
        });
        zero["runs"][field] = serde_json::json!(0);
        assert!(
            policy::parse(zero.to_string().as_bytes()).is_err(),
            "`runs.{field}: 0` is refused by the parser, as the schema refuses it"
        );
    }
}

/// **A document at every schema extreme is a document the parser accepts.**
///
/// The other direction of [`the_schema_bounds_are_the_parsers_own_rules`]: the
/// bounds above are compared one at a time, so a schema that is too STRICT —
/// refusing an install the controller would have been happy with — would pass
/// them. This builds the largest and the smallest document the schema admits
/// and runs both through the real parser.
///
/// MUTANT: lower `hardMaxTopics`'s maximum below the ceiling and the maximal
/// document still parses but the pin above fails; raise it above and this one
/// fails, because `validate()` refuses the document the schema now admits.
#[test]
fn the_extremes_the_schema_admits_are_documents_the_parser_accepts() {
    let ceiling = u64::from(logweir_core::check_contract::MAX_TOPICS_CEILING);
    let at = |maxima: bool| -> serde_json::Value {
        let n = |lo: u64, hi: u64| if maxima { hi } else { lo };
        serde_json::json!({
            "version": 1,
            "checks": {
                "maxActivePerNamespace": n(1, 4),
                "maxActiveTotal": n(1, 1_000_000),
                "maxActiveDiscoveriesPerConnection": n(1, 1_000_000),
                "maxEvidenceFetchActivePerNamespace": n(1, 1_000_000)
            },
            "discovery": {
                "freshSeconds": n(1, 1_000_000),
                "retentionSeconds": n(1, 1_000_000),
                "keepPerConnection": n(1, 1_000_000),
                // FX-10: withdrawn; the chart renders min(20 000, hardMaxTopics).
                "defaultMaxTopics": n(1, u64::from(policy::WITHDRAWN_DEFAULT_MAX_TOPICS)),
                "hardMaxTopics": n(1, ceiling),
                "visibilityAttestations": []
            },
            "preflight": {
                "defaultTimeoutSeconds": policy::WITHDRAWN_DEFAULT_TIMEOUT_SECONDS,
                "retentionSeconds": n(1, 1_000_000)
            },
            "engine": {"allowUnverifiedCustomCa": false},
            "evidence": {"controllerIdentityLocations": []},
            "legacyArchiveAddressing": {"endpoint": "", "region": "",
                                        "allowHttp": false, "virtualHostedStyle": false},
            "runs": {
                "maxManualBackupsActivePerNamespace": n(1, 1_000_000),
                "maxManualRestoresActivePerNamespace": n(1, 1_000_000)
            }
        })
    };
    for maxima in [false, true] {
        let doc = at(maxima);
        policy::parse(doc.to_string().as_bytes()).unwrap_or_else(|e| {
            panic!(
                "a document at the schema's {} is refused by the parser: {e}\n{doc}",
                if maxima {
                    "upper bounds"
                } else {
                    "lower bounds"
                }
            )
        });
    }
}

/// **The cross-field rule `Policy::validate` enforces is real**, so the `fail`
/// `charts/logweir/templates/policy.yaml` carries for it is not
/// belt-and-braces over a rule that does not exist — and the rule FX-10
/// withdrew with `defaultMaxTopics` is really gone, so a document an older
/// chart rendered is never refused for it.
///
/// JSON Schema draft-07 cannot compare two sibling values, which is why the
/// rule is refused at render time instead. This is the half that proves the
/// rule it mirrors.
#[test]
fn the_rule_the_schema_cannot_express_is_a_rule_the_parser_enforces() {
    let base: serde_json::Value = serde_json::from_str(
        &rendered_policies("default")
            .remove("weirkeeper-policy")
            .expect("the default render carries a policy"),
    )
    .expect("it is JSON");

    let mut inverted_pool = base.clone();
    inverted_pool["checks"]["maxActiveTotal"] = serde_json::json!(1);
    inverted_pool["checks"]["maxActivePerNamespace"] = serde_json::json!(4);
    assert!(
        policy::parse(inverted_pool.to_string().as_bytes()).is_err(),
        "`maxActiveTotal` below `maxActivePerNamespace` must be refused; \
         templates/policy.yaml fails the render for it"
    );

    // FX-10: the withdrawn pair's rule is gone. An older chart's document with
    // `defaultMaxTopics` above `hardMaxTopics` (a value nothing reads) is
    // accepted, where it used to fail closed.
    let mut inverted_topics = base;
    inverted_topics["discovery"]["defaultMaxTopics"] = serde_json::json!(50_000);
    inverted_topics["discovery"]["hardMaxTopics"] = serde_json::json!(100);
    let parsed = policy::parse(inverted_topics.to_string().as_bytes())
        .expect("a withdrawn value out of its old range is ignored, not refused");
    assert_eq!(parsed.discovery.hard_max_topics, 100);
}

/// **The chart's own template refuses the cross-field pair**, named, at
/// render time — asserted over the template text because `chart_lint` is the
/// crate that runs `helm` and this one does not.
#[test]
fn the_policy_template_names_its_cross_field_rule_in_its_refusal() {
    let template = std::fs::read_to_string(repo().join("charts/logweir/templates/policy.yaml"))
        .expect("the policy template is readable");
    let code: String = template
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code.matches("{{- fail (printf").count(),
        1,
        "one cross-field rule, one named refusal (FX-10 withdrew the other with its value)"
    );
    for needle in ["checks.maxActiveTotal (%d) must be at least checks.maxActivePerNamespace"] {
        assert!(
            code.contains(needle),
            "templates/policy.yaml must refuse the render naming the rule and both values; \
             `{needle}` is missing"
        );
    }
    assert!(
        code.contains("fails CLOSED"),
        "the refusal says WHY it is a render-time error and not a runtime one: a policy the \
         parser refuses discards every attestation and every evidence location silently"
    );

    // AND THE CONDITIONS ARE LIVE. Pinning the MESSAGES alone was not enough
    // and a mutant proved it: replacing `{{- if lt $maxTotal $maxNs -}}` with
    // `{{- if false -}}` leaves both `fail`s and both message strings exactly
    // where they were, so a text scan passes over a template that refuses
    // nothing. `scripts/check-chart.sh` renders the two inverted pairs and is
    // the live proof; this is the cheap half that says which comparison each
    // `fail` hangs off.
    for condition in ["{{- if lt $maxTotal $maxNs -}}"] {
        assert!(
            code.contains(condition),
            "templates/policy.yaml must guard its refusal with `{condition}`; a `fail` behind a \
             condition that cannot fire is a message nobody ever reads"
        );
    }
    assert!(
        !code.contains("{{- if false -}}"),
        "a disabled guard in the policy template"
    );
}

/// **P10 review L2: the numbers the chart renders NOTHING for are the
/// controller's own defaults.** `templates/policy.yaml` omits the `runs` block
/// when `runs.*` equals `RunsPolicy::default()` (so an older controller, which
/// refuses an unknown block, keeps reading a default install's policy), and
/// the values file ships those same defaults. Three spellings of one pair,
/// read together here.
#[test]
fn the_runs_block_is_omitted_exactly_at_the_controllers_defaults() {
    let defaults = policy::RunsPolicy::default();
    let values: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(repo().join("charts/logweir/values.yaml")).expect("values"),
    )
    .expect("YAML");
    assert_eq!(
        values["runs"]["maxManualBackupsActivePerNamespace"].as_u64(),
        Some(u64::from(defaults.max_manual_backups_active_per_namespace))
    );
    assert_eq!(
        values["runs"]["maxManualRestoresActivePerNamespace"].as_u64(),
        Some(u64::from(defaults.max_manual_restores_active_per_namespace))
    );
    let template = std::fs::read_to_string(repo().join("charts/logweir/templates/policy.yaml"))
        .expect("the template");
    let guard = format!(
        "(ne $runsBackups {}) (ne $runsRestores {})",
        defaults.max_manual_backups_active_per_namespace,
        defaults.max_manual_restores_active_per_namespace
    );
    assert!(
        template.contains(&guard),
        "templates/policy.yaml must carry `{guard}`"
    );
    // AND THE DEFAULT RENDER HAS NO BLOCK, which the parser reads as defaults.
    let rendered = rendered_policies("default")
        .remove("weirkeeper-policy")
        .expect("the default render carries a policy");
    let parsed = policy::parse(rendered.as_bytes()).expect("the default render parses");
    assert!(!rendered.contains("\"runs\""), "{rendered}");
    assert_eq!(parsed.runs, defaults);
}

// ======================================================================
// FX-10 — configured values that reach nothing
// ======================================================================

/// `values.yaml`, parsed.
fn values() -> serde_yaml::Value {
    serde_yaml::from_str(
        &std::fs::read_to_string(repo().join("charts/logweir/values.yaml")).expect("values"),
    )
    .expect("values.yaml is YAML")
}

/// One example values file, parsed.
fn example(name: &str) -> serde_yaml::Value {
    let path = repo().join(format!("charts/logweir/examples/{name}.values.yaml"));
    serde_yaml::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("the example is YAML")
}

fn yaml_at<'a>(node: &'a serde_yaml::Value, dotted: &str) -> &'a serde_yaml::Value {
    dotted.split('.').fold(node, |n, segment| &n[segment])
}

fn json_at<'a>(node: &'a serde_json::Value, dotted: &str) -> &'a serde_json::Value {
    dotted.split('.').fold(node, |n, segment| &n[segment])
}

/// Every leaf path under `node`: a mapping that is empty, and every sequence
/// and scalar, is one leaf.
fn yaml_leaves(node: &serde_yaml::Value, prefix: &str, out: &mut Vec<String>) {
    match node.as_mapping() {
        Some(map) if !map.is_empty() => {
            for (k, v) in map {
                let k = k.as_str().expect("a string key");
                let path = if prefix.is_empty() {
                    k.to_string()
                } else {
                    format!("{prefix}.{k}")
                };
                yaml_leaves(v, &path, out);
            }
        }
        _ => out.push(prefix.to_string()),
    }
}

fn json_leaves(node: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    match node.as_object() {
        Some(map) if !map.is_empty() => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                json_leaves(v, &path, out);
            }
        }
        _ => out.push(prefix.to_string()),
    }
}

/// The template's executable text: every `{{/* … */}}` comment removed, so a
/// value NAMED in prose is not mistaken for a value READ.
fn template_code(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("/*") {
        out.push_str(&rest[..start]);
        rest = rest[start..]
            .find("*/")
            .map_or("", |end| &rest[start + end + 2..]);
    }
    out.push_str(rest);
    out
}

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// **FX-10: the two withdrawn fields are rendered at the parser's fixed
/// compatibility values, never from a value, and nothing reads them.**
///
/// * The template reads neither withdrawn value (its comments may name them).
/// * It renders `defaultMaxTopics` as `min(WITHDRAWN_DEFAULT_MAX_TOPICS,
///   hardMaxTopics)` and `defaultTimeoutSeconds` as
///   `WITHDRAWN_DEFAULT_TIMEOUT_SECONDS` — the two constants the parser's
///   serde defaults use, so the digest of a document with and without them is
///   one digest.
/// * Every committed render carries both keys inside the range a controller
///   older than FX-10 requires (`1 <= defaultMaxTopics <= hardMaxTopics`,
///   `1..=600`): such a controller refuses a document without them, and a
///   refused document fails CLOSED. That is what keeps an image-only rollback
///   of an install safe.
/// * No source in any crate outside the parser reads either field.
///
/// MUTANTS: render `.Values.checks.discovery.defaultMaxTopics` again; drop
/// either key from the template; read `withdrawn_default_max_topics` anywhere.
#[test]
fn the_withdrawn_fields_are_rendered_at_the_parsers_compatibility_values_and_read_by_nothing() {
    let template = std::fs::read_to_string(repo().join("charts/logweir/templates/policy.yaml"))
        .expect("the policy template");
    let code = template_code(&template);
    for withdrawn in [
        ".Values.checks.discovery.defaultMaxTopics",
        ".Values.checks.preflight.defaultTimeoutSeconds",
    ] {
        assert!(
            !code.contains(withdrawn),
            "templates/policy.yaml reads `{withdrawn}` again; it is withdrawn (FX-10)"
        );
    }
    let compat_topics = format!(
        "$compatMaxTopics := min {} (int .Values.checks.discovery.hardMaxTopics)",
        policy::WITHDRAWN_DEFAULT_MAX_TOPICS
    );
    assert!(code.contains(&compat_topics), "missing `{compat_topics}`");
    assert!(code.contains("\"defaultMaxTopics\" $compatMaxTopics"));
    let compat_timeout = format!(
        "\"defaultTimeoutSeconds\" {}",
        policy::WITHDRAWN_DEFAULT_TIMEOUT_SECONDS
    );
    assert!(code.contains(&compat_timeout), "missing `{compat_timeout}`");

    let dir = repo().join("charts/logweir/rendered");
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&dir).expect("the rendered directory") {
        let path = entry.expect("an entry").path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        for (name, raw) in rendered_policies(stem) {
            let doc: serde_json::Value = serde_json::from_str(&raw).expect("JSON");
            let hard = doc["discovery"]["hardMaxTopics"]
                .as_u64()
                .expect("hardMaxTopics");
            let topics = doc["discovery"]["defaultMaxTopics"]
                .as_u64()
                .unwrap_or_else(|| panic!("{stem}.yaml's {name} lost discovery.defaultMaxTopics"));
            let timeout = doc["preflight"]["defaultTimeoutSeconds"]
                .as_u64()
                .unwrap_or_else(|| {
                    panic!("{stem}.yaml's {name} lost preflight.defaultTimeoutSeconds")
                });
            assert!(
                (1..=hard).contains(&topics),
                "{stem}.yaml: defaultMaxTopics {topics} is outside the 1..={hard} a pre-FX-10 \
                 controller requires"
            );
            assert_eq!(
                topics,
                hard.min(u64::from(policy::WITHDRAWN_DEFAULT_MAX_TOPICS)),
                "{stem}.yaml"
            );
            assert_eq!(
                timeout,
                u64::from(policy::WITHDRAWN_DEFAULT_TIMEOUT_SECONDS),
                "{stem}.yaml"
            );
            checked += 1;
        }
    }
    assert!(checked >= 8, "only {checked} rendered documents were read");

    let mut sources = Vec::new();
    rust_files(&repo().join("crates"), &mut sources);
    let parser = repo().join("crates/weirkeeper/src/check/policy.rs");
    for path in sources
        .iter()
        .filter(|p| **p != parser && p.components().any(|c| c.as_os_str() == "src"))
    {
        let text = std::fs::read_to_string(path).expect("a source file");
        for field in [
            "withdrawn_default_max_topics",
            "withdrawn_default_timeout_seconds",
        ] {
            assert!(
                !text.contains(field),
                "{} reads `{field}`. It is withdrawn (FX-10): wiring it up again is a contract \
                 change (the CRD defaults, the console's canonical form) that needs its own row",
                path.display()
            );
        }
    }
}

/// One installation-policy value, followed end to end.
struct Followed {
    /// The dotted `values.yaml` path an administrator sets.
    value: &'static str,
    /// The dotted path in the parsed `Policy`'s JSON (its wire names).
    field: &'static str,
    /// The controller source (under `crates/weirkeeper/src/`) that reads the
    /// parsed field, and a needle in it — or `None` with the reason no reader
    /// exists yet.
    reader: Result<(&'static str, &'static str), &'static str>,
    /// The test file (under `crates/weirkeeper/tests/`) and the `fn` that
    /// drives the reader at a NON-default value and asserts the effect.
    proof: Option<(&'static str, &'static str)>,
}

/// The table [`every_installation_policy_value_reaches_its_field_and_a_reader_at_a_non_default_value`]
/// holds complete: every `values.yaml` leaf the policy document is rendered
/// from, and every field the parser keeps.
const FOLLOWED: &[Followed] = &[
    Followed {
        value: "checks.maxActivePerNamespace",
        field: "checks.maxActivePerNamespace",
        reader: Ok(("check/limits.rs", "policy.max_active_per_namespace")),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_ceilings_reach_discovery_admission_and_the_plan",
        )),
    },
    Followed {
        value: "checks.maxActiveTotal",
        field: "checks.maxActiveTotal",
        reader: Ok(("check/limits.rs", "policy.max_active_total")),
        proof: Some(("check_framework.rs", "limits_queues_over_namespace_cap")),
    },
    Followed {
        value: "checks.maxActiveDiscoveriesPerConnection",
        field: "checks.maxActiveDiscoveriesPerConnection",
        reader: Ok((
            "check/limits.rs",
            "policy.max_active_discoveries_per_connection",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_ceilings_reach_discovery_admission_and_the_plan",
        )),
    },
    Followed {
        value: "checks.maxEvidenceFetchActivePerNamespace",
        field: "checks.maxEvidenceFetchActivePerNamespace",
        reader: Ok((
            "check/limits.rs",
            "policy.max_evidence_fetch_active_per_namespace",
        )),
        proof: Some((
            "check_framework.rs",
            "every_check_ceiling_is_the_policys_own_at_a_non_default_value",
        )),
    },
    Followed {
        value: "checks.discovery.freshSeconds",
        field: "discovery.freshSeconds",
        reader: Ok((
            "controllers/topic_discovery.rs",
            "load.policy().discovery.fresh_seconds",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_fresh_window_reaches_fresh_until",
        )),
    },
    Followed {
        value: "checks.discovery.retentionSeconds",
        field: "discovery.retentionSeconds",
        reader: Ok((
            "controllers/topic_discovery.rs",
            "discovery_policy.retention_seconds",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_collector_rules_reach_the_collector",
        )),
    },
    Followed {
        value: "checks.discovery.keepPerConnection",
        field: "discovery.keepPerConnection",
        reader: Ok((
            "controllers/topic_discovery.rs",
            "discovery_policy.keep_per_connection",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_collector_rules_reach_the_collector",
        )),
    },
    Followed {
        value: "checks.discovery.hardMaxTopics",
        field: "discovery.hardMaxTopics",
        reader: Ok((
            "controllers/topic_discovery.rs",
            "load.policy().discovery.hard_max_topics",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "the_installation_policys_ceilings_reach_discovery_admission_and_the_plan",
        )),
    },
    Followed {
        value: "checks.discovery.visibilityAttestations",
        field: "discovery.visibilityAttestations",
        reader: Ok((
            "controllers/topic_discovery.rs",
            "load.policy().discovery.visibility_attestations",
        )),
        proof: Some((
            "topic_discovery_controller.rs",
            "an_administrator_attestation_is_the_only_route_to_attested_complete",
        )),
    },
    Followed {
        value: "checks.preflight.retentionSeconds",
        field: "preflight.retentionSeconds",
        reader: Ok((
            "controllers/preflight.rs",
            "policy.policy().preflight.retention_seconds",
        )),
        proof: Some((
            "configured_values.rs",
            "the_installation_policys_preflight_window_reaches_the_preflight_collector",
        )),
    },
    Followed {
        value: "runs.maxManualBackupsActivePerNamespace",
        field: "runs.maxManualBackupsActivePerNamespace",
        reader: Ok((
            "run_pool.rs",
            "PoolKind::Backup => policy.runs.max_manual_backups_active_per_namespace",
        )),
        proof: Some((
            "configured_values.rs",
            "the_installation_policys_manual_backup_ceiling_admits_the_sixth_run",
        )),
    },
    Followed {
        value: "runs.maxManualRestoresActivePerNamespace",
        field: "runs.maxManualRestoresActivePerNamespace",
        reader: Ok((
            "run_pool.rs",
            "PoolKind::Restore => policy.runs.max_manual_restores_active_per_namespace",
        )),
        proof: Some((
            "configured_values.rs",
            "the_installation_policys_manual_restore_ceiling_is_the_one_the_pool_applies",
        )),
    },
    Followed {
        value: "engine.allowUnverifiedCustomCa",
        field: "engine.allowUnverifiedCustomCa",
        reader: Ok((
            "controllers/backup.rs",
            "policy.engine.allow_unverified_custom_ca",
        )),
        proof: Some((
            "configured_values.rs",
            "the_installation_policys_custom_ca_switch_reaches_backup_admission",
        )),
    },
    Followed {
        value: "evidence.controllerIdentityLocations",
        field: "evidence.controllerIdentityLocations",
        reader: Ok(("destination.rs", ".controller_identity_locations")),
        proof: Some((
            "preflight_controller.rs",
            "an_evidence_read_grant_no_pod_holds_is_named_and_projects_nothing",
        )),
    },
    Followed {
        value: "archive.s3.endpoint",
        field: "legacyArchiveAddressing.endpoint",
        reader: Ok((
            "controllers/preflight.rs",
            "or_installation(&addressing.endpoint)",
        )),
        proof: Some((
            "preflight_controller.rs",
            "a_legacy_plan_without_an_endpoint_takes_the_installations_legacy_addressing",
        )),
    },
    Followed {
        value: "archive.s3.region",
        field: "legacyArchiveAddressing.region",
        reader: Ok((
            "controllers/preflight.rs",
            "or_installation(&addressing.region)",
        )),
        proof: Some((
            "preflight_controller.rs",
            "a_legacy_plan_without_an_endpoint_takes_the_installations_legacy_addressing",
        )),
    },
    // PUBLISHED, READ BY NOTHING YET — listed, not hidden. Both values DO reach
    // a reader through the Deployment's env (`AWS_ALLOW_HTTP`,
    // `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` → `retention::storage_url_for`,
    // `configured_values.rs`); it is their copy in this document that nothing
    // reads.
    Followed {
        value: "archive.s3.allowHttp",
        field: "legacyArchiveAddressing.allowHttp",
        reader: Err(
            "D2 §3.12 (b): `POST …/destinations:from-legacy` is to read it, and refuses with \
             branch (c) today; the restore readiness check takes transport from the PLAN \
             (D-SEAMS S5). FX-10 report, Class sweep owed.",
        ),
        proof: None,
    },
    Followed {
        value: "archive.s3.virtualHostedStyle",
        field: "legacyArchiveAddressing.virtualHostedStyle",
        reader: Err(
            "D2 §3.12 (b), as allowHttp; the readiness check derives addressing from the \
             plan and the endpoint (engine G4). FX-10 report, Class sweep owed.",
        ),
        proof: None,
    },
];

/// **FX-10's guard: every installation-policy value reaches its own parsed
/// field at a NON-default value, and a controller site reads that field —
/// with the row that proves the reader acts on it named, and present.**
///
/// The defect this row exists for: `checks.discovery.defaultMaxTopics` and
/// `checks.preflight.defaultTimeoutSeconds` were documented, typed, rendered,
/// parsed and range-checked, and READ BY NOTHING — and every gate passed,
/// because every gate rendered every value at its default. So:
///
/// 1. the table is complete both ways: its `value`s are exactly the
///    `values.yaml` leaves the policy document is rendered from (`checks`,
///    `runs`, `engine`, `evidence`, and the four `archive.s3` values behind
///    `legacyArchiveAddressing`), and its `field`s plus the two withdrawn ones
///    and `version` are exactly the parser's fields — a new knob, or a new
///    parsed field, fails here until it is followed;
/// 2. `examples/tuned.values.yaml` sets every one OFF its default;
/// 3. its committed render, through the REAL `check::policy::parse`, carries
///    each at that value in its own field (a swap of two values with equal
///    defaults — `maxActivePerNamespace` and
///    `maxEvidenceFetchActivePerNamespace` are both 4 — fails here, where the
///    default renders could not tell);
/// 4. the named reader site exists, and so does the named proof row, which
///    drives that reader at a non-default value.
///
/// A field with no reader is listed with its reason (`reader: Err`), never
/// dropped: that is how `legacyArchiveAddressing.allowHttp` stays visible.
#[test]
fn every_installation_policy_value_reaches_its_field_and_a_reader_at_a_non_default_value() {
    let values = values();
    let tuned = example("tuned");

    // 1a. The values side is complete.
    let mut leaves = Vec::new();
    for block in ["checks", "runs", "engine", "evidence"] {
        yaml_leaves(&values[block], block, &mut leaves);
    }
    for key in ["endpoint", "region", "allowHttp", "virtualHostedStyle"] {
        leaves.push(format!("archive.s3.{key}"));
    }
    let leaves: std::collections::BTreeSet<String> = leaves.into_iter().collect();
    let followed: std::collections::BTreeSet<String> =
        FOLLOWED.iter().map(|f| f.value.to_string()).collect();
    assert_eq!(
        followed, leaves,
        "every values.yaml leaf the policy document is rendered from must be followed in \
         FOLLOWED, and nothing else"
    );

    // 1b. The parser side is complete.
    let mut fields = Vec::new();
    json_leaves(
        &serde_json::to_value(Policy::defaults()).expect("the policy serialises"),
        "",
        &mut fields,
    );
    let fields: std::collections::BTreeSet<String> = fields.into_iter().collect();
    let mut accounted: std::collections::BTreeSet<String> =
        FOLLOWED.iter().map(|f| f.field.to_string()).collect();
    for extra in [
        "version",
        "discovery.defaultMaxTopics",
        "preflight.defaultTimeoutSeconds",
    ] {
        accounted.insert(extra.to_string());
    }
    assert_eq!(
        accounted, fields,
        "every field check::policy::Policy parses must be followed in FOLLOWED (or be one of \
         the two withdrawn ones, which the row above holds to no reader)"
    );

    // 2–3. Off its default in the example, and in its own field after the parse.
    let raw = rendered_policies("tuned")
        .remove("weirkeeper-policy")
        .expect("the tuned render carries weirkeeper-policy");
    let parsed = serde_json::to_value(
        policy::parse(raw.as_bytes())
            .unwrap_or_else(|e| panic!("the tuned document is refused: {e}")),
    )
    .expect("serialises");
    for f in FOLLOWED {
        let default = yaml_at(&values, f.value);
        let set = yaml_at(&tuned, f.value);
        assert!(
            !set.is_null(),
            "examples/tuned.values.yaml does not set `{}`",
            f.value
        );
        assert_ne!(
            set, default,
            "examples/tuned.values.yaml sets `{}` to its default, which proves nothing",
            f.value
        );
        let want: serde_json::Value = serde_json::to_value(set).expect("YAML to JSON");
        assert_eq!(
            json_at(&parsed, f.field),
            &want,
            "`{}` set to {want} did not land in the parsed `{}`",
            f.value,
            f.field
        );
    }

    // 4. The reader site and the proof row exist.
    for f in FOLLOWED {
        match f.reader {
            Ok((file, needle)) => {
                let path = repo().join("crates/weirkeeper/src").join(file);
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                assert!(
                    text.contains(needle),
                    "`{}` names reader `{needle}` in {file}, and it is not there",
                    f.field
                );
                let (test_file, test_fn) = f
                    .proof
                    .unwrap_or_else(|| panic!("`{}` has a reader and no proof row", f.field));
                let path = repo().join("crates/weirkeeper/tests").join(test_file);
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                assert!(
                    text.contains(&format!("fn {test_fn}(")),
                    "`{}`'s proof row `{test_fn}` is not in {test_file}",
                    f.field
                );
            }
            Err(reason) => assert!(
                f.proof.is_none() && reason.contains("FX-10 report"),
                "`{}` is listed as unread; its reason must point at the report",
                f.field
            ),
        }
    }
}
