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
    let preflight = &schema["properties"]["checks"]["properties"]["preflight"]["properties"];
    assert_eq!(
        preflight["defaultTimeoutSeconds"]["minimum"].as_u64(),
        Some(1)
    );
    assert_eq!(
        preflight["defaultTimeoutSeconds"]["maximum"].as_u64(),
        Some(600),
        "`Policy::validate` enforces the contract's 1..=600"
    );
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
    for field in ["keepPerConnection", "defaultMaxTopics", "hardMaxTopics"] {
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
                "defaultMaxTopics": n(1, ceiling),
                "hardMaxTopics": n(1, ceiling),
                "visibilityAttestations": []
            },
            "preflight": {
                "defaultTimeoutSeconds": n(1, 600),
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

/// **The two cross-field rules `Policy::validate` enforces are real**, so the
/// `fail`s `charts/logweir/templates/policy.yaml` carries for them are not
/// belt-and-braces over a rule that does not exist.
///
/// JSON Schema draft-07 cannot compare two sibling values, which is why they
/// are refused at render time instead. This is the half that proves the rules
/// they mirror.
#[test]
fn the_two_rules_the_schema_cannot_express_are_rules_the_parser_enforces() {
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

    let mut inverted_topics = base;
    inverted_topics["discovery"]["defaultMaxTopics"] = serde_json::json!(50_000);
    inverted_topics["discovery"]["hardMaxTopics"] = serde_json::json!(100);
    assert!(
        policy::parse(inverted_topics.to_string().as_bytes()).is_err(),
        "`defaultMaxTopics` above `hardMaxTopics` must be refused; \
         templates/policy.yaml fails the render for it"
    );
}

/// **The chart's own template refuses both cross-field pairs**, named, at
/// render time — asserted over the template text because `chart_lint` is the
/// crate that runs `helm` and this one does not.
#[test]
fn the_policy_template_names_both_cross_field_rules_in_its_refusals() {
    let template = std::fs::read_to_string(repo().join("charts/logweir/templates/policy.yaml"))
        .expect("the policy template is readable");
    let code: String = template
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code.matches("{{- fail (printf").count(),
        2,
        "two cross-field rules, two named refusals"
    );
    for needle in [
        "checks.maxActiveTotal (%d) must be at least checks.maxActivePerNamespace",
        "checks.discovery.defaultMaxTopics (%d) must be at most checks.discovery.hardMaxTopics",
    ] {
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
    for condition in [
        "{{- if lt $maxTotal $maxNs -}}",
        "{{- if gt $defaultMax $hardMax -}}",
    ] {
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
