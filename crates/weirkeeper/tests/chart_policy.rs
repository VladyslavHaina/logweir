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
