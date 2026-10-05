//! FX-2 — `runnerResources` is applied or refused, never dropped and never
//! clamped.
//!
//! PURE ROWS ONLY. The reconciler rows — the `Restore` Job's container and
//! the `RehearsalSchedule`'s child — are in `restore_controller.rs` and
//! `rehearsal_controller.rs`, over their route tables.
//!
//! READ [`a_valid_block_reaches_the_container_verbatim_and_in_the_right_places`]
//! FIRST: it is the defect's own row, one layer down. Before FX-2 there was no
//! function to call — `job::build` rendered a container with no `resources`
//! whatever the object said.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use serde_json::{json, Value};
use weirkeeper::crds::rehearsal_schedule::{RunnerResources, QUANTITY_PATTERN};
use weirkeeper::job::{self, ContainerResources};
use weirkeeper::runner_resources::{
    parse, validate, Refused, CPU_CEILING, MEMORY_CEILING, MEMORY_LIMIT_FLOOR, REHEARSAL_PATH,
    RESTORE_PATH,
};

/// A `runnerResources` block, from the JSON an object would carry.
fn block(value: Value) -> RunnerResources {
    serde_json::from_value(value).expect("the fixture is a runnerResources block")
}

/// `validate` over a JSON block at the `Restore` path.
fn check(value: Value) -> Result<Option<ContainerResources>, Refused> {
    validate(Some(&block(value)), RESTORE_PATH)
}

/// The refusal text for a block that must be refused.
fn refused(value: Value) -> String {
    match check(value.clone()) {
        Err(refused) => refused.to_string(),
        Ok(rendered) => panic!("{value} must be refused, and rendered {rendered:?}"),
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// ===========================================================================
// What reaches the container
// ===========================================================================

/// **THE DEFECT'S OWN ROW, ONE LAYER DOWN.** A valid block comes back
/// verbatim, requests as requests and limits as limits, and `job::build` puts
/// exactly that on the one container.
///
/// Every request differs from its limit, so a swap anywhere between the object
/// and the container is visible.
///
/// MUTANTS: `resources: None` in `job::build`; `requests` and `limits`
/// exchanged in `ContainerResources::requirements` or in `validate`.
#[test]
fn a_valid_block_reaches_the_container_verbatim_and_in_the_right_places() {
    let rendered = check(json!({
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"cpu": "1", "memory": "2Gi"}
    }))
    .expect("a valid block is accepted")
    .expect("and renders resources");
    assert_eq!(
        rendered,
        ContainerResources {
            requests: map(&[("cpu", "250m"), ("memory", "512Mi")]),
            limits: map(&[("cpu", "1"), ("memory", "2Gi")]),
        },
        "the quantities are carried in the spelling the object used, per block"
    );

    let requirements = rendered.requirements();
    let q = |s: &str| Quantity(s.to_string());
    assert_eq!(
        requirements.requests,
        Some(BTreeMap::from([
            ("cpu".to_string(), q("250m")),
            ("memory".to_string(), q("512Mi"))
        ]))
    );
    assert_eq!(
        requirements.limits,
        Some(BTreeMap::from([
            ("cpu".to_string(), q("1")),
            ("memory".to_string(), q("2Gi"))
        ]))
    );
    assert!(requirements.claims.is_none());

    // …and through the ONE builder every runner Job goes through.
    let built = job::build(&job::RunnerJobSpec {
        name: "r".to_string(),
        namespace: "ns".to_string(),
        owner: job::RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "Restore".to_string(),
            name: "r".to_string(),
            uid: "u".to_string(),
        },
        args: Vec::new(),
        deadline_seconds: 60,
        service_account_name: "logweir-runner".to_string(),
        secret_mounts: Vec::new(),
        config_map_mounts: Vec::new(),
        env_from_secret: Vec::new(),
        env_literal: Vec::new(),
        plan_config_map: None,
        image: None,
        image_pull_policy: None,
        resources: Some(rendered),
    });
    let value = serde_json::to_value(&built).expect("the Job serialises");
    assert_eq!(
        value["spec"]["template"]["spec"]["containers"][0]["resources"],
        json!({
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"cpu": "1", "memory": "2Gi"}
        }),
        "the container carries exactly what the object asked for"
    );
}

/// Absent, empty, or a block with no quantity in it: NO `resources` key at all
/// — the Job shape every runner had before FX-2, so the namespace's
/// `LimitRange` defaults apply exactly as they did.
#[test]
fn an_absent_or_empty_block_renders_no_resources_at_all() {
    assert_eq!(validate(None, RESTORE_PATH), Ok(None));
    for empty in [
        json!({}),
        json!({"requests": {}}),
        json!({"limits": {}}),
        json!({"requests": {}, "limits": {}}),
    ] {
        assert_eq!(check(empty.clone()), Ok(None), "{empty}");
    }
    // One side only renders that side only — never an empty map for the other.
    let only_limits = check(json!({"limits": {"memory": "2Gi"}}))
        .expect("valid")
        .expect("renders");
    let requirements = only_limits.requirements();
    assert!(
        requirements.requests.is_none(),
        "an empty requests map is an ABSENT key, never `requests: {{}}`"
    );
    assert_eq!(
        requirements.limits,
        Some(BTreeMap::from([(
            "memory".to_string(),
            Quantity("2Gi".to_string())
        )]))
    );
}

// ===========================================================================
// The rules
// ===========================================================================

/// **Rule 5.** A request above its limit is refused, per resource, naming the
/// request's field and both values. Equal is fine.
///
/// MUTANT: dropping the request-versus-limit comparison.
#[test]
fn a_request_above_its_limit_is_refused_per_resource() {
    let cpu = refused(json!({
        "requests": {"cpu": "1500m"},
        "limits": {"cpu": "1"}
    }));
    assert!(
        cpu.contains("spec.runnerResources.requests.cpu")
            && cpu.contains("\"1500m\"")
            && cpu.contains("\"1\""),
        "{cpu}"
    );
    let memory = refused(json!({
        "requests": {"memory": "3Gi"},
        "limits": {"memory": "2Gi"}
    }));
    assert!(
        memory.contains("spec.runnerResources.requests.memory") && memory.contains("\"3Gi\""),
        "{memory}"
    );
    // Different spellings of ONE amount compare as one amount: equal is "at
    // most".
    for (request, limit) in [("1", "1000m"), ("0.5", "500m"), ("2", "2")] {
        assert!(
            check(json!({"requests": {"cpu": request}, "limits": {"cpu": limit}})).is_ok(),
            "cpu {request} <= {limit}"
        );
    }
    for (request, limit) in [("1Gi", "1073741824"), ("64Mi", "67108864"), ("1e9", "1G")] {
        assert!(
            check(json!({"requests": {"memory": request}, "limits": {"memory": limit}})).is_ok(),
            "memory {request} <= {limit}"
        );
    }
    // A request with no limit beside it is not compared with anything.
    assert!(check(json!({"requests": {"cpu": "2"}, "limits": {"memory": "1Gi"}})).is_ok());
}

/// **Rule 3.** Nothing above the ceiling is accepted — requests and limits
/// alike — and nothing is clamped: the refusal names the value and the
/// ceiling, and no rendering comes back at all.
///
/// MUTANT: removing the ceiling comparison; replacing the refusal with a
/// clamp (the rendering would then come back `Ok`).
#[test]
fn nothing_above_the_ceiling_is_accepted_and_nothing_is_clamped() {
    // At the ceiling: accepted.
    assert!(check(json!({"limits": {"cpu": "4", "memory": "8Gi"}})).is_ok());
    assert!(check(json!({"limits": {"cpu": "4000m", "memory": "8589934592"}})).is_ok());
    // …in every binary spelling below `Gi` too (review L8: with `Ki` read as
    // 1000, `8388609Ki` — one Ki over — was ACCEPTED and reached the Job).
    assert!(check(json!({"limits": {"memory": "8388608Ki"}})).is_ok());
    assert!(check(json!({"limits": {"memory": "8192Mi"}})).is_ok());
    // One unit above it: refused.
    for (label, value, field) in [
        (
            "cpu limit",
            json!({"limits": {"cpu": "4001m"}}),
            "limits.cpu",
        ),
        (
            "memory limit",
            json!({"limits": {"memory": "8589934593"}}),
            "limits.memory",
        ),
        (
            "memory limit one Ki above",
            json!({"limits": {"memory": "8388609Ki"}}),
            "limits.memory",
        ),
        (
            "memory limit one Mi above",
            json!({"limits": {"memory": "8193Mi"}}),
            "limits.memory",
        ),
        (
            "cpu request with no limit",
            json!({"requests": {"cpu": "5"}}),
            "requests.cpu",
        ),
        (
            "memory request with no limit",
            json!({"requests": {"memory": "9Gi"}}),
            "requests.memory",
        ),
        // `cpu: 5Gi` is well-formed and five billion cores: the pattern
        // cannot tell a cpu quantity from a memory one, the ceiling can.
        (
            "a memory unit on cpu",
            json!({"limits": {"cpu": "5Gi"}}),
            "limits.cpu",
        ),
        // Far beyond what a `u128` of nano-units holds.
        (
            "an exponent",
            json!({"limits": {"memory": "1e30"}}),
            "limits.memory",
        ),
        ("exa", json!({"limits": {"cpu": "9E"}}), "limits.cpu"),
    ] {
        let text = refused(value);
        assert!(
            text.contains(field) && text.contains("ceiling"),
            "{label}: {text}"
        );
    }
    let text = refused(json!({"limits": {"memory": "16Gi"}}));
    assert!(
        text.contains(MEMORY_CEILING) && text.contains("refuses rather than clamps"),
        "{text}"
    );
    let text = refused(json!({"limits": {"cpu": "8"}}));
    assert!(
        text.contains(&format!("ceiling of {CPU_CEILING}")),
        "{text}"
    );
}

/// **Rule 2.** The unit fits the resource: memory is whole bytes, CPU whole
/// millicores. `memory: 100m` — a tenth of a byte, the classic slip for
/// `100Mi` — is refused with a hint.
///
/// MUTANT: dropping the unit check (every row below would then fail on the
/// floor or pass, never on the unit).
#[test]
fn the_unit_must_fit_the_resource() {
    let text = refused(json!({"requests": {"memory": "100m"}}));
    assert!(
        text.contains("requests.memory")
            && text.contains("not a whole number of bytes")
            && text.contains("`Mi`"),
        "{text}"
    );
    for fractional in ["0.5", "1.0001k", "1e-3", "3n"] {
        let text = refused(json!({"requests": {"memory": fractional}}));
        assert!(
            text.contains("not a whole number of bytes"),
            "{fractional}: {text}"
        );
    }
    for fine in ["0.0001", "100u", "1500n", "1e-4"] {
        let text = refused(json!({"requests": {"cpu": fine}}));
        assert!(text.contains("finer than 1m"), "{fine}: {text}");
    }
    // …and the ordinary spellings of both are accepted.
    for cpu in ["1", "0.5", "250m", "1500m", "1.5", "2", "1e0", "0.001"] {
        assert!(
            check(json!({"requests": {"cpu": cpu}})).is_ok(),
            "cpu {cpu}"
        );
    }
    for memory in ["512Mi", "1.5Gi", "1e9", "2G", "1024Ki", "33554432", "1.5k"] {
        assert!(
            check(json!({"requests": {"memory": memory}})).is_ok(),
            "memory {memory}"
        );
    }
}

/// **Rule 4.** A limit is a cap: never zero (to a runtime zero is "no limit"),
/// and a memory limit is at least the floor. A REQUEST of zero, or a small
/// memory request, is not a cap and is accepted.
///
/// MUTANT: dropping either half of the rule.
#[test]
fn a_limit_is_a_cap() {
    for (resource, zero) in [
        ("cpu", "0"),
        ("memory", "0"),
        ("cpu", "0m"),
        ("memory", "0Mi"),
    ] {
        let text = refused(json!({"limits": {resource: zero}}));
        assert!(
            text.contains(&format!("limits.{resource}")) && text.contains("no limit"),
            "{resource} {zero}: {text}"
        );
    }
    for small in ["512", "1Mi", "33554431"] {
        let text = refused(json!({"limits": {"memory": small}}));
        assert!(
            text.contains(MEMORY_LIMIT_FLOOR) && text.contains("limits.memory"),
            "{small}: {text}"
        );
    }
    assert!(check(json!({"limits": {"memory": MEMORY_LIMIT_FLOOR}})).is_ok());
    assert!(check(json!({"limits": {"memory": "33554432"}})).is_ok());
    // The floor in `Ki` (review L8): exactly 32Mi is a cap, one Ki less is not.
    assert!(check(json!({"limits": {"memory": "32768Ki"}})).is_ok());
    let text = refused(json!({"limits": {"memory": "32767Ki"}}));
    assert!(text.contains(MEMORY_LIMIT_FLOOR), "32767Ki: {text}");
    assert!(check(json!({"requests": {"cpu": "0", "memory": "0"}})).is_ok());
    assert!(check(json!({"requests": {"memory": "512"}})).is_ok());
    // The smallest cpu limit is one millicore.
    assert!(check(json!({"limits": {"cpu": "1m"}})).is_ok());
}

/// **Rule 1.** The controller re-checks the grammar the schema enforces: it
/// never projects into a pod a field it has not read itself.
#[test]
fn the_grammar_is_checked_again_by_the_controller() {
    for bad in [
        "", "abc", "1K", "1ki", "1.", ".5", "-1", "+1", "1 Gi", "1Gi ", "1e", "1e+", "0x10",
        "1..5", "1Gii", "1mi", "1.5.5",
    ] {
        let text = refused(json!({"requests": {"cpu": bad}}));
        assert!(
            text.contains("not a Kubernetes quantity"),
            "`{bad}`: {text}"
        );
        assert!(parse(bad).is_none(), "`{bad}` parses");
    }
    // The schema's own pattern names the three suffix forms this accepts.
    assert_eq!(
        QUANTITY_PATTERN,
        r"^[0-9]+(\.[0-9]+)?(([KMGTPE]i)|[numkMGTPE]|([eE][-+]?[0-9]+))?$"
    );
    for good in [
        "0", "7", "0.5", "10.25", "1n", "1u", "1m", "1k", "1M", "1G", "1T", "1P", "1E", "1Ki",
        "1Mi", "1Gi", "1Ti", "1Pi", "1Ei", "1e3", "1E3", "1e+3", "1e-3", "1E-3", "000012",
    ] {
        assert!(parse(good).is_some(), "`{good}` is in the grammar");
    }
}

/// The value is exact — no floating point anywhere — so `1Gi` and
/// `1073741824` are ONE amount and `1000m` and `1` are one amount.
#[test]
fn quantities_compare_exactly_across_spellings() {
    let same = |a: &str, b: &str| {
        let (a_amount, b_amount) = (parse(a).expect("a quantity"), parse(b).expect("a quantity"));
        assert_eq!(
            a_amount.compare(&b_amount),
            std::cmp::Ordering::Equal,
            "{a} == {b}"
        );
    };
    same("1Gi", "1073741824");
    same("1000m", "1");
    same("0.5", "500m");
    same("1e3", "1k");
    same("1E3", "1000");
    same("1.5Gi", "1536Mi");
    same("8Gi", "8589934592");
    same("1e-3", "1m");
    same("2.5e2", "250");
    same("0", "0Ei");
    // EVERY SUFFIX, PINNED TO ITS EXACT VALUE IN PLAIN DIGITS (review L8). A
    // pair of two suffixed spellings cannot catch a table entry that is wrong
    // on both sides; digits can. `Ki` read as 1000 survived the whole suite.
    for (suffixed, digits) in [
        ("1Ki", "1024"),
        ("1Mi", "1048576"),
        ("1Gi", "1073741824"),
        ("1Ti", "1099511627776"),
        ("1Pi", "1125899906842624"),
        ("1Ei", "1152921504606846976"),
        ("1k", "1000"),
        ("1M", "1000000"),
        ("1G", "1000000000"),
        ("1T", "1000000000000"),
        ("1P", "1000000000000000"),
        ("1E", "1000000000000000000"),
        ("1000000000n", "1"),
        ("1000000u", "1"),
        ("1000m", "1"),
    ] {
        same(suffixed, digits);
    }
    // The two spellings review L8 named: the floor and the ceiling, in `Ki`.
    same("32768Ki", "32Mi");
    same("8388608Ki", "8Gi");
    let less = |a: &str, b: &str| {
        assert_eq!(
            parse(a)
                .expect("a quantity")
                .compare(&parse(b).expect("a quantity")),
            std::cmp::Ordering::Less,
            "{a} < {b}"
        );
    };
    less("999m", "1");
    less("8589934591", "8Gi");
    // Past what a `u128` of nano-units holds, a value is above everything
    // that fits — which is all an ordering against a ceiling needs.
    less("1Ei", "1e30");
    assert!(parse("0").expect("zero").is_zero());
    assert!(parse("0.000").expect("zero").is_zero());
    assert!(!parse("1n").expect("one nano").is_zero());
}

/// Every broken rule in a block is reported, each under its own full path:
/// the objects are immutable, and fixing one field per re-created object is
/// as many round trips as there are mistakes.
#[test]
fn every_refusal_is_reported_under_its_own_field() {
    let Err(refused) = check(json!({
        "requests": {"cpu": "100u", "memory": "100m"},
        "limits": {"cpu": "0", "memory": "16Gi"}
    })) else {
        panic!("four broken quantities are refused")
    };
    let fields: Vec<&str> = refused.0.iter().map(|r| r.field.as_str()).collect();
    assert_eq!(
        fields,
        vec![
            "spec.runnerResources.requests.cpu",
            "spec.runnerResources.requests.memory",
            "spec.runnerResources.limits.cpu",
            "spec.runnerResources.limits.memory",
        ]
    );
    // The schedule's block names the schedule's path.
    let Err(refused) = validate(
        Some(&block(json!({"limits": {"memory": "16Gi"}}))),
        REHEARSAL_PATH,
    ) else {
        panic!("refused")
    };
    assert_eq!(
        refused.0[0].field,
        "spec.bounds.runnerResources.limits.memory"
    );
}

/// **THE NUMBERS THE DOCUMENTATION STATES ARE THE NUMBERS THE CODE USES.**
/// D3 §4.1's ceilings, the floor, and the two CRDs' descriptions that
/// `kubectl explain` shows an operator — read from the generated files, so a
/// ceiling moved in one place and not the other fails here.
#[test]
fn the_bounds_the_crds_describe_are_the_bounds_the_controller_enforces() {
    assert_eq!(CPU_CEILING, "4", "D3 §4.1: cpu limit <= 4");
    assert_eq!(MEMORY_CEILING, "8Gi", "D3 §4.1: memory limit <= 8Gi");
    assert_eq!(MEMORY_LIMIT_FLOOR, "32Mi");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for crd in [
        "config/crd/restores.yaml",
        "config/crd/rehearsalschedules.yaml",
    ] {
        let text = std::fs::read_to_string(root.join(crd)).expect("the generated CRD");
        for needle in [
            format!("at most {CPU_CEILING} CPUs and {MEMORY_CEILING}"),
            format!("at least {MEMORY_LIMIT_FLOOR}"),
            "whole millicores and bytes".to_string(),
            "never clamped".to_string(),
        ] {
            assert!(
                text.contains(&needle),
                "{crd} must describe the bound the controller enforces: `{needle}`"
            );
        }
        // THE PER-QUANTITY DESCRIPTIONS TOO (review nit): `kubectl explain
        // …runnerResources.limits.memory` shows these, under `requests` and
        // under `limits`, so each sentence appears exactly twice per CRD.
        for needle in [
            format!("A whole number of millicores, at most {CPU_CEILING}:"),
            format!("at most {MEMORY_CEILING}, and as a limit at least {MEMORY_LIMIT_FLOOR}:"),
        ] {
            assert_eq!(
                text.matches(&needle).count(),
                2,
                "{crd} must state `{needle}` under requests and under limits"
            );
        }
    }
    let restores = std::fs::read_to_string(root.join("config/crd/restores.yaml")).expect("the CRD");
    assert!(restores.contains("ExecutionSpecInvalid"));
    let schedules =
        std::fs::read_to_string(root.join("config/crd/rehearsalschedules.yaml")).expect("the CRD");
    assert!(schedules.contains("skips the slot as `AuthorizationInvalid`"));
}
