//! The checked-in OpenAPI document: drift, coverage and the two contract
//! properties a schema can carry — strict requests, and no credential or
//! document bytes in a response.

mod support;

use std::collections::BTreeSet;

use logweir_api::openapi::openapi_document;
use logweir_api::problem::ProblemCode;
use serde_json::Value;
use support::TestApp;

const CHECKED_IN: &str = "../../schemas/logweir-api-v1.openapi.json";

fn document() -> Value {
    serde_json::from_str(&openapi_document()).expect("the document is JSON")
}

/// The gate: the checked-in file is exactly what the types generate.
#[test]
fn the_checked_in_openapi_document_is_what_the_types_generate() {
    let generated = openapi_document();
    let checked_in = include_str!("../../../schemas/logweir-api-v1.openapi.json");
    assert_eq!(
        generated.trim_end(),
        checked_in.trim_end(),
        "{CHECKED_IN} is stale. Run `just schema` and review the diff: a field added is a \
         MINOR change, a field removed or retyped is a MAJOR one."
    );
}

/// The route table, pinned. A route added to `crate::app::router` without a
/// path here fails this test, and a documented path with no route fails the
/// probe below.
#[test]
fn the_document_names_every_route_and_every_route_answers() {
    let document = document();
    let paths: Vec<&str> = document["paths"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        paths,
        vec![
            "/healthz",
            "/readyz",
            "/api/v1/session",
            "/api/v1/namespaces",
            "/api/v1/namespaces/{ns}/connections",
            "/api/v1/namespaces/{ns}/connections/{name}",
            "/api/v1/namespaces/{ns}/schedules",
            "/api/v1/namespaces/{ns}/schedules/{name}",
            "/api/v1/namespaces/{ns}/schedules/{name}:set-suspension",
            "/api/v1/namespaces/{ns}/backups",
            "/api/v1/namespaces/{ns}/backups/{name}",
            "/api/v1/namespaces/{ns}/restores",
            "/api/v1/namespaces/{ns}/restores/{name}",
            "/api/v1/namespaces/{ns}/approvals",
            "/api/v1/namespaces/{ns}/approvals/{name}",
            "/api/v1/namespaces/{ns}/approvals/{name}/packet",
            "/api/v1/namespaces/{ns}/operations/{kind}/{name}",
        ]
    );
    let mut operation_ids = BTreeSet::new();
    for (_, item) in document["paths"].as_object().unwrap() {
        for (_, operation) in item.as_object().unwrap() {
            assert!(operation_ids.insert(operation["operationId"].as_str().unwrap().to_string()));
            assert!(operation["summary"].is_string());
            assert!(
                operation["responses"]["default"]["content"]["application/problem+json"]
                    .is_object()
            );
        }
    }
    assert_eq!(operation_ids.len(), 20);
}

#[tokio::test]
async fn every_documented_operation_is_routed() {
    let app = TestApp::new();
    for (path, item) in document()["paths"].as_object().unwrap() {
        let concrete = path
            .replace("{ns}", support::NS_A)
            .replace("{kind}", "backup")
            .replace("{name}", "absent-object");
        for (method, _) in item.as_object().unwrap() {
            let response = match method.as_str() {
                "get" => app.get(&concrete).await,
                "post" => app.post(&concrete, Some("documented-route-01"), "{}").await,
                other => panic!("undocumented method {other}"),
            };
            assert_ne!(
                response.status.as_u16(),
                405,
                "{method} {concrete} is documented and not routed"
            );
            assert_ne!(response.status.as_u16(), 500, "{method} {concrete}");
        }
    }
}

fn walk<'a>(
    schema: &'a Value,
    schemas: &'a serde_json::Map<String, Value>,
    seen: &mut BTreeSet<String>,
    out: &mut Vec<(String, Value)>,
) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap().to_string();
        if seen.insert(name.clone()) {
            if let Some(target) = schemas.get(&name) {
                out.push((name, target.clone()));
                walk(target, schemas, seen, out);
            }
        }
        return;
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (_, property) in properties {
            walk(property, schemas, seen, out);
        }
    }
    if let Some(items) = schema.get("items") {
        walk(items, schemas, seen, out);
    }
}

/// Every request DTO, at every level, refuses unknown fields.
#[test]
fn request_schemas_are_strict_all_the_way_down() {
    let document = document();
    let schemas = document["components"]["schemas"]
        .as_object()
        .unwrap()
        .clone();
    let mut checked = 0;
    for request in [
        "CreateConnectionRequest",
        "CreateScheduleRequest",
        "CreateRestoreRequest",
        "SetSuspensionRequest",
    ] {
        let mut seen = BTreeSet::from([request.to_string()]);
        let mut objects = vec![(request.to_string(), schemas[request].clone())];
        walk(&schemas[request], &schemas, &mut seen, &mut objects);
        for (name, schema) in objects {
            if schema.get("properties").is_some() {
                assert_eq!(
                    schema["additionalProperties"],
                    Value::Bool(false),
                    "{name} (reached from {request}) accepts unknown fields"
                );
                checked += 1;
            }
        }
    }
    assert!(checked >= 8, "only {checked} request objects were checked");
}

/// No response schema names a credential, a token or the approval documents —
/// except `ApprovalPacket`, which is the one explicit route for them.
#[test]
fn response_schemas_carry_no_credential_or_document_bytes() {
    let document = document();
    let schemas = document["components"]["schemas"].as_object().unwrap();
    let forbidden = [
        "password",
        "token",
        "secret",
        "credential",
        "kubeconfig",
        "bearer",
        "privateKey",
        "spkiPem",
        "data",
    ];
    for (name, schema) in schemas {
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            continue;
        };
        for (property, property_schema) in properties {
            let lowered = property.to_ascii_lowercase();
            // A boolean capability flag names a domain, not a value; the one
            // reference field names a Secret and carries no data.
            let flag = property_schema.get("type") == Some(&Value::String("boolean".into()));
            for word in forbidden {
                // TWO NAMED EXCEPTIONS. `credentialRef` is a Secret NAME and
                // carries no data; `csrfToken` is the synchronizer token the
                // browser is meant to receive (null until PLAT-17.2 issues
                // sessions). Everything else that reads like a credential is
                // a defect.
                let allowed = property == "credentialRef" || property == "csrfToken" || flag;
                assert!(
                    !lowered.contains(word) || allowed,
                    "{name}.{property} looks like credential material"
                );
            }
            if property == "approvalBytes" || property == "sidecarBytes" {
                assert_eq!(
                    name, "ApprovalPacket",
                    "{name}.{property} exposes approval documents"
                );
            }
        }
    }
    // The packet is reachable only from its own response envelope.
    let referrers: Vec<&String> = schemas
        .iter()
        .filter(|(_, s)| {
            s.to_string()
                .contains("#/components/schemas/ApprovalPacket\"")
        })
        .map(|(n, _)| n)
        .collect();
    assert_eq!(referrers, vec!["ApprovalPacketResponse"]);
}

/// Every problem code is documented on a route or explicitly reserved.
#[test]
fn every_problem_code_is_documented() {
    let document = document();
    let text = document["paths"].to_string();
    let reserved: BTreeSet<&str> = document["x-logweir-reserved-problem-codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for code in ProblemCode::ALL {
        let quoted = format!("\"{}\"", code.as_str());
        assert!(
            text.contains(&quoted) || reserved.contains(code.as_str()),
            "{} appears on no route and is not reserved",
            code.as_str()
        );
    }
    // The enum itself is a component, so a client can generate the closed set.
    // `schemars` renders a documented unit-variant enum as `oneOf` branches,
    // each a one-value `enum`.
    let enum_values: BTreeSet<&str> = document["components"]["schemas"]["ProblemCode"]["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .map(|branch| branch["enum"][0].as_str().unwrap())
        .collect();
    assert_eq!(enum_values.len(), ProblemCode::ALL.len());
    for code in ProblemCode::ALL {
        assert!(enum_values.contains(code.as_str()));
    }
}
