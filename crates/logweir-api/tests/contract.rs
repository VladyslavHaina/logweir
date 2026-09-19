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
            "/api/v1/session/logout",
            "/auth/login",
            "/auth/callback",
            "/api/v1/namespaces",
            "/api/v1/namespaces/{ns}/connections",
            "/api/v1/namespaces/{ns}/connections/{name}",
            "/api/v1/cadence-previews",
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
            "/api/v1/namespaces/{ns}/destinations",
            "/api/v1/namespaces/{ns}/destinations:from-legacy",
            "/api/v1/namespaces/{ns}/destinations/{name}",
            "/api/v1/namespaces/{ns}/destinations/{name}:update-access",
            "/api/v1/namespaces/{ns}/destinations/{name}:test",
            "/api/v1/namespaces/{ns}/destinations/{name}/usage",
            "/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries",
            "/api/v1/namespaces/{ns}/topic-discoveries/{id}",
            "/api/v1/namespaces/{ns}/topic-discoveries/{id}:cancel",
            "/api/v1/namespaces/{ns}/topic-discoveries/{id}/topics",
            "/api/v1/namespaces/{ns}/preflights",
            "/api/v1/namespaces/{ns}/preflights/{id}",
            "/api/v1/namespaces/{ns}/preflights/{id}:cancel",
            "/api/v1/namespaces/{ns}/preflights/{id}/details",
            "/api/v1/namespaces/{ns}/operations/{kind}/{name}",
            // D3 W11: the stream, the five read families and the one write
            // among them.
            "/api/v1/namespaces/{ns}/operations/{kind}/{name}/events",
            "/api/v1/namespaces/{ns}/protection-policies",
            "/api/v1/namespaces/{ns}/protection-policies/{name}",
            "/api/v1/namespaces/{ns}/rehearsal-schedules",
            "/api/v1/namespaces/{ns}/rehearsal-schedules/{name}",
            "/api/v1/namespaces/{ns}/catalogs",
            "/api/v1/namespaces/{ns}/catalogs/{name}",
            "/api/v1/namespaces/{ns}/catalogs/{name}/points",
            "/api/v1/namespaces/{ns}/catalogs/{name}/signers",
            "/api/v1/namespaces/{ns}/retention-policies",
            "/api/v1/namespaces/{ns}/retention-policies/{name}",
            // The one cluster-scoped family: no `{ns}`, because a TrustPolicy
            // names the namespaces it governs rather than living in one.
            "/api/v1/trust-policies",
            "/api/v1/trust-policies/{name}",
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
    assert_eq!(operation_ids.len(), 56);
}

/// Three documented paths exist only in shared mode: the two `/auth` routes and
/// the logout command. In localAdmin mode they are ABSENT from the route table
/// — 404, not 501 and not a stub — so the probe below runs each documented path
/// against the mode that serves it, and separately asserts that localAdmin does
/// not serve the shared three.
const SHARED_ONLY_PATHS: [&str; 3] = ["/auth/login", "/auth/callback", "/api/v1/session/logout"];

#[tokio::test]
async fn every_documented_operation_is_routed() {
    let app = TestApp::new();
    let shared = support::SharedApp::new(
        support::FakeKube::new(),
        support::idp::MockIdp::new(support::ISSUER, &[]),
        support::SharedOptions {
            bindings: support::default_bindings(),
            ..support::SharedOptions::default()
        },
    );
    let cookie = shared.session_cookie("u-op", &["lw-a-operators"]);
    let csrf = shared.csrf_for("u-op");

    for (path, item) in document()["paths"].as_object().unwrap() {
        let concrete = path
            .replace("{ns}", support::NS_A)
            .replace("{kind}", "backup")
            .replace("{name}", "absent-object")
            .replace("{id}", "absent-object");
        let shared_only = SHARED_ONLY_PATHS.contains(&path.as_str());
        for (method, _) in item.as_object().unwrap() {
            let response = match (method.as_str(), shared_only) {
                ("get", false) => app.get(&concrete).await,
                ("post", false) => app.post(&concrete, Some("documented-route-01"), "{}").await,
                ("put", false) => app.put(&concrete, "{}").await,
                ("get", true) => shared.get(&concrete, &cookie).await,
                ("post", true) => {
                    shared
                        .post(&concrete, &cookie, Some(&csrf), None, "{}")
                        .await
                }
                (other, _) => panic!("undocumented method {other}"),
            };
            assert_ne!(
                response.status.as_u16(),
                405,
                "{method} {concrete} is documented and not routed"
            );
            assert_ne!(response.status.as_u16(), 500, "{method} {concrete}");
            if shared_only {
                assert_ne!(
                    response.status.as_u16(),
                    404,
                    "{method} {concrete} is documented and not routed in shared mode"
                );
            }
        }
    }

    // And the three shared-only paths are not served at all in localAdmin mode.
    assert_eq!(app.get("/auth/login").await.status.as_u16(), 404);
    assert_eq!(
        app.get("/auth/callback?code=x&state=y")
            .await
            .status
            .as_u16(),
        404
    );
    assert_eq!(
        app.post("/api/v1/session/logout", None, "{}")
            .await
            .status
            .as_u16(),
        404
    );
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
    // COMPOSITION KEYWORDS ARE FOLLOWED TOO. `schemars` renders a flattened
    // struct inline today, but it renders an `Option<T>` of a named type as a
    // `$ref` beside a `nullable`, and a future shape could put a payload under
    // `allOf`/`oneOf`/`anyOf`. A walk that stopped at those would quietly stop
    // reaching the schemas the credential scan below is the whole point of.
    for keyword in ["allOf", "oneOf", "anyOf"] {
        if let Some(branches) = schema.get(keyword).and_then(Value::as_array) {
            for branch in branches {
                walk(branch, schemas, seen, out);
            }
        }
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

/// Every schema reachable from a RESPONSE envelope, and the one type that
/// carries a credential value.
///
/// A REQUEST MAY CARRY A CREDENTIAL; A RESPONSE MAY NOT. Write-only entry
/// exists precisely so a value can be typed once into a request body
/// (`CreateDestinationRequest`, `UpdateDestinationAccessRequest`,
/// `DestinationFromLegacyRequest`), and the property scan below would refuse
/// every one of them by name. So the scan is applied to what a response can
/// reach, and `a_response_envelope_never_reaches_the_write_only_credential`
/// asserts the other half: no response schema in the document can contain the
/// type that holds a value.
fn response_reachable(document: &Value) -> BTreeSet<String> {
    let schemas = document["components"]["schemas"]
        .as_object()
        .unwrap()
        .clone();
    let mut reachable = BTreeSet::new();
    for name in schemas.keys() {
        if !(name.ends_with("Response") || name.ends_with("List")) {
            continue;
        }
        let mut seen = BTreeSet::new();
        let mut objects = Vec::new();
        walk(
            &serde_json::json!({ "$ref": format!("#/components/schemas/{name}") }),
            &schemas,
            &mut seen,
            &mut objects,
        );
        reachable.extend(objects.into_iter().map(|(n, _)| n));
    }
    reachable
}

/// THE WRITE-ONLY GUARANTEE, AS A SHAPE. The one type that holds a credential
/// value is unreachable from every response envelope the document publishes.
/// A projection that leaked a value would have to name a type that appears
/// here, and this fails before it ships.
#[test]
fn a_response_envelope_never_reaches_the_write_only_credential() {
    let document = document();
    let reachable = response_reachable(&document);
    assert!(
        reachable.contains("AccessGrantView"),
        "the walk reaches the destination projections at all"
    );
    for value_bearing in [
        "NewCredentialRequest",
        "SecretSourceRequest",
        "AccessGrantRequest",
    ] {
        assert!(
            document["components"]["schemas"]
                .get(value_bearing)
                .is_some(),
            "{value_bearing} is published (as a request type)"
        );
        assert!(
            !reachable.contains(value_bearing),
            "{value_bearing} holds or leads to a credential VALUE and is reachable from a \
             response envelope"
        );
    }
}

/// No response schema names a credential, a token or the approval documents —
/// except `ApprovalPacket`, which is the one explicit route for them.
#[test]
fn response_schemas_carry_no_credential_or_document_bytes() {
    let document = document();
    let reachable = response_reachable(&document);
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
        if !reachable.contains(name.as_str()) {
            continue;
        }
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
                // THREE NAMED EXCEPTIONS, EACH A REFERENCE AND NOT A VALUE.
                // `credentialRef` and `secretName` are Secret NAMES; a name is
                // what this service returns instead of anything about the
                // credential. `csrfToken` is the synchronizer token the
                // browser is meant to receive.
                let allowed = property == "credentialRef"
                    || property == "secretName"
                    || property == "csrfToken"
                    || flag;
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

/// **No two published types share a schema name.**
///
/// REGRESSION REASON, AND IT WAS REAL. `schemars` keys
/// `components/schemas` by a type's SHORT name, so two types called
/// `EvaluationView` in two route modules become ONE schema: the second
/// registration overwrites the first, both `$ref`s point at whichever won, and
/// nothing anywhere reports it. D3 W11 shipped exactly that pair — a
/// retention evaluation and a trust evaluation — and the document published
/// `planSha256` where the trust policy's `serverTime` should have been. A
/// generated document is only a contract if the generator cannot quietly
/// disagree with the types.
///
/// The scan is over declarations rather than over the document, because the
/// document is a map and a map cannot show a duplicate key.
#[test]
fn no_two_published_types_share_a_schema_name() {
    fn walk_dir(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).expect("src is readable") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                walk_dir(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push((
                    path.clone(),
                    std::fs::read_to_string(&path).expect("a source file"),
                ));
            }
        }
    }
    let mut sources = Vec::new();
    walk_dir(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );

    let published: BTreeSet<String> = document()["components"]["schemas"]
        .as_object()
        .expect("the document has schemas")
        .keys()
        .cloned()
        .collect();
    assert!(published.len() > 50, "the scan found no schemas at all");

    // ONLY A TYPE THAT DERIVES `JsonSchema` CAN OCCUPY A SCHEMA NAME. Matching
    // on the name alone would flag `lib.rs`'s startup `Preflight` — a plain
    // struct that shares a word with a DTO and is published by nothing.
    let mut declared: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (path, text) in &sources {
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            for keyword in ["pub struct ", "pub enum "] {
                let Some(rest) = trimmed.strip_prefix(keyword) else {
                    continue;
                };
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !published.contains(&name) {
                    continue;
                }
                // The derive sits in the attribute block immediately above.
                let from = index.saturating_sub(8);
                let derives = lines[from..index].iter().any(|l| l.contains("JsonSchema"));
                if derives {
                    declared
                        .entry(name)
                        .or_default()
                        .push(path.display().to_string());
                }
            }
        }
    }
    assert!(
        declared.len() > 30,
        "the derive filter matched almost nothing, so the scan proves nothing: {}",
        declared.len()
    );
    let collisions: Vec<(&String, &Vec<String>)> = declared
        .iter()
        .filter(|(_, wheres)| wheres.len() > 1)
        .collect();
    assert!(
        collisions.is_empty(),
        "these names are published as ONE schema and declared more than once, so one \
         $ref points at the wrong shape:\n{collisions:#?}"
    );
    // The pair that caused this test, both published and both distinct.
    let schemas = &document()["components"]["schemas"];
    assert!(schemas["RetentionEvaluationView"]["properties"]["planSha256"].is_object());
    assert!(schemas["TrustEvaluationView"]["properties"]["serverTime"].is_object());
}

/// **Every `required` property is one the projection always writes.**
///
/// REGRESSION REASON, FROM THE `d3w12` RECONCILIATION. `schemars` marks a
/// field required unless it is an `Option` or carries a default, and
/// `#[serde(skip_serializing_if = "Vec::is_empty")]` is neither: `CompletionView`
/// published `newTopics` and `TeardownView` published `failed` as REQUIRED
/// while omitting them from every body that had none. A console that validates
/// against the document would then refuse a response the server considers
/// correct — which is exactly what the sibling console branch's own pin does.
///
/// The check is a round trip rather than a reading of the attributes: build
/// each view from an object with the emptiest status the CRD allows, serialize
/// it, and require every `required` name to be there.
#[test]
fn a_required_property_is_never_omitted_by_its_own_projection() {
    use serde_json::json;

    let document = document();
    let schemas = document["components"]["schemas"]
        .as_object()
        .expect("the document has schemas");
    let now: chrono::DateTime<chrono::Utc> = "2026-09-19T01:30:00Z".parse().expect("an instant");

    // The emptiest object each kind admits: identity and nothing else.
    let meta = json!({"name": "x", "namespace": "team-a", "uid": "u", "resourceVersion": "1"});
    let backup = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup", "metadata": meta,
        "spec": {"sourceRef": {"name": "s"}, "topics": ["t"],
                 "archive": {"url": "s3://b/p"}, "triggeredBy": "manual", "deadlineSeconds": 60}
    });
    let restore = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "metadata": meta,
        "spec": {"planBytes": "{}", "approvalRef": {"name": "a"},
                 "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "b",
                 "pointInTime": "2026-09-19T00:00:00Z",
                 "target": {"clusterRef": {"name": "c"}, "mode": "scratch",
                            "topicNaming": {"prefix": "p-"}},
                 "deadlineSeconds": 60}
    });
    let catalog = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog", "metadata": meta,
        "spec": {"destinationRef": {"name": "d"},
                 "sync": {"intervalSeconds": 3600, "mode": "Index", "maxObjectsPerRun": 1000,
                          "deepCheck": "ManifestDigest", "viewLimit": 100}}
    });
    let protection = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "ProtectionPolicy", "metadata": meta,
        "spec": {"protects": {"sourceRef": {"name": "s"}},
                 "objectives": {"maxRecoveryPointAgeSeconds": 300},
                 "evaluationIntervalSeconds": 300}
    });
    let retention = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RetentionPolicy", "metadata": meta,
        "spec": {"destinationRef": {"name": "d"}, "catalogRef": {"name": "c"},
                 "scope": {"prefix": "p"}, "rules": {"minUsablePoints": 1}, "mode": "Report"}
    });
    let trust = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicy",
        "metadata": {"name": "x", "uid": "u", "resourceVersion": "1"},
        "spec": {"default": true, "keys": []}
    });

    // THE PANELS WITH EMPTY LISTS, WHICH IS THE SHAPE THAT CAUGHT THIS. A
    // completion whose scorecard created no topic, and a teardown that removed
    // nothing and failed at nothing, are the bodies where a `Vec` with
    // `skip_serializing_if` is omitted while the schema calls it required. A
    // minimal object alone never reaches those two views at all.
    let mut bare_panels = restore.clone();
    bare_panels["status"] = json!({
        "phase": "Succeeded", "exitCode": 0, "outcome": "pass",
        "completion": {"recordsRestored": 0},
        "teardown": {"attestationKey": "logweir/drills/x.teardown.json"}
    });

    let bodies: Vec<(&str, Value)> = vec![
        (
            "OperationView",
            serde_json::to_value(logweir_api::status::backup_view(
                &serde_json::from_value(backup).expect("a Backup"),
                now,
            ))
            .expect("serialises"),
        ),
        (
            "OperationView",
            serde_json::to_value(logweir_api::status::restore_view(
                &serde_json::from_value(bare_panels).expect("a Restore"),
                now,
            ))
            .expect("serialises"),
        ),
        (
            "OperationView",
            serde_json::to_value(logweir_api::status::restore_view(
                &serde_json::from_value(restore).expect("a Restore"),
                now,
            ))
            .expect("serialises"),
        ),
        (
            "CatalogView",
            serde_json::to_value(logweir_api::routes::catalogs::view(
                &serde_json::from_value(catalog).expect("a RecoveryCatalog"),
                now,
            ))
            .expect("serialises"),
        ),
        (
            "ProtectionPolicyView",
            serde_json::to_value(logweir_api::routes::protection::view(
                &serde_json::from_value(protection).expect("a ProtectionPolicy"),
            ))
            .expect("serialises"),
        ),
        (
            "RetentionPolicyView",
            serde_json::to_value(logweir_api::routes::retention::view(
                &serde_json::from_value(retention).expect("a RetentionPolicy"),
                now,
            ))
            .expect("serialises"),
        ),
        (
            "TrustPolicyView",
            serde_json::to_value(logweir_api::routes::trust::view(
                &serde_json::from_value(trust).expect("a TrustPolicy"),
                now,
                &[],
            ))
            .expect("serialises"),
        ),
    ];

    /// Every `(schema, object)` pair the body reaches, following `$ref`.
    fn check(
        name: &str,
        body: &Value,
        schemas: &serde_json::Map<String, Value>,
        problems: &mut Vec<String>,
    ) {
        let Some(schema) = schemas.get(name) else {
            problems.push(format!("{name} is not published"));
            return;
        };
        let object = body.as_object().expect("a view is an object");
        for required in schema["required"].as_array().into_iter().flatten() {
            let field = required.as_str().unwrap_or_default();
            if !object.contains_key(field) {
                problems.push(format!("{name}.{field} is required and was omitted"));
            }
        }
        // Recurse into the properties the body actually carries.
        let properties = schema["properties"].as_object();
        for (key, value) in object {
            let Some(property) = properties.and_then(|p| p.get(key)) else {
                continue;
            };
            let referenced = property
                .get("$ref")
                .or_else(|| property.get("items").and_then(|i| i.get("$ref")))
                .and_then(Value::as_str)
                .map(|r| r.rsplit('/').next().unwrap_or_default().to_string());
            let Some(child) = referenced else { continue };
            match value {
                Value::Object(_) => check(&child, value, schemas, problems),
                Value::Array(items) => {
                    for item in items {
                        if item.is_object() {
                            check(&child, item, schemas, problems);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let mut problems = Vec::new();
    for (name, body) in &bodies {
        check(name, body, schemas, &mut problems);
    }
    assert!(
        problems.is_empty(),
        "the document requires properties the projection omits:\n{problems:#?}"
    );
    // The scan is only meaningful if it reached the nested views.
    assert!(
        schemas.contains_key("ReadinessView") && schemas.contains_key("OperationTrust"),
        "the nested views are published"
    );
}
