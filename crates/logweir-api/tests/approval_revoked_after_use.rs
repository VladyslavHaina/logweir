//! Review M1 (poc-fixes-2), the API half: a CONSUMED `Approval` whose signer
//! was later revoked for `KeyCompromise` reaches the console exactly as the
//! controller recorded it — `verified: false`, the `Verified` condition with
//! reason `RecordedBeforeRevocation`, `Consumed=True` with the admission
//! instant, and the record (authorization, key id) kept.
//!
//! THE FIXTURE IS SHARED. `ui/tests/fixtures/console/approval-revoked-after-use.json`
//! is what this row projects a custom resource INTO, what
//! `ui/tests/approval-policy.spec.js` renders never green, and what
//! `crates/weirkeeper/tests/approval_policy.rs` checks the controller writes.
//! Three readers of one file, so the three sides cannot drift apart.

use logweir_api::projection;
use serde_json::{json, Value};
use weirkeeper::crds::approval::Approval;

fn fixture() -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ui/tests/fixtures/console/approval-revoked-after-use.json"
    );
    let text = std::fs::read_to_string(path).expect("the shared fixture is readable");
    serde_json::from_str(&text).expect("the shared fixture is JSON")
}

/// The custom resource the controller leaves behind, built from the fixture's
/// own facts, so the projection is the only thing under test.
fn object(item: &Value) -> Approval {
    let conditions: Vec<Value> = item["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .map(|c| {
            let mut c = c.clone();
            c["observedGeneration"] = json!(1);
            c
        })
        .collect();
    let bytes = |n: &Value| "x".repeat(usize::try_from(n.as_u64().unwrap_or(0)).unwrap_or(0));
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {
            "name": item["name"], "namespace": item["namespace"], "uid": item["uid"],
            "resourceVersion": item["resourceVersion"], "creationTimestamp": item["createdAt"],
            "generation": 1
        },
        "spec": {
            "subjectRef": item["subjectRef"],
            "planHash": item["planHash"],
            "approvalBytes": bytes(&item["approvalBytesLength"]),
            "sidecarBytes": bytes(&item["sidecarBytesLength"])
        },
        "status": {
            "verified": item["verified"],
            "matchedKeyId": item["matchedKeyId"],
            "approver": item["approver"],
            "ticket": item["ticket"],
            "selfAttestedRisk": item["selfAttestedRisk"],
            "authorization": item["authorization"],
            "verifiedSubjectRef": {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": item["verifiedSubject"]["kind"],
                "name": item["verifiedSubject"]["name"],
                "namespace": item["verifiedSubject"]["namespace"],
                "uid": item["verifiedSubject"]["uid"]
            },
            "conditions": conditions
        }
    }))
    .expect("the Approval deserializes")
}

#[test]
fn a_consumed_approval_with_a_compromised_signer_projects_never_verified_and_keeps_the_record() {
    let doc = fixture();
    let item = &doc["item"];
    let projected =
        serde_json::to_value(projection::approval(&object(item))).expect("the DTO serialises");
    assert_eq!(
        &projected, item,
        "the API projects the controller's record byte for byte"
    );
    assert_eq!(projected["verified"], json!(false), "never green");
    let reasons: Vec<&str> = projected["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .filter_map(|c| c["reason"].as_str())
        .collect();
    assert_eq!(reasons, ["RecordedBeforeRevocation", "RestoreAdmitted"]);
    assert!(
        projected["authorization"]["confirmationKeyId"].is_string(),
        "the record survives"
    );
}
