//! Backup product projection: the recovery point names its saved destination
//! and publishes only the location the controller froze for that run.

use logweir_api::projection;
use serde_json::{json, Value};
use weirkeeper::crds::backup::Backup;

fn backup(spec: Value, status: Option<Value>) -> Backup {
    let mut object = json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": "backup-1",
            "namespace": "team-a",
            "uid": "uid-backup-1",
            "resourceVersion": "7"
        },
        "spec": spec
    });
    if let Some(status) = status {
        object["status"] = status;
    }
    serde_json::from_value(object).expect("the Backup fixture deserializes")
}

fn base_spec() -> Value {
    json!({
        "sourceRef": {"name": "source"},
        "topics": ["orders"],
        "archive": {"url": "logweir-destination://primary"},
        "destinationRef": {"name": "primary"},
        "triggeredBy": "manual",
        "deadlineSeconds": 1800
    })
}

/// REGRESSION/MUTANT: deleting either assignment in `projection::backup`
/// removes a fact the restore wizard needs and this row fails on the missing
/// JSON key. Recomputing the digest from a live destination cannot satisfy it:
/// this fixture contains no destination object, only the run's frozen status.
#[test]
fn a_destination_backed_point_publishes_the_requested_ref_and_frozen_location() {
    let object = backup(
        base_spec(),
        Some(json!({
            "destination": {
                "name": "primary",
                "uid": "uid-primary",
                "generation": 4,
                "locationDigest": format!("sha256:{}", "a".repeat(64))
            }
        })),
    );

    let projected = serde_json::to_value(projection::backup(&object)).unwrap();
    assert_eq!(
        projected["destinationRef"],
        json!({"name": "primary", "uid": "uid-primary"})
    );
    assert_eq!(
        projected["locationDigest"],
        format!("sha256:{}", "a".repeat(64))
    );
}

/// A destination name and a frozen location are two different facts. During
/// an upgrade (or before the freeze) the first may exist without the second;
/// the API keeps the name and does not invent a digest or uid.
#[test]
fn a_destination_backed_point_without_frozen_status_keeps_only_the_name() {
    let object = backup(base_spec(), None);
    let projected = serde_json::to_value(projection::backup(&object)).unwrap();

    assert_eq!(projected["destinationRef"], json!({"name": "primary"}));
    assert!(projected.get("locationDigest").is_none());
}

/// Legacy inline-archive runs remain readable and are the only points for
/// which the wizard may send `legacySourceArchive`.
#[test]
fn a_legacy_inline_archive_point_publishes_no_saved_destination() {
    let mut spec = base_spec();
    spec.as_object_mut().unwrap().remove("destinationRef");
    spec["archive"] = json!({
        "url": "s3://archive/team-a",
        "secretRef": {"name": "legacy-reader"}
    });
    let object = backup(spec, None);
    let projected = serde_json::to_value(projection::backup(&object)).unwrap();

    assert!(projected.get("destinationRef").is_none());
    assert!(projected.get("locationDigest").is_none());
}
