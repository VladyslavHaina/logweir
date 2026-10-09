//! PROD-11.1b (review M1): the product API names a narrowed restore's
//! selection on the list row and on the detail, and never serves a subset
//! restore as a restore of everything.
//!
//! BOTH SIDES READ ONE FIXTURE, as `complete_coverage.rs` does:
//! `ui/tests/fixtures/console/restore-subset-pass.json` and
//! `.../operation-restore-subset-pass.json` are this crate's projections of the
//! custom resource `ui/tests/fixtures/restore-subset-pass.json` (`orders`
//! narrowed to partitions 0 and 2, `payments` restored whole, two engine
//! runs, a covered complete pass), and `ui/tests/replay-selection.spec.js`
//! renders the same files. Set `LOGWEIR_WRITE_SELECTION_FIXTURES=1` to
//! rewrite them from the projection.

mod support;

use logweir_api::projection::restore;
use logweir_api::status::restore_view;
use serde_json::{json, Value};
use weirkeeper::crds::restore::Restore as RestoreCr;

fn now() -> chrono::DateTime<chrono::Utc> {
    "2026-09-19T01:30:00Z".parse().expect("a fixed instant")
}

fn cr(value: &Value) -> RestoreCr {
    serde_json::from_value(value.clone()).expect("the fixture is a Restore")
}

fn golden(name: &str, projected: &Value) -> Value {
    let path = support::repo_root()
        .join("ui/tests/fixtures/console")
        .join(name);
    if std::env::var_os("LOGWEIR_WRITE_SELECTION_FIXTURES").is_some() {
        let doc = json!({"requestId": "req-selection-0001", "item": projected});
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&doc).unwrap()),
        )
        .expect("the fixture is written");
    }
    support::fixture(&format!("console/{name}"))
}

/// **A subset restore names its selection on the list row and on the
/// detail.** The row's `selection` and the operation view's
/// `verificationScope.selection` both carry the narrowed topic, its
/// partitions, the count and the engine runs; the covered complete block is
/// still served (it is the selection's). The console fixtures are these
/// projections. The CONTROL: an unnarrowed pass carries no `selection`
/// anywhere, exactly as before.
///
/// KILLS: the projection dropping `selection` from the row or the scope;
/// `selection_view` dropping the partitions or the count.
#[test]
fn a_subset_restore_names_its_selection_on_list_and_detail() {
    let object = cr(&support::fixture("restore-subset-pass.json"));
    let want = json!({
        "windowEndMs": 1_757_253_900_000i64,
        "narrowedTopics": 1,
        "partitions": [{"topic": "orders", "partitions": [0, 2]}],
        "engineRuns": 2
    });
    for with_bytes in [false, true] {
        let projected = serde_json::to_value(restore(&object, with_bytes)).unwrap();
        assert_eq!(projected["selection"], want, "{projected}");
        assert_eq!(projected["coverage"]["covered"], true);
        if with_bytes {
            let fixture = golden("restore-subset-pass.json", &projected);
            assert_eq!(projected, fixture["item"]);
        }
    }
    let view = serde_json::to_value(restore_view(&object, now())).unwrap();
    let scope = &view["verificationScope"];
    assert_eq!(scope["selection"], want, "{scope}");
    assert_eq!(scope["complete"]["covered"], true);
    assert_eq!(scope["complete"]["partitionCount"], 3);
    let fixture = golden("operation-restore-subset-pass.json", &view);
    assert_eq!(view, fixture["item"]);

    // The control: the unnarrowed pass is what it was.
    let plain = cr(&support::fixture("restore-valid-pass.json"));
    let row = serde_json::to_value(restore(&plain, true)).unwrap();
    assert!(row.get("selection").is_none(), "{row}");
    let view = serde_json::to_value(restore_view(&plain, now())).unwrap();
    assert!(view["verificationScope"].get("selection").is_none());
}

/// **A selection the controller could not read is still a selection.** A
/// status whose `integrity.selection` is empty (the controller's fail-safe
/// copy of an unreadable block) is served as `selection: {}`, never omitted;
/// a list past the status' bounds keeps its count.
#[test]
fn an_unreadable_or_oversized_selection_is_still_served_as_one() {
    let mut v = support::fixture("restore-subset-pass.json");
    v["status"]["integrity"]["selection"] = json!({});
    let row = serde_json::to_value(restore(&cr(&v), false)).unwrap();
    assert_eq!(row["selection"], json!({}), "{row}");
    v["status"]["integrity"]["selection"] = json!({"narrowedTopics": 300});
    let view = serde_json::to_value(restore_view(&cr(&v), now())).unwrap();
    assert_eq!(
        view["verificationScope"]["selection"],
        json!({"narrowedTopics": 300})
    );
}
