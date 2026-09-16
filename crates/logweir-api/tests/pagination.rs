//! Pagination bounds and the authenticated cursor: round trip, tamper,
//! cross-namespace, cross-route, cross-filter, cross-actor, expiry and
//! Kubernetes 410.

mod support;

use std::sync::Arc;

use logweir_api::auth::LocalAdminAuthenticator;
use serde_json::json;
use support::{FakeKube, Options, TestApp, NS_A, NS_B};

fn seed_backups(fake: &FakeKube, ns: &str, count: usize) {
    for i in 0..count {
        let schedule = if i % 2 == 0 { "nightly" } else { "hourly" };
        fake.seed(
            "backups",
            ns,
            json!({
                "metadata": {"name": format!("backup-{i:04}"), "labels": {"logweir.dev/schedule": schedule}},
                "spec": {"sourceRef": {"name": "source"}, "topics": ["orders"], "archive": {"url": "s3://b/p"}, "triggeredBy": "schedule", "deadlineSeconds": 1800}
            }),
        );
    }
}

#[tokio::test]
async fn the_default_page_is_fifty_and_the_maximum_is_two_hundred() {
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, 250);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");

    let page = app.get(&base).await.json();
    assert_eq!(page["items"].as_array().unwrap().len(), 50);
    assert_eq!(page["page"]["limit"], 50);
    assert!(page["page"]["nextCursor"].is_string());
    assert!(page["page"]["snapshot"].is_string());

    let page = app.get(&format!("{base}?limit=200")).await.json();
    assert_eq!(page["items"].as_array().unwrap().len(), 200);

    for bad in ["0", "201", "1000", "-5", "ten", ""] {
        let response = app.get(&format!("{base}?limit={bad}")).await;
        response.assert_problem(422, "validation_failed");
    }
    // The limit reaches Kubernetes as-is; no unbounded list is ever made.
    for r in app.fake.requests() {
        let q: std::collections::BTreeMap<String, String> =
            serde_urlencoded::from_str(&r.query).unwrap();
        let limit: u32 = q["limit"].parse().unwrap();
        assert!((1..=200).contains(&limit), "{:?}", r.query);
    }
    app.fake.assert_strict();
}

#[tokio::test]
async fn a_cursor_walks_every_item_exactly_once() {
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, 23);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");
    let mut seen = Vec::new();
    let mut url = format!("{base}?limit=5");
    let mut pages = 0;
    loop {
        let page = app.get(&url).await;
        assert_eq!(page.status, 200, "{}", String::from_utf8_lossy(&page.body));
        let v = page.json();
        for item in v["items"].as_array().unwrap() {
            seen.push(item["name"].as_str().unwrap().to_string());
        }
        pages += 1;
        match v["page"]["nextCursor"].as_str() {
            Some(cursor) => {
                // Opaque: the Kubernetes continue token is not visible in clear.
                assert!(!cursor.contains("fake-continue"));
                url = format!("{base}?limit=5&cursor={cursor}");
            }
            None => break,
        }
    }
    assert_eq!(pages, 5);
    let mut expected: Vec<String> = (0..23).map(|i| format!("backup-{i:04}")).collect();
    expected.sort();
    assert_eq!(seen, expected);
    app.fake.assert_strict();
}

async fn first_cursor(app: &TestApp, path: &str) -> String {
    app.get(path).await.json()["page"]["nextCursor"]
        .as_str()
        .expect("a next cursor")
        .to_string()
}

#[tokio::test]
async fn a_tampered_or_rescoped_cursor_is_invalid() {
    let fake = FakeKube::new();
    seed_backups(&fake, NS_A, 10);
    seed_backups(&fake, NS_B, 10);
    let app = TestApp::with(fake.clone(), Options::default());
    let base_a = format!("/api/v1/namespaces/{NS_A}/backups");
    let cursor = first_cursor(&app, &format!("{base_a}?limit=3")).await;

    // Tamper: flip a character in the payload and in the tag.
    for position in [2, cursor.len() - 2] {
        let mut bytes = cursor.clone().into_bytes();
        bytes[position] = if bytes[position] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();
        app.get(&format!("{base_a}?limit=3&cursor={tampered}"))
            .await
            .assert_problem(400, "cursor_invalid");
    }
    // Truncated, empty-tag and garbage.
    let (payload, _) = cursor.split_once('.').unwrap();
    for bad in [
        payload.to_string(),
        format!("{payload}."),
        "x".repeat(5000),
        "%%%".to_string(),
    ] {
        let response = app.get(&format!("{base_a}?limit=3&cursor={bad}")).await;
        assert!(
            response.status == 400,
            "{} {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        );
    }
    // Another namespace.
    app.get(&format!(
        "/api/v1/namespaces/{NS_B}/backups?limit=3&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    // Another route in the same namespace.
    app.get(&format!(
        "/api/v1/namespaces/{NS_A}/restores?limit=3&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    // Another filter set.
    app.get(&format!(
        "{base_a}?limit=3&labelSelector=logweir.dev/schedule%3Dnightly&cursor={cursor}"
    ))
    .await
    .assert_problem(400, "cursor_invalid");
    // Another actor.
    let other = TestApp::with(
        fake.clone(),
        Options {
            authenticator: Some(Arc::new(LocalAdminAuthenticator::new(
                "second-admin",
                "Second",
            ))),
            ..Options::default()
        },
    );
    other
        .get(&format!("{base_a}?limit=3&cursor={cursor}"))
        .await
        .assert_problem(400, "cursor_invalid");
    // Another cursor key (a rotated key invalidates outstanding cursors).
    let rotated = TestApp::with(
        fake.clone(),
        Options {
            cursor_key: vec![0x11; 32],
            ..Options::default()
        },
    );
    rotated
        .get(&format!("{base_a}?limit=3&cursor={cursor}"))
        .await
        .assert_problem(400, "cursor_invalid");

    // The untampered cursor still works, and none of the refusals above
    // reached Kubernetes with a continue token.
    let response = app.get(&format!("{base_a}?limit=3&cursor={cursor}")).await;
    assert_eq!(response.status, 200);
    let with_continue = fake
        .requests()
        .iter()
        .filter(|r| r.query.contains("continue="))
        .count();
    assert_eq!(
        with_continue, 1,
        "only the authentic cursor reached Kubernetes"
    );
    fake.assert_strict();
}

#[tokio::test]
async fn an_expired_cursor_is_410() {
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, 10);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");
    let cursor = first_cursor(&app, &format!("{base}?limit=3")).await;
    app.clock.advance(14 * 60 + 59);
    assert_eq!(
        app.get(&format!("{base}?limit=3&cursor={cursor}"))
            .await
            .status,
        200
    );
    app.clock.advance(1);
    let response = app.get(&format!("{base}?limit=3&cursor={cursor}")).await;
    response.assert_problem(410, "cursor_expired");
    assert!(response.json()["detail"]
        .as_str()
        .unwrap()
        .contains("restart the list"));
}

#[tokio::test]
async fn a_kubernetes_410_on_continue_is_cursor_expired() {
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, 10);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");
    let cursor = first_cursor(&app, &format!("{base}?limit=3")).await;
    app.fake.expire_continue_tokens();
    app.get(&format!("{base}?limit=3&cursor={cursor}"))
        .await
        .assert_problem(410, "cursor_expired");
    // Restarting the list without a cursor works.
    assert_eq!(app.get(&format!("{base}?limit=3")).await.status, 200);
}

#[tokio::test]
async fn label_selectors_are_equality_only_and_bound_into_the_cursor() {
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, 12);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");
    let page = app
        .get(&format!(
            "{base}?labelSelector=logweir.dev/schedule%3Dnightly&limit=4"
        ))
        .await
        .json();
    assert_eq!(page["items"].as_array().unwrap().len(), 4);
    let cursor = page["page"]["nextCursor"].as_str().unwrap();
    let next = app
        .get(&format!(
            "{base}?labelSelector=logweir.dev/schedule%3Dnightly&limit=4&cursor={cursor}"
        ))
        .await
        .json();
    assert_eq!(next["items"].as_array().unwrap().len(), 2);
    assert!(next["page"]["nextCursor"].is_null());
    for bad in ["a!%3Db", "a%20in%20(b)", "a", "!a"] {
        app.get(&format!("{base}?labelSelector={bad}"))
            .await
            .assert_problem(422, "validation_failed");
    }
    app.get(&format!("{base}?fieldSelector=metadata.name%3Dx"))
        .await
        .assert_problem(400, "malformed_request");
    app.fake.assert_strict();
}

/// **A catalog far past the maximum page size pages through with cursor
/// continuity, and never collects more than one page at a time.**
///
/// D0's required-test list names "large catalogs and bounded memory"; the live
/// smoke only reached 56 objects, which is one page and proves nothing about
/// either. 900 objects at the 200 maximum is five pages.
///
/// THE MEMORY CLAIM IS ASSERTED, not asserted-about. Every outbound list the
/// adapter issued carried a `limit`, none exceeded the requested page size, and
/// the number of list calls equals the number of pages served — so the API
/// never collected the catalog to serve a page of it. That is the thing D0 is
/// actually worried about: a substring search or a sort implemented by reading
/// everything.
#[tokio::test]
async fn a_large_catalog_pages_through_with_cursor_continuity_and_bounded_reads() {
    const TOTAL: usize = 900;
    const LIMIT: usize = 200;
    let app = TestApp::new();
    seed_backups(&app.fake, NS_A, TOTAL);
    let base = format!("/api/v1/namespaces/{NS_A}/backups");
    app.fake.clear_requests();

    let mut seen: Vec<String> = Vec::new();
    let mut sizes: Vec<usize> = Vec::new();
    let mut snapshots: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(
            pages <= 10,
            "the catalog did not terminate in {pages} pages"
        );
        let url = match &cursor {
            None => format!("{base}?limit={LIMIT}"),
            Some(c) => format!("{base}?limit={LIMIT}&cursor={}", percent_encode(c.as_str())),
        };
        let page = app.get(&url).await.json();
        let items = page["items"].as_array().unwrap();
        assert!(
            items.len() <= LIMIT,
            "page {pages} returned {} items for limit {LIMIT}",
            items.len()
        );
        sizes.push(items.len());
        for item in items {
            seen.push(item["name"].as_str().unwrap().to_string());
        }
        if let Some(snapshot) = page["page"]["snapshot"].as_str() {
            snapshots.insert(snapshot.to_string());
        }
        match page["page"]["nextCursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }

    assert_eq!(pages, 5, "sizes: {sizes:?}");
    assert_eq!(sizes, vec![200, 200, 200, 200, 100]);

    // Continuity: every object exactly once, in order, with no gap and no
    // repeat across the cursor boundaries.
    assert_eq!(seen.len(), TOTAL, "duplicate or missing rows");
    let unique: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), TOTAL, "a row appeared on two pages");
    let mut expected: Vec<String> = (0..TOTAL).map(|i| format!("backup-{i:04}")).collect();
    expected.sort();
    assert_eq!(
        seen, expected,
        "the pages did not cover the catalog in order"
    );

    // Bounded reads: one Kubernetes list per page, each with a limit, none
    // asking for more than the page the client asked for.
    let lists: Vec<_> = app
        .fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "GET" && r.path.ends_with("/backups"))
        .collect();
    assert_eq!(
        lists.len(),
        pages,
        "{} Kubernetes lists for {pages} pages: the API is reading more than it serves",
        lists.len()
    );
    for (index, list) in lists.iter().enumerate() {
        let query: std::collections::BTreeMap<String, String> =
            serde_urlencoded::from_str(&list.query).unwrap();
        let limit: usize = query
            .get("limit")
            .unwrap_or_else(|| panic!("list {index} carried no limit: {}", list.query))
            .parse()
            .unwrap();
        assert!(
            limit <= LIMIT,
            "list {index} asked Kubernetes for {limit} rows to serve at most {LIMIT}"
        );
        assert_eq!(
            index > 0,
            query.contains_key("continue"),
            "list {index} continue-token handling: {}",
            list.query
        );
    }
    app.fake.assert_strict();
}

/// Percent-encode a cursor for a query string. Cursors are base64url plus a
/// `.`, so only `=` padding would need it — but the encoding is applied rather
/// than assumed, so a cursor format change does not silently break this test.
fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}
