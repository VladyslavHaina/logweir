//! PLAT-20.2: the product API at the sizes its own bounds allow — what one
//! request costs, and what walking a whole list costs.
//!
//! TWO KINDS OF TEST LIVE HERE, AND ONLY ONE OF THEM RUNS BY DEFAULT.
//!
//! * The `measure_*` rows are `#[ignore]`d. They seed the in-process fake API
//!   server with the largest collections the product publishes — the console's
//!   whole list budget of 5,000 `Backup`s and 5,000 `Restore`s
//!   (`ui/client.js`, `LIST_PAGE_SIZE` × `LIST_PAGE_BUDGET`), the largest
//!   `RecoveryCatalog` view (`spec.sync.viewLimit` 5,000, in the most page
//!   `ConfigMap`s one request reads) beside more `Backup`s than the verdict join
//!   reads, and 10,000 and 50,000 stored topic names — walk each one the way the
//!   console does, and print the wall-clock cost per request. They are
//!   measurements, not assertions about time: the numbers and the machine they
//!   were taken on are recorded in `docs/stability.md`, *Measured scale
//!   limits*. Run them with
//!
//!   ```text
//!   cargo test --release -p logweir-api --test scale -- --ignored --nocapture --test-threads=1
//!   ```
//!
//! * The unignored rows are the regression checks, and each one guards a
//!   budget the product already states — never a wall-clock number, which a
//!   shared CI host cannot hold steady. What they assert is WORK: how many
//!   Kubernetes reads one request may make, whatever the size of what it pages
//!   over. An unbounded read is the regression class that turns a large
//!   namespace into an outage, and it is invisible to every test that seeds
//!   three objects.
//!
//!   1. `GET .../catalogs/{name}/points` reads the catalog, at most
//!      [`MAX_PAGES_PER_REQUEST`] page `ConfigMap`s and at most
//!      [`MAX_BACKUP_SCAN_PAGES`] `Backup` pages (D3 §10), however many pages the
//!      status names and however many `Backup`s the namespace holds.
//!   2. `GET .../topic-discoveries/{id}/topics` reads at most
//!      [`MAX_CHUNKS_PER_REQUEST`] chunks, even for a filter that matches
//!      nothing in a 50,000-topic inventory (D2 §5.5).
//!   3. `GET .../backups` and `GET .../restores` make exactly ONE bounded
//!      Kubernetes `LIST` per page, and no per-item read: walking the console's
//!      whole budget is `LIST_PAGE_BUDGET` requests and as many `LIST`s.

mod support;

use std::time::Instant;

use logweir_api::routes::catalogs::{
    BACKUP_SCAN_PAGE, MAX_BACKUP_SCAN_PAGES, MAX_PAGES_PER_REQUEST,
};
use logweir_api::routes::topic_discoveries::MAX_CHUNKS_PER_REQUEST;
use serde_json::{json, Value};
use support::{repo_root, seed_discovery, topic_line, FakeKube, Options, TestApp, NS_A};

/// The console's page size and page budget (`ui/client.js`). A console list
/// reads at most their product, and refuses to render a prefix as the whole.
const CONSOLE_PAGE_SIZE: usize = 200;
const CONSOLE_PAGE_BUDGET: usize = 25;
/// `RecoveryCatalog.spec.sync.viewLimit`'s CRD maximum — the most points a
/// materialised view carries, whatever the archive holds.
const MAX_VIEW_POINTS: usize = 5_000;
/// `TopicDiscovery`'s chunk size (`weirkeeper::check::chunks::MAX_CHUNK_LINES`).
const CHUNK_LINES: usize = 2_500;

fn fixture(name: &str) -> Value {
    let path = repo_root()
        .join("crates/logweir-api/tests/fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .expect("a fixture is JSON")
}

// ======================================================================
// Seeding
// ======================================================================

/// `count` copies of a live-derived `Backup` (verified, succeeded), each with
/// its own name and uid.
fn seed_backups(fake: &FakeKube, count: usize) {
    let template = fixture("backup-succeeded-verified.json");
    for i in 0..count {
        let mut b = template.clone();
        b["metadata"]["name"] = json!(format!("b-{i:06}"));
        b["metadata"]["uid"] = json!(format!("00000000-0000-4000-8000-{i:012}"));
        fake.seed("backups", NS_A, b);
    }
}

/// `count` copies of a live-derived `Restore`.
fn seed_restores(fake: &FakeKube, count: usize) {
    let template = fixture("restore-legacy-no-progress.json");
    for i in 0..count {
        let mut r = template.clone();
        r["metadata"]["name"] = json!(format!("r-{i:06}"));
        r["metadata"]["uid"] = json!(format!("00000000-0000-4000-9000-{i:012}"));
        fake.seed("restores", NS_A, r);
    }
}

/// `count` `Backup`s whose verdict the point join reads, none of them naming a
/// point in the view (so every row stays the catalog's to decide).
fn seed_verdict_backups(fake: &FakeKube, count: usize) {
    for i in 0..count {
        fake.seed(
            "backups",
            NS_A,
            json!({
                "metadata": {"name": format!("v-{i:06}")},
                "spec": {
                    "sourceRef": {"name": "prod-kafka"},
                    "topics": ["orders"],
                    "archive": {"url": "logweir-destination://primary"},
                    "destinationRef": {"name": "primary"},
                    "triggeredBy": "manual",
                    "deadlineSeconds": 3600
                },
                "status": {
                    "phase": "Succeeded",
                    "exitCode": 0,
                    "backupId": format!("set-v-{i:06}"),
                    "evidence": {
                        "receiptSha256": format!("sha256:{:064x}", 0xffff_0000_u64 + i as u64),
                        "verification": {"result": "NotAttempted"}
                    }
                }
            }),
        );
    }
}

/// `points` view entries, each a copy of the live-derived first fixture row
/// with its own point id, receipt digest, backup id and run id.
fn view_lines(points: usize) -> Vec<String> {
    let path = repo_root().join("crates/logweir-api/tests/fixtures/catalog-page-entries.jsonl");
    let first = std::fs::read_to_string(path)
        .expect("the entry fixture is readable")
        .lines()
        .find(|l| !l.trim().is_empty())
        .expect("one row")
        .to_string();
    let template: Value = serde_json::from_str(&first).expect("a row");
    (0..points)
        .map(|i| {
            let mut row = template.clone();
            let digest = format!("{:064x}", i as u64 + 1);
            let backup_id = format!("00000000-0000-4000-a000-{i:012}");
            // A 26-character, Crockford-shaped run id, as the runner writes.
            let run_id = format!("01M2VKC{i:019}");
            row["pointId"] = json!(format!("lwp1-{}", &digest[..32]));
            row["receiptSha256"] = json!(format!("sha256:{digest}"));
            row["backupId"] = json!(backup_id);
            row["runId"] = json!(run_id);
            row["receiptKey"] = json!(format!("logweir/backups/{backup_id}/{run_id}.receipt.json"));
            row["manifestKey"] = json!(format!("archive/{backup_id}/manifest.json"));
            row["recoveryPointAtMs"] = json!(1_789_780_191_192_i64 - i as i64 * 60_000);
            serde_json::to_string(&row).expect("a row serialises")
        })
        .collect()
}

/// A `RecoveryCatalog` named `primary` whose status names `pages` page
/// `ConfigMap`s holding `points` entries between them, every page seeded with
/// its recorded digest.
fn seed_view(fake: &FakeKube, points: usize, pages: usize) {
    let lines = view_lines(points);
    let per_page = points.div_ceil(pages);
    let mut index = Vec::new();
    for (i, chunk) in lines.chunks(per_page).enumerate() {
        let name = format!("primary-g1-p{i}");
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        let digest = weirkeeper::catalog_view::page_digest(&refs);
        fake.seed(
            "configmaps",
            NS_A,
            json!({
                "metadata": {
                    "name": name,
                    "annotations": {weirkeeper::catalog_view::PAGE_DIGEST_ANNOTATION: digest.clone()}
                },
                "immutable": true,
                "data": {weirkeeper::catalog_view::PAGE_DATA_KEY: chunk.join("\n") + "\n"},
            }),
        );
        index.push(json!({
            "configMapName": name,
            "index": i,
            "count": chunk.len(),
            "sha256": digest,
        }));
    }
    let mut catalog = fixture("recovery-catalog.json");
    catalog["status"]["pages"] = json!(index);
    catalog["spec"]["sync"]["viewLimit"] = json!(points);
    fake.seed("recoverycatalogs", NS_A, catalog);
}

/// `topics` topic lines in chunks of [`CHUNK_LINES`], stored as one discovery.
fn seed_inventory(fake: &FakeKube, name: &str, topics: usize) {
    let lines: Vec<String> = (0..topics)
        .map(|i| {
            topic_line(
                &format!("orders.region-{:02}.stream-{i:06}", i % 40),
                6,
                "-",
            )
        })
        .collect();
    let chunks: Vec<Vec<String>> = lines.chunks(CHUNK_LINES).map(<[String]>::to_vec).collect();
    seed_discovery(fake, NS_A, name, "source", None, &chunks);
}

// ======================================================================
// Walking, counting and timing
// ======================================================================

/// What the fake Kubernetes API was asked during one request, by kind.
#[derive(Debug, Default, Clone, Copy)]
struct Reads {
    lists: usize,
    gets: usize,
    config_maps: usize,
    other: usize,
}

fn reads(fake: &FakeKube) -> Reads {
    let mut r = Reads::default();
    for req in fake.requests() {
        if req.method != "GET" {
            r.other += 1;
        } else if req.path.contains("/configmaps/") {
            r.config_maps += 1;
        } else if req.query.contains("limit=") {
            r.lists += 1;
        } else {
            r.gets += 1;
        }
    }
    r
}

/// One walk of a paged route: every page's body, its reads and its time.
struct Walk {
    pages: Vec<Value>,
    reads: Vec<Reads>,
    millis: Vec<f64>,
}

impl Walk {
    fn items(&self) -> usize {
        self.pages
            .iter()
            .map(|p| p["items"].as_array().map_or(0, Vec::len))
            .sum()
    }

    fn report(&self, label: &str) {
        let mut sorted = self.millis.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("a finite time"));
        let total: f64 = self.millis.iter().sum();
        let median = sorted[sorted.len() / 2];
        let max = sorted[sorted.len() - 1];
        let lists = self.reads.iter().map(|r| r.lists).max().unwrap_or(0);
        let maps = self.reads.iter().map(|r| r.config_maps).max().unwrap_or(0);
        println!(
            "MEASURE {label}: requests={} items={} total_ms={total:.1} median_ms={median:.2} \
             max_ms={max:.2} max_lists_per_request={lists} max_configmap_reads_per_request={maps}",
            self.pages.len(),
            self.items(),
        );
    }
}

/// Follow `nextCursor` from `first` until the route says there is no more,
/// or `cap` requests were made.
async fn walk(app: &TestApp, first: &str, cap: usize) -> Walk {
    let mut walk = Walk {
        pages: Vec::new(),
        reads: Vec::new(),
        millis: Vec::new(),
    };
    let separator = if first.contains('?') { '&' } else { '?' };
    let mut url = first.to_string();
    for _ in 0..cap {
        app.fake.clear_requests();
        let started = Instant::now();
        let response = app.get(&url).await;
        walk.millis.push(started.elapsed().as_secs_f64() * 1_000.0);
        assert_eq!(response.status, 200, "{}: {}", url, response.text());
        walk.reads.push(reads(&app.fake));
        let body = response.json();
        let next = body["page"]["nextCursor"].as_str().map(str::to_string);
        walk.pages.push(body);
        match next {
            Some(cursor) => url = format!("{first}{separator}cursor={cursor}"),
            None => break,
        }
    }
    walk
}

// ======================================================================
// Regression checks: a request's reads are bounded by the stated budget
// ======================================================================

/// **A point page reads a bounded number of objects, whatever the view and the
/// namespace hold.**
///
/// The status names MORE pages than one request may read (nine, one past D3
/// §10's eight) and the namespace holds more `Backup`s than the verdict join
/// reads (one page past four of five hundred). Every request of a whole walk
/// must still read the catalog once, at most eight pages and at most four
/// `Backup` pages, and nothing else.
///
/// MUTANT: drop `.take(MAX_PAGES_PER_REQUEST)` in `routes/catalogs.rs::points`
/// and the ninth page is read — this row fails on `config_maps`. Drop the
/// `for _ in 0..MAX_BACKUP_SCAN_PAGES` bound and the join walks the whole
/// namespace — it fails on `lists`.
#[tokio::test]
async fn a_point_page_reads_a_bounded_number_of_objects_at_any_scale() {
    let fake = FakeKube::new();
    let pages = MAX_PAGES_PER_REQUEST + 1;
    seed_view(&fake, pages * 3, pages);
    seed_verdict_backups(&fake, MAX_BACKUP_SCAN_PAGES * BACKUP_SCAN_PAGE as usize + 1);
    let app = TestApp::with(fake, Options::default());

    let walked = walk(
        &app,
        "/api/v1/namespaces/team-a/catalogs/primary/points?limit=2",
        64,
    )
    .await;
    assert!(walked.pages.len() > 1, "the walk must page to be a walk");
    for (i, r) in walked.reads.iter().enumerate() {
        assert_eq!(r.gets, 1, "request {i} reads the catalog once: {r:?}");
        assert!(
            r.config_maps <= MAX_PAGES_PER_REQUEST,
            "request {i} read {} page ConfigMaps; the bound is {MAX_PAGES_PER_REQUEST}",
            r.config_maps
        );
        assert!(
            r.lists <= MAX_BACKUP_SCAN_PAGES,
            "request {i} made {} Backup LISTs; the join reads at most {MAX_BACKUP_SCAN_PAGES}",
            r.lists
        );
        assert_eq!(r.other, 0, "request {i} wrote something: {r:?}");
    }
    // The ninth page is never read, so the walk ends at eight pages' rows and
    // says the join is incomplete rather than pretending it saw every Backup.
    assert_eq!(walked.items(), MAX_PAGES_PER_REQUEST * 3);
    assert_eq!(
        walked.pages.last().expect("a page")["backupVerdictsIncomplete"],
        "Truncated"
    );
    app.fake.assert_strict();
}

/// **A topic search reads a bounded number of chunks, even when it matches
/// nothing.**
///
/// A 50,000-topic inventory is twenty chunks; a `q` that matches no name must
/// stop at [`MAX_CHUNKS_PER_REQUEST`] and hand back a cursor with
/// `scan.complete: false`, not read all twenty in one request. A plain walk at
/// the console's page size reads at most two chunks per page (a page may
/// straddle a chunk boundary).
///
/// MUTANT: remove the `chunks_scanned >= MAX_CHUNKS_PER_REQUEST` break in
/// `routes/topic_discoveries.rs::topics` and the first request reads twenty.
#[tokio::test]
async fn a_topic_search_reads_a_bounded_number_of_chunks_at_any_scale() {
    let fake = FakeKube::new();
    seed_inventory(&fake, "td-scale", 50_000);
    let app = TestApp::with(fake, Options::default());

    let base = "/api/v1/namespaces/team-a/topic-discoveries/td-scale/topics";
    let sparse = walk(&app, &format!("{base}?q=no-such-topic&limit=200"), 64).await;
    for (i, r) in sparse.reads.iter().enumerate() {
        assert!(
            r.config_maps <= MAX_CHUNKS_PER_REQUEST as usize,
            "search request {i} read {} chunks; the bound is {MAX_CHUNKS_PER_REQUEST}",
            r.config_maps
        );
    }
    assert_eq!(sparse.items(), 0);
    assert_eq!(
        sparse.pages.len(),
        (50_000usize.div_ceil(CHUNK_LINES)).div_ceil(MAX_CHUNKS_PER_REQUEST as usize),
        "a search that matches nothing takes one request per {MAX_CHUNKS_PER_REQUEST} chunks"
    );
    assert_eq!(sparse.pages[0]["scan"]["complete"], false);

    // The first 2,000 names at the console's page size: ten requests, never
    // more than two chunks each.
    let plain = walk(&app, &format!("{base}?limit={CONSOLE_PAGE_SIZE}"), 10).await;
    for (i, r) in plain.reads.iter().enumerate() {
        assert!(r.config_maps <= 2, "page {i} read {} chunks", r.config_maps);
    }
    assert_eq!(plain.items(), 10 * CONSOLE_PAGE_SIZE);
    app.fake.assert_strict();
}

/// **Walking the console's whole list budget is one bounded `LIST` per page,
/// and no per-item read.**
///
/// 5,000 `Backup`s at the console's page size of 200 are 25 requests. Each
/// must make exactly one `LIST` with `limit=200` and no `GET` of an item — the
/// N+1 shape that would make a history page cost one API call per run.
///
/// MUTANT: have `projection::backup` (or the list route) read anything per
/// item and `gets` stops being zero.
#[tokio::test]
async fn walking_the_console_list_budget_is_one_list_per_page() {
    let fake = FakeKube::new();
    let total = CONSOLE_PAGE_SIZE * CONSOLE_PAGE_BUDGET;
    seed_backups(&fake, total);
    let app = TestApp::with(fake, Options::default());

    let walked = walk(
        &app,
        &format!("/api/v1/namespaces/team-a/backups?limit={CONSOLE_PAGE_SIZE}"),
        CONSOLE_PAGE_BUDGET + 1,
    )
    .await;
    assert_eq!(walked.items(), total);
    assert_eq!(walked.pages.len(), CONSOLE_PAGE_BUDGET);
    for (i, r) in walked.reads.iter().enumerate() {
        assert_eq!(
            (r.lists, r.gets, r.config_maps, r.other),
            (1, 0, 0, 0),
            "page {i} made {r:?}; a list page is one LIST and nothing else"
        );
    }
    for req in app.fake.requests() {
        assert!(
            req.query.contains(&format!("limit={CONSOLE_PAGE_SIZE}")),
            "every LIST carries the page's own limit: {:?}",
            req.query
        );
    }
    app.fake.assert_strict();
}

// ======================================================================
// Measurements (ignored; `--ignored --nocapture`)
// ======================================================================

/// Status load: the console's whole list budget of `Backup`s and `Restore`s.
#[tokio::test]
#[ignore = "a measurement; run with --ignored --nocapture"]
async fn measure_status_load_over_the_console_list_budget() {
    let total = CONSOLE_PAGE_SIZE * CONSOLE_PAGE_BUDGET;
    let fake = FakeKube::new();
    seed_backups(&fake, total);
    seed_restores(&fake, total);
    let app = TestApp::with(fake, Options::default());
    for (plural, label) in [("backups", "Backup"), ("restores", "Restore")] {
        let walked = walk(
            &app,
            &format!("/api/v1/namespaces/team-a/{plural}?limit={CONSOLE_PAGE_SIZE}"),
            CONSOLE_PAGE_BUDGET + 1,
        )
        .await;
        assert_eq!(walked.items(), total);
        walked.report(&format!(
            "status load: {total} {label} objects at limit={CONSOLE_PAGE_SIZE}"
        ));
    }
    // One operation read, which is what an operation view polls.
    let mut millis = Vec::new();
    for _ in 0..50 {
        let started = Instant::now();
        let r = app
            .get("/api/v1/namespaces/team-a/operations/backup/b-002500")
            .await;
        millis.push(started.elapsed().as_secs_f64() * 1_000.0);
        assert_eq!(r.status, 200, "{}", r.text());
    }
    millis.sort_by(|a, b| a.partial_cmp(b).expect("a finite time"));
    println!(
        "MEASURE status load: one operation read among {total} Backups: median_ms={:.2} max_ms={:.2}",
        millis[millis.len() / 2],
        millis[millis.len() - 1]
    );
}

/// History queries: the largest catalog view, walked at the console's page
/// size, beside more `Backup`s than the verdict join reads.
#[tokio::test]
#[ignore = "a measurement; run with --ignored --nocapture"]
async fn measure_catalog_points_over_the_largest_view() {
    for backups in [0, MAX_BACKUP_SCAN_PAGES * BACKUP_SCAN_PAGE as usize] {
        let fake = FakeKube::new();
        seed_view(&fake, MAX_VIEW_POINTS, MAX_PAGES_PER_REQUEST);
        seed_verdict_backups(&fake, backups);
        let app = TestApp::with(fake, Options::default());
        let walked = walk(
            &app,
            &format!("/api/v1/namespaces/team-a/catalogs/primary/points?limit={CONSOLE_PAGE_SIZE}"),
            64,
        )
        .await;
        assert_eq!(walked.items(), MAX_VIEW_POINTS);
        walked.report(&format!(
            "history: {MAX_VIEW_POINTS}-point view in {MAX_PAGES_PER_REQUEST} pages, \
             {backups} Backups in the namespace, limit={CONSOLE_PAGE_SIZE}"
        ));
    }
}

/// Discovery: 10,000 and 50,000 stored topic names paged at the console's
/// page size, and one search that matches nothing.
#[tokio::test]
#[ignore = "a measurement; run with --ignored --nocapture"]
async fn measure_topic_inventory_paging() {
    for topics in [10_000usize, 50_000] {
        let fake = FakeKube::new();
        seed_inventory(&fake, "td-scale", topics);
        let app = TestApp::with(fake, Options::default());
        let base = "/api/v1/namespaces/team-a/topic-discoveries/td-scale/topics";
        let walked = walk(&app, &format!("{base}?limit={CONSOLE_PAGE_SIZE}"), 1_000).await;
        assert_eq!(walked.items(), topics);
        walked.report(&format!(
            "discovery: {topics} topics at limit={CONSOLE_PAGE_SIZE}"
        ));
        let sparse = walk(&app, &format!("{base}?q=no-such-topic&limit=200"), 1_000).await;
        sparse.report(&format!(
            "discovery: a search matching none of {topics} topics"
        ));
    }
}
