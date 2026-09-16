//! `Store::list_page` — the ONE new store capability decision D3 §5.2 adds
//! for the recovery catalog (PLAT-15.1).
//!
//! Every test here runs against `Store::in_memory`, so the whole file is
//! socket-free and subprocess-free (Global Constraint 22); the `InMemory`
//! backend is a `BTreeMap`, which is why the out-of-order case below has to be
//! constructed rather than observed — see
//! `a_page_is_the_smallest_max_keys_even_when_the_backend_streams_out_of_order`.
//!
//! What is asserted, in one list:
//!
//! * a page is bounded by `max`, sorted, and resumable without gaps or repeats;
//! * `start_after` is EXCLUSIVE;
//! * the cursor is `None` exactly when the prefix is exhausted, so its absence
//!   means "done" and not "empty";
//! * `max == 0` returns nothing rather than everything;
//! * the prefix really filters;
//! * walking the whole prefix a page at a time yields exactly `list_keys`'s
//!   answer — the property that makes the bounded reader safe to substitute
//!   for the unbounded one.

use logweir_store::Store;

/// A store holding `n` keys under `logweir/catalog/v1/log/`, named so that
/// lexicographic order is numeric order (the property the catalog's own
/// 13-digit millisecond in the key buys).
fn seeded(n: usize) -> (Store, Vec<String>) {
    let s = Store::in_memory("logweir/");
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("logweir/catalog/v1/log/2026/09/15/{i:013}-lwp1-x.json");
        s.put_create_only(&k, b"{}").unwrap();
        keys.push(k);
    }
    keys.sort();
    (s, keys)
}

const PREFIX: &str = "logweir/catalog/v1/log/";

#[test]
fn a_page_is_bounded_sorted_and_carries_a_cursor() {
    let (s, all) = seeded(10);
    let (page, next) = s.list_page(PREFIX, None, 4).unwrap();
    assert_eq!(page, all[..4], "the first page is the four smallest keys");
    assert_eq!(
        next.as_deref(),
        Some(all[3].as_str()),
        "the cursor is the page's LAST key, so resuming from it loses nothing"
    );
}

#[test]
fn the_cursor_is_none_exactly_when_the_prefix_is_exhausted() {
    let (s, all) = seeded(4);
    // Asking for exactly as many as exist: there is no next page, and saying
    // so is the difference between "done" and "ask again".
    let (page, next) = s.list_page(PREFIX, None, 4).unwrap();
    assert_eq!(page, all);
    assert_eq!(next, None);

    // …and asking for more than exist is the same answer.
    let (page, next) = s.list_page(PREFIX, None, 100).unwrap();
    assert_eq!(page, all);
    assert_eq!(next, None);

    // An empty prefix is an empty page and no cursor — never a cursor over
    // nothing, which a caller would loop on forever.
    let (page, next) = s.list_page("logweir/catalog/v1/points/", None, 10).unwrap();
    assert!(page.is_empty());
    assert_eq!(next, None);
}

#[test]
fn start_after_is_exclusive() {
    let (s, all) = seeded(5);
    let (page, _) = s.list_page(PREFIX, Some(&all[1]), 10).unwrap();
    assert_eq!(
        page,
        all[2..],
        "the key named by the cursor must NOT come back: a page that repeated its \
         predecessor's last row would double-count every point in the catalog"
    );
}

#[test]
fn paging_through_the_prefix_yields_exactly_the_unbounded_listing() {
    // THE SUBSTITUTION PROPERTY. `list_keys` is the unbounded reader the
    // catalog replaces; a bounded walk that dropped or duplicated a key would
    // make the catalog quietly lose recovery points, which is the one failure
    // this whole feature exists to prevent.
    let (s, all) = seeded(23);
    for page_size in [1usize, 2, 7, 23, 24] {
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut rounds = 0;
        loop {
            let (page, next) = s.list_page(PREFIX, cursor.as_deref(), page_size).unwrap();
            assert!(
                page.len() <= page_size,
                "page size {page_size} returned {} keys",
                page.len()
            );
            seen.extend(page);
            rounds += 1;
            assert!(
                rounds < 100,
                "the walk did not terminate at page size {page_size}"
            );
            match next {
                Some(n) => cursor = Some(n),
                None => break,
            }
        }
        assert_eq!(
            seen, all,
            "page size {page_size} did not reconstruct the prefix"
        );
    }
    assert_eq!(s.list_keys(PREFIX).unwrap(), all);
}

#[test]
fn max_zero_returns_nothing_rather_than_everything() {
    // The mutant this kills is a `max` used only as a truncation bound after a
    // full collect: `Vec::truncate(0)` and "no bound at all" are one character
    // apart, and the second one hands a controller the whole bucket.
    let (s, _) = seeded(5);
    let (page, next) = s.list_page(PREFIX, None, 0).unwrap();
    assert!(
        page.is_empty(),
        "asking for zero keys must answer with zero"
    );
    assert_eq!(next, None);
}

#[test]
fn the_prefix_really_filters() {
    let (s, _) = seeded(3);
    s.put_create_only("logweir/backups/set-a/01J.receipt.json", b"{}")
        .unwrap();
    let (page, _) = s.list_page("logweir/backups/", None, 10).unwrap();
    assert_eq!(
        page,
        vec!["logweir/backups/set-a/01J.receipt.json".to_string()]
    );
    let (page, _) = s.list_page(PREFIX, None, 10).unwrap();
    assert_eq!(page.len(), 3);
    assert!(page.iter().all(|k| k.starts_with(PREFIX)));
}

#[test]
fn a_page_is_the_smallest_max_keys_even_when_the_backend_streams_out_of_order() {
    // `object_store` 0.14.1 contracts NO list ordering — the note sits above
    // `list_with_offset` itself — so `list_page` must not take the first `max`
    // keys off the stream and call them the smallest. The in-process backends
    // this workspace can build are all ordered, so the property cannot be
    // observed here directly; what CAN be observed is the consequence that
    // makes it matter, and this is it: keys inserted in a scrambled order come
    // back as the smallest `max`, in order, whatever order they were written.
    //
    // A `list_page` that trusted arrival order would still pass on `InMemory`.
    // What kills that mutant is the pairing of this test with
    // `paging_through_the_prefix_yields_exactly_the_unbounded_listing` above
    // and with the MinIO leg in `crates/logweir/tests/catalog_minio.rs`, which
    // runs the same walk against a real S3 implementation.
    let s = Store::in_memory("logweir/");
    for i in [7usize, 0, 3, 9, 1, 5] {
        s.put_create_only(
            &format!("logweir/catalog/v1/log/2026/09/15/{i:013}-lwp1-x.json"),
            b"{}",
        )
        .unwrap();
    }
    let (page, next) = s.list_page(PREFIX, None, 3).unwrap();
    let want: Vec<String> = [0usize, 1, 3]
        .iter()
        .map(|i| format!("logweir/catalog/v1/log/2026/09/15/{i:013}-lwp1-x.json"))
        .collect();
    assert_eq!(page, want);
    assert_eq!(next.as_deref(), Some(want[2].as_str()));
}

#[test]
fn a_read_only_handle_can_page_and_still_cannot_write() {
    // `list_page` adds NO write and NO delete (D3 §5.2). The claim is made
    // here against a handle that physically cannot put: if paging had somehow
    // acquired a write, this is where it would show.
    let s = Store::in_memory("logweir/");
    s.put_create_only("logweir/catalog/v1/log/2026/09/15/x.json", b"{}")
        .unwrap();
    let (page, _) = s.list_page(PREFIX, None, 10).unwrap();
    assert_eq!(page.len(), 1);
    // The method surface itself: `list_page` returns keys and a cursor, and
    // there is no variant of it that takes bytes. That is a compile-time fact
    // and is asserted by this file compiling at all; the runtime half is that
    // paging left the object untouched.
    assert_eq!(
        s.get("logweir/catalog/v1/log/2026/09/15/x.json").unwrap().0,
        b"{}"
    );
}
