//! **PROD-03.0 — `Store::get_bounded`**, the read schema dependency detection
//! fetches segments through: nothing over the cap is fetched, an object at the
//! cap is read whole, a missing one is `NotFound`, and a read that cannot
//! finish within its budget fails instead of waiting. Over the real
//! `object_store` in-memory backend, so the size check and the ranged read
//! are the backend's own answers.
use logweir_store::{Store, StoreError};

fn store_with(key: &str, len: usize) -> (Store, Vec<u8>) {
    let store = Store::in_memory("logweir/");
    let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    store.put_create_only(key, &bytes).unwrap();
    (store, bytes)
}

#[test]
fn an_object_at_the_cap_is_read_whole_and_one_byte_over_is_never_fetched() {
    let key = "logweir/seg/a.kbak";
    let (store, bytes) = store_with(key, 166);
    assert_eq!(
        store.get_bounded(key, 166, None).unwrap(),
        Some(bytes.clone())
    );
    assert_eq!(store.get_bounded(key, 1 << 20, None).unwrap(), Some(bytes));
    // NEGATIVE CONTROL: one byte under the object's size refuses it.
    assert_eq!(store.get_bounded(key, 165, None).unwrap(), None);
    assert_eq!(store.get_bounded(key, 0, None).unwrap(), None);
}

#[test]
fn an_empty_object_is_empty_and_a_missing_one_is_not_found() {
    let (store, _) = store_with("logweir/seg/empty.kbak", 0);
    assert_eq!(
        store
            .get_bounded("logweir/seg/empty.kbak", 0, None)
            .unwrap(),
        Some(Vec::new())
    );
    match store.get_bounded("logweir/seg/none.kbak", 10, None) {
        Err(StoreError::NotFound(k)) => assert_eq!(k, "logweir/seg/none.kbak"),
        other => panic!("{other:?}"),
    }
}

/// A budget of zero cannot be met: the read fails naming the budget, never
/// waits and never returns bytes. A generous budget reads.
#[test]
fn a_read_past_its_budget_fails_instead_of_waiting() {
    let key = "logweir/seg/b.kbak";
    let (store, bytes) = store_with(key, 64);
    match store.get_bounded(key, 64, Some(std::time::Duration::ZERO)) {
        Err(StoreError::Io(m)) => assert!(m.contains("remaining budget"), "{m}"),
        other => panic!("a zero budget must fail: {other:?}"),
    }
    assert_eq!(
        store
            .get_bounded(key, 64, Some(std::time::Duration::from_secs(30)))
            .unwrap(),
        Some(bytes)
    );
}
