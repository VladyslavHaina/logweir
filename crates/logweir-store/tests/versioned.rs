//! **FX-7 — reading an object BY VERSION.**
//!
//! A backup receipt taken on a versioned bucket pins the version id of the
//! manifest it attests, and the readers that check a point against the archive
//! compare the key's CURRENT version with that pin and read the pinned version
//! by its id. Two properties of `Store` carry that, and both are pinned here:
//!
//! 1. `Store::get_version_capped` returns the pinned bytes or fails — it NEVER hands
//!    back the current object as if it were the pinned one. `object_store`'s
//!    in-memory and local-filesystem backends ignore a version request and
//!    answer with the current object; a reader that believed that answer would
//!    "verify" exactly the rewrite the pin exists to catch.
//! 2. The test double of a versioned bucket (`Store::in_memory_versioned`)
//!    behaves as S3 does: every write is a new current version, and earlier
//!    versions stay readable by id.
//!
//! In process, no endpoint: the S3 half (the `?versionId=` query and the
//! `x-amz-version-id` header) is `object_store`'s own and is exercised live on
//! SeaweedFS (`docs/formats/backup-receipt.md`, FX-7).
use logweir_store::{Store, StoreError};

const KEY: &str = "logweir/archive/set-1/manifest.json";

/// A backend that cannot read by version answers a version read with ITS
/// CURRENT OBJECT and no version id. `get_version_capped` must refuse that answer.
#[test]
fn a_store_that_ignores_the_version_is_an_error_not_the_current_object() {
    let store = Store::in_memory("logweir/");
    store.put_create_only(KEY, b"current bytes").unwrap();
    match store.get_version_capped(KEY, "some-version", logweir_store::caps::SIGNED_DOCUMENT) {
        Err(StoreError::Backend(message)) => {
            assert!(
                message.contains("does not read objects by version"),
                "{message}"
            );
        }
        other => {
            panic!("a version read answered with the CURRENT object must be refused, got {other:?}")
        }
    }
}

/// A plain in-memory store reports no version id at all: the unversioned
/// bucket's answer, which is what makes a receipt there carry no pin.
#[test]
fn an_unversioned_store_reports_no_version_id() {
    let store = Store::in_memory("logweir/");
    store.put_create_only(KEY, b"bytes").unwrap();
    let (_, version) = store
        .get_capped(KEY, logweir_store::caps::SIGNED_DOCUMENT)
        .unwrap();
    assert_eq!(version, None);
}

/// The versioned double: a create is version 1, an overwrite version 2, the
/// current read reports version 2, and version 1 is still readable by its id.
#[test]
fn a_versioned_bucket_keeps_every_version_readable_by_id() {
    let (store, bucket) = Store::in_memory_versioned("logweir/");
    let first = store
        .put_create_only(KEY, b"first")
        .unwrap()
        .version_id
        .expect("a versioned bucket names the version a put created");
    let second = bucket.overwrite(KEY, b"second");
    assert_ne!(first, second, "an overwrite is a new version");

    let (bytes, current) = store
        .get_capped(KEY, logweir_store::caps::SIGNED_DOCUMENT)
        .unwrap();
    assert_eq!(bytes, b"second");
    assert_eq!(current.as_deref(), Some(second.as_str()));

    let (old, answered) = store
        .get_version_capped(KEY, &first, logweir_store::caps::SIGNED_DOCUMENT)
        .unwrap();
    assert_eq!(old, b"first", "the earlier version is retained");
    assert_eq!(answered.as_deref(), Some(first.as_str()));
    assert_eq!(bucket.versions(KEY), vec![first, second]);
}

/// An id the key never had is `NotFound` — never the current bytes.
#[test]
fn an_unknown_version_id_is_not_found() {
    let (store, _bucket) = Store::in_memory_versioned("logweir/");
    store.put_create_only(KEY, b"first").unwrap();
    assert!(matches!(
        store.get_version_capped(KEY, "no-such-version", logweir_store::caps::SIGNED_DOCUMENT),
        Err(StoreError::NotFound(_))
    ));
}
