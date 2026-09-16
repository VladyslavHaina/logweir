#![cfg(feature = "e2e")]
//! The explicit store options (D2 W2) against a REAL S3-compatible server.
//!
//! # Gating
//!
//! `#![cfg(feature = "e2e")]` is this repository's gating convention for a test
//! that needs a live backend (`crates/logweir-kafka/tests/live.rs`,
//! `crates/logweir/tests/backup_run.rs`). Under the default feature set this
//! file compiles to nothing, so `cargo test -p logweir-store` skips it cleanly
//! when the compose MinIO is absent, and
//! `crates/logweir/tests/no_network_in_unit_tests.rs` reads the same attribute
//! to decide that a dialling constructor here is expected.
//!
//! Run it with the stack up:
//!
//! ```text
//! just e2e-up
//! cargo test --locked -p logweir-store --features e2e --test minio_options
//! ```
//!
//! `LOGWEIR_TEST_S3_ENDPOINT` overrides the endpoint (default
//! `http://localhost:9000`, the compose stack's published MinIO port) and
//! `LOGWEIR_TEST_S3_BUCKET` the bucket (default `kafka-backups`, created by
//! the `minio-setup` one-shot).
//!
//! # What it adds over `tests/options.rs`
//!
//! `tests/options.rs` proves the DECISION (what a store is configured with)
//! and one behaviour that needs no server. This file proves the decision
//! survives a real request/response: that explicit credentials really are the
//! ones MinIO authenticates, that a wrong one is classified as a credential
//! problem and not as a denial, and that a missing bucket and a missing key
//! stay distinguishable — the three classifications a readiness check turns
//! into three different remedies.

use logweir_core::engine::StorageUrl;
use logweir_store::{Store, StoreError, StoreErrorClass, StoreOptions};
use std::time::Duration;

const ROOT_USER: &str = "minioadmin";
const ROOT_PASSWORD: &str = "minioadmin";

fn endpoint() -> String {
    std::env::var("LOGWEIR_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

fn bucket() -> String {
    std::env::var("LOGWEIR_TEST_S3_BUCKET").unwrap_or_else(|_| "kafka-backups".into())
}

/// The compose MinIO serves PLAINTEXT on 9000, so this location is
/// `InsecureHTTP`'s shape: an explicit `http://` endpoint and
/// `allow_http: true`. That combination is the ONLY one a destination can
/// reach it with (D2 R3), and it is exactly what the "explicitly configured
/// local HTTP" acceptance row describes.
fn location(bucket_name: &str, prefix: &str) -> StorageUrl {
    StorageUrl::S3 {
        bucket: bucket_name.to_string(),
        prefix: prefix.to_string(),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint()),
        // MinIO with a custom endpoint is path-style, which is also the only
        // addressing engine 0.21.0 can honour here (D2 G4).
        path_style: true,
        allow_http: true,
    }
}

fn opts(key: &str, secret: &str) -> StoreOptions {
    StoreOptions::static_keys(key, secret, None)
        .with_request_timeout(Duration::from_secs(10))
        .with_max_retries(1)
}

/// Every `AWS_*` name this file poisons.
const POISON: [(&str, &str); 5] = [
    ("AWS_ACCESS_KEY_ID", "AKIAPOISONPOISON1234"),
    ("AWS_SECRET_ACCESS_KEY", "poison-secret"),
    ("AWS_ENDPOINT_URL", "https://wrong-endpoint.invalid:9000"),
    ("AWS_REGION", "ap-southeast-2"),
    ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
];

struct Poison(Vec<(String, Option<String>)>);

impl Poison {
    fn set() -> Self {
        let saved = POISON
            .iter()
            .map(|(k, _)| ((*k).to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in POISON {
            std::env::set_var(k, v);
        }
        Self(saved)
    }
}

impl Drop for Poison {
    fn drop(&mut self) {
        for (k, v) in &self.0 {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

/// THE POINT OF THE FILE. With a controller-shaped `AWS_*` environment in this
/// process — wrong keys, wrong endpoint, wrong region, wrong addressing — a
/// store built from explicit options still reaches the right server with the
/// right principal. Nothing but the arguments decided anything.
#[test]
fn explicit_options_beat_a_poisoned_environment_against_a_real_server() {
    let _p = Poison::set();
    let store = Store::read_only_with(&location(&bucket(), ""), &opts(ROOT_USER, ROOT_PASSWORD))
        .expect("the client builds");
    // A list against a bucket that exists succeeds; with the poisoned endpoint
    // or the poisoned key it could not.
    store
        .list_keys("")
        .expect("listing the compose bucket with explicit root credentials succeeds");
}

/// A wrong credential is `InvalidCredentials`, NOT `AccessDenied`. The two
/// have different remedies — "fix the Secret" against "grant the action" — and
/// a check that showed the wrong one sends an operator after the wrong
/// problem.
#[test]
fn a_wrong_secret_is_classified_as_a_credential_problem() {
    let _p = Poison::set();
    let store = Store::read_only_with(&location(&bucket(), ""), &opts(ROOT_USER, "not-the-secret"))
        .expect("the client builds");
    let err = store
        .list_keys("")
        .expect_err("a wrong secret cannot list the bucket");
    let class = StoreErrorClass::classify(&StoreError::Io(err.to_string()));
    assert_eq!(
        class,
        StoreErrorClass::InvalidCredentials,
        "MinIO answered: {err}"
    );
}

/// An unknown access key id is also a credential problem.
#[test]
fn an_unknown_access_key_id_is_classified_as_a_credential_problem() {
    let _p = Poison::set();
    let store = Store::read_only_with(
        &location(&bucket(), ""),
        &opts("nobody-by-that-name", "whatever-secret"),
    )
    .expect("the client builds");
    let err = store
        .list_keys("")
        .expect_err("an unknown key id cannot list the bucket");
    assert_eq!(
        StoreErrorClass::classify(&StoreError::Io(err.to_string())),
        StoreErrorClass::InvalidCredentials,
        "MinIO answered: {err}"
    );
}

/// A missing BUCKET and a missing KEY stay distinguishable. `get` on a key
/// that is not there is `ObjectNotFound`; a bucket that is not there is
/// `BucketNotFound`, and the remedy differs.
#[test]
fn a_missing_key_and_a_missing_bucket_are_different_answers() {
    let _p = Poison::set();
    let store = Store::read_only_with(&location(&bucket(), ""), &opts(ROOT_USER, ROOT_PASSWORD))
        .expect("the client builds");
    let err = store
        .get("logweir/definitely-not-here-d2w2.json")
        .expect_err("the key does not exist");
    assert!(
        matches!(err, StoreError::NotFound(_)),
        "a genuine absence must be NotFound and never Io: {err}"
    );
    assert_eq!(
        StoreErrorClass::classify(&err),
        StoreErrorClass::ObjectNotFound
    );

    let absent_bucket = Store::read_only_with(
        &location("lw-d2w2-no-such-bucket", ""),
        &opts(ROOT_USER, ROOT_PASSWORD),
    )
    .expect("the client builds");
    let err = absent_bucket
        .list_keys("")
        .expect_err("the bucket does not exist");
    assert_eq!(
        StoreErrorClass::classify(&StoreError::Io(err.to_string())),
        StoreErrorClass::BucketNotFound,
        "MinIO answered: {err}"
    );
}

/// D-SEAMS S5 end to end: with `allow_http: false` — what a `TLS` destination
/// renders — the very same explicit credentials cannot reach a plaintext
/// MinIO, whatever `AWS_ALLOW_HTTP` says. The request is refused while it is
/// being built, so nothing leaves the process.
#[test]
fn allow_http_false_cannot_reach_a_plaintext_server() {
    let _p = Poison::set();
    std::env::set_var("AWS_ALLOW_HTTP", "true");
    let mut https_only = location(&bucket(), "");
    let StorageUrl::S3 { allow_http, .. } = &mut https_only else {
        unreachable!()
    };
    *allow_http = false;
    let store =
        Store::read_only_with(&https_only, &opts(ROOT_USER, ROOT_PASSWORD)).expect("it builds");
    let err = store
        .list_keys("")
        .expect_err("a TLS destination must not reach a plaintext endpoint");
    let text = err.to_string().to_ascii_lowercase();
    assert!(
        text.contains("builder error"),
        "the refusal must happen while the request is built, before the transport: {err}"
    );
    std::env::remove_var("AWS_ALLOW_HTTP");
}

/// Global Constraint 6 survives the new constructor against a real backend:
/// the handle `read_only_with` returns physically cannot put, and
/// `from_url_with` refuses a prefix that is not exactly `logweir/`.
#[test]
fn the_write_surface_is_not_widened_by_the_new_constructors() {
    let _p = Poison::set();
    let store = Store::read_only_with(&location(&bucket(), ""), &opts(ROOT_USER, ROOT_PASSWORD))
        .expect("the client builds");
    assert!(matches!(
        store.put_create_only("logweir/x.json", b"{}"),
        Err(StoreError::ReadOnly(_))
    ));
    assert!(
        Store::from_url_with(
            &location(&bucket(), "team-a"),
            &opts(ROOT_USER, ROOT_PASSWORD)
        )
        .is_err(),
        "a writable handle over a non-evidence prefix is still refused at construction"
    );
}
