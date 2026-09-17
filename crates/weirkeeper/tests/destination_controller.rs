//! `BackupDestination`: the reconciler's verdict, the resolver's complete
//! `AWS_*` set, the frozen snapshot, the `ControllerIdentity` store cache and
//! the G14 retention guard — decision D2 §3.3–§3.7, §3.10, §3.13.
//!
//! # THE PROCESS ENVIRONMENT OF THIS BINARY IS POISONED ON PURPOSE
//!
//! [`poison_the_environment`] sets `AWS_ALLOW_HTTP=true`, `AWS_ENDPOINT_URL`,
//! `AWS_REGION` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true` in this test
//! process — the exact shape of defect **SEC-ENVHTTP**, in which a controller
//! started with those variables forwards them into every runner Job and the
//! engine's `AmazonS3Builder::from_env()` honours them, enabling plaintext HTTP
//! for a plan whose `allow_http` is `false`.
//!
//! Every rendering assertion below then runs WITH that environment set, so
//! "the controller environment never reaches a rendered set" is measured rather
//! than asserted from the absence of a `std::env::var` call. The mutation races
//! nothing: `weirkeeper::destination` reads no environment variable at all, so
//! no other row in this binary can observe the change.
//!
//! # The six planted mutants (D2 §12's "a guard without a mutant is not a
//! guard")
//!
//! Each was applied to the shipped source, measured, and reverted. The row that
//! dies is named beside each.
//!
//! The unplanted tree is **35 passed / 0 failed** here and **48 / 0** in
//! `tests/retention.rs`.
//!
//! | # | Mutant | Measured | Dies in |
//! |---|---|---|---|
//! | M1 | `job_env` appends `controllers::backup::archive_addressing_env()` | 32 / 3 | [`the_rendered_environment_never_carries_a_process_value`], [`two_destinations_render_distinct_complete_environments`], [`the_resolver_names_no_environment_read`] |
//! | M2 | `AWS_ALLOW_HTTP` computed from `!addressing.is_path_style()` | 33 / 2 | [`allow_http_comes_from_transport_and_never_from_addressing`], [`two_destinations_render_distinct_complete_environments`] |
//! | M3 | `secret_grant` drops its object-name check | 34 / 1 | [`a_cross_namespace_secret_reference_is_refused`] |
//! | M4 | `StoreCache::get_or_build` builds outside `spawn_blocking` | 47 / 1 | `retention::no_store_call_is_made_outside_spawn_blocking`, naming `evidence_store.rs:339` |
//! | M5 | `StoreCache::record` drops the eviction loop | 34 / 1 | [`the_store_cache_is_bounded`] |
//! | M6 | `retention_scope` returns `GlobalHandleApplies` unconditionally | 33 / 2 | [`retention_is_withheld_when_the_schedule_names_another_bucket`], [`a_destination_backed_schedule_gets_no_global_report`] |

use std::collections::BTreeSet;
use std::sync::{Arc, Once};

use logweir_core::check_contract::CheckCode;
use logweir_core::destination::{Addressing, DestinationLocation, TransportSecurity};
use logweir_core::engine::StorageUrl;
use serde_json::Value;

use weirkeeper::check::policy::{EvidencePolicy, IdentityLocation, Policy};
use weirkeeper::controllers::backup_destination::{reconcile_destination, status_for};
use weirkeeper::crds::backup_destination::BackupDestination;
use weirkeeper::destination::{
    self, retention_scope, CaObservation, DestinationRole, ResolvedDestination, ResolvedGrant,
    RetentionScope, AWS_ALLOW_HTTP_ENV, AWS_ENDPOINT_URL_ENV, AWS_METADATA_ENDPOINT_ENV,
    AWS_REGION_ENV, AWS_VIRTUAL_HOSTED_ENV, DESTINATION_CONDITION_REASONS,
};
use weirkeeper::evidence_store::{CacheKey, StoreCache, MAX_CACHED_STORES};
use weirkeeper::testing::{mock_client_recording, mock_client_recording_bodies, Route};

const NS: &str = "team-a";
const OTHER_NS: &str = "team-b";
const UID_A: &str = "11111111-0000-4000-8000-000000000001";
const UID_B: &str = "22222222-0000-4000-8000-000000000002";

/// The value no status, body, environment or snapshot may ever carry.
///
/// It is never PUT anywhere by these tests: it stands for the bytes inside the
/// Secret the references name, and every scan below asserts its absence, which
/// is the shape `no_status_or_configmap_body_contains_fixture_secret` takes on
/// the live paths.
const FIXTURE_SECRET_VALUE: &str = "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY";

// ===========================================================================
// The poisoned environment
// ===========================================================================

/// Set the four variables defect SEC-ENVHTTP is about, once per process.
///
/// `set_var` is `unsafe` from the 2024 edition; this crate is 2021. The
/// mutation is safe here for a reason stronger than the edition: NOTHING under
/// test reads these variables. `weirkeeper::destination` calls `std::env::var`
/// zero times — [`the_resolver_names_no_environment_read`] is the source-level
/// half of that claim, and every rendering row below is the behavioural half.
fn poison_the_environment() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("AWS_ALLOW_HTTP", "true");
        std::env::set_var("AWS_ENDPOINT_URL", "http://attacker.invalid:9000");
        std::env::set_var("AWS_REGION", "eu-west-9");
        std::env::set_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true");
    });
}

// ===========================================================================
// Fixtures
// ===========================================================================
//
// EVERY FIXTURE IS A `serde_json::Value` A TEST MUTATES BY PATH, never a string
// a test rewrites by substring. A `.replace("\"PathStyle\"", …)` that silently
// matches nothing is a test that asserts the DEFAULT fixture's behaviour while
// its name claims otherwise — the shape a reviewer cannot see in a diff.

/// A `BackupDestination` from a JSON value.
fn build(value: Value) -> BackupDestination {
    serde_json::from_value(value)
        .unwrap_or_else(|e| panic!("the fixture is a BackupDestination: {e}"))
}

/// The `status` an object whose reconciler already said `Valid=True` carries.
fn valid_status(generation: i64) -> Value {
    serde_json::json!({
        "observedGeneration": generation,
        "reason": "Valid",
        "conditions": [{
            "type": "Valid", "status": "True", "reason": "Valid",
            "observedGeneration": generation
        }]
    })
}

/// `dest-a`: MinIO over TLS with a private CA, path-style, separate read and
/// write grants, `evidenceRead` reusing the read-only one.
fn dest_a_value() -> Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": "dest-a", "namespace": NS, "uid": UID_A, "generation": 3},
        "spec": {
            "description": "Production archive (MinIO, private CA)",
            "storage": {
                "provider": "S3", "bucket": "lw-a", "prefix": "team-a/prod",
                "region": "us-east-1", "endpoint": "https://minio-a.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {
                "security": "TLS",
                "caBundle": {"configMapName": "minio-a-ca", "key": "ca.crt"}
            },
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-a-writer",
                    "accessKeyIdKey": "access-key-id",
                    "secretAccessKeyKey": "secret-access-key"
                }},
                "archiveRead": {"mode": "SecretKeys", "secret": {
                    "name": "lw-a-reader",
                    "accessKeyIdKey": "access-key-id",
                    "secretAccessKeyKey": "secret-access-key"
                }},
                "evidenceRead": {"mode": "ArchiveReadGrant"}
            }
        },
        "status": valid_status(3)
    })
}

/// `dest-b`: a lab MinIO over EXPLICIT plaintext HTTP, a different bucket, a
/// different credential with a session token, and no CA.
fn dest_b_value() -> Value {
    serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": "dest-b", "namespace": NS, "uid": UID_B, "generation": 1},
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-b", "prefix": "",
                "endpoint": "http://minio-b.storage.svc:9000", "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-b-writer",
                    "accessKeyIdKey": "id",
                    "secretAccessKeyKey": "key",
                    "sessionTokenKey": "token"
                }}
            }
        },
        "status": valid_status(1)
    })
}

/// `dest-a` with no `status` at all — what the reconciler sees on a first pass.
fn dest_a_unreconciled() -> Value {
    let mut v = dest_a_value();
    v.as_object_mut().expect("an object").remove("status");
    v
}

fn dest_a() -> BackupDestination {
    build(dest_a_value())
}

fn dest_b() -> BackupDestination {
    build(dest_b_value())
}

/// A policy allowing the controller's own identity at exactly `dest-b`'s
/// location.
fn policy_allowing_b() -> Policy {
    policy_for_bucket("lw-b")
}

fn policy_for_bucket(bucket: &str) -> Policy {
    Policy {
        evidence: EvidencePolicy {
            controller_identity_locations: vec![IdentityLocation {
                endpoint: "http://minio-b.storage.svc:9000".to_string(),
                region: String::new(),
                bucket: bucket.to_string(),
            }],
        },
        ..Policy::defaults()
    }
}

/// A DER-shaped certificate: a SEQUENCE (`0x30`) with a short body, base64'd
/// the way a PEM bundle carries it. Not a real certificate, and it does not
/// need to be: what this file asserts is that the controller can tell a
/// certificate-shaped block from a private key, a truncated block and a `.der`
/// file — not that a chain validates.
fn ca_pem(marker: u8) -> String {
    // 0x30 0x06 <six bytes>: a SEQUENCE of length 6. Nine bytes total, a
    // multiple of three, so the base64 has no padding to argue about.
    let der = [0x30u8, 0x06, marker, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
    format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        base64_encode(&der)
    )
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        let idx = [
            (n >> 18) & 0x3f,
            (n >> 12) & 0x3f,
            (n >> 6) & 0x3f,
            n & 0x3f,
        ];
        for (i, v) in idx.iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[*v as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The route table for a reconcile that reads a CA `ConfigMap` and patches a
/// status.
///
/// THE PATCH ANSWER IS A WHOLE OBJECT, because `Api::patch_status` deserialises
/// the response into the kind. A `{}` body makes the client fail with a serde
/// error that reads like a reconciler bug.
fn routes(ca: Option<&str>) -> Vec<Route> {
    vec![
        Route {
            method: "PATCH",
            path_suffix: "/status",
            status: 200,
            body: dest_a_value().to_string(),
        },
        match ca {
            Some(body) => Route {
                method: "GET",
                path_suffix: "/configmaps/minio-a-ca",
                status: 200,
                body: body.to_string(),
            },
            None => Route {
                method: "GET",
                path_suffix: "/configmaps/minio-a-ca",
                status: 404,
                body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","code":404,"reason":"NotFound"}"#
                    .to_string(),
            },
        },
    ]
}

fn configmap_body(key: &str, value: &str) -> String {
    serde_json::json!({
        "kind": "ConfigMap",
        "apiVersion": "v1",
        "metadata": {"name": "minio-a-ca", "namespace": NS},
        "data": {key: value},
    })
    .to_string()
}

// ===========================================================================
// §3.3 — the reconciler's verdict
// ===========================================================================

/// The happy path, end to end over the double: one CA `GET`, one `/status`
/// PATCH, and every published field.
#[tokio::test]
async fn a_valid_destination_publishes_its_url_digest_and_ca_digest() {
    let pem = ca_pem(0xAA);
    let (client, recorder, bodies) =
        mock_client_recording_bodies(routes(Some(&configmap_body("ca.crt", &pem))));
    let dest = build(dest_a_unreconciled());

    let verdict = reconcile_destination(&dest, &client)
        .await
        .expect("the reconcile completes");

    assert!(verdict.valid, "every check passes: {verdict:?}");
    assert_eq!(verdict.reason, "Valid");
    assert_eq!(verdict.canonical_url, "s3://lw-a/team-a/prod");
    assert_eq!(
        verdict.location_digest,
        DestinationLocation {
            provider: logweir_core::destination::StorageProvider::S3,
            bucket: "lw-a".to_string(),
            prefix: "team-a/prod".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://minio-a.storage.svc:9000".to_string()),
            addressing: Addressing::PathStyle,
            transport: TransportSecurity::Tls,
        }
        .location_digest(),
        "the published digest is `logweir_core`'s and not a second computation"
    );
    assert_eq!(
        verdict.ca_bundle_sha256.as_deref(),
        Some(logweir_core::ids::sha256_prefixed(pem.as_bytes()).as_str()),
        "the CA digest is over the bytes read from the ConfigMap"
    );

    // EXACTLY TWO CALLS: the CA read and the status patch. A destination is
    // exercised by nothing else — no bucket listing, no endpoint dial.
    let seen = recorder.lock().expect("the recorder").clone();
    assert_eq!(
        seen.len(),
        2,
        "a reconcile reads the CA ConfigMap and patches /status, and does nothing else: {seen:?}"
    );
    assert!(seen[0].uri.ends_with("/configmaps/minio-a-ca"));
    assert!(seen[1].uri.contains("/backupdestinations/dest-a/status"));

    // AND ONLY /status. A reconciler in this directory patches a status and
    // never a spec.
    let sent = bodies.lock().expect("the body recorder").clone();
    for body in sent.iter().filter(|b| b.method == "PATCH") {
        let path = body.uri.split('?').next().unwrap_or(&body.uri);
        assert!(
            path.ends_with("/status"),
            "every PATCH this reconciler sends targets the status subresource: {body:?}"
        );
        let parsed: Value = serde_json::from_str(&body.body).expect("the patch is JSON");
        assert_eq!(
            parsed
                .as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>()),
            Some(vec!["status".to_string()]),
            "the patch body carries `status` and nothing else: {parsed}"
        );
    }
}

/// **ENGINE-PATHSTYLE.** `VirtualHosted` with a custom endpoint is a setting
/// engine 0.21.0 cannot honour, so it is REFUSED rather than served as
/// path-style behind the operator's back.
#[test]
fn virtual_hosted_with_a_custom_endpoint_is_refused() {
    let mut value = dest_a_unreconciled();
    value["spec"]["storage"]["addressing"] = serde_json::json!("VirtualHosted");
    let verdict = destination::evaluate(
        &build(value),
        &CaObservation::Present(ca_pem(1).into_bytes()),
    );
    assert!(!verdict.valid);
    assert_eq!(
        verdict.reason,
        CheckCode::AddressingUnsupportedByEngine.as_str()
    );
    assert!(
        verdict.message.contains("path-style") && verdict.message.contains("endpoint"),
        "the refusal names the engine behaviour rather than saying `invalid`: {}",
        verdict.message
    );
}

/// …and `VirtualHosted` with NO endpoint is AWS S3's own default and is fine.
/// The refusal above is about the ENGINE, not about virtual-hosted addressing.
#[test]
fn virtual_hosted_without_an_endpoint_is_valid() {
    let mut value = dest_b_value();
    value["spec"]["storage"]["addressing"] = serde_json::json!("VirtualHosted");
    value["spec"]["storage"]["region"] = serde_json::json!("us-east-1");
    value["spec"]["storage"]
        .as_object_mut()
        .expect("an object")
        .remove("endpoint");
    value["spec"]["transport"]["security"] = serde_json::json!("TLS");
    let verdict = destination::evaluate(&build(value), &CaObservation::NotDeclared);
    assert!(verdict.valid, "{verdict:?}");
    assert_eq!(verdict.canonical_url, "s3://lw-b");
}

/// The four CA-bundle refusals, each with the code D2 §3.3 names.
#[test]
fn the_ca_bundle_refusal_table() {
    let dest = build(dest_a_unreconciled());
    let oversize = {
        let one = ca_pem(0x01);
        let mut s = String::new();
        while s.len() <= 64 * 1024 {
            s.push_str(&one);
        }
        s
    };
    let cases: [(CaObservation, CheckCode, &str); 5] = [
        (
            CaObservation::NotFound,
            CheckCode::CaBundleNotFound,
            "does not exist",
        ),
        (
            CaObservation::KeyMissing,
            CheckCode::CaBundleKeyMissing,
            "no key",
        ),
        (
            CaObservation::Present(oversize.into_bytes()),
            CheckCode::CaBundleTooLarge,
            "at most",
        ),
        (
            CaObservation::Present(
                b"-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n".to_vec(),
            ),
            CheckCode::CaBundleInvalid,
            "no parseable PEM certificate",
        ),
        (
            // A CERTIFICATE block with a truncated body: base64 that is not a
            // multiple of four. This is the paste an operator actually makes.
            CaObservation::Present(
                b"-----BEGIN CERTIFICATE-----\nMIIB\nQQ\n-----END CERTIFICATE-----\n".to_vec(),
            ),
            CheckCode::CaBundleInvalid,
            "no parseable PEM certificate",
        ),
    ];
    for (observation, code, fragment) in cases {
        let verdict = destination::evaluate(&dest, &observation);
        assert!(!verdict.valid, "{observation:?} must refuse");
        assert_eq!(verdict.reason, code.as_str(), "for {observation:?}");
        assert!(
            verdict.message.contains(fragment),
            "the message for {code:?} names what to do; got {}",
            verdict.message
        );
        assert_eq!(
            verdict.ca_bundle_sha256, None,
            "there were no usable bytes to digest"
        );
        // …AND THE LOCATION IS STILL PUBLISHED. An operator debugging a CA
        // problem still needs to see which bucket they were pointing at.
        assert_eq!(verdict.canonical_url, "s3://lw-a/team-a/prod");
    }
}

/// An absent CA `ConfigMap` reaches the status as `CaBundleNotFound` through
/// the real API path, not only through [`destination::evaluate`].
#[tokio::test]
async fn an_absent_ca_config_map_is_reported_as_ca_bundle_not_found() {
    let (client, _rec, bodies) = mock_client_recording_bodies(routes(None));
    let verdict = reconcile_destination(&build(dest_a_unreconciled()), &client)
        .await
        .expect("a 404 on the ConfigMap is a verdict, not an error");
    assert_eq!(verdict.reason, CheckCode::CaBundleNotFound.as_str());

    let sent = bodies.lock().expect("bodies").clone();
    let patch = sent
        .iter()
        .find(|b| b.method == "PATCH")
        .expect("the verdict was written");
    let parsed: Value = serde_json::from_str(&patch.body).expect("JSON");
    assert_eq!(parsed["status"]["reason"], "CaBundleNotFound");
    assert_eq!(
        parsed["status"]["conditions"][0]["status"], "False",
        "the Valid condition carries the verdict, and `status.reason` mirrors it"
    );
    assert_eq!(
        parsed["status"]["canonicalUrl"], "s3://lw-a/team-a/prod",
        "canonicalUrl and locationDigest are written on EVERY verdict (D2 §3.3)"
    );
    assert!(parsed["status"]["locationDigest"].is_string());
    assert!(
        parsed["status"]["caBundleSha256"].is_null(),
        "no bytes were read, so no digest is published: {parsed}"
    );
}

/// An object the API server admitted but this controller's own rules refuse —
/// an older CRD revision, or a hand-applied one.
///
/// R6 is the case: `prefix: logweir/x` is Logweir's own evidence root. A CRD
/// installed before R6 existed would accept it, and every run under it would
/// write engine segments into the directory the receipts live in.
#[test]
fn an_object_admitted_by_an_older_crd_is_still_refused() {
    let mut value = dest_a_unreconciled();
    value["spec"]["storage"]["prefix"] = serde_json::json!("logweir/x");
    let verdict = destination::evaluate(&build(value), &CaObservation::NotDeclared);
    assert!(!verdict.valid);
    assert_eq!(verdict.reason, CheckCode::DestinationNotValid.as_str());
    assert!(
        verdict.message.contains("R6") && verdict.message.contains("logweir"),
        "the message names the rule and the reserved root: {}",
        verdict.message
    );
}

/// A verdict that changed nothing sends NO patch — erratum E11(d).
#[tokio::test]
async fn an_unchanged_verdict_sends_no_patch() {
    let pem = ca_pem(0x5A);
    let dest = build(dest_a_unreconciled());
    let verdict = destination::evaluate(&dest, &CaObservation::Present(pem.clone().into_bytes()));
    let now = chrono::Utc::now();
    let status = status_for(&dest, &verdict, now);
    let with_status: BackupDestination = serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": dest.metadata,
        "spec": dest.spec,
        "status": status,
    }))
    .expect("the object round-trips");

    let (client, recorder) = mock_client_recording(routes(Some(&configmap_body("ca.crt", &pem))));
    reconcile_destination(&with_status, &client)
        .await
        .expect("the reconcile completes");

    let seen = recorder.lock().expect("recorder").clone();
    assert!(
        seen.iter().all(|r| r.method != "PATCH"),
        "the computed status equals the stored one, so nothing is sent — a patch that changed \
         only the clock is what spun the roster reconciler at 133 reconciles a second: {seen:?}"
    );
}

/// The condition reasons are a CLOSED set, and every one of them is a
/// [`CheckCode`].
#[test]
fn the_condition_reasons_are_closed_and_are_check_codes() {
    for reason in DESTINATION_CONDITION_REASONS {
        assert!(
            CheckCode::parse(reason).is_some(),
            "`{reason}` is written into a condition and must be a CheckCode, so a UI has text \
             for it and a check result and a status agree"
        );
    }
    // AND EVERY ONE IS REACHABLE. A table nothing produces is a table that
    // records a closed set while the code has another.
    let dest = dest_a();
    let produced: BTreeSet<&str> = [
        destination::evaluate(&dest, &CaObservation::Present(ca_pem(2).into_bytes())).reason,
        destination::evaluate(&dest, &CaObservation::NotFound).reason,
        destination::evaluate(&dest, &CaObservation::KeyMissing).reason,
        destination::evaluate(&dest, &CaObservation::Present(vec![b'x'; 70_000])).reason,
        destination::evaluate(&dest, &CaObservation::Present(b"nothing".to_vec())).reason,
        destination::evaluate(
            &build({
                let mut v = dest_a_unreconciled();
                v["spec"]["storage"]["addressing"] = serde_json::json!("VirtualHosted");
                v
            }),
            &CaObservation::Present(ca_pem(3).into_bytes()),
        )
        .reason,
        destination::evaluate(
            &build({
                let mut v = dest_a_unreconciled();
                v["spec"]["storage"]["prefix"] = serde_json::json!("..");
                v
            }),
            &CaObservation::NotDeclared,
        )
        .reason,
    ]
    .into_iter()
    .collect();
    let declared: BTreeSet<&str> = DESTINATION_CONDITION_REASONS.into_iter().collect();
    assert_eq!(
        produced, declared,
        "every declared reason is produced by a fixture and every produced reason is declared"
    );
}

// ===========================================================================
// §3.4 — the resolver
// ===========================================================================

/// Role defaulting, in one table.
#[test]
fn the_role_defaulting_table() {
    let policy = Policy::defaults();
    let a = dest_a();
    let writer = ResolvedGrant::SecretKeys {
        secret: "lw-a-writer".to_string(),
        access_key_id_key: "access-key-id".to_string(),
        secret_access_key_key: "secret-access-key".to_string(),
        session_token_key: None,
    };
    let reader = ResolvedGrant::SecretKeys {
        secret: "lw-a-reader".to_string(),
        access_key_id_key: "access-key-id".to_string(),
        secret_access_key_key: "secret-access-key".to_string(),
        session_token_key: None,
    };
    for (role, want) in [
        (DestinationRole::ArchiveWrite, &writer),
        // Declared explicitly on dest-a.
        (DestinationRole::ArchiveRead, &reader),
        // NOT declared: falls back to archiveWrite, never to "anything".
        (DestinationRole::EvidenceWrite, &writer),
        // `ArchiveReadGrant` resolves to the EXPLICIT read-only grant (R9).
        (DestinationRole::EvidenceRead, &reader),
    ] {
        let resolved = destination::resolve(&a, role, &policy)
            .unwrap_or_else(|e| panic!("{role:?} resolves: {e}"));
        assert_eq!(&resolved.grant, want, "role {role:?}");
        assert_eq!(resolved.role, role);
        assert_eq!(resolved.uid, UID_A);
        assert_eq!(resolved.generation, 3);
        assert_eq!(resolved.namespace, NS);
    }

    // `evidenceRead` ABSENT is a defined answer and never a wider grant.
    let b = dest_b();
    assert_eq!(
        destination::resolve(&b, DestinationRole::EvidenceRead, &policy)
            .expect("resolves")
            .grant,
        ResolvedGrant::NotConfigured,
        "an absent evidenceRead means verification is NotAttempted and says so — it does NOT \
         fall back to the write grant"
    );
}

/// `evidenceRead: ArchiveReadGrant` without an explicit `archiveRead` is
/// refused HERE as well as by CEL rule R9 — a write grant is never reused for
/// verification.
#[test]
fn archive_read_grant_without_an_explicit_read_grant_is_refused() {
    let mut value = dest_a_value();
    value["spec"]["access"]
        .as_object_mut()
        .expect("an object")
        .remove("archiveRead");
    let refusal = destination::resolve(
        &build(value),
        DestinationRole::EvidenceRead,
        &Policy::defaults(),
    )
    .map(|r| r.grant)
    .expect_err("R9 is re-evaluated by the resolver");
    assert_eq!(refusal.code, CheckCode::DestinationRoleNotConfigured);
    assert!(
        !refusal.is_hold(),
        "a missing grant is a decision, not a race"
    );
    assert!(refusal.message.contains("R9"), "{}", refusal.message);
}

/// A destination whose own reconciler has not said `Valid=True` yet is a HOLD:
/// the caller requeues and creates nothing.
#[test]
fn a_destination_without_a_current_valid_condition_is_a_hold() {
    let policy = Policy::defaults();
    let stale = {
        let mut v = dest_a_value();
        v["status"] = valid_status(2);
        v
    };
    let refused = {
        let mut v = dest_a_value();
        v["status"]["conditions"][0]["status"] = serde_json::json!("False");
        v
    };
    for value in [
        // No status at all.
        dest_a_unreconciled(),
        // `Valid=True`, but computed from a PREVIOUS generation: an edit landed
        // and the reconciler has not caught up. A stale True is not a True.
        stale,
        // Explicitly False.
        refused,
    ] {
        let refusal = destination::resolve(&build(value), DestinationRole::ArchiveWrite, &policy)
            .map(|r| r.grant)
            .expect_err("no current Valid=True");
        assert_eq!(refusal.code, CheckCode::DestinationNotValid);
        assert!(
            refusal.is_hold(),
            "a destination that is not yet valid is a race an operator resolves; failing a run \
             terminally on it would make the run unretryable"
        );
    }
}

/// The engine refusal and the location rules are checked BEFORE the object's
/// own verdict, so a destination whose reconciler has not run yet still reports
/// the real problem.
#[test]
fn the_real_problem_is_reported_before_not_yet_valid() {
    let mut value = dest_a_unreconciled();
    value["spec"]["storage"]["addressing"] = serde_json::json!("VirtualHosted");
    let refusal = destination::resolve(
        &build(value),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .map(|r| r.grant)
    .expect_err("VirtualHosted with an endpoint");
    assert_eq!(refusal.code, CheckCode::AddressingUnsupportedByEngine);
}

/// **M3.** A `<namespace>/<name>` grant reference is refused, naming the rule.
///
/// A `S3SecretKeysRef` has no `namespace` field, so a cross-namespace reference
/// cannot be EXPRESSED — but a name-shaped attempt at one would reach the
/// kubelet, which reports it as "Secret not found" and sends the operator
/// looking for the wrong thing. The controller reads no Secret (D2 §3.8 option
/// B, rejected), so this is a check on the SPELLING and it says so.
#[test]
fn a_cross_namespace_secret_reference_is_refused() {
    for spelling in [
        "team-b/lw-a-writer",
        "lw-a-writer/",
        "/lw-a-writer",
        "LW-A-Writer",
    ] {
        let mut value = dest_a_value();
        value["spec"]["access"]["archiveWrite"]["secret"]["name"] = serde_json::json!(spelling);
        let refusal = destination::resolve(
            &build(value),
            DestinationRole::ArchiveWrite,
            &Policy::defaults(),
        )
        .map(|r| r.grant);
        let refusal = match refusal {
            Err(e) => e,
            Ok(grant) => panic!("`{spelling}` must be refused; it resolved to {grant:?}"),
        };
        assert_eq!(
            refusal.code,
            CheckCode::DestinationRoleNotConfigured,
            "for `{spelling}`"
        );
        assert!(
            refusal.message.contains("cross-namespace") || refusal.message.contains("DNS-1123"),
            "the refusal names the rule rather than the symptom: {}",
            refusal.message
        );
    }
}

/// A Job placed in another namespace is refused: a bare reference is resolved
/// by the kubelet in the POD's namespace, so running elsewhere would project
/// whatever objects happen to carry those names there.
#[test]
fn a_job_in_another_namespace_is_refused() {
    let resolved = destination::resolve(
        &dest_a(),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .expect("resolves");
    assert!(resolved.check_job_namespace(NS).is_ok());
    let refusal = resolved
        .check_job_namespace(OTHER_NS)
        .expect_err("another namespace");
    assert_eq!(refusal.code, CheckCode::ExecutionContextConflict);
    assert!(refusal.message.contains(OTHER_NS) && refusal.message.contains(NS));
}

/// `ControllerIdentity` needs the administrator's allowlist, and the allowlist
/// matches on endpoint, region AND bucket.
#[test]
fn controller_identity_needs_the_policy_allowlist() {
    let mut value = dest_b_value();
    value["spec"]["access"]["evidenceRead"] = serde_json::json!({"mode": "ControllerIdentity"});
    let dest = build(value);

    // Default policy: EMPTY allowlist, which is the closed direction.
    let refusal = destination::resolve(&dest, DestinationRole::EvidenceRead, &Policy::defaults())
        .expect_err("an unlisted location");
    assert_eq!(refusal.code, CheckCode::ControllerIdentityNotAllowlisted);
    assert!(
        refusal.message.contains("administrator"),
        "the operator is told who can change it: {}",
        refusal.message
    );

    assert_eq!(
        destination::resolve(&dest, DestinationRole::EvidenceRead, &policy_allowing_b())
            .expect("the listed location resolves")
            .grant,
        ResolvedGrant::ControllerIdentity
    );

    // A BUCKET NAME IS NOT AN IDENTITY. The same bucket at a different endpoint
    // is a different store, and an allowlist that matched on the bucket alone
    // would let an operator point the controller's principal at any endpoint
    // they can name.
    let mut elsewhere = policy_allowing_b();
    elsewhere.evidence.controller_identity_locations[0].endpoint =
        "http://minio-c.storage.svc:9000".to_string();
    assert_eq!(
        destination::resolve(&dest, DestinationRole::EvidenceRead, &elsewhere)
            .expect_err("a different endpoint is a different location")
            .code,
        CheckCode::ControllerIdentityNotAllowlisted
    );
}

/// `DestinationNotFound` is a hold and is namespace-local.
#[tokio::test]
async fn an_absent_destination_is_a_namespace_local_hold() {
    let client = weirkeeper::testing::mock_client(vec![Route {
        method: "GET",
        path_suffix: "/namespaces/team-a/backupdestinations/dest-a",
        status: 404,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","code":404,"reason":"NotFound"}"#.to_string(),
    }]);
    let err = destination::resolve_ref(
        &client,
        NS,
        "dest-a",
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .await
    .expect_err("no such destination");
    match err {
        destination::ResolveError::Refused(r) => {
            assert_eq!(r.code, CheckCode::DestinationNotFound);
            assert!(r.is_hold());
            assert!(
                r.message.contains("namespace-local"),
                "the message says why another namespace was not searched: {}",
                r.message
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// `resolve_ref` reads the destination in the namespace it was given AND
/// NOWHERE ELSE — the double panics on any other route.
#[tokio::test]
async fn resolve_ref_reads_only_its_own_namespace() {
    let pem = ca_pem(0x7E);
    let client = weirkeeper::testing::mock_client(vec![
        Route {
            method: "GET",
            path_suffix: "/namespaces/team-a/backupdestinations/dest-a",
            status: 200,
            body: dest_a_value().to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/namespaces/team-a/configmaps/minio-a-ca",
            status: 200,
            body: configmap_body("ca.crt", &pem),
        },
    ]);
    let resolved = destination::resolve_ref(
        &client,
        NS,
        "dest-a",
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .await
    .expect("resolves");
    assert_eq!(resolved.namespace, NS);
    assert_eq!(
        resolved.ca_sha256.as_deref(),
        Some(logweir_core::ids::sha256_prefixed(pem.as_bytes()).as_str())
    );
    assert_eq!(resolved.ca_pem.as_deref(), Some(pem.as_bytes()));
}

// ===========================================================================
// §3.5 / §3.13 — the rendered environment
// ===========================================================================

/// **THE TWO-DESTINATION ACCEPTANCE, at the resolver level** (D2 §3.13).
///
/// Two destinations in one namespace with different endpoints, transports and
/// credentials yield COMPLETE and DISTINCT `AWS_*` sets, and neither carries a
/// value from the process environment — which this binary has deliberately
/// poisoned.
#[test]
fn two_destinations_render_distinct_complete_environments() {
    poison_the_environment();
    let policy = Policy::defaults();
    let a = destination::resolve(&dest_a(), DestinationRole::ArchiveWrite, &policy)
        .expect("dest-a resolves")
        .with_ca(&CaObservation::Present(ca_pem(0xA1).into_bytes()))
        .expect("the CA is usable");
    let b = destination::resolve(&dest_b(), DestinationRole::ArchiveWrite, &policy)
        .expect("dest-b resolves");

    let env_a = a.job_env();
    let env_b = b.job_env();

    // ---- dest-a: TLS, so plaintext is OFF although the process says `true`.
    assert_eq!(
        env_a.literal(AWS_ALLOW_HTTP_ENV),
        Some("false"),
        "dest-a declares `transport.security: TLS`. This process has AWS_ALLOW_HTTP=true set, \
         which is exactly defect SEC-ENVHTTP: a controller started that way used to forward it \
         into every runner Job and the engine's `from_env()` honoured it, enabling plaintext \
         for a plan whose `allow_http` is false"
    );
    assert_eq!(env_a.literal(AWS_REGION_ENV), Some("us-east-1"));
    assert_eq!(env_a.literal(AWS_VIRTUAL_HOSTED_ENV), Some("false"));
    assert_eq!(
        env_a.literal(AWS_METADATA_ENDPOINT_ENV),
        Some(logweir_store::DEAD_METADATA_ENDPOINT)
    );
    assert_eq!(
        env_a.literal(weirkeeper::destination::ARCHIVE_CREDENTIALS_ENV),
        Some("static")
    );
    assert_eq!(
        env_a.literal(weirkeeper::destination::STORE_CONTRACT_VERSION_ENV),
        Some("1"),
        "the version-skew handshake: a runner that does not know the store contract refuses \
         before it dispatches, so a new controller can never drive an old runner into using \
         ambient credentials"
    );
    assert_eq!(
        env_a.literal(weirkeeper::destination::ARCHIVE_CA_FILE_ENV),
        Some("/plan/archive-ca.pem")
    );

    // ---- dest-b: EXPLICIT plaintext, and no region, and no CA.
    assert_eq!(
        env_b.literal(AWS_ALLOW_HTTP_ENV),
        Some("true"),
        "dest-b declares `transport.security: InsecureHTTP` with an http:// endpoint — the one \
         way plaintext is ever enabled"
    );
    assert_eq!(
        env_b.literal(AWS_REGION_ENV),
        None,
        "dest-b names no region, and the process's AWS_REGION=eu-west-9 is not an answer"
    );
    assert_eq!(
        env_b.literal(weirkeeper::destination::ARCHIVE_CA_FILE_ENV),
        None
    );

    // ---- The credentials are different Secrets and different keys.
    let refs_a: Vec<(String, String, String)> = env_a
        .from_secret
        .iter()
        .map(|e| (e.name.clone(), e.secret_name.clone(), e.key.clone()))
        .collect();
    let refs_b: Vec<(String, String, String)> = env_b
        .from_secret
        .iter()
        .map(|e| (e.name.clone(), e.secret_name.clone(), e.key.clone()))
        .collect();
    assert_eq!(
        refs_a,
        vec![
            (
                "AWS_ACCESS_KEY_ID".to_string(),
                "lw-a-writer".to_string(),
                "access-key-id".to_string()
            ),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                "lw-a-writer".to_string(),
                "secret-access-key".to_string()
            ),
        ]
    );
    assert_eq!(
        refs_b,
        vec![
            (
                "AWS_ACCESS_KEY_ID".to_string(),
                "lw-b-writer".to_string(),
                "id".to_string()
            ),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                "lw-b-writer".to_string(),
                "key".to_string()
            ),
            (
                "AWS_SESSION_TOKEN".to_string(),
                "lw-b-writer".to_string(),
                "token".to_string()
            ),
        ],
        "dest-b's grant declares a sessionTokenKey and dest-a's does not; a session token \
         nobody configured is a token that does not exist"
    );

    // ---- And the plan storage blocks are different locations.
    assert_eq!(
        a.plan_storage(),
        StorageUrl::S3 {
            bucket: "lw-a".to_string(),
            prefix: "team-a/prod".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://minio-a.storage.svc:9000".to_string()),
            path_style: true,
            allow_http: false,
        }
    );
    assert_eq!(
        b.plan_storage(),
        StorageUrl::S3 {
            bucket: "lw-b".to_string(),
            prefix: String::new(),
            region: None,
            endpoint: Some("http://minio-b.storage.svc:9000".to_string()),
            path_style: true,
            allow_http: true,
        }
    );
    assert_ne!(a.location_digest, b.location_digest);
}

/// **M1.** No rendered variable carries a value this process holds, and
/// `AWS_ENDPOINT_URL` is absent from every set.
#[test]
fn the_rendered_environment_never_carries_a_process_value() {
    poison_the_environment();
    let policy = Policy::defaults();
    for dest in [dest_a(), dest_b()] {
        for role in DestinationRole::ALL {
            let Ok(resolved) = destination::resolve(&dest, role, &policy) else {
                continue;
            };
            let env = resolved.job_env();
            assert!(
                !env.names().contains(AWS_ENDPOINT_URL_ENV),
                "AWS_ENDPOINT_URL is ABSENT BY CONSTRUCTION: the endpoint travels in the plan's \
                 own `storage` block, which the runner reads explicitly. A variable \
                 `AmazonS3Builder::from_env()` would sweep up is a second, silent answer to \
                 `where is the bucket`. Got {:?}",
                env.literals
            );
            for (name, value) in &env.literals {
                assert_ne!(
                    value, "http://attacker.invalid:9000",
                    "`{name}` carries this process's AWS_ENDPOINT_URL"
                );
                assert_ne!(
                    value, "eu-west-9",
                    "`{name}` carries this process's AWS_REGION"
                );
            }
            assert_ne!(
                env.literal(AWS_VIRTUAL_HOSTED_ENV),
                Some("true"),
                "both fixtures declare PathStyle; this process declares \
                 AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true and it is not an answer"
            );
        }
    }
}

/// **M2.** `AWS_ALLOW_HTTP` is a function of `transport.security` and of
/// nothing else — D-SEAMS **S5**, defect UI-HTTPDOWNGRADE's server-side twin.
///
/// The table walks every legal (transport, addressing) pair. A mutant that
/// computed `allow_http` from the addressing agrees with the shipped code on
/// exactly zero of them.
#[test]
fn allow_http_comes_from_transport_and_never_from_addressing() {
    poison_the_environment();
    let table: [(&str, Option<&str>, &str, &str, &str); 3] = [
        // (transport, endpoint, addressing, want allow_http, want virtual hosted)
        (
            "TLS",
            Some("https://minio.svc:9000"),
            "PathStyle",
            "false",
            "false",
        ),
        (
            "InsecureHTTP",
            Some("http://minio.svc:9000"),
            "PathStyle",
            "true",
            "false",
        ),
        // VirtualHosted needs NO endpoint to be engine-compatible (G4).
        ("TLS", None, "VirtualHosted", "false", "true"),
    ];
    for (transport, endpoint, addressing, want_http, want_vhost) in table {
        let mut storage = serde_json::json!({
            "provider": "S3", "bucket": "lw-a", "addressing": addressing
        });
        if let Some(e) = endpoint {
            storage["endpoint"] = serde_json::json!(e);
        }
        let value = serde_json::json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
            "metadata": {"name": "d", "namespace": NS, "uid": UID_A, "generation": 1},
            "spec": {
                "storage": storage,
                "transport": {"security": transport},
                "access": {
                    "archiveWrite": {"mode": "SecretKeys", "secret": {"name": "s"}}
                }
            },
            "status": valid_status(1)
        });
        let resolved = destination::resolve(
            &build(value),
            DestinationRole::ArchiveWrite,
            &Policy::defaults(),
        )
        .unwrap_or_else(|e| panic!("{transport}/{addressing} resolves: {e}"));
        let env = resolved.job_env();
        assert_eq!(
            env.literal(AWS_ALLOW_HTTP_ENV),
            Some(want_http),
            "transport {transport} with addressing {addressing}: plaintext is enabled by the \
             TRANSPORT field and by nothing else. A path-style checkbox that set `allowHttp` is \
             the UI defect this rule exists to keep out of the server"
        );
        assert_eq!(
            env.literal(AWS_VIRTUAL_HOSTED_ENV),
            Some(want_vhost),
            "…and addressing answers its own variable, in the other direction"
        );
        // AND BOTH ARE ALWAYS RENDERED. An absent variable is one an ambient
        // value in the pod could still answer.
        assert!(env.literal_names().contains(AWS_ALLOW_HTTP_ENV));
        assert!(env.literal_names().contains(AWS_VIRTUAL_HOSTED_ENV));
    }
}

/// A workload-identity grant projects no key at all and names the
/// ServiceAccount the Job must run as.
#[test]
fn a_workload_identity_grant_projects_no_key() {
    let mut value = dest_b_value();
    value["spec"]["access"]["archiveWrite"] = serde_json::json!({
        "mode": "WorkloadIdentity",
        "workloadIdentity": {"serviceAccountName": "lw-runner"}
    });
    let resolved = destination::resolve(
        &build(value.clone()),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .expect("resolves");
    let env = resolved.job_env();
    assert!(
        env.from_secret.is_empty(),
        "a workload identity projects NO AWS_* key: static keys in the environment would take \
         precedence over the identity the operator asked for (object_store's chain puts static \
         first)"
    );
    assert_eq!(
        env.literal(weirkeeper::destination::ARCHIVE_CREDENTIALS_ENV),
        Some("workloadIdentity")
    );
    assert_eq!(env.service_account_name.as_deref(), Some("lw-runner"));
    assert_eq!(
        env.literal(AWS_METADATA_ENDPOINT_ENV),
        Some(logweir_store::DEAD_METADATA_ENDPOINT),
        "and the metadata endpoint is still pinned dead: a missing injected identity must be a \
         refusal, never a silent fall-back to the node's instance role (D2 G16)"
    );

    // The default ServiceAccount when the grant names none.
    value["spec"]["access"]["archiveWrite"] = serde_json::json!({"mode": "WorkloadIdentity"});
    let defaulted = destination::resolve(
        &build(value),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .expect("resolves")
    .job_env();
    assert_eq!(
        defaulted.service_account_name.as_deref(),
        Some("logweir-runner")
    );
}

/// Evidence and archive are DIFFERENT locations and may be different
/// principals — D2 §3.9, PLAT-08.1's "evidence/archive destination differences".
#[test]
fn evidence_and_archive_differ_in_prefix_and_credential() {
    let policy = Policy::defaults();
    let archive = destination::resolve(&dest_a(), DestinationRole::ArchiveRead, &policy)
        .expect("archiveRead resolves");
    let evidence = destination::resolve(&dest_a(), DestinationRole::EvidenceWrite, &policy)
        .expect("evidenceWrite resolves");

    assert_eq!(
        evidence.evidence_storage(),
        StorageUrl::S3 {
            bucket: "lw-a".to_string(),
            prefix: "logweir/".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://minio-a.storage.svc:9000".to_string()),
            path_style: true,
            allow_http: false,
        },
        "evidence lives at Global Constraint 6's `logweir/` root, whatever the archive prefix is"
    );
    assert_ne!(archive.plan_storage(), evidence.evidence_storage());

    // dest-a's evidenceWrite falls back to archiveWrite, which is the WRITER —
    // and its archiveRead is a different, read-only principal. So the two
    // resolutions carry two different Secrets, which is the separation G6 says
    // does not exist today.
    let env = evidence
        .evidence_env(&archive)
        .expect("one namespace, no workload identity");
    assert_eq!(
        env.literal(weirkeeper::destination::EVIDENCE_CREDENTIALS_ENV),
        Some("static"),
        "the grants differ, so the evidence store gets its own credential"
    );
    let names: Vec<&str> = env.from_secret.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID",
            "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY"
        ],
        "SEPARATELY NAMED so neither store's credential can shadow the other's: `AWS_*` is the \
         archive's"
    );
    assert!(
        env.from_secret
            .iter()
            .all(|e| e.secret_name == "lw-a-writer"),
        "the evidence WRITE grant defaults to archiveWrite; the read-only grant is for reading"
    );

    // Identical grants take the cheap path and project nothing twice.
    let same = destination::resolve(&dest_a(), DestinationRole::EvidenceRead, &policy)
        .expect("resolves")
        .evidence_env(&archive)
        .expect("same grant");
    assert_eq!(
        same.literal(weirkeeper::destination::EVIDENCE_CREDENTIALS_ENV),
        Some("archive")
    );
    assert!(same.from_secret.is_empty());
}

/// Two different workload-identity ServiceAccounts in one pod is an
/// `ExecutionContextConflict`: a pod has exactly one ServiceAccount.
#[test]
fn two_workload_identities_in_one_pod_conflict() {
    let policy = Policy::defaults();
    let make = |archive_sa: &str, evidence_sa: Option<&str>| {
        let mut v = dest_a_value();
        v["spec"]["access"]["archiveWrite"] = serde_json::json!({
            "mode": "WorkloadIdentity",
            "workloadIdentity": {"serviceAccountName": archive_sa}
        });
        if let Some(sa) = evidence_sa {
            v["spec"]["access"]["evidenceWrite"] = serde_json::json!({
                "mode": "WorkloadIdentity",
                "workloadIdentity": {"serviceAccountName": sa}
            });
        }
        build(v)
    };
    let archive = destination::resolve(
        &make("sa-archive", None),
        DestinationRole::ArchiveWrite,
        &policy,
    )
    .expect("resolves");
    let evidence = destination::resolve(
        &make("sa-archive", Some("sa-evidence")),
        DestinationRole::EvidenceWrite,
        &policy,
    )
    .expect("resolves");

    let refusal = evidence
        .evidence_env(&archive)
        .expect_err("two ServiceAccounts, one pod");
    assert_eq!(refusal.code, CheckCode::ExecutionContextConflict);
    assert!(
        refusal.message.contains("sa-archive") && refusal.message.contains("sa-evidence"),
        "the refusal names both: {}",
        refusal.message
    );

    // …and the SAME ServiceAccount on both sides is fine.
    let same = destination::resolve(
        &make("sa-archive", None),
        DestinationRole::EvidenceWrite,
        &policy,
    )
    .expect("resolves")
    .evidence_env(&archive)
    .expect("one ServiceAccount");
    assert_eq!(same.service_account_name.as_deref(), None);
}

/// Nothing this module renders is a credential VALUE.
#[test]
fn no_rendered_value_is_a_credential() {
    poison_the_environment();
    let policy = Policy::defaults();
    for dest in [dest_a(), dest_b()] {
        for role in DestinationRole::ALL {
            let Ok(resolved) = destination::resolve(&dest, role, &policy) else {
                continue;
            };
            let rendered = format!("{:?}", resolved.job_env());
            for forbidden in [FIXTURE_SECRET_VALUE, "BEGIN RSA PRIVATE KEY", "AKIA"] {
                assert!(
                    !rendered.contains(forbidden),
                    "the rendered environment names `{forbidden}`: {rendered}"
                );
            }
            // Every credential variable is a REFERENCE, never a literal.
            for (name, _) in &resolved.job_env().literals {
                assert!(
                    !name.ends_with("ACCESS_KEY_ID")
                        && !name.ends_with("SECRET_ACCESS_KEY")
                        && !name.ends_with("SESSION_TOKEN"),
                    "`{name}` is a literal environment entry and it names a credential; every \
                     credential variable is a `valueFrom.secretKeyRef` the kubelet resolves"
                );
            }
        }
    }
}

/// The source-level half of "this module reads no environment".
#[test]
fn the_resolver_names_no_environment_read() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/destination.rs",
        "src/controllers/backup_destination.rs",
    ] {
        let text = std::fs::read_to_string(root.join(relative)).expect("a readable source file");
        let code: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for needle in ["env::var", "env::vars", "archive_addressing_env"] {
            assert!(
                !code.contains(needle),
                "{relative} names `{needle}`. Defect SEC-ENVHTTP is a controller forwarding its \
                 own environment into a runner Job; the destination path renders a COMPLETE set \
                 from the object and must read nothing from this process"
            );
        }
    }
}

// ===========================================================================
// §3.7 / seam S4 — the frozen snapshot
// ===========================================================================

/// The snapshot W10 freezes: canonical, stable, and carrying no credential.
#[test]
fn the_snapshot_is_canonical_stable_and_value_free() {
    let resolved = destination::resolve(
        &dest_a(),
        DestinationRole::ArchiveWrite,
        &Policy::defaults(),
    )
    .expect("resolves")
    .with_ca(&CaObservation::Present(ca_pem(0xC0).into_bytes()))
    .expect("the CA is usable");
    let snapshot = resolved.snapshot();

    let bytes = snapshot.canonical_bytes().expect("the snapshot encodes");
    let text = String::from_utf8(bytes.clone()).expect("UTF-8");
    assert_eq!(
        snapshot.canonical_bytes().expect("again"),
        bytes,
        "the encoding is deterministic — the frozen document is re-encoded and compared byte for \
         byte on every later pass, so an encoder that reordered a map would turn every running \
         Backup into a PlanConfigMapConflict"
    );
    assert_eq!(
        snapshot.digest().expect("digests"),
        logweir_core::ids::sha256_prefixed(&bytes)
    );

    // IT ROUND-TRIPS, and the type denies unknown fields, so a snapshot written
    // by a later grammar is a conflict and never a partial read.
    let back: weirkeeper::destination::ResolvedDestinationSnapshot =
        serde_json::from_slice(&bytes).expect("round-trips");
    assert_eq!(back, snapshot);

    // THE CA BYTES ARE NOT IN IT — only their digest. The bytes live beside
    // `backup.yaml` in the same immutable plan ConfigMap.
    assert!(
        !text.contains("BEGIN CERTIFICATE"),
        "the snapshot carries the CA DIGEST and not the bundle: {text}"
    );
    assert_eq!(snapshot.ca_sha256, resolved.ca_sha256);
    assert!(snapshot.ca_sha256.is_some());

    // AND NO CREDENTIAL VALUE: the grant is a Secret name and two data keys.
    for forbidden in [FIXTURE_SECRET_VALUE, "AKIA"] {
        assert!(!text.contains(forbidden), "{text}");
    }
    assert!(text.contains("lw-a-writer") && text.contains("access-key-id"));

    // The identity fields are there, because a destination deleted and
    // recreated under the same name is a different input.
    assert_eq!(snapshot.uid, UID_A);
    assert_eq!(snapshot.generation, 3);
    assert_eq!(snapshot.location_digest, resolved.location_digest);
}

/// The snapshot digest moves with the generation and with the CA, and NOT with
/// the role — two roles of one destination describe the same frozen location.
#[test]
fn the_snapshot_digest_moves_with_generation_and_ca() {
    let policy = Policy::defaults();
    let base = destination::resolve(&dest_a(), DestinationRole::ArchiveWrite, &policy)
        .expect("resolves")
        .with_ca(&CaObservation::Present(ca_pem(1).into_bytes()))
        .expect("usable");
    let rotated = destination::resolve(&dest_a(), DestinationRole::ArchiveWrite, &policy)
        .expect("resolves")
        .with_ca(&CaObservation::Present(ca_pem(2).into_bytes()))
        .expect("usable");
    let newer_value = {
        let mut v = dest_a_value();
        v["metadata"]["generation"] = serde_json::json!(4);
        v["status"] = valid_status(4);
        v
    };
    let newer = destination::resolve(&build(newer_value), DestinationRole::ArchiveWrite, &policy)
        .expect("resolves")
        .with_ca(&CaObservation::Present(ca_pem(1).into_bytes()))
        .expect("usable");

    let d = |r: &ResolvedDestination| r.snapshot().digest().expect("digests");
    assert_ne!(
        d(&base),
        d(&rotated),
        "a rotated CA is a different snapshot"
    );
    assert_ne!(
        d(&base),
        d(&newer),
        "a new generation is a different snapshot"
    );
    assert_eq!(
        base.location_digest, newer.location_digest,
        "…but the LOCATION digest is unchanged: transport and addressing describe the route, \
         not the place, and a recovery point written under generation 3 is still at the same \
         bucket"
    );
}

// ===========================================================================
// §3.10 — the ControllerIdentity store cache
// ===========================================================================

/// A destination the policy allows, resolved for `EvidenceRead`.
fn controller_identity_destination(
    uid: &str,
    generation: i64,
    bucket: &str,
) -> ResolvedDestination {
    let value = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": format!("d-{}", &uid[..8]), "namespace": NS,
            "uid": uid, "generation": generation
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": bucket,
                "endpoint": "http://minio-b.storage.svc:9000", "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {"name": "w"}},
                "evidenceRead": {"mode": "ControllerIdentity"}
            }
        },
        "status": valid_status(generation)
    });
    destination::resolve(
        &build(value),
        DestinationRole::EvidenceRead,
        &policy_for_bucket(bucket),
    )
    .expect("resolves")
}

/// The same key returns the SAME handle — the point of a cache.
#[tokio::test]
async fn the_store_cache_shares_one_handle_per_key() {
    let cache = StoreCache::new();
    let resolved =
        controller_identity_destination("aaaaaaaa-0000-4000-8000-00000000aaaa", 1, "lw-b");
    let policy = policy_for_bucket("lw-b");

    let first = cache
        .get_or_build(&resolved, &policy)
        .await
        .expect("the handle builds");
    let second = cache
        .get_or_build(&resolved, &policy)
        .await
        .expect("the handle is cached");
    assert!(
        Arc::ptr_eq(&first, &second),
        "a handle rebuilt per call discards the connection pool AND builds a tokio runtime each \
         time (interface I13)"
    );
    assert_eq!(cache.len(), 1);

    // The handle CANNOT WRITE: `read_only_with` and never `from_url_with`
    // (guard G-RET).
    let refused = first.put_create_only("logweir/probe", b"x");
    assert!(
        refused.is_err(),
        "the controller's evidence handles are read-only by construction"
    );

    // RELEASED FROM A BLOCKING THREAD. A `Store` owns a
    // `tokio::runtime::Runtime`, and dropping a runtime on a runtime thread
    // panics with *Cannot drop a runtime in a context where blocking is not
    // allowed* — which is why `StoreCache::clear` exists and why an eviction
    // inside `get_or_build` goes through `spawn_blocking` too.
    drop(first);
    drop(second);
    cache.clear().await;
}

/// **M5.** The cache is BOUNDED, and it evicts the least recently USED.
#[tokio::test]
async fn the_store_cache_is_bounded() {
    let policy = policy_for_bucket("lw-b");
    let cache = StoreCache::with_capacity(4);
    let mut keys = Vec::new();
    for n in 0..4u8 {
        let resolved = controller_identity_destination(
            &format!("bbbbbbbb-0000-4000-8000-0000000000{n:02}"),
            1,
            "lw-b",
        );
        keys.push(CacheKey::of(&resolved));
        cache
            .get_or_build(&resolved, &policy)
            .await
            .expect("builds");
    }
    assert_eq!(cache.len(), 4);

    // TOUCH THE OLDEST so it is the most recently USED. A least-recently-
    // INSERTED cache would evict it next; an LRU evicts the second one.
    let touched =
        controller_identity_destination("bbbbbbbb-0000-4000-8000-000000000000", 1, "lw-b");
    cache.get_or_build(&touched, &policy).await.expect("cached");

    let fifth = controller_identity_destination("bbbbbbbb-0000-4000-8000-0000000000ff", 1, "lw-b");
    cache.get_or_build(&fifth, &policy).await.expect("builds");

    assert_eq!(
        cache.len(),
        4,
        "the cache is bounded at its capacity. An unbounded map keyed by \
         (uid, generation, caSha256) grows by one entry every time an operator rotates a CA or \
         edits a destination and never shrinks — a leak whose symptom is a controller OOM weeks \
         later, each entry holding a connection pool and a tokio runtime"
    );
    assert!(cache.holds(&keys[0]), "the touched entry survived");
    assert!(
        !cache.holds(&keys[1]),
        "the least recently USED entry was evicted"
    );
    assert_eq!(MAX_CACHED_STORES, 32, "D2 §3.10's bound, as shipped");
    cache.clear().await;
}

/// A rotated CA is a different key, although the `BackupDestination` did not
/// change at all.
#[tokio::test]
async fn a_rotated_ca_is_a_different_cache_key() {
    let policy = policy_for_bucket("lw-b");
    let cache = StoreCache::new();
    let base = controller_identity_destination("cccccccc-0000-4000-8000-00000000cccc", 1, "lw-b");
    let with_old = base
        .clone()
        .with_ca(&CaObservation::NotDeclared)
        .expect("no bundle declared");
    assert_eq!(CacheKey::of(&with_old).ca_sha256, None);

    let mut rotated = base.clone();
    rotated.ca_sha256 = Some("sha256:rotated".to_string());
    assert_ne!(
        CacheKey::of(&base),
        CacheKey::of(&rotated),
        "a CA rotated IN PLACE changes the ConfigMap and not the BackupDestination, so its \
         generation is unchanged; without the content digest in the key a rotated root would be \
         invisible until the controller restarted"
    );

    cache.get_or_build(&base, &policy).await.expect("builds");
    assert!(cache.holds(&CacheKey::of(&base)));
    assert!(!cache.holds(&CacheKey::of(&rotated)));
    cache.clear().await;
}

/// The cache refuses an unlisted location and a grant that is not
/// `ControllerIdentity`.
#[tokio::test]
async fn the_store_cache_refuses_what_it_may_not_build() {
    let cache = StoreCache::new();
    let resolved =
        controller_identity_destination("dddddddd-0000-4000-8000-00000000dddd", 1, "lw-b");

    // `Result::expect_err` needs `T: Debug`, and `Store` deliberately is not
    // (a derived `Debug` on a store handle is how a credential provider reaches
    // a log line). `map(drop)` throws the handle away and keeps the refusal.
    let unlisted = cache
        .get_or_build(&resolved, &Policy::defaults())
        .await
        .map(drop)
        .expect_err("the default policy allowlists nothing");
    assert_eq!(unlisted.code, CheckCode::ControllerIdentityNotAllowlisted);
    assert_eq!(cache.len(), 0, "a refusal caches nothing");

    let secret_keys = destination::resolve(
        &dest_a(),
        DestinationRole::EvidenceRead,
        &Policy::defaults(),
    )
    .expect("resolves");
    let wrong_grant = cache
        .get_or_build(&secret_keys, &policy_for_bucket("lw-a"))
        .await
        .map(drop)
        .expect_err("a SecretKeys grant is read by a JOB, not by the controller");
    assert_eq!(wrong_grant.code, CheckCode::DestinationRoleNotConfigured);
    assert!(
        wrong_grant.message.contains("evidence-fetch Job"),
        "the refusal says which path DOES serve this grant: {}",
        wrong_grant.message
    );
}

// ===========================================================================
// §3.10 — the G14 retention guard (defect RET-WRONGBUCKET)
// ===========================================================================

/// **M6.** A schedule on another bucket gets no retention report.
///
/// The defect: `controllers/backup_schedule.rs` lists manifests through the
/// controller's ONE global handle while rendering `aws s3 rm` / `mc rm`
/// commands for the SCHEDULE's own `archive.url`. On an installation with two
/// buckets that is a report about bucket A printed as if it described bucket B,
/// with commands naming keys in B that were listed in A.
#[test]
fn retention_is_withheld_when_the_schedule_names_another_bucket() {
    let scope = retention_scope("s3://lw-b/team-a", Some("s3://lw-a"), false);
    assert!(
        !scope.reports(),
        "a report about another bucket's catalogue is worse than no report: an operator who \
         runs its commands deletes the wrong objects, or nothing"
    );
    match scope {
        RetentionScope::WrongBucket {
            schedule_bucket,
            handle_bucket,
        } => {
            assert_eq!(schedule_bucket, "lw-b");
            assert_eq!(handle_bucket, "lw-a");
        }
        other => panic!("expected WrongBucket, got {other:?}"),
    }
}

/// …and the same bucket with different prefixes still reports, because the
/// listing is prefix-scoped by the report itself.
#[test]
fn retention_applies_when_the_buckets_match() {
    for (schedule, handle) in [
        ("s3://lw-a/team-a", "s3://lw-a"),
        ("s3://lw-a", "s3://lw-a/anything"),
        ("s3://lw-a/team-a", "s3://lw-a/team-b"),
    ] {
        assert!(
            retention_scope(schedule, Some(handle), false).reports(),
            "{schedule} through a handle on {handle} is the same bucket"
        );
    }
}

/// A destination-backed schedule gets no GLOBAL retention report at all, and no
/// handle means no report either.
#[test]
fn a_destination_backed_schedule_gets_no_global_report() {
    assert_eq!(
        retention_scope("s3://lw-a/team-a", Some("s3://lw-a"), true),
        RetentionScope::DestinationBacked,
        "the global handle's credential is not the destination's, so even the same bucket is \
         the wrong principal; PLAT-16.1 supplies the per-destination replacement"
    );
    assert_eq!(
        retention_scope("s3://lw-a", None, false),
        RetentionScope::NoHandle
    );
    // AN UNREADABLE URL WITHHOLDS THE REPORT — the safe direction. The sentinel
    // `logweir-destination://` is exactly that shape for an old controller.
    assert!(!retention_scope("logweir-destination://dest-a", Some("s3://lw-a"), false).reports());
    assert!(!retention_scope("s3://lw-a", Some("not-a-url"), false).reports());
}

// ===========================================================================
// The CA parser
// ===========================================================================

/// What counts as a certificate, and what does not.
#[test]
fn the_certificate_parser_table() {
    let one = ca_pem(0x11);
    assert_eq!(destination::certificate_count(one.as_bytes()), 1);
    assert_eq!(
        destination::certificate_count(format!("{one}{}", ca_pem(0x22)).as_bytes()),
        2,
        "a bundle of two is a bundle"
    );
    for (bytes, why) in [
        (b"".to_vec(), "empty"),
        (
            b"-----BEGIN RSA PRIVATE KEY-----\nMIIBAQ==\n-----END RSA PRIVATE KEY-----\n".to_vec(),
            "a private key",
        ),
        (
            b"-----BEGIN CERTIFICATE-----\nAA\n-----END CERTIFICATE-----\n".to_vec(),
            "base64 that is not a multiple of four",
        ),
        (
            b"-----BEGIN CERTIFICATE-----\nMIIB".to_vec(),
            "no END marker",
        ),
        // Valid base64 of something that is not a DER SEQUENCE.
        (
            b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_vec(),
            "not a SEQUENCE",
        ),
        (vec![0x30, 0x82, 0x01, 0x00], "raw DER, not PEM"),
    ] {
        assert_eq!(
            destination::certificate_count(&bytes),
            0,
            "{why} is not a certificate bundle"
        );
        assert_eq!(
            destination::check_ca_bundle(&bytes),
            Err(CheckCode::CaBundleInvalid)
        );
    }
    assert_eq!(
        destination::check_ca_bundle(&vec![b'x'; 70_000]),
        Err(CheckCode::CaBundleTooLarge),
        "size is checked BEFORE content: a megabyte of nonsense should not be parsed to find out"
    );
    assert_eq!(
        destination::check_ca_bundle(one.as_bytes()),
        Ok(logweir_core::ids::sha256_prefixed(one.as_bytes()))
    );
}
