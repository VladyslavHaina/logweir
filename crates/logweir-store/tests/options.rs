//! Explicit store construction (decision D2 W2): credentials, transport,
//! addressing, root certificates, the instance-metadata pin and error
//! classification.
//!
//! # No socket is opened here
//!
//! The decision a store is built with is read through `s3_effective`, which is
//! the SAME function `Store::from_url_with` consumes — not a parallel model of
//! it. The one behavioural assertion (`…wins_over_aws_allow_http_true`) uses
//! reqwest's `https_only`, which refuses an `http://` URL before it resolves a
//! name or opens a connection, and it points at a dead loopback port so that a
//! regression fails fast instead of hanging (Global Constraint 17 and 22).

use logweir_core::engine::StorageUrl;
use logweir_store::{
    is_workload_identity_not_injected, s3_effective, workload_identity_from_env, CredentialKind,
    CredentialSource, Store, StoreError, StoreErrorClass, StoreOptions, WorkloadIdentity,
    DEAD_METADATA_ENDPOINT, WORKLOAD_IDENTITY_NOT_INJECTED,
};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

/// `std::env::set_var` is process-wide, and this binary's tests run on several
/// threads. Every test that touches the environment takes this lock and
/// restores what it found, so one test's poison cannot leak into another's
/// assertion — which would make the whole file's evidence worthless.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

struct EnvGuard(Vec<(String, Option<String>)>);

impl EnvGuard {
    fn set(pairs: &[(&str, &str)]) -> Self {
        let saved = pairs
            .iter()
            .map(|(k, _)| ((*k).to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in pairs {
            std::env::set_var(k, v);
        }
        Self(saved)
    }

    fn clear(keys: &[&str]) -> Self {
        let saved = keys
            .iter()
            .map(|k| ((*k).to_string(), std::env::var(k).ok()))
            .collect();
        for k in keys {
            std::env::remove_var(k);
        }
        Self(saved)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in &self.0 {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

/// Every `AWS_*` name this file ever sets, so a guard can clear the lot.
const AWS_VARS: [&str; 11] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
];

fn https_tls() -> StorageUrl {
    StorageUrl::S3 {
        bucket: "kafka-backups".into(),
        prefix: "team-a/prod".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("https://minio.storage.svc:9000".into()),
        path_style: true,
        allow_http: false,
    }
}

// --------------------------------------------------- explicit over ambient

/// PLAT-08.1's `store_options::explicit_static_credentials_ignore_ambient_env`.
///
/// The poison values below are exactly what a controller's own environment
/// looks like today (`controllers/backup.rs:192-218` forwards four of them),
/// and a destination-backed store must be built from NONE of them.
#[test]
fn explicit_static_credentials_ignore_ambient_env() {
    let _l = env_lock();
    let _g = EnvGuard::set(&[
        ("AWS_ACCESS_KEY_ID", "AKIAPOISONPOISON1234"),
        ("AWS_SECRET_ACCESS_KEY", "poison-secret"),
        ("AWS_SESSION_TOKEN", "poison-token"),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
        ("AWS_ENDPOINT_URL", "https://wrong-endpoint.invalid:9000"),
        ("AWS_REGION", "ap-southeast-2"),
    ]);

    let opts = StoreOptions::static_keys("AKIADESTINATION00001", "the-real-secret", None);
    let eff = s3_effective(&https_tls(), &opts).unwrap();

    assert_eq!(eff.credentials, CredentialKind::Static);
    assert_eq!(eff.access_key_id.as_deref(), Some("AKIADESTINATION00001"));
    assert!(!eff.session_token_present);
    assert!(
        !eff.reads_environment,
        "an explicit static credential must not consult the environment at all"
    );
    // Location and route come from the StorageUrl, never from AWS_*.
    assert_eq!(
        eff.endpoint.as_deref(),
        Some("https://minio.storage.svc:9000")
    );
    assert_eq!(eff.region.as_deref(), Some("us-east-1"));
    assert!(!eff.allow_http);
    assert!(!eff.virtual_hosted_style);
    // And the node instance role is off the table.
    assert_eq!(
        eff.metadata_endpoint.as_deref(),
        Some(DEAD_METADATA_ENDPOINT)
    );

    // The secret never appears in the observable configuration.
    let shown = format!("{eff:?}");
    assert!(!shown.contains("the-real-secret"), "{shown}");
    assert!(!shown.contains("poison-secret"), "{shown}");
}

/// Tracker defect **SEC-ENVHTTP**, both halves.
///
/// (a) the decision: with `AWS_ALLOW_HTTP=true` in the process environment, an
/// explicit `allow_http: false` still renders `false`.
///
/// (b) the behaviour: the store built from that decision REFUSES an `http://`
/// request. reqwest's `https_only` rejects the scheme before it resolves a
/// name, so this opens no socket; the endpoint is a dead loopback port so a
/// regression fails in milliseconds rather than hanging.
///
/// MUTANT: dropping `.with_allow_http(eff.allow_http)` from
/// `build_backend_with` — i.e. letting `from_env`'s value stand — turns (b)
/// red, because the request then gets as far as the connection and reports a
/// refusal instead of a scheme error.
#[test]
fn explicit_allow_http_false_wins_over_aws_allow_http_true() {
    let _l = env_lock();
    let _g = EnvGuard::set(&[
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_ACCESS_KEY_ID", "AKIAPOISONPOISON1234"),
        ("AWS_SECRET_ACCESS_KEY", "poison-secret"),
    ]);

    let plan_says_https_only = StorageUrl::S3 {
        bucket: "kafka-backups".into(),
        prefix: "team-a".into(),
        region: Some("us-east-1".into()),
        // A plaintext endpoint with `allow_http: false` is the exact shape of
        // the defect: the plan forbids HTTP and the environment permits it.
        endpoint: Some("http://127.0.0.1:1".into()),
        path_style: true,
        allow_http: false,
    };

    // (a) the decision.
    for opts in [
        StoreOptions::static_keys("AKIADEST", "s", None),
        StoreOptions::ambient(),
    ] {
        let eff = s3_effective(&plan_says_https_only, &opts).unwrap();
        assert!(
            !eff.allow_http,
            "the plan says allow_http: false; AWS_ALLOW_HTTP=true must not change that"
        );
    }

    // (b) the behaviour. The same location, built twice, differing ONLY in
    // the `allow_http` the plan carries. reqwest is configured `https_only`
    // when it is false, so the request is refused while the client is still
    // BUILDING the request — no name resolution and no socket — while the
    // permissive twin gets as far as the transport and is refused by the dead
    // loopback port. Asserting the PAIR is what makes this a proof: a single
    // assertion on an error string is satisfied by any failure at all.
    let build = |allow_http: bool| {
        let mut u = plan_says_https_only.clone();
        let StorageUrl::S3 { allow_http: a, .. } = &mut u else {
            unreachable!()
        };
        *a = allow_http;
        Store::read_only_with(
            &u,
            &StoreOptions::static_keys("AKIADEST", "s", None)
                .with_request_timeout(Duration::from_secs(2))
                .with_max_retries(0),
        )
        .expect("the client builds either way")
    };

    let refused_scheme = format!("{}", build(false).get("team-a/anything.json").unwrap_err());
    let reached_transport = format!("{}", build(true).get("team-a/anything.json").unwrap_err());

    assert_ne!(
        refused_scheme, reached_transport,
        "allow_http made no difference at all, which means the plan's value is not \
         reaching the client (defect SEC-ENVHTTP)"
    );
    let lower = refused_scheme.to_ascii_lowercase();
    assert!(
        lower.contains("builder error"),
        "with allow_http: false reqwest must refuse the http:// URL while building the \
         request. Got: {refused_scheme}"
    );
    assert!(
        !lower.contains("sending request") && !lower.contains("connect"),
        "with allow_http: false nothing may reach the transport. Got: {refused_scheme}"
    );
    let lower = reached_transport.to_ascii_lowercase();
    assert!(
        lower.contains("sending request"),
        "with allow_http: true the request must reach the transport (and be refused by \
         the dead loopback port), or this test is comparing two failures that have \
         nothing to do with the scheme. Got: {reached_transport}"
    );
    // And the permissive twin's failure classifies as a transport problem,
    // which is the code a check would report.
    assert_eq!(
        StoreErrorClass::classify(&StoreError::Io(reached_transport)),
        StoreErrorClass::EndpointUnreachable
    );
}

/// The addressing half of the same override. `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`
/// in the environment must not move a path-style destination.
#[test]
fn explicit_addressing_wins_over_the_environment() {
    let _l = env_lock();
    let _g = EnvGuard::set(&[("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true")]);
    let eff = s3_effective(&https_tls(), &StoreOptions::ambient()).unwrap();
    assert!(
        !eff.virtual_hosted_style,
        "path_style: true in the plan means path-style addressing, whatever AWS_* says"
    );

    let mut virtual_hosted = https_tls();
    let StorageUrl::S3 { path_style, .. } = &mut virtual_hosted else {
        unreachable!()
    };
    *path_style = false;
    let eff = s3_effective(&virtual_hosted, &StoreOptions::ambient()).unwrap();
    assert!(eff.virtual_hosted_style);
}

// --------------------------------------------------------- workload identity

/// D2 §3.5: no injected identity is a REFUSAL, not a fall-through to the
/// node's instance role.
#[test]
fn workload_identity_without_injection_is_a_closed_refusal() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    // Static keys present and deliberately ignored: object_store's own chain
    // puts static FIRST, so without this source's explicit refusal the store
    // would quietly use a credential the operator did not ask for.
    let _g2 = EnvGuard::set(&[
        ("AWS_ACCESS_KEY_ID", "AKIAPOISONPOISON1234"),
        ("AWS_SECRET_ACCESS_KEY", "poison-secret"),
    ]);

    let err = s3_effective(&https_tls(), &StoreOptions::workload_identity()).unwrap_err();
    assert!(is_workload_identity_not_injected(&err), "{err}");
    assert!(format!("{err}").contains(WORKLOAD_IDENTITY_NOT_INJECTED));
    assert!(
        format!("{err}").contains("node instance role"),
        "the message must say what it refused to do"
    );
    assert!(Store::read_only_with(&https_tls(), &StoreOptions::workload_identity()).is_err());
}

#[test]
fn workload_identity_reads_only_the_injection_variables() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    let _g2 = EnvGuard::set(&[
        ("AWS_ACCESS_KEY_ID", "AKIAPOISONPOISON1234"),
        ("AWS_SECRET_ACCESS_KEY", "poison-secret"),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token"),
        ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/logweir"),
    ]);

    let eff = s3_effective(&https_tls(), &StoreOptions::workload_identity()).unwrap();
    assert_eq!(eff.credentials, CredentialKind::WorkloadIdentity);
    assert_eq!(
        eff.access_key_id, None,
        "a workload-identity store must not carry a static key id"
    );
    assert_eq!(
        eff.workload_identity,
        Some(WorkloadIdentity::WebIdentity {
            token_file: "/var/run/secrets/token".into(),
            role_arn: "arn:aws:iam::123456789012:role/logweir".into(),
            session_name: None,
            sts_endpoint: None,
        })
    );
    assert_eq!(
        eff.metadata_endpoint.as_deref(),
        Some(DEAD_METADATA_ENDPOINT)
    );
}

#[test]
fn the_three_injection_shapes_are_recognised_in_order() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    assert_eq!(workload_identity_from_env(), None);

    let _a = EnvGuard::set(&[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds")]);
    assert_eq!(
        workload_identity_from_env(),
        Some(WorkloadIdentity::ContainerRelativeUri {
            uri: "/v2/creds".into()
        })
    );

    let _b = EnvGuard::set(&[
        (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://169.254.170.23/v1",
        ),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/var/run/token"),
    ]);
    assert!(matches!(
        workload_identity_from_env(),
        Some(WorkloadIdentity::ContainerFullUri { .. })
    ));

    let _c = EnvGuard::set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token"),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
    ]);
    assert!(
        matches!(
            workload_identity_from_env(),
            Some(WorkloadIdentity::WebIdentity { .. })
        ),
        "IRSA wins when both are injected, matching object_store's own order"
    );

    // An EMPTY variable is unset: that is what a Deployment writes when a
    // value is deleted but the key is left behind.
    let _d = EnvGuard::set(&[("AWS_WEB_IDENTITY_TOKEN_FILE", "   ")]);
    assert!(matches!(
        workload_identity_from_env(),
        Some(WorkloadIdentity::ContainerFullUri { .. })
    ));
}

#[test]
fn static_from_env_needs_both_halves() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    let err = s3_effective(&https_tls(), &StoreOptions::static_from_env()).unwrap_err();
    assert!(format!("{err}").contains("AWS_ACCESS_KEY_ID"));

    let _g2 = EnvGuard::set(&[("AWS_ACCESS_KEY_ID", "AKIAPROJECTED0000001")]);
    let err = s3_effective(&https_tls(), &StoreOptions::static_from_env()).unwrap_err();
    assert!(format!("{err}").contains("AWS_SECRET_ACCESS_KEY"), "{err}");

    let _g3 = EnvGuard::set(&[("AWS_SECRET_ACCESS_KEY", "projected")]);
    let eff = s3_effective(&https_tls(), &StoreOptions::static_from_env()).unwrap();
    assert_eq!(eff.credentials, CredentialKind::Static);
    assert_eq!(eff.access_key_id.as_deref(), Some("AKIAPROJECTED0000001"));
    assert!(eff.reads_environment);
}

// ------------------------------------------------------- metadata endpoint

/// D2 G16: an explicit credential source can never fall back to instance
/// metadata. `Ambient` is the one case that keeps the default, because there
/// the instance role may be exactly what the operator configured.
#[test]
fn instance_metadata_is_pinned_for_every_explicit_source() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    let _g2 = EnvGuard::set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/t"),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
        ("AWS_ACCESS_KEY_ID", "AKIAPROJECTED0000001"),
        ("AWS_SECRET_ACCESS_KEY", "projected"),
    ]);

    for (name, opts) in [
        ("static", StoreOptions::static_keys("a", "b", None)),
        ("staticFromEnv", StoreOptions::static_from_env()),
        ("workloadIdentity", StoreOptions::workload_identity()),
    ] {
        assert!(opts.pins_instance_metadata(), "{name}");
        let eff = s3_effective(&https_tls(), &opts).unwrap();
        assert_eq!(
            eff.metadata_endpoint.as_deref(),
            Some(DEAD_METADATA_ENDPOINT),
            "{name}"
        );
    }

    let ambient = StoreOptions::ambient();
    assert!(!ambient.pins_instance_metadata());
    assert_eq!(
        s3_effective(&https_tls(), &ambient)
            .unwrap()
            .metadata_endpoint,
        None
    );

    // The default is overridable in both directions, explicitly.
    let mut pinned_ambient = StoreOptions::ambient();
    pinned_ambient.pin_instance_metadata = Some(true);
    assert!(pinned_ambient.pins_instance_metadata());
    let mut unpinned_static = StoreOptions::static_keys("a", "b", None);
    unpinned_static.pin_instance_metadata = Some(false);
    assert!(!unpinned_static.pins_instance_metadata());
}

// ------------------------------------------------------- root certificates

/// A destination's private CA. The PEM below is a throwaway self-signed
/// certificate generated for this test; its private key was discarded at
/// generation time and it signs nothing.
const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDIzCCAgugAwIBAgIURbs9+NCh+kv/Ze9LVyIUiMvroVgwDQYJKoZIhvcNAQEL
BQAwIDEeMBwGA1UEAwwVbG9nd2Vpci1zdG9yZS10ZXN0LWNhMCAXDTI2MDkxNjEz
NDU0NFoYDzIxMjYwODIzMTM0NTQ0WjAgMR4wHAYDVQQDDBVsb2d3ZWlyLXN0b3Jl
LXRlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDO2quWx8xZ
bT0BCloVXI7Vpay+0inrNaBrRTisk5Cobi2UypTi5kiEH1QMbd864v7UGi2IGQtW
FxA7TOWk7L2awu0xkNkz9ZmwzbzWsrgfjFcLGcDDPRKodB3GjBHt99lrIxLDr3Yy
NmuBNE/xmMXRvt32mqeBe7nNn+TQOLD+fHpJ+WntbLRnVwwjpwfFrOxerQdg4MAw
IOnwBcfIhsO6zJNKmEVIJZS/jO6MuRBKqQqP41ZJrws7h6o8jMtNxf0yEreClGzF
KbVg15MGTG0K0QFS5p/I8/jPuoB90TXqwwDpN65m/y1MSFsHuNYFYCCA1NMReBrn
7UNIWaLhVRhpAgMBAAGjUzBRMB0GA1UdDgQWBBRvZedXh+jwav3IZcENmNbQ7x/S
CDAfBgNVHSMEGDAWgBRvZedXh+jwav3IZcENmNbQ7x/SCDAPBgNVHRMBAf8EBTAD
AQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCVM0l4e2yiMFcpY+0paWPq48E/Mhp+7Kku
4+zbBXgErJzNzCcQZi2oYfZlmWob5bI0UhOdhgoS3JeU/u3P2gh/y2ku3NJwYOtp
W1NzQqnITbr90NqSiigXKEf7fbxTiTt1JMEntGnWBGCSE/XSJm8ZAO8YRjwjWU0Q
8+z9rb8/mQID9PljKcFdjCACSXoiU+l4taGZzbRTC0So/8yxF1MW5yycPji7ONh1
LiRFFfrcmf1tC+kNEQSJdIYnZ9PW7v85HFtHe8V4mV97aalUQm83IHDI0w4n6paY
ypmB+0f8Zz4L6+2W+GU84ZArzgwDymVWN1Wd2UN/NY79dhRFw70I
-----END CERTIFICATE-----
";

#[test]
fn a_root_certificate_is_accepted_and_garbage_is_refused() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    let opts = StoreOptions::static_keys("a", "b", None)
        .with_root_certificate(TEST_CA_PEM.as_bytes().to_vec());
    assert_eq!(
        s3_effective(&https_tls(), &opts)
            .unwrap()
            .root_certificate_count,
        1
    );
    // The whole construction path, so the PEM really is parsed and installed.
    assert!(Store::read_only_with(&https_tls(), &opts).is_ok());

    // A bundle of two: `from_pem_bundle` must install both.
    let two = format!("{TEST_CA_PEM}{TEST_CA_PEM}");
    let opts = StoreOptions::static_keys("a", "b", None).with_root_certificate(two.into_bytes());
    assert!(Store::read_only_with(&https_tls(), &opts).is_ok());

    let bad = StoreOptions::static_keys("a", "b", None)
        .with_root_certificate(b"-----BEGIN CERTIFICATE-----\nnot base64\n".to_vec());
    let Err(err) = Store::read_only_with(&https_tls(), &bad) else {
        panic!("a PEM that does not parse must be refused, not silently dropped");
    };
    assert!(format!("{err}").contains("root certificate"), "{err}");
}

// ------------------------------------------------------------ non-S3 urls

/// A `BackupDestination` is `provider: S3`. Silently ignoring an explicit
/// credential on another backend would build a store with a credential nobody
/// asked for, so it is refused by name.
#[test]
fn a_non_s3_url_with_explicit_options_is_refused() {
    let _l = env_lock();
    let fs = StorageUrl::Filesystem {
        path: std::path::PathBuf::from("/tmp/nope"),
    };
    let err = s3_effective(&fs, &StoreOptions::static_keys("a", "b", None)).unwrap_err();
    assert!(format!("{err}").contains("filesystem"), "{err}");
    assert!(Store::from_url_with(&fs, &StoreOptions::ambient()).is_err());
}

// -------------------------------------------- the existing constructors

/// Global Constraint 6 is enforced by ONE function that `from_url` and
/// `from_url_with` both call, so the new constructor cannot become a second
/// way to build a writable handle outside `logweir/`.
///
/// The `from_url` half of that claim is already covered, with both directions,
/// by `tests/storage.rs::a_prefix_deeper_than_the_sanctioned_root_is_refused_at_construction`
/// and `::the_sanctioned_root_still_builds` — which is why this file does not
/// name that constructor a second time (`no_network_in_unit_tests` allow-lists
/// `storage.rs` by path and this file is deliberately not on that list). What
/// is asserted here is the NEW half: the same refusal, with the same message,
/// from `from_url_with`, and the read-only flag surviving `read_only_with`.
#[test]
fn the_new_constructors_do_not_widen_the_write_surface() {
    let _l = env_lock();
    let _g = EnvGuard::clear(&AWS_VARS);
    let deep = StorageUrl::S3 {
        bucket: "b".into(),
        prefix: "logweir/prod".into(),
        region: None,
        endpoint: Some("https://s3.example.com".into()),
        path_style: true,
        allow_http: false,
    };
    let Err(err) = Store::from_url_with(&deep, &StoreOptions::static_keys("a", "b", None)) else {
        panic!("Global Constraint 6 must refuse a prefix deeper than `logweir/`");
    };
    let m = format!("{err}");
    assert!(m.contains("must be exactly `logweir/`"), "{m}");
    assert!(m.contains("Global Constraint 6"), "{m}");
    assert!(
        m.contains("logweir/prod"),
        "the refusal must name what it refused: {m}"
    );

    // A refusal that rejected everything would satisfy the assertion above and
    // break every destination-backed run.
    let evidence = StorageUrl::S3 {
        bucket: "b".into(),
        prefix: "logweir/".into(),
        region: None,
        endpoint: Some("https://s3.example.com".into()),
        path_style: true,
        allow_http: false,
    };
    assert!(Store::from_url_with(&evidence, &StoreOptions::static_keys("a", "b", None)).is_ok());

    // `read_only_with` keeps the read-only flag, so the handle physically
    // cannot put — the same guarantee `read_only_from_url` gives, and the
    // reason the archive prefix may be read without becoming writable.
    let archive =
        Store::read_only_with(&https_tls(), &StoreOptions::static_keys("a", "b", None)).unwrap();
    assert!(matches!(
        archive.put_create_only("logweir/x.json", b"{}"),
        Err(StoreError::ReadOnly(_))
    ));
}

// ------------------------------------------------------- classification

/// The classifier's table, pinned by messages in the shape object_store
/// actually produces. A token scan is a tripwire on spellings and nothing
/// more; what makes it safe to act on is that every token has a case here and
/// that an unmatched message answers `StoreErrorUnclassified` rather than
/// guessing.
#[test]
fn the_classifier_table() {
    use StoreErrorClass as C;
    let cases: [(&str, C); 14] = [
        (
            "Generic S3 error: Error performing GET https://minio:9000/b/k: response error \"<?xml version=\\\"1.0\\\"?><Error><Code>AccessDenied</Code><Message>Access Denied.</Message></Error>\", after 0 retries: HTTP status client error (403 Forbidden)",
            C::AccessDenied,
        ),
        (
            "The operation lacked the necessary privileges to complete for path b/k: 403",
            C::AccessDenied,
        ),
        (
            "response error \"<Error><Code>InvalidAccessKeyId</Code></Error>\"",
            C::InvalidCredentials,
        ),
        (
            "response error \"<Error><Code>SignatureDoesNotMatch</Code></Error>\"",
            C::InvalidCredentials,
        ),
        (
            "response error \"<Error><Code>ExpiredToken</Code></Error>\"",
            C::InvalidCredentials,
        ),
        (
            "The operation lacked valid authentication credentials for path b/k",
            C::InvalidCredentials,
        ),
        (
            "response error \"<Error><Code>NoSuchBucket</Code></Error>\"",
            C::BucketNotFound,
        ),
        (
            "response error \"<Error><Code>NoSuchKey</Code></Error>\"",
            C::ObjectNotFound,
        ),
        (
            "response error \"<Error><Code>PermanentRedirect</Code></Error>\"",
            C::RegionMismatch,
        ),
        (
            "<Error><Code>AuthorizationHeaderMalformed</Code><Message>the region us-east-1 is wrong; expecting eu-west-1</Message></Error>",
            C::RegionMismatch,
        ),
        (
            "error sending request for url (https://minio:9000/b): invalid peer certificate: UnknownIssuer",
            C::TlsTrustFailed,
        ),
        (
            "Generic S3 error: error sending request: operation timed out",
            C::Timeout,
        ),
        (
            "error sending request for url (https://minio:9000/b): tcp connect error: Connection refused (os error 61)",
            C::EndpointUnreachable,
        ),
        (
            "Generic S3 error: something nobody has a table entry for",
            C::StoreErrorUnclassified,
        ),
    ];
    for (message, want) in cases {
        let got = StoreErrorClass::classify(&StoreError::Io(message.to_string()));
        assert_eq!(got, want, "classifying: {message}");
    }
}

/// PLAT-08.1's `store_errors::access_denied_is_classified_without_body`: the
/// CLASS is a code, and a code carries no body. Combined with
/// `logweir_core::check_contract::redact`, the S3 XML that produced the class
/// never reaches a status.
#[test]
fn access_denied_is_classified_without_body() {
    let raw = "Generic S3 error: response error \"<?xml version=\"1.0\"?><Error>\
               <Code>AccessDenied</Code><Message>User arn:aws:iam::1:user/backup is not \
               authorized to perform s3:ListBucket</Message><RequestId>17C3E1</RequestId>\
               <HostId>abc</HostId></Error>\", after 0 retries";
    let err = StoreError::Io(raw.to_string());
    let class = StoreErrorClass::classify(&err);
    assert_eq!(class, StoreErrorClass::AccessDenied);
    assert_eq!(class.as_str(), "AccessDenied");
    // The class alone is what a status stores; it carries no request id, no
    // host id and no principal ARN.
    assert!(!class.as_str().contains("arn:aws"));

    let redacted = logweir_core::check_contract::redact(raw);
    assert!(!redacted.contains("<Message>"), "{redacted}");
    assert!(!redacted.contains("RequestId"), "{redacted}");
    assert!(redacted.contains("AccessDenied"), "{redacted}");
}

/// A genuine absence is never a denial. The variant carries that distinction
/// and the classifier must not lose it — `StoreError::NotFound`'s own doc
/// comment exists because collapsing the two put an unestablished claim into a
/// signed scorecard once already.
#[test]
fn not_found_stays_not_found() {
    assert_eq!(
        StoreErrorClass::classify(&StoreError::NotFound("logweir/x.json".into())),
        StoreErrorClass::ObjectNotFound
    );
    assert_eq!(
        StoreErrorClass::classify(&StoreError::ReadOnly("logweir/x.json".into())),
        StoreErrorClass::StoreErrorUnclassified,
        "a local refusal is not a fact about the bucket"
    );
}

/// The class names ARE `logweir_core::check_contract::CheckCode` spellings, so
/// a check result can carry one verbatim. A rename on either side fails here
/// instead of producing a condition reason no UI has text for.
#[test]
fn the_class_names_are_check_codes() {
    use logweir_core::check_contract::CheckCode;
    for c in StoreErrorClass::ALL {
        assert!(
            CheckCode::parse(c.as_str()).is_some(),
            "`{}` is not in the check-code table",
            c.as_str()
        );
    }
    assert_eq!(
        StoreErrorClass::ALL.len(),
        9,
        "D2 §4.2 lists nine store codes"
    );
}

/// The raw-error entry point uses object_store's STRUCTURED variants where it
/// can, so a `PermissionDenied` is `AccessDenied` even when the body is empty.
#[test]
fn the_raw_classifier_uses_structured_variants() {
    let denied = object_store::Error::PermissionDenied {
        path: "b/k".into(),
        source: "403".into(),
    };
    assert_eq!(
        StoreErrorClass::classify_object_store(&denied),
        StoreErrorClass::AccessDenied
    );
    let unauth = object_store::Error::Unauthenticated {
        path: "b/k".into(),
        source: "401".into(),
    };
    assert_eq!(
        StoreErrorClass::classify_object_store(&unauth),
        StoreErrorClass::InvalidCredentials
    );
    let missing_bucket = object_store::Error::NotFound {
        path: "b".into(),
        source: "<Error><Code>NoSuchBucket</Code></Error>".into(),
    };
    assert_eq!(
        StoreErrorClass::classify_object_store(&missing_bucket),
        StoreErrorClass::BucketNotFound,
        "`the bucket is not there` is a different remedy from `the key is not there`"
    );
    let missing_key = object_store::Error::NotFound {
        path: "b/k".into(),
        source: "<Error><Code>NoSuchKey</Code></Error>".into(),
    };
    assert_eq!(
        StoreErrorClass::classify_object_store(&missing_key),
        StoreErrorClass::ObjectNotFound
    );
    // A structured PermissionDenied whose body names a credential problem is
    // reported as the credential problem: a wrong key is not a missing grant.
    let wrong_key = object_store::Error::PermissionDenied {
        path: "b/k".into(),
        source: "<Error><Code>SignatureDoesNotMatch</Code></Error>".into(),
    };
    assert_eq!(
        StoreErrorClass::classify_object_store(&wrong_key),
        StoreErrorClass::InvalidCredentials
    );
}

#[test]
fn the_default_options_are_todays_behaviour() {
    let d = StoreOptions::default();
    assert_eq!(d.credentials, CredentialSource::Ambient);
    assert!(d.root_certificates.is_empty());
    assert!(!d.pins_instance_metadata());
    assert!(d.request_timeout.is_none());
    assert!(d.max_retries.is_none());
    assert!(d.reads_environment());
}
