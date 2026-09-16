//! The pure destination model (decision D2 §3.1-§3.4), rule by rule.
//!
//! Every rule here is asserted in BOTH directions — a violating value is
//! refused AND a neighbouring legal value is accepted. A one-directional test
//! is satisfied by a rule that always fails, which is the shape of guard this
//! repository has already been bitten by ("a guard without a mutant is not a
//! guard"): each `…_mutant` test below states the planted regression the
//! positive half kills.

use logweir_core::destination::{
    engine_compatible, validate, validate_ca_bundle, validate_transition, Addressing,
    DestinationLocation, DestinationRole, StorageProvider, TransportSecurity, EVIDENCE_PREFIX,
};
use logweir_core::engine::StorageUrl;

fn tls_minio() -> DestinationLocation {
    DestinationLocation {
        provider: StorageProvider::S3,
        bucket: "kafka-backups".into(),
        prefix: "team-a/prod".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("https://minio.storage.svc:9000".into()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::Tls,
    }
}

fn aws_plain() -> DestinationLocation {
    DestinationLocation {
        provider: StorageProvider::S3,
        bucket: "kafka-backups".into(),
        prefix: String::new(),
        region: Some("eu-west-1".into()),
        endpoint: None,
        addressing: Addressing::VirtualHosted,
        transport: TransportSecurity::Tls,
    }
}

fn fields(loc: &DestinationLocation) -> Vec<String> {
    match validate(loc) {
        Ok(()) => Vec::new(),
        Err(errs) => errs
            .into_iter()
            .map(|e| format!("{}:{}", e.rule, e.field))
            .collect(),
    }
}

// ------------------------------------------------------------------ R3 / S5

/// R3, both halves, plus the property the whole rule exists for: the
/// ADDRESSING field does not appear in the transport decision at all.
#[test]
fn transport_must_match_the_endpoint_scheme() {
    // TLS with https, TLS with no endpoint, InsecureHTTP with http: legal.
    assert!(validate(&tls_minio()).is_ok());
    assert!(validate(&aws_plain()).is_ok());
    let mut http = tls_minio();
    http.endpoint = Some("http://minio.storage.svc:9000".into());
    http.transport = TransportSecurity::InsecureHttp;
    assert!(validate(&http).is_ok(), "{:?}", validate(&http));

    // TLS with an http:// endpoint: refused.
    let mut bad = tls_minio();
    bad.endpoint = Some("http://minio.storage.svc:9000".into());
    assert_eq!(fields(&bad), vec!["R3:spec.transport.security"]);

    // InsecureHTTP with an https:// endpoint: refused.
    let mut bad = tls_minio();
    bad.transport = TransportSecurity::InsecureHttp;
    assert_eq!(fields(&bad), vec!["R3:spec.transport.security"]);

    // InsecureHTTP with NO endpoint: refused. "Plaintext to AWS S3" is not a
    // thing anyone can have meant.
    let mut bad = aws_plain();
    bad.transport = TransportSecurity::InsecureHttp;
    assert_eq!(fields(&bad), vec!["R3:spec.transport.security"]);
}

/// D-SEAMS S5 and tracker defect UI-HTTPDOWNGRADE, as a property rather than
/// a code reading: flipping ONLY the addressing style changes neither the
/// validity of a location nor `allow_http`.
///
/// MUTANT: making `archive_storage_url`'s `allow_http` read
/// `self.addressing.is_path_style()` — the exact shape of
/// `ui/pages/restore-wizard.js:1148` — turns the second assertion red.
#[test]
fn addressing_never_changes_transport_or_allow_http() {
    for transport in [TransportSecurity::Tls, TransportSecurity::InsecureHttp] {
        for addressing in [Addressing::PathStyle, Addressing::VirtualHosted] {
            let mut loc = tls_minio();
            loc.transport = transport;
            loc.endpoint = Some(match transport {
                TransportSecurity::Tls => "https://minio.storage.svc:9000".into(),
                TransportSecurity::InsecureHttp => "http://minio.storage.svc:9000".into(),
            });
            loc.addressing = addressing;
            assert!(validate(&loc).is_ok(), "{addressing:?}/{transport:?}");
            let StorageUrl::S3 {
                allow_http,
                path_style,
                ..
            } = loc.archive_storage_url()
            else {
                panic!("an S3 location renders an S3 storage url");
            };
            assert_eq!(
                allow_http,
                transport == TransportSecurity::InsecureHttp,
                "allow_http must come from transport alone ({addressing:?}/{transport:?})"
            );
            assert_eq!(path_style, addressing == Addressing::PathStyle);
        }
    }
}

// ------------------------------------------------------------------ R5

/// R5: the endpoint is an ORIGIN. The eight refusals are the ones an operator
/// actually types; the three acceptances are the forms the chart documents.
#[test]
fn endpoint_must_be_an_origin() {
    let legal = [
        "https://minio.storage.svc:9000",
        "https://s3.example.com",
        "https://s3.example.com/",
        "https://[2001:db8::1]:9000",
        "https://10.0.0.5:9000",
    ];
    for e in legal {
        let mut loc = tls_minio();
        loc.endpoint = Some(e.into());
        assert!(
            validate(&loc).is_ok(),
            "{e} must be accepted: {:?}",
            fields(&loc)
        );
    }
    let illegal = [
        "https://user:pass@minio.storage.svc:9000", // userinfo
        "https://minio.storage.svc:9000/bucket",    // path
        "https://minio.storage.svc:9000?x=1",       // query
        "https://minio.storage.svc:9000#frag",      // fragment
        "ftp://minio.storage.svc:9000",             // scheme
        "minio.storage.svc:9000",                   // no scheme
        "https://",                                 // no host
        "https://minio.storage.svc:900000",         // port too long
        "https://-minio.storage.svc",               // label starts with '-'
    ];
    for e in illegal {
        let mut loc = tls_minio();
        loc.endpoint = Some(e.into());
        assert!(
            fields(&loc).iter().any(|f| f.starts_with("R5:")),
            "{e} must be refused by R5, got {:?}",
            fields(&loc)
        );
    }
}

// ------------------------------------------------------------------ R6

/// R6: the prefix is relative, has no empty/`.`/`..` segment, and may never be
/// Global Constraint 6's evidence root.
#[test]
fn prefix_is_relative_and_never_the_evidence_root() {
    for p in ["", "team-a", "team-a/prod", "a.b/c-d/e_f", "logweirish"] {
        let mut loc = tls_minio();
        loc.prefix = p.into();
        assert!(
            validate(&loc).is_ok(),
            "`{p}` must be accepted: {:?}",
            fields(&loc)
        );
    }
    for p in [
        "/team-a",
        "team-a/",
        "team-a//prod",
        "team-a/../prod",
        "./team-a",
        "..",
        "logweir",
        "logweir/readiness",
        "team a",
        "team-a/prod?x",
    ] {
        let mut loc = tls_minio();
        loc.prefix = p.into();
        assert!(
            fields(&loc).iter().any(|f| f.starts_with("R6:")),
            "`{p}` must be refused by R6, got {:?}",
            fields(&loc)
        );
    }
}

// --------------------------------------------------------- bucket and region

#[test]
fn bucket_and_region_patterns() {
    for b in ["abc", "kafka-backups", "a.b-c", &"a".repeat(63)] {
        let mut loc = tls_minio();
        loc.bucket = b.into();
        assert!(validate(&loc).is_ok(), "`{b}` must be accepted");
    }
    for b in ["ab", "-abc", "abc-", "ABC", "a_b", &"a".repeat(64)] {
        let mut loc = tls_minio();
        loc.bucket = b.to_string();
        assert!(
            fields(&loc)
                .iter()
                .any(|f| f.ends_with("spec.storage.bucket")),
            "`{b}` must be refused, got {:?}",
            fields(&loc)
        );
    }
    let mut loc = tls_minio();
    loc.region = Some("US-EAST-1".into());
    assert!(fields(&loc)
        .iter()
        .any(|f| f.ends_with("spec.storage.region")));
    loc.region = Some("us-east-1".into());
    assert!(validate(&loc).is_ok());
}

/// Every failure, not the first: an API that reports one of three mistakes
/// makes the operator submit three times.
///
/// MUTANT: returning `Err(vec![first])` from `validate` leaves this red.
#[test]
fn validation_reports_every_failure_at_once() {
    let loc = DestinationLocation {
        provider: StorageProvider::S3,
        bucket: "NOPE".into(),
        prefix: "logweir/x".into(),
        region: Some("Bad Region".into()),
        endpoint: Some("http://minio:9000/path".into()),
        addressing: Addressing::PathStyle,
        transport: TransportSecurity::Tls,
    };
    let got = fields(&loc);
    assert_eq!(got.len(), 5, "{got:?}");
}

// ------------------------------------------------------------------ R4

#[test]
fn a_ca_bundle_requires_tls() {
    assert!(validate_ca_bundle(TransportSecurity::Tls, true).is_ok());
    assert!(validate_ca_bundle(TransportSecurity::InsecureHttp, false).is_ok());
    let err = validate_ca_bundle(TransportSecurity::InsecureHttp, true).unwrap_err();
    assert_eq!(err.rule, "R4");
    assert_eq!(err.field, "spec.transport.caBundle");
}

// ------------------------------------------------------------- R1 / R2

/// R1 and R2. Transport immutability is asserted in BOTH directions: an
/// "upgrade" is refused too, because it would silently change what an already
/// frozen execution input meant.
#[test]
fn location_and_transport_are_immutable() {
    let old = tls_minio();
    assert!(validate_transition(&old, &old.clone()).is_ok());

    let mut moved = old.clone();
    moved.bucket = "other-bucket".into();
    let rules: Vec<&str> = validate_transition(&old, &moved)
        .unwrap_err()
        .iter()
        .map(|e| e.rule)
        .collect();
    assert_eq!(rules, vec!["R1"]);

    // Addressing is part of the LOCATION, so flipping it is R1 and never R2.
    let mut readdressed = old.clone();
    readdressed.addressing = Addressing::VirtualHosted;
    let rules: Vec<&str> = validate_transition(&old, &readdressed)
        .unwrap_err()
        .iter()
        .map(|e| e.rule)
        .collect();
    assert_eq!(
        rules,
        vec!["R1"],
        "addressing is a location change, not a transport change"
    );

    // Downgrade AND upgrade.
    let mut http = old.clone();
    http.transport = TransportSecurity::InsecureHttp;
    http.endpoint = Some("http://minio.storage.svc:9000".into());
    let rules: Vec<&str> = validate_transition(&old, &http)
        .unwrap_err()
        .iter()
        .map(|e| e.rule)
        .collect();
    assert_eq!(rules, vec!["R1", "R2"]);
    let rules: Vec<&str> = validate_transition(&http, &old)
        .unwrap_err()
        .iter()
        .map(|e| e.rule)
        .collect();
    assert_eq!(
        rules,
        vec!["R1", "R2"],
        "an upgrade is a transport change too"
    );
}

// ------------------------------------------------------------------ G4

/// D2 G4 / tracker defect ENGINE-PATHSTYLE: engine 0.21.0 forces path-style
/// whenever an endpoint is set, so virtual-hosted WITH an endpoint is refused
/// rather than advertised.
#[test]
fn custom_endpoint_requires_path_style_for_engine() {
    let mut loc = tls_minio();
    loc.addressing = Addressing::VirtualHosted;
    assert!(engine_compatible(&loc).is_err());

    loc.addressing = Addressing::PathStyle;
    assert!(engine_compatible(&loc).is_ok());

    // No endpoint: virtual-hosted is AWS's own default and is fine.
    assert!(engine_compatible(&aws_plain()).is_ok());

    // It is an ENGINE compatibility question, not a validity question: the
    // object is well-formed and CEL admits it (D2 §3.3 makes it a controller
    // condition, not a CEL rule, because the engine version can change it).
    let mut loc = tls_minio();
    loc.addressing = Addressing::VirtualHosted;
    assert!(validate(&loc).is_ok());
}

// -------------------------------------------------- urls, digests, evidence

/// PLAT-08.1's "two destinations with different settings work without global
/// configuration leakage", at the layer that renders the settings.
#[test]
fn two_locations_render_distinct_storage_urls_and_digests() {
    let a = tls_minio();
    let mut b = tls_minio();
    b.bucket = "lw-b".into();
    b.prefix = String::new();
    b.endpoint = Some("http://minio-b.storage.svc:9000".into());
    b.transport = TransportSecurity::InsecureHttp;

    assert_ne!(a.location_digest(), b.location_digest());
    assert_ne!(a.canonical_url(), b.canonical_url());
    assert_eq!(a.canonical_url(), "s3://kafka-backups/team-a/prod");
    assert_eq!(b.canonical_url(), "s3://lw-b");
    assert_ne!(a.archive_storage_url(), b.archive_storage_url());
    assert_ne!(a.evidence_storage_url(), b.evidence_storage_url());

    let StorageUrl::S3 { allow_http, .. } = a.archive_storage_url() else {
        unreachable!()
    };
    assert!(!allow_http);
    let StorageUrl::S3 { allow_http, .. } = b.archive_storage_url() else {
        unreachable!()
    };
    assert!(allow_http);
}

/// The digest answers "is this the same place", so the route must not be in
/// it — and the place must.
#[test]
fn the_location_digest_covers_place_and_not_route() {
    let base = tls_minio();
    // Route-only changes: same digest.
    let mut path_style = base.clone();
    path_style.addressing = Addressing::VirtualHosted;
    assert_eq!(base.location_digest(), path_style.location_digest());

    let mut http = base.clone();
    http.transport = TransportSecurity::InsecureHttp;
    http.endpoint = Some("http://minio.storage.svc:9000".into());
    assert_eq!(
        base.location_digest(),
        http.location_digest(),
        "scheme and transport describe how the bucket is reached, not where it is"
    );

    // Place changes: different digest.
    for mutate in [
        (|l: &mut DestinationLocation| l.bucket = "other".into()) as fn(&mut DestinationLocation),
        |l: &mut DestinationLocation| l.prefix = "team-b".into(),
        |l: &mut DestinationLocation| l.endpoint = Some("https://other.svc:9000".into()),
    ] {
        let mut m = base.clone();
        mutate(&mut m);
        assert_ne!(base.location_digest(), m.location_digest());
    }

    // Host identity is case-insensitive and scheme-free; the region only
    // matters when there is no endpoint.
    let mut upper = base.clone();
    upper.endpoint = Some("https://MINIO.storage.svc:9000/".into());
    assert_eq!(base.location_digest(), upper.location_digest());
    assert_eq!(base.host_identity(), "minio.storage.svc:9000");

    let mut other_region = base.clone();
    other_region.region = Some("eu-west-1".into());
    assert_eq!(
        base.location_digest(),
        other_region.location_digest(),
        "with an explicit endpoint the region does not name the place"
    );
    let mut aws_a = aws_plain();
    let mut aws_b = aws_plain();
    aws_a.region = Some("eu-west-1".into());
    aws_b.region = Some("us-east-1".into());
    assert_ne!(
        aws_a.location_digest(),
        aws_b.location_digest(),
        "without an endpoint the region IS the host identity"
    );
    assert_eq!(aws_a.host_identity(), "aws/eu-west-1");
}

/// The digest is a value other tasks freeze into recovery points (D2 §3.7) and
/// compare on restore (§3.12), so its bytes are pinned here. A change to the
/// preimage is a change to that contract and must be a reviewed diff.
#[test]
fn the_location_digest_preimage_is_pinned() {
    let d = tls_minio().location_digest();
    let expected = logweir_core::ids::sha256_prefixed(
        b"s3\nminio.storage.svc:9000\nkafka-backups\nteam-a/prod\n",
    );
    assert_eq!(d, expected);
    assert!(d.starts_with("sha256:") && d.len() == 71);
}

/// D2 §3.4 / G7: evidence lives at the bucket root under `logweir/`, on the
/// same route as the archive. This is the prefix `Store::from_url` demands.
#[test]
fn evidence_url_is_bucket_root_logweir() {
    let a = tls_minio();
    let StorageUrl::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        path_style,
        allow_http,
    } = a.evidence_storage_url()
    else {
        unreachable!()
    };
    assert_eq!(bucket, "kafka-backups");
    assert_eq!(prefix, EVIDENCE_PREFIX);
    assert_eq!(prefix, "logweir/");
    assert_eq!(region.as_deref(), Some("us-east-1"));
    assert_eq!(endpoint.as_deref(), Some("https://minio.storage.svc:9000"));
    assert!(path_style);
    assert!(!allow_http);

    // The archive prefix is NOT touched by the evidence rendering.
    let StorageUrl::S3 { prefix, .. } = a.archive_storage_url() else {
        unreachable!()
    };
    assert_eq!(prefix, "team-a/prod");
}

/// PLAT-08.2's "HTTPS with path-style": the one combination operators most
/// often believe implies HTTP.
#[test]
fn https_path_style_renders_allow_http_false() {
    let loc = tls_minio();
    assert!(loc.addressing.is_path_style());
    let StorageUrl::S3 {
        allow_http,
        path_style,
        ..
    } = loc.archive_storage_url()
    else {
        unreachable!()
    };
    assert!(path_style);
    assert!(!allow_http);
}

/// PLAT-08.2's "explicitly configured local HTTP": nothing but an explicit
/// `http://` endpoint plus an explicit `InsecureHTTP` gets it.
#[test]
fn insecure_http_requires_explicit_http_endpoint() {
    let mut loc = tls_minio();
    loc.transport = TransportSecurity::InsecureHttp;
    loc.endpoint = None;
    assert!(validate(&loc).is_err());
    loc.endpoint = Some("https://minio.storage.svc:9000".into());
    assert!(validate(&loc).is_err());
    loc.endpoint = Some("http://minio.storage.svc:9000".into());
    assert!(validate(&loc).is_ok());
    let StorageUrl::S3 { allow_http, .. } = loc.archive_storage_url() else {
        unreachable!()
    };
    assert!(allow_http);
}

#[test]
fn the_four_roles_round_trip_and_are_stable() {
    assert_eq!(DestinationRole::ALL.len(), 4);
    for r in DestinationRole::ALL {
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, format!("\"{}\"", r.as_str()));
        let back: DestinationRole = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }
    assert_eq!(
        DestinationRole::ALL.map(DestinationRole::as_str),
        [
            "ArchiveWrite",
            "ArchiveRead",
            "EvidenceWrite",
            "EvidenceRead"
        ]
    );
}

/// The CRD spells these two enums; a rename here silently breaks every
/// existing object, so the wire strings are pinned.
#[test]
fn transport_and_addressing_wire_spellings_are_pinned() {
    assert_eq!(
        serde_json::to_string(&TransportSecurity::Tls).unwrap(),
        "\"TLS\""
    );
    assert_eq!(
        serde_json::to_string(&TransportSecurity::InsecureHttp).unwrap(),
        "\"InsecureHTTP\""
    );
    assert_eq!(
        serde_json::to_string(&Addressing::PathStyle).unwrap(),
        "\"PathStyle\""
    );
    assert_eq!(
        serde_json::to_string(&Addressing::VirtualHosted).unwrap(),
        "\"VirtualHosted\""
    );
    let loc: DestinationLocation = serde_json::from_str(
        r#"{"provider":"S3","bucket":"b","prefix":"p","addressing":"PathStyle","transport":"TLS"}"#,
    )
    .unwrap();
    assert_eq!(loc.transport, TransportSecurity::Tls);
    assert!(serde_json::from_str::<DestinationLocation>(
        r#"{"provider":"S3","bucket":"b","addressing":"PathStyle","transport":"TLS","nope":1}"#
    )
    .is_err());
}
