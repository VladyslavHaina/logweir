//! The runner half of the saved-connection contract's TLS trust — PLAT-07.1,
//! Global Constraint 29.
//!
//! A private CA has to reach BOTH of a runner's TLS clients: librdkafka's
//! `ssl.ca.location` and the engine's `ssl_ca_location`. The controller
//! projects one file and names it in one variable per side; these rows assert
//! that the variable is read once, that both attachments come from that one
//! value, and that a CA can never sit beside a connection dialled in the
//! clear.
//!
//! NOTHING HERE DIALS. `AuthConfig` and `AuthRender` are values; the engine
//! renderer is a function from a plan to bytes.

use logweir::tls_ca::{projected_ca_file, SOURCE_TLS_CA_FILE_VAR, TARGET_TLS_CA_FILE_VAR};
use logweir_core::engine::AuthRender;
use logweir_core::spec::AuthSpec;
use logweir_kafka::reader::{AuthConfig, KafkaError};

const CA: &str = "/connection/source-ca/ca.crt";

/// The two variables are the contract's own names, and they differ per side.
#[test]
fn the_two_variables_are_the_shared_contract_names() {
    assert_eq!(
        SOURCE_TLS_CA_FILE_VAR,
        logweir_core::connection::SOURCE_TLS_CA_FILE_ENV
    );
    assert_eq!(
        TARGET_TLS_CA_FILE_VAR,
        logweir_core::connection::TARGET_TLS_CA_FILE_ENV
    );
    assert_ne!(SOURCE_TLS_CA_FILE_VAR, TARGET_TLS_CA_FILE_VAR);
}

/// Unset is `None`, **and so is blank** — plan erratum E19(e): a Kubernetes
/// `env:` entry with an empty `value:` reads as `Ok("")`, and a CA location of
/// `""` is not a file anybody meant.
///
/// Each row uses a variable name of its own, so the rows cannot race each
/// other inside one test binary.
#[test]
fn a_blank_ca_variable_is_unset() {
    assert_eq!(
        projected_ca_file("LOGWEIR_TEST_CA_ABSENT_9f1"),
        Ok(None),
        "an unset variable names no CA"
    );
    for (name, value, expected) in [
        ("LOGWEIR_TEST_CA_EMPTY_9f2", "", None),
        ("LOGWEIR_TEST_CA_BLANK_9f3", "   ", None),
        ("LOGWEIR_TEST_CA_SET_9f4", CA, Some(CA.to_string())),
    ] {
        // SAFETY-BY-CONVENTION: each row owns a name no other test reads.
        std::env::set_var(name, value);
        assert_eq!(
            projected_ca_file(name),
            Ok(expected),
            "for {name}={value:?}"
        );
        std::env::remove_var(name);
    }
}

/// librdkafka's half: the path is attached to a SCRAM-over-TLS client and
/// refused for anything else, so a trust anchor never sits beside a clear dial.
#[test]
fn the_client_takes_a_ca_only_over_tls() {
    let scram_tls = AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    };
    let auth = AuthConfig::from_spec(&scram_tls, Some("pw".into()))
        .expect("a password was projected")
        .with_tls_ca_file(Some(CA.to_string()))
        .expect("a CA over TLS is attached");
    match auth {
        AuthConfig::ScramSha512 { tls_ca_file, .. } => {
            assert_eq!(tls_ca_file.as_deref(), Some(CA))
        }
        other => panic!("{other:?}"),
    }
    // …and the Debug still redacts the password while showing the path, which
    // is not a credential.
    let auth = AuthConfig::from_spec(&scram_tls, Some("hunter2".into()))
        .expect("builds")
        .with_tls_ca_file(Some(CA.to_string()))
        .expect("attaches");
    let debugged = format!("{auth:?}");
    assert!(
        debugged.contains(CA) && debugged.contains("***") && !debugged.contains("hunter2"),
        "{debugged}"
    );

    for spec in [
        AuthSpec::Plaintext,
        AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        },
    ] {
        let err = AuthConfig::from_spec(&spec, Some("pw".into()))
            .expect("builds")
            .with_tls_ca_file(Some(CA.to_string()))
            .expect_err("a CA with no TLS transport is refused");
        assert!(matches!(err, KafkaError::Client(_)), "{err:?}");
        let text = err.to_string();
        assert!(
            text.contains("silent downgrade") && text.contains(spec.mode_str()),
            "the refusal says why, and names the mode: {text}"
        );
    }

    // `None` changes nothing at all.
    let plain = AuthConfig::from_spec(&AuthSpec::Plaintext, None)
        .expect("builds")
        .with_tls_ca_file(None)
        .expect("no CA, no change");
    assert!(matches!(plain, AuthConfig::Plaintext));
}

/// The engine's half: the same path becomes `ssl_ca_location`, under
/// `security:`, and only for a TLS connection.
#[test]
fn the_engine_document_carries_the_same_path_as_ssl_ca_location() {
    let with_ca = AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    }
    .to_render()
    .with_tls_ca_file(Some(CA.to_string()))
    .expect("a CA over TLS is attached");
    assert_eq!(
        with_ca,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: true,
            tls_ca_file: Some(CA.to_string()),
        }
    );
    for spec in [
        AuthSpec::Plaintext,
        AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        },
    ] {
        spec.to_render()
            .with_tls_ca_file(Some(CA.to_string()))
            .expect_err("a CA with no TLS transport is refused on the engine side too");
    }
}

/// A backup plan carries the CA only when one was projected, and refuses it on
/// a source that is not SCRAM over TLS.
#[test]
fn a_backup_plan_takes_the_projected_ca_or_refuses_it() {
    let spec = |auth: AuthSpec| logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: vec!["b0.orders:9093".into()],
            auth,
            topics: vec!["orders".into()],
        },
        storage: logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "logweir".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        backup_id: "b1".into(),
        backup: logweir_core::spec::BackupSettings::default(),
    };
    let tls = spec(AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    });
    let plan = logweir::backup::build_plan_with_tls_ca(&tls, "b1", Some(CA.to_string()))
        .expect("a CA over TLS is attached");
    assert_eq!(
        plan.source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: true,
            tls_ca_file: Some(CA.to_string()),
        }
    );
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("the document renders");
    assert!(
        doc.contains(&format!("    ssl_ca_location: \"{CA}\"\n")),
        "the engine's own key, under `security:`, indented with it and quoted like every other \
         interpolated scalar: {doc}"
    );

    // …and with no CA the document is exactly what it was before this field
    // existed: no `ssl_ca_location` line at all.
    let plan = logweir::backup::build_plan_with_tls_ca(&tls, "b1", None).expect("no CA");
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("the document renders");
    assert!(!doc.contains("ssl_ca_location"), "{doc}");

    // A CA on a plaintext source is an operational refusal, before the engine.
    let plaintext = spec(AuthSpec::Plaintext);
    let err = logweir::backup::build_plan_with_tls_ca(&plaintext, "b1", Some(CA.to_string()))
        .expect_err("refused");
    assert_eq!(
        logweir::exit::ExitCode::from(err),
        logweir::exit::ExitCode::Operational,
        "nothing was refused about the PLAN; the controller never projects this shape, so it is \
         a hand-built Job and the fix is configuration"
    );
}

/// **ONE PROJECTED PATH, BOTH TLS CLIENTS, MEASURED** — PLAT-07.1 review
/// finding H2.
///
/// Global Constraint 29's runner half was stated in prose and asserted in two
/// halves that never met: `the_client_takes_a_ca_only_over_tls` checked the
/// shape of `AuthConfig`, `the_engine_document_carries_the_same_path_as_
/// ssl_ca_location` checked the rendered document, and nothing compared the
/// two. A review mutant that denied the backup runner's OWN librdkafka reader
/// the projected CA — while the engine still received it — survived all 725
/// rows.
///
/// This row starts where the runner starts: one environment variable, read
/// through the one reader (`tls_ca::projected_ca_file`). From that single
/// value it builds what librdkafka is actually handed
/// (`RdKafkaReader::client_config`, the map `connect` dials with) and what the
/// engine is actually handed (the rendered `backup.yaml`), and asserts the
/// SAME string appears as `ssl.ca.location` and as `ssl_ca_location`.
///
/// Half-wiring is fail-closed today (the un-CA'd client falls back to the
/// image's public roots and the handshake fails), so the blast radius is a
/// broken run rather than an insecure dial — but it survives every gate, and
/// D2's discovery and preflight check Jobs reuse this seam.
///
/// NOTHING DIALS: `client_config` returns a map, and `create()` is never
/// called on it.
#[test]
fn one_projected_path_reaches_both_tls_clients() {
    // The runner's own read, from the source side's variable. A name no other
    // row in this binary reads, so the rows cannot race.
    const VAR: &str = "LOGWEIR_TEST_CA_BOTH_CLIENTS_7c1";
    std::env::set_var(VAR, CA);
    let projected = projected_ca_file(VAR).expect("the variable is readable");
    std::env::remove_var(VAR);
    assert_eq!(projected.as_deref(), Some(CA));

    let source = AuthSpec::ScramSha512 {
        username: "logweir".into(),
        tls: true,
    };
    let spec = logweir_core::spec::BackupSpec {
        source: logweir_core::spec::BackupSourceSpec {
            bootstrap_servers: vec!["b0.orders:9093".into()],
            auth: source.clone(),
            topics: vec!["orders".into()],
        },
        storage: logweir_core::engine::StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "logweir".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        backup_id: "b1".into(),
        backup: logweir_core::spec::BackupSettings::default(),
    };

    // CLIENT ONE — librdkafka, exactly as `backup::run` builds it.
    let auth = AuthConfig::from_spec(&source, Some("pw".into()))
        .expect("a password was projected")
        .with_tls_ca_file(projected.clone())
        .expect("a CA over TLS is attached");
    let config = logweir_kafka::rdkafka_reader::RdKafkaReader::client_config(
        &spec.source.bootstrap_servers,
        &auth,
    )
    .expect("the client configures");
    let client_ca = config
        .get("ssl.ca.location")
        .expect("librdkafka is told which CA to trust")
        .to_string();

    // CLIENT TWO — the engine, from the same value through the same entry
    // point `backup::run` uses.
    let plan = logweir::backup::build_plan_with_tls_ca(&spec, "b1", projected.clone())
        .expect("the plan takes the CA");
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("the document renders");
    let engine_ca = doc
        .lines()
        .find_map(|l| l.trim().strip_prefix("ssl_ca_location: "))
        .expect("the engine is told which CA to trust")
        .trim_matches('"')
        .to_string();

    assert_eq!(
        client_ca, engine_ca,
        "the two TLS clients must trust the SAME file; one trusting the private CA and the other \
         the image's public roots is the half-wiring this row exists to catch"
    );
    assert_eq!(
        client_ca, CA,
        "and that file is the one the controller projected, verbatim — no path is derived, \
         rewritten or defaulted on either side"
    );

    // Hostname verification travels with it: the engine's rustls client
    // verifies the broker hostname with no way to turn that off, so
    // librdkafka must be pinned to the same behaviour (finding H1).
    assert_eq!(
        config.get("ssl.endpoint.identification.algorithm"),
        Some("https")
    );

    // AND THE NEGATIVE: with nothing projected, neither client names a CA.
    // A default that quietly pointed one of them somewhere would be the same
    // defect in the other direction.
    let auth = AuthConfig::from_spec(&source, Some("pw".into()))
        .expect("builds")
        .with_tls_ca_file(None)
        .expect("no CA is no change");
    let config = logweir_kafka::rdkafka_reader::RdKafkaReader::client_config(
        &spec.source.bootstrap_servers,
        &auth,
    )
    .expect("configures");
    assert_eq!(config.get("ssl.ca.location"), None);
    let plan = logweir::backup::build_plan_with_tls_ca(&spec, "b1", None).expect("no CA");
    let (doc, _digest) =
        logweir_engine_oso::render_backup::render_and_digest(&plan).expect("renders");
    assert!(!doc.contains("ssl_ca_location"), "{doc}");
}

/// **EVERY CLIENT-CONSTRUCTION SITE HANDS ITS CLIENTS THE PROJECTED PATH
/// ITSELF** — PLAT-07.1 review finding H2, the half a value test cannot reach.
///
/// `one_projected_path_reaches_both_tls_clients` proves the two clients agree
/// when they are handed the same value. It cannot prove that the four sites
/// that build a real client actually hand it: `backup::run`, `probe::dial`,
/// `drill::context` and `doctor::check_target` all need a broker, so no
/// in-process row reaches their wiring. That is exactly where the review's
/// surviving mutant lived — `with_tls_ca_file(source_tls_ca.clone().filter(|_|
/// false))` in `backup::run` denied the runner's OWN librdkafka reader the CA
/// while the engine kept it, and all 725 rows passed.
///
/// So this is a SOURCE row, the shape `auth_binding::
/// no_construction_site_hardcodes_plaintext` already uses for interface I1's
/// same problem. It reads the shipped sources and requires, per site: the CA
/// comes from `tls_ca::projected_ca_file`; it is read from that site's OWN
/// side's variable and never the other's; and every value handed to
/// `with_tls_ca_file` is that binding — a plain path expression, optionally
/// `.clone()`d. An adapter in between (`.filter`, `.map`, `.and_then`,
/// `.take`, `.or`), a literal, or `None` is what half-wiring looks like, and
/// each of those fails here.
#[test]
fn every_construction_site_hands_its_clients_the_path_it_read() {
    /// `with_tls_ca_file` arguments a site may pass: a path expression, with
    /// at most a trailing `.clone()`. Deliberately NOT a general expression —
    /// the property is "the value it read, untouched".
    fn is_the_binding(arg: &str) -> bool {
        let arg = arg.trim();
        let arg = arg.strip_suffix(".clone()").unwrap_or(arg);
        !arg.is_empty()
            && arg != "None"
            && arg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            && arg
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    }

    /// The balanced-paren argument of every `with_tls_ca_file(` call in `code`.
    fn arguments(code: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = code;
        while let Some(at) = rest.find("with_tls_ca_file(") {
            let from = at + "with_tls_ca_file(".len();
            let mut depth = 1usize;
            let mut end = from;
            for (i, c) in rest[from..].char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = from + i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            out.push(rest[from..end].to_string());
            rest = &rest[end..];
        }
        out
    }

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sites: Vec<(String, String)> = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                let body = std::fs::read_to_string(&p).expect("a readable source file");
                let code: String = body
                    .lines()
                    .filter(|l| !l.trim_start().starts_with("//"))
                    .collect::<Vec<_>>()
                    .join("\n");
                if code.contains("with_tls_ca_file(") {
                    out.push((p.display().to_string(), code));
                }
            }
        }
    }
    walk(&src, &mut sites);
    sites.sort();

    assert_eq!(
        sites.len(),
        5,
        "five files wire a CA into a client today — backup, probe, drill, doctor and D2 §4.2's \
         check runner. A change to that COUNT is a decision about Global Constraint 29's \
         runner half and belongs in a commit message, not in a passing test: {:?}",
        sites.iter().map(|(p, _)| p).collect::<Vec<_>>()
    );

    // THE ONE SITE WHOSE CA DOES NOT COME FROM AN ENVIRONMENT VARIABLE, and
    // the reason it is named here rather than exempted by a prefix rule.
    //
    // The four rows below read `LOGWEIR_SOURCE_TLS_CA_FILE` /
    // `LOGWEIR_TARGET_TLS_CA_FILE` through `tls_ca::projected_ca_file`,
    // because an execution Job is rendered with exactly one connection and the
    // controller projects its trust anchor under a fixed variable name. A
    // CHECK Job is not: one check plan can carry a source connection, a target
    // connection and up to two destinations, each with its own CA, so D2 §4.2
    // renders the trust files into the plan `ConfigMap`
    // (`source-ca.pem`, `target-ca.pem`, `archive-ca.pem`, `evidence-ca.pem`)
    // and the plan NAMES the path per connection
    // (`check_contract::ConnectionPlan::ca_file`). Four connections cannot
    // share two variable names, and adding four more variables would put the
    // "which side is this?" decision back in the runner — which is the defect
    // finding H2 names, reached from the other end.
    //
    // What the exemption does NOT relax: the site still hands the path it was
    // given to `AuthConfig::with_tls_ca_file`, which refuses a CA on a
    // connection that is not TLS, and `crates/logweir/tests/check_cli.rs`
    // asserts the check runner confines its dial to one function. It is
    // spelled as an exact path so a FIFTH site cannot inherit the exemption.
    const PLAN_PROJECTED: &str = "src/check/kafka.rs";
    let (plan_projected, env_projected): (Vec<_>, Vec<_>) = sites
        .iter()
        .partition(|(p, _)| p.replace('\\', "/").ends_with(PLAN_PROJECTED));
    assert_eq!(
        plan_projected.len(),
        1,
        "exactly one site takes its CA from the check plan rather than the environment: {:?}",
        plan_projected.iter().map(|(p, _)| p).collect::<Vec<_>>()
    );
    for (site, code) in &plan_projected {
        assert!(
            code.contains("plan.ca_file"),
            "{site} is the plan-projected site and does not read the plan's own `ca_file`"
        );
        assert!(
            !code.contains("TLS_CA_FILE"),
            "{site} reads an execution-side CA variable; a check plan names its own paths"
        );
    }

    for (site, code) in &env_projected {
        assert!(
            code.contains("tls_ca::projected_ca_file("),
            "{site} builds a client but does not read the projected CA through the ONE reader"
        );
        // The side is not a choice the site may get wrong: a restore that read
        // the SOURCE variable would trust whatever the backup side projected.
        let source = code.contains("SOURCE_TLS_CA_FILE");
        let target = code.contains("TARGET_TLS_CA_FILE");
        assert!(
            source ^ target,
            "{site} must read exactly one side's variable (source={source}, target={target})"
        );
        let args = arguments(code);
        assert!(
            !args.is_empty(),
            "{site} reads a CA and hands it to no client"
        );
        for arg in args {
            assert!(
                is_the_binding(&arg),
                "{site} hands `{arg}` to a TLS client instead of the path it read. An adapter, \
                 a literal or `None` here is the half-wiring finding H2 names: one client \
                 trusts the private CA and the other falls back to the image's public roots"
            );
        }
    }
}
