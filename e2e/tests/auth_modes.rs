#![cfg(feature = "e2e")]
//! **PROD-01.3 — every new client-authentication mode, end to end, on BOTH
//! clients, against PROD-01.5's `auth` profile.**
//!
//! Four listener shapes on the profile's `kafka-auth` cluster:
//!
//! | row | mode | listener | transport |
//! |---|---|---|---|
//! | `plain` | `plain` (SASL/PLAIN) | `PLAINTLS` (`LOGWEIR_E2E_AUTH_PLAIN_PORT`) | SASL_SSL, private CA |
//! | `scram256` | `scramSha256` | `SCRAM256` (`LOGWEIR_E2E_AUTH_SCRAM256_PORT`) | SASL_PLAINTEXT |
//! | `scram256-tls` | `scramSha256` | `PLAINTLS` (it serves SCRAM-SHA-256 too) | SASL_SSL, private CA |
//! | `mtls` | `mtls` | `MTLS` (`LOGWEIR_E2E_AUTH_MTLS_PORT`) | SSL, client certificate required |
//!
//! Each row runs a real `logweir backup run` (phase −1's cluster-id and
//! DescribeConfigs reads through LOGWEIR'S librdkafka client, then the pinned
//! ENGINE's own client takes the archive) and a real `logweir drill run` that
//! restores that archive into the same cluster's scratch namespace (Logweir's
//! client at phases 0, 3, 7 and 9, the engine's `restore` and `validation
//! run`). The receipt and the scorecard verify in BOTH readers, carry the mode
//! (receipt 1.4.0, scorecard 1.5.0), and — the seeded-secret scan — carry
//! neither the password nor a line of the client key, nor does any line the
//! two processes printed.
//!
//! NEGATIVE CONTROLS, each REQUIRING the refusal it records: a wrong password
//! (PLAIN, SCRAM-SHA-256 with and without TLS), a wrong CA (every TLS row), an
//! untrusted client certificate (mTLS), and PLAIN without TLS (refused by name,
//! `refusal-reason=PlainWithoutTls`, before any client exists).
//!
//! Requires the stack with the `auth` profile on the slot the environment
//! names: `eval "$(e2e/compose/stack-env.sh --slot N --profiles auth)"`,
//! `just e2e-up`. Rows share demo-dir files with the drill harness, so run the
//! file with `--test-threads=1`, as CI runs the package.

mod harness;
use harness::*;
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader, TopicDeleter};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The fixture password every SASL listener of the profile accepts for
/// `logweir` (the PLAIN JAAS line and the SCRAM-SHA-256 credential the setup
/// writes). A fixture value; it authenticates nowhere else.
const PASSWORD: &str = SCRAM_PASSWORD;
const USER: &str = SCRAM_USER;
const TOPIC: &str = "orders";

#[derive(Clone, Copy, Debug)]
struct Row {
    label: &'static str,
    mode: &'static str,
    port_var: &'static str,
    tls: bool,
}

const PLAIN: Row = Row {
    label: "plain",
    mode: "plain",
    port_var: "LOGWEIR_E2E_AUTH_PLAIN_PORT",
    tls: true,
};
const SCRAM256: Row = Row {
    label: "scram256",
    mode: "scramSha256",
    port_var: "LOGWEIR_E2E_AUTH_SCRAM256_PORT",
    tls: false,
};
const SCRAM256_TLS: Row = Row {
    label: "scram256-tls",
    mode: "scramSha256",
    port_var: "LOGWEIR_E2E_AUTH_PLAIN_PORT",
    tls: true,
};
const MTLS: Row = Row {
    label: "mtls",
    mode: "mtls",
    port_var: "LOGWEIR_E2E_AUTH_MTLS_PORT",
    tls: true,
};

impl Row {
    fn sasl(self) -> bool {
        self.mode != "mtls"
    }
    fn bootstrap(self) -> String {
        format!("localhost:{}", harness::stack::port(self.port_var))
    }
    /// The `auth:` block of a spec, as the runner's `AuthSpec` parses it.
    fn auth_yaml(self) -> serde_yaml::Value {
        let text = if self.sasl() {
            format!(
                "{{mode: {}, username: {USER}, tls: {}}}",
                self.mode, self.tls
            )
        } else {
            format!("{{mode: {}, tls: {}}}", self.mode, self.tls)
        };
        serde_yaml::from_str(&text).unwrap()
    }
}

/// What a row hands one side of a run: the password, the CA, the client pair.
#[derive(Clone, Default)]
struct Material {
    password: Option<String>,
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
}

/// The profile's TLS material, COPIED into the directory the engine container
/// mounts (`engine_mount()`), so the one path the runner is handed means the
/// same file to Logweir's client on the host and to the engine in its
/// container.
fn certs() -> PathBuf {
    let from = root().join(".e2e/auth").join(harness::stack::project());
    assert!(
        from.join("ca.pem").exists(),
        "no {}/ca.pem — bring the stack up with the `auth` profile",
        from.display()
    );
    let to = engine_mount().join("auth-certs");
    std::fs::create_dir_all(&to).unwrap();
    for f in [
        "ca.pem",
        "client.pem",
        "client.key",
        "wrong-ca.pem",
        "wrong-client.pem",
        "wrong-client.key",
    ] {
        std::fs::copy(from.join(f), to.join(f))
            .unwrap_or_else(|e| panic!("copy {f}: {e} — regenerate the auth profile's certs"));
    }
    to
}

fn good(row: Row) -> Material {
    let c = certs();
    Material {
        password: row.sasl().then(|| PASSWORD.to_string()),
        ca: row.tls.then(|| c.join("ca.pem")),
        cert: (!row.sasl()).then(|| c.join("client.pem")),
        key: (!row.sasl()).then(|| c.join("client.key")),
    }
}

fn env_for(side: &str, m: &Material) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if let Some(p) = &m.password {
        env.push((format!("LOGWEIR_{side}_PASSWORD"), p.clone()));
    }
    for (name, path) in [
        ("TLS_CA_FILE", &m.ca),
        ("TLS_CERT_FILE", &m.cert),
        ("TLS_KEY_FILE", &m.key),
    ] {
        if let Some(p) = path {
            env.push((format!("LOGWEIR_{side}_{name}"), p.display().to_string()));
        }
    }
    env
}

/// Logweir's own client against a row's listener, with that row's material.
fn client(row: Row, m: &Material) -> RdKafkaReader {
    let auth = match row.mode {
        "plain" => AuthConfig::Plain {
            username: USER.into(),
            password: m.password.clone().unwrap(),
            tls_ca_file: m.ca.as_ref().map(|p| p.display().to_string()),
        },
        "scramSha256" => AuthConfig::ScramSha256 {
            username: USER.into(),
            password: m.password.clone().unwrap(),
            tls: row.tls,
            tls_ca_file: m.ca.as_ref().map(|p| p.display().to_string()),
        },
        _ => AuthConfig::Mtls {
            tls_ca_file: m.ca.as_ref().map(|p| p.display().to_string()),
            client_certificate: Some(logweir_core::connection::ClientCertificateFiles {
                cert_file: m.cert.as_ref().unwrap().display().to_string(),
                key_file: m.key.as_ref().unwrap().display().to_string(),
            }),
        },
    };
    RdKafkaReader::connect(&[row.bootstrap()], auth).expect("connect is lazy")
}

/// Records into the auth cluster's `orders`, through the SCRAM-SHA-256
/// listener, with explicit CreateTime: the window a drill selects is then a
/// fact of this run, not of whatever the topic held. Returns `(min, max)` ts.
fn produce(n: usize) -> (i64, i64) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", SCRAM256.bootstrap())
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-256")
        .set("sasl.username", USER)
        .set("sasl.password", PASSWORD)
        .set("message.timeout.ms", "15000")
        .set("acks", "all")
        .create()
        .expect("a SCRAM-SHA-256 producer");
    let base = chrono::Utc::now().timestamp_millis() - 120_000;
    for i in 0..n {
        let ts = base + (i as i64) * 100;
        let key = format!("auth-{i:03}");
        let payload = format!("{{\"auth-row\":{i},\"ts\":{ts}}}");
        producer
            .send(
                BaseRecord::to(TOPIC)
                    .partition((i % 3) as i32)
                    .key(&key)
                    .payload(&payload)
                    .timestamp(ts),
            )
            .map_err(|(e, _)| e)
            .expect("enqueue");
    }
    producer
        .flush(std::time::Duration::from_secs(20))
        .expect("the auth cluster accepted the records");
    (base, base + (n as i64 - 1) * 100)
}

fn nonce() -> String {
    format!("{}", chrono::Utc::now().timestamp_millis())
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn text(o: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// The backup spec for a row: the auth cluster's `orders` over the row's
/// listener, into this stack's MinIO under a per-run prefix.
fn backup_spec(row: Row, backup_id: &str, auth: &serde_yaml::Value) -> PathBuf {
    let mut v: serde_yaml::Value = serde_yaml::from_str(&format!(
        "backup_id: {backup_id}\n\
         source:\n\
         \x20 bootstrap_servers: [\"{}\"]\n\
         \x20 topics: [{TOPIC}]\n\
         storage:\n\
         \x20 backend: s3\n\
         \x20 bucket: {ARCHIVE_BUCKET}\n\
         \x20 prefix: {backup_id}\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        row.bootstrap(),
        s3_endpoint()
    ))
    .unwrap();
    v["source"]["auth"] = auth.clone();
    let p = demo_dir().join(format!("auth-{backup_id}.yaml"));
    std::fs::write(&p, serde_yaml::to_string(&v).unwrap()).unwrap();
    p
}

/// `logweir backup run` with the pinned engine and a row's material on the
/// SOURCE side. The allowlist names a cluster that is not the source (GC18(c)
/// rail 4 refuses a source that is a permitted target).
fn backup(spec: &Path, receipt: &Path, m: &Material) -> Output {
    let allow = demo_dir().join("auth-backup-allowed.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .arg("--receipt-out")
        .arg(receipt)
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    for (k, v) in env_for("SOURCE", m) {
        c.env(k, v);
    }
    c.output().expect("logweir backup run")
}

/// Both readers over a receipt the run signed.
fn verify_receipt(receipt: &Path) -> (Option<i32>, Option<i32>) {
    let sig = receipt.with_extension("sig");
    let pubkey = root().join("e2e/fixtures/signed/public.pem");
    let rust = Command::new(bin())
        .args([
            "drill",
            "verify",
            "--payload-type",
            "backup-receipt",
            "--scorecard",
        ])
        .arg(receipt)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubkey)
        .output()
        .unwrap();
    let py = Command::new(auditor_python())
        .arg(root().join("docs/verify_scorecard.py"))
        .args(["--payload-type", "backup-receipt"])
        .arg(receipt)
        .arg(&sig)
        .arg(&pubkey)
        .output()
        .unwrap();
    (rust.status.code(), py.status.code())
}

/// Delete every `drill-` topic on the AUTH cluster (the harness's own sweep
/// addresses the default broker), through Logweir's client with the same
/// scratch-prefix fence the drill has.
fn sweep_drill_topics(row: Row) {
    let r = client(row, &good(row))
        .with_scratch_prefix(SCRATCH_PREFIX)
        .expect("`drill-` is a scratch namespace");
    let names: Vec<String> = ClusterReader::list_topics(&r)
        .expect("list over the row's listener")
        .into_iter()
        .map(|t| t.name)
        .filter(|n| n.starts_with(SCRATCH_PREFIX))
        .collect();
    if !names.is_empty() {
        TopicDeleter::delete_topics(&r, &names).unwrap();
        for _ in 0..60 {
            let left = ClusterReader::list_topics(&r)
                .unwrap()
                .into_iter()
                .filter(|t| t.name.starts_with(SCRATCH_PREFIX))
                .count();
            if left == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
}

/// The drill spec: the archive this row's backup wrote, restored into the
/// auth cluster over the row's listener.
fn drill_spec(row: Row, backup_id: &str, window: (i64, i64)) -> serde_yaml::Value {
    let mut v: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(root().join("examples/drill.yaml")).unwrap())
            .unwrap();
    rebind_addresses(&mut v);
    v["source"]["storage"]["prefix"] = backup_id.into();
    v["source"]["topics"] = serde_yaml::from_str(&format!("[{TOPIC}]")).unwrap();
    v["target"]["bootstrap_servers"] =
        serde_yaml::from_str(&format!("[\"{}\"]", row.bootstrap())).unwrap();
    v["target"]["auth"] = row.auth_yaml();
    v["target"]["teardown"] = "delete".into();
    // THE WINDOW STARTS BEFORE EVERY RECORD THE TOPIC HOLDS. `orders` on the
    // auth listener accumulates one batch per row per run, and a backup takes
    // the whole topic as one segment per partition. A window starting an hour
    // back stops covering the earliest batches once the stack is an hour old:
    // the segment then straddles the window, the engine restores it whole
    // (recovery-point selection is by segment, docs/stability.md), and the
    // head-anchored sample compares the window's first records with the
    // target's first records, which are older — a false `fail-integrity` that
    // depends on the stack's age, not on the auth mode. Thirty days back keeps
    // every batch inside, so the head of the window IS the head of the topic.
    v["sample"]["window_start"] = rfc3339(window.0 - 30 * 86_400_000).into();
    v["sample"]["window_end"] = rfc3339(window.1 + 60_000).into();
    v
}

fn allowlist_for(cluster_id: &str) -> PathBuf {
    let p = demo_dir().join("auth-allowed-clusters.json");
    std::fs::write(
        &p,
        serde_json::to_vec_pretty(&serde_json::json!({
            "allowed_cluster_ids": [cluster_id],
            "source_cluster_id": null,
        }))
        .unwrap(),
    )
    .unwrap();
    p
}

/// A distinctive line of the client private key — what the seeded-secret scan
/// looks for, so a key that reached any output is caught by its body, not by
/// its armour line (which is public text).
fn key_body_line(m: &Material) -> Option<String> {
    let key = std::fs::read_to_string(m.key.as_ref()?).ok()?;
    key.lines()
        .find(|l| !l.starts_with("-----") && l.len() > 40)
        .map(str::to_string)
}

fn assert_no_secret(label: &str, what: &str, haystack: &str, m: &Material) {
    if let Some(p) = &m.password {
        assert!(
            !haystack.contains(p.as_str()),
            "{label}: the password reached {what}"
        );
    }
    if let Some(line) = key_body_line(m) {
        assert!(
            !haystack.contains(&line),
            "{label}: client key material reached {what}"
        );
    }
}

/// THE ROW: backup → restore → verify over one mode, both clients, both
/// readers, and the seeded-secret scan.
fn backup_restore_verify(row: Row) {
    let label = row.label;
    let m = good(row);
    // Logweir's own client first: the cluster id the drill's allowlist names.
    let cluster_id = client(row, &m)
        .cluster_id()
        .unwrap_or_else(|e| panic!("{label}: Logweir's client over {}: {e}", row.bootstrap()));
    let window = produce(30);

    // --- the BACKUP, through both clients -----------------------------------
    let backup_id = format!("auth-{}-{}", row.label, nonce());
    let spec = backup_spec(row, &backup_id, &row.auth_yaml());
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let out = backup(&spec, &receipt, &m);
    let printed = text(&out);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{label}: backup over {} did not exit 0:\n{printed}",
        row.bootstrap()
    );
    for key in [
        "security_protocol",
        "sasl_mechanism",
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
    ] {
        assert!(
            !printed.contains(&format!(
                "Ignoring unknown config key `source.security.{key}`"
            )),
            "{label}: the engine dropped `{key}` and ran without it:\n{printed}"
        );
    }
    let receipt_text = std::fs::read_to_string(&receipt).expect("the receipt was written");
    let doc: serde_json::Value = serde_json::from_str(&receipt_text).unwrap();
    assert_eq!(doc["source"]["auth"]["mode"], row.mode, "{label}");
    assert_eq!(
        doc["source"]["auth"]["username"].as_str(),
        row.sasl().then_some(USER),
        "{label}: the SASL principal, or none for mtls"
    );
    // A receipt naming a PROD-01.3 mode declares a version that defines it.
    // At least 1.5.0 since PROD-01.4a (every receipt this build signs carries
    // generations, and 1.5.0 defines every earlier minor); "at least", not
    // exact, because the version is renumbered when format-bumping rows
    // integrate together.
    assert_format_at_least(
        doc["format_version"].as_str().expect("a version"),
        "1.5.0",
        &format!("{label}: a receipt naming a PROD-01.3 mode carries generations"),
    );
    assert_eq!(doc["source"]["cluster_id"], cluster_id, "{label}");
    assert_eq!(
        verify_receipt(&receipt),
        (Some(0), Some(0)),
        "{label}: both readers verify the receipt"
    );
    assert_no_secret(label, "the backup's output", &printed, &m);
    assert_no_secret(label, "the signed receipt", &receipt_text, &m);
    assert!(
        !receipt_text.contains("password"),
        "{label}: {receipt_text}"
    );

    // --- the RESTORE and VERIFY, through both clients ------------------------
    sweep_drill_topics(row);
    let dspec = drill_spec(row, &backup_id, window);
    let allow = allowlist_for(&cluster_id);
    let mut o = RunOpts::new(&dspec);
    o.allowlist = Some(&allow);
    o.env = env_for("TARGET", &m);
    let r = run_with(o);
    let printed = format!("{}\n{}", r.out.stdout_utf8(), r.out.stderr_utf8());
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "{label}: the drill over {} did not exit 0:\n{printed}",
        row.bootstrap()
    );
    let sc = read_scorecard(&r);
    assert_eq!(sc["outcome"], "pass", "{label}");
    let phases = phase_outcomes(&sc);
    for n in [0, 3, 6, 7] {
        assert_eq!(
            phases.get(&n).map(String::as_str),
            Some("ok"),
            "{label}: phase {n} — {phases:?}"
        );
    }
    // Phase 9 runs AFTER the scorecard is signed (so it is not in the phase
    // table): `teardown: delete` removed the scratch topics, over the row's
    // own listener and credential. Read back through Logweir's client.
    let left: Vec<String> = ClusterReader::list_topics(&client(row, &m))
        .expect("list after teardown")
        .into_iter()
        .map(|t| t.name)
        .filter(|n| n.starts_with(SCRATCH_PREFIX))
        .collect();
    assert!(
        left.is_empty(),
        "{label}: phase 9 left scratch topics on the auth cluster: {left:?}"
    );
    assert_eq!(sc["target"]["auth"]["mode"], row.mode, "{label}");
    // FX-23: a sampled drill is 1.6.0 (which defines every 1.5.0 auth mode),
    // so the version marks a build with FX-23's checks.
    assert_eq!(sc["format_version"], "1.6.0", "{label}");
    assert_eq!(sc["target"]["cluster_id"], cluster_id, "{label}");
    assert!(
        logweir_verify(&r).success(),
        "{label}: logweir drill verify"
    );
    assert!(python_verify(&r).success(), "{label}: verify_scorecard.py");
    let scorecard_text = std::fs::read_to_string(&r.scorecard).unwrap();
    assert_no_secret(label, "the drill's output", &printed, &m);
    assert_no_secret(label, "the signed scorecard", &scorecard_text, &m);
    eprintln!(
        "[prod-01-3] {label}: backup {backup_id} exit 0 (receipt {} verified by both readers), \
         drill pass phases {phases:?}, scorecard {} verified by both readers; no secret in any \
         output",
        doc["format_version"], sc["format_version"]
    );
}

#[test]
fn plain_over_tls_backs_up_restores_and_verifies() {
    backup_restore_verify(PLAIN);
}

#[test]
fn scram_sha_256_backs_up_restores_and_verifies() {
    backup_restore_verify(SCRAM256);
}

#[test]
fn scram_sha_256_over_tls_backs_up_restores_and_verifies() {
    backup_restore_verify(SCRAM256_TLS);
}

#[test]
fn mtls_backs_up_restores_and_verifies() {
    backup_restore_verify(MTLS);
}

// ---------------------------------------------------------------------------
// NEGATIVE CONTROLS — each requires the refusal, and runs beside its control.
// ---------------------------------------------------------------------------

/// A backup that must FAIL, before an archive exists: exit non-zero, a phrase
/// from `needles` in what it printed, no receipt, and no secret printed.
fn assert_refused(row: Row, m: &Material, needles: &[&str], why: &str) {
    let backup_id = format!("auth-neg-{}-{}", row.label, nonce());
    let spec = backup_spec(row, &backup_id, &row.auth_yaml());
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let out = backup(&spec, &receipt, m);
    let printed = text(&out);
    assert_ne!(
        out.status.code(),
        Some(0),
        "{}: {why} produced a backup:\n{printed}",
        row.label
    );
    assert!(
        needles.iter().any(|n| printed.contains(n)),
        "{}: {why} failed for another reason (wanted one of {needles:?}):\n{printed}",
        row.label
    );
    assert!(!receipt.exists(), "{}: {why} wrote a receipt", row.label);
    assert_no_secret(row.label, "a refused run's output", &printed, m);
    eprintln!(
        "[prod-01-3] {}: {why} refused with exit {:?}, naming {:?}",
        row.label,
        out.status.code(),
        needles
            .iter()
            .filter(|n| printed.contains(*n))
            .collect::<Vec<_>>()
    );
}

const AUTH_FAILED: [&str; 4] = [
    "SASL authentication error",
    "Authentication failed",
    "authentication failed",
    "SaslAuthenticationFailed",
];
const TLS_FAILED: [&str; 5] = [
    "certificate verify failed",
    "SSL handshake failed",
    "Certificate verification failed",
    "unable to get local issuer certificate",
    "self signed certificate",
];

#[test]
fn a_wrong_password_is_refused_on_every_sasl_mode() {
    for row in [PLAIN, SCRAM256, SCRAM256_TLS] {
        let mut m = good(row);
        m.password = Some("definitely-not-the-password".into());
        // Logweir's own client refuses first: phase −1 reads the cluster id.
        let refused = client(row, &m).cluster_id();
        assert!(
            refused.is_err(),
            "{}: a wrong password read the cluster id",
            row.label
        );
        assert_refused(row, &m, &AUTH_FAILED, "a wrong password");
        // CONTROL: the right password reads it.
        assert!(
            client(row, &good(row)).cluster_id().is_ok(),
            "{}: the control (right password) must authenticate",
            row.label
        );
    }
}

#[test]
fn a_wrong_ca_is_refused_on_every_tls_mode() {
    for row in [PLAIN, SCRAM256_TLS, MTLS] {
        let mut m = good(row);
        m.ca = Some(certs().join("wrong-ca.pem"));
        assert!(
            client(row, &m).cluster_id().is_err(),
            "{}: a wrong CA verified the broker",
            row.label
        );
        assert_refused(row, &m, &TLS_FAILED, "a wrong CA");
        assert!(
            client(row, &good(row)).cluster_id().is_ok(),
            "{}: the control (the right CA) must verify",
            row.label
        );
    }
}

#[test]
fn an_untrusted_client_certificate_is_refused() {
    let c = certs();
    let mut m = good(MTLS);
    m.cert = Some(c.join("wrong-client.pem"));
    m.key = Some(c.join("wrong-client.key"));
    assert!(
        client(MTLS, &m).cluster_id().is_err(),
        "a self-signed client certificate was accepted by a listener that requires the CA's"
    );
    // THE BROKER'S REFUSAL OF THIS CERTIFICATE, and nothing a stopped broker
    // or another TLS fault would also print (review F7): the TLS alert the
    // broker sends when it rejects the client's certificate — `bad
    // certificate` (alert 42) or, under TLS 1.3, `certificate required`
    // (alert 116) / `unknown ca` (alert 48).
    let needles = [
        "alert bad certificate",
        "SSL alert number 42",
        "alert certificate required",
        "SSL alert number 116",
        "alert unknown ca",
        "SSL alert number 48",
    ];
    assert_refused(MTLS, &m, &needles, "an untrusted client certificate");
    assert!(
        client(MTLS, &good(MTLS)).cluster_id().is_ok(),
        "the control"
    );
}

/// PLAIN WITHOUT TLS, against the real SASL_PLAINTEXT listener: refused by
/// name before any client exists — exit 3, `refusal-reason=PlainWithoutTls` —
/// so the password never reaches a listener that would carry it in the clear.
/// CONTROL: PLAIN over TLS on the TLS listener is the first row above.
#[test]
fn plain_without_tls_is_refused_by_name_before_dialling() {
    let row = Row {
        label: "plain-clear",
        mode: "plain",
        port_var: "LOGWEIR_E2E_AUTH_SCRAM256_PORT",
        tls: false,
    };
    let backup_id = format!("auth-neg-plain-clear-{}", nonce());
    let spec = backup_spec(row, &backup_id, &row.auth_yaml());
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let m = Material {
        password: Some(PASSWORD.to_string()),
        ..Material::default()
    };
    let out = backup(&spec, &receipt, &m);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));
    assert_eq!(
        stdout.lines().filter(|l| !l.trim().is_empty()).next_back(),
        Some("refusal-reason=PlainWithoutTls"),
        "{stdout}"
    );
    assert!(!receipt.exists());
    assert_no_secret("plain-clear", "the refusal", &text(&out), &m);
}
