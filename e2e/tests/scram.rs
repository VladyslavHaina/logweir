#![cfg(feature = "e2e")]
//! **The first evidence anywhere in this tree that a SCRAM handshake actually
//! succeeds.**
//!
//! Task 6 built the whole SCRAM path — `AuthConfig::from_spec` as the one
//! construction site, `AuthSpec::ScramSha512`, and the engine's `security:`
//! block with `sasl_mechanism: "SCRAM-SHA512"` — against a stack that had no
//! SASL listener, and said so: "Nothing in Task 6 claims a successful SCRAM
//! handshake." Task 7 adds the listener, and this file is where the claim is
//! finally made and checked.
//!
//! # BOTH clients, or it proves half of what it claims (spec §10 M12)
//!
//! There are two independent SCRAM implementations in play and they agree on
//! nothing but the wire:
//!
//! 1. **Logweir's own client** is librdkafka, whose mechanism spelling is
//!    `SCRAM-SHA-512` and whose configuration comes from
//!    `AuthConfig::from_spec` (`rdkafka_reader.rs:56-62`).
//! 2. **The engine's own client** is a from-scratch RFC 5802 implementation
//!    over `ring` (`U:crates/kafka-backup-core/src/kafka/scram.rs`), whose
//!    mechanism spelling is `SCRAM-SHA512` — ONE hyphen — and whose
//!    configuration comes from the four keys
//!    `logweir_engine_oso::yaml::render_security_block` emits under
//!    `security:`.
//!
//! A test that exercised only the first would leave the engine's spelling and
//! the block's nesting unproven, which is exactly the class of defect plan
//! errata E2 and E3 record: the engine treats a misplaced key as *unknown*,
//! warns, and proceeds **unauthenticated**. So the engine arm here is a real
//! `logweir backup run` that produces a real archive, and its success is the
//! only evidence that `"SCRAM-SHA512"` under `security:` is what the engine's
//! RFC 5802 client accepts.
//!
//! # And a pod, because a port is not a pod (critique A F17)
//!
//! `host.docker.internal` resolution **inside a Kubernetes pod** is a Docker
//! Desktop behaviour, not a Kubernetes one. A published host port and a
//! correct advertised string are necessary and not sufficient, and Demo 1's
//! `KafkaCluster.spec.bootstrapServers` is exactly `host.docker.internal:9095`
//! — so this file runs a real pod in namespace `logweir-t7` (STANDING RULE 13)
//! and deletes it.
//!
//! Requires `just e2e` — the stack up via `just e2e-up` (which runs
//! `scram-setup` as its third foreground step) and
//! `LOGWEIR_SEED_REFRESH_FIXTURES=0 ./scripts/e2e-seed.sh` already run.
mod harness;
use harness::*;
use logweir_core::spec::AuthSpec;
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Every `backup_id` this file uses starts with this, and nothing else in the
/// tree does — so the sweeps below can be exact rather than heuristic.
///
/// **Both rows that add an archive sweep at BOTH ends.**
/// `harness::corrupt_a_non_oldest_segment` picks its victim by listing the
/// bucket ROOT and taking the LAST key in sort order, and `mvp-scram…` sorts
/// after `drill-demo…` — so an archive left behind here would make
/// `full_drill.rs`'s corrupted-segment row quarantine one of THIS file's
/// segments and then watch an intact drill archive exit 0. That is a measured
/// failure mode, not a hypothetical: `backup_argv.rs`'s
/// `sweep_real_engine_archives` exists for the same reason.
const SCRAM_ID_PREFIX: &str = "mvp-scram";

/// The `backup_id` for the rendered (host-side, `SASLEXT`) engine arm — the
/// one spec §10 names.
const SCRAM_BACKUP_ID: &str = "mvp-scram";

/// The `backup_id` in `e2e/compose/config/backup-scram.yaml`, for the
/// in-network (`SASL`) engine arm. A DIFFERENT prefix, because the engine's
/// `backup` does not accumulate into an existing one.
const SCRAM_INNET_BACKUP_ID: &str = "mvp-scram-innet";

/// Everything `logweir backup run` writes as evidence lives under this
/// (Global Constraint 6), and nothing an archive contains does.
const RECEIPT_PREFIX: &str = "logweir/";

/// The namespace this task creates and deletes (STANDING RULE 13). Never
/// `logweir-e2e`, which is the compose PROJECT name (STANDING RULE 14).
const NAMESPACE: &str = "logweir-t7";

/// The one context, always named explicitly (STANDING RULE 12).
const KUBE_CONTEXT: &str = "docker-desktop";

// ---------------------------------------------------------------------------
// 1. The broker really has the listeners, and the JAAS variable really landed.
// ---------------------------------------------------------------------------

/// **The five changes, read back off the running broker.**
///
/// Two independent reads, because they answer different questions:
///
/// * a metadata read over `BOOTSTRAP` proves the broker is serving at all, so
///   a failure below is about the listeners and not about a dead stack;
/// * `grep -q` inside the broker container over
///   `/opt/kafka/config/server.properties` proves the listener-scoped JAAS
///   variable's SPELLING reached a property — and this is the read that closed
///   spec §15's sixth `[UNVERIFIED]` mark.
///
/// The `grep` exit codes are read DIRECTLY off `Output::status`, never through
/// a pipe (STANDING RULE 20).
///
/// # Why `server.properties` and not "the broker started"
///
/// A misspelled `KAFKA_…` variable is not an error under the `apache/kafka`
/// entrypoint: `kafka.docker.KafkaDockerWrapper` translates whatever it is
/// given and the broker starts happily without the property. The failure would
/// then appear as an authentication failure in a different test, which is the
/// same "silently ignored key" shape as plan errata E2/E3. So the property is
/// asserted where it lands.
#[test]
fn the_broker_advertises_five_listeners() {
    // The stack is alive, over the listener every other row uses.
    let live = cluster_id();
    assert!(!live.is_empty(), "the broker returned an empty cluster id");

    // THE JAAS TRANSCRIPT. `grep -q` exits 0 on a match and 1 on none.
    for property in [
        "listener.name.sasl.scram-sha-512.sasl.jaas.config",
        "listener.name.saslext.scram-sha-512.sasl.jaas.config",
    ] {
        let o = compose_exec_broker(&[
            "grep",
            "-q",
            property,
            "/opt/kafka/config/server.properties",
        ]);
        let rc = o.status.code();
        assert_eq!(
            rc,
            Some(0),
            "`{property}` is NOT in the broker's server.properties (grep exited {rc:?}).\n\
             The listener-scoped JAAS variable's spelling is wrong: \
             `kafka.docker.KafkaDockerWrapper` strips `KAFKA_`, lowercases, and maps \
             `___`->`-`, `__`->`_`, `_`->`.`, so the variable that produces this property is \
             KAFKA_LISTENER_NAME_<LISTENER>_SCRAM___SHA___512_SASL_JAAS_CONFIG. \
             The broker will still have started, and the failure would otherwise have \
             surfaced as an authentication error in a different test."
        );
    }

    // The rest of the five changes, in the properties the broker is running
    // with. `sasl.enabled.mechanisms` is the global half of change 4.
    let props = broker_server_properties();
    let line = |key: &str| -> String {
        props
            .lines()
            .find(|l| l.starts_with(&format!("{key}=")))
            .unwrap_or_else(|| panic!("no `{key}` in server.properties:\n{props}"))
            .to_string()
    };

    let listeners = line("listeners");
    for endpoint in [
        "PLAINTEXT://kafka-broker-1:9094",
        "EXTERNAL://kafka-broker-1:9092",
        "CONTROLLER://kafka-broker-1:9093",
        "SASL://kafka-broker-1:9096",
        "SASLEXT://kafka-broker-1:9097",
        "K8S://kafka-broker-1:9095",
    ] {
        assert!(
            listeners.contains(endpoint),
            "{endpoint} missing: {listeners}"
        );
    }

    let advertised = line("advertised.listeners");
    for endpoint in [
        "EXTERNAL://localhost:9092",
        "SASL://kafka-broker-1:9096",
        // ONE listener advertised TWICE: the host-side arm below cannot
        // resolve `kafka-broker-1`.
        "SASLEXT://localhost:9097",
        // The parameter's DEFAULT, which is what every local gate uses.
        "K8S://host.docker.internal:9095",
    ] {
        assert!(
            advertised.contains(endpoint),
            "{endpoint} missing: {advertised}"
        );
    }

    let map = line("listener.security.protocol.map");
    for entry in [
        "SASL:SASL_PLAINTEXT",
        "SASLEXT:SASL_PLAINTEXT",
        "K8S:PLAINTEXT",
        "EXTERNAL:PLAINTEXT",
    ] {
        assert!(map.contains(entry), "{entry} missing: {map}");
    }
    assert_eq!(
        line("sasl.enabled.mechanisms"),
        "sasl.enabled.mechanisms=SCRAM-SHA-512",
        "the ONE enabled mechanism is SCRAM-SHA-512"
    );
    eprintln!("[t7] {listeners}\n[t7] {advertised}\n[t7] {map}");
}

// ---------------------------------------------------------------------------
// 2. Logweir's own client: librdkafka, over the PUBLISHED SASL advertisement.
// ---------------------------------------------------------------------------

/// The SCRAM spec every row here authenticates with. **Constructed through
/// `AuthConfig::from_spec`**, which is interface I1's single construction
/// site: a row that built `AuthConfig::ScramSha512 {…}` directly would be
/// testing a shape no shipped call site uses.
fn scram_auth(password: &str) -> AuthConfig {
    AuthConfig::from_spec(
        &AuthSpec::ScramSha512 {
            username: SCRAM_USER.to_string(),
            tls: false,
        },
        Some(password.to_string()),
    )
    .expect("a projected password makes from_spec infallible")
}

/// **THE HOST-SIDE rdkafka ARM.** `RdKafkaReader::connect` over
/// `BOOTSTRAP_SASL` (`localhost:9097`, the published `SASLEXT` advertisement)
/// with the credential `scram-setup` created, and then the two reads every
/// phase of this product makes: `cluster_id()` and `list_topics()`.
///
/// It is the assertion the first mutant kills. Advertise the SASL listener
/// ONCE — drop `SASLEXT` and its published port — and this row fails at
/// assertion time on `cluster_id()`: the host cannot resolve
/// `kafka-broker-1`, which is the advertised name the broker would then hand
/// back whatever address the client bootstrapped against.
///
/// It is also the assertion the second mutant kills: with no `scram-setup`
/// service the credential does not exist in the metadata log, so the same
/// call fails — in KRaft there is no ZooKeeper to pre-seed and no
/// `--zookeeper` path, and the credential is a user config record a client has
/// to write after the quorum is serving.
///
/// The cluster id is compared with the one read over the PLAINTEXT `EXTERNAL`
/// listener, so this row proves it reached **the same broker** and not merely
/// *a* broker.
#[test]
fn scram_authenticates_through_the_rdkafka_client() {
    let plaintext_id = cluster_id();

    let r = RdKafkaReader::connect(&[BOOTSTRAP_SASL.to_string()], scram_auth(SCRAM_PASSWORD))
        .expect("RdKafkaReader::connect builds a client");

    let id = r.cluster_id().unwrap_or_else(|e| {
        panic!(
            "SCRAM over {BOOTSTRAP_SASL} could not read the cluster id: {e}\n\
             Three things this is: (a) `SASLEXT` is not advertised as \
             `localhost:9097` or its port is not published, so the client was redirected to \
             a name the host cannot resolve; (b) `scram-setup` did not exit 0, so the \
             credential does not exist; (c) the listener-scoped JAAS property is absent, so \
             the broker cannot serve SCRAM on this listener."
        )
    });
    assert_eq!(
        id, plaintext_id,
        "the SCRAM listener answered for a DIFFERENT cluster than the EXTERNAL listener"
    );

    let topics = ClusterReader::list_topics(&r).expect("list_topics over SCRAM");
    assert!(
        topics.iter().any(|t| t.name == "orders"),
        "the authenticated principal saw no `orders` topic; it saw {:?}",
        topics.iter().map(|t| &t.name).collect::<Vec<_>>()
    );
    eprintln!(
        "[t7] rdkafka SCRAM ok · cluster {id} · {} topic(s)",
        topics.len()
    );
}

/// **A WRONG PASSWORD IS REFUSED**, and this row is the only thing standing
/// between `SASL:SASL_PLAINTEXT` and a silent `SASL:PLAINTEXT`.
///
/// Map `SASL`/`SASLEXT` to `PLAINTEXT` in the protocol map (mutant 5) and the
/// broker stops authenticating anything: librdkafka's SASL configuration is
/// then irrelevant, every password is accepted, and `cluster_id()` returns
/// `Ok`. So the assertion is that it returns `Err` — with the
/// success arm above as the control that makes the `Err` mean "the password
/// was rejected" rather than "nothing was listening".
///
/// # What the error type can and cannot say
///
/// librdkafka builds its connection LAZILY: `connect` succeeds, and the
/// failure surfaces on the first read. `cluster_id()` maps a read that got no
/// answer within its own 20 s budget to `KafkaError::Unreachable`
/// (`rdkafka_reader.rs`'s `fetch_cluster_id` arm), which cannot distinguish
/// "authentication rejected" from "no broker" **by its type**. It is
/// distinguished HERE instead, structurally: the row above dialled the same
/// address, the same listener and the same mechanism a moment ago and got
/// `Ok`, so the only difference between the two is the password. That is the
/// stronger claim, not the weaker one — a string match on librdkafka's log
/// text would break on a librdkafka upgrade while proving less.
///
/// The `AUTHENTICATION` naming Task 6's third carry asks for is asserted where
/// it is observable: on the drill rows below, where the failure text is a
/// child process's stderr.
#[test]
fn scram_refuses_a_wrong_password_through_the_rdkafka_client() {
    let r = RdKafkaReader::connect(
        &[BOOTSTRAP_SASL.to_string()],
        scram_auth("definitely-not-the-password"),
    )
    .expect("connect is lazy and does not authenticate");

    let got = r.cluster_id();
    assert!(
        got.is_err(),
        "a WRONG SCRAM password read the cluster id anyway ({got:?}) — \
         `SASL`/`SASLEXT` must be mapped to SASL_PLAINTEXT in \
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, or the broker authenticates nothing and \
         every password is accepted"
    );
    let topics = ClusterReader::list_topics(&r);
    assert!(
        topics.is_err(),
        "a WRONG SCRAM password listed topics anyway: {:?}",
        topics.map(|t| t.len())
    );
    eprintln!("[t7] wrong password refused: {:?}", got.unwrap_err());
}

// ---------------------------------------------------------------------------
// 3. The engine's own client: RFC 5802 over `ring`.
// ---------------------------------------------------------------------------

/// An allowlist that does NOT name the live cluster. `--allowed-clusters` is
/// the restore-TARGET allowlist and GC18(c) rail 4 refuses a SOURCE cluster
/// that appears in it, so the compose broker must be absent for a backup of
/// that broker to be admitted at all. (`backup_argv.rs` writes the same file
/// for the same reason.)
fn backup_allowlist() -> PathBuf {
    let p = demo_dir().join("scram-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    p
}

/// The host-side backup spec, with the SCRAM `auth:` block and the bootstrap
/// the caller names.
fn scram_backup_spec(name: &str, backup_id: &str, bootstrap: &str) -> PathBuf {
    let p = demo_dir().join(name);
    std::fs::write(
        &p,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [orders]\n\
             \x20 auth:\n\
             \x20   mode: scramSha512\n\
             \x20   username: {SCRAM_USER}\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: http://localhost:9000\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n"
        ),
    )
    .unwrap();
    p
}

/// `logweir backup run` with the **REAL, digest-pinned engine**, exactly as
/// `backup_argv.rs::backup_run_real_engine` builds it, plus the projected
/// SASL password.
fn scram_backup_run(spec: &Path, receipt_out: &Path) -> Command {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .arg("--receipt-out")
        .arg(receipt_out)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount())
        // The ONE place the secret exists: this child's environment. It
        // reaches Logweir's rdkafka client through `AuthConfig::from_spec`
        // and the engine through the engine's own `${…}` expansion of the
        // rendered document. It is in no spec, no plan, no rendered
        // document and no receipt.
        .env("LOGWEIR_SOURCE_PASSWORD", SCRAM_PASSWORD);
    c
}

/// **THE ENGINE'S OWN RFC 5802 CLIENT AUTHENTICATES — and this row is the only
/// evidence in the tree that `sasl_mechanism: "SCRAM-SHA512"` is the spelling
/// it accepts** (Task 6 carry 1).
///
/// A complete `logweir backup run` against the SASL listener: phase −1 reads
/// the source cluster id over SCRAM with Logweir's rdkafka client, and the
/// pinned engine then takes a real archive over SCRAM with its own client. Two
/// implementations, one credential, one listener.
///
/// Four assertions, and each one catches a different way this can be wrong:
///
/// 1. **exit 0**, read directly off `Output::status`;
/// 2. **no `Ignoring unknown config key` naming any SASL key** — that is plan
///    erratum E3's exact defect, where all four keys are dropped and the run
///    proceeds UNAUTHENTICATED until `assert_no_dropped_logweir_key` aborts it
///    after the archive is written;
/// 3. **the receipt's `source.auth` is `{mode: "scramSha512", username:
///    "logweir"}`** and carries no password field at any depth;
/// 4. **the archive is really in MinIO** — a `manifest.json` and at least one
///    `topics/*/partition-*/segment-*` object — because "the command exited 0"
///    is not evidence that an archive exists.
///
/// Swept at both ends; see `SCRAM_ID_PREFIX`.
#[test]
fn scram_authenticates_through_the_engines_own_client() {
    sweep_scram_archives();
    let spec = scram_backup_spec("backup-scram-e2e.yaml", SCRAM_BACKUP_ID, BOOTSTRAP_SASL);
    let receipt = demo_dir().join("t7-scram-receipt.json");
    let receipt_sig = demo_dir().join("t7-scram-receipt.sig");
    for f in [&receipt, &receipt_sig] {
        let _ = std::fs::remove_file(f);
    }

    let out = scram_backup_run(&spec, &receipt).output().expect("logweir");
    let stdout = out.stdout_utf8();
    let stderr = out.stderr_utf8();
    let both = format!("{stdout}\n{stderr}");
    eprintln!("[t7] engine route: {}", engine_bin().display());

    assert_eq!(
        out.status.code(),
        Some(0),
        "a SCRAM backup against {BOOTSTRAP_SASL} did not exit 0.\n\
         `No available brokers` here means a silent PLAINTEXT downgrade or an unresolvable \
         advertised name; a SCRAM/authentication failure means the credential or the \
         mechanism spelling.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // (2) Plan erratum E3, in the bytes that actually ran.
    for key in [
        "security_protocol",
        "sasl_mechanism",
        "sasl_username",
        "sasl_password",
    ] {
        let dropped = format!("Ignoring unknown config key `source.{key}`");
        assert!(
            !both.contains(&dropped),
            "the engine DROPPED `{key}`, so this run authenticated with nothing: the four \
             keys are fields of KafkaConfig.security and a two-space nesting makes the whole \
             block a no-op (plan erratum E3):\n{both}"
        );
        // Also in the looser form, in case the engine's message changes shape.
        assert!(
            !both.contains(&format!("Ignoring unknown config key `{key}`")),
            "the engine dropped `{key}`:\n{both}"
        );
    }
    assert!(
        !both.contains("unknown variant `SCRAM-SHA-512`"),
        "the engine's mechanism spelling has ONE hyphen — `SCRAM-SHA512`:\n{both}"
    );

    // (3) The receipt names the principal, and holds no secret.
    let doc: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&receipt)
            .unwrap_or_else(|e| panic!("no receipt at {}: {e}\n{stderr}", receipt.display())),
    )
    .expect("the receipt is JSON");
    assert_eq!(
        doc["source"]["auth"]["mode"].as_str(),
        Some("scramSha512"),
        "source.auth.mode in {}",
        doc["source"]["auth"]
    );
    assert_eq!(
        doc["source"]["auth"]["username"].as_str(),
        Some(SCRAM_USER),
        "source.auth.username in {}",
        doc["source"]["auth"]
    );
    let receipt_text = std::fs::read_to_string(&receipt).unwrap();
    assert!(
        !receipt_text.contains(SCRAM_PASSWORD),
        "the projected password reached the SIGNED receipt"
    );
    assert!(
        !receipt_text.contains("password"),
        "the receipt carries a field whose name is `password`: {receipt_text}"
    );

    // (4) The archive exists, with a manifest and a segment.
    let keys = archive_keys_under(SCRAM_BACKUP_ID);
    assert!(
        keys.iter().any(|k| k.ends_with("manifest.json")),
        "no manifest under {ARCHIVE_BUCKET}/{SCRAM_BACKUP_ID}/ — the run exited 0 without \
         writing an archive. mc listed: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| is_segment_key(k)),
        "no `topics/*/partition=*/segment-*` object under \
         {ARCHIVE_BUCKET}/{SCRAM_BACKUP_ID}/: {keys:?}"
    );
    eprintln!(
        "[t7] engine SCRAM ok · exit 0 · {} object(s) under {SCRAM_BACKUP_ID}/",
        keys.len()
    );

    sweep_scram_archives();
}

/// **THE IN-NETWORK HALF**, and the only row that can exercise
/// `BOOTSTRAP_SASL_INNET`.
///
/// `harness::BOOTSTRAP_SASL_INNET` is `kafka-broker-1:9096` — the SAME SCRAM
/// credential store as `BOOTSTRAP_SASL`, advertised for clients on
/// `kafka-net`. Nothing Logweir drives on this host can reach it:
/// `e2e/fixtures/engine-docker.sh` deliberately does not join `kafka-net` (it
/// rewrites `localhost` to the Docker host gateway so the rendered document's
/// `localhost:9092` means the same thing to the engine as it does to
/// `logweir`), and `logweir` itself runs on the host, where the name does not
/// resolve at all. So the constant that Tasks 11, 12, 28 and 31 consume would
/// otherwise be a string no test had ever dialled.
///
/// This row dials it, through the `kafka-backup` compose service — which IS on
/// `kafka-net` and bind-mounts `./config` — with
/// `e2e/compose/config/backup-scram.yaml`, whose `security:` block mirrors
/// `render_security_block`'s output key for key and indent for indent.
///
/// Swept at both ends; see `SCRAM_ID_PREFIX`.
#[test]
fn scram_authenticates_through_the_engines_own_client_in_network() {
    sweep_scram_archives();

    let out = compose_engine_innet(
        &[("LOGWEIR_SOURCE_PASSWORD", SCRAM_PASSWORD)],
        &["backup", "--config", "/config/backup-scram.yaml"],
    );
    let stdout = out.stdout_utf8();
    let stderr = out.stderr_utf8();
    let both = format!("{stdout}\n{stderr}");

    assert_eq!(
        out.status.code(),
        Some(0),
        "the engine could not back up over {BOOTSTRAP_SASL_INNET} from inside kafka-net.\n\
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    for key in [
        "security_protocol",
        "sasl_mechanism",
        "sasl_username",
        "sasl_password",
    ] {
        assert!(
            !both.contains(&format!("Ignoring unknown config key `source.{key}`")),
            "the engine dropped `{key}` from the checked-in config, so this backup ran \
             unauthenticated:\n{both}"
        );
    }
    assert!(
        !both.contains("Environment variable 'LOGWEIR_SOURCE_PASSWORD' is not set"),
        "the password did not reach the engine's own `${{…}}` expansion:\n{both}"
    );

    let keys = archive_keys_under(SCRAM_INNET_BACKUP_ID);
    assert!(
        keys.iter().any(|k| k.ends_with("manifest.json")),
        "no manifest under {ARCHIVE_BUCKET}/{SCRAM_INNET_BACKUP_ID}/: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| is_segment_key(k)),
        "no segment under {ARCHIVE_BUCKET}/{SCRAM_INNET_BACKUP_ID}/: {keys:?}"
    );
    eprintln!(
        "[t7] in-network engine SCRAM ok · exit 0 · {} object(s) under \
         {SCRAM_INNET_BACKUP_ID}/",
        keys.len()
    );

    sweep_scram_archives();
}

// ---------------------------------------------------------------------------
// 4. The drill over SCRAM — Task 6's carries 2 and 3.
// ---------------------------------------------------------------------------

/// `harness::spec_default()` with the target moved to the SASL listener.
///
/// The SAME `DrillSpec` shape `full_drill.rs`'s row uses, the same seed and the
/// same window binding, with two fields changed: `target.bootstrap_servers`
/// and `target.auth`. Anything else changed would make a failure ambiguous
/// between "SCRAM" and "a different drill".
fn scram_drill_spec() -> serde_yaml::Value {
    let mut v = spec_default();
    v["target"]["bootstrap_servers"] =
        serde_yaml::from_str(&format!("[{BOOTSTRAP_SASL}]")).unwrap();
    v["target"]["auth"] =
        serde_yaml::from_str(&format!("{{mode: scramSha512, username: {SCRAM_USER}}}")).unwrap();
    v
}

/// **A WHOLE DRILL COMPLETES OVER SCRAM, THE ENGINE'S `validation run`
/// INCLUDED** (Task 6 carry 2).
///
/// # Two corrections to the carry's wording, both measured
///
/// Task 6's carry 2 asks for "phase 6 (`validation run`) completed". In the
/// SHIPPED phase table `record(&mut sc, 6, "restore", …)` makes phase 6 the
/// engine's `restore`, and `validation run` is driven from **phase 7**
/// (`drill::phase7_verify::engine_validation_run`). And the `PhaseRecord`
/// outcome vocabulary is `"ok"`, not `"completed"` (`drill::record`). So this
/// row asserts what the carry is actually about, with the numbers the code
/// uses: phases 6 AND 7 both `"ok"`.
///
/// Why phase 7 is the one that matters here: it is the phase whose rendered
/// `validation.yaml` carries its own `security:` block — the
/// `render_validation.rs` change Task 6 made and could not prove — and it runs
/// AFTER phase 6's restore has already succeeded, so an unauthenticated
/// validation document would fail late, with a signed scorecard already
/// half-built. Asserting only `outcome: pass` would not distinguish that from
/// a drill that never reached the engine's validator at all.
#[test]
fn a_drill_passes_over_scram() {
    let spec = scram_drill_spec();
    let mut o = RunOpts::new(&spec);
    o.env = vec![(
        "LOGWEIR_TARGET_PASSWORD".to_string(),
        SCRAM_PASSWORD.to_string(),
    )];
    let r = run_with(o);
    let stdout = r.out.stdout_utf8();
    let stderr = r.out.stderr_utf8();
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "a SCRAM drill did not exit 0.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let sc = read_scorecard(&r);
    assert_eq!(
        sc["outcome"].as_str(),
        Some("pass"),
        "outcome in {}",
        sc["outcome"]
    );
    let phases = phase_outcomes(&sc);
    assert_eq!(
        phases.get(&6).map(String::as_str),
        Some("ok"),
        "PHASE 6 (`restore`) must have completed — the engine's `restore` over SCRAM. \
         Phase table: {phases:?}"
    );
    assert_eq!(
        phase(&sc, 6)["name"].as_str(),
        Some("restore"),
        "phase 6 is `restore` in the shipped table"
    );
    assert_eq!(
        phases.get(&7).map(String::as_str),
        Some("ok"),
        "PHASE 7 (`verify`) must have completed — it is the phase that drives the engine's \
         `validation run`, whose rendered document carries its own `security:` block. \
         Phase table: {phases:?}"
    );
    assert_eq!(
        phase(&sc, 7)["name"].as_str(),
        Some("verify"),
        "phase 7 is `verify` in the shipped table"
    );
    // And the scorecard records the principal it authenticated as (Task 6's
    // nested `target.auth`), so a reader can tell a SCRAM drill from a
    // plaintext one.
    assert_eq!(
        sc["target"]["auth"]["mode"].as_str(),
        Some("scramSha512"),
        "target.auth in {}",
        sc["target"]
    );
    eprintln!("[t7] SCRAM drill pass · phases {phases:?}");
}

/// **A WRONG PASSWORD FAILS AS AN AUTHENTICATION FAILURE — not as a timeout
/// and not as `No available brokers`** (Task 6 carry 3).
///
/// `No available brokers` is what a silent downgrade to PLAINTEXT against a
/// SASL-only listener looks like from the client's side, so the distinction is
/// the whole point of the row: it is the difference between "the broker
/// rejected this principal" and "the client never spoke SASL at all". Here the
/// failure text IS observable — it is a child process's stderr — so unlike the
/// in-process rdkafka row above, this one can assert on the words.
#[test]
fn a_drill_with_a_wrong_scram_password_fails_as_authentication() {
    let spec = scram_drill_spec();
    let mut o = RunOpts::new(&spec);
    o.env = vec![(
        "LOGWEIR_TARGET_PASSWORD".to_string(),
        "definitely-not-the-password".to_string(),
    )];
    let r = run_with(o);
    let stdout = r.out.stdout_utf8();
    let stderr = r.out.stderr_utf8();
    let both = format!("{stdout}\n{stderr}");

    assert_ne!(
        r.out.status.code(),
        Some(0),
        "a WRONG SCRAM password produced a passing drill.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // The failure must be ABOUT authentication, and the needles are the exact
    // phrases the two clients emit — MEASURED, not guessed. A loose match on
    // `sasl` or `scram` would be satisfied by the spec's own `scramSha512`
    // echoed in a log line, which is not evidence of anything.
    //   * librdkafka: `SASL authentication error: Authentication failed during
    //     authentication due to invalid credentials with SASL mechanism
    //     SCRAM-SHA-512`
    //   * the engine's RFC 5802 client: `Authentication error: SCRAM
    //     authentication failed: …`
    let authy = [
        "SASL authentication error",
        "Authentication failed during authentication",
        "SCRAM authentication failed",
        "Authentication error:",
    ];
    let matched: Vec<&&str> = authy.iter().filter(|n| both.contains(**n)).collect();
    assert!(
        !matched.is_empty(),
        "the failure names nothing about authentication, so it is indistinguishable from a \
         dead broker:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !both.contains("No available brokers"),
        "`No available brokers` is what a silent PLAINTEXT downgrade against a SASL-only \
         listener looks like — the protocol map must say SASL_PLAINTEXT:\n{both}"
    );
    // The transcript, so the report can quote the product rather than this
    // test's expectation of it.
    for line in both.lines() {
        if authy.iter().any(|n| line.contains(n)) {
            let cut = line.len().min(400);
            eprintln!("[t7] auth-failure line: {}", &line[..cut]);
            break;
        }
    }
    eprintln!(
        "[t7] wrong-password drill failed with exit {:?}, matched {matched:?}",
        r.out.status.code()
    );
}

// ---------------------------------------------------------------------------
// 5. The K8S listener: a published port, and a POD.
// ---------------------------------------------------------------------------

/// **The published half.** Port 9095 answers on the host, and the advertised
/// name a client on `kafka-net` reads back out of the broker's METADATA is
/// `host.docker.internal:9095`.
///
/// Two reads and they are not the same claim. A published port says a TCP
/// connection can be made; the advertised name says where the broker will send
/// the client NEXT, which is the thing that made `EXTERNAL://localhost:9092`
/// useless from a pod. `kafka-broker-api-versions.sh` prints the metadata's
/// own broker endpoint, so the string asserted here is the broker's answer and
/// not this file's.
///
/// Mutant 4 — drop `K8S` from `ports:` — fails the first half at assertion
/// time.
#[test]
fn a_pod_reachable_listener_is_published() {
    // `nc -z localhost 9095` as a TCP connect, so the assertion is on a
    // syscall rather than on a tool this repository does not pin.
    let sock: std::net::SocketAddr = "127.0.0.1:9095".parse().unwrap();
    let conn = std::net::TcpStream::connect_timeout(&sock, std::time::Duration::from_secs(5));
    assert!(
        conn.is_ok(),
        "nothing answers on localhost:9095 ({:?}) — the K8S listener is not published; \
         `ports:` must carry \"9095:9095\"",
        conn.err()
    );

    // The advertised name, read from the broker's metadata by a client ON
    // `kafka-net`.
    let md = metadata_from_kafka_net();
    assert!(
        md.contains(&format!("{BOOTSTRAP_K8S} (id:")),
        "the broker's metadata over the K8S listener does not advertise {BOOTSTRAP_K8S}; \
         it said:\n{md}"
    );
    eprintln!("[t7] K8S advertised name, from kafka-net: {BOOTSTRAP_K8S}");
}

/// `kafka-broker-api-versions.sh --bootstrap-server kafka-broker-1:9095` run
/// from a container ON `kafka-net`. Its first line per broker is
/// `<advertised host>:<port> (id: … rack: …)`, i.e. the metadata's own
/// endpoint — which is the string this file needs and the one a pod will be
/// handed.
fn metadata_from_kafka_net() -> String {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "run",
        "--rm",
        "-T",
        "-e",
        "KAFKA_OPTS=",
        "--entrypoint",
        "kafka-broker-api-versions",
        "topic-setup",
        "--bootstrap-server",
        "kafka-broker-1:9095",
    ]);
    let o = c.current_dir(root()).output().expect("docker compose run");
    let text = format!("{}{}", o.stdout_utf8(), o.stderr_utf8());
    assert!(
        o.status.success(),
        "a client on kafka-net could not read metadata over the K8S listener. This is the \
         `host.docker.internal` resolution the whole Kubernetes path depends on:\n{text}"
    );
    text
}

/// **A POD, NOT A PORT (critique A F17).**
///
/// `host.docker.internal` resolution inside pods is a Docker Desktop
/// behaviour, not a Kubernetes one, and spec §10 makes
/// `KafkaCluster.spec.bootstrapServers` in Demo 1 exactly
/// `host.docker.internal:9095`. Phase B (T24) and Phase C (T28) are built on
/// it. So this row runs a real pod in namespace `logweir-t7`, which it creates
/// and deletes (STANDING RULE 13), against the context `docker-desktop` and no
/// other (STANDING RULE 12).
///
/// The probe is `nc -z host.docker.internal 9095` and the exit code is read
/// DIRECTLY off the `kubectl run` status (`--restart=Never` makes `kubectl
/// run` exit with the container's own code).
///
/// Mutant 4 — drop `K8S` from `ports:` — fails this at assertion time too.
#[test]
fn a_pod_really_reaches_the_k8s_listener() {
    kubectl(&["delete", "ns", NAMESPACE, "--ignore-not-found"]);
    let created = kubectl(&["create", "ns", NAMESPACE]);
    assert!(
        created.status.success(),
        "could not create namespace {NAMESPACE}: {}",
        created.stderr_utf8()
    );

    let out = kubectl(&[
        "-n",
        NAMESPACE,
        "run",
        "probe",
        "--rm",
        "-i",
        "--restart=Never",
        &format!("--image={}", probe_image()),
        "--",
        "sh",
        "-c",
        "nc -z host.docker.internal 9095",
    ]);
    let rc = out.status.code();
    let text = format!("{}{}", out.stdout_utf8(), out.stderr_utf8());

    // Delete the namespace BEFORE asserting, so a failure still leaves the
    // cluster as this task found it (STANDING RULE 13).
    let deleted = kubectl(&["delete", "ns", NAMESPACE, "--ignore-not-found"]);
    eprintln!(
        "[t7] namespace {NAMESPACE} deleted, rc={:?}",
        deleted.status.code()
    );

    assert_eq!(
        rc,
        Some(0),
        "a pod could not reach {BOOTSTRAP_K8S}. If the NAME did not resolve inside the pod \
         this is a controller decision and not an implementer's: the fallback is \
         `hostNetwork` or the host's LAN address in \
         `KafkaCluster.spec.bootstrapServers` (spec §10). STOP AND REPORT.\n{text}"
    );
    eprintln!("[t7] pod reached {BOOTSTRAP_K8S} · rc=0");
}

/// `kubectl`, always with the context named explicitly (STANDING RULE 12).
fn kubectl(args: &[&str]) -> std::process::Output {
    let mut c = Command::new("kubectl");
    c.args(["--context", KUBE_CONTEXT]);
    c.args(args);
    c.current_dir(root()).output().expect("kubectl")
}

/// The probe pod's image, and the claim it carries is
/// **`host.docker.internal:9095` resolution inside a pod** — never anything
/// about the image.
///
/// `busybox` (i.e. `busybox:latest`) is what spec §10's procedure names, and
/// it is not present on this host, so Docker Desktop's Kubernetes would pull
/// it. `LOGWEIR_T7_PROBE_IMAGE` overrides it with a locally present image
/// (`alpine:3.21` carries the same BusyBox `nc -z`) for a host that cannot
/// pull, which is STANDING RULE 8's `imagePullPolicy: Never` posture applied
/// to a one-shot probe.
fn probe_image() -> String {
    std::env::var("LOGWEIR_T7_PROBE_IMAGE").unwrap_or_else(|_| "busybox".to_string())
}

// ---------------------------------------------------------------------------
// The shared bucket, left exactly as it was found.
// ---------------------------------------------------------------------------

/// An archive segment object, in the shape the PINNED ENGINE really writes:
/// `<backup_id>/<backup_id>/topics/<topic>/partition=<n>/segment-<offset>.bin.zst`.
///
/// **The task brief's acceptance writes `topics/*/partition-*/segment-*`, with
/// a HYPHEN, and the engine writes `partition=`.** Measured on Task 7's first
/// run of this file, whose in-network arm exited 0 and wrote
/// `mvp-scram-innet/mvp-scram-innet/topics/orders/partition=0/segment-00000000000000000000.bin.zst`
/// — so a hyphen predicate reported "no segment" over a complete archive. The
/// engine's shape is the contract; the brief's glob was the error. The same
/// `partition=` shape is already relied on by
/// `harness::archive_segment_keys`'s own `segment-` leaf test, which is
/// deliberately mirrored here rather than tightened past it.
fn is_segment_key(key: &str) -> bool {
    key.contains("/topics/")
        && key.contains("/partition=")
        && key
            .rsplit('/')
            .next()
            .is_some_and(|leaf| leaf.starts_with("segment-"))
}

/// Every object under one archive prefix, listed from the bucket ROOT so the
/// keys come back bucket-relative (`harness::archive_segment_keys` explains
/// why `mc --json ls` on a sub-path does not).
fn archive_keys_under(backup_id: &str) -> Vec<String> {
    let o = mc(&[
        "--json",
        "ls",
        "--recursive",
        &format!("local/{ARCHIVE_BUCKET}"),
    ]);
    o.stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .filter(|k| k.starts_with(&format!("{backup_id}/")))
        .collect()
}

/// Remove every archive AND every receipt this file's two engine rows put in
/// the shared bucket, and PROVE they are gone.
///
/// Both classes name a `mvp-scram…` prefix: the archive at
/// `mvp-scram…/…` and the evidence at Global Constraint 6's
/// `logweir/backups/mvp-scram…/…`. That is what makes the sweep exact rather
/// than heuristic — the same shape `backup_argv.rs::sweep_real_engine_archives`
/// uses, for the same reason.
fn sweep_scram_archives() {
    let mine = |keys: Vec<String>| -> Vec<String> {
        keys.into_iter()
            .filter(|k| {
                k.starts_with(SCRAM_ID_PREFIX)
                    || k.starts_with(&format!("{RECEIPT_PREFIX}backups/{SCRAM_ID_PREFIX}"))
            })
            .collect()
    };
    let list = || -> Vec<String> {
        mc(&[
            "--json",
            "ls",
            "--recursive",
            &format!("local/{ARCHIVE_BUCKET}"),
        ])
        .stdout_utf8()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(str::to_string))
        .collect()
    };

    let found = mine(list());
    if !found.is_empty() {
        let prefixes: std::collections::BTreeSet<String> = found
            .iter()
            .map(|k| {
                let segs: Vec<&str> = k.split('/').collect();
                if k.starts_with(RECEIPT_PREFIX) {
                    // `logweir/backups/<backup_id>/…`
                    segs[..3].join("/")
                } else {
                    segs[0].to_string()
                }
            })
            .collect();
        for p in prefixes {
            // A prefix that does not exist is not a failure worth stopping
            // for; the LISTING below is the property that matters.
            let _ = mc(&[
                "rm",
                "--recursive",
                "--force",
                &format!("local/{ARCHIVE_BUCKET}/{p}/"),
            ]);
        }
    }

    let left = mine(list());
    assert!(
        left.is_empty(),
        "this file's archive is still in {ARCHIVE_BUCKET}, and \
         harness::corrupt_a_non_oldest_segment would quarantine one of ITS segments instead \
         of the drill's: {left:?}"
    );
}
