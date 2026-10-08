//! FX-20 fix round, review F1: an inline `Restore`'s LOCATION binding left out
//! the S3 `region`, and on an endpoint-less location `object_store` builds the
//! host from it (`s3.<region>.amazonaws.com`). The reviewer's probe kept the
//! victim's bucket, set `region: "x@127.0.0.1:<port>/"`, got the victim
//! Secret's binding from `inline_restore_binding_env`, and the runner's store
//! dialled the loopback listener (a TLS ClientHello arrived).
//!
//! These rows are that probe, now refused twice, each beside its negative
//! control: the controller expects ANOTHER binding for the attacker's plan
//! (the region is bound), and no store is built over the injected region, so
//! nothing reaches the listener — while a store over a real region and the
//! listener's own endpoint does dial it, which is what makes "nothing arrived"
//! an observation and not a vacuous one.

use logweir_core::credential_binding as cb;
use logweir_store::{CredentialSource, Store, StoreOptions};
use std::net::TcpListener;
use std::time::{Duration, Instant};
use weirkeeper::controllers::restore::inline_restore_binding_env;
use weirkeeper::destination::DestinationEnv;

/// The reviewer's victim plan: the source archive and the evidence store in
/// `victim-backups` on the AWS default endpoint, region `us-east-1`.
const VICTIM_PLAN: &str = "\
source:
  storage:
    backend: s3
    bucket: victim-backups
    prefix: team-a
    region: us-east-1
    path_style: true
    allow_http: false
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: [scratch-0:9092]
  mode: scratch
  topic_mapping_prefix: \"drill-\"
  marker_topic: logweir.scratch
  default_replication_factor: 1
  teardown: delete
restore:
  point_in_time: \"2026-09-07T14:05:00Z\"
sample:
  window_start: \"2026-09-07T12:00:00Z\"
  window_end: \"2026-09-07T15:00:00Z\"
  records_per_partition: 25
  anchor: head
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
evidence:
  backend: s3
  bucket: victim-backups
  prefix: logweir/
  region: us-east-1
  path_style: true
  allow_http: false
";

fn expected(env: &DestinationEnv, var: &str) -> Option<String> {
    env.literals
        .iter()
        .find(|(name, _)| name == var)
        .map(|(_, value)| value.clone())
}

/// The plan with `region: us-east-1` replaced in the source block (and, with
/// `both`, in the evidence block too).
fn with_region(region: &str, both: bool) -> String {
    let line = "    region: us-east-1\n";
    let source = VICTIM_PLAN.replacen(line, &format!("    region: \"{region}\"\n"), 1);
    if both {
        source.replacen(
            "  region: us-east-1\n  path_style",
            &format!("  region: \"{region}\"\n  path_style"),
            1,
        )
    } else {
        source
    }
}

#[test]
fn fx20_an_injected_region_beside_the_victims_bucket_expects_another_binding() {
    let victim = inline_restore_binding_env("victim-s3", VICTIM_PLAN, &[]);
    let victim_archive = expected(&victim, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV)
        .expect("the source location's expectation");
    assert!(
        victim_archive.starts_with("v1:location:sha256:"),
        "{victim_archive}"
    );

    // THE PROBE: same bucket, injected region, on the source and then also on
    // the evidence store. Neither expectation is the victim's any more.
    for both in [false, true] {
        let attacker =
            inline_restore_binding_env("victim-s3", &with_region("x@127.0.0.1:9/", both), &[]);
        assert_ne!(
            expected(&attacker, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV).as_deref(),
            Some(victim_archive.as_str()),
            "the injected source region still expects the victim's binding"
        );
        if both {
            for (var, value) in &attacker.literals {
                assert_ne!(
                    value, &victim_archive,
                    "{var} expects the victim's location binding"
                );
            }
        }
    }
    // A real other region is another location too: the region is bound, not
    // merely validated.
    let eu = inline_restore_binding_env("victim-s3", &with_region("eu-west-1", true), &[]);
    assert_ne!(
        expected(&eu, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV).as_deref(),
        Some(victim_archive.as_str())
    );

    // NEGATIVE CONTROL: a plan that differs only in its PREFIX (the key space,
    // which an inline location leaves out) expects the victim's binding, so
    // the inequalities above are about the region and not about any edit.
    let other_prefix = VICTIM_PLAN.replacen("prefix: team-a", "prefix: team-b", 1);
    assert_ne!(other_prefix, VICTIM_PLAN);
    let same = inline_restore_binding_env("victim-s3", &other_prefix, &[]);
    assert_eq!(
        expected(&same, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV).as_deref(),
        Some(victim_archive.as_str())
    );

    // A plan that omits the region expects the region the Job is handed
    // (`AWS_REGION`, object_store's own fall-back) — the same location as the
    // plan that states it, and so the same binding as a `Backup` there.
    let unstated: String = VICTIM_PLAN
        .lines()
        .filter(|line| line.trim() != "region: us-east-1")
        .map(|line| format!("{line}\n"))
        .collect();
    assert!(!unstated.contains("region"));
    let forwarded = inline_restore_binding_env(
        "victim-s3",
        &unstated,
        &[("AWS_REGION".to_string(), "us-east-1".to_string())],
    );
    assert_eq!(
        expected(&forwarded, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV).as_deref(),
        Some(victim_archive.as_str())
    );
    let not_forwarded = inline_restore_binding_env("victim-s3", &unstated, &[]);
    assert_ne!(
        expected(&not_forwarded, cb::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV).as_deref(),
        Some(victim_archive.as_str())
    );
}

/// Accepts connections on `listener` for `window`, and counts them.
fn connections_within(listener: &TcpListener, window: Duration) -> usize {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + window;
    let mut accepted = 0;
    while Instant::now() < deadline {
        match listener.accept() {
            Ok((socket, _)) => {
                accepted += 1;
                drop(socket);
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    accepted
}

fn options() -> StoreOptions {
    // Static keys assembled at run time: a fixture, never a real credential.
    let access_key_id = ["AKIA", "FX20", "REGION", "PROBE"].concat();
    StoreOptions {
        credentials: CredentialSource::Static {
            access_key_id,
            secret_access_key: "fx20-not-a-secret".repeat(2),
            session_token: None,
        },
        request_timeout: Some(Duration::from_secs(2)),
        max_retries: Some(0),
        ..StoreOptions::default()
    }
}

#[test]
fn fx20_no_store_is_built_over_an_injected_region_and_a_real_one_dials() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let plan: logweir_core::spec::RestoreSpec =
        serde_yaml::from_str(&with_region(&format!("x@127.0.0.1:{port}/"), true)).unwrap();

    // THE PROBE'S DIAL, REFUSED: neither constructor builds a client over the
    // injected region, by name, and the listener sees nothing.
    let source = Store::read_only_from_url(&plan.source.storage)
        .err()
        .expect("the legacy read path refuses the injected region");
    let evidence = Store::from_url_with(&plan.evidence, &options())
        .err()
        .expect("the explicit path refuses the injected region");
    for refusal in [source.to_string(), evidence.to_string()] {
        assert!(refusal.contains("StorageRegionInvalid"), "{refusal}");
        assert!(!refusal.contains("127.0.0.1"), "never the value: {refusal}");
    }
    assert_eq!(connections_within(&listener, Duration::from_millis(500)), 0);

    // NEGATIVE CONTROL: a real region and the listener as the endpoint — the
    // same constructor dials, so the listener can see a dial when one happens.
    let control = logweir_core::engine::StorageUrl::S3 {
        bucket: "victim-backups".into(),
        prefix: "logweir/".into(),
        region: Some("us-east-1".into()),
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        path_style: true,
        allow_http: true,
    };
    let store = Store::from_url_with(&control, &options()).expect("a real region builds");
    let reader = std::thread::spawn(move || store.get("probe").err());
    let seen = connections_within(&listener, Duration::from_secs(3));
    let _ = reader.join();
    assert!(seen >= 1, "the control store never dialled the listener");
}
