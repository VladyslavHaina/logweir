//! FX-20: the Job builder's backstop.
//!
//! Every builder in this crate projects a credential WITH its binding pair
//! (the per-site rows say so: `protection_controller`, `destination_controller`,
//! `retention_policy_controller`, `backup_controller`, `restore_controller`,
//! `preflight_controller`, `connection`). This file holds the one place that
//! makes a FUTURE builder that forgets the pair fail closed: `job::build` gives
//! a credential projected with no expectation the never-satisfied
//! `unbound:missing-expectation`, so the runner refuses it rather than treating
//! it as a hand-run credential nobody checks.

use logweir_core::credential_binding::{GUARDED_CREDENTIALS, UNBOUND_MISSING_EXPECTATION};
use weirkeeper::job::{self, EnvFromSecret, RunnerJobSpec, RunnerOwner};

fn spec(from_secret: Vec<EnvFromSecret>, literal: Vec<(String, String)>) -> RunnerJobSpec {
    RunnerJobSpec {
        name: "j".to_string(),
        namespace: "ns".to_string(),
        owner: RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "Backup".to_string(),
            name: "b".to_string(),
            uid: "u".to_string(),
        },
        args: vec!["backup".to_string()],
        deadline_seconds: 60,
        service_account_name: "logweir-runner".to_string(),
        secret_mounts: Vec::new(),
        config_map_mounts: Vec::new(),
        env_from_secret: from_secret,
        env_literal: literal,
        plan_config_map: None,
        image: None,
        image_pull_policy: None,
        resources: None,
    }
}

fn env_values(spec: &RunnerJobSpec, name: &str) -> Vec<Option<String>> {
    let job = job::build(spec);
    job.spec
        .and_then(|s| s.template.spec)
        .and_then(|p| p.containers.into_iter().next())
        .and_then(|c| c.env)
        .unwrap_or_default()
        .into_iter()
        .filter(|e| e.name == name)
        .map(|e| e.value)
        .collect()
}

/// **Each guarded credential projected with no expectation gets the
/// fail-closed one; with an expectation, the builder's own value stands
/// alone; an unguarded variable and a Job with no credential get nothing.**
///
/// KILLS: the backstop removed; the backstop overriding (or duplicating) a
/// builder's real expectation; the table missing a credential.
#[test]
fn a_credential_projected_without_an_expectation_fails_closed() {
    for (credential, _, expected) in GUARDED_CREDENTIALS {
        let projected = EnvFromSecret {
            name: credential.to_string(),
            secret_name: "some-secret".to_string(),
            optional: false,
            key: "k".to_string(),
        };
        let forgot = spec(vec![projected.clone()], Vec::new());
        assert_eq!(
            env_values(&forgot, expected),
            vec![Some(UNBOUND_MISSING_EXPECTATION.to_string())],
            "{credential}: a forgotten pair fails closed"
        );
        let paired = spec(
            vec![projected],
            vec![(expected.to_string(), "v1:mine:sha256:00".to_string())],
        );
        assert_eq!(
            env_values(&paired, expected),
            vec![Some("v1:mine:sha256:00".to_string())],
            "{credential}: the builder's own expectation, once"
        );
    }
    // CONTROL: no credential, nothing added.
    let none = spec(Vec::new(), Vec::new());
    for (_, _, expected) in GUARDED_CREDENTIALS {
        assert!(env_values(&none, expected).is_empty());
    }
    // The table covers every credential variable the builders project.
    let names: Vec<&str> = GUARDED_CREDENTIALS.iter().map(|(c, _, _)| *c).collect();
    for credential in [
        weirkeeper::destination::AWS_ACCESS_KEY_ID_ENV,
        weirkeeper::destination::EVIDENCE_ACCESS_KEY_ID_ENV,
        logweir_core::check_contract::EVIDENCE_READ_ENV.access_key_id,
        weirkeeper::controllers::protection_policy::ROUTING_KEY_ENV,
        weirkeeper::controllers::protection_policy::WEBHOOK_URL_ENV,
        weirkeeper::controllers::protection_policy::SLACK_WEBHOOK_URL_ENV,
    ] {
        assert!(names.contains(&credential), "{credential} is not guarded");
    }
}
