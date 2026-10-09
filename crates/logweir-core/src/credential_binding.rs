//! FX-20: the credential binding for EVERY credential reference, not only a
//! Kafka connection's.
//!
//! # The class this closes
//!
//! PROD-01.3 bound a `KafkaCluster`'s credential Secret to the connection that
//! names it ([`crate::connection::credential_binding`]). The same confused
//! deputy exists wherever a writable object names a credential Secret BESIDE an
//! endpoint its author chooses:
//!
//! | Object | Credential | Where it is presented |
//! |---|---|---|
//! | `ProtectionPolicy` notification route | a PagerDuty routing key, a webhook or Slack URL | the route's `endpoint` (PagerDuty), or the URL inside the Secret |
//! | `BackupDestination` `SecretKeys` grant | an S3 key pair and session token | `spec.storage` (bucket, endpoint) |
//! | an inline `archive.secretRef` (Backup, Restore, BackupSchedule, RecoveryCatalog, Preflight) | the same | the archive location the runner dials |
//! | `RetentionPolicy` `enforcement.credentialSecretRef` | a DELETE-capable S3 key pair | the destination's storage |
//!
//! Without a binding, a principal who may write one of those objects but may
//! not read Secrets names a Secret written for somebody else, points the
//! endpoint at a host they control, and Logweir's runner reads the Secret and
//! presents it there. For PagerDuty the routing key itself travels in the
//! request body. For S3, SigV4 never sends the secret key, but it does send the
//! access key id, a replayable signature and any session token in clear
//! headers.
//!
//! # The rule, unchanged from PROD-01.3
//!
//! A credential Secret is used only when it carries, under
//! [`CREDENTIAL_BINDING_KEY`], the binding the CONTROLLER computed for the
//! object and endpoint the Job was built for. The controller projects that
//! expectation as a literal and the Secret's key as an OPTIONAL `secretKeyRef`
//! (an unbound Secret still reaches the runner, which refuses it by name
//! instead of the pod failing to start), and the runner compares the two
//! BEFORE it builds any client or opens any socket
//! ([`crate::connection::check_credential_binding`]). The refusal is
//! [`CREDENTIAL_BINDING_MISMATCH`] everywhere.
//!
//! # What each binding covers
//!
//! Every binding is a pure function of PUBLIC values — an object UID, a
//! location, an endpoint — so the console API (which writes it into the Secrets
//! it creates), the controller (which projects and publishes it) and an
//! operator (who reads it from the object's status) compute one string. No
//! binding ever covers a credential value.
//!
//! * [`destination_binding`]: the `BackupDestination`'s UID and its whole
//!   archive route (bucket, prefix, region, endpoint, addressing, transport).
//!   `spec.storage` and `spec.transport.security` are immutable, so a different
//!   endpoint is a different destination with a different UID.
//! * [`retention_binding`]: the `RetentionPolicy`'s UID, the archive route of
//!   the destination it resolves to and its scope prefix. `destinationRef` is
//!   immutable by NAME only, so the route is what stops a re-created
//!   destination of the same name from receiving the delete-capable key.
//! * [`notification_binding`]: the `ProtectionPolicy`'s UID, the sink kind
//!   and, for PagerDuty, the endpoint. The policy's spec is MUTABLE, so the
//!   endpoint in the digest is the only thing that makes "edit the endpoint,
//!   keep the routing key" a refusal.
//! * [`archive_location_binding`]: an inline (legacy) archive has no object of
//!   its own — a `Backup` is one-shot and a schedule's `Backup`s are minted by
//!   the controller — so its Secret is bound to the LOCATION the runner dials:
//!   the scheme, the bucket, the endpoint, the region, the addressing style
//!   and `allowHttp` — every field that shapes the URL or its transport —
//!   never the prefix. Any object in the namespace may then use that
//!   credential, but only at that location, which is what a
//!   `BackupDestination` in the same namespace already allows. The region is
//!   ALSO refused outright when it is not a region name
//!   ([`crate::guard::reject_invalid_storage_region`]), FX-20's fix round F1.

use crate::destination::DestinationRole;
use crate::engine::StorageUrl;

pub use crate::connection::{
    check_credential_binding, CredentialBindingRefusal, CREDENTIAL_BINDING_KEY,
    CREDENTIAL_BINDING_MISMATCH,
};

/// The expected value a controller projects when it cannot compute a binding
/// (no UID). No Secret can carry it by accident, so the credential is refused:
/// FAIL CLOSED. The same spelling PROD-01.3's connection projection uses.
pub const UNBOUND_NO_UID: &str = "unbound:no-uid";

/// The projected binding of the credential in `AWS_ACCESS_KEY_ID`,
/// `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` — a destination's archive
/// grant, an inline archive's `secretRef`, or a retention Job's delete-capable
/// key. Read from the Secret's [`CREDENTIAL_BINDING_KEY`], optionally.
pub const ARCHIVE_CREDENTIAL_BINDING_ENV: &str = "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING";
/// What the controller expects [`ARCHIVE_CREDENTIAL_BINDING_ENV`] to be.
pub const ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED";
/// The projected binding of the credential in the `LOGWEIR_EVIDENCE_AWS_*`
/// variables, or — when the evidence grant IS the archive grant — of the
/// archive Secret, compared against the EVIDENCE destination's binding.
pub const EVIDENCE_CREDENTIAL_BINDING_ENV: &str = "LOGWEIR_EVIDENCE_CREDENTIAL_BINDING";
/// What the controller expects [`EVIDENCE_CREDENTIAL_BINDING_ENV`] to be.
pub const EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "LOGWEIR_EVIDENCE_CREDENTIAL_BINDING_EXPECTED";

/// The projected binding of a CHECK pod's separately projected
/// `evidenceRead` grant (`LOGWEIR_EVIDENCE_READ_AWS_*`,
/// [`crate::check_contract::EVIDENCE_READ_ENV`]).
pub const EVIDENCE_READ_CREDENTIAL_BINDING_ENV: &str = "LOGWEIR_EVIDENCE_READ_CREDENTIAL_BINDING";
/// What the controller expects [`EVIDENCE_READ_CREDENTIAL_BINDING_ENV`] to be.
pub const EVIDENCE_READ_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "LOGWEIR_EVIDENCE_READ_CREDENTIAL_BINDING_EXPECTED";

/// The projected binding of a delivery Job's PagerDuty routing-key Secret.
pub const NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_ENV: &str = "NOTIFY_PAGERDUTY_CREDENTIAL_BINDING";
/// What the controller expects [`NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_ENV`] to be.
pub const NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_EXPECTED";
/// The projected binding of a delivery Job's webhook-URL Secret.
pub const NOTIFY_WEBHOOK_CREDENTIAL_BINDING_ENV: &str = "NOTIFY_WEBHOOK_CREDENTIAL_BINDING";
/// What the controller expects [`NOTIFY_WEBHOOK_CREDENTIAL_BINDING_ENV`] to be.
pub const NOTIFY_WEBHOOK_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "NOTIFY_WEBHOOK_CREDENTIAL_BINDING_EXPECTED";
/// The projected binding of a delivery Job's Slack webhook-URL Secret.
pub const NOTIFY_SLACK_CREDENTIAL_BINDING_ENV: &str = "NOTIFY_SLACK_CREDENTIAL_BINDING";
/// What the controller expects [`NOTIFY_SLACK_CREDENTIAL_BINDING_ENV`] to be.
pub const NOTIFY_SLACK_CREDENTIAL_BINDING_EXPECTED_ENV: &str =
    "NOTIFY_SLACK_CREDENTIAL_BINDING_EXPECTED";

/// FX-20c: the `(projected, expected)` pair a CHECK pod carries for one
/// Secret-backed grant its plan lists in
/// [`crate::check_contract::DestinationPlan::grant_bindings`] — the grant's
/// Secret's [`CREDENTIAL_BINDING_KEY`] as an OPTIONAL `secretKeyRef`, and the
/// destination's binding as a literal.
///
/// **ONLY THE BINDING.** No credential variable is projected beside it, so a
/// grant the check does not exercise (`archiveWrite`, whose write no check may
/// probe, or any grant beside the one a `destinationAccess` reads with) is
/// COMPARED and never USED: nothing is dialled with a foreign Secret to learn
/// that it is foreign. One pair per role, so one plan names each role once.
#[must_use]
pub const fn grant_binding_env(role: DestinationRole) -> (&'static str, &'static str) {
    match role {
        DestinationRole::ArchiveWrite => (
            "LOGWEIR_ARCHIVE_WRITE_GRANT_BINDING",
            "LOGWEIR_ARCHIVE_WRITE_GRANT_BINDING_EXPECTED",
        ),
        DestinationRole::ArchiveRead => (
            "LOGWEIR_ARCHIVE_READ_GRANT_BINDING",
            "LOGWEIR_ARCHIVE_READ_GRANT_BINDING_EXPECTED",
        ),
        DestinationRole::EvidenceWrite => (
            "LOGWEIR_EVIDENCE_WRITE_GRANT_BINDING",
            "LOGWEIR_EVIDENCE_WRITE_GRANT_BINDING_EXPECTED",
        ),
        DestinationRole::EvidenceRead => (
            "LOGWEIR_EVIDENCE_READ_GRANT_BINDING",
            "LOGWEIR_EVIDENCE_READ_GRANT_BINDING_EXPECTED",
        ),
    }
}

/// The `spec.access` field that names `role`'s grant — how a
/// `destination.credentialBound` row names a grant to an operator, and the
/// key of its per-grant fact.
#[must_use]
pub const fn grant_field(role: DestinationRole) -> &'static str {
    match role {
        DestinationRole::ArchiveWrite => "archiveWrite",
        DestinationRole::ArchiveRead => "archiveRead",
        DestinationRole::EvidenceWrite => "evidenceWrite",
        DestinationRole::EvidenceRead => "evidenceRead",
    }
}

/// The object-store credential pairs a runner checks at start-up, as
/// `(projected, expected)`, in a fixed order. With
/// [`crate::connection::SOURCE_CREDENTIAL_BINDING_ENV`] and its target twin
/// they are every binding pair a store- or Kafka-reading runner can carry.
pub const STORE_BINDING_PAIRS: [(&str, &str); 3] = [
    (
        ARCHIVE_CREDENTIAL_BINDING_ENV,
        ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        EVIDENCE_CREDENTIAL_BINDING_ENV,
        EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        EVIDENCE_READ_CREDENTIAL_BINDING_ENV,
        EVIDENCE_READ_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
];

/// The value a Job builder backstop projects as the expectation of a
/// credential it found projected WITHOUT one — never satisfied, so the runner
/// refuses that credential rather than using it unchecked.
pub const UNBOUND_MISSING_EXPECTATION: &str = "unbound:missing-expectation";

/// Every credential variable a controller-built Job may project from a
/// Secret, with its binding pair: `(credential, projected binding, expected
/// binding)`. The Job builder's backstop reads this table: a credential
/// projected with no expectation beside it gets
/// [`UNBOUND_MISSING_EXPECTATION`], so a builder that forgets the pair fails
/// CLOSED instead of reaching a runner as a "hand-run" credential nobody
/// checks.
pub const GUARDED_CREDENTIALS: [(&str, &str, &str); 8] = [
    (
        "LOGWEIR_SOURCE_PASSWORD",
        crate::connection::SOURCE_CREDENTIAL_BINDING_ENV,
        crate::connection::SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "LOGWEIR_TARGET_PASSWORD",
        crate::connection::TARGET_CREDENTIAL_BINDING_ENV,
        crate::connection::TARGET_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "AWS_ACCESS_KEY_ID",
        ARCHIVE_CREDENTIAL_BINDING_ENV,
        ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID",
        EVIDENCE_CREDENTIAL_BINDING_ENV,
        EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "LOGWEIR_EVIDENCE_READ_AWS_ACCESS_KEY_ID",
        EVIDENCE_READ_CREDENTIAL_BINDING_ENV,
        EVIDENCE_READ_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "PAGERDUTY_ROUTING_KEY",
        NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_ENV,
        NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "NOTIFY_WEBHOOK_URL",
        NOTIFY_WEBHOOK_CREDENTIAL_BINDING_ENV,
        NOTIFY_WEBHOOK_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
    (
        "NOTIFY_SLACK_WEBHOOK_URL",
        NOTIFY_SLACK_CREDENTIAL_BINDING_ENV,
        NOTIFY_SLACK_CREDENTIAL_BINDING_EXPECTED_ENV,
    ),
];

/// One binding: `v1:<subject>:sha256:<hex>` over a domain-separated canonical
/// form. `subject` is the object UID, or `location` for an inline archive.
fn binding(subject: &str, kind: &str, lines: &[(&str, &str)]) -> String {
    let mut canonical = format!("logweir-credential-binding/v1\nkind={kind}\nsubject={subject}\n");
    for (name, value) in lines {
        canonical.push_str(name);
        canonical.push('=');
        canonical.push_str(value);
        canonical.push('\n');
    }
    let digest = crate::ids::sha256_prefixed(canonical.as_bytes());
    format!("v1:{subject}:{digest}")
}

/// The ROUTE of an archive `StorageUrl`, every field that decides where a
/// request goes and how, in a fixed order. `with_prefix` is `false` for an
/// inline archive, whose binding is the location and not the key space.
///
/// **EVERY S3 FIELD BUT THE PREFIX IS IN BOTH FORMS** (FX-20 fix round,
/// review F1). The location form once kept only scheme, bucket and endpoint;
/// but with no endpoint `object_store` builds the host from the REGION
/// (`s3.<region>.amazonaws.com`), so a plan keeping the victim's bucket and
/// naming `region: "x@attacker/"` carried the victim's binding to the
/// attacker's host. The addressing style decides whether the bucket is a host
/// label, and `allowHttp` whether the request may travel in the clear, so
/// they are bound too. The prefix only names keys under the bound bucket on
/// the bound host (`object_store` encodes each path segment), which is why an
/// inline location leaves it out.
fn route_lines(storage: &StorageUrl, with_prefix: bool) -> Vec<(&'static str, String)> {
    match storage {
        StorageUrl::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            path_style,
            allow_http,
        } => {
            let mut lines = vec![
                ("scheme", "s3".to_string()),
                ("bucket", bucket.clone()),
                ("endpoint", normalized_endpoint(endpoint.as_deref())),
            ];
            if with_prefix {
                lines.push(("prefix", prefix.clone()));
            }
            lines.push(("region", region.clone().unwrap_or_default()));
            lines.push(("pathStyle", path_style.to_string()));
            lines.push(("allowHttp", allow_http.to_string()));
            lines
        }
        StorageUrl::Gcs { bucket, prefix } => {
            let mut lines = vec![("scheme", "gs".to_string()), ("bucket", bucket.clone())];
            if with_prefix {
                lines.push(("prefix", prefix.clone()));
            }
            lines
        }
        StorageUrl::Azure {
            account_name,
            container_name,
            prefix,
        } => {
            let mut lines = vec![
                ("scheme", "az".to_string()),
                ("account", account_name.clone()),
                ("container", container_name.clone()),
            ];
            if with_prefix {
                lines.push(("prefix", prefix.clone()));
            }
            lines
        }
        StorageUrl::Filesystem { path } => vec![
            ("scheme", "file".to_string()),
            ("path", path.display().to_string()),
        ],
    }
}

/// An endpoint as the binding sees it: trimmed, ASCII-lower-cased, with any
/// trailing `/` removed, and `aws` for none. The SCHEME is kept — `http://`
/// and `https://` to one host are two routes, and only one of them encrypts the
/// signed request.
fn normalized_endpoint(endpoint: Option<&str>) -> String {
    match endpoint.map(str::trim).filter(|e| !e.is_empty()) {
        Some(e) => e.trim_end_matches('/').to_ascii_lowercase(),
        None => "aws".to_string(),
    }
}

/// The binding of a `BackupDestination`'s `SecretKeys` grants: its UID and its
/// whole archive route ([`crate::destination::DestinationLocation::
/// archive_storage_url`]). One value for every grant of the destination, so
/// one Secret may serve several of its roles; the roles are separated by the
/// object store's principals, not by the binding.
#[must_use]
pub fn destination_binding(uid: &str, archive: &StorageUrl) -> String {
    let lines = route_lines(archive, true);
    let borrowed: Vec<(&str, &str)> = lines.iter().map(|(n, v)| (*n, v.as_str())).collect();
    binding(uid, "BackupDestination", &borrowed)
}

/// The binding of an inline archive's `secretRef`: the LOCATION the runner
/// dials — for S3 the scheme, bucket, endpoint, region, addressing style and
/// `allowHttp` (every field but the prefix); the bucket for GCS; the account
/// and container for Azure — and no object UID. See the module header for why
/// an inline archive has no object to bind to.
#[must_use]
pub fn archive_location_binding(storage: &StorageUrl) -> String {
    let lines = route_lines(storage, false);
    let borrowed: Vec<(&str, &str)> = lines.iter().map(|(n, v)| (*n, v.as_str())).collect();
    binding("location", "ArchiveLocation", &borrowed)
}

/// The binding of a `RetentionPolicy`'s delete-capable credential: the
/// policy's UID, the archive route of the destination it resolves to, and the
/// scope prefix the key may delete under.
#[must_use]
pub fn retention_binding(uid: &str, archive: &StorageUrl, scope_prefix: &str) -> String {
    let mut lines = route_lines(archive, true);
    lines.push(("scope", scope_prefix.to_string()));
    let borrowed: Vec<(&str, &str)> = lines.iter().map(|(n, v)| (*n, v.as_str())).collect();
    binding(uid, "RetentionPolicy", &borrowed)
}

/// Which kind of notification sink a credential is for. Part of the binding,
/// so a policy's Slack Secret is never accepted as its PagerDuty routing key
/// (and posted to the PagerDuty endpoint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NotificationSink {
    /// PagerDuty Events v2: the routing key travels in the body, to
    /// `endpoint`.
    PagerDuty,
    /// A generic webhook: the URL is in the Secret.
    Webhook,
    /// A Slack incoming webhook: the URL is in the Secret.
    Slack,
}

impl NotificationSink {
    /// The spelling in the binding, in `status.credentialBindings[].sink` and
    /// in the delivery Job's `notify-result=` line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PagerDuty => "pagerduty",
            Self::Webhook => "webhook",
            Self::Slack => "slack",
        }
    }

    /// `(projected, expected)` — the two variables of this sink's binding pair.
    #[must_use]
    pub const fn binding_env(self) -> (&'static str, &'static str) {
        match self {
            Self::PagerDuty => (
                NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_ENV,
                NOTIFY_PAGERDUTY_CREDENTIAL_BINDING_EXPECTED_ENV,
            ),
            Self::Webhook => (
                NOTIFY_WEBHOOK_CREDENTIAL_BINDING_ENV,
                NOTIFY_WEBHOOK_CREDENTIAL_BINDING_EXPECTED_ENV,
            ),
            Self::Slack => (
                NOTIFY_SLACK_CREDENTIAL_BINDING_ENV,
                NOTIFY_SLACK_CREDENTIAL_BINDING_EXPECTED_ENV,
            ),
        }
    }
}

/// The binding of a `ProtectionPolicy` route's credential Secret: the policy's
/// UID, the sink kind, and — for PagerDuty — the endpoint the routing key is
/// POSTed to, `default` when the route names none. A webhook or Slack URL is
/// the credential itself, so its host is whatever the Secret's writer put
/// there and nothing else is bound.
#[must_use]
pub fn notification_binding(uid: &str, sink: NotificationSink, endpoint: Option<&str>) -> String {
    let endpoint = match sink {
        NotificationSink::PagerDuty => endpoint
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .unwrap_or("default")
            .to_string(),
        NotificationSink::Webhook | NotificationSink::Slack => "in-secret".to_string(),
    };
    binding(
        uid,
        "ProtectionPolicy",
        &[("sink", sink.as_str()), ("endpoint", endpoint.as_str())],
    )
}

/// Check one `(projected, expected)` pair through a lookup, so a caller can
/// test it without the process environment. A non-UTF-8 expected value is
/// refused (a controller never writes one); a non-UTF-8 projected value reads
/// as absent.
///
/// # Errors
///
/// [`CredentialBindingRefusal`] naming `projected_var`.
pub fn check_pair(
    projected_var: &'static str,
    expected_var: &'static str,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<(), CredentialBindingRefusal> {
    check_credential_binding(
        projected_var,
        get(expected_var).as_deref(),
        get(projected_var).as_deref(),
    )
}

/// FX-20c: compare one LISTED grant's binding ([`grant_binding_env`]) through
/// a lookup — [`check_pair`]'s rule with one difference. A grant the plan
/// lists is a grant the controller said it projected, so an ABSENT or blank
/// expectation is not "a hand-run process" but a pod that lost the
/// controller's half: it is refused, as [`UNBOUND_MISSING_EXPECTATION`],
/// never accepted.
///
/// # Errors
///
/// [`CredentialBindingRefusal`] naming the grant's projected variable; never
/// a value.
pub fn check_grant_binding(
    role: DestinationRole,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<(), CredentialBindingRefusal> {
    let (projected_var, expected_var) = grant_binding_env(role);
    let expected = get(expected_var)
        .filter(|e| !e.trim().is_empty())
        .unwrap_or_else(|| UNBOUND_MISSING_EXPECTATION.to_string());
    check_credential_binding(
        projected_var,
        Some(&expected),
        get(projected_var).as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(bucket: &str, prefix: &str, endpoint: Option<&str>) -> StorageUrl {
        StorageUrl::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: endpoint.map(str::to_string),
            path_style: true,
            allow_http: false,
        }
    }

    #[test]
    fn the_destination_binding_covers_the_uid_and_every_route_field() {
        let base_url = s3("b", "p", Some("https://minio.example:9000"));
        let base = destination_binding("uid-1", &base_url);
        assert!(base.starts_with("v1:uid-1:sha256:"), "{base}");
        let mut moved = vec![
            destination_binding("uid-2", &base_url),
            destination_binding(
                "uid-1",
                &s3("other", "p", Some("https://minio.example:9000")),
            ),
            destination_binding("uid-1", &s3("b", "q", Some("https://minio.example:9000"))),
            destination_binding("uid-1", &s3("b", "p", Some("https://evil.example:9000"))),
            destination_binding("uid-1", &s3("b", "p", Some("http://minio.example:9000"))),
            destination_binding("uid-1", &s3("b", "p", None)),
        ];
        for (field, flip) in [("region", 0), ("pathStyle", 1), ("allowHttp", 2)] {
            let mut url = base_url.clone();
            if let StorageUrl::S3 {
                region,
                path_style,
                allow_http,
                ..
            } = &mut url
            {
                match flip {
                    0 => *region = Some("eu-west-1".to_string()),
                    1 => *path_style = false,
                    _ => *allow_http = true,
                }
            }
            let b = destination_binding("uid-1", &url);
            assert_ne!(b, base, "the destination binding ignores {field}");
            moved.push(b);
        }
        for b in &moved {
            assert_ne!(*b, base);
        }
        // Case and a trailing slash are spelling, not a route.
        assert_eq!(
            destination_binding("uid-1", &s3("b", "p", Some("HTTPS://MinIO.example:9000/"))),
            base
        );
    }

    #[test]
    fn the_location_binding_covers_the_region_and_every_url_field_but_never_the_prefix() {
        // Review F1's probe shape: the victim's bucket, no endpoint, and a
        // region that is a host injection. Each S3 field that shapes the URL
        // or its transport moves the binding.
        let aws = archive_location_binding(&s3("victim-backups", "team-a", None));
        for (field, flip) in [("region", 0), ("pathStyle", 1), ("allowHttp", 2)] {
            let mut url = s3("victim-backups", "team-a", None);
            if let StorageUrl::S3 {
                region,
                path_style,
                allow_http,
                ..
            } = &mut url
            {
                match flip {
                    0 => *region = Some("x@127.0.0.1:9/".to_string()),
                    1 => *path_style = false,
                    _ => *allow_http = true,
                }
            }
            assert_ne!(
                archive_location_binding(&url),
                aws,
                "the location binding ignores {field}"
            );
        }
        let mut no_region = s3("victim-backups", "team-a", None);
        if let StorageUrl::S3 { region, .. } = &mut no_region {
            *region = None;
        }
        assert_ne!(archive_location_binding(&no_region), aws);

        let a = archive_location_binding(&s3("b", "team-a", Some("https://minio:9000")));
        assert!(a.starts_with("v1:location:sha256:"), "{a}");
        assert_eq!(
            a,
            archive_location_binding(&s3("b", "team-b", Some("https://minio:9000"))),
            "the prefix is the key space, not where the credential goes"
        );
        assert_ne!(
            a,
            archive_location_binding(&s3("evil", "team-a", Some("https://minio:9000")))
        );
        assert_ne!(
            a,
            archive_location_binding(&s3("b", "team-a", Some("https://evil:9000")))
        );
        assert_ne!(a, archive_location_binding(&s3("b", "team-a", None)));
        // An inline archive's binding is never a destination's, even at the
        // same location: a destination's Secret is its own.
        assert_ne!(
            a,
            destination_binding("location", &s3("b", "team-a", Some("https://minio:9000")))
        );
    }

    #[test]
    fn the_retention_binding_covers_the_uid_route_and_scope() {
        let url = s3("b", "p", Some("https://minio:9000"));
        let base = retention_binding("uid-1", &url, "p/old");
        assert_ne!(base, retention_binding("uid-2", &url, "p/old"));
        assert_ne!(base, retention_binding("uid-1", &url, "p"));
        assert_ne!(
            base,
            retention_binding("uid-1", &s3("b", "p", Some("https://evil:9000")), "p/old")
        );
        // A retention key is never accepted where the destination's own grant
        // is expected, nor the other way round.
        assert_ne!(base, destination_binding("uid-1", &url));
    }

    #[test]
    fn the_notification_binding_covers_the_uid_the_sink_and_the_pagerduty_endpoint() {
        let base = notification_binding("uid-1", NotificationSink::PagerDuty, None);
        assert!(base.starts_with("v1:uid-1:sha256:"), "{base}");
        assert_ne!(
            base,
            notification_binding("uid-2", NotificationSink::PagerDuty, None)
        );
        assert_ne!(
            base,
            notification_binding(
                "uid-1",
                NotificationSink::PagerDuty,
                Some("https://evil.example/v2/enqueue")
            ),
            "an endpoint edit must invalidate the routing key"
        );
        assert_eq!(
            base,
            notification_binding("uid-1", NotificationSink::PagerDuty, Some("  ")),
            "a blank endpoint is the default one"
        );
        let webhook = notification_binding("uid-1", NotificationSink::Webhook, None);
        let slack = notification_binding("uid-1", NotificationSink::Slack, None);
        assert_ne!(webhook, slack);
        assert_ne!(webhook, base);
        assert_ne!(slack, base);
        // The endpoint argument means nothing for a URL-in-Secret sink.
        assert_eq!(
            webhook,
            notification_binding("uid-1", NotificationSink::Webhook, Some("https://x"))
        );
    }

    #[test]
    fn no_binding_kind_can_collide_with_a_kafka_connections() {
        // PROD-01.3's canonical form has no `kind=` line, so no digest of
        // this module can equal a connection's for the same UID.
        let auth = crate::spec::AuthSpec::default();
        let kafka = crate::connection::credential_binding("uid-1", &[], &auth, None);
        assert_ne!(
            kafka,
            notification_binding("uid-1", NotificationSink::Webhook, None)
        );
        assert_ne!(kafka, destination_binding("uid-1", &s3("b", "", None)));
    }

    #[test]
    fn check_pair_reads_through_the_lookup_and_fails_closed() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| (*v).to_string())
            }
        };
        let p = ARCHIVE_CREDENTIAL_BINDING_ENV;
        let e = ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV;
        // No expectation: a hand-run process, not checked.
        assert_eq!(check_pair(p, e, &env(&[])), Ok(()));
        // Expected and absent: refused, as absent.
        let absent = check_pair(
            p,
            e,
            &env(&[(ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV, "v1:a")]),
        )
        .unwrap_err();
        assert!(absent.absent);
        assert_eq!(absent.binding_env, p);
        // Foreign: refused.
        let foreign = check_pair(
            p,
            e,
            &env(&[
                (ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV, "v1:a"),
                (ARCHIVE_CREDENTIAL_BINDING_ENV, "v1:b"),
            ]),
        )
        .unwrap_err();
        assert!(!foreign.absent);
        // The fail-closed expectation matches nothing a Secret carries.
        assert!(check_pair(
            p,
            e,
            &env(&[
                (ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV, UNBOUND_NO_UID),
                (ARCHIVE_CREDENTIAL_BINDING_ENV, "v1:b"),
            ]),
        )
        .is_err());
        // CONTROL: its own binding is accepted.
        assert_eq!(
            check_pair(
                p,
                e,
                &env(&[
                    (ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV, "v1:a"),
                    (ARCHIVE_CREDENTIAL_BINDING_ENV, "v1:a"),
                ]),
            ),
            Ok(())
        );
    }

    /// FX-20c: a LISTED grant's binding is compared fail-closed — an absent
    /// expectation is a refusal, not a hand-run pass — and each role has its
    /// own pair, distinct from every credential pair.
    ///
    /// KILLS: `check_grant_binding` delegating to `check_pair` (an absent
    /// expectation would pass); two roles sharing one variable.
    #[test]
    fn a_listed_grant_is_compared_fail_closed_under_its_own_pair() {
        let map = |pairs: Vec<(&'static str, &'static str)>| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| (*v).to_string())
            }
        };
        let (p, e) = grant_binding_env(DestinationRole::ArchiveWrite);
        // No expectation at all: REFUSED, unlike `check_pair`.
        let refusal = check_grant_binding(DestinationRole::ArchiveWrite, &map(vec![(p, "v1:a")]))
            .unwrap_err();
        assert_eq!(refusal.binding_env, p);
        assert!(check_pair(p, e, &map(vec![(p, "v1:a")])).is_ok());
        // Absent and foreign: refused, and told apart.
        assert!(
            check_grant_binding(DestinationRole::ArchiveWrite, &map(vec![(e, "v1:a")]))
                .unwrap_err()
                .absent
        );
        assert!(
            !check_grant_binding(
                DestinationRole::ArchiveWrite,
                &map(vec![(e, "v1:a"), (p, "v1:b")])
            )
            .unwrap_err()
            .absent
        );
        // CONTROL: its own binding, alone or among several, is accepted.
        assert_eq!(
            check_grant_binding(
                DestinationRole::ArchiveWrite,
                &map(vec![(e, "v1:a"), (p, "v1:z, v1:a")])
            ),
            Ok(())
        );
        // One pair per role, none of them a credential pair.
        let mut names: Vec<&str> = DestinationRole::ALL
            .iter()
            .flat_map(|r| {
                let (p, e) = grant_binding_env(*r);
                [p, e]
            })
            .collect();
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), before, "two roles share a variable");
        for (projected, expected) in STORE_BINDING_PAIRS {
            assert!(!names.contains(&projected) && !names.contains(&expected));
        }
        for (credential, _, _) in GUARDED_CREDENTIALS {
            assert!(!names.contains(&credential));
        }
    }
}
