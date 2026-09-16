//! The PURE half of a saved backup destination (decision D2 §3.1–§3.4).
//!
//! A `BackupDestination` names WHERE archives live (an immutable location) and
//! HOW they are reached (an immutable transport security choice), never a
//! credential VALUE. This module holds the half that needs no I/O: the closed
//! enums, the validation that duplicates the CRD's CEL rules R3–R6 for API 422
//! messages and for the controller, the engine-compatibility refusal (D2 G4),
//! the canonical URL, the location digest and the two `StorageUrl` renderings.
//!
//! **Addressing never implies transport** (D-SEAMS S5). `PathStyle` versus
//! `VirtualHosted` is an addressing choice; `TLS` versus `InsecureHTTP` is a
//! transport choice; neither derives the other, in either direction, and
//! [`validate_transition`] refuses an edit that would change either the
//! location or the transport of an existing destination. The defect this
//! closes is UI-HTTPDOWNGRADE (`ui/pages/restore-wizard.js:1142-1149` sets
//! `allowHttp` from the path-style checkbox) and its server-side twin.
//!
//! No I/O, no clock, no entropy — Global Constraint 1, checked by
//! `scripts/check-pure-core.sh`.

use crate::engine::StorageUrl;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The only provider the location model admits. An enum, not a string, so a
/// second provider is a reviewable event with its own validation rules rather
/// than an unvalidated free-text field.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum StorageProvider {
    S3,
}

/// How a request addresses the bucket. **Never** a transport choice.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum Addressing {
    PathStyle,
    VirtualHosted,
}

/// Transport security, spelled exactly as the CRD enum spells it. **Never**
/// derived from addressing, from the endpoint shape alone, or from any process
/// environment value (D-SEAMS S5).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum TransportSecurity {
    #[serde(rename = "TLS")]
    #[schemars(rename = "TLS")]
    Tls,
    #[serde(rename = "InsecureHTTP")]
    #[schemars(rename = "InsecureHTTP")]
    InsecureHttp,
}

impl TransportSecurity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tls => "TLS",
            Self::InsecureHttp => "InsecureHTTP",
        }
    }

    /// `true` only for [`TransportSecurity::InsecureHttp`]. This is the ONE
    /// function that decides whether plaintext HTTP is permitted, so a
    /// reviewer can read every caller of it.
    #[must_use]
    pub fn allows_plaintext_http(self) -> bool {
        matches!(self, Self::InsecureHttp)
    }
}

impl Addressing {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PathStyle => "PathStyle",
            Self::VirtualHosted => "VirtualHosted",
        }
    }

    /// `true` only for [`Addressing::PathStyle`]. Deliberately separate from
    /// [`TransportSecurity::allows_plaintext_http`]: the two answers come from
    /// two fields and one may never be computed from the other.
    #[must_use]
    pub fn is_path_style(self) -> bool {
        matches!(self, Self::PathStyle)
    }
}

/// Which grant an operation needs. Lives in the pure layer because the check
/// plan (`logweir_core::check_contract`), the controller resolver
/// (`weirkeeper::destination`) and the API DTOs must all name the same four
/// roles; two enums would be two vocabularies.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum DestinationRole {
    ArchiveWrite,
    ArchiveRead,
    EvidenceWrite,
    EvidenceRead,
}

impl DestinationRole {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ArchiveWrite => "ArchiveWrite",
            Self::ArchiveRead => "ArchiveRead",
            Self::EvidenceWrite => "EvidenceWrite",
            Self::EvidenceRead => "EvidenceRead",
        }
    }

    /// Every role, in declaration order — for exhaustive table tests and for
    /// the API's `roles` enumeration.
    pub const ALL: [Self; 4] = [
        Self::ArchiveWrite,
        Self::ArchiveRead,
        Self::EvidenceWrite,
        Self::EvidenceRead,
    ];
}

/// The immutable half of a `BackupDestination.spec`: the location plus the
/// transport security choice. Field-for-field the struct D2 §3.4 names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DestinationLocation {
    pub provider: StorageProvider,
    pub bucket: String,
    /// Relative, no leading or trailing `/`, `""` for the bucket root.
    #[serde(default)]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// An http(s) ORIGIN — scheme, host and optional port, nothing else.
    /// Absent means AWS S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub addressing: Addressing,
    pub transport: TransportSecurity,
}

/// One field-level validation failure, shaped so the API can render a 422 and
/// the controller can render a condition message from the SAME value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FieldError {
    /// A dotted path rooted at `spec`, e.g. `spec.storage.endpoint`.
    pub field: String,
    /// The CEL rule this duplicates, e.g. `R3`. Present so a reviewer can see
    /// at a glance that the two enforcement points say the same thing.
    pub rule: &'static str,
    pub message: String,
}

impl FieldError {
    fn new(field: &str, rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            field: field.to_string(),
            rule,
            message: message.into(),
        }
    }
}

/// `spec.storage.prefix` is capped here, matching the CRD's `maxLength`.
pub const PREFIX_MAX_LEN: usize = 512;
/// `spec.storage.endpoint` is capped here, matching the CRD's `maxLength`.
pub const ENDPOINT_MAX_LEN: usize = 2048;
/// Global Constraint 6's evidence root, and therefore a prefix a destination
/// may never claim: `logweir/` is where Logweir's own evidence lives.
pub const RESERVED_PREFIX: &str = "logweir";

/// R5's character set for a host label.
fn is_host_label_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-'
}

/// One DNS label: starts and ends alphanumeric, hyphens allowed inside.
fn is_dns_label(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(is_host_label_char)
        && s.starts_with(|c: char| c.is_ascii_alphanumeric())
        && s.ends_with(|c: char| c.is_ascii_alphanumeric())
}

/// R6's character set for one prefix segment: the S3 "safe characters" set.
fn is_prefix_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || "!_.*'()-".contains(c)
}

/// R5, hand-written rather than pulled from a URL crate: the pure layer
/// carries none, and CEL enforces the same shape with a regex because the 1.29
/// floor has no URL library (D2 §3.2). Scheme, host and optional port only —
/// no userinfo, path, query or fragment. A single trailing `/` is tolerated
/// because operators type it.
fn endpoint_shape_ok(endpoint: &str) -> bool {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return false;
    };
    if scheme != "http" && scheme != "https" {
        return false;
    }
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest.is_empty() || rest.contains(['/', '?', '#', '@']) {
        return false;
    }
    // An IPv6 literal is bracketed; everything after the bracket is an
    // optional `:port`. Everything else is a dotted DNS name or an IPv4
    // literal, which `is_dns_label` accepts because its labels are
    // alphanumeric.
    let port = if let Some(after) = rest.strip_prefix('[') {
        let Some((inside, tail)) = after.split_once(']') else {
            return false;
        };
        let inside_ok = !inside.is_empty()
            && inside
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.');
        if !inside_ok {
            return false;
        }
        tail
    } else {
        // `rsplit_once(':')` on an unbracketed authority splits host from
        // port; an IPv6 literal without brackets is therefore refused, which
        // is correct — it is ambiguous by construction.
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (rest, None),
        };
        if host.is_empty() || !host.split('.').all(is_dns_label) {
            return false;
        }
        return match port {
            None => true,
            Some(p) => !p.is_empty() && p.len() <= 5 && p.chars().all(|c| c.is_ascii_digit()),
        };
    };
    if port.is_empty() {
        return true;
    }
    let Some(digits) = port.strip_prefix(':') else {
        return false;
    };
    !digits.is_empty() && digits.len() <= 5 && digits.chars().all(|c| c.is_ascii_digit())
}

/// The bucket pattern from D2 §3.1: `^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`.
fn bucket_shape_ok(bucket: &str) -> bool {
    let n = bucket.chars().count();
    if !(3..=63).contains(&n) {
        return false;
    }
    let first_last_ok = bucket
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && bucket
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    first_last_ok
        && bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
}

/// R3–R6, plus the bucket and region patterns of D2 §3.1, evaluated over a
/// single value.
///
/// Every rule that CEL enforces on the CRD is duplicated here ON PURPOSE: the
/// API answers a 422 before the object is ever POSTed, and the controller
/// re-evaluates what it read, so an object admitted by an older CRD revision
/// is still refused. R1 and R2 are transition rules and live in
/// [`validate_transition`]; R4 needs the CA-bundle reference, which this
/// struct deliberately does not carry, and lives in [`validate_ca_bundle`].
///
/// Returns EVERY failure, not the first, because a 422 that names one of three
/// mistakes makes the operator submit three times.
pub fn validate(loc: &DestinationLocation) -> Result<(), Vec<FieldError>> {
    let mut errs = Vec::new();

    if !bucket_shape_ok(&loc.bucket) {
        errs.push(FieldError::new(
            "spec.storage.bucket",
            "bucket",
            format!(
                "storage.bucket `{}` is not a bucket name: 3-63 characters matching \
                 ^[a-z0-9][a-z0-9.-]{{1,61}}[a-z0-9]$",
                loc.bucket
            ),
        ));
    }

    if let Some(region) = &loc.region {
        let ok = !region.is_empty()
            && region.chars().count() <= 32
            && region
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !ok {
            errs.push(FieldError::new(
                "spec.storage.region",
                "region",
                format!("storage.region `{region}` must match ^[a-z0-9-]{{1,32}}$"),
            ));
        }
    }

    // R5 before R3, so an endpoint that is not an origin at all is reported as
    // such rather than as a transport mismatch.
    if let Some(endpoint) = &loc.endpoint {
        if endpoint.chars().count() > ENDPOINT_MAX_LEN {
            errs.push(FieldError::new(
                "spec.storage.endpoint",
                "R5",
                format!("storage.endpoint is longer than {ENDPOINT_MAX_LEN} characters"),
            ));
        } else if !endpoint_shape_ok(endpoint) {
            errs.push(FieldError::new(
                "spec.storage.endpoint",
                "R5",
                "storage.endpoint must be an http(s) origin: scheme, host and optional port \
                 only (no userinfo, path, query or fragment)",
            ));
        }
    }

    // R3 — the transport/scheme agreement, and the one rule that keeps
    // addressing out of the transport decision.
    let r3_ok = match loc.transport {
        TransportSecurity::InsecureHttp => loc
            .endpoint
            .as_deref()
            .is_some_and(|e| e.starts_with("http://")),
        TransportSecurity::Tls => loc
            .endpoint
            .as_deref()
            .is_none_or(|e| e.starts_with("https://")),
    };
    if !r3_ok {
        errs.push(FieldError::new(
            "spec.transport.security",
            "R3",
            "transport.security must match the endpoint scheme: TLS needs an https:// \
             endpoint or none; InsecureHTTP needs an explicit http:// endpoint. \
             storage.addressing never changes transport",
        ));
    }

    // R6 — the prefix.
    if !loc.prefix.is_empty() {
        if loc.prefix.chars().count() > PREFIX_MAX_LEN {
            errs.push(FieldError::new(
                "spec.storage.prefix",
                "R6",
                format!("storage.prefix is longer than {PREFIX_MAX_LEN} characters"),
            ));
        } else if let Err(why) = prefix_shape(&loc.prefix) {
            errs.push(FieldError::new("spec.storage.prefix", "R6", why));
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

/// R6's body, split out so its three clauses each have a named reason.
fn prefix_shape(prefix: &str) -> Result<(), String> {
    if prefix.starts_with('/') || prefix.ends_with('/') {
        return Err(
            "storage.prefix is relative with no empty, '.' or '..' segment, and may not be \
             the reserved evidence root logweir/"
                .to_string(),
        );
    }
    for seg in prefix.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." || !seg.chars().all(is_prefix_char) {
            return Err(
                "storage.prefix is relative with no empty, '.' or '..' segment, and may not \
                 be the reserved evidence root logweir/"
                    .to_string(),
            );
        }
    }
    if prefix == RESERVED_PREFIX || prefix.starts_with("logweir/") {
        return Err(format!(
            "storage.prefix may not be the reserved evidence root `{RESERVED_PREFIX}/`: \
             Logweir writes its own evidence there (Global Constraint 6)"
        ));
    }
    Ok(())
}

/// R4, which needs one fact this struct does not carry: whether a CA bundle
/// reference is set. Exposed as its own function so the controller and the API
/// evaluate the same rule rather than each writing it out.
pub fn validate_ca_bundle(
    transport: TransportSecurity,
    has_ca_bundle: bool,
) -> Result<(), FieldError> {
    if has_ca_bundle && transport != TransportSecurity::Tls {
        return Err(FieldError::new(
            "spec.transport.caBundle",
            "R4",
            "transport.caBundle requires transport.security TLS",
        ));
    }
    Ok(())
}

/// R1 and R2: the location is immutable and the transport can never be changed
/// in place. A different location is a different `BackupDestination`.
///
/// This is not merely "no downgrade": an UPGRADE is refused too. A destination
/// whose transport moved from `InsecureHTTP` to `TLS` would silently change
/// what every already-frozen execution input meant, and the recovery points
/// that named it would no longer describe how their archive was reached.
pub fn validate_transition(
    old: &DestinationLocation,
    new: &DestinationLocation,
) -> Result<(), Vec<FieldError>> {
    let mut errs = Vec::new();
    let location_changed = old.provider != new.provider
        || old.bucket != new.bucket
        || old.prefix != new.prefix
        || old.region != new.region
        || old.endpoint != new.endpoint
        || old.addressing != new.addressing;
    if location_changed {
        errs.push(FieldError::new(
            "spec.storage",
            "R1",
            "spec.storage is immutable: a different location is a different BackupDestination",
        ));
    }
    if old.transport != new.transport {
        errs.push(FieldError::new(
            "spec.transport.security",
            "R2",
            "spec.transport.security is immutable: transport can never be changed in place",
        ));
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

/// D2 G4: the pinned engine (kafka-backup 0.21.0) ignores `path_style` and
/// FORCES path-style addressing whenever an endpoint is set
/// (`kafka-backup-core/src/storage/s3.rs:66`). So `VirtualHosted` with a custom
/// endpoint is a setting the engine cannot honour, and Logweir refuses it
/// rather than advertising a behaviour it does not have (tracker defect
/// ENGINE-PATHSTYLE).
///
/// `VirtualHosted` with NO endpoint is AWS S3's own default and is fine.
pub fn engine_compatible(loc: &DestinationLocation) -> Result<(), &'static str> {
    if loc.addressing == Addressing::VirtualHosted && loc.endpoint.is_some() {
        return Err(
            "engine 0.21.0 forces path-style addressing whenever a custom endpoint is set, so \
             storage.addressing VirtualHosted with storage.endpoint cannot be honoured",
        );
    }
    Ok(())
}

impl DestinationLocation {
    /// `s3://<bucket>[/<prefix>]` — what `status.canonicalUrl` publishes and
    /// what a human reads in `kubectl get`.
    #[must_use]
    pub fn canonical_url(&self) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }

    /// The endpoint's host identity: lower-cased, scheme-stripped, with any
    /// trailing `/` removed — or `aws/<region or "">` when there is no
    /// endpoint.
    #[must_use]
    pub fn host_identity(&self) -> String {
        match &self.endpoint {
            Some(e) => {
                let no_scheme = e.split_once("://").map_or(e.as_str(), |(_, rest)| rest);
                no_scheme.trim_end_matches('/').to_ascii_lowercase()
            }
            None => format!("aws/{}", self.region.as_deref().unwrap_or("")),
        }
    }

    /// A stable identity for WHERE the data is, excluding HOW it is reached.
    ///
    /// Transport and addressing are deliberately NOT in the digest: they
    /// describe the route, not the location, and two destinations that differ
    /// only in addressing hold the same objects. This is the value a recovery
    /// point freezes (D2 §3.7) and the one the restore API compares when
    /// offering destinations for an existing recovery point (§3.12 step 5), so
    /// a TLS destination can serve a recovery point written through a
    /// plaintext one at the same bucket, and neither can serve a recovery
    /// point from a different bucket.
    #[must_use]
    pub fn location_digest(&self) -> String {
        let canonical = format!(
            "s3\n{}\n{}\n{}\n",
            self.host_identity(),
            self.bucket,
            self.prefix
        );
        crate::ids::sha256_prefixed(canonical.as_bytes())
    }

    /// The archive `StorageUrl` a plan block and a `Store` are built from.
    ///
    /// `allow_http` comes from `transport` and from NOTHING else — not from
    /// the addressing style, not from the endpoint scheme, and never from a
    /// process environment variable (defect SEC-ENVHTTP, D-SEAMS S5).
    #[must_use]
    pub fn archive_storage_url(&self) -> StorageUrl {
        StorageUrl::S3 {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            path_style: self.addressing.is_path_style(),
            allow_http: self.transport.allows_plaintext_http(),
        }
    }

    /// The evidence `StorageUrl`: the same bucket and the same route, rooted
    /// at Global Constraint 6's `logweir/`.
    ///
    /// Exactly what `logweir::backup::evidence_location` produces for the
    /// legacy inline path, so a destination-backed run writes its receipt to
    /// the same place an inline one would and `Store::from_url`'s
    /// "prefix must be exactly `logweir/`" guard is satisfied by construction.
    #[must_use]
    pub fn evidence_storage_url(&self) -> StorageUrl {
        StorageUrl::S3 {
            bucket: self.bucket.clone(),
            prefix: EVIDENCE_PREFIX.to_string(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            path_style: self.addressing.is_path_style(),
            allow_http: self.transport.allows_plaintext_http(),
        }
    }
}

/// Global Constraint 6's evidence root, with the trailing slash
/// `logweir_store::LOGWEIR_ROOT` uses and `Store::from_url` requires.
pub const EVIDENCE_PREFIX: &str = "logweir/";
