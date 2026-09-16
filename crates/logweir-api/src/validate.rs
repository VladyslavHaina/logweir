//! Field validators shared by the request DTOs.
//!
//! Hand-written, with no regular-expression dependency: every grammar here is
//! a character class and a length. Each validator returns `Ok(())` or the
//! stable per-field `code` string that lands in a [`crate::problem::FieldError`].

/// Kubernetes DNS-1123 label: namespace names.
#[must_use]
pub fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
}

/// Kubernetes DNS-1123 subdomain: object and Secret names.
#[must_use]
pub fn is_dns_subdomain(value: &str) -> bool {
    !value.is_empty() && value.len() <= 253 && value.split('.').all(is_dns_label_part)
}

fn is_dns_label_part(part: &str) -> bool {
    let bytes = part.as_bytes();
    !bytes.is_empty()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
}

/// A Kafka topic name: `[a-zA-Z0-9._-]{1,249}`, and not `.` or `..`.
///
/// Glob metacharacters (`*`, `?`, `[`) are outside the class, so a pattern is
/// refused here as well as by the controller's G-GLOB rail.
#[must_use]
pub fn is_topic_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 249
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// A bootstrap address `host:port`: a DNS name or IPv4 literal, or a bracketed
/// IPv6 literal, and a port in `1..=65535`. No scheme, no userinfo, no path.
pub fn check_bootstrap_server(value: &str) -> Result<(), &'static str> {
    if value.len() > 261 {
        return Err("too_long");
    }
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (inner, after) = rest.split_once(']').ok_or("invalid_host")?;
        let port = after.strip_prefix(':').ok_or("port_required")?;
        if inner.is_empty() || !inner.bytes().all(|b| b.is_ascii_hexdigit() || b == b':') {
            return Err("invalid_host");
        }
        (inner, port)
    } else {
        let (host, port) = value.rsplit_once(':').ok_or("port_required")?;
        if host.is_empty()
            || host.len() > 253
            || !host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
            || host.starts_with('.')
            || host.starts_with('-')
            || host.ends_with('.')
            || host.ends_with('-')
        {
            return Err("invalid_host");
        }
        (host, port)
    };
    let _ = host;
    if port.is_empty() || port.len() > 5 || !port.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid_port");
    }
    match port.parse::<u32>() {
        Ok(p) if (1..=65_535).contains(&p) => Ok(()),
        _ => Err("invalid_port"),
    }
}

/// The object-store schemes the controller's `retention::storage_url_for`
/// accepts (Global Constraint 9's feature set).
pub const ARCHIVE_SCHEMES: [&str; 4] = ["s3", "gs", "az", "file"];

/// An archive URL: one of [`ARCHIVE_SCHEMES`], a non-empty location, no
/// userinfo (credentials never travel in a URL), no whitespace or control
/// characters, no query or fragment, at most 2048 bytes.
pub fn check_archive_url(value: &str) -> Result<(), &'static str> {
    if value.len() > 2048 {
        return Err("too_long");
    }
    if value
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || c == '\\')
    {
        return Err("invalid_character");
    }
    let (scheme, rest) = value.split_once("://").ok_or("scheme_required")?;
    if !ARCHIVE_SCHEMES.contains(&scheme) {
        return Err("unsupported_scheme");
    }
    if rest.is_empty() {
        return Err("location_required");
    }
    if rest.contains('?') || rest.contains('#') {
        return Err("query_not_allowed");
    }
    let authority = rest.split('/').next().unwrap_or("");
    if authority.contains('@') {
        return Err("userinfo_not_allowed");
    }
    if scheme == "file" {
        if !rest.starts_with('/') {
            return Err("file_url_must_be_absolute");
        }
    } else if authority.is_empty() {
        return Err("location_required");
    }
    if scheme == "az" {
        let mut parts = rest.trim_matches('/').split('/');
        let account = parts.next().unwrap_or("");
        let container = parts.next().unwrap_or("");
        if account.is_empty() || container.is_empty() {
            return Err("container_required");
        }
    }
    Ok(())
}

/// A free-text value that must be a single printable line: no control
/// characters, at most `max` bytes.
pub fn check_single_line(value: &str, max: usize) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("required");
    }
    if value.len() > max {
        return Err("too_long");
    }
    if value.chars().any(char::is_control) {
        return Err("invalid_character");
    }
    Ok(())
}

/// Replace the userinfo of a URL-shaped string with `redacted@`, for
/// projections of stored objects that predate this API's validation.
#[must_use]
pub fn redact_url_userinfo(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_string();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    match authority.rfind('@') {
        Some(at) => format!("{scheme}://redacted@{}", &rest[at + 1..]),
        None => value.to_string(),
    }
}

/// Truncate a status message to `max` bytes on a character boundary, marking
/// the cut. Bounded diagnostics are part of the status contract.
#[must_use]
pub fn bounded(value: &str, max: usize) -> String {
    if value.len() <= max {
        return value.to_string();
    }
    let mut end = max;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_names() {
        assert!(is_dns_label("team-a"));
        assert!(!is_dns_label("Team-a"));
        assert!(!is_dns_label("-a"));
        assert!(!is_dns_label(&"a".repeat(64)));
        assert!(is_dns_subdomain("a.b-c.d"));
        assert!(!is_dns_subdomain("a..b"));
        assert!(!is_dns_subdomain("a/b"));
    }

    #[test]
    fn topics_refuse_globs() {
        assert!(is_topic_name("orders.v1_x-y"));
        for bad in ["", ".", "..", "orders*", "ord?rs", "[a]", "a b"] {
            assert!(!is_topic_name(bad), "{bad}");
        }
    }

    #[test]
    fn bootstrap_servers() {
        assert!(check_bootstrap_server("kafka-0.kafka.svc:9092").is_ok());
        assert!(check_bootstrap_server("10.0.0.1:9092").is_ok());
        assert!(check_bootstrap_server("[fd00::1]:9092").is_ok());
        for bad in [
            "kafka",
            "kafka:",
            "kafka:0",
            "kafka:70000",
            "PLAINTEXT://kafka:9092",
            "user@kafka:9092",
            "kafka:9092/x",
        ] {
            assert!(check_bootstrap_server(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn archive_urls() {
        assert!(check_archive_url("s3://bucket/prefix").is_ok());
        assert!(check_archive_url("gs://bucket").is_ok());
        assert!(check_archive_url("az://account/container/prefix").is_ok());
        assert!(check_archive_url("file:///var/archive").is_ok());
        assert_eq!(
            check_archive_url("s3://key:secret@bucket/x"),
            Err("userinfo_not_allowed")
        );
        assert_eq!(check_archive_url("http://x/y"), Err("unsupported_scheme"));
        assert_eq!(
            check_archive_url("file://relative/x"),
            Err("file_url_must_be_absolute")
        );
        assert_eq!(check_archive_url("az://account"), Err("container_required"));
        assert_eq!(check_archive_url("s3://b/x?y"), Err("query_not_allowed"));
    }

    #[test]
    fn userinfo_is_redacted() {
        assert_eq!(
            redact_url_userinfo("s3://key:secret@bucket/x"),
            "s3://redacted@bucket/x"
        );
        assert_eq!(redact_url_userinfo("s3://bucket/x@y"), "s3://bucket/x@y");
    }

    #[test]
    fn bounded_cuts_on_a_boundary() {
        assert_eq!(bounded("abc", 5), "abc");
        assert_eq!(bounded("ééé", 3), "é…");
    }
}
