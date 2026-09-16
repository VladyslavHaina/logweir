//! The configuration file, and the refusals that happen before any socket.
//!
//! ONE MODE IN THIS STAGE, AND IT MUST BE NAMED. `mode: localAdmin` is an
//! explicit administrator mode: the listener must be a loopback address, the
//! actor is the configured local administrator, and the namespaces are the
//! configured list and nothing discovered. There is no default mode, so a
//! file that forgets to say which mode it wants is refused rather than read as
//! the most permissive one. OIDC/shared mode is PLAT-17.2 and is refused here
//! by name.
//!
//! EVERY REFUSAL IS A [`ConfigError`] NAMING THE FIELD, and `main` exits 2 on
//! any of them before it builds a Kubernetes client or binds a socket.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::validate;

/// The smallest cursor key this service accepts, in bytes.
pub const MIN_CURSOR_KEY_BYTES: usize = 32;

/// The most namespaces one configuration may grant.
pub const MAX_NAMESPACES: usize = 256;

/// The file as written. Unknown keys are refused.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ConfigFile {
    mode: String,
    listen: String,
    public_origin: String,
    ui_directory: PathBuf,
    #[serde(default)]
    local_admin: Option<LocalAdminFile>,
    namespaces: Vec<String>,
    kubernetes: KubernetesFile,
    cursor_key_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LocalAdminFile {
    subject: String,
    #[serde(default)]
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct KubernetesFile {
    source: String,
    #[serde(default)]
    kubeconfig: Option<PathBuf>,
    #[serde(default)]
    context: Option<String>,
}

/// Where the Kubernetes client configuration comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeSource {
    /// The in-cluster service-account environment.
    InCluster,
    /// A kubeconfig file and an EXPLICIT context. The file's
    /// `current-context` is never consulted.
    Kubeconfig {
        /// The kubeconfig path; `None` means `KUBECONFIG` or `~/.kube/config`.
        path: Option<PathBuf>,
        /// The context to load. Required.
        context: String,
    },
}

/// The local administrator this mode acts as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalAdmin {
    /// The subject, e.g. `admin`. The actor ID is
    /// `urn:logweir:local-admin#<subject>`.
    pub subject: String,
    /// A display name. Never used for authorization.
    pub display_name: String,
}

/// A validated configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// The loopback socket address to bind.
    pub listen: SocketAddr,
    /// The exact origin unsafe requests must carry, e.g.
    /// `http://127.0.0.1:8484`.
    pub public_origin: String,
    /// `Host` header values this listener serves.
    pub allowed_hosts: Vec<String>,
    /// The static UI directory.
    pub ui_directory: PathBuf,
    /// The configured local administrator.
    pub local_admin: LocalAdmin,
    /// The explicitly granted namespaces, in configuration order.
    pub namespaces: Vec<String>,
    /// The Kubernetes client source.
    pub kubernetes: KubeSource,
    /// The cursor MAC key file.
    pub cursor_key_file: PathBuf,
}

/// A refusal, naming the field.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("cannot read the configuration file {path}: {reason}")]
    Read {
        /// The path.
        path: String,
        /// The I/O error.
        reason: String,
    },
    /// The file is not the expected YAML shape.
    #[error("the configuration file is not valid: {0}")]
    Parse(String),
    /// A field failed validation.
    #[error("configuration field `{field}`: {reason}")]
    Field {
        /// The field.
        field: &'static str,
        /// Why.
        reason: String,
    },
}

fn field(field: &'static str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Field {
        field,
        reason: reason.into(),
    }
}

impl Config {
    /// Read and validate a configuration file. Relative paths inside it are
    /// resolved against the file's own directory.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] naming the unreadable file or the refused field.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        Config::parse(&text, base)
    }

    /// Validate configuration text. Relative paths resolve against `base`.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] naming the refused field.
    pub fn parse(text: &str, base: &Path) -> Result<Config, ConfigError> {
        let file: ConfigFile =
            serde_yaml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;

        match file.mode.as_str() {
            "localAdmin" => {}
            "shared" | "oidc" => {
                return Err(field(
                    "mode",
                    "shared OIDC mode is not implemented in this release (PLAT-17.2); only \
                     `localAdmin` is accepted",
                ))
            }
            other => {
                return Err(field(
                    "mode",
                    format!("`{other}` is not a mode; the only accepted value is `localAdmin`"),
                ))
            }
        }

        let listen = parse_loopback_listen(&file.listen)?;
        let public_origin = parse_local_origin(&file.public_origin, listen.port())?;
        let allowed_hosts = allowed_hosts(&public_origin, listen.port());

        let local_admin = match file.local_admin {
            None => {
                return Err(field(
                    "localAdmin",
                    "localAdmin mode requires `localAdmin.subject`, the administrator this \
                     process acts as",
                ))
            }
            Some(admin) => {
                validate::check_single_line(&admin.subject, 128)
                    .map_err(|code| field("localAdmin.subject", code))?;
                if admin.subject.contains('#') || admin.subject.trim() != admin.subject {
                    return Err(field(
                        "localAdmin.subject",
                        "must not contain `#` or surrounding whitespace",
                    ));
                }
                let display_name = admin
                    .display_name
                    .unwrap_or_else(|| "Local administrator".to_string());
                validate::check_single_line(&display_name, 128)
                    .map_err(|code| field("localAdmin.displayName", code))?;
                LocalAdmin {
                    subject: admin.subject,
                    display_name,
                }
            }
        };

        if file.namespaces.is_empty() {
            return Err(field(
                "namespaces",
                "at least one namespace must be granted explicitly; this service never lists \
                 core Namespace objects",
            ));
        }
        if file.namespaces.len() > MAX_NAMESPACES {
            return Err(field(
                "namespaces",
                format!("at most {MAX_NAMESPACES} namespaces may be granted"),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for ns in &file.namespaces {
            if !validate::is_dns_label(ns) {
                return Err(field(
                    "namespaces",
                    format!("`{ns}` is not a DNS-1123 label"),
                ));
            }
            if !seen.insert(ns.as_str()) {
                return Err(field("namespaces", format!("`{ns}` is listed twice")));
            }
        }

        let kubernetes = match file.kubernetes.source.as_str() {
            "inCluster" => {
                if file.kubernetes.kubeconfig.is_some() || file.kubernetes.context.is_some() {
                    return Err(field(
                        "kubernetes",
                        "`source: inCluster` takes neither `kubeconfig` nor `context`",
                    ));
                }
                KubeSource::InCluster
            }
            "kubeconfig" => {
                let context = match file.kubernetes.context {
                    Some(context) if !context.trim().is_empty() => context,
                    _ => {
                        return Err(field(
                            "kubernetes.context",
                            "`source: kubeconfig` requires an explicit `context`; the \
                             kubeconfig's current-context is never used",
                        ))
                    }
                };
                KubeSource::Kubeconfig {
                    path: file.kubernetes.kubeconfig.map(|p| resolve(base, &p)),
                    context,
                }
            }
            other => {
                return Err(field(
                    "kubernetes.source",
                    format!("`{other}` is not a source; use `inCluster` or `kubeconfig`"),
                ))
            }
        };

        Ok(Config {
            listen,
            public_origin,
            allowed_hosts,
            ui_directory: resolve(base, &file.ui_directory),
            local_admin,
            namespaces: file.namespaces,
            kubernetes,
            cursor_key_file: resolve(base, &file.cursor_key_file),
        })
    }
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

/// Parse `listen` and refuse anything that is not a loopback IP literal.
///
/// A hostname is refused even when it resolves to loopback: resolution is an
/// input this process does not control, and `localhost` has been observed to
/// resolve elsewhere.
///
/// # Errors
///
/// [`ConfigError::Field`] for `listen`.
pub fn parse_loopback_listen(value: &str) -> Result<SocketAddr, ConfigError> {
    let addr: SocketAddr = value.parse().map_err(|_| {
        field(
            "listen",
            format!("`{value}` is not an IP:port socket address (hostnames are refused)"),
        )
    })?;
    if !is_loopback(addr.ip()) {
        return Err(field(
            "listen",
            format!(
                "`{value}` is not a loopback address; localAdmin mode binds only 127.0.0.0/8 \
                 or ::1 and must never be reachable from another host"
            ),
        ));
    }
    if addr.port() == 0 {
        return Err(field("listen", "an explicit, non-zero port is required"));
    }
    Ok(addr)
}

fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

/// Validate `publicOrigin` for localAdmin mode: `http` or `https`, a loopback
/// host, the listen port explicitly, and nothing else.
fn parse_local_origin(value: &str, port: u16) -> Result<String, ConfigError> {
    let refuse = |reason: &str| field("publicOrigin", format!("`{value}`: {reason}"));
    let (scheme, rest) = value
        .split_once("://")
        .ok_or_else(|| refuse("must be scheme://host:port"))?;
    if scheme != "http" && scheme != "https" {
        return Err(refuse("the scheme must be http or https"));
    }
    if rest.is_empty() || rest.contains('/') || rest.contains('?') || rest.contains('#') {
        return Err(refuse(
            "an origin carries no path, query or fragment (no trailing slash)",
        ));
    }
    if rest.contains('@') {
        return Err(refuse("an origin carries no userinfo"));
    }
    let (host, origin_port) = if let Some(inner) = rest.strip_prefix('[') {
        let (host, after) = inner
            .split_once(']')
            .ok_or_else(|| refuse("unterminated IPv6 literal"))?;
        let p = after
            .strip_prefix(':')
            .ok_or_else(|| refuse("an explicit port is required"))?;
        (host.to_string(), p)
    } else {
        let (host, p) = rest
            .rsplit_once(':')
            .ok_or_else(|| refuse("an explicit port is required"))?;
        (host.to_string(), p)
    };
    let origin_port: u16 = origin_port
        .parse()
        .map_err(|_| refuse("the port is not a number"))?;
    if origin_port != port {
        return Err(refuse(&format!(
            "the port must be the listen port {port}; a loopback administrator origin is the \
             listener itself"
        )));
    }
    let loopback_host =
        host == "localhost" || host.parse::<IpAddr>().map(is_loopback).unwrap_or(false);
    if !loopback_host {
        return Err(refuse(
            "localAdmin mode serves only a loopback origin (127.0.0.1, [::1] or localhost)",
        ));
    }
    Ok(value.to_string())
}

/// The `Host` values accepted: the origin's own authority and the three
/// loopback spellings of the listen port. Anything else is a request that
/// reached this listener under a name it does not serve — the DNS-rebinding
/// shape `kubectl proxy --accept-hosts` exists to refuse.
fn allowed_hosts(public_origin: &str, port: u16) -> Vec<String> {
    let mut hosts = vec![
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
    ];
    if let Some((_, authority)) = public_origin.split_once("://") {
        if !hosts.iter().any(|h| h == authority) {
            hosts.push(authority.to_string());
        }
    }
    hosts
}

/// Read the cursor key and refuse a short one.
///
/// # Errors
///
/// [`ConfigError`] naming `cursorKeyFile`.
pub fn read_cursor_key(path: &Path) -> Result<Vec<u8>, ConfigError> {
    let bytes = std::fs::read(path).map_err(|e| ConfigError::Read {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;
    if bytes.len() < MIN_CURSOR_KEY_BYTES {
        return Err(field(
            "cursorKeyFile",
            format!(
                "the key holds {} bytes; at least {MIN_CURSOR_KEY_BYTES} random bytes are \
                 required (for example `openssl rand -out <file> 32`)",
                bytes.len()
            ),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(listen: &str, origin: &str) -> String {
        format!(
            "mode: localAdmin\nlisten: \"{listen}\"\npublicOrigin: \"{origin}\"\nuiDirectory: ui\n\
             localAdmin:\n  subject: admin\nnamespaces: [team-a]\nkubernetes:\n  source: \
             kubeconfig\n  context: docker-desktop\ncursorKeyFile: key\n"
        )
    }

    #[test]
    fn a_loopback_configuration_parses() {
        let c = Config::parse(
            &text("127.0.0.1:8484", "http://127.0.0.1:8484"),
            Path::new("/etc/lw"),
        )
        .unwrap();
        assert_eq!(c.listen.port(), 8484);
        assert_eq!(c.ui_directory, PathBuf::from("/etc/lw/ui"));
        assert!(c.allowed_hosts.contains(&"127.0.0.1:8484".to_string()));
        assert_eq!(
            c.kubernetes,
            KubeSource::Kubeconfig {
                path: None,
                context: "docker-desktop".into()
            }
        );
    }

    #[test]
    fn ipv6_loopback_is_accepted() {
        assert!(Config::parse(&text("[::1]:8484", "http://[::1]:8484"), Path::new(".")).is_ok());
    }

    #[test]
    fn non_loopback_listeners_are_refused() {
        for listen in [
            "0.0.0.0:8484",
            "[::]:8484",
            "192.168.1.5:8484",
            "10.0.0.1:8484",
        ] {
            let err =
                Config::parse(&text(listen, "http://127.0.0.1:8484"), Path::new(".")).unwrap_err();
            assert!(
                matches!(
                    err,
                    ConfigError::Field {
                        field: "listen",
                        ..
                    }
                ),
                "{listen}: {err}"
            );
        }
        let err = Config::parse(
            &text("localhost:8484", "http://127.0.0.1:8484"),
            Path::new("."),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Field {
                field: "listen",
                ..
            }
        ));
    }

    #[test]
    fn origins_must_be_the_loopback_listener() {
        for origin in [
            "http://127.0.0.1:9999",
            "http://example.com:8484",
            "http://127.0.0.1:8484/",
            "ftp://127.0.0.1:8484",
            "http://user@127.0.0.1:8484",
            "http://127.0.0.1",
        ] {
            let err = Config::parse(&text("127.0.0.1:8484", origin), Path::new(".")).unwrap_err();
            assert!(
                matches!(
                    err,
                    ConfigError::Field {
                        field: "publicOrigin",
                        ..
                    }
                ),
                "{origin}: {err}"
            );
        }
    }

    #[test]
    fn a_kubeconfig_source_needs_an_explicit_context() {
        let t = text("127.0.0.1:8484", "http://127.0.0.1:8484")
            .replace("  context: docker-desktop\n", "");
        let err = Config::parse(&t, Path::new(".")).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Field {
                field: "kubernetes.context",
                ..
            }
        ));
    }

    #[test]
    fn unknown_keys_and_modes_are_refused() {
        let t = text("127.0.0.1:8484", "http://127.0.0.1:8484") + "extra: 1\n";
        assert!(matches!(
            Config::parse(&t, Path::new(".")),
            Err(ConfigError::Parse(_))
        ));
        let t = text("127.0.0.1:8484", "http://127.0.0.1:8484").replace("localAdmin\n", "shared\n");
        assert!(matches!(
            Config::parse(&t, Path::new(".")),
            Err(ConfigError::Field { field: "mode", .. })
        ));
    }
}
