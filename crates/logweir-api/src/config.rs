//! The configuration file, and the refusals that happen before any socket.
//!
//! TWO MODES, AND THE FILE MUST NAME ONE. There is no default mode, so a file
//! that forgets to say which it wants is refused rather than read as the more
//! permissive one.
//!
//! `mode: localAdmin` is an explicit administrator mode: the listener must be a
//! loopback address, the actor is the configured local administrator, and the
//! namespaces are the configured list and nothing discovered. It is not SSO and
//! it is not a shared console.
//!
//! `mode: shared` is the SSO console. It refuses a non-HTTPS `publicBaseUrl`
//! outright — TLS at the shared entry point is not a recommendation — derives
//! the exact OIDC redirect URI from that one administrator value, reads the
//! client secret and the session and cursor keys from mounted files, and takes
//! role and namespace bindings as EXACT strings. There is no regex, no
//! wildcard, no email-domain inference and no default namespace: `*` is refused
//! by name rather than silently matching nothing.
//!
//! WHAT `trustedProxyCidrs` IS FOR, AND WHAT IT IS NOT. Forwarded client
//! addresses are recorded in the audit line when the IMMEDIATE peer is inside
//! one of these ranges. No authentication, authorization, redirect or callback
//! decision reads a forwarded header, whatever the peer is. It is transport
//! logging, exactly as D0 says.
//!
//! EVERY REFUSAL IS A [`ConfigError`] NAMING THE FIELD, and `main` exits 2 on
//! any of them before it builds a Kubernetes client or binds a socket.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::auth::oidc::{TokenAuthMethod, SUPPORTED_ALGORITHMS};
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
    #[serde(default)]
    public_origin: Option<String>,
    #[serde(default)]
    public_base_url: Option<String>,
    #[serde(default)]
    allowed_hosts: Option<Vec<String>>,
    ui_directory: PathBuf,
    #[serde(default)]
    local_admin: Option<LocalAdminFile>,
    #[serde(default)]
    oidc: Option<OidcFile>,
    #[serde(default)]
    roles: Option<RolesFile>,
    #[serde(default)]
    session_key: Option<KeyRefFile>,
    #[serde(default)]
    session_max_age_seconds: Option<i64>,
    #[serde(default)]
    trusted_proxy_cidrs: Option<Vec<String>>,
    #[serde(default)]
    require_trusted_proxy: Option<bool>,
    namespaces: Vec<String>,
    kubernetes: KubernetesFile,
    #[serde(default)]
    cursor_key_file: Option<PathBuf>,
    #[serde(default)]
    cursor_key: Option<KeyRefFile>,
    /// PLAT-19.2: the installation's approval-policy document — the SAME
    /// file the controller reads (`LOGWEIR_APPROVAL_POLICY_FILE`).
    #[serde(default)]
    approval_policy_file: Option<PathBuf>,
    /// PLAT-19.2: the console's `ConsoleConfirmation` private key, PKCS#8
    /// PEM, from a mounted Secret.
    #[serde(default)]
    confirmation_key_file: Option<PathBuf>,
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
struct KeyRefFile {
    file: PathBuf,
    expected_version: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct OidcFile {
    issuer: String,
    client_id: String,
    client_secret_file: PathBuf,
    #[serde(default)]
    allowed_algorithms: Option<Vec<String>>,
    #[serde(default)]
    scopes: Option<Vec<String>>,
    #[serde(default)]
    groups_claim: Option<String>,
    #[serde(default)]
    display_name_claim: Option<String>,
    #[serde(default)]
    token_auth_method: Option<String>,
    #[serde(default)]
    insecure_loopback_issuer: Option<bool>,
    #[serde(default)]
    ca_bundle_file: Option<PathBuf>,
    #[serde(default)]
    system_roots: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RolesFile {
    revision: String,
    bindings: Vec<RoleBindingFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RoleBindingFile {
    role: String,
    namespace: String,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    subjects: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct KubernetesFile {
    source: String,
    #[serde(default)]
    kubeconfig: Option<PathBuf>,
    #[serde(default)]
    context: Option<String>,
    #[serde(default)]
    principal: Option<String>,
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

/// A versioned key file and the version the configuration expects it to carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRef {
    /// The mounted file.
    pub file: PathBuf,
    /// The version the file must declare. A mismatch is a startup refusal.
    pub expected_version: u32,
}

/// Where the cursor MAC key comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorKeySource {
    /// localAdmin mode: a file of raw random bytes, unversioned. Unchanged
    /// from the first release so an existing administrator setup keeps working.
    RawFile(PathBuf),
    /// shared mode: a versioned key file, like the session key.
    Versioned(KeyRef),
}

/// The validated OIDC block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcConfig {
    /// The exact issuer, with no trailing slash.
    pub issuer: String,
    /// The exact client ID, which is also the expected audience.
    pub client_id: String,
    /// The mounted client-secret file.
    pub client_secret_file: PathBuf,
    /// The allowed JWS algorithms, a non-empty subset of the two this service
    /// verifies.
    pub allowed_algorithms: Vec<String>,
    /// The scopes requested. Always contains `openid`.
    pub scopes: Vec<String>,
    /// The exact claim name carrying group membership.
    pub groups_claim: String,
    /// The exact claim name carrying a display name.
    pub display_name_claim: String,
    /// How the client authenticates at the token endpoint.
    pub token_auth_method: TokenAuthMethod,
    /// Whether a plain-HTTP loopback issuer is permitted. For a local mock
    /// provider in development and tests; refused for any non-loopback host.
    pub insecure_loopback_issuer: bool,
    /// A PEM bundle of ADDITIONAL trust anchors for the provider's TLS
    /// certificate — a private CA that issued the issuer's certificate. Read
    /// before any socket exists (`crate::preflight`); an unreadable file, a
    /// file with no certificate and a file carrying anything but certificates
    /// are all refusals, never an empty addition.
    pub ca_bundle_file: Option<PathBuf>,
    /// Whether the operating system's trust store is consulted as well.
    /// `true` unless the administrator says otherwise; `false` requires
    /// `ca_bundle_file`, because a client with no trust anchor trusts nothing.
    pub system_roots: bool,
}

/// The validated role-binding table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolesConfig {
    /// The administrator's revision string, recorded in every audit line.
    pub revision: String,
    /// The bindings, in configuration order.
    pub bindings: Vec<crate::authz::RoleBinding>,
}

/// Everything shared mode adds.
#[derive(Clone, Debug)]
pub struct SharedConfig {
    /// The exact HTTPS base URL, with no path and no trailing slash. It is the
    /// public origin and the root of the redirect URI.
    pub public_base_url: String,
    /// The exact redirect URI, `<publicBaseUrl>/auth/callback`.
    pub redirect_uri: String,
    /// The OIDC block.
    pub oidc: OidcConfig,
    /// The session key file and its expected version.
    pub session_key: KeyRef,
    /// The session lifetime in seconds, at most fifteen minutes.
    pub session_max_age_seconds: i64,
    /// The role bindings.
    pub roles: RolesConfig,
    /// Proxy ranges whose forwarded headers may be recorded in the transport
    /// log. Never an identity input.
    pub trusted_proxy_cidrs: Vec<Cidr>,
    /// Whether every request (the two probes excepted) must arrive from a
    /// `trustedProxyCidrs` peer that asserts `X-Forwarded-Proto: https`.
    /// See `crate::http::entry_point`.
    pub require_trusted_proxy: bool,
}

/// Which mode the file asked for, with that mode's settings.
#[derive(Clone, Debug)]
pub enum Mode {
    /// The explicit loopback administrator mode.
    LocalAdmin(LocalAdmin),
    /// The SSO console.
    Shared(Box<SharedConfig>),
}

impl Mode {
    /// The mode's name, as written in the file and logged at startup.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Mode::LocalAdmin(_) => "localAdmin",
            Mode::Shared(_) => "shared",
        }
    }

    /// The shared settings, if this is shared mode.
    #[must_use]
    pub fn shared(&self) -> Option<&SharedConfig> {
        match self {
            Mode::Shared(shared) => Some(shared),
            Mode::LocalAdmin(_) => None,
        }
    }
}

/// A validated configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// The socket address to bind. Loopback only in localAdmin mode.
    pub listen: SocketAddr,
    /// The exact origin unsafe requests must carry.
    pub public_origin: String,
    /// `Host` header values this listener serves.
    pub allowed_hosts: Vec<String>,
    /// The static UI directory.
    pub ui_directory: PathBuf,
    /// The mode and its settings.
    pub mode: Mode,
    /// The explicitly granted namespaces, in configuration order.
    pub namespaces: Vec<String>,
    /// The Kubernetes client source.
    pub kubernetes: KubeSource,
    /// Where the cursor MAC key comes from.
    pub cursor_key: CursorKeySource,
    /// The Kubernetes identity this process writes as, for the attribution
    /// every created object and every audit line carries. `kubernetes.principal`
    /// when set (the chart always sets it, to
    /// `system:serviceaccount:<namespace>:<release>-api`); otherwise
    /// `inCluster` or `kubeconfig-context:<context>`, which name WHERE the
    /// identity comes from without claiming to know it.
    pub kubernetes_principal: String,
    /// PLAT-19.2: the approval-policy document, when one is configured.
    /// Absent is every namespace on `legacy-governed-v1`.
    pub approval_policy_file: Option<PathBuf>,
    /// PLAT-19.2: the console confirmation key file, when one is configured.
    pub confirmation_key_file: Option<PathBuf>,
}

impl Config {
    /// The local administrator, when this is localAdmin mode.
    #[must_use]
    pub fn local_admin(&self) -> Option<&LocalAdmin> {
        match &self.mode {
            Mode::LocalAdmin(admin) => Some(admin),
            Mode::Shared(_) => None,
        }
    }

    /// The shared settings, when this is shared mode.
    #[must_use]
    pub fn shared(&self) -> Option<&SharedConfig> {
        self.mode.shared()
    }
}

/// The widest IPv4 prefix `requireTrustedProxy` accepts in `trustedProxyCidrs`.
pub const MIN_TRUSTED_PREFIX_V4: u8 = 16;
/// The widest IPv6 prefix `requireTrustedProxy` accepts in `trustedProxyCidrs`.
pub const MIN_TRUSTED_PREFIX_V6: u8 = 48;

/// An IPv4 or IPv6 CIDR range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    base: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Parse `a.b.c.d/len` or `addr::/len`.
    ///
    /// # Errors
    ///
    /// A short reason.
    pub fn parse(value: &str) -> Result<Cidr, &'static str> {
        let (address, prefix) = value.split_once('/').ok_or("a CIDR needs a `/<length>`")?;
        let base: IpAddr = address.parse().map_err(|_| "not an IP address")?;
        let prefix: u8 = prefix.parse().map_err(|_| "not a prefix length")?;
        let max = match base {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max {
            return Err("the prefix length is longer than the address");
        }
        Ok(Cidr { base, prefix })
    }

    /// Whether this range is narrow enough to be a trusted-proxy range under
    /// `requireTrustedProxy`: at least `/16` for IPv4, `/48` for IPv6.
    #[must_use]
    pub const fn is_narrow_enough_to_trust(&self) -> bool {
        match self.base {
            IpAddr::V4(_) => self.prefix >= MIN_TRUSTED_PREFIX_V4,
            IpAddr::V6(_) => self.prefix >= MIN_TRUSTED_PREFIX_V6,
        }
    }

    /// Whether an address falls in this range.
    #[must_use]
    pub fn contains(&self, address: IpAddr) -> bool {
        match (self.base, address) {
            (IpAddr::V4(base), IpAddr::V4(other)) => {
                masked_v4(base, self.prefix) == masked_v4(other, self.prefix)
            }
            (IpAddr::V6(base), IpAddr::V6(other)) => {
                masked_v6(base, self.prefix) == masked_v6(other, self.prefix)
            }
            // An IPv4-mapped IPv6 peer is the IPv4 address it maps to.
            (IpAddr::V4(_), IpAddr::V6(other)) => other
                .to_ipv4_mapped()
                .is_some_and(|v4| self.contains(IpAddr::V4(v4))),
            (IpAddr::V6(_), IpAddr::V4(_)) => false,
        }
    }
}

fn masked_v4(address: Ipv4Addr, prefix: u8) -> u32 {
    let bits = u32::from(address);
    if prefix == 0 {
        0
    } else {
        bits & (u32::MAX << (32 - prefix))
    }
}

fn masked_v6(address: Ipv6Addr, prefix: u8) -> u128 {
    let bits = u128::from(address);
    if prefix == 0 {
        0
    } else {
        bits & (u128::MAX << (128 - prefix))
    }
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

        let namespaces = check_namespaces(&file.namespaces)?;
        let kubernetes = kube_source(base, &file.kubernetes)?;
        let kubernetes_principal = kubernetes_principal(&file.kubernetes, &kubernetes)?;

        let config = match file.mode.as_str() {
            "localAdmin" => Config::local_admin_mode(file, base, namespaces, kubernetes),
            "shared" => Config::shared_mode(file, base, namespaces, kubernetes),
            other => Err(field(
                "mode",
                format!(
                    "`{other}` is not a mode; the accepted values are `localAdmin` (an explicit \
                     loopback administrator listener) and `shared` (the SSO console)"
                ),
            )),
        }?;
        Ok(Config {
            kubernetes_principal,
            ..config
        })
    }

    fn local_admin_mode(
        file: ConfigFile,
        base: &Path,
        namespaces: Vec<String>,
        kubernetes: KubeSource,
    ) -> Result<Config, ConfigError> {
        let listen = parse_loopback_listen(&file.listen)?;
        let public_origin = file.public_origin.clone().ok_or_else(|| {
            field(
                "publicOrigin",
                "localAdmin mode requires the exact loopback origin the browser uses",
            )
        })?;
        let public_origin = parse_local_origin(&public_origin, listen.port())?;
        refuse_shared_only_fields(&file)?;
        if file.allowed_hosts.is_some() {
            return Err(field(
                "allowedHosts",
                "localAdmin mode derives the served Host values from `publicOrigin` and the \
                 listen port",
            ));
        }
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

        let cursor_key_file = file.cursor_key_file.ok_or_else(|| {
            field(
                "cursorKeyFile",
                "localAdmin mode requires `cursorKeyFile`, a file of at least 32 random bytes",
            )
        })?;
        if file.cursor_key.is_some() {
            return Err(field(
                "cursorKey",
                "`cursorKey` is the shared-mode versioned form; localAdmin mode uses \
                 `cursorKeyFile`",
            ));
        }

        Ok(Config {
            listen,
            public_origin,
            allowed_hosts,
            ui_directory: resolve(base, &file.ui_directory),
            mode: Mode::LocalAdmin(local_admin),
            namespaces,
            kubernetes,
            cursor_key: CursorKeySource::RawFile(resolve(base, &cursor_key_file)),
            kubernetes_principal: String::new(),
            approval_policy_file: file.approval_policy_file.map(|p| resolve(base, &p)),
            confirmation_key_file: file.confirmation_key_file.map(|p| resolve(base, &p)),
        })
    }

    fn shared_mode(
        file: ConfigFile,
        base: &Path,
        namespaces: Vec<String>,
        kubernetes: KubeSource,
    ) -> Result<Config, ConfigError> {
        if file.local_admin.is_some() {
            return Err(field(
                "localAdmin",
                "shared mode has no local administrator; every actor is an authenticated OIDC \
                 subject",
            ));
        }
        if file.public_origin.is_some() {
            return Err(field(
                "publicOrigin",
                "shared mode derives the origin from `publicBaseUrl`, so that the origin the \
                 browser is checked against and the redirect URI the provider is given cannot \
                 disagree",
            ));
        }
        let listen: SocketAddr = file.listen.parse().map_err(|_| {
            field(
                "listen",
                format!(
                    "`{}` is not an IP:port socket address (hostnames are refused)",
                    file.listen
                ),
            )
        })?;
        if listen.port() == 0 {
            return Err(field("listen", "an explicit, non-zero port is required"));
        }

        let raw_base = file.public_base_url.clone().ok_or_else(|| {
            field(
                "publicBaseUrl",
                "shared mode requires the exact HTTPS URL the browser uses; the redirect URI is \
                 derived from it and nothing reads Host or X-Forwarded-*",
            )
        })?;
        let public_base_url = parse_public_base_url(&raw_base)?;
        let redirect_uri = format!("{public_base_url}{}", crate::auth::login::CALLBACK_SUFFIX);

        // `allowedHosts` WIDENS THE DNS-REBINDING GUARD, so each entry is
        // validated as an authority rather than as a line of text: a `Host` a
        // browser could never send is a typo that silently does nothing, and a
        // value carrying a scheme, a path or a space is a misunderstanding of
        // what the field is. `docs/api.md` states the consequence of adding
        // one. Review finding F-5.
        let mut allowed = vec![authority_of(&public_base_url)];
        for extra in file.allowed_hosts.unwrap_or_default() {
            if !is_host_authority(&extra) {
                return Err(field(
                    "allowedHosts",
                    format!(
                        "`{extra}` is not a Host value: each entry is `host` or \
                         `host:port` with no scheme, userinfo, path or whitespace, and \
                         there is no wildcard"
                    ),
                ));
            }
            if !allowed.iter().any(|h| h == &extra) {
                allowed.push(extra);
            }
        }

        let oidc_file = file
            .oidc
            .ok_or_else(|| field("oidc", "shared mode requires the `oidc` block"))?;
        let oidc = parse_oidc(base, oidc_file)?;

        let session_key = file.session_key.ok_or_else(|| {
            field(
                "sessionKey",
                "shared mode requires `sessionKey.file` and `sessionKey.expectedVersion`; the \
                 version travels in the cookie and a mismatch is a startup refusal",
            )
        })?;
        let cursor_key = file.cursor_key.ok_or_else(|| {
            field(
                "cursorKey",
                "shared mode requires `cursorKey.file` and `cursorKey.expectedVersion`",
            )
        })?;
        if file.cursor_key_file.is_some() {
            return Err(field(
                "cursorKeyFile",
                "`cursorKeyFile` is the localAdmin unversioned form; shared mode uses \
                 `cursorKey.{file,expectedVersion}`",
            ));
        }

        let session_max_age_seconds = file
            .session_max_age_seconds
            .unwrap_or(crate::auth::session::MAX_SESSION_SECONDS);
        if !(60..=crate::auth::session::MAX_SESSION_SECONDS).contains(&session_max_age_seconds) {
            return Err(field(
                "sessionMaxAgeSeconds",
                format!(
                    "must be between 60 and {} seconds; a stateless session cannot be revoked \
                     before it expires, so its maximum is fixed",
                    crate::auth::session::MAX_SESSION_SECONDS
                ),
            ));
        }

        let roles = parse_roles(
            file.roles.ok_or_else(|| {
                field(
                    "roles",
                    "shared mode requires `roles.revision` and at least one `roles.bindings` \
                     entry; there is no default namespace and no implicit grant",
                )
            })?,
            &namespaces,
        )?;

        let mut trusted_proxy_cidrs = Vec::new();
        let mut trusted_proxy_raw = Vec::new();
        for raw in file.trusted_proxy_cidrs.unwrap_or_default() {
            trusted_proxy_cidrs.push(
                Cidr::parse(&raw)
                    .map_err(|reason| field("trustedProxyCidrs", format!("`{raw}`: {reason}")))?,
            );
            trusted_proxy_raw.push(raw);
        }
        // A GATE THAT TRUSTS A WHOLE NETWORK IS A HEADER CHECK. With
        // `requireTrustedProxy`, a range wider than /16 (IPv4) or /48 (IPv6)
        // would admit every pod of a typical cluster — which can dial the
        // console Service directly and send the proxy's header itself — so the
        // gate would distinguish nothing (review M3). Logging-only ranges keep
        // their old latitude: they decide nothing.
        let require_trusted_proxy = file.require_trusted_proxy.unwrap_or(false);
        if require_trusted_proxy {
            if let Some(wide) = trusted_proxy_raw
                .iter()
                .zip(&trusted_proxy_cidrs)
                .find(|(_, cidr)| !cidr.is_narrow_enough_to_trust())
                .map(|(raw, _)| raw.clone())
            {
                return Err(field(
                    "trustedProxyCidrs",
                    format!(
                        "`{wide}` is wider than /{MIN_TRUSTED_PREFIX_V4} (IPv4) or \
                         /{MIN_TRUSTED_PREFIX_V6} (IPv6); with `requireTrustedProxy` a range \
                         this wide trusts the pods the gate exists to refuse. Name the \
                         ingress controller's own range"
                    ),
                ));
            }
        }
        // A GATE WITH NOTHING BEHIND IT IS A REFUSAL OF EVERYTHING. Requiring
        // the trusted proxy with no range to trust would answer every request
        // 421 and look like an outage; it is refused here, by name, instead.
        if require_trusted_proxy && trusted_proxy_cidrs.is_empty() {
            return Err(field(
                "requireTrustedProxy",
                "requires at least one `trustedProxyCidrs` range: the entry point would \
                 otherwise refuse every request",
            ));
        }

        Ok(Config {
            listen,
            public_origin: public_base_url.clone(),
            allowed_hosts: allowed,
            ui_directory: resolve(base, &file.ui_directory),
            mode: Mode::Shared(Box::new(SharedConfig {
                public_base_url,
                redirect_uri,
                oidc,
                session_key: KeyRef {
                    file: resolve(base, &session_key.file),
                    expected_version: session_key.expected_version,
                },
                session_max_age_seconds,
                roles,
                trusted_proxy_cidrs,
                require_trusted_proxy,
            })),
            namespaces,
            kubernetes,
            cursor_key: CursorKeySource::Versioned(KeyRef {
                file: resolve(base, &cursor_key.file),
                expected_version: cursor_key.expected_version,
            }),
            kubernetes_principal: String::new(),
            approval_policy_file: file.approval_policy_file.map(|p| resolve(base, &p)),
            confirmation_key_file: file.confirmation_key_file.map(|p| resolve(base, &p)),
        })
    }
}

fn refuse_shared_only_fields(file: &ConfigFile) -> Result<(), ConfigError> {
    for (present, name) in [
        (file.public_base_url.is_some(), "publicBaseUrl"),
        (file.oidc.is_some(), "oidc"),
        (file.roles.is_some(), "roles"),
        (file.session_key.is_some(), "sessionKey"),
        (
            file.session_max_age_seconds.is_some(),
            "sessionMaxAgeSeconds",
        ),
        (file.trusted_proxy_cidrs.is_some(), "trustedProxyCidrs"),
        (file.require_trusted_proxy.is_some(), "requireTrustedProxy"),
    ] {
        if present {
            return Err(field(
                name,
                "this field belongs to `mode: shared`; localAdmin mode has no session, no \
                 identity provider and no role bindings",
            ));
        }
    }
    Ok(())
}

fn check_namespaces(namespaces: &[String]) -> Result<Vec<String>, ConfigError> {
    if namespaces.is_empty() {
        return Err(field(
            "namespaces",
            "at least one namespace must be granted explicitly; this service never lists \
             core Namespace objects",
        ));
    }
    if namespaces.len() > MAX_NAMESPACES {
        return Err(field(
            "namespaces",
            format!("at most {MAX_NAMESPACES} namespaces may be granted"),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for ns in namespaces {
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
    Ok(namespaces.to_vec())
}

/// The Kubernetes principal attribution records. See [`Config::kubernetes_principal`].
///
/// A DECLARATION, CHECKED FOR SHAPE. The value goes onto every object this
/// service creates, so it is a single line with no whitespace, and an
/// in-cluster value must be a ServiceAccount username — the only identity a
/// pod's projected token can carry.
fn kubernetes_principal(file: &KubernetesFile, source: &KubeSource) -> Result<String, ConfigError> {
    match (&file.principal, source) {
        (Some(principal), source) => {
            validate::check_single_line(principal, 253)
                .map_err(|code| field("kubernetes.principal", code))?;
            if principal.chars().any(char::is_whitespace) {
                return Err(field("kubernetes.principal", "must not contain whitespace"));
            }
            if matches!(source, KubeSource::InCluster) {
                let parts: Vec<&str> = principal.split(':').collect();
                let shaped = parts.len() == 4
                    && parts[0] == "system"
                    && parts[1] == "serviceaccount"
                    && validate::is_dns_label(parts[2])
                    && validate::is_dns_subdomain(parts[3]);
                if !shaped {
                    return Err(field(
                        "kubernetes.principal",
                        "an in-cluster principal is the pod's ServiceAccount, written \
                         `system:serviceaccount:<namespace>:<name>`",
                    ));
                }
            }
            Ok(principal.clone())
        }
        (None, KubeSource::InCluster) => Ok("inCluster".to_string()),
        (None, KubeSource::Kubeconfig { context, .. }) => {
            Ok(format!("kubeconfig-context:{context}"))
        }
    }
}

fn kube_source(base: &Path, file: &KubernetesFile) -> Result<KubeSource, ConfigError> {
    match file.source.as_str() {
        "inCluster" => {
            if file.kubeconfig.is_some() || file.context.is_some() {
                return Err(field(
                    "kubernetes",
                    "`source: inCluster` takes neither `kubeconfig` nor `context`",
                ));
            }
            Ok(KubeSource::InCluster)
        }
        "kubeconfig" => {
            let context = match file.context.clone() {
                Some(context) if !context.trim().is_empty() => context,
                _ => {
                    return Err(field(
                        "kubernetes.context",
                        "`source: kubeconfig` requires an explicit `context`; the \
                         kubeconfig's current-context is never used",
                    ))
                }
            };
            Ok(KubeSource::Kubeconfig {
                path: file.kubeconfig.as_deref().map(|p| resolve(base, p)),
                context,
            })
        }
        other => Err(field(
            "kubernetes.source",
            format!("`{other}` is not a source; use `inCluster` or `kubeconfig`"),
        )),
    }
}

/// `https://host[:port]`, nothing else. The refusal of `http` in shared mode
/// is the TLS requirement, enforced before a socket exists.
fn parse_public_base_url(value: &str) -> Result<String, ConfigError> {
    let refuse = |reason: &str| field("publicBaseUrl", format!("`{value}`: {reason}"));
    let (scheme, rest) = value
        .split_once("://")
        .ok_or_else(|| refuse("must be https://host[:port]"))?;
    if scheme == "http" {
        return Err(refuse(
            "shared mode refuses a non-HTTPS public URL; TLS at the shared entry point is \
             required, and terminating it at the supported ingress controller is the \
             documented deployment",
        ));
    }
    if scheme != "https" {
        return Err(refuse("the scheme must be https"));
    }
    if rest.is_empty() || rest.contains('/') || rest.contains('?') || rest.contains('#') {
        return Err(refuse(
            "the base URL carries no path, query or fragment (no trailing slash); the redirect \
             URI is this value plus /auth/callback",
        ));
    }
    if rest.contains('@') {
        return Err(refuse("a URL used as an origin carries no userinfo"));
    }
    if rest.contains(char::is_whitespace) || rest.chars().any(char::is_control) {
        return Err(refuse("the authority carries no whitespace"));
    }
    let host = rest.split(':').next().unwrap_or("");
    if host.is_empty() {
        return Err(refuse("the authority carries no host"));
    }
    Ok(value.to_string())
}

/// `host` or `host:port`, the two shapes a `Host` header takes, plus the
/// bracketed IPv6 literal. No scheme, no userinfo, no path, no whitespace, and
/// no wildcard.
fn is_host_authority(value: &str) -> bool {
    if value.is_empty() || value.len() > 261 {
        return false;
    }
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let Some((inner, after)) = rest.split_once(']') else {
            return false;
        };
        if inner.is_empty() || !inner.bytes().all(|b| b.is_ascii_hexdigit() || b == b':') {
            return false;
        }
        match after {
            "" => (inner, None),
            other => match other.strip_prefix(':') {
                Some(port) => (inner, Some(port)),
                None => return false,
            },
        }
    } else {
        match value.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (value, None),
        }
    };
    let host_ok = !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        && !host.starts_with('.')
        && !host.starts_with('-')
        && !host.ends_with('.')
        && !host.ends_with('-');
    if !host_ok {
        return false;
    }
    match port {
        None => true,
        Some(port) => {
            !port.is_empty()
                && port.len() <= 5
                && port.bytes().all(|b| b.is_ascii_digit())
                && port.parse::<u32>().is_ok_and(|p| (1..=65_535).contains(&p))
        }
    }
}

fn authority_of(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| url.to_string())
}

fn parse_oidc(base: &Path, file: OidcFile) -> Result<OidcConfig, ConfigError> {
    let issuer = file.issuer.trim().to_string();
    let insecure_loopback_issuer = file.insecure_loopback_issuer.unwrap_or(false);
    let (scheme, rest) = issuer
        .split_once("://")
        .ok_or_else(|| field("oidc.issuer", "must be an absolute URL"))?;
    if issuer.ends_with('/') {
        return Err(field(
            "oidc.issuer",
            "must be the issuer EXACTLY as the provider states it in its discovery document, \
             which never ends in a slash",
        ));
    }
    if rest.contains('?') || rest.contains('#') || rest.contains('@') {
        return Err(field(
            "oidc.issuer",
            "carries no query, fragment or userinfo",
        ));
    }
    let host = rest
        .split('/')
        .next()
        .unwrap_or("")
        .rsplit_once(':')
        .map_or_else(
            || rest.split('/').next().unwrap_or("").to_string(),
            |(h, _)| h.to_string(),
        );
    let loopback_host = host == "localhost"
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .map(is_loopback)
            .unwrap_or(false);
    match scheme {
        "https" => {}
        "http" if insecure_loopback_issuer && loopback_host => {}
        "http" => {
            return Err(field(
                "oidc.issuer",
                "a plain-HTTP issuer is accepted only for a loopback host and only with \
                 `oidc.insecureLoopbackIssuer: true`, which exists for a local mock provider \
                 in development and tests",
            ))
        }
        other => {
            return Err(field(
                "oidc.issuer",
                format!("`{other}` is not a scheme an issuer may use"),
            ))
        }
    }
    if insecure_loopback_issuer && !loopback_host {
        return Err(field(
            "oidc.insecureLoopbackIssuer",
            "may be set only when the issuer's host is a loopback address",
        ));
    }

    validate::check_single_line(&file.client_id, 256)
        .map_err(|code| field("oidc.clientId", code))?;
    if file.client_id.trim().is_empty() {
        return Err(field("oidc.clientId", "must not be empty"));
    }

    let allowed_algorithms = file.allowed_algorithms.unwrap_or_else(|| {
        SUPPORTED_ALGORITHMS
            .iter()
            .map(|a| (*a).to_string())
            .collect()
    });
    if allowed_algorithms.is_empty() {
        return Err(field(
            "oidc.allowedAlgorithms",
            "at least one algorithm must be allowed",
        ));
    }
    for algorithm in &allowed_algorithms {
        if !SUPPORTED_ALGORITHMS.contains(&algorithm.as_str()) {
            return Err(field(
                "oidc.allowedAlgorithms",
                format!(
                    "`{algorithm}` is not verified by this service; the supported values are {}",
                    SUPPORTED_ALGORITHMS.join(", ")
                ),
            ));
        }
    }

    let scopes = file.scopes.unwrap_or_else(|| {
        ["openid", "profile", "groups"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    });
    if !scopes.iter().any(|s| s == "openid") {
        return Err(field(
            "oidc.scopes",
            "must contain `openid`; without it the provider issues no ID token",
        ));
    }
    for scope in &scopes {
        if scope.is_empty() || scope.contains(char::is_whitespace) {
            return Err(field("oidc.scopes", "a scope carries no whitespace"));
        }
    }

    let groups_claim = file.groups_claim.unwrap_or_else(|| "groups".to_string());
    let display_name_claim = file
        .display_name_claim
        .unwrap_or_else(|| "name".to_string());
    for (value, name) in [
        (&groups_claim, "oidc.groupsClaim"),
        (&display_name_claim, "oidc.displayNameClaim"),
    ] {
        validate::check_single_line(value, 64).map_err(|code| field(name, code))?;
        if value.trim().is_empty() {
            return Err(field(name, "must not be empty"));
        }
    }

    let token_auth_method = match file.token_auth_method.as_deref() {
        None | Some("clientSecretBasic") => TokenAuthMethod::ClientSecretBasic,
        Some("clientSecretPost") => TokenAuthMethod::ClientSecretPost,
        Some(other) => {
            return Err(field(
                "oidc.tokenAuthMethod",
                format!("`{other}` is not a method; use `clientSecretBasic` or `clientSecretPost`"),
            ))
        }
    };

    // THE PROVIDER'S TLS TRUST (chart gap G1). A private issuer CA is ADDED to
    // the system roots by default; dropping the system roots is an explicit
    // second decision, and it is refused without a bundle to put in their
    // place — a client with no trust anchor would refuse every provider and
    // look like an outage. The bundle is only a PATH here: it is read, and
    // every refusal about its CONTENT made, in `crate::preflight`.
    let system_roots = file.system_roots.unwrap_or(true);
    if !system_roots && file.ca_bundle_file.is_none() {
        return Err(field(
            "oidc.systemRoots",
            "`false` requires `oidc.caBundleFile`: without the system roots and without a \
             bundle the console would trust no certificate at all",
        ));
    }

    Ok(OidcConfig {
        issuer,
        client_id: file.client_id,
        client_secret_file: resolve(base, &file.client_secret_file),
        allowed_algorithms,
        scopes,
        groups_claim,
        display_name_claim,
        token_auth_method,
        insecure_loopback_issuer,
        ca_bundle_file: file.ca_bundle_file.map(|p| resolve(base, &p)),
        system_roots,
    })
}

fn parse_roles(file: RolesFile, namespaces: &[String]) -> Result<RolesConfig, ConfigError> {
    validate::check_single_line(&file.revision, 128)
        .map_err(|code| field("roles.revision", code))?;
    if file.revision.trim().is_empty() {
        return Err(field(
            "roles.revision",
            "must name the revision of this binding table; it is recorded in every audit line",
        ));
    }
    if file.bindings.is_empty() {
        return Err(field(
            "roles.bindings",
            "at least one binding is required; an empty table grants nothing to anyone",
        ));
    }
    let mut bindings = Vec::with_capacity(file.bindings.len());
    for (index, binding) in file.bindings.into_iter().enumerate() {
        let at = |what: &str| format!("roles.bindings[{index}]: {what}");
        let role = crate::authz::Role::parse(&binding.role).ok_or_else(|| {
            field(
                "roles.bindings",
                at(&format!(
                    "`{}` is not a role; the roles are viewer, operator, approver, administrator",
                    binding.role
                )),
            )
        })?;
        if !namespaces.iter().any(|n| n == &binding.namespace) {
            return Err(field(
                "roles.bindings",
                at(&format!(
                    "namespace `{}` is not in `namespaces`; a binding cannot grant a namespace \
                     this service does not manage",
                    binding.namespace
                )),
            ));
        }
        if binding.groups.is_empty() && binding.subjects.is_empty() {
            return Err(field(
                "roles.bindings",
                at("a binding needs at least one exact `groups` or `subjects` entry"),
            ));
        }
        for group in &binding.groups {
            check_exact(group, "groups", &at)?;
        }
        for subject in &binding.subjects {
            check_exact(subject, "subjects", &at)?;
            if !subject.contains('#') {
                return Err(field(
                    "roles.bindings",
                    at(&format!(
                        "`{subject}` is not an actor id; a subject binding is the exact \
                         `<issuer>#<subject>` string the session reports"
                    )),
                ));
            }
        }
        bindings.push(crate::authz::RoleBinding {
            role,
            namespace: binding.namespace,
            groups: binding.groups,
            subjects: binding.subjects,
        });
    }
    Ok(RolesConfig {
        revision: file.revision,
        bindings,
    })
}

fn check_exact(value: &str, what: &str, at: &impl Fn(&str) -> String) -> Result<(), ConfigError> {
    validate::check_single_line(value, 320)
        .map_err(|code| field("roles.bindings", at(&format!("{what}: {code}"))))?;
    if value.trim().is_empty() {
        return Err(field(
            "roles.bindings",
            at(&format!(
                "{what}: an empty string matches nothing and is refused"
            )),
        ));
    }
    if value.contains('*') || value.contains('?') {
        return Err(field(
            "roles.bindings",
            at(&format!(
                "{what}: `{value}` looks like a pattern. Bindings are EXACT strings: there is \
                 no wildcard, no regex and no domain inference, so a `*` here would match \
                 nothing rather than everything"
            )),
        ));
    }
    Ok(())
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

    /// **The configuration in `docs/api.md` is a configuration this code
    /// accepts, and its paths mean what the prose says.**
    ///
    /// REGRESSION REASON (review finding R2). The example used to write
    /// `kubeconfig: ~/.kube/config`. Nothing here expands `~` — [`resolve`]
    /// joins any relative path onto the configuration file's own directory — so
    /// the documented example resolved to `<config-dir>/~/.kube/config`, which
    /// does not exist, and a first-time reader following the guide verbatim got
    /// exit 2 and no service. A documented example that cannot start is worse
    /// than no example, because the reader debugs their cluster instead of the
    /// line they copied.
    ///
    /// So the block is parsed from the document rather than retyped here: the
    /// two cannot drift.
    #[test]
    fn the_documented_example_configuration_parses() {
        let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("crates/logweir-api sits two levels under the workspace root")
            .join("docs/api.md");
        let text = std::fs::read_to_string(&doc).expect("docs/api.md is readable");
        let block = text
            .split("```yaml")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("docs/api.md has a ```yaml block");
        assert!(
            block.contains("# config.yaml"),
            "the first yaml block in docs/api.md is no longer the configuration example"
        );

        let config = Config::parse(block, Path::new("/etc/logweir"))
            .expect("the documented example is a configuration this code accepts");
        assert_eq!(config.listen.port(), 8484);
        assert_eq!(config.namespaces, vec!["team-a".to_string()]);
        // Relative paths resolve against the CONFIG FILE's directory, as the
        // prose under the block says.
        assert_eq!(config.ui_directory, PathBuf::from("/etc/logweir/ui"));
        assert_eq!(
            config.cursor_key,
            CursorKeySource::RawFile(PathBuf::from("/etc/logweir/cursor.key"))
        );
        // The example omits `kubeconfig`, so the client library's own
        // KUBECONFIG/home lookup applies — the only place `~` expansion belongs.
        assert_eq!(
            config.kubernetes,
            KubeSource::Kubeconfig {
                path: None,
                context: "docker-desktop".into()
            }
        );
    }

    /// **The shared-mode configuration in `docs/api.md` is one this code
    /// accepts, and its refusal table is not aspirational.**
    ///
    /// Same reasoning as the localAdmin example above (review finding R2): a
    /// documented example that cannot start sends the reader to debug their
    /// cluster instead of the line they copied. The block is parsed out of the
    /// document rather than retyped, so the two cannot drift, and the derived
    /// redirect URI is asserted because it is the one value an administrator
    /// must also register at the identity provider.
    #[test]
    fn the_documented_shared_configuration_parses() {
        let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("crates/logweir-api sits two levels under the workspace root")
            .join("docs/api.md");
        let text = std::fs::read_to_string(&doc).expect("docs/api.md is readable");
        let block = text
            .split("```yaml")
            .find(|rest| rest.trim_start().starts_with("# console.yaml"))
            .and_then(|rest| rest.split("```").next())
            .expect("docs/api.md has a ```yaml block starting `# console.yaml`");

        let config = Config::parse(block, Path::new("/etc/logweir"))
            .expect("the documented shared example is a configuration this code accepts");
        let shared = config.shared().expect("it is shared mode");
        assert_eq!(config.public_origin, "https://console.example.com");
        assert_eq!(
            shared.redirect_uri,
            "https://console.example.com/auth/callback"
        );
        // `publicBaseUrl`'s own authority FIRST, then whatever `allowedHosts`
        // adds — and the document explains what adding one costs (F-5).
        assert_eq!(
            config.allowed_hosts,
            vec![
                "console.example.com".to_string(),
                "console.internal.example".to_string()
            ]
        );
        assert_eq!(shared.session_max_age_seconds, 900);
        assert_eq!(shared.roles.bindings.len(), 3);
        assert_eq!(shared.roles.revision, "2026-09-16.1");
        assert_eq!(shared.session_key.expected_version, 1);
        assert_eq!(config.kubernetes, KubeSource::InCluster);
        assert!(shared.trusted_proxy_cidrs[0]
            .contains("192.0.2.17".parse().expect("a literal address")));
        assert!(
            !shared.trusted_proxy_cidrs[0].contains("10.4.5.6".parse().expect("a literal address"))
        );
        assert!(!shared.oidc.insecure_loopback_issuer);
        assert!(shared.require_trusted_proxy);
        assert_eq!(
            config.kubernetes_principal,
            "system:serviceaccount:logweir-system:logweir-api"
        );

        // AND THE REFUSALS THE DOCUMENT TABULATES ARE REAL. Each row below
        // changes exactly one line of the accepted example.
        let refusals: [(&str, &str, &str); 15] = [
            // PLAT-17.2: a trusted-proxy requirement with no range to trust
            // would refuse every request, so it is refused by name instead.
            (
                "trustedProxyCidrs: [\"192.0.2.0/24\"]",
                "trustedProxyCidrs: []",
                "requireTrustedProxy",
            ),
            // …and one whose range is so wide it trusts every pod on a typical
            // cluster (or everybody) is not a trusted proxy at all (review M3).
            (
                "trustedProxyCidrs: [\"192.0.2.0/24\"]",
                "trustedProxyCidrs: [\"10.0.0.0/8\"]",
                "trustedProxyCidrs",
            ),
            (
                "trustedProxyCidrs: [\"192.0.2.0/24\"]",
                "trustedProxyCidrs: [\"0.0.0.0/0\"]",
                "trustedProxyCidrs",
            ),
            (
                "trustedProxyCidrs: [\"192.0.2.0/24\"]",
                "trustedProxyCidrs: [\"::/0\"]",
                "trustedProxyCidrs",
            ),
            // An in-cluster principal is a ServiceAccount username or nothing.
            (
                "principal: system:serviceaccount:logweir-system:logweir-api",
                "principal: admin",
                "kubernetes.principal",
            ),
            (
                "principal: system:serviceaccount:logweir-system:logweir-api",
                "principal: \"system:serviceaccount:logweir-system:logweir api\"",
                "kubernetes.principal",
            ),
            (
                "publicBaseUrl: \"https://console.example.com\"",
                "publicBaseUrl: \"http://console.example.com\"",
                "publicBaseUrl",
            ),
            (
                "publicBaseUrl: \"https://console.example.com\"",
                "publicBaseUrl: \"https://console.example.com/\"",
                "publicBaseUrl",
            ),
            (
                "issuer: https://idp.example.com/realms/logweir",
                "issuer: https://idp.example.com/realms/logweir/",
                "oidc.issuer",
            ),
            (
                "issuer: https://idp.example.com/realms/logweir",
                "issuer: http://idp.example.com/realms/logweir",
                "oidc.issuer",
            ),
            (
                "allowedAlgorithms: [RS256, ES256]",
                "allowedAlgorithms: [HS256]",
                "oidc.allowedAlgorithms",
            ),
            (
                "groups: [\"logweir-team-a-viewers\"]",
                "groups: [\"*\"]",
                "roles.bindings",
            ),
            ("namespace: team-a", "namespace: team-zzz", "roles.bindings"),
            (
                "sessionMaxAgeSeconds: 900",
                "sessionMaxAgeSeconds: 86400",
                "sessionMaxAgeSeconds",
            ),
            (
                "allowedHosts: [\"console.internal.example\"]",
                "allowedHosts: [\"a host with spaces\"]",
                "allowedHosts",
            ),
        ];
        for (from, to, expected_field) in refusals {
            let broken = block.replacen(from, to, 1);
            assert_ne!(broken, block, "the example no longer contains `{from}`");
            match Config::parse(&broken, Path::new("/etc/logweir")) {
                Err(ConfigError::Field { field, .. }) => {
                    assert_eq!(
                        field, expected_field,
                        "`{to}` was refused by the wrong field"
                    )
                }
                other => panic!("`{to}` was not refused: {other:?}"),
            }
        }
    }

    /// **The principal attribution records names where it comes from when no
    /// value is declared, and `requireTrustedProxy` belongs to shared mode.**
    #[test]
    fn the_kubernetes_principal_defaults_to_its_source_and_the_proxy_flag_is_shared_only() {
        let base = text("127.0.0.1:8484", "http://127.0.0.1:8484");
        let config = Config::parse(&base, Path::new("/etc/logweir")).unwrap();
        assert_eq!(
            config.kubernetes_principal,
            "kubeconfig-context:docker-desktop"
        );
        // A kubeconfig principal is not a ServiceAccount, so only its shape as
        // a single token is checked.
        let declared = base.replace(
            "  context: docker-desktop\n",
            "  context: docker-desktop\n  principal: admin@docker-desktop\n",
        );
        assert_eq!(
            Config::parse(&declared, Path::new("/etc/logweir"))
                .unwrap()
                .kubernetes_principal,
            "admin@docker-desktop"
        );
        let local_with_proxy = format!("{base}requireTrustedProxy: true\n");
        match Config::parse(&local_with_proxy, Path::new("/etc/logweir")) {
            Err(ConfigError::Field { field, .. }) => assert_eq!(field, "requireTrustedProxy"),
            other => panic!("localAdmin accepted requireTrustedProxy: {other:?}"),
        }
    }

    /// A leading `~` is a directory name here, not a home reference. Pinned so
    /// that the sentence in `docs/api.md` stays true, and so that adding
    /// expansion later is a deliberate change rather than a silent one.
    #[test]
    fn a_tilde_in_a_path_is_not_expanded() {
        let text = text("127.0.0.1:8484", "http://127.0.0.1:8484").replace(
            "  context: docker-desktop\n",
            "  kubeconfig: ~/.kube/config\n  context: docker-desktop\n",
        );
        let config = Config::parse(&text, Path::new("/etc/logweir")).unwrap();
        assert_eq!(
            config.kubernetes,
            KubeSource::Kubeconfig {
                path: Some(PathBuf::from("/etc/logweir/~/.kube/config")),
                context: "docker-desktop".into()
            },
            "`~` must be joined literally; if this ever expands, docs/api.md says it does not"
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

    /// The two modes do not share fields, and the file has to pick one.
    #[test]
    fn a_field_belonging_to_the_other_mode_is_refused_by_name() {
        let local = text("127.0.0.1:8484", "http://127.0.0.1:8484");
        for (line, field) in [
            ("publicBaseUrl: \"https://c.example\"\n", "publicBaseUrl"),
            ("sessionMaxAgeSeconds: 600\n", "sessionMaxAgeSeconds"),
            ("trustedProxyCidrs: [\"10.0.0.0/8\"]\n", "trustedProxyCidrs"),
        ] {
            let t = local.clone() + line;
            match Config::parse(&t, Path::new(".")) {
                Err(ConfigError::Field { field: got, .. }) => assert_eq!(got, field),
                other => panic!("`{line}` was accepted in localAdmin mode: {other:?}"),
            }
        }
    }

    /// Shared mode needs every one of its blocks, and says which is missing.
    #[test]
    fn shared_mode_names_the_block_it_is_missing() {
        let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("crates/logweir-api sits two levels under the workspace root")
            .join("docs/api.md");
        let text = std::fs::read_to_string(&doc).expect("docs/api.md is readable");
        let block = text
            .split("```yaml")
            .find(|rest| rest.trim_start().starts_with("# console.yaml"))
            .and_then(|rest| rest.split("```").next())
            .expect("the console example is present")
            .to_string();

        // Removing a whole block, by the indentation its keys carry.
        let without = |heading: &str| -> String {
            let mut out = String::new();
            let mut skipping = false;
            for line in block.lines() {
                if skipping && (line.starts_with(' ') || line.starts_with('-')) {
                    continue;
                }
                skipping = false;
                if line.starts_with(heading) {
                    skipping = true;
                    continue;
                }
                out.push_str(line);
                out.push('\n');
            }
            out
        };
        for (heading, field) in [
            ("oidc:", "oidc"),
            ("roles:", "roles"),
            ("sessionKey:", "sessionKey"),
            ("cursorKey:", "cursorKey"),
        ] {
            let t = without(heading);
            match Config::parse(&t, Path::new(".")) {
                Err(ConfigError::Field { field: got, .. }) => {
                    assert_eq!(got, field, "removing `{heading}`")
                }
                other => panic!("shared mode started without `{heading}`: {other:?}"),
            }
        }

        // A localAdmin-only field in shared mode, and the unversioned cursor
        // key, are refused by name rather than ignored.
        for (line, field) in [
            ("localAdmin:\n  subject: admin\n", "localAdmin"),
            ("cursorKeyFile: ./cursor.key\n", "cursorKeyFile"),
            (
                "publicOrigin: \"https://console.example.com\"\n",
                "publicOrigin",
            ),
        ] {
            let t = block.clone() + line;
            match Config::parse(&t, Path::new(".")) {
                Err(ConfigError::Field { field: got, .. }) => assert_eq!(got, field),
                other => panic!("`{line}` was accepted in shared mode: {other:?}"),
            }
        }
    }

    /// A loopback issuer over plain HTTP is a development affordance, and it is
    /// bounded to loopback in both directions.
    #[test]
    fn an_insecure_issuer_is_confined_to_loopback() {
        let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("crates/logweir-api sits two levels under the workspace root")
            .join("docs/api.md");
        let text = std::fs::read_to_string(&doc).expect("docs/api.md is readable");
        let block = text
            .split("```yaml")
            .find(|rest| rest.trim_start().starts_with("# console.yaml"))
            .and_then(|rest| rest.split("```").next())
            .expect("the console example is present")
            .to_string();

        // Loopback + the flag: accepted.
        let ok = block.replace(
            "issuer: https://idp.example.com/realms/logweir",
            "issuer: http://127.0.0.1:38399/realms/logweir\n  insecureLoopbackIssuer: true",
        );
        let config = Config::parse(&ok, Path::new(".")).expect("a loopback mock issuer is allowed");
        assert!(config.shared().unwrap().oidc.insecure_loopback_issuer);

        // The flag WITHOUT a loopback host: refused, so it cannot be left on
        // in a production file by accident.
        let hostile = block.replace(
            "issuer: https://idp.example.com/realms/logweir",
            "issuer: https://idp.example.com/realms/logweir\n  insecureLoopbackIssuer: true",
        );
        assert!(matches!(
            Config::parse(&hostile, Path::new(".")),
            Err(ConfigError::Field {
                field: "oidc.insecureLoopbackIssuer",
                ..
            })
        ));

        // And plain HTTP on loopback WITHOUT the flag is still refused.
        let unflagged = block.replace(
            "issuer: https://idp.example.com/realms/logweir",
            "issuer: http://127.0.0.1:38399/realms/logweir",
        );
        assert!(matches!(
            Config::parse(&unflagged, Path::new(".")),
            Err(ConfigError::Field {
                field: "oidc.issuer",
                ..
            })
        ));

        // publicBaseUrl stays HTTPS in every one of those cases.
        let downgraded = ok.replace(
            "publicBaseUrl: \"https://console.example.com\"",
            "publicBaseUrl: \"http://127.0.0.1:8484\"",
        );
        assert!(matches!(
            Config::parse(&downgraded, Path::new(".")),
            Err(ConfigError::Field {
                field: "publicBaseUrl",
                ..
            })
        ));
    }

    #[test]
    fn cidrs_parse_and_match_only_their_own_range() {
        let v4 = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(v4.contains("10.255.1.2".parse().unwrap()));
        assert!(!v4.contains("11.0.0.1".parse().unwrap()));
        // An IPv4-mapped IPv6 peer is the IPv4 address it maps to.
        assert!(v4.contains("::ffff:10.1.2.3".parse().unwrap()));
        assert!(!v4.contains("fd00::1".parse().unwrap()));

        let v6 = Cidr::parse("fd00::/8").unwrap();
        assert!(v6.contains("fd12::9".parse().unwrap()));
        assert!(!v6.contains("fe80::1".parse().unwrap()));
        assert!(!v6.contains("10.0.0.1".parse().unwrap()));

        let all = Cidr::parse("0.0.0.0/0").unwrap();
        assert!(all.contains("203.0.113.9".parse().unwrap()));

        for bad in [
            "10.0.0.0",
            "10.0.0.0/33",
            "10.0.0.0/x",
            "not-an-ip/8",
            "fd00::/129",
        ] {
            assert!(Cidr::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn unknown_keys_and_modes_are_refused() {
        let t = text("127.0.0.1:8484", "http://127.0.0.1:8484") + "extra: 1\n";
        assert!(matches!(
            Config::parse(&t, Path::new(".")),
            Err(ConfigError::Parse(_))
        ));
        let t = text("127.0.0.1:8484", "http://127.0.0.1:8484")
            .replace("localAdmin\n", "proxyEverything\n");
        assert!(matches!(
            Config::parse(&t, Path::new(".")),
            Err(ConfigError::Field { field: "mode", .. })
        ));
    }
}
