//! The static UI, served byte for byte from an allowlist built at startup.
//!
//! THE SAME SELECTION `Dockerfile.ui` MAKES, AND NOTHING ELSE. The top-level
//! `*.html`, `*.js` and `*.css` files of the configured directory and the same
//! three extensions directly inside `pages/`. `README.md`, `tests/` (which
//! carries a throwaway keypair and fixtures), any other subdirectory, any
//! other extension, dotfiles and symbolic links are never in the allowlist, so
//! no request path can reach them: a request path is looked up EXACTLY, with
//! no decoding, normalisation or directory index, and anything not in the map
//! is 404.
//!
//! ONE ENTRY IS NOT A FILE: the page icon, [`ICON_PATH`]. `ui/index.html`
//! names it (`<link rel="icon" href="./favicon.svg">`), and it is compiled in
//! here rather than shipped under `ui/` because every shipped UI file is held
//! to plain ASCII text with no URL scheme on any line
//! (`scripts/check-ui-offline.sh`), and a standalone SVG cannot render without
//! its XML namespace URI. It used to be `data:,`, which this server's
//! Content-Security-Policy (`img-src` falls back to `default-src 'self'`)
//! refuses, so every console page load logged a CSP violation (PLAT-18.2's
//! live run). Served from `'self'`, the icon needs no widening of the policy.
//! A `.svg` FILE under `ui/` is still never selected: the extension list below
//! is unchanged, so the selection stays `Dockerfile.ui`'s.
//!
//! READ ONCE, INTO MEMORY. The bytes a browser receives are the bytes this
//! process read at startup; nothing on disk can swap a file under a running
//! server, and no request performs file-system I/O.

use std::collections::BTreeMap;
use std::path::Path;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use http::header::{self, HeaderValue};

use crate::app::AppState;
use crate::problem::ApiError;

/// The largest single asset accepted, 8 MiB.
pub const MAX_ASSET_BYTES: u64 = 8 * 1024 * 1024;

/// The page icon's path under `/ui/` — what `ui/index.html`'s
/// `<link rel="icon">` names, relative to the page.
pub const ICON_PATH: &str = "favicon.svg";

/// The page icon: the masthead's brand mark (`ui/index.html`'s inline
/// `.brand-mark`), in the light theme's `--accent` / `--on-accent`. Colours are
/// presentation attributes, not a `<style>` element, so nothing in it is an
/// inline style the policy would have to allow; it carries no script and no
/// reference to anything.
pub const ICON_SVG: &str = concat!(
    r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">"##,
    r##"<rect x="1" y="1" width="30" height="30" rx="8" fill="#0b5cad"/>"##,
    r##"<path fill="#ffffff" d="M6 8h20v3.2H6zM12 14.4h14v3.2H12zM18 20.8h8v3.2h-8z"/>"##,
    "</svg>\n"
);

/// The path of the console's own `runtime.js` under `/ui/`: the one script
/// `ui/index.html` loads before `app.js`.
pub const CONSOLE_RUNTIME_PATH: &str = "runtime.js";

/// THE CONSOLE'S `runtime.js`, compiled in and served IN PLACE OF the file
/// (console-ux-1 review L1).
///
/// The page decides which API is in front of it once, at boot, by asking
/// `GET /api/v1/session`. A probe is a request, and a request can be slow or
/// fail: before this, a console whose API answered late, with a 5xx or a 429,
/// or with a body the page could not decode was read as "not the product API"
/// and mounted legacy `kubectl proxy` mode behind `logweir-api` -- every page
/// then read `/apis/...` paths this service does not serve. This file is
/// served by THIS process with the page itself, so it is deterministic: a page
/// that loaded it knows it is the Logweir console, and a failed probe becomes
/// "the service could not be reached; retry", never legacy mode. The legacy
/// UI (`kubectl proxy --www=./ui`, or the chart's UI Deployment) serves the
/// file under `ui/`, which carries no such marker.
///
/// It carries no credential and no per-request value: the namespace context is
/// the file's empty one, because in console mode the grants come from
/// `/session`.
pub const CONSOLE_RUNTIME_JS: &str = concat!(
    "// runtime.js -- served by logweir-api (crates/logweir-api/src/assets.rs):
",
    "// this page is the Logweir console. Compiled into the service, never a file.
",
    "window.LOGWEIR_NAMESPACE_CONTEXT = Object.freeze({ allowed: [], selected: \"\" });
",
    "window.LOGWEIR_CONSOLE = Object.freeze({ servedBy: \"logweir-api\" });
",
);

/// One served file.
#[derive(Clone, Debug)]
pub struct Asset {
    /// The file's bytes.
    pub bytes: Bytes,
    /// Its media type.
    pub content_type: &'static str,
}

/// The allowlisted assets, keyed by their path under `/ui/`.
#[derive(Clone, Debug, Default)]
pub struct StaticAssets {
    files: BTreeMap<String, Asset>,
}

fn content_type_for(name: &str) -> Option<&'static str> {
    let extension = name.rsplit_once('.').map(|(_, e)| e)?;
    match extension {
        "html" => Some("text/html; charset=utf-8"),
        "js" => Some("text/javascript; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        _ => None,
    }
}

fn safe_name(name: &str) -> bool {
    !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

impl StaticAssets {
    /// Load the allowlist from `dir`.
    ///
    /// # Errors
    ///
    /// A reason when the directory is unreadable, has no `index.html`, or a
    /// selected file exceeds [`MAX_ASSET_BYTES`].
    pub fn load(dir: &Path) -> Result<StaticAssets, String> {
        let mut files = BTreeMap::new();
        collect(dir, "", &mut files)?;
        let pages = dir.join("pages");
        match std::fs::symlink_metadata(&pages) {
            Ok(meta) if meta.file_type().is_dir() => collect(&pages, "pages/", &mut files)?,
            _ => {}
        }
        if !files.contains_key("index.html") {
            return Err(format!(
                "the UI directory {} has no index.html",
                dir.display()
            ));
        }
        // THE CONSOLE'S OWN runtime.js REPLACES THE FILE (review L1): the
        // marker that tells the page it is behind this service.
        files.insert(
            CONSOLE_RUNTIME_PATH.to_string(),
            Asset {
                bytes: Bytes::from_static(CONSOLE_RUNTIME_JS.as_bytes()),
                content_type: "text/javascript; charset=utf-8",
            },
        );
        // No file can collide with it: `.svg` is not a selected extension.
        files.insert(
            ICON_PATH.to_string(),
            Asset {
                bytes: Bytes::from_static(ICON_SVG.as_bytes()),
                content_type: "image/svg+xml",
            },
        );
        Ok(StaticAssets { files })
    }

    /// The asset at a path under `/ui/`, looked up exactly.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<&Asset> {
        self.files.get(path)
    }

    /// Every served path, sorted.
    #[must_use]
    pub fn paths(&self) -> Vec<String> {
        self.files.keys().cloned().collect()
    }
}

fn collect(dir: &Path, prefix: &str, files: &mut BTreeMap<String, Asset>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
        // `DirEntry::file_type` does not follow symbolic links: a link is
        // neither a file nor a directory here and is skipped.
        let file_type = entry
            .file_type()
            .map_err(|e| format!("cannot stat an entry of {}: {e}", dir.display()))?;
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(content_type) = content_type_for(&name) else {
            continue;
        };
        if !safe_name(&name) {
            continue;
        }
        let path = entry.path();
        let size = entry
            .metadata()
            .map_err(|e| format!("cannot stat {}: {e}", path.display()))?
            .len();
        if size > MAX_ASSET_BYTES {
            return Err(format!(
                "{} is {size} bytes; the largest asset served is {MAX_ASSET_BYTES}",
                path.display()
            ));
        }
        let bytes =
            std::fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        files.insert(
            format!("{prefix}{name}"),
            Asset {
                bytes: Bytes::from(bytes),
                content_type,
            },
        );
    }
    Ok(())
}

/// `GET /ui/` and `GET /ui/{*path}`.
pub async fn serve(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path();
    let Some(rest) = path.strip_prefix("/ui/") else {
        return ApiError::not_found().into_response();
    };
    let key = if rest.is_empty() { "index.html" } else { rest };
    let Some(asset) = state.assets().get(key) else {
        return ApiError::not_found().into_response();
    };
    let mut response = asset.bytes.clone().into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(asset.content_type),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_three_extensions_and_safe_names_are_selected() {
        assert_eq!(
            content_type_for("a.js"),
            Some("text/javascript; charset=utf-8")
        );
        assert_eq!(content_type_for("README.md"), None);
        assert_eq!(content_type_for("key.pem"), None);
        assert_eq!(content_type_for("noext"), None);
        assert!(!safe_name(".hidden.js"));
        assert!(!safe_name("sp ace.js"));
        assert!(safe_name("restore-wizard.js"));
        // The icon is compiled in, and a file of its extension is never
        // selected — so no `ui/` file can shadow it.
        assert_eq!(content_type_for(ICON_PATH), None);
    }
}
