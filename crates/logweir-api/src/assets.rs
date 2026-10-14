//! The console, served byte for byte from an allowlist built at startup.
//!
//! THE BUILT CONSOLE AND NOTHING ELSE (PROD-18.1). The configured directory is
//! `console/dist` as `Dockerfile.console` copies it: the top-level `*.html`, `*.js`
//! and `*.css` files and, one level deeper, the build's hashed files in `assets/`
//! (scripts, styles and the design system's `woff2` fonts and `svg` images). Any
//! other subdirectory, any other extension, dotfiles (`.vite/`) and symbolic links
//! are never in the allowlist, so no request path can reach them: a request path
//! is looked up EXACTLY, with no decoding, normalisation or directory index, and
//! anything not in the map is 404. `/ui/` is `index.html`, served `no-cache`; a
//! hashed file is `immutable`, because its name changes whenever its bytes do.
//!
//! ONE ENTRY IS NOT A FILE: the page icon, [`ICON_PATH`], compiled in so that no
//! file can shadow it (`.svg` is selected only under `assets/`). A `data:,` icon
//! would be refused by this server's Content-Security-Policy (`img-src` falls back
//! to `default-src 'self'`); served from `'self'`, the icon needs no widening of
//! the policy.
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

/// The build's hashed output: the one place fonts, images and `immutable` caching are allowed.
pub const ASSETS_DIR: &str = "assets/";

/// `Cache-Control` for a hashed file: its name changes whenever its bytes do.
pub const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// The largest single asset accepted, 8 MiB.
pub const MAX_ASSET_BYTES: u64 = 8 * 1024 * 1024;

/// The page icon's path under `/ui/`: what the console's `<link rel="icon">`
/// names, relative to the page.
pub const ICON_PATH: &str = "favicon.svg";

/// The page icon: the masthead's brand mark, in Logweir's accent. Colours are
/// presentation attributes, not a `<style>` element, so nothing in it is an
/// inline style the policy would have to allow; it carries no script and no
/// reference to anything.
pub const ICON_SVG: &str = concat!(
    r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">"##,
    r##"<rect x="1" y="1" width="30" height="30" rx="8" fill="#0b5cad"/>"##,
    r##"<path fill="#ffffff" d="M6 8h20v3.2H6zM12 14.4h14v3.2H12zM18 20.8h8v3.2h-8z"/>"##,
    "</svg>\n"
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

/// The build's hashed files: scripts, styles and the design system's fonts and
/// images. No page: the one page is `index.html`.
fn hashed_content_type_for(name: &str) -> Option<&'static str> {
    let extension = name.rsplit_once('.').map(|(_, e)| e)?;
    match extension {
        "js" | "css" => content_type_for(name),
        "woff2" => Some("font/woff2"),
        "svg" => Some("image/svg+xml"),
        _ => None,
    }
}

fn is_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_dir())
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
        collect(dir, "", content_type_for, &mut files)?;
        // Joined WITHOUT the key's trailing slash: `assets/` would make the
        // operating system resolve a symbolic link named `assets` before
        // `symlink_metadata` could refuse it.
        let hashed = dir.join(ASSETS_DIR.trim_end_matches('/'));
        if is_dir(&hashed) {
            collect(&hashed, ASSETS_DIR, hashed_content_type_for, &mut files)?;
        }
        if !files.contains_key("index.html") {
            return Err(format!(
                "the UI directory {} has no index.html",
                dir.display()
            ));
        }
        // No file can collide with it: `.svg` is selected only under `assets/`.
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

fn collect(
    dir: &Path,
    prefix: &str,
    content_type_for: fn(&str) -> Option<&'static str>,
    files: &mut BTreeMap<String, Asset>,
) -> Result<(), String> {
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
    let cache = if key.starts_with(ASSETS_DIR) {
        IMMUTABLE
    } else {
        "no-cache"
    };
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
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
        assert!(safe_name("index-AbC_1-x.js"));
        // The icon is compiled in, and a file of its extension is never
        // selected at the top level, so no file can shadow it.
        assert_eq!(content_type_for(ICON_PATH), None);
        // Fonts and images only among the hashed files, and no page there.
        assert_eq!(content_type_for("a.woff2"), None);
        assert_eq!(hashed_content_type_for("a.woff2"), Some("font/woff2"));
        assert_eq!(hashed_content_type_for("a.svg"), Some("image/svg+xml"));
        assert_eq!(hashed_content_type_for("a.html"), None);
        assert_eq!(hashed_content_type_for("a.js.map"), None);
        assert_eq!(hashed_content_type_for("a.woff"), None);
    }
}
