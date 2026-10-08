//! THE engine identity Logweir signs: `engine.version` and `engine.digest` in
//! every drill scorecard and every backup receipt (PROD-00.2, OD-3).
//!
//! TWO SOURCES, AND THE IMAGE WINS. The runner image is built with one engine
//! — Logweir's build of the vendored OSO source by default, or OSO's released
//! binary under the documented one-release rollback — and the `Dockerfile`
//! writes WHICH one into the image at [`IMAGE_IDENTITY_PATH`], copied from the
//! same build stage as the binary. A Job also carries `LOGWEIR_ENGINE_VERSION`
//! and `LOGWEIR_ENGINE_DIGEST`, which the controller stamps from its own
//! constants and which a standalone binary on a host reads from whatever the
//! operator exported. The environment describes what someone EXPECTED the
//! image to hold; the file describes what the image DOES hold. So the file is
//! read first, and the environment only where there is no file: a standalone
//! `logweir` with an engine on `$PATH`, the e2e harness, `scripts/demo.sh`.
//!
//! The consequence is the property OD-3 asks for: a rollback image can never
//! sign under the default build's name, and the default image can never sign
//! under OSO's, whatever the controller that created the Job was compiled
//! with. Where the two disagree, a notice names both, once, on stderr; the
//! signed document names the image's.
//!
//! A FILE THAT EXISTS AND DOES NOT PARSE IS A REFUSAL, never a fall-back to the
//! environment: an image whose identity file is broken is a broken image, and
//! signing the environment's claim instead would be exactly the mislabelling
//! this file exists to prevent.

/// Where the runner image declares its engine (`Dockerfile`, runtime stage).
pub const IMAGE_IDENTITY_PATH: &str = "/etc/logweir/engine-identity";

/// The two environment variables a Job, or an operator, states the identity in.
pub const VERSION_ENV: &str = "LOGWEIR_ENGINE_VERSION";
/// See [`VERSION_ENV`].
pub const DIGEST_ENV: &str = "LOGWEIR_ENGINE_DIGEST";

/// What a signed document names as its engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineIdentity {
    /// `engine.version`: the token `kafka-backup --version` prints, e.g.
    /// `0.23.3+logweir.1` for Logweir's build or `0.23.3` for OSO's release.
    pub version: String,
    /// `engine.digest`: `sha256:<hex>`, Logweir's build-input digest or the
    /// digest of OSO's image the binary came from.
    pub digest: String,
    /// Where the pair came from, for messages.
    pub source: IdentitySource,
}

/// Where an [`EngineIdentity`] was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentitySource {
    /// [`IMAGE_IDENTITY_PATH`].
    Image,
    /// [`VERSION_ENV`] and [`DIGEST_ENV`]; either may be empty, which the
    /// signing paths refuse before anything is signed.
    Environment,
}

impl std::fmt::Display for IdentitySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentitySource::Image => write!(f, "the image's {IMAGE_IDENTITY_PATH}"),
            IdentitySource::Environment => write!(f, "{VERSION_ENV}/{DIGEST_ENV}"),
        }
    }
}

/// Parses the image's identity file: exactly the two lines `version=<v>` and
/// `digest=sha256:<64 lowercase hex>`, in that order, each once.
///
/// # Errors
/// A message naming what is wrong, for any other content.
pub fn parse_image_identity(text: &str) -> Result<(String, String), String> {
    let lines: Vec<&str> = text.lines().collect();
    let [version_line, digest_line] = lines.as_slice() else {
        return Err(format!(
            "{IMAGE_IDENTITY_PATH} must hold exactly two lines, `version=` and `digest=`; it \
             holds {}",
            lines.len()
        ));
    };
    let version = version_line
        .strip_prefix("version=")
        .filter(|v| {
            !v.is_empty()
                && v.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '-'))
        })
        .ok_or_else(|| {
            format!("{IMAGE_IDENTITY_PATH}: line 1 must be `version=<engine version>`")
        })?;
    let digest = digest_line
        .strip_prefix("digest=")
        .filter(|d| {
            d.strip_prefix("sha256:").is_some_and(|hex| {
                hex.len() == 64 && hex.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
            })
        })
        .ok_or_else(|| format!("{IMAGE_IDENTITY_PATH}: line 2 must be `digest=sha256:<hex>`"))?;
    Ok((version.to_string(), digest.to_string()))
}

/// The decision, as a pure function of what was read: `image` is the identity
/// file's content (`Ok(None)` when there is no such file), `env_version` and
/// `env_digest` the two variables (absent reads as empty).
///
/// Returns the identity and, when the environment states a DIFFERENT identity
/// from the image's, the notice naming both.
///
/// # Errors
/// The image's file exists but could not be read or does not parse.
pub fn decide(
    image: Result<Option<String>, String>,
    env_version: Option<String>,
    env_digest: Option<String>,
) -> Result<(EngineIdentity, Option<String>), String> {
    let env_version = env_version.unwrap_or_default();
    let env_digest = env_digest.unwrap_or_default();
    match image? {
        Some(text) => {
            let (version, digest) = parse_image_identity(&text)?;
            let stated = !env_version.trim().is_empty() || !env_digest.trim().is_empty();
            let notice = (stated && (env_version != version || env_digest != digest)).then(|| {
                format!(
                    "engine identity: this image carries kafka-backup {version} ({digest}), \
                     declared in {IMAGE_IDENTITY_PATH}; the environment's {VERSION_ENV}=`{env_version}` \
                     {DIGEST_ENV}=`{env_digest}` describe another engine and are not what is signed"
                )
            });
            Ok((
                EngineIdentity {
                    version,
                    digest,
                    source: IdentitySource::Image,
                },
                notice,
            ))
        }
        None => Ok((
            EngineIdentity {
                version: env_version,
                digest: env_digest,
                source: IdentitySource::Environment,
            },
            None,
        )),
    }
}

/// Reads [`IMAGE_IDENTITY_PATH`]: `Ok(None)` when it does not exist.
fn read_image_file() -> Result<Option<String>, String> {
    match std::fs::read_to_string(IMAGE_IDENTITY_PATH) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "{IMAGE_IDENTITY_PATH} exists but cannot be read: {e}"
        )),
    }
}

/// The identity this process would sign, from the real file and environment.
/// Prints the disagreement notice, if any, to stderr.
///
/// # Errors
/// See [`decide`].
pub fn resolve() -> Result<EngineIdentity, String> {
    let (identity, notice) = decide(
        read_image_file(),
        std::env::var(VERSION_ENV).ok(),
        std::env::var(DIGEST_ENV).ok(),
    )?;
    if let Some(notice) = notice {
        eprintln!("{notice}");
    }
    Ok(identity)
}

/// The identity this process DECLARES, for `doctor`: `None` when there is no
/// image file and neither variable is set — a host that has not said which
/// engine it runs.
///
/// # Errors
/// See [`decide`].
pub fn declared() -> Result<Option<EngineIdentity>, String> {
    let identity = resolve()?;
    Ok(
        (!identity.version.trim().is_empty() || !identity.digest.trim().is_empty())
            .then_some(identity),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOGWEIR_DIGEST: &str =
        "sha256:fb04aa95f2a09085018044f2a498b12b8eda6d1762d47ea3e2e61d077b908def";
    const OSO_DIGEST: &str =
        "sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db";

    fn file(version: &str, digest: &str) -> Result<Option<String>, String> {
        Ok(Some(format!("version={version}\ndigest={digest}\n")))
    }

    #[test]
    fn the_image_file_wins_over_the_environment() {
        // The rollback image under a controller compiled for Logweir's build:
        // the image's OSO identity is what is signed, and the notice names both.
        let (id, notice) = decide(
            file("0.23.3", OSO_DIGEST),
            Some("0.23.3+logweir.1".into()),
            Some(LOGWEIR_DIGEST.into()),
        )
        .unwrap();
        assert_eq!(id.version, "0.23.3");
        assert_eq!(id.digest, OSO_DIGEST);
        assert_eq!(id.source, IdentitySource::Image);
        let notice = notice.expect("a disagreement is named");
        assert!(notice.contains("0.23.3+logweir.1") && notice.contains(OSO_DIGEST));

        // And the reverse: the default image under an older controller.
        let (id, _) = decide(
            file("0.23.3+logweir.1", LOGWEIR_DIGEST),
            Some("0.23.3".into()),
            Some(OSO_DIGEST.into()),
        )
        .unwrap();
        assert_eq!(
            (id.version.as_str(), id.digest.as_str()),
            ("0.23.3+logweir.1", LOGWEIR_DIGEST)
        );
    }

    #[test]
    fn agreement_and_silence_print_nothing() {
        let (_, notice) = decide(
            file("0.23.3+logweir.1", LOGWEIR_DIGEST),
            Some("0.23.3+logweir.1".into()),
            Some(LOGWEIR_DIGEST.into()),
        )
        .unwrap();
        assert!(notice.is_none());
        let (_, notice) = decide(file("0.23.3+logweir.1", LOGWEIR_DIGEST), None, None).unwrap();
        assert!(
            notice.is_none(),
            "an unset environment is not a disagreement"
        );
    }

    #[test]
    fn without_the_file_the_environment_is_the_identity_even_empty() {
        let (id, notice) =
            decide(Ok(None), Some("0.23.3".into()), Some(OSO_DIGEST.into())).unwrap();
        assert_eq!(
            id,
            EngineIdentity {
                version: "0.23.3".into(),
                digest: OSO_DIGEST.into(),
                source: IdentitySource::Environment,
            }
        );
        assert!(notice.is_none());
        // Empty stays empty: the signing paths refuse it, exactly as before.
        let (id, _) = decide(Ok(None), None, None).unwrap();
        assert!(id.version.is_empty() && id.digest.is_empty());
    }

    #[test]
    fn a_broken_image_file_is_a_refusal_never_the_environment() {
        let env = || (Some("0.23.3".to_string()), Some(OSO_DIGEST.to_string()));
        for broken in [
            "",
            "version=0.23.3+logweir.1\n",
            &format!("digest={LOGWEIR_DIGEST}\nversion=0.23.3+logweir.1\n"),
            &format!("version=\ndigest={LOGWEIR_DIGEST}\n"),
            "version=0.23.3+logweir.1\ndigest=sha256:abc\n",
            &format!(
                "version=0.23.3+logweir.1\ndigest={}\n",
                LOGWEIR_DIGEST.to_uppercase()
            ),
            &format!("version=0.23 3\ndigest={LOGWEIR_DIGEST}\n"),
            &format!("version=0.23.3+logweir.1\ndigest={LOGWEIR_DIGEST}\nextra=1\n"),
        ] {
            let (v, d) = env();
            assert!(
                decide(Ok(Some(broken.to_string())), v, d).is_err(),
                "must refuse {broken:?}"
            );
        }
        let (v, d) = env();
        assert!(decide(Err("permission denied".into()), v, d).is_err());
    }
}
