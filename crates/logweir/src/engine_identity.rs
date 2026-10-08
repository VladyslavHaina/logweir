//! THE engine identity Logweir signs: `engine.version` and `engine.digest` in
//! every drill scorecard and every backup receipt (PROD-00.2, OD-3).
//!
//! TWO SOURCES, AND THE IMAGE WINS. The runner image is built with one engine
//! — Logweir's build of the vendored OSO source by default, or OSO's released
//! binary under the documented one-release rollback — and the `Dockerfile`
//! writes WHICH one into the image at [`IMAGE_IDENTITY_PATH`], copied from the
//! same build stage as the binary. `LOGWEIR_ENGINE_VERSION` and
//! `LOGWEIR_ENGINE_DIGEST` are what a standalone binary on a host reads from
//! whatever the operator exported; since PROD-00.2's review the controller
//! never puts them in a Job. The environment describes what someone EXPECTED
//! the engine to be; the file describes what the image DOES hold. So the file
//! is read first, and the environment only where there is no file: a
//! standalone `logweir` with an engine on `$PATH`, the e2e harness,
//! `scripts/demo.sh`. And either way the run asks the binary itself:
//! [`verify_engine_reports`] refuses a version it does not print.
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
    /// `0.23.3+logweir.2` for Logweir's build or `0.23.3` for OSO's release.
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
    read_identity_file(std::path::Path::new(IMAGE_IDENTITY_PATH))
}

/// [`IMAGE_IDENTITY_PATH`], or another path for a test: `Ok(None)` when it
/// does not exist, an error when it exists and cannot be read.
fn read_identity_file(path: &std::path::Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{} exists but cannot be read: {e}", path.display())),
    }
}

/// How long the engine may take to print its version: a container shim starts
/// a container for it.
const VERSION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// **The identity is held to the binary that runs** (PROD-00.2 review L3/L4).
/// Runs `<binary> --version` once — a flag that prints a string and acts on no
/// cluster or bucket (ruling GR8) — and refuses unless one whitespace token of
/// the output is exactly `version`, the `engine.version` this run would sign.
/// So neither a Job's environment nor an image's declaration can name an
/// engine other than the one executed: an older runner image under a newer
/// controller, `LOGWEIR_ENGINE_BIN` pointed at another binary, or a rollback
/// image whose declaration is wrong all refuse before anything is signed.
///
/// # Errors
/// The binary cannot be run, does not finish within a minute, exits non-zero,
/// or does not print `version` as a whole token.
pub fn verify_engine_reports(engine: &std::path::Path, version: &str) -> Result<(), String> {
    use std::io::Read;
    // `--version` is a flag, not an engine subcommand: it prints a string and
    // acts on no cluster or bucket (ruling GR8), as `doctor`'s own probe does.
    let mut child = std::process::Command::new(engine)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            format!(
                "the engine at {} could not be run for --version: {e}",
                engine.display()
            )
        })?;
    let deadline = std::time::Instant::now() + VERSION_PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "the engine at {} did not print its version within {}s",
                    engine.display(),
                    VERSION_PROBE_TIMEOUT.as_secs()
                ));
            }
            Err(e) => {
                return Err(format!("waiting for {} --version: {e}", engine.display()));
            }
        }
    };
    let mut out = String::new();
    if let Some(mut s) = child.stdout.take() {
        let _ = s.read_to_string(&mut out);
    }
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut out);
    }
    check_reported(engine, status.success(), &out, version)
}

/// The decision of [`verify_engine_reports`], pure.
fn check_reported(
    binary: &std::path::Path,
    ran: bool,
    output: &str,
    version: &str,
) -> Result<(), String> {
    if !ran {
        return Err(format!(
            "the engine at {} failed to print its version: `{}`",
            binary.display(),
            output.trim()
        ));
    }
    if output
        .split_ascii_whitespace()
        .any(|token| token == version)
    {
        Ok(())
    } else {
        Err(format!(
            "the engine at {} reports `{}`, but this run would sign engine.version `{version}`: \
             refusing to sign a document that names an engine other than the one that runs. \
             The runner image declares its engine in {IMAGE_IDENTITY_PATH}; roll the controller \
             and the runner image together, and roll the engine back only with the rollback \
             image (docs/install.md, \"Rolling the engine back\")",
            binary.display(),
            output.trim()
        ))
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
        "sha256:6385b2d3aecb9d107010b14362bb60db756e6774b2181cd2273d7c6f92ed9af3";
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

    /// REVIEW L6 (mutant R3): an identity file that EXISTS but cannot be read
    /// is an error, never "no file" (which would fall back to the
    /// environment). A directory at the path is unreadable as a file on every
    /// platform and under any privilege.
    #[test]
    fn an_unreadable_identity_file_is_an_error_and_a_missing_one_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let unreadable = dir.path().join("engine-identity");
        std::fs::create_dir(&unreadable).unwrap();
        let e = read_identity_file(&unreadable).unwrap_err();
        assert!(e.contains("exists but cannot be read"), "{e}");
        assert_eq!(read_identity_file(&dir.path().join("absent")), Ok(None));
        let present = dir.path().join("present");
        std::fs::write(&present, "version=1\n").unwrap();
        assert_eq!(read_identity_file(&present), Ok(Some("version=1\n".into())));
    }

    /// REVIEW L3/L4: the version signed must be one the engine prints.
    #[test]
    fn the_signed_version_must_be_the_one_the_engine_prints() {
        let bin = std::path::Path::new("/opt/engine/kafka-backup");
        check_reported(
            bin,
            true,
            "kafka-backup 0.23.3+logweir.1\n",
            "0.23.3+logweir.1",
        )
        .unwrap();
        // An older runner image (OSO's 0.23.3) under a controller naming
        // Logweir's build, the reverse, and a near miss.
        for (prints, signs) in [
            ("kafka-backup 0.23.3", "0.23.3+logweir.1"),
            ("kafka-backup 0.23.3+logweir.1", "0.23.3"),
            ("kafka-backup 0.23.3+logweir.10", "0.23.3+logweir.1"),
        ] {
            let e = check_reported(bin, true, prints, signs).unwrap_err();
            assert!(e.contains("refusing to sign"), "{prints} as {signs}: {e}");
        }
        assert!(check_reported(
            bin,
            false,
            "kafka-backup 0.23.3+logweir.1",
            "0.23.3+logweir.1"
        )
        .is_err());
    }

    /// The probe runs a real process: a script that prints a version passes
    /// when it is the signed one and refuses otherwise.
    #[test]
    fn verify_engine_reports_runs_the_binary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("kafka-backup");
        std::fs::write(&exe, "#!/bin/sh\necho 'kafka-backup 0.23.3+logweir.1'\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        verify_engine_reports(&exe, "0.23.3+logweir.1").unwrap();
        assert!(verify_engine_reports(&exe, "0.23.3").is_err());
        assert!(verify_engine_reports(&dir.path().join("absent"), "0.23.3+logweir.1").is_err());
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
