//! THE engine-binary resolution. One function, consulted by `logweir doctor`
//! and by `logweir drill run`, so a green `doctor` and the drill it green-lit
//! cannot disagree about which binary they mean.
//!
//! THE DEFECT THIS EXISTS FOR. `doctor` resolved
//! `$LOGWEIR_ENGINE_BIN` -> `/usr/local/bin/kafka-backup` -> a `$PATH` scan.
//! `drill run` resolved `$LOGWEIR_ENGINE_BIN` -> the literal
//! `.engine/kafka-backup`, and `Command::new` performs NO `$PATH` search for a
//! path containing `/`. `README.md` and `docs/quickstart.md` both tell an
//! adopter that the engine on `$PATH` is enough. So the documented install
//! produced a `doctor` run printing `ok engine present` and `ok engine
//! version`, followed by a `drill run` that read the target cluster and the
//! archive manifest through phases 0-4 and then died at phase 5 the first time
//! it tried to execute an engine that, as far as it was concerned, was not
//! there. Two resolutions, one of them documented, neither of them shared.
//!
//! The `.engine/kafka-backup` entry is kept — it is where `just engine` and
//! `scripts/extract-engine.sh` put the extracted binary, and dropping it would
//! break every contributor's local drill — but it is now in the SAME chain
//! `doctor` walks, so `doctor` finds it too. Before this, a contributor with a
//! working `.engine/kafka-backup` was told by `doctor` that they had no engine
//! at all: the disagreement ran in both directions.
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

/// The name the engine is installed under, everywhere.
pub const ENGINE_NAME: &str = "kafka-backup";

/// The container image's location, set as `LOGWEIR_ENGINE_BIN` by the
/// `Dockerfile` and named by `docs/quickstart.md` and `docs/kubernetes.md`.
const WELL_KNOWN: &str = "/usr/local/bin/kafka-backup";

/// Where `just engine` / `scripts/extract-engine.sh` put the extracted,
/// digest-pinned binary in a checkout. Relative to the current directory, as
/// it always was.
const REPO_LOCAL: &str = ".engine/kafka-backup";

/// Resolve the engine, reading the real process environment.
///
/// Order: `$LOGWEIR_ENGINE_BIN` (an explicit override always wins, including
/// when it points at nothing — an operator who names a path deserves to be
/// told about THAT path, not about a fallback they did not ask for), then the
/// repo-local extract, then the image's well-known location, then a `$PATH`
/// scan, and finally the bare name so a failure message still says what was
/// being looked for.
pub fn engine_path() -> PathBuf {
    resolve(
        std::env::var_os("LOGWEIR_ENGINE_BIN"),
        std::env::var_os("PATH").as_deref(),
    )
}

/// The resolution as a function of its inputs, so it is testable without
/// touching this process's environment (`std::env::set_var` on a shared test
/// binary races every other test that reads `PATH`).
pub fn resolve(env_bin: Option<OsString>, path_var: Option<&OsStr>) -> PathBuf {
    if let Some(p) = env_bin.filter(|p| !p.is_empty()) {
        return PathBuf::from(p);
    }
    for candidate in [REPO_LOCAL, WELL_KNOWN] {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return p;
        }
    }
    if let Some(found) = path_var.and_then(|v| find_on_path(v, ENGINE_NAME)) {
        return found;
    }
    // Nothing resolved. The BARE name, so the failure message names what was
    // looked for — and `.exists()` on it correctly reports absent, because it
    // is no longer being asked to resolve `$PATH` itself.
    PathBuf::from(ENGINE_NAME)
}

/// The `$PATH`-search logic, as a pure function of an explicit `PATH`-like
/// value. Mirrors what `execve` does for a bare command name: scan each
/// directory in order, return the first entry that exists and is executable.
pub fn find_on_path(path_var: &OsStr, name: &str) -> Option<PathBuf> {
    std::env::split_paths(path_var).find_map(|dir| {
        let candidate = dir.join(name);
        if is_executable(&candidate) {
            Some(candidate)
        } else {
            None
        }
    })
}

#[cfg(unix)]
fn is_executable(p: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.is_file() && (m.permissions().mode() & 0o111) != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(p: &std::path::Path) -> bool {
    p.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path_of(dirs: &[&std::path::Path]) -> OsString {
        std::env::join_paths(dirs.iter().copied()).unwrap()
    }

    fn exe_in(dir: &std::path::Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let exe = dir.join(ENGINE_NAME);
        std::fs::write(&exe, "#!/bin/sh\necho hi\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        exe
    }

    /// Moved here from `doctor::tests` when the resolution was unified: a bare
    /// name on `$PATH`, not in the current directory and not in
    /// `/usr/local/bin`, must resolve. Uses a synthetic `PATH`-like value (two
    /// tempdirs) rather than the real process environment.
    #[test]
    fn find_on_path_resolves_a_bare_name_through_path_entries_in_order() {
        let empty_dir = tempfile::tempdir().unwrap();
        let bin_dir = tempfile::tempdir().unwrap();
        let exe = exe_in(bin_dir.path());
        let path_var = path_of(&[empty_dir.path(), bin_dir.path()]);
        assert_eq!(find_on_path(&path_var, ENGINE_NAME), Some(exe));
    }

    #[test]
    fn find_on_path_skips_a_non_executable_candidate() {
        let dir = tempfile::tempdir().unwrap();
        // `fs::write` does not set the executable bit.
        std::fs::write(dir.path().join(ENGINE_NAME), "not executable").unwrap();
        let path_var = path_of(&[dir.path()]);
        assert_eq!(find_on_path(&path_var, ENGINE_NAME), None);
    }

    #[test]
    fn find_on_path_returns_none_when_nothing_matches() {
        let dir = tempfile::tempdir().unwrap();
        let path_var = path_of(&[dir.path()]);
        assert_eq!(find_on_path(&path_var, ENGINE_NAME), None);
    }

    /// THE FINDING, as a unit: an engine installed only on `$PATH` — the
    /// install `README.md` documents — must resolve to the file on `$PATH`.
    /// The drill's old default, the literal `.engine/kafka-backup`, is not a
    /// `$PATH` lookup at all: `Command::new` on a path containing `/` never
    /// searches.
    #[test]
    fn an_engine_installed_only_on_path_resolves_to_the_file_on_path() {
        let bin_dir = tempfile::tempdir().unwrap();
        let exe = exe_in(bin_dir.path());
        let path_var = path_of(&[bin_dir.path()]);
        let got = resolve(None, Some(&path_var));
        assert_eq!(
            got, exe,
            "an engine on $PATH must resolve to it; `{}` is what `drill run` used \
             to return here, and `Command::new` does not search $PATH for a path \
             containing a slash",
            REPO_LOCAL
        );
    }

    #[test]
    fn an_explicit_override_wins_over_everything_including_path() {
        let bin_dir = tempfile::tempdir().unwrap();
        exe_in(bin_dir.path());
        let path_var = path_of(&[bin_dir.path()]);
        assert_eq!(
            resolve(
                Some(OsString::from("/opt/custom/kafka-backup")),
                Some(&path_var)
            ),
            PathBuf::from("/opt/custom/kafka-backup")
        );
    }

    /// An empty override is not an override. `env: [{name: LOGWEIR_ENGINE_BIN,
    /// value: ""}]` in a Helm values file is a real way to end up here, and
    /// `Command::new("")` fails with a message that names nothing.
    #[test]
    fn an_empty_override_falls_through_rather_than_resolving_to_nothing() {
        let bin_dir = tempfile::tempdir().unwrap();
        let exe = exe_in(bin_dir.path());
        let path_var = path_of(&[bin_dir.path()]);
        assert_eq!(resolve(Some(OsString::new()), Some(&path_var)), exe);
    }

    #[test]
    fn nothing_anywhere_yields_the_bare_name_so_the_message_can_say_what_was_sought() {
        let empty = tempfile::tempdir().unwrap();
        let path_var = path_of(&[empty.path()]);
        assert_eq!(resolve(None, Some(&path_var)), PathBuf::from(ENGINE_NAME));
    }
}
