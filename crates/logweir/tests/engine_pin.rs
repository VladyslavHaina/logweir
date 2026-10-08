//! **The engine pin is ONE version, and everything that names it agrees.**
//!
//! PROD-00.3f moved the pin from `kafka-backup` 0.21.0 to 0.23.3
//! (`docs/to-do/decisions/PROD-00-engine-route.md` section 12). A bump touches
//! nine places, and a bump that misses one is quiet:
//!
//! | place | what goes wrong when it lags |
//! |---|---|
//! | `logweir::doctor::ENGINE_PIN` | `doctor` refuses the shipped engine, or accepts the old one |
//! | `scripts/extract-engine.sh` `TAG` | a refresh re-pins the old tag |
//! | the one `third_party/kafka-backup-v*.tar.gz` and its `.sha256` | the drift gate checks the vendored shapes against another engine's source |
//! | `weirkeeper::job::ENGINE_VERSION` / `ENGINE_DIGEST` | every signed receipt the controller's Jobs write names the wrong engine |
//! | the `Dockerfile` engine stage | the image ships another engine than the one the digest file names |
//! | PROD-01.1's `CONTRACT_ENGINE` (`record_semantics.rs`) | every contract row on the new engine records its outcome, asserts NOTHING, and stays green (A-3f-1) |
//! | `e2e/fixtures/fake-engine-ok.sh` | `doctor`'s own tests stop exercising the accepting path |
//! | `examples/cronjob-drill.yaml` and `docs/quickstart.md`'s `export` lines | an adopter's scheduled drill signs scorecards naming an engine its image does not carry |
//!
//! So this file reads each of them and compares it with `ENGINE_PIN`. It is a
//! text scan in the default test set: no Docker, no network, no stack. Every
//! comparison is a pure function over the text it is given, and each one is
//! also run over a planted lagging copy (the old `0.21.0` pin), which it must
//! refuse: a guard that cannot fail is not a guard.
//!
//! `CONTRACT_ENGINE` stays a literal rather than a reference to the pin on
//! purpose: the contract is what was MEASURED on one engine, and it moves only
//! after its rows have been re-run on the new one. This test is what makes
//! that re-run impossible to forget.
//!
//! **WHY IT LIVES IN `logweir`'s TESTS AND NEVER IN THE `e2e` PACKAGE.**
//! `third_party/kafka-backup-binary.digest` is the pin only in a checked-out
//! tree. Each `engine-matrix` row overwrites it, and `.engine/`, with ITS
//! engine (`.github/workflows/engine-matrix.yml`, "Extract this engine and
//! point the stack at it") and then runs `cargo test -p e2e --features e2e`.
//! This file first lived at `e2e/tests/engine_pin.rs`, and run 37728540932
//! recorded the floor row (v0.21.0) and the operator-default row (v0.22.0)
//! as `fail(e2e suite)`: the guard failed on the row's digest and cargo
//! stopped before a single drill suite ran (review H1). CI's workspace job
//! runs this crate's tests on the committed tree; the matrix never runs them.
//! `crates/logweir/tests/engine_matrix.rs`'s
//! `no_e2e_test_compares_the_engine_with_a_committed_pin` keeps every pin
//! statement out of `e2e/tests/`.

use std::path::{Path, PathBuf};

use logweir::doctor::ENGINE_PIN;

/// The pin before PROD-00.3f: every planted negative control lags to it.
const OLD_PIN: &str = "0.21.0";
const OLD_DIGEST: &str = "sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// The value of the first `<prefix>"<value>"…` on a line of `src`.
fn quoted_after(src: &str, prefix: &str) -> Option<String> {
    src.lines().find_map(|l| {
        let rest = l.trim_start().strip_prefix(prefix)?;
        let rest = rest.strip_prefix('"')?;
        Some(rest[..rest.find('"')?].to_string())
    })
}

/// `scripts/extract-engine.sh`: `TAG="${OSO_TAG:-v<pin>}"`.
fn check_extract_script(src: &str, pin: &str) -> Result<(), String> {
    let tag = src
        .lines()
        .find_map(|l| l.strip_prefix("TAG=\"${OSO_TAG:-"))
        .and_then(|l| l.strip_suffix("}\""))
        .ok_or("extract-engine.sh names no default TAG")?;
    if tag == format!("v{pin}") {
        Ok(())
    } else {
        Err(format!("extract-engine.sh pins {tag}, the pin is v{pin}"))
    }
}

/// `third_party/`: exactly one source tarball, named for the pin, whose
/// `.sha256` names it and matches its bytes.
fn check_third_party(names: &[String], pin: &str) -> Result<(), String> {
    let tarballs: Vec<&String> = names
        .iter()
        .filter(|n| n.starts_with("kafka-backup-v") && n.ends_with(".tar.gz"))
        .collect();
    match tarballs.as_slice() {
        [one] if **one == format!("kafka-backup-v{pin}.tar.gz") => Ok(()),
        [one] => Err(format!("third_party/ vendors {one}, the pin is {pin}")),
        many => Err(format!(
            "third_party/ must vendor exactly one engine tarball, found {many:?}"
        )),
    }
}

fn check_tarball_checksum(sha_file: &str, tarball: &str, bytes: &[u8]) -> Result<(), String> {
    let want = format!(
        "{}  third_party/{tarball}",
        logweir_core::ids::sha256_hex(bytes)
    );
    if sha_file.trim() == want {
        Ok(())
    } else {
        Err(format!(
            "third_party/{tarball}.sha256 reads `{}`, the bytes give `{want}`",
            sha_file.trim()
        ))
    }
}

/// `crates/weirkeeper/src/job.rs`: the version and digest every runner Job
/// stamps into `LOGWEIR_ENGINE_VERSION` / `LOGWEIR_ENGINE_DIGEST`.
fn check_job_constants(src: &str, pin: &str, digest: &str) -> Result<(), String> {
    let version = quoted_after(src, "pub const ENGINE_VERSION: &str = ")
        .ok_or("job.rs: no ENGINE_VERSION")?;
    // `ENGINE_DIGEST` is formatted over two lines by rustfmt: take the first
    // string literal after its name.
    let at = src
        .find("pub const ENGINE_DIGEST: &str =")
        .ok_or("job.rs: no ENGINE_DIGEST")?;
    let job_digest = src[at..]
        .split('"')
        .nth(1)
        .ok_or("job.rs: ENGINE_DIGEST holds no literal")?
        .to_string();
    if version != pin {
        return Err(format!(
            "weirkeeper::job::ENGINE_VERSION is {version}, the pin is {pin}"
        ));
    }
    if job_digest != digest {
        return Err(format!(
            "weirkeeper::job::ENGINE_DIGEST is {job_digest}, the digest file holds {digest}"
        ));
    }
    Ok(())
}

/// The `Dockerfile` engine stage pulls the digest file's digest.
fn check_dockerfile(src: &str, digest: &str) -> Result<(), String> {
    let line = format!("FROM osodevops/kafka-backup@{digest} AS engine");
    if src.lines().any(|l| l.trim() == line) {
        Ok(())
    } else {
        Err(format!("the Dockerfile has no line `{line}`"))
    }
}

/// PROD-01.1's `CONTRACT_ENGINE` (A-3f-1).
fn check_contract_engine(src: &str, pin: &str) -> Result<(), String> {
    let v = quoted_after(src, "const CONTRACT_ENGINE: &str = ")
        .ok_or("record_semantics.rs declares no CONTRACT_ENGINE literal")?;
    if v == pin {
        Ok(())
    } else {
        Err(format!(
            "CONTRACT_ENGINE is {v} but the pin is {pin}: every record-semantics row on the \
             pinned engine would record its outcome and assert nothing. Re-run PROD-01.1's rows \
             on {pin}, record any difference in its decision record, then move the constant"
        ))
    }
}

/// `e2e/fixtures/fake-engine-ok.sh` prints the pin, so `doctor`'s accepting
/// path is what its tests exercise.
fn check_fake_engine(src: &str, pin: &str) -> Result<(), String> {
    let want = format!("echo \"kafka-backup {pin}\"");
    if src.lines().any(|l| l.trim() == want) {
        Ok(())
    } else {
        Err(format!("fake-engine-ok.sh does not `{want}`"))
    }
}

/// The adopter-facing statements of the engine identity a standalone run must
/// export: `LOGWEIR_ENGINE_VERSION` and `LOGWEIR_ENGINE_DIGEST`, as the
/// CronJob example's `value:` lines and as the quickstart's `export` lines.
fn check_documented_identity(src: &str, pin: &str, digest: &str) -> Result<(), String> {
    let mut version_seen = false;
    let mut digest_seen = false;
    let lines: Vec<&str> = src.lines().map(str::trim).collect();
    for (i, line) in lines.iter().enumerate() {
        for (name, want, seen) in [
            ("LOGWEIR_ENGINE_VERSION", pin, &mut version_seen),
            ("LOGWEIR_ENGINE_DIGEST", digest, &mut digest_seen),
        ] {
            let value = if let Some(v) = line.strip_prefix(&format!("export {name}=")) {
                Some(v.to_string())
            } else if *line == format!("- name: {name}") {
                lines
                    .get(i + 1)
                    .and_then(|next| next.strip_prefix("value: "))
                    .map(|v| v.trim_matches('"').to_string())
            } else {
                None
            };
            if let Some(v) = value {
                if v != want {
                    return Err(format!("{name} is documented as {v}, the pin is {want}"));
                }
                *seen = true;
            }
        }
    }
    if version_seen && digest_seen {
        Ok(())
    } else {
        Err("the engine version or digest is not stated".to_string())
    }
}

fn digest_file() -> String {
    read("third_party/kafka-backup-binary.digest")
        .trim()
        .to_string()
}

fn third_party_names() -> Vec<String> {
    std::fs::read_dir(root().join("third_party"))
        .expect("third_party/ exists")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn the_pin_is_the_one_prod_00_3f_evaluated() {
    // Moving the pin is a decision with a record (section 12 of the engine
    // route); this line makes the next bump touch this file too.
    assert_eq!(ENGINE_PIN, "0.23.3");
    assert_eq!(
        digest_file(),
        "sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db",
        "the digest PROD-00.3f resolved for v0.23.3 (revision afb160e7)"
    );
}

#[test]
fn extract_engine_pins_the_same_tag() {
    check_extract_script(&read("scripts/extract-engine.sh"), ENGINE_PIN).unwrap();
}

#[test]
fn third_party_vendors_exactly_the_pinned_source() {
    let names = third_party_names();
    check_third_party(&names, ENGINE_PIN).unwrap();
    let tarball = format!("kafka-backup-v{ENGINE_PIN}.tar.gz");
    let bytes = std::fs::read(root().join("third_party").join(&tarball)).unwrap();
    check_tarball_checksum(
        &read(&format!("third_party/{tarball}.sha256")),
        &tarball,
        &bytes,
    )
    .unwrap();
}

#[test]
fn the_controllers_job_constants_name_the_pin() {
    check_job_constants(
        &read("crates/weirkeeper/src/job.rs"),
        ENGINE_PIN,
        &digest_file(),
    )
    .unwrap();
}

#[test]
fn the_dockerfile_pulls_the_pinned_digest() {
    check_dockerfile(&read("Dockerfile"), &digest_file()).unwrap();
}

/// A-3f-1.
#[test]
fn the_record_semantics_contract_is_stated_for_the_pinned_engine() {
    check_contract_engine(&read("e2e/tests/record_semantics.rs"), ENGINE_PIN).unwrap();
}

#[test]
fn doctors_accepting_fixture_prints_the_pin() {
    check_fake_engine(&read("e2e/fixtures/fake-engine-ok.sh"), ENGINE_PIN).unwrap();
}

#[test]
fn the_documented_standalone_identity_names_the_pin() {
    for file in ["examples/cronjob-drill.yaml", "docs/quickstart.md"] {
        check_documented_identity(&read(file), ENGINE_PIN, &digest_file())
            .unwrap_or_else(|e| panic!("{file}: {e}"));
    }
}

/// THE NEGATIVE CONTROLS: each check above, over a copy of the real file that
/// lags to the old pin, refuses. A-3f-1's own control is the first one: the
/// old `CONTRACT_ENGINE` on the new pin fails.
#[test]
fn every_check_refuses_a_copy_that_lags_to_the_old_pin() {
    let digest = digest_file();
    let contract = read("e2e/tests/record_semantics.rs").replace(
        &format!("const CONTRACT_ENGINE: &str = \"{ENGINE_PIN}\";"),
        &format!("const CONTRACT_ENGINE: &str = \"{OLD_PIN}\";"),
    );
    let e = check_contract_engine(&contract, ENGINE_PIN).unwrap_err();
    assert!(e.contains("assert nothing"), "{e}");
    assert!(check_contract_engine("// no constant at all", ENGINE_PIN).is_err());

    let script = read("scripts/extract-engine.sh").replace(
        &format!("TAG=\"${{OSO_TAG:-v{ENGINE_PIN}}}\""),
        &format!("TAG=\"${{OSO_TAG:-v{OLD_PIN}}}\""),
    );
    assert!(check_extract_script(&script, ENGINE_PIN).is_err());

    let mut names = third_party_names();
    names.push(format!("kafka-backup-v{OLD_PIN}.tar.gz"));
    assert!(
        check_third_party(&names, ENGINE_PIN).is_err(),
        "two tarballs"
    );
    let lagging: Vec<String> = third_party_names()
        .into_iter()
        .map(|n| n.replace(ENGINE_PIN, OLD_PIN))
        .collect();
    assert!(
        check_third_party(&lagging, ENGINE_PIN).is_err(),
        "the old one"
    );
    assert!(check_third_party(&[], ENGINE_PIN).is_err(), "none");
    let tarball = format!("kafka-backup-v{ENGINE_PIN}.tar.gz");
    assert!(
        check_tarball_checksum(
            &read(&format!("third_party/{tarball}.sha256")),
            &tarball,
            b"other bytes"
        )
        .is_err(),
        "a checksum that does not describe the bytes"
    );

    let job = read("crates/weirkeeper/src/job.rs");
    let job_old_version = job.replace(
        &format!("pub const ENGINE_VERSION: &str = \"{ENGINE_PIN}\";"),
        &format!("pub const ENGINE_VERSION: &str = \"{OLD_PIN}\";"),
    );
    assert!(check_job_constants(&job_old_version, ENGINE_PIN, &digest).is_err());
    let job_old_digest = job.replace(&digest, OLD_DIGEST);
    assert!(check_job_constants(&job_old_digest, ENGINE_PIN, &digest).is_err());

    let dockerfile = read("Dockerfile").replace(&digest, OLD_DIGEST);
    assert!(check_dockerfile(&dockerfile, &digest).is_err());

    let fake = read("e2e/fixtures/fake-engine-ok.sh").replace(ENGINE_PIN, OLD_PIN);
    assert!(check_fake_engine(&fake, ENGINE_PIN).is_err());

    for file in ["examples/cronjob-drill.yaml", "docs/quickstart.md"] {
        let text = read(file);
        let old_version = text
            .replace(&format!("=\"{ENGINE_PIN}\""), &format!("=\"{OLD_PIN}\""))
            .replace(&format!("\"{ENGINE_PIN}\""), &format!("\"{OLD_PIN}\""))
            .replace(
                &format!("LOGWEIR_ENGINE_VERSION={ENGINE_PIN}"),
                &format!("LOGWEIR_ENGINE_VERSION={OLD_PIN}"),
            );
        assert!(
            check_documented_identity(&old_version, ENGINE_PIN, &digest).is_err(),
            "{file}: the old version"
        );
        let old_digest = text.replace(&digest, OLD_DIGEST);
        assert!(
            check_documented_identity(&old_digest, ENGINE_PIN, &digest).is_err(),
            "{file}: the old digest"
        );
    }
    assert!(check_documented_identity("nothing here", ENGINE_PIN, &digest).is_err());
}
