//! **Logweir's build of the engine: the inputs, the patch folder's policy and
//! the identity** (PROD-00.2, OD-3).
//!
//! `scripts/engine-source.sh` is the recipe the `Dockerfile`'s
//! `engine-logweir` stage runs, and its refusals are the build's gate. This
//! file states the same rules a second time, in Rust, and runs the script over
//! planted trees, so each rule is held by two implementations that must agree:
//!
//! - **The digest.** `ENGINE_DIGEST` in `third_party/kafka-backup-build.env`
//!   is the sha256 of `logweir-engine-inputs/v1`, the tarball's name and
//!   sha256, every patch's name and sha256 in order, and `ENGINE_VERSION`.
//!   A change to any input without a new recorded digest is refused.
//! - **The version.** `<release>+logweir.<n>`, `n` from 1, the release the one
//!   vendored tarball's name gives.
//! - **One version, one engine** (review M1). `third_party/kafka-backup-builds.txt`
//!   is the append-only ledger of builds: the last line is the build env's
//!   pair, no version and no digest appears twice, and `n` rises within a
//!   release. A new digest under an old version is refused whichever way it is
//!   written down; a rewritten shipped line is `engine_pin.rs`'s to refuse.
//! - **The patch folder** (`third_party/kafka-backup-patches/README.md`):
//!   `NNNN-<slug>.patch` files, uniquely numbered, line 1 `Reason: <one
//!   line>`, line 2 blank or `Upstream: https://…`, then a unified diff; the
//!   README and nothing else beside them.
//!
//! Every rule has a planted negative control that BOTH implementations must
//! refuse, and a planted positive control both must accept — the second is
//! what proves the first is not refusing everything.

use std::path::{Path, PathBuf};
use std::process::Command;

use logweir_core::ids::sha256_hex;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

fn env_value(src: &str, key: &str) -> Result<String, String> {
    let values: Vec<&str> = src
        .lines()
        .filter_map(|l| l.strip_prefix(&format!("{key}=")))
        .collect();
    match values.as_slice() {
        [one] => Ok(one.to_string()),
        _ => Err(format!(
            "kafka-backup-build.env must set {key} exactly once"
        )),
    }
}

/// `<release>+logweir.<n>` with `n >= 1`; returns the release.
fn release_of(version: &str) -> Result<&str, String> {
    let (release, n) = version
        .split_once("+logweir.")
        .ok_or_else(|| format!("ENGINE_VERSION `{version}` is not <release>+logweir.<n>"))?;
    let parts: Vec<&str> = release.split('.').collect();
    let release_ok = parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    let n_ok = !n.is_empty() && !n.starts_with('0') && n.bytes().all(|b| b.is_ascii_digit());
    if release_ok && n_ok {
        Ok(release)
    } else {
        Err(format!(
            "ENGINE_VERSION `{version}` is not <release>+logweir.<n>, n from 1"
        ))
    }
}

/// The patch folder's format, over `(file name, content)` pairs. Returns the
/// patch names in the order the build applies them.
fn check_patch_folder(entries: &[(String, Vec<u8>)]) -> Result<Vec<String>, String> {
    let mut names: Vec<String> = Vec::new();
    let mut numbers: Vec<String> = Vec::new();
    let mut readme = false;
    let mut sorted: Vec<&(String, Vec<u8>)> = entries.iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    for (name, bytes) in sorted {
        if name == "README.md" {
            readme = !bytes.is_empty();
            continue;
        }
        let shape_ok = name.len() > "0000-x.patch".len() - 1
            && name.ends_with(".patch")
            && name.as_bytes()[..4].iter().all(u8::is_ascii_digit)
            && name.as_bytes()[4] == b'-'
            && {
                let slug = &name[5..name.len() - ".patch".len()];
                !slug.is_empty()
                    && slug
                        .bytes()
                        .next()
                        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    && slug
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            };
        if !shape_ok {
            return Err(format!(
                "{name} is not NNNN-<slug>.patch; nothing else lives in the patch folder"
            ));
        }
        let number = name[..4].to_string();
        if numbers.contains(&number) {
            return Err(format!("two patches are numbered {number}"));
        }
        numbers.push(number);
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.lines().collect();
        let reason_ok = lines.first().is_some_and(|l| {
            l.strip_prefix("Reason: ")
                .is_some_and(|r| r.chars().next().is_some_and(|c| !c.is_whitespace()))
        });
        if !reason_ok {
            return Err(format!("{name}: line 1 must be `Reason: <one line>`"));
        }
        let line2 = lines.get(1).copied().unwrap_or("");
        if !line2.is_empty() {
            let url_ok = line2
                .strip_prefix("Upstream: https://")
                .is_some_and(|rest| !rest.is_empty() && !rest.chars().any(char::is_whitespace));
            if !url_ok {
                return Err(format!(
                    "{name}: line 2 must be blank or `Upstream: https://…`"
                ));
            }
            if !lines.get(2).copied().unwrap_or("").is_empty() {
                return Err(format!("{name}: line 3 must be blank, before the diff"));
            }
        }
        if !lines.iter().any(|l| l.starts_with("+++ b/")) {
            return Err(format!("{name} carries no unified diff"));
        }
        names.push(name.clone());
    }
    if !readme {
        return Err("the patch folder's README.md, which states the policy, is missing".into());
    }
    Ok(names)
}

/// The ledger's rules, as `scripts/engine-source.sh` states them.
fn check_ledger(ledger: &str, version: &str, digest: &str) -> Result<(), String> {
    let mut seen: Vec<(String, u64, String)> = Vec::new();
    for line in ledger.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (v, d) = line
            .split_once(' ')
            .ok_or_else(|| format!("`{line}` is not `<release>+logweir.<n> sha256:<hex>`"))?;
        let release = release_of(v)?;
        let n: u64 = v.rsplit_once('.').unwrap().1.parse().unwrap();
        let digest_ok = d.strip_prefix("sha256:").is_some_and(|h| {
            h.len() == 64 && h.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        });
        if !digest_ok {
            return Err(format!(
                "`{line}` is not `<release>+logweir.<n> sha256:<hex>`"
            ));
        }
        if seen.iter().any(|(sv, _, _)| sv == v) {
            return Err(format!("version {v} is recorded twice: bump the <n>"));
        }
        if seen.iter().any(|(_, _, sd)| sd == d) {
            return Err(format!("digest {d} is recorded twice"));
        }
        if seen
            .iter()
            .any(|(sv, sn, _)| release_of(sv).unwrap() == release && *sn >= n)
        {
            return Err(format!("{v}: <n> must rise within release {release}"));
        }
        seen.push((v.to_string(), n, d.to_string()));
    }
    match seen.last() {
        Some((v, _, d)) if v == version && d == digest => Ok(()),
        Some((v, _, d)) => Err(format!(
            "the ledger ends with `{v} {d}`, the build env builds `{version} {digest}`: bump \
             the <n> and append"
        )),
        None => Err("the ledger records no build".into()),
    }
}

/// A tree with the build's inputs: `third_party/` as the script reads it.
struct Inputs {
    env: String,
    ledger: String,
    tarball_name: String,
    tarball: Vec<u8>,
    checksum: String,
    patches: Vec<(String, Vec<u8>)>,
}

impl Inputs {
    fn real() -> Self {
        let env = read("third_party/kafka-backup-build.env");
        let version = env_value(&env, "ENGINE_VERSION").unwrap();
        let tarball_name = format!("kafka-backup-v{}.tar.gz", release_of(&version).unwrap());
        let tarball = std::fs::read(root().join("third_party").join(&tarball_name)).unwrap();
        let checksum = read(&format!("third_party/{tarball_name}.sha256"));
        let dir = root().join("third_party/kafka-backup-patches");
        let patches = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.file_name().to_string_lossy().into_owned(),
                    std::fs::read(e.path()).unwrap(),
                )
            })
            .collect();
        Inputs {
            env,
            ledger: read("third_party/kafka-backup-builds.txt"),
            tarball_name,
            tarball,
            checksum,
            patches,
        }
    }

    /// The digest the inputs give, by the definition in the module header.
    fn digest(&self) -> Result<String, String> {
        let version = env_value(&self.env, "ENGINE_VERSION")?;
        release_of(&version)?;
        let mut text = String::from("logweir-engine-inputs/v1\n");
        text.push_str(&format!(
            "source {} {}\n",
            self.tarball_name,
            sha256_hex(&self.tarball)
        ));
        let names = check_patch_folder(&self.patches)?;
        for name in names {
            let bytes = &self.patches.iter().find(|(n, _)| *n == name).unwrap().1;
            text.push_str(&format!("patch {name} {}\n", sha256_hex(bytes)));
        }
        text.push_str(&format!("version {version}\n"));
        Ok(format!("sha256:{}", sha256_hex(text.as_bytes())))
    }

    /// Everything the script refuses, in Rust.
    fn check(&self) -> Result<(), String> {
        let want = format!(
            "{}  third_party/{}",
            sha256_hex(&self.tarball),
            self.tarball_name
        );
        if self.checksum.trim_end_matches('\n') != want {
            return Err(format!("{} does not match its .sha256", self.tarball_name));
        }
        let version = env_value(&self.env, "ENGINE_VERSION")?;
        if format!("kafka-backup-v{}.tar.gz", release_of(&version)?) != self.tarball_name {
            return Err(format!("ENGINE_VERSION {version} names another release"));
        }
        let recorded = env_value(&self.env, "ENGINE_DIGEST")?;
        check_ledger(&self.ledger, &version, &recorded)?;
        let digest = self.digest()?;
        if digest != recorded {
            return Err(format!(
                "the inputs give {digest}, but kafka-backup-build.env records {recorded}"
            ));
        }
        Ok(())
    }

    /// A NEW BUILD, the way the README says to make one: bump `n`, record
    /// the digest the inputs then give, and append the pair to the ledger.
    fn with_new_build(self) -> Self {
        let version = env_value(&self.env, "ENGINE_VERSION").unwrap();
        let (release, n) = version.split_once("+logweir.").unwrap();
        let bumped = format!("{release}+logweir.{}", n.parse::<u64>().unwrap() + 1);
        let mut next = self.with_env_line("ENGINE_VERSION", &bumped);
        let digest = next.digest().expect("a digest");
        next = next.with_env_line("ENGINE_DIGEST", &digest);
        next.ledger.push_str(&format!("{bumped} {digest}\n"));
        next
    }

    /// Re-records the digest WITHOUT bumping `n` (review M1's mistake), and
    /// writes the ledger one of three ways.
    fn with_digest_rerecorded_unbumped(self, ledger: LedgerEdit) -> Self {
        let version = env_value(&self.env, "ENGINE_VERSION").unwrap();
        let old = env_value(&self.env, "ENGINE_DIGEST").unwrap();
        let digest = self.digest().expect("a digest");
        let mut next = self.with_env_line("ENGINE_DIGEST", &digest);
        match ledger {
            LedgerEdit::Untouched => {}
            LedgerEdit::Appended => next.ledger.push_str(&format!("{version} {digest}\n")),
            LedgerEdit::RewrittenInPlace => next.ledger = next.ledger.replace(&old, &digest),
        }
        next
    }

    fn with_patch(mut self, name: &str, content: &str) -> Self {
        self.patches
            .push((name.into(), content.as_bytes().to_vec()));
        self
    }

    /// Writes the tree under `dir` for `LOGWEIR_ROOT`.
    fn plant(&self, dir: &Path) {
        let tp = dir.join("third_party");
        let patches = tp.join("kafka-backup-patches");
        std::fs::create_dir_all(&patches).unwrap();
        std::fs::write(tp.join("kafka-backup-build.env"), &self.env).unwrap();
        std::fs::write(tp.join("kafka-backup-builds.txt"), &self.ledger).unwrap();
        std::fs::write(tp.join(&self.tarball_name), &self.tarball).unwrap();
        std::fs::write(
            tp.join(format!("{}.sha256", self.tarball_name)),
            &self.checksum,
        )
        .unwrap();
        for (name, bytes) in &self.patches {
            std::fs::write(patches.join(name), bytes).unwrap();
        }
    }
}

/// How a re-recorded digest is written into the ledger.
enum LedgerEdit {
    Untouched,
    Appended,
    RewrittenInPlace,
}

/// Runs `scripts/engine-source.sh <args>` over a planted tree.
fn script(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new("bash")
        .arg(root().join("scripts/engine-source.sh"))
        .args(args)
        .env("LOGWEIR_ROOT", dir)
        .output()
        .expect("bash runs scripts/engine-source.sh")
}

fn text(o: &std::process::Output) -> String {
    String::from_utf8_lossy(&o.stdout).to_string() + &String::from_utf8_lossy(&o.stderr)
}

/// A patch that applies to the vendored 0.23.3 source: it changes one comment
/// of `crates/kafka-backup-core/src/kafka/fetch.rs` (lines 50-56).
const GOOD_DIFF: &str = "\
diff --git a/crates/kafka-backup-core/src/kafka/fetch.rs b/crates/kafka-backup-core/src/kafka/fetch.rs
--- a/crates/kafka-backup-core/src/kafka/fetch.rs
+++ b/crates/kafka-backup-core/src/kafka/fetch.rs
@@ -50,7 +50,7 @@ pub async fn fetch(
         .with_max_wait_ms(500)
         .with_min_bytes(1)
         .with_max_bytes(max_bytes)
-        .with_isolation_level(0) // READ_UNCOMMITTED
+        .with_isolation_level(0) // READ_UNCOMMITTED (planted by engine_build.rs)
         .with_topics(vec![fetch_topic]);

     let response: KafkaFetchResponse = client.send_request(ApiKey::Fetch, request).await?;
";

fn good_patch() -> String {
    format!("Reason: a planted fix, for the test\nUpstream: https://github.com/osodevops/kafka-backup/pull/1\n\n{GOOD_DIFF}")
}

#[test]
fn the_recorded_digest_is_the_digest_the_inputs_give() {
    let inputs = Inputs::real();
    inputs.check().unwrap();
    assert_eq!(
        env_value(&inputs.env, "ENGINE_VERSION").unwrap(),
        logweir::doctor::ENGINE_PIN,
        "the build stamps the version doctor pins"
    );
}

#[test]
fn the_script_computes_the_same_digest_and_accepts_the_real_inputs() {
    let out = Command::new("bash")
        .arg(root().join("scripts/engine-source.sh"))
        .arg("digest")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        Inputs::real().digest().unwrap(),
        "scripts/engine-source.sh and this file define one digest"
    );
    let check = Command::new("bash")
        .arg(root().join("scripts/engine-source.sh"))
        .arg("check")
        .output()
        .unwrap();
    assert!(check.status.success(), "{}", text(&check));
    assert_eq!(
        String::from_utf8_lossy(&check.stdout),
        format!(
            "version={}\ndigest={}\n",
            logweir::doctor::ENGINE_PIN,
            env_value(&Inputs::real().env, "ENGINE_DIGEST").unwrap()
        ),
        "`check` prints the identity the image declares"
    );
}

#[test]
fn the_shipped_patch_folder_follows_its_policy() {
    let names = check_patch_folder(&Inputs::real().patches).unwrap();
    // The set the build applies, in order. A patch that lands or is dropped
    // moves this line, and the README's table, with it.
    assert_eq!(names, ["0001-lockfile-rustls-h2-spin.patch"], "{names:?}");
}

/// THE POSITIVE CONTROL: a well-formed patch with its digest recorded is
/// accepted by both implementations, and `prepare` applies it and stamps the
/// version — so the negative controls below are refusals of the rule they
/// name, not of everything.
#[test]
fn a_well_formed_patch_is_accepted_and_applied_by_both() {
    let inputs = Inputs::real()
        .with_patch("0901-planted-fix.patch", &good_patch())
        .with_new_build();
    inputs.check().unwrap();
    let version = env_value(&inputs.env, "ENGINE_VERSION").unwrap();
    assert_ne!(
        version,
        logweir::doctor::ENGINE_PIN,
        "a new build has a new version"
    );
    let dir = tempfile::tempdir().unwrap();
    inputs.plant(dir.path());
    let out = script(dir.path(), &["check"]);
    assert!(out.status.success(), "{}", text(&out));

    let src = dir.path().join("prepared");
    let out = script(dir.path(), &["prepare", src.to_str().unwrap()]);
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        text(&out).contains("applied 0901-planted-fix.patch: a planted fix, for the test"),
        "{}",
        text(&out)
    );
    let fetch =
        std::fs::read_to_string(src.join("crates/kafka-backup-core/src/kafka/fetch.rs")).unwrap();
    assert!(fetch.contains("(planted by engine_build.rs)"));
    let main = std::fs::read_to_string(src.join("crates/kafka-backup-cli/src/main.rs")).unwrap();
    assert!(
        main.contains(&format!("#[command(version = \"{version}\")]"))
            && !main.contains("#[command(version)]"),
        "the version stamp landed exactly"
    );
    assert_eq!(
        std::fs::read_to_string(src.join("LOGWEIR-ENGINE-IDENTITY")).unwrap(),
        format!("version={version}\ndigest={}\n", inputs.digest().unwrap())
    );
}

/// THE NEGATIVE CONTROLS, one per rule, each refused by the Rust checker AND
/// by the script, with the script's message naming the rule.
#[test]
fn every_rule_is_refused_by_both_implementations() {
    let good = good_patch();
    let cases: Vec<(&str, Inputs, &str)> = vec![
        (
            "a patch with no reason",
            Inputs::real()
                .with_patch("0901-fix.patch", &good.replacen("Reason: ", "Why: ", 1))
                .with_digest_recorded_unchecked(),
            "Reason: <one line>",
        ),
        (
            "an empty reason",
            Inputs::real()
                .with_patch(
                    "0901-fix.patch",
                    &good.replacen("Reason: a planted fix, for the test", "Reason: ", 1),
                )
                .with_digest_recorded_unchecked(),
            "Reason: <one line>",
        ),
        (
            "an upstream line that is not an https URL",
            Inputs::real()
                .with_patch(
                    "0901-fix.patch",
                    &good.replacen("https://github.com", "http://github.com", 1),
                )
                .with_digest_recorded_unchecked(),
            "Upstream: https://",
        ),
        (
            "a second line that is neither blank nor upstream",
            Inputs::real()
                .with_patch(
                    "0901-fix.patch",
                    &good.replacen(
                        "Upstream: https://github.com/osodevops/kafka-backup/pull/1",
                        "and a second line of reason",
                        1,
                    ),
                )
                .with_digest_recorded_unchecked(),
            "line 2",
        ),
        (
            "a patch with no diff",
            Inputs::real()
                .with_patch("0901-fix.patch", "Reason: nothing\n\njust words\n")
                .with_digest_recorded_unchecked(),
            "no unified diff",
        ),
        (
            "an unnumbered patch",
            Inputs::real()
                .with_patch("fix.patch", &good)
                .with_digest_recorded_unchecked(),
            "NNNN-<slug>.patch",
        ),
        (
            "a three-digit number",
            Inputs::real()
                .with_patch("901-fix.patch", &good)
                .with_digest_recorded_unchecked(),
            "NNNN-<slug>.patch",
        ),
        (
            "an uppercase slug",
            Inputs::real()
                .with_patch("0901-Fix.patch", &good)
                .with_digest_recorded_unchecked(),
            "NNNN-<slug>.patch",
        ),
        (
            "a stray file",
            Inputs::real()
                .with_patch("notes.txt", "remember to upstream this\n")
                .with_digest_recorded_unchecked(),
            "nothing else lives in the patch folder",
        ),
        (
            "two patches with one number",
            Inputs::real()
                .with_patch("0901-fix.patch", &good)
                .with_patch("0901-other.patch", &good)
                .with_digest_recorded_unchecked(),
            "two patches are numbered 0901",
        ),
        (
            "a patch added without re-recording the digest",
            Inputs::real().with_patch("0901-fix.patch", &good),
            "bump the <n>",
        ),
        (
            "OSO's release as the version",
            Inputs::real().with_env_line("ENGINE_VERSION", "0.23.3"),
            "<release>+logweir.<n>",
        ),
        (
            "build zero",
            Inputs::real().with_env_line("ENGINE_VERSION", "0.23.3+logweir.0"),
            "<release>+logweir.<n>",
        ),
        (
            "a version bump without re-recording the digest",
            Inputs::real().with_env_line("ENGINE_VERSION", "0.23.3+logweir.2"),
            "bump the <n>",
        ),
        (
            "a tarball that does not match its checksum",
            Inputs::real().with_tarball_byte_flipped(),
            "does not match",
        ),
        (
            "no README stating the policy",
            Inputs::real().without_readme(),
            "README.md",
        ),
        // REVIEW M1: ONE VERSION, ONE ENGINE. A patch lands and its digest is
        // re-recorded, but `n` is not bumped; whichever way the ledger is then
        // written, the gate refuses (a line rewritten in place is
        // `engine_pin.rs`'s, below).
        (
            "a new digest under the same version, appended to the ledger",
            Inputs::real()
                .with_patch("0901-fix.patch", &good)
                .with_digest_rerecorded_unbumped(LedgerEdit::Appended),
            "is recorded twice",
        ),
        (
            "a new digest under the same version, the ledger untouched",
            Inputs::real()
                .with_patch("0901-fix.patch", &good)
                .with_digest_rerecorded_unbumped(LedgerEdit::Untouched),
            "bump the <n>",
        ),
        (
            "a bumped version the ledger does not record",
            Inputs::real()
                .with_patch("0901-fix.patch", &good)
                .with_new_build()
                .with_ledger(&read("third_party/kafka-backup-builds.txt")),
            "bump the <n>",
        ),
        (
            "an <n> that falls",
            Inputs::real().with_ledger(&format!(
                "0.23.3+logweir.2 sha256:{}\n0.23.3+logweir.1 {}\n",
                "a".repeat(64),
                env_value(&read("third_party/kafka-backup-build.env"), "ENGINE_DIGEST").unwrap()
            )),
            "must rise",
        ),
        (
            "one digest under two versions",
            Inputs::real()
                .with_env_line("ENGINE_VERSION", "0.23.3+logweir.2")
                .with_ledger(&format!(
                    "{}\n0.23.3+logweir.2 {}\n",
                    read("third_party/kafka-backup-builds.txt").trim_end(),
                    env_value(&read("third_party/kafka-backup-build.env"), "ENGINE_DIGEST")
                        .unwrap()
                )),
            "is recorded twice",
        ),
        (
            "a malformed ledger line",
            Inputs::real().with_ledger("0.23.3+logweir.1 not-a-digest\n"),
            "is not `<release>+logweir.<n> sha256:<hex>`",
        ),
        (
            "no ledger",
            Inputs::real().with_ledger(""),
            "kafka-backup-builds.txt is missing",
        ),
    ];
    for (why, inputs, needle) in cases {
        assert!(inputs.check().is_err(), "Rust must refuse {why}");
        let dir = tempfile::tempdir().unwrap();
        inputs.plant(dir.path());
        let out = script(dir.path(), &["check"]);
        assert_eq!(
            out.status.code(),
            Some(1),
            "the script must refuse {why}:\n{}",
            text(&out)
        );
        assert!(
            text(&out).contains(needle),
            "the script's refusal of {why} must name `{needle}`:\n{}",
            text(&out)
        );
    }
}

/// REVIEW L8: `prepare` — the only mode the Dockerfile runs — refuses a stale
/// digest itself, before it extracts anything. (Mutant R4 deleted that check
/// from the `prepare` arm and survived every case above, which all run
/// `check`.)
#[test]
fn prepare_refuses_a_stale_digest_before_it_extracts() {
    let inputs = Inputs::real().with_patch("0901-fix.patch", &good_patch());
    let dir = tempfile::tempdir().unwrap();
    inputs.plant(dir.path());
    let dest = dir.path().join("prepared");
    let out = script(dir.path(), &["prepare", dest.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(text(&out).contains("bump the <n>"), "{}", text(&out));
    assert!(
        !dest.join("Cargo.toml").exists(),
        "nothing is extracted from a build whose digest is stale"
    );
}

/// A patch that no longer applies — the release now contains it, or it was
/// written against another release — stops the build (README, rule 4).
#[test]
fn a_patch_that_does_not_apply_stops_prepare() {
    let stale = good_patch().replace(
        "-        .with_isolation_level(0) // READ_UNCOMMITTED\n",
        "-        .with_isolation_level(7) // A LINE THE SOURCE DOES NOT HAVE\n",
    );
    let inputs = Inputs::real()
        .with_patch("0901-stale.patch", &stale)
        .with_new_build();
    let dir = tempfile::tempdir().unwrap();
    inputs.plant(dir.path());
    let out = script(
        dir.path(),
        &["prepare", dir.path().join("prepared").to_str().unwrap()],
    );
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(
        text(&out).contains("0901-stale.patch does not apply"),
        "{}",
        text(&out)
    );
}

/// The digest moves with every input: the same patch under another name or
/// another content, and another build number, each give another digest.
#[test]
fn the_digest_names_every_input() {
    let base = Inputs::real().with_patch("0901-fix.patch", &good_patch());
    let d = base.digest().unwrap();
    let renamed = Inputs::real().with_patch("0902-fix.patch", &good_patch());
    let edited = Inputs::real().with_patch(
        "0901-fix.patch",
        &good_patch().replace("planted", "Planted"),
    );
    let bumped = Inputs::real()
        .with_patch("0901-fix.patch", &good_patch())
        .with_env_line("ENGINE_VERSION", "0.23.3+logweir.2");
    let none = Inputs::real();
    for (why, other) in [
        ("renamed", renamed),
        ("edited", edited),
        ("bumped", bumped),
        ("dropped", none),
    ] {
        assert_ne!(other.digest().unwrap(), d, "{why}");
    }
}

impl Inputs {
    /// Records whatever digest the Rust definition gives, even for a folder
    /// the policy refuses (then the env keeps its old digest): the refusal a
    /// case exists for must be the policy's, not the digest's.
    fn with_digest_recorded_unchecked(self) -> Self {
        if self.digest().is_ok() {
            self.with_new_build()
        } else {
            self
        }
    }

    fn with_ledger(mut self, ledger: &str) -> Self {
        self.ledger = ledger.to_string();
        self
    }

    fn with_env_line(mut self, key: &str, value: &str) -> Self {
        let old = env_value(&self.env, key).unwrap();
        self.env = self
            .env
            .replace(&format!("{key}={old}"), &format!("{key}={value}"));
        self
    }

    fn with_tarball_byte_flipped(mut self) -> Self {
        let last = self.tarball.len() - 1;
        self.tarball[last] ^= 0xff;
        self
    }

    fn without_readme(mut self) -> Self {
        self.patches.retain(|(n, _)| n != "README.md");
        self
    }
}

/// The `[sources]` table of a cargo-deny policy, as `key = value` pairs with
/// whitespace removed. Pure text: the workspace carries no TOML parser.
fn sources_table(policy: &str) -> Vec<(String, String)> {
    let mut inside = false;
    let mut out = Vec::new();
    for line in policy.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.starts_with('[') {
            inside = line == "[sources]";
            continue;
        }
        if inside {
            if let Some((k, v)) = line.split_once('=') {
                out.push((k.trim().to_string(), v.split_whitespace().collect()));
            }
        }
    }
    out
}

/// Crates.io only, or refused: the policy and the gate that runs it.
fn check_sources_policy(policy: &str, gate_line: &str) -> Result<(), String> {
    let table = sources_table(policy);
    for (key, want) in [
        ("unknown-registry", "\"deny\""),
        ("unknown-git", "\"deny\""),
        (
            "allow-registry",
            "[\"https://github.com/rust-lang/crates.io-index\"]",
        ),
        ("allow-git", "[]"),
    ] {
        match table.iter().find(|(k, _)| k == key) {
            Some((_, v)) if v == want => {}
            Some((_, v)) => return Err(format!("[sources] {key} is {v}, must be {want}")),
            None => return Err(format!("[sources] sets no {key}")),
        }
    }
    if !gate_line.split_whitespace().any(|w| w == "sources") {
        return Err(format!("the gate does not check sources: `{gate_line}`"));
    }
    Ok(())
}

/// REVIEW M2: both graphs — the engine's own lockfile and Logweir's — admit
/// crates from crates.io only, and `scripts/ci-check.sh` checks `sources` for
/// both. The live negative control (a planted `git+file://` source for `spin`
/// in a prepared engine tree) exits 8 with `source-not-allowed` under this
/// policy and 0 with `unknown-git = "warn"`
/// (`claude/artifacts/prod-00-2/deny/engine-git-source-*.log`).
#[test]
fn both_graphs_admit_crates_io_only_and_the_gate_checks_it() {
    let ci = read("scripts/ci-check.sh");
    let engine_gate = ci
        .lines()
        .find(|l| l.contains("cargo deny --locked --manifest-path \"$engine_src/Cargo.toml\""))
        .expect("ci-check.sh runs cargo deny over the engine");
    let own_gate = ci
        .lines()
        .find(|l| l.trim_start().starts_with("cargo deny check"))
        .expect("ci-check.sh runs cargo deny over Logweir's graph");
    let engine_policy = read("third_party/kafka-backup-deny.toml");
    check_sources_policy(&engine_policy, engine_gate).unwrap();
    check_sources_policy(&read("deny.toml"), own_gate).unwrap();

    // Negative controls: each loosening is refused.
    for (loosened, why) in [
        (
            engine_policy.replace("unknown-git = \"deny\"", "unknown-git = \"warn\""),
            "a git source only warned about",
        ),
        (
            engine_policy.replace(
                "unknown-registry = \"deny\"",
                "unknown-registry = \"allow\"",
            ),
            "any registry",
        ),
        (
            engine_policy.replace(
                "allow-git = []",
                "allow-git = [\"https://github.com/someone/kafka-protocol-rs\"]",
            ),
            "an unrecorded git exception",
        ),
        (
            engine_policy.replace("[sources]", "[not-sources]"),
            "no table",
        ),
    ] {
        assert!(
            check_sources_policy(&loosened, engine_gate).is_err(),
            "{why}"
        );
    }
    assert!(
        check_sources_policy(&engine_policy, &engine_gate.replace(" sources", "")).is_err(),
        "a gate that skips sources"
    );
}
