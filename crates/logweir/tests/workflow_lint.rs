//! Small publication-boundary checks. Runtime behavior is tested by the shared
//! quality command and the Compose/image suites, rather than historical prose.
use serde_yaml::Value;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn workflow(name: &str) -> Value {
    serde_yaml::from_str(
        &std::fs::read_to_string(root().join(".github/workflows").join(name)).unwrap(),
    )
    .unwrap()
}
fn dependencies(job: &Value) -> Vec<&str> {
    match &job["needs"] {
        Value::String(s) => vec![s.as_str()],
        Value::Sequence(s) => s.iter().map(|v| v.as_str().unwrap()).collect(),
        Value::Null => vec![],
        other => panic!("invalid job dependencies: {other:?}"),
    }
}

#[test]
fn all_workflows_parse_and_default_to_read_only() {
    for entry in std::fs::read_dir(root().join(".github/workflows")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        let doc = workflow(&name);
        assert!(doc["on"].is_mapping(), "{name} needs explicit triggers");
        assert!(doc["jobs"].is_mapping());
        assert_eq!(doc["permissions"]["contents"].as_str(), Some("read"));
    }
}

#[test]
fn main_publication_requires_quality_and_integration_success() {
    let ci = workflow("ci.yml");
    let publish = &ci["jobs"]["publish"];
    assert_eq!(dependencies(publish), ["check", "e2e"]);
    assert_eq!(
        publish["if"].as_str(),
        Some("github.event_name == 'push' && github.ref == 'refs/heads/main'")
    );
    assert_eq!(
        publish["uses"].as_str(),
        Some("./.github/workflows/images.yml")
    );
    assert_eq!(publish["with"]["promote_latest"].as_bool(), Some(true));
    assert!(ci["on"].as_mapping().unwrap().contains_key("workflow_call"));
    assert!(ci["jobs"]["check"]["secrets"].is_null());
    assert!(ci["jobs"]["e2e"]["secrets"].is_null());
}

#[test]
fn images_are_loaded_and_checked_before_credentials_or_push() {
    let images = workflow("images.yml");
    let steps = images["jobs"]["build"]["steps"].as_sequence().unwrap();
    let check = steps
        .iter()
        .position(|s| s["run"] == "bash scripts/ci-images.sh check")
        .unwrap();
    let login = steps
        .iter()
        .position(|s| s["uses"] == "docker/login-action@v3")
        .unwrap();
    let push = steps
        .iter()
        .position(|s| s["run"] == "bash scripts/ci-images.sh candidates")
        .unwrap();
    assert!(check < login && login < push);
    let mut builds = 0;
    for (index, step) in steps.iter().enumerate() {
        if step["uses"]
            .as_str()
            .is_some_and(|s| s.starts_with("docker/build-push-action@"))
        {
            builds += 1;
            assert!(index < check);
            assert_eq!(step["with"]["load"].as_bool(), Some(true));
            assert_ne!(step["with"]["push"].as_bool(), Some(true));
        }
    }
    // FOUR SINCE D0 STAGE 7: the controller and the console on both
    // architectures, the UI on both, the runner on amd64 only. The number is
    // asserted rather than derived because a build step that stopped running is
    // an image that stops being tested while every other row here stays green.
    assert_eq!(builds, 4);
    // AND EACH COMPILING IMAGE HAS ITS FAST-FAILING IDENTITY CHECK BEFORE THE
    // LOGIN STEP. `scripts/ci-images.sh check` runs the full gates a moment
    // later; these two exist because they fail in seconds and NAME the defect —
    // 457f651 added the first after RET-NOIMAGE shipped a retention Job whose
    // command resolved to nothing, and review finding L1 then established that
    // the version STRING has to be inspected, because a COPY from the wrong
    // source exits 0 while printing another binary's name.
    for (name, marker) in [
        (
            "retention",
            "--entrypoint logweir-retention logweir:check --version",
        ),
        (
            "console",
            "--entrypoint logweir-api logweir-console:check --version",
        ),
    ] {
        let probe = steps
            .iter()
            .position(|s| {
                s["run"]
                    .as_str()
                    .is_some_and(|r| r.contains(marker) && r.contains("case \"$version\" in"))
            })
            .unwrap_or_else(|| {
                panic!(
                    "images.yml must run the {name} binary BY BARE NAME and inspect its --version \
                     output before the login step; exit 0 is not identity (review finding L1)"
                )
            });
        assert!(
            probe < check && probe < login,
            "the {name} identity probe must run before the full gates and before any credential"
        );
    }
    let matrix = images["jobs"]["build"]["strategy"]["matrix"]["include"]
        .as_sequence()
        .unwrap();
    assert!(matrix
        .iter()
        .any(|r| r["arch"] == "amd64" && r["runner"] == "ubuntu-24.04"));
    assert!(matrix
        .iter()
        .any(|r| r["arch"] == "arm64" && r["runner"] == "ubuntu-24.04-arm"));
    assert_eq!(dependencies(&images["jobs"]["promote"]), ["build"]);
    assert_eq!(
        images["jobs"]["promote"]["concurrency"]["cancel-in-progress"].as_bool(),
        Some(false)
    );
}

/// **Chart gap G4: the chart is published from the EXISTING image workflow,
/// after the images it names, with the existing credentials, versioned with
/// them, and verified by content.** No workflow file is added for it.
#[test]
fn the_chart_is_published_beside_the_images_it_names() {
    let mut names: Vec<String> = std::fs::read_dir(root().join(".github/workflows"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "ci.yml",
            "engine-matrix.yml",
            "helm-demo.yml",
            "images.yml",
            "release-drill.yml",
            "release.yml"
        ],
        "the chart is published by images.yml's promote job; a new workflow is a new \
         publication path nobody reviewed"
    );
    let images = workflow("images.yml");
    let promote = &images["jobs"]["promote"];
    let steps = promote["steps"].as_sequence().unwrap();
    let position = |pred: &dyn Fn(&Value) -> bool, what: &str| {
        steps
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("images.yml promote job has no {what}"))
    };
    let login = position(&|s| s["uses"] == "docker/login-action@v3", "docker login");
    let images_step = position(
        &|s| s["run"] == "bash scripts/ci-images.sh promote",
        "image promotion",
    );
    // THE ACTION ON THE CREDENTIALED PATH IS PINNED BY COMMIT (review L6):
    // the Helm it installs receives the Docker Hub token on stdin.
    let helm = position(
        &|s| {
            s["uses"].as_str().is_some_and(|u| {
                u.strip_prefix("azure/setup-helm@").is_some_and(|sha| {
                    sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit())
                })
            })
        },
        "Helm setup pinned by a 40-hex commit",
    );
    let chart = position(
        &|s| s["run"] == "bash scripts/ci-images.sh chart",
        "chart publication",
    );
    assert!(
        login < images_step && images_step < chart && helm < chart,
        "the chart is published after the images it names are public, with Helm set up first"
    );
    assert_eq!(steps[helm]["with"]["version"].as_str(), Some("v4.0.1"));
    assert_eq!(steps[chart]["id"].as_str(), Some("chart"));
    assert_eq!(
        steps[chart]["env"]["DOCKERHUB_USERNAME"].as_str(),
        Some("${{ secrets.DOCKERHUB_USERNAME }}")
    );
    assert_eq!(
        steps[chart]["env"]["DOCKERHUB_TOKEN"].as_str(),
        Some("${{ secrets.DOCKERHUB_TOKEN }}"),
        "the chart reuses the image credentials; no new secret"
    );
    assert_eq!(
        promote["outputs"]["chart_version"].as_str(),
        Some("${{ steps.chart.outputs.chart_version }}")
    );
    assert!(images["on"]["workflow_call"]["outputs"]["chart_version"].is_mapping());
    // Both callers pass the tag the chart is versioned with.
    let ci = workflow("ci.yml");
    assert_eq!(
        ci["jobs"]["publish"]["with"]["tag"].as_str(),
        Some("sha-${{ github.sha }}")
    );
    let release = workflow("release.yml");
    assert_eq!(
        release["jobs"]["images"]["with"]["tag"].as_str(),
        Some("${{ github.ref_name }}")
    );

    let script = std::fs::read_to_string(root().join("scripts/ci-images.sh")).unwrap();
    for needle in [
        "CHART_NAME=logweir-chart",
        "CHART_REPOSITORY=\"oci://registry-1.docker.io/$NS\"",
        "echo \"$base-sha-$GITHUB_SHA\"",
        "--app-version \"$TAG\"",
        "[[ \"$rewritten\" -eq 4 ]]",
        "helm registry login registry-1.docker.io",
        "--password-stdin",
        "helm push \"$package\" \"$CHART_REPOSITORY\"",
        "helm pull \"$CHART_REPOSITORY/$CHART_NAME\" --version \"$version\"",
        "[[ \"$pushed\" == \"$served\" ]]",
        "docker buildx imagetools inspect \"docker.io/$NS/$product:$TAG\"",
        // "Public" is asked anonymously (review L5).
        "DOCKER_CONFIG=\"$anonymous_docker\" docker buildx imagetools inspect",
        "HELM_REGISTRY_CONFIG=\"$dir/anonymous/config.json\"",
    ] {
        assert!(
            script.contains(needle),
            "scripts/ci-images.sh's chart arm must keep `{needle}`"
        );
    }
    assert!(
        !script.contains("--password \"$DOCKERHUB_TOKEN\"")
            && !script.contains("-p \"$DOCKERHUB_TOKEN\""),
        "the token reaches Helm on stdin, never on a command line"
    );
}

#[test]
fn releases_reuse_checks_and_test_packaged_binary_before_images() {
    let release = workflow("release.yml");
    let jobs = &release["jobs"];
    assert_eq!(jobs["tests"]["uses"], "./.github/workflows/ci.yml");
    assert_eq!(
        jobs["drill"]["uses"],
        "./.github/workflows/release-drill.yml"
    );
    assert!(dependencies(&jobs["images"]).contains(&"drill"));
    assert_eq!(
        jobs["images"]["with"]["promote_latest"].as_bool(),
        Some(false)
    );
    assert!(dependencies(&jobs["publish"]).contains(&"images"));
    assert_eq!(jobs["publish"]["permissions"]["contents"], "write");
    let drill = workflow("release-drill.yml");
    assert!(
        drill["on"]["release"].is_null(),
        "do not rely on suppressed GITHUB_TOKEN release events"
    );
    let text = std::fs::read_to_string(root().join(".github/workflows/release-drill.yml")).unwrap();
    assert!(text.contains("binary-x86_64-unknown-linux-gnu"));
    assert!(
        !text.contains("head -1"),
        "select a specific platform artifact"
    );
}

#[test]
fn release_archives_are_checked_for_an_engine_before_upload() {
    let release = workflow("release.yml");
    let steps = release["jobs"]["build"]["steps"].as_sequence().unwrap();
    let build = steps
        .iter()
        .position(|s| {
            s["run"]
                .as_str()
                .is_some_and(|r| r.starts_with("dist build "))
        })
        .unwrap();
    let check = steps
        .iter()
        .position(|s| {
            s["run"]
                == "bash scripts/check-no-engine-in-binary.sh \
                    \"target/distrib/logweir-${{ matrix.target }}.tar.xz\""
        })
        .expect("the build job must check the archive it ships for an engine");
    let upload = steps
        .iter()
        .position(|s| s["uses"] == "actions/upload-artifact@v4")
        .unwrap();
    assert!(build < check && check < upload);
    let ci_check = std::fs::read_to_string(root().join("scripts/ci-check.sh")).unwrap();
    assert!(
        ci_check.contains("bash scripts/check-no-engine-in-binary.sh \"$LOGWEIR_BIN\""),
        "CI must run the release engine check too, so it cannot first fail at a tag"
    );
}

#[test]
fn extended_kubernetes_checks_are_explicit() {
    let helm = workflow("helm-demo.yml");
    assert!(helm["on"]["push"].is_null());
    assert!(helm["on"]["pull_request"].is_null());
    assert!(helm["on"]
        .as_mapping()
        .unwrap()
        .contains_key("workflow_dispatch"));
}

/// A tracker-only change starts no run; any other docs-only change runs
/// `check` (its contract tests read the docs) but neither `e2e` nor `publish`,
/// because no binary embeds a document and no image copies one out of its
/// build stage. `check` itself is never conditional.
#[test]
fn docs_only_changes_skip_what_they_cannot_affect() {
    let ci = workflow("ci.yml");
    for event in ["push", "pull_request"] {
        let ignored: Vec<&str> = ci["on"][event]["paths-ignore"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{event} needs paths-ignore"))
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(
            ignored,
            ["docs/to-do/**"],
            "{event} ignores only the tracker records"
        );
    }
    let jobs = &ci["jobs"];
    assert!(
        jobs["check"]["if"].is_null(),
        "check must run on every change"
    );
    assert!(dependencies(&jobs["check"]).is_empty());
    assert_eq!(dependencies(&jobs["e2e"]), ["changes"]);
    assert_eq!(
        jobs["e2e"]["if"].as_str(),
        Some("needs.changes.outputs.code == 'true'")
    );
    let classify = jobs["changes"]["steps"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|s| s["id"] == "classify")
        .expect("the changes job classifies the diff");
    assert!(classify["run"]
        .as_str()
        .unwrap()
        .starts_with("bash scripts/ci-changes.sh "));
}

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.invalid")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.invalid")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn commit(dir: &std::path::Path, path: &str) -> String {
    let file = dir.join(path);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(&file, format!("{path}\n")).unwrap();
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", path]);
    git(dir, &["rev-parse", "HEAD"])
}

fn classify(dir: &std::path::Path, base: &str, head: &str) -> String {
    let out = std::process::Command::new("bash")
        .arg(root().join("scripts/ci-changes.sh"))
        .args([base, head])
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

/// The classifier runs everything unless every changed path is under docs/,
/// and runs everything when it cannot tell (no base, all-zero base, a base
/// the checkout lacks). Each `false` below has a `true` twin one path away.
#[test]
fn ci_changes_classifies_docs_only_diffs() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    git(dir, &["init", "-q"]);
    let base = commit(dir, "crates/a.rs");
    let docs = commit(dir, "docs/api.md");
    assert_eq!(classify(dir, &base, &docs), "code=false");
    let tracker = commit(dir, "docs/to-do/tracker.md");
    assert_eq!(classify(dir, &base, &tracker), "code=false");
    let code = commit(dir, "crates/b.rs");
    assert_eq!(classify(dir, &base, &code), "code=true");
    assert_eq!(classify(dir, &tracker, &code), "code=true");
    // A root Markdown file is not under docs/: tests and images read some.
    let root_md = commit(dir, "THIRD_PARTY_NOTICES.md");
    assert_eq!(classify(dir, &code, &root_md), "code=true");
    // A path that merely starts with "docs" is not the docs directory.
    let lookalike = commit(dir, "docs-site/x.md");
    assert_eq!(classify(dir, &root_md, &lookalike), "code=true");
    assert_eq!(classify(dir, "", &lookalike), "code=true");
    assert_eq!(classify(dir, &"0".repeat(40), &lookalike), "code=true");
    assert_eq!(classify(dir, &"f".repeat(40), &lookalike), "code=true");
}
