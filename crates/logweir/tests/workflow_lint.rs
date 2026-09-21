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
        ("retention", "--entrypoint logweir-retention logweir:check --version"),
        ("console", "--entrypoint logweir-api logweir-console:check --version"),
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
