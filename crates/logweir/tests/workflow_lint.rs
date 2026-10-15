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
    assert_eq!(dependencies(publish), ["check", "e2e", "console"]);
    // The console job's gates hold the image as well; its skip (no console
    // change) passes, a skipped check or e2e does not, and a failure never does.
    assert_eq!(
        publish["if"].as_str(),
        Some(
            "github.event_name == 'push' && github.ref == 'refs/heads/main' && !cancelled() \
             && needs.check.result == 'success' && needs.e2e.result == 'success' \
             && needs.console.result != 'failure'"
        )
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
    // NO npm CODE WHERE THE CREDENTIALS ARE (PROD-18.1 review M1). The console
    // build the image check compares with runs in `console-dist`, which holds no
    // secret and pushes nothing; `build`, which logs in and pushes, takes only
    // its files.
    assert_eq!(dependencies(&images["jobs"]["build"]), ["console-dist"]);
    for step in steps {
        let run = step["run"].as_str().unwrap_or("");
        let uses = step["uses"].as_str().unwrap_or("");
        assert!(
            !run.contains("npm") && !run.contains("npx") && !uses.starts_with("actions/setup-node"),
            "images.yml `build` runs npm code beside the registry credentials: {step:?}"
        );
    }
    let dist = serde_yaml::to_string(&images["jobs"]["console-dist"]).unwrap();
    assert!(dist.contains("npm ci && npx vite build"));
    assert!(
        !dist.contains("secrets.")
            && !dist.contains("login-action")
            && dist.contains("persist-credentials: false"),
        "console-dist must hold no credential: {dist}"
    );
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
    // THREE: the controller, the console and the runner, each built per
    // matrix architecture. The number is asserted rather than derived because a
    // build step that stopped running is an image that stops being tested
    // while every other row here stays green.
    assert_eq!(builds, 3);
    // AND EACH COMPILING IMAGE HAS ITS FAST-FAILING IDENTITY CHECK BEFORE THE
    // LOGIN STEP. `scripts/ci-images.sh check` runs the full gates a moment
    // later; these fail in seconds and NAME the defect, and the version STRING
    // is inspected because a COPY from the wrong source exits 0 while printing
    // another binary's name.
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
        // compat-weekly.yml runs e2e rows weekly and publishes
        // nothing: contents read, no secret, no registry login.
        [
            "ci.yml",
            "compat-weekly.yml",
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
    // THE ACTION ON THE CREDENTIALED PATH IS PINNED BY COMMIT:
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
    // A RELEASE publishes the chart package it LISTS: packaged once
    // by `release.sh assemble`, pinned by digest, and pushed as those bytes by
    // `ci-images.sh chart-push` after the version tags exist, with the same
    // credentials and the same commit-pinned Helm as the main publication.
    let release = workflow("release.yml");
    let publish = &release["jobs"]["publish-images"];
    let steps = publish["steps"].as_sequence().unwrap();
    let position = |pred: &dyn Fn(&Value) -> bool, what: &str| {
        steps
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("release.yml publish-images has no {what}"))
    };
    let login = position(
        &|s| {
            s["uses"]
                .as_str()
                .is_some_and(|u| u.starts_with("docker/login-action@"))
        },
        "docker login",
    );
    let promote = position(
        &|s| s["run"] == "bash scripts/release.sh promote release-assets/release-in/images.json",
        "promotion of the resolved publication",
    );
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
    let push = position(
        &|s| {
            s["run"]
                .as_str()
                .is_some_and(|r| r.starts_with("bash scripts/ci-images.sh chart-push "))
        },
        "the chart push",
    );
    assert!(
        login < promote && promote < push && helm < push,
        "the release's chart is pushed after the version tags it names exist"
    );
    assert_eq!(steps[helm]["with"]["version"].as_str(), Some("v4.0.1"));
    for (name, value) in [
        ("DOCKERHUB_USERNAME", "${{ secrets.DOCKERHUB_USERNAME }}"),
        ("DOCKERHUB_TOKEN", "${{ secrets.DOCKERHUB_TOKEN }}"),
    ] {
        assert_eq!(
            steps[push]["env"][name].as_str(),
            Some(value),
            "no new secret"
        );
    }

    let script = std::fs::read_to_string(root().join("scripts/ci-images.sh")).unwrap();
    for needle in [
        "CHART_NAME=logweir-chart",
        "CHART_REPOSITORY=\"oci://registry-1.docker.io/$NS\"",
        "echo \"$base-sha-$GITHUB_SHA\"",
        "--app-version \"$TAG\"",
        "[[ \"$rewritten\" -eq 3 ]]",
        "helm registry login registry-1.docker.io",
        "--password-stdin",
        "helm push \"$package\" \"$CHART_REPOSITORY\"",
        "helm pull \"$CHART_REPOSITORY/$CHART_NAME\" --version \"$version\"",
        "[[ \"$pushed\" == \"$served\" ]]",
        "docker buildx imagetools inspect \"docker.io/$NS/$product:$TAG\"",
        // "Public" is asked anonymously.
        "DOCKER_CONFIG=\"$anonymous_docker\" docker buildx imagetools inspect",
        "DOCKER_CONFIG=\"$anonymous_docker\" HELM_REGISTRY_CONFIG=\"$dir/anonymous/config.json\"",
        "HELM_REGISTRY_CONFIG=\"$dir/login/config.json\" helm push",
        // A release chart pins digests, and a published release
        // version is never replaced.
        "a release chart ($TAG) pins its three images by digest",
        "  chart-push)",
        "chart_publish \"$package\" \"$version\" replace",
        "chart_publish \"$package\" \"$version\" immutable",
        "a release version is never replaced",
        // An immutable push only on the registry's own "not found" for THIS
        // version; any other failed read refuses before the login.
        "elif grep -qxF \"Error: failed to perform \\\"FetchReference\\\" on source: \
         ${CHART_REPOSITORY#oci://}/$CHART_NAME:$version: not found\"",
        "could not tell whether $CHART_NAME $version exists; nothing pushed",
    ] {
        assert!(
            script.contains(needle),
            "scripts/ci-images.sh's chart arm must keep `{needle}`"
        );
    }
    // THE OPERATOR ACTION A FAIL-CLOSED PULL-BACK IMPLIES IS DOCUMENTED (re-check
    // L-rf1): a chart repository created private makes the step fail until it
    // is made Public and the job re-run, and both documents say so.
    for doc in ["docs/install.md", "docs/release-notes.md"] {
        let text = std::fs::read_to_string(root().join(doc)).unwrap();
        assert!(
            text.contains("`vladyslavhaina/logweir-chart` must be Public in Docker Hub")
                && text.contains("re-run"),
            "{doc} must say the chart repository must be Public on first publication, or the \
             chart step fails closed until it is made Public and the job re-run"
        );
    }
    assert!(
        !script.contains("--password \"$DOCKERHUB_TOKEN\"")
            && !script.contains("-p \"$DOCKERHUB_TOKEN\""),
        "the token reaches Helm on stdin, never on a command line"
    );
}

#[test]
fn releases_reuse_checks_and_test_packaged_binary_before_publishing() {
    let release = workflow("release.yml");
    let jobs = &release["jobs"];
    assert_eq!(jobs["tests"]["uses"], "./.github/workflows/ci.yml");
    assert!(
        jobs["tests"]["secrets"].is_null(),
        "the test gate gets no secret"
    );
    assert_eq!(
        jobs["drill"]["uses"],
        "./.github/workflows/release-drill.yml"
    );
    assert_eq!(dependencies(&jobs["drill"]), ["build"]);
    for need in ["build", "drill", "images"] {
        assert!(
            dependencies(&jobs["assemble"]).contains(&need),
            "assemble needs {need}"
        );
    }
    for need in ["tests", "assemble"] {
        assert!(
            dependencies(&jobs["publish-images"]).contains(&need),
            "publish-images needs {need}"
        );
    }
    for need in ["tests", "assemble", "publish-images"] {
        assert!(
            dependencies(&jobs["github-release"]).contains(&need),
            "github-release needs {need}"
        );
    }
    assert_eq!(jobs["github-release"]["permissions"]["contents"], "write");
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

/// A DISPATCH IS A DRY RUN. Every job that can write outside the
/// run — ANY write scope of its token, a credential, a release, a tag push, a
/// registry write, a GitHub API call — carries the one gate that admits a
/// pushed `v*` tag and nothing else, so a dispatched run ends at `assemble`
/// with workflow artifacts only. The classification covers every
/// write scope (`packages`, `id-token`, …), not `contents` alone, and the
/// scripts the ungated jobs run.
#[test]
fn a_dispatch_is_a_dry_run_that_reaches_no_credential() {
    const GATE: &str = "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v') \
                        && needs.validate.outputs.publish == 'true'";
    // What marks a job's text as writing outside the run.
    const WRITES: [&str; 14] = [
        "secrets.",
        "gh release create",
        "gh release upload",
        "gh release edit",
        "gh release delete",
        "gh api",
        "git push",
        "release.sh promote",
        "chart-push",
        "docker push",
        "helm push",
        "registry login",
        "imagetools create",
        "docker/login-action",
    ];
    let release = workflow("release.yml");
    let on = release["on"].as_mapping().unwrap();
    assert_eq!(on.len(), 2, "a tag push and a dispatch, nothing else");
    assert!(on.contains_key("workflow_dispatch"));
    assert_eq!(
        release["on"]["push"]["tags"].as_sequence().unwrap(),
        &vec![Value::from("v*")]
    );
    assert!(release["on"]["push"]["branches"].is_null());
    // The workflow's default grants a read and nothing else: a write scope
    // there would reach every job, gated or not.
    assert_eq!(
        release["permissions"].as_mapping().map(|m| m.len()),
        Some(1),
        "release.yml's default permissions are `contents: read` alone"
    );
    assert_eq!(release["permissions"]["contents"].as_str(), Some("read"));
    let mut gated: Vec<String> = Vec::new();
    for (name, job) in release["jobs"].as_mapping().unwrap() {
        let name = name.as_str().unwrap().to_string();
        let body = serde_yaml::to_string(job).unwrap();
        // ANY write scope of the job's token, or `write-all`.
        let write_scope = match &job["permissions"] {
            Value::Null => false,
            Value::String(all) => all != "read-all",
            Value::Mapping(scopes) => scopes
                .values()
                .any(|v| !matches!(v.as_str(), Some("read") | Some("none"))),
            other => panic!("{name} has invalid permissions: {other:?}"),
        };
        // `tests` calls ci.yml, whose `publish` job asks for id-token and
        // attestations; GitHub refuses to start the call unless the calling
        // job grants them. It is held to exactly those, handed no secret, and
        // `the_release_tests_call_can_never_reach_ci_publish` proves no ci.yml
        // job that uses them can run inside a release.
        if name == "tests" {
            assert!(job["secrets"].is_null(), "tests is handed no secret");
            let scopes = job["permissions"]
                .as_mapping()
                .expect("tests grants explicit scopes");
            let mut granted: Vec<(String, String)> = scopes
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().unwrap().to_string(),
                        v.as_str().unwrap().to_string(),
                    )
                })
                .collect();
            granted.sort();
            assert_eq!(
                granted,
                [
                    ("attestations".to_string(), "write".to_string()),
                    ("contents".to_string(), "read".to_string()),
                    ("id-token".to_string(), "write".to_string()),
                ],
                "tests grants what ci.yml's publish asks for, and nothing more"
            );
            assert_ne!(
                job["if"].as_str(),
                Some(GATE),
                "tests runs on a dispatch too"
            );
            continue;
        }
        // A reusable workflow handed secrets (`secrets: inherit` included).
        let handed_secrets = !job["secrets"].is_null();
        let writes = write_scope || handed_secrets || WRITES.iter().any(|w| body.contains(w));
        if writes {
            assert_eq!(
                job["if"].as_str(),
                Some(GATE),
                "{name} writes outside the run; only a pushed v* tag may reach it"
            );
            gated.push(name);
        } else {
            assert_ne!(job["if"].as_str(), Some(GATE), "{name} needs no gate");
        }
    }
    gated.sort();
    assert_eq!(gated, ["github-release", "publish-images"]);
    // The scripts the ungated jobs run write nothing either: release.sh's
    // registry writes are its `promote` arm's alone (publish-images runs it),
    // and release-build.sh, the `build` job, has none.
    let script = std::fs::read_to_string(root().join("scripts/release.sh")).unwrap();
    let build = std::fs::read_to_string(root().join("scripts/release-build.sh")).unwrap();
    for (file, text, allowed) in [
        ("scripts/release.sh", &script, Some("promote() {")),
        ("scripts/release-build.sh", &build, None),
    ] {
        let mut inside = false;
        for line in text.lines() {
            if allowed.is_some_and(|start| line.starts_with(start)) {
                inside = true;
            }
            let code = line.trim_start();
            if !inside && !code.starts_with('#') {
                for verb in WRITES.iter().filter(|w| **w != "secrets.") {
                    assert!(
                        !code.contains(verb),
                        "{file} writes (`{verb}`) outside the arm only a pushed tag runs: {line}"
                    );
                }
            }
            if inside && line == "}" {
                inside = false;
            }
        }
    }
    // The gate's own input: `release.sh validate` says `publish=true` for a
    // tag push and nothing else (scripts/test-release.py, Validate).
    assert!(script.contains("tag=\"$REF_NAME\" publish=true ;;"));
    assert!(script.contains("tag=\"${REHEARSAL_TAG:-}\" publish=false ;;"));
}

/// release.yml's `tests` job grants ci.yml id-token and attestations, which
/// only ci.yml's `publish` asks for. Every ci.yml job that asks for a write
/// scope, or is handed secrets, runs only on a push to main: inside a release
/// run (a tag push or a dispatch) its `if` is false, so nothing mints a token.
#[test]
fn the_release_tests_call_can_never_reach_ci_publish() {
    const MAIN_PUSH: &str = "github.event_name == 'push' && github.ref == 'refs/heads/main'";
    let ci = workflow("ci.yml");
    let mut credentialed = Vec::new();
    for (name, job) in ci["jobs"].as_mapping().unwrap() {
        let name = name.as_str().unwrap();
        let write_scope = match &job["permissions"] {
            Value::Null => false,
            Value::String(all) => all != "read-all",
            Value::Mapping(scopes) => scopes
                .values()
                .any(|v| !matches!(v.as_str(), Some("read") | Some("none"))),
            other => panic!("{name} has invalid permissions: {other:?}"),
        };
        if write_scope || !job["secrets"].is_null() {
            // The push-to-main test, alone or ANDed with more conditions (publish
            // also waits on its gates); an `||` anywhere could reopen it.
            let condition = job["if"].as_str().unwrap_or("");
            let rest = condition.strip_prefix(MAIN_PUSH);
            assert!(
                rest.is_some_and(|r| (r.is_empty() || r.starts_with(" && ")) && !r.contains("||")),
                "ci.yml's {name} asks for a write scope or secrets, so it must run only on a push to main: {condition}"
            );
            credentialed.push(name.to_string());
        }
    }
    assert_eq!(credentialed, ["publish"]);
}

/// A TAG DELETION IS A PUSH TOO, and so is a re-run
/// after a tag was re-cut. `validate` is handed `github.event.deleted` and
/// refuses unless it is `false`, then refuses a tag that origin no longer
/// points at this run's commit (scripts/test-release.py, Validate, runs both
/// against a real origin).
#[test]
fn a_deleted_or_re_cut_tag_publishes_nothing() {
    let release = workflow("release.yml");
    let step = release["jobs"]["validate"]["steps"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|s| s["id"] == "release")
        .expect("validate has its `release` step");
    assert_eq!(
        step["env"]["DELETED"].as_str(),
        Some("${{ github.event.deleted }}")
    );
    assert!(step["run"]
        .as_str()
        .is_some_and(|r| r.starts_with("bash scripts/release.sh validate")));
    let script = std::fs::read_to_string(root().join("scripts/release.sh")).unwrap();
    for needle in [
        "[[ \"${DELETED:-}\" == false ]]",
        "git ls-remote --tags origin \"refs/tags/$tag\" \"refs/tags/$tag^{}\"",
        "[[ -n \"$tagged\" ]] || die",
        "[[ \"$tagged\" == \"${GITHUB_SHA:?}\" ]]",
    ] {
        assert!(
            script.contains(needle),
            "scripts/release.sh validate must keep `{needle}`"
        );
    }
}

/// A GitHub Release is created only when
/// GitHub SAYS the tag has none (`release not found`); any other failed read
/// refuses rather than being taken for an absent release.
#[test]
fn a_release_is_created_only_when_github_says_there_is_none() {
    let release = workflow("release.yml");
    let create = release["jobs"]["github-release"]["steps"]
        .as_sequence()
        .unwrap()
        .iter()
        .filter_map(|s| s["run"].as_str())
        .find(|r| r.contains("gh release create"))
        .expect("github-release creates the release");
    let read = create
        .find("gh release view \"$TAG\" --repo \"$GITHUB_REPOSITORY\" > /dev/null 2> \"$RUNNER_TEMP/view.err\"")
        .expect("the release is looked up first, its error kept");
    let absent = create
        .find("elif grep -qxF 'release not found' \"$RUNNER_TEMP/view.err\"; then")
        .expect("absent only on gh's own answer");
    let created = create.find("gh release create").unwrap();
    let refused = create
        .find("could not tell whether $TAG has a GitHub Release; nothing created")
        .expect("any other failure is refused");
    assert!(read < absent && absent < created && created < refused);
}

/// THE PUBLISHED RELEASE IS VERIFIED AGAIN AFTER
/// DOWNLOAD. github-release's LAST step downloads what GitHub serves, checks it
/// with `release.sh verify`, compares every asset byte for byte with the one
/// `assemble` verified, and reads the release back as published (not a
/// draft); the step that creates it verifies the assets first.
#[test]
fn the_published_release_is_downloaded_and_verified_last() {
    let release = workflow("release.yml");
    let steps = release["jobs"]["github-release"]["steps"]
        .as_sequence()
        .unwrap();
    let last = steps.last().unwrap()["run"]
        .as_str()
        .expect("github-release ends in a run step");
    let mut from = 0;
    for needle in [
        "set -euo pipefail",
        "gh release download \"$TAG\" --repo \"$GITHUB_REPOSITORY\" --dir downloaded",
        "bash scripts/release.sh verify downloaded",
        "for asset in release-assets/release/assets/*; do cmp \"$asset\" \"downloaded/${asset##*/}\"; done",
        "[ \"$state\" = \"false $PRERELEASE $TAG\" ]",
    ] {
        let at = last[from..].find(needle).unwrap_or_else(|| {
            panic!("github-release's last step must run `{needle}`, in this order (review L-3)")
        });
        from += at + needle.len();
    }
    let create = steps
        .iter()
        .filter_map(|s| s["run"].as_str())
        .find(|r| r.contains("gh release create"))
        .expect("github-release creates the release");
    let verified = create
        .find("bash scripts/release.sh verify \"$assets\"")
        .expect("the assets are verified before the release is created");
    assert!(verified < create.find("gh release create").unwrap());
}

/// EVERY ACTION IN A JOB THAT HOLDS A PUBLISHING
/// CREDENTIAL IS PINNED BY COMMIT — publish-images (the Docker Hub token) and
/// github-release (`contents: write`) — with the release tag that commit
/// carried named beside it, the convention `azure/setup-helm` set.
#[test]
fn credentialed_release_jobs_pin_every_action_by_commit() {
    let release = workflow("release.yml");
    let text = std::fs::read_to_string(root().join(".github/workflows/release.yml")).unwrap();
    for job in ["publish-images", "github-release"] {
        let mut actions = Vec::new();
        for step in release["jobs"][job]["steps"].as_sequence().unwrap() {
            let Some(uses) = step["uses"].as_str() else {
                continue;
            };
            let (action, sha) = uses
                .split_once('@')
                .unwrap_or_else(|| panic!("{job}: `{uses}` names no version"));
            assert!(
                sha.len() == 40 && sha.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')),
                "{job}: `{uses}` is not pinned by a 40-hex commit (review L-7)"
            );
            let line = text
                .lines()
                .find(|l| {
                    l.trim_start()
                        .trim_start_matches("- ")
                        .starts_with(&format!("uses: {uses}"))
                })
                .unwrap();
            let tag = line
                .split_once(" # ")
                .map(|(_, c)| c.trim())
                .unwrap_or_else(|| {
                    panic!("{job}: `{uses}` must name its release tag in a comment")
                });
            let numbers: Vec<&str> = tag.strip_prefix('v').unwrap_or("").split('.').collect();
            assert!(
                numbers.len() == 3
                    && numbers
                        .iter()
                        .all(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())),
                "{job}: `{uses}` names `{tag}`, not a vX.Y.Z release tag"
            );
            actions.push(action.to_string());
        }
        assert!(
            actions.iter().any(|a| a == "actions/download-artifact"),
            "{job} pins its actions: {actions:?}"
        );
        if job == "publish-images" {
            for action in [
                "docker/login-action",
                "docker/setup-buildx-action",
                "azure/setup-helm",
            ] {
                assert!(
                    actions.iter().any(|a| a == action),
                    "publish-images uses {action}"
                );
            }
        }
    }
}

/// The CLI is the only release archive: `weirkeeper` and `logweir-retention`
/// ship in images only.
#[test]
fn the_cli_is_the_only_release_archive() {
    let cargo = std::fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let table = cargo
        .split("\n[workspace.metadata.dist]\n")
        .nth(1)
        .expect("Cargo.toml has [workspace.metadata.dist]");
    let table = table.split("\n[").next().unwrap();
    for line in [
        "dist = false",
        "precise-builds = true",
        "source-tarball = false",
        "include = [\"NOTICE\", \"THIRD_PARTY_NOTICES.md\"]",
        "allow-dirty = [\"ci\"]",
    ] {
        assert!(
            table.lines().any(|l| l.trim() == line),
            "[workspace.metadata.dist] must carry `{line}`"
        );
    }
    let cli = std::fs::read_to_string(root().join("crates/logweir/Cargo.toml")).unwrap();
    assert!(cli.contains("[package.metadata.dist]\ndist = true\n"));
    let mut members: Vec<PathBuf> = std::fs::read_dir(root().join("crates"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    members.push(root().join("xtask"));
    members.push(root().join("e2e"));
    for member in members {
        if member.ends_with("logweir") {
            continue;
        }
        let manifest = std::fs::read_to_string(member.join("Cargo.toml")).unwrap();
        assert!(
            !manifest.contains("[package.metadata.dist]"),
            "{} opts into the release archives; only the CLI ships as one",
            member.display()
        );
    }
}

#[test]
fn release_archives_are_built_and_checked_by_one_script_before_upload() {
    let release = workflow("release.yml");
    let build = &release["jobs"]["build"];
    let matrix: Vec<(String, String)> = build["strategy"]["matrix"]["include"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["target"].as_str().unwrap().to_string(),
                r["runner"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        matrix,
        [
            ("x86_64-unknown-linux-gnu".into(), "ubuntu-24.04".into()),
            (
                "aarch64-unknown-linux-gnu".into(),
                "ubuntu-24.04-arm".into()
            ),
            ("aarch64-apple-darwin".into(), "macos-14".into()),
        ],
        "one NATIVE runner per target: no cross toolchain"
    );
    let steps = build["steps"].as_sequence().unwrap();
    let linux = steps
        .iter()
        .position(|s| {
            s["if"] == "runner.os == 'Linux'"
                && s["run"].as_str().is_some_and(|r| {
                    r.contains(" rust:1.89-bookworm ")
                        && r.ends_with("bash scripts/release-build.sh \"$TARGET\"")
                })
        })
        .expect("Linux archives are built in the runner image's builder base");
    let mac = steps
        .iter()
        .position(|s| {
            s["if"] == "runner.os == 'macOS'"
                && s["run"] == "bash scripts/release-build.sh \"$TARGET\""
        })
        .expect("the macOS archive is built by the same script");
    let upload = steps
        .iter()
        .position(|s| s["uses"] == "actions/upload-artifact@v4")
        .unwrap();
    assert!(linux < upload && mac < upload);
    let dockerfile = std::fs::read_to_string(root().join("Dockerfile")).unwrap();
    // The runner's compiling stages derive from one `cross` stage
    // (the toolchain for either platform), and the builder is one of them.
    assert!(
        dockerfile.lines().any(|l| l
            .starts_with("FROM --platform=$BUILDPLATFORM rust:1.89-bookworm@sha256:")
            && l.ends_with(" AS cross"))
            && dockerfile.contains("FROM cross AS builder"),
        "the Linux archives' builder base is the runner image's; change both together"
    );
    let script = std::fs::read_to_string(root().join("scripts/release-build.sh")).unwrap();
    let mut last = 0;
    for step in [
        "\"$dist\" build --tag \"$RELEASE_TAG\" --force-tag --artifacts=local",
        "bash scripts/check-no-engine-in-binary.sh \"$archive\"",
        "drill countersign --help",
        "scripts/release-countersign-check.py \"$bin\"",
        "objdump -T \"$bin\"",
    ] {
        let at = script
            .find(step)
            .unwrap_or_else(|| panic!("scripts/release-build.sh no longer runs `{step}`"));
        assert!(
            at > last,
            "`{step}` runs out of order in scripts/release-build.sh"
        );
        last = at;
    }
    let ci_check = std::fs::read_to_string(root().join("scripts/ci-check.sh")).unwrap();
    assert!(
        ci_check.contains("bash scripts/check-no-engine-in-binary.sh \"$LOGWEIR_BIN\""),
        "CI must run the release engine check too, so it cannot first fail at a tag"
    );
    assert!(
        ci_check.contains("python3 scripts/test-release.py"),
        "CI runs the release script's tests"
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
    // FX-53: the shards also wait for the run's one engine build, which runs
    // exactly when they do.
    assert_eq!(dependencies(&jobs["e2e"]), ["changes", "engine"]);
    for job in ["e2e", "engine"] {
        assert_eq!(
            jobs[job]["if"].as_str(),
            Some("needs.changes.outputs.code == 'true'"),
            "{job}"
        );
    }
    assert_eq!(dependencies(&jobs["engine"]), ["changes"]);
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

/// FX-53: LOGWEIR'S ENGINE IS BUILT ONCE PER RUN, BY THE COMMAND THAT BUILT
/// IT IN EVERY SHARD, AND RESTORED ONLY UNDER A KEY OF ITS INPUTS. The shards
/// install the `engine` job's artifact over OSO's extracted binary and refuse
/// one that does not print the build env's ENGINE_VERSION; none of them
/// compiles it. Only main saves the cache.
#[test]
fn the_engine_is_built_once_per_run_from_its_inputs() {
    let ci = workflow("ci.yml");
    let engine = &ci["jobs"]["engine"];
    let steps = engine["steps"].as_sequence().unwrap();
    let run = |s: &Value| s["run"].as_str().unwrap_or("").to_string();
    let uses = |s: &Value| s["uses"].as_str().unwrap_or("").to_string();
    let at = |pred: &dyn Fn(&Value) -> bool, what: &str| {
        steps
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("ci.yml's engine job has no {what}"))
    };
    let key = at(
        &|s| run(s).contains("bash scripts/engine-source.sh digest"),
        "key step built from `engine-source.sh digest`",
    );
    let key_run = run(&steps[key]);
    for input in [
        "bash scripts/engine-source.sh check",
        "scripts/engine-source.sh",
        "third_party/kafka-backup-build.env",
        "rust-toolchain.toml",
        "$ENGINE_APT",
        "${RUNNER_OS}",
        "${RUNNER_ARCH}",
        "${ImageOS}",
        "${ImageVersion}",
    ] {
        assert!(
            key_run.contains(input),
            "the engine cache key leaves out `{input}`: {key_run}"
        );
    }
    let restore = at(
        &|s| uses(s).starts_with("actions/cache/restore@"),
        "cache restore",
    );
    let build = at(
        &|s| run(s) == "bash scripts/engine-source.sh build",
        "`bash scripts/engine-source.sh build`",
    );
    let save = at(&|s| uses(s).starts_with("actions/cache/save@"), "cache save");
    let identity = at(
        &|s| {
            run(s).contains("third_party/kafka-backup-build.env")
                && run(s).contains(".engine/kafka-backup --version")
        },
        "identity check against the build env",
    );
    let upload = at(
        &|s| uses(s).starts_with("actions/upload-artifact@"),
        "artifact upload",
    );
    assert!(key < restore && restore < build && build < save && save < identity && identity < upload);
    let miss = "steps.cache.outputs.cache-hit != 'true'";
    assert_eq!(steps[build]["if"].as_str(), Some(miss));
    assert_eq!(
        steps[save]["if"].as_str(),
        Some("steps.cache.outputs.cache-hit != 'true' && github.ref == 'refs/heads/main'"),
        "only main saves the engine cache"
    );
    for step in [restore, save] {
        assert_eq!(steps[step]["with"]["path"].as_str(), Some(".engine/kafka-backup"));
        assert_eq!(
            steps[step]["with"]["key"].as_str(),
            Some("${{ steps.inputs.outputs.key }}")
        );
    }
    assert_eq!(steps[upload]["with"]["name"].as_str(), Some("engine"));
    assert_eq!(steps[upload]["with"]["path"].as_str(), Some(".engine/kafka-backup"));

    let e2e = ci["jobs"]["e2e"]["steps"].as_sequence().unwrap();
    let text = serde_yaml::to_string(&ci["jobs"]["e2e"]).unwrap();
    assert!(
        !text.contains("engine-source.sh build"),
        "a shard compiles the engine again: {text}"
    );
    let shard_at = |pred: &dyn Fn(&Value) -> bool, what: &str| {
        e2e.iter()
            .position(pred)
            .unwrap_or_else(|| panic!("ci.yml's e2e job has no {what}"))
    };
    let extract = shard_at(&|s| run(s) == "./scripts/extract-engine.sh", "OSO extraction");
    let download = shard_at(
        &|s| uses(s).starts_with("actions/download-artifact@") && s["with"]["name"] == "engine",
        "download of the `engine` artifact",
    );
    let install = shard_at(
        &|s| {
            run(s).contains(".engine/kafka-backup")
                && run(s).contains("third_party/kafka-backup-build.env")
                && run(s).contains("--version")
        },
        "install over OSO's binary with the identity check",
    );
    let up = shard_at(&|s| run(s) == "just e2e-up", "`just e2e-up`");
    assert!(extract < download && download < install && install < up);
}

/// FX-53: EVERY RUST CACHE IN ci.yml IS ONE KEY PER JOB KIND, SAVED BY main
/// ONLY. A pull request's cache lives where no other run can read it; and the
/// shards share one key that exactly one of them saves.
#[test]
fn rust_caches_are_shared_per_job_kind_and_saved_by_main_only() {
    let ci = workflow("ci.yml");
    let mut kinds = Vec::new();
    for (name, job) in ci["jobs"].as_mapping().unwrap() {
        let Some(steps) = job["steps"].as_sequence() else {
            continue;
        };
        for step in steps {
            let Some(uses) = step["uses"].as_str() else {
                continue;
            };
            if !uses.starts_with("Swatinem/rust-cache@") {
                continue;
            }
            let sha = uses.split_once('@').unwrap().1;
            assert!(
                sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()),
                "{name:?}: `{uses}` is not pinned by commit"
            );
            let save_if = step["with"]["save-if"].as_str().unwrap_or_default();
            assert!(
                save_if.starts_with("${{ github.ref == 'refs/heads/main'"),
                "{name:?}: rust-cache must save on main only, not `{save_if}`"
            );
            kinds.push(step["with"]["shared-key"].as_str().unwrap().to_string());
        }
    }
    kinds.sort();
    assert_eq!(kinds, ["check", "e2e"]);
    let e2e = ci["jobs"]["e2e"]["steps"].as_sequence().unwrap();
    let cache = e2e
        .iter()
        .find(|s| s["uses"].as_str().is_some_and(|u| u.starts_with("Swatinem/rust-cache@")))
        .unwrap();
    assert_eq!(
        cache["with"]["save-if"].as_str(),
        Some("${{ github.ref == 'refs/heads/main' && matrix.crates == true }}"),
        "one shard saves the shared e2e key"
    );
    let savers = ci["jobs"]["e2e"]["strategy"]["matrix"]["include"]
        .as_sequence()
        .unwrap()
        .iter()
        .filter(|row| row["crates"].as_bool() == Some(true))
        .count();
    assert_eq!(savers, 1, "exactly one shard saves the e2e cache");
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
    // FX-53 GAP 1: THE VERIFIER IS CODE UNDER docs/. Every e2e harness user
    // runs `docs/verify_scorecard.py`, and a release ships it, so a change to
    // it or to its suite runs e2e; a document beside it does not.
    let prose = commit(dir, "docs/install.md");
    let verifier = commit(dir, "docs/verify_scorecard.py");
    assert_eq!(classify(dir, &prose, &verifier), "code=true");
    let suite = commit(dir, "docs/test_verify_scorecard.py");
    assert_eq!(classify(dir, &verifier, &suite), "code=true");
    let beside = commit(dir, "docs/verify_scorecard.md");
    assert_eq!(classify(dir, &suite, &beside), "code=false");
    let nested = commit(dir, "docs/formats/verify_scorecard.py");
    assert_eq!(classify(dir, &beside, &nested), "code=false");
    assert_eq!(classify(dir, &lookalike, &nested), "code=true");
}

/// PROD-18.1: the `console` job runs `just console` only when the diff touches
/// `console/` or the API schema its types are generated from, and whenever the
/// classifier cannot tell. Each `false` has a `true` twin one path away.
#[test]
fn the_console_job_runs_when_the_console_or_its_schema_changed() {
    let ci = workflow("ci.yml");
    let job = &ci["jobs"]["console"];
    assert_eq!(dependencies(job), ["changes"]);
    assert_eq!(
        job["if"].as_str(),
        Some("needs.changes.outputs.console == 'true'")
    );
    assert!(job["steps"]
        .as_sequence()
        .unwrap()
        .iter()
        .any(|s| s["run"] == "just console"));
    assert_eq!(
        ci["jobs"]["changes"]["outputs"]["console"].as_str(),
        Some("${{ steps.classify.outputs.console }}")
    );

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    git(dir, &["init", "-q"]);
    let base = commit(dir, "crates/a.rs");
    let console = |a: &str, b: &str| {
        let out = std::process::Command::new("bash")
            .arg(root().join("scripts/ci-changes.sh"))
            .args([a, b, "console"])
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    let docs = commit(dir, "docs/api.md");
    assert_eq!(console(&base, &docs), "console=false");
    let page = commit(dir, "console/src/main.tsx");
    assert_eq!(console(&docs, &page), "console=true");
    let rust = commit(dir, "crates/b.rs");
    assert_eq!(console(&page, &rust), "console=false");
    let schema = commit(dir, "schemas/logweir-api-v1.openapi.json");
    assert_eq!(console(&rust, &schema), "console=true");
    // The recorded documents the console tests import live under console/.
    let fixture = commit(dir, "console/fixtures/console/session.json");
    assert_eq!(console(&schema, &fixture), "console=true");
    let other_fixture = commit(
        dir,
        "crates/logweir-api/tests/fixtures/backup-succeeded-verified.json",
    );
    assert_eq!(console(&fixture, &other_fixture), "console=false");
    let other_schema = commit(dir, "schemas/logweir-receipt.json");
    assert_eq!(console(&other_fixture, &other_schema), "console=false");
    let lookalike = commit(dir, "console-notes/x.md");
    assert_eq!(console(&other_schema, &lookalike), "console=false");
    assert_eq!(console("", &lookalike), "console=true");
    // The default question is unchanged.
    assert_eq!(classify(dir, &base, &docs), "code=false");
}

/// EVERY IMAGE BASE IS PINNED BY DIGEST. `images.yml` pulls Docker Hub bases
/// through a public mirror (rate limits); a digest makes the mirror serve
/// exactly those bytes or nothing, because BuildKit verifies the content
/// address. A tag would let the mirror choose. So every `FROM` in every
/// Dockerfile names `image@sha256:<64 hex>` or an earlier build stage.
#[test]
fn every_dockerfile_base_is_pinned_by_digest() {
    let mut checked = 0;
    for entry in std::fs::read_dir(root()).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.starts_with("Dockerfile") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut stages: Vec<String> = Vec::new();
        for line in text.lines() {
            let Some(rest) = line.trim_start().strip_prefix("FROM ") else {
                continue;
            };
            let words: Vec<&str> = rest
                .split_whitespace()
                .filter(|w| !w.starts_with("--platform="))
                .collect();
            let image = words[0];
            if let Some(i) = words.iter().position(|w| w.eq_ignore_ascii_case("AS")) {
                stages.push(words[i + 1].to_string());
            }
            let is_stage = stages.iter().any(|s| s == image) || image.contains("${");
            let pinned = image
                .split_once("@sha256:")
                .is_some_and(|(_, d)| d.len() == 64 && d.chars().all(|c| c.is_ascii_hexdigit()));
            assert!(
                is_stage || pinned,
                "{name}: `{line}` names a base by tag; pin it by digest (image@sha256:...)"
            );
            checked += 1;
        }
    }
    assert!(checked >= 8, "only {checked} FROM lines checked");
}
