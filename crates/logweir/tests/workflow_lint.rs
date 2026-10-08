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
    // A RELEASE publishes the chart package it LISTS (PROD-14.0): packaged once
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
        "[[ \"$rewritten\" -eq 4 ]]",
        "helm registry login registry-1.docker.io",
        "--password-stdin",
        "helm push \"$package\" \"$CHART_REPOSITORY\"",
        "helm pull \"$CHART_REPOSITORY/$CHART_NAME\" --version \"$version\"",
        "[[ \"$pushed\" == \"$served\" ]]",
        "docker buildx imagetools inspect \"docker.io/$NS/$product:$TAG\"",
        // "Public" is asked anonymously (review L5).
        "DOCKER_CONFIG=\"$anonymous_docker\" docker buildx imagetools inspect",
        "DOCKER_CONFIG=\"$anonymous_docker\" HELM_REGISTRY_CONFIG=\"$dir/anonymous/config.json\"",
        "HELM_REGISTRY_CONFIG=\"$dir/login/config.json\" helm push",
        // PROD-14.0: a release chart pins digests, and a published release
        // version is never replaced.
        "a release chart ($TAG) pins its four images by digest",
        "  chart-push)",
        "chart_publish \"$package\" \"$version\" replace",
        "chart_publish \"$package\" \"$version\" immutable",
        "a release version is never replaced",
        // An immutable push only on the registry's own "not found" for THIS
        // version; any other failed read refuses before the login (review M-1).
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

/// PROD-14.0: A DISPATCH IS A DRY RUN. Every job that can write outside the
/// run — ANY write scope of its token, a credential, a release, a tag push, a
/// registry write, a GitHub API call — carries the one gate that admits a
/// pushed `v*` tag and nothing else, so a dispatched run ends at `assemble`
/// with workflow artifacts only. Review L-3: the classification covers every
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

/// PROD-14.0 (review L-5): A TAG DELETION IS A PUSH TOO, and so is a re-run
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

/// PROD-14.0 (review M-1, swept): a GitHub Release is created only when
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

/// PROD-14.0 (review L-3): THE PUBLISHED RELEASE IS VERIFIED AGAIN AFTER
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

/// PROD-14.0 (review L-7): EVERY ACTION IN A JOB THAT HOLDS A PUBLISHING
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

/// PROD-14.0: the CLI is the only release archive. Every v0.1.x plan
/// announced `weirkeeper` too, and the current graph adds `logweir-retention`;
/// both ship in images only.
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
    // PROD-00.2: the runner's compiling stages derive from one `cross` stage
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
