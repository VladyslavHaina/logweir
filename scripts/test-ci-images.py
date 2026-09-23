#!/usr/bin/env python3
"""Exercise image promotion against an isolated, in-memory registry stand-in.

No Docker daemon, credentials, Git remote or network is used. The stand-in
records actual subprocess calls and tracks tag-to-digest relationships.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SHA = "a" * 40
NS = "promotion-test"
PLATFORMS = {
    "logweir": ("amd64",),
    "weirkeeper": ("amd64", "arm64"),
    "logweir-ui": ("amd64", "arm64"),
    "logweir-console": ("amd64", "arm64"),
}

DOCKER = r'''#!/usr/bin/env python3
import hashlib, json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["MOCK_DOCKER_LOG"], "a") as out:
    out.write(json.dumps(args) + "\n")
state_path = pathlib.Path(os.environ["MOCK_REGISTRY"])
state = json.loads(state_path.read_text())
if args[:3] == ["buildx", "imagetools", "inspect"]:
    ref = args[3]
    if os.environ.get("MOCK_DOCKER_ENV_LOG"):
        with open(os.environ["MOCK_DOCKER_ENV_LOG"], "a") as out:
            out.write(json.dumps({"ref": ref, "docker_config": os.environ.get("DOCKER_CONFIG")}) + "\n")
    if ref == os.environ.get("MOCK_FAIL_INSPECT_REF") or ref not in state:
        sys.exit(1)
    record = state[ref]
    digest = record["digest"]
    if ref.endswith(":latest") and os.environ.get("MOCK_WRONG_ROLLING"):
        digest = "sha256:" + "f" * 64
    if "--format" in args:
        template = args[args.index("--format") + 1]
        if template != "{{json .Manifest}}":
            print("Name: " + ref + "\nDigest: " + digest)
            sys.exit(0)
    print(json.dumps({**record, "digest": digest}))
elif args[:3] == ["buildx", "imagetools", "create"]:
    tags, refs = [], []
    i = 3
    while i < len(args):
        if args[i] == "--tag":
            tags.append(args[i + 1]); i += 2
        elif args[i] == "--prefer-index=false":
            i += 1
        else:
            refs.append(args[i]); i += 1
    if not tags or not refs or any(ref not in state for ref in refs):
        sys.exit(2)
    if len(refs) == 1:
        record = state[refs[0]]
    else:
        record = {
            "digest": "sha256:" + hashlib.sha256(json.dumps(refs).encode()).hexdigest(),
            "platforms": [p for ref in refs for p in state[ref]["platforms"]],
        }
    for tag in tags:
        state[tag] = record
        state[tag.rsplit(":", 1)[0] + "@" + record["digest"]] = record
    state_path.write_text(json.dumps(state))
else:
    print("unexpected docker call: " + repr(args), file=sys.stderr)
    sys.exit(2)
'''

GIT = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["MOCK_GIT_LOG"], "a") as out:
    out.write(json.dumps(sys.argv[1:]) + "\n")
if sys.argv[1:] != ["ls-remote", "origin", "refs/heads/main"]:
    sys.exit(2)
print(os.environ["MOCK_MAIN_SHA"] + "\trefs/heads/main")
'''


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="logweir-image-promotion-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "scripts").mkdir()
        shutil.copyfile(ROOT / "scripts/ci-images.sh", self.root / "scripts/ci-images.sh")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, text in (("docker", DOCKER), ("git", GIT)):
            executable = self.bin / name
            executable.write_text(text)
            executable.chmod(0o755)
        self.artifacts = self.root / "image-digests"
        self.artifacts.mkdir()
        self.registry = self.root / "registry.json"
        self.docker_log = self.root / "docker.jsonl"
        self.git_log = self.root / "git.jsonl"
        self.env = {
            **os.environ,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "GITHUB_SHA": SHA,
            "NS": NS,
            "TAG": "sha-" + SHA,
            "GITHUB_REF": "refs/heads/main",
            "PROMOTE_LATEST": "true",
            "GITHUB_OUTPUT": str(self.root / "output"),
            "GITHUB_STEP_SUMMARY": str(self.root / "summary"),
            "MOCK_REGISTRY": str(self.registry),
            "MOCK_DOCKER_LOG": str(self.docker_log),
            "MOCK_GIT_LOG": str(self.git_log),
            "MOCK_MAIN_SHA": SHA,
        }
        self.env.pop("MOCK_FAIL_INSPECT_REF", None)
        self.env.pop("MOCK_WRONG_ROLLING", None)
        self.expected_refs = {}
        state = {}
        for product, arches in PLATFORMS.items():
            self.expected_refs[product] = []
            for arch in arches:
                digest = "sha256:" + hashlib.sha256(f"{product}-{arch}".encode()).hexdigest()
                ref = f"docker.io/{NS}/{product}@{digest}"
                self.expected_refs[product].append(ref)
                state[ref] = {"digest": digest, "platforms": [f"linux/{arch}"]}
                self.artifact(product, arch).write_text(json.dumps({
                    "product": product, "arch": arch, "sha": SHA, "digest": digest,
                }))
        self.registry.write_text(json.dumps(state))
        self.initial_state = state

    def artifact(self, product, arch):
        return self.artifacts / f"{product}-{arch}.json"

    def run_promotion(self, success=True):
        result = subprocess.run(
            ["bash", str(self.root / "scripts/ci-images.sh"), "promote"],
            env=self.env, cwd=self.root, capture_output=True, text=True, timeout=30,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def calls(self):
        if not self.docker_log.exists():
            return []
        return [json.loads(line) for line in self.docker_log.read_text().splitlines()]

    def writes(self):
        return [call for call in self.calls() if call[:3] == ["buildx", "imagetools", "create"]]

    def assert_no_publication(self):
        self.assertEqual(self.writes(), [], "refused input changed a public image tag")
        self.assertEqual(json.loads(self.registry.read_text()), self.initial_state)
        output = Path(self.env["GITHUB_OUTPUT"])
        self.assertFalse(output.exists() and output.read_text(), "refused input exported a release digest")

    def test_missing_candidate_refuses_before_publishing_any_product(self):
        for product, arches in PLATFORMS.items():
            for arch in arches:
                with self.subTest(product=product, arch=arch):
                    artifact = self.artifact(product, arch)
                    original = artifact.read_text()
                    artifact.unlink()
                    self.run_promotion(success=False)
                    self.assert_no_publication()
                    artifact.write_text(original)

    def test_candidate_identity_and_digest_must_match_before_publication(self):
        artifact = self.artifact("logweir-ui", "arm64")
        original = json.loads(artifact.read_text())
        for field, value in (("sha", "b" * 40), ("product", "logweir"),
                             ("arch", "amd64"), ("digest", "not-a-digest")):
            with self.subTest(field=field):
                artifact.write_text(json.dumps({**original, field: value}))
                self.run_promotion(success=False)
                self.assert_no_publication()
        artifact.write_text(json.dumps(original))

    def test_unavailable_candidate_refuses_before_publication(self):
        self.env["MOCK_FAIL_INSPECT_REF"] = self.expected_refs["logweir-ui"][-1]
        self.run_promotion(success=False)
        self.assert_no_publication()

    def test_current_main_publishes_all_platforms_and_both_rolling_tags(self):
        self.run_promotion()
        state = json.loads(self.registry.read_text())
        writes = self.writes()
        self.assertEqual(len(writes), 2 * len(PLATFORMS))
        for product, arches in PLATFORMS.items():
            repo = f"docker.io/{NS}/{product}"
            immutable = repo + ":sha-" + SHA
            creation = next(call for call in writes if immutable in call)
            self.assertEqual([arg for arg in creation if "@sha256:" in arg], self.expected_refs[product])
            published = state[immutable]
            self.assertEqual(published["platforms"], [f"linux/{arch}" for arch in arches])
            self.assertEqual(state[repo + ":main"], published)
            self.assertEqual(state[repo + ":latest"], published)
            # The release output must describe exactly the digest now reachable by both tags.
            output_name = {
                "logweir": "runner_digest",
                "weirkeeper": "controller_digest",
                "logweir-ui": "ui_digest",
                "logweir-console": "console_digest",
            }[product]
            self.assertIn(f"{output_name}={published['digest']}\n", Path(self.env["GITHUB_OUTPUT"]).read_text())
            inspected = [call[3] for call in self.calls() if call[:3] == ["buildx", "imagetools", "inspect"]]
            self.assertIn(repo + ":main", inspected)
            self.assertIn(repo + ":latest", inspected)

    def test_stale_main_keeps_existing_rolling_tags(self):
        self.env["MOCK_MAIN_SHA"] = "b" * 40
        state = json.loads(self.registry.read_text())
        previous = {"digest": "sha256:" + "c" * 64, "platforms": ["linux/amd64"]}
        for product in PLATFORMS:
            for tag in ("main", "latest"):
                state[f"docker.io/{NS}/{product}:{tag}"] = previous
        self.registry.write_text(json.dumps(state))
        self.run_promotion()
        state = json.loads(self.registry.read_text())
        self.assertEqual(len(self.writes()), len(PLATFORMS))
        for product in PLATFORMS:
            for tag in ("main", "latest"):
                self.assertEqual(state[f"docker.io/{NS}/{product}:{tag}"], previous)

    def test_version_release_does_not_move_main_or_latest(self):
        self.env.update(TAG="v1.2.3", GITHUB_REF="refs/tags/v1.2.3", PROMOTE_LATEST="false")
        self.run_promotion()
        state = json.loads(self.registry.read_text())
        self.assertEqual(len(self.writes()), len(PLATFORMS))
        for product in PLATFORMS:
            repo = f"docker.io/{NS}/{product}"
            self.assertIn(repo + ":v1.2.3", state)
            self.assertNotIn(repo + ":main", state)
            self.assertNotIn(repo + ":latest", state)
        self.assertFalse(self.git_log.exists(), "version publishing should not depend on main branch availability")

    def test_invalid_rolling_promotion_refuses_before_publication(self):
        self.env.update(TAG="v1.2.3", GITHUB_REF="refs/tags/v1.2.3", PROMOTE_LATEST="true")
        self.run_promotion(success=False)
        self.assert_no_publication()

    def test_wrong_rolling_digest_fails_verification(self):
        self.env["MOCK_WRONG_ROLLING"] = "1"
        result = self.run_promotion(success=False)
        self.assertIn("Tag verification failed", result.stderr)

    def test_runner_candidate_executes_identity_bootstrap_help_before_promotion(self):
        image_check = (ROOT / "scripts/check-image.sh").read_text()
        self.assertIn(
            'docker run --rm --platform "$PLATFORM" "$ref" identity bootstrap --help',
            image_check,
        )
        publication = (ROOT / "scripts/ci-images.sh").read_text()
        pull = publication.index('docker pull --platform "linux/$ARCH" "$repo@$digest"')
        exact_check = publication.index('check_image "$product" "$repo@$digest"', pull)
        promote = publication.index('  promote)')
        self.assertLess(pull, exact_check)
        self.assertLess(exact_check, promote)


# A Helm stand-in: `lint`, `template` and `registry login` succeed (login
# records whether the password came on stdin); `package` writes a real tarball
# of the chart directory plus the version and appVersion it was given; `push`
# stores the bytes under repo/name:version; `pull` writes them back, or other
# bytes under MOCK_CORRUPT_PULL. Every call is logged WITHOUT stdin.
HELM = r'''#!/usr/bin/env python3
import json, os, pathlib, sys, tarfile, io
args = sys.argv[1:]
with open(os.environ["MOCK_HELM_LOG"], "a") as out:
    out.write(json.dumps(args) + "\n")
state_path = pathlib.Path(os.environ["MOCK_CHARTS"])
state = json.loads(state_path.read_text()) if state_path.exists() else {}
if args[:1] in (["lint"], ["template"]):
    sys.exit(0)
if args[:2] == ["registry", "login"]:
    secret = sys.stdin.read() if "--password-stdin" in args else ""
    pathlib.Path(os.environ["MOCK_HELM_LOGIN"]).write_text(json.dumps({"stdin": secret, "args": args}))
    sys.exit(0)
if args[:1] == ["package"]:
    chart = pathlib.Path(args[1])
    version = args[args.index("--version") + 1]
    app = args[args.index("--app-version") + 1]
    out = pathlib.Path(args[args.index("-d") + 1])
    name = [l.split(": ", 1)[1] for l in (chart / "Chart.yaml").read_text().splitlines() if l.startswith("name: ")][0]
    target = out / f"{name}-{version}.tgz"
    with tarfile.open(target, "w:gz") as tar:
        tar.add(chart, arcname=name)
        meta = json.dumps({"version": version, "appVersion": app}).encode()
        info = tarfile.TarInfo(f"{name}/__mock_package.json"); info.size = len(meta)
        tar.addfile(info, io.BytesIO(meta))
    print(f"Successfully packaged chart and saved it to: {target}")
    sys.exit(0)
if args[:1] == ["push"]:
    package = pathlib.Path(args[1]); repo = args[2]
    stem = package.name[:-4]
    state[repo + "/" + stem] = package.read_bytes().hex()
    state_path.write_text(json.dumps(state))
    sys.exit(0)
if args[:1] == ["pull"]:
    ref = args[1]; version = args[args.index("--version") + 1]; out = pathlib.Path(args[args.index("-d") + 1])
    pathlib.Path(os.environ["MOCK_HELM_PULL_ENV"]).write_text(json.dumps(
        {"registry_config": os.environ.get("HELM_REGISTRY_CONFIG")}))
    name = ref.rsplit("/", 1)[1]
    key = ref.rsplit("/", 1)[0] + "/" + f"{name}-{version}"
    if key not in state:
        sys.exit(1)
    data = bytes.fromhex(state[key])
    if os.environ.get("MOCK_CORRUPT_PULL"):
        data = data + b"tampered"
    (out / f"{name}-{version}.tgz").write_bytes(data)
    sys.exit(0)
print("unexpected helm call: " + repr(args), file=sys.stderr)
sys.exit(2)
'''


class ChartPublicationTests(unittest.TestCase):
    """Chart gap G4: the chart is published after the images, versioned with
    them, with the image credentials on stdin, and verified by content. The
    registry stand-in and the promotion are PromotionTests' own, borrowed rather
    than inherited so the promotion rows do not run twice."""

    TOKEN = "dckr_pat_" + "x" * 20
    artifact = PromotionTests.artifact
    run_promotion = PromotionTests.run_promotion

    def setUp(self):
        PromotionTests.setUp(self)
        shutil.copytree(ROOT / "charts/logweir", self.root / "charts/logweir",
                        ignore=shutil.ignore_patterns("rendered"))
        helm = self.bin / "helm"
        helm.write_text(HELM)
        helm.chmod(0o755)
        self.helm_log = self.root / "helm.jsonl"
        self.charts = self.root / "charts.json"
        self.login = self.root / "login.json"
        self.docker_env = self.root / "docker-env.jsonl"
        self.pull_env = self.root / "pull-env.json"
        self.env.update(
            MOCK_DOCKER_ENV_LOG=str(self.docker_env), MOCK_HELM_PULL_ENV=str(self.pull_env),
            MOCK_HELM_LOG=str(self.helm_log), MOCK_CHARTS=str(self.charts),
            MOCK_HELM_LOGIN=str(self.login), DOCKERHUB_USERNAME="publisher",
            DOCKERHUB_TOKEN=self.TOKEN,
        )

    def run_chart(self, success=True):
        result = subprocess.run(
            ["bash", str(self.root / "scripts/ci-images.sh"), "chart"],
            env=self.env, cwd=self.root, capture_output=True, text=True, timeout=60,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def helm_calls(self):
        if not self.helm_log.exists():
            return []
        return [json.loads(line) for line in self.helm_log.read_text().splitlines()]

    def pushed(self):
        return json.loads(self.charts.read_text()) if self.charts.exists() else {}

    def packaged(self, key):
        import tarfile, io
        data = bytes.fromhex(self.pushed()[key])
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            meta = json.loads(tar.extractfile("logweir-chart/__mock_package.json").read())
            values = tar.extractfile("logweir-chart/values.yaml").read().decode()
            chart = tar.extractfile("logweir-chart/Chart.yaml").read().decode()
        return meta, values, chart

    def test_main_publishes_the_chart_versioned_with_the_images(self):
        self.run_promotion()
        self.run_chart()
        version = "0.1.0-sha-" + SHA
        key = f"oci://registry-1.docker.io/{NS}/logweir-chart-{version}"
        meta, values, chart = self.packaged(key)
        self.assertEqual(meta, {"version": version, "appVersion": "sha-" + SHA})
        self.assertIn("name: logweir-chart\n", chart)
        for image in ("weirkeeper", "logweir", "logweir-console", "logweir-ui"):
            self.assertIn(f"docker.io/{NS}/{image}:sha-{SHA}", values)
        self.assertNotIn(":latest", values)
        self.assertIn(f"chart_version={version}\n", Path(self.env["GITHUB_OUTPUT"]).read_text())
        # The token reached Helm on stdin and on no command line.
        login = json.loads(self.login.read_text())
        self.assertEqual(login["stdin"], self.TOKEN)
        self.assertNotIn(self.TOKEN, self.helm_log.read_text())
        pull = next(c for c in self.helm_calls() if c[0] == "pull")
        self.assertEqual(pull[:4], ["pull", f"oci://registry-1.docker.io/{NS}/logweir-chart",
                                    "--version", version])
        push = next(i for i, c in enumerate(self.helm_calls()) if c[0] == "push")
        self.assertLess(push, self.helm_calls().index(pull), "verified after the push")
        # "Public" is asked anonymously (review L5): the four image inspections
        # of the chart step run with a fresh Docker config holding no login, and
        # the pull-back with a Helm registry configuration that does not exist.
        chart_inspects = [json.loads(line) for line in self.docker_env.read_text().splitlines()]
        chart_inspects = [i for i in chart_inspects
                          if i["ref"].endswith(f":sha-{SHA}") and i["docker_config"]]
        self.assertEqual(len(chart_inspects), 4, chart_inspects)
        registry_config = json.loads(self.pull_env.read_text())["registry_config"]
        self.assertTrue(registry_config, "the pull-back must name its own registry config")
        self.assertFalse(Path(registry_config).exists(), "the pull-back must carry no registry login")

    def test_a_release_tag_publishes_the_semver_chart(self):
        self.env.update(TAG="v1.2.3", GITHUB_REF="refs/tags/v1.2.3", PROMOTE_LATEST="false")
        self.run_promotion()
        self.run_chart()
        meta, values, _ = self.packaged(f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3")
        self.assertEqual(meta, {"version": "1.2.3", "appVersion": "v1.2.3"})
        self.assertIn(f"docker.io/{NS}/weirkeeper:v1.2.3", values)

    def test_no_chart_before_its_images_are_public(self):
        # Promotion did not run: no image carries TAG yet.
        self.run_chart(success=False)
        self.assertEqual(self.pushed(), {})
        self.assertFalse(any(c[:1] == ["push"] for c in self.helm_calls()))

    def test_a_tag_that_is_not_a_version_publishes_nothing(self):
        self.run_promotion()
        self.env["TAG"] = "main"
        self.run_chart(success=False)
        self.assertEqual(self.pushed(), {})

    def test_different_bytes_served_back_fail_the_publication(self):
        self.run_promotion()
        self.env["MOCK_CORRUPT_PULL"] = "1"
        result = self.run_chart(success=False)
        self.assertIn("different chart bytes", result.stderr)
        self.assertNotIn("chart_version=", Path(self.env["GITHUB_OUTPUT"]).read_text())


if __name__ == "__main__":
    unittest.main()
