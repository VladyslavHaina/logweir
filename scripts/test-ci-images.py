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
    if ref == os.environ.get("MOCK_UNREACHABLE_REF"):
        # No answer about the reference: a refused connection (as measured), or
        # the text MOCK_UNREACHABLE_ERROR names.
        print(os.environ.get("MOCK_UNREACHABLE_ERROR")
              or f"ERROR: failed to do request: Head \"https://registry-1.docker.io/v2/{ref}\": dial tcp: connection refused",
              file=sys.stderr)
        sys.exit(1)
    if ref == os.environ.get("MOCK_FAIL_INSPECT_REF") or ref not in state:
        print(f"ERROR: {ref}: not found", file=sys.stderr)
        sys.exit(1)
    record = state[ref]
    digest = record["digest"]
    if ref.endswith(":latest") and os.environ.get("MOCK_WRONG_ROLLING"):
        digest = "sha256:" + "f" * 64
    if "--format" in args:
        template = args[args.index("--format") + 1]
        if template == "{{json .Image}}":
            # One platform's configuration, or a map of them for a list.
            print(json.dumps(record.get("image", {})))
            sys.exit(0)
        if template != "{{json .Manifest}}":
            print("Name: " + ref + "\nDigest: " + digest)
            sys.exit(0)
    print(json.dumps({k: v for k, v in {**record, "digest": digest}.items() if k != "image"}))
elif args[:3] == ["buildx", "imagetools", "create"]:
    tags, refs = [], []
    prefer_index = True
    i = 3
    while i < len(args):
        if args[i] == "--tag":
            tags.append(args[i + 1]); i += 2
        elif args[i] == "--prefer-index=false":
            prefer_index = False; i += 1
        else:
            refs.append(args[i]); i += 1
    if not tags or not refs or any(ref not in state for ref in refs):
        sys.exit(2)
    if len(refs) == 1 and not prefer_index and not os.environ.get("MOCK_REWRAP"):
        # One source and no index: a carbon copy, the source's own digest.
        record = state[refs[0]]
    elif len(refs) == 1:
        # RE-WRAPPED: buildx's default (--prefer-index=true) wraps a single
        # manifest in a NEW index, and MOCK_REWRAP does so whatever the flag
        # says (review L-4). Either way the tag names a digest the source
        # never had.
        source = state[refs[0]]
        record = {**source, "digest": "sha256:" + hashlib.sha256(("index:" + refs[0]).encode()).hexdigest()}
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
        for knob in ("MOCK_FAIL_INSPECT_REF", "MOCK_WRONG_ROLLING", "MOCK_UNREACHABLE_REF", "MOCK_UNREACHABLE_ERROR",
                     "MOCK_REWRAP", "MOCK_HELM_UNREACHABLE", "MOCK_CORRUPT_PULL"):
            self.env.pop(knob, None)
        # Hermetic: a caller's registry configuration (a local gate run with
        # DOCKER_CONFIG set) must not reach the script, or the recorded
        # per-call configurations stop meaning what the assertions read.
        self.env.pop("DOCKER_CONFIG", None)
        self.env.pop("HELM_REGISTRY_CONFIG", None)
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
            env=self.env, cwd=self.root, capture_output=True, text=True, timeout=120,
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
if args[:2] in (["show", "values"], ["show", "chart"]):
    with tarfile.open(args[2]) as tar:
        top = tar.getnames()[0].split("/")[0]
        if args[1] == "values":
            sys.stdout.write(tar.extractfile(f"{top}/values.yaml").read().decode())
        else:
            meta = json.loads(tar.extractfile(f"{top}/__mock_package.json").read())
            for line in tar.extractfile(f"{top}/Chart.yaml").read().decode().splitlines():
                if line.startswith("version:"):
                    line = "version: " + meta["version"]
                elif line.startswith("appVersion:"):
                    line = "appVersion: " + meta["appVersion"]
                print(line)
    sys.exit(0)
if args[:2] == ["registry", "login"]:
    secret = sys.stdin.read() if "--password-stdin" in args else ""
    pathlib.Path(os.environ["MOCK_HELM_LOGIN"]).write_text(json.dumps(
        {"stdin": secret, "args": args, "registry_config": os.environ.get("HELM_REGISTRY_CONFIG")}))
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
    docker_config = os.environ.get("DOCKER_CONFIG")
    pathlib.Path(os.environ["MOCK_HELM_PULL_ENV"]).write_text(json.dumps({
        "registry_config": os.environ.get("HELM_REGISTRY_CONFIG"),
        "docker_config": docker_config,
        "docker_config_entries": sorted(os.listdir(docker_config))
            if docker_config and os.path.isdir(docker_config) else None}))
    reference = ref[len("oci://"):] if ref.startswith("oci://") else ref
    unreachable = os.environ.get("MOCK_HELM_UNREACHABLE")
    if unreachable:
        # The registry gave no answer: "1" is the refusal measured for an
        # unresolvable host; any other value is printed as Helm's error.
        host, path = reference.split("/", 1)
        print(f'Error: failed to perform "FetchReference" on source: Get "https://{host}/v2/{path}/manifests/{version}": '
              f'dial tcp: lookup {host}: no such host' if unreachable == "1" else unreachable, file=sys.stderr)
        sys.exit(1)
    name = ref.rsplit("/", 1)[1]
    key = ref.rsplit("/", 1)[0] + "/" + f"{name}-{version}"
    if key not in state:
        # Helm v4.0.1's answer for a version the registry does not have,
        # measured anonymously against Docker Hub on 2026-10-05.
        print(f'Error: failed to perform "FetchReference" on source: {reference}:{version}: not found', file=sys.stderr)
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
        pull_env = json.loads(self.pull_env.read_text())
        registry_config = pull_env["registry_config"]
        self.assertTrue(registry_config, "the pull-back must name its own registry config")
        self.assertFalse(Path(registry_config).exists(), "the pull-back must carry no registry login")
        # Helm falls back to Docker's stored credentials: the pull-back must
        # also run with an EMPTY Docker configuration directory (re-check L-rf1).
        self.assertTrue(pull_env["docker_config"], "the pull-back must name an empty DOCKER_CONFIG")
        self.assertEqual(pull_env["docker_config_entries"], [],
                         "the pull-back's DOCKER_CONFIG must be an existing, empty directory")
        # The login is scoped to its own registry config, not the default one.
        self.assertTrue(login["registry_config"], "the login must use its own registry config")
        self.assertNotEqual(login["registry_config"], registry_config)

    def test_a_release_tag_publishes_the_semver_chart(self):
        self.env.update(TAG="v1.2.3", GITHUB_REF="refs/tags/v1.2.3", PROMOTE_LATEST="false")
        self.run_promotion()
        self.run_chart()
        meta, values, _ = self.packaged(f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3")
        self.assertEqual(meta, {"version": "1.2.3", "appVersion": "v1.2.3"})
        # PROD-14.0: a release chart pins each image by the digest `promote`
        # just published under the tag, not by the tag alone.
        state = json.loads(self.registry.read_text())
        for image in PLATFORMS:
            digest = state[f"docker.io/{NS}/{image}:v1.2.3"]["digest"]
            self.assertIn(f"docker.io/{NS}/{image}:v1.2.3@{digest}", values)

    def package(self, tag, digests):
        """`chart-package` for TAG with IMAGE_DIGESTS = digests (None: unset)."""
        env = {**self.env, "TAG": tag}
        env.pop("IMAGE_DIGESTS", None)
        if digests is not None:
            path = self.root / "digests.json"
            path.write_text(json.dumps(digests))
            env["IMAGE_DIGESTS"] = str(path)
        return subprocess.run(
            ["bash", str(self.root / "scripts/ci-images.sh"), "chart-package", str(self.root / "pkg")],
            env=env, cwd=self.root, capture_output=True, text=True, timeout=60,
        )

    RELEASE_DIGESTS = {image: "sha256:" + hashlib.sha256(image.encode()).hexdigest() for image in PLATFORMS}

    def test_a_release_chart_without_digests_is_refused(self):
        result = self.package("v1.2.3", None)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("pins its four images by digest", result.stderr)
        self.assertFalse((self.root / "pkg/logweir-chart-1.2.3.tgz").exists())
        # A digest missing for ONE image refuses the whole package too.
        partial = dict(self.RELEASE_DIGESTS)
        partial.pop("logweir-ui")
        result = self.package("v1.2.3", partial)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("no sha256 digest for logweir-ui", result.stderr)
        # A main chart keeps its tag: no digests asked for, none written.
        result = self.package("sha-" + SHA, None)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def push(self, package, success=True):
        result = subprocess.run(
            ["bash", str(self.root / "scripts/ci-images.sh"), "chart-push", str(package)],
            env=self.env, cwd=self.root, capture_output=True, text=True, timeout=60,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def release_package(self):
        self.env.update(TAG="v1.2.3", GITHUB_REF="refs/tags/v1.2.3", PROMOTE_LATEST="false")
        self.run_promotion()
        result = self.package("v1.2.3", self.RELEASE_DIGESTS)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        package = Path(result.stdout.strip())
        self.assertEqual(package.name, "logweir-chart-1.2.3.tgz")
        return package

    def test_chart_push_publishes_exactly_the_package_it_is_given(self):
        package = self.release_package()
        result = self.push(package)
        # Pushed because the registry SAID the version is absent (review M-1).
        self.assertIn("logweir-chart 1.2.3 is not published yet (the registry says not found)", result.stderr)
        key = f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3"
        self.assertEqual(bytes.fromhex(self.pushed()[key]), package.read_bytes(),
                         "the release's chart asset and the pushed chart are the same bytes")
        _, values, _ = self.packaged(key)
        for image, digest in self.RELEASE_DIGESTS.items():
            self.assertIn(f"docker.io/{NS}/{image}:v1.2.3@{digest}", values)
        self.assertIn("chart_version=1.2.3\n", Path(self.env["GITHUB_OUTPUT"]).read_text())
        self.assertEqual(json.loads(self.login.read_text())["stdin"], self.TOKEN)

    def test_chart_push_never_replaces_a_published_version(self):
        package = self.release_package()
        key = f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3"
        self.charts.write_text(json.dumps({key: b"other bytes".hex()}))
        result = self.push(package, success=False)
        self.assertIn("never replaced", result.stderr)
        self.assertEqual(self.pushed()[key], b"other bytes".hex())
        self.assertFalse(any(c[:1] == ["push"] for c in self.helm_calls()))
        self.assertFalse(self.login.exists(), "no credential is used for a refused publication")

    # Existence reads that end in no answer about THIS version. The first is
    # the knob's default, the refusal measured for an unresolvable host; the
    # connection refusal was measured too (2026-10-05, Helm v4.0.1); the 429
    # and 401 lines are Helm's form for those statuses. The last two are a
    # registry's "not found" for ANOTHER version and ANOTHER chart.
    COULD_NOT_TELL = (
        "1",
        'Error: failed to perform "FetchReference" on source: Get "https://registry-1.docker.io/v2/{ns}/logweir-chart/'
        'manifests/1.2.3": dial tcp 127.0.0.1:443: connect: connection refused',
        'Error: failed to perform "FetchReference" on source: GET "https://registry-1.docker.io/v2/{ns}/logweir-chart/'
        'manifests/1.2.3": response status code 429: toomanyrequests: You have reached your unauthenticated pull rate limit',
        'Error: failed to perform "FetchReference" on source: GET "https://registry-1.docker.io/v2/{ns}/logweir-chart/'
        'manifests/1.2.3": response status code 401: unauthorized: authentication required',
        "Error: context deadline exceeded",
        'Error: failed to perform "FetchReference" on source: registry-1.docker.io/{ns}/logweir-chart:1.2.4: not found',
        'Error: failed to perform "FetchReference" on source: registry-1.docker.io/{ns}/logweir-chart-old:1.2.3: not found',
    )

    def test_chart_push_that_cannot_tell_whether_the_version_exists_pushes_nothing(self):
        """Review M-1: a FAILED existence read is not "unpublished". With other
        bytes already published under the version (the reviewer's probe) a push
        would replace a release; with none it would still be a guess. Either
        way: refused before any login, nothing pushed, the registry as it was."""
        package = self.release_package()
        key = f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3"
        for published in (b"other bytes", None):
            for error in self.COULD_NOT_TELL:
                with self.subTest(published=published, error=error):
                    state = {key: published.hex()} if published else {}
                    self.charts.write_text(json.dumps(state))
                    for record in (self.login, self.helm_log):
                        record.unlink(missing_ok=True)
                    self.env["MOCK_HELM_UNREACHABLE"] = error.format(ns=NS)
                    result = self.push(package, success=False)
                    self.assertIn("could not tell whether logweir-chart 1.2.3 exists; nothing pushed", result.stderr)
                    self.assertEqual([c[0] for c in self.helm_calls()], ["pull"],
                                     "one existence read, then nothing: no login, no push, no pull-back")
                    self.assertFalse(self.login.exists(), "no credential is used when the read cannot tell")
                    self.assertEqual(self.pushed(), state, "the registry is left as it was")
        self.env.pop("MOCK_HELM_UNREACHABLE")

    def test_chart_push_of_the_same_bytes_again_is_a_verified_no_op(self):
        package = self.release_package()
        key = f"oci://registry-1.docker.io/{NS}/logweir-chart-1.2.3"
        self.charts.write_text(json.dumps({key: package.read_bytes().hex()}))
        result = self.push(package)
        self.assertIn("not pushed again", result.stderr)
        self.assertFalse(any(c[:1] == ["push"] for c in self.helm_calls()))
        self.assertIn("chart_sha256=", Path(self.env["GITHUB_OUTPUT"]).read_text())

    def test_chart_push_refuses_what_is_not_a_release_package(self):
        package = self.release_package()
        for tag in ("sha-" + SHA, "main"):
            with self.subTest(tag=tag):
                self.env["TAG"] = tag
                self.push(package, success=False)
        self.env["TAG"] = "v1.2.3"
        renamed = package.with_name("logweir-chart-9.9.9.tgz")
        renamed.write_bytes(package.read_bytes())
        result = self.push(renamed, success=False)
        self.assertIn("is not logweir-chart-1.2.3.tgz", result.stderr)
        self.assertEqual(self.pushed(), {})

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
