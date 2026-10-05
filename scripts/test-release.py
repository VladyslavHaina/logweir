#!/usr/bin/env python3
"""scripts/release.sh against a real git history and an in-memory registry.

PROD-14.0. The registry and Helm stand-ins are test-ci-images.py's own; git is
real, in a temporary repository holding a copy of the files the release reads.
No Docker daemon, network, credential or GitHub API is used. Every refusal
below is a row that fails if the refusal is removed.
"""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("test_ci_images", ROOT / "scripts/test-ci-images.py")
MOCKS = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(MOCKS)

NS = "release-test"
PRODUCTS = ("weirkeeper", "logweir", "logweir-console", "logweir-ui")
TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin")
COPIED = ("LICENSE", "NOTICE", "README.md", "THIRD_PARTY_NOTICES.md", "docs/verify_scorecard.py",
          "third_party/LICENSE-MIT", "scripts/release.sh", "scripts/ci-changes.sh",
          "scripts/ci-images.sh", "scripts/check-no-engine-in-binary.sh")
GIT_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid"}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


class Release(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="logweir-release-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.repo = self.base / "repo"
        for rel in COPIED:
            (self.repo / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / rel, self.repo / rel)
        shutil.copytree(ROOT / "ui", self.repo / "ui")
        shutil.copytree(ROOT / "charts/logweir", self.repo / "charts/logweir",
                        ignore=shutil.ignore_patterns("rendered"))
        self.bin = self.base / "bin"
        self.bin.mkdir()
        for name, text in (("docker", MOCKS.DOCKER), ("helm", MOCKS.HELM)):
            (self.bin / name).write_text(text)
            (self.bin / name).chmod(0o755)
        self.registry = self.base / "registry.json"
        self.registry.write_text("{}")
        self.docker_log = self.base / "docker.jsonl"
        self.summary = self.base / "summary"
        self.env = {
            **{k: v for k, v in os.environ.items() if k not in ("DOCKER_CONFIG", "HELM_REGISTRY_CONFIG")},
            **GIT_ENV,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "NS": NS,
            "MOCK_REGISTRY": str(self.registry),
            "MOCK_DOCKER_LOG": str(self.docker_log),
            "MOCK_HELM_LOG": str(self.base / "helm.jsonl"),
            "MOCK_CHARTS": str(self.base / "charts.json"),
            "MOCK_HELM_LOGIN": str(self.base / "login.json"),
            "MOCK_HELM_PULL_ENV": str(self.base / "pull-env.json"),
            "GITHUB_STEP_SUMMARY": str(self.summary),
        }
        self.git("init", "-q")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base")
        self.head = self.git("rev-parse", "HEAD")

    # ---------------------------------------------------------------- helpers
    def git(self, *args):
        out = subprocess.run(["git", *args], cwd=self.repo, capture_output=True, text=True,
                             env={**os.environ, **GIT_ENV}, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        return out.stdout.strip()

    def commit(self, path, text="x\n"):
        (self.repo / path).parent.mkdir(parents=True, exist_ok=True)
        (self.repo / path).write_text(text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", path)
        return self.git("rev-parse", "HEAD")

    def state(self):
        return json.loads(self.registry.read_text())

    def publish(self, commit, revision=None, skip=(), platforms=None):
        """main CI's publication of `commit`: four sha-<commit> images."""
        state = self.state()
        for product in PRODUCTS:
            if product in skip:
                continue
            arches = (platforms or {}).get(product) or (("amd64",) if product == "logweir" else ("amd64", "arm64"))
            labels = {"org.opencontainers.image.revision": revision or commit}
            configs = {f"linux/{a}": {"os": "linux", "architecture": a, "config": {"Labels": labels}} for a in arches}
            # One platform: its configuration. Several: a manifest list's map.
            image = next(iter(configs.values())) if len(arches) == 1 else configs
            digest = "sha256:" + sha256(f"{product}-{commit}-{arches}".encode())
            record = {"digest": digest, "platforms": [f"linux/{a}" for a in arches], "image": image}
            state[f"docker.io/{NS}/{product}:sha-{commit}"] = record
            state[f"docker.io/{NS}/{product}@{digest}"] = record
        self.registry.write_text(json.dumps(state))

    def release(self, *args, env=None, success=True):
        result = subprocess.run(["bash", str(self.repo / "scripts/release.sh"), *args],
                                env={**self.env, **(env or {})}, cwd=self.repo,
                                capture_output=True, text=True, timeout=120)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def resolve(self, commit, success=True, **env):
        out = self.base / "images.json"
        if out.exists():
            out.unlink()
        result = self.release("resolve", str(out), env={"COMMIT": commit, **env}, success=success)
        return (json.loads(out.read_text()) if success else None), result

    def creates(self):
        if not self.docker_log.exists():
            return []
        calls = [json.loads(line) for line in self.docker_log.read_text().splitlines()]
        return [c for c in calls if c[:3] == ["buildx", "imagetools", "create"]]


class Validate(Release):
    def outputs(self, **env):
        result = self.release("validate", env=env)
        return dict(line.split("=", 1) for line in result.stdout.splitlines())

    def test_a_version_tag_push_publishes(self):
        out = self.outputs(EVENT="push", REF="refs/tags/v0.2.0-rc.1", REF_NAME="v0.2.0-rc.1")
        self.assertEqual(out, {"tag": "v0.2.0-rc.1", "version": "0.2.0-rc.1",
                               "prerelease": "true", "publish": "true"})
        out = self.outputs(EVENT="push", REF="refs/tags/v1.2.3", REF_NAME="v1.2.3")
        self.assertEqual((out["prerelease"], out["publish"]), ("false", "true"))

    def test_a_dispatch_is_a_dry_run(self):
        out = self.outputs(EVENT="workflow_dispatch", REF="refs/heads/claude/x", REF_NAME="claude/x",
                           REHEARSAL_TAG="v0.2.0-rc.1")
        self.assertEqual(out["publish"], "false")
        self.assertEqual(out["tag"], "v0.2.0-rc.1")
        # Even dispatched ON a tag, a dispatch publishes nothing.
        out = self.outputs(EVENT="workflow_dispatch", REF="refs/tags/v0.2.0-rc.1", REF_NAME="v0.2.0-rc.1",
                           REHEARSAL_TAG="v0.2.0-rc.1")
        self.assertEqual(out["publish"], "false")

    def test_anything_else_is_refused(self):
        for env in (
            {"EVENT": "push", "REF": "refs/heads/main", "REF_NAME": "main"},
            {"EVENT": "push", "REF": "refs/tags/v1.2", "REF_NAME": "v1.2"},
            {"EVENT": "push", "REF": "refs/tags/1.2.3", "REF_NAME": "1.2.3"},
            {"EVENT": "push", "REF": "refs/tags/v1.2.3+build", "REF_NAME": "v1.2.3+build"},
            {"EVENT": "push", "REF": "refs/tags/v01.2.3", "REF_NAME": "v01.2.3"},
            {"EVENT": "push", "REF": "refs/heads/v1.2.3", "REF_NAME": "v1.2.3"},
            {"EVENT": "workflow_dispatch", "REF": "refs/heads/x", "REF_NAME": "x", "REHEARSAL_TAG": "latest"},
            {"EVENT": "pull_request", "REF": "refs/pull/1/merge", "REF_NAME": "1/merge"},
        ):
            with self.subTest(env=env):
                self.release("validate", env=env, success=False)


class Resolve(Release):
    def test_the_release_commit_s_own_publication(self):
        self.publish(self.head)
        images, _ = self.resolve(self.head)
        self.assertEqual((images["commit"], images["publication"]), (self.head, self.head))
        self.assertEqual(images["how"], "the release commit's own")
        state = self.state()
        for product in PRODUCTS:
            ref = f"docker.io/{NS}/{product}:sha-{self.head}"
            self.assertEqual(images["images"][product]["digest"], state[ref]["digest"])
            self.assertEqual(images["images"][product]["published_as"], ref)
        self.assertEqual(images["images"]["logweir"]["platforms"], ["linux/amd64"])
        self.assertEqual(images["images"]["weirkeeper"]["platforms"], ["linux/amd64", "linux/arm64"])

    def test_a_docs_only_commit_ships_its_newest_published_ancestor(self):
        self.publish(self.head)
        notes = self.commit("docs/release-notes.md")
        tracker = self.commit("docs/to-do/tracker.md")
        images, _ = self.resolve(tracker)
        self.assertEqual(images["publication"], self.head)
        self.assertEqual(images["commit"], tracker)
        self.assertIn("only under docs/", images["how"])
        self.assertNotEqual(notes, self.head)

    def test_a_code_change_is_never_skipped(self):
        self.publish(self.head)
        self.commit("docs/a.md")
        self.commit("scripts/new.sh")          # unpublished code
        tip = self.commit("docs/b.md")
        _, result = self.resolve(tip, success=False)
        self.assertIn("no complete sha-<commit> publication", result.stderr)
        # A root Markdown file is not under docs/: it counts as code too.
        self.publish(tip)
        notice = self.commit("THIRD_PARTY_NOTICES.md", "changed\n")
        self.resolve(notice, success=False)

    def test_an_incomplete_publication_is_not_one(self):
        self.publish(self.head, skip=("logweir-ui",))
        _, result = self.resolve(self.head, success=False)
        self.assertIn("no complete", result.stderr)

    def test_the_images_must_be_what_main_ci_publishes(self):
        self.publish(self.head, revision="b" * 40)
        _, result = self.resolve(self.head, success=False)
        self.assertIn("org.opencontainers.image.revision", result.stderr)
        self.registry.write_text("{}")
        self.publish(self.head, platforms={"weirkeeper": ("amd64",)})
        _, result = self.resolve(self.head, success=False)
        self.assertIn("not linux/amd64,linux/arm64", result.stderr)
        self.registry.write_text("{}")
        self.publish(self.head, platforms={"logweir": ("amd64", "arm64")})
        self.resolve(self.head, success=False)

    def test_a_dry_run_may_name_the_publication_and_a_tag_may_not(self):
        self.publish(self.head)
        branch = self.commit("scripts/feature.sh")   # a branch: code, unpublished
        self.resolve(branch, success=False)
        images, _ = self.resolve(branch, PUBLICATION=self.head, PUBLISH="false")
        self.assertEqual(images["publication"], self.head)
        self.assertIn("named by the dry run", images["how"])
        self.assertNotIn("(", images["how"])
        _, result = self.resolve(branch, success=False, PUBLICATION=self.head, PUBLISH="true")
        self.assertIn("dry-run input", result.stderr)
        other = "c" * 40
        self.publish(other)
        _, result = self.resolve(branch, success=False, PUBLICATION=other, PUBLISH="false")
        self.assertIn("not an ancestor", result.stderr)


class Promote(Release):
    TAG = "v0.2.0-rc.1"

    def setUp(self):
        super().setUp()
        self.publish(self.head)
        self.images, _ = self.resolve(self.head)
        self.images_path = self.base / "images.json"

    def promote(self, success=True, **env):
        return self.release("promote", str(self.images_path), env={"TAG": self.TAG, **env}, success=success)

    def test_the_version_tag_names_the_publication_s_own_digests(self):
        self.promote()
        state = self.state()
        for product in PRODUCTS:
            self.assertEqual(state[f"docker.io/{NS}/{product}:{self.TAG}"]["digest"],
                             self.images["images"][product]["digest"])
        creates = self.creates()
        self.assertEqual(len(creates), 4)
        for call in creates:
            self.assertIn("--prefer-index=false", call, "one source and no index: a carbon copy")
            self.assertEqual(len([a for a in call if "@sha256:" in a]), 1)
        self.assertEqual(self.summary.read_text().count(self.TAG), 4)

    def test_a_re_run_writes_nothing(self):
        self.promote()
        self.promote()
        self.assertEqual(len(self.creates()), 4)

    def test_a_tag_already_on_other_bytes_is_never_moved(self):
        state = self.state()
        state[f"docker.io/{NS}/logweir-console:{self.TAG}"] = {"digest": "sha256:" + "e" * 64, "platforms": []}
        self.registry.write_text(json.dumps(state))
        result = self.promote(success=False)
        self.assertIn("never moved", result.stderr)
        self.assertEqual(self.creates(), [], "nothing is tagged when one tag conflicts")

    def test_a_failed_read_is_not_an_absent_tag(self):
        self.env["MOCK_UNREACHABLE_REF"] = f"docker.io/{NS}/logweir-ui:{self.TAG}"
        result = self.promote(success=False)
        self.assertIn("could not tell", result.stderr)
        self.assertEqual(self.creates(), [])

    def test_only_a_version_tag(self):
        for tag in ("sha-" + self.head, "latest", "main"):
            with self.subTest(tag=tag):
                self.promote(success=False, TAG=tag)
        self.assertEqual(self.creates(), [])


class Assemble(Release):
    TAG = "v0.2.0-rc.1"

    def setUp(self):
        super().setUp()
        self.publish(self.head)
        self.images, _ = self.resolve(self.head)
        self.inputs = self.base / "in"
        self.inputs.mkdir()
        shutil.copyfile(self.base / "images.json", self.inputs / "images.json")
        for target in TARGETS:
            self.archive(target)

    def archive(self, target, binary=b"\x7fELF a logweir binary\n", leave_out=(), sidecar=None, notice=None):
        name = f"logweir-{target}"
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w:xz") as tar:
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            tar.addfile(info)
            files = {"logweir": binary}
            for doc in ("LICENSE", "NOTICE", "README.md", "THIRD_PARTY_NOTICES.md"):
                files[doc] = (self.repo / doc).read_bytes()
            if notice is not None:
                files["NOTICE"] = notice
            for member, content in files.items():
                if member in leave_out:
                    continue
                info = tarfile.TarInfo(f"{name}/{member}")
                info.size = len(content)
                info.mode = 0o755 if member == "logweir" else 0o644
                tar.addfile(info, io.BytesIO(content))
        archive = self.inputs / f"{name}.tar.xz"
        archive.write_bytes(data.getvalue())
        digest = sidecar or sha256(data.getvalue())
        (self.inputs / f"{name}.tar.xz.sha256").write_text(f"{digest} *{name}.tar.xz\n")
        (self.inputs / f"{name}.linkage.txt").write_text(
            f"target: {target}\nglibc: 2.34 or newer\nneeds: libssl.so.3\nsystem: libc.so.6\n")

    def assemble(self, success=True, out=None):
        out = out or self.base / "assets"
        result = self.release("assemble", str(self.inputs), str(out), str(self.base / "notes.md"), env={
            "TAG": self.TAG, "VERSION": self.TAG[1:], "PRERELEASE": "true", "COMMIT": self.head,
            "RUN_URL": "https://example.invalid/run/1"}, success=success)
        return out, result

    def test_the_assets_are_complete_checked_and_described(self):
        out, _ = self.assemble()
        names = sorted(p.name for p in out.iterdir())
        expected = sorted([f"logweir-{t}.tar.xz" for t in TARGETS] + [f"logweir-{t}.tar.xz.sha256" for t in TARGETS]
                          + ["verify_scorecard.py", "LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md",
                             "kafka-backup-LICENSE", "logweir-chart-0.2.0-rc.1.tgz", "ui-files.sha256",
                             "release.json", "SHA256SUMS"])
        self.assertEqual(names, expected)
        self.assertEqual((out / "kafka-backup-LICENSE").read_bytes(), (self.repo / "third_party/LICENSE-MIT").read_bytes())
        self.assertEqual((out / "verify_scorecard.py").read_bytes(), (self.repo / "docs/verify_scorecard.py").read_bytes())
        sums = dict(reversed(line.split("  ", 1)) for line in (out / "SHA256SUMS").read_text().splitlines())
        self.assertEqual(sorted(sums), sorted(n for n in names if n != "SHA256SUMS"))
        for name, digest in sums.items():
            self.assertEqual(sha256((out / name).read_bytes()), digest, name)
        release = json.loads((out / "release.json").read_text())
        self.assertEqual((release["tag"], release["version"], release["prerelease"], release["commit"]),
                         (self.TAG, "0.2.0-rc.1", True, self.head))
        self.assertEqual(release["images"]["publication"], self.head)
        for product in PRODUCTS:
            digest = self.images["images"][product]["digest"]
            self.assertEqual(release["images"]["refs"][product]["reference"],
                             f"docker.io/{NS}/{product}:{self.TAG}@{digest}")
        self.assertEqual(release["chart"]["sha256"], sha256((out / "logweir-chart-0.2.0-rc.1.tgz").read_bytes()))
        with tarfile.open(out / "logweir-chart-0.2.0-rc.1.tgz") as tar:
            values = tar.extractfile("logweir-chart/values.yaml").read().decode()
        for product in PRODUCTS:
            self.assertIn(f"docker.io/{NS}/{product}:{self.TAG}@{self.images['images'][product]['digest']}", values)
        self.assertNotIn(":latest", values)
        ui = (out / "ui-files.sha256").read_text().splitlines()
        self.assertTrue(ui and all("  ui/" in line for line in ui))
        self.assertFalse(any("ui/tests/" in line or line.endswith(".md") for line in ui))
        notes = (self.base / "notes.md").read_text()
        for needle in (f"--version 0.2.0-rc.1", "(pre-release)", f"sha-{self.head}", "drill countersign",
                       "sha256sum -c SHA256SUMS", "kafka-backup-LICENSE", "glibc: 2.34 or newer"):
            self.assertIn(needle, notes)
        self.assertNotIn("libc.so.6", notes, "the notes list what to install, not the OS itself")
        self.release("verify", str(out))

    def test_verify_refuses_a_changed_missing_or_extra_asset(self):
        out, _ = self.assemble()
        (out / "NOTICE").write_text("tampered\n")
        self.release("verify", str(out), success=False)
        shutil.copyfile(self.repo / "NOTICE", out / "NOTICE")
        self.release("verify", str(out))
        (out / "extra.txt").write_text("x\n")
        self.release("verify", str(out), success=False)
        (out / "extra.txt").unlink()
        (out / "verify_scorecard.py").unlink()
        self.release("verify", str(out), success=False)

    def test_an_archive_that_is_not_the_contract_is_refused(self):
        cases = {
            "no NOTICE": dict(leave_out=("NOTICE",)),
            "an engine inside": dict(binary=b"logweir \x00/src/kafka-backup-core/src/lib.rs\x00"),
            "a wrong sidecar": dict(sidecar="0" * 64),
            "another commit's NOTICE": dict(notice=b"an older NOTICE\n"),
        }
        for label, change in cases.items():
            with self.subTest(case=label):
                self.archive("aarch64-apple-darwin", **change)
                out = self.base / ("assets-" + label.replace(" ", "-"))
                self.assemble(success=False, out=out)
                self.archive("aarch64-apple-darwin")

    def test_inputs_from_another_commit_or_an_old_directory_are_refused(self):
        images = json.loads((self.inputs / "images.json").read_text())
        (self.inputs / "images.json").write_text(json.dumps({**images, "commit": "d" * 40}))
        self.assemble(success=False)
        (self.inputs / "images.json").write_text(json.dumps(images))
        out = self.base / "existing"
        out.mkdir()
        self.assemble(success=False, out=out)


# A stand-in for the packaged binary's `drill countersign`, for the check's own
# rows: MODE `honest` countersigns and refuses as the real binary does (the
# positive control), `copy` adds no signature, `wrongkey` adds one by a key it
# was not given, `lenient` countersigns everything it is handed.
STUB = r'''#!PYTHON
import base64, hashlib, json, os, sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
args = sys.argv[1:]
if args[:2] != ["drill", "countersign"]:
    sys.exit(2)
opt = {args[i]: args[i + 1] for i in range(2, len(args), 2)}
mode = os.environ["STUB_MODE"]
doc = open(opt["--document"], "rb").read()
sidecar = json.load(open(opt["--confirmation"]))
key = serialization.load_pem_private_key(open(opt["--key"], "rb").read(), None)
def keyid(k):
    return hashlib.sha256(k.public_key().public_bytes(serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo)).hexdigest()
document = json.loads(doc)
if mode != "lenient":
    if document["authorizationMode"] != "Governed":
        print("an Ordinary authorization: nothing to countersign", file=sys.stderr); sys.exit(1)
    if any(s["keyid"] == keyid(key) for s in sidecar["signatures"]):
        print("key has already signed this document", file=sys.stderr); sys.exit(1)
pt = sidecar["payloadType"].encode()
pae = b"DSSEv1 " + str(len(pt)).encode() + b" " + pt + b" " + str(len(doc)).encode() + b" " + doc
signer = ec.generate_private_key(ec.SECP256R1()) if mode == "wrongkey" else key
if mode != "copy":
    sidecar["signatures"].append({"keyid": keyid(key), "sig": base64.b64encode(
        signer.sign(pae, ec.ECDSA(hashes.SHA256()))).decode()})
open(opt["--out"], "w").write(json.dumps(sidecar))
r = document["requester"]
print("countersigned\n  requester  " + r["issuer"] + "#" + r["subject"])
'''


class CountersignCheck(unittest.TestCase):
    """scripts/release-countersign-check.py must be able to fail."""

    def run_check(self, mode):
        with tempfile.TemporaryDirectory(prefix="logweir-countersign-stub-") as tmp:
            stub = Path(tmp) / "logweir"
            stub.write_text(STUB.replace("#!PYTHON", "#!" + sys.executable))
            stub.chmod(0o755)
            return subprocess.run([sys.executable, str(ROOT / "scripts/release-countersign-check.py"), str(stub)],
                                  env={**os.environ, "STUB_MODE": mode}, capture_output=True, text=True, timeout=120)

    def test_an_honest_countersignature_passes(self):
        result = self.run_check("honest")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("both signatures verify independently", result.stdout)

    def test_a_binary_that_does_not_countersign_fails(self):
        for mode, reason in (("copy", "exactly one more"), ("wrongkey", "does not verify"),
                             ("lenient", "was not refused")):
            with self.subTest(mode=mode):
                result = self.run_check(mode)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(reason, result.stderr)


if __name__ == "__main__":
    unittest.main()
