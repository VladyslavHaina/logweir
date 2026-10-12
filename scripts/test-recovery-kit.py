#!/usr/bin/env python3
"""scripts/recovery-kit.sh over a synthetic release and a stubbed registry (PROD-14.1).

The kit is built from this checkout (schemas, chart, docs/install.md) into a
temporary directory. No network, Docker daemon or credential is used: the
release's assets are made here, and `docker` is a stub that answers
`buildx imagetools inspect`. Every refusal below is a row that fails if the
refusal is removed.
"""

import base64
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/recovery-kit.sh"
VERSION = "v9.9.9"
TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin")
PRODUCTS = ("weirkeeper", "logweir", "logweir-console")
DIGEST = "sha256:" + "ab" * 32
BOOTSTRAP = "docker.io/vladyslavhaina/logweir@sha256:" + "cd" * 32
CHART = "logweir-chart-9.9.9.tgz"
CHART_MEMBERS = {"logweir-chart/Chart.yaml": "name: logweir-chart\n",
                 "logweir-chart/values.yaml": f"identity:\n  bootstrapImage: {BOOTSTRAP} # pin\n"}
# Assembled at run time, never one literal (push protection scans for the shape).
PEM_BEGIN = "-----BEGIN " + "PRIVATE KEY-----"
PEM_END = "-----END " + "PRIVATE KEY-----"
STUB_DOCKER = """#!/usr/bin/env bash
if [[ "$1 $2 $3" == "buildx imagetools inspect" ]]; then
  printf '{"digest":"%s"}\\n' "$STUB_DIGEST"; exit 0
fi
echo "stub docker: unexpected $*" >&2; exit 9
"""


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def tgz(members: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, text in members.items():
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def appledouble(archive: Path) -> list:
    """The AppleDouble `._*` members of a tarball. macOS tar lists them merged into their
    file, so this reads the archive itself."""
    with tarfile.open(archive) as tar:
        return [n for n in tar.getnames() if Path(n).name.startswith("._")]


def helm_show_chart(archive: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["helm", "show", "chart", str(archive)], capture_output=True, text=True, timeout=60)


def set_xattr(path: Path):
    """An extended attribute, as macOS gives a checked-out or downloaded file."""
    subprocess.run(["xattr", "-w", "dev.logweir.test", "1", str(path)], check=True, timeout=30)


def manifest_mismatches(kit: Path) -> list:
    """Every file on disk that MANIFEST.sha256 does not list with its sha256, and every entry with no file."""
    listed = {}
    for line in (kit / "MANIFEST.sha256").read_text().splitlines():
        digest, name = line.split("  ", 1)
        listed[name] = digest
    on_disk = {str(p.relative_to(kit)) for p in kit.rglob("*") if p.is_file()} - {"MANIFEST.sha256"}
    bad = sorted(on_disk ^ set(listed))
    bad += sorted(n for n in on_disk & set(listed) if sha256((kit / n).read_bytes()) != listed[n])
    return bad


class RecoveryKit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="logweir-kit-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.out = self.base / "out"
        self.kit = self.out / f"logweir-recovery-kit-{VERSION}"
        self.assets = self.base / "assets"
        self.assets.mkdir()
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True,
                              check=True, timeout=30).stdout.strip()
        self.files = {}
        for target in TARGETS:
            name = f"logweir-{target}.tar.xz"
            self.files[name] = os.urandom(64)
            self.files[name + ".sha256"] = f"{sha256(self.files[name])}  {name}\n".encode()
        self.files[CHART] = tgz(CHART_MEMBERS)
        self.files["verify_scorecard.py"] = (ROOT / "docs/verify_scorecard.py").read_bytes()
        self.release = {
            "tag": VERSION, "commit": head, "chart": {"package": CHART},
            "images": {"refs": {p: {"reference": f"docker.io/vladyslavhaina/{p}:{VERSION}@{DIGEST}"}
                                for p in PRODUCTS}}}
        self.write_assets()

    def write_assets(self):
        files = dict(self.files, **{"release.json": json.dumps(self.release).encode()})
        for name, data in files.items():
            (self.assets / name).write_bytes(data)
        (self.assets / "SHA256SUMS").write_text("".join(f"{sha256(d)}  {n}\n" for n, d in sorted(files.items())))

    def run_kit(self, *args, env=None, version=VERSION):
        return subprocess.run(["bash", str(SCRIPT), version, *args, "--out", str(self.out)], cwd=ROOT,
                              capture_output=True, text=True, timeout=300, env=env)

    def trust_dir(self, files: dict, name: str = "trust") -> Path:
        trust = self.base / name
        trust.mkdir()
        for name, text in files.items():
            (trust / name).write_text(text)
        return trust

    def assert_nothing_written(self):
        self.assertFalse(self.kit.exists())
        self.assertEqual([p.name for p in self.out.iterdir()] if self.out.exists() else [], [])

    def test_the_manifest_lists_every_file_with_its_sha256(self):
        done = self.run_kit("--release-assets", str(self.assets))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(manifest_mismatches(self.kit), [])
        listed = (self.kit / "MANIFEST.sha256").read_text()
        for name in ("RECOVERY.md", "kit.json", "values-offline.yaml", "images/images.txt",
                     "chart/logweir-chart-9.9.9.tgz", "verify/verify_scorecard.py", "release.json",
                     "schemas/logweir-drill-scorecard-1.0.0.json", "bin/logweir-aarch64-apple-darwin.tar.xz"):
            self.assertIn(f"  {name}\n", listed)
        tarball = self.out / f"logweir-recovery-kit-{VERSION}.tar.gz"
        self.assertEqual((self.out / (tarball.name + ".sha256")).read_text(),
                         f"{sha256(tarball.read_bytes())}  {tarball.name}\n")
        # CONTROL: the comparison is not vacuous; a file the manifest does not list is found.
        (self.kit / "trust").mkdir()
        (self.kit / "trust" / "late.pem").write_text("added after the manifest\n")
        self.assertEqual(manifest_mismatches(self.kit), ["trust/late.pem"])

    def test_the_kit_names_its_binaries_images_and_instructions(self):
        done = self.run_kit("--release-assets", str(self.assets))
        self.assertEqual(done.returncode, 0, done.stderr)
        kit = json.loads((self.kit / "kit.json").read_text())
        self.assertEqual(kit["binaries"], "release")
        self.assertEqual(kit["absentTargets"], [])
        self.assertEqual(set(kit["images"]), {"controller", "runner", "console", "bootstrap"})
        self.assertEqual(kit["images"]["bootstrap"], BOOTSTRAP)
        values = (self.kit / "values-offline.yaml").read_text()
        for ref in kit["images"].values():
            self.assertRegex(ref, r"@sha256:[0-9a-f]{64}$")
            self.assertIn(ref, values)
        recovery = (self.kit / "RECOVERY.md").read_text()
        self.assertIn("\n## Recover onto a clean installation with the kit\n", recovery)
        flat = " ".join(recovery.split())
        for step in ("Connect an existing archive", "verify_scorecard.py", "Trust the archive's signer"):
            self.assertIn(step, flat)
        self.assertNotIn("## Upgrade CRDs", recovery)

    def test_the_private_key_guard_refuses(self):
        body = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg"
        pem = f"{PEM_BEGIN}\n{body}\n{PEM_END}\n"
        cases = {
            "a private-key PEM": {"old-signer.pem": pem},
            "a base64 private key inside a ConfigMap": {
                "export.yaml": "kind: ConfigMap\ndata:\n  k: " + base64.b64encode(pem.encode()).decode() + "\n"},
            "a Kubernetes Secret": {"identity.yaml": "apiVersion: v1\nkind: Secret\ndata:\n  x: eA==\n"},
            "a Secret whose kind is double-quoted": {"s3.yaml": 'apiVersion: v1\nkind: "Secret"\nstringData:\n  k: v\n'},
            "a Secret whose kind is single-quoted": {"oidc.yaml": "apiVersion: v1\nkind: 'Secret'  # hand-written\n"
                                                                 "stringData:\n  k: v\n"},
            "a password file": {"minio.password": "not-a-real-one\n"},
        }
        for i, (what, files) in enumerate(cases.items()):
            with self.subTest(what):
                shutil.rmtree(self.out, ignore_errors=True)
                trust = self.trust_dir(files, f"trust-{i}")
                done = self.run_kit("--release-assets", str(self.assets), "--trust", str(trust))
                self.assertEqual(done.returncode, 3, (what, done.stderr))
                self.assertIn("REFUSED trust/" + next(iter(files)), done.stderr)
                self.assert_nothing_written()

    def test_the_guard_refuses_a_private_key_early_in_a_large_chart(self):
        # The key comes first and more than a pipe's 64 KB follows it, as in the real
        # chart, whose crds/ come after 688 KB of values, examples and templates. A
        # `tar | grep -q` exits at the key, tar dies of SIGPIPE, and pipefail says "no match".
        self.files[CHART] = tgz({"logweir-chart/files/old-signer.pem": f"{PEM_BEGIN}\nMIGHAgEA\n{PEM_END}\n",
                                 "logweir-chart/templates/large.yaml": "# filler\n" * 120_000,
                                 **CHART_MEMBERS})
        self.write_assets()
        done = self.run_kit("--release-assets", str(self.assets))
        self.assertEqual(done.returncode, 3, done.stderr)
        self.assertIn(f"REFUSED chart/{CHART}", done.stderr)
        self.assert_nothing_written()

    def test_its_twin_public_material_is_packed(self):
        trust = self.trust_dir({
            "signing.pub.pem": "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQc=\n-----END PUBLIC KEY-----\n",
            "trustpolicy-org.yaml": "apiVersion: logweir.dev/v1alpha1\nkind: TrustPolicy\nmetadata:\n  name: org\n",
        })
        done = self.run_kit("--release-assets", str(self.assets), "--trust", str(trust))
        self.assertEqual(done.returncode, 0, done.stderr)
        listed = (self.kit / "MANIFEST.sha256").read_text()
        self.assertIn("  trust/signing.pub.pem\n", listed)
        self.assertIn("  trust/trustpolicy-org.yaml\n", listed)

    def commit_like_head_but(self, path: str, text: str) -> tuple:
        """A commit whose tree is HEAD's with `path` holding `text`, written to a scratch
        object store so the checkout's repository is never written to. Returns the commit
        and the environment under which git (and so the kit script) can see it."""
        objects = self.base / "objects"
        objects.mkdir(exist_ok=True)
        real = subprocess.run(["git", "rev-parse", "--path-format=absolute", "--git-path", "objects"], cwd=ROOT,
                              capture_output=True, text=True, check=True, timeout=30).stdout.strip()
        env = dict(os.environ, GIT_OBJECT_DIRECTORY=str(objects), GIT_ALTERNATE_OBJECT_DIRECTORIES=real)
        plumbing = dict(env, GIT_INDEX_FILE=str(self.base / "index"), GIT_AUTHOR_NAME="kit test",
                        GIT_AUTHOR_EMAIL="kit@test.invalid", GIT_COMMITTER_NAME="kit test",
                        GIT_COMMITTER_EMAIL="kit@test.invalid")

        def git(*args, stdin=None):
            return subprocess.run(["git", *args], cwd=ROOT, env=plumbing, input=stdin, capture_output=True,
                                  text=True, check=True, timeout=30).stdout.strip()
        git("read-tree", "HEAD")
        blob = git("hash-object", "-w", "--stdin", stdin=text)
        git("update-index", "--cacheinfo", f"100644,{blob},{path}")
        return git("commit-tree", "--no-gpg-sign", git("write-tree"), "-p", "HEAD", "-m", "the release"), env

    def test_the_checkout_must_hold_the_releases_guide_and_schemas(self):
        # RECOVERY.md and schemas/ come from this checkout but claim the release commit.
        guide = (ROOT / "docs/install.md").read_text()
        cases = {"docs/install.md": guide + "\nA step this release does not have.\n",
                 "schemas/logweir-drill-scorecard-1.0.0.json": "{}\n"}
        for path, text in cases.items():
            with self.subTest(path):
                shutil.rmtree(self.out, ignore_errors=True)
                self.release["commit"], env = self.commit_like_head_but(path, text)
                self.write_assets()
                done = self.run_kit("--release-assets", str(self.assets), env=env)
                self.assertEqual(done.returncode, 1, (path, done.stderr))
                self.assertIn(f"check out {VERSION}", done.stderr)
                self.assert_nothing_written()

    def test_an_asset_that_does_not_match_its_checksum_is_refused(self):
        archive = self.assets / f"logweir-{TARGETS[0]}.tar.xz"
        archive.write_bytes(archive.read_bytes() + b"tampered")
        done = self.run_kit("--release-assets", str(self.assets))
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("does not match SHA256SUMS", done.stderr)
        self.assert_nothing_written()

    def local_build_env(self) -> dict:
        """A stubbed registry and a stand-in binary for `--local-build`."""
        stub = self.base / "stub"
        stub.mkdir()
        (stub / "docker").write_text(STUB_DOCKER)
        (stub / "docker").chmod(0o755)
        binary = self.base / "logweir"
        binary.write_text("#!/bin/sh\necho logweir 0.0.0\n")
        binary.chmod(0o755)
        return dict(os.environ, PATH=f"{stub}:{os.environ['PATH']}", STUB_DIGEST=DIGEST,
                    LOGWEIR_BIN=str(binary), DOCKER_CONFIG=str(self.base / "docker-config"))

    def test_a_local_build_is_a_dry_run_and_says_so(self):
        done = self.run_kit("--local-build", env=self.local_build_env())
        self.assertEqual(done.returncode, 0, done.stderr)
        kit = json.loads((self.kit / "kit.json").read_text())
        self.assertEqual(kit["binaries"], "local-build")
        self.assertEqual(len(kit["absentTargets"]), 2)
        host = {"Darwin": "aarch64-apple-darwin"}.get(platform.system(),
                                                       f"{platform.machine()}-unknown-linux-gnu")
        self.assertTrue((self.kit / "bin" / host / "logweir").is_file())
        self.assertEqual(kit["images"]["runner"], f"docker.io/vladyslavhaina/logweir:{VERSION}@{DIGEST}")
        with tarfile.open(self.kit / "chart" / "logweir-chart-local.tgz") as tar:
            names = tar.getnames()
        self.assertIn("logweir/values.yaml", names)
        self.assertFalse([n for n in names if "/rendered" in n])
        self.assertEqual(manifest_mismatches(self.kit), [])

    def test_a_short_commit_names_the_full_one_and_an_ambiguous_one_is_refused(self):
        # poc-14-1a B2: `just recovery-kit sha-22dc6147` was refused for want of 40 hex.
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True,
                              check=True, timeout=30).stdout.strip()
        env = self.local_build_env()
        done = self.run_kit("--local-build", env=env, version=f"sha-{head[:8]}")
        self.assertEqual(done.returncode, 0, done.stderr)
        kit = json.loads((self.out / f"logweir-recovery-kit-sha-{head}" / "kit.json").read_text())
        self.assertEqual(kit["version"], f"sha-{head}")
        self.assertEqual(kit["images"]["runner"], f"docker.io/vladyslavhaina/logweir:sha-{head}@{DIGEST}")
        # Two commits sharing a 4-hex prefix (a birthday search over commit bodies), written to
        # a scratch object store so the checkout's repository is never written to.
        tree = subprocess.run(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT, capture_output=True, text=True,
                              check=True, timeout=30).stdout.strip()
        seen = {}
        for i in range(1_000_000):
            body = (f"tree {tree}\nparent {head}\nauthor kit test <kit@test.invalid> 0 +0000\n"
                    f"committer kit test <kit@test.invalid> 0 +0000\n\nambiguous {i}\n").encode()
            prefix = hashlib.sha1(b"commit %d\0" % len(body) + body).hexdigest()[:4]
            if prefix in seen:
                break
            seen[prefix] = body
        objects = self.base / "objects"
        objects.mkdir()
        real = subprocess.run(["git", "rev-parse", "--path-format=absolute", "--git-path", "objects"], cwd=ROOT,
                              capture_output=True, text=True, check=True, timeout=30).stdout.strip()
        env.update(GIT_OBJECT_DIRECTORY=str(objects), GIT_ALTERNATE_OBJECT_DIRECTORIES=real)
        for commit in (seen[prefix], body):
            written = subprocess.run(["git", "hash-object", "-t", "commit", "-w", "--stdin"], cwd=ROOT, env=env,
                                     input=commit, capture_output=True, check=True, timeout=30).stdout
            self.assertTrue(written.decode().startswith(prefix))
        shutil.rmtree(self.out)
        done = self.run_kit("--local-build", env=env, version=f"sha-{prefix}")
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn(f"short object ID {prefix} is ambiguous", done.stderr)
        self.assertIn(f"'sha-{prefix}' names no single commit in this checkout", done.stderr)
        self.assert_nothing_written()

    def test_the_kit_chart_loads_in_helm_and_no_tarball_has_an_appledouble_member(self):
        # poc-14-1a D1: macOS tar packed a `._*` member for every file with an extended
        # attribute, and `helm show chart` refused the kit's chart.
        trust = self.trust_dir({"signing.pub.pem": "-----BEGIN PUBLIC KEY-----\nMFkw\n-----END PUBLIC KEY-----\n"})
        if platform.system() == "Darwin":
            set_xattr(trust / "signing.pub.pem")
        done = self.run_kit("--local-build", "--trust", str(trust), env=self.local_build_env())
        self.assertEqual(done.returncode, 0, done.stderr)
        chart = self.kit / "chart" / "logweir-chart-local.tgz"
        shown = helm_show_chart(chart)
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertIn("name: logweir\n", shown.stdout)
        self.assertEqual(appledouble(chart) + appledouble(self.out / f"logweir-recovery-kit-{VERSION}.tar.gz"), [])
        # CONTROL: the old packing, a plain `tar -czf`, fails both checks. On Linux tar never
        # wrote the member, so the control plants the one macOS tar wrote.
        old = self.base / "old.tgz"
        chart_yaml = "apiVersion: v2\nname: logweir\nversion: 0.1.0\n"
        if platform.system() == "Darwin":
            source = self.base / "old" / "logweir"
            source.mkdir(parents=True)
            (source / "Chart.yaml").write_text(chart_yaml)
            set_xattr(source)
            set_xattr(source / "Chart.yaml")
            subprocess.run(["tar", "-czf", str(old), "-C", str(source.parent), "logweir"], check=True, timeout=60,
                           env={k: v for k, v in os.environ.items() if k != "COPYFILE_DISABLE"})
        else:
            old.write_bytes(tgz({"._logweir": "\0\5\26\7", "logweir/Chart.yaml": chart_yaml}))
        self.assertNotEqual(appledouble(old), [])
        refused = helm_show_chart(old)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("outside the base directory", refused.stderr)


if __name__ == "__main__":
    unittest.main()
