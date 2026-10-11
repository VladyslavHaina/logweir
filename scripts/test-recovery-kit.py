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
        files = {}
        for target in TARGETS:
            name = f"logweir-{target}.tar.xz"
            files[name] = os.urandom(64)
            files[name + ".sha256"] = f"{sha256(files[name])}  {name}\n".encode()
        chart = "logweir-chart-9.9.9.tgz"
        files[chart] = tgz({"logweir-chart/Chart.yaml": "name: logweir-chart\n",
                            "logweir-chart/values.yaml": f"identity:\n  bootstrapImage: {BOOTSTRAP} # pin\n"})
        files["verify_scorecard.py"] = (ROOT / "docs/verify_scorecard.py").read_bytes()
        files["release.json"] = json.dumps({
            "tag": VERSION, "commit": head, "chart": {"package": chart},
            "images": {"refs": {p: {"reference": f"docker.io/vladyslavhaina/{p}:{VERSION}@{DIGEST}"}
                                for p in PRODUCTS}}}).encode()
        for name, data in files.items():
            (self.assets / name).write_bytes(data)
        (self.assets / "SHA256SUMS").write_text("".join(f"{sha256(d)}  {n}\n" for n, d in sorted(files.items())))

    def run_kit(self, *args, env=None):
        return subprocess.run(["bash", str(SCRIPT), VERSION, *args, "--out", str(self.out)], cwd=ROOT,
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

    def test_an_asset_that_does_not_match_its_checksum_is_refused(self):
        archive = self.assets / f"logweir-{TARGETS[0]}.tar.xz"
        archive.write_bytes(archive.read_bytes() + b"tampered")
        done = self.run_kit("--release-assets", str(self.assets))
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("does not match SHA256SUMS", done.stderr)
        self.assert_nothing_written()

    def test_a_local_build_is_a_dry_run_and_says_so(self):
        stub = self.base / "stub"
        stub.mkdir()
        (stub / "docker").write_text(STUB_DOCKER)
        (stub / "docker").chmod(0o755)
        binary = self.base / "logweir"
        binary.write_text("#!/bin/sh\necho logweir 0.0.0\n")
        binary.chmod(0o755)
        env = dict(os.environ, PATH=f"{stub}:{os.environ['PATH']}", STUB_DIGEST=DIGEST,
                   LOGWEIR_BIN=str(binary), DOCKER_CONFIG=str(self.base / "docker-config"))
        done = self.run_kit("--local-build", env=env)
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


if __name__ == "__main__":
    unittest.main()
