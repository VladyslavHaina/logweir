#!/usr/bin/env python3
"""Negative controls for scripts/check-cosign-verify.py (PROD-00.2 security review).

Each planted tree holds one verification command. The gate must refuse every
command that drops a pin, loosens one into a regular expression or names
another value, and must accept the fully pinned one — the positive control
that shows the refusals are the pins', not the gate refusing everything. The
real repository must pass with its sites required.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATE = ROOT / "scripts/check-cosign-verify.py"

IDENTITY = "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main"
PINNED = [
    f'--certificate-identity "{IDENTITY}"',
    '--certificate-oidc-issuer "https://token.actions.githubusercontent.com"',
    '--certificate-github-workflow-repository "VladyslavHaina/logweir"',
    '--certificate-github-workflow-ref "refs/heads/main"',
    '--certificate-github-workflow-trigger "push"',
]
GH_PINNED = [
    "--repo VladyslavHaina/logweir",
    "--signer-workflow VladyslavHaina/logweir/.github/workflows/images.yml",
    "--source-ref refs/heads/main",
    "--cert-oidc-issuer https://token.actions.githubusercontent.com",
]


def cosign(args, verb="verify"):
    return f"cosign {verb} \\\n  " + " \\\n  ".join(args + ['"docker.io/ns/logweir@$DIGEST"']) + "\n"


def run(root, *extra):
    return subprocess.run(
        [sys.executable, str(GATE), "--root", str(root), *extra],
        capture_output=True, text=True, timeout=60,
    )


class Gate(unittest.TestCase):
    def plant(self, rel, text):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return tmp.name

    def refused(self, rel, text, needle):
        result = run(self.plant(rel, text), "--no-sites")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(needle, result.stderr)

    def accepted(self, rel, text):
        result = run(self.plant(rel, text), "--no-sites")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn(" 0 verification", result.stdout)

    # The positive controls.
    def test_the_fully_pinned_commands_pass(self):
        self.accepted("scripts/x.sh", cosign(PINNED))
        self.accepted("scripts/x.sh", cosign(["--type spdxjson"] + PINNED, "verify-attestation"))
        self.accepted("docs/x.md", "```bash\n" + cosign(PINNED) + "```\n")
        self.accepted("docs/x.md", "```bash\ngh attestation verify oci://x \\\n  " + " \\\n  ".join(GH_PINNED) + "\n```\n")
        # `--flag=value` is the same pin.
        self.accepted("scripts/x.sh", "cosign verify " + " ".join(
            p.replace(" ", "=", 1) for p in PINNED) + " img\n")

    # The review's finding, exactly: an identity regexp and no caller pins.
    def test_the_reviewed_command_is_refused(self):
        self.refused("scripts/ci-images.sh", (
            'cosign verify --certificate-identity-regexp '
            '"^https://github\\.com/${GITHUB_REPOSITORY:?}/\\.github/workflows/images\\.yml@refs/heads/main$" '
            '--certificate-oidc-issuer "$issuer" "docker.io/$NS/$product@$digest"\n'),
            "--certificate-identity-regexp: a regular expression is not a pin")

    def test_each_missing_pin_is_refused(self):
        for i, pin in enumerate(PINNED):
            flag = pin.split()[0]
            with self.subTest(flag=flag):
                self.refused("scripts/x.sh", cosign(PINNED[:i] + PINNED[i + 1:]), f"no {flag}")
                self.refused("docs/x.md", "```\n" + cosign(PINNED[:i] + PINNED[i + 1:], "verify-attestation") + "```\n",
                             f"no {flag}")
        for i, pin in enumerate(GH_PINNED):
            flag = pin.split()[0]
            with self.subTest(flag=flag):
                text = "gh attestation verify oci://x " + " ".join(GH_PINNED[:i] + GH_PINNED[i + 1:]) + "\n"
                self.refused("scripts/x.sh", text, f"no {flag}")

    def test_another_value_is_refused(self):
        for old, new in (
            ("VladyslavHaina/logweir\"", "attacker/logweir\""),
            ("refs/heads/main\"", "refs/heads/feature\""),
            ('"push"', '"workflow_dispatch"'),
            ("images.yml@refs/heads/main", "images.yml@refs/heads/other"),
            ("token.actions.githubusercontent.com", "accounts.google.com"),
        ):
            with self.subTest(new=new):
                self.refused("scripts/x.sh", cosign([p.replace(old, new) for p in PINNED]),
                             "must be exactly")

    def test_an_issuer_regexp_is_refused(self):
        self.refused("scripts/x.sh", cosign(PINNED + ['--certificate-oidc-issuer-regexp ".*"']),
                     "a regular expression is not a pin")

    def test_prose_is_not_a_command_but_a_fenced_block_is(self):
        self.accepted("docs/x.md", "Run `cosign verify` on the index digest.\n\n```bash\n" + cosign(PINNED) + "```\n")
        self.refused("docs/x.md", "```\ncosign verify img\n```\n", "no --certificate-identity")
        # A shell comment is not a command; the line after it is.
        self.refused("scripts/x.sh", "# cosign verify img\ncosign verify img\n", "no --certificate-identity")

    def test_the_sites_must_carry_verifications(self):
        result = run(self.plant("scripts/ci-images.sh", "echo nothing\n"))
        self.assertEqual(result.returncode, 1)
        self.assertIn("the gate would pass by finding nothing", result.stderr)

    # The repository itself.
    def test_the_repository_pins_every_verification(self):
        result = run(ROOT)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
