"""Offline guards for the PLAT-17.2 live harness (no cluster, no network).

Each test reads the committed files and fails on the shape it forbids; the
planted twins at the bottom show every check can fail.
"""

from __future__ import annotations

import pathlib
import py_compile
import re

HERE = pathlib.Path(__file__).resolve().parent
PY = sorted(p for p in HERE.glob("*.py") if p.name != "test_harness.py")
SH = sorted(HERE.glob("*.sh"))

# Credential-shaped literals (WORKER-RULES, 2026-09-18): a PEM private-key
# header, a JWT, an AWS-style key id, and the fixed client secret this harness
# used before it minted one per run.
FORBIDDEN = {
    "pem-private-key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "jwt": re.compile(r"eyJ[A-Za-z0-9_-]{4,}"),
    "aws-key-id": re.compile(r"AKIA[0-9A-Z]{16}"),
    "fixed-client-secret": re.compile(r"p172-client-secret"),
}
HOST_PATH = re.compile(r"/Users/|/tmp/logweir-roadmap-run/venv|artifacts/lab-refresh-9|lw-lr9p172")


def credential_hits(text: str) -> list[str]:
    return [name for name, pattern in FORBIDDEN.items() if pattern.search(text)]


def kubectl_without_context(text: str) -> list[str]:
    """Every kubectl INVOCATION names docker-desktop: a shell word `kubectl`
    followed by anything but `--context`, or a Python argv element "kubectl"
    not followed by "--context". `$K` (env.sh) and the Python `kubectl()`
    helper carry it once."""
    shell = re.compile(r"(?<![\w$\"'./-])kubectl\s+(?!--context\b)")
    argv = re.compile(r"[\"']kubectl[\"']\s*,(?!\s*[\"']--context[\"'])")
    bad = []
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip().startswith("#"):
            continue
        if shell.search(line) or argv.search(line):
            bad.append(f"{n}: {line.strip()[:100]}")
    return bad


def test_every_python_file_compiles():
    for path in PY:
        py_compile.compile(str(path), doraise=True)


def test_no_credential_shaped_literal_and_no_host_path():
    for path in PY + SH + [HERE / "README.md"]:
        text = path.read_text()
        assert credential_hits(text) == [], (path.name, credential_hits(text))
        if path.name != "README.md":
            assert not HOST_PATH.search(text), (path.name, HOST_PATH.search(text).group(0))


def test_every_kubectl_names_the_docker_desktop_context():
    for path in PY + SH:
        assert kubectl_without_context(path.read_text()) == [], (path.name, kubectl_without_context(path.read_text()))


def test_cleanup_deletes_only_what_the_owner_label_names():
    cleanup = (HERE / "cleanup.sh").read_text()
    assert '[ "$owner" = "$OWNER" ]' in cleanup
    assert "delete ns" in cleanup and "rm -f \"$OUT\"/kubeconfig-*" in cleanup
    expired = (HERE / "live_p172_expired.py").read_text()
    assert 'get("logweir.dev/test-owner") == OWNER and meta["uid"] == uid' in expired
    run = (HERE / "run.sh").read_text()
    assert "trap finish EXIT" in run and "PIPESTATUS" not in run


def test_the_client_secret_is_passed_by_path():
    idp = (HERE / "mock_idp.py").read_text()
    assert "with open(sys.argv[3])" in idp
    for name in ("live_p172.py", "live_p172_expired.py"):
        assert "CLIENT_SECRET_FILE" in (HERE / name).read_text()


# --- planted twins: each check can fail ------------------------------------


def test_planted_credentials_are_found():
    jwt = "ey" + "JhbGciOiJFUzI1NiJ9"
    assert credential_hits(jwt) == ["jwt"]
    assert credential_hits("-----BEGIN " + "PRIVATE KEY-----") == ["pem-private-key"]
    assert credential_hits("p172-" + "client-secret") == ["fixed-client-secret"]


def test_a_planted_kubectl_without_context_is_found():
    assert kubectl_without_context("kubectl get ns\n") != []
    assert kubectl_without_context('subprocess.run(["kubectl", "get", "ns"])\n') != []
    assert kubectl_without_context('$K get ns\nkubectl --context docker-desktop get ns\n'
                                   'K = ["kubectl", "--context", "docker-desktop"]\n') == []
