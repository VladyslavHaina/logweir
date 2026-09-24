"""Offline guards for the PoC install's live harness (scripts/live/poc/).

  python3 -m pytest scripts/live/poc/test_harness.py

Every kubectl invocation names `--context docker-desktop`; no file carries a credential-shaped
literal or a host-specific path; secrets are read from the credentials file or the cluster and
never printed. Each guard is a function of a file's text, so a planted twin can fail it
(`test_the_guards_catch_their_planted_twins`).
"""
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
FILES = sorted(p for p in HERE.iterdir() if p.suffix in (".py", ".mjs") and p.name != "test_harness.py")

PEM = "-----BEGIN " + "PRIVATE KEY"
JWT = re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")
AWS = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
HOST_PATH = re.compile(r"/Users/[a-z]+/|/home/[a-z]+/")


def kubectl_without_context(text: str) -> list[str]:
    """Every place a kubectl command is assembled must name the docker-desktop context."""
    bad = []
    for m in re.finditer(r'"kubectl"', text):
        window = text[m.start(): m.start() + 160]
        if '"--context", "docker-desktop"' not in window:
            bad.append(window.split("\n")[0])
    return bad


def secret_literals(text: str) -> list[str]:
    found = []
    if PEM in text:
        found.append("PEM private key header")
    found += JWT.findall(text) + AWS.findall(text)
    return found


def prints_a_secret(text: str) -> list[str]:
    """A password, client secret or session value must never reach print/console.log."""
    return [line.strip() for line in text.splitlines()
            if re.search(r"(print|console\.log)\(.*\b(pw|password|secretAccessKey|clientSecret|session\[\"session\"\])\b", line)]


def test_there_are_files_to_guard():
    names = {p.name for p in FILES}
    assert {"poclib.py", "console.mjs", "journey.mjs", "p172_ingress.py"} <= names, names


def test_every_kubectl_names_the_docker_desktop_context():
    for p in FILES:
        assert not kubectl_without_context(p.read_text()), (p.name, kubectl_without_context(p.read_text()))


def test_no_credential_shaped_literal():
    for p in FILES:
        assert not secret_literals(p.read_text()), (p.name, secret_literals(p.read_text()))


def test_no_host_path():
    for p in FILES:
        assert not HOST_PATH.search(p.read_text()), p.name


def test_no_secret_is_printed():
    for p in FILES:
        assert not prints_a_secret(p.read_text()), (p.name, prints_a_secret(p.read_text()))


def test_the_guards_catch_their_planted_twins():
    assert kubectl_without_context('run(["kubectl", "-n", "x", "get", "pods"])')
    assert not kubectl_without_context('K = ["kubectl", "--context", "docker-desktop", "--request-timeout=60s"]')
    assert secret_literals(PEM + "-----")
    assert secret_literals("AKIA" + "ABCDEFGHIJKLMNOP")
    assert HOST_PATH.search("/Users/someone/.kube/config")
    assert prints_a_secret('print(pw)') and prints_a_secret('console.log(secretAccessKey)')
