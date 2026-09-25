"""Offline guards for the PoC install's live harness (scripts/live/poc/).

  python3 -m pytest scripts/live/poc/test_poc_harness.py

Every kubectl invocation names `--context docker-desktop`; no file carries a credential-shaped
literal or a host-specific path; secrets are read from the credentials file or the cluster and
never printed. Each guard is a function of a file's text, so a planted twin can fail it
(`test_the_guards_catch_their_planted_twins`).
"""
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
FILES = sorted(p for p in HERE.iterdir() if p.suffix in (".py", ".mjs") and p.name != "test_poc_harness.py")

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


def unguarded_minted_name_fills(text: str) -> list[str]:
    """The shared console has NO name field on the Clusters form (P7) or the schedule form
    (poc-fixes-2 review L5): the product API names both objects. A harness that fills one
    unconditionally waits out Playwright's 30 s and fails the journey. Every fill of a
    connection or schedule name must sit behind a `count() > 0` guard on the same line."""
    bad = []
    blocks = []
    m = re.search(r"export async function createCluster\(.*?\n}\n", text, re.S)
    if m:
        blocks.append(m.group(0))
    m = re.search(r'if \(step\("J4"\)\).*?\n  }\n', text, re.S)
    if m:
        blocks.append(m.group(0))
    for block in blocks:
        for line in block.splitlines():
            if ".fill(" in line and ('name="name"' in line or "Name.fill(" in line or "nameInput.fill(" in line):
                if "count()" not in line:
                    bad.append(line.strip())
    return bad


def test_a_name_the_console_mints_is_filled_only_where_the_field_exists():
    for p in FILES:
        assert not unguarded_minted_name_fills(p.read_text()), (p.name, unguarded_minted_name_fills(p.read_text()))


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
    planted = ("export async function createCluster(page, ns, c, log) {\n"
               "  await form.locator('input[name=\"name\"]').fill(c.name);\n}\n")
    assert unguarded_minted_name_fills(planted)
    guarded = ("export async function createCluster(page, ns, c, log) {\n"
               "  if (await nameInput.count() > 0) await nameInput.fill(c.name);\n}\n")
    assert not unguarded_minted_name_fills(guarded)

def keeps_a_csrf_value(text: str) -> list[str]:
    """A row's evidence must never hold the session's synchronizer token: a captured
    `x-csrf-token` header or a session's `csrfToken` is only ever COMPARED (`===`/`==`)
    where it is read, and the boolean is what is kept (poc-upgrade-1's P4 viewer row once
    stored the header value in its evidence and printed it)."""
    bad = []
    for line in text.splitlines():
        for m in re.finditer(r':\s*[A-Za-z_.]*\(?\)?\[?"?(x-csrf-token"\]|csrfToken)', line):
            rest = line[m.end():].lstrip()
            if not (rest.startswith("===") or rest.startswith("==")):
                bad.append(line.strip())
    return bad


def test_no_row_keeps_a_csrf_value():
    for p in FILES:
        assert not keeps_a_csrf_value(p.read_text()), (p.name, keeps_a_csrf_value(p.read_text()))


def test_the_csrf_guard_catches_its_planted_twin():
    assert keeps_a_csrf_value('vposts.push({ csrf: r.headers()["x-csrf-token"] || null });')
    assert keeps_a_csrf_value('row("x", true, { token: sess.csrfToken });')
    assert not keeps_a_csrf_value('vposts.push({ sessionToken: r.headers()["x-csrf-token"] === vsess.csrfToken });')
