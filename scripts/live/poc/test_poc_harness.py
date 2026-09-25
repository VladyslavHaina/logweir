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


# --------------------------------------------------------------------------------------------
# THE ONE-STEP WIZARD (console-ux-1, MCP-29) AND THE CREATE-DESTINATION DISCLOSURE (MCP-10).
# Every control a journey fills or clicks on the restore wizard is on ONE of six steps, hidden
# while another is on screen; the destination form is hidden until its disclosure is opened.
# `scripts/live/console_steps.py` reads each flow and requires the walk to the control's step
# (or the opening) above the action, in the same function -- the offline half of what the next
# PoC round proves live.
import sys  # noqa: E402

sys.path.insert(0, str(HERE.parent))
import console_steps  # noqa: E402

MJS = [p for p in FILES if p.suffix == ".mjs"]


def test_every_flow_reaches_the_step_of_each_wizard_control_it_drives():
    driving = []
    for p in MJS:
        text = p.read_text()
        assert not console_steps.unreached_controls(text), (p.name, console_steps.unreached_controls(text))
        if console_steps.controls_driven(text) > 0:
            driving.append(p.name)
    # NOT VACUOUS: the three files that drive the wizard are the ones read.
    assert {"console.mjs", "reproof.mjs", "restore_burst.mjs"} <= set(driving), driving


def test_the_step_of_each_control_is_the_pages_own():
    assert console_steps.ui_disagreements() == []


def unreached(js: str) -> list[str]:
    return console_steps.unreached_controls(js)


def test_the_wizard_guard_catches_its_planted_twins():
    opened = 'await openWizard(page, "#/restore?ns=a&uid=u");\n'
    # a control filled with no walk: the wizard opens on step 1, the prefix is step 4's
    assert unreached(opened + 'await page.fill("#topic-prefix", "x");\n')
    assert not unreached(opened + 'await wizardStep(page, 4);\nawait page.fill("#topic-prefix", "x");\n')
    # a walk to the wrong step, and a wait for a hidden step's element
    assert unreached(opened + 'await wizardStep(page, 3);\nawait page.fill("#topic-prefix", "x");\n')
    assert unreached(opened + 'await page.waitForSelector("#create-restore");\n')
    assert unreached(opened + 'await waitFor(page, "#step-target", "the target");\n')
    # a deep link opens its own step
    assert not unreached('await openWizard(page, "#/restore?ns=a&step=6");\nawait page.click("#create-restore");\n')
    # a navigation after the walk leaves no step known
    assert unreached(opened + 'await wizardStep(page, 6);\nawait gotoHash(page, "#/history");\n'
                     'await page.click("#create-restore");\n')
    # through a locator on the line, and through a variable holding one
    assert unreached(opened + "await page.locator('input[name=\"topicPrefix\"]').blur();\n")
    assert unreached('const field = page.locator("#archive-secret");\n' + opened +
                     'await wizardStep(page, 5);\nawait field.fill("other");\n')
    # a helper starts with no step; the caller's own step comes back after its body
    assert unreached('async function helper(page) {\n  await page.click("#create-restore");\n}\n')
    assert not unreached(opened + 'await wizardStep(page, 6);\n'
                         'const walk = async (p) => {\n  await wizardStep(p, 4);\n};\n'
                         'await page.click("#create-restore");\n')
    # a comment, and a wait for "this field's error OR the page's failure", are not step actions
    assert not unreached('// await page.click("#create-restore");\n')
    assert not unreached('await page.waitForSelector("#target-cluster-error, .mutation-failed");\n')
    # the destination form: filled only after its disclosure is opened
    assert unreached('await gotoHash(page, "#/destinations?ns=a");\nawait page.fill("#destination-name", "d");\n')
    assert not unreached('await gotoHash(page, "#/destinations?ns=a");\nawait openDestinationCreate(page);\n'
                         'await page.fill("#destination-name", "d");\n')


def test_the_ui_check_catches_a_page_that_moved_a_control():
    wizard = console_steps.WIZARD_JS.read_text()
    assert console_steps.ui_disagreements(wizard=wizard) == []
    # Create moved out of step 6's renderer.
    moved = wizard.replace('id=\\"create-restore\\"', 'id=\\"create-restore-moved\\"')
    assert any("#create-restore" in d for d in console_steps.ui_disagreements(wizard=moved))
    # FIELD_STEP says the prefix is step 3's.
    shifted = wizard.replace("  topicPrefix: 3,", "  topicPrefix: 2,")
    assert shifted != wizard and any("topicPrefix" in d for d in console_steps.ui_disagreements(wizard=shifted))
    # a step renamed on the page and not in scripts/console-steps.mjs
    renamed = wizard.replace('{ id: "step-plan", title: "Plan, hash and names" }',
                             '{ id: "step-plan", title: "Review and create" }')
    assert renamed != wizard and any("WIZARD_STEPS" in d for d in console_steps.ui_disagreements(wizard=renamed))
    # the destination form out of its disclosure
    destinations = console_steps.DESTINATIONS_JS.read_text()
    loose = destinations.replace('"<details class=\\"create-disclosure\\" id=\\"destination-create-disclosure\\""',
                                 '"<div class=\\"create-disclosure\\" id=\\"destination-create-disclosure-x\\""')
    assert loose != destinations and console_steps.ui_disagreements(destinations_js=loose)


# --------------------------------------------------------------------------------------------
# A POINT ON A PAGINATED LIST IS ABSENT, NOT HIDDEN (console-ux-1, MCP-26; poc-upgrade-2 J6).
# The schedule card's point, run and manual-run tables and the wizard's selector hold only
# their current page in the DOM ("1-20 of 280 recovery points"), so a row that counts a
# point's "Restore this point" link right after navigation fails for any point past the first
# page -- or passes only while the namespace is young. Every row that asserts such an offer
# must first reveal the point the way a person does: `revealInGrid` (the grid's own filter)
# or `showEveryPoint` (the selector's "Show more"), within the lines above it.
def unrevealed_point_offers(text: str, window: int = 14) -> list[str]:
    lines = text.splitlines()
    bad = []
    for i, line in enumerate(lines):
        if re.search(r"row\(.*offers 'Restore this point'", line):
            above = "\n".join(lines[max(0, i - window): i])
            if "revealInGrid(" not in above and "showEveryPoint(" not in above:
                bad.append(line.strip()[:160])
    return bad


def test_every_point_offer_row_reveals_the_point_first():
    for p in MJS:
        assert not unrevealed_point_offers(p.read_text()), (p.name, unrevealed_point_offers(p.read_text()))


def test_the_point_offer_guard_catches_its_planted_twin():
    # the J6 row as it stood before poc-upgrade-2: the link counted straight after navigation
    planted = ('    await waitForText(page, /Restore this point/, 90, "the schedule\'s points");\n'
               '    const link = page.locator(`a[href*="backup=${b.metadata.name}"]`, { hasText: /Restore this point/ }).first();\n'
               '    row("J6 the schedule page offers \'Restore this point\' for the run", (await link.count()) > 0, {});\n')
    assert unrevealed_point_offers(planted)
    fixed = planted.replace("    row(", "    await revealInGrid(page, link, b.metadata.name);\n    row(")
    assert not unrevealed_point_offers(fixed)
