"""Offline guard: a live harness drives each hidden-until-reached console control where it is shown.

The twin of scripts/console-steps.mjs, which the harnesses walk with; this reads them.

console-ux-1 (MCP-29) renders the restore wizard ONE STEP AT A TIME: all six steps are in the
document and every one but the step on screen is `hidden`. A harness that fills, clicks,
selects, ticks, tabs to or waits for a control while another step is on screen waits out
Playwright's timeout live, and fails the journey there -- the next PoC round, not here. So this
module reads a harness's text, finds every such action on a wizard control, and requires that
the flow walked to the control's step first: the nearest step-setting call above the action,
in the same function, names that step.

  step-setting calls   wizardStep(p, N), wizardAt(p, N), wizardStepByKeyboard(p, N, ...)
                       (scripts/console-steps.mjs), and openWizard(p, route) -- step N when
                       the route says `step=N`, else 1 (scripts/live/poc/console.mjs)
  step-clearing calls  a navigation: goto(, gotoHash(, open(, openRoute(, freshPage(,
                       `location.hash =`, waitForURL( -- the wizard is not known to be on
                       screen until a step-setting call says so
  actions              fill click dblclick check uncheck setChecked selectOption type press tap
                       hover waitForSelector (visible by default) and the harnesses' own
                       waitFor / waitForSelector / tabTo wrappers, on a wizard selector -- given
                       as the call's first string, through `.locator("<sel>")` on the same
                       line, or through a variable holding `.locator("<sel>")`

A nested function starts with no step and the enclosing one gets its own step back when the
nested body closes, so a helper must walk to the steps it drives itself.

THE SAME RULE FOR THE CREATE-DESTINATION FORM (MCP-10), which sits behind a closed "Create
destination" disclosure: a fill or click on its inputs (`#destination-name`, `-bucket`, ... and
`#destination-form` itself) must follow `openDestinationCreate(p)` in the same function, with no
navigation between. Its context is spelled DESTINATION_FORM where a wizard step is a number.

WHICH STEP A CONTROL IS ON is the page's own statement, not this file's: `ui_disagreements`
reads ui/pages/restore-wizard.js and requires every entry of WIZARD_CONTROLS to be rendered by
its step's render function (or a helper that function calls), every named input to agree with
the page's FIELD_STEP, and the step titles to be scripts/console-steps.mjs's WIZARD_STEPS.
"""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
WIZARD_JS = ROOT / "ui" / "pages" / "restore-wizard.js"
SELECT_JS = ROOT / "ui" / "select.js"
DESTINATIONS_JS = ROOT / "ui" / "pages" / "destinations.js"
CONSOLE_STEPS = ROOT / "scripts" / "console-steps.mjs"

# (pattern over a selector string, step 1-6, witness the step's renderers must contain).
# Only selectors that name the wizard and nothing else: `input[name="endpoint"]`, `region`,
# `archiveSecret` and `select[name="mode"]` are also inputs of other pages, so a harness drives
# the wizard's by id (#store-endpoint, #store-region, #archive-secret, #target-mode).
WIZARD_CONTROLS: tuple[tuple[str, int, str], ...] = (
    (r"#step-archive\b", 1, 'id=\\"step-archive\\"'),
    (r"#archive-secret\b", 1, 'id=\\"archive-secret\\"'),
    (r"#store-endpoint\b", 1, 'id=\\"store-endpoint\\"'),
    (r"#store-region\b", 1, 'id=\\"store-region\\"'),
    (r"#store-pathStyle\b", 1, 'id=\\"store-pathStyle\\"'),
    (r"#store-allow-insecure\b", 1, 'id=\\"store-allow-insecure\\"'),
    (r'name=\\?"pathStyle\\?"', 1, 'name=\\"pathStyle\\"'),
    (r'name=\\?"allowHttp\\?"', 1, 'name=\\"allowHttp\\"'),
    (r"#evidence-bucket\b", 1, 'id=\\"evidence-bucket\\"'),
    (r'name=\\?"evidenceBucket\\?"', 1, 'name=\\"evidenceBucket\\"'),
    (r"#evidence-destination\b(?!-)", 1, 'id=\\"evidence-destination\\"'),
    (r'name=\\?"evidenceDestination\\?"', 1, 'name=\\"evidenceDestination\\"'),
    (r"#evidence-same-store\b", 1, 'id=\\"evidence-same-store\\"'),
    (r"#evidence-endpoint\b", 1, 'id=\\"evidence-endpoint\\"'),
    (r"#evidence-region\b", 1, 'id=\\"evidence-region\\"'),
    (r"#evidence-allow-insecure\b", 1, 'id=\\"evidence-allow-insecure\\"'),
    (r"#legacy-evidence-bucket\b", 1, 'id=\\"legacy-evidence-bucket\\"'),
    (r"#step-backup-set\b", 2, 'id=\\"step-backup-set\\"'),
    (r"#point-uid\b", 2, 'id=\\"point-uid\\"'),
    (r"#point-name\b", 2, 'id=\\"point-name\\"'),
    (r"#catalog-topics\b", 2, 'id=\\"catalog-topics\\"'),
    (r'name=\\?"catalogTopics\\?"', 2, 'name=\\"catalogTopics\\"'),
    (r"#choose-another-point\b", 2, 'id=\\"choose-another-point\\"'),
    (r"#step-point-in-time\b", 3, 'id=\\"step-point-in-time\\"'),
    (r"#point-in-time\b(?!-)", 3, 'id=\\"point-in-time\\"'),
    (r"#point-in-time-complaint\b", 3, 'id=\\"point-in-time-complaint\\"'),
    (r'name=\\?"pointInTime\\?"', 3, 'name=\\"pointInTime\\"'),
    (r"#step-target\b", 4, 'id=\\"step-target\\"'),
    (r"#target-cluster(-search|-uid|-name|-error)?\b", 4, 'id: "target-cluster"'),
    (r'name=\\?"targetCluster\\?"', 4, 'name: "targetCluster"'),
    (r"#target-mode\b", 4, 'id=\\"target-mode\\"'),
    (r"#topic-prefix\b", 4, 'id=\\"topic-prefix\\"'),
    (r'name=\\?"topicPrefix\\?"', 4, 'name=\\"topicPrefix\\"'),
    (r"\.topic-box\b", 4, 'class=\\"topic-box\\"'),
    (r"#topic-\d+\b", 4, 'id=\\"topic-" + String(i)'),
    (r"#subset-topics-", 4, 'id: "subset-topics"'),
    (r"#select-(all|no)-topics\b", 4, 'id=\\"select-all-topics\\"'),
    (r"#step-preflight\b", 5, 'id=\\"step-preflight\\"'),
    (r"#restore-readiness-(start|cancel)\b", 5, 'id=\\"restore-readiness-start\\"'),
    (r"#step-plan\b", 6, 'id=\\"step-plan\\"'),
    (r"#plan-bytes\b", 6, 'id=\\"plan-bytes\\"'),
    (r"#plan-hash-value\b", 6, 'id=\\"plan-hash-value\\"'),
    (r"#create-restore\b", 6, 'id=\\"create-restore\\"'),
    (r"#change-ticket\b", 6, 'id=\\"change-ticket\\"'),
    (r'name=\\?"ticket\\?"', 6, 'name=\\"ticket\\"'),
    (r"#copy-plan\b", 6, 'id=\\"copy-plan\\"'),
    (r"#download-plan\b", 6, 'id=\\"download-plan\\"'),
    (r"#go-to-readiness\b", 6, 'id=\\"go-to-readiness\\"'),
    (r"#approval-policy-[a-z-]+", 6, 'id=\\"approval-policy-ordinary-unavailable\\"'),
    (r"#readiness-(blocked|not-run)\b", 6, 'id=\\"readiness-blocked\\"'),
)

# The create-destination form's own controls, shown only once its disclosure is open. Its
# rotation form, access test and adoption form are outside the disclosure and are not listed.
DESTINATION_FORM = "the opened create-destination form"
DESTINATION_CONTROLS = re.compile(
    r"#destination-(form\b|name\b|description\b|bucket\b|prefix\b|region\b|endpoint\b|security-|"
    r"addressing-|archiveWrite-|archiveRead-|evidenceRead-|evidenceWrite-|write-probe\b|default\b)")
DESTINATION_WITNESSES = ('id=\\"destination-create-disclosure\\"', 'id=\\"destination-form\\"',
                         'id=\\"destination-name\\"', 'id=\\"destination-bucket\\"')

# The named inputs FIELD_STEP (0-based) speaks for, keyed by the name a harness selects by.
FIELD_NAMES = {"pathStyle": None, "allowHttp": None, "evidenceBucket": None,
               "evidenceDestination": "evidenceDestination", "catalogTopics": None,
               "pointInTime": "pointInTime", "targetCluster": "targetCluster",
               "topicPrefix": "topicPrefix", "ticket": "ticket"}

ACTION_VERBS = ("fill", "click", "dblclick", "check", "uncheck", "setChecked", "selectOption",
                "type", "press", "tap", "hover", "waitForSelector")
_ACTION_ON_PAGE = re.compile(
    r"\.(" + "|".join(ACTION_VERBS) + r")\(\s*(?P<q>[\"'`])(?P<sel>(?:\\.|(?!(?P=q)).)*)(?P=q)")
# The harnesses' own wrappers: waitFor(sel, ...), waitFor(page, sel, ...), waitForSelector(page,
# sel, ...), tabTo(page, sel, ...) -- each waits for (or Tabs to) a VISIBLE element.
_WRAPPER = re.compile(
    r"(?<![\w.])(waitFor|waitForSelector|tabTo)\(\s*(?:[\w.]+\s*,\s*)?(?P<q>[\"'`])"
    r"(?P<sel>(?:\\.|(?!(?P=q)).)*)(?P=q)")
_LOCATOR = re.compile(
    r"\.locator\(\s*(?P<q>[\"'`])(?P<sel>(?:\\.|(?!(?P=q)).)*)(?P=q)\s*\)(?P<rest>[^;]*)")
_LOCATOR_VAR = re.compile(
    r"(?:const|let)\s+(?P<var>\w+)\s*=\s*[\w.]+\.locator\(\s*(?P<q>[\"'`])"
    r"(?P<sel>(?:\\.|(?!(?P=q)).)*)(?P=q)\s*\)\s*;")
_VERB_AFTER = re.compile(r"^\s*(?:\.first\(\)\s*)?\.(" + "|".join(ACTION_VERBS + ("blur",)) + r")\(")
_STEP_CALL = re.compile(r"\b(?:wizardStep|wizardAt|wizardStepByKeyboard)\(\s*[\w.]+\s*,\s*([1-6])\s*[,)]")
_OPEN_WIZARD = re.compile(r"\bopenWizard\(")
_OPEN_DESTINATION = re.compile(r"\bopenDestinationCreate\(")
_NAVIGATION = re.compile(r"(\.goto\(|(?<![\w.])(gotoHash|open|openRoute|freshPage)\(|"
                         r"location\.hash\s*=[^=]|\.waitForURL\()")
_FUNCTION_START = re.compile(
    r"^\s*(?:export\s+)?(?:async\s+)?function\s*\w*\s*\(|"
    r"^\s*(?:const|let)\s+\w+\s*=\s*(?:async\s*)?(?:\([^)]*\)|\w+)\s*=>\s*\{")


def _step_of_one(selector: str) -> int | str | None:
    if DESTINATION_CONTROLS.search(selector):
        return DESTINATION_FORM
    for pattern, step, _ in WIZARD_CONTROLS:
        if re.search(pattern, selector):
            return step
    return None


def step_of(selector: str) -> int | str | None:
    """The wizard step a selector names (or DESTINATION_FORM), or None when it names no control
    that is hidden until reached. A selector
    LIST (`a, b`) names a step only when every alternative names that same step: a wait for
    "this field's error, or the page's failure" is not a wait for one step."""
    steps = {_step_of_one(part) for part in selector.split(",")}
    return steps.pop() if len(steps) == 1 else None


def _mask(text: str) -> str:
    """The text with every string, template, regex literal and comment blanked (newlines kept),
    so braces and keywords are counted only where they are code."""
    out = list(text)
    i, n = 0, len(text)
    prev = ""  # the last significant code character, to tell a regex from a division

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            j = n if j == -1 else j
            blank(i, j)
            i = j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            j = n if j == -1 else j + 2
            blank(i, j)
            i = j
            continue
        if c in "\"'`":
            j = i + 1
            while j < n and text[j] != c:
                j += 2 if text[j] == "\\" else 1
            blank(i + 1, j)
            i = j + 1
            prev = c
            continue
        if c == "/" and (prev == "" or prev in "(,=:[!&|?{};+-*%~^<>\n"):
            j, klass = i + 1, False
            while j < n and text[j] != "\n":
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == "[":
                    klass = True
                elif text[j] == "]":
                    klass = False
                elif text[j] == "/" and not klass:
                    break
                j += 1
            if j < n and text[j] == "/":
                blank(i + 1, j)
                i = j + 1
                prev = "/"
                continue
        if not c.isspace():
            prev = c
        i += 1
    return "".join(out)


def _where(context: int | str | None) -> str:
    if context is None:
        return "no step"
    return context if isinstance(context, str) else "step %d" % context


def unreached_controls(text: str) -> list[str]:
    """Every action on a hidden-until-reached control that is not preceded, in its own function,
    by the walk (or the opening) that shows it: `line N: <code> -- step S, but the flow is on
    <where>`."""
    masked = _mask(text).split("\n")
    lines = text.split("\n")
    depth = 0
    stack: list[tuple[int, int | None, dict]] = []
    step: int | str | None = None
    variables: dict[str, str] = {}
    bad: list[str] = []
    for number, (line, code) in enumerate(zip(lines, masked), start=1):
        if _FUNCTION_START.search(code):
            stack.append((depth, step, dict(variables)))
            step = None
        for m in _LOCATOR_VAR.finditer(line):
            variables[m.group("var")] = m.group("sel")
        # the events on this line, in order of position
        events: list[tuple[int, str, object]] = []
        # a function's own DEFINITION is not a call to it
        called = lambda m: re.search(r"function\s+$", code[:m.start()]) is None  # noqa: E731
        for m in filter(called, _STEP_CALL.finditer(code)):
            events.append((m.start(), "set", int(line[m.start(1)])))
        for m in filter(called, _OPEN_WIZARD.finditer(code)):
            wanted = re.search(r"[?&]step=([1-6])(?![0-9])", line[m.start():])
            events.append((m.start(), "set", int(wanted.group(1)) if wanted else 1))
        for m in filter(called, _OPEN_DESTINATION.finditer(code)):
            events.append((m.start(), "set", DESTINATION_FORM))
        for m in _NAVIGATION.finditer(code):
            if not _OPEN_WIZARD.match(code, m.start()):
                events.append((m.start(), "clear", None))
        for m in _ACTION_ON_PAGE.finditer(line):
            events.append((m.start(), "act", m.group("sel")))
        for m in _WRAPPER.finditer(line):
            events.append((m.start(), "act", m.group("sel")))
        for m in _LOCATOR.finditer(line):
            if _VERB_AFTER.match(m.group("rest")):
                events.append((m.start(), "act", m.group("sel")))
        for var, sel in variables.items():
            for m in re.finditer(r"(?<![\w.])" + re.escape(var) + r"\.(" + "|".join(ACTION_VERBS + ("blur",)) + r")\(", code):
                events.append((m.start(), "act", sel))
        # an action spelled inside a comment or a string is not an action
        events = [e for e in events if e[1] != "act" or (e[0] < len(code) and code[e[0]] != " ")]
        for _, kind, value in sorted(events, key=lambda e: e[0]):
            if kind == "set":
                step = value  # type: ignore[assignment]
            elif kind == "clear":
                step = None
            else:
                wanted = step_of(str(value))
                if wanted is not None and wanted != step:
                    bad.append("line %d: %s -- %s, but the flow is on %s" % (
                        number, line.strip()[:140], _where(wanted), _where(step)))
        for ch in code:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                while stack and depth <= stack[-1][0]:
                    _, step, variables = stack.pop()
    return bad


def controls_driven(text: str) -> int:
    """How many actions on hidden-until-reached controls a harness has (non-vacuity)."""
    count = 0
    for line in text.split("\n"):
        for rx in (_ACTION_ON_PAGE, _WRAPPER):
            count += sum(1 for m in rx.finditer(line) if step_of(m.group("sel")) is not None)
        count += sum(1 for m in _LOCATOR.finditer(line)
                     if _VERB_AFTER.match(m.group("rest")) and step_of(m.group("sel")) is not None)
    return count


# --------------------------------------------------------------------------------------------
# The page's own statement of which control is on which step.

def _functions(text: str) -> dict[str, str]:
    starts = [(m.start(), m.group(1)) for m in
              re.finditer(r"^(?:export )?(?:async )?function (\w+)\(", text, re.M)]
    return {name: text[pos:(starts[i + 1][0] if i + 1 < len(starts) else len(text))]
            for i, (pos, name) in enumerate(starts)}


def step_renderers(wizard: str | None = None, select: str | None = None) -> dict[int, str]:
    """Each step's rendered source, 1-6: its render function (from `wizardPage(s, i, fn(`) and
    every render helper it calls, transitively, in ui/pages/restore-wizard.js and ui/select.js."""
    wizard = WIZARD_JS.read_text() if wizard is None else wizard
    select = SELECT_JS.read_text() if select is None else select
    bodies = _functions(select)
    bodies.update(_functions(wizard))
    out: dict[int, str] = {}
    for index, fn in re.findall(r"wizardPage\(s, (\d), (\w+)\(", wizard):
        seen: list[str] = []
        todo = [fn]
        while todo:
            name = todo.pop()
            if name in seen or name not in bodies:
                continue
            seen.append(name)
            todo += re.findall(r"\b(render\w+|approvalPolicyBlock|submissionStatus)\(", bodies[name])
        out[int(index) + 1] = "\n".join(bodies[name] for name in seen)
    return out


def ui_disagreements(wizard: str | None = None, select: str | None = None,
                     steps_module: str | None = None, destinations_js: str | None = None) -> list[str]:
    """Where WIZARD_CONTROLS, the destination form's list, or the harnesses' WIZARD_STEPS no
    longer say what the page says."""
    wizard = WIZARD_JS.read_text() if wizard is None else wizard
    destinations_js = DESTINATIONS_JS.read_text() if destinations_js is None else destinations_js
    steps_module = CONSOLE_STEPS.read_text() if steps_module is None else steps_module
    rendered = step_renderers(wizard, select)
    bad: list[str] = []
    if sorted(rendered) != [1, 2, 3, 4, 5, 6]:
        bad.append("the page no longer renders six wizardPage(s, i, ...) steps: %s" % sorted(rendered))
    for pattern, step, witness in WIZARD_CONTROLS:
        if witness not in rendered.get(step, ""):
            where = [s for s, src in rendered.items() if witness in src]
            bad.append("%s is mapped to step %d, but the page renders %s in step(s) %s" % (
                pattern, step, witness, where))
    field_step = re.search(r"export const FIELD_STEP = Object\.freeze\(\{(.*?)\}\);", wizard, re.S)
    declared = dict((k, int(v)) for k, v in re.findall(r"(\w+):\s*(\d)", field_step.group(1))) \
        if field_step else {}
    if not declared:
        bad.append("the page's FIELD_STEP could not be read")
    for name, key in FIELD_NAMES.items():
        if key is None:
            continue
        mapped = step_of('[name="%s"]' % name)
        if declared.get(key) is None or mapped != declared[key] + 1:
            bad.append("the input named %s is mapped to step %s, the page's FIELD_STEP says %s" % (
                name, mapped, None if declared.get(key) is None else declared[key] + 1))
    titles = re.search(r"export const STEPS = Object\.freeze\(\[(.*?)\]\);", wizard, re.S)
    page_titles = re.findall(r'title: "([^"]+)"', titles.group(1)) if titles else []
    ours = re.search(r"export const WIZARD_STEPS = Object\.freeze\(\[(.*?)\]\);", steps_module, re.S)
    harness_titles = re.findall(r'"([^"]+)"', ours.group(1)) if ours else []
    if not page_titles or page_titles != harness_titles:
        bad.append("scripts/console-steps.mjs WIZARD_STEPS %s is not the page's STEPS %s" % (
            harness_titles, page_titles))
    form = re.search(r"export function renderDestinationForm\(.*?\n}\n", destinations_js, re.S)
    for witness in DESTINATION_WITNESSES:
        if form is None or witness not in form.group(0):
            bad.append("renderDestinationForm no longer renders %s" % witness)
    if form is not None and form.group(0).find("<details") > form.group(0).find('id=\\"destination-form\\"'):
        bad.append("the create-destination form is no longer inside its disclosure")
    for needle in ('id=\\"wizard-next\\"', 'id=\\"wizard-back\\"', 'id=\\"wizard-position\\"',
                   'data-wizard-step=\\"', 'Step " + String(shown + 1) +'):
        if needle not in wizard:
            bad.append("the page no longer renders %s, which the walk reads" % needle)
    return bad
