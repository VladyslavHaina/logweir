"""The pure half of the PLAT-20.1 journey runner: the data model, the verdict
arithmetic, redaction and the credential sweep.

Nothing in this module talks to a cluster, starts a process or reads the
clock, so every rule that decides whether a run is green is testable offline
(`test_core.py`) — including against planted-wrong inputs, which is the only
way to know the rule can fail.

The verdict vocabulary is deliberately small:

    PASS     every row the journey names was read back, and every one passed
    FAIL     a row failed, was missing, or its harness did not exit cleanly
    SKIPPED  the journey needs something the lab does not have yet
             (`requires`), and the reason is printed. NEVER a pass: it is
             counted separately and it can never make `ok` true on its own.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import re
import secrets
from typing import Any, Iterable, Mapping

SCHEMA = "logweir.dev/journeys/v1"
PASS, FAIL, SKIPPED = "PASS", "FAIL", "SKIPPED"

# What a row's assertion actually reads. The acceptance clause is "journeys
# verify archive data/evidence and durable resources, not only rendered text",
# so every row is classified by the strongest thing its ASSERTION line reads —
# not by what its name suggests — and `catalogue_violations` refuses a journey
# made only of text.
ARCHIVE = "archive-data"      # objects in MinIO, records in Kafka
EVIDENCE = "evidence"         # signed receipts/scorecards, verifier verdicts
RESOURCE = "durable-resource" # CR status/conditions, Jobs, ConfigMaps, UIDs
TEXT = "rendered-text"        # page text or log lines only
VERIFIES = (ARCHIVE, EVIDENCE, RESOURCE, TEXT)
STRONG = frozenset({ARCHIVE, EVIDENCE, RESOURCE})

# Gates a journey may declare. Each names the missing thing and what opens it,
# so a SKIPPED line tells the reader exactly what the next lab refresh owes.
REQUIREMENTS: dict[str, str] = {
    "lab-refresh-8": (
        "needs the evidence-fetch controller (claude/evidence-fetch, "
        "EVIDENCE-FETCH-JOB-UNBUILT) on the lab: without it no destination-backed "
        "scheduled run gets windowCovered, so the console offers no Restore for it"
    ),
    "PLAT-19.2": (
        "needs PLAT-19.2's ordinary/governed approval policy: no build routes a "
        "restore through two separate approvals yet"
    ),
}


@dataclasses.dataclass(frozen=True)
class Row:
    """One row of an existing (or native) suite that a journey depends on."""

    suite: str
    name: str
    verifies: tuple[str, ...]
    cite: str  # file:line of the ASSERTION that fails when the behaviour is absent
    note: str = ""


@dataclasses.dataclass(frozen=True)
class Journey:
    id: str
    title: str
    tests: tuple[str, ...]  # the tracker's named tests this journey carries
    rows: tuple[Row, ...]
    requires: str | None = None
    data: bool = False  # a backup/restore journey: must read archive data or evidence


@dataclasses.dataclass
class SuiteResult:
    """What one harness invocation left behind, reduced to row verdicts."""

    suite: str
    rc: int | None  # None: never started
    rows: dict[str, str]
    error: str = ""
    seconds: float = 0.0
    artifacts: str = ""


def catalogue_violations(journeys: Iterable[Journey]) -> list[str]:
    """Static rules over the journey catalogue, checked before anything runs."""
    problems: list[str] = []
    seen: set[str] = set()
    for j in journeys:
        if j.id in seen:
            problems.append(f"{j.id}: duplicate journey id")
        seen.add(j.id)
        if not j.rows:
            problems.append(f"{j.id}: a journey with no rows proves nothing")
        if not j.tests:
            problems.append(f"{j.id}: names none of the tracker's tests")
        if j.requires is not None and j.requires not in REQUIREMENTS:
            problems.append(f"{j.id}: unknown requirement {j.requires!r}")
        kinds = {v for r in j.rows for v in r.verifies}
        for r in j.rows:
            bad = [v for v in r.verifies if v not in VERIFIES]
            if bad or not r.verifies:
                problems.append(f"{j.id}/{r.name}: unknown verifies {bad or '[]'}")
            if not re.search(r":\d+", r.cite):
                problems.append(f"{j.id}/{r.name}: cite {r.cite!r} names no line")
        if not kinds & STRONG:
            problems.append(f"{j.id}: every row reads only rendered text")
        if RESOURCE not in kinds:
            problems.append(f"{j.id}: no row reads a durable resource back")
        if j.data and not kinds & {ARCHIVE, EVIDENCE}:
            problems.append(f"{j.id}: a data journey reads neither archive data nor evidence")
    return problems


def judge(journey: Journey, results: Mapping[str, SuiteResult], opened: set[str]) -> dict[str, Any]:
    """One journey's verdict. Strict by construction: anything not read back
    as PASS is a FAIL, and SKIPPED is only ever produced by a closed gate."""
    base: dict[str, Any] = {
        "id": journey.id,
        "title": journey.title,
        "tests": list(journey.tests),
        "requires": journey.requires,
        "verifies": sorted({v for r in journey.rows for v in r.verifies}),
    }
    if journey.requires is not None and journey.requires not in opened:
        return {
            **base,
            "verdict": SKIPPED,
            "reason": f"requires: {journey.requires} — {REQUIREMENTS[journey.requires]}",
            "rows": [
                {"suite": r.suite, "row": r.name, "verdict": "NOT-RUN", "cite": r.cite,
                 "verifies": list(r.verifies)}
                for r in journey.rows
            ],
        }
    rows: list[dict[str, Any]] = []
    reasons: list[str] = []
    for r in journey.rows:
        result = results.get(r.suite)
        entry = {"suite": r.suite, "row": r.name, "cite": r.cite, "verifies": list(r.verifies)}
        if result is None or result.rc is None:
            entry["verdict"] = "NOT-RUN"
            reasons.append(f"{r.suite} never ran")
        else:
            verdict = result.rows.get(r.name, "MISSING")
            entry["verdict"] = verdict
            if verdict != PASS:
                reasons.append(f"{r.suite}/{r.name} is {verdict}")
            if result.rc != 0:
                reasons.append(f"{r.suite} exited rc={result.rc}: {result.error[:300]}")
        rows.append(entry)
    if not rows:
        reasons.append("no rows")
    verdict = PASS if not reasons else FAIL
    return {**base, "verdict": verdict, "reason": "; ".join(dict.fromkeys(reasons)) or "every row PASS",
            "rows": rows}


def summarise(
    journeys: Iterable[Journey],
    results: Mapping[str, SuiteResult],
    opened: set[str],
    sweep: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """The whole run's verdict. `ok` needs: no FAIL, at least one PASS, a
    clean credential sweep and a sweep that proved it can fail."""
    judged = [judge(j, results, opened) for j in journeys]
    counts = {PASS: 0, FAIL: 0, SKIPPED: 0}
    for j in judged:
        counts[j["verdict"]] += 1
    sweep_ok = bool(
        sweep
        and sweep.get("selfTest", {}).get("killed") is True
        and not sweep.get("hits")
    )
    return {
        "schema": SCHEMA,
        "journeys": judged,
        "counts": counts,
        "suites": {
            k: {"rc": v.rc, "rows": v.rows, "error": v.error[:500], "seconds": v.seconds,
                "artifacts": v.artifacts}
            for k, v in sorted(results.items())
        },
        "credentialSweep": dict(sweep or {}),
        "ok": counts[FAIL] == 0 and counts[PASS] > 0 and sweep_ok,
    }


def summary_selftest() -> dict[str, Any]:
    """THE PLANTED-FAILURE TWIN, run live before a summary is trusted.

    A summariser that turns everything green is indistinguishable from a
    working one on a green run, so every run first feeds it a planted
    catalogue — one passing row, one failing, one missing, one harness that
    crashed, one closed gate — and refuses to continue unless each comes out
    exactly as it must."""
    row = lambda s, n: Row(s, n, (RESOURCE,), "planted:1")  # noqa: E731
    planted = [
        Journey("p-pass", "planted pass", ("t",), (row("s", "ok"),)),
        Journey("p-fail", "planted fail", ("t",), (row("s", "ok"), row("s", "bad"))),
        Journey("p-missing", "planted missing", ("t",), (row("s", "absent"),)),
        Journey("p-crash", "planted crash", ("t",), (row("c", "ok"),)),
        Journey("p-gated", "planted gate", ("t",), (row("s", "ok"),), requires="PLAT-19.2"),
    ]
    results = {
        "s": SuiteResult("s", 0, {"ok": PASS, "bad": FAIL}),
        "c": SuiteResult("c", 1, {"ok": PASS}, error="planted crash"),
    }
    clean = {"selfTest": {"killed": True}, "hits": []}
    got = summarise(planted, results, set(), clean)
    verdicts = {j["id"]: j["verdict"] for j in got["journeys"]}
    want = {"p-pass": PASS, "p-fail": FAIL, "p-missing": FAIL, "p-crash": FAIL, "p-gated": SKIPPED}
    all_skipped = summarise([planted[-1]], results, set(), clean)
    dirty = summarise([planted[0]], results, set(), {"selfTest": {"killed": True}, "hits": [{}]})
    untested = summarise([planted[0]], results, set(), {"selfTest": {"killed": False}, "hits": []})
    failures = [
        k for k, ok in {
            "verdicts": verdicts == want,
            "a planted FAIL makes the run not ok": got["ok"] is False,
            "a run of only SKIPPED is not ok": all_skipped["ok"] is False,
            "a sweep hit makes the run not ok": dirty["ok"] is False,
            "an unproven sweep makes the run not ok": untested["ok"] is False,
            "the planted pass alone is ok": summarise([planted[0]], results, set(), clean)["ok"],
        }.items() if not ok
    ]
    if failures:
        raise RuntimeError(f"the summary logic cannot fail as it must: {failures}; got {verdicts}")
    return {"planted": want, "killed": True}


# ---------------------------------------------------------------------------
# Redaction and the credential sweep
# ---------------------------------------------------------------------------


class Needles:
    """Exact values that must never reach an artifact.

    Two sources: values this run MINTED, and values it READ — the shared lab's
    Secret values and the approver private key, loaded into memory only so the
    sweep can search for them literally. A pattern is a guess about what a
    secret looks like; membership is exact. Neither set is ever written."""

    def __init__(self) -> None:
        self._values: set[str] = set()

    def add(self, value: str) -> None:
        value = value.strip()
        # Very short values ("admin", "user") would match ordinary prose and
        # make the sweep fail on nothing; they are covered by the key=value
        # patterns instead. Eight characters is the same floor the patterns use.
        if len(value) >= 8:
            self._values.add(value)
            # A PEM is also searched for its body lines, so a key re-wrapped
            # at another width or with its armour stripped is still found.
            for line in value.splitlines():
                line = line.strip()
                if len(line) >= 40 and not line.startswith("-----"):
                    self._values.add(line)

    def mint(self, nbytes: int = 24) -> str:
        value = secrets.token_urlsafe(nbytes)
        self._values.add(value)
        return value

    def discard(self, value: str) -> None:
        self._values.discard(value)

    def __iter__(self):
        return iter(sorted(self._values, key=len, reverse=True))

    def __len__(self) -> int:
        return len(self._values)


_KV = re.compile(
    r"(?i)(password|passwd|secret[-_]?access[-_]?key|access[-_]key[-_]?id|token|"
    r"sasl[-_]password|client[-_]secret)([\"'= :]+)(?![\[<])([^\s\"',}]{8,})"
)
_PEM = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.DOTALL
)


def redact(text: str, needles: Needles) -> str:
    """Anything credential-shaped, removed before it reaches an artifact."""
    for value in needles:
        text = text.replace(value, "[REDACTED]")
    text = _PEM.sub("[REDACTED PRIVATE KEY]", text)
    return _KV.sub(lambda m: f"{m.group(1)}{m.group(2)}[REDACTED]", text)


CREDENTIAL_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)\baws_secret_access_key\b\s*[\"']?\s*[:=]\s*[\"']?(?![\[<])[A-Za-z0-9+/=_-]{8,}"),
    re.compile(r"(?i)\bpassword\b\s*[\"']?\s*[:=]\s*[\"']?(?![\[<])[A-Za-z0-9+/=_-]{8,}"),
    # A positional secret (`mc admin user add <alias> <user> <secret>`) is
    # where no key=value pattern can see it — d3's first sweep missed exactly
    # that shape (e2e/k8s/d3/d3_live.py, CREDENTIAL_PATTERNS).
    re.compile(r"(?i)\b(?:admin\s+user\s+add|user\s+add)\s+\S+\s+\S+\s+(?!\[REDACTED)(\S{8,})"),
    # A Kubernetes Secret dumped whole, whatever its keys are called. Either
    # order: `kubectl -o json` and `Lab.write(sort_keys=True)` put `data`
    # BEFORE `kind`, which the kind-first spelling alone never matched
    # (plat20-1.review.md L-2); a hand-built object may put `kind` first.
    re.compile(
        r"\"data\"\s*:\s*\{\s*\"[^\"]+\"\s*:\s*\"[A-Za-z0-9+/=]{12,}\"[^}]*\}[^{}]*\"kind\"\s*:\s*\"Secret\""
        r"|\"kind\"\s*:\s*\"Secret\"[^}]*\"data\"\s*:\s*\{\s*\"[^\"]+\"\s*:\s*\"[A-Za-z0-9+/=]{12,}"),
]


def sweep(root: pathlib.Path, needles: Needles) -> dict[str, Any]:
    """Every file under `root`, read back and searched for anything
    credential-shaped and for every needle by exact match. Binary files
    (screenshots, traces) are searched as bytes decoded with replacement, so a
    value inside a trace zip's uncompressed member is still found."""
    hits: list[dict[str, Any]] = []
    scanned = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        scanned += 1
        text = path.read_bytes().decode("utf-8", errors="replace")
        rel = str(path.relative_to(root))
        for pattern in CREDENTIAL_PATTERNS:
            for match in pattern.finditer(text):
                hits.append({"file": rel, "pattern": pattern.pattern[:60], "at": match.start()})
        for needle in needles:
            at = text.find(needle)
            if at >= 0:
                # The needle itself is never reproduced — only where it was.
                hits.append({"file": rel, "pattern": "<exact value>", "at": at})
    return {"filesScanned": scanned, "hits": hits, "needles": len(needles),
            "patterns": len(CREDENTIAL_PATTERNS)}


SELFTEST_FILE = "sweep-selftest.tmp"


def selftest_lines() -> list[tuple[int, str]]:
    """One planted line per CREDENTIAL_PATTERNS entry (by index), both orders
    of the Secret dump, assembled at run time so no source line carries a
    credential-shaped literal."""
    import base64

    fake = base64.b64encode(secrets.token_bytes(18)).decode()
    return [
        (0, "-----BEGIN " + "PRIVATE KEY-----"),
        (1, "aws_secret_access_key" + " = " + "S" * 24),
        (2, "pass" + "word=" + "Q" * 16),
        (3, "kubectl exec pod -- mc admin user add adm probeuser " + "P" * 20),
        # kubectl's own order (data before kind), under a key that is not `password`
        (4, json.dumps({"apiVersion": "v1", "data": {"secret-access-key": fake}, "kind": "Secret"},
                       sort_keys=True)),
        (4, json.dumps({"kind": "Secret", "data": {"token": fake}})),
    ]


def sweep_selftest(root: pathlib.Path, needles: Needles) -> dict[str, Any]:
    """The sweep's mutant, planted and killed in one step: a file carrying a
    minted exact value and one line for EVERY credential pattern (the Secret
    dump in both key orders) is written, the sweep must report each line by
    its own pattern and the exact value, and the file is removed either way."""
    probe = needles.mint()
    planted = selftest_lines()
    lines = [probe] + [text for _, text in planted]
    path = root / SELFTEST_FILE
    path.write_text("\n".join(lines) + "\n")
    starts, at = [], 0
    for line in lines:
        starts.append(at)
        at += len(line) + 1
    try:
        hits = [h for h in sweep(root, needles)["hits"] if h["file"] == SELFTEST_FILE]
    finally:
        path.unlink(missing_ok=True)
        needles.discard(probe)
    names = [p.pattern[:60] for p in CREDENTIAL_PATTERNS]

    def line_of(offset: int) -> int:
        return max(i for i, s in enumerate(starts) if s <= offset)

    found = {(h["pattern"], line_of(h["at"])) for h in hits}
    missed = [] if ("<exact value>", 0) in found else ["<exact value>"]
    missed += [f"pattern {index} on line {n}" for n, (index, _) in enumerate(planted, start=1)
               if (names[index], n) not in found]
    if missed:
        raise RuntimeError(f"the credential sweep cannot fail: the planted probe missed {missed}")
    return {"planted": len(lines), "found": len(found), "patterns": len(CREDENTIAL_PATTERNS), "killed": True}
