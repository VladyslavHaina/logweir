"""The journey catalogue: which existing harness rows prove which journey,
how each harness is invoked in this run's own namespaces, and how its result
document is reduced to row verdicts.

A composed row is cited by the file:line of its ASSERTION — the line that
fails when the behaviour is absent — not by where its name is printed, and is
classified by what that assertion reads (`core.VERIFIES`). `test_catalogue.py`
re-reads every cited file and fails when a cite points past the end of it, or
when a composed row's name no longer appears in the harness that should record
it, so the catalogue cannot silently drift from the suites it composes.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Any, Callable

from core import ARCHIVE, EVIDENCE, PASS, RESOURCE, TEXT, Journey, Row

# ---------------------------------------------------------------------------
# Suites
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Ctx:
    """Everything a suite needs to be pointed at this run and nowhere else."""

    stamp: str
    run: pathlib.Path      # the artifact tree (swept)
    private: pathlib.Path  # 0700, outside the artifact tree, removed at the end
    root: pathlib.Path
    python: str
    api_bin: str
    cli: str
    node_path: str
    owner: str

    def out(self, suite: str) -> pathlib.Path:
        return self.run / "suites" / suite

    @property
    def prefix(self) -> str:
        """This run's own archive prefix in the shared bucket."""
        return f"lw-plat20-{self.stamp}"


@dataclasses.dataclass(frozen=True)
class Suite:
    id: str
    script: str
    runner: str                   # "python" | "node" | "native" | "console"
    phases: tuple[str, ...]       # one process per phase, in order
    cleanup: tuple[str, ...]      # run in `finally`, always
    infra: frozenset[str]         # phases whose nonzero exit makes the suite rc nonzero
    env: Callable[[Ctx], dict[str, str]]
    adapter: Callable[[Ctx, dict[str, int]], dict[str, str]]
    namespaces: Callable[[Ctx], list[str]]
    timeout: int = 1800           # per phase
    accept_rcs: frozenset[int] = frozenset({0})
    why_rcs: str = ""


def _json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text()) if path.is_file() else {}


def _newest(root: pathlib.Path, pattern: str) -> pathlib.Path | None:
    found = sorted(root.rglob(pattern), key=lambda p: p.stat().st_mtime)
    return found[-1] if found else None


def cases_adapter(markers: dict[str, str]) -> Callable[[Ctx, dict[str, int]], dict[str, str]]:
    """plat06/plat07: a case is PASS only if its PROCESS exited 0 — each case
    raises on its first failed assertion — AND the key it writes last is in
    `state.json`, so a phase that exited 0 without running is not a pass."""

    def adapt(ctx: Ctx, rcs: dict[str, int], suite: str) -> dict[str, str]:
        cases = _json(ctx.out(suite) / "state.json").get("cases") or {}
        return {phase: PASS if rcs.get(phase) == 0 and marker in cases else "FAIL"
                for phase, marker in markers.items() if phase in rcs}

    return adapt


def _plat06_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    return cases_adapter({"case-a": "case-a-idempotence", "case-c": "case-c-detail",
                          "case-e": "case-e-detail", "case-g": "case-g-detail"})(ctx, rcs, "plat06")


def _plat07_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    return cases_adapter({"case-a": "case-a-idempotence", "case-e": "case-e",
                          "case-f": "case-f"})(ctx, rcs, "plat07")


def _d1_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    scenarios = _json(ctx.out("d1") / "results.json").get("scenarios") or {}
    return {k: PASS if (v or {}).get("status") == "pass" else str((v or {}).get("status")).upper()
            for k, v in scenarios.items()}


def _d2_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    scenarios = _json(ctx.out("d2") / "results.json").get("scenarios") or {}
    return {k: PASS if (v or {}).get("outcome") == "pass" else str((v or {}).get("outcome")).upper()
            for k, v in scenarios.items()}


def _d3_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    scenarios = _json(ctx.out("d3") / "state.json").get("scenarios") or {}
    return {k: (v or {}).get("verdict", "MISSING") for k, v in scenarios.items()}


def ui_adapter(pattern: str) -> Callable[[Ctx, dict[str, int], str], dict[str, str]]:
    """The console scripts push a journey onto `journeys[]` only once every
    `check` in it has held, so presence is the verdict; one that threw is
    absent (MISSING -> FAIL). A `blocked[]` entry is recorded as BLOCKED —
    never a pass."""

    def adapt(ctx: Ctx, rcs: dict[str, int], suite: str) -> dict[str, str]:
        doc = _json(_newest(ctx.out(suite), pattern) or pathlib.Path("/nonexistent"))
        rows = {j["journey"]: PASS for j in doc.get("journeys") or [] if j.get("journey")}
        for b in doc.get("blocked") or []:
            rows.setdefault(b.get("journey", "?"), "BLOCKED")
        return rows

    return adapt


def _native_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    return (_json(ctx.out("native") / "result.json").get("rows")) or {}


def _console_adapter(ctx: Ctx, rcs: dict[str, int]) -> dict[str, str]:
    return (_json(ctx.out("console") / "result.json").get("rows")) or {}


def _ui_env(ctx: Ctx, suite: str, prefix: str) -> dict[str, str]:
    return {"UI_E2E_OWNER": ctx.owner, "UI_E2E_PREFIX": prefix, "UI_E2E_NAMESPACE": f"{prefix}{ctx.stamp}",
            "UI_E2E_ARTIFACTS": str(ctx.out(suite)), "UI_E2E_API_BIN": ctx.api_bin, "NODE_PATH": ctx.node_path}


SUITES: dict[str, Suite] = {s.id: s for s in [
    Suite(
        "plat06", "scripts/test-plat06-live.py", "python",
        # case-d is left out ON PURPOSE: it deletes the SHARED controller pod.
        ("setup", "case-a", "case-c", "case-e", "case-g", "report"), ("cleanup",),
        frozenset({"setup", "report", "cleanup"}),
        lambda c: {"LOGWEIR_PLAT06_STAMP": f"p20{c.stamp}", "LOGWEIR_PLAT06_OUT": str(c.out("plat06")),
                   "LOGWEIR_PLAT06_ARCHIVE": f"s3://kafka-backups/{c.prefix}/plat06"},
        _plat06_adapter, lambda c: [f"lw-plat06-p20{c.stamp}"]),
    Suite(
        "plat07", "scripts/test-plat07-live.py", "python",
        # No lab-baseline/lab-swap/lab-restore: those repoint the SHARED
        # controller. Without them the cases measure the controller the lab runs.
        ("setup", "case-a", "case-e", "case-f", "report"), ("cleanup",),
        frozenset({"setup", "report", "cleanup"}),
        lambda c: {"LOGWEIR_PLAT07_STAMP": f"p20{c.stamp}", "LOGWEIR_PLAT07_OUT": str(c.out("plat07")),
                   "LOGWEIR_PLAT07_KEYS": str(c.private / "plat07-keys")},
        _plat07_adapter, lambda c: [f"lw-plat07live-p20{c.stamp}"]),
    Suite(
        "d1", "scripts/live/d1/run.py", "python",
        ("setup", "L-09-1", "L-09-2", "report"), ("cleanup",),
        frozenset({"setup", "report", "cleanup"}),
        lambda c: {"LOGWEIR_D1_STAMP": c.stamp, "LOGWEIR_D1_OWNER": f"{c.owner}-d1",
                   "LOGWEIR_D1_OUT": str(c.out("d1"))},
        _d1_adapter, lambda c: [f"{c.owner}-d1-{c.stamp}"]),
    Suite(
        "d2", "e2e/k8s/d2/d2_live.py", "python",
        ("setup", "s1", "s11", "report"), ("cleanup",),
        frozenset({"setup", "report", "cleanup"}),
        lambda c: {"D2_OWNER": f"{c.owner}-d2", "D2_NAMESPACE_PREFIX": "lw-plat20-d2-",
                   "D2W14_STAMP": c.stamp, "D2W14_OUT": str(c.private / "d2"),
                   "D2W14_ART": str(c.out("d2"))},
        _d2_adapter, lambda c: [f"lw-plat20-d2-{c.stamp}", f"lw-plat20-d2-{c.stamp}-b"]),
    Suite(
        "d3", "e2e/k8s/d3/d3_live.py", "python",
        # `control` is d3's own negative control and must run before cleanup;
        # `report` fails the phase on any credential-sweep hit.
        ("setup", "catalog", "control", "report"), ("cleanup",),
        frozenset({"setup", "control", "report", "cleanup"}),
        lambda c: {"LOGWEIR_D3_STAMP": c.stamp, "LOGWEIR_D3_OWNER": f"{c.owner}-d3",
                   "LOGWEIR_D3_OUT": str(c.out("d3")), "LOGWEIR_BIN": c.cli},
        _d3_adapter, lambda c: [f"{c.owner}-d3-{c.stamp}"]),
    Suite(
        "plat12-13", "scripts/plat12-13-ui-e2e.mjs", "node", ("run",), (), frozenset({"run"}),
        lambda c: _ui_env(c, "plat12-13", "lw-plat20-p1213-"),
        lambda c, r: ui_adapter("live-result-*.json")(c, r, "plat12-13"),
        lambda c: [f"lw-plat20-p1213-{c.stamp}"], timeout=900),
    Suite(
        "plat11-2", "scripts/plat11-2-ui-e2e.mjs", "node", ("run",), (), frozenset({"run"}),
        lambda c: _ui_env(c, "plat11-2", "lw-plat20-p112-"),
        lambda c, r: ui_adapter("result.json")(c, r, "plat11-2"),
        lambda c: [f"lw-plat20-p112-{c.stamp}"], timeout=900),
    Suite(
        "plat10", "scripts/plat10-ui-e2e.mjs", "node", ("run",), (), frozenset({"run"}),
        lambda c: _ui_env(c, "plat10", "lw-plat20-p10-"),
        lambda c, r: ui_adapter("live.json")(c, r, "plat10"),
        lambda c: [f"lw-plat20-p10-{c.stamp}"], timeout=900,
        accept_rcs=frozenset({0, 3}),
        why_rcs="plat10 exits 3 exactly when a row is recorded in `blocked[]` "
                "(scripts/plat10-ui-e2e.mjs:1882-1886); those rows are adapted as BLOCKED, which "
                "never passes, and only the lab-refresh-8 journey names them"),
    Suite("native", "e2e/journeys/native.py", "native", ("setup", "journeys"), ("cleanup",),
          frozenset({"setup", "cleanup"}), lambda c: {}, _native_adapter,
          lambda c: [f"lw-plat20-n-{c.stamp}"]),
    Suite("console", "e2e/journeys/console.mjs", "console", ("run",), (), frozenset({"run"}),
          lambda c: {}, _console_adapter,
          lambda c: [f"lw-plat20-ca-{c.stamp}", f"lw-plat20-cb-{c.stamp}"], timeout=1500),
]}


# ---------------------------------------------------------------------------
# Journeys
# ---------------------------------------------------------------------------

R = Row
P06, P07 = "scripts/test-plat06-live.py", "scripts/test-plat07-live.py"
D1, D2, D3 = "scripts/live/d1/run.py", "e2e/k8s/d2/d2_live.py", "e2e/k8s/d3/d3_live.py"
U1213, U112, U10 = "scripts/plat12-13-ui-e2e.mjs", "scripts/plat11-2-ui-e2e.mjs", "scripts/plat10-ui-e2e.mjs"
NAT, CON = "e2e/journeys/native.py", "e2e/journeys/console.mjs"

P10_RESTORE_ROWS = (
    R("plat10", "PLAT-10.2 each real point carries its own Restore, from the controller's own window",
      (RESOURCE, EVIDENCE), f"{U10}:1278"),
    R("plat10", "PLAT-10.2 navigation to an older real backup opens the wizard bound to it, not the newest",
      (RESOURCE,), f"{U10}:1357"),
    R("plat10", "DONE EVIDENCE create -> backup -> detail -> restore: the wizard submits with the "
      "schedule's destination and point carried; admission BLOCKED ON HOST KEY MATERIAL",
      (RESOURCE,), f"{U10}:1492"),
)

JOURNEYS: tuple[Journey, ...] = (
    Journey(
        "registration-and-discovery", "a connection and a destination are registered and a discovery "
        "completes against the real broker", ("journey: registration and discovery",),
        (R("console", "console-registers-a-connection-the-controller-reaches", (RESOURCE,), f"{CON}:205"),
         R("console", "console-registers-a-destination-the-controller-validates", (RESOURCE,), f"{CON}:231"),
         R("console", "console-discovery-completes-and-lists-the-run-topic", (RESOURCE, ARCHIVE), f"{CON}:263"),
         R("d1", "L-09-1", (RESOURCE,), f"{D1}:1279"),
         R("d2", "S1", (RESOURCE, EVIDENCE, ARCHIVE), f"{D2}:1827",
           "two destinations: each run's manifest lands in its own bucket and not the other")),
        data=True),
    Journey(
        "manual-backup", "a manual Backup runs once, freezes its inputs and is verified from the archive",
        ("journey: manual backup",),
        (R("plat06", "case-a", (RESOURCE, EVIDENCE), f"{P06}:662",
           "receipt read from MinIO, digest equals status.evidence, independent verifier, Valid"),
         R("plat07", "case-a", (RESOURCE, EVIDENCE), f"{P07}:1434"),
         R("d2", "S1", (RESOURCE, EVIDENCE, ARCHIVE), f"{D2}:1827")),
        data=True),
    Journey(
        "scheduled-backup", "a schedule fires once per slot under the shipped RBAC and its run is verified",
        ("journey: scheduled backup",),
        (R("plat06", "case-c", (RESOURCE, EVIDENCE), f"{P06}:908",
           "reservation seen and cleared, one Backup per slot, trigger schedule, receipt verified"),
         R("plat10", "PLAT-10.1 selected-topic creation through the guided form, and the first-run redirect",
           (RESOURCE,), f"{U10}:850"),
         R("plat10", "PLAT-10.2 verified runs: the lab controller's catalog says Available/Verified and the "
           "detail renders exactly that", (ARCHIVE, EVIDENCE, RESOURCE), f"{U10}:1227")),
        data=True),
    Journey(
        "scram-rotation", "a rotated SCRAM credential needs no edit, and the old value is refused",
        ("SCRAM rotation",),
        (R("plat07", "case-e", (RESOURCE, EVIDENCE), f"{P07}:1801",
           "frozen plan unchanged across the rotation; fresh run verified; stale secret fails on SASL auth"),
         R("plat07", "case-f", (RESOURCE, TEXT), f"{P07}:1979",
           "no password, key or Secret value in any CR, ConfigMap, Job, pod log or event")),
        data=True),
    Journey(
        "new-topic-dynamic-policy", "a topic created after run 1 enters run 2; run 1's frozen snapshot "
        "and receipt do not move", ("new topic in dynamic policy",),
        (R("d1", "L-09-1", (RESOURCE,), f"{D1}:1279"),
         R("d1", "L-09-2", (RESOURCE, EVIDENCE), f"{D1}:1393",
           "run 2 froze [t1,t2,t3]; run 1's plan sha/rv unchanged; run 1's receipt from MinIO still [t1,t2]")),
        data=True),
    Journey(
        "overlap", "under Forbid a second slot is blocked while a run is active, and the active run "
        "survives its Job being deleted", ("overlap",),
        (R("plat06", "case-e", (RESOURCE, EVIDENCE), f"{P06}:1109",
           "Ready=ConcurrencyBlocked, exactly one child while blocked, re-created Job identical"),)),
    Journey(
        "two-approvals", "a governed restore needs two separate approvals and refuses one",
        ("two approvals",),
        (R("native", "two-approvals-governed-restore", (RESOURCE,),
           "docs/to-do/platform-improvements.md:3674"),),
        requires="PLAT-19.2"),
    Journey(
        "source-offline", "a source nobody answers for fails the run by name and writes nothing; a "
        "discovery against it fails by name", ("source offline",),
        (R("native", "backup-from-an-offline-source-fails-and-writes-nothing", (RESOURCE, ARCHIVE), f"{NAT}:294"),
         R("d2", "S11", (RESOURCE,), f"{D2}:3058")),
        data=True),
    Journey(
        "cr-loss", "the catalog is rebuilt from the archive after every Backup CR is deleted, and a run "
        "survives its Job being deleted", ("CR loss",),
        (R("d3", "catalog-records-written", (ARCHIVE, EVIDENCE), f"{D3}:903"),
         R("d3", "catalog-reconstruction-after-cr-loss", (RESOURCE, EVIDENCE), f"{D3}:943"),
         R("plat06", "case-e", (RESOURCE, EVIDENCE), f"{P06}:1109")),
        data=True),
    Journey(
        "stale-namespace-request", "a slow answer for namespace A never renders over B, a form left in A "
        "writes nothing, and a submit after the switch lands in B only", ("stale namespace request",),
        (R("console", "console-slow-a-response-never-renders-over-b", (RESOURCE, TEXT), f"{CON}:314"),
         R("console", "console-left-form-in-a-writes-nothing", (RESOURCE,), f"{CON}:350"),
         R("console", "console-submit-after-switch-lands-in-b-only", (RESOURCE,), f"{CON}:356"))),
    Journey(
        "duplicate-submit", "a double click, a lost response, a resubmitted restore, a duplicate create "
        "and a replayed API create each leave exactly one durable object", ("duplicate submit",),
        (R("plat12-13", "double click creates exactly one object", (RESOURCE,), f"{U1213}:448"),
         R("plat12-13", "lost response, retried, resolves to the same object", (RESOURCE,), f"{U1213}:502"),
         R("plat12-13", "restore submission routes to Awaiting approval, and a resubmission creates nothing",
           (RESOURCE,), f"{U1213}:631"),
         R("plat06", "case-g", (RESOURCE, EVIDENCE), f"{P06}:1274"),
         R("native", "api-restore-replay-is-one-object", (RESOURCE,), f"{NAT}:376"))),
    Journey(
        "old-point-selection", "an older point chosen in the console and through the API is the point the "
        "durable Restore names, and the point that is restored", ("old-point selection",),
        (R("plat12-13", "an older point stays selected when a newer Backup completes mid-wizard",
           (TEXT,), f"{U1213}:1013"),
         R("plat11-2", "the wizard submits, and the created Restore is the preview byte for byte",
           (RESOURCE,), f"{U112}:972"),
         R("native", "restore-cr-carries-the-selected-older-point", (RESOURCE,), f"{NAT}:386"),
         R("native", "older-point-restore-restores-exactly-its-records", (ARCHIVE, RESOURCE), f"{NAT}:437")),
        data=True),
    Journey(
        "selected-point-restore-and-progress", "a selected point restores to completion with verified "
        "evidence, and its progress is durable on the object and projected by the API",
        ("journey: selected-point restore and durable progress",),
        (R("native", "older-point-restore-restores-exactly-its-records", (ARCHIVE, RESOURCE), f"{NAT}:437"),
         R("native", "restore-evidence-verifies-with-two-verifiers", (EVIDENCE,), f"{NAT}:500"),
         R("native", "restore-progress-is-durable-and-projected", (RESOURCE,), f"{NAT}:457")),
        data=True),
    Journey(
        "scheduled-backup-restore", "a SCHEDULED run's point is offered for restore and the wizard opens "
        "bound to it", ("journey: scheduled backup -> restore",),
        P10_RESTORE_ROWS, requires="lab-refresh-8"),
)


def suites_for(journeys: tuple[Journey, ...] | list[Journey], opened: set[str]) -> list[str]:
    """The suites the selected, un-gated journeys read, in execution order.
    `native` always runs first and cleans up last: it owns the run's utility
    namespace (the `mc` pod that removes this run's archive prefix, and the
    Kafka client that created the topic the console discovery must find)."""
    wanted = {r.suite for j in journeys if j.requires is None or j.requires in opened for r in j.rows}
    order = ["native", "console", "plat12-13", "plat11-2", "plat10", "plat06", "plat07", "d1", "d2", "d3"]
    if wanted:
        wanted.add("native")
    if "console" in wanted:
        wanted.add("native")
    return [s for s in order if s in wanted]
