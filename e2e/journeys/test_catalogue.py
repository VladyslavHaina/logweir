"""Offline tests over the journey catalogue and the suite adapters.

The catalogue composes rows of harnesses other workers keep changing, so
these tests re-read the composed files: a cite past the end of its file, or a
row name the harness no longer records, fails here before a live run would
misreport it.
"""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import core  # noqa: E402
import native  # noqa: E402
import suites  # noqa: E402
from core import FAIL, PASS, SKIPPED  # noqa: E402

ROOT = HERE.parents[1]
NAMED_TESTS = {"SCRAM rotation", "new topic in dynamic policy", "overlap", "two approvals",
               "source offline", "CR loss", "stale namespace request", "duplicate submit",
               "old-point selection"}
JOURNEY_SCOPE = {"journey: registration and discovery", "journey: manual backup",
                 "journey: scheduled backup",
                 "journey: selected-point restore and durable progress"}


def test_the_catalogue_obeys_its_own_rules():
    assert core.catalogue_violations(suites.JOURNEYS) == []


def test_every_named_tracker_test_and_journey_has_a_journey():
    carried = {t for j in suites.JOURNEYS for t in j.tests}
    assert NAMED_TESTS <= carried, NAMED_TESTS - carried
    assert JOURNEY_SCOPE <= carried, JOURNEY_SCOPE - carried


def test_exactly_the_two_briefed_gates_exist():
    gated = {j.id: j.requires for j in suites.JOURNEYS if j.requires}
    assert gated == {"two-approvals": "PLAT-19.2", "scheduled-backup-restore": "lab-refresh-8"}


def test_every_suite_a_row_names_exists_and_its_script_exists():
    for j in suites.JOURNEYS:
        for r in j.rows:
            assert r.suite in suites.SUITES, (j.id, r.suite)
    for s in suites.SUITES.values():
        assert (ROOT / s.script).is_file(), s.script


@pytest.mark.parametrize("journey", suites.JOURNEYS, ids=lambda j: j.id)
def test_every_cite_is_inside_its_file_and_the_file_names_the_row(journey):
    for r in journey.rows:
        path, _, line = r.cite.rpartition(":")
        lines = (ROOT / path).read_text().splitlines()
        assert 1 <= int(line) <= len(lines), r.cite
        if path.startswith("docs/"):
            continue  # a gated journey's cite names the tracker task that owes it
        text = "\n".join(lines)
        # plat06/plat07 rows are phase names; everything else is the row's own words.
        assert r.name in text, f"{r.cite}: {r.name!r} is no longer in {path}"


@pytest.mark.parametrize("path,rows", [("e2e/journeys/native.py", native.ROWS),
                                       ("e2e/journeys/console.mjs", None)])
def test_native_cites_point_at_the_call_that_records_the_row(path, rows):
    lines = (ROOT / path).read_text().splitlines()
    for j in suites.JOURNEYS:
        for r in j.rows:
            if not r.cite.startswith(path):
                continue
            at = int(r.cite.rpartition(":")[2])
            window = "\n".join(lines[at - 1:at + 2])
            assert r.name in window, f"{r.cite} does not record {r.name!r}: {window!r}"
            if rows is not None:
                assert tuple(r.verifies) == tuple(rows[r.name]), (r.name, r.verifies, rows[r.name])


def test_every_native_row_is_named_by_some_journey():
    named = {r.name for j in suites.JOURNEYS for r in j.rows if r.suite == "native"}
    assert set(native.ROWS) <= named


def test_gated_journeys_pull_no_suite_until_opened():
    j = [x for x in suites.JOURNEYS if x.id == "scheduled-backup-restore"]
    assert suites.suites_for(j, set()) == []
    assert suites.suites_for(j, {"lab-refresh-8"}) == ["native", "plat10"]


def test_native_runs_first_whenever_anything_runs():
    order = suites.suites_for(list(suites.JOURNEYS), set())
    assert order[0] == "native" and "plat10" in order and "console" in order


def test_no_suite_runs_a_phase_that_touches_the_shared_release():
    # plat06 case-d deletes the shared controller pod; plat07's lab-* phases
    # repoint the shared controller; k8s-scram is the lab's own installer.
    forbidden = {"plat06": {"case-d"}, "plat07": {"lab-baseline", "lab-swap", "lab-restore"}}
    for sid, phases in forbidden.items():
        assert not phases & set(suites.SUITES[sid].phases + suites.SUITES[sid].cleanup)
    assert not any("test-k8s-scram" in s.script for s in suites.SUITES.values())


# --- adapters: each shape, with its planted-failure twin --------------------


def ctx(tmp_path):
    return suites.Ctx(stamp="20260922t0000z", run=tmp_path, private=tmp_path / "p", root=ROOT,
                      python="python3", api_bin="api", cli="cli", node_path="", owner="plat20-1")


def write(tmp_path, suite, name, doc):
    d = tmp_path / "suites" / suite
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(doc))


def test_cases_adapter_needs_both_a_clean_exit_and_the_final_marker(tmp_path):
    c = ctx(tmp_path)
    write(tmp_path, "plat06", "state.json", {"cases": {"case-a-idempotence": {}, "case-e-detail": {}}})
    rows = suites.SUITES["plat06"].adapter(c, {"case-a": 0, "case-c": 0, "case-e": 1})
    assert rows == {"case-a": PASS, "case-c": FAIL, "case-e": FAIL}


def test_d1_adapter_passes_only_pass(tmp_path):
    write(tmp_path, "d1", "results.json", {"scenarios": {"L-09-1": {"status": "pass"},
                                                         "L-09-2": {"status": "partial"},
                                                         "L-04-1": {"status": "not-run"}}})
    rows = suites.SUITES["d1"].adapter(ctx(tmp_path), {})
    assert rows == {"L-09-1": PASS, "L-09-2": "PARTIAL", "L-04-1": "NOT-RUN"}


def test_d2_adapter_passes_only_pass(tmp_path):
    write(tmp_path, "d2", "results.json", {"scenarios": {"S1": {"outcome": "pass"}, "S11": {"outcome": "notRun"}}})
    assert suites.SUITES["d2"].adapter(ctx(tmp_path), {}) == {"S1": PASS, "S11": "NOTRUN"}


def test_d3_adapter_keeps_the_harness_verdict(tmp_path):
    write(tmp_path, "d3", "state.json", {"scenarios": {"a": {"verdict": "PASS"}, "b": {"verdict": "NOT-REACHED"}}})
    assert suites.SUITES["d3"].adapter(ctx(tmp_path), {}) == {"a": PASS, "b": "NOT-REACHED"}


def test_ui_adapter_blocked_is_never_a_pass(tmp_path):
    write(tmp_path, "plat10", "live.json", {"journeys": [{"journey": "ok"}], "blocked": [{"journey": "held"}]})
    rows = suites.SUITES["plat10"].adapter(ctx(tmp_path), {"run": 3})
    assert rows == {"ok": PASS, "held": "BLOCKED"}
    j = core.Journey("j", "t", ("x",), (core.Row("plat10", "held", (core.RESOURCE,), "f:1"),))
    got = core.judge(j, {"plat10": core.SuiteResult("plat10", 0, rows)}, {"x"})
    assert got["verdict"] == FAIL


def test_ui_adapter_a_missing_result_document_is_no_rows(tmp_path):
    assert suites.SUITES["plat12-13"].adapter(ctx(tmp_path), {"run": 1}) == {}


def test_plat10_is_the_only_suite_that_accepts_a_nonzero_exit():
    lenient = {s.id: s.accept_rcs for s in suites.SUITES.values() if s.accept_rcs != frozenset({0})}
    assert lenient == {"plat10": frozenset({0, 3})}
    assert suites.SUITES["plat10"].why_rcs


def test_a_whole_catalogue_run_with_every_row_passing_is_ok_and_gates_stay_skipped():
    results = {}
    for j in suites.JOURNEYS:
        for r in j.rows:
            results.setdefault(r.suite, core.SuiteResult(r.suite, 0, {})).rows[r.name] = PASS
    got = core.summarise(suites.JOURNEYS, results, set(), {"selfTest": {"killed": True}, "hits": []})
    assert got["ok"] is True
    assert {j["id"] for j in got["journeys"] if j["verdict"] == SKIPPED} == {"two-approvals",
                                                                             "scheduled-backup-restore"}


def test_its_twin_one_composed_row_failing_fails_its_journeys_and_the_run():
    results = {}
    for j in suites.JOURNEYS:
        for r in j.rows:
            results.setdefault(r.suite, core.SuiteResult(r.suite, 0, {})).rows[r.name] = PASS
    results["plat06"].rows["case-e"] = FAIL
    got = core.summarise(suites.JOURNEYS, results, set(), {"selfTest": {"killed": True}, "hits": []})
    failed = {j["id"] for j in got["journeys"] if j["verdict"] == FAIL}
    assert failed == {"overlap", "cr-loss"} and got["ok"] is False
