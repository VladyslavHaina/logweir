"""Offline tests for the journey runner's verdict arithmetic, catalogue rules,
redaction and credential sweep. Each rule is driven with a planted-wrong input
as well as a right one: a rule that cannot be made to fail is not a rule.

    /tmp/logweir-roadmap-run/venv/bin/python3 -m pytest e2e/journeys -q
"""

from __future__ import annotations

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import core  # noqa: E402
from core import (  # noqa: E402
    ARCHIVE, EVIDENCE, FAIL, PASS, RESOURCE, SKIPPED, TEXT, Journey, Needles, Row, SuiteResult,
)


def row(suite="s", name="ok", verifies=(RESOURCE,)):
    return Row(suite, name, tuple(verifies), "file.py:1")


def clean_sweep():
    return {"selfTest": {"killed": True}, "hits": []}


# --- judge / summarise ------------------------------------------------------


def test_every_row_pass_is_a_pass():
    j = Journey("j", "t", ("x",), (row(), row(name="two")))
    got = core.judge(j, {"s": SuiteResult("s", 0, {"ok": PASS, "two": PASS})}, set())
    assert got["verdict"] == PASS


def test_planted_failure_twin_one_failing_row_fails_the_journey():
    """THE TWIN of the test above: identical but for one row's verdict."""
    j = Journey("j", "t", ("x",), (row(), row(name="two")))
    got = core.judge(j, {"s": SuiteResult("s", 0, {"ok": PASS, "two": FAIL})}, set())
    assert got["verdict"] == FAIL
    assert "s/two is FAIL" in got["reason"]


@pytest.mark.parametrize("verdict", ["NOT-RUN", "NOT-REACHED", "HARNESS-FAULT", "INCONCLUSIVE",
                                     "BLOCKED", "notRun", "skipped", "pass", ""])
def test_anything_but_the_exact_pass_word_fails(verdict):
    j = Journey("j", "t", ("x",), (row(),))
    assert core.judge(j, {"s": SuiteResult("s", 0, {"ok": verdict})}, set())["verdict"] == FAIL


def test_a_missing_row_fails():
    j = Journey("j", "t", ("x",), (row(name="absent"),))
    got = core.judge(j, {"s": SuiteResult("s", 0, {"ok": PASS})}, set())
    assert got["verdict"] == FAIL and got["rows"][0]["verdict"] == "MISSING"


def test_a_suite_that_never_ran_fails():
    j = Journey("j", "t", ("x",), (row(),))
    assert core.judge(j, {}, set())["verdict"] == FAIL
    assert core.judge(j, {"s": SuiteResult("s", None, {})}, set())["verdict"] == FAIL


def test_a_passing_row_from_a_crashed_harness_fails():
    j = Journey("j", "t", ("x",), (row(),))
    got = core.judge(j, {"s": SuiteResult("s", 1, {"ok": PASS}, error="boom")}, set())
    assert got["verdict"] == FAIL and "rc=1" in got["reason"]


def test_a_gated_journey_is_skipped_with_its_reason_and_never_reads_rows():
    j = Journey("j", "t", ("x",), (row(),), requires="lab-refresh-8")
    got = core.judge(j, {"s": SuiteResult("s", 0, {"ok": PASS})}, set())
    assert got["verdict"] == SKIPPED
    assert got["reason"].startswith("requires: lab-refresh-8 — ")
    assert all(r["verdict"] == "NOT-RUN" for r in got["rows"])


def test_an_opened_gate_is_judged_like_any_other_journey():
    j = Journey("j", "t", ("x",), (row(),), requires="lab-refresh-8")
    assert core.judge(j, {"s": SuiteResult("s", 0, {"ok": FAIL})}, {"lab-refresh-8"})["verdict"] == FAIL
    assert core.judge(j, {"s": SuiteResult("s", 0, {"ok": PASS})}, {"lab-refresh-8"})["verdict"] == PASS


def test_ok_needs_a_pass_no_fail_and_a_proven_clean_sweep():
    good = Journey("g", "t", ("x",), (row(),))
    bad = Journey("b", "t", ("x",), (row(name="bad"),))
    gated = Journey("s", "t", ("x",), (row(),), requires="PLAT-19.2")
    results = {"s": SuiteResult("s", 0, {"ok": PASS, "bad": FAIL})}
    assert core.summarise([good, gated], results, set(), clean_sweep())["ok"] is True
    # planted failures, one at a time
    assert core.summarise([good, bad], results, set(), clean_sweep())["ok"] is False
    assert core.summarise([gated], results, set(), clean_sweep())["ok"] is False
    assert core.summarise([good], results, set(), None)["ok"] is False
    assert core.summarise([good], results, set(), {"selfTest": {"killed": False}, "hits": []})["ok"] is False
    assert core.summarise([good], results, set(), {"selfTest": {"killed": True}, "hits": [{"f": 1}]})["ok"] is False


def test_skipped_is_counted_apart_from_pass():
    gated = Journey("s", "t", ("x",), (row(),), requires="PLAT-19.2")
    good = Journey("g", "t", ("x",), (row(),))
    got = core.summarise([good, gated], {"s": SuiteResult("s", 0, {"ok": PASS})}, set(), clean_sweep())
    assert got["counts"] == {PASS: 1, FAIL: 0, SKIPPED: 1}


def test_summary_selftest_kills_its_planted_failures():
    assert core.summary_selftest()["killed"] is True


def test_summary_selftest_fails_when_judge_is_lenient(monkeypatch):
    """The live self-test must itself be able to fail: a judge that passes
    everything is caught."""
    real = core.judge

    def lenient(journey, results, opened):
        got = real(journey, results, opened)
        if got["verdict"] == FAIL:
            got["verdict"] = PASS
        return got

    monkeypatch.setattr(core, "judge", lenient)
    with pytest.raises(RuntimeError, match="cannot fail as it must"):
        core.summary_selftest()


# --- catalogue rules ----------------------------------------------------------


def test_catalogue_refuses_a_text_only_journey():
    j = Journey("j", "t", ("x",), (row(verifies=(TEXT,)),))
    assert any("only rendered text" in p for p in core.catalogue_violations([j]))


def test_catalogue_refuses_a_data_journey_without_archive_or_evidence():
    j = Journey("j", "t", ("x",), (row(verifies=(RESOURCE,)),), data=True)
    assert any("neither archive data nor evidence" in p for p in core.catalogue_violations([j]))
    ok = Journey("j", "t", ("x",), (row(verifies=(RESOURCE, ARCHIVE)),), data=True)
    assert core.catalogue_violations([ok]) == []


def test_catalogue_refuses_empty_unknown_and_uncited():
    assert core.catalogue_violations([Journey("j", "t", ("x",), ())])
    assert core.catalogue_violations([Journey("j", "t", (), (row(),))])
    assert core.catalogue_violations([Journey("j", "t", ("x",), (row(),), requires="lab-refresh-99")])
    assert core.catalogue_violations([Journey("j", "t", ("x",), (Row("s", "n", (RESOURCE,), "nowhere"),))])
    assert core.catalogue_violations([Journey("j", "t", ("x",), (row(verifies=("vibes",)),))])
    two = Journey("j", "t", ("x",), (row(),))
    assert any("duplicate" in p for p in core.catalogue_violations([two, two]))


def test_catalogue_refuses_a_journey_that_reads_no_durable_resource():
    j = Journey("j", "t", ("x",), (row(verifies=(EVIDENCE,)),))
    assert any("durable resource" in p for p in core.catalogue_violations([j]))


# --- redaction and sweep -----------------------------------------------------


def test_redact_removes_needles_pems_and_key_values():
    needles = Needles()
    needles.add("s3cr3t-value-123")
    pem = "-----BEGIN " + "PRIVATE KEY-----\nAAAA\n-----END " + "PRIVATE KEY-----"
    text = f"x s3cr3t-value-123 y {pem} password=hunter2hunter2 token: abcdefghijk"
    out = core.redact(text, needles)
    assert "s3cr3t-value-123" not in out
    assert "PRIVATE KEY-----\nAAAA" not in out
    assert "hunter2hunter2" not in out and "abcdefghijk" not in out


def test_needles_ignore_short_values_and_index_pem_body_lines():
    needles = Needles()
    needles.add("admin")
    assert len(needles) == 0
    body = "M" * 64
    needles.add("-----BEGIN X-----\n" + body + "\n-----END X-----")
    assert body in list(needles)


def test_sweep_finds_a_needle_and_a_pattern(tmp_path):
    needles = Needles()
    needles.add("exact-lab-secret-value")
    (tmp_path / "a.json").write_text('{"note": "exact-lab-secret-value"}')
    (tmp_path / "b.log").write_text("password: " + "Z" * 12)
    got = core.sweep(tmp_path, needles)
    files = {h["file"] for h in got["hits"]}
    assert files == {"a.json", "b.log"}
    # the needle itself is never reproduced in a hit
    assert "exact-lab-secret-value" not in repr(got)


def test_sweep_is_clean_over_redacted_text(tmp_path):
    needles = Needles()
    needles.add("exact-lab-secret-value")
    (tmp_path / "a.txt").write_text(core.redact("pw exact-lab-secret-value password=Zzzzzzzzzz", needles))
    assert core.sweep(tmp_path, needles)["hits"] == []


def test_sweep_selftest_is_killed_and_leaves_nothing(tmp_path):
    needles = Needles()
    got = core.sweep_selftest(tmp_path, needles)
    assert got["killed"] is True
    assert not (tmp_path / core.SELFTEST_FILE).exists()
    assert len(needles) == 0


def test_sweep_selftest_fails_when_the_sweep_is_blind(tmp_path, monkeypatch):
    """The self-test's own negative control: a sweep that finds nothing must
    make the self-test raise, not report a clean run."""
    monkeypatch.setattr(core, "sweep", lambda root, needles: {"hits": []})
    with pytest.raises(RuntimeError, match="cannot fail"):
        core.sweep_selftest(tmp_path, Needles())
    assert not (tmp_path / core.SELFTEST_FILE).exists()
