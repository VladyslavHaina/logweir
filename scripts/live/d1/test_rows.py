#!/usr/bin/env python3
"""Unit rows for the D1 live harness's row decisions and its source-matching.

    python3 scripts/live/d1/test_rows.py

No cluster, no network: every case is a recorded status block or a stubbed
`docker inspect`. Each decision is exercised against the shape the controller
publishes today AND against the shape the row used to look for; the second must
be REFUSED. A decision that cannot be made to say False is not a decision.

The recorded shapes come from lab-refresh-3 §8.1 and §9.1 and from this
harness's own run of 2026-09-18.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="d1-test-rows-")
os.environ.setdefault("LOGWEIR_D1_OUT", _TMP)
os.environ.setdefault("LOGWEIR_D1_NS", "hr-d1-unit")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import run as d1  # noqa: E402
from fence import fenced  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[3]
FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


# --- L-09-1: where the discovery's accounting lives -------------------------
PLAN_SELECTION = {
    "coverage": "VisibleUserTopicsOnly",
    "mode": "AllUserTopics",
    "resolvedTopicCount": 2,
    "discovery": {
        "basis": "metadata-list",
        "visibility": "unknown",
        "visibleTopicCount": 5,
        "limitedTopicCount": 0,
        "internalExcluded": {"count": 1, "names": ["__consumer_offsets"]},
        "excludedByRule": {"count": 2, "names": ["pfx-a", "skip-me"]},
    },
}
STATUS_SELECTION = {
    "mode": "AllUserTopics",
    "coverage": "VisibleUserTopicsOnly",
    "resolvedTopicCount": 2,
    "internalExcludedCount": 1,
    "excludedByRuleCount": 2,
    "limitedTopicCount": 0,
}
# What the row used to read: the flat spellings, on the PLAN, where no build has
# ever written them.
PLAN_SELECTION_AS_THE_ROW_READ_IT = {
    "coverage": "VisibleUserTopicsOnly",
    "internalExcludedCount": 1,
    "excludedByRuleCount": 2,
}


def test_the_discovery_counts_are_read_where_they_are_written() -> None:
    row("L-09-1: the plan's discovery block and the status agree",
        all(d1.selection_counts_agree(PLAN_SELECTION, STATUS_SELECTION).values()))
    row("MUTANT: the pre-fix reading — flat counts on the plan — is refused",
        not all(d1.selection_counts_agree(
            PLAN_SELECTION_AS_THE_ROW_READ_IT, STATUS_SELECTION).values()))
    disagree = dict(STATUS_SELECTION, excludedByRuleCount=1)
    row("MUTANT: a status that disagrees with the plan it was frozen from",
        not all(d1.selection_counts_agree(PLAN_SELECTION, disagree).values()))
    leaked = dict(STATUS_SELECTION, names=["__consumer_offsets", "t1"])
    row("MUTANT: an unbounded name list reaching the status",
        not all(d1.selection_counts_agree(PLAN_SELECTION, leaked).values()))
    unnamed = json.loads(json.dumps(PLAN_SELECTION))
    unnamed["discovery"]["internalExcluded"]["names"] = []
    row("MUTANT: a count with no sample to justify it",
        not all(d1.selection_counts_agree(unnamed, STATUS_SELECTION).values()))
    wrong_rule = json.loads(json.dumps(PLAN_SELECTION))
    wrong_rule["discovery"]["excludedByRule"] = {"count": 2, "names": ["pfx-a", "t1"]}
    row("MUTANT: the right count over the wrong topics",
        not all(d1.selection_counts_agree(wrong_rule, STATUS_SELECTION).values()))


# --- L-09-4: how a discovery terminal state is shaped ------------------------
EMPTY_RUN_STATUS = {"phase": "Failed", "exitReason": "operational"}
FAILED_CONDITION = {
    "type": "Failed", "status": "True", "reason": "SelectionEmpty",
    "message": ("the discovery for empty-run resolved no topics at all: 5 visible, 1 internal, "
                "4 removed by spec.allUserTopics.exclude, 0 the broker refused to describe. "
                "A mandatory allowlist whose absence means `all topics` is not an allowlist "
                "(guard G-GLOB), so no runner Job is created and this run is not retried"),
}
RESOLVED_CONDITION = {"type": "TopicsResolved", "status": "False", "reason": "SelectionEmpty"}


def test_an_empty_resolution_is_refused_on_the_conditions() -> None:
    row("L-09-4: Failed=True/SelectionEmpty, TopicsResolved=False, operational, no exitCode",
        all(d1.selection_empty_refusal(
            EMPTY_RUN_STATUS, FAILED_CONDITION, RESOLVED_CONDITION).values()))
    row("MUTANT: the pre-fix reading — status.reason set and no conditions at all",
        not all(d1.selection_empty_refusal(
            dict(EMPTY_RUN_STATUS, reason="SelectionEmpty"), {}, {}).values()))
    row("MUTANT: an exitCode claims a runner ran",
        not all(d1.selection_empty_refusal(
            dict(EMPTY_RUN_STATUS, exitCode=0), FAILED_CONDITION, RESOLVED_CONDITION).values()))
    row("MUTANT: TopicsResolved left True beside a Failed run",
        not all(d1.selection_empty_refusal(
            EMPTY_RUN_STATUS, FAILED_CONDITION,
            dict(RESOLVED_CONDITION, status="True")).values()))
    row("MUTANT: a refusal that does not say a runner Job never follows",
        not all(d1.selection_empty_refusal(
            EMPTY_RUN_STATUS, dict(FAILED_CONDITION, message="resolved no topics"),
            RESOLVED_CONDITION).values()))
    row("MUTANT: a different terminal reason wearing the same shape",
        not all(d1.selection_empty_refusal(
            EMPTY_RUN_STATUS, dict(FAILED_CONDITION, reason="DiscoveryFailed"),
            RESOLVED_CONDITION).values()))


# --- the fence's source matching --------------------------------------------
class _Failure(Exception):
    def __init__(self, message: str, obj: object = None, dumps: object = None) -> None:
        super().__init__(message)


class _Harness:
    """Enough of `run.py` for `fenced.assert_source_matched`, with `docker
    inspect` answered from a table instead of from a daemon."""

    Failure = _Failure
    ROOT = ROOT

    def __init__(self, labels: dict[str, str]) -> None:
        self.labels = labels

    def run(self, args, timeout=120, check=True, record=True, data=None):
        if args[0] == "docker":
            ref = args[args.index("inspect") + 1]
            label = self.labels.get(ref, fenced.NO_LABEL)
            return subprocess.CompletedProcess(args, 0, f"sha256:feed{ref} {label}\n", "")
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                              cwd=str(ROOT), check=False)


def _head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(ROOT), capture_output=True,
                          text=True, check=True).stdout.strip()


def _matched(labels: dict[str, str], which: str = "new") -> tuple[bool, str]:
    fenced.REVISION_OVERRIDE.clear()
    try:
        fenced.assert_source_matched(_Harness(labels), which)
        return True, ""
    except _Failure as exc:
        return False, str(exc)


def test_the_fence_matches_its_source_without_a_written_down_revision() -> None:
    head = _head()
    ok, why = _matched({"weirkeeper:scram-reviewed": head, "logweir:scram-local": head})
    row("fence: a pair labelled with a commit this checkout contains is accepted", ok, why)
    ok, why = _matched({"weirkeeper:scram-reviewed": "0" * 40, "logweir:scram-local": "0" * 40})
    row("MUTANT: a commit this checkout does not have is refused",
        not ok and "does not have" in why, why)
    ok, why = _matched({"weirkeeper:scram-reviewed": head,
                        "logweir:scram-local": "4956785d00d74fe960c84d396d2eff852c68ebd8"})
    row("MUTANT: a controller and a runner from different commits are not one build",
        not ok and "not" in why, why)
    ok, why = _matched({"weirkeeper:scram-reviewed": fenced.NO_LABEL,
                        "logweir:scram-local": fenced.NO_LABEL})
    row("MUTANT: an unlabelled image cannot be source-matched",
        not ok and "no org.opencontainers.image.revision label" in why, why)

    # A FLAG NAMES AN EXPECTATION; IT NEVER SUSPENDS THE CHECK.
    fenced.REVISION_OVERRIDE["new"] = "4956785d00d74fe960c84d396d2eff852c68ebd8"
    try:
        fenced.assert_source_matched(
            _Harness({"weirkeeper:scram-reviewed": head, "logweir:scram-local": head}), "new")
        row("MUTANT: --fence-revision does not excuse an image that carries something else",
            False, "the pin was accepted against a differently labelled image")
    except _Failure as exc:
        row("MUTANT: --fence-revision does not excuse an image that carries something else",
            "expected from the flag" in str(exc), str(exc))
    finally:
        fenced.REVISION_OVERRIDE.clear()


def test_the_non_image_allowlist_is_what_lets_a_harness_branch_run() -> None:
    row("the allowlist covers the live harnesses, their fixtures and prose",
        fenced.NON_IMAGE_PATHS == ("scripts/live/", "scripts/fixtures/", "e2e/", "docs/"),
        str(fenced.NON_IMAGE_PATHS))
    row("a product path is not in it",
        not "crates/weirkeeper/src/lib.rs".startswith(fenced.NON_IMAGE_PATHS)
        and not "charts/logweir/values.yaml".startswith(fenced.NON_IMAGE_PATHS)
        and not "Dockerfile".startswith(fenced.NON_IMAGE_PATHS))
    row("this file is in it",
        "scripts/live/d1/test_rows.py".startswith(fenced.NON_IMAGE_PATHS))


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
