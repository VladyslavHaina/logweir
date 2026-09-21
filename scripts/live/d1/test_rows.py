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
from fence import rows as fence_rows  # noqa: E402

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


# --- L-05.1-3: D1 §6.2 as amended, and how wide its carve-out is ------------
# The recorded shapes are this branch's own fenced run of 2026-09-18: a Backup
# written by `main@4956785`, read before and after the swap to `c6422a7`.
VERIFICATION_BEFORE_UPGRADE = {
    "result": "Valid",
    "matchedKeyId": "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe",
    "payloadType": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    "verifiedAt": "2026-09-18T18:54:36Z",
}
VERIFICATION_AFTER_UPGRADE = dict(
    VERIFICATION_BEFORE_UPGRADE,
    signedAt="2026-09-18T18:54:31Z",
    trust={"basis": "Current", "keyState": "Active", "policy": {"name": "legacy-roster-v1"}},
)


def _status(verification: dict) -> dict:
    return {
        "phase": "Succeeded",
        "exitCode": 0,
        "backupId": "95154a1a-e730-4536-9898-92f8405c2c0d",
        "evidence": {
            "receiptKey": "logweir/backups/95154a1a-…/01M2TXXH98XY8XFH2AJDE11CBH.receipt.json",
            "receiptSha256": "sha256:" + "f1" * 32,
            "sidecarKey": "logweir/backups/95154a1a-…/01M2TXXH98XY8XFH2AJDE11CBH.receipt.sig",
            "verification": verification,
        },
    }


BEFORE = _status(VERIFICATION_BEFORE_UPGRADE)
AFTER = _status(VERIFICATION_AFTER_UPGRADE)


def _carve(before: dict, after: dict) -> bool:
    clauses, _ = fence_rows.status_delta_is_only_a_trust_reread(before, after)
    return all(clauses.values())


def test_the_upgrade_may_add_signedAt_and_trust_and_nothing_else() -> None:
    row("L-05.1-3: the trust re-read ADDS signedAt and trust, verdict untouched",
        _carve(BEFORE, AFTER))
    row("an upgrade that moved nothing at all is allowed too", _carve(BEFORE, BEFORE))

    # THE MUTANT THE CARVE-OUT EXISTS FOR: a verdict that moved without a read.
    verdict_moved = _status(dict(VERIFICATION_AFTER_UPGRADE, result="Untrusted"))
    row("MUTANT: `result` changed across the upgrade", not _carve(BEFORE, verdict_moved))
    row("MUTANT: `matchedKeyId` changed across the upgrade",
        not _carve(BEFORE, _status(dict(VERIFICATION_AFTER_UPGRADE, matchedKeyId="0" * 64))))
    row("MUTANT: `verifiedAt` restamped by a pass that verified nothing",
        not _carve(BEFORE,
                   _status(dict(VERIFICATION_AFTER_UPGRADE,
                                verifiedAt="2026-09-18T18:55:10Z"))))
    row("MUTANT: `signedAt` REWRITTEN rather than added — not a re-read of an absence",
        not _carve(_status(dict(VERIFICATION_BEFORE_UPGRADE, signedAt="2026-01-01T00:00:00Z")),
                   AFTER))
    row("MUTANT: a third verification field added under cover of the carve-out",
        not _carve(BEFORE, _status(dict(VERIFICATION_AFTER_UPGRADE, payloadType=None,
                                        somethingElse="x"))))
    receipt_moved = json.loads(json.dumps(AFTER))
    receipt_moved["evidence"]["receiptKey"] = "logweir/backups/elsewhere/receipt.json"
    row("MUTANT: the evidence's own keys moved beside the verification",
        not _carve(BEFORE, receipt_moved))
    phase_moved = json.loads(json.dumps(AFTER))
    phase_moved["phase"] = "Failed"
    row("MUTANT: a status field outside `evidence` moved", not _carve(BEFORE, phase_moved))
    row("MUTANT: the whole verification block appearing where none existed is not this "
        "carve-out",
        not _carve(_status({}), AFTER))


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


def _provenance(labels: dict[str, str], which: str = "new") -> dict[str, str]:
    return fenced.assert_source_matched(_Harness(labels), which)


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


def test_the_evidence_says_which_door_a_run_came_through() -> None:
    """`--fence-revision` and the `old` pin SUSPEND the drift check; the label
    match they cannot suspend. The provenance has to say so, because a reader of
    `results.json` otherwise cannot tell a compared run from an uncompared one
    (review L-4)."""
    head = _head()
    fenced.REVISION_OVERRIDE.clear()
    out = _provenance({"weirkeeper:scram-reviewed": head, "logweir:scram-local": head})
    row("a discovered expectation records the drift check as enforced",
        out["revisionSource"] == "label" and out["driftCheck"] == "enforced"
        and out["checkoutHead"] == head, str(out))
    out = _provenance({"weirkeeper:plat0102-4956785": "4956785d00d74fe960c84d396d2eff852c68ebd8",
                       "logweir:plat0102-4956785": "4956785d00d74fe960c84d396d2eff852c68ebd8"},
                      "old")
    row("the frozen `old` pin records the drift check as suspended",
        out["revisionSource"] == "pinned" and out["driftCheck"].startswith("suspended"),
        str(out))
    fenced.REVISION_OVERRIDE["new"] = head
    try:
        out = _provenance({"weirkeeper:scram-reviewed": head, "logweir:scram-local": head})
    finally:
        fenced.REVISION_OVERRIDE.clear()
    row("--fence-revision records the drift check as suspended, naming the flag",
        out["revisionSource"] == "flag" and out["driftCheck"] == "suspended: the expected "
        "revision came from the flag", str(out))


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


# --- the negative control's own certification (review harness-rows-4 D-L1) ---
DELIBERATE = {
    "status": "fail",
    "failure": ("DELIBERATELY FALSE ASSERTION: a named allowlist created no discovery Job, "
                "which is the landed behaviour L-09-6 asserts."),
}
# A `wait_for` timeout raises the SAME `Failure` type, so it lands as `fail`
# with a message about waiting. This is the shape that must not certify.
TIMEOUT_SHAPED = {
    "status": "fail",
    "failure": "timeout waiting for backup/neg-control to finish: dump /tmp/…/failure.json",
}


def test_only_the_deliberate_failure_certifies_the_negative_control() -> None:
    verdict = d1.negative_control_verdict(DELIBERATE)
    row("NEG-1 failing on its own false assertion certifies the harness",
        verdict["harnessCanFail"] and verdict["failedOnItsOwnAssertion"], str(verdict))
    row("and the recorded failure travels with the verdict",
        "DELIBERATELY FALSE ASSERTION" in verdict["recordedFailure"])
    timed_out = d1.negative_control_verdict(TIMEOUT_SHAPED)
    row("MUTANT: A TIMEOUT-SHAPED FAILURE DOES NOT COUNT — the false assertion "
        "was never reached",
        not timed_out["harnessCanFail"] and timed_out["observed"] == "fail",
        str(timed_out))
    row("MUTANT: a run in which NEG-1 never ran at all",
        not d1.negative_control_verdict(None)["harnessCanFail"])
    row("MUTANT: NEG-1 recorded as not-run",
        not d1.negative_control_verdict({"status": "not-run"})["harnessCanFail"])
    row("MUTANT: NEG-1 PASSED — the false assertion held, so the harness is not "
        "reading the cluster",
        not d1.negative_control_verdict(
            dict(DELIBERATE, status="pass"))["harnessCanFail"])
    row("MUTANT: a failure with no message at all",
        not d1.negative_control_verdict({"status": "fail", "failure": ""})["harnessCanFail"])


# --- L-09-5: an ACL-limited discovery, and what it may claim ----------------
#
# The recorded shapes are this worker's own live run against a KRaft broker with
# `StandardAuthorizer` and a SCRAM principal with no `Describe` on `secret-t`.
ACL_PLAN_SELECTION = {
    "mode": "AllUserTopics",
    "coverage": "VisibleUserTopicsOnly",
    "resolvedTopicCount": 2,
    "discovery": {
        "basis": "metadata-list",
        "visibility": "unknown",
        "visibleTopicCount": 2,
        "limitedTopicCount": 0,
        "internalExcluded": {"count": 0, "names": []},
        "excludedByRule": {"count": 0, "names": []},
    },
}
ACL_STATUS_SELECTION = {
    "mode": "AllUserTopics",
    "coverage": "VisibleUserTopicsOnly",
    "resolvedTopicCount": 2,
    "internalExcludedCount": 0,
    "excludedByRuleCount": 0,
    "limitedTopicCount": 0,
}
ACL_PRINCIPAL_SEES = ["acl-a", "acl-b"]


def test_an_acl_limited_discovery_never_claims_the_cluster() -> None:
    ok = d1.partial_discovery_never_claims_the_cluster(
        ACL_PLAN_SELECTION, ACL_STATUS_SELECTION, ["acl-a", "acl-b"],
        ACL_PRINCIPAL_SEES, "secret-t")
    row("L-09-5: a visible-only run describes itself as visible-only", all(ok.values()),
        str(ok))
    # THE ONE CLAIM THE ACCEPTANCE SENTENCE FORBIDS.
    attested_plan = dict(ACL_PLAN_SELECTION, coverage="AllUserTopicsAttested")
    attested_status = dict(ACL_STATUS_SELECTION, coverage="AllUserTopicsAttested")
    row("MUTANT: a run that claims AllUserTopicsAttested is refused",
        not all(d1.partial_discovery_never_claims_the_cluster(
            attested_plan, attested_status, ["acl-a", "acl-b"],
            ACL_PRINCIPAL_SEES, "secret-t").values()))
    row("MUTANT: the denied topic appearing in the frozen list is refused",
        not all(d1.partial_discovery_never_claims_the_cluster(
            ACL_PLAN_SELECTION, ACL_STATUS_SELECTION,
            ["acl-a", "acl-b", "secret-t"], ACL_PRINCIPAL_SEES, "secret-t").values()))
    # THE TWO NUMBERS ARE NOT ONE FACT, AND THE ROW NO LONGER PRETENDS THEY ARE
    # (review L-1). `limitedTopicCount` counts ANY per-entry error
    # (`backup_selection.rs:326`); `visibility: limited` is raised only by
    # `TopicAuthorizationFailed`. Each of the two rows below is a LEGAL product
    # state that the earlier biconditional would have painted red.
    limited_without_a_count = json.loads(json.dumps(ACL_PLAN_SELECTION))
    limited_without_a_count["discovery"]["visibility"] = "limited"
    row("LEGAL: `limited` with a zero count — an INTERNAL topic the broker "
        "refused, taken by the internal arm before the limited one — is accepted",
        all(d1.partial_discovery_never_claims_the_cluster(
            limited_without_a_count, ACL_STATUS_SELECTION, ["acl-a", "acl-b"],
            ACL_PRINCIPAL_SEES, "secret-t").values()))
    count_without_limited = json.loads(json.dumps(ACL_PLAN_SELECTION))
    count_without_limited["discovery"]["limitedTopicCount"] = 1
    row("LEGAL: a positive count beside `unknown` — a listing entry with a "
        "NON-authorization error — is accepted",
        all(d1.partial_discovery_never_claims_the_cluster(
            count_without_limited, dict(ACL_STATUS_SELECTION, limitedTopicCount=1),
            ["acl-a", "acl-b"], ACL_PRINCIPAL_SEES, "secret-t").values()))
    honest = json.loads(json.dumps(ACL_PLAN_SELECTION))
    honest["discovery"]["visibility"] = "limited"
    honest["discovery"]["limitedTopicCount"] = 1
    row("a genuinely `limited` listing, with its count, is accepted",
        all(d1.partial_discovery_never_claims_the_cluster(
            honest, dict(ACL_STATUS_SELECTION, limitedTopicCount=1),
            ["acl-a", "acl-b"], ACL_PRINCIPAL_SEES, "secret-t").values()))
    row("MUTANT: a status that disagrees with the plan's limited count is refused",
        not all(d1.partial_discovery_never_claims_the_cluster(
            honest, dict(ACL_STATUS_SELECTION, limitedTopicCount=0),
            ["acl-a", "acl-b"], ACL_PRINCIPAL_SEES, "secret-t").values()))
    no_count = json.loads(json.dumps(ACL_PLAN_SELECTION))
    del no_count["discovery"]["limitedTopicCount"]
    row("MUTANT: a plan that publishes NO limited count at all is refused",
        not all(d1.partial_discovery_never_claims_the_cluster(
            no_count, ACL_STATUS_SELECTION, ["acl-a", "acl-b"],
            ACL_PRINCIPAL_SEES, "secret-t").values()))
    row("MUTANT: a discovery block with no visibility at all is refused",
        not all(d1.partial_discovery_never_claims_the_cluster(
            {"coverage": "VisibleUserTopicsOnly"}, ACL_STATUS_SELECTION,
            ["acl-a", "acl-b"], ACL_PRINCIPAL_SEES, "secret-t").values()))


ACL_REFUSE_STATUS = {"phase": "Failed", "exitReason": "operational"}
ACL_REFUSE_FAILED = {
    "status": "True",
    "reason": "DiscoveryIncomplete",
    "message": (
        "the discovery for acl-refuse could not establish that it saw every user topic "
        "(visibility `unknown`, 0 topic(s) the broker refused to describe) and "
        "spec.allUserTopics.incompleteDiscovery is `Refuse`. Kafka omits topics a principal "
        "cannot describe, so a successful listing alone is never proof; grant the principal "
        "Describe on the cluster, record an administrator attestation, or choose "
        "`BackUpVisibleTopics` and accept the `VisibleUserTopicsOnly` label"
    ),
}
ACL_REFUSE_RESOLVED = {"status": "False", "reason": "DiscoveryIncomplete"}


def test_the_refuse_policy_refuses_on_the_conditions() -> None:
    row("L-09-5: Refuse ends Failed/DiscoveryIncomplete on both conditions",
        all(d1.discovery_incomplete_refusal(
            ACL_REFUSE_STATUS, ACL_REFUSE_FAILED, ACL_REFUSE_RESOLVED).values()))
    row("MUTANT: an exitCode would claim a runner ran, and is refused",
        not all(d1.discovery_incomplete_refusal(
            dict(ACL_REFUSE_STATUS, exitCode=1), ACL_REFUSE_FAILED,
            ACL_REFUSE_RESOLVED).values()))
    row("MUTANT: TopicsResolved carrying a different reason is refused",
        not all(d1.discovery_incomplete_refusal(
            ACL_REFUSE_STATUS, ACL_REFUSE_FAILED,
            {"status": "False", "reason": "DiscoveryFailed"}).values()))
    row("MUTANT: the pre-fix reading — `status.reason` and no conditions — is refused",
        not all(d1.discovery_incomplete_refusal(
            dict(ACL_REFUSE_STATUS, reason="DiscoveryIncomplete"), {}, {}).values()))
    row("MUTANT: a refusal that does not say why a listing is not proof is refused",
        not all(d1.discovery_incomplete_refusal(
            ACL_REFUSE_STATUS, dict(ACL_REFUSE_FAILED, message="discovery incomplete"),
            ACL_REFUSE_RESOLVED).values()))


# --- L-09-3a: the frozen list outlives the topic ----------------------------
PLAN_AT_HOLD = {
    "name": "race-delete-plan",
    "sha256": "sha256:aaaa",
    "resourceVersion": "4711",
    "inputs": {"topics": ["rc-gone", "rc-keep"]},
}
RECEIPT_ZERO = {"source": {"topics": ["rc-gone", "rc-keep"]},
                "records": {"rc-gone": 0, "rc-keep": 5}}


# What the runner said. The `Failed` branch is only evidence when the runner's
# own words are about THIS topic (review M-1) — this build exits 1 for plenty of
# other reasons, and did so twice in three runs on 2026-09-21.
NAMES_THE_TOPIC = (
    "the runner exited 1 (operational)\n"
    "\nERROR reading topic rc-gone: UNKNOWN_TOPIC_OR_PARTITION\n"
)
UNRELATED_FAILURE = (
    "the runner exited 1 (operational)\n"
    "\nERROR archive upload to s3://kafka-backups timed out after 120s\n"
)


def test_a_topic_deleted_between_freeze_and_execution() -> None:
    ok = fence_rows.frozen_list_survives_a_deleted_topic(
        PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Succeeded"}, RECEIPT_ZERO, "rc-gone")
    row("L-09-3a: Succeeded with 0 records for the deleted topic is admitted",
        all(ok.values()), str(ok))
    row("L-09-3a: Failed exit 1 whose text names the deleted topic is the other "
        "outcome D1 admits",
        all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Failed", "exitCode": 1},
            None, "rc-gone", NAMES_THE_TOPIC).values()))
    # THE FINDING THIS ROW EXISTS FOR.
    row("MUTANT: `Failed` exit 1 whose message does not name the deleted topic is "
        "refused — an unrelated operational failure is not this measurement",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Failed", "exitCode": 1},
            None, "rc-gone", UNRELATED_FAILURE).values()))
    row("MUTANT: `Failed` exit 1 with NO failure text at all — a reaped pod — is "
        "refused rather than passing on the status strings",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Failed", "exitCode": 1},
            None, "rc-gone", "").values()))
    row("the Succeeded branch needs no failure text — it is attributed by "
        "records[gone] == 0",
        all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Succeeded"}, RECEIPT_ZERO,
            "rc-gone", "").values()))
    moved = dict(PLAN_AT_HOLD, resourceVersion="4712")
    row("MUTANT: a frozen plan that MOVED is refused — the snapshot is immutable",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, moved, {"phase": "Succeeded"}, RECEIPT_ZERO, "rc-gone").values()))
    rewritten = {"name": "race-delete-plan", "sha256": "sha256:aaaa",
                 "resourceVersion": "4711", "inputs": {"topics": ["rc-keep"]}}
    row("MUTANT: a frozen list rewritten to drop the deleted topic is refused",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, rewritten, {"phase": "Succeeded"},
            {"source": {"topics": ["rc-keep"]}, "records": {"rc-keep": 5}},
            "rc-gone").values()))
    row("MUTANT: records claimed for the topic that was deleted are refused",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Succeeded"},
            {"source": {"topics": ["rc-gone", "rc-keep"]},
             "records": {"rc-gone": 5, "rc-keep": 5}}, "rc-gone").values()))
    row("MUTANT: a receipt naming a topic OUTSIDE the frozen list is refused",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Succeeded"},
            {"source": {"topics": ["rc-gone", "rc-keep"]},
             "records": {"rc-gone": 0, "rc-keep": 5, "rc-other": 9}}, "rc-gone").values()))
    row("MUTANT: Failed with exit 0 is neither outcome D1 admits, however well "
        "its text names the topic",
        not all(fence_rows.frozen_list_survives_a_deleted_topic(
            PLAN_AT_HOLD, PLAN_AT_HOLD, {"phase": "Failed", "exitCode": 0},
            None, "rc-gone", NAMES_THE_TOPIC).values()))


# --- L-09-3b: the source moved under the run --------------------------------
def test_a_source_change_between_discovery_and_freeze_is_refused() -> None:
    status = {"phase": "Failed", "reason": "SourceChangedDuringResolution"}
    failed = {"status": "True", "reason": "SourceChangedDuringResolution"}
    resolved = {"status": "False", "reason": "SourceChangedDuringResolution"}
    row("L-09-3b: Failed/SourceChangedDuringResolution with no runner Job",
        all(fence_rows.source_change_is_refused(status, failed, resolved, []).values()))
    row("MUTANT: a runner Job created anyway is refused",
        not all(fence_rows.source_change_is_refused(
            status, failed, resolved, ["race-source"]).values()))
    row("MUTANT: Succeeded with the reason on the status is refused",
        not all(fence_rows.source_change_is_refused(
            {"phase": "Succeeded", "reason": "SourceChangedDuringResolution"},
            failed, resolved, []).values()))
    row("MUTANT: a DIFFERENT terminal reason does not satisfy this row",
        not all(fence_rows.source_change_is_refused(
            {"phase": "Failed", "reason": "DiscoveryFailed"},
            {"status": "True", "reason": "DiscoveryFailed"},
            {"status": "False", "reason": "DiscoveryFailed"}, []).values()))


def test_every_negative_control_is_judged_by_its_own_sentence() -> None:
    row("the four negative controls each name the row they certify",
        set(d1.NEGATIVE_CONTROLS) == {"NEG-1", "NEG-09-3a", "NEG-09-3b", "NEG-09-5"}
        and set(d1.NEGATIVE_CONTROLS.values()) == {"L-09-6", "L-09-3a", "L-09-3b", "L-09-5"})
    verdict = d1.negative_control_verdict(DELIBERATE, "NEG-09-5")
    row("a control's verdict carries the row it certifies",
        verdict["id"] == "NEG-09-5" and verdict["certifies"] == "L-09-5"
        and verdict["harnessCanFail"], str(verdict))
    row("MUTANT: a timeout-shaped failure certifies nothing, for any control",
        not d1.negative_control_verdict(TIMEOUT_SHAPED, "NEG-09-3a")["harnessCanFail"])


# --- the proxy's capture log outlives a row -------------------------------
STALE = {"at": 1.0, "kind": "job_create", "name": "race-delete",
         "bodySha256": "old", "paused": True}
FRESH = {"at": 2.0, "kind": "job_create", "name": "race-delete",
         "bodySha256": "new", "paused": True}


class _StubHarness:
    """Just enough of `run.py` for `_await_capture`: one poll, no cluster."""

    Failure = RuntimeError

    @staticmethod
    def wait_until(predicate, *, timeout=0, interval=0, what=""):
        value = predicate()
        if not value:
            raise RuntimeError(f"nothing matched: {what}")
        return value


def _await_with(captures, mark):
    saved_captures, saved_mark = fenced.captures, fenced.LAST_ARM_MARK
    fenced.captures = lambda H, kind=None, **kw: [  # type: ignore[assignment]
        c for c in captures if kind is None or c["kind"] == kind
    ]
    fenced.LAST_ARM_MARK = mark
    try:
        return fence_rows._await_capture(_StubHarness, "job_create", name="race-delete")
    except RuntimeError:
        return None
    finally:
        fenced.captures, fenced.LAST_ARM_MARK = saved_captures, saved_mark


def test_a_row_never_matches_the_capture_its_previous_run_left() -> None:
    mark = {fenced.capture_identity(STALE)}
    row("the capture this row's arm did not see is the one it waits for",
        (_await_with([STALE, FRESH], mark) or {}).get("bodySha256") == "new")
    row("MUTANT: with the mark empty, the PREVIOUS run's held request is matched — "
        "which is the defect of 2026-09-21",
        (_await_with([STALE, FRESH], set()) or {}).get("bodySha256") == "new"
        and (_await_with([STALE], set()) or {}).get("bodySha256") == "old")
    row("a stale capture alone leaves the row waiting rather than measuring",
        _await_with([STALE], mark) is None)
    row("`arm` snapshots identities, not just names",
        fenced.capture_identity(STALE) != fenced.capture_identity(FRESH))



# --- L-06-2-cli: the CLI/API comparison's own decisions ---------------------
#
# The row compares two objects leaf by leaf and calls them the same. A
# comparison that cannot say "different" is not a comparison, so every case
# below is a recorded shape AND the mutation of it the row must refuse.

P062_SCHEDULE = {
    "metadata": {"name": "manual-cli", "uid": "3f1c-uid", "generation": 3},
    "spec": {
        "schedule": "0 3 * * *",
        "suspend": True,
        "sourceRef": {"name": "source"},
        "topics": ["t1", "t2"],
        "archive": {"url": "logweir-destination://dest"},
        "destinationRef": {"name": "dest"},
        "concurrencyPolicy": "Forbid",
        "activeDeadlineSeconds": 600,
        "retention": {"keepLast": 3},
    },
    "status": {
        "observedGeneration": 3,
        "policy": {"generation": 3, "runPolicySha256": "sha256:" + "ab" * 32},
    },
}


def test_the_manual_copy_is_the_policy_half_and_the_identity_half() -> None:
    copied = d1.run_policy_copy(P062_SCHEDULE)
    row("L-06-2-cli: the copy carries the schedule's policy fields",
        copied["sourceRef"] == {"name": "source"}
        and copied["topics"] == ["t1", "t2"]
        and copied["archive"] == {"url": "logweir-destination://dest"}
        and copied["destinationRef"] == {"name": "dest"}
        and copied["deadlineSeconds"] == 600)
    row("L-06-2-cli: and the manual identity, with no slot",
        copied["triggeredBy"] == "manual"
        and copied["trigger"] == {"kind": "Manual", "attempt": 0}
        and "slot" not in copied)
    row("L-06-2-cli: scheduleRef records uid, generation and the PUBLISHED digest",
        copied["scheduleRef"] == {
            "name": "manual-cli", "uid": "3f1c-uid", "generation": 3,
            "runPolicySha256": "sha256:" + "ab" * 32,
        })
    # WHAT IT MUST NOT CARRY. Cadence, concurrency and retention decide WHEN a
    # run happens and are excluded from the run policy by D1 §3.2; a copy that
    # dragged them onto the Backup would not match the API's object and would
    # not be a run policy either.
    row("MUTANT: the cadence half is not copied onto the run",
        not ({"schedule", "suspend", "concurrencyPolicy", "retention",
              "activeDeadlineSeconds"} & set(copied)))
    absent = json.loads(json.dumps(P062_SCHEDULE))
    del absent["spec"]["activeDeadlineSeconds"]
    row("L-06-2-cli: an absent deadline resolves to 3600, as the digest requires",
        d1.run_policy_copy(absent)["deadlineSeconds"] == 3600)
    dynamic = json.loads(json.dumps(P062_SCHEDULE))
    dynamic["spec"]["topics"] = []
    dynamic["spec"]["allUserTopics"] = {"incompleteDiscovery": "Refuse"}
    row("L-06-2-cli: a dynamic selection is copied whole, with topics []",
        d1.run_policy_copy(dynamic)["allUserTopics"] == {"incompleteDiscovery": "Refuse"}
        and d1.run_policy_copy(dynamic)["topics"] == [])
    row("L-06-2-cli: the four labels, and no others",
        d1.manual_labels(P062_SCHEDULE) == {
            "logweir.dev/trigger": "manual",
            "logweir.dev/attempt": "0",
            "logweir.dev/schedule": "manual-cli",
            "logweir.dev/schedule-uid": "3f1c-uid",
        })


def test_the_spec_comparison_can_say_different() -> None:
    left = d1.run_policy_copy(P062_SCHEDULE)
    row("L-06-2-cli: two copies of the same schedule are byte-equal",
        d1.spec_differences(left, d1.run_policy_copy(P062_SCHEDULE)) == [])
    for path, change in (
        ("deadlineSeconds", lambda s: s.update(deadlineSeconds=601)),
        ("sourceRef.name", lambda s: s["sourceRef"].update(name="other")),
        ("topics[1]", lambda s: s["topics"].__setitem__(1, "t3")),
        ("archive.url", lambda s: s["archive"].update(url="s3://elsewhere")),
        ("scheduleRef.runPolicySha256",
         lambda s: s["scheduleRef"].update(runPolicySha256="sha256:" + "cd" * 32)),
        ("trigger.kind", lambda s: s["trigger"].update(kind="Scheduled")),
    ):
        mutated = json.loads(json.dumps(left))
        change(mutated)
        found = d1.spec_differences(left, mutated)
        row(f"MUTANT: a changed {path} is reported at that path",
            any(d.startswith(path) for d in found), repr(found))
    # A field that is present on one side and absent on the other is a
    # difference and not a match: the row's whole claim is "the only fields the
    # API adds are the name and its own annotations".
    dropped = json.loads(json.dumps(left))
    del dropped["destinationRef"]
    row("MUTANT: an absent field is a difference, not a tie",
        any(d.startswith("destinationRef") for d in d1.spec_differences(left, dropped)))
    # A list that is shorter is a difference even when every entry it has
    # matches: a topic quietly dropped from a copy is exactly the defect this
    # comparison exists to catch.
    short = json.loads(json.dumps(left))
    short["topics"] = ["t1"]
    row("MUTANT: a dropped topic is a difference",
        d1.spec_differences(left, short) != [])



def test_the_digest_control_requires_a_refusal_and_refuses_a_success() -> None:
    """Review F2: the negative control recorded the product's refusal instead
    of requiring it, and its wait admitted `Succeeded`."""
    def phase(name: str) -> dict:
        return {"status": {"phase": name}}
    row("L-06-2-cli: a Failed mutated copy is judged",
        d1.digest_refusal_judged(phase("Failed")) is True)
    row("L-06-2-cli: a Refused one is judged too",
        d1.digest_refusal_judged(phase("Refused")) is True)
    row("L-06-2-cli: a run still going is not judged yet",
        d1.digest_refusal_judged(phase("Running")) is False
        and d1.digest_refusal_judged({}) is False)
    # THE MUTANT THE ROW EXISTS FOR: a controller that EXECUTED the mutated
    # copy. The pre-fix predicate (`terminal`) returned True here and the row
    # went green with `refusedForTheDigest: false` in the artifact.
    row("MUTANT: a SUCCEEDED mutated copy fails the control immediately",
        not d1.terminal(phase("Running")) and _raises(phase("Succeeded")))
    row("MUTANT: the pre-fix predicate accepted exactly that",
        d1.terminal(phase("Succeeded")) is True)
    # And the reason set is the product's, not "any refusal at all".
    row("L-06-2-cli: RunPolicyDigestMismatch is in the accepted set",
        "RunPolicyDigestMismatch" in d1.DIGEST_REFUSALS)
    row("MUTANT: an unrelated terminal reason is NOT in it",
        "Operational" not in d1.DIGEST_REFUSALS
        and "VolumeMountFailed" not in d1.DIGEST_REFUSALS)


def _raises(obj: dict) -> bool:
    try:
        d1.digest_refusal_judged(obj)
    except Exception as failed:  # noqa: BLE001 - the point is that it raises
        return "digest is not being checked" in str(failed)
    return False


def test_the_raw_idempotency_key_never_reaches_an_artifact() -> None:
    captured = json.dumps({
        "argv": ["curl", "-H", "Idempotency-Key: plat06-2-finish.deadbeefcafe", "-X", "POST"],
        "note": "after it",
    })
    cleaned = d1.redact(captured)
    row("L-06-2-cli: the raw Idempotency-Key is redacted out of a recorded command",
        "plat06-2-finish.deadbeefcafe" not in cleaned and "[REDACTED]" in cleaned, cleaned)
    row("MUTANT: the redaction stops at the JSON string and eats nothing after it",
        json.loads(cleaned)["note"] == "after it", cleaned)
    row("MUTANT: a document with no key at all is returned unchanged",
        d1.redact('{"a": "b"}') == '{"a": "b"}')


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
