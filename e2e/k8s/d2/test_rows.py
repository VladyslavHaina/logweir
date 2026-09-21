#!/usr/bin/env python3
"""Unit rows for the D2 live harness's row decisions.

    python3 e2e/k8s/d2/test_rows.py

No cluster, no network, no credential: every case is a recorded status block.
Each decision is exercised against the shape the product publishes today AND
against the shape it published before the defect was fixed; the second must be
REFUSED. A decision that cannot be made to say False is not a decision.

The recorded shapes come from lab-refresh-3 §8.4 and from this harness's own
run of 2026-09-18.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import sys
import tempfile
from typing import Any

_TMP = tempfile.mkdtemp(prefix="d2-test-rows-")
os.environ.setdefault("D2W14_OUT", str(pathlib.Path(_TMP) / "private"))
os.environ.setdefault("D2W14_ART", str(pathlib.Path(_TMP) / "artifacts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d2_live as d2  # noqa: E402

FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


# THE GATE HAD TO BE ABLE TO FAIL. `row()` records a failure and returns, which
# is right for `main()` — every row runs and the count is the report. But under
# `python3 -m pytest e2e/k8s/d2` nothing called `main()`, no `test_` function
# asserted anything, and the suite passed with every row failing: a harness row
# that passes when the product does nothing, which is the one failure mode this
# file exists to prevent. The autouse fixture closes it, so the same rows are a
# real gate under pytest and a full report under python.
try:  # pytest is not needed for the `python3 test_rows.py` path
    import pytest as _pytest
except ImportError:  # pragma: no cover - exercised by the CLI path
    _pytest = None

if _pytest is not None:

    @_pytest.fixture(autouse=True)
    def _no_row_may_fail():
        before = len(FAILURES)
        yield
        new_failures = FAILURES[before:]
        assert not new_failures, "failing rows: " + "; ".join(new_failures)


# --- S11, as the fixed product answers it -----------------------------------
S11_STATUS = {
    "phase": "Failed",
    "reason": "BrokerUnreachable",
    "message": "connection.authenticated: all-topics metadata reported BrokerUnreachable.",
    "jobRef": {"name": "lwc-td-2298bc38fca9898e4fd7"},
}
S11_JOB_COMPLETE = [
    {"type": "SuccessCriteriaMet", "status": "True", "reason": "CompletionsReached"},
    {"type": "Complete", "status": "True", "reason": "CompletionsReached"},
]
S11_RELAYED = {
    "contract": "logweir.dev/check-result/v1",
    "kind": "topicInventory",
    "checks": [{
        "id": "connection.authenticated",
        "code": "BrokerUnreachable",
        "gating": "blocking",
        "state": "notReady",
        "authority": "checkJob",
    }],
}


def test_s11_judges_the_reachable_form_of_the_fifth_criterion() -> None:
    criteria = d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, S11_RELAYED, [])
    row("S11: all five criteria met by a Job that COMPLETED",
        all(criteria.values()), json.dumps(criteria))
    row("MUTANT: the pre-fix criterion — a Job with no terminal condition at all",
        not all(d2.s11_criteria(S11_STATUS, [], S11_RELAYED, []).values()))
    disagree = json.loads(json.dumps(S11_RELAYED))
    disagree["checks"][0]["code"] = "MetadataTimeout"
    row("MUTANT: the object's reason and the relayed frame disagreeing is refused",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, disagree, []).values()))
    advisory = json.loads(json.dumps(S11_RELAYED))
    advisory["checks"][0]["gating"] = "advisory"
    row("MUTANT: an advisory notReady cannot carry a blocking classification",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, advisory, []).values()))
    row("MUTANT: no relayed frame at all — the controller guessed",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, None, []).values()))
    row("MUTANT: a chunk ConfigMap written by a run that failed",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, S11_RELAYED,
                                ["td-timeout-r0-0"]).values()))
    succeeded = dict(S11_STATUS, phase="Succeeded")
    row("MUTANT: an unreachable broker that ended Succeeded",
        not all(d2.s11_criteria(succeeded, S11_JOB_COMPLETE, S11_RELAYED, []).values()))
    # §14.4 S11 as amended at `ce69be4` says the check Job is `Complete`. A
    # FAILED Job on this fixture is the runner going back to failing the Job on
    # the Kafka-timeout path — the pre-W9 behaviour the amendment describes as
    # past. It used to be asserted here as a POSITIVE (review L-2).
    row("MUTANT: a FAILED check Job on the Kafka-timeout fixture",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
            S11_RELAYED, []).values()))
    row("MUTANT: SuccessCriteriaMet alone is not Complete",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "SuccessCriteriaMet", "status": "True", "reason": "CompletionsReached"}],
            S11_RELAYED, []).values()))
    row("MUTANT: a Complete condition that is not True",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "Complete", "status": "False", "reason": "CompletionsReached"}],
            S11_RELAYED, []).values()))


# --- S1.statusVerification, as the fixed product answers it -----------------
NOT_ATTEMPTED = {
    "result": "NotAttempted",
    "detail": ("BackupDestination lw-d2w14/dest-a reads evidence with a grant only a pod may "
               "hold (D2 §3.9's evidence-fetch Job)"),
    "payloadType": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    "verifiedAt": "2026-09-18T18:47:10Z",
}


def test_not_attempted_is_written_and_says_why() -> None:
    row("the block exists, names the verdict and the reason, and matched no key",
        all(d2.not_attempted_is_honest(NOT_ATTEMPTED, []).values()))
    row("MUTANT: the pre-fix shape — no verification block at all",
        not all(d2.not_attempted_is_honest(None, []).values()))
    row("MUTANT: NotAttempted with nothing said about why",
        not all(d2.not_attempted_is_honest(dict(NOT_ATTEMPTED, detail=""), []).values()))
    row("MUTANT: a verdict outside the three published ones",
        not all(d2.not_attempted_is_honest(dict(NOT_ATTEMPTED, result="Unknown"), []).values()))
    row("MUTANT: nothing was verified, yet a key is named",
        not all(d2.not_attempted_is_honest(
            dict(NOT_ATTEMPTED, matchedKeyId="2c76e22f"), []).values()))
    row("MUTANT: the premise moved — the policy now allows a controller identity",
        not all(d2.not_attempted_is_honest(
            NOT_ATTEMPTED, ["s3://lw-a/logweir/"]).values()))
    valid = {"result": "Valid", "matchedKeyId": "2c76e22f", "verifiedAt": "2026-09-18T18:47:10Z"}
    row("MUTANT: a green Valid where nobody could read the evidence",
        not all(d2.not_attempted_is_honest(valid, []).values()))


# --- S7's baseline: a broker that has stopped moving -------------------------
_BULK = pathlib.Path(__file__).resolve().parent / "bulk_topics.py"
_spec = importlib.util.spec_from_file_location("bulk_topics", _BULK)
bulk_topics = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bulk_topics)


class _Broker:
    """A broker whose metadata catches up over several polls, like a real one."""

    def __init__(self, counts: list[int]) -> None:
        self.counts = counts
        self.polls = 0

    def topics(self):
        n = self.counts[min(self.polls, len(self.counts) - 1)]
        self.polls += 1
        return [(f"bulk-{i:05d}", False, 1) for i in range(n)]


def _converge(counts: list[int], **over):
    args = dict(stable_for=0.05, timeout=5.0, interval=0.0)
    args.update(over)
    return bulk_topics.converged_topics(_Broker(counts), **args)


def test_the_baseline_waits_for_the_broker_to_converge() -> None:
    rows, settle = _converge([503, 1500, 3004, 5000, 5000, 5000, 5000, 5000])
    row("the listing is the settled count, not the first snapshot",
        len(rows) == 5000 and settle["converged"], f"{len(rows)} {settle}")
    row("and the counts it walked through are recorded",
        settle["countsSeen"][0] == 503 and settle["countsSeen"][-1] == 5000,
        str(settle["countsSeen"]))
    rows, settle = _converge([5000] * 6)
    row("a broker that was already still converges immediately",
        len(rows) == 5000 and settle["converged"])
    # THE DEFECT THIS EXISTS FOR: one snapshot of a broker mid-creation.
    rows, settle = _converge([503])
    row("MUTANT: a single snapshot is only a baseline once it has held still",
        settle["converged"] and len(rows) == 503,
        "a constant count is legitimately converged; the guard is the timeout below")
    growing = list(range(500, 20000, 100))
    rows, settle = _converge(growing, timeout=0.3)
    row("MUTANT: a count still moving when the budget runs out does NOT converge",
        not settle["converged"], str(settle))
    row("and the caller can tell, because `converged` is False and the counts moved",
        len(settle["countsSeen"]) > 1)


# --- PLAT-03.2's `expired approval`, after the preflight fix ------------------
GREEN = {"state": "ready", "code": "ApprovalVerified", "gating": "blocking",
         "message": "the Approval is Verified against a rostered approver key"}
EXPIRED = {"state": "notReady", "code": "ApprovalExpired", "gating": "blocking",
           "message": "the approver key that verified this approval has expired"}
APPROVAL_EXPIRED = {"verified": False, "reason": "KeyIdExpired",
                    "matchedKeyId": "a16169914cf8"}
NOT_ADMITTED = {"type": "Admitted", "status": "False", "reason": "ApprovalNotVerified"}


def _s18(**over):
    args = dict(green=GREEN, expired=EXPIRED, approval_after=APPROVAL_EXPIRED,
                restore_jobs=[], admitted=NOT_ADMITTED)
    args.update(over)
    return all(d2.approval_expiry_is_relayed(**args).values())


def test_an_expired_approver_key_stops_a_previously_green_restore() -> None:
    row("green before, ApprovalExpired after, and no Job for the green plan", _s18())
    row("MUTANT: the preview was never green — the row would be about nothing",
        not _s18(green={"state": "notReady", "code": "ApprovalPending",
                        "gating": "blocking"}))
    row("MUTANT: the Approval kept its verdict, so nothing expired",
        not _s18(approval_after={"verified": True, "reason": "ApprovalVerified"}))
    row("MUTANT: Verified=False for another reason entirely",
        not _s18(approval_after={"verified": False, "reason": "SignatureInvalid"}))
    row("MUTANT: THE PRE-FIX ANSWER — the preflight recomputes and says NotVerified",
        not _s18(expired={"state": "notReady", "code": "ApprovalNotVerified",
                          "gating": "blocking", "message": "x"}))
    row("MUTANT: expired but ADVISORY, so the aggregate could still be ready",
        not _s18(expired=dict(EXPIRED, gating="advisory")))
    row("MUTANT: expired with no sentence naming the key",
        not _s18(expired=dict(EXPIRED, message="   ")))
    row("MUTANT: THE RESTORE STARTED ANYWAY on the previously green preview",
        not _s18(restore_jobs=["rs-expired-runner"]))
    row("MUTANT: the Restore was admitted",
        not _s18(admitted={"type": "Admitted", "status": "True", "reason": "Admitted"}))


# --- the revision guard's untracked boundary (lab-refresh-6 §15) -------------
def test_the_revision_guard_ignores_only_untracked_non_inputs() -> None:
    row("the orchestrator's own scratch file at the root is ignorable",
        d2.untracked_is_ignorable("prompt") and d2.untracked_is_ignorable("notes.txt"))
    row("the harness trees, their fixtures and prose are ignorable",
        all(d2.untracked_is_ignorable(p) for p in
            ("scripts/live/d1/x.py", "scripts/fixtures/y.py", "e2e/k8s/d2/z.json",
             "docs/note.md")))
    row("MUTANT: an untracked Rust source is compiled into the image",
        not d2.untracked_is_ignorable("crates/weirkeeper/src/zz.rs"))
    row("MUTANT: an untracked Cargo.lock decides what the image was built from",
        not d2.untracked_is_ignorable("Cargo.lock")
        and not d2.untracked_is_ignorable("rust-toolchain.toml")
        and not d2.untracked_is_ignorable(".cargo/config.toml"))
    row("MUTANT: shipped artifacts a lab is equally built from",
        not d2.untracked_is_ignorable("charts/logweir/values.yaml")
        and not d2.untracked_is_ignorable("config/crd/backups.yaml")
        and not d2.untracked_is_ignorable("ui/src/app.ts"))
    row("MUTANT: FAIL CLOSED — a directory the rule has never heard of",
        not d2.untracked_is_ignorable("scripts/check-image.sh")
        and not d2.untracked_is_ignorable("newdir/x.rs"))
    row("the rule is the same one the D1 fence applies",
        d2.IMAGE_BUILD_INPUTS[:5] == ("crates/", "Cargo.toml", "Cargo.lock",
                                      "rust-toolchain.toml", ".cargo/"))


# --- U6: a measured per-role row, and what makes it a measurement -----------
#
# `u6_row_is_proved` is the whole of the D2 §15 U6 claim: a role's minimal set
# is what it is only if the starting set ran, every unit came out exactly once,
# every required unit's removal failed WITH THE PRODUCT'S OWN denial, every
# unit left out really did work without it, and the minimal set was itself
# executed. Each clause below is made to say False.

def _denial(code: str = "AccessDenied") -> dict[str, Any]:
    return {"destination.archiveListable": {"state": "notReady", "code": code}}


def _removal(unit: str, ok: bool, classified: Any = None, unclassified: bool = False):
    return {"unit": unit, "ok": ok, "unclassified": unclassified,
            "classified": ({} if ok else _denial()) if classified is None else classified}


U6_STARTING = ["s3:ListBucket@bucket:archive", "s3:GetObject@archive",
               "s3:GetBucketLocation@bucket"]
U6_BASELINE = {"ok": True, "object": "u6-da-001", "classified": {}}
U6_REMOVALS = [
    _removal("s3:ListBucket@bucket:archive", False),
    _removal("s3:GetObject@archive", False),
    _removal("s3:GetBucketLocation@bucket", True),
]
U6_MINIMAL = ["s3:ListBucket@bucket:archive", "s3:GetObject@archive"]
U6_CONFIRM = {"ok": True, "object": "u6-da-005", "classified": {}}


def _proved(**over) -> bool:
    args = dict(role="archive-read", baseline=U6_BASELINE, removals=U6_REMOVALS,
                minimal=U6_MINIMAL, confirm=U6_CONFIRM)
    args.update(over)
    return all(d2.u6_row_is_proved(**args).values())


def test_a_measured_minimal_set_is_proved_only_by_what_was_run() -> None:
    row("a starting set that worked, one removal each, and a minimal set that ran",
        _proved())
    row("MUTANT: the RECORDED starting set never worked, so there was nothing to bisect",
        not _proved(baseline={"ok": False, "classified": _denial()}))
    row("MUTANT: a required unit whose removal produced no product verdict at all",
        not _proved(removals=[
            _removal("s3:ListBucket@bucket:archive", False,
                     {"destination.archiveListable": {"state": None, "code": None}},
                     unclassified=True),
            U6_REMOVALS[1], U6_REMOVALS[2]]))
    row("MUTANT: a required unit that failed for a reason that is not a denial",
        not _proved(removals=[
            _removal("s3:ListBucket@bucket:archive", False, _denial("BrokerUnreachable")),
            U6_REMOVALS[1], U6_REMOVALS[2]]))
    row("MUTANT: THE RECORDED-NOT-MEASURED SHAPE — a minimal set asserted with no removals",
        not _proved(removals=[], minimal=U6_STARTING))
    row("MUTANT: the minimal set keeps a unit whose removal SUCCEEDED",
        not _proved(minimal=U6_STARTING))
    row("MUTANT: the minimal set drops a unit whose removal FAILED",
        not _proved(minimal=["s3:ListBucket@bucket:archive"]))
    row("MUTANT: no unit is required — the role needs none of them, which is not a measurement",
        not _proved(removals=[_removal(u, True) for u in U6_STARTING], minimal=[]))
    row("MUTANT: one unit removed twice and another never removed",
        not _proved(removals=[U6_REMOVALS[0], U6_REMOVALS[0], U6_REMOVALS[1]]))
    row("MUTANT: the measured minimal set was never executed as a whole",
        not _proved(confirm={"ok": False, "classified": _denial()}))
    row("the minimal set needs no separate confirmation when nothing was dropped",
        _proved(removals=[_removal(u, False) for u in U6_STARTING],
                minimal=list(U6_STARTING), confirm=None))
    row("MUTANT: nothing was dropped, yet the starting set is reported as not working",
        not _proved(baseline={"ok": False, "classified": _denial()},
                    removals=[_removal(u, False) for u in U6_STARTING],
                    minimal=list(U6_STARTING), confirm=None))


def test_a_grant_unit_names_an_action_and_a_resource_scope() -> None:
    listing = d2.u6_statement("s3:ListBucket@bucket:archive")
    row("`s3:ListBucket` is authorised on the BUCKET arn, under an `s3:prefix` condition",
        listing["Resource"] == ["arn:aws:s3:::lw-u6"]
        and listing["Condition"]["StringLike"]["s3:prefix"] == ["team/u6/*"],
        json.dumps(listing))
    getting = d2.u6_statement("s3:GetObject@archive")
    row("`s3:GetObject` is authorised on the OBJECT arn, and carries no prefix condition",
        getting["Resource"] == ["arn:aws:s3:::lw-u6/team/u6/*"] and "Condition" not in getting,
        json.dumps(getting))
    evidence = d2.u6_statement("s3:PutObject@evidence")
    row("the evidence root is a different scope from the archive prefix",
        evidence["Resource"] == ["arn:aws:s3:::lw-u6/logweir/*"])
    location = d2.u6_statement("s3:GetBucketLocation@bucket")
    row("MinIO refuses an `s3:prefix` condition on `s3:GetBucketLocation`, so it has none",
        "Condition" not in location and location["Resource"] == ["arn:aws:s3:::lw-u6"])
    both = d2.u6_statement("s3:ListBucket@bucket:both")
    row("a writer lists under BOTH roots in one condition",
        both["Condition"]["StringLike"]["s3:prefix"] == ["team/u6/*", "logweir/*"])
    try:
        d2.u6_statement("s3:GetObject@somewhere-else")
        unknown_refused = False
    except ValueError:
        unknown_refused = True
    row("MUTANT: FAIL CLOSED — a scope the units vocabulary has never heard of",
        unknown_refused)


def _rows(*pairs) -> dict[str, Any]:
    return {rid: {"state": state, "code": code} for rid, state, code in pairs}


def test_a_row_the_check_never_reached_is_not_a_failed_measurement() -> None:
    row("a denial, with the row after it honestly BlockedByPrerequisite, is answered",
        not d2.u6_unanswered(_rows(
            ("archive.backupSet", "notReady", "AccessDenied"),
            ("archive.segments", "unknown", "BlockedByPrerequisite")), ok=False))
    row("a plain denial is answered",
        not d2.u6_unanswered(
            _rows(("destination.archiveListable", "notReady", "AccessDenied")), ok=False))
    row("a check that passed is never unanswered",
        not d2.u6_unanswered(
            _rows(("destination.archiveListable", "ready", "ArchiveListable")), ok=True))
    row("MUTANT: the pod never started, so nothing was measured about the grant",
        d2.u6_unanswered(_rows(
            ("archive.backupSet", "unknown", "PodNotStarted"),
            ("archive.segments", "unknown", "BlockedByPrerequisite")), ok=False))
    row("MUTANT: the row was not in the result at all",
        d2.u6_unanswered(_rows(("archive.backupSet", None, None)), ok=False))
    row("MUTANT: a notReady row with no code is not a classification",
        d2.u6_unanswered(_rows(("archive.backupSet", "notReady", None)), ok=False))


SINCE = "2026-09-21T17:58:41Z"
AFTER = "2026-09-21T17:58:50Z"


def _cat(*conditions, pages=None, synced_at=None, token="u6"):
    status = {"observedSyncRequest": token,
              "conditions": [{"type": t, "status": st, "reason": r,
                              "lastTransitionTime": AFTER} for t, st, r in conditions]}
    if pages is not None:
        status["pages"] = pages
    if synced_at is not None:
        status["syncedAt"] = synced_at
    return status


def test_a_catalog_sync_has_answered_only_when_it_has_published_or_failed() -> None:
    # THE RECORDED SHAPE, nine seconds after the Job was created: no view, no
    # failure, and a `Stale=False/ViewFresh` that used to be read as an answer.
    row("MUTANT: `Stale=False` means NOT stale, and is not a sync verdict",
        not d2.u6_catalog_settled(_cat(
            ("Ready", "Unknown", "NeverSynced"),
            ("Synced", "Unknown", "SyncInProgress"),
            ("Stale", "False", "ViewFresh"),
            ("TrustAvailable", "True", "TrustMaterialPresent")), SINCE, "u6"))
    row("a published view for this token, after this walk started, is an answer",
        d2.u6_catalog_settled(_cat(("Synced", "True", "ViewPublished"),
                                   pages=[{"index": 0}], synced_at=AFTER), SINCE, "u6"))
    row("a `Synced=False` verdict for this walk is an answer",
        d2.u6_catalog_settled(_cat(("Synced", "False", "AccessDenied")), SINCE, "u6"))
    row("a `Ready=False` verdict is an answer too",
        d2.u6_catalog_settled(_cat(("Ready", "False", "ViewUnreadable")), SINCE, "u6"))
    row("MUTANT: the controller has not even observed this syncRequest yet",
        not d2.u6_catalog_settled(_cat(("Synced", "False", "AccessDenied"),
                                       token="previous"), SINCE, "u6"))
    row("MUTANT: the PREVIOUS walk's view, republished before this one started",
        not d2.u6_catalog_settled(_cat(("Synced", "True", "ViewPublished"),
                                       pages=[{"index": 0}],
                                       synced_at="2026-09-21T17:00:00Z"), SINCE, "u6"))
    row("MUTANT: a verdict that transitioned before this walk started",
        not d2.u6_catalog_settled(
            {"observedSyncRequest": "u6",
             "conditions": [{"type": "Synced", "status": "False", "reason": "AccessDenied",
                             "lastTransitionTime": "2026-09-21T17:00:00Z"}]},
            SINCE, "u6"))
    row("MUTANT: pages with no `syncedAt` at all",
        not d2.u6_catalog_settled(_cat(("Synced", "True", "ViewPublished"),
                                       pages=[{"index": 0}]), SINCE, "u6"))


# One real `catalogSync` frame set, from `lwc-cs-80d65db5d3363255802b` on
# 2026-09-21: the sentence that names the refusal exists ONLY here, because the
# controller publishes a verdict from these frames and not the rows.
FRAME_1 = 'eyJjb250cmFjdCI6ICJsb2d3ZWlyLmRldi9jaGVjay1yZXN1bHQvdjEiLCAia2luZCI6ICJjYXRhbG9nU3luYyIsICJjaGVja3MiOiBbeyJpZCI6ICJydW5uZXIuY29udHJhY3QiLCAic3RhdGUiOiAicmVhZHkiLCAiY29kZSI6ICJDb250cmFjdFN1cHBvcnRlZCIsICJtZXNzYWdlIjogInRoaXMgcnVubmVyIGltcGxlbWVudHMgY2hlY2sgY29u'
FRAME_2 = 'dHJhY3QgdmVyc2lvbiAxIn0sIHsiaWQiOiAiZGVzdGluYXRpb24uYXJjaGl2ZUxpc3RhYmxlIiwgInN0YXRlIjogIm5vdFJlYWR5IiwgImNvZGUiOiAiQWNjZXNzRGVuaWVkIiwgIm1lc3NhZ2UiOiAidGhlIGR1cmFibGUgcmVjb3ZlcnkgY2F0YWxvZyB1bmRlciBgbG9nd2Vpci9jYXRhbG9nL3YxL2AgY291bGQgbm90IGJlIGxpc3RlZCJ9XX0='
RELAY = ("--- pod ---\n"
         "logweir-check-part=result:1/2:" + FRAME_1 + "\n"
         "logweir-check-part=result:2/2:" + FRAME_2 + "\n")


def test_the_sentence_that_names_the_refusal_is_decoded_from_the_job() -> None:
    rows = d2.u6_relayed_checks(RELAY)
    denial = [r for r in rows if r["state"] == "notReady"]
    row("the relayed rows decode, and one of them is the store's own AccessDenied",
        len(rows) == 2 and len(denial) == 1
        and denial[0]["id"] == "destination.archiveListable"
        and denial[0]["code"] == "AccessDenied", json.dumps(rows))
    row("MUTANT: a frame missing from the set is not a verdict",
        d2.u6_relayed_checks("logweir-check-part=result:1/2:" + FRAME_1 + "\n") == [])
    row("MUTANT: a stream that is not `result`",
        d2.u6_relayed_checks(RELAY.replace("result:", "details:")) == [])
    row("MUTANT: frames that do not decode",
        d2.u6_relayed_checks("logweir-check-part=result:1/1:!!!not-base64!!!\n") == [])
    row("MUTANT: a pod that printed no frame at all",
        d2.u6_relayed_checks("--- pod ---\nno frames here\n") == [])


def test_a_catalog_records_the_sentence_it_did_say() -> None:
    row("a recorded refusal is the product's own denial",
        d2.u6_denied({"deniedBy": "`Synced=False/PartialScan`, with 13 of 13 points unreadable",
                      "counts": {"unreadable": 13, "total": 13}}))
    row("so is a relayed AccessDenied",
        d2.u6_denied({"deniedBy": "the sync Job relayed `destination.archiveListable` "
                                  "AccessDenied"}))
    row("MUTANT: a sync that simply did not finish records no refusal",
        not d2.u6_denied({"deniedBy": "", "timedOut": True, "counts": {}, "conditions": []}))
    row("MUTANT: an empty `deniedBy` is not a denial however much else is recorded",
        not d2.u6_denied({"deniedBy": "", "counts": {"total": 13, "available": 13},
                          "conditions": [{"type": "Stale", "status": "False",
                                          "reason": "ViewFresh"}]}))


def test_only_the_products_own_denial_counts_as_a_denial() -> None:
    row("a check row classified `AccessDenied`",
        d2.u6_denied({"destination.archiveListable": {"state": "notReady",
                                                      "code": "AccessDenied"}}))
    row("a runner that printed its own `Access Denied`",
        d2.u6_denied({"exitCode": 1, "runnerLogTail": "... S3 error: Access Denied ..."}))
    row("credentials the backend rejected outright",
        d2.u6_denied({"code": "InvalidCredentials"}))
    row("MUTANT: a check that never answered",
        not d2.u6_denied({"destination.archiveListable": {"state": None, "code": None}}))
    row("MUTANT: a broker timeout is not an object-store denial",
        not d2.u6_denied({"phase": "Failed", "reason": "MetadataTimeout",
                          "runnerLogTail": "connection timed out"}))
    row("MUTANT: a catalog sync that simply ran out of time",
        not d2.u6_denied({"counts": {}, "pages": 0, "timedOut": True, "conditions": []}))


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
