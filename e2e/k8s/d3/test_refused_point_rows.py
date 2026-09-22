#!/usr/bin/env python3
"""Unit rows for lab-refresh-8's refused-point rows and rehearsal row 7b.

    python3 e2e/k8s/d3/test_refused_point_rows.py

No cluster. `d3_live.refused_point` measures verdict-precedence §5 rows 1, 2
and 4 and fix-standing-verify rows 11 and 12 over ONE fixture, and
`rehearsal_row_7b` measures verdict-precedence §5 row 3. Every predicate they
judge with is driven here over the shape the fixed controllers write, and over
the shape each defect produced — a stale row that still counts as usable, a
`selectable: true` beside a refused Backup, a skip that defers its slot — which
must be REFUSED. A predicate that cannot be made to say False is not a row.
"""

from __future__ import annotations

import copy
import json
import os
import pathlib
import re
import sys
import tempfile
from typing import Any

_TMP = tempfile.mkdtemp(prefix="d3-refused-rows-")
os.environ.setdefault("LOGWEIR_D3_OUT", _TMP)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d3_live as d3  # noqa: E402

_REPO = pathlib.Path(__file__).resolve().parents[3]
FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def refused(clauses: dict[str, bool]) -> bool:
    return not all(clauses.values())


# --- the fixture, as the fixed controllers write it ---------------------------

KEY = "7e0a5b4d" * 8
DIGEST = {n: "sha256:" + c * 64 for n, c in (("rp-1", "1"), ("rp-2", "2"), ("rp-3", "3"),
                                                ("rp-4", "4"))}
PID = {n: f"lwp1-{d[len('sha256:'):][:32]}" for n, d in DIGEST.items()}


def _backup(name: str, result: str, *, attempt: int | None = None,
            retry: str | None = None, detail: str = "", matched: str | None = None,
            facts: bool = True, schedule: str | None = None) -> dict[str, Any]:
    ver: dict[str, Any] = {"result": result, "verifiedAt": "2026-09-22T20:10:00Z"}
    if detail:
        ver["detail"] = detail
    if matched:
        ver["matchedKeyId"] = matched
    status: dict[str, Any] = {
        "phase": "Succeeded", "exitCode": 0, "backupId": f"set-{name}",
        "evidence": {"receiptKey": f"logweir/backups/set-{name}/01K.receipt.json",
                     "receiptSha256": DIGEST[name], "verification": ver},
        "conditions": [{"type": "Verified", "status": "True" if result == "Valid" else "False"}],
    }
    if attempt is not None:
        status["evidence"]["observation"] = {"mode": "SecretKeys", "attempt": attempt,
                                             "retryAfter": retry,
                                             "jobRef": {"name": f"lwc-ev-{name}"}}
    if facts:
        status["windowCovered"] = {"fromMs": 1, "toMs": 2}
        status["records"] = {"orders": 10}
        status["capture"] = {"startedAt": "2026-09-22T20:00:00Z"}
    spec: dict[str, Any] = {"topics": ["orders", "payments"]}
    if schedule:
        spec["scheduleRef"] = {"name": schedule, "uid": "sched-uid"}
    return {"metadata": {"name": name}, "spec": spec, "status": status}


NA_DETAIL = ("evidence-fetch Job ns/lwc-ev-x (attempt 1 of 4) ended without a verified relay: "
             "CredentialSecretKeyMissing: …; attempt 2 starts at 2026-09-22T20:11:00Z")
POINTS = {
    "rp-1": _backup("rp-1", "Valid", attempt=1, matched="lab"),
    "rp-2": _backup("rp-2", "Valid", attempt=1, matched="lab"),
    "rp-4": _backup("rp-4", "Valid", attempt=1, matched=KEY, schedule=d3.RP_REVOKED_SCHEDULE),
    "rp-3": _backup("rp-3", "NotAttempted", attempt=1, retry="2026-09-22T20:11:00Z",
                    detail=NA_DETAIL, facts=False, schedule=d3.RP_REFUSED_SCHEDULE),
}


def _entry(name: str, at: int) -> dict[str, Any]:
    return {"pointId": PID[name], "backupId": f"set-{name}", "receiptSha256": DIGEST[name],
            "recoveryPointAtMs": at, "availability": "Available", "verification": "Verified",
            "selectable": True}


ENTRIES = {"rp-1": _entry("rp-1", 1000), "rp-2": _entry("rp-2", 2000),
           "rp-4": _entry("rp-4", 3000), "rp-3": _entry("rp-3", 4000)}


def test_the_fixture_is_the_state_the_rows_are_about() -> None:
    ok = d3.refused_point_fixture_is_real(POINTS, ENTRIES, KEY)
    row("fixture: two sound points, one under the minted key, the newest NotAttempted with a "
        "retry owed, all four offered by the view", all(ok.values()), json.dumps(ok))
    spent = copy.deepcopy(POINTS)
    spent["rp-3"]["status"]["evidence"]["observation"].update(attempt=4, retryAfter=None)
    row("MUTANT: rp-3's fetch has no attempt left — the replacement would never be read",
        refused(d3.refused_point_fixture_is_real(spent, ENTRIES, KEY)))
    read = copy.deepcopy(POINTS)
    read["rp-3"] = _backup("rp-3", "Valid", attempt=1, matched="lab")
    row("MUTANT: rp-3 was already read — the broken grant did not break",
        refused(d3.refused_point_fixture_is_real(read, ENTRIES, KEY)))
    row("MUTANT: rp-4 verified under the lab key, not the minted one row 12 revokes",
        refused(d3.refused_point_fixture_is_real(POINTS, ENTRIES, "another-key")))
    order = copy.deepcopy(ENTRIES)
    order["rp-3"]["recoveryPointAtMs"] = 500
    row("MUTANT: the refused point is not the newest — retention would not promote anything",
        refused(d3.refused_point_fixture_is_real(POINTS, order, KEY)))
    missing = {k: v for k, v in ENTRIES.items() if k != "rp-3"}
    row("MUTANT: the view never harvested rp-3", refused(
        d3.refused_point_fixture_is_real(POINTS, missing, KEY)))
    rehash = copy.deepcopy(ENTRIES)
    rehash["rp-3"]["receiptSha256"] = "sha256:" + "f" * 64
    row("MUTANT: a row keyed by another digest — the join would never find it",
        refused(d3.refused_point_fixture_is_real(POINTS, rehash, KEY)))


def test_the_fault_waits_for_the_attempt_the_controller_still_owes() -> None:
    at = d3.slot_epoch("20260922-201000")
    row("a scheduled retry 60 s away is waited for, plus one attempt's budget",
        d3.read_again_within(POINTS["rp-3"], now_epoch=at) == 60 + d3.VERDICT_SETTLE_SECONDS)
    pending3 = _backup("rp-3", "Pending", attempt=3, facts=False)
    row("an attempt 3 in flight may fail and schedule +15 m — that is waited for too",
        d3.read_again_within(pending3, now_epoch=at) >= 900 + d3.VERDICT_SETTLE_SECONDS)
    row("and every wait is bounded by the whole retry schedule",
        d3.read_again_within(pending3, now_epoch=at) <= 1500)
    row("a scheduled retry is a read still owed", d3.fetch_will_read_again(POINTS["rp-3"]))
    row("so is an attempt in flight", d3.fetch_will_read_again(pending3))
    spent = _backup("rp-3", "NotAttempted", attempt=4, facts=False)
    row("MUTANT: attempts spent — nothing will read the replacement (HARNESS-FAULT, never "
        "a product verdict)", not d3.fetch_will_read_again(spent))
    row("MUTANT: attempt 4 in flight has no retry behind it",
        not d3.fetch_will_read_again(_backup("rp-3", "Pending", attempt=4, facts=False)))


REPLACED = "sha256:" + "9" * 64
INVALID = _backup("rp-3", "Invalid", attempt=3, facts=False, schedule=d3.RP_REFUSED_SCHEDULE,
                  detail="the relayed receipt hashes to sha256:99…, the runner reported sha256:33…")


def test_row_eleven_a_replaced_receipt_is_invalid_and_projects_nothing() -> None:
    ok = d3.replaced_receipt_is_invalid(POINTS["rp-3"], INVALID, ENTRIES["rp-3"], REPLACED)
    row("row 11: Invalid on a retry, the runner's digest kept, no window/records/capture, "
        "the stale row still offering it", all(ok.values()), json.dumps(ok))
    windowed = copy.deepcopy(INVALID)
    windowed["status"]["windowCovered"] = {"fromMs": 1, "toMs": 2}
    row("MUTANT M27 (fix-standing LOW-1): a window projected from bytes that fail the digest",
        refused(d3.replaced_receipt_is_invalid(POINTS["rp-3"], windowed, ENTRIES["rp-3"],
                                               REPLACED)))
    selfhash = copy.deepcopy(INVALID)
    selfhash["status"]["evidence"]["receiptSha256"] = REPLACED
    row("MUTANT: the controller published the replacement's own digest",
        refused(d3.replaced_receipt_is_invalid(POINTS["rp-3"], selfhash, ENTRIES["rp-3"],
                                               REPLACED)))
    valid = _backup("rp-3", "Valid", attempt=3, matched="lab")
    row("MUTANT: a self-hashed replacement that came back Valid",
        refused(d3.replaced_receipt_is_invalid(POINTS["rp-3"], valid, ENTRIES["rp-3"],
                                               REPLACED)))
    na = _backup("rp-3", "NotAttempted", attempt=4, facts=False)
    row("MUTANT: the retry never read anything — still NotAttempted",
        refused(d3.replaced_receipt_is_invalid(POINTS["rp-3"], na, ENTRIES["rp-3"], REPLACED)))
    fresh_row = dict(ENTRIES["rp-3"], verification="Invalid", selectable=False)
    row("PREMISE: a view re-harvested after the replacement is not the stale row the rows need",
        refused(d3.replaced_receipt_is_invalid(POINTS["rp-3"], INVALID, fresh_row, REPLACED)))


UNTRUSTED = _backup("rp-4", "Untrusted", attempt=1, matched=KEY, facts=True,
                    schedule=d3.RP_REVOKED_SCHEDULE)


def test_row_twelve_a_revoked_signer_is_untrusted() -> None:
    ok = d3.revoked_signer_is_untrusted(POINTS["rp-4"], UNTRUSTED, ENTRIES["rp-4"], KEY)
    row("row 12: Valid -> Untrusted about the same key, the run untouched, the stale row "
        "still offering it", all(ok.values()), json.dumps(ok))
    row("MUTANT: the revocation never reached the terminal Backup", refused(
        d3.revoked_signer_is_untrusted(POINTS["rp-4"], POINTS["rp-4"], ENTRIES["rp-4"], KEY)))
    other = _backup("rp-4", "Untrusted", matched="someone-else")
    row("MUTANT: Untrusted about a different key", refused(
        d3.revoked_signer_is_untrusted(POINTS["rp-4"], other, ENTRIES["rp-4"], KEY)))


def _ev(kept, candidates, skipped=(), plan="sha256:p0"):
    return {"kept": list(kept),
            "candidates": [{"pointId": c, "reason": "BeyondKeepLast"} for c in candidates],
            "skipped": [{"pointId": p, "reason": r} for p, r in skipped],
            "protected": [], "planSha256": plan}


CONTROL_EVAL = _ev([PID["rp-3"]], [PID["rp-1"], PID["rp-2"], PID["rp-4"]], plan="sha256:c")
AFTER_EVAL = _ev([PID["rp-4"]], [PID["rp-1"], PID["rp-2"]],
                 [(PID["rp-3"], "Unreadable")], plan="sha256:a")
# RETENTION-PLAN-IGNORES-REFUSED-VERDICT: the stale row still counts, so the
# refused point keeps the keepLast rank and the good one is planned for deletion.
DEFECT_EVAL = copy.deepcopy(CONTROL_EVAL)


def test_retention_skips_the_refused_newest_point() -> None:
    ok = d3.refused_point_is_skipped_by_retention(CONTROL_EVAL, AFTER_EVAL, PID["rp-3"],
                                                  PID["rp-4"])
    row("VP row 1: the refused newest is skipped Unreadable and the next good point is kept",
        all(ok.values()), json.dumps(ok))
    row("MUTANT (the defect): the refused point still kept, the good one still a candidate",
        refused(d3.refused_point_is_skipped_by_retention(CONTROL_EVAL, DEFECT_EVAL,
                                                         PID["rp-3"], PID["rp-4"])))
    both = _ev([], [PID["rp-1"], PID["rp-2"], PID["rp-4"]], [(PID["rp-3"], "Unreadable")],
               plan="sha256:b")
    row("MUTANT: skipped, but the good point still planned for deletion", refused(
        d3.refused_point_is_skipped_by_retention(CONTROL_EVAL, both, PID["rp-3"], PID["rp-4"])))
    invented = _ev([PID["rp-4"]], [PID["rp-1"], PID["rp-2"]], [(PID["rp-3"], "Refused")],
                   plan="sha256:a")
    row("MUTANT: a skip reason outside the closed vocabulary", refused(
        d3.refused_point_is_skipped_by_retention(CONTROL_EVAL, invented, PID["rp-3"],
                                                 PID["rp-4"])))
    same_plan = dict(AFTER_EVAL, planSha256="sha256:c")
    row("MUTANT: the plan digest did not move", refused(
        d3.refused_point_is_skipped_by_retention(CONTROL_EVAL, same_plan, PID["rp-3"],
                                                 PID["rp-4"])))
    row("MUTANT: a control in which the point was never kept proves nothing", refused(
        d3.refused_point_is_skipped_by_retention(AFTER_EVAL, AFTER_EVAL, PID["rp-3"],
                                                 PID["rp-4"])))


def _item(name, *, selectable=True, verdict=None):
    item = {"pointId": PID[name], "availability": "Available", "verification": "Verified",
            "selectable": selectable}
    if verdict:
        item["backupVerdict"] = verdict
    return item


CONTROL_PAGE = {"items": [_item(n) for n in ("rp-3", "rp-4", "rp-2", "rp-1")]}
PAGE = {"items": [_item("rp-3", selectable=False, verdict="Invalid"), _item("rp-4"),
                  _item("rp-2"), _item("rp-1")]}
SELECTABLE = {"items": [_item("rp-4"), _item("rp-2"), _item("rp-1")]}


def test_points_list_a_refused_point_as_not_selectable() -> None:
    ok = d3.refused_point_is_not_selectable(CONTROL_PAGE, PAGE, SELECTABLE, PID["rp-3"],
                                            "Invalid", PID["rp-1"])
    row("VP row 2: selectable false with backupVerdict, omitted from ?selectable=true, a sound "
        "point beside it still selectable", all(ok.values()), json.dumps(ok))
    row("MUTANT (CATALOG-LIST-IGNORES-REFUSED-VERDICT): the row's own selectable published",
        refused(d3.refused_point_is_not_selectable(CONTROL_PAGE, CONTROL_PAGE, CONTROL_PAGE,
                                                   PID["rp-3"], "Invalid", PID["rp-1"])))
    unfiltered = {"items": PAGE["items"]}
    row("MUTANT: the filter disagrees with the row — ?selectable=true still lists it",
        refused(d3.refused_point_is_not_selectable(CONTROL_PAGE, PAGE, unfiltered,
                                                   PID["rp-3"], "Invalid", PID["rp-1"])))
    silent = {"items": [_item("rp-3", selectable=False)] + PAGE["items"][1:]}
    row("MUTANT: not selectable, and no word for why", refused(
        d3.refused_point_is_not_selectable(CONTROL_PAGE, silent, SELECTABLE, PID["rp-3"],
                                           "Invalid", PID["rp-1"])))
    everything = {"items": [_item(n, selectable=False, verdict="Invalid")
                            for n in ("rp-3", "rp-4", "rp-2", "rp-1")]}
    row("MUTANT: every row refused — a build that refuses all is not the rule", refused(
        d3.refused_point_is_not_selectable(CONTROL_PAGE, everything, {"items": []},
                                           PID["rp-3"], "Invalid", PID["rp-1"])))
    incomplete = dict(PAGE, backupVerdictsIncomplete="Truncated")
    row("MUTANT: a page whose Backup verdicts were not all read", refused(
        d3.refused_point_is_not_selectable(CONTROL_PAGE, incomplete, SELECTABLE, PID["rp-3"],
                                           "Invalid", PID["rp-1"])))
    old_shape = dict(PAGE, backupVerdictsTruncated=True)
    row("MUTANT: the superseded backupVerdictsTruncated flag", refused(
        d3.refused_point_is_not_selectable(CONTROL_PAGE, old_shape, SELECTABLE, PID["rp-3"],
                                           "Invalid", PID["rp-1"])))
    pending = {"items": [_item("rp-3", selectable=False, verdict="Pending")]
               + PAGE["items"][1:]}
    row("MUTANT (verdict-precedence M2): Pending published as a refusal", refused(
        d3.refused_point_is_not_selectable(CONTROL_PAGE, pending, SELECTABLE, PID["rp-3"],
                                           "Invalid", PID["rp-1"])))


def _schedule(reason: str | None, slot: str = "20260922-201100") -> dict[str, Any]:
    status: dict[str, Any] = {"lastScheduledSlot": slot}
    if reason:
        status["lastSkipped"] = {"slot": slot, "reason": reason}
    return {"status": status}


VERIFIED = {"status": {"conditions": [{"type": "Verified", "status": "True"}]}}
SINCE = "20260922-201030"


def test_the_rehearsal_never_selects_a_refused_point() -> None:
    verdict, ok = d3.refused_point_is_never_selected(
        _schedule("NoQualifyingPoint"), VERIFIED, [], [], ENTRIES["rp-3"], INVALID, SINCE)
    row("rows 11/12 (rehearsal): NoQualifyingPoint, no Restore, no Job — with the "
        "authorization verified and the stale row selectable",
        verdict == "PASS", f"{verdict} {json.dumps(ok)}")
    verdict, _ = d3.refused_point_is_never_selected(
        _schedule(None), VERIFIED, ["logweir-rehearsal-x-20260922-201100"],
        ["logweir-rehearsal-x-20260922-201100"], ENTRIES["rp-3"], INVALID, SINCE)
    row("MUTANT (fix-standing MEDIUM-1): the stale row overruled the refusal and the slot fired",
        verdict == "FAIL", verdict)
    verdict, _ = d3.refused_point_is_never_selected(
        _schedule("AuthorizationInvalid"), {"status": {"conditions": [
            {"type": "Verified", "status": "False"}]}}, [], [], ENTRIES["rp-3"], INVALID, SINCE)
    row("a refusal of the AUTHORIZATION is not this row's refusal — FAIL, never PASS",
        verdict == "FAIL", verdict)
    verdict, _ = d3.refused_point_is_never_selected(
        _schedule("TargetBusy"), VERIFIED, [], [], ENTRIES["rp-3"], INVALID, SINCE)
    row("TargetBusy is the harness's ordering — HARNESS-FAULT", verdict == "HARNESS-FAULT")
    verdict, _ = d3.refused_point_is_never_selected(
        _schedule("NoQualifyingPoint"), VERIFIED, [], [],
        dict(ENTRIES["rp-3"], selectable=False), INVALID, SINCE)
    row("PREMISE: a view that no longer offers the point makes the skip prove nothing — "
        "INCONCLUSIVE", verdict == "INCONCLUSIVE", verdict)
    verdict, _ = d3.refused_point_is_never_selected(
        _schedule("NoQualifyingPoint", "20260922-200000"), VERIFIED, [], [],
        ENTRIES["rp-3"], INVALID, SINCE)
    row("MUTANT: a skip recorded before the arm was unsuspended is not this run's",
        verdict == "FAIL", verdict)


def _obs(at, *, active, skip_slot=None, scheduled=None, reason="ConcurrencyBlocked"):
    return {"at": at, "active": active,
            "skip": {"slot": skip_slot, "reason": reason} if skip_slot else None,
            "lastScheduledSlot": scheduled}


def _epoch(slot: str) -> float:
    return d3.slot_epoch(slot)


S0, S1, S2, S3 = "20260922-050800", "20260922-050900", "20260922-051000", "20260922-051100"
FIXED = [
    _obs(_epoch(S0) + 20, active=True, scheduled=S0),
    _obs(_epoch(S1) + 5, active=True, skip_slot=S1, scheduled=S1),
    _obs(_epoch(S1) + 40, active=True, skip_slot=S1, scheduled=S1),
    _obs(_epoch(S2) + 5, active=True, skip_slot=S2, scheduled=S2),
    _obs(_epoch(S2) + 35, active=False, skip_slot=S2, scheduled=S2),
    _obs(_epoch(S3) + 5, active=False, skip_slot=S2, scheduled=S3),
]
FIXED_RESTORES = {"first": S0, "second": S3}
# REHEARSAL-SKIP-DEFERS-SLOT: `slot_name(now)`, rewritten each requeue, and
# `lastScheduledSlot` left at the first's slot — so S2 fires the moment the
# first ends.
DEFERRED = [
    _obs(_epoch(S1) + 3, active=True, skip_slot="20260922-050903", scheduled=S0),
    _obs(_epoch(S1) + 33, active=True, skip_slot="20260922-050933", scheduled=S0),
    _obs(_epoch(S2) + 35, active=False, skip_slot="20260922-050933", scheduled=S2),
    _obs(_epoch(S3) + 20, active=False, skip_slot="20260922-050933", scheduled=S2),
]
DEFERRED_RESTORES = {"first": S0, "late": S2}


def test_a_skipped_slot_is_consumed_and_never_fired_late() -> None:
    verdict, ok = d3.skipped_slot_is_consumed(FIXED, FIXED_RESTORES, "first")
    row("VP row 3: each skip names its due slot and consumes it; the next rehearsal is for a "
        "later slot", verdict == "PASS", f"{verdict} {json.dumps(ok)}")
    verdict, ok = d3.skipped_slot_is_consumed(DEFERRED, DEFERRED_RESTORES, "first")
    row("MUTANT (the defect): the evaluation instant, rewritten every requeue, and the blocked "
        "slot fired late", verdict == "FAIL", f"{verdict} {json.dumps(ok)}")
    late = dict(FIXED_RESTORES, late=S2)
    verdict, _ = d3.skipped_slot_is_consumed(FIXED, late, "first")
    row("MUTANT: a Restore for a slot recorded as skipped", verdict == "FAIL", verdict)
    stuck = [dict(o, lastScheduledSlot=S0) for o in FIXED]
    verdict, _ = d3.skipped_slot_is_consumed(stuck, FIXED_RESTORES, "first")
    row("MUTANT: the skip is named right but lastScheduledSlot never advanced",
        verdict == "FAIL", verdict)
    verdict, _ = d3.skipped_slot_is_consumed(FIXED[:4], {"first": S0}, "first")
    row("the first still running when the watch ended is NOT-REACHED, never PASS",
        verdict == "NOT-REACHED", verdict)
    verdict, _ = d3.skipped_slot_is_consumed(FIXED[:1], {"first": S0}, "first")
    row("no skip at all is NOT-REACHED here (step 7 FAILS it)", verdict == "NOT-REACHED")


def test_the_refused_point_phase_is_declared_and_ordered() -> None:
    row("refused_point is a phase, after rehearsal", "refused_point" in d3.PHASES
        and d3.PHASES.index("refused_point") > d3.PHASES.index("rehearsal"))
    row("and its preconditions are declared", d3.PHASE_PRECONDITIONS.get("refused_point")
        == ("setup", "trust", "rehearsal"))
    row("the verdict wait covers one whole fetch attempt, not the old 45 s",
        d3.VERDICT_SETTLE_SECONDS >= 240 and "Pending" not in d3.VERDICTS)
    import inspect

    row("run_backup and settle_verdict wait that bound by default",
        inspect.signature(d3.run_backup).parameters["settle"].default
        == d3.VERDICT_SETTLE_SECONDS
        and inspect.signature(d3.settle_verdict).parameters["seconds"].default
        == d3.VERDICT_SETTLE_SECONDS)
    body = json.dumps(d3.destination("x", "b", evidence_read=False))
    row("a destination can declare NO evidenceRead grant — the one shape still NotAttempted",
        "evidenceRead" not in body and "evidenceWrite" in body)
    split = d3.destination("x", "b", evidence_read_secret="broken")
    row("and can read with a Secret of its own, leaving the other three grants alone",
        split["spec"]["access"]["evidenceRead"]["secret"]["name"] == "broken"
        and split["spec"]["access"]["archiveWrite"]["secret"]["name"] == "logweir-s3")


# THE CLASS GUARD. The sentence the pre-fetch controller wrote — and the claims
# built on it — must not survive in a LIVE harness module, where it would be
# asserted against a build that no longer writes it. Fixtures in test files may
# quote it as the named pre-fetch shape; the live code may not.
RETIRED = re.compile(r"this build does not create (that Job|it)\b"
                     r"|this build creates no evidence-fetch Job")


def live_harness_modules() -> list[pathlib.Path]:
    found = set(_REPO.glob("e2e/k8s/*/*_live.py")) | set(_REPO.glob("e2e/k8s/*/*_live.mjs"))
    found |= set(_REPO.glob("scripts/test-*-live.py")) | set(_REPO.glob("scripts/live/**/*.py"))
    return sorted(found)


def test_no_live_harness_asserts_the_retired_sentence() -> None:
    modules = live_harness_modules()
    row("the sweep reads d2_live.py, d3_live.py and scripts/test-*-live.py",
        any(p.name == "d2_live.py" for p in modules) and any(p.name == "d3_live.py"
                                                             for p in modules)
        and any(p.name.startswith("test-plat") for p in modules))
    for path in modules:
        hits = [f"{i}: {line.strip()[:80]}" for i, line in
                enumerate(path.read_text().splitlines(), 1) if RETIRED.search(line)]
        row(f"{path.relative_to(_REPO)} asserts no 'this build creates no evidence-fetch Job'",
            not hits, "; ".join(hits))
    row("PLANTED: the guard names the sentence it exists for",
        bool(RETIRED.search("…(D2 §3.9's evidence-fetch Job), and this build does not "
                            "create that Job")))


def test_zz_every_row_in_this_file_passed() -> None:
    """The file's own gate under pytest (see `test_rows.py`'s twin)."""
    assert not FAILURES, f"{len(FAILURES)} failing row(s): {FAILURES}"


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and name != "test_zz_every_row_in_this_file_passed":
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
