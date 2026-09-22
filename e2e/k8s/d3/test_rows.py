#!/usr/bin/env python3
"""Unit rows for the D3 live harness's row decisions.

    python3 e2e/k8s/d3/test_rows.py

No cluster, no network, no credential: every case is a recorded status block.
Each decision is exercised TWICE — once against the shape the product publishes
today, and once against the shape it published before the defect was fixed (or
the shape the row used to look for). The second must be REFUSED. A row that
cannot be made to say False proves nothing, and five of these rows spent a lab
refresh proving exactly that: `retention-two-destinations` passed while reading
a key no bucket carries, so all four of its sets were empty and it asserted
disjointness between nothing and nothing.

The recorded shapes come from lab-refresh-3 (§8.2, §8.3) and from this
harness's own run of 2026-09-18.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="d3-test-rows-")
os.environ.setdefault("LOGWEIR_D3_OUT", _TMP)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d3_live as d3  # noqa: E402

FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


# --- the evaluation the controller publishes today --------------------------
KEEP_B = {
    "pointsEvaluated": 6,
    "candidateCount": 3,
    "candidates": [
        {"pointId": "lwp1-8033c08b", "reason": "BeyondKeepLast"},
        {"pointId": "lwp1-c45de002", "reason": "BeyondKeepLast"},
        {"pointId": "lwp1-d01b282f", "reason": "BeyondKeepLast"},
    ],
    "kept": ["lwp1-7b42facc", "lwp1-bfe50a47", "lwp1-f491e748"],
    "protected": [{"pointId": "lwp1-f491e748", "reason": "MinUsablePoints"}],
    "skipped": [],
}
KEEP_A = {
    "pointsEvaluated": 4,
    "candidates": [
        {"pointId": "lwp1-053b3534", "reason": "BeyondKeepLast"},
        {"pointId": "lwp1-bbb32faf", "reason": "BeyondKeepLast"},
    ],
    "kept": ["lwp1-b16f0ff2"],
    "protected": [],
    "skipped": [{"pointId": "lwp1-caac1744", "reason": "Unreadable"}],
}
# What the rows used to look for: `backupId` on every bucket, `state` instead of
# `reason`, and `protected` meaning the whole retained set.
KEEP_B_AS_THE_ROW_READ_IT = {
    "pointsEvaluated": 6,
    "candidates": [{"backupId": None, "reason": "BeyondKeepLast"}] * 3,
    "kept": ["lwp1-7b42facc", "lwp1-bfe50a47", "lwp1-f491e748"],
    "protected": [{"backupId": None, "reason": "MinUsablePoints"}] * 3,
    "skipped": [],
}
KEEP_A_AS_THE_ROW_READ_IT = {
    "pointsEvaluated": 4,
    "candidates": [{"backupId": None, "reason": "BeyondKeepLast"}] * 2,
    "kept": ["lwp1-b16f0ff2"],
    "protected": [],
    "skipped": [{"backupId": None, "state": "Unreadable"}],
}


def test_point_ids_are_read_from_every_bucket() -> None:
    ids = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_B}})
    row("point ids: candidates, protected, skipped and the bare-string kept list",
        ids["candidates"] == {"lwp1-8033c08b", "lwp1-c45de002", "lwp1-d01b282f"}
        and ids["protected"] == {"lwp1-f491e748"}
        and ids["kept"] == {"lwp1-7b42facc", "lwp1-bfe50a47", "lwp1-f491e748"}
        and ids["skipped"] == set())
    stale = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_B_AS_THE_ROW_READ_IT}})
    row("MUTANT: reading backupId leaves candidates and protected empty",
        stale["candidates"] == set() and stale["protected"] == set(),
        f"{stale}")


def test_two_destinations_cannot_pass_on_empty_reports() -> None:
    ids_a = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_A}})
    ids_b = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_B}})
    a_seen = set().union(*ids_a.values())
    b_seen = set().union(*ids_b.values())
    a_universe = {"lwp1-053b3534", "lwp1-bbb32faf", "lwp1-b16f0ff2", "lwp1-caac1744"}
    b_universe = set(KEEP_B["kept"]) | {c["pointId"] for c in KEEP_B["candidates"]}
    row("two destinations: both reports name their own ids and nothing crosses",
        d3.reports_are_disjoint(KEEP_A, KEEP_B, a_seen, b_seen, a_universe, b_universe, 6))
    row("MUTANT: two empty reports are not disjointness",
        not d3.reports_are_disjoint(KEEP_A, KEEP_B, set(), set(), a_universe, b_universe, 6))
    row("MUTANT: an id that crosses destinations is refused",
        not d3.reports_are_disjoint(KEEP_A, KEEP_B, a_seen | {"lwp1-7b42facc"}, b_seen,
                                    a_universe, b_universe, 6))


def test_protected_is_the_override_not_the_retained_set() -> None:
    want = d3.keep_rule_expectation(6, 2, 3)
    row("keepLast 2 and minUsablePoints 3 over 6 points is 3 candidates, 3 kept, 1 override",
        want == {"kept": 3, "candidates": 3, "protected": 1}, f"{want}")
    ids = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_B}})
    row("overlapping keep rules: the published evaluation satisfies it",
        d3.overlapping_keep_rules_ok(KEEP_B, ids, want, 6))
    stale_ids = d3.evaluation_point_ids(
        {"status": {"lastEvaluation": KEEP_B_AS_THE_ROW_READ_IT}})
    row("MUTANT: `protected` meaning the whole retained set is refused",
        not d3.overlapping_keep_rules_ok(KEEP_B_AS_THE_ROW_READ_IT, stale_ids, want, 6))
    crossed = json.loads(json.dumps(KEEP_B))
    crossed["protected"] = [{"pointId": "lwp1-8033c08b", "reason": "MinUsablePoints"}]
    row("MUTANT: a protected point that is also a candidate is refused",
        not d3.overlapping_keep_rules_ok(
            crossed, d3.evaluation_point_ids({"status": {"lastEvaluation": crossed}}), want, 6))
    invented = json.loads(json.dumps(KEEP_B))
    invented["candidates"][0]["reason"] = "Whatever"
    row("MUTANT: a candidate reason outside the vocabulary is refused",
        not d3.overlapping_keep_rules_ok(
            invented, d3.evaluation_point_ids({"status": {"lastEvaluation": invented}}), want, 6))


def test_an_unreadable_point_is_named_explained_and_never_a_candidate() -> None:
    ids = d3.evaluation_point_ids({"status": {"lastEvaluation": KEEP_A}})
    row("unreadable point: skipped with a published reason, and not a candidate",
        d3.skipped_never_a_candidate(KEEP_A, ids))
    stale_ids = d3.evaluation_point_ids(
        {"status": {"lastEvaluation": KEEP_A_AS_THE_ROW_READ_IT}})
    row("MUTANT: `state` instead of `reason`, and no pointId, is refused",
        not d3.skipped_never_a_candidate(KEEP_A_AS_THE_ROW_READ_IT, stale_ids))
    nothing_skipped = json.loads(json.dumps(KEEP_A))
    nothing_skipped["skipped"] = []
    row("MUTANT: an evaluation that skipped nothing cannot satisfy this row",
        not d3.skipped_never_a_candidate(
            nothing_skipped,
            d3.evaluation_point_ids({"status": {"lastEvaluation": nothing_skipped}})))
    also_candidate = json.loads(json.dumps(KEEP_A))
    also_candidate["candidates"].append({"pointId": "lwp1-caac1744", "reason": "BeyondKeepLast"})
    row("MUTANT: an unreadable point proposed for deletion is refused",
        not d3.skipped_never_a_candidate(
            also_candidate,
            d3.evaluation_point_ids({"status": {"lastEvaluation": also_candidate}})))


def test_the_digest_prefix_defect_has_a_fingerprint_of_its_own() -> None:
    same = "sha256:" + "a" * 64
    bare = "a" * 64
    row("the defect's own shape: two digests equal once the prefix is off",
        d3.digest_prefix_signature(
            f"page digest {bare} does not match published {same}"))
    row("MUTANT: an absent catalog is not this defect",
        not d3.digest_prefix_signature(
            "namespace d3w14 has no RecoveryCatalog named secondary; retention evaluates "
            "the catalog's bounded view and never a bucket walk of its own"))
    row("MUTANT: two digests that genuinely differ are not this defect",
        not d3.digest_prefix_signature(
            f"page digest {bare} does not match published sha256:{'b' * 64}"))
    row("MUTANT: one digest alone is not two compared",
        not d3.digest_prefix_signature(f"page digest {same} is unreadable"))
    row("MUTANT: the SAME digest printed twice, both prefixed, is somebody else's "
        "equality bug",
        not d3.digest_prefix_signature(f"expected {same} got {same}"))
    row("MUTANT: the same digest printed twice, both bare, likewise",
        not d3.digest_prefix_signature(f"expected {bare} got {bare}"))
    row("the fingerprint survives the other order — prefixed first, bare second",
        d3.digest_prefix_signature(f"published {same} does not match computed {bare}"))
    row("MUTANT: an empty message decides nothing", not d3.digest_prefix_signature(""))


def test_the_enforcer_is_in_the_shipped_runner_image() -> None:
    present = {"terminated": {"exitCode": 3, "reason": "Error",
                              "startedAt": "2026-09-18T18:48:40Z"}}
    row("packaging: a container that started and refused for itself",
        d3.enforcer_is_in_the_image(present))
    row("MUTANT: exit 127 is a binary that is not there",
        not d3.enforcer_is_in_the_image({"terminated": {"exitCode": 127, "reason": "Error"}}))
    row("MUTANT: the pre-fix shape — the kubelet could not start it at all",
        not d3.enforcer_is_in_the_image(
            {"waiting": {"reason": "CreateContainerError",
                         "message": 'exec: "logweir-retention": executable file not found in $PATH'}}))
    row("MUTANT: a `no such file` message is refused even with an exit code",
        not d3.enforcer_is_in_the_image(
            {"terminated": {"exitCode": 3, "message": "no such file or directory"}}))
    row("MUTANT: a clean exit 0 is not the enforcer refusing a plan it was never given",
        not d3.enforcer_is_in_the_image({"terminated": {"exitCode": 0}}))


def test_notification_delivery_follows_the_hatch_and_never_rewrites_a_backup() -> None:
    row("the hatch, as the controller reads it",
        d3.hatch_is_open("1") and d3.hatch_is_open("true") and not d3.hatch_is_open(None)
        and not d3.hatch_is_open("0"))
    row("one POST per transition this window opened, none while the hatch is shut",
        d3.expected_posts(True, 1) == 1 and d3.expected_posts(True, 0) == 0
        and d3.expected_posts(False, 1) == 0)
    row("hatch open, one new transition: one POST, Delivered, no Backup rewritten",
        d3.notify_delivery_ok(True, 1, d3.expected_posts(True, 1), True, 7, 7))
    row("hatch open, a re-run that opened no transition: no POST, still Delivered",
        d3.notify_delivery_ok(True, 0, d3.expected_posts(True, 0), True, 7, 7))
    row("hatch shut: no POST, not Delivered, no Backup rewritten",
        d3.notify_delivery_ok(False, 0, d3.expected_posts(False, 1), False, 7, 7))
    row("MUTANT: the pre-fix row — zero POSTs for a transition the hatch let through",
        not d3.notify_delivery_ok(True, 0, d3.expected_posts(True, 1), False, 7, 7))
    row("MUTANT: a re-notification with no transition behind it",
        not d3.notify_delivery_ok(True, 1, d3.expected_posts(True, 0), True, 7, 7))
    row("MUTANT: a POST that got out while the hatch was shut",
        not d3.notify_delivery_ok(False, 1, d3.expected_posts(False, 1), True, 7, 7))
    row("MUTANT: delivery rewrote a Backup",
        not d3.notify_delivery_ok(True, 1, d3.expected_posts(True, 1), True, 6, 7))
    # The weak case is real and is why the row says so in its own message
    # (review L-6): with no transition owed, the POST half is `0 == 0` and a
    # swallowed delivery would look identical.
    row("the re-used-namespace case owes nothing, and so observes nothing",
        d3.expected_posts(True, 0) == 0
        and d3.notify_delivery_ok(True, 0, d3.expected_posts(True, 0), True, 7, 7))


# --- PLAT-16.2's two controller-side guards ---------------------------------
# The recorded shapes are this branch's own live run of 2026-09-18: 6 points on
# dest-b, keepLast 2, minUsablePoints 3, one held by `spec.holds[]` and one
# whose set a nonterminal Restore names.
HELD = "lwp1-c7632434ab586a67359dd5d86af68f83"
RESTORED = "lwp1-ffaebc76ef7a6c9e95b01edfc279513a"
CANDIDATE = "lwp1-900afc610438ad76e0dcd0658bd7a3d1"
GUARDED = {
    "pointsEvaluated": 6,
    "candidates": [{"pointId": CANDIDATE, "reason": "BeyondKeepLast"}],
    "kept": ["lwp1-aaa", "lwp1-bbb", "lwp1-ccc", HELD, RESTORED],
    "protected": [
        {"pointId": RESTORED, "reason": "ActiveRestore"},
        {"pointId": "lwp1-ccc", "reason": "MinUsablePoints"},
        {"pointId": HELD, "reason": "Hold"},
    ],
    "skipped": [],
}
GUARD_PLAN = {"lines": [{"point_id": CANDIDATE, "set_prefix": "archive/set-c/"}]}
OBJECTS_BEFORE = [{"key": k} for k in (
    "archive/set-held/manifest.json", "archive/set-held/s0.bin.zst",
    "archive/set-restored/manifest.json", "archive/set-c/manifest.json",
    "archive/set-c/s0.bin.zst", "logweir/retention/rec.json")]
OBJECTS_AFTER = [o for o in OBJECTS_BEFORE if not o["key"].startswith("archive/set-c/")]
PROTECTED_PREFIXES = {"archive/set-held/", "archive/set-restored/"}


def test_a_held_point_and_a_restored_one_are_kept_with_their_reason() -> None:
    held = d3.protection_verdict(GUARDED, HELD)
    restored = d3.protection_verdict(GUARDED, RESTORED)
    row("the verdict reads all three buckets for one point",
        held == {"pointId": HELD, "isCandidate": False, "isKept": True,
                 "protectReason": "Hold"}, str(held))
    row("`spec.holds[]` is reported as Hold, kept, and not a candidate",
        d3.point_is_protected(held, "Hold"))
    row("a set a nonterminal Restore names is reported as ActiveRestore",
        d3.point_is_protected(restored, "ActiveRestore"))
    row("MUTANT: the right reason on a point that is STILL a candidate",
        not d3.point_is_protected(
            {"isCandidate": True, "isKept": True, "protectReason": "Hold"}, "Hold"))
    row("MUTANT: protected but not kept — a plan that contradicts itself",
        not d3.point_is_protected(
            {"isCandidate": False, "isKept": False, "protectReason": "Hold"}, "Hold"))
    row("MUTANT: kept with NO reason is a retention decision nobody can audit",
        not d3.point_is_protected(
            {"isCandidate": False, "isKept": True, "protectReason": None}, "Hold"))
    row("MUTANT: `LegalHold` is a provider refusal, not what `spec.holds[]` writes",
        not d3.point_is_protected(held, "LegalHold"))
    row("MUTANT: a point the evaluation never mentions is absent, not protected",
        not d3.point_is_protected(d3.protection_verdict(GUARDED, "lwp1-never"), "Hold"))
    row("MUTANT: the two guards are not interchangeable",
        not d3.point_is_protected(held, "ActiveRestore")
        and not d3.point_is_protected(restored, "Hold"))


def test_a_protected_point_never_reaches_the_enforcer() -> None:
    row("the plan omits both protected points",
        d3.plan_omits(GUARD_PLAN, {HELD, RESTORED}))
    row("MUTANT: a held point that reached the plan",
        not d3.plan_omits({"lines": [{"point_id": HELD}]}, {HELD, RESTORED}))
    row("MUTANT: an actively-restored point that reached the plan",
        not d3.plan_omits({"lines": GUARD_PLAN["lines"] + [{"point_id": RESTORED}]},
                          {HELD, RESTORED}))
    row("an empty plan omits everything, trivially",
        d3.plan_omits({"lines": []}, {HELD, RESTORED}))


def test_the_protected_objects_survive_an_enforced_pass() -> None:
    row("every object under both protected sets is still there afterwards",
        d3.survived_enforcement(OBJECTS_BEFORE, OBJECTS_AFTER, PROTECTED_PREFIXES))
    lost = [o for o in OBJECTS_AFTER if o["key"] != "archive/set-held/s0.bin.zst"]
    row("MUTANT: one segment of the held set was deleted",
        not d3.survived_enforcement(OBJECTS_BEFORE, lost, PROTECTED_PREFIXES))
    gone = [o for o in OBJECTS_AFTER if not o["key"].startswith("archive/set-restored/")]
    row("MUTANT: the actively-restored set was removed whole",
        not d3.survived_enforcement(OBJECTS_BEFORE, gone, PROTECTED_PREFIXES))
    row("MUTANT: a run in which the protected sets had no objects to begin with "
        "proves nothing",
        not d3.survived_enforcement([{"key": "archive/set-c/manifest.json"}],
                                    OBJECTS_AFTER, PROTECTED_PREFIXES))


# --- PLAT-16.1 and 16.2's new rows ------------------------------------------
# The recorded shapes are this branch's own live runs of 2026-09-18.
def _pol(conds: list[dict], **status) -> dict:
    return {"status": dict(status, conditions=conds)}


LIFECYCLE_GUARANTEES = {
    "ageExpiry": "ProviderEnforcedUnverified", "legalHold": "ProviderEnforcedUnverified",
    "minUsablePoints": "NotEnforced", "activeRestoreProtection": "NotEnforced",
    "sharedSegments": "NotEnforced",
}
LIFECYCLE_CONDS = [
    {"type": "Evaluated", "status": "Unknown", "reason": "NeverEvaluated",
     "message": "an ExternalLifecycle policy produces no Logweir evaluation"},
    {"type": "Enforced", "status": "False", "reason": "RecommendationOnly"},
]
LIFECYCLE_POLICY = _pol(LIFECYCLE_CONDS, guarantees=LIFECYCLE_GUARANTEES)


def test_a_lifecycle_nobody_read_is_reported_as_unknown() -> None:
    row("the declared rule is unknown, unenforced and unevaluated",
        all(d3.external_lifecycle_is_unknown(LIFECYCLE_POLICY).values()))
    row("MUTANT: ageExpiry claimed as LogweirEnforced",
        not all(d3.external_lifecycle_is_unknown(
            _pol(LIFECYCLE_CONDS,
                 guarantees=dict(LIFECYCLE_GUARANTEES, ageExpiry="LogweirEnforced"))).values()))
    row("MUTANT: a count-based guarantee claimed for a bucket rule",
        not all(d3.external_lifecycle_is_unknown(
            _pol(LIFECYCLE_CONDS,
                 guarantees=dict(LIFECYCLE_GUARANTEES,
                                 minUsablePoints="LogweirEnforced"))).values()))
    row("MUTANT: an EMPTY evaluation — a claim to know nothing will be deleted",
        not all(d3.external_lifecycle_is_unknown(
            _pol(LIFECYCLE_CONDS, guarantees=LIFECYCLE_GUARANTEES,
                 lastEvaluation={"candidates": [], "candidateCount": 0})).values()))
    row("MUTANT: Evaluated=True on a policy nothing evaluated",
        not all(d3.external_lifecycle_is_unknown(
            _pol([{"type": "Evaluated", "status": "True", "reason": "EvaluationComplete"},
                  {"type": "Enforced", "status": "False", "reason": "RecommendationOnly"}],
                 guarantees=LIFECYCLE_GUARANTEES)).values()))


FAILED_EVAL = _pol([
    {"type": "Evaluated", "status": "False", "reason": "ViewUnreadable",
     "message": "namespace X has no RecoveryCatalog named Y"},
    {"type": "Ready", "status": "False", "reason": "CatalogUnusable"},
])


def test_an_evaluation_failure_is_never_an_empty_report() -> None:
    row("a failure carries its own reason, message and a not-ready policy",
        all(d3.evaluation_failure_is_distinguishable(FAILED_EVAL).values()))
    row("MUTANT: THE DEFECT — a failure that answers `candidates: []`",
        not all(d3.evaluation_failure_is_distinguishable(_pol(
            FAILED_EVAL["status"]["conditions"],
            lastEvaluation={"candidates": [], "candidateCount": 0})).values()))
    row("MUTANT: a candidateCount of 0 beside no candidate list",
        not all(d3.evaluation_failure_is_distinguishable(_pol(
            FAILED_EVAL["status"]["conditions"],
            lastEvaluation={"candidateCount": 0})).values()))
    row("MUTANT: Evaluated=False with no reason at all",
        not all(d3.evaluation_failure_is_distinguishable(_pol(
            [{"type": "Evaluated", "status": "False", "reason": "", "message": "x"},
             {"type": "Ready", "status": "False"}])).values()))
    row("MUTANT: a success reason wearing a False status",
        not all(d3.evaluation_failure_is_distinguishable(_pol(
            [{"type": "Evaluated", "status": "False", "reason": "EvaluationComplete",
              "message": "x"},
             {"type": "Ready", "status": "False"}])).values()))
    row("MUTANT: Ready=True, so a console shows the policy as working",
        not all(d3.evaluation_failure_is_distinguishable(_pol(
            [{"type": "Evaluated", "status": "False", "reason": "ViewUnreadable",
              "message": "x"},
             {"type": "Ready", "status": "True"}])).values()))


def test_the_shared_segment_guarantee_is_derived_from_the_view() -> None:
    plain = [{"pointId": "p1"}, {"pointId": "p2"}]
    with_keys = [{"pointId": "p1", "segmentKeys": ["a/s0"]}, {"pointId": "p2"}]
    row("no segment keys in the view means NotEnforced and no SharedSegment",
        all(d3.shared_segment_contract(plain, {"protected": []},
                                       {"sharedSegments": "NotEnforced"}).values()))
    row("segment keys in the view would mean LogweirEnforced",
        all(d3.shared_segment_contract(with_keys, {"protected": []},
                                       {"sharedSegments": "LogweirEnforced"}).values()))
    row("MUTANT: THE WITHDRAWN-GUARANTEE DEFECT — LogweirEnforced on a view "
        "that cannot support it",
        not all(d3.shared_segment_contract(plain, {"protected": []},
                                           {"sharedSegments": "LogweirEnforced"}).values()))
    row("MUTANT: a SharedSegment protection the view cannot justify",
        not all(d3.shared_segment_contract(
            plain, {"protected": [{"pointId": "p1", "reason": "SharedSegment"}]},
            {"sharedSegments": "NotEnforced"}).values()))
    row("MUTANT: a guarantee outside the published vocabulary",
        not all(d3.shared_segment_contract(plain, {"protected": []},
                                           {"sharedSegments": "Probably"}).values()))


PARTIAL_POINTS = [{"retention-point": "pA", "state": "Deleted"},
                  {"retention-point": "pB", "state": "Kept", "code": "AccessDenied"}]
PARTIAL_DOC = {
    "exit_code": 1, "objects_deleted": 3,
    "points": [
        {"point_id": "pA", "state": "Deleted", "objects_deleted": 3},
        {"point_id": "pB", "state": "Kept", "objects_deleted": 0, "code": "AccessDenied",
         "remaining_keys": ["archive/B/manifest.json", "archive/B/s0.bin.zst"]},
    ],
}
GONE = {"archive/A/manifest.json", "archive/A/s0.bin.zst", "archive/A/s1.bin.zst"}
REMAIN = {"archive/B/manifest.json", "archive/B/s0.bin.zst", "logweir/retention/r.json"}


def _partial(**over):
    args = dict(points=PARTIAL_POINTS, exit_code=1, gone=GONE, remaining=REMAIN,
                allowed_prefix="archive/A/", denied_prefix="archive/B/", doc=PARTIAL_DOC)
    args.update(over)
    return all(d3.partial_failure_is_attributable(**args).values())


def test_a_partial_failure_says_exactly_what_it_did() -> None:
    row("one deleted, one denied, and a record that adds up", _partial())
    row("MUTANT: exit 0 on a run that did not complete", not _partial(exit_code=0))
    row("MUTANT: a record total larger than what actually went",
        not _partial(doc=dict(PARTIAL_DOC, objects_deleted=5)))
    row("MUTANT: a key removed under the DENIED set",
        not _partial(gone=GONE | {"archive/B/manifest.json"}))
    row("MUTANT: the denied point claiming a deletion",
        not _partial(doc={**PARTIAL_DOC, "points": [
            PARTIAL_DOC["points"][0],
            dict(PARTIAL_DOC["points"][1], objects_deleted=2)]}))
    row("MUTANT: no closed code on the point that did not complete",
        not _partial(doc={**PARTIAL_DOC, "points": [
            PARTIAL_DOC["points"][0],
            {"point_id": "pB", "state": "Kept", "objects_deleted": 0,
             "remaining_keys": ["archive/B/manifest.json"]}]}))
    row("MUTANT: leftovers it cannot name — the next plan has nothing to go on",
        not _partial(doc={**PARTIAL_DOC, "points": [
            PARTIAL_DOC["points"][0],
            {"point_id": "pB", "state": "Kept", "objects_deleted": 0,
             "code": "AccessDenied"}]}))
    row("MUTANT: leftovers that are not actually there any more",
        not _partial(remaining={"logweir/retention/r.json"}))
    row("MUTANT: only the first point reported — a run that stopped dead",
        not _partial(points=PARTIAL_POINTS[:1]))


DEGRADED = _pol([{"type": "EnforcementDegraded", "status": "True", "reason": "RunFailures",
                  "message": "three consecutive retention runs failed"}])


def test_bounded_retry_degrades_and_stops() -> None:
    row("three failed Jobs, counted, a degraded condition with words, no further Job",
        all(d3.bounded_retry_degrades(DEGRADED, 3, 3, 0).values()))
    row("MUTANT: degraded before the budget is spent",
        not all(d3.bounded_retry_degrades(DEGRADED, 2, 2, 0).values()))
    row("MUTANT: three failures and no degraded condition",
        not all(d3.bounded_retry_degrades(_pol([]), 3, 3, 0).values()))
    row("MUTANT: degraded, and still creating Jobs",
        not all(d3.bounded_retry_degrades(DEGRADED, 3, 3, 2).values()))
    row("MUTANT: a degraded condition with no message",
        not all(d3.bounded_retry_degrades(
            _pol([{"type": "EnforcementDegraded", "status": "True", "reason": "RunFailures",
                   "message": "  "}]), 3, 3, 0).values()))
    # RET-DEGRADED-UNREACHABLE, as the live row meets it: five Jobs failed and
    # the policy's counter stayed at 1, so the status patch never landed. A
    # harness that read the counter for BOTH numbers could not see this at all —
    # it would look like a policy that had not run.
    row("MUTANT: THE DEFECT — runs failed and the policy did not count them",
        not all(d3.bounded_retry_degrades(_pol([]), 5, 1, 5).values()))
    row("MUTANT: a counter that moved with no failed run behind it",
        not all(d3.bounded_retry_degrades(DEGRADED, 0, 3, 0).values()))


# --- PLAT-19.1's re-pointed trust rows ---------------------------------------
# The recorded shapes are lab-refresh-5 §8.2 and this branch's own run.
FRESH_V = {"result": "Valid", "signedAt": "2026-09-19T01:50:17Z",
           "matchedKeyId": "2c76e22ff899", "verifiedAt": "2026-09-19T01:50:20Z"}
HEALED_V = dict(FRESH_V, verifiedAt="2026-09-19T01:52:02Z")
# What the defect did: the same receipt, stripped of `signedAt`, re-derived to
# `Untrusted` with "carries no signing-time field".
DEFECT_V = {"result": "Untrusted", "matchedKeyId": "2c76e22ff899",
            "reason": "SignedOutsideValidity"}


def test_a_stripped_signing_time_is_re_derived_rather_than_distrusted() -> None:
    row("the fix: Valid comes back, with its signing time and the same key",
        all(d3.signedat_heals(FRESH_V, HEALED_V).values()))
    row("MUTANT: THE DEFECT — the stripped object re-derives Untrusted",
        not all(d3.signedat_heals(FRESH_V, DEFECT_V).values()))
    row("MUTANT: Valid again but the signing time never came back",
        not all(d3.signedat_heals(
            FRESH_V, {k: v for k, v in HEALED_V.items() if k != "signedAt"}).values()))
    row("MUTANT: a repair that matched a DIFFERENT signing key is a new verdict, "
        "not a repair",
        not all(d3.signedat_heals(FRESH_V, dict(HEALED_V, matchedKeyId="0000")).values()))
    row("MUTANT: the fresh run never recorded a signing time, so there is nothing "
        "to strip",
        not all(d3.signedat_heals(
            {k: v for k, v in FRESH_V.items() if k != "signedAt"}, HEALED_V).values()))
    row("MUTANT: an empty re-derivation is not a healing",
        not all(d3.signedat_heals(FRESH_V, {}).values()))


def test_clearing_the_block_reads_no_archive() -> None:
    row("nothing comes back when the stored claim is gone",
        all(d3.cleared_block_is_not_re_read({}).values()))
    row("MUTANT: a verdict appeared, so something DID read the archive",
        not all(d3.cleared_block_is_not_re_read(
            {"result": "Valid", "signedAt": "2026-09-19T01:55:00Z"}).values()))
    row("MUTANT: a signing time with no verdict is still a read",
        not all(d3.cleared_block_is_not_re_read(
            {"signedAt": "2026-09-19T01:55:00Z"}).values()))


# --- PLAT-19.1's `unauthorized update`, as an RBAC result --------------------
RBAC_REFUSAL = (
    'Error from server (Forbidden): trustpolicies.logweir.dev "d3w14-x" is forbidden: '
    'User "system:serviceaccount:d3w14-x:d3w14-nonadmin" cannot patch resource '
    '"trustpolicies" in API group "logweir.dev" at the cluster scope'
)
# What the OTHER refusal looks like — the CRD's own CEL, which fires for a
# cluster-admin too and is what `trust-lifecycle-is-monotonic` already proves.
CEL_REFUSAL = (
    'The TrustPolicy "d3w14-x" is invalid: * spec.keys[0]: Invalid value: "object": '
    "a key's state moves Active -> Retired, Active|Retired -> Revoked, and never backwards"
)


def test_an_unauthorized_trust_edit_is_refused_by_rbac_not_by_cel() -> None:
    row("a non-admin is refused the write, allowed the read, and the admin may write",
        all(d3.rbac_refused_the_update("no", "yes", RBAC_REFUSAL, "yes").values()))
    row("MUTANT: THE WRONG BOUNDARY — CEL's refusal accepted as an RBAC result",
        not all(d3.rbac_refused_the_update("no", "yes", CEL_REFUSAL, "yes").values()))
    row("MUTANT: the subject could patch after all",
        not all(d3.rbac_refused_the_update("yes", "yes", RBAC_REFUSAL, "yes").values()))
    row("MUTANT: a subject with no access at all — the refusal says nothing about trust",
        not all(d3.rbac_refused_the_update("no", "no", RBAC_REFUSAL, "yes").values()))
    row("MUTANT: a cluster where nobody may write a TrustPolicy",
        not all(d3.rbac_refused_the_update("no", "yes", RBAC_REFUSAL, "no").values()))
    row("MUTANT: an empty refusal proves nothing",
        not all(d3.rbac_refused_the_update("no", "yes", "", "yes").values()))
    row("MUTANT: a refusal about another resource",
        not all(d3.rbac_refused_the_update(
            "no", "yes",
            'backups.logweir.dev is forbidden: User "x" cannot patch resource "backups"',
            "yes").values()))


# --- PLAT-19.1's `old archive` — a second signer, then retired ---------------
SIGNED_WHILE_ACTIVE = {"result": "Valid", "matchedKeyId": "7d1fb29eae5fd0ea",
                       "signedAt": "2026-09-19T03:07:02Z",
                       "trust": {"basis": "Current", "keyState": "Active"}}
AFTER_RETIREMENT = {"result": "Valid", "matchedKeyId": "7d1fb29eae5fd0ea",
                    "signedAt": "2026-09-19T03:07:02Z",
                    "trust": {"basis": "Historical", "keyState": "Retired"}}
SIGNED_AFTER_RETIREMENT = {"result": "Untrusted", "matchedKeyId": "7d1fb29eae5fd0ea"}


def _old_archive(**over):
    args = dict(before=SIGNED_WHILE_ACTIVE, after=AFTER_RETIREMENT,
                fresh=SIGNED_AFTER_RETIREMENT)
    args.update(over)
    return all(d3.old_archive_survives_retirement(**args).values())


def test_a_retired_key_keeps_what_it_signed_and_signs_nothing_new() -> None:
    row("Valid/Current while active, Valid/Historical after, and no new Valid",
        _old_archive())
    row("MUTANT: THE RETIREMENT HIDDEN — still Valid, still `Current`",
        not _old_archive(after=dict(AFTER_RETIREMENT,
                                    trust={"basis": "Current", "keyState": "Active"})))
    row("MUTANT: RETIREMENT AS REVOCATION — the old archive goes Untrusted",
        not _old_archive(after={"result": "Untrusted",
                                "matchedKeyId": "7d1fb29eae5fd0ea",
                                "trust": {"basis": "Historical"}}))
    row("MUTANT: `retired` means nothing — a run signed AFTER it is Valid too",
        not _old_archive(fresh={"result": "Valid", "matchedKeyId": "7d1fb29eae5fd0ea"}))
    row("MUTANT: the archive never verified while the key was active",
        not _old_archive(before={"result": "Untrusted",
                                 "trust": {"basis": "Current"}}))
    row("MUTANT: a different key matched afterwards — not the same evidence re-judged",
        not _old_archive(after=dict(AFTER_RETIREMENT, matchedKeyId="0000")))
    row("MUTANT: no trust block at all after the retirement",
        not _old_archive(after={"result": "Valid", "matchedKeyId": "7d1fb29eae5fd0ea"}))


# --- PLAT-19.1's `multiple namespaces` ---------------------------------------
LAB_KEY = "2c76e22ff89969dc"
GOVERNED = {"result": "Valid", "matchedKeyId": LAB_KEY,
            "trust": {"basis": "Current", "keyState": "Active"}}
UNGOVERNED = {"result": "Untrusted", "matchedKeyId": LAB_KEY,
              "trust": {"basis": "None"}}


def _multi(**over):
    args = dict(governed=GOVERNED, ungoverned=UNGOVERNED, key_id=LAB_KEY)
    args.update(over)
    return all(d3.verdicts_differ_by_namespace(**args).values())


def test_two_namespaces_resolve_their_own_policies() -> None:
    row("trusted here, not there, and both about the same key", _multi())
    row("MUTANT: the other namespace trusts it too — no resolution happened",
        not _multi(ungoverned={"result": "Valid", "matchedKeyId": LAB_KEY,
                               "trust": {"basis": "Current"}}))
    row("MUTANT: the governed namespace does not trust it either",
        not _multi(governed={"result": "Untrusted", "matchedKeyId": LAB_KEY,
                             "trust": {"basis": "None"}}))
    row("MUTANT: the other namespace is SILENT rather than deciding",
        not _multi(ungoverned={"matchedKeyId": LAB_KEY}))
    row("MUTANT: different keys, so the differing verdicts say nothing about "
        "which policy governed which namespace",
        not _multi(governed=dict(GOVERNED, matchedKeyId="0000")))
    row("MUTANT: trusted here but on a historical basis, which is a different claim",
        not _multi(governed=dict(GOVERNED, trust={"basis": "Historical"})))


# --- the phase order, as the phases' own preconditions --------------------
def test_the_declared_phase_order_satisfies_its_preconditions() -> None:
    row("the shipped order violates nothing", not d3.phase_order_violations(d3.PHASES))
    broken = [p for p in d3.PHASES if p != "bounded_retry"]
    broken.insert(broken.index("packaging"), "bounded_retry")
    row("MUTANT: THE TRAP — bounded_retry before preview, which deletes what "
        "preview reads",
        "bounded_retry runs before preview" in d3.phase_order_violations(broken),
        str(d3.phase_order_violations(broken)))
    row("and it names every phase that would then fail, not only the first",
        len(d3.phase_order_violations(broken)) == 5)
    # review G-M2: `legal_hold` plants a nonterminal Restore that blocks every
    # enforcement run at dest-b, so `bounded_retry` must follow it.
    after_bounded = [p for p in d3.PHASES if p != "legal_hold"]
    after_bounded.append("legal_hold")
    row("MUTANT: legal_hold after bounded_retry — its leftover Restore would block "
        "every enforcement run",
        "bounded_retry runs before legal_hold" in d3.phase_order_violations(after_bounded))
    swapped = [p for p in d3.PHASES if p != "catalog"]
    swapped.append("catalog")
    row("MUTANT: catalog after the phases that read its view",
        bool(d3.phase_order_violations(swapped)))
    row("a phase list missing a phase entirely is not a violation, just a shorter run",
        not d3.phase_order_violations(["setup", "catalog", "retention"]))


# --- the old archive still RESTORES (review G-M1) ----------------------------
HIST = {"result": "Valid", "matchedKeyId": "6607952c", "trust": {"basis": "Historical"}}
AFTER_RETIREMENT_SIG = {"result": "Untrusted", "matchedKeyId": "6607952c"}


def _restores(**over):
    args = dict(verdict=HIST, admitted={"type": "Admitted", "status": "True"},
                job="d3w14-historical-restore", phase="Running",
                fresh=AFTER_RETIREMENT_SIG)
    args.update(over)
    return all(d3.historical_archive_still_restores(**args).values())


def test_a_retired_keys_archive_is_still_readable() -> None:
    row("Valid/Historical, Admitted=True, a Job running, no new signature", _restores())
    row("a Succeeded restore counts too", _restores(phase="Succeeded"))
    row("MUTANT: a HOLD at admission — Admitted=False",
        not _restores(admitted={"status": "False", "reason": "ApprovalNotVerified"}))
    # RESTORE-ADMITTED-DROPPED's own shape: the controller wrote `Admitted=True`
    # and the next reconcile of the same RUNNING object replaced the condition
    # array without it. The row asked `!= "False"` and passed straight through
    # it; it asks `== "True"` now and FAILS on a lab build that predates the fix
    # on `claude/status-sweep` — the honest reading, because an auditor looking
    # at the object cannot tell it was approved.
    row("MUTANT: the condition is GONE — RESTORE-ADMITTED-DROPPED's own shape",
        not _restores(admitted={}))
    row("MUTANT: no runner Job, so nothing proceeded",
        not _restores(job=None))
    row("MUTANT: still Pending — admitted by nobody",
        not _restores(phase="Pending"))
    row("MUTANT: the archive is not on the historical basis",
        not _restores(verdict={"result": "Valid", "trust": {"basis": "Current"}}))
    row("MUTANT: the archive does not verify at all",
        not _restores(verdict={"result": "Untrusted", "trust": {"basis": "None"}}))
    row("MUTANT: `retired` MEANS NOTHING — a signature made after it is Valid too",
        not _restores(fresh={"result": "Valid", "matchedKeyId": "6607952c"}))


# --- PLAT-15.1: a catalog larger than the view it publishes ------------------
#
# The shapes are the ones `catalog_scale` reads off a `RecoveryCatalog` whose
# archive holds more real signed points than the CRD's smallest `viewLimit`.
SCALE_OK = dict(
    records=104,
    counts={"total": 104, "available": 104, "missing": 0},
    truncated=True,
    entries=100,
    pages=1,
    cursor={"indexShard": "2026-09-21", "complete": True},
)


NEWEST_HUNDRED = [f"lwp1-{i:032x}" for i in range(100)]


def _truncates(**over):
    args = dict(SCALE_OK)
    args.update(over)
    listed = args.pop("listed", NEWEST_HUNDRED)
    newest = args.pop("newest", NEWEST_HUNDRED)
    return d3.view_truncates_honestly(newest_in_archive=newest, listed_ids=listed, **args)


def test_a_truncated_view_says_so_and_counts_the_whole_archive() -> None:
    row("104 real points, a 100-entry view: the flag, the count and the cursor agree",
        all(_truncates().values()), f"{_truncates()}")
    row("MUTANT: the view reports its own page as the whole archive",
        not all(_truncates(counts={"total": 100}).values()))
    row("MUTANT: truncation is not flagged",
        not all(_truncates(truncated=False).values()))
    row("MUTANT: the status field is absent entirely",
        not all(_truncates(truncated=None).values()))
    row("MUTANT: no cursor is published",
        not all(_truncates(cursor={}).values()))
    row("MUTANT: the hundred listed are not the newest hundred",
        not all(_truncates(listed=[f"lwp1-{i:032x}" for i in range(500, 600)]).values()))
    row("MUTANT: a fixture that never exceeded the floor would pass everything else",
        not all(_truncates(records=100, counts={"total": 100}, truncated=False).values()))
    row("MUTANT: more page ConfigMaps than the CRD allows",
        not all(_truncates(pages=9).values()))


CLI_PAGE = """lwp1-aaaa  2026-09-21T12:00:00Z  covered [1, 2)  backup=b run=r logweir/catalog/v1/points/lwp1-aaaa/record.json
lwp1-bbbb  2026-09-21T11:00:00Z  covered [1, 2)  backup=b run=r logweir/catalog/v1/points/lwp1-bbbb/record.json
catalog-listed=2
catalog-unsupported-format=0
catalog-unreadable=0
catalog-inconsistent=0
catalog-searched-days=1
catalog-oldest-day-searched=2026-09-21
catalog-truncated=true
note: these rows come from the UNSIGNED day-sharded index."""


def test_the_cli_page_is_read_as_the_cli_prints_it() -> None:
    page = d3.parse_list(CLI_PAGE)
    row("rows, count, and the truncation line the CLI only prints when it means it",
        page["rows"] == ["lwp1-aaaa", "lwp1-bbbb"] and page["listed"] == 2
        and page["truncated"] is True and page["searchedDays"] == 1, f"{page}")
    full = d3.parse_list(CLI_PAGE.replace("catalog-truncated=true\n", ""))
    row("MUTANT: no truncation line means the page is the whole window",
        full["truncated"] is False)
    row("MUTANT: a page that dropped rows it could not read is not silent",
        d3.parse_list(CLI_PAGE.replace("catalog-unreadable=0", "catalog-unreadable=3"))
        ["unreadable"] == 3)


# --- PLAT-15.1: 403 is "could not tell", never "it is gone" ------------------
DENIED = ["lwp1-1", "lwp1-2"]
READABLE = ["lwp1-3", "lwp1-4"]
PARTIAL = {
    "lwp1-1": {"availability": "Unreadable", "selectable": False},
    "lwp1-2": {"availability": "Unreadable", "selectable": False},
    "lwp1-3": {"availability": "Available", "selectable": True},
    "lwp1-4": {"availability": "Available", "selectable": True},
}
PARTIAL_COUNTS = {"total": 4, "available": 2, "unreadable": 2, "missing": 0}


def test_a_key_scoped_credential_yields_unreadable_and_never_missing() -> None:
    row("two denied objects are Unreadable, the rest Available, nothing Missing",
        all(d3.partial_access_ok(PARTIAL, PARTIAL_COUNTS, DENIED, READABLE).values()))
    gone = dict(PARTIAL, **{"lwp1-1": {"availability": "Missing", "selectable": False}})
    row("MUTANT: a 403 reported as Missing — the defect this row exists for",
        not all(d3.partial_access_ok(gone, dict(PARTIAL_COUNTS, unreadable=1, missing=1),
                                     DENIED, READABLE).values()))
    blanket = {k: {"availability": "Unreadable", "selectable": False} for k in PARTIAL}
    row("MUTANT: a credential denied the WHOLE bucket proves nothing about one prefix",
        not all(d3.partial_access_ok(blanket, dict(PARTIAL_COUNTS, available=0, unreadable=4),
                                     DENIED, READABLE).values()))
    row("MUTANT: the unreadable count is not the number of denied objects",
        not all(d3.partial_access_ok(PARTIAL, dict(PARTIAL_COUNTS, unreadable=1),
                                     DENIED, READABLE).values()))
    row("MUTANT: no denied point at all, so the row asserts over an empty set",
        not all(d3.partial_access_ok(PARTIAL, PARTIAL_COUNTS, [], READABLE).values()))


# --- PLAT-15.1: deleted, corrupted and readable are three answers ------------
MISSING_ENTRY = {"availability": "Missing", "selectable": False}
CONFLICT_ENTRY = {"availability": "Conflict", "selectable": False,
                  "remedy": "the bytes in the bucket are not the ones the signed receipt names"}
CORRUPT_COUNTS = {"total": 5, "available": 2, "missing": 1, "conflict": 1,
                  "unsupportedFormat": 1}


def _corrupt(**over):
    args = dict(corrupt=CONFLICT_ENTRY, missing=MISSING_ENTRY, counts=CORRUPT_COUNTS)
    args.update(over)
    return d3.corrupt_is_not_missing(**args)


def test_a_corrupt_manifest_is_never_reported_as_a_missing_one() -> None:
    row("deleted is Missing, corrupted is Conflict, and the corrupt one carries a remedy",
        all(_corrupt().values()), f"{_corrupt()}")
    row("an unreadable manifest is the other honest answer",
        all(_corrupt(corrupt=dict(CONFLICT_ENTRY, availability="Unreadable"),
                     counts=dict(CORRUPT_COUNTS, conflict=0, unreadable=1)).values()))
    row("MUTANT: the corrupt manifest reported as Missing",
        not all(_corrupt(corrupt=dict(MISSING_ENTRY, remedy="gone"),
                         counts=dict(CORRUPT_COUNTS, missing=2, conflict=0)).values()))
    row("MUTANT: corrupt bytes still Available and still offered",
        not all(_corrupt(corrupt={"availability": "Available", "selectable": True,
                                  "remedy": None}).values()))
    row("MUTANT: the deleted manifest is not Missing either, so the two never differed",
        not all(_corrupt(missing=CONFLICT_ENTRY,
                         counts=dict(CORRUPT_COUNTS, missing=0, conflict=2)).values()))
    row("MUTANT: no remedy sentence on a state nobody can act on",
        not all(_corrupt(corrupt={"availability": "Conflict", "selectable": False}).values()))


# --- PLAT-15.1: a record from a future major --------------------------------
PLANTED = "lwp1-" + "f" * 32
LISTED_FOUR = {f"lwp1-{i}": {"availability": "Available"} for i in range(4)}


def _future(**over):
    args = dict(counts=CORRUPT_COUNTS, listed=LISTED_FOUR, planted=PLANTED, pages=1,
                others=len(d3.ACCESS_POINTS))
    args.update(over)
    return d3.unsupported_format_ok(**args)


def test_a_future_major_is_counted_never_offered_and_never_fatal() -> None:
    row("counted once, absent from the view, the walk published, the rest still listed",
        all(_future().values()), f"{_future()}")
    row("MUTANT: not counted at all — what a camelCase `formatVersion` edit produces",
        not all(_future(counts=dict(CORRUPT_COUNTS, unsupportedFormat=0)).values()))
    row("MUTANT: listed, and therefore offered to a restore",
        not all(_future(listed=dict(LISTED_FOUR, **{PLANTED: {"availability":
                                                              "UnsupportedFormat"}})).values()))
    row("MUTANT: the walk died on it — refusal per catalog instead of per entry",
        not all(_future(pages=0).values()))
    row("MUTANT: the other points vanished with it",
        not all(_future(others=1).values()))


# --- PLAT-14.2: a fresh point resolves the alert, and it is delivered once ---
OPEN = {"kind": "Staleness", "state": "Open", "notifiedTransition": 1,
        "delivery": {"state": "Delivered", "attempts": 1}}
RESOLVED = {"kind": "Staleness", "state": "Resolved", "notifiedTransition": 2,
            "delivery": {"state": "Delivered", "attempts": 1}}


def _resolve(**over):
    args = dict(before=OPEN, after=RESOLVED, posts=1, new_transitions=1,
                point_before="lwp1-old", point_after="lwp1-new")
    args.update(over)
    return d3.resolve_delivered_once(**args)


def test_the_recovery_notification_is_one_delivery_for_one_transition() -> None:
    row("open -> resolved, one new transition, one POST, Delivered",
        all(_resolve().values()), f"{_resolve()}")
    row("MUTANT: the view's newest point never changed — nothing recovered",
        not all(_resolve(point_after="lwp1-old").values()))
    row("MUTANT: the alert never resolved",
        not all(_resolve(after=dict(RESOLVED, state="Open")).values()))
    row("MUTANT: resolved but nothing was sent",
        not all(_resolve(posts=0, new_transitions=0).values()))
    row("MUTANT: two POSTs for one transition",
        not all(_resolve(posts=2).values()))
    row("MUTANT: the resolve was not a new transition, so the row read an old delivery",
        not all(_resolve(after=dict(RESOLVED, notifiedTransition=1)).values()))
    row("MUTANT: three attempts and never delivered",
        not all(_resolve(after=dict(RESOLVED, delivery={"state": "Failed",
                                                        "attempts": 3})).values()))


# --- PLAT-14.2: an archive that can no longer serve its newest point --------
AVAILABLE_ENTRY = {"pointId": "lwp1-new", "availability": "Available", "selectable": True}
GONE_ENTRY = {"pointId": "lwp1-new", "availability": "Missing", "selectable": False}
UNAVAILABLE = {"kind": "ArchiveUnavailable", "state": "Open", "notifiedTransition": 1,
               "delivery": {"state": "Delivered", "attempts": 1}}


def _unavailable(**over):
    args = dict(entry_before=AVAILABLE_ENTRY, entry_after=GONE_ENTRY, before=[RESOLVED],
                after=[RESOLVED, UNAVAILABLE], posts=1, new_transitions=1)
    args.update(over)
    return d3.archive_unavailable_opened(**args)


def test_an_unavailable_archive_opens_the_alert_named_for_it() -> None:
    row("Available -> Missing in the view, one ArchiveUnavailable opened and delivered",
        all(_unavailable().values()), f"{_unavailable()}")
    row("Unreadable is the same trigger", all(_unavailable(
        entry_after=dict(GONE_ENTRY, availability="Unreadable")).values()))
    row("MUTANT: the catalog entry never flipped, so the archive was never broken",
        not all(_unavailable(entry_after=AVAILABLE_ENTRY).values()))
    row("MUTANT: the alert was already open before this window",
        not all(_unavailable(before=[RESOLVED, UNAVAILABLE]).values()))
    row("MUTANT: no alert of that kind at all",
        not all(_unavailable(after=[RESOLVED]).values()))
    row("MUTANT: the transitions this window opened were not delivered one for one",
        not all(_unavailable(posts=0).values()))
    row("MUTANT: the point was not the one the view was offering",
        not all(_unavailable(entry_before=dict(AVAILABLE_ENTRY, selectable=False)).values()))


# --- PLAT-14.2: the word that is never `complete` ---------------------------
SAMPLED_EVENT = {"alert": {"kind": "Staleness", "action": "trigger"},
                 "verification_scope": "sampled",
                 "summary": "protect-recovery: newest available recovery point is 41m old",
                 "last_available_point": {"point_id": "lwp1-old", "evidence": "Valid"}}


def test_a_notification_never_claims_exhaustive_verification() -> None:
    row("`sampled` on every document, and the vocabulary holds",
        all(d3.scope_is_never_complete([SAMPLED_EVENT]).values()))
    row("`none` is honest too",
        all(d3.scope_is_never_complete([dict(SAMPLED_EVENT,
                                             verification_scope="none")]).values()))
    row("MUTANT: `complete`",
        not all(d3.scope_is_never_complete([dict(SAMPLED_EVENT,
                                                 verification_scope="complete")]).values()))
    row("MUTANT: no scope at all — a reader would assume the best",
        not all(d3.scope_is_never_complete([{k: v for k, v in SAMPLED_EVENT.items()
                                             if k != "verification_scope"}]).values()))
    row("MUTANT: the scope is honest and the summary claims an exhaustive comparison",
        not all(d3.scope_is_never_complete(
            [dict(SAMPLED_EVENT, summary="every record completely verified")]).values()))
    row("MUTANT: no events at all is not evidence of a correct label",
        not all(d3.scope_is_never_complete([]).values()))


# --- the planted future-major document, which is a harness row's own honesty -
REAL_RECORD = {"format_version": "1.0.0", "point_id": "lwp1-" + "a" * 32,
               "backup_id": "b", "run_id": "r",
               "receipt": {"key": "logweir/backups/b/r.receipt.json"}}
REAL_INDEX = {"format_version": "1.0.0", "point_id": "lwp1-" + "a" * 32,
              "record_key": "logweir/catalog/v1/points/lwp1-" + "a" * 32 + "/record.json",
              "recovery_point_at_ms": 1789994969454}
PLANT = "lwp1-" + "b" * 32


def test_the_planted_record_really_declares_a_future_major() -> None:
    doc = d3.future_major_document(REAL_RECORD, PLANT)
    row("the document this harness plants declares major 2 under the planted id",
        doc["format_version"] == "2.0.0" and doc["point_id"] == PLANT, f"{doc}")
    row("MUTANT: THE 2026-09-18 PROBE — `formatVersion` beside an untouched "
        "`format_version` is a major-1 record with an unknown field",
        dict(REAL_RECORD, formatVersion="2.0.0", pointId=PLANT)["format_version"] == "1.0.0")
    row("a camelCase key that arrived in the template is not carried over",
        "formatVersion" not in d3.future_major_document(
            dict(REAL_RECORD, formatVersion="2.0.0"), PLANT))
    entry = d3.future_major_index_entry(REAL_INDEX, PLANT)
    row("the index row points at the record the planted id implies",
        entry["point_id"] == PLANT
        and entry["record_key"] == f"{d3.CATALOG_PREFIX}/points/{PLANT}/record.json",
        f"{entry}")
    row("MUTANT: a record_key the point_id does not imply — the reader drops the row as "
        "Inconsistent and the format is never reached",
        dict(REAL_INDEX, point_id=PLANT)["record_key"] != entry["record_key"])


# --- PLAT-14.2: what a policy needs off a Backup to see a point at all -------
#
# The recorded shape is `Backup/recovery-point` on 2026-09-21: a run that
# Succeeded against a destination whose `evidenceRead` is a `SecretKeys` grant,
# on a build that PREDATES D2 §3.9's evidence-fetch Job and a runner that
# predates the receipt digest. It is kept as the historical shape: on the
# build lab-refresh-8 runs, the same destination's point is `Valid` with all
# four facts (`READ_RECEIPT_STATUS`), and the shape a policy still cannot place
# is `NOREAD_STATUS` — a destination with NO `evidenceRead` grant.
PRE_FETCH_SECRETKEYS_STATUS = {
    "phase": "Succeeded", "exitCode": 0, "backupId": "366d2922",
    "evidence": {"receiptKey": "logweir/backups/366d2922/01M32.receipt.json",
                 "sidecarKey": "logweir/backups/366d2922/01M32.receipt.sig",
                 # THE VERDICT IS WRITTEN. `NotAttempted` with a sentence
                 # naming the grant is what D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN's
                 # fix landed for. The first draft of this fixture said
                 # `result: None`, which made the defect row claim the
                 # controller says nothing — what it says is exact, and the
                 # failure is in how protection READS it.
                 "verification": {"result": "NotAttempted",
                                  "verifiedAt": "2026-09-21T13:26:21Z",
                                  "detail": "…reads evidence with a grant only a pod may "
                                            "hold (D2 §3.9's evidence-fetch Job)…"}},
}
# `protection-verdicts` row 1's point since the evidence-fetch Job: the runner
# reports its receipt digest, nothing may read the receipt, so no capture.
NOREAD_STATUS = {
    "phase": "Succeeded", "exitCode": 0, "backupId": "4b1e0d77",
    "evidence": {"receiptKey": "logweir/backups/4b1e0d77/01M40.receipt.json",
                 "sidecarKey": "logweir/backups/4b1e0d77/01M40.receipt.sig",
                 "receiptSha256": "sha256:" + "b" * 64,
                 "verification": {"result": "NotAttempted",
                                  "verifiedAt": "2026-09-22T20:10:00Z",
                                  "detail": "BackupDestination ns/dest-noread declares no "
                                            "evidenceRead grant; nothing was verified"}},
}
PENDING_STATUS = {
    "phase": "Succeeded", "exitCode": 0,
    "evidence": {"receiptSha256": "sha256:" + "b" * 64,
                 "verification": {"result": "Pending"},
                 "observation": {"mode": "SecretKeys", "attempt": 1,
                                 "jobRef": {"name": "lwc-ev-0123456789abcdef0123"}}},
}
READ_RECEIPT_STATUS = {
    "phase": "Succeeded", "exitCode": 0,
    "capture": {"startedAt": "2026-09-21T13:06:49Z", "finishedAt": "2026-09-21T13:06:58Z"},
    "evidence": {"receiptSha256": "sha256:" + "a" * 64,
                 "verification": {"result": "Valid"}},
}


CAPTURE = "capture.startedAt — D3 §3.2's recoveryPointAt"
DIGEST = "evidence.receiptSha256 — the point id is its first 128 bits"
WRITTEN = "evidence.verification.result is written at all"
SATISFIED = "…and it satisfies requireVerifiedEvidence (Valid/ValidHistorical)"


def test_a_policy_cannot_place_a_point_it_has_no_facts_about() -> None:
    have = d3.point_facts_the_policy_needs(READ_RECEIPT_STATUS)
    row("a Backup whose receipt WAS read carries all four facts", all(have.values()), f"{have}")
    blind = d3.point_facts_the_policy_needs(PRE_FETCH_SECRETKEYS_STATUS)
    row("THE PRE-FETCH SHAPE: a SecretKeys destination on a build with no evidence-fetch Job "
        "lost both facts and kept a written verdict that does not satisfy the objective",
        not blind[CAPTURE] and not blind[DIGEST] and blind[WRITTEN] and not blind[SATISFIED],
        f"{blind}")
    noread = d3.point_facts_the_policy_needs(NOREAD_STATUS)
    row("THE LIVE SHAPE NOW: no evidenceRead grant keeps the runner's digest and loses the "
        "capture, with a written verdict that does not satisfy the objective",
        not noread[CAPTURE] and noread[DIGEST] and noread[WRITTEN] and not noread[SATISFIED],
        f"{noread}")
    pending = d3.point_facts_the_policy_needs(PENDING_STATUS)
    row("a fetch still Pending is written and NOT satisfied — no capture until the Job's Valid",
        pending[WRITTEN] and not pending[SATISFIED] and not pending[CAPTURE], f"{pending}")
    row("MUTANT: reading `written at all` as the objective being met would report this "
        "candidate as verified", blind[WRITTEN] is True and blind[SATISFIED] is False)
    row("MUTANT: a verdict block that is genuinely absent is a different fact again",
        not d3.point_facts_the_policy_needs({"evidence": {"receiptKey": "k"}})[WRITTEN])
    verdict_only = d3.point_facts_the_policy_needs(
        {"evidence": {"verification": {"result": "Valid"}}})
    row("a verdict alone is neither a point id nor a time",
        verdict_only[WRITTEN] and verdict_only[SATISFIED]
        and not verdict_only[CAPTURE] and not verdict_only[DIGEST], f"{verdict_only}")
    partial = d3.point_facts_the_policy_needs(dict(READ_RECEIPT_STATUS, capture={}))
    row("the capture clause fails on its own", not partial[CAPTURE] and partial[DIGEST])


# --- the echo sink's own counter --------------------------------------------
#
# The recorded shape is `/tmp/posts.log` on 2026-09-21 after five deliveries:
# a request ends with its body and no newline, so the next request's `POST`
# continues that same line.
ONE_POST = "POST /alerts HTTP/1.1\r\nHost: 10.1.36.254:8080\r\n\r\n{\"alert\":1}"
# The same request to a sink mounted on another route: the fact counted is the
# METHOD, so a route change must not zero the counter the way the line-anchored
# version did.
ONE_POST_OTHER_ROUTE = ONE_POST.replace("/alerts", "/hook")


def test_the_sink_counts_every_post_and_not_every_line() -> None:
    row("one request is one POST", d3.sink_post_count(ONE_POST) == 1)
    concatenated = ONE_POST + ONE_POST + ONE_POST
    row("three requests concatenated on one line are three POSTs",
        d3.sink_post_count(concatenated) == 3, f"{d3.sink_post_count(concatenated)}")
    row("MUTANT: THE FROZEN COUNTER — counting lines that BEGIN with POST reports 1 "
        "for all three, which is what `grep -c '^POST'` did",
        len([ln for ln in concatenated.splitlines() if ln.startswith("POST")]) == 1)
    row("an empty log is zero, not an error", d3.sink_post_count("") == 0)
    row("MUTANT: keying on the route would zero the counter the day the sink moves",
        d3.sink_post_count(ONE_POST_OTHER_ROUTE) == 1
        and ONE_POST_OTHER_ROUTE.count("POST /alerts") == 0)


# --- the view after CR loss, and the key a restore binds to -----------------
#
# Recorded from `catalog/view-entries-after-cr-loss.json` of the 2026-09-19 run
# (`lr520260919t0109z`), with `receiptKey` restored to what
# `catalog/archive-objects-before.json` of the SAME capture shows the runner
# actually wrote: the capture's view column held
# `"[redacted].receipt.json"`, which is the defect
# CATALOG-RECEIPTKEY-REDACTED itself.
_RECONSTRUCTED = [
    {"backupId": "e1c4ff19-62fb-4d76-9a3f-2177d6fd9d4a",
     "runId": "01M2VKCST7EF12EW5T2Y7SJ86Q",
     "receiptKey": "logweir/backups/e1c4ff19-62fb-4d76-9a3f-2177d6fd9d4a/"
                   "01M2VKCST7EF12EW5T2Y7SJ86Q.receipt.json",
     "availability": "Available", "verification": "Verified", "selectable": True},
    {"backupId": "bca2d600-d37e-4149-8dc0-c15caf496e4a",
     "runId": "01M2VKCKQSTCRDVMPD866R0WP9",
     "receiptKey": "logweir/backups/bca2d600-d37e-4149-8dc0-c15caf496e4a/"
                   "01M2VKCKQSTCRDVMPD866R0WP9.receipt.json",
     "availability": "Available", "verification": "Verified", "selectable": True},
    {"backupId": "a97f49e0-a48a-40b4-831d-12a1219cc0f2",
     "runId": "01M2VKCDQBZJ34Q5J12KJBE05A",
     "receiptKey": "logweir/backups/a97f49e0-a48a-40b4-831d-12a1219cc0f2/"
                   "01M2VKCDQBZJ34Q5J12KJBE05A.receipt.json",
     "availability": "Available", "verification": "Verified", "selectable": True},
]
_EXPECTED_BACKUPS = {e["backupId"] for e in _RECONSTRUCTED}


def test_the_reconstructed_view_publishes_the_key_a_restore_binds_to() -> None:
    """CATALOG-RECEIPTKEY-REDACTED — and the row that could not fail.

    `catalog-reconstruction-after-cr-loss` asked only for the count and the two
    axes, so it PASSED on the 2026-09-19 refresh while all three points
    published `receiptKey: "[redacted].receipt.json"`. A live row that cannot
    fail when the defect is present is not evidence, so the mutants below are
    the point of this test: each one is the shape the capture really had, or a
    plausible near miss, and each must be refused.
    """
    ok = d3.reconstructed_view_ok(_RECONSTRUCTED, _EXPECTED_BACKUPS, 3)
    row("the reconstructed view: three points, both axes, and the whole receipt key",
        all(ok.values()), f"{ok}")

    redacted = json.loads(json.dumps(_RECONSTRUCTED))
    for e in redacted:
        e["receiptKey"] = "[redacted].receipt.json"
    bad = d3.reconstructed_view_ok(redacted, _EXPECTED_BACKUPS, 3)
    row("MUTANT: the capture's own shape — `[redacted].receipt.json` — is refused",
        not all(bad.values())
        and not bad["no published receiptKey carries the redaction marker"]
        and not bad["every receiptKey is the key the backup runner wrote"],
        f"{bad}")

    planted = json.loads(json.dumps(_RECONSTRUCTED))
    planted[1]["receiptKey"] = planted[1]["receiptKey"].replace(
        planted[1]["runId"], "[redacted]")
    row("MUTANT: ONE point with the marker planted mid-key is refused",
        not all(d3.reconstructed_view_ok(planted, _EXPECTED_BACKUPS, 3).values()))

    truncated = json.loads(json.dumps(_RECONSTRUCTED))
    truncated[0]["receiptKey"] = "logweir/backups/x/y.receipt.json"
    row("MUTANT: a key that is not derived from this point's own ids is refused",
        not all(d3.reconstructed_view_ok(truncated, _EXPECTED_BACKUPS, 3).values()))

    short = json.loads(json.dumps(_RECONSTRUCTED))
    short[2]["runId"] = "run-a"
    short[2]["receiptKey"] = (
        f"logweir/backups/{short[2]['backupId']}/run-a.receipt.json")
    row("MUTANT: a short fake run id passes the derivation but is not a ULID, "
        "so it cannot stand in for the shape that made the key redactable",
        not all(d3.reconstructed_view_ok(short, _EXPECTED_BACKUPS, 3).values()))

    row("MUTANT: an empty view satisfies no clause vacuously",
        not all(d3.reconstructed_view_ok([], set(), 0).values()))


# --- PLAT-14.2: the three protection arms, over planted status blocks -------
#
# Every predicate `protection_verdicts` decides from, exercised without a
# cluster. Each is driven TWICE at least: once over the shape the FIXED
# controller publishes (`docs/kubernetes.md`'s "A point whose receipt the
# controller could not read", D3 §3.2's health table and §3.3's resolve
# column), and once over a shape that must be REFUSED — the pre-fix answer, the
# neighbouring `Unknown` cause, the rewritten verdict, the resolve that D3 does
# not allow. The sentences are the controller's own (`protection::summarize`),
# copied so a row cannot pass against a message the product never writes.

_UNREAD_MESSAGE = (
    "a run for this policy succeeded, but its recovery point could not be placed in time: "
    "the controller read no verification verdict for it and so holds no capture time, and an "
    "age cannot be compared to the objective of 1d 0h. This is not a pass and not a failure"
)
_UNPROTECTED_MESSAGE = "there is no available recovery point for this policy at all"
_HEALTHY_MESSAGE = (
    "the newest available recovery point is 3m old, inside the objective of 1d 0h"
)
_CATALOG_STALE_MESSAGE = (
    "protection could not be evaluated (CatalogStale); this is not a pass and not a failure"
)

# The catalog row for row 1's point (a destination with no `evidenceRead`
# grant; it was a `SecretKeys` one before the evidence-fetch Job), as the view
# publishes it: both of
# D3 §5.4's axes, the archive set id the join runs on where the point has no
# receipt-derived identity, and `recoveryPointAtMs`, which IS the receipt's
# `started_at` carried through.
_ENTRY = {
    "pointId": "lwp1-622d7a41",
    "backupId": "bk-01jw8y0e2n",
    "recoveryPointAtMs": 1_790_012_000_000,
    "availability": "Available",
    "verification": "Verified",
    "selectable": True,
}
_PLACED_POINT = {
    "pointId": "lwp1-622d7a41",
    "recoveryPointAt": "2026-09-21T17:33:20Z",
    "ageSeconds": 180,
    # NOT REWRITTEN. The controller still did not read this receipt; the
    # catalog answered availability, not verification (D3 §5.4).
    "evidence": "NotAttempted",
}


_COUNTED = {"backupRef": {"name": "the-run"}, "phase": "Succeeded",
            "at": "2026-09-21T17:33:20Z"}


def _policy(health, protected, reason, message, *, point=None, alerts=None,
            basis=None, generation=3, attempt=_COUNTED):
    return {
        "metadata": {"name": "p", "generation": generation},
        "status": {
            "health": health,
            "availabilityBasis": basis,
            "lastAvailablePoint": point,
            "lastAttempt": attempt,
            "alerts": alerts or [],
            "observedGeneration": generation,
            "evaluatedAt": "2026-09-21T17:36:00Z",
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "Evaluated", "message": "ok"},
                {"type": "Protected", "status": protected, "reason": reason,
                 "message": message},
            ],
        },
    }


def _alert(state, *, transition=1, notified=1, delivery="Delivered", kind="Staleness"):
    return {"key": "logweir-protection-uid-staleness", "kind": kind, "state": state,
            "transition": transition, "notifiedTransition": notified,
            "delivery": {"state": delivery}}


_UNKNOWN_POLICY = _policy("Unknown", "Unknown", "PointFactsUnread", _UNREAD_MESSAGE)
_PLACED_POLICY = _policy("Healthy", "True", "WithinObjective", _HEALTHY_MESSAGE,
                         point=_PLACED_POINT, basis="Catalog")
# WHAT `af64073` ANSWERS on the same objects, and the whole of the defect:
# `Unprotected` is D3 §3.2's "no available point at all", and it PAGES.
_PREFIX_DEFECT_POLICY = _policy("Unprotected", "False", "NoAvailablePoint",
                                _UNPROTECTED_MESSAGE, alerts=[_alert("Open")],
                                basis="Catalog")


def test_unplaceable_point_is_unknown_not_unprotected() -> None:
    ok = d3.unread_point_is_unknown(d3.policy_view(_UNKNOWN_POLICY), "the-run")
    row("PointFactsUnread: Unknown/Unknown/PointFactsUnread with no point published",
        all(ok.values()), f"{ok}")

    defect = d3.unread_point_is_unknown(d3.policy_view(_PREFIX_DEFECT_POLICY), "the-run")
    row("MUTANT: `af64073`'s Unprotected/NoAvailablePoint — the defect — is refused on "
        "every clause that names the answer (it publishes no point either, which is the "
        "one thing the two states agree about)",
        not defect["health is Unknown — D3 §3.2's `evaluation impossible`, never `Unprotected`"]
        and not defect["the Protected condition mirrors it (`Unknown` for `Unknown`)"]
        and not defect["with reason PointFactsUnread, and not another Unknown cause"]
        and not defect["and a message naming the READ rather than a missing object"],
        f"{defect}")

    # The OTHER `Unknown` causes. D3 §3.2 lands five different failures on
    # `Unknown`; a row that accepted any of them would pass on a policy whose
    # catalog went stale and would say nothing about a point that could not be
    # placed.
    stale = d3.unread_point_is_unknown(d3.policy_view(
        _policy("Unknown", "Unknown", "CatalogStale", _CATALOG_STALE_MESSAGE)), "the-run")
    row("MUTANT: Unknown for a STALE CATALOG is not this row's Unknown",
        not stale["with reason PointFactsUnread, and not another Unknown cause"]
        and not stale["and a message naming the READ rather than a missing object"],
        f"{stale}")

    # A reason string with the generic message under it: the reason is right
    # and the sentence sends an operator looking for a missing object.
    generic = d3.unread_point_is_unknown(d3.policy_view(
        _policy("Unknown", "Unknown", "PointFactsUnread",
                "protection could not be evaluated (PointFactsUnread); this is not a pass "
                "and not a failure")), "the-run")
    row("MUTANT: the reason without the sentence that names the READ is refused",
        not generic["and a message naming the READ rather than a missing object"],
        f"{generic}")

    # `Unknown` while still publishing a point would be a policy claiming it
    # cannot evaluate and naming what it evaluated.
    contradictory = d3.unread_point_is_unknown(d3.policy_view(
        _policy("Unknown", "Unknown", "PointFactsUnread", _UNREAD_MESSAGE,
                point=_PLACED_POINT)), "the-run")
    row("MUTANT: Unknown that still publishes a lastAvailablePoint is refused",
        not contradictory["nothing is published as the newest available point"])


def test_the_catalog_places_the_point_and_does_not_rewrite_the_verdict() -> None:
    ok = d3.placed_point_is_protected(d3.policy_view(_PLACED_POLICY), _ENTRY, "the-run")
    row("the control: Healthy/Protected=True, the capture time and the id off the row, "
        "verdict still NotAttempted",
        all(ok.values()), f"{ok}")

    everything_unread = d3.placed_point_is_protected(
        d3.policy_view(_UNKNOWN_POLICY), _ENTRY, "the-run")
    row("MUTANT: a controller answering PointFactsUnread for EVERY unverified point "
        "fails the control, which is what makes the control a control",
        not any(v for k, v in everything_unread.items()
                if k != "the policy counted this run — status.lastAttempt names it"),
        f"{everything_unread}")

    # THE VACUITY THE FIRST LIVE RUN PRODUCED, as a row. A policy whose
    # `protects.topics` excludes every point has an EMPTY candidate set, and
    # `Unprotected` over nothing looks exactly like `Unprotected` over a point
    # it refused. `status.lastAttempt` is the one field that tells them apart.
    somebody_elses = d3.placed_point_is_protected(
        d3.policy_view(_policy("Unprotected", "False", "NoAvailablePoint",
                               _UNPROTECTED_MESSAGE, attempt=None)), _ENTRY, "the-run")
    row("MUTANT: a verdict about no run at all — lastAttempt absent — is refused",
        not somebody_elses["the policy counted this run — status.lastAttempt names it"])
    other_run = d3.refused_signature_is_unprotected(
        d3.policy_view(_policy("Unprotected", "False", "NoAvailablePoint",
                               _UNPROTECTED_MESSAGE, alerts=[_alert("Open")],
                               attempt={"backupRef": {"name": "someone-elses-run"}})),
        "Invalid", _ENTRY, "the-run")
    row("MUTANT: Unprotected about a DIFFERENT run is not a measurement of this one — "
        "the vacuity the first live run of `protection_verdicts` produced",
        not other_run["the policy counted this run — status.lastAttempt names it"]
        and all(v for k, v in other_run.items()
                if k != "the policy counted this run — status.lastAttempt names it"),
        f"{other_run}")

    rewritten = json.loads(json.dumps(_PLACED_POLICY))
    rewritten["status"]["lastAvailablePoint"]["evidence"] = "Valid"
    forged = d3.placed_point_is_protected(d3.policy_view(rewritten), _ENTRY, "the-run")
    row("MUTANT: a verdict rewritten to Valid — the catalog answering VERIFICATION, "
        "which D3 §5.4 keeps on its own axis — is refused",
        not forged["the verdict is NOT rewritten — it still reads NotAttempted"]
        and all(v for k, v in forged.items()
                if k != "the verdict is NOT rewritten — it still reads NotAttempted"),
        f"{forged}")

    other_row = dict(_ENTRY, recoveryPointAtMs=_ENTRY["recoveryPointAtMs"] - 7_200_000)
    wrong_time = d3.placed_point_is_protected(d3.policy_view(_PLACED_POLICY), other_row,
                                              "the-run")
    row("MUTANT: a capture time that is not THIS row's is refused — a point placed from "
        "the wrong entry looks exactly like a measurement",
        not wrong_time["which is the catalog row's own recoveryPointAtMs, to the second"])

    other_id = dict(_ENTRY, pointId="lwp1-deadbeef")
    wrong_id = d3.placed_point_is_protected(d3.policy_view(_PLACED_POLICY), other_id, "the-run")
    row("MUTANT: an identity that is not the row's is refused",
        not wrong_id["and the catalog row's own point id"])

    kubernetes_basis = json.loads(json.dumps(_PLACED_POLICY))
    kubernetes_basis["status"]["availabilityBasis"] = "KubernetesStatus"
    basis = d3.placed_point_is_protected(d3.policy_view(kubernetes_basis), _ENTRY, "the-run")
    row("MUTANT: Healthy decided from Kubernetes status alone does not prove the catalog "
        "placed anything",
        not basis["availability was decided by the catalog, and says so"])

    nothing = d3.placed_point_is_protected(
        d3.policy_view(_policy("Healthy", "True", "WithinObjective", _HEALTHY_MESSAGE,
                               basis="Catalog")), _ENTRY, "the-run")
    row("MUTANT: Healthy with no point published satisfies no clause vacuously",
        not nothing["a point is published with a capture time"])


_REFUSED_POLICY = _policy("Unprotected", "False", "NoAvailablePoint", _UNPROTECTED_MESSAGE,
                          alerts=[_alert("Open")])
_SOUND_POLICY = _policy(
    "Healthy", "True", "WithinObjective", _HEALTHY_MESSAGE,
    point={"pointId": "lwp1-91ac0f33", "recoveryPointAt": "2026-09-21T17:40:00Z",
           "ageSeconds": 60, "evidence": "Valid"},
    alerts=[_alert("Resolved", transition=2, notified=2)], basis="Catalog")


def test_a_refused_signature_is_unprotected_at_every_posture() -> None:
    for verdict in ("Untrusted", "Invalid"):
        ok = d3.refused_signature_is_unprotected(
            d3.policy_view(_REFUSED_POLICY), verdict, _ENTRY, "the-run")
        row(f"refused signature ({verdict}) with a catalogRef: Unprotected, Staleness Open, "
            f"and the catalog row that could have rescued it did not",
            all(ok.values()), f"{ok}")
    no_catalog = d3.refused_signature_is_unprotected(
        d3.policy_view(_REFUSED_POLICY), "Untrusted", None, "the-run")
    row("refused signature with no catalogRef: the same answer, and no rescue clause",
        all(no_catalog.values())
        and "the catalog held ONE row for this point, so it COULD have placed it"
        not in no_catalog)

    # MEDIUM-1 of `claude/fix-protection.review-2.md`, measured on the branch it
    # was found on: `requireVerifiedEvidence: false` + a reached-and-refused
    # verdict read `Healthy`, `Protected=True`, nothing open, publishing
    # `evidence: "Invalid"` beside it.
    rescued = d3.refused_signature_is_unprotected(
        d3.policy_view(_policy(
            "Healthy", "True", "WithinObjective", _HEALTHY_MESSAGE,
            point={"pointId": "lwp1-622d7a41", "recoveryPointAt": "2026-09-21T17:33:20Z",
                   "ageSeconds": 180, "evidence": "Invalid"}, basis="Catalog")),
        "Invalid", _ENTRY, "the-run")
    row("MUTANT: MEDIUM-1's shape — a catalog row rescuing a REFUSED verdict into Healthy "
        "— is refused on every clause that matters",
        not rescued["health is Unprotected — D3 §3.2's `no available point at all`"]
        and not rescued["Protected=False"]
        and not rescued["the Staleness incident is Open — this PAGES"]
        and not rescued["nothing is published as the newest available point"],
        f"{rescued}")

    # HIGH-1b inverted: `Unknown` with `PointFactsUnread`'s sentence says the
    # controller read no verdict, which is FALSE about a point it refused — and
    # `Unknown` opens no alert, so it stops paging an installation whose own
    # TrustPolicy rejects the archive.
    as_unread = d3.refused_signature_is_unprotected(
        d3.policy_view(_UNKNOWN_POLICY), "Untrusted", None, "the-run")
    row("MUTANT: HIGH-1b's shape — a refused point reported Unknown/PointFactsUnread — is "
        "refused, including on the message clause",
        not as_unread["health is Unprotected — D3 §3.2's `no available point at all`"]
        and not as_unread[
            "and the message does NOT say the controller read no verdict — it read one"],
        f"{as_unread}")

    not_reached = d3.refused_signature_is_unprotected(
        d3.policy_view(_REFUSED_POLICY), "NotAttempted", _ENTRY, "the-run")
    row("MUTANT: `NotAttempted` is not a refusal — the fixture clause refuses it, so this "
        "row can never be satisfied by row 1's point",
        not not_reached["the controller REACHED a verdict, and it refuses the signature"])

    empty_view = d3.refused_signature_is_unprotected(
        d3.policy_view(_REFUSED_POLICY), "Untrusted", {}, "the-run")
    row("MUTANT: a catalogRef cell whose view held NO row for the point proves nothing "
        "about rescue, and says so",
        not empty_view["the catalog held ONE row for this point, so it COULD have placed it"])

    silent = json.loads(json.dumps(_REFUSED_POLICY))
    silent["status"]["alerts"] = []
    quiet = d3.refused_signature_is_unprotected(d3.policy_view(silent), "Untrusted", _ENTRY,
                                                "the-run")
    row("MUTANT: Unprotected that opens no incident does not page, and is refused",
        not quiet["the Staleness incident is Open — this PAGES"])


def test_a_valid_signature_is_the_control_that_requires_protection() -> None:
    ok = d3.valid_signature_is_protected(d3.policy_view(_SOUND_POLICY), "Valid", "the-run")
    row("the control: the same archive under a trusted signer is Healthy/Protected=True",
        all(ok.values()), f"{ok}")
    always_unprotected = d3.valid_signature_is_protected(
        d3.policy_view(_REFUSED_POLICY), "Valid", "the-run")
    row("MUTANT: a controller answering Unprotected for EVERY point on this archive would "
        "pass all four refused cells and fails the control",
        not any(v for k, v in always_unprotected.items()
                if k not in {"the same archive under a trusted signer verifies Valid",
                             "the policy counted this run — status.lastAttempt names it"}),
        f"{always_unprotected}")
    unread_verdict = d3.valid_signature_is_protected(d3.policy_view(_PLACED_POLICY),
                                                     "NotAttempted", "the-run")
    row("MUTANT: a point that is Healthy because the CATALOG placed it is not this "
        "control — the control is about a verdict the controller reached",
        not unread_verdict["the same archive under a trusted signer verifies Valid"]
        and not unread_verdict["carrying the verdict the controller reached"],
        f"{unread_verdict}")


def test_the_resolve_column_closes_an_incident_exactly_once() -> None:
    before = _alert("Open", transition=1, notified=1, delivery="Delivered")
    after = _alert("Resolved", transition=2, notified=2, delivery="Delivered")
    ok = d3.incident_resolves_exactly_once(before, after, "Healthy", 1, 1)
    row("D3 §3.3 resolve column: Open -> Resolved at Healthy, one transition, one POST",
        all(ok.values()), f"{ok}")
    row("AtRisk resolves too — the column is two values, not one",
        all(d3.incident_resolves_exactly_once(before, after, "AtRisk", 1, 1).values()))

    for health in ("Unknown", "Stale", "Unprotected"):
        bad = d3.incident_resolves_exactly_once(before, after, health, 1, 1)
        row(f"MUTANT: a resolve at health {health} is not \"back to Healthy/AtRisk\"",
            not bad["health came back to Healthy/AtRisk — D3 §3.3's resolve column"])

    stuck = d3.incident_resolves_exactly_once(before, _alert("Open", transition=1, notified=1),
                                              "Healthy", 0, 0)
    row("MUTANT: an incident that never closed is refused",
        not stuck["the incident is Resolved after it"]
        and not stuck["one POST per transition this window opened, and no more"])

    twice = d3.incident_resolves_exactly_once(
        before, _alert("Resolved", transition=3, notified=3), "Healthy", 2, 2)
    row("MUTANT: two notified transitions for one close is not `exactly once`",
        not twice["which is exactly one new notified transition on this incident"])

    unbalanced = d3.incident_resolves_exactly_once(before, after, "Healthy", 3, 1)
    row("MUTANT: three POSTs for one transition breaks D3 §3.3's dedup rule",
        not unbalanced["one POST per transition this window opened, and no more"])

    silent = d3.incident_resolves_exactly_once(before, after, "Healthy", 0, 0)
    row("MUTANT: a resolve nobody was told about is refused (0 POSTs, 0 transitions)",
        not silent["one POST per transition this window opened, and no more"])

    attempted = d3.incident_resolves_exactly_once(
        before, _alert("Resolved", transition=2, notified=2, delivery="Failed"),
        "Healthy", 1, 1)
    row("MUTANT: `Failed` delivery is attempted, not delivered",
        not attempted["and the delivery says Delivered, not merely attempted"])

    # lab-refresh-7 04:02Z, verbatim: the window opened while transition 1's
    # delivery was still `Pending`, so its POST landed inside it — posts 2,
    # newTransitions 1 — and the row blamed the product's dedup rule.
    pending = _alert("Open", transition=1, notified=1, delivery="Pending")
    in_flight = d3.deliveries_in_flight([pending])
    row("a Pending delivery is IN FLIGHT — its POST is still owed",
        in_flight == [{"kind": "Staleness", "transition": 1, "delivery": "Pending"}])
    row("an alert with no delivery decided yet is in flight too",
        len(d3.deliveries_in_flight([{"kind": "Staleness", "transition": 1}])) == 1)
    row("Delivered, Failed and Suppressed are finished — no POST is owed for any of them",
        not d3.deliveries_in_flight([_alert("Open", delivery=state)
                                     for state in ("Delivered", "Failed", "Suppressed")]))
    raced = d3.incident_resolves_exactly_once(before, after, "Healthy", 2, 1,
                                              in_flight_at_open=in_flight)
    row("MUTANT: THE RACE — a window opened over a Pending delivery is named, not "
        "passed off as a dedup verdict",
        not raced["the POST window opened with no earlier delivery still in flight"])
    quiet_but_doubled = d3.incident_resolves_exactly_once(
        before, after, "Healthy", 2, 1, in_flight_at_open=[])
    row("MUTANT: a quiet window with TWO POSTs for one transition still fails — the row can "
        "still catch a real duplicate delivery",
        quiet_but_doubled["the POST window opened with no earlier delivery still in flight"]
        and not quiet_but_doubled["one POST per transition this window opened, and no more"])


def test_unknown_never_clears_an_open_incident() -> None:
    before = _alert("Open", transition=1, notified=1)
    unchanged = _alert("Open", transition=1, notified=1)
    ok = d3.incident_stays_open_on_unknown(
        before, unchanged, d3.policy_view(_UNKNOWN_POLICY), 0)
    row("D3 §3.3, the other direction: Unknown/PointFactsUnread keeps the incident open, "
        "un-renotified, undelivered",
        all(ok.values()), f"{ok}")

    resolved = d3.incident_stays_open_on_unknown(
        before, _alert("Resolved", transition=2, notified=2),
        d3.policy_view(_UNKNOWN_POLICY), 1)
    row("MUTANT: a controller that resolved on `no longer Stale` closes a real incident "
        "because it stopped being able to look — refused on four clauses",
        not resolved["the incident is STILL Open — `Unknown` is not back to Healthy/AtRisk"]
        and not resolved["no new transition was recorded"]
        and not resolved["it was not re-notified"]
        and not resolved["and nothing was delivered for it"],
        f"{resolved}")

    renotified = d3.incident_stays_open_on_unknown(
        before, _alert("Open", transition=2, notified=2),
        d3.policy_view(_UNKNOWN_POLICY), 1)
    row("MUTANT: re-paging an incident the policy can no longer measure is refused",
        not renotified["no new transition was recorded"]
        and not renotified["it was not re-notified"])

    still_unprotected = d3.incident_stays_open_on_unknown(
        before, unchanged, d3.policy_view(_PREFIX_DEFECT_POLICY), 0)
    row("MUTANT: `af64073` keeps the incident open too, because it stays Unprotected — "
        "which is the defect and not the rule, so the health clause refuses it",
        not still_unprotected["the policy is now Unknown/PointFactsUnread"]
        and all(v for k, v in still_unprotected.items()
                if k != "the policy is now Unknown/PointFactsUnread"),
        f"{still_unprotected}")


def test_an_unmeasurable_policy_neither_pages_nor_unpages() -> None:
    open_staleness = [_alert("Open")]
    row("nothing opened, nothing closed, no transition",
        all(d3.alert_ledger_unchanged(open_staleness, open_staleness).values()))
    row("an empty ledger that stays empty is unchanged",
        all(d3.alert_ledger_unchanged([], []).values()))
    opened = d3.alert_ledger_unchanged(
        [], [_alert("Open", kind="ArchiveUnavailable")])
    row("MUTANT: an alert kind that opened is refused",
        not opened["no alert kind opened that was not open before"]
        and not opened["and no transition was recorded at all"])
    closed = d3.alert_ledger_unchanged(
        open_staleness, [_alert("Resolved", transition=2, notified=2)])
    row("MUTANT: an open incident that was resolved is refused",
        not closed["nothing that was open was resolved"]
        and not closed["and no transition was recorded at all"])


def test_policy_view_reads_the_condition_the_rows_decide_from() -> None:
    view = d3.policy_view(_PLACED_POLICY)
    row("policy_view: health, basis, point, and the Protected condition's three fields",
        view["health"] == "Healthy" and view["availabilityBasis"] == "Catalog"
        and view["protectedStatus"] == "True" and view["protectedReason"] == "WithinObjective"
        and view["lastAvailablePoint"] == _PLACED_POINT
        and view["observedGeneration"] == view["generation"] == 3,
        f"{view}")
    # `Ready` is first in the list and carries `status: True` and a reason of
    # its own; a view that read the first condition would report every policy
    # as Protected=True/Evaluated.
    row("MUTANT: the Protected condition is selected by TYPE, not by position — `Ready` "
        "is first in the list and would report every policy as Evaluated",
        view["protectedReason"] == "WithinObjective"
        and view["protectedMessage"] == _HEALTHY_MESSAGE
        and _PLACED_POLICY["status"]["conditions"][0]["reason"] == "Evaluated")
    empty = d3.policy_view({"metadata": {"name": "p"}})
    row("a policy with no status at all reads as absent, not as healthy",
        empty["health"] is None and empty["protectedStatus"] is None
        and empty["alerts"] == [] and not empty["lastAvailablePoint"])


def test_the_catalog_row_is_joined_on_the_archive_set_id() -> None:
    backup = {"status": {"backupId": "bk-01jw8y0e2n"}}
    entries = [dict(_ENTRY), dict(_ENTRY, pointId="lwp1-otherpoint", backupId="bk-other")]
    row("the ONE row for this archive set is the one the join returns",
        d3.entry_for_backup(entries, backup)["pointId"] == "lwp1-622d7a41")
    row("MUTANT: no row for this set fills nothing",
        d3.entry_for_backup([entries[1]], backup) == {})
    row("MUTANT: TWO rows for one archive set are two POINTS — ambiguity fills nothing "
        "rather than picking one",
        d3.entry_for_backup([dict(_ENTRY), dict(_ENTRY, pointId="lwp1-twin")], backup) == {})
    row("MUTANT: a Backup with no archive set id matches nothing",
        d3.entry_for_backup(entries, {"status": {}}) == {})


def test_the_capture_time_comparison_is_in_milliseconds() -> None:
    row("an RFC3339 Time is epoch milliseconds",
        d3.rfc3339_ms("2026-09-21T17:33:20Z") == 1_790_012_000_000)
    # THE SHAPE THIS RUN'S OWN ARTIFACTS CARRY. `status.evaluatedAt` read
    # `2026-09-22T02:28:38.193596887Z` on the first live run, and a parser
    # that took whole seconds only would answer `None` for a real capture time
    # — the clause comparing it to the catalog row would then fail against a
    # product that was right.
    row("nanosecond precision, as logweir.dev's Time actually serializes it",
        d3.rfc3339_ms("2026-09-21T17:33:20.193596887Z") == 1_790_012_000_193)
    row("and a millisecond fraction is not truncated to the second",
        d3.rfc3339_ms("2026-09-21T17:33:20.500Z") == 1_790_012_000_500)
    row("absent is None, and None never compares equal to a row's timestamp",
        d3.rfc3339_ms(None) is None and d3.rfc3339_ms("") is None)
    row("MUTANT: an unparseable timestamp is None, not a silent zero",
        d3.rfc3339_ms("not-a-time") is None and d3.rfc3339_ms("2026-09-21T17:33:20.xyzZ")
        is None)
    # `.` is 0x2E and `Z` is 0x5A, so the fractional form sorts BEFORE the
    # whole-second one: the string comparison this replaced said a controller
    # that HAD re-evaluated had not.
    row("moved_past compares instants, not bytes",
        d3.moved_past("2026-09-22T02:28:38.193596887Z", "2026-09-22T02:28:38Z")
        and "2026-09-22T02:28:38.193596887Z" < "2026-09-22T02:28:38Z")
    row("an instant before the mark has not moved past it",
        not d3.moved_past("2026-09-22T02:28:37.999Z", "2026-09-22T02:28:38Z"))
    row("MUTANT: an absent or unparseable evaluatedAt is not proof the controller looked",
        not d3.moved_past(None, "2026-09-22T02:28:38Z")
        and not d3.moved_past("garbage", "2026-09-22T02:28:38Z"))


def test_the_policy_fixture_asks_what_the_rows_claim_it_asks() -> None:
    spec = d3.verdict_policy(
        "p", subject={"destinationRef": {"name": "dest-a"}}, catalog="primary")["spec"]
    row("no scheduleRefs — every Backup these rows create is manual, and `is_member` "
        "would exclude all of them from a policy that named a schedule",
        "scheduleRefs" not in spec["protects"])
    row("maxConsecutiveFailedRuns 0 disables the failure axis, so `Healthy` and "
        "`Staleness` mean what the rows say",
        spec["objectives"]["maxConsecutiveFailedRuns"] == 0)
    row("the catalogRef axis is a parameter, present and absent",
        spec["protects"]["catalogRef"] == {"name": "primary"}
        and "catalogRef" not in d3.verdict_policy(
            "p", subject={"destinationRef": {"name": "dest-a"}}, catalog=None
        )["spec"]["protects"])
    row("and the evidence objective is the other axis, defaulting to the safe direction",
        spec["objectives"]["requireVerifiedEvidence"] is True
        and d3.verdict_policy("p", subject={"destinationRef": {"name": "d"}}, catalog=None,
                              require_verified=False
                              )["spec"]["objectives"]["requireVerifiedEvidence"] is False)
    legacy = d3.verdict_policy(
        "p", subject={"legacyArchive": {"url": "s3://kafka-backups/x"}}, catalog=None)["spec"]
    row("a legacy-archive subject names no destination — CEL rule H1 is an XOR",
        "legacyArchive" in legacy["protects"] and "destinationRef" not in legacy["protects"])
    row("every alert kind these rows read is routed, or a transition would be Suppressed "
        "and no POST would be owed",
        set(spec["notifications"]["kinds"]) >= {"Staleness", "ArchiveUnavailable"}
        and spec["notifications"]["sendResolved"] is True)


def test_the_legacy_destination_names_the_archive_the_backups_write() -> None:
    dest = d3.destination("dest-legacy", "kafka-backups", prefix="owner-stamp")
    row("the prefix is the archive root the legacy Backups use, not the default",
        dest["spec"]["storage"]["prefix"] == "owner-stamp"
        and dest["spec"]["storage"]["bucket"] == "kafka-backups")
    row("MUTANT: the default prefix would point the catalog at a root nothing wrote to",
        d3.destination("dest-a", "b")["spec"]["storage"]["prefix"] == d3.DEST_PREFIX
        and d3.DEST_PREFIX != "owner-stamp")


def test_a_policy_that_selected_nothing_is_reported_as_such() -> None:
    """`Unprotected` over an EMPTY candidate set is not a verdict.

    D3 §3.2's `Unprotected` — "no available point at all" — is what a policy
    says when every point it covers is unusable AND what it says when it covers
    no points at all, and `status.health` cannot tell them apart. That is how
    every protection row in `d3_live.py` came to measure nothing: they named
    `scheduleRefs` while their points were manual `Backup`s with no
    `spec.scheduleRef`, so `identity::is_run_of_schedule` excluded all of them.
    Live, on 2026-09-22, that policy read `lastAttempt: null`
    (`verdicts/probe-schedulerefs-membership.json`).
    """
    counted = {"lastAttempt": {"backupRef": {"name": "recovery-point"},
                               "phase": "Succeeded", "at": "2026-09-22T02:38:02Z"},
               "health": "Unprotected"}
    row("a policy that selected a run names it in status.lastAttempt",
        all(d3.selector_matched_a_run(counted).values()))

    # The exact shape the live probe produced: the policy evaluated, opened a
    # `Staleness` incident, and had selected nothing at all.
    vacuum = {"health": "Unprotected", "lastAttempt": None,
              "alerts": [_alert("Open")]}
    empty = d3.selector_matched_a_run(vacuum)
    row("MUTANT: the same Unprotected verdict over an EMPTY candidate set is REFUSED, "
        "not silently passed",
        not any(empty.values()), f"{empty}")
    row("MUTANT: a status with no lastAttempt key at all is refused the same way",
        not any(d3.selector_matched_a_run({"health": "Unprotected"}).values()))
    row("MUTANT: a lastAttempt carrying no backupRef name is refused",
        not any(d3.selector_matched_a_run(
            {"lastAttempt": {"phase": "Succeeded"}}).values()))

    # AND THE ROW IT GUARDS SAYS FALSE. The `notify` row's other clauses are
    # all satisfiable over an empty set — one alert, one transition, one
    # delivery attempt, health `Unprotected` — which is precisely why it passed
    # for a year over nothing.
    alerts = [_alert("Open")]
    other_clauses_ok = (len(alerts) == 1 and len(alerts) == 1
                        and (alerts[0].get("delivery") or {}).get("state") == "Delivered"
                        and vacuum["health"] in {"Stale", "Unprotected", "Unknown"})
    row("MUTANT: every OTHER clause of notify-stale-point-alerts-exactly-once is satisfied "
        "by the empty set, so the selector clause is the only thing that refuses it",
        other_clauses_ok and not all(d3.selector_matched_a_run(vacuum).values()))


def test_a_manual_backup_is_a_member_only_when_it_references_the_schedule() -> None:
    """D3 §3.2: "the `spec.scheduleRef.uid` field is the authority and the label
    is the index"."""
    plain = d3.backup_object("b", "dest-a")
    row("a plain manual Backup carries no scheduleRef, and so is no schedule's run",
        "scheduleRef" not in plain["spec"] and plain["spec"]["triggeredBy"] == "manual")
    member = d3.backup_object("b", "dest-a", schedule={"name": "keeps-running", "uid": "u-1"})
    row("a manual run OF a schedule carries name AND uid",
        member["spec"]["scheduleRef"] == {"name": "keeps-running", "uid": "u-1"})
    row("MUTANT: no runPolicySha256 is invented — `check_run_policy_digest` returns Ok when "
        "it is absent and TERMINALLY refuses a mismatch when it is present",
        "runPolicySha256" not in member["spec"]["scheduleRef"])
    row("MUTANT: and no ownerReference — a manual run OF a schedule is not a run the "
        "schedule created and may garbage-collect",
        "ownerReferences" not in member["metadata"])
    row("the two policies name DIFFERENT schedules, so neither selects the other's point",
        d3.protection_policy("protect-a", max_age=300)["spec"]["protects"]["scheduleRefs"]
        == [{"name": "keeps-running"}]
        and d3.protection_policy("p", max_age=600, schedule=d3.RECOVERY_SCHEDULE)
        ["spec"]["protects"]["scheduleRefs"] == [{"name": d3.RECOVERY_SCHEDULE}]
        and d3.RECOVERY_SCHEDULE != "keeps-running")


# --- D3 §15 L6: a rehearsal executes end to end (PLAT-14.3) ------------------
#
# EVERY PREDICATE THE `rehearsal` PHASE DECIDES FROM, TWICE: once over the shape
# a correct build publishes, and once over a PLANTED-WRONG shape that must be
# REFUSED. The wrong shapes are not invented — each is a state the product could
# actually reach and that the review's §4 names as forbidden: a standing Restore
# that still carries `approvalRef`, a seven-member bundle, a Job env carrying a
# per-run approval digest, a scorecard naming a person, a schedule that recorded
# no pass, an owned topic that survived teardown, an unrelated topic that did
# not, a second Restore during an occupied slot, and a Job for the arm that was
# refused.

L6_SCHEDULE = "l6-rehearsal"
L6_SLOT = "20260921-030000"
L6_UID = "3f2a91c7-1d2e-4f00-9a11-77c0ffee1234"
L6_PREFIX = "rehearsal-3f2a91c7-"
L6_RESTORE_NAME = f"logweir-rehearsal-{L6_SCHEDULE}-{L6_SLOT}"
L6_APPROVAL = f"{L6_SCHEDULE}-standing"
L6_RUN = "01JBQ8Z2M3N4P5Q6R7S8T9UVWX"

L6_VERIFIED_APPROVAL = {
    "metadata": {"name": L6_APPROVAL},
    "status": {
        "matchedKeyId": "9c" * 32,
        "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1",
                               "kind": "RehearsalSchedule", "name": L6_SCHEDULE,
                               "namespace": "lw-hr9", "uid": L6_UID},
        "conditions": [{"type": "Verified", "status": "True", "reason": "Verified"}],
    },
}
# The same Approval, verified for a schedule that was DELETED AND RECREATED
# under the same name: same name, different uid.
L6_APPROVAL_FOR_A_RECREATED_SCHEDULE = json.loads(json.dumps(L6_VERIFIED_APPROVAL))
L6_APPROVAL_FOR_A_RECREATED_SCHEDULE["status"]["verifiedSubjectRef"]["uid"] = (
    "00000000-dead-beef-0000-000000000000")

L6_STANDING_RESTORE = {
    "metadata": {
        "name": L6_RESTORE_NAME,
        "labels": {"logweir.dev/rehearsal-schedule": L6_SCHEDULE,
                   "logweir.dev/rehearsal-slot": L6_SLOT,
                   "logweir.dev/rehearsal-target": "rehearsal-target"},
    },
    "spec": {"authorization": {"kind": "Standing",
                               "approvalRef": {"name": L6_APPROVAL},
                               "rehearsalScheduleRef": {"name": L6_SCHEDULE}}},
    "status": {
        "phase": "Succeeded", "reason": "Completed", "outcome": "pass",
        "jobRef": {"name": L6_RESTORE_NAME},
        "evidence": {"scorecardKey": f"logweir/drills/{L6_RUN}.json",
                     "offsetReportKey": f"logweir/drills/{L6_RUN}.offsets.json",
                     "verification": {"result": "Valid"}},
    },
}
# PLANTED: the pre-14.3b shape, where the standing document sat BESIDE a per-run
# approval slot instead of replacing it.
L6_RESTORE_WITH_APPROVALREF = json.loads(json.dumps(L6_STANDING_RESTORE))
L6_RESTORE_WITH_APPROVALREF["spec"]["approvalRef"] = {"name": L6_APPROVAL}
# PLANTED: what an OLDER controller writes for a standing-authorized Restore —
# the documented fail-closed rollback, and the defect L6 reproduces.
L6_RESTORE_HELD_AT_APPROVALNOTRECEIVED = json.loads(json.dumps(L6_STANDING_RESTORE))
L6_RESTORE_HELD_AT_APPROVALNOTRECEIVED["status"] = {
    "phase": "Refused", "reason": "ApprovalNotReceived"}

L6_ARGV = [
    "restore", "run", "--execution-contract-version", "2.0.0",
    "--spec", "/plan/spec.yaml",
    "--standing-authorization", "/approval/standing-authorization.json",
    "--authorization-keys", "/approval/authorization-keys.json",
    "--approver-key", "/approval/approver.pub.pem",
    "--allowed-clusters", "/approval/allowed-clusters.json",
    "--signing-key", "/signing/signing.pem",
    "--out", "/work/scorecard.json",
    "--offset-report-out", "/work/offsets.json",
    "--triggered-by", f"rehearsal/{L6_SCHEDULE}/{L6_SLOT}",
]
L6_ENV = {
    "LOGWEIR_EXECUTION_CONTRACT_VERSION": "2.0.0",
    "LOGWEIR_EXECUTION_SUBJECT_KIND": "Restore",
    "LOGWEIR_EXECUTION_SUBJECT_NAME": L6_RESTORE_NAME,
    "LOGWEIR_EXECUTION_APPROVAL_NAME": L6_APPROVAL,
    "LOGWEIR_EXECUTION_APPROVAL_UID": "11111111-2222-3333-4444-555555555555",
    "LOGWEIR_EXECUTION_PLAN_SHA256": "sha256:" + "ab" * 32,
    "LOGWEIR_EXECUTION_AUTHORIZATION_KIND": "standing",
    "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID": L6_UID,
    "LOGWEIR_EXECUTION_AUTHORIZATION_SHA256": "sha256:" + "cd" * 32,
}
L6_BUNDLE = set(d3.STANDING_BUNDLE_KEYS)
# PLANTED: D3 W7's placeholder shape — the standing envelope written into the
# per-run `approval.json` slot, so the bundle carries SEVEN members.
L6_BUNDLE_OF_SEVEN = L6_BUNDLE | {"approval.json", "approval.sig"}
# PLANTED: the env of a standing run that still pins the per-run digests.
L6_ENV_WITH_THE_APPROVAL_SHA = dict(L6_ENV)
L6_ENV_WITH_THE_APPROVAL_SHA["LOGWEIR_EXECUTION_APPROVAL_SHA256"] = "sha256:" + "ef" * 32

L6_SCORECARD = {
    "format_version": "1.0.0", "run_id": L6_RUN, "outcome": "pass",
    "triggered_by": f"rehearsal/{L6_SCHEDULE}/{L6_SLOT}",
    "approval": {"approver": f"standing-authorization/{L6_SCHEDULE}", "ticket": "",
                 "plan_hash": "sha256:" + "ab" * 32, "key_id": "9c" * 32,
                 "self_attested": False},
}
# PLANTED: a scorecard that names a PERSON. v1.0.0 of the standing document
# carries no approver at all, so any human name here is invented — and it tells
# the reader a person approved THIS run when what a person approved was a
# schedule.
L6_SCORECARD_APPROVED_BY_A_PERSON = json.loads(json.dumps(L6_SCORECARD))
L6_SCORECARD_APPROVED_BY_A_PERSON["approval"]["approver"] = "ada@example.invalid"
L6_SCORECARD_APPROVED_BY_A_PERSON["approval"]["ticket"] = "CHG-42"

L6_SCHEDULE_PASSED = {
    "status": {
        "lastSucceeded": {"restoreRef": {"name": L6_RESTORE_NAME},
                          "at": "2026-09-21T03:04:05Z", "pointId": "lwp1-" + "a" * 32,
                          "evidence": "Valid", "rtoSeconds": 137},
        "lastScheduledSlot": L6_SLOT,
        "conditions": [{"type": "RehearsalHealthy", "status": "True", "reason": "Passed"}],
    }
}
# PLANTED: the schedule that ran and recorded NOTHING — `RehearsalHealthy` still
# says `NoResult` and `activeRestoreRef` was never released, which is what a
# reservation that only ever writes and never clears leaves behind.
L6_SCHEDULE_WITHOUT_LASTSUCCEEDED = {
    "status": {
        "activeRestoreRef": {"name": L6_RESTORE_NAME},
        "lastScheduledSlot": L6_SLOT,
        "conditions": [{"type": "RehearsalHealthy", "status": "Unknown",
                        "reason": "NoResult"}],
    }
}

L6_MAPPED = {f"{L6_PREFIX}orders"}
L6_UNRELATED = "rehearsal-not-ours"
L6_DURING = L6_MAPPED | {"logweir.scratch", L6_UNRELATED}
L6_AFTER = {"logweir.scratch", L6_UNRELATED}
# PLANTED: teardown left the topic it created behind.
L6_AFTER_WITH_A_SURVIVING_OWNED_TOPIC = L6_AFTER | L6_MAPPED
# PLANTED: teardown took a topic it did not create.
L6_AFTER_WITH_THE_UNRELATED_TOPIC_DELETED = {"logweir.scratch"}

L6_CONCURRENCY_SKIPPED = {
    "status": {"lastSkipped": {"slot": "20260921-030100", "reason": "ConcurrencyBlocked"},
               "activeRestoreRef": {"name": L6_RESTORE_NAME}}
}
L6_SECOND_RESTORE = f"logweir-rehearsal-{L6_SCHEDULE}-20260921-030100"

L6_LEFTOVER_SKIPPED = {
    "status": {"lastSkipped": {"slot": L6_SLOT, "reason": "LeftoverTopics"},
               "cleanup": {"pendingTopics": [f"{L6_PREFIX}orders"],
                           "since": "2026-09-21T03:00:00Z"}}
}
L6_GUARD_REFUSED_RESTORE = {
    "metadata": {"name": L6_RESTORE_NAME},
    "status": {"phase": "Refused", "reason": "GuardRefused", "exitReason": "GuardRefused"},
}

L6_REFUSED_SCHEDULE = {
    "status": {"lastSkipped": {"slot": L6_SLOT, "reason": "AuthorizationInvalid"},
               "conditions": [{"type": "RehearsalHealthy", "status": "False",
                               "reason": "Failed"},
                              {"type": "Authorized", "status": "False",
                               "reason": "AuthorizationInvalid"}]}
}
# PLANTED: the state a build that never reconciles this kind AT ALL leaves —
# nothing recorded, and therefore also zero Jobs. This is the shape that made
# "zero Jobs" worthless as evidence, and the reason step 10's first clause
# exists.
L6_REFUSED_SCHEDULE_THAT_RECORDED_NOTHING = {"status": {"conditions": []}}


def test_the_standing_approval_verdict_is_about_this_schedule_object() -> None:
    row("L6 step 1: Verified=True with a matchedKeyId and this schedule's own uid",
        all(d3.standing_approval_is_verified(L6_VERIFIED_APPROVAL, L6_UID).values()))
    recreated = d3.standing_approval_is_verified(
        L6_APPROVAL_FOR_A_RECREATED_SCHEDULE, L6_UID)
    row("L6 step 1 refuses a verdict recorded for a DIFFERENT object of the same name",
        not all(recreated.values())
        and not recreated[
            "status.verifiedSubjectRef.uid is this RehearsalSchedule's own uid"])
    unverified = json.loads(json.dumps(L6_VERIFIED_APPROVAL))
    unverified["status"]["conditions"] = [
        {"type": "Verified", "status": "False", "reason": "SignatureInvalid"}]
    row("L6 step 1 refuses an Approval object that exists but did not verify",
        not all(d3.standing_approval_is_verified(unverified, L6_UID).values()))


def test_a_standing_restore_carries_no_approval_ref_and_is_not_held() -> None:
    row("L6 step 2: the labelled Restore carries spec.authorization and no approvalRef",
        all(d3.restore_is_created_on_the_standing_authorization(
            L6_STANDING_RESTORE, L6_SCHEDULE, L6_APPROVAL).values()))
    beside = d3.restore_is_created_on_the_standing_authorization(
        L6_RESTORE_WITH_APPROVALREF, L6_SCHEDULE, L6_APPROVAL)
    row("L6 step 2 refuses a standing Restore that ALSO carries spec.approvalRef",
        not all(beside.values()) and not beside["and it carries NO spec.approvalRef"])
    held = d3.restore_is_created_on_the_standing_authorization(
        L6_RESTORE_HELD_AT_APPROVALNOTRECEIVED, L6_SCHEDULE, L6_APPROVAL)
    row("L6 step 2 refuses the pre-14.3b hold at ApprovalNotReceived — the defect itself",
        not all(held.values())
        and not held["status.reason is not ApprovalNotReceived"])
    foreign = json.loads(json.dumps(L6_STANDING_RESTORE))
    foreign["metadata"]["labels"]["logweir.dev/rehearsal-schedule"] = "someone-elses"
    row("L6 step 2 refuses a Restore labelled for another schedule",
        not all(d3.restore_is_created_on_the_standing_authorization(
            foreign, L6_SCHEDULE, L6_APPROVAL).values()))
    admitted = "and it was ADMITTED — status.jobRef names its runner Job, or the phase is " \
        "Running/Succeeded"
    not_refused = "and it was not refused at admission — no Failed/Refused phase without a Job"
    for reason in ("StandingAuthorizationRefused", "PlanHashMismatch", "ClusterNotReachable"):
        refused = json.loads(json.dumps(L6_STANDING_RESTORE))
        refused["status"] = {"phase": "Failed", "reason": reason}
        got = d3.restore_is_created_on_the_standing_authorization(
            refused, L6_SCHEDULE, L6_APPROVAL)
        row(f"MUTANT (review MEDIUM-2): a standing Restore refused {reason} at admission is "
            f"NOT the unblocking, although its reason is not ApprovalNotReceived",
            not all(got.values()) and not got[admitted] and not got[not_refused]
            and got["status.reason is not ApprovalNotReceived"])
    held = json.loads(json.dumps(L6_STANDING_RESTORE))
    held["status"] = {"phase": "Pending", "reason": "ApprovalNotVerified"}
    row("MUTANT: a Restore still HELD at admission (Pending, no Job) is not yet admitted",
        not d3.restore_is_created_on_the_standing_authorization(
            held, L6_SCHEDULE, L6_APPROVAL)[admitted])
    running = json.loads(json.dumps(L6_STANDING_RESTORE))
    running["status"] = {"phase": "Running", "jobRef": {"name": L6_RESTORE_NAME}}
    row("L6 step 2 accepts a Restore admitted and Running with its Job",
        all(d3.restore_is_created_on_the_standing_authorization(
            running, L6_SCHEDULE, L6_APPROVAL).values()))
    runner_failed = json.loads(json.dumps(L6_STANDING_RESTORE))
    runner_failed["status"] = {"phase": "Failed", "reason": "RunnerFailed",
                               "jobRef": {"name": L6_RESTORE_NAME}}
    row("L6 step 2 accepts a Restore whose RUNNER failed after admission — steps 3-5 judge "
        "that, and it was admitted",
        all(d3.restore_is_created_on_the_standing_authorization(
            runner_failed, L6_SCHEDULE, L6_APPROVAL).values()))


def test_the_standing_job_mounts_five_members_and_no_per_run_approval() -> None:
    row("L6 step 3: the argv, the five-member bundle and the standing env",
        all(d3.the_job_carries_the_standing_mount(
            L6_ARGV, L6_ENV, L6_BUNDLE, L6_SCHEDULE, L6_SLOT, L6_UID).values()))
    seven = d3.the_job_carries_the_standing_mount(
        L6_ARGV, L6_ENV, L6_BUNDLE_OF_SEVEN, L6_SCHEDULE, L6_SLOT, L6_UID)
    row("L6 step 3 refuses a SEVEN-key bundle carrying approval.json/.sig",
        not all(seven.values())
        and not seven["the bundle ConfigMap has exactly the five standing members"]
        and not seven["and neither approval.json nor approval.sig"])
    digested = d3.the_job_carries_the_standing_mount(
        L6_ARGV, L6_ENV_WITH_THE_APPROVAL_SHA, L6_BUNDLE, L6_SCHEDULE, L6_SLOT, L6_UID)
    row("L6 step 3 refuses an env that still pins LOGWEIR_EXECUTION_APPROVAL_SHA256",
        not all(digested.values()) and not digested["and neither approval sha env is set"])
    with_approval = d3.the_job_carries_the_standing_mount(
        L6_ARGV + ["--approval", "/approval/approval.json"], L6_ENV, L6_BUNDLE,
        L6_SCHEDULE, L6_SLOT, L6_UID)
    row("L6 step 3 refuses an argv that passes --approval beside the standing document",
        not all(with_approval.values()) and not with_approval["argv carries NO --approval"])
    wrong_trigger = d3.the_job_carries_the_standing_mount(
        [a if a != f"rehearsal/{L6_SCHEDULE}/{L6_SLOT}" else f"approval/{L6_APPROVAL}"
         for a in L6_ARGV], L6_ENV, L6_BUNDLE, L6_SCHEDULE, L6_SLOT, L6_UID)
    row("L6 step 3 refuses --triggered-by naming an approval instead of the slot",
        not all(wrong_trigger.values()))
    wrong_uid = d3.the_job_carries_the_standing_mount(
        L6_ARGV, {**L6_ENV, "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID": ""},
        L6_BUNDLE, L6_SCHEDULE, L6_SLOT, L6_UID)
    row("L6 step 3 refuses a blank rehearsal-schedule UID in the execution contract",
        not all(wrong_uid.values()))


def test_the_scorecard_names_a_schedule_and_never_a_person() -> None:
    row("L6 step 4: outcome pass, Valid, and the schedule in both signed fields",
        all(d3.the_scorecard_names_the_schedule_and_the_slot(
            L6_STANDING_RESTORE, L6_SCORECARD, L6_SCHEDULE, L6_SLOT).values()))
    person = d3.the_scorecard_names_the_schedule_and_the_slot(
        L6_STANDING_RESTORE, L6_SCORECARD_APPROVED_BY_A_PERSON, L6_SCHEDULE, L6_SLOT)
    row("L6 step 4 refuses a scorecard whose approval.approver is a PERSON",
        not all(person.values())
        and not person["its approval.approver is standing-authorization/<schedule>"])
    missing = d3.the_scorecard_names_the_schedule_and_the_slot(
        L6_STANDING_RESTORE, {}, L6_SCHEDULE, L6_SLOT)
    row("L6 step 4 refuses a run whose scorecard could not be fetched at all",
        not all(missing.values())
        and not missing["the signed scorecard was fetched from the evidence destination"])
    not_valid = json.loads(json.dumps(L6_STANDING_RESTORE))
    not_valid["status"]["evidence"]["verification"] = {"result": "NotAttempted"}
    row("L6 step 4 refuses a rehearsal whose evidence was never verified",
        not all(d3.the_scorecard_names_the_schedule_and_the_slot(
            not_valid, L6_SCORECARD, L6_SCHEDULE, L6_SLOT).values()))


def test_the_schedule_publishes_rehearsal_last_star() -> None:
    row("L6 step 5: lastSucceeded, RehearsalHealthy=True/Passed, activeRestoreRef cleared",
        all(d3.the_schedule_records_the_pass(
            L6_SCHEDULE_PASSED, L6_RESTORE_NAME).values()))
    nothing = d3.the_schedule_records_the_pass(
        L6_SCHEDULE_WITHOUT_LASTSUCCEEDED, L6_RESTORE_NAME)
    row("L6 step 5 refuses a schedule that recorded no lastSucceeded and never released "
        "activeRestoreRef",
        not all(nothing.values())
        and not nothing["status.lastSucceeded.restoreRef names the rehearsal that ran"]
        and not nothing["and status.activeRestoreRef is cleared"])
    no_rto = json.loads(json.dumps(L6_SCHEDULE_PASSED))
    del no_rto["status"]["lastSucceeded"]["rtoSeconds"]
    row("L6 step 5 refuses a pass with no measured RTO — the objective it exists to measure",
        not all(d3.the_schedule_records_the_pass(no_rto, L6_RESTORE_NAME).values()))


def test_the_rehearsal_owns_its_topics_and_touches_no_others() -> None:
    row("L6 step 6: the mapped topics during, none after, and the unrelated one survives",
        all(d3.the_target_holds_exactly_the_mapped_topics(
            L6_DURING, L6_AFTER, L6_MAPPED, L6_PREFIX, L6_UNRELATED).values()))
    survived = d3.the_target_holds_exactly_the_mapped_topics(
        L6_DURING, L6_AFTER_WITH_A_SURVIVING_OWNED_TOPIC, L6_MAPPED, L6_PREFIX, L6_UNRELATED)
    row("L6 step 6 refuses an OWNED topic that teardown left behind",
        not all(survived.values()) and not survived["after teardown it holds none of them"])
    eaten = d3.the_target_holds_exactly_the_mapped_topics(
        L6_DURING, L6_AFTER_WITH_THE_UNRELATED_TOPIC_DELETED, L6_MAPPED, L6_PREFIX,
        L6_UNRELATED)
    row("L6 step 6 refuses an UNRELATED topic that teardown deleted",
        not all(eaten.values())
        and not eaten["and it still exists afterwards — teardown never touched it"])
    extra = d3.the_target_holds_exactly_the_mapped_topics(
        L6_DURING | {f"{L6_PREFIX}payments"}, L6_AFTER, L6_MAPPED, L6_PREFIX, L6_UNRELATED)
    row("L6 step 6 refuses a topic under the rendered prefix that the plan never mapped",
        not all(extra.values())
        and not extra["and no other topic under this schedule's rendered prefix"])


def test_a_second_slot_during_a_rehearsal_creates_no_restore() -> None:
    row("L6 step 7: ConcurrencyBlocked recorded, and still exactly one Restore",
        all(d3.the_second_slot_is_concurrency_blocked(
            L6_CONCURRENCY_SKIPPED, [L6_RESTORE_NAME], L6_RESTORE_NAME).values()))
    second = d3.the_second_slot_is_concurrency_blocked(
        L6_CONCURRENCY_SKIPPED, [L6_RESTORE_NAME, L6_SECOND_RESTORE], L6_RESTORE_NAME)
    row("L6 step 7 refuses a SECOND Restore created during the occupied slot",
        not all(second.values())
        and not second["and the second slot created no Restore"])
    other_reason = json.loads(json.dumps(L6_CONCURRENCY_SKIPPED))
    other_reason["status"]["lastSkipped"]["reason"] = "TargetBusy"
    row("L6 step 7 refuses a skip recorded under a different reason",
        not all(d3.the_second_slot_is_concurrency_blocked(
            other_reason, [L6_RESTORE_NAME], L6_RESTORE_NAME).values()))
    silent = d3.the_second_slot_is_concurrency_blocked(
        {"status": {}}, [L6_RESTORE_NAME], L6_RESTORE_NAME)
    row("L6 step 7 refuses a schedule that skipped silently — the control needs the reason",
        not all(silent.values()))


def test_the_leftover_guard_refuses_and_never_adopts_the_topic() -> None:
    row("L6 step 8, controller arm: LeftoverTopics with no Restore, topic untouched",
        all(d3.the_leftover_guard_refuses_and_keeps_the_topic(
            L6_LEFTOVER_SKIPPED, None, f"{L6_PREFIX}orders",
            {f"{L6_PREFIX}orders", "logweir.scratch"}).values()))
    row("L6 step 8, runner arm: the Restore is refused with GuardRefused, topic untouched",
        all(d3.the_leftover_guard_refuses_and_keeps_the_topic(
            {"status": {}}, L6_GUARD_REFUSED_RESTORE, f"{L6_PREFIX}orders",
            {f"{L6_PREFIX}orders", "logweir.scratch"}).values()))
    adopted = d3.the_leftover_guard_refuses_and_keeps_the_topic(
        {"status": {}},
        {"metadata": {"name": L6_RESTORE_NAME},
         "status": {"phase": "Succeeded", "reason": "Completed"}},
        f"{L6_PREFIX}orders", {"logweir.scratch"})
    row("L6 step 8 refuses a rehearsal that ADOPTED the pre-created name and tore it down",
        not all(adopted.values())
        and not adopted["no rehearsal succeeded against the pre-created name"]
        and not adopted["and the pre-created topic is untouched — it still exists"])
    silent = d3.the_leftover_guard_refuses_and_keeps_the_topic(
        {"status": {}}, None, f"{L6_PREFIX}orders",
        {f"{L6_PREFIX}orders", "logweir.scratch"})
    row("L6 step 8 refuses a slot that neither ran nor recorded a refusal",
        not all(silent.values()))


def test_the_rehearsal_evidence_outlives_its_job() -> None:
    fetched = {"scorecard": True, "sidecar": True, "offset report": True,
               "teardown attestation": True}
    row("L6 step 9: the Job is collected and all four signed objects are still fetchable",
        all(d3.the_evidence_outlives_the_job(True, fetched).values()))
    gone = d3.the_evidence_outlives_the_job(True, {**fetched, "teardown attestation": False})
    row("L6 step 9 refuses a teardown attestation that is no longer fetchable",
        not all(gone.values()))
    still_there = d3.the_evidence_outlives_the_job(False, fetched)
    row("L6 step 9 refuses the claim while the Job is still there — nothing was retained yet",
        not all(still_there.values())
        and not still_there["the runner Job is gone after its TTL"])


def test_the_refused_arm_requires_the_refusal_and_not_merely_silence() -> None:
    row("L6 step 10: a named refusal, no passing health, zero Jobs, zero bundles",
        all(d3.the_refused_arm_reaches_no_job(
            L6_REFUSED_SCHEDULE, [], [], []).values()))
    silent = d3.the_refused_arm_reaches_no_job(
        L6_REFUSED_SCHEDULE_THAT_RECORDED_NOTHING, [], [], [])
    row("L6 step 10 REFUSES silence: zero Jobs is also what a build that never reconciles "
        "this kind leaves behind",
        not all(silent.values())
        and not silent["the refusal is RECORDED and NAMED — AuthorizationInvalid/Expired on "
                       "the schedule, or StandingAuthorizationRefused on its Restore"])
    with_job = d3.the_refused_arm_reaches_no_job(
        L6_REFUSED_SCHEDULE, [], [f"logweir-rehearsal-{L6_SCHEDULE}-{L6_SLOT}"], [])
    row("L6 step 10 refuses a runner Job created for the arm that was refused",
        not all(with_job.values()) and not with_job["zero runner Jobs exist for it"])
    with_bundle = d3.the_refused_arm_reaches_no_job(
        L6_REFUSED_SCHEDULE, [], [], [f"{L6_RESTORE_NAME}-approval-bundle"])
    row("L6 step 10 refuses an approval-bundle ConfigMap written for the refused arm",
        not all(with_bundle.values()))
    reconciler_side = d3.the_refused_arm_reaches_no_job(
        {"status": {"conditions": [{"type": "RehearsalHealthy", "status": "Unknown",
                                    "reason": "NoResult"}]}},
        [{"metadata": {"name": L6_RESTORE_NAME},
          "status": {"phase": "Refused", "reason": "StandingAuthorizationRefused"}}],
        [], [])
    row("L6 step 10 accepts the reconciler-side refusal, StandingAuthorizationRefused",
        all(reconciler_side.values()))
    healthy = d3.the_refused_arm_reaches_no_job(
        {"status": {"lastSkipped": {"reason": "AuthorizationInvalid"},
                    "conditions": [{"type": "RehearsalHealthy", "status": "True",
                                    "reason": "Passed"}]}}, [], [], [])
    row("L6 step 10 refuses an arm that claims a passing rehearsal while refusing its slots",
        not all(healthy.values()))
    health = "RehearsalHealthy is False, or Unknown/NoResult — nothing finished, nothing passed"
    absent = d3.the_refused_arm_reaches_no_job(
        {"status": {"lastSkipped": {"reason": "AuthorizationInvalid"}, "conditions": []}},
        [], [], [])
    row("MUTANT: an arm with NO RehearsalHealthy condition at all fails — the controller "
        "always writes one", not absent[health])
    other_unknown = d3.the_refused_arm_reaches_no_job(
        {"status": {"lastSkipped": {"reason": "AuthorizationInvalid"},
                    "conditions": [{"type": "RehearsalHealthy", "status": "Unknown",
                                    "reason": "Mystery"}]}}, [], [], [])
    row("MUTANT: Unknown for a reason other than NoResult fails", not other_unknown[health])
    busy = d3.the_refused_arm_reaches_no_job(
        {"status": {"lastSkipped": {"reason": "TargetBusy"},
                    "conditions": [{"type": "RehearsalHealthy", "status": "Unknown",
                                    "reason": "NoResult"}]}}, [], [], [])
    row("L6 step 10: TargetBusy is NOT a refusal of the authorization (decide checks the "
        "target first)",
        not busy["the refusal is RECORDED and NAMED — AuthorizationInvalid/Expired on "
                 "the schedule, or StandingAuthorizationRefused on its Restore"])


_RETIRED = {"status": {"conditions": [{"type": "Verified", "status": "False",
                                       "reason": "KeyRetired"}]}}
_ACTIVE_VERIFIED = {"status": {"conditions": [{"type": "Verified", "status": "True",
                                               "reason": "Verified"}]}}
# PLANTED: F1, as the live run of 2026-09-22 recorded it — EVERY standing
# Approval refused `PlanHashMismatch`, the retired one and the active one alike.
_F1 = {"status": {"conditions": [{"type": "Verified", "status": "False",
                                  "reason": "PlanHashMismatch"}]}}


def test_the_refused_arm_passes_only_when_the_retired_key_rule_is_what_refused() -> None:
    good = d3.the_refused_arm_reaches_no_job(L6_REFUSED_SCHEDULE, [], [], [])
    verdict, _ = d3.refused_arm_verdict(good, skip_reason="AuthorizationInvalid",
                                        refused_approval=_RETIRED,
                                        passing_approval=_ACTIVE_VERIFIED)
    row("L6 step 10 PASS: refused, KeyRetired on its Approval, and the passing arm verified",
        verdict == "PASS", verdict)
    verdict, mech = d3.refused_arm_verdict(good, skip_reason="AuthorizationInvalid",
                                           refused_approval=_F1, passing_approval=_F1)
    row("MUTANT (review MEDIUM-1): F1 — nothing verifies at all — is INCONCLUSIVE, never PASS",
        verdict == "INCONCLUSIVE" and not any(mech.values()), f"{verdict} {mech}")
    verdict, _ = d3.refused_arm_verdict(good, skip_reason="AuthorizationInvalid",
                                        refused_approval=_RETIRED, passing_approval=_F1)
    row("MUTANT: KeyRetired on the refused arm but the passing arm unverified — no "
        "differential, INCONCLUSIVE", verdict == "INCONCLUSIVE", verdict)
    verdict, _ = d3.refused_arm_verdict(good, skip_reason="AuthorizationInvalid",
                                        refused_approval=_ACTIVE_VERIFIED,
                                        passing_approval=_ACTIVE_VERIFIED)
    row("MUTANT: a retired key's Approval reported Verified=True is not the rule shown",
        verdict == "INCONCLUSIVE", verdict)
    silent = d3.the_refused_arm_reaches_no_job(L6_REFUSED_SCHEDULE_THAT_RECORDED_NOTHING,
                                               [], [], [])
    verdict, _ = d3.refused_arm_verdict(silent, skip_reason=None, refused_approval=_RETIRED,
                                        passing_approval=_ACTIVE_VERIFIED)
    row("MUTANT: silence is a FAIL even when the mechanism clauses hold — never softened",
        verdict == "FAIL", verdict)
    with_job = d3.the_refused_arm_reaches_no_job(
        L6_REFUSED_SCHEDULE, [], [f"logweir-rehearsal-l6-refused-{L6_SLOT}"], [])
    verdict, _ = d3.refused_arm_verdict(with_job, skip_reason="AuthorizationInvalid",
                                        refused_approval=_RETIRED,
                                        passing_approval=_ACTIVE_VERIFIED)
    row("MUTANT: a Job for the refused arm is a FAIL", verdict == "FAIL", verdict)
    busy = d3.the_refused_arm_reaches_no_job(
        {"status": {"lastSkipped": {"reason": "TargetBusy"},
                    "conditions": [{"type": "RehearsalHealthy", "status": "Unknown",
                                    "reason": "NoResult"}]}}, [], [], [])
    verdict, _ = d3.refused_arm_verdict(busy, skip_reason="TargetBusy",
                                        refused_approval=_RETIRED,
                                        passing_approval=_ACTIVE_VERIFIED)
    row("MUTANT (review HIGH-2): a TargetBusy that masked the authorization is HARNESS-FAULT, "
        "not a product FAIL and not a PASS", verdict == "HARNESS-FAULT", verdict)


def test_step_seven_fails_on_a_second_restore_and_on_a_silent_skip() -> None:
    slot = L6_SLOT
    t0 = d3.slot_epoch(slot)
    obliged = t0 + 60 + d3.REHEARSAL_REQUEUE_SECONDS + d3.REHEARSAL_OBLIGATION_MARGIN_SECONDS
    row("slot names are read as UTC epoch seconds",
        t0 == 1789959600.0 and d3.slot_epoch("20260921-030100") == t0 + 60, str(t0))

    def decide(**kw):
        base = dict(first_slot=slot, overlap=False, new_skip_reason=None,
                    first_terminal=False, last_active_at=None, timed_out=False)
        base.update(kw)
        return d3.concurrency_decision(**base)

    row("MUTANT (review HIGH-1): a SECOND Restore listed while the first ran is a FAIL — the "
        "old loop called it NOT-REACHED once the first finished",
        decide(overlap=True, first_terminal=True, last_active_at=t0 + 30) == "FAIL"
        and decide(overlap=True) == "FAIL")
    row("MUTANT (review HIGH-1): a first rehearsal still running AFTER the controller was "
        "obliged to evaluate the next slot, with no skip recorded, is judged — and the "
        "predicate fails the silence",
        decide(first_terminal=True, last_active_at=obliged + 1) == "EVALUATE"
        and not all(d3.the_second_slot_is_concurrency_blocked(
            {"status": {"lastSkipped": {}}}, [L6_RESTORE_NAME], L6_RESTORE_NAME).values()))
    row("NOT-REACHED only when the first finished before the controller was obliged to look",
        decide(first_terminal=True, last_active_at=t0 + 40) == "NOT-REACHED"
        and decide(first_terminal=True, last_active_at=None) == "NOT-REACHED"
        and decide(first_terminal=True, last_active_at=obliged + 1) != "NOT-REACHED")
    row("a new ConcurrencyBlocked skip is judged by the predicate, and passes it",
        decide(new_skip_reason="ConcurrencyBlocked") == "EVALUATE"
        and all(d3.the_second_slot_is_concurrency_blocked(
            L6_CONCURRENCY_SKIPPED, [L6_RESTORE_NAME], L6_RESTORE_NAME).values()))
    row("MUTANT (review HIGH-2): a TargetBusy skip is the harness's ordering — HARNESS-FAULT",
        decide(new_skip_reason="TargetBusy") == "HARNESS-FAULT")
    row("a first rehearsal still running before the deadline, with nothing recorded, is waited "
        "on", decide(last_active_at=t0 + 30) == "WAIT")
    row("MUTANT: the deadline with nothing recorded is judged, and silence fails",
        decide(last_active_at=obliged + 200, timed_out=True) == "EVALUATE")


def test_the_broker_sweep_deletes_only_this_runs_own_topics() -> None:
    uid_a = "3f2a91c7-1d2e-4f00-9a11-77c0ffee1234"
    uid_b = "0badc0de-1d2e-4f00-9a11-77c0ffee1234"
    arms = {"l6-rehearsal": uid_a, "l6-leftover": uid_b}
    ours = {"metadata": {"uid": uid_a, "labels": {"logweir.dev/test-owner": d3.OWNER}}}
    owned, refused = d3.owned_rehearsal_prefixes(
        arms, {"l6-rehearsal": ours, "l6-leftover": None},
        {"l6-rehearsal": True, "l6-leftover": True})
    row("a quiet schedule this run created is swept by its own rendered prefix, and one whose "
        "object is already gone by the uid its create returned",
        owned == {"l6-rehearsal": "rehearsal-3f2a91c7-", "l6-leftover": "rehearsal-0badc0de-"}
        and not refused, f"{owned} {refused}")
    theirs = {"metadata": {"uid": uid_a, "labels": {"logweir.dev/test-owner": "someone"}}}
    owned, refused = d3.owned_rehearsal_prefixes(
        {"l6-rehearsal": uid_a}, {"l6-rehearsal": theirs}, {"l6-rehearsal": True})
    row("MUTANT: a live schedule carrying another run's owner label is never swept",
        not owned and "l6-rehearsal" in refused)
    recreated = {"metadata": {"uid": uid_b, "labels": {"logweir.dev/test-owner": d3.OWNER}}}
    owned, _ = d3.owned_rehearsal_prefixes(
        {"l6-rehearsal": uid_a}, {"l6-rehearsal": recreated}, {"l6-rehearsal": True})
    row("MUTANT: a schedule of the same name with ANOTHER uid is not the one this run made",
        not owned)
    owned, refused = d3.owned_rehearsal_prefixes(
        {"l6-rehearsal": uid_a}, {"l6-rehearsal": ours}, {"l6-rehearsal": False})
    row("MUTANT: a schedule with a rehearsal still running is not swept under its runner",
        not owned and "still running" in refused["l6-rehearsal"])
    broker = {"rehearsal-3f2a91c7-orders", "rehearsal-deadbeef-orders", "orders",
              d3.rehearsal_witness_topic(), "rehearsal-not-ours", "logweir.scratch"}
    row("the sweep deletes ONLY names under this run's prefixes — never another schedule's "
        "rehearsal topic, never a witness, never a lab topic",
        d3.topics_to_sweep(broker, ["rehearsal-3f2a91c7-"]) == ["rehearsal-3f2a91c7-orders"])
    row("MUTANT: an empty prefix matches nothing rather than everything",
        d3.topics_to_sweep(broker, [""]) == [])


def test_the_witness_topic_is_this_runs_and_no_prefix_can_claim_it() -> None:
    import re as _re

    witness = d3.rehearsal_witness_topic()
    row("the witness keeps D3 §15's name as its prefix and carries this run's owner and stamp",
        witness.startswith(d3.REHEARSAL_UNRELATED_TOPIC + "-")
        and d3.OWNER_TAG in witness and d3.STAMP in witness, witness)
    row("MUTANT (review MEDIUM-3): the bare fixed name two runs used to share is NOT the "
        "witness", witness != "rehearsal-not-ours")
    row("no rendered prefix `rehearsal-<8 hex>-` can ever match the witness",
        _re.match(r"^rehearsal-[0-9a-f]{8}-", witness) is None)
    row("and it is a legal Kafka topic name",
        _re.fullmatch(r"[A-Za-z0-9._-]{1,249}", witness) is not None)


def test_the_rehearsal_harness_constants_are_the_products_own() -> None:
    src = _REPO / "crates/weirkeeper/src"
    rehearsal_rs = (src / "rehearsal.rs").read_text()
    schedule_rs = (src / "controllers/rehearsal_schedule.rs").read_text()
    approval_rs = (src / "controllers/approval.rs").read_text()
    row("REHEARSAL_RESTORE_PREFIX is rehearsal.rs's own",
        f'pub const REHEARSAL_RESTORE_PREFIX: &str = "{d3.REHEARSAL_RESTORE_PREFIX}";'
        in rehearsal_rs)
    row("REHEARSAL_REQUEUE_SECONDS is rehearsal_schedule.rs's REQUEUE_SECONDS",
        f"pub const REQUEUE_SECONDS: u64 = {d3.REHEARSAL_REQUEUE_SECONDS};" in schedule_rs)
    row("the NoResult reason step 10 accepts is the controller's REASON_NO_RESULT",
        'pub const REASON_NO_RESULT: &str = "NoResult";' in schedule_rs)
    row("the KeyRetired reason step 10 requires is the Approval controller's own",
        'Self::KeyRetired { .. } => "KeyRetired"' in approval_rs)
    row("TargetBusy is checked BEFORE the authorization — the premise of HARNESS-FAULT",
        schedule_rs.index("SkipReason::TargetBusy,")
        < schedule_rs.index("let authorization = match authorize(facts)"))


def test_the_rendered_prefix_is_the_arithmetic_the_signed_scope_carries() -> None:
    """The harness's ONE copy of D3 §4.4's `<prefix><uid[..8]>-`.

    The signed scope carries the RENDERED value, so a prefix computed
    differently here would mint a document the controller refuses for a reason
    that is the harness's and not the product's — and the row above it would
    then be about the harness.
    """
    row("the rendered prefix is <topicPrefix><schedule-uid[..8]>-",
        d3.rendered_prefix(L6_UID) == L6_PREFIX
        and d3.mapped_topic(L6_UID, "orders") == f"{L6_PREFIX}orders")
    row("two schedules never render the same prefix, which is what makes teardown scopable",
        d3.rendered_prefix(L6_UID) != d3.rendered_prefix("00000000-dead-beef-0000-0000"))


def test_the_five_standing_bundle_members_are_the_controllers_own_names() -> None:
    """A fixture two sides must agree on, read from the side that writes it.

    `STANDING_BUNDLE_KEYS` is the harness's copy of
    `controllers/restore.rs`'s five member names. It is pinned here as a SET
    and asserted against the strings that file defines, so a rename on the
    product side turns into a failing row rather than a bundle assertion that
    quietly compares nothing.
    """
    source = (pathlib.Path(__file__).resolve().parents[3]
              / "crates/weirkeeper/src/controllers/restore.rs").read_text()
    for member in sorted(d3.STANDING_BUNDLE_KEYS):
        row(f"the controller defines the bundle member `{member}`",
            f'"{member}"' in source)
    row("and the harness expects exactly five of them",
        len(d3.STANDING_BUNDLE_KEYS) == 5)


_RUNNING = {"spec": {"schedule": "* * * * *", "suspend": False}}
# PLANTED: the schedule exactly as `schedule_object` creates it, which is how
# the row was left between 23d4800 and this fix — suspended at birth, never
# started, and therefore unpassable whatever the product did.
_SUSPENDED_AT_BIRTH = {"spec": {"schedule": "* * * * *", "suspend": True}}
_CHILD = "logweir-backup-keeps-running-20260922-041500"


def test_the_scheduled_backups_row_can_pass_and_can_fail() -> None:
    row("retention-scheduled-backups-continue: a running schedule and a new Succeeded child",
        all(d3.the_schedule_kept_running(_RUNNING, [_CHILD], [_CHILD], set()).values()))
    suspended = d3.the_schedule_kept_running(_SUSPENDED_AT_BIRTH, [], [], set())
    row("MUTANT: THE DEFECT — a schedule left suspended at birth is named as the fault",
        not all(suspended.values())
        and not suspended["the schedule was NOT suspended while the row measured it "
                          "(suspended, the row cannot pass whatever the product does)"])
    row("MUTANT: a running schedule that produced no Succeeded child fails",
        not all(d3.the_schedule_kept_running(_RUNNING, [_CHILD], [], set()).values()))
    row("MUTANT: a child an EARLIER run left behind is not this window's evidence",
        not all(d3.the_schedule_kept_running(_RUNNING, [], [_CHILD], {_CHILD}).values()))


class _Swap:
    """Replace module attributes of `d3_live` for one block, and put them back."""

    def __init__(self, **attrs):
        self.attrs = attrs
        self.saved = {}

    def __enter__(self):
        for name, value in self.attrs.items():
            self.saved[name] = getattr(d3, name)
            setattr(d3, name, value)
        return self

    def __exit__(self, *exc):
        for name, value in self.saved.items():
            setattr(d3, name, value)
        return False


def test_a_trust_policy_is_deleted_only_when_this_run_owns_it() -> None:
    calls: list[list[str]] = []

    def fake_run(args, **_kw):
        calls.append(list(args))

    ours = {"metadata": {"name": "p", "labels": {"logweir.dev/test-owner": d3.OWNER}}}
    theirs = {"metadata": {"name": "p", "labels": {"logweir.dev/test-owner": "someone-else"}}}
    with _Swap(get_opt=lambda *a, **k: ours, run=fake_run):
        deleted = d3.delete_owned_trust_policy("p")
    row("a TrustPolicy carrying this run's owner label is deleted",
        deleted and any("delete" in c and "trustpolicy" in c for c in calls))
    calls.clear()
    refused = False
    with _Swap(get_opt=lambda *a, **k: theirs, run=fake_run):
        try:
            d3.delete_owned_trust_policy("p")
        except RuntimeError:
            refused = True
    row("MUTANT: another run's TrustPolicy of the SAME name is refused, and nothing is deleted",
        refused and not calls)
    with _Swap(get_opt=lambda *a, **k: None, run=fake_run):
        row("an absent TrustPolicy is not an error and deletes nothing",
            d3.delete_owned_trust_policy("p") is False and not calls)


def test_a_mint_that_fails_half_way_leaves_no_private_key() -> None:
    made: list[pathlib.Path] = []
    real_mkdtemp = d3.tempfile.mkdtemp

    def tracking_mkdtemp(*a, **k):
        path = real_mkdtemp(*a, **k)
        made.append(pathlib.Path(path))
        return path

    def failing_run(args, **_kw):
        # the first openssl call "writes" a private key, the second fails
        if "ecparam" in args:
            pathlib.Path(args[args.index("-out") + 1]).write_text("PRIVATE")
            return None
        raise RuntimeError("openssl pkcs8 failed")

    d3.tempfile.mkdtemp = tracking_mkdtemp
    raised = False
    try:
        with _Swap(run=failing_run):
            try:
                d3.mint_signing_key("d3-test-rows-mint")
            except RuntimeError:
                raised = True
    finally:
        d3.tempfile.mkdtemp = real_mkdtemp
    row("MUTANT: a mint that fails after writing the private half removes its directory",
        raised and bool(made) and not any(p.exists() for p in made),
        f"left: {[str(p) for p in made if p.exists()]}")


def test_the_signer_is_the_newest_build_and_must_know_standing() -> None:
    import time as _time

    root = pathlib.Path(tempfile.mkdtemp(prefix="d3-cli-"))
    (root / "target/debug").mkdir(parents=True)
    (root / "target/release").mkdir(parents=True)
    debug, release = root / "target/debug/logweir", root / "target/release/logweir"
    debug.write_text("stale")
    release.write_text("fresh")
    old = _time.time() - 3600
    os.utime(debug, (old, old))
    saved_bin = os.environ.pop("LOGWEIR_BIN", None)
    try:
        with _Swap(ROOT=root):
            row("MUTANT (review LOW-5): a STALE debug build is not chosen over a fresh release "
                "build", d3.logweir_cli() == str(release), d3.logweir_cli())
            os.utime(release, (old - 10, old - 10))
            row("and a debug build newer than the release build is the one just built",
                d3.logweir_cli() == str(debug), d3.logweir_cli())
    finally:
        shutil.rmtree(root, ignore_errors=True)
        if saved_bin is not None:
            os.environ["LOGWEIR_BIN"] = saved_bin

    class _Help:
        def __init__(self, text):
            self.stdout, self.stderr, self.returncode = text, "", 0

    d3._STANDING_CLI.clear()
    refused = ""
    with _Swap(run=lambda *a, **k: _Help("Usage: logweir drill approve --plan <PLAN>")):
        try:
            d3.require_standing_signer("/sim/old-logweir")
        except RuntimeError as e:
            refused = str(e)
    row("MUTANT: a binary without `drill approve --standing` is refused by name, before it "
        "can fail as if the product refused", "--standing" in refused, refused)
    with _Swap(run=lambda *a, **k: _Help("  --standing   mint a standing authorization")):
        d3.require_standing_signer("/sim/new-logweir")
    row("and one that has it is accepted", d3._STANDING_CLI.get("/sim/new-logweir") is True)


def test_the_lab_key_material_is_read_from_its_durable_home_first() -> None:
    home = pathlib.Path(tempfile.mkdtemp(prefix="d3-home-"))
    (home / ".logweir-lab" / "scram-e2e").mkdir(parents=True)
    saved = {k: os.environ.get(k) for k in ("HOME", "LOGWEIR_SCRAM_OUT")}
    try:
        os.environ["HOME"] = str(home)
        os.environ.pop("LOGWEIR_SCRAM_OUT", None)
        row("the durable $HOME/.logweir-lab/scram-e2e is read before the /tmp symlink",
            d3.lab_key_dir() == home / ".logweir-lab" / "scram-e2e", str(d3.lab_key_dir()))
        os.environ["LOGWEIR_SCRAM_OUT"] = "/sim/override"
        row("and LOGWEIR_SCRAM_OUT still overrides both",
            d3.lab_key_dir() == pathlib.Path("/sim/override"))
    finally:
        shutil.rmtree(home, ignore_errors=True)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


_REPO = pathlib.Path(__file__).resolve().parents[3]


def duplicated_top_level_names(source: str) -> dict[str, list[int]]:
    """Every top-level `def`/`class` name a module binds more than once.

    Python binds a module's definitions in file order, so the LAST one wins
    for every caller, including callers written above it — no error, no
    warning. That is how `patch_policy` (RetentionPolicy) was replaced by
    `patch_policy` (ProtectionPolicy) and `legal_hold` quietly patched the
    wrong kind (lab-refresh-7, 03:55Z).
    """
    import ast
    import collections

    seen: dict[str, list[int]] = collections.defaultdict(list)
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            seen[node.name].append(node.lineno)
    return {name: lines for name, lines in seen.items() if len(lines) > 1}


def harness_modules() -> list[pathlib.Path]:
    """Every Python harness module the live rows run from: D2, D3, `scripts/`."""
    found = set(_REPO.glob("e2e/k8s/*/*.py"))
    found |= set(_REPO.glob("scripts/*.py"))
    found |= set(_REPO.glob("scripts/live/**/*.py"))
    found |= set(_REPO.glob("scripts/fixtures/*.py"))
    return sorted(found)


def test_no_harness_module_defines_a_top_level_name_twice() -> None:
    modules = harness_modules()
    row("the duplicate-definition sweep reads the D2 and D3 harnesses and scripts/*.py",
        any(p.name == "d3_live.py" for p in modules)
        and any(p.name == "d2_live.py" for p in modules)
        and any(p.name == "test-plat06-live.py" for p in modules),
        f"modules: {[str(p.relative_to(_REPO)) for p in modules]}")
    for path in modules:
        dups = duplicated_top_level_names(path.read_text())
        row(f"{path.relative_to(_REPO)} binds no top-level name twice", not dups, str(dups))
    # PLANTED: the exact shape of the defect, which the sweep must name.
    planted = ("def patch_policy(name, spec_patch):\n    return 'retentionpolicy'\n\n"
               "def other():\n    return patch_policy('keep-b', {})\n\n"
               "def patch_policy(name, patch):\n    return 'protectionpolicy'\n")
    row("the sweep NAMES a module that defines patch_policy twice",
        duplicated_top_level_names(planted) == {"patch_policy": [1, 7]})
    import inspect

    retention_body = inspect.getsource(d3.patch_policy).split('"""')[-1]
    protection_body = inspect.getsource(d3.patch_protection_policy).split('"""')[-1]
    row("patch_policy (legal_hold, bounded_retry) patches a RetentionPolicy",
        '"retentionpolicy"' in retention_body and '"protectionpolicy"' not in retention_body)
    row("and patch_protection_policy patches a ProtectionPolicy",
        '"protectionpolicy"' in protection_body and '"retentionpolicy"' not in protection_body)


def test_zz_every_row_in_this_file_passed() -> None:
    """The file's own gate, for `python3 -m pytest e2e/k8s/d3`.

    It is a whole-module gate reading a module-global, so a partial selection
    (`-k`, `-x`, `pytest-xdist`) can pass it vacuously: run the file whole.
    `row()` records a failure instead of raising, so that one run prints every
    row rather than stopping at the first — which under pytest meant a module
    whose rows all failed still reported six passing TESTS. This is the last
    row by name on purpose: `main()` sorts, and pytest runs in definition
    order, so `zz` is last either way.
    """
    assert not FAILURES, f"{len(FAILURES)} failing row(s): {FAILURES}"


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
