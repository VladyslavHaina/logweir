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


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
