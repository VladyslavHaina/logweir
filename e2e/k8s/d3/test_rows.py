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
# Succeeded against a destination whose `evidenceRead` is a `SecretKeys` grant.
SECRETKEYS_STATUS = {
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
                                  "detail": "…this build does not create that Job…"}},
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
    blind = d3.point_facts_the_policy_needs(SECRETKEYS_STATUS)
    row("THE LIVE SHAPE: a SecretKeys destination loses the two receipt-derived facts and "
        "keeps a written verdict that does not satisfy the objective",
        not blind[CAPTURE] and not blind[DIGEST] and blind[WRITTEN] and not blind[SATISFIED],
        f"{blind}")
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
