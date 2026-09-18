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


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
