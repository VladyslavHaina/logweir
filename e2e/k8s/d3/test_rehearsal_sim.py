#!/usr/bin/env python3
"""The `rehearsal` phase, run end to end against a FAKE cluster on a fake clock.

    python3 -m pytest e2e/k8s/d3/test_rehearsal_sim.py -q

WHY THIS EXISTS. `test_rows.py` drives every PREDICATE over planted fixtures,
and that is where the rows' clauses are proved able to fail. What it cannot
reach is the LIVE WRAPPER around them — the loops, the waits, the order the
four arms run in, the sweep — and that is exactly where the harness-rows-9
review found its two HIGH defects: step 7's loop turned a second `Restore` into
NOT-REACHED, and four arms sharing one target turned later rows into
`TargetBusy` verdicts that belonged to the harness. Neither can be shown by a
predicate, and the live proof waits for the next lab refresh.

So this module replaces the cluster, not the harness: `d3_live.rehearsal()`
runs unmodified, its reads and writes land in `FakeCluster`, and the fake
controller implements the parts of `controllers/rehearsal_schedule.rs` the rows
depend on — the cron, `ConcurrencyBlocked` for this schedule's own child
BEFORE `TargetBusy` for anybody else's, both BEFORE the authorization, the
reservation, a runner that creates and tears down its mapped topic, phase 0's
`GuardRefused` over a pre-existing mapped name, and a Job TTL. A suspended
schedule records nothing, as the real one does.

It is NOT live evidence and says so: it proves the harness's own logic
against a model, and each mutant below is a model of one defect the rows claim
to catch. The live rows at lab-refresh-8 are the proof of the product.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import math
import os
import pathlib
import sys
import tempfile
import uuid
from typing import Any

_TMP = tempfile.mkdtemp(prefix="d3-rehearsal-sim-")
os.environ.setdefault("LOGWEIR_D3_OUT", _TMP)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d3_live as d3  # noqa: E402

KINDS = {
    "rehearsalschedule": "rehearsalschedule", "rehearsalschedules": "rehearsalschedule",
    "approval": "approval", "approvals": "approval",
    "restore": "restore", "restores": "restore",
    "job": "job", "jobs": "job",
    "pod": "pod", "pods": "pod",
    "configmap": "configmap", "configmaps": "configmap",
    "limitrange": "limitrange", "limitranges": "limitrange",
}
START = dt.datetime(2026, 9, 22, 5, 0, 30, tzinfo=dt.timezone.utc).timestamp()


class Result:
    def __init__(self, returncode: int = 0, stdout: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


class FakeCluster:
    """The objects, the broker's topics, and a controller that moves on a fake clock."""

    def __init__(self, *, second_restore_during_slot: bool = False, silent_skip: bool = False,
                 nothing_verifies: bool = False, rehearsal_seconds: int = 150,
                 skip_defers_slot: bool = False,
                 job_ttl_seconds: int = 300, topics: set[str] | None = None) -> None:
        self.now = START
        self.objs: dict[str, dict[str, dict[str, Any]]] = {k: {} for k in set(KINDS.values())}
        self.topics: set[str] = set(topics or {"logweir.scratch"})
        self.second_restore_during_slot = second_restore_during_slot
        self.silent_skip = silent_skip
        self.nothing_verifies = nothing_verifies
        # THE PRE-verdict-precedence CONTROLLER (REHEARSAL-SKIP-DEFERS-SLOT): a
        # skip names the evaluation instant and never advances
        # `lastScheduledSlot`, so the blocked slot fires late.
        self.skip_defers_slot = skip_defers_slot
        self.rehearsal_seconds = rehearsal_seconds
        self.job_ttl_seconds = job_ttl_seconds
        self.last_reconcile: dict[str, float] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.target_busy_seen: list[str] = []

    # --- the clock ------------------------------------------------------
    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        end = self.now + seconds
        while self.now < end:
            self.now = min(end, self.now + 1.0)
            self.tick()

    # --- the API --------------------------------------------------------
    def apply(self, obj: dict[str, Any]) -> dict[str, Any]:
        kind = KINDS[obj["kind"].lower()]
        name = obj["metadata"]["name"]
        existing = self.objs[kind].get(name)
        if existing is None:
            new = copy.deepcopy(obj)
            new["metadata"]["uid"] = str(uuid.uuid4())
            new["metadata"]["generation"] = 1
            new["metadata"]["creationTimestamp"] = self.stamp()
            new["status"] = {}
            self.objs[kind][name] = new
            if kind == "rehearsalschedule":
                new["status"] = {"templateDigest": "sha256:" + "7d" * 32, "conditions": [
                    {"type": "Ready", "status": "True", "reason": "Suspended", "message": ""},
                    {"type": "RehearsalHealthy", "status": "Unknown", "reason": "NoResult",
                     "message": "no rehearsal has finished yet"}]}
            if kind == "approval":
                self.verify(new)
        else:
            existing["spec"] = copy.deepcopy(obj.get("spec") or {})
        return copy.deepcopy(self.objs[kind][name])

    def get_opt(self, kind: str, name: str, namespace: str | None = None):
        obj = self.objs.get(KINDS.get(kind, kind), {}).get(name)
        return copy.deepcopy(obj) if obj is not None else None

    def get(self, kind: str, name: str, namespace: str | None = None):
        obj = self.get_opt(kind, name, namespace)
        if obj is None:
            raise RuntimeError(f"rc=1: {kind}/{name} not found")
        return obj

    def lst(self, kind: str, namespace: str | None = None, selector: str | None = None):
        items = list(self.objs[KINDS[kind]].values())
        if selector:
            key, value = selector.split("=", 1)
            items = [o for o in items if (o["metadata"].get("labels") or {}).get(key) == value]
        return [copy.deepcopy(o) for o in sorted(items, key=lambda o: o["metadata"]["name"])]

    def run(self, args: list[str], **_kw) -> Result:
        if "patch" in args:
            at = args.index("patch")
            kind, name = KINDS[args[at + 1]], args[at + 2]
            patch = json.loads(args[args.index("-p") + 1])
            obj = self.objs[kind].get(name)
            if obj is None:
                return Result(1)
            merge(obj, patch)
            if kind == "rehearsalschedule":
                self.last_reconcile.pop(name, None)  # a spec change wakes the controller
            return Result(0)
        if "delete" in args:
            at = args.index("delete")
            kind, name = KINDS[args[at + 1]], args[at + 2]
            self.objs[kind].pop(name, None)
            if kind == "restore":
                for owned in ("job", "pod", "configmap"):
                    for other in list(self.objs[owned]):
                        if other.startswith(name):
                            self.objs[owned].pop(other)
            return Result(0)
        raise AssertionError(f"the simulated harness ran an unexpected command: {args[:6]}")

    def stamp(self) -> str:
        return dt.datetime.fromtimestamp(self.now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- the Approval controller ----------------------------------------
    def verify(self, approval: dict[str, Any]) -> None:
        subject = (approval.get("spec") or {}).get("subjectRef") or {}
        schedule = self.objs["rehearsalschedule"].get(subject.get("name") or "") or {}
        if self.nothing_verifies:
            status, reason = "False", "PlanHashMismatch"
        elif approval["metadata"]["name"] == f"{d3.REHEARSAL_REFUSED_SCHEDULE}-standing":
            status, reason = "False", "KeyRetired"
        else:
            status, reason = "True", "Verified"
        approval["status"] = {
            "matchedKeyId": "ab" * 32,
            "verifiedSubjectRef": {"kind": "RehearsalSchedule", "name": subject.get("name"),
                                   "uid": (schedule.get("metadata") or {}).get("uid")},
            "conditions": [{"type": "Verified", "status": status, "reason": reason}],
        }

    # --- the RehearsalSchedule controller -------------------------------
    def tick(self) -> None:
        for restore in list(self.objs["restore"].values()):
            self.advance_restore(restore)
        for job in list(self.objs["job"].values()):
            done = job.get("finishedAt")
            if done is not None and self.now >= done + self.job_ttl_seconds:
                self.objs["job"].pop(job["metadata"]["name"], None)
                self.objs["pod"].pop(job["metadata"]["name"], None)
        for name in list(self.objs["rehearsalschedule"]):
            if self.now - self.last_reconcile.get(name, -1e18) >= d3.REHEARSAL_REQUEUE_SECONDS:
                self.last_reconcile[name] = self.now
                self.reconcile(self.objs["rehearsalschedule"][name])

    def reconcile(self, schedule: dict[str, Any]) -> None:
        spec, status = schedule["spec"], schedule["status"]
        name = schedule["metadata"]["name"]
        if spec.get("suspend"):
            return  # a suspended schedule observes nothing and records nothing
        active_name = (status.get("activeRestoreRef") or {}).get("name")
        active = self.objs["restore"].get(active_name or "")
        if active is not None and d3.terminal(active) and not active.get("observed"):
            active["observed"] = True
            st = active["status"]
            if st.get("phase") == "Succeeded":
                status["lastSucceeded"] = {"restoreRef": {"name": active_name},
                                           "at": self.stamp(), "evidence": "Valid",
                                           "rtoSeconds": self.rehearsal_seconds}
                set_condition(schedule, "RehearsalHealthy", "True", "Passed")
            else:
                status["lastFailed"] = {"restoreRef": {"name": active_name},
                                        "at": self.stamp(), "reason": st.get("reason")}
                set_condition(schedule, "RehearsalHealthy", "False", "Failed")
            status["activeRestoreRef"] = None
        period = 60 if spec["schedule"] == "* * * * *" else 600
        due = math.floor(self.now / period) * period
        slot = slot_name_for_sim(due)
        if status.get("lastScheduledSlot") == slot:
            return
        if active is not None and not d3.terminal(active):
            if not self.silent_skip:
                self.skip(schedule, slot, "ConcurrencyBlocked")
            if self.second_restore_during_slot:
                self.fire(schedule, slot)
            return
        target = spec["target"]["clusterRef"]["name"]
        for other in self.objs["restore"].values():
            labels = other["metadata"].get("labels") or {}
            if (labels.get("logweir.dev/rehearsal-target") == target
                    and labels.get("logweir.dev/rehearsal-schedule") != name
                    and not d3.terminal(other)):
                self.skip(schedule, slot, "TargetBusy")
                set_condition(schedule, "Ready", "True", "Scheduled",
                              f"the Restore {other['metadata']['name']} is rehearsing against "
                              f"the same target cluster `{target}`")
                self.target_busy_seen.append(name)
                return
        approval = self.objs["approval"].get(spec["authorization"]["standingApprovalRef"]["name"])
        if approval is None or d3.condition(approval, "Verified").get("status") != "True":
            self.skip(schedule, slot, "AuthorizationInvalid")
            set_condition(schedule, "Authorized", "False", "AuthorizationInvalid")
            return
        self.fire(schedule, slot)

    def skip(self, schedule: dict[str, Any], slot: str, reason: str) -> None:
        """`rehearsal_schedule.rs::status_patch` since verdict-precedence: the
        DUE slot is named and consumed in the same patch (a skipped slot is
        skipped, never deferred) — or, under the mutant, the old behaviour."""
        status = schedule["status"]
        if self.skip_defers_slot:
            status["lastSkipped"] = {"slot": slot_name_for_sim(self.now), "reason": reason}
            return
        status["lastSkipped"] = {"slot": slot, "reason": reason}
        status["lastScheduledSlot"] = slot

    def fire(self, schedule: dict[str, Any], slot: str) -> None:
        name = schedule["metadata"]["name"]
        child = f"{d3.REHEARSAL_RESTORE_PREFIX}{name}-{slot}"
        if self.second_restore_during_slot and child in self.objs["restore"]:
            child = f"{child}-2"
        spec = schedule["spec"]
        self.objs["restore"][child] = {
            "metadata": {"name": child, "uid": str(uuid.uuid4()),
                         "creationTimestamp": self.stamp(),
                         "labels": {"logweir.dev/rehearsal-schedule": name,
                                    "logweir.dev/rehearsal-slot": slot,
                                    "logweir.dev/rehearsal-target":
                                        spec["target"]["clusterRef"]["name"]}},
            "spec": {"authorization": {
                "kind": "Standing",
                "approvalRef": {"name": spec["authorization"]["standingApprovalRef"]["name"]},
                "rehearsalScheduleRef": {"name": name}}},
            "status": {},
            "scheduleUid": schedule["metadata"]["uid"],
        }
        schedule["status"]["lastScheduledSlot"] = slot
        schedule["status"]["activeRestoreRef"] = {"name": child}
        set_condition(schedule, "Authorized", "True", "Authorized")

    def advance_restore(self, restore: dict[str, Any]) -> None:
        name = restore["metadata"]["name"]
        status = restore["status"]
        labels = restore["metadata"]["labels"]
        schedule, slot = labels["logweir.dev/rehearsal-schedule"], labels["logweir.dev/rehearsal-slot"]
        mapped = d3.mapped_topic(restore["scheduleUid"], d3.REHEARSAL_TOPIC)
        if not status:
            # admission, the Job, its pod and the five-member bundle
            status.update({"phase": "Running", "jobRef": {"name": name}})
            container = {
                "command": ["logweir"],
                "args": ["drill", "run",
                         "--standing-authorization", "/approval/standing-authorization.json",
                         "--authorization-keys", "/approval/authorization-keys.json",
                         "--triggered-by", f"rehearsal/{schedule}/{slot}"],
                "env": [{"name": "LOGWEIR_EXECUTION_AUTHORIZATION_KIND", "value": "standing"},
                        {"name": "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID",
                         "value": restore["scheduleUid"]}],
            }
            self.objs["job"][name] = {"metadata": {"name": name},
                                      "spec": {"ttlSecondsAfterFinished": self.job_ttl_seconds,
                                               "template": {"spec": {"containers": [container]}}}}
            self.objs["pod"][name] = {"metadata": {"name": name, "labels": {
                "batch.kubernetes.io/job-name": name}}, "spec": {"containers": [container]}}
            self.objs["configmap"][f"{name}-approval-bundle"] = {
                "metadata": {"name": f"{name}-approval-bundle"},
                "data": {k: "x" for k in d3.STANDING_BUNDLE_KEYS}}
            restore["startedAt"] = self.now
            # phase 0: a mapped name that already exists is refused, untouched
            restore["guardRefused"] = mapped in self.topics
            if not restore["guardRefused"]:
                self.topics.add(mapped)
            return
        if d3.terminal(restore):
            return
        if restore["guardRefused"] and self.now >= restore["startedAt"] + 20:
            status.update({"phase": "Failed", "reason": "GuardRefused",
                           "exitReason": "GuardRefused"})
            self.objs["job"][name]["finishedAt"] = self.now
            self.last_reconcile.pop(schedule, None)
        elif not restore["guardRefused"] and self.now >= restore["startedAt"] + self.rehearsal_seconds:
            self.topics.discard(mapped)
            run_id = f"run-{name}"
            self.runs[f"logweir/drills/{run_id}.json"] = {
                "run_id": run_id, "triggered_by": f"rehearsal/{schedule}/{slot}",
                "approval": {"approver": f"standing-authorization/{schedule}", "ticket": ""}}
            status.update({"phase": "Succeeded", "reason": "Completed", "outcome": "pass",
                           "evidence": {"scorecardKey": f"logweir/drills/{run_id}.json",
                                        "offsetReportKey": f"logweir/drills/{run_id}.offsets.json",
                                        "verification": {"result": "Valid"}}})
            self.objs["job"][name]["finishedAt"] = self.now
            self.last_reconcile.pop(schedule, None)  # `.owns(restores)` wakes the schedule


def merge(into: dict[str, Any], patch: dict[str, Any]) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(into.get(key), dict):
            merge(into[key], value)
        else:
            into[key] = copy.deepcopy(value)


def set_condition(obj: dict[str, Any], kind: str, status: str, reason: str,
                  message: str = "") -> None:
    conditions = obj["status"].setdefault("conditions", [])
    for c in conditions:
        if c["type"] == kind:
            c.update({"status": status, "reason": reason, "message": message})
            return
    conditions.append({"type": kind, "status": status, "reason": reason, "message": message})


def slot_name_for_sim(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y%m%d-%H%M%S")




def fake_key(tag: str) -> dict[str, Any]:
    work = pathlib.Path(tempfile.mkdtemp(prefix=f"{tag}-"))
    private = work / "signing.pem"
    private.write_text("not a key\n")
    return {"dir": work, "private": private, "spkiPem": "", "keyId": uuid.uuid4().hex * 2}


def simulate(cluster: FakeCluster, **extra_patches: Any) -> dict[str, str]:
    """Run `d3_live.rehearsal()` against `cluster` and return scenario -> verdict."""
    patches = {
        "time": cluster, "apply": cluster.apply, "get": cluster.get, "get_opt": cluster.get_opt,
        "lst": cluster.lst, "run": cluster.run,
        "target_topics": lambda: set(cluster.topics),
        "target_topic_create": lambda name: (name not in cluster.topics
                                             and not cluster.topics.add(name)),
        "target_topic_delete": lambda name: cluster.topics.discard(name),
        "rehearsal_target_cluster": lambda: {"status": {"clusterId": "sim-cluster"}},
        "rehearsal_trust": lambda *a, **k: None,
        "rehearsal_point": lambda: {"pointId": "lwp1-" + "a" * 32, "selectable": True},
        "mint_signing_key": fake_key,
        "mint_standing": lambda *a, **k: ("envelope", "sidecar"),
        "cat": lambda bucket, key: json.dumps(cluster.runs.get(key, {})).encode(),
        "fetchable": lambda bucket, key: bool(key),
        **extra_patches,
    }
    saved = {name: getattr(d3, name) for name in patches}
    d3.STATE["scenarios"] = {}
    d3.STATE["commands"] = []
    for name, value in patches.items():
        setattr(d3, name, value)
    try:
        d3.rehearsal()
    finally:
        for name, value in saved.items():
            setattr(d3, name, value)
    return {k: v["verdict"] for k, v in d3.STATE["scenarios"].items()}


ROWS = [
    "rehearsal-1-setup-standing-approval-verifies",
    "rehearsal-2-restore-on-spec-authorization",
    "rehearsal-3-job-carries-the-standing-mount",
    "rehearsal-4-scorecard-names-the-schedule",
    "rehearsal-5-schedule-records-the-pass",
    "rehearsal-6-topics-owned-torn-down-unrelated-survives",
    "rehearsal-7-second-slot-is-concurrency-blocked",
    d3.REHEARSAL_SKIP_CONSUMED_ROW,
    "rehearsal-8-leftover-guard-keeps-the-pre-created-topic",
    "rehearsal-9-evidence-outlives-the-job-ttl",
    "rehearsal-10-refused-arm-reaches-no-job",
    "rehearsal-shared-broker-left-as-found",
    "rehearsal-minted-private-keys-never-outlive-the-row",
]


# What ANOTHER run and the lab keep on the shared scratch broker: the lab's own
# marker, another run's rehearsal topic under ITS rendered prefix, and the bare
# fixed witness name every run used to share (review MEDIUM-3).
FOREIGN = {"logweir.scratch", "rehearsal-deadbeef-orders", "rehearsal-not-ours",
           "rehearsal-not-ours-someoneelse-20260101t0000z"}


def test_a_correct_controller_passes_every_row_and_nothing_is_left_behind() -> None:
    cluster = FakeCluster(topics=FOREIGN)
    verdicts = simulate(cluster)
    assert verdicts == {row: "PASS" for row in ROWS}, verdicts
    # ISOLATION, MEASURED: with the arms quiesced one after another, no slot
    # anywhere was ever skipped TargetBusy (review HIGH-2).
    assert cluster.target_busy_seen == [], cluster.target_busy_seen
    # THE BROKER AS IT WAS FOUND: every topic this run made is gone, and not
    # one topic it did not make was touched.
    assert cluster.topics == FOREIGN, cluster.topics ^ FOREIGN
    assert all(s["spec"]["suspend"] for s in cluster.objs["rehearsalschedule"].values())


def test_a_ttl_longer_than_the_row_can_wait_is_stood_in_for() -> None:
    """lab-refresh-9: the product's default Job TTL is seven days. Step 9
    removes the finished Job the way the TTL controller would and still
    requires the four signed objects; the row says who removed it."""
    cluster = FakeCluster(topics=FOREIGN, job_ttl_seconds=604800)
    verdicts = simulate(cluster)
    assert verdicts["rehearsal-9-evidence-outlives-the-job-ttl"] == "PASS", verdicts


def test_a_witness_that_already_exists_is_neither_adopted_nor_deleted() -> None:
    witness = d3.rehearsal_witness_topic()
    cluster = FakeCluster(topics=FOREIGN | {witness})
    raised = ""
    try:
        simulate(cluster)
    except RuntimeError as e:
        raised = str(e)
    assert "will neither adopt nor delete it" in raised, raised
    assert witness in cluster.topics, "the run deleted a witness it did not create"
    verdicts = {k: v["verdict"] for k, v in d3.STATE["scenarios"].items()}
    assert verdicts.get("rehearsal-minted-private-keys-never-outlive-the-row") == "PASS", verdicts


def test_mutant_a_second_restore_during_the_occupied_slot_fails_step_seven() -> None:
    """Review HIGH-1's first scenario, exactly: a second `Restore` while the first
    runs, and NO skip recorded. The pre-fix loop waited for the first to finish
    and recorded NOT-REACHED."""
    verdicts = simulate(FakeCluster(second_restore_during_slot=True, silent_skip=True))
    assert verdicts["rehearsal-7-second-slot-is-concurrency-blocked"] == "FAIL", verdicts
    verdicts = simulate(FakeCluster(second_restore_during_slot=True))
    assert verdicts["rehearsal-7-second-slot-is-concurrency-blocked"] == "FAIL", verdicts


def test_mutant_a_skip_that_defers_its_slot_fails_step_seven_b() -> None:
    """REHEARSAL-SKIP-DEFERS-SLOT, modelled: the skip names `now` and leaves
    `lastScheduledSlot` alone, so the blocked slot fires the moment the first
    rehearsal ends. Step 7 still passes (a skip WAS recorded); 7b must not."""
    verdicts = simulate(FakeCluster(skip_defers_slot=True))
    assert verdicts["rehearsal-7-second-slot-is-concurrency-blocked"] == "PASS", verdicts
    assert verdicts[d3.REHEARSAL_SKIP_CONSUMED_ROW] == "FAIL", verdicts


def test_mutant_a_slot_skipped_without_a_record_fails_step_seven() -> None:
    verdicts = simulate(FakeCluster(silent_skip=True))
    assert verdicts["rehearsal-7-second-slot-is-concurrency-blocked"] == "FAIL", verdicts


def test_mutant_a_first_rehearsal_too_short_to_meet_a_slot_is_not_reached() -> None:
    verdicts = simulate(FakeCluster(rehearsal_seconds=20))
    assert verdicts["rehearsal-7-second-slot-is-concurrency-blocked"] == "NOT-REACHED", verdicts


def test_mutant_nothing_verifies_is_never_a_passing_control() -> None:
    verdicts = simulate(FakeCluster(nothing_verifies=True))
    assert verdicts["rehearsal-1-setup-standing-approval-verifies"] == "FAIL", verdicts
    assert all(verdicts[r] == "NOT-REACHED" for r in ROWS[1:9]), verdicts
    assert verdicts["rehearsal-10-refused-arm-reaches-no-job"] == "INCONCLUSIVE", verdicts


def test_mutant_without_isolation_the_harness_names_its_own_contamination() -> None:
    """The harness as the review found it: no arm is ever suspended again.

    `quiesce_arm` is replaced by a no-op, so every arm keeps firing after its
    row. The rows must then NAME the contamination — `TargetBusy` is seen and
    whatever it decides is HARNESS-FAULT — rather than hand the harness's
    ordering to the product as a FAIL.
    """
    cluster = FakeCluster()

    def no_quiesce(name, evidence, **_kw):
        return {"schedule": name, "restores": {}, "heldAndDeleted": [], "stillRunning": [],
                "quiet": True}

    verdicts = simulate(cluster, quiesce_arm=no_quiesce)
    assert cluster.target_busy_seen, "the mutant did not reproduce the contamination"
    contaminated = [r for r in ("rehearsal-7-second-slot-is-concurrency-blocked",
                                "rehearsal-8-leftover-guard-keeps-the-pre-created-topic",
                                "rehearsal-10-refused-arm-reaches-no-job")
                    if verdicts[r] != "PASS"]
    assert contaminated, verdicts
    assert all(verdicts[r] == "HARNESS-FAULT" for r in contaminated), verdicts


if __name__ == "__main__":
    failures = 0
    for fn_name, fn in sorted(globals().items()):
        if fn_name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {fn_name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL  {fn_name} — {e}")
    raise SystemExit(1 if failures else 0)
