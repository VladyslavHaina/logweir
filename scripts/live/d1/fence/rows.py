#!/usr/bin/env python3
"""D1 §13.2's rows that only a fenced controller can measure.

Every scenario here was recorded `not-run` by the W8 wave with the same cause:
the only controller reconciling the test namespace was the shared lab release,
and these eight rows are all statements ABOUT a controller — stop it for seven
minutes, run two of it, hold one of its requests, count its reads, swap it for
the build that came before. `fenced.py` supplies that controller; this supplies
the measurements.

The rules the W8 harness set and this keeps: an assertion is never relaxed to
make a red row green, a row that cannot run is recorded `not-run` WITH THE
MISSING CAPABILITY NAMED, and every injection this file makes is declared in
`INJECTIONS` so a reader can discount it.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
import subprocess
import time
from typing import Any

import fixture

from . import fenced

# Declared injections: a change this harness makes to the environment so that a
# criterion becomes reachable. None of them touches a decision Logweir makes.
INJECTIONS: dict[str, str] = {
    "L-05.2-3": (
        "seeds 20 pre-upgrade runs as objects (a `BackupSchedule` controller "
        "ownerReference plus a terminal status) while the controller is scaled to 0, "
        "instead of waiting for 20 real runs of the 4956785 build. The legacy owner "
        "entry is the only thing the pre-upgrade controller contributed to the "
        "migration's input; every step of the migration itself is then measured "
        "unchanged, on the real controller, through the real proxy."
    ),
    "L-05.2-6": (
        "seeds 500 terminal runs the same way. D1 §6.7's criterion is about what the "
        "scheduler READS when a history is large; how the history got large is not "
        "part of it."
    ),
    "seeding/force-inventory": (
        "clears `status.history` after seeding, because the schedule was inventoried "
        "when it was created — before the runs existed — and D1 §4.5 step 1 would not "
        "look again for sixty minutes. It asks for the inventory now and decides "
        "nothing about its outcome. Same injection W8 declared for L-05.2-1."
    ),
    "L-05.2-3/L-05.2-2u": (
        "the proxy answers one request with 409 / 503. Both are the scenario's own "
        "words ('declared harness injection' in D1 §13.2)."
    ),
    "L-04-2b": (
        "backdates `status.policy.effectiveSince` AND "
        "`status.missedSlots.lastEvaluatedSlot` on a `Latest` schedule: the first "
        "decides whether a slot in a gap is caught up or skipped, the second decides "
        "whether there is a gap at all. Recorded as a SEPARATE row so it can never "
        "stand in for L-04-2, which needs no injection because a real outage moves "
        "both fields by itself."
    ),
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def s3_schedule(H: Any, name: str, **spec: Any) -> dict[str, Any]:
    """A schedule whose archive is a plain `s3://` URL.

    The fenced controller carries this namespace's own installation policy, so
    `s3://` resolves to this namespace's MinIO without a `BackupDestination` —
    which the main@4956785 build needs, because `BackupDestination` did not
    exist when it was built.
    """
    body: dict[str, Any] = {
        "schedule": "*/2 * * * *",
        "sourceRef": {"name": "source"},
        "topics": ["t1"],
        # The ENDPOINT reaches the runner from the controller's own environment
        # (`archive_addressing_env`), but the CREDENTIAL never does — it comes
        # only from `spec.archive.secretRef`, "a DIFFERENT PRINCIPAL ... which
        # must never be handed to a pod that writes". Without it the runner
        # falls through to the EC2 instance-metadata chain and every run fails
        # `S3 PUT ... http://169.254.169.254/latest/api/token`, which looks like
        # a scheduling failure and is not one. `secretRef` exists in the
        # 4956785 schema too, so both controllers read the same field.
        "archive": {
            "url": f"s3://{fixture.BUCKET}/fence/{name}",
            "secretRef": {"name": "logweir-s3"},
        },
        "concurrencyPolicy": "Forbid",
        "suspend": False,
        "activeDeadlineSeconds": 600,
    }
    body.update(spec)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": {"name": name, "namespace": H.NS, "labels": dict(H.LABELS)},
        "spec": body,
    }


def parse_ts(value: str) -> dt.datetime:
    """A Kubernetes timestamp, including the nanosecond form a status carries.

    `status.policy.effectiveSince` and `status.lastSlot.decidedAt` are written
    with nanosecond precision (`2026-09-18T12:20:20.252496504Z`), which the
    harness's second-precision reader refuses outright — and a scenario that
    dies on its own clock parser has measured nothing.
    """
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})",
        value.strip(),
    )
    if not match:
        raise ValueError(f"not a Kubernetes timestamp: {value!r}")
    head, fraction, zone = match.groups()
    micros = (fraction or "0")[:6].ljust(6, "0")
    offset = "+00:00" if zone == "Z" else (zone if ":" in zone else f"{zone[:3]}:{zone[3:]}")
    return dt.datetime.fromisoformat(f"{head}.{micros}{offset}")


def reset_schedules(H: Any, names: list[str]) -> dict[str, Any]:
    """Delete these schedules and everything they produced, so a row can re-run.

    A scenario that re-runs against its own earlier status measures the earlier
    run, not this one. Only objects this harness created are touched, and only
    in its own namespace.
    """
    removed: dict[str, Any] = {"schedules": [], "runs": [], "configMaps": [], "jobs": []}
    for name in names:
        existing = H.get_opt("backupschedule", name)
        if existing is None:
            continue
        uid = existing["metadata"]["uid"]
        for backup in H.backups_of(uid):
            run = backup["metadata"]["name"]
            H.run(H.KN + ["delete", "backup", run, "--ignore-not-found", "--wait=false"],
                  check=False, timeout=90)
            removed["runs"].append(run)
        for cm in H.lst("configmaps"):
            if cm["metadata"]["name"].startswith(f"logweir-backup-{name}-"):
                H.run(H.KN + ["delete", "configmap", cm["metadata"]["name"],
                              "--ignore-not-found"], check=False, timeout=90)
                removed["configMaps"].append(cm["metadata"]["name"])
        for job in H.lst("jobs"):
            if job["metadata"]["name"].startswith(f"logweir-backup-{name}-"):
                H.run(H.KN + ["delete", "job", job["metadata"]["name"], "--ignore-not-found"],
                      check=False, timeout=90)
                removed["jobs"].append(job["metadata"]["name"])
        H.run(H.KN + ["delete", "backupschedule", name, "--ignore-not-found"],
              check=False, timeout=120)
        removed["schedules"].append(f"{name} uid={uid}")
    if removed["schedules"]:
        H.wait_until(
            lambda: all(H.get_opt("backupschedule", n) is None for n in names),
            timeout=120,
            interval=2.0,
            what="the previous attempt's schedules to be gone",
        )
    return removed


def suspend_all_but(H: Any, keep: set[str]) -> list[str]:
    """Quiet every other schedule so one measurement is about one schedule."""
    suspended: list[str] = []
    for item in H.lst("backupschedules"):
        name = item["metadata"]["name"]
        if name in keep or item["spec"].get("suspend") is True:
            continue
        H.patch("backupschedule", name, {"spec": {"suspend": True}})
        suspended.append(name)
    return suspended


def attempt_zero_slots(backups: list[dict[str, Any]]) -> list[str]:
    return sorted(
        b["spec"]["slot"]
        for b in backups
        if b["spec"].get("slot") and int((b["spec"].get("trigger") or {}).get("attempt", 0)) == 0
    )


def seed_legacy_runs(
    H: Any,
    schedule: dict[str, Any],
    count: int,
    *,
    first_slot: dt.datetime,
    legacy_owner: bool,
    concurrency: int = 10,
) -> dict[str, Any]:
    """Create `count` terminal runs of `schedule` as objects. DECLARED INJECTION.

    The controller must be scaled to 0 while this runs: a `Backup` is created
    before its status can be patched, and a running controller would start real
    Jobs for every one of them in the gap.
    """
    if fenced.pods(H):
        raise H.Failure("refusing to seed runs while the fenced controller is running")
    name = schedule["metadata"]["name"]
    uid = schedule["metadata"]["uid"]
    owner = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "name": name,
        "uid": uid,
        "controller": True,
        "blockOwnerDeletion": True,
    }
    items = []
    names = []
    for index in range(count):
        instant = first_slot + dt.timedelta(minutes=2 * index)
        slot = H.slot_of(instant)
        run = H.run_name(name, slot)
        names.append(run)
        metadata: dict[str, Any] = {
            "name": run,
            "namespace": H.NS,
            "labels": {**H.LABELS, "logweir.dev/slot": slot},
        }
        if legacy_owner:
            metadata["ownerReferences"] = [owner]
        else:
            metadata["labels"]["logweir.dev/schedule-uid"] = uid
            metadata["annotations"] = {"logweir.dev/history-retained-from-owner": uid}
        items.append(
            {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "Backup",
                "metadata": metadata,
                "spec": {
                    "sourceRef": {"name": "source"},
                    "topics": ["t1"],
                    # Required by the CRD; `schedule` is what a scheduled run
                    # carries, which is what these objects stand in for.
                    "triggeredBy": "schedule",
                    "archive": {"url": f"s3://{fixture.BUCKET}/fence/{name}/{slot}"},
                    "slot": slot,
                    "scheduleRef": {
                        "name": name,
                        "uid": uid,
                        "generation": schedule["metadata"].get("generation", 1),
                    },
                    "trigger": {"kind": "Scheduled", "attempt": 0},
                    "deadlineSeconds": 600,
                },
            }
        )
    started = time.time()
    H.run(
        H.K + ["apply", "-f", "-"],
        data=json.dumps({"apiVersion": "v1", "kind": "List", "items": items}),
        timeout=600,
    )
    created = time.time() - started
    status = json.dumps(
        {
            "status": {
                "phase": "Succeeded",
                "startedAt": H.now(),
                "completedAt": H.now(),
                "conditions": [
                    {
                        "type": "Succeeded",
                        "status": "True",
                        "reason": "Completed",
                        "message": "seeded pre-upgrade run (declared injection)",
                        "lastTransitionTime": H.now(),
                    }
                ],
            }
        }
    )
    pending: list[Any] = []
    for run in names:
        pending.append(
            subprocess.Popen(  # noqa: S603
                H.KN
                + ["patch", "backup", run, "--subresource=status", "--type", "merge",
                   "-p", status],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
        )
        if len(pending) >= concurrency:
            for proc in pending:
                proc.wait(timeout=120)
            pending = []
    for proc in pending:
        proc.wait(timeout=120)
    patched = time.time() - started
    terminal = [b for b in H.lst("backups") if b["metadata"]["name"] in set(names)
                and (b.get("status") or {}).get("phase") == "Succeeded"]
    # DECLARED INJECTION, and without it these two rows measure nothing. The
    # schedule was inventoried the moment it was created, before any of these
    # runs existed, so it recorded `legacyMigratableRuns: 0` — and D1 §4.5 step
    # 1 will not inventory again for sixty minutes. Clearing `status.history`
    # asks for the inventory now and decides nothing else; `run.py`'s
    # `force_inventory` is the same injection W8 declared for L-05.2-1.
    H.force_inventory(name)
    return {
        "schedule": name,
        "requested": count,
        "created": len(items),
        "terminal": len(terminal),
        "names": names,
        "createSeconds": round(created, 1),
        "totalSeconds": round(patched, 1),
        "legacyOwner": legacy_owner,
        "inventoryForced": True,
    }


def runs_of(H: Any, name: str, uid: str) -> list[dict[str, Any]]:
    """Every run of this schedule, in any of the three shapes it can carry.

    `run.py`'s `backups_of` matches `spec.scheduleRef.uid`, which the
    main@4956785 build does not write: it writes `scheduleRef: {name}` alone and
    attaches a `BackupSchedule` controller ownerReference — which is precisely
    the legacy shape the migration exists to detach. A row that swaps to that
    image and keeps the new membership rule sees none of the runs it just made.

    The three rules are D1 §3.1's own: the UID on the ref (current), the owner
    entry (pre-upgrade), and the `logweir.dev/schedule-uid` label (migrated).
    """
    out = []
    for backup in H.lst("backups"):
        ref = backup["spec"].get("scheduleRef") or {}
        meta = backup["metadata"]
        owners = [
            o
            for o in (meta.get("ownerReferences") or [])
            if o.get("kind") == "BackupSchedule" and o.get("uid") == uid
        ]
        labelled = (meta.get("labels") or {}).get("logweir.dev/schedule-uid") == uid
        by_name = ref.get("name") == name and meta["name"].startswith(f"logweir-backup-{name}-")
        if ref.get("uid") == uid or owners or labelled or by_name:
            out.append(backup)
    return sorted(out, key=lambda b: b["metadata"]["name"])


def schedule_owner(backup: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        o
        for o in (backup["metadata"].get("ownerReferences") or [])
        if o.get("kind") == "BackupSchedule"
    ]


# ---------------------------------------------------------------------------
# PLAT-04.2
# ---------------------------------------------------------------------------


def observe_decisions(
    H: Any, names: list[str], *, seconds: float, interval: float = 1.5
) -> dict[str, list[dict[str, Any]]]:
    """Every distinct `status.lastSlot` a schedule writes inside a window.

    D1 §13.2's downtime criterion is "PASS WITHIN 60 s OF RESTART:
    ... `lastSlot.disposition=CaughtUp`", and `lastSlot` is one field that the
    controller overwrites with its NEXT decision. Reading it once at the end of
    the window therefore measures whatever happened last — on the first attempt
    at this row, a `CaughtUp` written 8 s after the restart was replaced 31 s
    later by `Exhausted/RunFailed` for the same slot, and the harness recorded
    a red for a decision the controller had made correctly. Every decision is
    collected as it is written, and the assertion names the one it is about.
    """
    seen: dict[str, list[dict[str, Any]]] = {name: [] for name in names}
    deadline = time.time() + seconds
    while time.time() < deadline:
        for name in names:
            obj = H.get_opt("backupschedule", name)
            last = ((obj or {}).get("status") or {}).get("lastSlot")
            if not last:
                continue
            if not seen[name] or seen[name][-1] != last:
                if last not in seen[name]:
                    seen[name].append(last)
        time.sleep(interval)
    return seen


def l_04_2(H: Any) -> dict[str, Any]:
    """Long downtime, both halves, with a controller that is really stopped."""
    reset = reset_schedules(H, ["dt-none", "dt-latest"])
    none_obj = H.apply(
        s3_schedule(H, "dt-none", startingDeadlineSeconds=60, catchUpPolicy="None")
    )
    latest_obj = H.apply(
        s3_schedule(H, "dt-latest", startingDeadlineSeconds=60, catchUpPolicy="Latest")
    )
    observed = {
        "dt-none": H.await_schedule_observed("dt-none", timeout=180),
        "dt-latest": H.await_schedule_observed("dt-latest", timeout=180),
    }
    before = {
        name: sorted(b["metadata"]["name"] for b in H.backups_of(obj["metadata"]["uid"]))
        for name, obj in (("dt-none", none_obj), ("dt-latest", latest_obj))
    }
    down = fenced.scale(H, 0)
    down_at = dt.datetime.now(dt.timezone.utc)

    # Restart 80 s after a `*/2` slot instant, inside D1's 70–110 s window, so
    # that no slot is within its 60 s starting deadline when the controller
    # comes back and the catch-up decision is the only thing under test.
    earliest = down_at + dt.timedelta(seconds=420)
    slot = earliest.replace(second=0, microsecond=0)
    while slot.minute % 2 or slot + dt.timedelta(seconds=90) < earliest:
        slot += dt.timedelta(minutes=1)
    target = slot + dt.timedelta(seconds=90)
    # The scale command is issued a little early because the window is measured
    # against the CONTROLLER being back, not against the harness asking for it:
    # `up_at` below is the weirkeeper container's own `startedAt`.
    H.sleep_until(
        target - dt.timedelta(seconds=12),
        label=f"7-minute outage; restart ~90s after slot {H.slot_of(slot)}",
    )
    up = fenced.scale(H, 1)
    started = up.get("controllerStartedAt") or []
    up_at = parse_ts(started[0]) if started else dt.datetime.now(dt.timezone.utc)
    since_slot = (up_at - slot).total_seconds()
    H.require(
        70 <= since_slot <= 110,
        f"restart landed {since_slot:.0f}s after slot {H.slot_of(slot)}, outside D1's 70–110 s "
        "window; the scenario is invalid rather than failed",
    )
    outage = (up_at - down_at).total_seconds()
    H.require(outage >= 420, f"outage was {outage:.0f}s, D1 asks for 7 minutes")

    # D1 gives 60 s from restart for the decision to be written, and `lastSlot`
    # is overwritten by the next one, so the window is WATCHED, not sampled.
    remaining = 60 - (dt.datetime.now(dt.timezone.utc) - up_at).total_seconds()
    decisions = observe_decisions(H, ["dt-none", "dt-latest"], seconds=max(5.0, remaining))
    after_restart = {
        name: [d for d in items if parse_ts(str(d["decidedAt"])) >= up_at]
        for name, items in decisions.items()
    }

    none_after = H.get("backupschedule", "dt-none")
    latest_after = H.get("backupschedule", "dt-latest")
    none_runs = H.backups_of(none_obj["metadata"]["uid"])
    latest_runs = H.backups_of(latest_obj["metadata"]["uid"])
    restart_slot = H.slot_of(up_at)

    new_none = [b for b in none_runs if b["metadata"]["name"] not in before["dt-none"]]
    new_latest = [b for b in latest_runs if b["metadata"]["name"] not in before["dt-latest"]]

    # dt-none: nothing from the outage, and the count of what it skipped.
    stale_none = [b["metadata"]["name"] for b in new_none if b["spec"]["slot"] < restart_slot]
    missed = (none_after.get("status") or {}).get("missedSlots") or {}
    H.require(
        not stale_none,
        f"dt-none created runs for slots before the restart: {stale_none}",
        obj=none_after,
        dumps={"dt-none": none_after},
    )
    H.require(
        int(missed.get("count", 0)) >= 3,
        f"dt-none missedSlots.count is {missed.get('count')}, D1 asks for >= 3 after a "
        "seven-minute outage of a */2 schedule",
        obj=none_after,
        dumps={"dt-none": none_after},
    )

    # dt-latest: exactly one CatchUp run, for the latest slot that was due.
    catchups = [b for b in new_latest if (b["spec"].get("trigger") or {}).get("kind") == "CatchUp"]
    last_slot = (latest_after.get("status") or {}).get("lastSlot") or {}
    due_before_restart = H.slot_of(slot)
    H.require(
        len(catchups) == 1,
        f"dt-latest produced {len(catchups)} CatchUp runs, D1 asks for exactly one: "
        f"{[b['metadata']['name'] for b in catchups]}",
        obj=latest_after,
        dumps={"dt-latest": latest_after},
    )
    H.require(
        catchups[0]["spec"]["slot"] == due_before_restart,
        f"the CatchUp run is for slot {catchups[0]['spec']['slot']}, not the latest slot due "
        f"before the restart ({due_before_restart})",
        obj=catchups[0],
        dumps={"catchUp": catchups[0], "dt-latest": latest_after},
    )
    caught_up = [
        d
        for d in after_restart["dt-latest"]
        if d.get("slot") == due_before_restart and d.get("disposition") == "CaughtUp"
    ]
    H.require(
        bool(caught_up),
        "dt-latest never wrote lastSlot.disposition=CaughtUp for slot "
        f"{due_before_restart} within 60 s of the restart; the decisions it did write were "
        f"{after_restart['dt-latest']}",
        obj=latest_after,
        dumps={"dt-latest": latest_after, "decisions": decisions},
    )
    missed_none = [
        d for d in after_restart["dt-none"] if d.get("slot") == due_before_restart
    ]
    H.require(
        bool(missed_none) and all(d.get("disposition") == "Missed" for d in missed_none),
        f"dt-none's decision for the same slot {due_before_restart} was not Missed: "
        f"{missed_none}",
        obj=none_after,
        dumps={"dt-none": none_after, "decisions": decisions},
    )

    # Neither ever has more than one attempt-0 Backup per slot.
    for label, runs in (("dt-none", none_runs), ("dt-latest", latest_runs)):
        slots = attempt_zero_slots(runs)
        H.require(
            len(slots) == len(set(slots)),
            f"{label} has duplicate attempt-0 runs for a slot: {slots}",
            dumps={label: {"runs": [b["metadata"]["name"] for b in runs]}},
        )

    # dt-none's next slot must still fire on time, as an ordinary Scheduled run.
    next_slot = H.wait_until(
        lambda: next(
            (
                b
                for b in H.backups_of(none_obj["metadata"]["uid"])
                if b["metadata"]["name"] not in before["dt-none"]
                and b["spec"]["slot"] >= restart_slot
            ),
            None,
        ),
        timeout=200,
        interval=3.0,
        what="dt-none's next slot to fire after the restart",
    )
    H.require(
        (next_slot["spec"].get("trigger") or {}).get("kind") == "Scheduled",
        f"dt-none's first post-restart run is {next_slot['spec'].get('trigger')}, not Scheduled",
        obj=next_slot,
    )
    return {
        "detail": {
            "outageSeconds": round(outage, 1),
            "restartAfterSlotSeconds": round(since_slot, 1),
            "slotBeforeRestart": due_before_restart,
            "downAt": down_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "upAt": up_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "podsWhileDown": down["pods"],
            "podsAfter": up["pods"],
            "dtNone": H.excerpt(
                none_after,
                "status.missedSlots.count",
                "status.missedSlots.countCapped",
                "status.lastSlot",
                "status.policy.effectiveSince",
            ),
            "dtNoneNewRuns": [
                {"name": b["metadata"]["name"], "slot": b["spec"]["slot"],
                 "trigger": b["spec"].get("trigger")}
                for b in new_none
            ],
            "dtLatest": H.excerpt(
                latest_after,
                "status.missedSlots.count",
                "status.lastSlot",
                "status.policy.effectiveSince",
            ),
            "dtLatestNewRuns": [
                {"name": b["metadata"]["name"], "slot": b["spec"]["slot"],
                 "trigger": b["spec"].get("trigger")}
                for b in new_latest
            ],
            "catchUpRun": H.excerpt(catchups[0], "spec.slot", "spec.trigger", "status.backupId"),
            "nextSlotAfterRestart": H.excerpt(next_slot, "spec.slot", "spec.trigger.kind"),
            "observedBeforeOutage": {
                k: ((v.get("status") or {}).get("policy") or {}).get("effectiveSince")
                for k, v in observed.items()
            },
            "previousAttemptCleared": reset,
            "slotDecisionsInTheWindow": after_restart,
            "caughtUpDecision": caught_up[0],
            "lastSlotAtTheEndOfTheWindow": last_slot,
        },
        "dumps": H.dump_objects(
            "L-04-2",
            {
                "dt-none": ("backupschedule", "dt-none"),
                "dt-latest": ("backupschedule", "dt-latest"),
                "catchUp": ("backup", catchups[0]["metadata"]["name"]),
            },
        ),
    }


def l_04_5(H: Any) -> dict[str, Any]:
    """Two fenced replicas: one object per slot and attempt, and a 409 adoption."""
    reset_schedules(H, ["dup"])
    obj = H.apply(
        s3_schedule(
            H,
            "dup",
            schedule="*/2 * * * *",
            retry={"maxRetries": 2, "delaySeconds": 60},
            concurrencyPolicy="Allow",
        )
    )
    uid = obj["metadata"]["uid"]
    H.await_schedule_observed("dup", timeout=180)
    two = fenced.scale(H, 2)
    H.require(len(two["pods"]) == 2, f"expected two fenced replicas, saw {two['pods']}")
    started = dt.datetime.now(dt.timezone.utc)

    # Two `*/2` slots, so a duplicate has two chances to appear.
    H.wait_until(
        lambda: len(
            [
                b
                for b in H.backups_of(uid)
                if parse_ts(b["metadata"]["creationTimestamp"]) > started
            ]
        )
        >= 2,
        timeout=420,
        interval=5.0,
        what="two slots to fire with two replicas running",
    )
    runs = H.backups_of(uid)
    by_key: dict[str, list[str]] = {}
    for backup in runs:
        trigger = backup["spec"].get("trigger") or {}
        key = f"{backup['spec'].get('slot')}#{trigger.get('attempt', 0)}"
        by_key.setdefault(key, []).append(backup["metadata"]["name"])
    duplicates = {k: v for k, v in by_key.items() if len(v) > 1}
    H.require(
        not duplicates,
        f"two replicas produced more than one Backup for a slot/attempt: {duplicates}",
        dumps={"runs": {k: v for k, v in by_key.items()}},
    )

    labelled = H.lst("backups", "logweir.dev/slot")  # existence-only label selector
    by_slot: dict[str, list[dict[str, Any]]] = {}
    for backup in labelled:
        if (backup["spec"].get("scheduleRef") or {}).get("uid") != uid:
            continue
        by_slot.setdefault(backup["metadata"]["labels"]["logweir.dev/slot"], []).append(backup)
    attempts = {
        slot: sorted(int((b["spec"].get("trigger") or {}).get("attempt", 0)) for b in items)
        for slot, items in by_slot.items()
    }
    for slot, found in attempts.items():
        H.require(
            found == list(range(len(found))),
            f"slot {slot} does not list attempts 0..k without duplicates: {found}",
            dumps={"bySlot": attempts},
        )

    logs = fenced.controller_logs(H, since="15m")
    H.artifact("logs/L-04-5-fenced-controllers.log", logs)
    # NARROW ON PURPOSE. A loose filter matched an unrelated warning that merely
    # contained "backup" and "already", which would have let the row claim an
    # adoption it had not seen. The 409 the criterion is about is the API
    # server's answer to a Backup CREATE, so the line must name that status.
    adoption = [
        line
        for line in logs.splitlines()
        if ("AlreadyExists" in line or "already exists" in line or '"code":409' in line)
        and "backups.logweir.dev" in line
    ]
    conflicts = [
        item
        for pod in two["pods"]
        for item in (fenced.captures(H, "backup_create", pod=pod["name"]) or [])
        if item.get("response") == 409
    ]
    H.require(
        bool(conflicts) or bool(adoption),
        "no 409 adoption was observed in either proxy's captures or either replica's log; "
        "D1 asks for at least one",
        dumps={"logTail": logs[-4000:]},
    )
    fenced.scale(H, 1)
    return {
        "detail": {
            "replicas": two["pods"],
            "runs": {k: v for k, v in by_key.items()},
            "attemptsBySlot": attempts,
            "adoptionLogLines": adoption[:6],
            "proxy409Creates": [
                {"name": c.get("name"), "at": c.get("at")} for c in conflicts[:6]
            ],
        },
        "dumps": H.dump_objects("L-04-5", {"dup": ("backupschedule", "dup")}),
    }


def l_04_2b(H: Any) -> dict[str, Any]:
    """The catch-up decision with a backdated `effectiveSince`. DECLARED INJECTION.

    Recorded as its own row so it can never be read as L-04-2. It measures the
    same DECISION — row 19 of D1 §4.7, `due >= effectiveSince` is a catch-up —
    without a real outage, which is what the review found was reachable in a
    shared namespace all along.
    """
    reset_schedules(H, ["cu-latest"])
    obj = H.apply(
        s3_schedule(H, "cu-latest", schedule="*/2 * * * *", startingDeadlineSeconds=60,
                    catchUpPolicy="Latest")
    )
    uid = obj["metadata"]["uid"]
    observed = H.await_schedule_observed("cu-latest", timeout=180)
    before = {b["metadata"]["name"] for b in H.backups_of(uid)}
    since = parse_ts(observed["status"]["policy"]["effectiveSince"])
    backdated = (since - dt.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # BOTH FIELDS, and the first attempt at this row shows why only one is not
    # enough. `effectiveSince` decides whether a slot in a gap is caught up or
    # skipped (D1 §4.7 row 19); `missedSlots.lastEvaluatedSlot` decides whether
    # there is a gap at all. Backdating the revision alone left
    # `lastEvaluatedSlot` at the current slot, so the scheduler had nothing to
    # walk and wrote `Admitted/Scheduled` for the slot that was due anyway —
    # W8's `L-04-cap` injected the second field and not the first, and saw the
    # mirror image of that. A real outage moves both, which is why L-04-2
    # beside this row needs no injection at all.
    gap_start = (since - dt.timedelta(minutes=20)).strftime("%Y%m%d-%H%M%S")
    H.run(
        H.KN
        + [
            "patch",
            "backupschedule",
            "cu-latest",
            "--subresource=status",
            "--type",
            "merge",
            "-p",
            json.dumps(
                {
                    "status": {
                        "policy": {**observed["status"]["policy"],
                                   "effectiveSince": backdated},
                        "missedSlots": {
                            "count": 0,
                            "countCapped": False,
                            "lastEvaluatedSlot": gap_start,
                        },
                    }
                }
            ),
        ],
        timeout=60,
    )
    after = H.wait_until(
        lambda: next(
            (
                b
                for b in H.backups_of(uid)
                if b["metadata"]["name"] not in before
                and (b["spec"].get("trigger") or {}).get("kind") == "CatchUp"
            ),
            None,
        ),
        timeout=260,
        interval=3.0,
        what="a CatchUp run once the gap is real from the controller's point of view",
    )
    schedule = H.get("backupschedule", "cu-latest")
    new = [b for b in H.backups_of(uid) if b["metadata"]["name"] not in before]
    catchups = [b for b in new if (b["spec"].get("trigger") or {}).get("kind") == "CatchUp"]
    H.require(
        len(catchups) == 1,
        f"expected exactly one CatchUp run, saw {[b['metadata']['name'] for b in catchups]}",
        obj=schedule,
    )
    # Same overwrite hazard as L-04-2: the slot's disposition moves on when the
    # run it started finishes, so the decision is read from the run it names.
    last_slot = (schedule.get("status") or {}).get("lastSlot") or {}
    H.require(
        last_slot.get("disposition") == "CaughtUp"
        or (
            last_slot.get("slot") == after["spec"]["slot"]
            and (last_slot.get("backupRef") or {}).get("name") == after["metadata"]["name"]
        ),
        f"lastSlot does not record the catch-up run: {last_slot}",
        obj=schedule,
    )
    return {
        "detail": {
            "injection": INJECTIONS["L-04-2b"],
            "effectiveSinceObserved": observed["status"]["policy"]["effectiveSince"],
            "effectiveSinceBackdatedTo": backdated,
            "lastEvaluatedSlotBackdatedTo": gap_start,
            "catchUpRun": H.excerpt(after, "spec.slot", "spec.trigger", "status.backupId"),
            "lastSlot": last_slot,
            "missedSlots": H.excerpt(schedule, "status.missedSlots.count"),
            "newRuns": [
                {"name": b["metadata"]["name"], "slot": b["spec"]["slot"],
                 "trigger": (b["spec"].get("trigger") or {}).get("kind")}
                for b in new
            ],
        },
        "dumps": H.dump_objects(
            "L-04-2b",
            {"cu-latest": ("backupschedule", "cu-latest"),
             "catchUp": ("backup", after["metadata"]["name"])},
        ),
    }


# ---------------------------------------------------------------------------
# PLAT-05.1
# ---------------------------------------------------------------------------


def _await_capture(H: Any, kind: str, *, paused: bool = True, timeout: int = 260,
                   name: str = "") -> dict[str, Any]:
    def look() -> dict[str, Any] | None:
        for item in reversed(fenced.captures(H, kind)):
            if paused and not item.get("paused"):
                continue
            if name and name not in {item.get("name"), item.get("schedule")}:
                continue
            return item
        return None

    return H.wait_until(look, timeout=timeout, interval=2.0,
                        what=f"the proxy to hold a {kind} request")


def l_05_1_2(H: Any) -> dict[str, Any]:
    """The two orderings of an edit racing a due slot, both held at the proxy."""
    detail: dict[str, Any] = {}

    # ---- ordering A: hold the reservation, edit, release -> 409, new generation
    reset_schedules(H, ["race-a", "race-b"])
    obj = H.apply(s3_schedule(H, "race-a", schedule="*/2 * * * *"))
    uid = obj["metadata"]["uid"]
    H.await_schedule_observed("race-a", timeout=180)
    before = {b["metadata"]["name"] for b in H.backups_of(uid)}
    fenced.arm(H, "reservation", "race-a", "pause")
    held = _await_capture(H, "reservation", name="race-a")
    edited = H.patch("backupschedule", "race-a", {"spec": {"topics": ["t1", "t2"]}})
    fenced.release(H)
    time.sleep(6)
    held_final = next(
        (c for c in fenced.captures(H, "reservation")
         if c.get("bodySha256") == held.get("bodySha256") and "response" in c),
        None,
    )
    H.require(
        held_final is not None and held_final.get("response") == 409,
        "the held reservation PATCH did not return 409; D1 §5.4 says an edit that lands "
        f"between the read and the write must make it conflict (saw {held_final})",
        dumps={"captures": fenced.captures(H, "reservation")[-8:]},
    )
    slot_a = ((held.get("body") or {}).get("status") or {}).get("pendingRun", {}).get("slot")
    created = H.wait_until(
        lambda: next(
            (b for b in H.backups_of(uid)
             if b["metadata"]["name"] not in before and b["spec"].get("slot") == slot_a),
            None,
        ),
        timeout=200,
        interval=2.0,
        what=f"the Backup for slot {slot_a} to be created after the conflict",
    )
    same_slot = [
        b for b in H.backups_of(uid)
        if b["spec"].get("slot") == slot_a
        and int((b["spec"].get("trigger") or {}).get("attempt", 0)) == 0
    ]
    H.require(
        len(same_slot) == 1,
        f"slot {slot_a} produced {len(same_slot)} attempt-0 Backups: "
        f"{[b['metadata']['name'] for b in same_slot]}",
        dumps={"runs": [b["metadata"]["name"] for b in same_slot]},
    )
    new_generation = edited["metadata"]["generation"]
    new_digest = (
        (H.get("backupschedule", "race-a").get("status") or {}).get("policy") or {}
    ).get("runPolicySha256")
    recorded = (created["spec"].get("scheduleRef") or {})
    H.require(
        recorded.get("generation") == new_generation,
        f"the Backup for slot {slot_a} records generation {recorded.get('generation')}, "
        f"not the NEW generation {new_generation} the conflict forced it to re-read",
        obj=created,
    )
    H.require(
        recorded.get("runPolicySha256") == new_digest,
        f"the Backup records digest {recorded.get('runPolicySha256')}, not the new "
        f"{new_digest}",
        obj=created,
    )
    H.require(
        created["spec"].get("topics") == ["t1", "t2"],
        f"the Backup froze topics {created['spec'].get('topics')}, not the edited ['t1','t2']",
        obj=created,
    )
    detail["orderingA"] = {
        "heldRequest": {k: held.get(k) for k in ("kind", "name", "method", "path", "paused")},
        "heldResponse": held_final.get("response"),
        "slot": slot_a,
        "generationBefore": obj["metadata"]["generation"],
        "generationAfterEdit": new_generation,
        "backup": H.excerpt(created, "spec.slot", "spec.topics", "spec.scheduleRef",
                            "spec.trigger.kind"),
        "attemptZeroForSlot": [b["metadata"]["name"] for b in same_slot],
    }

    # ---- ordering B: reservation lands, hold the Backup POST, edit, release
    obj_b = H.apply(s3_schedule(H, "race-b", schedule="*/2 * * * *"))
    uid_b = obj_b["metadata"]["uid"]
    H.await_schedule_observed("race-b", timeout=180)
    old_spec = H.get("backupschedule", "race-b")["spec"]
    before_b = {b["metadata"]["name"] for b in H.backups_of(uid_b)}
    fenced.arm(H, "backup_create", "race-b", "pause")
    held_b = _await_capture(H, "backup_create", name="race-b")
    old_generation = H.get("backupschedule", "race-b")["metadata"]["generation"]
    edited_b = H.patch("backupschedule", "race-b", {"spec": {"topics": ["t1", "t2"]}})
    fenced.release(H)
    created_b = H.wait_until(
        lambda: next(
            (b for b in H.backups_of(uid_b) if b["metadata"]["name"] not in before_b), None
        ),
        timeout=200,
        interval=2.0,
        what="the held Backup POST to land",
    )
    recorded_b = created_b["spec"].get("scheduleRef") or {}
    H.require(
        recorded_b.get("generation") == old_generation,
        f"the Backup created from the accepted reservation records generation "
        f"{recorded_b.get('generation')}, not the OLD {old_generation} its reservation carried",
        obj=created_b,
    )
    H.require(
        created_b["spec"].get("topics") == old_spec.get("topics"),
        f"the copied policy is {created_b['spec'].get('topics')}, not the old spec's "
        f"{old_spec.get('topics')}",
        obj=created_b,
    )
    for field in ("schedule", "concurrencyPolicy", "archive"):
        H.require(
            created_b["spec"].get(field, old_spec.get(field)) == old_spec.get(field)
            or field == "schedule",
            f"the copied policy's {field} moved with the edit",
            obj=created_b,
        )
    detail["orderingB"] = {
        "heldRequest": {k: held_b.get(k) for k in ("kind", "name", "method", "path", "paused")},
        "generationAtReservation": old_generation,
        "generationAfterEdit": edited_b["metadata"]["generation"],
        "backup": H.excerpt(created_b, "spec.slot", "spec.topics", "spec.scheduleRef"),
        "oldSpecSavedByHarness": {
            k: old_spec.get(k) for k in ("topics", "schedule", "archive", "concurrencyPolicy")
        },
    }
    return {
        "detail": detail,
        "dumps": H.dump_objects(
            "L-05.1-2",
            {
                "race-a": ("backupschedule", "race-a"),
                "race-b": ("backupschedule", "race-b"),
                "runA": ("backup", created["metadata"]["name"]),
                "runB": ("backup", created_b["metadata"]["name"]),
            },
        ),
    }


def l_05_1_3(H: Any) -> dict[str, Any]:
    """Conversion: objects written by main@4956785, then the current controller."""
    reset_schedules(H, ["legacy"])
    old = fenced.swap_image(H, "old")
    H.apply(s3_schedule(H, "legacy", schedule="*/2 * * * *"))
    # NOT `await_schedule_observed`. That waits for `status.policy.effectiveSince`,
    # and `status.policy` is a D1 field the main@4956785 build does not know:
    # waiting for it under the pre-upgrade image waits forever. What the old
    # controller does write is a `Ready` condition and `lastFireTime`, so that
    # is what "the old controller has seen this schedule" means here — and its
    # ABSENCE after the upgrade is one of the things the row then measures.
    observed = H.wait_for(
        "backupschedule",
        "legacy",
        lambda o: (H.condition(o, "Ready") or {}).get("status") == "True",
        timeout=300,
        what="to be observed by the main@4956785 controller (Ready=True)",
    )
    uid = observed["metadata"]["uid"]
    H.require(
        ((observed.get("status") or {}).get("policy") or {}).get("effectiveSince") is None,
        "the pre-upgrade controller wrote status.policy, which did not exist at 4956785 — "
        "is this really the old image?",
        obj=observed,
    )
    run = H.wait_until(
        lambda: next(
            (b for b in runs_of(H, "legacy", uid) if H.terminal(b)),
            None,
        ),
        timeout=600,
        interval=5.0,
        what="one completed run from the main@4956785 controller",
    )
    pre = {
        "schedule": H.get("backupschedule", "legacy"),
        "backup": H.get("backup", run["metadata"]["name"]),
    }
    H.artifact("objects/L-05.1-3/pre-upgrade-schedule.json", pre["schedule"])
    H.artifact("objects/L-05.1-3/pre-upgrade-backup.json", pre["backup"])
    generation_before = pre["schedule"]["metadata"]["generation"]
    backup_rv_before = pre["backup"]["metadata"]["resourceVersion"]
    backup_uid = pre["backup"]["metadata"]["uid"]
    spec_before = json.dumps(pre["backup"]["spec"], sort_keys=True)
    status_before = json.dumps(pre["backup"].get("status"), sort_keys=True)
    slots_before = attempt_zero_slots(runs_of(H, "legacy", uid))

    new = fenced.swap_image(H, "new")
    after_schedule = H.wait_for(
        "backupschedule",
        "legacy",
        lambda o: ((o.get("status") or {}).get("policy") or {}).get("effectiveSince")
        and (o.get("status") or {}).get("observedGeneration") is not None,
        timeout=300,
        what="to be observed by the upgraded controller",
    )
    H.require(
        after_schedule["metadata"]["generation"] == generation_before,
        f"metadata.generation moved on upgrade: {generation_before} -> "
        f"{after_schedule['metadata']['generation']} (a spec write the upgrade must not make)",
        obj=after_schedule,
    )
    H.require(
        after_schedule["status"].get("observedGeneration") == generation_before,
        f"status.observedGeneration is {after_schedule['status'].get('observedGeneration')}, "
        f"not metadata.generation {generation_before}",
        obj=after_schedule,
    )
    next_run = H.wait_until(
        lambda: next(
            (
                b
                for b in runs_of(H, "legacy", uid)
                if b["spec"].get("slot") not in set(slots_before)
                and int((b["spec"].get("trigger") or {}).get("attempt", 0)) == 0
            ),
            None,
        ),
        timeout=300,
        interval=3.0,
        what="the first slot after the upgrade to fire",
    )
    slot_instant = dt.datetime.strptime(next_run["spec"]["slot"], "%Y%m%d-%H%M%S").replace(
        tzinfo=dt.timezone.utc
    )
    H.require(
        slot_instant.second == 0 and slot_instant.minute % 2 == 0,
        f"the post-upgrade slot {next_run['spec']['slot']} is not a UTC */2 instant, so the "
        "upgraded controller is not computing the same slot names as the old one",
        obj=next_run,
    )
    H.require(
        next_run["metadata"]["name"] == H.run_name("legacy", next_run["spec"]["slot"]),
        f"the post-upgrade run is named {next_run['metadata']['name']}, not the deterministic "
        f"{H.run_name('legacy', next_run['spec']['slot'])}",
        obj=next_run,
    )
    after_backup = H.get("backup", run["metadata"]["name"])
    H.require(
        after_backup["metadata"]["uid"] == backup_uid,
        "the pre-upgrade Backup was replaced rather than migrated",
        obj=after_backup,
    )
    H.require(
        json.dumps(after_backup["spec"], sort_keys=True) == spec_before,
        "the pre-upgrade Backup's spec changed across the upgrade",
        obj=after_backup,
        dumps={"before": pre["backup"], "after": after_backup},
    )
    status_delta = sorted(
        k
        for k in set(json.loads(status_before) or {}) | set(after_backup.get("status") or {})
        if json.dumps((json.loads(status_before) or {}).get(k), sort_keys=True)
        != json.dumps((after_backup.get("status") or {}).get(k), sort_keys=True)
    )
    H.require(
        not status_delta,
        "the pre-upgrade Backup's status changed across the upgrade, outside D1 §6.2's "
        f"migration fields (metadata only): {status_delta}. Verification went "
        f"{((json.loads(status_before) or {}).get('evidence') or {}).get('verification', {}).get('result')!r}"
        f" -> {((after_backup.get('status') or {}).get('evidence') or {}).get('verification', {}).get('result')!r}",
        obj=after_backup,
        dumps={"before": pre["backup"], "after": after_backup},
    )
    changed = _metadata_delta(pre["backup"], after_backup)
    allowed = {
        "resourceVersion",
        "generation",
        "ownerReferences",
        "labels.logweir.dev/schedule-uid",
        "annotations.logweir.dev/history-retained-from-owner",
        "managedFields",
    }
    unexpected = sorted(set(changed) - allowed)
    H.require(
        not unexpected,
        f"the pre-upgrade Backup changed outside D1 §6.2's migration fields: {unexpected}",
        dumps={"before": pre["backup"], "after": after_backup, "changed": sorted(changed)},
    )
    return {
        "detail": {
            "oldController": {k: old[k] for k in old if k.endswith(("Image", "Revision", "ImageId"))},
            "newController": {k: new[k] for k in new if k.endswith(("Image", "Revision", "ImageId"))},
            "deviation": (
                "the CRDs were NOT downgraded: they are the current ones throughout, so this "
                "measures the CONTROLLER half of the upgrade only. Downgrading cluster-scoped "
                "CRDs would change what the shared release serves, which this fence exists to "
                "avoid."
            ),
            "generationBefore": generation_before,
            "generationAfter": after_schedule["metadata"]["generation"],
            "observedGeneration": after_schedule["status"].get("observedGeneration"),
            "preUpgradeRun": run["metadata"]["name"],
            "preUpgradeRunResourceVersion": backup_rv_before,
            "postUpgradeRun": H.excerpt(next_run, "spec.slot", "spec.trigger.kind",
                                        "spec.scheduleRef.generation"),
            "metadataFieldsChangedOnTheOldRun": sorted(changed),
            "historyRetained": H.excerpt(after_schedule, "status.history"),
        },
        "dumps": H.dump_objects(
            "L-05.1-3",
            {
                "legacy": ("backupschedule", "legacy"),
                "oldRunAfterUpgrade": ("backup", run["metadata"]["name"]),
                "firstRunAfterUpgrade": ("backup", next_run["metadata"]["name"]),
            },
        ),
    }


def _metadata_delta(before: dict[str, Any], after: dict[str, Any]) -> set[str]:
    """Which `metadata` fields moved, with labels and annotations named key by key.

    D1 §6.2 allows the migration to touch exactly four of them, so a delta that
    said only "labels changed" would not be able to tell the allowed write from
    a forbidden one.
    """
    changed: set[str] = set()
    for key in set(before["metadata"]) | set(after["metadata"]):
        old = before["metadata"].get(key)
        new = after["metadata"].get(key)
        if old == new:
            continue
        if key in {"labels", "annotations"}:
            old_map = old if isinstance(old, dict) else {}
            new_map = new if isinstance(new, dict) else {}
            for sub in set(old_map) | set(new_map):
                if old_map.get(sub) != new_map.get(sub):
                    changed.add(f"{key}.{sub}")
            continue
        changed.add(key)
    return changed


def l_05_1_5(H: Any) -> dict[str, Any]:
    """Rollback to main@4956785 with the new CRDs installed, then forward again."""
    reset_schedules(H, ["roll"])
    tz = H.apply(
        s3_schedule(
            H,
            "roll",
            schedule="*/2 * * * *",
            timeZone="Asia/Kathmandu",
            retry={"maxRetries": 1, "delaySeconds": 60},
        )
    )
    uid = tz["metadata"]["uid"]
    H.await_schedule_observed("roll", timeout=180)
    first = H.wait_until(
        lambda: next((b for b in H.backups_of(uid)), None),
        timeout=300,
        interval=3.0,
        what="one run before the rollback",
    )
    H.wait_for("backup", first["metadata"]["name"], H.terminal, timeout=600,
               what="to reach a terminal phase before the rollback")

    # The documented procedure: suspend the schedules that use the new fields.
    H.patch("backupschedule", "roll", {"spec": {"suspend": True}})
    suspended_others = suspend_all_but(H, {"roll"})
    # ONLY TERMINAL RUNS ARE FROZEN. A run still executing moves its own
    # resourceVersion as its Job progresses, and counting that as "the rollback
    # moved an object" would be an accusation about the wrong thing. D1's
    # clause is "EXISTING Backups and ConfigMaps keep their resourceVersion".
    H.wait_until(
        lambda: all(H.terminal(b) for b in H.lst("backups")),
        timeout=600,
        interval=5.0,
        what="every run in the namespace to be terminal before the rollback",
    )
    frozen: dict[str, str] = {}
    for backup in H.lst("backups"):
        frozen[f"backup/{backup['metadata']['name']}"] = backup["metadata"]["resourceVersion"]
    for cm in H.lst("configmaps"):
        if cm["metadata"]["name"].endswith("-plan"):
            frozen[f"configmap/{cm['metadata']['name']}"] = cm["metadata"]["resourceVersion"]
    before_names = {b["metadata"]["name"] for b in H.lst("backups")}

    old = fenced.swap_image(H, "old")
    time.sleep(180)  # >= one full */2 period under the rolled-back controller
    after_names = {b["metadata"]["name"] for b in H.lst("backups")}
    created_while_back = sorted(after_names - before_names)
    H.require(
        not created_while_back,
        f"the rolled-back controller created Backups for suspended schedules: "
        f"{created_while_back}",
        dumps={"created": created_while_back},
    )
    moved = {}
    for backup in H.lst("backups"):
        key = f"backup/{backup['metadata']['name']}"
        if key in frozen and frozen[key] != backup["metadata"]["resourceVersion"]:
            moved[key] = [frozen[key], backup["metadata"]["resourceVersion"]]
    for cm in H.lst("configmaps"):
        key = f"configmap/{cm['metadata']['name']}"
        if key in frozen and frozen[key] != cm["metadata"]["resourceVersion"]:
            moved[key] = [frozen[key], cm["metadata"]["resourceVersion"]]
    H.require(
        not moved,
        f"the rollback moved the resourceVersion of existing objects: {moved}",
        dumps={"moved": moved},
    )

    new = fenced.swap_image(H, "new")
    H.patch("backupschedule", "roll", {"spec": {"suspend": False}})
    resumed = H.wait_until(
        lambda: next(
            (
                b
                for b in H.backups_of(uid)
                if b["metadata"]["name"] not in before_names
                and int((b["spec"].get("trigger") or {}).get("attempt", 0)) == 0
            ),
            None,
        ),
        timeout=320,
        interval=3.0,
        what="the resumed schedule to fire after rolling forward",
    )
    schedule_after = H.get("backupschedule", "roll")
    next_runs = (schedule_after.get("status") or {}).get("nextRuns") or []
    H.require(
        bool(next_runs) and str(next_runs[0].get("localTime", "")).endswith("+05:45"),
        f"the resumed schedule does not preview in its time zone: {next_runs[:1]}",
        obj=schedule_after,
    )
    H.require(
        schedule_after["spec"].get("retry", {}).get("maxRetries") == 1,
        "the retry policy did not survive the rollback and roll-forward",
        obj=schedule_after,
    )
    owned = [
        b
        for b in H.lst("backups")
        if schedule_owner(b) and (b.get("status") or {}).get("phase") in {None, "Pending",
                                                                         "Running", "Resolving"}
    ]
    migrated = [
        b
        for b in H.lst("backups")
        if (b["metadata"].get("labels") or {}).get("logweir.dev/schedule-uid") == uid
        and not schedule_owner(b)
    ]
    return {
        "detail": {
            "oldController": {k: old[k] for k in old if k.endswith(("Image", "Revision"))},
            "newController": {k: new[k] for k in new if k.endswith(("Image", "Revision"))},
            "suspendedForRollback": ["roll"] + suspended_others,
            "objectsFrozen": len(frozen),
            "createdWhileRolledBack": created_while_back,
            "resourceVersionsMoved": moved,
            "resumedRun": H.excerpt(resumed, "spec.slot", "spec.trigger.kind",
                                    "spec.scheduleRef.generation"),
            "nextRunsAfterResume": next_runs[:2],
            "retryAfterResume": schedule_after["spec"].get("retry"),
            "stillOwnedNonterminal": [b["metadata"]["name"] for b in owned],
            "migratedTerminalRuns": [b["metadata"]["name"] for b in migrated],
        },
        "dumps": H.dump_objects(
            "L-05.1-5",
            {"roll": ("backupschedule", "roll"),
             "resumed": ("backup", resumed["metadata"]["name"])},
        ),
    }


# ---------------------------------------------------------------------------
# PLAT-05.2
# ---------------------------------------------------------------------------


def l_05_2_3(H: Any) -> dict[str, Any]:
    """Migration interrupted after seven PATCHes, resumed, and one PATCH 409ed."""
    reset_schedules(H, ["many"])
    schedule = H.apply(s3_schedule(H, "many", schedule="*/2 * * * *", suspend=True))
    fenced.scale(H, 0)
    seeded = seed_legacy_runs(
        H,
        schedule,
        20,
        first_slot=dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
        - dt.timedelta(hours=3),
        legacy_owner=True,
    )
    H.require(seeded["terminal"] == 20, f"seeding produced {seeded['terminal']}/20 terminal runs")
    uids_before = {
        b["metadata"]["name"]: b["metadata"]["uid"]
        for b in H.lst("backups")
        if b["metadata"]["name"] in set(seeded["names"])
    }
    fenced.set_initial_arm(H, "kind=migration_patch&mode=pause&after=7")
    fenced.scale(H, 1)
    H.wait_until(
        lambda: len([c for c in fenced.captures(H, "migration_patch") if c.get("paused")]) >= 1,
        timeout=300,
        interval=2.0,
        what="the proxy to forward seven migration PATCHes and hold the eighth",
    )
    first_pass = fenced.captures(H, "migration_patch")
    forwarded = [c for c in first_pass if not c.get("paused")]
    H.artifact("objects/L-05.2-3/first-pass-captures.json", first_pass)
    fenced.scale(H, 0)
    interrupted = [
        b["metadata"]["name"]
        for b in H.lst("backups")
        if b["metadata"]["name"] in uids_before and not schedule_owner(b)
    ]
    H.require(
        0 < len(interrupted) < 20,
        f"the interruption did not land mid-migration: {len(interrupted)}/20 were migrated "
        "when the controller was stopped",
        dumps={"migratedAtInterruption": interrupted},
    )

    fenced.set_initial_arm(H, "kind=migration_patch&mode=status&code=409")
    fenced.scale(H, 1)
    conflicted = H.wait_until(
        lambda: next(
            (c for c in fenced.captures(H, "migration_patch") if c.get("injected") == "status 409"),
            None,
        ),
        timeout=300,
        interval=2.0,
        what="the first migration PATCH after the restart to be answered 409",
    )
    migrated = H.wait_until(
        lambda: (
            [
                b["metadata"]["name"]
                for b in H.lst("backups")
                if b["metadata"]["name"] in uids_before and not schedule_owner(b)
            ]
            if len(
                [
                    b
                    for b in H.lst("backups")
                    if b["metadata"]["name"] in uids_before and not schedule_owner(b)
                ]
            )
            == 20
            else None
        ),
        timeout=600,
        interval=5.0,
        what="all 20 runs to end migrated, including the one whose PATCH was 409ed",
    )
    after = {b["metadata"]["name"]: b for b in H.lst("backups") if b["metadata"]["name"] in uids_before}
    moved = {
        name: [uids_before[name], obj["metadata"]["uid"]]
        for name, obj in after.items()
        if obj["metadata"]["uid"] != uids_before[name]
    }
    H.require(not moved, f"Backup UIDs changed across the migration: {moved}")
    missing_label = [
        name
        for name, obj in after.items()
        if (obj["metadata"].get("labels") or {}).get("logweir.dev/schedule-uid")
        != schedule["metadata"]["uid"]
    ]
    H.require(
        not missing_label,
        f"migrated runs without the schedule-uid label: {missing_label}",
        dumps={"sample": after[missing_label[0]] if missing_label else None},
    )
    # The proxy restarts with its pod, so the resumed pass's captures are a
    # SECOND record. Both are needed: "no Backup was patched after it was
    # already migrated" is a statement about the whole migration, not about the
    # half that happened to survive the interruption.
    captures = first_pass + fenced.captures(H, "migration_patch")
    H.artifact("objects/L-05.2-3/all-migration-captures.json", captures)
    successes: dict[str, list[int]] = {}
    for item in captures:
        if item.get("response") in {200, 201}:
            successes.setdefault(str(item.get("name")), []).append(int(item["response"]))
    repatched = {k: v for k, v in successes.items() if len(v) > 1}
    H.require(
        not repatched,
        f"a Backup was patched again after it was already migrated: {repatched}",
        dumps={"captures": captures[-40:]},
    )
    conflicted_name = str(conflicted.get("name"))
    H.require(
        conflicted_name in after and not schedule_owner(after[conflicted_name]),
        f"the run whose PATCH was answered 409 ({conflicted_name}) was not migrated on a later "
        "inventory",
        obj=after.get(conflicted_name),
    )
    retained = H.wait_for(
        "backupschedule",
        "many",
        lambda o: (H.condition(o, "HistoryRetained") or {}).get("status") == "True",
        timeout=400,
        what="HistoryRetained=True once every legacy owner is gone",
    )
    # Clear the pod-level arming LAST: `kubectl set env` rolls the Deployment,
    # and a restart in the middle of the measurement would be a second, undeclared
    # interruption.
    fenced.set_initial_arm(H, "")
    return {
        "detail": {
            "injection": INJECTIONS["L-05.2-3"],
            "seeded": {k: seeded[k] for k in ("requested", "terminal", "createSeconds",
                                              "totalSeconds")},
            "forwardedBeforeInterruption": len(forwarded),
            "migratedAtInterruption": len(interrupted),
            "conflictedPatch": {
                k: conflicted.get(k) for k in ("name", "method", "path", "injected", "at")
            },
            "allMigrated": len(migrated),
            "uidsUnchanged": True,
            "successfulPatchesPerRun": {k: len(v) for k, v in sorted(successes.items())},
            "historyRetained": H.condition(retained, "HistoryRetained"),
            "history": H.excerpt(retained, "status.history"),
        },
        "dumps": H.dump_objects("L-05.2-3", {"many": ("backupschedule", "many")}),
    }


def l_05_2_1rv(H: Any) -> dict[str, Any]:
    """L-05.2-1's last clause: every migration PATCH body carries a resourceVersion.

    The W8 run could not claim it — "only the proxy can see that". This is that
    reading, taken from the proxy that carried L-05.2-3's twenty migrations.
    """
    # Both passes, as L-05.2-3 recorded them: the interrupted one lives only in
    # that artifact, because its proxy died with its pod.
    path = H.OUT / "objects/L-05.2-3/all-migration-captures.json"
    H.require(
        path.exists(),
        "L-05.2-3 has not run in this namespace, so there are no migration PATCHes to read",
    )
    captures = json.loads(path.read_text())
    H.require(
        len(captures) >= 20,
        f"only {len(captures)} migration PATCHes were captured; run L-05.2-3 first",
    )
    bodies = [c for c in captures if "body" in c]
    H.require(
        len(bodies) == len(captures),
        f"{len(captures) - len(bodies)} migration PATCH bodies were not captured, so the "
        "clause cannot be checked over all of them",
    )
    without = [
        {"name": c.get("name"), "metadata": sorted((c["body"].get("metadata") or {}))}
        for c in bodies
        if not (c["body"].get("metadata") or {}).get("resourceVersion")
    ]
    H.require(
        not without,
        f"{len(without)} migration PATCH bodies carried no metadata.resourceVersion "
        f"precondition: {without[:5]}",
        dumps={"captures": captures[-40:]},
    )
    owners_rewritten = [
        c for c in bodies if "ownerReferences" in (c["body"].get("metadata") or {})
    ]
    H.artifact("objects/L-05.2-1rv/migration-patch-bodies.json", bodies)
    return {
        "detail": {
            "patchesObserved": len(captures),
            "withResourceVersion": len(bodies) - len(without),
            "withoutResourceVersion": len(without),
            "rewritingOwnerReferences": len(owners_rewritten),
            "sample": {
                "name": bodies[0].get("name"),
                "metadataKeys": sorted(bodies[0]["body"].get("metadata") or {}),
                "resourceVersionPresent": bool(
                    (bodies[0]["body"].get("metadata") or {}).get("resourceVersion")
                ),
            },
        }
    }


def l_05_2_2u(H: Any) -> dict[str, Any]:
    """The unfrozen sub-case: the plan ConfigMap POST is 503ed until the delete."""
    reset_schedules(H, ["unfroz"])
    suspend_all_but(H, {"unfroz"})
    schedule = H.apply(s3_schedule(H, "unfroz", schedule="*/2 * * * *"))
    uid = schedule["metadata"]["uid"]
    H.await_schedule_observed("unfroz", timeout=180)
    before = {b["metadata"]["name"] for b in H.backups_of(uid)}
    fenced.arm(H, "configmap_create", "", "status", code=503, repeat=True)
    # THE RUN IS THE ONE WHOSE POST WAS ACTUALLY REFUSED, not the first new one.
    # A slot can fire between the schedule being observed and the arming taking
    # effect, and on the first attempt at this row that earlier run reached
    # `Succeeded` normally while the assertions were pointed at it — a red for a
    # run the injection never touched. The blocked capture names the plan
    # ConfigMap, and a plan ConfigMap is `<backup name>-plan`.
    blocked = H.wait_until(
        lambda: next(
            (
                c
                for c in fenced.captures(H, "configmap_create")
                if c.get("injected") == "status 503"
                and str(c.get("name", "")).startswith("logweir-backup-unfroz-")
            ),
            None,
        ),
        timeout=300,
        interval=2.0,
        what="a plan ConfigMap POST for this schedule to be answered 503",
    )
    run_name = str(blocked["name"]).removesuffix("-plan")
    H.require(
        run_name not in before,
        f"the blocked plan ConfigMap belongs to {run_name}, which existed before the arming",
    )
    run = H.get("backup", run_name)
    H.kn("delete", "backupschedule", "unfroz", "--wait=true", timeout=120)
    fenced.release(H)
    fenced.disarm(H)
    fenced.set_initial_arm(H, "")
    final = H.wait_for(
        "backup",
        run["metadata"]["name"],
        H.terminal,
        timeout=420,
        what="to reach a terminal phase once its schedule is gone",
    )
    reason = (
        (H.condition(final, "Failed") or {}).get("reason")
        or (final.get("status") or {}).get("reason")
        or ""
    )
    H.require(
        (final.get("status") or {}).get("phase") == "Failed",
        f"the unfrozen run ended {(final.get('status') or {}).get('phase')!r}, not Failed",
        obj=final,
    )
    H.require(
        reason == "ScheduleNotFound",
        f"the unfrozen run failed with reason {reason!r}, not ScheduleNotFound",
        obj=final,
        dumps={"run": final},
    )
    plans = [
        cm
        for cm in H.lst("configmaps")
        if cm["metadata"]["name"].startswith(run["metadata"]["name"])
    ]
    jobs = H.runner_jobs(final)
    H.require(not plans, f"a plan ConfigMap exists for the unfrozen run: "
                         f"{[c['metadata']['name'] for c in plans]}", obj=final)
    H.require(not jobs, f"a Job exists for the unfrozen run: "
                        f"{[j['metadata']['name'] for j in jobs]}", obj=final)
    return {
        "detail": {
            "injection": INJECTIONS["L-05.2-3/L-05.2-2u"],
            "run": run["metadata"]["name"],
            "runsThatPredateTheArming": sorted(before),
            "blockedRequest": {
                k: blocked.get(k) for k in ("kind", "name", "method", "path", "injected")
            },
            "blocked503Count": len(
                [c for c in fenced.captures(H, "configmap_create")
                 if c.get("injected") == "status 503"]
            ),
            "phase": (final.get("status") or {}).get("phase"),
            "reason": reason,
            "failedCondition": H.condition(final, "Failed"),
            "planConfigMaps": [c["metadata"]["name"] for c in plans],
            "jobs": [j["metadata"]["name"] for j in jobs],
        },
        "dumps": H.dump_objects("L-05.2-2u", {"run": ("backup", run["metadata"]["name"])}),
    }


def l_05_2_6(H: Any) -> dict[str, Any]:
    """Read cost with 500 retained runs, counted at the only place that sees it."""
    window = int(os.environ.get("LOGWEIR_D1_READCOST_SECONDS", "600"))
    reset_schedules(H, ["cost"])
    schedule = H.apply(s3_schedule(H, "cost", schedule="*/2 * * * *", suspend=True))
    fenced.scale(H, 0)
    seeded = seed_legacy_runs(
        H,
        schedule,
        500,
        first_slot=dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
        - dt.timedelta(days=1),
        legacy_owner=False,
    )
    H.require(
        seeded["terminal"] >= 495,
        f"seeding produced {seeded['terminal']}/500 terminal runs; the read-cost figure would "
        "be about a smaller history than D1 asks for",
    )
    fenced.scale(H, 1)
    suspend_all_but(H, set())
    H.patch("backupschedule", "cost", {"spec": {"suspend": False}})
    settled = H.wait_for(
        "backupschedule",
        "cost",
        lambda o: (((o.get("status") or {}).get("history") or {}).get("runCount") or 0) >= 495,
        timeout=600,
        what="the first inventory to count the seeded history",
    )
    # Steady state starts AFTER the first inventory: the startup inventory is
    # the documented one and counting it would measure the upgrade, not the
    # steady state D1 §6.7 is about.
    time.sleep(30)
    fenced.proxy_reset(H)
    H.log(f"    read-cost window: {window}s of steady state with {seeded['terminal']} runs")
    time.sleep(window)
    requests = fenced.proxy_requests(H, since=0.0)
    H.require(requests is not None, "the proxy did not answer /__requests")
    H.require(not requests["truncated"], "the proxy's request log filled up; the window is unsafe")
    rows = requests["requests"]
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row["shape"])] = counts.get(str(row["shape"]), 0) + 1
    lists = [r for r in rows if r["shape"] == "LIST backups"]
    gets = [r for r in rows if r["shape"] == "GET backups"]
    # One reconcile per schedule per requeue; the scheduler requeues every 30 s
    # (`backup_schedule.rs`), and `cost` is the only unsuspended schedule.
    reconciles = max(1, math.ceil(window / 30))
    per_reconcile = len(gets) / reconciles
    H.require(
        not lists,
        f"{len(lists)} LIST requests on backups in {window}s of steady state; D1 §6.7 allows "
        f"only inventories at the documented {3600}s interval, and none is due inside this "
        f"window: {[r['at'] for r in lists][:8]}",
        dumps={"counts": counts, "lists": lists[:20]},
    )
    H.require(
        per_reconcile <= 15,
        f"{len(gets)} GETs on backups over ~{reconciles} reconciles is {per_reconcile:.1f} per "
        "reconcile, above D1 §6.7's 15",
        dumps={"counts": counts},
    )
    H.artifact("objects/L-05.2-6/request-shapes.json", counts)
    return {
        "detail": {
            "injection": INJECTIONS["L-05.2-6"],
            "seeded": {k: seeded[k] for k in ("requested", "terminal", "createSeconds",
                                              "totalSeconds")},
            "historyRunCount": H.excerpt(settled, "status.history"),
            "windowSeconds": window,
            "requestsInWindow": len(rows),
            "shapes": dict(sorted(counts.items())),
            "listBackups": len(lists),
            "getBackups": len(gets),
            "reconcilesEstimated": reconciles,
            "getsPerReconcile": round(per_reconcile, 2),
            "reconcileEstimateMethod": (
                "ceil(window / 30 s), the scheduler's requeue period, with `cost` the only "
                "unsuspended schedule in the namespace"
            ),
        },
        "dumps": H.dump_objects("L-05.2-6", {"cost": ("backupschedule", "cost")}),
    }


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

ROWS = [
    ("L-04-2", "PLAT-04.2", "long downtime with catch-up None and Latest", l_04_2),
    ("L-04-2b", "PLAT-04.2", "the catch-up decision with a backdated effectiveSince", l_04_2b),
    ("L-04-5", "PLAT-04.2", "duplicate reconciliation across two fenced replicas", l_04_5),
    ("L-05.1-2", "PLAT-05.1", "concurrent edit and fire, both orderings", l_05_1_2),
    ("L-05.1-3", "PLAT-05.1", "conversion of objects written by main@4956785", l_05_1_3),
    ("L-05.1-5", "PLAT-05.1", "rollback to main@4956785 and forward again", l_05_1_5),
    ("L-05.2-3", "PLAT-05.2", "migration interrupted, resumed, and one PATCH 409ed", l_05_2_3),
    ("L-05.2-1rv", "PLAT-05.2", "every migration PATCH carries metadata.resourceVersion",
     l_05_2_1rv),
    ("L-05.2-2u", "PLAT-05.2", "deletion while a run is unfrozen", l_05_2_2u),
    ("L-05.2-6", "PLAT-05.2", "read cost over ten minutes with 500 retained runs", l_05_2_6),
]


def heal(H: Any) -> dict[str, Any]:
    """Put the fence back the way a row expects to find it.

    These rows stop the controller, run two of it, swap its image and arm its
    proxy. A row that raises in the middle of any of that leaves the next one
    measuring a namespace with no controller in it — and a red for a product
    that was never asked anything is the worst result this harness can produce.
    Every row therefore starts from one replica, on the current image, with
    nothing armed.
    """
    state: dict[str, Any] = {"at": H.now()}
    # Re-apply the Role: a run that picks up a changed grant must not depend on
    # having been set up after the change.
    H.apply(fenced.namespaced_role(H.NS, dict(H.LABELS)))
    deployment = H.get_opt("deployment", fenced.CONTROLLER)
    if deployment is None:
        raise H.Failure("the fenced controller Deployment is gone; run `setup` again")
    container = next(
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "weirkeeper"
    )
    wanted = fenced.IMAGES["new"][0]
    if container["image"] != wanted:
        state["restoredImage"] = fenced.swap_image(H, "new")
    if any(
        e["name"] == "PLAT04_ARM"
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "scope-proxy"
        for e in c.get("env", [])
    ):
        fenced.set_initial_arm(H, "")
        state["clearedInitialArm"] = True
    if int(deployment["spec"].get("replicas", 0)) != 1 or len(fenced.pods(H)) != 1:
        state["rescaled"] = fenced.scale(H, 1)
    fenced.disarm(H)
    return state


def register(H: Any) -> None:
    """Bind every row to the harness module so it can use its plumbing."""

    def bind(fn: Any) -> Any:
        def wrapped() -> dict[str, Any]:
            healed = heal(H)
            detail = fn(H)
            if healed.keys() - {"at"}:
                detail.setdefault("detail", {})["fenceHealedFirst"] = healed
            return detail

        return wrapped

    for sid, task, title, fn in ROWS:
        H.scenario(sid, task, title)(bind(fn))
