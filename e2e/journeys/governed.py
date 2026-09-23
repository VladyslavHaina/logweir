#!/usr/bin/env python3
"""The `two-approvals` journey's suite: `scripts/plat19-2-ui-e2e.mjs` run
against the shared lab controller with THAT run's approval-policy document
mounted, the chart's way, under the cluster lock.

Three phases, one process each (`run.py` drives them like any python suite):

    swap-on   write the run's policy document (the harness's own
              `UI_E2E_POLICY_ONLY=1` mode, same stamp), take the cluster lock
              unless this owner already holds it, and mount the document into
              the shared controller (`scripts/live/approval_policy_swap.py on`);
    run       the plat19-2 harness itself, which refuses to start unless the
              controller binds its namespaces;
    swap-off  (run.py's `finally`) take the document out again, fail unless the
              controller is byte-identical to the recorded baseline, and give
              the lock back only if this suite took it and the restore held.

This is the ONE suite that changes the shared release, which is why its
journey stays behind `requires: PLAT-19.2`: a plain `run.py run` never takes
the lock. `test_catalogue.py` pins both facts.

DEADLINES. `run.py` gives each phase `suites.SUITES["plat19-2"].timeout`
seconds and then kills the phase's whole process group. `swap-on` must end on
its own before that, so it waits at most `LOCK_WAIT_MINUTES` for the lock and
its worst case (`SWAP_ON_BUDGET`) stays below the phase timeout
(`test_catalogue.py` pins the inequality): a swap-on still queued for the
lock when its journey has been judged would later mount this run's policy on
the shared controller and exit holding the lock. The process-group kill is
the second fence: nothing this script starts leaves its group.

Environment (from `suites.py`): GOV_STAMP, GOV_OUT, GOV_OWNER, GOV_PREFIX,
UI_E2E_API_BIN, UI_E2E_LOGWEIR_BIN, NODE_PATH, LOGWEIR_PYTHON, LOGWEIR_K8S_LOCK.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
HARNESS = ROOT / "scripts" / "plat19-2-ui-e2e.mjs"
SWAP = ROOT / "scripts" / "live" / "approval_policy_swap.py"
LOCK = os.environ.get("LOGWEIR_K8S_LOCK", "/tmp/logweir-roadmap-run/claude/k8s-lock.sh")

# The deadlines (see the module docstring). Children are started WITHOUT a new
# session, so run.py's process-group kill reaches every one of them.
POLICY_ONLY_SECONDS = 120        # the harness writing the run's policy document
LOCK_WAIT_MINUTES = 15           # `k8s-lock.sh acquire` gives up after this
SWAP_WORK_SECONDS = 600          # the swap itself: patch, rollout, bound-line read
SWAP_SECONDS = LOCK_WAIT_MINUTES * 60 + SWAP_WORK_SECONDS
SWAP_ON_BUDGET = POLICY_ONLY_SECONDS + 30 + SWAP_SECONDS  # + the `status` probe
RUN_SECONDS = 1500


def env() -> dict[str, str]:
    e = dict(os.environ)
    e.update({"UI_E2E_STAMP": e["GOV_STAMP"], "UI_E2E_OWNER": e["GOV_OWNER"], "UI_E2E_PREFIX": e["GOV_PREFIX"],
              "UI_E2E_ARTIFACTS": e["GOV_OUT"]})
    return e


def out() -> pathlib.Path:
    return pathlib.Path(os.environ["GOV_OUT"])


def lock_holder() -> str:
    done = subprocess.run([LOCK, "status"], capture_output=True, text=True, timeout=30)
    words = done.stdout.split()
    return words[1] if words[:1] == ["held:"] and len(words) > 1 else ""


def swap(verb: str, *extra: str) -> int:
    python = os.environ.get("LOGWEIR_PYTHON") or sys.executable
    argv = [python, str(SWAP), verb, "--owner", os.environ["GOV_OWNER"], "--record", str(out() / "swap"), *extra]
    return subprocess.run(argv, timeout=SWAP_SECONDS).returncode


def swap_on() -> int:
    e = env()
    e["UI_E2E_POLICY_ONLY"] = "1"
    done = subprocess.run(["node", str(HARNESS)], env=e, timeout=POLICY_ONLY_SECONDS)
    policy = out() / e["GOV_STAMP"] / "approval-policy.yaml"
    if done.returncode != 0 or not policy.is_file():
        print(f"the harness did not write {policy}", file=sys.stderr)
        return 1
    (out() / "swap").mkdir(parents=True, exist_ok=True)
    extra = ["--policy", str(policy)]
    if lock_holder() != os.environ["GOV_OWNER"]:
        (out() / "swap" / "lock-taken-by-this-suite").write_text(os.environ["GOV_OWNER"] + "\n")
        extra += ["--acquire", "--wait-minutes", str(LOCK_WAIT_MINUTES)]
    return swap("on", *extra)


def run() -> int:
    return subprocess.run(["node", str(HARNESS)], env=env(), timeout=RUN_SECONDS).returncode


def swap_off() -> int:
    taken = (out() / "swap" / "lock-taken-by-this-suite").is_file()
    if not (out() / "swap" / "deploy-baseline.json").is_file():
        # swap-on never changed the controller; give back a lock it took.
        if taken and lock_holder() == os.environ["GOV_OWNER"]:
            return subprocess.run([LOCK, "release", os.environ["GOV_OWNER"]], timeout=60).returncode
        return 0
    extra = ["--release"] if taken else []
    return swap("off", *extra)


PHASES = {"swap-on": swap_on, "run": run, "swap-off": swap_off}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in PHASES:
        raise SystemExit(f"usage: governed.py {{{'|'.join(PHASES)}}}")
    raise SystemExit(PHASES[sys.argv[1]]())
