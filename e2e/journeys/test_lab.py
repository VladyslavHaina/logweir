"""Offline tests for `lab.run_killing_group`: a phase that times out, is
interrupted or exits leaves no process of its group behind
(plat20-1.review.md M-1 — a lock waiter under `governed.py swap-on` survived
run.py's phase timeout and later changed the shared controller).

Each test drives a toy child that starts a grandchild the way
`approval_policy_swap.py` starts `k8s-lock.sh acquire`, and the negative
control shows the same toy under plain `subprocess.run(timeout=)` leaves the
grandchild alive — the detector can see a survivor.

    /tmp/logweir-roadmap-run/venv/bin/python3 -m pytest e2e/journeys -q
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lab  # noqa: E402

# The child writes its grandchild's pid, then (unless told to exit) sleeps.
TOY = """
import subprocess, sys, time
g = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
open(sys.argv[1], "w").write(str(g.pid))
print("grandchild started", flush=True)
if sys.argv[2] == "hang":
    time.sleep(60)
"""


def alive(pid: int) -> bool:
    """True while `pid` is a live (non-zombie) process."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True,
                           timeout=10).stdout.strip()
    return bool(state) and not state.startswith("Z")


def gone_within(pid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        time.sleep(0.1)
    return False


def grandchild(pidfile: pathlib.Path) -> int:
    for _ in range(50):
        if pidfile.is_file() and pidfile.read_text().strip():
            return int(pidfile.read_text())
        time.sleep(0.1)
    raise AssertionError("the toy never started its grandchild")


def test_a_timeout_kills_the_whole_group_not_only_the_child(tmp_path):
    pidfile = tmp_path / "g.pid"
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        lab.run_killing_group([sys.executable, "-c", TOY, str(pidfile), "hang"], timeout=2)
    assert time.monotonic() - started < 15
    assert gone_within(grandchild(pidfile)), "a grandchild outlived its phase's timeout"


def test_a_child_that_exits_takes_its_leftover_grandchild_with_it(tmp_path):
    pidfile = tmp_path / "g.pid"
    done = lab.run_killing_group([sys.executable, "-c", TOY, str(pidfile), "exit"], timeout=30)
    assert done.returncode == 0 and "grandchild started" in done.stdout
    assert gone_within(grandchild(pidfile)), "a grandchild outlived its phase"


def test_negative_control_plain_subprocess_run_leaves_the_grandchild_alive(tmp_path):
    """What the review reproduced: `subprocess.run(timeout=)` kills only the
    direct child. If this ever stops holding, the two tests above prove
    nothing about the fix."""
    pidfile = tmp_path / "g.pid"
    with pytest.raises(subprocess.TimeoutExpired):
        subprocess.run([sys.executable, "-c", TOY, str(pidfile), "hang"], timeout=2)
    pid = grandchild(pidfile)
    try:
        time.sleep(0.5)
        assert alive(pid), "the control could not observe a surviving grandchild"
    finally:
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
            pass


def test_lab_run_goes_through_the_group_killer(tmp_path):
    """`Lab.run` — which run.py uses for every phase — reports the timeout as
    before (RuntimeError, read as rc 124) and leaves nothing behind."""
    pidfile = tmp_path / "g.pid"
    handle = lab.Lab(tmp_path, "owner", lab.Needles(), tmp_path)
    with pytest.raises(RuntimeError, match="timed out after 2s"):
        handle.run([sys.executable, "-c", TOY, str(pidfile), "hang"], timeout=2)
    assert handle.commands[-1]["rc"] == "timeout"
    assert gone_within(grandchild(pidfile))
