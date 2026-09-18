#!/usr/bin/env python3
"""Unit rows for the fence's source matching and its drift check.

    python3 scripts/live/d1/fence/test_drift.py

No cluster and no daemon: `docker inspect` is answered from a table and git is
the real repository, read-only. The drift check is exercised against this
checkout AND against a deliberately dirtied one, and a tracked modification
must still trip it — an ignore that ignored everything would be no check.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fence import fenced  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[4]
FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


class _Failure(Exception):
    def __init__(self, message: str, obj: object = None, dumps: object = None) -> None:
        super().__init__(message)


class _Harness:
    Failure = _Failure
    ROOT = ROOT

    def __init__(self, labels: dict[str, str] | None = None) -> None:
        self.labels = labels or {}

    def run(self, args, timeout=120, check=True, record=True, data=None):
        if args[0] == "docker":
            ref = args[args.index("inspect") + 1]
            label = self.labels.get(ref, fenced.NO_LABEL)
            return subprocess.CompletedProcess(args, 0, f"sha256:feed{ref} {label}\n", "")
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                              cwd=str(ROOT), check=False)


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(ROOT), capture_output=True, text=True,
                          check=True).stdout.strip()


def _contains(revision: str) -> tuple[bool, str, dict]:
    try:
        return True, "", fenced.assert_checkout_contains(_Harness(), revision)
    except _Failure as exc:
        return False, str(exc), {}


def test_an_untracked_file_does_not_trip_the_drift_check() -> None:
    head = _git("rev-parse", "HEAD")
    stray = ROOT / "hf-drift-probe-untracked.txt"
    stray.write_text("an untracked file at the repository root, like the orchestrator's "
                     "own `prompt`\n")
    try:
        ok, why, out = _contains(head)
        row("an untracked root-level file is ignored, not refused", ok, why)
        row("and it is RECORDED as ignored rather than silently dropped",
            ok and stray.name in out.get("untrackedIgnored", []),
            str(out.get("untrackedIgnored")))
    finally:
        stray.unlink(missing_ok=True)


def test_a_tracked_modification_still_trips_it() -> None:
    head = _git("rev-parse", "HEAD")
    victim = ROOT / "logweir.yaml"
    if not victim.is_file():
        row("MUTANT: a tracked product file modified in the working tree is refused",
            False, "logweir.yaml is not present; the probe needs a tracked product file")
        return
    original = victim.read_bytes()
    victim.write_bytes(original + b"\n# hf drift probe - reverted immediately\n")
    try:
        ok, why, _ = _contains(head)
        row("MUTANT: a tracked product file modified in the working tree is refused",
            not ok and "logweir.yaml" in why, why[:160])
    finally:
        victim.write_bytes(original)
    ok, why, _ = _contains(head)
    row("the probe left the checkout clean again", ok, why)


def test_a_tracked_harness_file_is_still_allowed() -> None:
    head = _git("rev-parse", "HEAD")
    victim = pathlib.Path(__file__)
    original = victim.read_bytes()
    victim.write_bytes(original + b"\n# hf drift probe - reverted immediately\n")
    try:
        ok, why, _ = _contains(head)
        row("a tracked file inside the non-image allowlist is allowed", ok, why[:160])
    finally:
        victim.write_bytes(original)


def test_the_other_refusals_are_untouched() -> None:
    ok, why, _ = _contains("0" * 40)
    row("MUTANT: a commit this checkout does not have is refused",
        not ok and "does not have" in why, why[:120])
    head = _git("rev-parse", "HEAD")
    harness = _Harness({"weirkeeper:scram-reviewed": head, "logweir:scram-local": head})
    fenced.REVISION_OVERRIDE.clear()
    try:
        out = fenced.assert_source_matched(harness, "new")
        row("a matching lab needs no flag: the drift check is ENFORCED and passes",
            out["driftCheck"] == "enforced" and out["revisionSource"] == "label", str(out))
    except _Failure as exc:
        row("a matching lab needs no flag: the drift check is ENFORCED and passes",
            False, str(exc)[:200])
    row("an unlabelled image is still refused",
        not _contains_label_match({}), "")


def _contains_label_match(labels: dict[str, str]) -> bool:
    fenced.REVISION_OVERRIDE.clear()
    try:
        fenced.assert_source_matched(_Harness(labels), "new")
        return True
    except _Failure:
        return False


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
