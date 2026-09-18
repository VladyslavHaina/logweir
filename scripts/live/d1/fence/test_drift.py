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


def test_the_boundary_is_what_the_image_build_consumes() -> None:
    """Pure, and one row per side of the line the review drew (**F-4**)."""
    row("the orchestrator's own scratch file at the root is ignorable",
        fenced.untracked_is_ignorable("prompt")
        and fenced.untracked_is_ignorable("notes.txt"))
    row("MUTANT: an untracked Rust source is compiled into the image",
        not fenced.untracked_is_ignorable("crates/weirkeeper/src/zz.rs"))
    row("MUTANT: an untracked Cargo.lock decides what the image was built from",
        not fenced.untracked_is_ignorable("Cargo.lock")
        and not fenced.untracked_is_ignorable("Cargo.toml")
        and not fenced.untracked_is_ignorable("rust-toolchain.toml"))
    row("MUTANT: an untracked file under .cargo/ changes the compiler's inputs",
        not fenced.untracked_is_ignorable(".cargo/config.toml"))
    row("MUTANT: the files the runtime stages COPY by name",
        not fenced.untracked_is_ignorable("third_party/org-root.fingerprint")
        and not fenced.untracked_is_ignorable("THIRD_PARTY_NOTICES.md")
        and not fenced.untracked_is_ignorable("LICENSE"))
    row("MUTANT: shipped product artifacts a lab is equally built from",
        not fenced.untracked_is_ignorable("charts/logweir/values.yaml")
        and not fenced.untracked_is_ignorable("config/crd/backups.yaml")
        and not fenced.untracked_is_ignorable("ui/src/app.ts")
        and not fenced.untracked_is_ignorable("logweir.yaml"))
    row("the harness trees, their fixtures and prose are ignorable",
        fenced.untracked_is_ignorable("scripts/live/d1/scratch.py")
        and fenced.untracked_is_ignorable("scripts/fixtures/x.py")
        and fenced.untracked_is_ignorable("e2e/k8s/d3/x.json")
        and fenced.untracked_is_ignorable("docs/note.md"))
    row("MUTANT: FAIL CLOSED — an untracked file in a directory it has never "
        "heard of is refused",
        not fenced.untracked_is_ignorable("scripts/check-image.sh")
        and not fenced.untracked_is_ignorable("newthing/x.rs")
        and not fenced.untracked_is_ignorable(".github/workflows/images.yml"))


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


def test_an_untracked_build_input_trips_it_live() -> None:
    """The reviewer's own case, against this repository."""
    head = _git("rev-parse", "HEAD")
    stray = ROOT / "crates" / "hr2-drift-probe-untracked.rs"
    stray.write_text("// an untracked Rust source inside the build inputs\n")
    try:
        ok, why, _ = _contains(head)
        row("MUTANT: an untracked file inside crates/ is refused, and named",
            not ok and "hr2-drift-probe-untracked.rs" in why, why[:180])
    finally:
        stray.unlink(missing_ok=True)
    ok, why, _ = _contains(head)
    row("the probe left the checkout clean again", ok, why[:160])


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
