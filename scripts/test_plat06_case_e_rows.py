#!/usr/bin/env python3
"""Offline rows for `scripts/test-plat06-live.py`'s case e judges (RECEIPT-DUP).

No cluster: the two judges are pure, and every row here is a set of facts the
live run could observe. The negative controls REQUIRE the judge to fail — the
first one is the exact pre-fix outcome lab-refresh-9 recorded (the re-created
Job exits 0 and signs a second receipt over a rewritten manifest), so a judge
that passes it is a judge that cannot see RECEIPT-DUP.

    python3 scripts/test_plat06_case_e_rows.py
"""

from __future__ import annotations

import copy
import importlib.util
import os
import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]

# The harness makes its output directory at import; point it somewhere private.
os.environ["LOGWEIR_PLAT06_OUT"] = tempfile.mkdtemp(prefix="plat06-rows-")
_spec = importlib.util.spec_from_file_location("plat06", ROOT / "scripts" / "test-plat06-live.py")
plat06 = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(plat06)

M1 = "sha256:" + "1" * 64
M2 = "sha256:" + "2" * 64

RECEIPT_1 = {
    "key": "logweir/backups/exec-1/01RUN1.receipt.json",
    "runId": "01RUN1",
    "verifierValid": True,
    "manifestSha256Attested": M1,
    "manifestSha256Actual": M1,
}

CLAIMED = {
    "phase": "Failed",
    "exitCode": 1,
    "exitReason": "ExecutionAlreadyClaimed",
    "rerunLog": "progress-contract=2\nprogress-phase=-1:admit\n"
    "operational: ExecutionAlreadyClaimed: …\nfailure-reason=ExecutionAlreadyClaimed\n",
    "claim": {"backup_id": "exec-1", "run_id": "01RUN1"},
    "receipts": [RECEIPT_1],
}

UNCLAIMED = {
    "phase": "Succeeded",
    "exitCode": 0,
    "claim": {"backup_id": "exec-2", "run_id": "01RUN1"},
    "receipts": [RECEIPT_1],
}

FAILED: list[str] = []


def row(name: str, ok: bool) -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}")
    if not ok:
        FAILED.append(name)


def claimed(**over: object) -> list[str]:
    f = copy.deepcopy(CLAIMED)
    f.update(over)
    return plat06.judge_case_e_claimed(f)


def unclaimed(**over: object) -> list[str]:
    f = copy.deepcopy(UNCLAIMED)
    f.update(over)
    return plat06.judge_case_e_unclaimed(f)


def main() -> int:
    row("claimed arm: Failed/ExecutionAlreadyClaimed, one valid receipt, is accepted", claimed() == [])
    row("unclaimed arm: Succeeded, one valid receipt, is accepted", unclaimed() == [])

    # -- negative controls: each REQUIRES the judge to refuse ---------------
    pre_fix = dict(
        phase="Succeeded",
        exitCode=0,
        exitReason="ok",
        rerunLog="progress-phase=-1:engine\nreceipt-key=…\n",
        receipts=[
            dict(RECEIPT_1, manifestSha256Actual=M2),
            dict(RECEIPT_1, key="logweir/backups/exec-1/01RUN2.receipt.json", runId="01RUN2",
                 manifestSha256Attested=M2, manifestSha256Actual=M2),
        ],
    )
    row("NEGATIVE CONTROL: the pre-fix outcome (second receipt, rewritten manifest) is refused",
        claimed(**pre_fix) != [])
    row("NEGATIVE CONTROL: the pre-fix outcome is refused by the unclaimed judge too",
        unclaimed(**pre_fix) != [])
    row("refused: exitReason not lifted (plain `operational`)",
        any("exitReason" in m for m in claimed(exitReason="operational")))
    row("refused: the re-created pod started the engine",
        any("STARTED THE ENGINE" in m for m in claimed(rerunLog=CLAIMED["rerunLog"] + "progress-phase=-1:engine\n")))
    row("refused: the re-created pod's log does not name the claim",
        claimed(rerunLog="progress-phase=-1:admit\n") != [])
    row("refused: zero receipts (the orphaned pod never signed)", claimed(receipts=[]) != [])
    row("refused: the one receipt does not verify",
        claimed(receipts=[dict(RECEIPT_1, verifierValid=False)]) != [])
    row("refused: the manifest no longer hashes to the attested digest",
        claimed(receipts=[dict(RECEIPT_1, manifestSha256Actual=M2)]) != [])
    row("refused: no claim object", claimed(claim=None) != [])
    row("refused: the claim names another run", claimed(claim={"run_id": "01OTHER"}) != [])
    row("refused: unclaimed arm that Failed", unclaimed(phase="Failed", exitCode=1) != [])
    row("refused: unclaimed arm with two receipts",
        unclaimed(receipts=[RECEIPT_1, dict(RECEIPT_1, runId="01RUN2")]) != [])

    print(f"\n{len(FAILED)} failed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
