#!/usr/bin/env python3
"""Unit rows for the D2 live harness's row decisions.

    python3 e2e/k8s/d2/test_rows.py

No cluster, no network, no credential: every case is a recorded status block.
Each decision is exercised against the shape the product publishes today AND
against the shape it published before the defect was fixed; the second must be
REFUSED. A decision that cannot be made to say False is not a decision.

The recorded shapes come from lab-refresh-3 §8.4 and from this harness's own
run of 2026-09-18.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="d2-test-rows-")
os.environ.setdefault("D2W14_OUT", str(pathlib.Path(_TMP) / "private"))
os.environ.setdefault("D2W14_ART", str(pathlib.Path(_TMP) / "artifacts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d2_live as d2  # noqa: E402

FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


# --- S11, as the fixed product answers it -----------------------------------
S11_STATUS = {
    "phase": "Failed",
    "reason": "BrokerUnreachable",
    "message": "connection.authenticated: all-topics metadata reported BrokerUnreachable.",
    "jobRef": {"name": "lwc-td-2298bc38fca9898e4fd7"},
}
S11_JOB_COMPLETE = [
    {"type": "SuccessCriteriaMet", "status": "True", "reason": "CompletionsReached"},
    {"type": "Complete", "status": "True", "reason": "CompletionsReached"},
]
S11_RELAYED = {
    "contract": "logweir.dev/check-result/v1",
    "kind": "topicInventory",
    "checks": [{
        "id": "connection.authenticated",
        "code": "BrokerUnreachable",
        "gating": "blocking",
        "state": "notReady",
        "authority": "checkJob",
    }],
}


def test_s11_judges_the_reachable_form_of_the_fifth_criterion() -> None:
    criteria = d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, S11_RELAYED, [])
    row("S11: all five criteria met by a Job that COMPLETED",
        all(criteria.values()), json.dumps(criteria))
    row("MUTANT: the pre-fix criterion — a Job with no terminal condition at all",
        not all(d2.s11_criteria(S11_STATUS, [], S11_RELAYED, []).values()))
    disagree = json.loads(json.dumps(S11_RELAYED))
    disagree["checks"][0]["code"] = "MetadataTimeout"
    row("MUTANT: the object's reason and the relayed frame disagreeing is refused",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, disagree, []).values()))
    advisory = json.loads(json.dumps(S11_RELAYED))
    advisory["checks"][0]["gating"] = "advisory"
    row("MUTANT: an advisory notReady cannot carry a blocking classification",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, advisory, []).values()))
    row("MUTANT: no relayed frame at all — the controller guessed",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, None, []).values()))
    row("MUTANT: a chunk ConfigMap written by a run that failed",
        not all(d2.s11_criteria(S11_STATUS, S11_JOB_COMPLETE, S11_RELAYED,
                                ["td-timeout-r0-0"]).values()))
    succeeded = dict(S11_STATUS, phase="Succeeded")
    row("MUTANT: an unreachable broker that ended Succeeded",
        not all(d2.s11_criteria(succeeded, S11_JOB_COMPLETE, S11_RELAYED, []).values()))
    # §14.4 S11 as amended at `ce69be4` says the check Job is `Complete`. A
    # FAILED Job on this fixture is the runner going back to failing the Job on
    # the Kafka-timeout path — the pre-W9 behaviour the amendment describes as
    # past. It used to be asserted here as a POSITIVE (review L-2).
    row("MUTANT: a FAILED check Job on the Kafka-timeout fixture",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
            S11_RELAYED, []).values()))
    row("MUTANT: SuccessCriteriaMet alone is not Complete",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "SuccessCriteriaMet", "status": "True", "reason": "CompletionsReached"}],
            S11_RELAYED, []).values()))
    row("MUTANT: a Complete condition that is not True",
        not all(d2.s11_criteria(
            S11_STATUS,
            [{"type": "Complete", "status": "False", "reason": "CompletionsReached"}],
            S11_RELAYED, []).values()))


# --- S1.statusVerification, as the fixed product answers it -----------------
NOT_ATTEMPTED = {
    "result": "NotAttempted",
    "detail": ("BackupDestination lw-d2w14/dest-a reads evidence with a grant only a pod may "
               "hold (D2 §3.9's evidence-fetch Job)"),
    "payloadType": "application/vnd.logweir.backup-receipt+json;version=1.0.0",
    "verifiedAt": "2026-09-18T18:47:10Z",
}


def test_not_attempted_is_written_and_says_why() -> None:
    row("the block exists, names the verdict and the reason, and matched no key",
        all(d2.not_attempted_is_honest(NOT_ATTEMPTED, []).values()))
    row("MUTANT: the pre-fix shape — no verification block at all",
        not all(d2.not_attempted_is_honest(None, []).values()))
    row("MUTANT: NotAttempted with nothing said about why",
        not all(d2.not_attempted_is_honest(dict(NOT_ATTEMPTED, detail=""), []).values()))
    row("MUTANT: a verdict outside the three published ones",
        not all(d2.not_attempted_is_honest(dict(NOT_ATTEMPTED, result="Unknown"), []).values()))
    row("MUTANT: nothing was verified, yet a key is named",
        not all(d2.not_attempted_is_honest(
            dict(NOT_ATTEMPTED, matchedKeyId="2c76e22f"), []).values()))
    row("MUTANT: the premise moved — the policy now allows a controller identity",
        not all(d2.not_attempted_is_honest(
            NOT_ATTEMPTED, ["s3://lw-a/logweir/"]).values()))
    valid = {"result": "Valid", "matchedKeyId": "2c76e22f", "verifiedAt": "2026-09-18T18:47:10Z"}
    row("MUTANT: a green Valid where nobody could read the evidence",
        not all(d2.not_attempted_is_honest(valid, []).values()))


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
