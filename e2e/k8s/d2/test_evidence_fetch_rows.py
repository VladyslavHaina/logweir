#!/usr/bin/env python3
"""Unit rows for lab-refresh-8's evidence-fetch rows (D2 §3.9, `claude/evidence-fetch`).

    python3 e2e/k8s/d2/test_evidence_fetch_rows.py

No cluster. Every predicate `d2_live.py`'s `evf*`, `s1b` and `s1c` phases judge
a live object with is driven here over the shape the FIXED controller writes,
and over planted-wrong shapes — the pre-fetch build's `NotAttempted`, a Job
holding the archive-WRITE Secret, a foreign pod, a lost retry, a second Job
after a restart — each of which must be REFUSED. A predicate that cannot be
made to say False is not a row.
"""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import os
import pathlib
import sys
import tempfile
from typing import Any

_TMP = tempfile.mkdtemp(prefix="d2-evf-rows-")
os.environ.setdefault("D2W14_OUT", str(pathlib.Path(_TMP) / "private"))
os.environ.setdefault("D2W14_ART", str(pathlib.Path(_TMP) / "artifacts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import d2_live as d2  # noqa: E402

_REPO = pathlib.Path(__file__).resolve().parents[3]
FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def _fails_on_a_recorded_row(fn):
    """Assert INSIDE the call, so pytest names the test that recorded the row
    (the pattern `test_rows.py` uses, for the reason it gives)."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        before = len(FAILURES)
        fn(*args, **kwargs)
        recorded = FAILURES[before:]
        assert not recorded, "failing rows: " + "; ".join(recorded)

    return wrapper


def refused(clauses: dict[str, bool]) -> bool:
    return not all(clauses.values())


# --- the fixtures: what the fixed controller writes ---------------------------

UID = "5f0c2e0e-7a51-4c1e-9d0e-2b8f1a6c3d41"
EV1 = d2.evidence_fetch_job_name(UID, 1)
EV2 = d2.evidence_fetch_job_name(UID, 2)
JOB_UID = "0d1c2b3a-0000-4000-8000-00000000e001"
T0 = "2026-09-22T20:00:00Z"
T_RETRY = "2026-09-22T20:01:00Z"


def _backup(result: str | None, *, observation: dict[str, Any] | None = None,
            window: bool = True, facts: bool = True, verified: str = "True",
            **ver: Any) -> dict[str, Any]:
    status: dict[str, Any] = {
        "phase": "Succeeded", "exitCode": 0, "backupId": "8a1f",
        "evidence": {"receiptKey": "logweir/backups/8a1f/01K.receipt.json",
                     "sidecarKey": "logweir/backups/8a1f/01K.receipt.sig",
                     "receiptSha256": "sha256:" + "c" * 64},
        "conditions": [{"type": "Verified", "status": verified,
                        "reason": "Verified" if verified == "True" else "VerificationNotAttempted"}],
    }
    if result is not None:
        status["evidence"]["verification"] = {"result": result, "verifiedAt": T0, **ver}
    if observation is not None:
        status["evidence"]["observation"] = observation
    if window:
        status["windowCovered"] = {"fromMs": 1758570000000, "toMs": 1758570060000}
    if facts:
        status["records"] = {"orders": 50}
        status["capture"] = {"startedAt": "2026-09-22T19:59:40Z",
                             "finishedAt": "2026-09-22T19:59:58Z"}
    return {"apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
            "metadata": {"name": "bk-evf1-1", "uid": UID, "resourceVersion": "900"},
            "status": status}


OBS_VALID = {"mode": "SecretKeys", "jobRef": {"name": EV1, "uid": JOB_UID}, "attempt": 1,
             "presence": "Complete", "retryAfter": None}
VALID = _backup("Valid", observation=OBS_VALID, matchedKeyId="2c76e22ff89969dc")
# THE PRE-FETCH BUILD: `NotAttempted` naming the Job it did not create.
PRE_FETCH = _backup("NotAttempted", window=False, facts=False, verified="False",
                    detail="BackupDestination ns/dest-a reads evidence with a grant only a pod "
                           "may hold (D2 §3.9's evidence-fetch Job), and this build does not "
                           "create that Job")


def _event(kind: str, rv: int, obj: dict[str, Any]) -> dict[str, Any]:
    obj = copy.deepcopy(obj)
    obj["metadata"]["resourceVersion"] = str(rv)
    return {"type": kind, "object": obj}


def _stream(*events: dict[str, Any], truncated_tail: bool = True) -> str:
    """What `kubectl --watch --output-watch-events -o json` prints: indented
    documents back to back, and — when the harness killed it — half of one."""
    text = "\n".join(json.dumps(e, indent=4) for e in events)
    if truncated_tail:
        text += '\n{\n    "type": "MODIFIED",\n    "object": {"metadata": {"resou'
    return text


def _running() -> dict[str, Any]:
    b = _backup(None, window=False, facts=False, verified="Unknown")
    b["status"]["phase"] = "Running"
    return b


def _terminal_without_verdict() -> dict[str, Any]:
    return _backup(None, window=False, facts=False, verified="Unknown")


PENDING = _backup("Pending", observation={"mode": "SecretKeys",
                                          "jobRef": {"name": EV1, "uid": JOB_UID},
                                          "attempt": 1, "presence": None, "retryAfter": None},
                  window=False, facts=False, verified="False",
                  detail=f"evidence-fetch Job ns/{EV1} (attempt 1 of 4) is reading this run's "
                         f"evidence with the destination's evidenceRead grant")
TRAIL_OK = d2.verdict_trail(d2.parse_watch_stream(_stream(
    _event("ADDED", 101, _running()),
    _event("MODIFIED", 102, _terminal_without_verdict()),
    _event("MODIFIED", 103, PENDING),
    _event("MODIFIED", 103, PENDING),  # a duplicate delivery of one version
    _event("MODIFIED", 104, VALID),
)))


@_fails_on_a_recorded_row
def test_the_watch_stream_is_read_version_by_version() -> None:
    row("four versions, the duplicate delivery of one of them counted once, and the "
        "truncated tail the harness's kill left dropped rather than guessed",
        [t["resourceVersion"] for t in TRAIL_OK] == ["101", "102", "103", "104"],
        json.dumps(TRAIL_OK))
    row("the Pending version names attempt 1's Job", TRAIL_OK[2]["result"] == "Pending"
        and TRAIL_OK[2]["jobRef"] == EV1 and TRAIL_OK[2]["attempt"] == 1)
    row("an empty or garbage stream is no trail, not an exception",
        d2.parse_watch_stream("") == [] and d2.parse_watch_stream("not json") == [])


@_fails_on_a_recorded_row
def test_the_job_name_is_the_controllers_own_arithmetic() -> None:
    want = "lwc-ev-" + hashlib.sha256(f"{UID}:1".encode()).hexdigest()[:20]
    row("lwc-ev-<first 20 hex of sha256(uid:attempt)>", EV1 == want and len(EV1) == 27,
        EV1)
    row("MUTANT: the attempt dropped from the name — attempt 2 would re-observe attempt 1",
        EV1 != EV2)
    row("MUTANT: the plain check-Job name sha256(uid) is not the fetch Job's",
        EV1 != "lwc-ev-" + hashlib.sha256(UID.encode()).hexdigest()[:20])
    # THE FIXTURE BOTH SIDES AGREE ON, read from the side that writes it. On a
    # checkout that predates `claude/evidence-fetch` the function does not
    # exist yet and the row says so; once it lands, a change to the formula
    # on either side fails here.
    job_rs = (_REPO / "crates/weirkeeper/src/check/job.rs").read_text()
    if "pub fn evidence_fetch_job_name" in job_rs:
        body = job_rs.split("pub fn evidence_fetch_job_name", 1)[1].split("\n}\n", 1)[0]
        row("check/job.rs derives the name from \"{owner_uid}:{attempt}\" through the lwc-/ev "
            "check-Job arithmetic",
            '"{owner_uid}:{attempt}"' in body and "CheckPlanKind::EvidenceFetch" in body, body)
        fetch_rs = (_REPO / "crates/weirkeeper/src/evidence_fetch.rs").read_text()
        row("the first retry is +60 s and the TTL 600 s, as the rows wait for",
            "RETRY_DELAYS_SECONDS: [i64; 3] = [60, 300, 900]" in fetch_rs
            and "pub const TTL_SECONDS: i32 = 600;" in job_rs)
    else:
        row("check/job.rs has no evidence_fetch_job_name on this checkout (claude/evidence-fetch "
            "has not landed); the formula is pinned by the vector above", True)


@_fails_on_a_recorded_row
def test_evf1_pending_then_valid_through_the_job() -> None:
    ok = d2.fetch_reached_valid(TRAIL_OK, VALID, [])
    row("EVF-1: Pending named attempt 1's Job, then Valid with window, records and capture",
        all(ok.values()), json.dumps(ok))
    row("MUTANT: the pre-fetch build — NotAttempted, no Job, no window",
        refused(d2.fetch_reached_valid(d2.verdict_trail([_event("MODIFIED", 1, PRE_FETCH)]),
                                       PRE_FETCH, [])))
    no_pending = [t for t in TRAIL_OK if t["result"] != "Pending"]
    row("MUTANT: a Valid that never went through Pending — not the Job's verdict",
        refused(d2.fetch_reached_valid(no_pending, VALID, [])))
    after = TRAIL_OK + [dict(TRAIL_OK[2], resourceVersion="105")]
    row("MUTANT: Pending written again AFTER Valid — a second fetch over a reached verdict",
        refused(d2.fetch_reached_valid(after, VALID, [])))
    detour = TRAIL_OK[:3] + [dict(TRAIL_OK[3], result="Invalid", resourceVersion="104a")] \
        + TRAIL_OK[3:]
    row("MUTANT: an Invalid on the way to Valid", refused(d2.fetch_reached_valid(detour, VALID, [])))
    for label, mutate in (
        ("no windowCovered — the receipt's facts were not projected",
         lambda b: b["status"].pop("windowCovered")),
        ("no records", lambda b: b["status"].pop("records")),
        ("no capture", lambda b: b["status"].pop("capture")),
        ("no matchedKeyId", lambda b: b["status"]["evidence"]["verification"].pop("matchedKeyId")),
        ("Verified is not True", lambda b: b["status"]["conditions"][0].update(status="False")),
        ("the observation names another run's Job",
         lambda b: b["status"]["evidence"]["observation"]["jobRef"].update(
             name=d2.evidence_fetch_job_name("someone-else", 1))),
        ("presence Unknown — a relay that was not read whole",
         lambda b: b["status"]["evidence"]["observation"].update(presence="Unknown")),
        ("mode ControllerIdentity — not the Job's read",
         lambda b: b["status"]["evidence"]["observation"].update(mode="ControllerIdentity")),
        ("a verdict still Pending at the bound", lambda b: b["status"]["evidence"][
            "verification"].update(result="Pending")),
    ):
        bad = copy.deepcopy(VALID)
        mutate(bad)
        row(f"MUTANT: {label}", refused(d2.fetch_reached_valid(TRAIL_OK, bad, [])))
    row("MUTANT: the premise moved — a controller-identity location is allowlisted",
        refused(d2.fetch_reached_valid(TRAIL_OK, VALID, ["s3://lw-a/logweir/"])))


@_fails_on_a_recorded_row
def test_s1_status_verification_now_expects_the_jobs_valid() -> None:
    row("S1.statusVerification: a SecretKeys destination reads Valid through the Job",
        all(d2.fetched_verdict_is_valid(VALID, []).values()))
    row("MUTANT: the sentence this row used to require — NotAttempted, 'this build does not "
        "create that Job' — is now the defect", refused(d2.fetched_verdict_is_valid(PRE_FETCH, [])))
    row("MUTANT: Pending at the bound is a fetch that never finished",
        refused(d2.fetched_verdict_is_valid(PENDING, [])))


def _job(*, secret: str = "a-archread", uid: str = JOB_UID, owner: str = UID,
         name: str = EV1) -> dict[str, Any]:
    ref = lambda var, key: {"name": var, "valueFrom": {"secretKeyRef": {  # noqa: E731
        "name": secret, "key": key}}}
    return {
        "metadata": {"name": name, "uid": uid,
                     "labels": {"logweir.dev/check-kind": "evidenceFetch",
                                "logweir.dev/check-owner-uid": owner},
                     "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
                                          "name": "bk-evf1-1", "uid": owner,
                                          "controller": True}]},
        "spec": {"ttlSecondsAfterFinished": 600, "template": {"spec": {
            "serviceAccountName": "logweir-runner",
            "automountServiceAccountToken": False,
            "volumes": [{"name": "check-plan", "configMap": {"name": f"{name}-plan"}},
                        {"name": "work", "emptyDir": {}}],
            "containers": [{"name": "runner", "env": [
                ref("AWS_ACCESS_KEY_ID", "access-key-id"),
                ref("AWS_SECRET_ACCESS_KEY", "secret-access-key"),
                {"name": "AWS_ALLOW_HTTP", "value": "true"},
                {"name": "LOGWEIR_CHECK_PLAN_SHA256", "value": "sha256:" + "d" * 64},
            ]}],
        }}},
    }


POD = {"metadata": {"name": f"{EV1}-x7k2p", "ownerReferences": [
    {"apiVersion": "batch/v1", "kind": "Job", "name": EV1, "uid": JOB_UID, "controller": True}]}}
PLAN = {"metadata": {"name": f"{EV1}-plan", "ownerReferences": [
    {"kind": "Backup", "uid": UID, "controller": True}]}, "immutable": True}
FORBIDDEN = {"a-writer", "logweir-signing-key"}


def _grant(job=None, pod=None, plan=None, owner=None) -> dict[str, bool]:
    return d2.evidence_job_holds_only_the_read_grant(
        _job() if job is None else job, POD if pod is None else pod,
        PLAN if plan is None else plan, VALID if owner is None else owner,
        owner_kind="Backup", read_secret="a-archread", forbidden_secrets=FORBIDDEN)


@_fails_on_a_recorded_row
def test_evf2_the_job_holds_only_the_read_grant() -> None:
    ok = _grant()
    row("EVF-2: exactly the archiveRead grant, no token, check-plan + work, pod owned by the "
        "Job's UID, TTL 600", all(ok.values()), json.dumps(ok))
    row("MUTANT M01: the grant resolved as archiveWrite — the Job holds a-writer",
        refused(_grant(job=_job(secret="a-writer"))))
    signer = _job()
    signer["spec"]["template"]["spec"]["volumes"].append(
        {"name": "signer", "secret": {"secretName": "logweir-signing-key"}})
    row("MUTANT M02: the signing key mounted", refused(_grant(job=signer)))
    token = _job()
    token["spec"]["template"]["spec"]["automountServiceAccountToken"] = True
    row("MUTANT: a ServiceAccount token automounted", refused(_grant(job=token)))
    writer = _job()
    writer["spec"]["template"]["spec"]["containers"][0]["env"].append(
        {"name": "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID",
         "valueFrom": {"secretKeyRef": {"name": "a-archread", "key": "access-key-id"}}})
    row("MUTANT: an evidence-WRITE variable beside the read grant", refused(_grant(job=writer)))
    env_from = _job()
    env_from["spec"]["template"]["spec"]["containers"][0]["envFrom"] = [
        {"secretRef": {"name": "a-writer"}}]
    row("MUTANT: a whole Secret projected by envFrom", refused(_grant(job=env_from)))
    foreign = copy.deepcopy(POD)
    foreign["metadata"]["ownerReferences"][0]["uid"] = "a-pod-somebody-else-made"
    row("MUTANT M15 (SEC-PODLOG): the pod read is not controlled by this Job's UID",
        refused(_grant(pod=foreign)))
    row("MUTANT: no pod found at all", refused(_grant(pod={})))
    no_ttl = _job()
    no_ttl["spec"].pop("ttlSecondsAfterFinished")
    row("MUTANT: the TTL never patched after the verdict", refused(_grant(job=no_ttl)))
    row("MUTANT: a mutable plan ConfigMap", refused(_grant(plan=dict(PLAN, immutable=False))))
    squatter = _job()
    squatter["metadata"]["ownerReferences"][0]["controller"] = False
    row("MUTANT: a Job this Backup does not CONTROL (owner, not controller)",
        refused(_grant(job=squatter)))
    row("MUTANT: attempt 2's name while the status records attempt 1",
        refused(_grant(job=_job(name=EV2))))
    row("MUTANT: no Job at all", refused(_grant(job={})))


NA_DETAIL = (f"evidence-fetch Job ns/{EV1} (attempt 1 of 4) ended without a verified relay: "
             f"CredentialSecretKeyMissing: Secret ev-broken has no key secret-access-key; "
             f"attempt 2 starts at {T_RETRY}")
NA1 = _backup("NotAttempted", observation={"mode": "SecretKeys",
                                           "jobRef": {"name": EV1, "uid": JOB_UID},
                                           "attempt": 1, "presence": None,
                                           "retryAfter": T_RETRY},
              window=False, facts=False, verified="False", detail=NA_DETAIL)
PENDING2 = _backup("Pending", observation={"mode": "SecretKeys",
                                           "jobRef": {"name": EV2, "uid": "job-2"},
                                           "attempt": 2, "presence": None, "retryAfter": None},
                   window=False, facts=False, verified="False", detail="attempt 2 reading")
BROKEN_TRAIL = d2.verdict_trail([_event("MODIFIED", 1, _terminal_without_verdict()),
                                 _event("MODIFIED", 2, PENDING), _event("MODIFIED", 3, NA1),
                                 _event("MODIFIED", 4, PENDING2)])
ATTEMPT_TWO = _job(name=EV2, uid="job-2", secret="ev-broken")


@_fails_on_a_recorded_row
def test_evf4_a_broken_grant_is_not_attempted_and_retried() -> None:
    ok = d2.broken_grant_retries(BROKEN_TRAIL, PENDING2, ATTEMPT_TWO)
    row("EVF-4: NotAttempted naming lwc-ev-…(attempt 1) and the code, retry at +60 s, then "
        "attempt 2's own Job", all(ok.values()), json.dumps(ok))
    spent = copy.deepcopy(NA1)
    spent["status"]["evidence"]["observation"]["retryAfter"] = None
    spent["status"]["evidence"]["verification"]["detail"] = NA_DETAIL.split("; attempt 2")[0] \
        + "; no attempts remain — run the printed logweir drill verify command"
    row("MUTANT M10: no retry scheduled", refused(d2.broken_grant_retries(
        d2.verdict_trail([_event("MODIFIED", 3, spent)]), spent, None)))
    row("MUTANT: attempt 2's Job never created", refused(d2.broken_grant_retries(
        BROKEN_TRAIL, PENDING2, None)))
    row("MUTANT: attempt 2 re-used attempt 1's name", refused(d2.broken_grant_retries(
        BROKEN_TRAIL, PENDING2, _job(name=EV1, secret="ev-broken"))))
    green = BROKEN_TRAIL + d2.verdict_trail([_event("MODIFIED", 5, VALID)])
    row("MUTANT: a broken grant that came back Valid", refused(d2.broken_grant_retries(
        green, VALID, ATTEMPT_TWO)))
    windowed = d2.verdict_trail([_event("MODIFIED", 3, dict(NA1, status=dict(
        NA1["status"], windowCovered={"fromMs": 1, "toMs": 2})))])
    row("MUTANT: a window projected from a receipt nobody read", refused(d2.broken_grant_retries(
        BROKEN_TRAIL[:2] + windowed + BROKEN_TRAIL[3:], PENDING2, ATTEMPT_TWO)))
    vague = copy.deepcopy(NA1)
    vague["status"]["evidence"]["verification"]["detail"] = "the fetch failed"
    row("MUTANT: a detail that names neither the Job nor the code", refused(
        d2.broken_grant_retries(d2.verdict_trail([_event("MODIFIED", 3, vague)])
                                + BROKEN_TRAIL[3:], PENDING2, ATTEMPT_TWO)))
    late = copy.deepcopy(NA1)
    late["status"]["evidence"]["observation"]["retryAfter"] = "2026-09-22T20:15:00Z"
    row("MUTANT: the first retry at +15 m, not +1 m", refused(d2.broken_grant_retries(
        d2.verdict_trail([_event("MODIFIED", 3, late)]) + BROKEN_TRAIL[3:], PENDING2,
        ATTEMPT_TWO)))


def _restart(jobs, final=VALID, *, up_at="2026-09-22T20:03:00Z", pending=True):
    return d2.restart_resumed_the_fetch(TRAIL_OK, final, jobs, down_at="2026-09-22T20:02:00Z",
                                        up_at=up_at, pending_before_down=pending)


FIRST_JOB = {"metadata": {"name": EV1, "uid": JOB_UID,
                          "creationTimestamp": "2026-09-22T20:01:50Z"}}
AFTER_UP = copy.deepcopy(VALID)
AFTER_UP["status"]["evidence"]["verification"]["verifiedAt"] = "2026-09-22T20:03:20Z"


@_fails_on_a_recorded_row
def test_evf5_a_restarted_controller_finds_the_same_job() -> None:
    ok = _restart([FIRST_JOB], AFTER_UP)
    row("EVF-5: one Job, created before the stop; Valid written after the new controller "
        "started, naming that Job", all(ok.values()), json.dumps(ok))
    row("MUTANT M07/M11: the restarted controller created a second Job", refused(_restart(
        [FIRST_JOB, {"metadata": {"name": EV2, "uid": "job-2"}}], AFTER_UP)))
    recreated = copy.deepcopy(AFTER_UP)
    recreated["status"]["evidence"]["observation"]["jobRef"]["uid"] = "a-recreated-job"
    row("MUTANT: the verdict names a RE-CREATED Job of the same name", refused(_restart(
        [FIRST_JOB], recreated)))
    row("MUTANT: the verdict was written before the new controller started — the old one "
        "finished and nothing was restarted", refused(_restart([FIRST_JOB], VALID)))
    row("MUTANT: no Pending when the controller went down", refused(_restart(
        [FIRST_JOB], AFTER_UP, pending=False)))


def _restore(outcome: str = "pass", verified: str = "True", *, objectives: bool = True):
    r = copy.deepcopy(VALID)
    r["kind"] = "Restore"
    r["metadata"]["name"] = "rs-evf6-1"
    for key in ("windowCovered", "records", "capture"):
        r["status"].pop(key, None)
    r["status"]["outcome"] = outcome
    if objectives:
        r["status"]["objectives"] = {"rto": {"met": True}, "passRate": {"met": True}}
    r["status"]["evidence"] = {
        "scorecardKey": "logweir/drills/01K.json", "sidecarKey": "logweir/drills/01K.sig",
        "scorecardSha256": "sha256:" + "e" * 64,
        "verification": {"result": "Valid", "matchedKeyId": "2c76", "verifiedAt": T0},
        "observation": OBS_VALID}
    r["status"]["conditions"] = [{"type": "Verified", "status": verified}]
    return r


RESTORE_JOB = _job(owner=UID)
RESTORE_JOB["metadata"]["ownerReferences"][0]["kind"] = "Restore"


@_fails_on_a_recorded_row
def test_evf6_a_restore_scorecard_goes_pending_then_valid() -> None:
    ok = d2.restore_fetch_reached_valid(TRAIL_OK, _restore(), RESTORE_JOB)
    row("EVF-6: Pending → Valid, outcome and objectives copied, Verified=True on pass, the "
        "Job controlled by the Restore", all(ok.values()), json.dumps(ok))
    row("MUTANT: Verified=True on outcome fail", refused(d2.restore_fetch_reached_valid(
        TRAIL_OK, _restore("fail", "True"), RESTORE_JOB)))
    row("and Verified=False on outcome fail is the rule, not a failure",
        all(d2.restore_fetch_reached_valid(TRAIL_OK, _restore("fail", "False"),
                                           RESTORE_JOB).values()))
    row("MUTANT: the Job owned by a Backup, not the Restore", refused(
        d2.restore_fetch_reached_valid(TRAIL_OK, _restore(), _job())))
    row("MUTANT: objectives not copied", refused(d2.restore_fetch_reached_valid(
        TRAIL_OK, _restore(objectives=False), RESTORE_JOB)))
    row("MUTANT: a Valid that never went through Pending", refused(
        d2.restore_fetch_reached_valid([t for t in TRAIL_OK if t["result"] != "Pending"],
                                       _restore(), RESTORE_JOB)))


NOREAD = _backup("NotAttempted", window=False, facts=False, verified="False",
                 detail="BackupDestination ns/dest-noread declares no evidenceRead grant; "
                        "nothing was verified")


@_fails_on_a_recorded_row
def test_s1_not_attempted_for_a_grant_no_job_may_use() -> None:
    trail = d2.verdict_trail([_event("MODIFIED", 1, _terminal_without_verdict()),
                              _event("MODIFIED", 2, NOREAD)])
    ok = d2.no_fetch_for_a_grant_no_job_may_use(NOREAD, trail, [], [])
    row("S1.notAttempted: no Job, no observation, never Pending, an honest NotAttempted",
        all(ok.values()), json.dumps(ok))
    row("MUTANT: a fetch Job created for a destination with no evidenceRead", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(NOREAD, trail, [_job()], [])))
    row("MUTANT: the verdict went through Pending", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(NOREAD, TRAIL_OK, [], [])))
    observed = copy.deepcopy(NOREAD)
    observed["status"]["evidence"]["observation"] = OBS_VALID
    row("MUTANT: an observation naming a Job", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(observed, trail, [], [])))
    silent = _terminal_without_verdict()
    row("MUTANT: silence — no verification block at all", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(silent, trail, [], [])))
    row("MUTANT: a Valid from somewhere", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(VALID, trail, [], [])))
    row("MUTANT: the premise moved — the location is allowlisted", refused(
        d2.no_fetch_for_a_grant_no_job_may_use(NOREAD, trail, [], ["s3://lw-a/logweir/"])))


@_fails_on_a_recorded_row
def test_the_evidence_fetch_phases_are_runnable_by_name() -> None:
    table = d2.phase_table()
    want = {"evf", "evf4", "evf5", "evf6", "s1b", "s1c"}
    row("every lab-refresh-8 evidence-fetch phase is in the phase table",
        want <= set(table), f"missing {sorted(want - set(table))}")
    row("the settle bound covers one whole fetch attempt (~240 s), not the old 45 s",
        d2.VERDICT_SETTLE_SECONDS >= 240 and "Pending" not in d2.REACHED_VERDICTS)


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except AssertionError:
                pass
    print(f"\n{len(FAILURES)} failing row(s)" if FAILURES else "\nall rows pass")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
