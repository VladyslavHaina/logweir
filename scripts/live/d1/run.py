#!/usr/bin/env python3
"""Live docker-desktop acceptance for D1 §13.2 (PLAT-04.2/05.1/05.2/09.2).

Run one phase per invocation so that a scenario which waits on a cron slot can
be watched, retried or abandoned without re-running the ones that already
passed:

    export LOGWEIR_D1_STAMP=$(date -u +%Y%m%dt%H%Mz)
    scripts/live/d1/run.py setup
    scripts/live/d1/run.py L-04-1 L-04-3 …
    scripts/live/d1/run.py report cleanup

Every scenario records the commands it issued, the UIDs of the objects it
touched, the exact status excerpt it asserted on, its verdict and its timing
into `results.json` under `$LOGWEIR_D1_OUT`, and dumps the full object of
anything that fails next to it. A scenario that cannot run in this environment
is recorded `not-run` WITH ITS REASON; it is never quietly dropped and an
assertion is never relaxed to make a red scenario green.

Kubernetes rules this file obeys and re-asserts at runtime (WORKER-RULES.md):
every call carries `--context docker-desktop`; the namespace is unique, is
labelled `logweir.dev/test-owner=d1w8`, and is deleted only after that label
and the recorded UID are read back; the shared `logweir-scram-local` release is
read, never written.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time
from typing import Any, Callable

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fixture  # noqa: E402
from fence import fenced  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[3]
STAMP = os.environ.get("LOGWEIR_D1_STAMP") or dt.datetime.now(dt.timezone.utc).strftime(
    "%Y%m%dt%H%M%Sz"
)
# The owner label and namespace are settable so that a second wave can run the
# same scenarios under its own fence without pretending to be the first one.
# Defaults are W8's, so an unparameterised invocation behaves exactly as it did.
OWNER = os.environ.get("LOGWEIR_D1_OWNER") or "d1w8"
NS = os.environ.get("LOGWEIR_D1_NS") or f"{OWNER}-{STAMP}"
FENCED = os.environ.get("LOGWEIR_D1_FENCE") == "1"
LABELS = {"logweir.dev/test-owner": OWNER, "d1.logweir.dev/run": STAMP}
if FENCED:
    # The namespaceSelector the ValidatingAdmissionPolicyBinding matches on.
    # It is set here, on the namespace itself, so the fence is in force from
    # the instant the namespace exists rather than from the instant the
    # controller is deployed into it.
    LABELS["d1fence.logweir.dev/fenced"] = STAMP
OUT = pathlib.Path(
    os.environ.get("LOGWEIR_D1_OUT")
    or f"/tmp/logweir-roadmap-run/claude/artifacts/d1-live/{STAMP}"
)
CONTEXT = "docker-desktop"
K = ["kubectl", "--context", CONTEXT]
KN = K + ["-n", NS]
RESULTS = OUT / "results.json"

# Credential key names appear in manifests; their VALUES must never reach an
# artifact. `redact` is applied to every captured stream and every object dump.
SECRET_KEYS = ("access-key-id", "secret-access-key", "password", "user", "signing.pem")

STATE: dict[str, Any] = {
    "run": STAMP,
    "context": CONTEXT,
    "namespace": NS,
    "owner": OWNER,
    "scenarios": {},
    "environment": {},
    "commands": [],
}


# ---------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def redact(text: str) -> str:
    """Never let a base64 Secret value or a PEM block into an artifact."""
    text = re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED private key]",
        text,
        flags=re.S,
    )
    for key in SECRET_KEYS:
        text = re.sub(
            rf'("{re.escape(key)}"\s*:\s*)"[^"]*"', r'\1"[REDACTED]"', text
        )
        text = re.sub(rf"(^\s*{re.escape(key)}:\s*)\S+", r"\1[REDACTED]", text, flags=re.M)
    return text


def save() -> None:
    OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    RESULTS.write_text(redact(json.dumps(STATE, indent=2, sort_keys=True)) + "\n")


def load() -> None:
    if RESULTS.exists():
        stored = json.loads(RESULTS.read_text())
        if stored.get("namespace") == NS:
            STATE.update(stored)


def artifact(name: str, body: Any) -> str:
    OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = OUT / name
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    text = body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True)
    path.write_text(redact(text) + ("" if text.endswith("\n") else "\n"))
    return str(path)


def run(
    args: list[str],
    *,
    data: str | None = None,
    check: bool = True,
    timeout: int = 120,
    record: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Every subprocess has a timeout: a hung child blocks the whole worker."""
    started = time.time()
    result = subprocess.run(
        args, input=data, text=True, capture_output=True, timeout=timeout, cwd=ROOT
    )
    if record:
        STATE["commands"].append(
            {
                "at": now(),
                "argv": args,
                "rc": result.returncode,
                "seconds": round(time.time() - started, 2),
            }
        )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed rc={result.returncode}: {args!r}\n"
            f"stdout={redact(result.stdout[-3000:])}\nstderr={redact(result.stderr[-3000:])}"
        )
    return result


def kubectl(*args: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return run(K + list(args), check=check, timeout=timeout)


def kn(*args: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return run(KN + list(args), check=check, timeout=timeout)


def apply(obj: dict[str, Any]) -> dict[str, Any]:
    return json.loads(run(K + ["apply", "-f", "-", "-o", "json"], data=json.dumps(obj)).stdout)


def create(obj: dict[str, Any]) -> dict[str, Any]:
    return json.loads(run(K + ["create", "-f", "-", "-o", "json"], data=json.dumps(obj)).stdout)


def get(kind: str, name: str, namespace: str | None = NS) -> dict[str, Any]:
    args = K + (["-n", namespace] if namespace else []) + ["get", kind, name, "-o", "json"]
    return json.loads(run(args).stdout)


def get_opt(kind: str, name: str, namespace: str | None = NS) -> dict[str, Any] | None:
    args = K + (["-n", namespace] if namespace else []) + ["get", kind, name, "-o", "json"]
    result = run(args, check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def lst(kind: str, selector: str | None = None, namespace: str | None = NS) -> list[dict[str, Any]]:
    args = K + (["-n", namespace] if namespace else []) + ["get", kind, "-o", "json"]
    if selector:
        args += ["-l", selector]
    return json.loads(run(args).stdout)["items"]


def patch(kind: str, name: str, body: dict[str, Any], kind_of: str = "merge") -> dict[str, Any]:
    return json.loads(
        run(
            KN + ["patch", kind, name, "--type", kind_of, "-p", json.dumps(body), "-o", "json"]
        ).stdout
    )


def condition(obj: dict[str, Any], kind: str) -> dict[str, Any] | None:
    for item in (obj.get("status") or {}).get("conditions") or []:
        if item.get("type") == kind:
            return item
    return None


def wait_for(
    kind: str,
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout: int = 240,
    interval: float = 3.0,
    what: str = "",
) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        last = get_opt(kind, name)
        if last is not None and predicate(last):
            return last
        time.sleep(interval)
    raise Failure(
        f"timed out after {timeout}s waiting for {kind}/{name} {what}".strip(),
        obj=last,
    )


def wait_until(predicate: Callable[[], Any], *, timeout: int = 240, interval: float = 3.0,
               what: str = "") -> Any:
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise Failure(f"timed out after {timeout}s waiting for {what}")


class Failure(Exception):
    """An assertion the live cluster did not satisfy. Reported, never relaxed."""

    def __init__(self, message: str, obj: Any = None, dumps: dict[str, Any] | None = None):
        super().__init__(message)
        self.obj = obj
        self.dumps = dumps or {}


# ---------------------------------------------------------------------------
# Scenario bookkeeping
# ---------------------------------------------------------------------------

SCENARIOS: dict[str, dict[str, Any]] = {}


def scenario(sid: str, task: str, title: str) -> Callable:
    def decorate(fn: Callable[[], dict[str, Any]]) -> Callable:
        SCENARIOS[sid] = {"id": sid, "task": task, "title": title, "fn": fn}
        return fn

    return decorate


def dump_objects(sid: str, names: dict[str, tuple[str, str]]) -> dict[str, str]:
    """Write the full object of everything a scenario cared about."""
    written: dict[str, str] = {}
    for label, (kind, name) in names.items():
        obj = get_opt(kind, name)
        if obj is None:
            written[label] = "absent"
            continue
        written[label] = artifact(f"objects/{sid}/{label}.json", obj)
    return written


def execute(sid: str) -> None:
    spec = SCENARIOS[sid]
    started = time.time()
    entry: dict[str, Any] = {
        "id": sid,
        "task": spec["task"],
        "title": spec["title"],
        "startedAt": now(),
    }
    mark = len(STATE["commands"])
    log(f"=== {sid} — {spec['title']}")
    try:
        detail = spec["fn"]()
        entry.update(detail or {})
        entry["status"] = entry.get("status", "pass")
    except Failure as error:
        entry["status"] = "fail"
        entry["failure"] = str(error)
        if error.obj is not None:
            entry["failureObject"] = artifact(f"objects/{sid}/failure.json", error.obj)
        for label, obj in (error.dumps or {}).items():
            entry.setdefault("dumps", {})[label] = artifact(
                f"objects/{sid}/{label}.json", obj
            )
        log(f"    FAIL {sid}: {error}")
    except Exception as error:  # noqa: BLE001 - recorded, not swallowed
        entry["status"] = "error"
        entry["failure"] = f"{type(error).__name__}: {error}"
        log(f"    ERROR {sid}: {error}")
    entry["finishedAt"] = now()
    entry["seconds"] = round(time.time() - started, 1)
    entry["quiesced"] = quiesce()
    entry["commands"] = compact(STATE["commands"][mark:])
    del STATE["commands"][mark:]
    STATE["scenarios"][sid] = entry
    save()
    log(f"    {entry['status'].upper()} {sid} in {entry['seconds']}s")


def compact(commands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every command this scenario issued, folded by argv.

    A scenario that polls a phase twice a second issues thousands of identical
    reads, and a verbatim list of them would bury the handful of commands that
    CHANGED anything. Folding keeps every distinct argv, how often it ran, when
    it first and last ran, and every distinct exit code it returned — which is
    what a reader checking the method actually needs.
    """
    folded: dict[str, dict[str, Any]] = {}
    for item in commands:
        key = " ".join(item["argv"])
        entry = folded.setdefault(
            key,
            {"argv": item["argv"], "count": 0, "firstAt": item["at"], "returnCodes": []},
        )
        entry["count"] += 1
        entry["lastAt"] = item["at"]
        if item["rc"] not in entry["returnCodes"]:
            entry["returnCodes"].append(item["rc"])
    return sorted(folded.values(), key=lambda e: e["firstAt"])


def quiesce() -> list[str]:
    """Suspend every schedule this namespace still holds, between scenarios.

    Two other live waves share the controller that reconciles this namespace.
    A scenario that has finished measuring has no business firing a run every
    two minutes for the rest of the session, so each one hands the controller
    back a quiet namespace. Nothing is deleted: the objects stay for the
    evidence dumps and for the cleanup inventory.
    """
    suspended: list[str] = []
    try:
        for item in lst("backupschedules"):
            if item["spec"].get("suspend") is not True:
                name = item["metadata"]["name"]
                run(
                    KN + ["patch", "backupschedule", name, "--type", "merge",
                          "-p", json.dumps({"spec": {"suspend": True}})],
                    check=False,
                    record=False,
                )
                suspended.append(name)
    except Exception as error:  # noqa: BLE001 - reported, never fatal
        return [f"quiesce failed: {error}"]
    return suspended


def not_run(sid: str, reason: str, *, task: str = "", title: str = "") -> None:
    STATE["scenarios"][sid] = {
        "id": sid,
        "task": task or SCENARIOS.get(sid, {}).get("task", ""),
        "title": title or SCENARIOS.get(sid, {}).get("title", ""),
        "status": "not-run",
        "reason": reason,
        "recordedAt": now(),
    }
    save()


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

ARCHIVE_URL = "logweir-destination://dest"
DEST = "dest"


def kafka_exec(*argv: str, timeout: int = 120, check: bool = True) -> str:
    """Run a Kafka CLI tool inside the broker pod."""
    pod = STATE["environment"]["kafkaPod"]
    result = run(KN + ["exec", pod, "--", *argv], timeout=timeout, check=check)
    return result.stdout


def kafka_pod_name() -> str:
    items = lst("pods", "app=d1-kafka")
    ready = [
        p["metadata"]["name"]
        for p in items
        if all(c.get("ready") for c in (p.get("status") or {}).get("containerStatuses") or [])
    ]
    if not ready:
        raise Failure("no ready Kafka pod in the fixture namespace", obj=items)
    return ready[0]


def create_topics(names: list[str], *, records: int = 5) -> None:
    for topic in names:
        kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        )
        if records:
            payload = "\n".join(f"d1-{topic}-{i}" for i in range(records)) + "\n"
            pod = STATE["environment"]["kafkaPod"]
            run(
                KN
                + [
                    "exec",
                    "-i",
                    pod,
                    "--",
                    "/opt/kafka/bin/kafka-console-producer.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    topic,
                ],
                data=payload,
                timeout=120,
            )


def delete_topic(topic: str) -> None:
    kafka_exec(
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--delete",
        "--topic",
        topic,
    )


def list_topics() -> list[str]:
    out = kafka_exec(
        "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"
    )
    return sorted(line.strip() for line in out.splitlines() if line.strip())


def copy_secret(name: str) -> None:
    """Copy a Secret by reference. No value is decoded, printed or stored."""
    source = get("secret", name, namespace=fixture.FIXTURE_NS)
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NS, "labels": LABELS},
        "type": source.get("type", "Opaque"),
        "data": source["data"],
    }
    # Deliberately NOT through `apply`/`create`, which echo the object (and so
    # its data) into this process's captured stdout and then into results.json.
    run(K + ["apply", "-f", "-"], data=json.dumps(body))


def ensure_bucket() -> None:
    """Recreate the archive bucket after a MinIO restart.

    The fixture's MinIO stores on an `emptyDir`, so scaling it to zero — which
    L-04-4 does on purpose — destroys the bucket with it. An earlier run of
    this harness restored the Deployment and not the bucket, and the next
    scenario's run failed `NoSuchBucket`, which looked like a Logweir failure
    and was not one. Whoever takes the store down puts the bucket back.
    """
    mc("mb", "--ignore-existing", f"local/{fixture.BUCKET}", check=False)


def mc(*args: str, check: bool = True) -> str:
    result = run(KN + ["exec", "d1-mc", "--", "mc", *args], check=check, timeout=120)
    return result.stdout


def schedule_object(name: str, **spec: Any) -> dict[str, Any]:
    body = {
        "schedule": "*/2 * * * *",
        "sourceRef": {"name": "source"},
        "topics": ["t1"],
        "archive": {"url": ARCHIVE_URL},
        "destinationRef": {"name": DEST},
        "concurrencyPolicy": "Forbid",
        "suspend": False,
        "activeDeadlineSeconds": 600,
    }
    body.update(spec)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": {"name": name, "namespace": NS, "labels": dict(LABELS)},
        "spec": body,
    }


def backup_object(name: str, **spec: Any) -> dict[str, Any]:
    body = {
        "sourceRef": {"name": "source"},
        "topics": ["t1"],
        "archive": {"url": ARCHIVE_URL},
        "destinationRef": {"name": DEST},
        "triggeredBy": "manual",
        "deadlineSeconds": 600,
    }
    body.update(spec)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": NS, "labels": dict(LABELS)},
        "spec": body,
    }


def terminal(obj: dict[str, Any]) -> bool:
    return (obj.get("status") or {}).get("phase") in {"Succeeded", "Failed", "Refused"}


def backups_of(schedule_uid: str) -> list[dict[str, Any]]:
    return sorted(
        (
            b
            for b in lst("backups")
            if ((b["spec"].get("scheduleRef") or {}).get("uid")) == schedule_uid
        ),
        key=lambda b: b["metadata"]["name"],
    )


def setup() -> None:
    existing = get_opt("namespace", NS, namespace=None)
    if existing is None:
        create(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": NS, "labels": dict(LABELS)},
            }
        )
        existing = get("namespace", NS, namespace=None)
    if existing["metadata"]["labels"].get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError(f"refusing to use namespace {NS}: it is not labelled ours")
    STATE["environment"]["namespaceUid"] = existing["metadata"]["uid"]

    # The revision under test, asserted rather than assumed: the shared lab's
    # controller image carries the commit it was built from as an OCI label and
    # the harness refuses to produce evidence for a revision it cannot name.
    head = run(["git", "rev-parse", "HEAD"], record=False).stdout.strip()
    origin = run(["git", "rev-parse", "origin/main"], record=False).stdout.strip()
    STATE["environment"]["gitHead"] = head
    STATE["environment"]["originMain"] = origin
    controller = json.loads(
        run(
            K
            + [
                "-n",
                fixture.FIXTURE_NS,
                "get",
                "deploy",
                "weirkeeper",
                "-o",
                "json",
            ]
        ).stdout
    )
    pods = json.loads(
        run(
            K
            + [
                "-n",
                fixture.FIXTURE_NS,
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=control-plane",
                "-o",
                "json",
            ]
        ).stdout
    )["items"]
    running = [p for p in pods if p["status"]["phase"] == "Running"]
    STATE["environment"]["controller"] = {
        "namespace": fixture.FIXTURE_NS,
        "deploymentUid": controller["metadata"]["uid"],
        "image": controller["spec"]["template"]["spec"]["containers"][0]["image"],
        "pods": [
            {
                "name": p["metadata"]["name"],
                "uid": p["metadata"]["uid"],
                "imageID": p["status"]["containerStatuses"][0].get("imageID"),
            }
            for p in running
        ],
        "replicas": len(running),
    }
    if len(running) != 1:
        raise RuntimeError(f"expected exactly one shared controller pod, saw {len(running)}")

    apply(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "logweir-runner", "namespace": NS, "labels": dict(LABELS)},
            "automountServiceAccountToken": False,
        }
    )
    for secret in ("logweir-s3", "logweir-signing-key", "minio-root"):
        copy_secret(secret)
    for manifest in fixture.minio_manifests(NS, dict(LABELS)) + fixture.kafka_manifests(
        NS, dict(LABELS)
    ):
        apply(manifest)
    apply(fixture.mc_pod(NS, dict(LABELS)))
    kn("rollout", "status", "deploy/minio", "--timeout=180s", timeout=200)
    kn("rollout", "status", "deploy/kafka", "--timeout=240s", timeout=260)
    kn("wait", "--for=condition=Ready", "pod/d1-mc", "--timeout=180s", timeout=200)
    STATE["environment"]["kafkaPod"] = kafka_pod_name()

    if FENCED:
        # Before the first object whose status a controller must write. With
        # the fence up, the shared release cannot write here, so nothing in
        # this namespace reaches a ready condition until OUR controller runs.
        STATE["environment"]["fencedController"] = fenced.deploy(sys.modules[__name__])
        STATE["environment"]["fenceProof"] = fenced.prove(sys.modules[__name__])
        artifact("fence-proof.json", STATE["environment"]["fenceProof"])
        if not STATE["environment"]["fenceProof"]["fenced"]:
            raise RuntimeError(
                "the fence is not proven; refusing to run scenarios that assume it: "
                + json.dumps(STATE["environment"]["fenceProof"], sort_keys=True)[:800]
            )

    dest = apply(fixture.destination(NS, DEST, dict(LABELS), f"d1/{STAMP}"))
    STATE["environment"]["destinationUid"] = dest["metadata"]["uid"]
    cluster = apply(fixture.kafka_cluster(NS, "source", dict(LABELS)))
    STATE["environment"]["sourceUid"] = cluster["metadata"]["uid"]
    create_topics(["t1", "t2"])
    ready = wait_for(
        "kafkacluster",
        "source",
        lambda o: (o.get("status") or {}).get("reachable") is True,
        timeout=300,
        what="to be reachable",
    )
    STATE["environment"]["sourceClusterId"] = ready["status"].get("clusterId")
    # `BackupDestination` reports `Valid`, not `Ready`: readiness there is a
    # separate, optional write probe (`spec.readiness.writeProbe`), and this
    # destination deliberately leaves it `Disabled` so the fixture never writes
    # a marker object of its own.
    dest_ready = wait_for(
        "backupdestination",
        DEST,
        lambda o: (condition(o, "Valid") or {}).get("status") == "True",
        timeout=180,
        what="to be validated by the controller",
    )
    STATE["environment"]["destinationValid"] = condition(dest_ready, "Valid")
    STATE["environment"]["destinationCanonicalUrl"] = dest_ready["status"]["canonicalUrl"]
    STATE["environment"]["topics"] = list_topics()
    save()
    log(f"namespace {NS} ready; source clusterId={STATE['environment'].get('sourceClusterId')}")


# ---------------------------------------------------------------------------
# Shared scenario helpers
# ---------------------------------------------------------------------------


def run_name(schedule: str, slot: str, attempt: int = 0) -> str:
    """D1 §3.1's deterministic run name."""
    base = f"logweir-backup-{schedule}-{slot}"
    return base if attempt == 0 else f"{base}-r{attempt}"


def slot_of(instant: dt.datetime) -> str:
    return instant.astimezone(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")


def parse(instant: str) -> dt.datetime:
    return dt.datetime.strptime(instant.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")


def sleep_until(instant: dt.datetime, *, label: str = "") -> None:
    delay = (instant - dt.datetime.now(dt.timezone.utc)).total_seconds()
    if delay > 0:
        log(f"    bounded wait {delay:.0f}s {label}")
        time.sleep(delay)


def await_schedule_observed(name: str, *, timeout: int = 120) -> dict[str, Any]:
    """`status.policy.effectiveSince` is the controller's first observation."""
    return wait_for(
        "backupschedule",
        name,
        lambda o: ((o.get("status") or {}).get("policy") or {}).get("effectiveSince"),
        timeout=timeout,
        what="to be observed (status.policy.effectiveSince)",
    )


def require(check: bool, message: str, obj: Any = None, dumps: dict[str, Any] | None = None) -> None:
    if not check:
        raise Failure(message, obj=obj, dumps=dumps)


def excerpt(obj: dict[str, Any], *paths: str) -> dict[str, Any]:
    """The exact status excerpt an assertion rests on, for the record."""
    out: dict[str, Any] = {}
    for path in paths:
        cursor: Any = obj
        for part in path.split("."):
            if isinstance(cursor, list):
                cursor = cursor[int(part)] if part.isdigit() and int(part) < len(cursor) else None
            elif isinstance(cursor, dict):
                cursor = cursor.get(part)
            else:
                cursor = None
            if cursor is None:
                break
        out[path] = cursor
    return out


# ---------------------------------------------------------------------------
# PLAT-04.2
# ---------------------------------------------------------------------------


@scenario("L-04-1", "PLAT-04.2", "time zone evaluation, with a UTC negative control")
def l_04_1() -> dict[str, Any]:
    zone = "Asia/Kathmandu"
    offset = dt.timedelta(hours=5, minutes=45)
    target_local = (dt.datetime.now(dt.timezone.utc) + offset + dt.timedelta(minutes=3)).replace(
        second=0, microsecond=0
    )
    cron = f"{target_local.minute} {target_local.hour} * * *"
    expected_utc = (target_local - offset).replace(tzinfo=dt.timezone.utc)
    tz_sched = create(schedule_object("tz", schedule=cron, timeZone=zone))
    utc_sched = create(schedule_object("tznone", schedule=cron))
    tz_uid = tz_sched["metadata"]["uid"]
    utc_uid = utc_sched["metadata"]["uid"]
    observed = await_schedule_observed("tz")
    preview = wait_for(
        "backupschedule",
        "tz",
        lambda o: ((o.get("status") or {}).get("nextRuns") or []),
        timeout=120,
        what="to publish status.nextRuns",
    )
    first = preview["status"]["nextRuns"][0]
    require(
        parse(first["at"]) == expected_utc,
        f"nextRuns[0].at is {first['at']}, expected the UTC instant {expected_utc.isoformat()} "
        f"(local {target_local.strftime('%H:%M')} in {zone}, minus 05:45)",
        obj=preview,
    )
    require(
        str(first.get("localTime", "")).endswith("+05:45"),
        f"nextRuns[0].localTime is {first.get('localTime')!r}, which does not end in +05:45",
        obj=preview,
    )
    slot = slot_of(expected_utc)
    name = run_name("tz", slot)
    sleep_until(expected_utc + dt.timedelta(seconds=90), label="for the tz slot + 90 s")
    child = get_opt("backup", name)
    require(
        child is not None,
        f"no Backup {name} exists within 90 s of the {zone} slot",
        dumps={"schedule": get("backupschedule", "tz")},
    )
    tz_children = backups_of(tz_uid)
    require(
        len(tz_children) == 1,
        f"expected exactly one Backup for tz, saw {[c['metadata']['name'] for c in tz_children]}",
        obj=tz_children,
    )
    require(
        child["spec"].get("slot") == slot,
        f"spec.slot is {child['spec'].get('slot')!r}, expected {slot!r}",
        obj=child,
    )
    require(
        (child["spec"].get("trigger") or {}).get("kind") == "Scheduled",
        f"spec.trigger.kind is {(child['spec'].get('trigger') or {}).get('kind')!r}",
        obj=child,
    )
    utc_children = backups_of(utc_uid)
    require(
        utc_children == [],
        "negative control: the same cron WITHOUT timeZone fired at the Kathmandu instant "
        f"({[c['metadata']['name'] for c in utc_children]})",
        obj=utc_children,
    )
    return {
        "uids": {"tz": tz_uid, "tznone": utc_uid, "backup": child["metadata"]["uid"]},
        "asserted": {
            "cron": cron,
            "timeZone": zone,
            "expectedUtc": expected_utc.isoformat(),
            "nextRuns0": first,
            "policy": (observed["status"] or {}).get("policy"),
            "backup": excerpt(child, "spec.slot", "spec.trigger.kind", "spec.scheduleRef.generation"),
            "negativeControlBackups": [c["metadata"]["name"] for c in utc_children],
        },
        "dumps": dump_objects("L-04-1", {"tz": ("backupschedule", "tz"),
                                          "tznone": ("backupschedule", "tznone"),
                                          "backup": ("backup", name)}),
    }


@scenario("L-04-3", "PLAT-04.2", "invalid cron refuses, admits nothing, and recovers")
def l_04_3() -> dict[str, Any]:
    sched = create(schedule_object("badcron", schedule="*/2 * * * *"))
    uid = sched["metadata"]["uid"]
    await_schedule_observed("badcron")
    patched = patch("backupschedule", "badcron", {"spec": {"schedule": "61 * * * *"}})
    require(
        patched["spec"]["schedule"] == "61 * * * *",
        "the API server refused an unparseable cron; D1 §5.5 says the schema accepts it and "
        "the controller refuses it",
        obj=patched,
    )
    before = {b["metadata"]["name"] for b in backups_of(uid)}
    refused = wait_for(
        "backupschedule",
        "badcron",
        lambda o: (condition(o, "Ready") or {}).get("reason") == "UnparseableSchedule",
        timeout=180,
        what="Ready=False reason=UnparseableSchedule",
    )
    ready = condition(refused, "Ready")
    require(ready["status"] == "False", f"Ready is {ready}", obj=refused)
    # Two cadence periods of the */2 schedule, with the slot boundaries inside.
    log("    holding 250 s across two cadence periods with the cron unparseable")
    time.sleep(250)
    during = {b["metadata"]["name"] for b in backups_of(uid)}
    require(
        during == before,
        f"an unparseable cron admitted {sorted(during - before)}",
        obj=sorted(during - before),
    )
    patch("backupschedule", "badcron", {"spec": {"schedule": "*/2 * * * *"}})
    recovered = wait_for(
        "backupschedule",
        "badcron",
        lambda o: (condition(o, "Ready") or {}).get("status") == "True",
        timeout=180,
        what="Ready=True after the revert",
    )
    fired = wait_until(
        lambda: sorted(set(b["metadata"]["name"] for b in backups_of(uid)) - before),
        timeout=200,
        what="the next slot to fire after the revert",
    )
    child = get("backup", fired[0])
    return {
        "uids": {"badcron": uid, "firstBackupAfterRevert": child["metadata"]["uid"]},
        "asserted": {
            "refusedCondition": ready,
            "backupsWhileUnparseable": sorted(during),
            "recoveredCondition": condition(recovered, "Ready"),
            "firedAfterRevert": fired,
            "firedTrigger": excerpt(child, "spec.slot", "spec.trigger.kind"),
        },
        "dumps": dump_objects("L-04-3", {"badcron": ("backupschedule", "badcron"),
                                          "firstAfterRevert": ("backup", fired[0])}),
    }


def plan_of(backup: dict[str, Any]) -> dict[str, Any]:
    """The frozen plan ConfigMap, verified against the digest on the status."""
    execution = (backup.get("status") or {}).get("execution") or {}
    ref = (execution.get("inputsRef") or {}).get("name")
    require(bool(ref), f"{backup['metadata']['name']} has no status.execution.inputsRef", obj=backup)
    cm = get("configmap", ref)
    inputs = cm["data"]["execution-inputs.json"]
    digest = "sha256:" + hashlib.sha256(inputs.encode()).hexdigest()
    require(
        execution.get("inputsSha256") == digest,
        f"{ref}: status digest {execution.get('inputsSha256')} != recomputed {digest}",
        obj=cm,
    )
    return {
        "configMap": cm,
        "name": ref,
        "resourceVersion": cm["metadata"]["resourceVersion"],
        "uid": cm["metadata"]["uid"],
        "sha256": digest,
        "inputs": json.loads(inputs),
        "backupYaml": cm["data"].get("backup.yaml", ""),
    }


def receipt_of(backup: dict[str, Any]) -> dict[str, Any]:
    key = ((backup.get("status") or {}).get("evidence") or {}).get("receiptKey")
    require(bool(key), f"{backup['metadata']['name']} has no status.evidence.receiptKey", obj=backup)
    body = mc("cat", f"local/{fixture.BUCKET}/{key}")
    return json.loads(body)


def discovery_jobs(backup_uid: str) -> list[dict[str, Any]]:
    return [j for j in lst("jobs") if j["metadata"]["name"] == f"lwd-{backup_uid}"]


def runner_jobs(backup: dict[str, Any]) -> list[dict[str, Any]]:
    """Jobs owned by this Backup that are NOT its discovery Job."""
    uid = backup["metadata"]["uid"]
    out = []
    for job in lst("jobs"):
        owners = job["metadata"].get("ownerReferences") or []
        if any(o.get("uid") == uid for o in owners) and job["metadata"]["name"] != f"lwd-{uid}":
            out.append(job)
    return out


def dynamic_selection(**over: Any) -> dict[str, Any]:
    body = {
        "exclude": {"topics": ["skip-me"], "prefixes": ["pfx-"]},
        "incompleteDiscovery": "BackUpVisibleTopics",
    }
    body.update(over)
    return body


# ---------------------------------------------------------------------------
# PLAT-09.2
# ---------------------------------------------------------------------------


@scenario("L-09-6", "PLAT-09.2", "a named allowlist resolves without discovery")
def l_09_6() -> dict[str, Any]:
    obj = create(backup_object("named-run", topics=["t1"]))
    uid = obj["metadata"]["uid"]
    done = wait_for("backup", "named-run", terminal, timeout=300, what="to reach a terminal phase")
    require(
        done["status"]["phase"] == "Succeeded",
        f"named allowlist run ended {done['status']['phase']} "
        f"({done['status'].get('reason')})",
        obj=done,
    )
    jobs = discovery_jobs(uid)
    require(jobs == [], f"a named allowlist created a discovery Job: {jobs}", obj=jobs)
    selection = done["status"]["selection"]
    require(selection.get("mode") == "SelectedTopics", f"selection.mode is {selection}", obj=done)
    require(selection.get("coverage") == "NamedTopics", f"selection.coverage is {selection}", obj=done)
    plan = plan_of(done)
    require(plan["inputs"]["topics"] == ["t1"], f"frozen topics {plan['inputs']['topics']}", obj=plan["inputs"])
    return {
        "uids": {"backup": uid},
        "asserted": {
            "selection": selection,
            "discoveryJobs": [],
            "frozenTopics": plan["inputs"]["topics"],
            "planSha256": plan["sha256"],
        },
        "dumps": dump_objects("L-09-6", {"backup": ("backup", "named-run")}),
    }


@scenario("L-09-1", "PLAT-09.2", "dynamic resolution: exclusions, internal topics and coverage")
def l_09_1() -> dict[str, Any]:
    create_topics(["skip-me", "pfx-a"])
    # A consumer group, so that `__consumer_offsets` exists and the internal
    # exclusion has something to exclude.
    kafka_exec(
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        "t1",
        "--group",
        "d1-group",
        "--from-beginning",
        "--timeout-ms",
        "6000",
        "--max-messages",
        "1",
        check=False,
    )
    topics_before = list_topics()
    require(
        "__consumer_offsets" in topics_before,
        f"the fixture has no internal topic to exclude: {topics_before}",
        obj=topics_before,
    )
    obj = create(
        backup_object(
            "dyn-run", topics=[], allUserTopics=dynamic_selection(), deadlineSeconds=600
        )
    )
    uid = obj["metadata"]["uid"]
    resolving = wait_until(
        lambda: (
            (get("backup", "dyn-run").get("status") or {}).get("phase") == "Resolving"
            or discovery_jobs(uid)
        ),
        timeout=180,
        interval=1.0,
        what="phase=Resolving and the discovery Job",
    )
    seen_phase = (get("backup", "dyn-run").get("status") or {}).get("phase")
    jobs = discovery_jobs(uid)
    require(len(jobs) == 1, f"expected exactly one lwd-{uid} discovery Job, saw {len(jobs)}", obj=jobs)
    done = wait_for("backup", "dyn-run", terminal, timeout=420, what="to reach a terminal phase")
    require(
        done["status"]["phase"] == "Succeeded",
        "the dynamic run ended {} ({}); TopicsResolved={}".format(
            done["status"]["phase"],
            done["status"].get("reason")
            or (condition(done, "Failed") or {}).get("reason"),
            (condition(done, "TopicsResolved") or {}).get("message"),
        ),
        obj=done,
        dumps={"discoveryJob": get_opt("job", jobs[0]["metadata"]["name"]) or jobs[0],
               "discoveryPods": {"items": lst("pods", f"job-name={jobs[0]['metadata']['name']}")}},
    )
    plan = plan_of(done)
    inputs = plan["inputs"]
    # D1 §3.3 as amended: the frozen names are the single top-level `topics`.
    require(
        inputs["topics"] == ["t1", "t2"],
        f"frozen topics are {inputs['topics']}, expected ['t1', 't2']",
        obj=inputs,
    )
    sel = inputs["selection"]
    require(sel.get("coverage") == "VisibleUserTopicsOnly", f"inputs selection {sel}", obj=inputs)
    require(
        (sel.get("discovery") or {}).get("visibility") == "unknown"
        or sel.get("visibility") == "unknown",
        f"discovery visibility is not 'unknown': {sel}",
        obj=inputs,
    )
    # WHERE THE COUNTS LIVE, and why this row used to read the wrong place.
    # The frozen plan records the discovery's own accounting one level down,
    # under `selection.discovery`, as `{count, names}` pairs with a bounded name
    # sample — `crates/weirkeeper/tests/backup_selection.rs`'s
    # `discovery["internalExcluded"]["count"]` is the same shape from the other
    # side. `status.selection` carries the same two numbers flattened
    # (`internalExcludedCount`, `excludedByRuleCount`) for an operator reading
    # `kubectl get -o yaml`, and deliberately carries NO names: an unbounded
    # name list never reaches a status. This row asserted the flat spellings on
    # the plan, where neither shape has ever been written, and so failed on a
    # path while the product was right (lab-refresh-3 §8.1).
    discovery = sel.get("discovery") or {}
    internal_block = discovery.get("internalExcluded") or {}
    by_rule_block = discovery.get("excludedByRule") or {}
    internal = internal_block.get("count")
    by_rule = by_rule_block.get("count")
    require(
        isinstance(internal, int) and internal >= 1,
        f"selection.discovery.internalExcluded.count is {internal!r}; expected at least the "
        f"one internal topic the fixture made ({sel})",
        obj=inputs,
    )
    require(
        "__consumer_offsets" in (internal_block.get("names") or []),
        f"the internal exclusion does not name __consumer_offsets: {internal_block}",
        obj=inputs,
    )
    require(
        by_rule == 2 and sorted(by_rule_block.get("names") or []) == ["pfx-a", "skip-me"],
        f"selection.discovery.excludedByRule is {by_rule_block}, expected count 2 naming "
        f"skip-me and pfx-a",
        obj=inputs,
    )
    yaml_topics = re.findall(r"^\s*-\s*(\S+)\s*$", plan["backupYaml"], flags=re.M)
    require(
        [x for x in yaml_topics if x in {"t1", "t2", "skip-me", "pfx-a", "__consumer_offsets"}]
        == ["t1", "t2"],
        f"backup.yaml lists {yaml_topics}",
        obj=plan["backupYaml"],
    )
    receipt = receipt_of(done)
    require(
        receipt["source"]["topics"] == ["t1", "t2"],
        f"the receipt names {receipt['source']['topics']}",
        obj=receipt,
    )
    status_sel = done["status"]["selection"]
    require(
        status_sel.get("resolvedTopicCount") == 2,
        f"status.selection.resolvedTopicCount is {status_sel.get('resolvedTopicCount')}",
        obj=done,
    )
    # The status is the flattened view of the plan it was frozen from. A status
    # that disagreed with its own plan would be the worse defect, so the two are
    # compared rather than each being read alone.
    require(
        status_sel.get("internalExcludedCount") == internal
        and status_sel.get("excludedByRuleCount") == by_rule,
        f"status.selection says internalExcludedCount="
        f"{status_sel.get('internalExcludedCount')!r}/excludedByRuleCount="
        f"{status_sel.get('excludedByRuleCount')!r} while the frozen plan says "
        f"{internal!r}/{by_rule!r}",
        obj=done,
    )
    require(
        "names" not in status_sel and "__consumer_offsets" not in json.dumps(status_sel),
        f"an unbounded name list reached the status: {status_sel}",
        obj=done,
    )
    return {
        "uids": {"backup": uid, "discoveryJob": jobs[0]["metadata"]["uid"]},
        "asserted": {
            "observedPhase": seen_phase,
            "discoveryJobName": jobs[0]["metadata"]["name"],
            "frozenTopics": inputs["topics"],
            "inputsSelection": sel,
            "discoveryCounts": {"internalExcluded": internal_block,
                                "excludedByRule": by_rule_block},
            "statusSelection": status_sel,
            "backupYamlTopics": yaml_topics,
            "receiptTopics": receipt["source"]["topics"],
            "planSha256": plan["sha256"],
            "planResourceVersion": plan["resourceVersion"],
            "topicsInBroker": topics_before,
        },
        "dumps": dump_objects("L-09-1", {"backup": ("backup", "dyn-run"),
                                          "plan": ("configmap", plan["name"])}),
    }


@scenario("L-09-2", "PLAT-09.2", "a new topic enters the next run; run 1's snapshot is immutable")
def l_09_2() -> dict[str, Any]:
    first = STATE["scenarios"].get("L-09-1")
    require(
        first is not None and first["status"] == "pass",
        "L-09-2 builds on L-09-1's frozen run; run L-09-1 first",
    )
    run1 = get("backup", "dyn-run")
    plan1 = plan_of(run1)
    create_topics(["t3"])
    obj = create(
        backup_object("dyn-run-2", topics=[], allUserTopics=dynamic_selection(), deadlineSeconds=600)
    )
    uid = obj["metadata"]["uid"]
    done = wait_for("backup", "dyn-run-2", terminal, timeout=420, what="to reach a terminal phase")
    require(
        done["status"]["phase"] == "Succeeded",
        f"run 2 ended {done['status']['phase']} ({done['status'].get('reason')})",
        obj=done,
    )
    plan2 = plan_of(done)
    require(
        plan2["inputs"]["topics"] == ["t1", "t2", "t3"],
        f"run 2 froze {plan2['inputs']['topics']}, expected ['t1', 't2', 't3']",
        obj=plan2["inputs"],
    )
    plan1_after = plan_of(get("backup", "dyn-run"))
    require(
        plan1_after["sha256"] == plan1["sha256"]
        and plan1_after["resourceVersion"] == plan1["resourceVersion"],
        f"run 1's plan moved: sha {plan1['sha256']}->{plan1_after['sha256']}, "
        f"rv {plan1['resourceVersion']}->{plan1_after['resourceVersion']}",
        obj=plan1_after["configMap"],
    )
    receipt1 = receipt_of(get("backup", "dyn-run"))
    require(
        receipt1["source"]["topics"] == ["t1", "t2"],
        f"run 1's receipt now names {receipt1['source']['topics']}",
        obj=receipt1,
    )
    return {
        "uids": {"run2": uid},
        "asserted": {
            "run2Topics": plan2["inputs"]["topics"],
            "run1PlanSha256": plan1_after["sha256"],
            "run1PlanResourceVersion": plan1_after["resourceVersion"],
            "run1ReceiptTopics": receipt1["source"]["topics"],
            "run2Selection": done["status"]["selection"],
        },
        "dumps": dump_objects("L-09-2", {"run2": ("backup", "dyn-run-2"),
                                          "run1": ("backup", "dyn-run")}),
    }


@scenario("L-09-4", "PLAT-09.2", "an empty resolution refuses before any runner Job")
def l_09_4() -> dict[str, Any]:
    everything = [t for t in list_topics() if not t.startswith("__")]
    obj = create(
        backup_object(
            "empty-run",
            topics=[],
            allUserTopics=dynamic_selection(exclude={"topics": everything, "prefixes": []}),
            deadlineSeconds=600,
        )
    )
    uid = obj["metadata"]["uid"]
    done = wait_for("backup", "empty-run", terminal, timeout=420, what="to reach a terminal phase")
    require(done["status"]["phase"] == "Failed", f"phase is {done['status']['phase']}", obj=done)
    # WHERE THE REASON LIVES. D1 §3.4 defines the discovery terminal states as
    # `Failed=True` CONDITIONS carrying the reason, with `exitReason:
    # operational` and no `exitCode` — an `exitCode` would claim a runner ran,
    # and for `SelectionEmpty` none is ever created. `status.reason` is the
    # runner-exit projection and is deliberately absent here, so reading it
    # failed this row on a path while the controller was answering exactly what
    # the contract asks (lab-refresh-3 §8.1). `TopicsResolved=False` carries the
    # same reason, and both are asserted: the phase, the refusal and the
    # resolution verdict have to tell one story.
    failed = condition(done, "Failed") or {}
    resolved = condition(done, "TopicsResolved") or {}
    require(
        failed.get("status") == "True" and failed.get("reason") == "SelectionEmpty",
        f"the Failed condition is {failed.get('status')!r}/{failed.get('reason')!r}, "
        f"expected True/SelectionEmpty",
        obj=done,
    )
    require(
        resolved.get("status") == "False" and resolved.get("reason") == "SelectionEmpty",
        f"the TopicsResolved condition is {resolved.get('status')!r}/"
        f"{resolved.get('reason')!r}, expected False/SelectionEmpty",
        obj=done,
    )
    require(
        done["status"].get("exitReason") == "operational"
        and done["status"].get("exitCode") is None,
        f"a controller refusal is exitReason=operational with no exitCode; this one is "
        f"exitReason={done['status'].get('exitReason')!r}, "
        f"exitCode={done['status'].get('exitCode')!r}",
        obj=done,
    )
    require(
        "no runner Job is created" in (failed.get("message") or ""),
        f"the refusal does not say that no runner Job follows: {failed.get('message')!r}",
        obj=done,
    )
    jobs = discovery_jobs(uid)
    require(len(jobs) == 1, f"expected exactly the discovery Job, saw {[j['metadata']['name'] for j in jobs]}", obj=jobs)
    runners = runner_jobs(done)
    require(runners == [], f"a runner Job was created: {[j['metadata']['name'] for j in runners]}", obj=runners)
    # Not retried: the object is a manual run, so the retry rail that could
    # create `-r1` is the schedule's; this asserts no sibling appeared at all.
    time.sleep(20)
    siblings = [b["metadata"]["name"] for b in lst("backups") if b["metadata"]["name"].startswith("empty-run")]
    require(siblings == ["empty-run"], f"a retry sibling appeared: {siblings}", obj=siblings)
    return {
        "uids": {"backup": uid},
        "asserted": {
            "phase": done["status"]["phase"],
            "failedCondition": {k: failed.get(k) for k in ("status", "reason", "message")},
            "topicsResolvedCondition": {k: resolved.get(k) for k in ("status", "reason")},
            "exitReason": done["status"].get("exitReason"),
            "statusReason": done["status"].get("reason"),
            "excludedTopics": everything,
            "jobs": [j["metadata"]["name"] for j in lst("jobs") if j["metadata"]["name"].endswith(uid)],
            "runnerJobs": [],
            "siblings": siblings,
        },
        "dumps": dump_objects("L-09-4", {"backup": ("backup", "empty-run")}),
    }


@scenario("L-09-3b", "PLAT-09.2", "the source changes between discovery and freeze")
def l_09_3b() -> dict[str, Any]:
    """D1 L-09-3, second case only.

    The first case needs the harness to hold the runner Job POST, which needs
    the namespace-scoping proxy this run does not deploy; it is recorded
    `not-run` under `L-09-3a`. This is the second case verbatim: change the
    source `KafkaCluster` between discovery and freeze (delete and recreate
    with a different bootstrap) and expect `SourceChangedDuringResolution` with
    no runner Job.
    """
    attempts: list[dict[str, Any]] = []
    for attempt in range(3):
        name = f"race-{attempt}"
        cluster_name = f"source-race-{attempt}"
        apply(fixture.kafka_cluster(NS, cluster_name, dict(LABELS)))
        wait_for(
            "kafkacluster",
            cluster_name,
            lambda o: (o.get("status") or {}).get("reachable") is True,
            timeout=240,
            what="to be reachable",
        )
        obj = create(
            backup_object(
                name,
                sourceRef={"name": cluster_name},
                topics=[],
                allUserTopics=dynamic_selection(),
                deadlineSeconds=600,
            )
        )
        uid = obj["metadata"]["uid"]
        started = time.time()
        won = False
        while time.time() - started < 180:
            if discovery_jobs(uid):
                kn("delete", "kafkacluster", cluster_name, "--wait=true", timeout=120)
                changed = dict(fixture.kafka_cluster(NS, cluster_name, dict(LABELS)))
                changed["spec"]["bootstrapServers"] = [f"kafka-2.{NS}.svc.cluster.local:9092"]
                apply(changed)
                won = True
                break
            time.sleep(0.5)
        done = wait_for("backup", name, terminal, timeout=420, what="to reach a terminal phase")
        record = {
            "attempt": attempt,
            "backup": name,
            "uid": uid,
            "raceWindowEntered": won,
            "phase": done["status"]["phase"],
            "reason": done["status"].get("reason"),
            "runnerJobs": [j["metadata"]["name"] for j in runner_jobs(done)],
        }
        attempts.append(record)
        if record["reason"] == "SourceChangedDuringResolution":
            require(
                record["phase"] == "Failed",
                f"SourceChangedDuringResolution but phase is {record['phase']}",
                obj=done,
            )
            require(
                record["runnerJobs"] == [],
                f"a runner Job was created despite the source change: {record['runnerJobs']}",
                obj=done,
            )
            return {
                "uids": {"backup": uid},
                "asserted": {"attempts": attempts, "terminal": excerpt(done, "status.phase", "status.reason")},
                "dumps": dump_objects("L-09-3b", {"backup": ("backup", name)}),
            }
    raise Failure(
        "the source change never landed inside the discovery window in three attempts; "
        "this case needs the request-holding proxy to be deterministic",
        obj=attempts,
    )


# ---------------------------------------------------------------------------
# PLAT-04.2 (continued)
# ---------------------------------------------------------------------------


def wait_for_running_child(schedule_uid: str, *, timeout: int = 300,
                           exclude: set[str] | None = None) -> dict[str, Any]:
    """Catch a schedule-created run while it is still nonterminal.

    The window is the Job's lifetime — pod scheduling, image start and a short
    engine run, several seconds on this node — so the poll is deliberately
    tight rather than the 3 s the rest of the harness uses.
    """
    exclude = exclude or set()
    deadline = time.time() + timeout
    while time.time() < deadline:
        for child in backups_of(schedule_uid):
            if child["metadata"]["name"] in exclude:
                continue
            phase = (child.get("status") or {}).get("phase")
            if phase in {"Running", "Resolving"}:
                return child
        time.sleep(0.5)
    raise Failure(f"no run of {schedule_uid} was observed nonterminal within {timeout}s")


@scenario("L-04-4", "PLAT-04.2", "retry, exhaustion, and a recovered second slot")
def l_04_4() -> dict[str, Any]:
    """The retry chain, its budget, and the slot that recovers.

    THE CADENCE IS SIX MINUTES AND THE FIRST RUN OF THIS SCENARIO IS WHY. D1
    §13.2 writes `*/3 * * * *`, but the chain attempt 0 -> `-r1` -> `-r2` takes
    about four minutes with a sixty-second retry delay, and §4.5 step 5 decides
    about the LATEST due slot only: at a three-minute cadence the next slot
    comes due before the chain ends, `status.lastSlot` moves to it, and
    `disposition: Exhausted` is either never written or overwritten before any
    poll can see it. The chain itself is identical; only the room around it
    changes, and the criterion's own assertion becomes observable.
    """
    kn("delete", "backupschedule", "retry", "--ignore-not-found=true", "--wait=true")
    sched = create(
        schedule_object(
            "retry",
            schedule="*/6 * * * *",
            retry={"maxRetries": 2, "delaySeconds": 60},
            activeDeadlineSeconds=300,
        )
    )
    uid = sched["metadata"]["uid"]
    observed = await_schedule_observed("retry")
    effective_since = parse(observed["status"]["policy"]["effectiveSince"][:19] + "Z")
    # The archive this schedule writes to is the namespace's own MinIO, so
    # taking it down is a change to an object this harness created and to
    # nothing else.
    kn("scale", "deploy/minio", "--replicas=0")
    kn("rollout", "status", "deploy/minio", "--timeout=120s", timeout=140)
    log("    MinIO scaled to 0; waiting for attempt 0 of the next slot")
    STATE.setdefault("restore", {})["minio"] = "scaled to 0 by L-04-4"
    save()
    try:
        return _l_04_4_body(uid, effective_since)
    finally:
        kn("scale", "deploy/minio", "--replicas=1", check=False)
        kn("rollout", "status", "deploy/minio", "--timeout=180s", timeout=200, check=False)
        ensure_bucket()
        STATE.setdefault("restore", {})["minio"] = "restored to 1 by L-04-4"
        save()


def first_due_attempt_zero(uid: str, after: dt.datetime, *, exclude_slot: str | None = None,
                           timeout: int = 500) -> dict[str, Any]:
    """Attempt 0 of the first slot that came due AFTER `after`.

    D1 §4.7 row 17 admits a slot that came due before the schedule existed but
    is still inside `startingDeadlineSeconds`, and §4.5 steps 5-6 then consider
    the attempt chain of the LATEST due slot only, so that stale slot's retry is
    superseded (row 22) and never created. Latching onto it measured the wrong
    thing on the first run of L-04-4 and is why this filter exists.
    """
    return wait_until(
        lambda: next(
            (
                b
                for b in backups_of(uid)
                if (b["spec"].get("trigger") or {}).get("attempt") == 0
                and b["spec"].get("slot")
                and b["spec"]["slot"] != exclude_slot
                and dt.datetime.strptime(b["spec"]["slot"], "%Y%m%d-%H%M%S").replace(
                    tzinfo=dt.timezone.utc
                )
                >= after
            ),
            None,
        ),
        timeout=timeout,
        interval=2.0,
        what="attempt 0 of a slot that came due after the schedule was observed",
    )


def failed_at(backup: dict[str, Any]) -> dt.datetime:
    entry = next(c for c in backup["status"]["conditions"] if c["type"] == "Failed")
    return parse(entry["lastTransitionTime"][:19] + "Z")


def _l_04_4_body(uid: str, effective_since: dt.datetime) -> dict[str, Any]:
    """The measurements L-04-4 makes while its own MinIO is down.

    Split out so that the restore is a `finally` on the caller: an earlier run
    of this scenario failed an assertion and left the namespace's MinIO at zero
    replicas, which then invalidated the NEXT scenario. A harness that can fail
    must put the fixture back when it does.
    """
    attempt0 = first_due_attempt_zero(uid, effective_since)
    name0 = attempt0["metadata"]["name"]
    slot = attempt0["spec"]["slot"]
    failed0 = wait_for("backup", name0, terminal, timeout=420, what="attempt 0 to end")
    exit_code = failed0["status"].get("exitCode")
    if failed0["status"]["phase"] != "Failed" or exit_code != 1:
        return {
            "status": "not-run",
            "reason": "the scenario's declared precondition did not hold: attempt 0 ended "
            f"{failed0['status']['phase']} with exitCode {exit_code!r}, not Failed/1. D1 §13.2 "
            "says such a run makes the scenario invalid rather than passed.",
            "asserted": excerpt(failed0, "status.phase", "status.exitCode", "status.reason"),
            "dumps": dump_objects("L-04-4", {"attempt0": ("backup", name0)}),
        }
    name1 = run_name("retry", slot, 1)
    r1 = wait_for("backup", name1, lambda o: True, timeout=300, what="the first retry to be created")
    delay = (parse(r1["metadata"]["creationTimestamp"]) - failed_at(failed0)).total_seconds()
    require(delay >= 60, f"-r1 was created {delay:.0f}s after attempt 0 failed, not >= 60 s", obj=r1)
    trig = r1["spec"]["trigger"]
    require(trig.get("kind") == "Retry" and trig.get("attempt") == 1, f"trigger is {trig}", obj=r1)
    require(
        (trig.get("retryOf") or {}).get("name") == name0,
        f"retryOf is {trig.get('retryOf')}, expected {name0}",
        obj=r1,
    )
    r1_done = wait_for("backup", name1, terminal, timeout=420, what="-r1 to end")
    require(
        str(r1_done["status"].get("backupId", "")).endswith("-r1"),
        f"status.backupId is {r1_done['status'].get('backupId')!r}, expected it to end -r1",
        obj=r1_done,
    )
    require(r1_done["status"]["phase"] == "Failed", f"-r1 ended {r1_done['status']['phase']}", obj=r1_done)
    name2 = run_name("retry", slot, 2)
    r2_done = wait_for("backup", name2, terminal, timeout=420, what="-r2 to end")
    require(r2_done["status"]["phase"] == "Failed", f"-r2 ended {r2_done['status']['phase']}", obj=r2_done)
    exhausted = wait_for(
        "backupschedule",
        "retry",
        lambda o: (((o.get("status") or {}).get("lastSlot") or {}).get("disposition")) == "Exhausted"
        and (((o.get("status") or {}).get("lastSlot") or {}).get("slot")) == slot,
        timeout=300,
        what=f"lastSlot.disposition=Exhausted for slot {slot}",
    )
    require(
        get_opt("backup", run_name("retry", slot, 3)) is None,
        "an -r3 exists: the retry budget was not respected",
        obj=get_opt("backup", run_name("retry", slot, 3)),
    )
    first_slot_names = sorted(
        b["metadata"]["name"] for b in backups_of(uid) if b["spec"].get("slot") == slot
    )
    # THE SECOND SLOT, RESTORED BETWEEN ITS ATTEMPT 0 AND ITS RETRY. MinIO
    # stays down until this slot's attempt 0 has failed; restoring it earlier
    # would let attempt 0 succeed and the "retry then succeed" half would never
    # be exercised at all.
    log("    waiting for a second slot's attempt 0, still with MinIO down")
    second = first_due_attempt_zero(uid, effective_since, exclude_slot=slot, timeout=560)
    slot2 = second["spec"]["slot"]
    second_done = wait_for(
        "backup", second["metadata"]["name"], terminal, timeout=420,
        what="the second slot's attempt 0 to end",
    )
    require(
        second_done["status"]["phase"] == "Failed",
        f"the second slot's attempt 0 ended {second_done['status']['phase']} with MinIO down",
        obj=second_done,
    )
    kn("scale", "deploy/minio", "--replicas=1")
    kn("rollout", "status", "deploy/minio", "--timeout=180s", timeout=200)
    ensure_bucket()
    log(f"    MinIO restored inside slot {slot2}'s retry delay")
    second_r1 = run_name("retry", slot2, 1)
    r1b = wait_for("backup", second_r1, terminal, timeout=420, what="the second slot's -r1")
    require(
        r1b["status"]["phase"] == "Succeeded",
        f"the second slot's -r1 ended {r1b['status']['phase']} with MinIO restored",
        obj=r1b,
    )
    receipt = receipt_of(r1b)
    require(
        receipt["exit_code"] == 0 and receipt["source"]["topics"] == ["t1"],
        f"the recovered retry's receipt does not read back as a completed run: {receipt}",
        obj=receipt,
    )
    require(
        get_opt("backup", run_name("retry", slot2, 2)) is None,
        "an -r2 exists although -r1 succeeded",
        obj=get_opt("backup", run_name("retry", slot2, 2)),
    )
    return {
        "uids": {
            "schedule": uid,
            "attempt0": attempt0["metadata"]["uid"],
            "r1": r1["metadata"]["uid"],
            "secondSlotR1": r1b["metadata"]["uid"],
        },
        "asserted": {
            "slot": slot,
            "attempt0": excerpt(failed0, "status.phase", "status.exitCode", "status.reason"),
            "retryDelaySeconds": round(delay),
            "r1Trigger": trig,
            "r1BackupId": r1_done["status"].get("backupId"),
            "r2Phase": r2_done["status"]["phase"],
            "r3": "absent",
            "lastSlot": (exhausted["status"] or {}).get("lastSlot"),
            "firstSlotRuns": first_slot_names,
            "secondSlot": {
                "slot": slot2,
                "attempt0Phase": second_done["status"]["phase"],
                "attempt0ExitCode": second_done["status"].get("exitCode"),
                "r1Phase": r1b["status"]["phase"],
                "r1BackupId": r1b["status"].get("backupId"),
                "r1ReceiptRecords": receipt["records"],
                "r2": "absent",
            },
        },
        "dumps": dump_objects(
            "L-04-4",
            {
                "schedule": ("backupschedule", "retry"),
                "attempt0": ("backup", name0),
                "r1": ("backup", name1),
                "r2": ("backup", name2),
                "secondSlotAttempt0": ("backup", second["metadata"]["name"]),
                "secondSlotR1": ("backup", second_r1),
            },
        ),
    }


@scenario("L-04-6", "PLAT-04.2", "the Allow cap, Forbid, and a manual run that is never blocked")
def l_04_6() -> dict[str, Any]:
    """Three clauses, and only two of them are reachable on this node.

    D1 §13.2 asks for `Allow` to be driven to its cap of ten nonterminal
    schedule-created runs so that `Ready` reason `ActiveRunLimit` appears. A run
    here finishes in about four seconds, so ten are never in flight at once, and
    TWO injections were tried and are reported rather than hidden:

      * a `ResourceQuota` of `count/pods: 0` — the controller diagnosed the
        pod-less Jobs within a minute and the runs went terminal anyway; the
        highest concurrent count reached was five;
      * a source at `192.0.2.1` (RFC 5737 TEST-NET-1, routed nowhere) — the
        runner exited 1 `operational` within seconds rather than retrying
        metadata to its deadline, so the runs terminated even faster.

    Holding ten runs open needs either a controller this harness may pause (D1
    §13.1's fenced replica) or a genuinely large topic, and neither is in this
    run's reach. So the cap clause is recorded UNMET and the scenario is
    `partial`. What IS measured, on the real source and for a bounded window:
    `Allow` never exceeds ten, `Forbid` never holds two, and a manual run
    started while a scheduled run is active is admitted and completes.
    """
    for stale in ("allow", "forbid"):
        kn("delete", "backupschedule", stale, "--ignore-not-found=true", "--wait=true")
    kn("delete", "backup", "manual-during-active", "--ignore-not-found=true", "--wait=true")
    allow = create(
        schedule_object("allow", schedule="*/1 * * * *", concurrencyPolicy="Allow")
    )
    forbid = create(
        schedule_object("forbid", schedule="*/1 * * * *", concurrencyPolicy="Forbid")
    )
    allow_uid = allow["metadata"]["uid"]
    forbid_uid = forbid["metadata"]["uid"]
    await_schedule_observed("allow")
    await_schedule_observed("forbid")
    observations: list[dict[str, Any]] = []
    limit_reason_seen: dict[str, Any] | None = None
    allow_max = 0
    forbid_max = 0
    manual_uid = ""
    manual_job: list[dict[str, Any]] = []
    manual_done: dict[str, Any] = {}
    active_when_manual_started: list[str] = []
    deadline = time.time() + 330
    try:
        while time.time() < deadline:
            a = get("backupschedule", "allow")
            a_active = [b for b in backups_of(allow_uid) if not terminal(b)]
            f_active = [b for b in backups_of(forbid_uid) if not terminal(b)]
            allow_max = max(allow_max, len(a_active))
            forbid_max = max(forbid_max, len(f_active))
            ready = condition(a, "Ready") or {}
            observations.append(
                {
                    "at": now(),
                    "allowNonterminal": len(a_active),
                    "forbidNonterminal": len(f_active),
                    "allowReadyReason": ready.get("reason"),
                }
            )
            require(
                allow_max <= 10,
                f"Allow admitted {allow_max} nonterminal schedule-created runs, above the "
                "cap of 10",
                obj=[b["metadata"]["name"] for b in a_active],
            )
            require(
                forbid_max <= 1,
                f"Forbid admitted {forbid_max} nonterminal schedule-created runs",
                obj=[b["metadata"]["name"] for b in f_active],
            )
            if ready.get("reason") == "ActiveRunLimit":
                limit_reason_seen = ready
                break
            if not manual_uid and a_active:
                # A manual run WHILE a scheduled run of the same schedule is
                # nonterminal — the clause the criterion ends on.
                active_when_manual_started = [b["metadata"]["name"] for b in a_active]
                manual = create(backup_object("manual-during-active", topics=["t1"]))
                manual_uid = manual["metadata"]["uid"]
                manual_job = wait_until(
                    lambda: runner_jobs(get("backup", "manual-during-active")),
                    timeout=120,
                    interval=0.5,
                    what="the manual run's Job",
                )
                manual_done = wait_for(
                    "backup", "manual-during-active", terminal, timeout=300,
                    what="the manual run to end",
                )
            time.sleep(2)
    finally:
        for name in ("allow", "forbid"):
            run(
                KN + ["patch", "backupschedule", name, "--type", "merge",
                      "-p", json.dumps({"spec": {"suspend": True}})],
                check=False,
            )
    require(
        bool(manual_uid),
        "no scheduled run was ever observed nonterminal, so the manual-run clause could not "
        "be exercised either",
        obj=observations[-12:],
    )
    require(
        manual_done["status"]["phase"] == "Succeeded",
        f"the manual run ended {manual_done['status']['phase']} "
        f"({manual_done['status'].get('reason')}) while a scheduled run was active",
        obj=manual_done,
    )
    unmet = (
        []
        if limit_reason_seen is not None
        else [
            "the Allow cap clause: Ready reason ActiveRunLimit was NOT observed. The highest "
            f"nonterminal count reached was {allow_max} of the cap's 10, because a run on this "
            "node terminates in seconds. See this scenario's docstring for the two injections "
            "that were tried and why neither held ten runs open."
        ]
    )
    return {
        "status": "pass" if limit_reason_seen is not None else "partial",
        "uids": {"allow": allow_uid, "forbid": forbid_uid, "manual": manual_uid},
        "asserted": {
            "allowMaxNonterminal": allow_max,
            "forbidMaxNonterminal": forbid_max,
            "activeRunLimitCondition": limit_reason_seen,
            "manualJob": manual_job[0]["metadata"]["name"],
            "manualPhase": manual_done["status"]["phase"],
            "scheduledRunsActiveWhenTheManualRunStarted": active_when_manual_started,
            "observationsTail": observations[-12:],
        },
        "unmet": unmet,
        "dumps": dump_objects(
            "L-04-6",
            {
                "allow": ("backupschedule", "allow"),
                "forbid": ("backupschedule", "forbid"),
                "manual": ("backup", "manual-during-active"),
            },
        ),
    }


# ---------------------------------------------------------------------------
# PLAT-05.1
# ---------------------------------------------------------------------------


@scenario("L-05.1-1", "PLAT-05.1", "an edit during execution changes the next run, never this one")
def l_05_1_1() -> dict[str, Any]:
    second = apply(fixture.destination(NS, "dest2", dict(LABELS), f"d1/{STAMP}/edited"))
    wait_for(
        "backupdestination",
        "dest2",
        lambda o: (condition(o, "Valid") or {}).get("status") == "True",
        timeout=180,
        what="dest2 to be validated",
    )
    sched = create(schedule_object("ed", schedule="*/2 * * * *", topics=["t1"]))
    uid = sched["metadata"]["uid"]
    await_schedule_observed("ed")
    running = wait_for_running_child(uid, timeout=300)
    name = running["metadata"]["name"]
    plan_before = plan_of(wait_for("backup", name,
                                   lambda o: ((o.get("status") or {}).get("execution") or {}).get("inputsRef"),
                                   timeout=120, what="its frozen plan"))
    generation_before = get("backupschedule", "ed")["metadata"]["generation"]
    policy_before = (get("backupschedule", "ed")["status"] or {}).get("policy")
    # D1 §5.1 as amended on 2026-09-17: `destinationRef` is mutable and is the
    # spelling this fixture's archive location has, so "change the archive.url
    # prefix" is exercised by moving the schedule to a destination with a
    # different prefix. `archive.url` moves with it, as the CEL rule requires.
    edited = patch(
        "backupschedule",
        "ed",
        {
            "spec": {
                "topics": ["t1", "t2"],
                "destinationRef": {"name": "dest2"},
                "archive": {"url": "logweir-destination://dest2"},
            }
        },
    )
    generation_after = edited["metadata"]["generation"]
    require(
        generation_after > generation_before,
        f"the edit did not bump metadata.generation ({generation_before} -> {generation_after})",
        obj=edited,
    )
    during = get("backup", name)
    during_phase = (during.get("status") or {}).get("phase")
    # Recorded, not asserted: the edit is issued while the run is nonterminal,
    # but a run on this node lasts seconds and the API round trip is not
    # instantaneous. What the criterion is ABOUT — that the frozen plan and the
    # receipt do not move — is asserted below either way, and this says which
    # of the two the measurement was.
    require(
        during["spec"]["topics"] == ["t1"],
        f"the running Backup's spec.topics changed to {during['spec']['topics']}",
        obj=during,
    )
    finished = wait_for("backup", name, terminal, timeout=420, what="the first run to end")
    plan_after = plan_of(finished)
    require(
        plan_after["resourceVersion"] == plan_before["resourceVersion"]
        and plan_after["sha256"] == plan_before["sha256"],
        f"the frozen plan moved under the edit: rv {plan_before['resourceVersion']} -> "
        f"{plan_after['resourceVersion']}, sha {plan_before['sha256']} -> {plan_after['sha256']}",
        obj=plan_after["configMap"],
    )
    require(
        finished["status"]["phase"] == "Succeeded",
        f"the first run ended {finished['status']['phase']}",
        obj=finished,
    )
    receipt = receipt_of(finished)
    require(
        receipt["source"]["topics"] == ["t1"],
        f"the first run's receipt names {receipt['source']['topics']}",
        obj=receipt,
    )
    nxt = wait_until(
        lambda: next((b for b in backups_of(uid) if b["metadata"]["name"] != name), None),
        timeout=320,
        interval=2.0,
        what="the next run after the edit",
    )
    nxt = wait_for("backup", nxt["metadata"]["name"], terminal, timeout=420, what="the next run to end")
    require(
        nxt["spec"]["topics"] == ["t1", "t2"],
        f"the next run froze {nxt['spec']['topics']}",
        obj=nxt,
    )
    require(
        nxt["spec"]["archive"]["url"] == "logweir-destination://dest2",
        f"the next run's archive is {nxt['spec']['archive']['url']}",
        obj=nxt,
    )
    ref = nxt["spec"]["scheduleRef"]
    require(
        ref.get("generation") == generation_after,
        f"the next run records generation {ref.get('generation')}, the schedule is at "
        f"{generation_after}",
        obj=nxt,
    )
    require(
        ref.get("runPolicySha256") != (running["spec"].get("scheduleRef") or {}).get("runPolicySha256"),
        "the next run's runPolicySha256 equals the first run's although topics and the "
        "destination changed",
        obj=nxt,
    )
    return {
        "uids": {"schedule": uid, "run1": running["metadata"]["uid"], "run2": nxt["metadata"]["uid"]},
        "asserted": {
            "generationBefore": generation_before,
            "generationAfter": generation_after,
            "policyBefore": policy_before,
            "policyAfter": (get("backupschedule", "ed")["status"] or {}).get("policy"),
            "run1TopicsDuringEdit": during["spec"]["topics"],
            "run1PhaseWhenEditLanded": during_phase,
            "run1PlanResourceVersion": plan_after["resourceVersion"],
            "run1PlanSha256": plan_after["sha256"],
            "run1ReceiptTopics": receipt["source"]["topics"],
            "run1ScheduleRef": running["spec"].get("scheduleRef"),
            "run2ScheduleRef": ref,
            "run2Topics": nxt["spec"]["topics"],
            "run2Archive": nxt["spec"]["archive"],
        },
        "dumps": dump_objects(
            "L-05.1-1",
            {"schedule": ("backupschedule", "ed"), "run1": ("backup", name),
             "run2": ("backup", nxt["metadata"]["name"])},
        ),
    }


@scenario("L-05.1-4", "PLAT-05.1", "the real API server refuses R1, R2 and R3, and accepts a bad tz")
def l_05_1_4() -> dict[str, Any]:
    base = create(schedule_object("edits", schedule="*/2 * * * *"))
    outcomes: dict[str, Any] = {}

    r1 = run(
        KN + ["patch", "backupschedule", "edits", "--type", "merge", "-p",
              json.dumps({"spec": {"sourceRef": {"name": "other"}}})],
        check=False,
    )
    outcomes["R1"] = {"rc": r1.returncode, "stderr": r1.stderr.strip()}
    require(r1.returncode != 0, "changing sourceRef was accepted", obj=outcomes["R1"])
    require(
        "spec.sourceRef is immutable" in r1.stderr,
        f"R1's message is not the decided text: {r1.stderr.strip()}",
        obj=outcomes["R1"],
    )

    r2 = run(
        KN + ["patch", "backupschedule", "edits", "--type", "merge", "-p",
              json.dumps({"spec": {"allUserTopics": {"incompleteDiscovery": "Refuse"}}})],
        check=False,
    )
    outcomes["R2"] = {"rc": r2.returncode, "stderr": r2.stderr.strip()}
    require(r2.returncode != 0, "allUserTopics with a non-empty topics list was accepted", obj=outcomes["R2"])
    require(
        "spec.allUserTopics requires spec.topics to be empty" in r2.stderr,
        f"R2's message is not the decided text: {r2.stderr.strip()}",
        obj=outcomes["R2"],
    )

    long_name = "a" * 30
    short_name = "b" * 29
    require(len(long_name) == 30 and len(short_name) == 29, "name lengths")
    long_obj = schedule_object(long_name, schedule="*/2 * * * *", suspend=True,
                               retry={"maxRetries": 1, "delaySeconds": 60})
    r3 = run(K + ["create", "-f", "-"], data=json.dumps(long_obj), check=False)
    outcomes["R3-30"] = {"rc": r3.returncode, "stderr": r3.stderr.strip()}
    require(r3.returncode != 0, "a 30-character name with retries was accepted", obj=outcomes["R3-30"])
    require(
        "must be named in 29 characters or fewer" in r3.stderr,
        f"R3's message is not the decided text: {r3.stderr.strip()}",
        obj=outcomes["R3-30"],
    )
    short_obj = schedule_object(short_name, schedule="*/2 * * * *", suspend=True,
                                retry={"maxRetries": 1, "delaySeconds": 60})
    ok = create(short_obj)
    outcomes["R3-29"] = {"created": ok["metadata"]["name"], "uid": ok["metadata"]["uid"]}

    tz_obj = create(schedule_object("badtz", schedule="*/2 * * * *", timeZone="Mars/Olympus"))
    tz_uid = tz_obj["metadata"]["uid"]
    outcomes["badTimeZoneAcceptedBySchema"] = tz_obj["spec"]["timeZone"]
    refused = wait_for(
        "backupschedule",
        "badtz",
        lambda o: (condition(o, "Ready") or {}).get("reason") == "UnknownTimeZone",
        timeout=180,
        what="Ready=False reason=UnknownTimeZone",
    )
    outcomes["unknownTimeZoneCondition"] = condition(refused, "Ready")
    require(
        outcomes["unknownTimeZoneCondition"]["status"] == "False",
        f"Ready is {outcomes['unknownTimeZoneCondition']}",
        obj=refused,
    )
    log("    holding 250 s across two cadence periods with an unknown time zone")
    time.sleep(250)
    children = [b["metadata"]["name"] for b in backups_of(tz_uid)]
    require(children == [], f"an unknown time zone admitted {children}", obj=children)
    outcomes["badTimeZoneBackups"] = children
    unchanged = get("backupschedule", "edits")
    require(
        unchanged["spec"]["sourceRef"] == base["spec"]["sourceRef"]
        and unchanged["spec"]["topics"] == base["spec"]["topics"],
        "a refused edit changed the stored object",
        obj=unchanged,
    )
    return {
        "uids": {"edits": base["metadata"]["uid"], "badtz": tz_uid},
        "asserted": outcomes,
        "dumps": dump_objects("L-05.1-4", {"edits": ("backupschedule", "edits"),
                                            "badtz": ("backupschedule", "badtz")}),
    }


# ---------------------------------------------------------------------------
# PLAT-05.2
# ---------------------------------------------------------------------------

SCHEDULE_OWNER_KIND = "BackupSchedule"


def schedule_owner_refs(backup: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        o
        for o in backup["metadata"].get("ownerReferences") or []
        if o.get("kind") == SCHEDULE_OWNER_KIND
    ]


def force_inventory(schedule: str) -> None:
    """Clear `status.history` so the next reconcile inventories immediately.

    DECLARED HARNESS INJECTION. D1 §4.5 step 1 runs the inventory when
    `status.history.inventoriedAt` is absent or older than sixty minutes; the
    harness clears the block rather than waiting an hour. It writes a status
    field the controller owns and nothing else, and the controller's own
    decision — whether to migrate, and how — is untouched.
    """
    run(
        KN
        + [
            "patch",
            "backupschedule",
            schedule,
            "--subresource=status",
            "--type",
            "merge",
            "-p",
            json.dumps({"status": {"history": None}}),
        ]
    )


@scenario("L-05.2-1", "PLAT-05.2", "legacy ownerReferences are migrated without touching anything else")
def l_05_2_1() -> dict[str, Any]:
    """D1 L-05.2-1 with a declared substitution.

    The criterion starts "with the old controller, create `hist`". The old
    controller cannot be run here without fencing the shared release out of
    this namespace, so the harness injects the ONE thing the old controller
    contributed: the `BackupSchedule` controller ownerReference on a terminal
    run. Everything the criterion then asserts — the detach, the preserved
    foreign owner, label and annotation, the UID label, the marker annotation
    and `HistoryRetained=True reason=Retained` — is measured on the real
    controller exactly as written.
    """
    anchor_cm = apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "anchor", "namespace": NS, "labels": dict(LABELS)},
            "data": {"why": "a foreign owner the migration must not touch"},
        }
    )
    sched = create(schedule_object("hist", schedule="*/2 * * * *"))
    uid = sched["metadata"]["uid"]
    await_schedule_observed("hist")
    names: list[str] = []
    deadline = time.time() + 480
    while time.time() < deadline and len(names) < 2:
        done = [
            b["metadata"]["name"]
            for b in backups_of(uid)
            if (b.get("status") or {}).get("phase") == "Succeeded"
        ]
        names = sorted(set(done))
        if len(names) < 2:
            time.sleep(5)
    require(len(names) >= 2, f"hist produced {names}, expected two Succeeded runs", obj=names)
    names = names[:2]
    legacy_owner = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": SCHEDULE_OWNER_KIND,
        "name": "hist",
        "uid": uid,
        "controller": True,
        "blockOwnerDeletion": True,
    }
    foreign_owner = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "name": "anchor",
        "uid": anchor_cm["metadata"]["uid"],
        "controller": False,
        "blockOwnerDeletion": False,
    }
    before: dict[str, Any] = {}
    for index, name in enumerate(names):
        owners = [legacy_owner] + ([foreign_owner] if index == 0 else [])
        body: dict[str, Any] = {"metadata": {"ownerReferences": owners}}
        if index == 0:
            body["metadata"]["labels"] = {"team": "x"}
            body["metadata"]["annotations"] = {"note": "y"}
        patched = patch("backup", name, body)
        before[name] = {
            "uid": patched["metadata"]["uid"],
            "ownerReferences": patched["metadata"].get("ownerReferences"),
            "labels": patched["metadata"].get("labels"),
            "annotations": patched["metadata"].get("annotations"),
        }
    force_inventory("hist")
    migrated = wait_until(
        lambda: all(schedule_owner_refs(get("backup", n)) == [] for n in names),
        timeout=300,
        interval=3.0,
        what="both runs to be detached from the schedule",
    )
    retained = wait_for(
        "backupschedule",
        "hist",
        lambda o: (condition(o, "HistoryRetained") or {}).get("reason") == "Retained",
        timeout=300,
        what="HistoryRetained=True reason=Retained",
    )
    cond = condition(retained, "HistoryRetained")
    require(cond["status"] == "True", f"HistoryRetained is {cond}", obj=retained)
    after: dict[str, Any] = {}
    for index, name in enumerate(names):
        obj = get("backup", name)
        meta = obj["metadata"]
        after[name] = {
            "uid": meta["uid"],
            "ownerReferences": meta.get("ownerReferences"),
            "labels": meta.get("labels"),
            "annotations": meta.get("annotations"),
        }
        require(
            schedule_owner_refs(obj) == [],
            f"{name} still carries a BackupSchedule ownerReference",
            obj=obj,
        )
        require(
            meta.get("labels", {}).get("logweir.dev/schedule-uid") == uid,
            f"{name} label logweir.dev/schedule-uid is "
            f"{meta.get('labels', {}).get('logweir.dev/schedule-uid')!r}, expected {uid}",
            obj=obj,
        )
        require(
            meta.get("annotations", {}).get("logweir.dev/history-retained-from-owner") == uid,
            f"{name} annotation logweir.dev/history-retained-from-owner is "
            f"{meta.get('annotations', {}).get('logweir.dev/history-retained-from-owner')!r}",
            obj=obj,
        )
        if index == 0:
            owners = meta.get("ownerReferences") or []
            require(
                owners == [foreign_owner],
                f"the foreign ownerReference was not preserved byte for byte: {owners}",
                obj=obj,
            )
            require(
                meta["labels"].get("team") == "x",
                f"label team is {meta['labels'].get('team')!r}",
                obj=obj,
            )
            require(
                meta["annotations"].get("note") == "y",
                f"annotation note is {meta['annotations'].get('note')!r}",
                obj=obj,
            )
    return {
        "uids": {"schedule": uid, "anchor": anchor_cm["metadata"]["uid"],
                 **{n: before[n]["uid"] for n in names}},
        "asserted": {
            "runs": names,
            "injectedOwnerReference": legacy_owner,
            "before": before,
            "after": after,
            "historyRetained": cond,
            "history": (retained["status"] or {}).get("history"),
            "unverified": [
                "D1 L-05.2-1's last clause — 'proxy logs show every migration PATCH body "
                "carries metadata.resourceVersion' — is NOT verified here: it needs the "
                "namespace-scoping proxy this run does not deploy."
            ],
        },
        "dumps": dump_objects("L-05.2-1", {"schedule": ("backupschedule", "hist"),
                                            "run1": ("backup", names[0]),
                                            "run2": ("backup", names[1])}),
    }


@scenario("L-05.2-2", "PLAT-05.2", "deleting a schedule leaves its runs, plans and Jobs alive")
def l_05_2_2() -> dict[str, Any]:
    cases = {"bg": [], "fg": ["--cascade=foreground"], "orph": ["--cascade=orphan"]}
    recorded: dict[str, Any] = {}
    for name, flags in cases.items():
        sched = create(
            schedule_object(name, schedule="*/1 * * * *", concurrencyPolicy="Allow")
        )
        uid = sched["metadata"]["uid"]
        await_schedule_observed(name)
        state = wait_until(
            lambda uid=uid: (
                lambda children: (
                    children
                    if len([c for c in children if (c.get("status") or {}).get("phase") == "Succeeded"]) >= 1
                    and len([c for c in children if not terminal(c)]) >= 1
                    else None
                )
            )(backups_of(uid)),
            timeout=420,
            interval=0.5,
            what=f"{name} to hold one Succeeded and one nonterminal run at once",
        )
        succeeded = [c for c in state if (c.get("status") or {}).get("phase") == "Succeeded"]
        active = [c for c in state if not terminal(c)]
        snapshot = {
            "scheduleUid": uid,
            "deleteFlags": flags,
            "backups": {
                c["metadata"]["name"]: {
                    "uid": c["metadata"]["uid"],
                    "phase": (c.get("status") or {}).get("phase"),
                }
                for c in state
            },
            "plans": {},
            "jobs": {},
        }
        for child in state:
            execution = (child.get("status") or {}).get("execution") or {}
            ref = (execution.get("inputsRef") or {}).get("name")
            if ref:
                cm = get_opt("configmap", ref)
                if cm:
                    snapshot["plans"][ref] = cm["metadata"]["uid"]
            for job in runner_jobs(child):
                snapshot["jobs"][job["metadata"]["name"]] = job["metadata"]["uid"]
        kn("delete", "backupschedule", name, *flags, "--wait=true", timeout=180)
        snapshot["deletedAt"] = now()
        snapshot["succeededAtDeletion"] = [c["metadata"]["name"] for c in succeeded]
        snapshot["activeAtDeletion"] = [c["metadata"]["name"] for c in active]
        recorded[name] = snapshot
    log("    holding 120 s after the three deletions")
    time.sleep(120)
    for name, snapshot in recorded.items():
        for backup, info in snapshot["backups"].items():
            obj = get_opt("backup", backup)
            require(obj is not None, f"{backup} was collected with schedule {name}", obj=snapshot)
            require(
                obj["metadata"]["uid"] == info["uid"],
                f"{backup} was replaced (UID moved)",
                obj=obj,
            )
        for plan, plan_uid in snapshot["plans"].items():
            cm = get_opt("configmap", plan)
            require(cm is not None, f"plan {plan} was collected with schedule {name}", obj=snapshot)
            require(cm["metadata"]["uid"] == plan_uid, f"plan {plan} was replaced", obj=cm)
        for job, job_uid in snapshot["jobs"].items():
            obj = get_opt("job", job)
            require(obj is not None, f"Job {job} was collected with schedule {name}", obj=snapshot)
            require(obj["metadata"]["uid"] == job_uid, f"Job {job} was replaced", obj=obj)
        for backup in snapshot["activeAtDeletion"]:
            done = wait_for("backup", backup, terminal, timeout=420,
                            what="the run that was active at deletion to finish")
            require(
                done["status"]["phase"] == "Succeeded",
                f"{backup} ended {done['status']['phase']} ({done['status'].get('reason')}) "
                "after its schedule was deleted",
                obj=done,
            )
            receipt = receipt_of(done)
            require(
                receipt["exit_code"] == 0 and receipt["backup_id"],
                f"{backup}'s receipt does not verify as a completed run: {receipt}",
                obj=receipt,
            )
            snapshot.setdefault("receipts", {})[backup] = {
                "runId": receipt["run_id"],
                "records": receipt["records"],
            }
        after = [
            b["metadata"]["name"]
            for b in lst("backups")
            if ((b["spec"].get("scheduleRef") or {}).get("uid")) == snapshot["scheduleUid"]
        ]
        new = sorted(set(after) - set(snapshot["backups"]))
        require(new == [], f"a new run appeared for deleted schedule {name}: {new}", obj=new)
        snapshot["runsAfter"] = sorted(after)
    return {
        "uids": {name: s["scheduleUid"] for name, s in recorded.items()},
        "asserted": recorded,
        "notRunSubCase": (
            "D1 L-05.2-2's 'unfrozen case' — the proxy answering a plan ConfigMap POST 503 "
            "until the schedule is deleted, expecting Failed/ScheduleNotFound — is NOT run: "
            "it is a declared proxy injection and this run deploys no proxy."
        ),
    }


@scenario("L-05.2-4", "PLAT-05.2", "a same-name schedule is a different schedule")
def l_05_2_4() -> dict[str, Any]:
    first = create(schedule_object("recr", schedule="*/2 * * * *"))
    old_uid = first["metadata"]["uid"]
    await_schedule_observed("recr")
    child = wait_until(
        lambda: next(iter(backups_of(old_uid)), None),
        timeout=320,
        interval=0.5,
        what="the first run of the original schedule",
    )
    old_runs = {c["metadata"]["name"]: c["metadata"]["uid"] for c in backups_of(old_uid)}
    child_phase = (child.get("status") or {}).get("phase")
    slot = child["spec"]["slot"]
    kn("delete", "backupschedule", "recr", "--wait=true", timeout=120)
    second = create(schedule_object("recr", schedule="*/2 * * * *"))
    new_uid = second["metadata"]["uid"]
    require(new_uid != old_uid, "the recreated schedule kept the old UID", obj=second)
    await_schedule_observed("recr")
    # The old run's slot is still the latest due slot for a short while; its
    # deterministic name is held by an object of the OTHER schedule UID.
    observed = wait_until(
        lambda: (
            lambda o: o
            if ((o.get("status") or {}).get("lastSlot") or {}).get("slot") is not None
            else None
        )(get("backupschedule", "recr")),
        timeout=240,
        interval=2.0,
        what="the recreated schedule to decide a slot",
    )
    last_slot = (observed["status"] or {}).get("lastSlot") or {}
    name_unavailable = None
    deadline = time.time() + 180
    while time.time() < deadline:
        current = get("backupschedule", "recr")
        candidate = ((current.get("status") or {}).get("lastSlot") or {})
        if candidate.get("slot") == slot:
            name_unavailable = candidate
            break
        if get_opt("backup", run_name("recr", slot)) and (
            get("backup", run_name("recr", slot))["metadata"]["uid"] == old_runs.get(run_name("recr", slot))
        ):
            pass
        time.sleep(2)
    selected = lst("backups", f"logweir.dev/schedule-uid={new_uid}")
    selected_names = sorted(b["metadata"]["name"] for b in selected)
    require(
        set(selected_names).isdisjoint(old_runs),
        f"the new UID's label selector returned old runs: {sorted(set(selected_names) & set(old_runs))}",
        obj=selected_names,
    )
    history = wait_until(
        lambda: ((get("backupschedule", "recr").get("status") or {}).get("history")),
        timeout=240,
        interval=3.0,
        what="the recreated schedule's history block",
    )
    require(
        history.get("runCount", 0) <= len(selected_names),
        f"history.runCount is {history.get('runCount')} but only {len(selected_names)} runs "
        "carry the new UID",
        obj=history,
    )
    new_active = ((get("backupschedule", "recr").get("status") or {}).get("activeRuns")) or []
    require(
        all(entry.get("name") not in old_runs for entry in new_active),
        f"an old run appears in the recreated schedule's activeRuns: {new_active}",
        obj=new_active,
    )
    detail: dict[str, Any] = {
        "oldUid": old_uid,
        "newUid": new_uid,
        "oldRuns": old_runs,
        "oldRunPhaseAtDeletion": child_phase,
        "oldSlot": slot,
        "lastSlotAtFirstDecision": last_slot,
        "nameUnavailableDisposition": name_unavailable,
        "runsSelectedByNewUid": selected_names,
        "history": history,
        "activeRuns": new_active,
    }
    if name_unavailable is not None:
        require(
            name_unavailable.get("disposition") == "NameUnavailable",
            f"the recreated schedule decided {name_unavailable} for the old run's slot, "
            "expected disposition=NameUnavailable",
            obj=name_unavailable,
        )
        require(
            get("backup", run_name("recr", slot))["metadata"]["uid"] == old_runs[run_name("recr", slot)],
            "a new Backup replaced the old run's name",
        )
    else:
        detail["note"] = (
            "the recreated schedule never evaluated the old run's slot as its latest due slot "
            "(the slot had already rolled over), so the NameUnavailable clause was not "
            "exercised; every other clause of L-05.2-4 was."
        )
    return {
        "uids": {"old": old_uid, "new": new_uid},
        "asserted": detail,
        "status": "pass" if name_unavailable is not None else "partial",
        "dumps": dump_objects("L-05.2-4", {"schedule": ("backupschedule", "recr")}),
    }


@scenario("L-05.2-5a", "PLAT-05.2", "record the resourceVersions of three unrelated objects")
def l_05_2_5a() -> dict[str, Any]:
    manual = create(backup_object("unrelated-manual", topics=["t1"]))
    manual = wait_for("backup", "unrelated-manual", terminal, timeout=420, what="to finish")
    cm = apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "unrelated", "namespace": NS, "labels": dict(LABELS)},
            "data": {"why": "an object no D1 code path may touch"},
        }
    )
    other = None
    for backup in lst("backups"):
        ref = backup["spec"].get("scheduleRef") or {}
        if ref.get("name") == "tz":
            other = backup
            break
    require(
        other is not None,
        "no Backup of another schedule was found to record; run L-04-1 before L-05.2-5a",
    )
    baseline = {
        "manualBackup": {
            "name": manual["metadata"]["name"],
            "uid": manual["metadata"]["uid"],
            "resourceVersion": manual["metadata"]["resourceVersion"],
        },
        "otherScheduleBackup": {
            "name": other["metadata"]["name"],
            "uid": other["metadata"]["uid"],
            "resourceVersion": other["metadata"]["resourceVersion"],
        },
        "configMap": {
            "name": cm["metadata"]["name"],
            "uid": cm["metadata"]["uid"],
            "resourceVersion": cm["metadata"]["resourceVersion"],
        },
        "recordedAt": now(),
    }
    STATE["unrelatedBaseline"] = baseline
    save()
    return {"asserted": baseline}


@scenario("L-05.2-5", "PLAT-05.2", "unrelated resources are untouched by migration and deletion")
def l_05_2_5() -> dict[str, Any]:
    baseline = STATE.get("unrelatedBaseline")
    require(
        baseline is not None,
        "no baseline: run L-05.2-5a before the migration and the deletions, then L-05.2-5 after",
    )
    observed: dict[str, Any] = {}
    for label, kind in (
        ("manualBackup", "backup"),
        ("otherScheduleBackup", "backup"),
        ("configMap", "configmap"),
    ):
        recorded = baseline[label]
        obj = get_opt(kind, recorded["name"])
        require(obj is not None, f"{label} {recorded['name']} no longer exists", obj=recorded)
        observed[label] = {
            "uid": obj["metadata"]["uid"],
            "resourceVersion": obj["metadata"]["resourceVersion"],
        }
        require(
            obj["metadata"]["uid"] == recorded["uid"],
            f"{label} was replaced: UID {recorded['uid']} -> {obj['metadata']['uid']}",
            obj=obj,
        )
        require(
            obj["metadata"]["resourceVersion"] == recorded["resourceVersion"],
            f"{label} {recorded['name']} moved: resourceVersion "
            f"{recorded['resourceVersion']} -> {obj['metadata']['resourceVersion']}",
            obj=obj,
        )
    return {"asserted": {"baseline": baseline, "observed": observed}}


@scenario("NEG-1", "harness", "negative control: an assertion this harness must fail")
def neg_1() -> dict[str, Any]:
    """A guard without a mutant is not a guard.

    This scenario asserts the OPPOSITE of L-09-6's landed behaviour — that a
    named allowlist dispatches a topic-discovery Job — against the same live
    objects. It must be recorded `fail`. If it is ever recorded `pass`, the
    harness is not measuring the cluster and every other verdict in this file
    is worthless.
    """
    obj = create(backup_object("neg-control", topics=["t1"]))
    uid = obj["metadata"]["uid"]
    done = wait_for("backup", "neg-control", terminal, timeout=300, what="to finish")
    jobs = discovery_jobs(uid)
    require(
        jobs != [],
        "DELIBERATELY FALSE ASSERTION: a named allowlist created no discovery Job, which is "
        "the landed behaviour L-09-6 asserts. This scenario failing is the expected result.",
        obj={"backup": done, "discoveryJobs": jobs},
    )
    return {"asserted": {"unexpected": "the false assertion held"}}


# ---------------------------------------------------------------------------
# PLAT-04.2 acceptance evidence beyond the numbered scenarios
# ---------------------------------------------------------------------------

API_PORT = int(os.environ.get("LOGWEIR_D1_API_PORT", "18484"))
API_DIR = pathlib.Path(os.environ.get("LOGWEIR_D1_API_DIR", "/tmp/logweir-d1w8-api"))


@scenario("L-04-preview", "PLAT-04.2", "the cadence preview API answers from the same module")
def l_04_preview() -> dict[str, Any]:
    """`GET /api/v1/cadence-previews` through a real `logweir-api`.

    D1 §4.4 puts the draft preview in Rust and nowhere else, and the acceptance
    this task owns is that a user can PREDICT the next runs. That claim has two
    halves: the saved schedule's `status.nextRuns` (L-04-1) and the draft
    preview the form reads before anything is saved. This runs the real binary
    in localAdmin mode against docker-desktop and compares the two.
    """
    binary = ROOT / "target/debug/logweir-api"
    require(binary.exists(), f"{binary} has not been built; run `cargo build -p logweir-api`")
    API_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    cursor = API_DIR / "cursor.key"
    if not cursor.exists():
        cursor.write_bytes(os.urandom(48))
        cursor.chmod(0o600)
    config = API_DIR / "config.yaml"
    config.write_text(
        "mode: localAdmin\n"
        f'listen: "127.0.0.1:{API_PORT}"\n'
        f'publicOrigin: "http://127.0.0.1:{API_PORT}"\n'
        f"uiDirectory: {ROOT / 'ui'}\n"
        "localAdmin:\n"
        "  subject: d1w8\n"
        "  displayName: D1 W8 acceptance\n"
        f"namespaces: [{NS}]\n"
        "kubernetes:\n"
        "  source: kubeconfig\n"
        "  context: docker-desktop\n"
        f"cursorKeyFile: {cursor}\n"
    )
    log_path = API_DIR / "api.log"
    handle = subprocess.Popen(  # noqa: S603 - a repository binary with a literal argv
        [str(binary), "--config", str(config)],
        stdout=log_path.open("wb"),
        stderr=subprocess.STDOUT,
    )
    try:
        base = f"http://127.0.0.1:{API_PORT}"
        wait_until(
            lambda: run(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
                         f"{base}/healthz"], check=False, timeout=10, record=False).stdout == "200",
            timeout=60,
            interval=1.0,
            what="logweir-api to answer /healthz",
        )
        def preview(query: str) -> tuple[int, Any]:
            result = run(
                ["curl", "-sS", "-w", "\n%{http_code}", f"{base}/api/v1/cadence-previews?{query}"],
                timeout=30,
            )
            body, _, code = result.stdout.rpartition("\n")
            try:
                return int(code), json.loads(body)
            except json.JSONDecodeError:
                return int(code), body

        zone = "Asia/Kathmandu"
        code, ok = preview(f"schedule=7%209%20*%20*%20*&timeZone={zone}&count=3")
        require(code == 200, f"the preview answered {code}: {ok}", obj=ok)
        require(ok["timeZone"] == zone, f"timeZone is {ok.get('timeZone')!r}", obj=ok)
        require(bool(ok.get("tzdb")), "the preview did not name its tz database", obj=ok)
        require(len(ok["runs"]) == 3, f"asked for 3 runs, got {len(ok['runs'])}", obj=ok)
        for entry in ok["runs"]:
            require(
                str(entry.get("localTime", "")).endswith("+05:45"),
                f"a preview run's localTime is {entry.get('localTime')!r}",
                obj=ok,
            )
        # The saved schedule and the draft preview must agree: `tz` from L-04-1
        # is still in the namespace with the same cron and zone.
        saved = get_opt("backupschedule", "tz")
        agreement: dict[str, Any] = {}
        if saved is not None:
            cron = saved["spec"]["schedule"]
            encoded = cron.replace(" ", "%20").replace("*", "*")
            code2, mirror = preview(f"schedule={encoded}&timeZone={zone}&count=5")
            require(code2 == 200, f"the preview refused the saved cron: {mirror}", obj=mirror)
            agreement = {
                "savedCron": cron,
                "savedTimeZone": saved["spec"].get("timeZone"),
                "previewSchedule": mirror["schedule"],
                "previewTzdb": mirror["tzdb"],
                "previewRuns": mirror["runs"][:2],
                "savedNextRuns": ((saved.get("status") or {}).get("nextRuns") or [])[:2],
            }
            require(
                mirror["schedule"] == cron,
                f"the preview canonicalised {cron!r} to {mirror['schedule']!r}",
                obj=mirror,
            )
        bad_cron_code, bad_cron = preview("schedule=61%20*%20*%20*%20*")
        require(
            bad_cron_code == 422,
            f"an unparseable cron answered {bad_cron_code}, expected 422 validation_failed",
            obj=bad_cron,
        )
        bad_tz_code, bad_tz = preview("schedule=*%2F5%20*%20*%20*%20*&timeZone=Mars%2FOlympus")
        require(
            bad_tz_code == 422,
            f"an unknown time zone answered {bad_tz_code}, expected 422 validation_failed",
            obj=bad_tz,
        )
        return {
            "asserted": {
                "route": "GET /api/v1/cadence-previews",
                "mode": "localAdmin",
                "listen": f"127.0.0.1:{API_PORT}",
                "ok": ok,
                "agreementWithSavedSchedule": agreement,
                "unparseableCron": {"code": bad_cron_code, "body": bad_cron},
                "unknownTimeZone": {"code": bad_tz_code, "body": bad_tz},
            }
        }
    finally:
        handle.terminate()
        try:
            handle.wait(timeout=30)
        except subprocess.TimeoutExpired:
            handle.kill()
        artifact("logs/logweir-api.log", log_path.read_text(errors="replace"))


@scenario("L-04-cap", "PLAT-04.2", "the missed-slot walk is capped, and a backlog admits nothing")
def l_04_cap() -> dict[str, Any]:
    """`status.missedSlots.countCapped` — the backlog bound, measured.

    DECLARED HARNESS INJECTION, and the reason it is one: the enumeration D1
    §4.5 step 5 caps at 1000 only runs over slots between
    `missedSlots.lastEvaluatedSlot` and the latest due slot, and a schedule
    created now has no such gap. Rather than keep a controller down for the
    seventeen hours a one-minute cadence would need, the harness writes the
    field the gap is measured from — and nothing else. Every number below is
    then the controller's own.

    This is NOT a substitute for L-04-2, which is recorded `not-run`: it proves
    the cap and the no-admission rule, not the controller-restart path.
    """
    sched = create(
        schedule_object("capped", schedule="* * * * *", catchUpPolicy="Latest",
                        startingDeadlineSeconds=60)
    )
    uid = sched["metadata"]["uid"]
    observed = await_schedule_observed("capped")
    effective_since = observed["status"]["policy"]["effectiveSince"]
    baseline = {b["metadata"]["name"] for b in backups_of(uid)}
    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
    injected = slot_of(past)
    run(
        KN
        + [
            "patch",
            "backupschedule",
            "capped",
            "--subresource=status",
            "--type",
            "merge",
            "-p",
            # `count` and `countCapped` are required by the schema, so a merge
            # that created `missedSlots` without them would be refused by the
            # API server. They are written at their identity values — zero and
            # false — so that everything the controller reports afterwards is
            # something it computed, not something this patch seeded.
            json.dumps(
                {
                    "status": {
                        "missedSlots": {
                            "lastEvaluatedSlot": injected,
                            "count": 0,
                            "countCapped": False,
                        }
                    }
                }
            ),
        ]
    )
    capped = wait_for(
        "backupschedule",
        "capped",
        lambda o: (((o.get("status") or {}).get("missedSlots") or {}).get("countCapped")) is True,
        timeout=300,
        what="status.missedSlots.countCapped to become true",
    )
    missed = capped["status"]["missedSlots"]
    require(
        missed.get("count", 0) >= 1000,
        f"countCapped is true but count is {missed.get('count')}",
        obj=capped,
    )
    require(
        len(missed.get("recent") or []) <= 10,
        f"missedSlots.recent holds {len(missed.get('recent') or [])} entries, bound is 10",
        obj=capped,
    )
    log("    holding 150 s: a two-day backlog under catchUpPolicy Latest must admit nothing extra")
    time.sleep(150)
    after = {b["metadata"]["name"] for b in backups_of(uid)}
    new = sorted(after - baseline)
    require(
        len(new) <= 3,
        f"a two-day backlog admitted {len(new)} runs ({new}); the bound is one admission per "
        "reconcile for the latest due slot only",
        obj=new,
    )
    catch_up = [
        n for n in new if (get("backup", n)["spec"].get("trigger") or {}).get("kind") == "CatchUp"
    ]
    require(
        catch_up == [],
        f"slots older than status.policy.effectiveSince ({effective_since}) were caught up: "
        f"{catch_up}; D1 §4.7 row 19 skips them with reason BeforeRevision",
        obj=catch_up,
    )
    final = get("backupschedule", "capped")
    return {
        "uids": {"schedule": uid},
        "asserted": {
            "injectedLastEvaluatedSlot": injected,
            "effectiveSince": effective_since,
            "missedSlots": final["status"]["missedSlots"],
            "runsAdmittedAfterInjection": new,
            "catchUpRuns": catch_up,
            "lastSlot": (final["status"] or {}).get("lastSlot"),
        },
        "dumps": dump_objects("L-04-cap", {"schedule": ("backupschedule", "capped")}),
    }


# ---------------------------------------------------------------------------
# Report and cleanup
# ---------------------------------------------------------------------------

TASK_SCENARIOS = {
    "PLAT-04.2": ["L-04-1", "L-04-2", "L-04-3", "L-04-4", "L-04-5", "L-04-6"],
    "PLAT-05.1": ["L-05.1-1", "L-05.1-2", "L-05.1-3", "L-05.1-4", "L-05.1-5"],
    "PLAT-05.2": [
        "L-05.2-1",
        "L-05.2-2",
        "L-05.2-3",
        "L-05.2-4",
        "L-05.2-5",
        "L-05.2-6",
        "L-05.2-cap",
    ],
    "PLAT-09.2": ["L-09-1", "L-09-2", "L-09-3a", "L-09-3b", "L-09-4", "L-09-5", "L-09-6"],
}

# Evidence the brief names for PLAT-04.2's acceptance ("users can predict the
# next runs"; "policy never creates an unbounded backlog") that D1 §13.2 does
# not give an L- number. Counted separately so it can never flatter a
# scenario table.
# `L-04-2b`, `L-05.2-1rv` and `L-05.2-2u` are here and NOT in `TASK_SCENARIOS`
# on purpose. The first measures the same decision as L-04-2's catch-up half
# without a real outage, so it must never stand in for it; the other two are
# clauses INSIDE numbered rows that already have their own line, so counting
# them again would inflate PLAT-05.2's table.
ACCEPTANCE_EVIDENCE = {
    "PLAT-04.2": ["L-04-preview", "L-04-cap", "L-04-2b"],
    "PLAT-05.2": ["L-05.2-1rv", "L-05.2-2u"],
}

# Why a scenario could not be measured in THIS environment. Each of these is a
# missing capability, named exactly, not a judgement that the behaviour is
# absent: none of them was attempted and none of them is claimed either way.
FENCED_CONTROLLER = (
    "needs a controller this harness can stop, restart or run two of. The only controller "
    "reconciling this namespace is the shared lab release, which two other live waves "
    "(d2w14, d3w14) depend on concurrently; D1 §13.1's fenced replica needs a "
    "cluster-scoped ValidatingAdmissionPolicy (the cluster lock) and was not deployed."
)
PROXY = (
    "needs D1 §13.1's namespace-scoping API proxy to hold or fault one identified "
    "controller request. The proxy only sits in front of a controller the harness owns, so "
    "it is unavailable for the same reason as the fenced controller."
)
OLD_IMAGE = (
    "needs the main@4956785 controller to write the pre-upgrade objects and then be swapped "
    "out. That is a controller swap on the shared release. " + FENCED_CONTROLLER
)

NOT_RUN_REASONS = {
    "L-04-2": ("PLAT-04.2", "long downtime with catch-up None and Latest",
               "scales the controller to 0 for seven minutes. " + FENCED_CONTROLLER),
    "L-04-5": ("PLAT-04.2", "duplicate reconciliation across two replicas",
               "needs two fenced controller replicas. " + FENCED_CONTROLLER),
    "L-05.1-2": ("PLAT-05.1", "concurrent edit and fire", PROXY),
    "L-05.1-3": ("PLAT-05.1", "conversion of pre-upgrade objects", OLD_IMAGE),
    "L-05.1-5": ("PLAT-05.1", "rollback to the previous controller", OLD_IMAGE),
    "L-05.2-3": ("PLAT-05.2", "migration interrupted and resumed",
                 "needs both a controller the harness can scale to 0 mid-migration and a "
                 "proxy that answers one migration PATCH 409. " + FENCED_CONTROLLER),
    "L-05.2-6": ("PLAT-05.2", "read cost over ten minutes of steady state",
                 "counts the controller's own LIST and GET requests, which only the proxy "
                 "sees. " + PROXY),
    "L-05.2-cap": ("PLAT-05.2", "the capped ownership walk above 10 000 Backups",
                   "needs more than 10 000 Backup objects in one namespace "
                   "(MAX_INVENTORY_PAGES 20 x INVENTORY_PAGE_SIZE 500). Every one of them "
                   "would be reconciled by the SHARED controller that two other live waves "
                   "are using, so seeding it here would be a denial of service against them. "
                   + FENCED_CONTROLLER),
    "L-09-3a": ("PLAT-09.2", "a topic deleted between freeze and execution",
                "holds the runner Job POST. " + PROXY),
    "L-09-3b": ("PLAT-09.2", "the source changes between discovery and freeze",
                "NOT blocked any more. It was blocked by the defect L-09-1 measured — the "
                "discovery Job carried the compile-time image pin instead of the "
                "LOGWEIR_RUNNER_IMAGE the process was given, no node held that pin, and "
                "`SourceChangedDuringResolution` is decided AFTER discovery succeeds, so the "
                "case could not be reached. That defect (D1-DISCOVERY-IMAGE) is closed: "
                "lab-refresh-3 §8.1 shows the discovery Job naming the configured image and "
                "succeeding. The row is implemented and runnable; a run that did not take it "
                "records it here as unrun for budget, never as blocked."),
    "L-09-5": ("PLAT-09.2", "an ACL-limited principal",
               "needs a Kafka with StandardAuthorizer and a SCRAM principal without Describe "
               "on one topic. KRaft SCRAM credentials are bootstrapped at storage-format "
               "time, which the apache/kafka entrypoint does not do from environment alone; "
               "building that broker was out of this run's budget and is PLAT-07's ground."),
}


# The eight environment-limited rows, registered only when the fence is up:
# without it they stay `not-run` with the reason above, which is the honest
# record and the one this import must not quietly overwrite.
if FENCED:
    from fence import rows as _fence_rows  # noqa: E402

    _fence_rows.register(sys.modules[__name__])


# What a row that did NOT run is blocked by once the fence EXISTS. The reasons
# above were written for a wave that had no fenced controller and no proxy, and
# every clause in them is false in a run that deployed both — `results.json` is
# what a tracker update is written from, so a stale reason there is a false
# blocker in the record (review R-5).
FENCED_NOT_RUN_REASONS = {
    "L-05.2-cap": (
        "NOT blocked by the fence any more — the fence makes it reachable, and seeding it "
        "here is no longer a denial of service against anyone, because this namespace has "
        "its own controller. It was not run for BUDGET: >10 000 Backup objects "
        "(MAX_INVENTORY_PAGES 20 x INVENTORY_PAGE_SIZE 500) at this run's measured seeding "
        "rate (500 created and made terminal in 32.7 s) is about 15 minutes of seeding plus "
        "the walk, on a host with ~22 GB free, and it did not fit this run's window. It is "
        "the one row the fence unblocks that this run did not take."
    ),
    "L-09-3a": (
        "NOT blocked by the fence or the proxy any more — the fence run held a migration "
        "PATCH and injected a 503 through that same proxy — and no longer blocked by the "
        "defect L-09-1 measured either: D1-DISCOVERY-IMAGE is closed (lab-refresh-3 §8.1), "
        "so a dynamic run now reaches the freeze step. It is a PLAT-09.2 row outside the "
        "fence worker's brief and was not attempted."
    ),
}


# A reason is stale if it asserts the absence of something this run built.
STALE_WHEN_FENCED = ("was not deployed", "is unavailable", "was not deployed.")

FENCE_EXISTS_PREFIX = (
    "THE FENCE EXISTS IN THIS RUN — a fenced controller was deployed behind the scoping "
    "proxy and both were used, so any claim below that the replica was not deployed or the "
    "proxy unavailable is from the earlier unfenced wave and is FALSE here. This row was "
    "simply not attempted in this namespace. The earlier wave's text is kept verbatim after "
    "the marker so nothing is lost: "
)


def register_not_run() -> None:
    """Record every row that did not run, with a reason true for THIS run.

    NEVER RECORDS A STALE REASON. `NOT_RUN_REASONS` was written for a wave with
    no fenced controller and no proxy, and it asserts that the replica "was not
    deployed" and the proxy "is unavailable". In a run that deployed both, those
    clauses are false — and `results.json` is what a tracker update is written
    from, so a false blocker there becomes a false blocker in the tracker
    (review R-5). A specific true reason is used where one is known; otherwise
    the stale text is marked as stale rather than deleted, because throwing the
    earlier wave's reasoning away would lose information too.
    """
    for sid, (task, title, reason) in NOT_RUN_REASONS.items():
        if sid in STATE["scenarios"]:
            continue
        if FENCED:
            if sid in FENCED_NOT_RUN_REASONS:
                reason = FENCED_NOT_RUN_REASONS[sid]
            elif any(phrase in reason for phrase in STALE_WHEN_FENCED):
                reason = FENCE_EXISTS_PREFIX + reason
        not_run(sid, reason, task=task, title=title)


def report() -> None:
    register_not_run()
    summary: dict[str, Any] = {}
    for task, ids in TASK_SCENARIOS.items():
        counts = {"pass": 0, "partial": 0, "fail": 0, "error": 0, "not-run": 0, "missing": 0}
        rows = []
        for sid in ids:
            entry = STATE["scenarios"].get(sid)
            status = entry["status"] if entry else "missing"
            counts[status] = counts.get(status, 0) + 1
            rows.append({"id": sid, "status": status, "reason": (entry or {}).get("reason")})
        summary[task] = {"counts": counts, "scenarios": rows}
        summary[task]["satisfiedInFull"] = counts["pass"] == len(ids)
        summary[task]["unmet"] = [
            row["id"] for row in rows if row["status"] != "pass"
        ]
    evidence: dict[str, Any] = {}
    for task, ids in ACCEPTANCE_EVIDENCE.items():
        evidence[task] = [
            {"id": sid, "status": (STATE["scenarios"].get(sid) or {}).get("status", "missing")}
            for sid in ids
        ]
    STATE["acceptanceEvidence"] = evidence
    control = STATE["scenarios"].get("NEG-1")
    STATE["negativeControl"] = {
        "id": "NEG-1",
        "expected": "fail",
        "observed": (control or {}).get("status", "missing"),
        "harnessCanFail": (control or {}).get("status") == "fail",
    }
    STATE["summary"] = summary
    STATE["reportedAt"] = now()
    save()
    for task, value in summary.items():
        log(f"{task}: {value['counts']} full={value['satisfiedInFull']}")


def stored_credentials() -> dict[str, str]:
    """The base64 values the sweep searches for, read while they still exist.

    Called BEFORE the namespace is deleted. A sweep that reads the Secrets
    afterwards finds nothing to search for and passes vacuously, which is the
    one way a credential check can be worse than no check at all.
    """
    stored: dict[str, str] = {}
    for name in ("logweir-s3", "minio-root", "logweir-signing-key"):
        obj = get_opt("secret", name)
        if obj is None:
            continue
        for key, value in (obj.get("data") or {}).items():
            stored[f"{name}/{key}"] = value
    return stored


def sweep_for_credentials(stored: dict[str, str]) -> dict[str, Any]:
    """Prove no artifact carries a credential before the evidence is published."""
    findings: list[str] = []
    if not stored:
        findings.append(
            "NO SECRET VALUE WAS AVAILABLE TO SEARCH FOR — this sweep proves nothing"
        )
    for path in sorted(OUT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(errors="replace")
        for ref, value in stored.items():
            plain = base64.b64decode(value).decode(errors="replace")
            if value in text or (len(plain) >= 6 and plain in text):
                findings.append(f"{path}: {ref}")
        if "BEGIN PRIVATE KEY" in text or "BEGIN EC PRIVATE KEY" in text:
            findings.append(f"{path}: PEM private key block")
    return {
        "checkedFiles": sum(1 for p in OUT.rglob("*") if p.is_file()),
        "searchedFor": sorted(stored),
        "findings": findings,
    }


def cleanup() -> None:
    """Delete only what this run created, after an owner-label and UID check."""
    proof: dict[str, Any] = {"namespace": NS, "checkedAt": now()}
    credentials = stored_credentials()
    namespace = get_opt("namespace", NS, namespace=None)
    if namespace is None:
        proof["state"] = "already absent"
    else:
        labels = namespace["metadata"].get("labels", {})
        uid = namespace["metadata"]["uid"]
        proof["observedLabels"] = labels
        proof["observedUid"] = uid
        proof["recordedUid"] = STATE["environment"].get("namespaceUid")
        if labels.get("logweir.dev/test-owner") != OWNER:
            raise RuntimeError(f"refusing to delete {NS}: owner label is {labels}")
        if proof["recordedUid"] and proof["recordedUid"] != uid:
            raise RuntimeError(f"refusing to delete {NS}: UID moved")
        proof["inventory"] = {
            kind: [i["metadata"]["name"] for i in lst(kind)]
            for kind in (
                "backupschedules",
                "backups",
                "backupdestinations",
                "kafkaclusters",
                "topicdiscoveries",
                "jobs",
                "configmaps",
            )
        }
        kubectl("delete", "namespace", NS, "--wait=true", timeout=300)
        proof["deletedAt"] = now()
        proof["state"] = "deleted"
    after = get_opt("namespace", NS, namespace=None)
    proof["afterDelete"] = "absent" if after is None else "STILL PRESENT"
    proof["otherNamespacesUntouched"] = [
        item["metadata"]["name"]
        for item in json.loads(run(K + ["get", "ns", "-o", "json"]).stdout)["items"]
    ]
    proof["credentialSweep"] = sweep_for_credentials(credentials)
    STATE["cleanup"] = proof
    save()
    lines = [
        "# D1 W8 cleanup proof",
        "",
        f"* context `{CONTEXT}`, namespace `{NS}`",
        f"* owner label observed: `{proof.get('observedLabels', {}).get('logweir.dev/test-owner')}`",
        f"* UID recorded at setup: `{proof.get('recordedUid')}`; UID observed before deletion: "
        f"`{proof.get('observedUid')}`",
        f"* state: {proof['state']}; after deletion: {proof['afterDelete']}",
        f"* namespaces now present: {', '.join(proof['otherNamespacesUntouched'])}",
        f"* credential sweep over {proof['credentialSweep']['checkedFiles']} artifact files: "
        f"{proof['credentialSweep']['findings'] or 'no credential value found'}",
        "",
        "The shared release `scram-local` in `logweir-scram-local` was read, never written:",
        "no `helm` command was issued and no object in that namespace was created, patched or",
        "deleted by this harness.",
        "",
    ]
    artifact("cleanup.md", "\n".join(lines))
    log(f"cleanup: {proof['state']}, after={proof['afterDelete']}")


def fence_pre() -> None:
    """Create the cluster-scoped half of the fence BEFORE the namespace exists.

    Order matters and this is why it is its own phase: the binding selects the
    namespace by a label, so if the namespace were created first there would be
    a window — however short — in which the shared controller could see and
    write objects here. Requires the cluster lock.
    """
    STATE["environment"]["fence"] = fenced.create_fence(sys.modules[__name__])
    save()
    log(f"fence: {STATE['environment']['fence']}")


def fence_teardown() -> None:
    STATE["environment"]["fenceTeardown"] = fenced.teardown(sys.modules[__name__])
    save()
    log(f"fence teardown: {STATE['environment']['fenceTeardown']}")


# `--fence-revision <sha>` / `--fence-old-revision <sha>`: the revision the
# fenced rows must find on the image pair they swap in. Without them the `new`
# pair's expectation comes from its own OCI label and must equal this checkout's
# `origin/main` (fenced.py::assert_source_matched); with them it is whatever the
# operator names, and the image must still carry it. A flag names an
# expectation; it never suspends the check.
FENCE_REVISION_FLAGS = {"--fence-revision": "new", "--fence-old-revision": "old"}


def split_fence_flags(argv: list[str]) -> list[str]:
    """Strip the revision flags out of the phase list, recording each pin."""
    phases: list[str] = []
    pending: str | None = None
    for token in argv:
        if pending is not None:
            fenced.REVISION_OVERRIDE[FENCE_REVISION_FLAGS[pending]] = token
            pending = None
            continue
        flag, sep, value = token.partition("=")
        if flag in FENCE_REVISION_FLAGS:
            if sep:
                fenced.REVISION_OVERRIDE[FENCE_REVISION_FLAGS[flag]] = value
            else:
                pending = flag
            continue
        phases.append(token)
    if pending is not None:
        raise SystemExit(f"{pending} needs a commit sha")
    if fenced.REVISION_OVERRIDE:
        STATE["environment"]["fenceRevisionOverride"] = dict(fenced.REVISION_OVERRIDE)
    return phases


def main(argv: list[str]) -> int:
    load()
    phases = split_fence_flags(argv) or ["setup"]
    for phase in phases:
        if phase == "fence-pre":
            fence_pre()
        elif phase == "fence-teardown":
            fence_teardown()
        elif phase == "setup":
            setup()
        elif phase == "report":
            report()
        elif phase == "cleanup":
            cleanup()
        elif phase in SCENARIOS:
            execute(phase)
        elif phase == "all":
            for sid in SCENARIOS:
                execute(sid)
        else:
            raise SystemExit(f"unknown phase {phase}; known: {sorted(SCENARIOS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
