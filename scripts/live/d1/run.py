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
import secrets
import subprocess
import sys
import time
from typing import Any, Callable

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fixture  # noqa: E402
from fence import fenced  # noqa: E402
from fixtures import acl_kafka  # noqa: E402

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
    # AND NEVER THE RAW `Idempotency-Key` (L-06-2-cli). It is not a credential,
    # but `crates/logweir-api/src/idempotency.rs` is explicit that the raw key
    # is never stored or logged in any form -- only hashes derived from it --
    # because the published request hash is salted with its digest precisely so
    # that the hash cannot be recomputed from the object. An artifact that
    # printed the key would hand a reader the one input that makes it
    # recomputable, which is the property that rule exists to keep.
    text = re.sub(
        r"(Idempotency-Key:\s*)[^\"\\\\]+",
        r"\1[REDACTED]",
        text,
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
# L-09-5's broker: KRaft with StandardAuthorizer, and one topic the backup
# principal may not describe (`fixtures/acl_kafka.py`)
# ---------------------------------------------------------------------------

ACL_SOURCE = "acl-source"
ACL_CLIENT_PROPERTIES = "/tmp/d1-backup-client.properties"


def acl_kafka_pod() -> str:
    items = lst("pods", f"app={acl_kafka.APP}")
    ready = [
        p["metadata"]["name"]
        for p in items
        if all(c.get("ready") for c in (p.get("status") or {}).get("containerStatuses") or [])
    ]
    if not ready:
        raise Failure("no ready ACL Kafka pod in this namespace", obj=items)
    return ready[0]


def acl_exec(*argv: str, timeout: int = 120, check: bool = True) -> str:
    pod = STATE["environment"]["aclKafkaPod"]
    return run(KN + ["exec", pod, "--", *argv], timeout=timeout, check=check).stdout


def acl_create_topics(names: list[str], *, records: int = 5) -> None:
    """Create topics on the ACL broker as the pod-local super user.

    `localhost:9092` and not the published listener: the PLAINTEXT listener is
    the `ANONYMOUS` super-user path and exists only inside the pod, so no
    credential is needed and none appears in a recorded argv.
    """
    pod = STATE["environment"]["aclKafkaPod"]
    for topic in names:
        acl_exec(
            "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
            "--create", "--if-not-exists", "--topic", topic,
            "--partitions", "1", "--replication-factor", "1",
        )
        if records:
            payload = "\n".join(f"d1-{topic}-{i}" for i in range(records)) + "\n"
            run(
                KN + ["exec", "-i", pod, "--", "/opt/kafka/bin/kafka-console-producer.sh",
                      "--bootstrap-server", "localhost:9092", "--topic", topic],
                data=payload,
                timeout=120,
            )


def acl_principal_listing() -> list[str]:
    """What the MEASURED principal itself sees, read from inside the pod.

    THE PASSWORD IS EXPANDED IN THE CONTAINER, NOT HERE. The client properties
    file is written by a `sh -c` whose argv carries the literal
    `$D1_BACKUP_PASSWORD`; the shell inside the pod substitutes it from the
    Secret-backed environment variable. `run` records argv, so what lands in
    `results.json` is the variable's name.

    This is the fixture's own reading, not Logweir's: it says what a SCRAM
    client with these ACLs is shown, so that a row which later finds the same
    set in a frozen plan can tell "Logweir resolved the visible topics" from
    "Logweir happened to agree with a broken broker".
    """
    pod = STATE["environment"]["aclKafkaPod"]
    # AN UNQUOTED HEREDOC, AND THAT IS THE WHOLE POINT. `<<'EOF'` would write
    # the seven characters `$D1_BA…` into the properties file and the broker
    # would answer `SaslAuthenticationException: invalid credentials` — which is
    # what it did the first time this ran. The delimiter is bare so the shell
    # INSIDE the container substitutes the Secret-backed variable; nothing in
    # the body needs quoting for any other reason (no backticks, no backslashes,
    # no second `$`).
    script = (
        f"cat > {ACL_CLIENT_PROPERTIES} <<EOF\n"
        "security.protocol=SASL_PLAINTEXT\n"
        "sasl.mechanism=SCRAM-SHA-512\n"
        "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule "
        f'required username="{acl_kafka.BACKUP_USER}" password="$D1_BACKUP_PASSWORD";\n'
        "EOF\n"
    )
    run(KN + ["exec", pod, "--", "sh", "-c", script], timeout=120)
    out = acl_exec(
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        f"{acl_kafka.SERVICE}.{NS}.svc.cluster.local:{acl_kafka.SASL_PORT}",
        "--command-config", ACL_CLIENT_PROPERTIES,
        "--list",
    )
    return sorted(line.strip() for line in out.splitlines() if line.strip())


def ensure_acl_broker(*, allowed: tuple[str, ...] = acl_kafka.ALLOWED_TOPICS) -> dict[str, Any]:
    """Build (or re-attach to) the ACL broker and its saved connection.

    Idempotent, because L-09-5 and its negative control both need it and a
    second format would throw the first one's credential away. `allowed` is the
    ONE knob: the negative control passes every topic, which is the single
    input it flips.
    """
    state = STATE["environment"].setdefault("aclKafka", {})
    if get_opt("secret", acl_kafka.SECRET) is None:
        # GENERATED PER RUN, NEVER RETURNED, NEVER LOGGED. `token_urlsafe`
        # draws from `[A-Za-z0-9_-]`, which needs no escaping in a Java
        # properties file, in a JAAS string or in `--add-scram`'s bracket
        # syntax — a password that has to be escaped three times is a fixture
        # that fails for a reason nobody can see.
        password = secrets.token_urlsafe(18)
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": acl_kafka.SECRET, "namespace": NS, "labels": dict(LABELS)},
            "type": "Opaque",
            "data": {
                acl_kafka.SECRET_KEY: base64.b64encode(password.encode()).decode()
            },
        }
        # Not through `apply`, which would echo the object — and so the value —
        # into this process's captured stdout and then into `results.json`.
        run(K + ["apply", "-f", "-"], data=json.dumps(body))
        del password
        state["secret"] = acl_kafka.SECRET
        state["passwordGeneratedPerRun"] = True
    for manifest in acl_kafka.manifests(NS, dict(LABELS)):
        apply(manifest)
    kn("rollout", "status", f"deploy/{acl_kafka.DEPLOYMENT}", "--timeout=240s", timeout=260)
    STATE["environment"]["aclKafkaPod"] = acl_kafka_pod()
    acl_create_topics([*acl_kafka.ALLOWED_TOPICS, acl_kafka.DENIED_TOPIC])
    acls = []
    for argv in acl_kafka.acl_commands(allowed=allowed):
        acl_exec(*argv)
        acls.append(" ".join(argv[1:]))
    state["aclsApplied"] = acls
    state["grantedDescribeOn"] = list(allowed)
    state["brokerTopics"] = sorted(
        line.strip()
        for line in acl_exec(
            "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"
        ).splitlines()
        if line.strip()
    )
    state["principalVisibleTopics"] = acl_principal_listing()
    state["clusterId"] = acl_kafka.CLUSTER_ID
    cluster = apply(acl_kafka.kafka_cluster(NS, ACL_SOURCE, dict(LABELS)))
    state["kafkaClusterUid"] = cluster["metadata"]["uid"]
    ready = wait_for(
        "kafkacluster",
        ACL_SOURCE,
        lambda o: (o.get("status") or {}).get("reachable") is True,
        timeout=300,
        what="to be reachable as the ACL-limited principal",
    )
    state["observedClusterId"] = (ready.get("status") or {}).get("clusterId")
    save()
    # A SNAPSHOT, NOT THE LIVE DICT. `state` is `STATE["environment"]["aclKafka"]`
    # itself, and the caller puts what it gets back into its own `asserted`
    # block. Returning the live object makes those two the SAME dict, so
    # `NEG-09-5` — which calls this again with a wider ACL set — would rewrite
    # L-09-5's already-recorded evidence the next time `save()` ran, inside one
    # invocation. It survived here only because each phase ran as its own
    # process and `load()` re-read the file; a single
    # `run.py L-09-5 NEG-09-5` would have silently changed the record of a row
    # that had already passed.
    return json.loads(json.dumps(state))


def acl_backup(name: str, policy: str, **over: Any) -> dict[str, Any]:
    """A dynamic `Backup` against the ACL broker. No exclusion rules at all.

    The exclusions are empty ON PURPOSE: this row's only subtraction must be
    the one the broker makes. An `exclude` block here would give the frozen
    list a second reason to omit `secret-t` and the row could no longer say
    which one did it.
    """
    body: dict[str, Any] = {
        "sourceRef": {"name": ACL_SOURCE},
        "topics": [],
        "allUserTopics": {"exclude": {"topics": [], "prefixes": []},
                          "incompleteDiscovery": policy},
        "deadlineSeconds": 600,
    }
    body.update(over)
    return backup_object(name, **body)


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


# ---------------------------------------------------------------------------
# Two row decisions, as named functions
#
# Lifted out of their call sites so they can be fed a recorded object without a
# cluster: `scripts/live/d1/test_rows.py` runs each against the shape the
# controller publishes today AND the shape the rows used to look for, and
# requires the second to be refused. A decision that cannot be made to say
# False is not a decision.
# ---------------------------------------------------------------------------


def selection_counts_agree(sel: dict[str, Any], status_sel: dict[str, Any]) -> dict[str, bool]:
    """Where the discovery's accounting lives, judged clause by clause.

    The frozen plan carries it under `selection.discovery` as `{count, names}`
    pairs with a bounded name sample; `status.selection` carries the same two
    numbers flattened and no names at all. The two must agree: a status that
    disagreed with the plan it was frozen from would be the worse defect.
    """
    discovery = sel.get("discovery") or {}
    internal = discovery.get("internalExcluded") or {}
    by_rule = discovery.get("excludedByRule") or {}
    return {
        "the plan counts at least the one internal topic the fixture made":
            isinstance(internal.get("count"), int) and internal.get("count") >= 1,
        "the internal exclusion names __consumer_offsets":
            "__consumer_offsets" in (internal.get("names") or []),
        "the plan counts the two rule exclusions and names them":
            by_rule.get("count") == 2
            and sorted(by_rule.get("names") or []) == ["pfx-a", "skip-me"],
        "the status flattens the plan's own two numbers":
            status_sel.get("internalExcludedCount") == internal.get("count")
            and status_sel.get("excludedByRuleCount") == by_rule.get("count"),
        "no unbounded name list reached the status":
            "names" not in status_sel and "__consumer_offsets" not in json.dumps(status_sel),
    }


def selection_empty_refusal(status: dict[str, Any], failed: dict[str, Any],
                            resolved: dict[str, Any]) -> dict[str, bool]:
    """D1 §3.4's shape for a discovery terminal state, clause by clause.

    `Failed=True` carrying the reason, `TopicsResolved=False` carrying the same
    one, `exitReason: operational`, and NO `exitCode` — an exit code would claim
    a runner ran, and for `SelectionEmpty` none is ever created.
    """
    return {
        "phase is Failed": status.get("phase") == "Failed",
        "Failed=True/SelectionEmpty":
            failed.get("status") == "True" and failed.get("reason") == "SelectionEmpty",
        "TopicsResolved=False/SelectionEmpty":
            resolved.get("status") == "False" and resolved.get("reason") == "SelectionEmpty",
        "a controller refusal is operational with no exitCode":
            status.get("exitReason") == "operational" and status.get("exitCode") is None,
        "the refusal says no runner Job follows":
            "no runner Job is created" in (failed.get("message") or ""),
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
    counts = selection_counts_agree(sel, status_sel)
    require(
        all(counts.values()),
        "the discovery accounting is not where the controller publishes it: "
        + "; ".join(sorted(k for k, ok in counts.items() if not ok))
        + f". Plan selection.discovery={discovery}, status.selection={status_sel}",
        obj=done,
        dumps={"inputsSelection": sel, "statusSelection": status_sel},
    )
    return {
        "uids": {"backup": uid, "discoveryJob": jobs[0]["metadata"]["uid"]},
        "asserted": {
            "observedPhase": seen_phase,
            "discoveryJobName": jobs[0]["metadata"]["name"],
            "frozenTopics": inputs["topics"],
            "inputsSelection": sel,
            "discoveryCounts": {"internalExcluded": internal_block,
                                "excludedByRule": by_rule_block,
                                "clauses": counts},
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
    refusal = selection_empty_refusal(done["status"], failed, resolved)
    require(
        all(refusal.values()),
        "the empty resolution is not refused the way D1 §3.4 says: "
        + "; ".join(sorted(k for k, ok in refusal.items() if not ok))
        + f". phase={done['status'].get('phase')!r}, "
        f"exitReason={done['status'].get('exitReason')!r}, "
        f"exitCode={done['status'].get('exitCode')!r}, "
        f"Failed={failed.get('status')!r}/{failed.get('reason')!r}, "
        f"TopicsResolved={resolved.get('status')!r}/{resolved.get('reason')!r}",
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
            "refusalClauses": refusal,
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
# L-09-5's two decisions, as named functions
#
# Same reason as `selection_counts_agree` above: `scripts/live/d1/test_rows.py`
# feeds each of them the shape the controller publishes AND a shape that must be
# refused, without a cluster. A decision that cannot be made to say False is not
# a decision.
# ---------------------------------------------------------------------------


def discovery_incomplete_refusal(status: dict[str, Any], failed: dict[str, Any],
                                 resolved: dict[str, Any]) -> dict[str, bool]:
    """D1 §7.2 R6's `Refuse` terminal state, clause by clause.

    The same shape as `selection_empty_refusal` — `Failed=True` and
    `TopicsResolved=False` carrying one reason, `exitReason: operational`, no
    `exitCode` — with the reason `DiscoveryIncomplete` and the controller's own
    sentence about WHY a successful listing is not proof. That sentence is
    asserted because it is the whole product decision in D1 §7.4: Kafka omits
    what a principal cannot describe, so "the listing worked" never upgrades
    coverage.
    """
    message = failed.get("message") or ""
    return {
        "phase is Failed": status.get("phase") == "Failed",
        "Failed=True/DiscoveryIncomplete":
            failed.get("status") == "True" and failed.get("reason") == "DiscoveryIncomplete",
        "TopicsResolved=False/DiscoveryIncomplete":
            resolved.get("status") == "False"
            and resolved.get("reason") == "DiscoveryIncomplete",
        "a controller refusal is operational with no exitCode":
            status.get("exitReason") == "operational" and status.get("exitCode") is None,
        "the refusal says a successful listing is not proof of completeness":
            "omits topics a principal cannot describe" in message,
    }


def partial_discovery_never_claims_the_cluster(
    sel: dict[str, Any],
    status_sel: dict[str, Any],
    frozen_topics: list[str],
    visible_to_the_principal: list[str],
    denied: str,
) -> dict[str, bool]:
    """PLAT-09.2's acceptance half, measured against an ACL-limited principal.

    WHY THIS DOES NOT DEMAND `visibility: limited`, AND WHY THAT IS THE PRODUCT
    BEING RIGHT RATHER THAN THE ROW BEING SOFT.

    `check_contract::visibility` says `limited` on exactly two signals: a
    LISTING ENTRY that carries `TopicAuthorizationFailed`, or an EXPECTED name
    the targeted probe was answered `TopicAuthorizationFailed` for. A run
    discovery sends neither: `backup_selection::discovery_plan` builds its
    `topicInventory` with `expected_topics: Vec::new()`, so nothing is probed by
    name; and Kafka's all-topics metadata response does not report a topic the
    principal cannot describe AS AN ERROR — it omits it, which is exactly what
    D1 §7.5 writes down ("Omitted by Kafka; visibility stays `unknown`"). This
    fixture's own reading confirms it from the client side: the principal's
    `--list` shows the allowed topics and nothing else.

    AND IT DOES NOT DEMAND THE CONVERSE EITHER (review **L-1**). An earlier
    version asserted `visibility == "limited"` **if and only if**
    `limitedTopicCount > 0`, and both halves of that biconditional are wrong
    against the contract, because the two numbers do not come from one
    predicate. `limitedTopicCount` is `classification.limited.len()` and
    `classify` buckets an entry on ANY per-entry error
    (`backup_selection.rs:326`, whose own comment says so), so a
    non-authorization error gives a positive count beside `visibility:
    unknown`. In the other direction an INTERNAL topic carrying
    `TopicAuthorizationFailed` is taken by the internal arm, which is tested
    first (`backup_selection.rs:324`), so `limited` can be raised with a zero
    count. Both are legal states and the row must not paint either red.

    What is left is sound and is what is asserted: the verdict is one of the
    two a run discovery can produce, the count is published and the status
    agrees with the plan it was frozen from, the frozen list is what the
    principal can see, and — the acceptance sentence's own clause — no
    observation upgrades coverage to `AllUserTopicsAttested` without an
    administrator attestation.
    """
    discovery = sel.get("discovery") or {}
    visibility = discovery.get("visibility") or sel.get("visibility")
    limited = discovery.get("limitedTopicCount")
    return {
        "the frozen list is exactly what the principal can see":
            frozen_topics == sorted(t for t in visible_to_the_principal
                                    if not t.startswith("__")),
        "the topic the principal may not describe is not in the frozen list":
            denied not in frozen_topics,
        "the discovery published a completeness verdict at all":
            visibility in {"unknown", "limited"},
        "the status flattens the plan's own limited count":
            isinstance(limited, int) and status_sel.get("limitedTopicCount") == limited,
        "the run is labelled visible-only":
            status_sel.get("coverage") == "VisibleUserTopicsOnly"
            and sel.get("coverage") == "VisibleUserTopicsOnly",
        "NOTHING claims whole-cluster coverage":
            "AllUserTopicsAttested" not in {status_sel.get("coverage"), sel.get("coverage")},
    }


@scenario("L-09-5", "PLAT-09.2", "an ACL-limited principal backs up what it can see, and says so")
def l_09_5() -> dict[str, Any]:
    """D1 §13.2 L-09-5, both halves, against a broker this row builds.

    `acl-refuse` (`incompleteDiscovery: Refuse`) must end `Failed` /
    `DiscoveryIncomplete` with no runner Job; `acl-visible`
    (`BackUpVisibleTopics`) must end `Succeeded` with coverage
    `VisibleUserTopicsOnly`, and `secret-t` must be absent from the frozen
    topics AND from the signed receipt.

    Negative control: `NEG-09-5`, which flips exactly one input — the ACL set —
    granting `Describe` on every topic including `secret-t`, and then asserts
    this row's two conclusions (`secret-t` is still out; the coverage is
    complete). It must fail, and its failure proves this row reads the broker.
    """
    broker = ensure_acl_broker()
    denied = acl_kafka.DENIED_TOPIC
    require(
        denied in broker["brokerTopics"],
        f"the fixture never created {denied}: {broker['brokerTopics']}",
        obj=broker,
    )
    require(
        denied not in broker["principalVisibleTopics"],
        f"the broker shows {denied} to the measured principal, so no ACL is being enforced "
        f"and this row would measure nothing: {broker['principalVisibleTopics']}",
        obj=broker,
    )

    # Half one: Refuse.
    refuse = create(acl_backup("acl-refuse", "Refuse"))
    refuse_uid = refuse["metadata"]["uid"]
    refused = wait_for("backup", "acl-refuse", terminal, timeout=420,
                       what="to reach a terminal phase")
    refuse_failed = condition(refused, "Failed") or {}
    refuse_resolved = condition(refused, "TopicsResolved") or {}
    refusal = discovery_incomplete_refusal(refused["status"], refuse_failed, refuse_resolved)
    require(
        all(refusal.values()),
        "the `Refuse` policy did not refuse the way D1 §7.2 R6 says: "
        + "; ".join(sorted(k for k, ok in refusal.items() if not ok))
        + f". phase={refused['status'].get('phase')!r}, "
        f"Failed={refuse_failed.get('status')!r}/{refuse_failed.get('reason')!r}, "
        f"TopicsResolved={refuse_resolved.get('status')!r}/{refuse_resolved.get('reason')!r}",
        obj=refused,
    )
    require(
        len(discovery_jobs(refuse_uid)) == 1,
        "the Refuse run did not run exactly one discovery Job",
        obj=[j["metadata"]["name"] for j in lst("jobs")],
    )
    refuse_runners = runner_jobs(refused)
    require(
        refuse_runners == [],
        "a runner Job was created for a run that refused on incomplete discovery: "
        f"{[j['metadata']['name'] for j in refuse_runners]}",
        obj=refused,
    )

    # Half two: BackUpVisibleTopics.
    visible = create(acl_backup("acl-visible", "BackUpVisibleTopics"))
    visible_uid = visible["metadata"]["uid"]
    done = wait_for("backup", "acl-visible", terminal, timeout=600,
                    what="to reach a terminal phase")
    require(
        done["status"]["phase"] == "Succeeded",
        "the BackUpVisibleTopics run ended {} ({}); TopicsResolved={}".format(
            done["status"]["phase"],
            done["status"].get("reason") or (condition(done, "Failed") or {}).get("reason"),
            (condition(done, "TopicsResolved") or {}).get("message"),
        ),
        obj=done,
    )
    plan = plan_of(done)
    sel = plan["inputs"]["selection"]
    status_sel = done["status"]["selection"]
    frozen = plan["inputs"]["topics"]
    clauses = partial_discovery_never_claims_the_cluster(
        sel, status_sel, frozen, broker["principalVisibleTopics"], denied
    )
    require(
        all(clauses.values()),
        "an ACL-limited discovery did not describe itself the way D1 §7.4/§7.5 say: "
        + "; ".join(sorted(k for k, ok in clauses.items() if not ok))
        + f". frozen={frozen}, plan selection={sel}, status.selection={status_sel}",
        obj=done,
        dumps={"inputsSelection": sel, "statusSelection": status_sel, "broker": broker},
    )
    receipt = receipt_of(done)
    require(
        receipt["source"]["topics"] == frozen,
        f"the receipt names {receipt['source']['topics']}, the frozen list is {frozen}",
        obj=receipt,
    )
    require(
        denied not in (receipt.get("records") or {}),
        f"the receipt claims records for {denied}, which this run never selected: "
        f"{sorted((receipt.get('records') or {}))}",
        obj=receipt,
    )
    return {
        "uids": {"refuse": refuse_uid, "visible": visible_uid,
                 "kafkaCluster": broker.get("kafkaClusterUid")},
        "asserted": {
            "broker": broker,
            "refuseClauses": refusal,
            "refuseCondition": {k: refuse_failed.get(k) for k in ("status", "reason", "message")},
            "refuseRunnerJobs": [],
            "visibleClauses": clauses,
            "frozenTopics": frozen,
            "inputsSelection": sel,
            "statusSelection": status_sel,
            "receiptTopics": receipt["source"]["topics"],
            "receiptRecords": receipt.get("records"),
            "planSha256": plan["sha256"],
        },
        "dumps": dump_objects("L-09-5", {
            "refuse": ("backup", "acl-refuse"),
            "visible": ("backup", "acl-visible"),
            "plan": ("configmap", plan["name"]),
            "aclSource": ("kafkacluster", ACL_SOURCE),
        }),
    }


@scenario("NEG-09-5", "PLAT-09.2", "L-09-5's negative control: Describe granted everywhere")
def neg_09_5() -> dict[str, Any]:
    """Flips ONE input — the ACL set — and re-asserts L-09-5's conclusions.

    `secret-t` gets the same `Describe`/`Read` grant the other topics have, so
    the broker now shows the measured principal every topic. This scenario then
    asserts what L-09-5 asserts: that `secret-t` is still missing from the
    frozen list, and that a discovery which really did see everything is
    labelled complete. Both are now false, and **this scenario failing is its
    pass**: it is what separates "the row read the broker" from "the row
    restated a constant".

    The second clause is worth its own sentence. `AllUserTopicsAttested` is
    reachable ONLY through an administrator attestation in the installation
    policy (`coverage_for`), so even a principal that can describe every topic
    in the cluster gets `VisibleUserTopicsOnly`. That is the acceptance
    sentence's second half — "failed or partial discovery never claims
    whole-cluster coverage" — proved from the other side.
    """
    broker = ensure_acl_broker(
        allowed=(*acl_kafka.ALLOWED_TOPICS, acl_kafka.DENIED_TOPIC)
    )
    denied = acl_kafka.DENIED_TOPIC
    visible_now = acl_principal_listing()
    obj = create(acl_backup("neg-acl", "BackUpVisibleTopics"))
    uid = obj["metadata"]["uid"]
    done = wait_for("backup", "neg-acl", terminal, timeout=600, what="to finish")
    plan = plan_of(done) if (done.get("status") or {}).get("execution") else None
    frozen = (plan or {}).get("inputs", {}).get("topics", [])
    status_sel = (done.get("status") or {}).get("selection") or {}
    require(
        denied not in visible_now
        or (denied not in frozen and status_sel.get("coverage") == "AllUserTopicsAttested"),
        f"{DELIBERATE_FAILURE_MARK}: with Describe granted on every topic the principal now "
        f"sees {visible_now}, the run froze {frozen} and recorded coverage "
        f"{status_sel.get('coverage')!r} — so `{denied}` is absent from the frozen list only "
        "when the broker hides it, and no discovery claims whole-cluster coverage without an "
        "administrator attestation. This scenario failing is the expected result.",
        obj={"backup": done, "principalVisibleTopics": visible_now,
             "frozenTopics": frozen, "statusSelection": status_sel},
    )
    return {"uids": {"backup": uid}, "asserted": {"unexpected": "the false assertion held"}}


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
        # INTERPOLATED, so the sentence the certification matches on and the
        # sentence this row raises cannot drift apart. They are the same string
        # or the negative control silently stops certifying.
        f"{DELIBERATE_FAILURE_MARK}: a named allowlist created no discovery Job, which is "
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
    # PLAT-06.2's done evidence, which D1 §13.2 does not give an L- number:
    # "Demonstrate the same manual CR path through CLI/API and UI." The UI half
    # is `scripts/plat06-2-ui-e2e.mjs`; this is the CLI/API half.
    "PLAT-06.2": ["L-06-2-cli"],
}

# ---------------------------------------------------------------------------
# PLAT-06.2 — the same manual CR path through the CLI, beside the API
# ---------------------------------------------------------------------------

MANUAL_ROUTE = "POST /api/v1/namespaces/{ns}/backups"
API_SUBJECT = os.environ.get("LOGWEIR_D1_API_SUBJECT") or OWNER


def api_binary() -> pathlib.Path:
    """The `logweir-api` this harness drives, release first.

    The Lean-loop rule is "CLI branches run the RELEASE binary against the
    lab", and `L-04-preview` predates it and looks in `target/debug`. Both are
    accepted here, newest first, and the one that was used is recorded in the
    scenario's own `asserted` block so a reader never has to guess which.
    """
    for candidate in ("target/release/logweir-api", "target/debug/logweir-api"):
        binary = ROOT / candidate
        if binary.exists():
            return binary
    raise Failure(
        "neither target/release/logweir-api nor target/debug/logweir-api exists; build one "
        "with `cargo build --release -p logweir-api`"
    )


def page_derived_name(namespace: str, key: str, *, issuer: str, subject: str) -> str:
    """D1 section 8.2's name, computed by THE PAGE'S OWN FUNCTION.

    This deliberately shells into node and imports `ui/client.js` rather than
    reimplementing the rule in Python. A third implementation would prove that
    three things agree with each other and nothing about the two that ship;
    what this row is for is that the CONSOLE's derivation and the PRODUCT API's
    derivation are one rule, so the console's derivation has to be the thing
    that runs here.
    """
    script = (
        "import { manualBackupName, MANUAL_BACKUP_ROUTE } from "
        f"{json.dumps(str(ROOT / 'ui' / 'client.js'))};\n"
        "const name = await manualBackupName({\n"
        f"  issuer: {json.dumps(issuer)}, subject: {json.dumps(subject)},\n"
        f"  namespace: {json.dumps(namespace)}, route: MANUAL_BACKUP_ROUTE,\n"
        f"  key: {json.dumps(key)},\n"
        "});\n"
        "process.stdout.write(name);\n"
    )
    return run(
        ["node", "--input-type=module", "-e", script], timeout=60, record=False
    ).stdout.strip()


def run_policy_copy(schedule: dict[str, Any]) -> dict[str, Any]:
    """The policy half of the `Backup` a manual run of `schedule` becomes.

    An INDEPENDENT copy, written from `config/samples/backup-manual.yaml`'s own
    instructions ("copy `spec.sourceRef`, `spec.topics` (or `spec.allUserTopics`
    with `topics: []`), `spec.archive` and `spec.activeDeadlineSeconds` from the
    same object"), in a third language, by a reader of the sample rather than by
    the code under test. That is what makes the comparison below mean something:
    it is not the API's output compared with itself.
    """
    spec = schedule["spec"]
    copied: dict[str, Any] = {
        "sourceRef": {"name": spec["sourceRef"]["name"]},
        "topics": list(spec.get("topics") or []),
        "archive": json.loads(json.dumps(spec["archive"])),
        "deadlineSeconds": spec.get("activeDeadlineSeconds", 3600),
        "triggeredBy": "manual",
        "trigger": {"kind": "Manual", "attempt": 0},
        "scheduleRef": {
            "name": schedule["metadata"]["name"],
            "uid": schedule["metadata"]["uid"],
            "generation": schedule["metadata"]["generation"],
            "runPolicySha256": schedule["status"]["policy"]["runPolicySha256"],
        },
    }
    if spec.get("allUserTopics") is not None:
        copied["allUserTopics"] = json.loads(json.dumps(spec["allUserTopics"]))
    if spec.get("destinationRef") is not None:
        copied["destinationRef"] = {"name": spec["destinationRef"]["name"]}
    return copied


def manual_labels(schedule: dict[str, Any]) -> dict[str, str]:
    """The four labels D1 section 8.1 writes, and no others."""
    return {
        "logweir.dev/trigger": "manual",
        "logweir.dev/attempt": "0",
        "logweir.dev/schedule": schedule["metadata"]["name"],
        "logweir.dev/schedule-uid": schedule["metadata"]["uid"],
    }


def scoped_sample(schedule: dict[str, Any], name: str, spec: dict[str, Any]) -> str:
    """`config/samples/backup-manual.yaml`, scoped to this schedule.

    The SHIPPED file is read and its five placeholder values are replaced --
    the name, the two schedule labels, and the policy block the sample's own
    prose tells an operator to copy. Everything else, prose and structure, is
    the file as it ships, and the exact bytes applied are written next to the
    result so a reader can diff them against `config/samples/backup-manual.yaml`.
    """
    text = (ROOT / "config/samples/backup-manual.yaml").read_text()
    head, _, _ = text.partition("\n---\n")
    body = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "labels": manual_labels(schedule)},
        "spec": spec,
    }
    return (
        head
        + "\n# ---------------------------------------------------------------------------\n"
        + f"# SCOPED to schedule {schedule['metadata']['name']} (generation "
        + f"{schedule['metadata']['generation']}) in namespace {NS} by\n"
        + "# scripts/live/d1/run.py::L-06-2-cli. Structure and prose above are the shipped\n"
        + "# file's; the values below are this schedule's, copied as the prose says.\n"
        + "# ---------------------------------------------------------------------------\n---\n"
        + json.dumps(body, indent=2, sort_keys=True)
        + "\n"
    )


def await_manual_run(name: str, *, timeout: int = 420) -> dict[str, Any]:
    """Wait until this `Backup` has actually become a run.

    "Reconciled to a run" is not "the object exists": it is the controller
    having frozen the inputs and dispatched a Job. The execution id is asserted
    to be the object's own UID, which is D1 section 3.1 rule 2 for a manual run -- a
    manual run's archive prefix can therefore never collide with a scheduled
    run's `<scheduleUID>-<slot>`.
    """
    reconciled = wait_for(
        "backup",
        name,
        lambda o: ((o.get("status") or {}).get("execution") or {}).get("id") is not None,
        timeout=timeout,
        what="to freeze its execution inputs",
    )
    jobs = wait_until(
        lambda: runner_jobs(get("backup", name)),
        timeout=timeout,
        interval=1.0,
        what=f"the runner Job of {name}",
    )
    return {
        "name": name,
        "uid": reconciled["metadata"]["uid"],
        "executionId": reconciled["status"]["execution"]["id"],
        "inputsRef": reconciled["status"]["execution"].get("inputsRef"),
        "inputsSha256": reconciled["status"]["execution"].get("inputsSha256"),
        "job": jobs[0]["metadata"]["name"],
        "jobUid": jobs[0]["metadata"]["uid"],
        "phase": (reconciled.get("status") or {}).get("phase"),
    }


def spec_differences(left: dict[str, Any], right: dict[str, Any], path: str = "") -> list[str]:
    """Every leaf at which two specs differ, by path. `[]` is byte equality."""
    out: list[str] = []
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right)):
            here = f"{path}.{key}" if path else key
            if key not in left:
                out.append(f"{here}: absent vs {right[key]!r}")
            elif key not in right:
                out.append(f"{here}: {left[key]!r} vs absent")
            else:
                out.extend(spec_differences(left[key], right[key], here))
        return out
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return [f"{path}: {len(left)} entries vs {len(right)}"]
        for i, (a, b) in enumerate(zip(left, right)):
            out.extend(spec_differences(a, b, f"{path}[{i}]"))
        return out
    if left != right:
        out.append(f"{path}: {left!r} vs {right!r}")
    return out


@scenario(
    "L-06-2-cli",
    "PLAT-06.2",
    "one CR path: kubectl apply and the product API build the same manual Backup",
)
def l_06_2_cli() -> dict[str, Any]:
    """PLAT-06.2's done evidence: "Demonstrate the same manual CR path through
    CLI/API and UI."

    The claim `config/samples/backup-manual.yaml` makes in its own header is
    that `POST /api/v1/namespaces/<ns>/backups` and the console's "Back up now"
    "produce exactly this shape", and that "the only fields they add are the
    deterministic name and the console's own audit annotations". This measures
    it, on the real cluster, both ways round:

      1. The product API creates a manual run from a schedule under a known
         idempotency key. Its NAME is compared with the name the CONSOLE'S OWN
         `manualBackupName` derives for the same scope -- the two
         implementations of D1 section 8.2, live, with no fixture in between.
      2. That object is read back, reconciled to a run, and then DELETED, so
         the same name is free.
      3. `kubectl apply -f` the shipped sample, scoped to the same schedule
         under the same name, with its policy block copied INDEPENDENTLY here
         in Python from the sample's own prose. Its `spec` and its labels are
         compared with the API's, leaf by leaf.
      4. It reconciles to a run too.
      5. A second POST under the same key, while the kubectl object holds the
         name, is `409 state_conflict`: the API targeted exactly that name and
         refused to adopt an object it did not create.
      6. NEGATIVE CONTROL: one snapshot field of the copy is changed and the
         comparison must FAIL, and the controller must refuse the run
         terminally rather than executing a policy whose digest it cannot
         reproduce.
    """
    name = "manual-cli"
    kn("delete", "backupschedule", name, "--ignore-not-found=true", "--wait=true")
    # SUSPENDED, so nothing this scenario measures can be a scheduled run that
    # happened to fire. D1 section 8.3: a suspended schedule never blocks a manual run,
    # and that is exactly the property being leaned on here.
    schedule = create(schedule_object(name, schedule="0 3 * * *", suspend=True, topics=["t1", "t2"]))
    observed = await_schedule_observed(name)
    require(
        ((observed.get("status") or {}).get("policy") or {}).get("generation")
        == observed["metadata"]["generation"],
        "the controller has not published a policy digest for the current generation, so "
        "neither the console nor this harness may copy one",
        obj=excerpt(observed, "metadata.generation", "status.policy", "status.observedGeneration"),
    )
    schedule = observed
    generation = schedule["metadata"]["generation"]

    binary = api_binary()
    API_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    cursor = API_DIR / "cursor.key"
    if not cursor.exists():
        cursor.write_bytes(os.urandom(48))
        cursor.chmod(0o600)
    config = API_DIR / "config-p062.yaml"
    config.write_text(
        "mode: localAdmin\n"
        f'listen: "127.0.0.1:{API_PORT}"\n'
        f'publicOrigin: "http://127.0.0.1:{API_PORT}"\n'
        f"uiDirectory: {ROOT / 'ui'}\n"
        "localAdmin:\n"
        f"  subject: {API_SUBJECT}\n"
        "  displayName: PLAT-06.2 CLI/API comparison\n"
        f"namespaces: [{NS}]\n"
        "kubernetes:\n"
        "  source: kubeconfig\n"
        "  context: docker-desktop\n"
        f"cursorKeyFile: {cursor}\n"
    )
    log_path = API_DIR / "api-p062.log"
    handle = subprocess.Popen(  # noqa: S603 - a repository binary with a literal argv
        [str(binary), "--config", str(config)],
        stdout=log_path.open("wb"),
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{API_PORT}"
    key = "plat06-2-finish." + secrets.token_hex(16)

    def post(idempotency_key: str, body: dict[str, Any]) -> tuple[int, Any]:
        result = run(
            [
                "curl", "-sS", "-w", "\n%{http_code}",
                "-X", "POST",
                "-H", "Content-Type: application/json",
                # An unsafe request must carry the configured public origin:
                # `logweir-api` refuses one that does not with 403
                # `origin_mismatch`, which is the same-origin rule the console
                # relies on and which a curl has to satisfy like any browser.
                "-H", f"Origin: {base}",
                "-H", f"Idempotency-Key: {idempotency_key}",
                "--data-binary", json.dumps(body),
                f"{base}/api/v1/namespaces/{NS}/backups",
            ],
            timeout=60,
        )
        text, _, code = result.stdout.rpartition("\n")
        try:
            return int(code), json.loads(text)
        except json.JSONDecodeError:
            return int(code), text

    try:
        wait_until(
            lambda: run(
                ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", f"{base}/healthz"],
                check=False, timeout=10, record=False,
            ).stdout == "200",
            timeout=60,
            interval=1.0,
            what="logweir-api to answer /healthz",
        )

        # --- 0. the fixture is the rule this row is about --------------------
        # `ui/tests/fixtures/manual-backup-names.json` is the pin between the
        # two implementations, and a fixture only one side checks pins a
        # function to itself. The page's half is checked by `ui/tests/d1.spec.js`;
        # this is where the rule meets the REAL binary, so the fixture's own
        # rule block is asserted to be the rule used below, and every recorded
        # row is re-derived by the page's function on this machine.
        fixture_path = ROOT / "ui/tests/fixtures/manual-backup-names.json"
        name_fixture = json.loads(fixture_path.read_text())
        require(
            name_fixture["rule"]["route"] == MANUAL_ROUTE
            and name_fixture["rule"]["prefix"] == "logweir-manual-"
            and name_fixture["rule"]["hashChars"] == 26
            and name_fixture["rule"]["fieldOrder"]
            == ["issuer", "subject", "namespace", "route", "key"],
            "the name fixture describes a different rule from the one this row measures",
            obj=name_fixture["rule"],
        )
        fixture_rows = []
        for entry in name_fixture["rows"]:
            scope = entry["scope"]
            derived = page_derived_name(
                scope["namespace"], scope["key"],
                issuer=scope["issuer"], subject=scope["subject"],
            )
            require(
                derived == entry["name"],
                f"the page derives {derived!r} for a scope the fixture records as "
                f"{entry['name']!r} ({entry['note']})",
                obj=entry,
            )
            fixture_rows.append({"note": entry["note"], "name": entry["name"]})

        # --- 1. the API's run, and the name the console would derive ---------
        expected_name = page_derived_name(
            NS, key, issuer="urn:logweir:local-admin", subject=API_SUBJECT
        )
        code, created = post(key, {"scheduleRef": {"name": name, "expectedGeneration": generation}})
        require(code == 201, f"POST .../backups answered {code}", obj=created)
        api_name = created["item"]["name"]
        require(
            api_name == expected_name,
            "THE TWO IMPLEMENTATIONS OF D1 section 8.2 DISAGREE. The product API named the run "
            f"{api_name!r}; the console's own manualBackupName derives {expected_name!r} for "
            "the same scope (issuer urn:logweir:local-admin, subject "
            f"{API_SUBJECT!r}, namespace {NS!r}, route {MANUAL_ROUTE!r}).",
            obj=created,
        )
        api_object = get("backup", api_name)
        api_run = await_manual_run(api_name)
        api_spec = json.loads(json.dumps(api_object["spec"]))
        api_labels = dict(api_object["metadata"].get("labels") or {})
        api_annotations = sorted((api_object["metadata"].get("annotations") or {}).keys())

        # --- 2. free the name --------------------------------------------------
        kn("delete", "backup", api_name, "--wait=true", timeout=180)
        wait_until(
            lambda: get_opt("backup", api_name) is None,
            timeout=120, interval=1.0, what=f"{api_name} to be gone",
        )

        # --- 3. the same object, through kubectl -------------------------------
        copied = run_policy_copy(schedule)
        sample_path = OUT / "objects/L-06-2-cli/backup-manual.scoped.yaml"
        sample_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        sample_path.write_text(scoped_sample(schedule, api_name, copied))
        kn("apply", "-f", str(sample_path))
        cli_object = get("backup", api_name)
        cli_spec = json.loads(json.dumps(cli_object["spec"]))
        cli_labels = dict(cli_object["metadata"].get("labels") or {})

        spec_diff = spec_differences(api_spec, cli_spec)
        require(
            not spec_diff,
            "the kubectl object's spec differs from the API's for the same scope: "
            + "; ".join(spec_diff),
            obj={"api": api_spec, "kubectl": cli_spec},
        )
        require(
            api_labels == cli_labels,
            f"the labels differ: API {api_labels} vs kubectl {cli_labels}",
            obj={"api": api_labels, "kubectl": cli_labels},
        )
        require(
            cli_object["metadata"]["name"] == api_name,
            "the two objects do not share the name",
        )
        require(
            api_spec["trigger"] == {"kind": "Manual", "attempt": 0}
            and api_spec["triggeredBy"] == "manual"
            and api_spec["scheduleRef"]["generation"] == generation
            and api_spec["scheduleRef"]["runPolicySha256"]
            == schedule["status"]["policy"]["runPolicySha256"],
            "the shared spec is not D1 section 8.1's manual shape",
            obj=api_spec,
        )
        cli_run = await_manual_run(api_name)
        require(
            cli_run["uid"] != api_run["uid"] and cli_run["executionId"] == cli_run["uid"],
            "a re-created name is a NEW run with its own execution id (D1 section 3.1 rule 2)",
            obj={"api": api_run, "kubectl": cli_run},
        )

        # --- 5. the API targets that name and refuses to adopt it --------------
        conflict_code, conflict = post(
            key, {"scheduleRef": {"name": name, "expectedGeneration": generation}}
        )
        require(
            conflict_code == 409 and conflict.get("code") == "state_conflict",
            f"a repeat POST over the kubectl object answered {conflict_code} "
            f"{conflict.get('code') if isinstance(conflict, dict) else conflict!r}; the API "
            "derives the same name and must refuse an object it did not create rather than "
            "adopt or replace it",
            obj=conflict,
        )

        # --- 6. the negative control -------------------------------------------
        # ONE SNAPSHOT FIELD CHANGED. The comparison must fail, and the
        # controller must refuse the run: `runPolicySha256` is recomputed from
        # the object's own fields, so a copied digest beside an edited field is
        # a control-plane defect and terminal.
        mutated = json.loads(json.dumps(copied))
        mutated["deadlineSeconds"] = int(copied["deadlineSeconds"]) + 1
        mutated_name = api_name[:-4] + "zzzz"
        mutated_path = OUT / "objects/L-06-2-cli/backup-manual.mutated.yaml"
        mutated_path.write_text(scoped_sample(schedule, mutated_name, mutated))
        kn("apply", "-f", str(mutated_path))
        mutated_object = get("backup", mutated_name)
        mutated_diff = spec_differences(api_spec, json.loads(json.dumps(mutated_object["spec"])))
        require(
            any(d.startswith("deadlineSeconds") for d in mutated_diff),
            "the comparison did not notice a changed snapshot field, so it could not have "
            "noticed a real one either: " + repr(mutated_diff),
            obj=mutated_object,
        )
        refused = wait_for(
            "backup",
            mutated_name,
            terminal,
            timeout=300,
            what="the mutated copy to be judged",
        )
        refused_condition = condition(refused, "Failed") or {}
        refusal_is_the_digest = refused_condition.get("reason") in {
            "RunPolicyDigestMismatch",
            "ScheduleRefInvalid",
            "ExecutionSpecInvalid",
        }
        kn("delete", "backup", mutated_name, "--ignore-not-found=true", "--wait=false")

        return {
            "asserted": {
                "route": MANUAL_ROUTE,
                "apiBinary": str(binary.relative_to(ROOT)),
                "mode": "localAdmin",
                "subject": API_SUBJECT,
                "schedule": {
                    "name": name,
                    "uid": schedule["metadata"]["uid"],
                    "generation": generation,
                    "suspended": True,
                    "runPolicySha256": schedule["status"]["policy"]["runPolicySha256"],
                },
                "nameRule": {
                    "fixture": str(fixture_path.relative_to(ROOT)),
                    "fixtureRowsRederivedByThePage": fixture_rows,
                    "consoleDerived": expected_name,
                    "apiDerived": api_name,
                    "equal": expected_name == api_name,
                    "derivedBy": "ui/client.js::manualBackupName",
                },
                "api": {
                    "status": code,
                    "run": api_run,
                    "annotations": api_annotations,
                    "spec": api_spec,
                    "labels": api_labels,
                },
                "kubectl": {
                    "command": f"kubectl --context {CONTEXT} -n {NS} apply -f "
                    "config/samples/backup-manual.yaml (scoped)",
                    "file": artifact(
                        "objects/L-06-2-cli/backup-manual.scoped.yaml",
                        sample_path.read_text(),
                    ),
                    "run": cli_run,
                    "spec": cli_spec,
                    "labels": cli_labels,
                    "annotations": sorted(
                        (cli_object["metadata"].get("annotations") or {}).keys()
                    ),
                },
                "specDifferences": spec_diff,
                "labelsEqual": api_labels == cli_labels,
                "repeatPostOverTheKubectlObject": {
                    "status": conflict_code,
                    "code": conflict.get("code") if isinstance(conflict, dict) else None,
                },
                "negativeControl": {
                    "changedField": "deadlineSeconds",
                    "from": copied["deadlineSeconds"],
                    "to": mutated["deadlineSeconds"],
                    "name": mutated_name,
                    "comparisonFailedAt": mutated_diff,
                    "controllerPhase": (refused.get("status") or {}).get("phase"),
                    "controllerReason": refused_condition.get("reason"),
                    "controllerMessage": refused_condition.get("message"),
                    "refusedForTheDigest": refusal_is_the_digest,
                },
            },
            "uids": {
                "schedule": schedule["metadata"]["uid"],
                "apiBackup": api_run["uid"],
                "kubectlBackup": cli_run["uid"],
                "mutatedBackup": mutated_object["metadata"]["uid"],
            },
            "dumps": dump_objects(
                "L-06-2-cli",
                {"schedule": ("backupschedule", name), "kubectlBackup": ("backup", api_name)},
            ),
        }
    finally:
        handle.terminate()
        try:
            handle.wait(timeout=30)
        except subprocess.TimeoutExpired:
            handle.kill()
        artifact("logs/logweir-api-p062.log", log_path.read_text(errors="replace"))


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
                "holds the runner Job POST, which only the proxy in front of a fenced "
                "controller can do. The row EXISTS now (`fence/rows.py::l_09_3a`) and runs "
                "whenever the fence is up; without the fence it cannot run at all. " + PROXY),
    "L-09-3b": ("PLAT-09.2", "the source changes between discovery and freeze",
                "NO LONGER blocked by the defect L-09-1 measured — the discovery Job carried "
                "the compile-time image pin, no node held it, and "
                "`SourceChangedDuringResolution` is decided AFTER discovery succeeds, so the "
                "case could not be reached at all. D1-DISCOVERY-IMAGE is closed "
                "(lab-refresh-3 §8.1) and the row now REACHES its case: run unfenced on "
                "2026-09-18 it executed all three attempts and FAILED, because the discovery "
                "window is a few seconds wide and the source swap has to land inside it. "
                "That is the race this row has always been, and it is what "
                "D1 §13.1's request-holding proxy is for. Recorded as a FAIL when it is run "
                "unfenced, never as a pass, and as this reason only when it is not run at all."),
    "L-09-5": ("PLAT-09.2", "an ACL-limited principal",
               "NO LONGER blocked by the broker: `fixtures/acl_kafka.py` builds a KRaft "
               "`apache/kafka:3.7.1` with `StandardAuthorizer` whose SCRAM credential is "
               "written by `kafka-storage format --add-scram` before the broker starts, "
               "which is the only order that closes the bootstrap loop. This reason is "
               "recorded only when the row was not attempted at all."),
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
        "NOT blocked by the fence or the proxy: both exist in a fenced run and this row is "
        "registered in `fence/rows.py`. Recorded not-run here only if it was not among the "
        "phases this invocation was asked for."
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


# The sentence NEG-1 raises when its deliberately false assertion holds long
# enough to be refused. Matched literally, because the point is to tell THIS
# failure from every other one.
DELIBERATE_FAILURE_MARK = "DELIBERATELY FALSE ASSERTION"


# Every negative control in this harness, and the row each one certifies.
#
# ONE PER NEW BEHAVIOUR (WORKER-RULES "Lean loop"). NEG-1 certifies the harness
# as a whole — it was the only one when this file had one dynamic row — and the
# three below certify the rows added for PLAT-09.2's remaining gap, each by
# flipping exactly ONE input of its row and re-making that row's own assertion.
# All four are expected to be recorded `fail`; a `pass` means the row it
# certifies is not reading the cluster.
NEGATIVE_CONTROLS = {
    "NEG-1": "L-09-6",
    "NEG-09-3a": "L-09-3a",
    "NEG-09-3b": "L-09-3b",
    "NEG-09-5": "L-09-5",
}


def negative_control_verdict(control: dict[str, Any] | None,
                             sid: str = "NEG-1") -> dict[str, Any]:
    """Whether this run demonstrated that the harness can produce a FAIL.

    WHICH FAILURE, NOT JUST A FAILURE (review harness-rows-4 **D-L1**). Every
    scenario in this file fails by raising `Failure`, and a `wait_for` timeout
    raises exactly the same type — so `status == "fail"` alone would certify the
    negative control on a run in which the cluster never answered and NEG-1's
    false assertion was never reached. That is the one failure mode a negative
    control must not accept: it would stamp "this harness can fail" on a run
    that only proved the cluster can be slow.

    The recorded failure must therefore be the deliberate one, by its own
    sentence. The sentence is carried in the row and matched literally here;
    `recordedFailure` goes into the record either way, so a reader can see what
    the run actually failed on.
    """
    control = control or {}
    failure = control.get("failure") or ""
    deliberate = DELIBERATE_FAILURE_MARK in failure
    return {
        "id": sid,
        "certifies": NEGATIVE_CONTROLS.get(sid, ""),
        "expected": "fail",
        "observed": control.get("status", "missing"),
        "failedOnItsOwnAssertion": deliberate,
        "recordedFailure": failure[:200],
        "harnessCanFail": control.get("status") == "fail" and deliberate,
    }


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
    # WHICH FAILURE, NOT JUST A FAILURE (review harness-rows-4 **D-L1**). Every
    # scenario here fails by raising `Failure`, and a `wait_for` timeout raises
    # exactly the same thing — so `status == "fail"` would certify the negative
    # control on a run in which the cluster never answered and NEG-1's false
    # assertion was never reached. That is the one failure mode a negative
    # control must not accept: it would stamp "this harness can fail" on a run
    # that only proved the cluster can be slow. The recorded failure must be the
    # deliberate one, by its own sentence.
    STATE["negativeControl"] = negative_control_verdict(STATE["scenarios"].get("NEG-1"))
    # AND EVERY OTHER ONE, per row. `negativeControl` stays as it was so a
    # reader (and `test_rows.py`) keeps the field it knows; `negativeControls`
    # is the whole set, and `certifiedRows` is the list a tracker update may
    # rely on — a row whose control did not fail on its OWN sentence is not
    # certified, however green the row itself is.
    STATE["negativeControls"] = [
        negative_control_verdict(STATE["scenarios"].get(sid), sid)
        for sid in sorted(NEGATIVE_CONTROLS)
    ]
    STATE["certifiedRows"] = sorted(
        NEGATIVE_CONTROLS[v["id"]] for v in STATE["negativeControls"] if v["harnessCanFail"]
    )
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
    # `acl_kafka.SECRET` carries a password this run GENERATED, which is the one
    # credential here that exists nowhere else: if it leaked into an artifact
    # nobody else could notice.
    for name in ("logweir-s3", "minio-root", "logweir-signing-key", acl_kafka.SECRET):
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
