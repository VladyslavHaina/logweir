#!/usr/bin/env python3
"""D2 W14 — live docker-desktop acceptance for the destination, discovery and
readiness contracts (PLAT-03.1, PLAT-03.2, PLAT-07.2, PLAT-08.1, PLAT-09.1).

This harness proves D2 §14's scenarios against a REAL cluster and the REAL
shared controller. Nothing here is a fake: every object is created with
`kubectl` against `docker-desktop`, reconciled by the `weirkeeper` Deployment
the lab release owns, executed by real runner Jobs, and read back by name and
by UID. A scenario that cannot run on this build is recorded `notRun` with the
reason; it is never recorded as a pass.

Run phases in order, with the same `D2W14_OUT` directory:

    export LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py setup
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py bulk
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py s1 s2 s3 ...
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py s1b s1c evf evf4 evf6   # D2 §3.9's Job
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py evf5    # CLUSTER LOCK: stops the controller
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py report
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py cleanup

The evidence-fetch rows (`s1b`, `s1c`, `evf*`) and what PASS requires for
each are tabled in `e2e/k8s/d3/README.md`, "lab-refresh-8's rows".

Environment:
    D2W14_OUT        private state and key material (mode 0700); default
                     /tmp/logweir-d2w14. NEVER inside the artifact tree.
    D2W14_ART        the artifact tree every reader sees; default
                     /tmp/logweir-roadmap-run/claude/artifacts/d2-live/<stamp>.
    D2W14_STAMP      the run stamp; default a UTC timestamp, then pinned in
                     state.json so a later phase joins the same run.

Safety rules this file enforces in code, not in prose:
  * every kubectl call carries `--context docker-desktop`;
  * every namespace it will touch starts with `lw-d2w14-`, asserted before the
    first create AND again before the delete, together with the owner label
    and the UID recorded at creation;
  * no credential value is ever written to an artifact: `redact()` runs over
    every captured string, and `sweep()` re-reads the whole artifact tree at
    the end and fails the run if a marker survives;
  * every subprocess has a timeout (a hung child blocks the whole wave).
"""

from __future__ import annotations

import argparse
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
from typing import Any, Callable, Iterable

CONTEXT = "docker-desktop"
# The D2 matrix runs as `d2w14`. U6 (the per-role minimal-permission
# measurement) is a SEPARATE worker with its own namespaces and its own owner
# label, so both are overridable — and both are read by `assert_safe_namespace`
# and by `cleanup`, which is what keeps a run from ever touching a namespace
# another owner created.
OWNER = os.environ.get("D2_OWNER", "d2w14")
OWNER_LABEL_KEY = "logweir.dev/test-owner"
NAMESPACE_PREFIX = os.environ.get("D2_NAMESPACE_PREFIX", "lw-d2w14-")
LAB_NS = "logweir-scram-local"

ROOT = pathlib.Path(__file__).resolve().parents[3]
OUT = pathlib.Path(os.environ.get("D2W14_OUT", "/tmp/logweir-d2w14"))
OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
OUT.chmod(0o700)
KEYS = OUT / "keys"
KEYS.mkdir(mode=0o700, parents=True, exist_ok=True)
KEYS.chmod(0o700)
STATE_PATH = OUT / "state.json"

KAFKA_IMAGE = "apache/kafka:3.7.1"
KAFKA_BIN = "/opt/kafka/bin"
MINIO_IMAGE = "minio/minio:latest"
MINIO_ROOT_USER = "d2w14root"
MC_IMAGE = "minio/mc:latest"

state: dict[str, Any] = (
    json.loads(STATE_PATH.read_text()) if STATE_PATH.is_file() else {}
)
if "stamp" not in state:
    state["stamp"] = os.environ.get(
        "D2W14_STAMP", dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dt%H%M%Sz")
    )
STAMP = state["stamp"]
NS = state.setdefault("namespace", NAMESPACE_PREFIX + STAMP)
NS_B = state.setdefault("namespaceB", NS + "-b")

ART = pathlib.Path(
    os.environ.get(
        "D2W14_ART",
        "/tmp/logweir-roadmap-run/claude/artifacts/d2-live/" + STAMP,
    )
)
ART.mkdir(parents=True, exist_ok=True)

CTX = ["kubectl", "--context", CONTEXT]
K = CTX + ["-n", NS]
KB = CTX + ["-n", NS_B]

# Credential values live here and NOWHERE else. `redact()` walks this dict.
SECRETS: dict[str, str] = {}


# --------------------------------------------------------------------------
# state, logging, redaction
# --------------------------------------------------------------------------


def save() -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))
    STATE_PATH.chmod(0o600)


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    line = f"{now()} {message}"
    print(line, flush=True)
    with (ART / "harness.log").open("a") as handle:
        handle.write(line + "\n")


def load_secrets() -> None:
    """Generate this run's credential values once, outside the artifact tree."""
    path = KEYS / "credentials.json"
    existing: dict[str, str] = (
        json.loads(path.read_text()) if path.is_file() else {}
    )
    values = {
        "kafka-admin": "A" + secrets.token_urlsafe(18),
        "kafka-limited": "L" + secrets.token_urlsafe(18),
        "kafka-rotating-1": "R" + secrets.token_urlsafe(18),
        "kafka-rotating-2": "S" + secrets.token_urlsafe(18),
        "kafka-empty-admin": "E" + secrets.token_urlsafe(18),
        "kafka-target-admin": "G" + secrets.token_urlsafe(18),
        "minio-root": "M" + secrets.token_urlsafe(20),
        "a-writer": "W" + secrets.token_urlsafe(20),
        "a-reader": "D" + secrets.token_urlsafe(20),
        "a-evidence-ro": "V" + secrets.token_urlsafe(20),
        # EVF-1's `archiveRead` principal: read-only on the archive prefix AND
        # `logweir/`, so an `ArchiveReadGrant` evidence read can succeed.
        "a-archread": "K" + secrets.token_urlsafe(20),
        "a-denied": "X" + secrets.token_urlsafe(20),
        "b-writer": "B" + secrets.token_urlsafe(20),
        "b-evidence-ro": "N" + secrets.token_urlsafe(20),
        "c-writer": "C" + secrets.token_urlsafe(20),
        "c-reader": "F" + secrets.token_urlsafe(20),
        "tls-writer": "T" + secrets.token_urlsafe(20),
        "tls-reader": "U" + secrets.token_urlsafe(20),
        # U6's one principal per role: a policy change then isolates exactly
        # one role, which is what makes a bisection row about that role.
        "u6-writer": "P" + secrets.token_urlsafe(20),
        "u6-reader": "Q" + secrets.token_urlsafe(20),
        "u6-evwriter": "Y" + secrets.token_urlsafe(20),
        "u6-evreader": "Z" + secrets.token_urlsafe(20),
        "u6-deleter": "H" + secrets.token_urlsafe(20),
        "u6-catreader": "J" + secrets.token_urlsafe(20),
        "keystore": secrets.token_urlsafe(18),
    }
    # A later phase may need a credential an earlier one did not: keep every
    # value that already exists (objects in the cluster carry it) and mint only
    # the ones that are new.
    values.update(existing)
    if values != existing:
        path.write_text(json.dumps(values, indent=2, sort_keys=True))
        path.chmod(0o600)
    SECRETS.update(values)


def redact(text: str) -> str:
    """Remove every credential value, in plain and base64 form, plus any PEM
    private key, from a string bound for an artifact."""
    for value in SECRETS.values():
        if not value:
            continue
        text = text.replace(value, "[REDACTED]")
        text = text.replace(base64.b64encode(value.encode()).decode(), "[REDACTED]")
    return re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.DOTALL,
    )


def artifact(name: str, body: Any) -> pathlib.Path:
    path = ART / name
    path.parent.mkdir(parents=True, exist_ok=True)
    text = body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True, default=str)
    path.write_text(redact(text))
    return path


def digest(data: str | bytes) -> str:
    raw = data.encode() if isinstance(data, str) else data
    return "sha256:" + hashlib.sha256(raw).hexdigest()


# --------------------------------------------------------------------------
# kubectl
# --------------------------------------------------------------------------


def run(args: list[str], *, data: str | None = None, check: bool = True, timeout: int = 180):
    started = time.monotonic()
    proc = subprocess.run(
        args, input=data, text=True, capture_output=True, cwd=ROOT, timeout=timeout
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            "command failed ("
            + str(proc.returncode)
            + "): "
            + " ".join(args)
            + "\nstdout: "
            + redact(proc.stdout)
            + "\nstderr: "
            + redact(proc.stderr)
        )
    proc.elapsed = time.monotonic() - started  # type: ignore[attr-defined]
    return proc


def assert_safe_namespace(name: str) -> None:
    if not name.startswith(NAMESPACE_PREFIX):
        raise RuntimeError(
            f"refusing to touch namespace {name!r}: it does not start with {NAMESPACE_PREFIX!r}"
        )


def owned(name: str, namespace: str = "") -> dict[str, Any]:
    meta: dict[str, Any] = {"name": name, "labels": {OWNER_LABEL_KEY: OWNER}}
    meta["namespace"] = namespace or NS
    return meta


def apply(obj: dict[str, Any], *, namespace: str = "") -> dict[str, Any]:
    ns = namespace or obj.get("metadata", {}).get("namespace") or NS
    assert_safe_namespace(ns)
    proc = run(CTX + ["-n", ns, "apply", "-f", "-", "-o", "json"], data=json.dumps(obj))
    return json.loads(proc.stdout)


def apply_expect_failure(obj: dict[str, Any], *, namespace: str = "") -> str:
    ns = namespace or obj.get("metadata", {}).get("namespace") or NS
    assert_safe_namespace(ns)
    proc = run(
        CTX + ["-n", ns, "apply", "-f", "-"], data=json.dumps(obj), check=False
    )
    if proc.returncode == 0:
        raise RuntimeError("expected a refusal, but the apply succeeded:\n" + proc.stdout)
    return proc.stderr


def get(kind: str, name: str, namespace: str = "") -> dict[str, Any]:
    ns = namespace or NS
    return json.loads(run(CTX + ["-n", ns, "get", kind, name, "-o", "json"]).stdout)


def get_opt(kind: str, name: str, namespace: str = "") -> dict[str, Any] | None:
    ns = namespace or NS
    proc = run(CTX + ["-n", ns, "get", kind, name, "-o", "json"], check=False)
    return json.loads(proc.stdout) if proc.returncode == 0 else None


def get_list(kind: str, namespace: str = "", selector: str = "") -> list[dict[str, Any]]:
    ns = namespace or NS
    args = CTX + ["-n", ns, "get", kind, "-o", "json"]
    if selector:
        args += ["-l", selector]
    return json.loads(run(args).stdout).get("items", [])


def wait_for(
    kind: str,
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout: int = 180,
    namespace: str = "",
    what: str = "",
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = get_opt(kind, name, namespace)
        if last is not None and predicate(last):
            return last
        time.sleep(2)
    raise TimeoutError(
        f"{kind}/{name} never satisfied {what or 'the predicate'} within {timeout}s; "
        + "last status: "
        + redact(json.dumps((last or {}).get("status", {}), sort_keys=True))[:2000]
    )


def terminal_phase(obj: dict[str, Any]) -> bool:
    return obj.get("status", {}).get("phase") in {
        "Succeeded",
        "Failed",
        "Cancelled",
        "Refused",
    }


def wait_pod_ready(label: str, *, timeout: int = 240, namespace: str = "") -> None:
    ns = namespace or NS
    run(
        CTX
        + ["-n", ns, "wait", "--for=condition=Ready", "pod", "-l", label,
           f"--timeout={timeout}s"],
        timeout=timeout + 30,
    )


def pod_logs_for_job(job: str, *, tail: int = 200, namespace: str = "") -> str:
    ns = namespace or NS
    pods = json.loads(
        run(
            CTX + ["-n", ns, "get", "pods", "-l", f"batch.kubernetes.io/job-name={job}",
                   "-o", "json"],
            check=False,
        ).stdout
        or '{"items":[]}'
    ).get("items", [])
    out = []
    for pod in pods:
        proc = run(
            CTX + ["-n", ns, "logs", pod["metadata"]["name"], f"--tail={tail}"],
            check=False,
            timeout=60,
        )
        out.append(f"--- {pod['metadata']['name']} ---\n{proc.stdout}{proc.stderr}")
    return redact("\n".join(out))


def controller_log(since: str = "20m") -> str:
    pods = json.loads(
        run(
            CTX + ["-n", LAB_NS, "get", "pods", "-l",
                   "app.kubernetes.io/component=control-plane", "-o", "json"]
        ).stdout
    )["items"]
    chunks = []
    for pod in pods:
        proc = run(
            CTX + ["-n", LAB_NS, "logs", pod["metadata"]["name"], f"--since={since}"],
            check=False,
            timeout=120,
        )
        chunks.append(proc.stdout)
    return redact("\n".join(chunks))


# --------------------------------------------------------------------------
# results
# --------------------------------------------------------------------------

RESULTS_PATH = ART / "results.json"


def load_results() -> dict[str, Any]:
    if RESULTS_PATH.is_file():
        return json.loads(RESULTS_PATH.read_text())
    return {
        "harness": "e2e/k8s/d2/d2_live.py",
        "kubeContext": CONTEXT,
        "owner": OWNER,
        "stamp": STAMP,
        "namespaces": {"a": NS, "b": NS_B},
        "scenarios": {},
    }


def record(
    scenario: str,
    title: str,
    outcome: str,
    *,
    detail: Any = None,
    commands: Iterable[str] = (),
    seconds: float | None = None,
    reason: str = "",
) -> None:
    if outcome not in {"pass", "fail", "notRun"}:
        raise ValueError(outcome)
    results = load_results()
    results["scenarios"][scenario] = {
        "id": scenario,
        "title": title,
        "outcome": outcome,
        "reason": reason,
        "recordedAt": now(),
        "seconds": round(seconds, 1) if seconds is not None else None,
        "commands": list(commands),
        "detail": json.loads(redact(json.dumps(detail, default=str, sort_keys=True)))
        if detail is not None
        else None,
    }
    RESULTS_PATH.write_text(json.dumps(results, indent=2, sort_keys=True))
    log(f"[{outcome.upper():6}] {scenario} — {title}" + (f" ({reason})" if reason else ""))


class Scenario:
    """A scenario that records `fail` with the exception when it raises, so a
    broken assertion is evidence and not a lost run."""

    def __init__(self, sid: str, title: str) -> None:
        self.sid = sid
        self.title = title
        self.detail: dict[str, Any] = {}
        self.commands: list[str] = []
        self.started = 0.0

    def __enter__(self) -> "Scenario":
        self.started = time.monotonic()
        log(f"---- {self.sid}: {self.title}")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        seconds = time.monotonic() - self.started
        if exc is None:
            record(self.sid, self.title, "pass", detail=self.detail,
                   commands=self.commands, seconds=seconds)
            return False
        self.detail["exception"] = redact(f"{exc_type.__name__}: {exc}")
        record(self.sid, self.title, "fail", detail=self.detail,
               commands=self.commands, seconds=seconds,
               reason=redact(str(exc))[:400])
        return True


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


# --------------------------------------------------------------------------
# fixtures: a private CA, three MinIOs, two Kafkas
# --------------------------------------------------------------------------


def openssl(*args: str) -> subprocess.CompletedProcess:
    return run(["openssl", *args], timeout=120)


def generate_ca_and_server_certificate() -> dict[str, Any]:
    """One CA that signs `minio-tls`'s certificate and one that signs nothing.

    The second exists so the negative control for a CA bundle is a real TRUST
    failure rather than a missing file. Both private keys stay under KEYS and
    reach no Kubernetes object and no artifact."""
    ca = KEYS / "ca.crt"
    if not ca.is_file():
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-keyout", str(KEYS / "ca.key"), "-out", str(ca),
                "-subj", "/CN=logweir-d2w14-ca")
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-keyout", str(KEYS / "wrong-ca.key"), "-out", str(KEYS / "wrong-ca.crt"),
                "-subj", "/CN=logweir-d2w14-wrong-ca")
        openssl("req", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(KEYS / "server.key"), "-out", str(KEYS / "server.csr"),
                "-subj", f"/CN=minio-tls.{NS}.svc")
        (KEYS / "san.cnf").write_text(
            f"subjectAltName = DNS:minio-tls.{NS}.svc, "
            f"DNS:minio-tls.{NS}.svc.cluster.local, DNS:minio-tls\n"
            "extendedKeyUsage = serverAuth\n"
        )
        openssl("x509", "-req", "-in", str(KEYS / "server.csr"),
                "-CA", str(ca), "-CAkey", str(KEYS / "ca.key"), "-CAcreateserial",
                "-out", str(KEYS / "server.crt"), "-days", "2",
                "-extfile", str(KEYS / "san.cnf"))
        for name in ["ca.key", "wrong-ca.key", "server.key"]:
            (KEYS / name).chmod(0o600)
    artifact("certs/ca.crt", ca.read_text())
    artifact("certs/wrong-ca.crt", (KEYS / "wrong-ca.crt").read_text())
    artifact("certs/server.crt", (KEYS / "server.crt").read_text())
    subject = run(["openssl", "x509", "-in", str(KEYS / "server.crt"), "-noout",
                   "-subject", "-issuer", "-ext", "subjectAltName"]).stdout
    artifact("certs/server-certificate.txt", subject)
    return {
        "caSha256": digest(ca.read_bytes()),
        "wrongCaSha256": digest((KEYS / "wrong-ca.crt").read_bytes()),
        "serverCertSha256": digest((KEYS / "server.crt").read_bytes()),
        "privateKeysLiveIn": str(KEYS),
    }


def literal_secret(name: str, values: dict[str, str], *, namespace: str = "") -> dict[str, Any]:
    created = apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned(name, namespace or NS),
            "type": "Opaque",
            "data": {k: base64.b64encode(v.encode()).decode() for k, v in values.items()},
        },
        namespace=namespace or NS,
    )
    return {"name": name, "uid": created["metadata"]["uid"], "keys": sorted(values)}


def config_map(name: str, values: dict[str, str], *, namespace: str = "") -> dict[str, Any]:
    created = apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned(name, namespace or NS),
            "data": values,
        },
        namespace=namespace or NS,
    )
    return {"name": name, "uid": created["metadata"]["uid"]}


def kafka_objects(name: str, cluster_id: str, *, heap: str, memory: str) -> list[dict[str, Any]]:
    """A KRaft broker with a SASL_PLAINTEXT SCRAM-SHA-512 listener on 9096 and
    an in-pod-only PLAINTEXT listener on 9092.

    `StandardAuthorizer` with `allow.everyone.if.no.acl.found=false` is the
    point of the fixture: an ACL-limited principal must be a real authorization
    outcome, not a configuration that happens to hide topics. `ANONYMOUS` is a
    super user so the broker's own inter-broker traffic and the in-pod admin
    tools work; every dial from outside the pod is SASL."""
    host = f"{name}.{NS}.svc.cluster.local"
    env = [
        ("CLUSTER_ID", cluster_id),
        ("KAFKA_NODE_ID", "1"),
        ("KAFKA_PROCESS_ROLES", "broker,controller"),
        ("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SASL://0.0.0.0:9096"),
        ("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://localhost:9092,SASL://{host}:9096"),
        ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
        ("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
         "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT"),
        ("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512"),
        ("KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG",
         "org.apache.kafka.common.security.scram.ScramLoginModule required;"),
        ("KAFKA_AUTHORIZER_CLASS_NAME",
         "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
        ("KAFKA_SUPER_USERS", "User:ANONYMOUS"),
        ("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false"),
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_NUM_PARTITIONS", "1"),
        ("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs"),
        ("KAFKA_HEAP_OPTS", heap),
    ]
    return [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": owned(name),
            "spec": {
                "selector": {"app": name},
                "ports": [{"name": "sasl", "port": 9096, "targetPort": 9096}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": owned(name),
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name, OWNER_LABEL_KEY: OWNER}},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "kafka",
                                "image": KAFKA_IMAGE,
                                "imagePullPolicy": "Never",
                                "env": [{"name": k, "value": v} for k, v in env],
                                "readinessProbe": {
                                    "tcpSocket": {"port": 9096},
                                    "initialDelaySeconds": 8,
                                    "periodSeconds": 3,
                                },
                                "resources": {
                                    "requests": {"cpu": "200m", "memory": "512Mi"},
                                    "limits": {"memory": memory},
                                },
                            }
                        ],
                    },
                },
            },
        },
    ]


def minio_objects(name: str, *, tls: bool) -> list[dict[str, Any]]:
    """A MinIO with its own root credential. `minio-tls` serves HTTPS with the
    certificate this run's private CA signed; the other two serve HTTP, which
    is what makes `transport.security` a real difference between destinations
    rather than a field nobody reads."""
    container: dict[str, Any] = {
        "name": "minio",
        "image": MINIO_IMAGE,
        "imagePullPolicy": "Never",
        "args": ["server", "/data"],
        "env": [
            {"name": "MINIO_ROOT_USER", "value": MINIO_ROOT_USER},
            {
                "name": "MINIO_ROOT_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": "minio-root", "key": "password"}},
            },
        ],
        "resources": {
            "requests": {"cpu": "50m", "memory": "192Mi"},
            "limits": {"memory": "768Mi"},
        },
        "readinessProbe": {
            "tcpSocket": {"port": 9000},
            "initialDelaySeconds": 3,
            "periodSeconds": 2,
        },
    }
    volumes: list[dict[str, Any]] = []
    if tls:
        container["args"] = ["server", "/data", "--certs-dir", "/certs"]
        container["volumeMounts"] = [
            {"name": "certs", "mountPath": "/certs", "readOnly": True}
        ]
        volumes = [
            {
                "name": "certs",
                "secret": {"secretName": "minio-tls-cert", "defaultMode": 0o400},
            }
        ]
    return [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": owned(name),
            "spec": {
                "selector": {"app": name},
                "ports": [{"name": "s3", "port": 9000, "targetPort": 9000}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": owned(name),
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name, OWNER_LABEL_KEY: OWNER}},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "volumes": volumes,
                        "containers": [container],
                    },
                },
            },
        },
    ]


def mc_pod() -> dict[str, Any]:
    """One `mc` client with an alias per MinIO, so every bucket assertion in
    this file is made by a real S3 client against the real store."""
    alias = (
        "for i in $(seq 1 90); do "
        "  mc alias set %s %s " + MINIO_ROOT_USER + " \"$ROOT\" >/dev/null 2>&1 && break; sleep 2; "
        "done; mc alias set %s %s " + MINIO_ROOT_USER + " \"$ROOT\" >/dev/null"
    )
    script = "; ".join(
        [
            "set -e",
            "mkdir -p $MC_CONFIG_DIR/certs/CAs",
            "cp /ca/ca.crt $MC_CONFIG_DIR/certs/CAs/d2w14-ca.crt",
            alias % ("a", f"http://minio-a.{NS}.svc:9000", "a", f"http://minio-a.{NS}.svc:9000"),
            alias % ("b", f"http://minio-b.{NS}.svc:9000", "b", f"http://minio-b.{NS}.svc:9000"),
            alias % ("t", f"https://minio-tls.{NS}.svc:9000", "t", f"https://minio-tls.{NS}.svc:9000"),
            "touch /tmp/ready",
            "sleep 10800",
        ]
    )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": owned("mc"),
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "volumes": [{"name": "ca", "configMap": {"name": "minio-ca"}}],
            "containers": [
                {
                    "name": "mc",
                    "image": MC_IMAGE,
                    "imagePullPolicy": "Never",
                    "command": ["/bin/sh", "-c"],
                    "args": [script],
                    "volumeMounts": [{"name": "ca", "mountPath": "/ca", "readOnly": True}],
                    "env": [
                        {"name": "MC_CONFIG_DIR", "value": "/tmp/mcconfig"},
                        {
                            "name": "ROOT",
                            "valueFrom": {
                                "secretKeyRef": {"name": "minio-root", "key": "password"}
                            },
                        },
                    ],
                    "readinessProbe": {
                        "exec": {"command": ["test", "-f", "/tmp/ready"]},
                        "periodSeconds": 1,
                    },
                }
            ],
        },
    }


def mc(*args: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    # `kubectl exec` occasionally dies with "error stream protocol error" on
    # this host. That is a fact about the connection and never about the store,
    # so it is retried rather than recorded as a measurement.
    last = None
    for attempt_no in range(3):
        proc = run(K + ["exec", "mc", "--", "mc", *args], check=False, timeout=timeout)
        if proc.returncode == 0:
            return proc
        last = proc
        if "error stream protocol error" not in (proc.stderr + proc.stdout):
            break
        time.sleep(2 * (attempt_no + 1))
    if check and last is not None and last.returncode != 0:
        raise RuntimeError(
            "mc failed (" + str(last.returncode) + "): mc " + " ".join(args)
            + "\nstdout: " + redact(last.stdout) + "\nstderr: " + redact(last.stderr))
    return last  # type: ignore[return-value]


def broker_exec(pod_label: str, args: list[str], *, check: bool = True, timeout: int = 300):
    pods = get_list("pods", selector=f"app={pod_label}")
    running = [p for p in pods if p["status"]["phase"] == "Running"]
    if not running:
        raise RuntimeError(f"no running pod for app={pod_label}")
    name = running[0]["metadata"]["name"]
    return run(K + ["exec", name, "--", *args], check=check, timeout=timeout)


def set_scram_credential(pod_label: str, user: str, password: str) -> None:
    """Set a SCRAM-SHA-512 credential on THIS RUN's own broker over its in-pod
    PLAINTEXT listener. Nothing is printed."""
    broker_exec(
        pod_label,
        [
            f"{KAFKA_BIN}/kafka-configs.sh", "--bootstrap-server", "localhost:9092",
            "--alter", "--add-config", f"SCRAM-SHA-512=[password={password}]",
            "--entity-type", "users", "--entity-name", user,
        ],
    )


MINIO_POLICIES: dict[str, dict[str, Any]] = {}


def _policy(bucket: str, *, write: bool, prefixes: list[str]) -> dict[str, Any]:
    actions = ["s3:GetObject"]
    if write:
        actions += ["s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            # `s3:GetBucketLocation` takes no `s3:prefix` condition (MinIO
            # refuses the policy outright), so the two live in separate
            # statements and only the listing is prefix-bounded.
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation"],
                "Resource": [f"arn:aws:s3:::{bucket}"],
            },
            (
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                    "Condition": {"StringLike": {"s3:prefix": [p + "*" for p in prefixes]}},
                }
                if all(prefixes)
                # A whole-bucket principal takes NO `s3:prefix` condition: a
                # `list-type=2` with no prefix parameter sends no such key, and
                # `StringLike` on an absent key does not match, so the condition
                # would deny the very listing it means to allow.
                else {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                }
            ),
            {
                "Effect": "Allow",
                "Action": actions,
                "Resource": [f"arn:aws:s3:::{bucket}/{p}*" for p in prefixes],
            },
        ],
    }


def minio_user(alias: str, user: str, policy_name: str, policy: dict[str, Any] | None) -> None:
    run(
        K + ["exec", "-i", "mc", "--", "sh", "-c",
             f"mc admin user add {alias} {user} \"$(cat)\" >/dev/null"],
        data=SECRETS[user],
        timeout=120,
    )
    if policy is None:
        return
    run(
        K + ["exec", "-i", "mc", "--", "sh", "-c",
             f"cat > /tmp/{policy_name}.json"],
        data=json.dumps(policy),
        timeout=60,
    )
    mc("admin", "policy", "create", alias, policy_name, f"/tmp/{policy_name}.json")
    mc("admin", "policy", "attach", alias, policy_name, "--user", user)
    MINIO_POLICIES[user] = policy


def destination(
    name: str,
    *,
    bucket: str,
    prefix: str,
    endpoint: str | None,
    security: str,
    addressing: str = "PathStyle",
    ca_bundle: str | None = None,
    region: str | None = "us-east-1",
    archive_write: str,
    archive_read: str | None = None,
    evidence_write: str | None = None,
    evidence_read: str | None = None,
    evidence_read_mode: str = "SecretKeys",
    write_probe: bool = False,
    description: str = "",
) -> dict[str, Any]:
    def grant(secret_name: str | None, mode: str = "SecretKeys") -> dict[str, Any]:
        if mode != "SecretKeys":
            return {"mode": mode}
        return {
            "mode": "SecretKeys",
            "secret": {
                "name": secret_name,
                "accessKeyIdKey": "access-key-id",
                "secretAccessKeyKey": "secret-access-key",
            },
        }

    storage: dict[str, Any] = {
        "provider": "S3",
        "bucket": bucket,
        "prefix": prefix,
        "addressing": addressing,
    }
    if endpoint:
        storage["endpoint"] = endpoint
    if region:
        storage["region"] = region
    transport: dict[str, Any] = {"security": security}
    if ca_bundle:
        transport["caBundle"] = {"configMapName": ca_bundle, "key": "ca.crt"}
    access: dict[str, Any] = {"archiveWrite": grant(archive_write)}
    if archive_read:
        access["archiveRead"] = grant(archive_read)
    if evidence_write:
        access["evidenceWrite"] = grant(evidence_write)
    if evidence_read or evidence_read_mode != "SecretKeys":
        access["evidenceRead"] = grant(evidence_read, evidence_read_mode)
    spec: dict[str, Any] = {"storage": storage, "transport": transport, "access": access}
    if write_probe:
        # The ONLY thing that makes `destination.evidenceWritable` a probed row
        # rather than `WriteNotProbed` (`check/kinds/access.rs`).
        spec["readiness"] = {"writeProbe": "CreateOnlyMarker"}
    if description:
        spec["description"] = description
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupDestination",
        "metadata": owned(name),
        "spec": spec,
    }


def kafka_cluster(
    name: str,
    *,
    servers: list[str],
    username: str | None = None,
    secret: str | None = None,
    password_key: str | None = None,
    role: str = "source",
    namespace: str = "",
) -> dict[str, Any]:
    auth: dict[str, Any] = {"mode": "scramSha512" if username else "plaintext", "tls": False}
    if username:
        auth["username"] = username
    if secret:
        ref: dict[str, Any] = {"name": secret}
        if password_key:
            ref["passwordKey"] = password_key
        auth["secretRef"] = ref
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": owned(name, namespace or NS),
        "spec": {"bootstrapServers": servers, "auth": auth, "role": role},
    }


def backup(
    name: str,
    *,
    source: str,
    topics: list[str],
    destination_ref: str | None = None,
    archive: dict[str, Any] | None = None,
    deadline: int = 600,
    namespace: str = "",
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "sourceRef": {"name": source},
        "topics": topics,
        "triggeredBy": "manual",
        "deadlineSeconds": deadline,
    }
    if destination_ref:
        # D2 §3.6: `archive` stays REQUIRED so an older controller can still
        # decode the object; a destination-backed Backup spells the sentinel
        # and carries no secretRef of its own.
        spec["destinationRef"] = {"name": destination_ref}
        spec["archive"] = {"url": "logweir-destination://" + destination_ref}
    if archive:
        spec["archive"] = archive
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": owned(name, namespace or NS),
        "spec": spec,
    }


def topic_discovery(
    name: str,
    connection: str,
    *,
    max_topics: int | None = None,
    timeout_seconds: int | None = None,
    include_internal: bool | None = None,
    expected: list[str] | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {"connectionRef": {"name": connection}}
    if max_topics is not None:
        request["maxTopics"] = max_topics
    if timeout_seconds is not None:
        request["timeoutSeconds"] = timeout_seconds
    if include_internal is not None:
        request["includeInternal"] = include_internal
    if expected is not None:
        request["expectedTopics"] = expected
    meta = owned(name)
    # `logweir-api`'s per-connection list is a LABEL read
    # (`routes/topic_discoveries.rs:85`), so a discovery created with kubectl
    # and no label is invisible on the console's connection page even though
    # the controller reconciles it. The console sets this label on every
    # discovery it starts; this harness sets the same one so the UI journey
    # reads the states this file created rather than an empty panel.
    meta["labels"]["logweir.dev/connection"] = connection
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TopicDiscovery",
        "metadata": meta,
        "spec": {"request": request},
    }


def preflight(name: str, request: dict[str, Any], *, timeout_seconds: int | None = None) -> dict[str, Any]:
    body = dict(request)
    if timeout_seconds is not None:
        body["timeoutSeconds"] = timeout_seconds
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Preflight",
        "metadata": owned(name),
        "spec": {"request": body},
    }


# --------------------------------------------------------------------------
# setup
# --------------------------------------------------------------------------

BUCKETS = {"a": "lw-a", "b": "lw-b", "t": "lw-tls"}
ARCHIVE_PREFIX = "team/prod"


def create_namespace(name: str) -> dict[str, Any]:
    assert_safe_namespace(name)
    existing = json.loads(
        run(CTX + ["get", "namespace", name, "-o", "json"], check=False).stdout or "{}"
    )
    if existing.get("metadata"):
        return existing
    run(CTX + ["create", "namespace", name])
    run(CTX + ["label", "namespace", name, f"{OWNER_LABEL_KEY}={OWNER}"])
    return json.loads(run(CTX + ["get", "namespace", name, "-o", "json"]).stdout)


def setup() -> None:
    load_secrets()
    log(f"setup: namespaces {NS} and {NS_B}")
    for name, key in ((NS, "namespaceUid"), (NS_B, "namespaceBUid")):
        ns = create_namespace(name)
        state[key] = ns["metadata"]["uid"]
    save()
    certs = generate_ca_and_server_certificate()
    state["certs"] = certs
    save()

    # --- secrets -------------------------------------------------------
    created: dict[str, Any] = {}
    created["minio-root"] = literal_secret("minio-root", {"password": SECRETS["minio-root"]})
    created["minio-tls-cert"] = apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned("minio-tls-cert"),
            "type": "Opaque",
            "data": {
                "public.crt": base64.b64encode((KEYS / "server.crt").read_bytes()).decode(),
                "private.key": base64.b64encode((KEYS / "server.key").read_bytes()).decode(),
            },
        }
    )["metadata"]["uid"]
    created["minio-ca"] = config_map("minio-ca", {"ca.crt": (KEYS / "ca.crt").read_text()})
    created["minio-wrong-ca"] = config_map(
        "minio-wrong-ca", {"ca.crt": (KEYS / "wrong-ca.crt").read_text()}
    )
    for user in ["a-writer", "a-reader", "a-evidence-ro", "a-denied",
                 "b-writer", "b-evidence-ro", "tls-writer", "tls-reader"]:
        created[user] = literal_secret(
            user, {"access-key-id": user, "secret-access-key": SECRETS[user]}
        )
    created["kafka-admin"] = literal_secret("kafka-admin", {"password": SECRETS["kafka-admin"]})
    created["kafka-limited"] = literal_secret("kafka-limited", {"password": SECRETS["kafka-limited"]})
    created["kafka-rotating"] = literal_secret(
        "kafka-rotating", {"password": SECRETS["kafka-rotating-1"]}
    )
    created["kafka-empty-admin"] = literal_secret(
        "kafka-empty-admin", {"password": SECRETS["kafka-empty-admin"]}
    )
    created["kafka-target-admin"] = literal_secret(
        "kafka-target-admin", {"password": SECRETS["kafka-target-admin"]}
    )
    # The runner signs every receipt with the key the cluster-scoped roster
    # already carries, so evidence verifies without touching that shared object.
    keys = approver_material()
    created["logweir-signing-key"] = apply({
        "apiVersion": "v1", "kind": "Secret", "metadata": owned("logweir-signing-key"),
        "type": "Opaque",
        "data": {"signing.pem": base64.b64encode(keys["signing"].read_bytes()).decode()},
    })["metadata"]["uid"]
    apply({
        "apiVersion": "v1", "kind": "ServiceAccount",
        "metadata": owned("logweir-runner"),
        "automountServiceAccountToken": False,
    })
    created["kafka-wrong-password"] = literal_secret(
        "kafka-wrong-password", {"password": "not-the-password-" + secrets.token_hex(4)}
    )
    created["kafka-other-key"] = literal_secret(
        "kafka-other-key", {"other": SECRETS["kafka-admin"]}
    )
    state["secrets"] = created
    save()

    # --- workloads -----------------------------------------------------
    objects: list[dict[str, Any]] = []
    objects += kafka_objects("kafka-acl", "D2W14AclAAAAAAAAAAAAAA", heap="-Xmx1400M -Xms512M",
                             memory="2Gi")
    objects += kafka_objects("kafka-empty", "D2W14EmptyAAAAAAAAAAAA", heap="-Xmx512M -Xms256M",
                             memory="1Gi")
    # A THIRD broker, because a restore target that is the source is
    # `TargetEqualsSource` and because `kafka-empty` has to STAY empty for S10.
    objects += kafka_objects("kafka-target", "D2W14TargetAAAAAAAAAAA", heap="-Xmx512M -Xms256M",
                             memory="1Gi")
    objects += minio_objects("minio-a", tls=False)
    objects += minio_objects("minio-b", tls=False)
    objects += minio_objects("minio-tls", tls=True)
    for obj in objects:
        apply(obj)
    for app in ["kafka-acl", "kafka-empty", "kafka-target", "minio-a", "minio-b", "minio-tls"]:
        wait_pod_ready(f"app={app}", timeout=300)
        log(f"setup: {app} ready")
    run(K + ["delete", "pod", "mc", "--ignore-not-found", "--wait=true"], timeout=120)
    apply(mc_pod())
    run(K + ["wait", "--for=condition=Ready", "pod/mc", "--timeout=300s"], timeout=330)
    log("setup: mc ready")

    # --- buckets, users, policies --------------------------------------
    for alias, bucket in BUCKETS.items():
        mc("mb", f"{alias}/{bucket}", check=False)
    minio_user("a", "a-writer", "d2w14-a-writer",
               _policy("lw-a", write=True, prefixes=[ARCHIVE_PREFIX + "/", "logweir/"]))
    minio_user("a", "a-reader", "d2w14-a-reader",
               _policy("lw-a", write=False, prefixes=[ARCHIVE_PREFIX + "/"]))
    minio_user("a", "a-evidence-ro", "d2w14-a-evidence-ro",
               _policy("lw-a", write=False, prefixes=["logweir/"]))
    minio_user("a", "a-denied", "d2w14-a-denied", None)
    minio_user("b", "b-writer", "d2w14-b-writer",
               _policy("lw-b", write=True, prefixes=[ARCHIVE_PREFIX + "/", "logweir/"]))
    minio_user("b", "b-evidence-ro", "d2w14-b-evidence-ro",
               _policy("lw-b", write=False, prefixes=["logweir/"]))
    minio_user("t", "tls-writer", "d2w14-tls-writer",
               _policy("lw-tls", write=True, prefixes=[ARCHIVE_PREFIX + "/", "logweir/"]))
    minio_user("t", "tls-reader", "d2w14-tls-reader",
               _policy("lw-tls", write=False, prefixes=[ARCHIVE_PREFIX + "/", "logweir/"]))
    artifact("fixtures/minio-policies.json", MINIO_POLICIES)
    log("setup: MinIO users and policies created")

    # --- SCRAM users, topics, ACLs --------------------------------------
    for user, key in (("admin", "kafka-admin"), ("limited", "kafka-limited"),
                      ("rotating", "kafka-rotating-1")):
        set_scram_credential("kafka-acl", user, SECRETS[key])
    set_scram_credential("kafka-empty", "admin", SECRETS["kafka-empty-admin"])
    set_scram_credential("kafka-target", "admin", SECRETS["kafka-target-admin"])
    for broker in ("kafka-empty", "kafka-target"):
        broker_exec(broker, [
            f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
            "--add", "--allow-principal", "User:admin", "--operation", "All",
            "--cluster", "--topic", "*", "--group", "*",
        ], check=False)
    for topic, partitions in (("orders", 3), ("payments", 3), ("audit", 1)):
        broker_exec("kafka-acl", [
            f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
            "--create", "--topic", topic, "--partitions", str(partitions),
            "--replication-factor", "1",
        ], check=False)
    if not state.get("seeded"):
        for topic in ("orders", "payments", "audit"):
            payload = "\n".join(f"{topic}-record-{i:04d}" for i in range(200)) + "\n"
            pods = get_list("pods", selector="app=kafka-acl")
            pod = [p for p in pods if p["status"]["phase"] == "Running"][0]["metadata"]["name"]
            run(
                K + ["exec", "-i", pod, "--", f"{KAFKA_BIN}/kafka-console-producer.sh",
                     "--bootstrap-server", "localhost:9092", "--topic", topic],
                data=payload, timeout=180,
            )
        state["seeded"] = True
        save()
        log("setup: three topics seeded with 200 records each")
    else:
        log("setup: records already seeded; not producing again")
    for op in (["--operation", "Describe", "--operation", "Read"],):
        broker_exec("kafka-acl", [
            f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
            "--add", "--allow-principal", "User:limited", *op, "--topic", "orders",
        ])
    broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
        "--add", "--allow-principal", "User:limited", "--operation", "Read",
        "--group", "*",
    ])
    broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
        "--add", "--allow-principal", "User:rotating", "--operation", "Describe",
        "--topic", "*",
    ])
    broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
        "--add", "--allow-principal", "User:admin", "--operation", "All",
        "--cluster", "--topic", "*", "--group", "*",
    ], check=False)
    for principal in ("admin", "limited", "rotating"):
        broker_exec("kafka-acl", [
            f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
            "--add", "--allow-principal", f"User:{principal}", "--operation", "Describe",
            "--cluster",
        ], check=False)
    acls = broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092", "--list",
    ]).stdout
    artifact("fixtures/kafka-acls.txt", acls)
    # One consumer group commit, so `__consumer_offsets` exists for S8.
    broker_exec("kafka-acl", [
        "/bin/sh", "-c",
        f"{KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server localhost:9092 "
        "--topic orders --from-beginning --max-messages 1 --group d2w14-group "
        "--timeout-ms 15000 >/dev/null 2>&1 || true",
    ], check=False)
    topics = broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
        "--list", "--exclude-internal",
    ]).stdout
    artifact("fixtures/kafka-topics-user.txt", topics)
    log("setup: ACLs applied")

    # --- saved connections ---------------------------------------------
    acl_servers = [f"kafka-acl.{NS}.svc.cluster.local:9096"]
    empty_servers = [f"kafka-empty.{NS}.svc.cluster.local:9096"]
    connections = [
        kafka_cluster("source-admin", servers=acl_servers, username="admin", secret="kafka-admin"),
        kafka_cluster("source-limited", servers=acl_servers, username="limited",
                      secret="kafka-limited"),
        kafka_cluster("source-rotating", servers=acl_servers, username="rotating",
                      secret="kafka-rotating"),
        kafka_cluster("target", servers=[f"kafka-target.{NS}.svc.cluster.local:9096"],
                      username="admin", secret="kafka-target-admin", role="target"),
        kafka_cluster("empty", servers=empty_servers, username="admin",
                      secret="kafka-empty-admin"),
        kafka_cluster("blackhole", servers=["10.255.255.1:9096"], username="admin",
                      secret="kafka-admin"),
        kafka_cluster("missing-secret", servers=acl_servers, username="admin",
                      secret="missing-secret"),
        kafka_cluster("missing-key", servers=acl_servers, username="admin",
                      secret="kafka-other-key"),
        kafka_cluster("wrong-password", servers=acl_servers, username="admin",
                      secret="kafka-wrong-password"),
    ]
    for obj in connections:
        apply(obj)
    for name in ("source-admin", "target", "empty"):
        found = wait_for("kafkacluster", name,
                         lambda o: o.get("status", {}).get("reachable") is True,
                         timeout=240, what="reachable=true")
        state.setdefault("clusterIds", {})[name] = found["status"].get("clusterId")
    save()
    log("setup: connections probed: " + json.dumps(state["clusterIds"]))

    # --- destinations ---------------------------------------------------
    dests = [
        destination("dest-a", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                    endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                    archive_write="a-writer", archive_read="a-reader",
                    evidence_write="a-writer", evidence_read="a-evidence-ro",
                    description="MinIO A, plain HTTP, three distinct credentials"),
        destination("dest-b", bucket="lw-b", prefix=ARCHIVE_PREFIX,
                    endpoint=f"http://minio-b.{NS}.svc:9000", security="InsecureHTTP",
                    archive_write="b-writer", archive_read="b-writer",
                    evidence_write="b-writer", evidence_read="b-evidence-ro",
                    description="MinIO B, a different endpoint and a different credential"),
        destination("dest-denied", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                    endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                    archive_write="a-denied", archive_read="a-denied",
                    evidence_write="a-denied", evidence_read="a-denied",
                    description="every grant is a principal with no policy at all"),
        destination("dest-tls", bucket="lw-tls", prefix=ARCHIVE_PREFIX,
                    endpoint=f"https://minio-tls.{NS}.svc:9000", security="TLS",
                    ca_bundle="minio-ca",
                    archive_write="tls-writer", archive_read="tls-reader",
                    evidence_write="tls-writer", evidence_read="tls-reader",
                    description="TLS with the private CA this run generated"),
        destination("dest-tls-noca", bucket="lw-tls", prefix=ARCHIVE_PREFIX,
                    endpoint=f"https://minio-tls.{NS}.svc:9000", security="TLS",
                    archive_write="tls-writer", archive_read="tls-reader",
                    evidence_write="tls-writer", evidence_read="tls-reader",
                    description="TLS with no CA bundle: admission passes, trust does not"),
    ]
    for obj in dests:
        apply(obj)
    for name in ("dest-a", "dest-b", "dest-denied", "dest-tls", "dest-tls-noca"):
        found = wait_for(
            "backupdestination", name,
            lambda o: any(c.get("type") == "Valid" for c in o.get("status", {}).get("conditions", [])),
            timeout=180, what="a Valid condition",
        )
        artifact(f"objects/setup/backupdestination-{name}.json", found)
    log("setup: destinations created")

    # --- namespace B -----------------------------------------------------
    literal_secret("kafka-admin", {"password": SECRETS["kafka-admin"]}, namespace=NS_B)
    apply(kafka_cluster("source-admin", servers=acl_servers, username="admin",
                        secret="kafka-admin", namespace=NS_B), namespace=NS_B)
    state["setup"] = True
    save()
    log("setup complete")


# --------------------------------------------------------------------------
# credential sweep, cleanup, report
# --------------------------------------------------------------------------


def sweep() -> dict[str, Any]:
    """Re-read every artifact byte and fail if a credential value survived.

    The markers are the run's own generated values, so a hit is a real leak and
    not a heuristic. Private-key PEM blocks are searched for as well."""
    load_secrets()
    markers = {label: value for label, value in SECRETS.items() if value}
    encoded = {
        label + " (base64)": base64.b64encode(value.encode()).decode()
        for label, value in markers.items()
    }
    needles = {**markers, **encoded}
    hits: list[dict[str, Any]] = []
    scanned = 0
    for path in sorted(ART.rglob("*")):
        if not path.is_file():
            continue
        scanned += 1
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for label, needle in needles.items():
            if needle in text:
                hits.append({"file": str(path.relative_to(ART)), "marker": label})
        if "-----BEGIN" in text and "PRIVATE KEY-----" in text:
            hits.append({"file": str(path.relative_to(ART)), "marker": "PEM private key"})
    outcome = {"filesScanned": scanned, "markers": sorted(needles), "hits": hits}
    artifact("credential-sweep.json", outcome)
    return outcome


def cleanup() -> None:
    proof: list[str] = [f"# {OWNER} cleanup proof", ""]
    for name, key in ((NS, "namespaceUid"), (NS_B, "namespaceBUid")):
        assert_safe_namespace(name)
        live = json.loads(
            run(CTX + ["get", "namespace", name, "-o", "json"], check=False).stdout or "{}"
        )
        if not live.get("metadata"):
            proof.append(f"- `{name}`: already absent")
            continue
        label = live["metadata"].get("labels", {}).get(OWNER_LABEL_KEY)
        uid = live["metadata"]["uid"]
        if label != OWNER:
            raise RuntimeError(f"refusing to delete {name}: owner label is {label!r}")
        if state.get(key) and uid != state[key]:
            raise RuntimeError(
                f"refusing to delete {name}: uid {uid} != recorded {state[key]}"
            )
        proof.append(f"- `{name}`: label `{OWNER_LABEL_KEY}={label}`, uid `{uid}`"
                     f" (recorded at creation: `{state.get(key)}`) — deleting")
        run(CTX + ["delete", "namespace", name, "--wait=false"], timeout=120)
    for name in (NS, NS_B):
        deadline = time.monotonic() + 420
        while time.monotonic() < deadline:
            probe = run(CTX + ["get", "namespace", name], check=False, timeout=60)
            if probe.returncode != 0 and "NotFound" in (probe.stderr + probe.stdout):
                proof.append(f"- `{name}`: NotFound")
                break
            time.sleep(5)
        else:
            proof.append(f"- `{name}`: STILL PRESENT after 420 s — reported, not hidden")
    listing = run(CTX + ["get", "namespaces"], timeout=60).stdout
    proof += ["", "## `kubectl --context docker-desktop get namespaces` afterwards", "",
              "```", listing.strip(), "```", ""]
    leftovers = run(
        CTX + ["get", "backupdestinations,topicdiscoveries,preflights,backups,restores",
               "-A", "-l", f"{OWNER_LABEL_KEY}={OWNER}"],
        check=False, timeout=60,
    )
    proof += ["## Objects still carrying this run's owner label, cluster-wide", "",
              "```", (leftovers.stdout + leftovers.stderr).strip(), "```", ""]
    artifact("cleanup.md", "\n".join(proof))
    log("cleanup complete")


def report() -> None:
    results = load_results()
    scenarios = results["scenarios"]
    counts = {"pass": 0, "fail": 0, "notRun": 0}
    for entry in scenarios.values():
        counts[entry["outcome"]] += 1
    results["counts"] = counts
    results["finishedAt"] = now()
    results["sweep"] = sweep()
    results["revision"] = state.get("revision")
    results["images"] = state.get("images")
    RESULTS_PATH.write_text(json.dumps(results, indent=2, sort_keys=True))
    rows = ["| id | scenario | outcome | reason |", "|---|---|---|---|"]
    for sid in sorted(scenarios, key=lambda s: (len(s), s)):
        entry = scenarios[sid]
        rows.append(
            f"| {sid} | {entry['title']} | **{entry['outcome']}** | {entry.get('reason', '')} |"
        )
    artifact("summary.md", "\n".join(rows) + "\n\n" + json.dumps(counts, indent=2))
    print(json.dumps(counts, indent=2))


# Paths that cannot change what an image contains: the live harnesses
# themselves, the fixtures they mount, and prose. Everything else is a product
# build input. DELIBERATELY AN ALLOWLIST — a product directory this list has
# never heard of is refused rather than silently tolerated.
NON_IMAGE_PATHS = ("scripts/live/", "scripts/fixtures/", "e2e/", "docs/")

# What the image build actually CONSUMES, derived from the two Dockerfiles —
# the same set and the same rule `scripts/live/d1/fence/fenced.py` applies, and
# for the same reason. Both Dockerfiles end their builder stage with `COPY . .`,
# so an untracked `crates/**/x.rs` really is compiled in and an untracked
# `Cargo.lock` really does decide what the image was built from.
IMAGE_BUILD_INPUTS = (
    "crates/", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/",
    "third_party/", "LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md",
    "ui/", "charts/", "config/", "logweir.yaml",
)


def untracked_is_ignorable(path: str) -> bool:
    """Whether an UNTRACKED file can be ignored when comparing lab and checkout.

    Three questions, and the last is what makes this fail closed: inside the
    build inputs is refused; inside a tree no image contains is ignored;
    otherwise it is ignorable only if it is a bare file at the repository root
    — the orchestrator's `prompt` and its like, which land in the build CONTEXT,
    invalidate a cargo layer and change no output byte because no crate names a
    file at the root. Anything in a directory this function has never heard of
    is REFUSED.

    Without this the guard refused on `prompt` and `d2_live.py revision` could
    not run at all (lab-refresh-5 §7.3, lab-refresh-6 §15) — while the D1 fence,
    which had the same defect, has applied this rule since harness-rows-2.
    """
    if path.startswith(IMAGE_BUILD_INPUTS):
        return False
    if path.startswith(NON_IMAGE_PATHS):
        return True
    return "/" not in path


def revision_guard() -> None:
    """Abort unless the product code in this checkout IS the build the lab runs.

    This compared the images' revision label with `origin/main`, and that ref
    moves the moment any worker's branch lands: on 2026-09-18 origin/main
    advanced past the commit the lab had been refreshed to an hour earlier and
    this guard refused a run that was measuring exactly the right thing. A
    guard a third party can break by pushing is not a guard. Comparing with
    `HEAD` instead would be worse in the other direction — a harness branch
    carries test-only commits over the build the lab runs.

    So the rule is the narrower, checkable one
    (`scripts/live/d1/fence/fenced.py::assert_checkout_contains` states it the
    same way): both images carry the SAME revision, this checkout knows and
    contains that commit, and everything that has moved since — committed or
    not — is a file no image contains.
    """
    proc = run(
        ["docker", "image", "inspect", "weirkeeper:scram-reviewed", "logweir:scram-local",
         "--format", '{{index .Config.Labels "org.opencontainers.image.revision"}} {{.Id}}'],
        timeout=120,
    )
    lines = [line.split() for line in proc.stdout.strip().splitlines()]
    revisions = {line[0] for line in lines}
    if len(revisions) != 1 or not revisions or "<no" in "".join(revisions):
        raise RuntimeError(
            f"the lab's controller and runner carry revisions {sorted(revisions)}: a "
            "controller and a runner from different commits, or an unlabelled image, are "
            "not one build"
        )
    revision = revisions.pop()
    head = run(["git", "rev-parse", "HEAD"], timeout=60).stdout.strip()
    if run(["git", "cat-file", "-e", f"{revision}^{{commit}}"],
           check=False, timeout=60).returncode:
        raise RuntimeError(
            f"the lab images name revision {revision}, which this checkout does not have"
        )
    if run(["git", "merge-base", "--is-ancestor", revision, "HEAD"],
           check=False, timeout=60).returncode:
        raise RuntimeError(
            f"the lab images were built from {revision}, which is not an ancestor of HEAD "
            f"{head}: the lab runs a build this branch does not contain"
        )
    changed = [ln.strip() for ln in
               run(["git", "diff", "--name-only", revision, "HEAD"], timeout=120)
               .stdout.splitlines() if ln.strip()]
    porcelain = run(["git", "status", "--porcelain", "--untracked-files=all"],
                    timeout=120).stdout.splitlines()
    dirty = [ln[3:].strip() for ln in porcelain if ln.strip() and not ln.startswith("?? ")]
    untracked = [ln[3:].strip() for ln in porcelain if ln.startswith("?? ")]
    untracked_ignored = [u for u in untracked if untracked_is_ignorable(u)]
    untracked_refused = [u for u in untracked if not untracked_is_ignorable(u)]
    product = sorted(
        {p for p in changed + dirty if not p.startswith(NON_IMAGE_PATHS)}
        | set(untracked_refused)
    )
    if product:
        raise RuntimeError(
            f"the lab runs {revision[:12]} and this checkout has moved product files since: "
            f"{product[:8]}{' …' if len(product) > 8 else ''}: refusing to run"
        )
    pods = get_list("pods", namespace=LAB_NS, selector="app.kubernetes.io/component=control-plane")
    image_ids = [
        st["imageID"] for pod in pods for st in pod["status"].get("containerStatuses", [])
    ]
    state["revision"] = revision
    state["images"] = {
        "controller": lines[0],
        "runner": lines[1],
        "controllerPodImageIds": image_ids,
    }
    save()
    artifact(
        "revision.json",
        state["images"] | {"labRevision": revision, "checkoutHead": head,
                           "originMain": run(["git", "rev-parse", "origin/main"],
                                             check=False, timeout=60).stdout.strip(),
                           "changedSinceImage": changed, "uncommitted": dirty,
                           "untrackedIgnored": untracked_ignored,
                           "untrackedRefused": untracked_refused},
    )
    log(f"revision guard: lab images are {revision}; HEAD {head} differs only outside the image")


# --------------------------------------------------------------------------
# shared scenario helpers
# --------------------------------------------------------------------------


def job_env(job_name: str, *, namespace: str = "") -> dict[str, Any]:
    """The rendered env of a Job's first container, as {name: value-or-source}."""
    job = get("job", job_name, namespace)
    container = job["spec"]["template"]["spec"]["containers"][0]
    out: dict[str, Any] = {}
    for entry in container.get("env", []):
        if "value" in entry:
            out[entry["name"]] = entry["value"]
        else:
            out[entry["name"]] = entry.get("valueFrom")
    return out


def job_facts(job_name: str, *, namespace: str = "") -> dict[str, Any]:
    job = get("job", job_name, namespace)
    container = job["spec"]["template"]["spec"]["containers"][0]
    return {
        "name": job["metadata"]["name"],
        "uid": job["metadata"]["uid"],
        "ownerReferences": job["metadata"].get("ownerReferences", []),
        "image": container["image"],
        "args": container.get("args", []),
        "command": container.get("command", []),
        "serviceAccountName": job["spec"]["template"]["spec"].get("serviceAccountName"),
        "env": job_env(job_name, namespace=namespace),
        "volumes": [v.get("name") for v in job["spec"]["template"]["spec"].get("volumes", [])],
    }


def secret_ref_name(env: dict[str, Any], key: str) -> str | None:
    entry = env.get(key)
    if isinstance(entry, dict):
        return entry.get("secretKeyRef", {}).get("name")
    return None


def plan_config_map_for(job_name: str) -> dict[str, Any] | None:
    """The immutable plan ConfigMap a run's Job mounts, read back from the
    cluster rather than re-rendered."""
    job = get("job", job_name)
    for volume in job["spec"]["template"]["spec"].get("volumes", []):
        name = volume.get("configMap", {}).get("name")
        if name:
            found = get_opt("configmap", name)
            if found is not None:
                return found
    return None


def mc_get(alias_path: str) -> bytes:
    """Read one object's exact bytes out of a bucket, through `mc`, base64 on
    the wire so a signature check sees the stored bytes and not a transcoding."""
    proc = run(K + ["exec", "mc", "--", "sh", "-c",
                    f"mc cat {alias_path} | base64"], timeout=180)
    return base64.b64decode("".join(proc.stdout.split()))


LOGWEIR_BIN_ENV = "LOGWEIR_BIN"


def logweir_cli() -> str:
    """The shipped CLI this harness verifies stored documents with.

    `target/debug/logweir` was hard-coded, so a checkout carrying a release
    build and no debug build — the normal shape on a host that is short of disk
    — failed S1 and the approval helper on a missing file rather than on
    anything the product did. The env var comes first so a caller can name the
    binary it means; the two build directories are then tried in the order a
    developer produces them.
    """
    override = os.environ.get(LOGWEIR_BIN_ENV)
    if override:
        if not pathlib.Path(override).is_file():
            raise RuntimeError(f"{LOGWEIR_BIN_ENV}={override!r} is not a file")
        return override
    for candidate in (ROOT / "target/debug/logweir", ROOT / "target/release/logweir"):
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError(
        "no `logweir` binary under target/debug or target/release: build one "
        f"(`cargo build -p logweir`) or set {LOGWEIR_BIN_ENV} to its path"
    )


def verify_document(payload: bytes, sidecar: bytes, public_key: pathlib.Path,
                    payload_type: str) -> dict[str, Any]:
    """Check a stored Logweir document's DSSE signature with the SHIPPED CLI,
    against the public half of the key the cluster-scoped roster carries."""
    work = KEYS / "verify"
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    doc = work / "document.json"
    sig = work / "document.sig"
    doc.write_bytes(payload)
    sig.write_bytes(sidecar)
    proc = run([logweir_cli(), "drill", "verify",
                "--scorecard", str(doc), "--signature", str(sig),
                "--public-key", str(public_key),
                "--payload-type", payload_type], check=False, timeout=120)
    return {
        "verifierBinary": logweir_cli(),
        "exitCode": proc.returncode,
        "stdout": redact(proc.stdout)[:2000],
        "stderr": redact(proc.stderr)[:2000],
        "payloadSha256": digest(payload),
        "payloadType": payload_type,
    }


def mc_ls(alias_path: str) -> list[str]:
    proc = mc("ls", "--recursive", alias_path, check=False, timeout=180)
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def checks_by_id(obj: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        c["id"]: c
        for c in obj.get("status", {}).get("result", {}).get("checks", [])
    }


def chunk_config_maps(discovery: dict[str, Any]) -> list[dict[str, Any]]:
    names = [
        c["name"] for c in discovery.get("status", {}).get("result", {}).get("chunks", [])
    ]
    return [get("configmap", name) for name in names]


def topics_from_chunks(discovery: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for cm in chunk_config_maps(discovery):
        for line in cm["data"]["topics.tsv"].splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            rows.append({"name": parts[0], "fields": parts[1:]})
    return rows


def lab_key_dir() -> pathlib.Path:
    """Where the lab's key material lives: `LOGWEIR_SCRAM_OUT`, else the durable home.

    `$HOME/.logweir-lab/scram-e2e` since 2026-09-22, with `/tmp/logweir-scram-e2e`
    left as a symlink to it: macOS deletes files under `/tmp` that go untouched
    for three days, which is how the lab's approver PRIVATE key vanished once
    already. The durable path is read first so a tidied symlink does not
    matter; the `/tmp` path is kept for a host that predates the move.
    """
    override = os.environ.get("LOGWEIR_SCRAM_OUT")
    if override:
        return pathlib.Path(override)
    durable = pathlib.Path.home() / ".logweir-lab" / "scram-e2e"
    for candidate in (durable, pathlib.Path("/tmp/logweir-scram-e2e")):
        if candidate.is_dir():
            return candidate
    return durable


def approver_material() -> dict[str, pathlib.Path]:
    """The lab roster's own approver and signing key pair.

    The cluster-scoped `TrustRoster/default` is SHARED with two other live
    waves, so this harness never edits it. It signs with the key the roster
    already carries, whose private half `scripts/test-k8s-scram.py` wrote when
    the lab was built."""
    base = lab_key_dir()
    needed = {
        "approver": base / "approver.pem",
        "approverPub": base / "approver.pub.pem",
        "signing": base / "signing.pem",
        "signingPub": base / "signing.pub.pem",
    }
    missing = [str(p) for p in needed.values() if not p.is_file()]
    if missing:
        raise RuntimeError("lab key material is absent: " + ", ".join(missing))
    return needed


def wait_backup(name: str, *, timeout: int = 600, namespace: str = "") -> dict[str, Any]:
    return wait_for("backup", name, terminal_phase, timeout=timeout, namespace=namespace,
                    what="a terminal phase")


def dump(prefix: str, kind: str, name: str, *, namespace: str = "") -> dict[str, Any]:
    obj = get(kind, name, namespace)
    artifact(f"objects/{prefix}/{kind}-{name}.json", obj)
    return obj


# --------------------------------------------------------------------------
# S1 — two destinations, one namespace, nothing shared
# --------------------------------------------------------------------------


def s1() -> None:
    with Scenario("S1", "two destinations with distinct endpoints and credentials") as sc:
        objects = [
            backup("bk-a", source="source-admin", topics=["orders"], destination_ref="dest-a"),
            backup("bk-b", source="source-admin", topics=["orders"], destination_ref="dest-b"),
        ]
        for obj in objects:
            created = apply(obj)
            sc.detail.setdefault("uids", {})[created["metadata"]["name"]] = created["metadata"]["uid"]
        sc.commands += [
            f"kubectl --context {CONTEXT} -n {NS} apply -f - # Backup/bk-a (destinationRef dest-a)",
            f"kubectl --context {CONTEXT} -n {NS} apply -f - # Backup/bk-b (destinationRef dest-b)",
        ]
        results = {}
        for name in ("bk-a", "bk-b"):
            obj = wait_backup(name, timeout=720)
            artifact(f"objects/s1/backup-{name}.json", obj)
            results[name] = obj
        for name, obj in results.items():
            status = obj["status"]
            check(status.get("phase") == "Succeeded",
                  f"{name}: phase is {status.get('phase')}, expected Succeeded "
                  f"(reason {status.get('reason')}: {status.get('message')})")
            check(status.get("exitCode") == 0, f"{name}: exitCode {status.get('exitCode')}")
            evidence = status.get("evidence", {})
            check(evidence.get("receiptKey") and evidence.get("sidecarKey"),
                  f"{name}: evidence keys were not recorded: {evidence}")
            sc.detail.setdefault("backups", {})[name] = {
                "backupId": status.get("backupId"),
                "receiptKey": evidence.get("receiptKey"),
                "sidecarKey": evidence.get("sidecarKey"),
                "statusVerification": evidence.get("verification"),
                "jobRef": status.get("jobRef", {}).get("name"),
            }

        # --- the receipt each run signed is in ITS OWN destination, and it
        #     verifies against the roster's public signing key. What the STATUS
        #     says about it is S1.statusVerification's subject; this reads the
        #     stored bytes and verifies them independently of the controller.
        keys = approver_material()
        for name, alias, bucket in (("bk-a", "a", "lw-a"), ("bk-b", "b", "lw-b")):
            evidence = results[name]["status"]["evidence"]
            payload = mc_get(f"{alias}/{bucket}/{evidence['receiptKey']}")
            sidecar = mc_get(f"{alias}/{bucket}/{evidence['sidecarKey']}")
            outcome = verify_document(payload, sidecar, keys["signingPub"], "backup-receipt")
            artifact(f"objects/s1/receipt-{name}.json", payload.decode())
            artifact(f"objects/s1/receipt-verify-{name}.json", outcome)
            sc.detail.setdefault("independentVerification", {})[name] = outcome
            check(outcome["exitCode"] == 0,
                  f"{name}: the stored receipt did not verify against the roster's "
                  f"signing key: {outcome['stderr']}")

        # --- the two Jobs took nothing from each other or from the controller
        for name, secret_name, allow_http in (("bk-a", "a-writer", "true"),
                                              ("bk-b", "b-writer", "true")):
            job = results[name]["status"]["jobRef"]["name"]
            facts = job_facts(job)
            artifact(f"objects/s1/job-{name}.json", facts)
            env = facts["env"]
            check("AWS_ENDPOINT_URL" not in env,
                  f"{name}: AWS_ENDPOINT_URL is present in the Job env "
                  "(D2 §3.5: absent by construction)")
            check(env.get("AWS_ALLOW_HTTP") == allow_http,
                  f"{name}: AWS_ALLOW_HTTP is {env.get('AWS_ALLOW_HTTP')!r}")
            check(env.get("AWS_VIRTUAL_HOSTED_STYLE_REQUEST") == "false",
                  f"{name}: AWS_VIRTUAL_HOSTED_STYLE_REQUEST is "
                  f"{env.get('AWS_VIRTUAL_HOSTED_STYLE_REQUEST')!r}")
            check(env.get("LOGWEIR_STORE_CONTRACT_VERSION") == "1",
                  f"{name}: store contract version {env.get('LOGWEIR_STORE_CONTRACT_VERSION')!r}")
            check(secret_ref_name(env, "AWS_ACCESS_KEY_ID") == secret_name,
                  f"{name}: AWS_ACCESS_KEY_ID comes from "
                  f"{secret_ref_name(env, 'AWS_ACCESS_KEY_ID')!r}, expected {secret_name!r}")
            check(secret_ref_name(env, "AWS_SECRET_ACCESS_KEY") == secret_name,
                  f"{name}: AWS_SECRET_ACCESS_KEY comes from "
                  f"{secret_ref_name(env, 'AWS_SECRET_ACCESS_KEY')!r}")
            plan = plan_config_map_for(job)
            if plan is not None:
                artifact(f"objects/s1/plan-{name}.json", plan)
                blob = json.dumps(plan.get("data", {}))
                expected_host = "minio-a" if name == "bk-a" else "minio-b"
                other_host = "minio-b" if name == "bk-a" else "minio-a"
                check(expected_host in blob,
                      f"{name}: the frozen plan does not name {expected_host}")
                check(other_host not in blob,
                      f"{name}: the frozen plan names the OTHER destination's endpoint "
                      f"({other_host})")
                check("minio.logweir-scram-local" not in blob,
                      f"{name}: the frozen plan names the CONTROLLER's global endpoint")
                sc.detail.setdefault("planStorage", {})[name] = {
                    "configMap": plan["metadata"]["name"],
                    "namesOwnEndpoint": expected_host in blob,
                    "namesOtherEndpoint": other_host in blob,
                    "namesControllerEndpoint": "minio.logweir-scram-local" in blob,
                }

        # --- the bytes landed in one bucket and nowhere else
        a_id = results["bk-a"]["status"]["backupId"]
        b_id = results["bk-b"]["status"]["backupId"]
        listing = {
            "a": mc_ls(f"a/lw-a/{ARCHIVE_PREFIX}/"),
            "b": mc_ls(f"b/lw-b/{ARCHIVE_PREFIX}/"),
            "a-all": mc_ls("a/lw-a/"),
            "b-all": mc_ls("b/lw-b/"),
            "tls-all": mc_ls("t/lw-tls/"),
        }
        artifact("objects/s1/bucket-listing.json", listing)
        sc.detail["bucketListing"] = listing
        check(any(a_id in line and "manifest.json" in line for line in listing["a"]),
              f"bk-a's manifest is not under lw-a/{ARCHIVE_PREFIX}/{a_id}/")
        check(any(b_id in line and "manifest.json" in line for line in listing["b"]),
              f"bk-b's manifest is not under lw-b/{ARCHIVE_PREFIX}/{b_id}/")
        check(not any(a_id in line for line in listing["b-all"]),
              "bk-a's backup id appears in bucket lw-b")
        check(not any(b_id in line for line in listing["a-all"]),
              "bk-b's backup id appears in bucket lw-a")
        check(not any(a_id in line or b_id in line for line in listing["tls-all"]),
              "a backup id appears in the third bucket, which no run named")

        # --- the controller still holds no verb on Secrets in this namespace
        probe = run(
            CTX + ["auth", "can-i", "get", "secrets", "-n", NS,
                   f"--as=system:serviceaccount:{LAB_NS}:weirkeeper"],
            check=False, timeout=60,
        )
        sc.commands.append(
            f"kubectl --context {CONTEXT} auth can-i get secrets -n {NS} "
            f"--as=system:serviceaccount:{LAB_NS}:weirkeeper"
        )
        sc.detail["controllerCanGetSecrets"] = probe.stdout.strip()
        check(probe.stdout.strip() == "no",
              f"the controller CAN get Secrets in {NS}: {probe.stdout.strip()!r}")

        # --- no credential value anywhere the operator or an API can see
        haystacks = {
            "controller-log": controller_log("30m"),
            "backup-bk-a": json.dumps(results["bk-a"]),
            "backup-bk-b": json.dumps(results["bk-b"]),
        }
        for cm in get_list("configmaps"):
            name = cm["metadata"]["name"]
            if name.startswith("lwc-") or name.endswith("-plan") or name.endswith("-inputs"):
                haystacks["configmap-" + name] = json.dumps(cm)
        leaks = []
        for label, value in SECRETS.items():
            if not value:
                continue
            for where, text in haystacks.items():
                if value in text or base64.b64encode(value.encode()).decode() in text:
                    leaks.append({"marker": label, "where": where})
        sc.detail["credentialScan"] = {
            "haystacks": sorted(haystacks),
            "markers": len(SECRETS),
            "leaks": leaks,
        }
        artifact("objects/s1/credential-scan.json", sc.detail["credentialScan"])
        check(not leaks, f"credential values leaked: {leaks}")


# ---------------------------------------------------------------------------
# The row decisions, as named functions
#
# Lifted out of their call sites so they can be fed a recorded object without a
# cluster: `e2e/k8s/d2/test_rows.py` runs each against the shape the product
# publishes today AND the shape it published before the defect was fixed, and
# requires the second to be refused. A decision that cannot be made to say
# False is not a decision.
# ---------------------------------------------------------------------------


def s11_criteria(status: dict[str, Any], job_conditions: list[dict[str, Any]],
                 relayed: Any, new_chunks: list[str]) -> dict[str, bool]:
    """D2 §14.4's S11 criteria, each judged on its own.

    THE FIFTH ONE IS THE AMENDED CRITERION, AND IT IS NARROW ON PURPOSE. §14.4
    S11 as amended 2026-09-18 (main `ce69be4`, D2 W14 / lab-refresh-3) reads
    "the check Job is `Complete` — the runner exits 0 after relaying its
    `notReady` result and the failure is carried in the projected reason".
    `Failed` belongs to the deadline path (`Job DeadlineExceeded →
    Failed/DeadlineExceeded`), which is NOT this scenario's fixture: S11 points
    `td-timeout` at a blackhole broker with `timeoutSeconds: 10` and the runner
    classifies the timeout itself.

    An earlier form of this accepted `Complete` OR `Failed`, which was looser
    than the document it cites: a regression in which the runner went back to
    failing the check Job on the Kafka-timeout path would have satisfied it
    (review L-2). Requiring `Complete` is what the fixture says should happen;
    the reason and the relayed frame then have to agree, because "the Job
    completed" alone would accept a Job that merely finished.
    """
    blocking = [c for c in ((relayed or {}).get("checks") or [])
                if isinstance(c, dict) and c.get("gating") == "blocking"
                and c.get("state") == "notReady"]
    complete = [c for c in job_conditions
                if c.get("type") == "Complete" and c.get("status") == "True"]
    return {
        "phase == Failed": status.get("phase") == "Failed",
        "status.reason in {BrokerUnreachable, MetadataTimeout}":
            status.get("reason") in {"BrokerUnreachable", "MetadataTimeout"},
        "no chunks in status": not (status.get("result") or {}).get("chunks"),
        "no chunk ConfigMaps": not new_chunks,
        "the check Job is Complete and the reason came from its relayed blocking check":
            bool(complete) and bool(blocking)
            and status.get("reason") in {c.get("code") for c in blocking},
    }


def not_attempted_is_honest(verification: Any,
                            identity_locations: Any) -> dict[str, bool]:
    """Each clause S1.notAttempted rests on, judged on its own (it was
    S1.statusVerification's, until dest-a's grant became readable by the Job).

    `NotAttempted` exists as a verdict distinct from `Invalid` precisely so an
    operator can tell "we did not check" from "it did not verify"; a block that
    is absent altogether says neither, which is the defect. The premise is a
    clause too: the verdict is only predictable while the installation policy
    allows the controller's own identity nowhere.
    """
    block = verification if isinstance(verification, dict) else {}
    return {
        "a verification block exists": verification is not None,
        # The CRD's whole vocabulary: `Untrusted` since D3 §7.4, `Pending`
        # since D2 §3.9's evidence-fetch Job. A word outside it is refused.
        "the verdict is one of the published results":
            block.get("result") in {"Valid", "Invalid", "Untrusted", "NotAttempted", "Pending"},
        "no controller identity location is allowed": not identity_locations,
        "the verdict is NotAttempted": block.get("result") == "NotAttempted",
        "NotAttempted says why": bool((block.get("detail") or "").strip()),
        "nothing was verified, so no key matched": block.get("matchedKeyId") is None,
    }


def s1b() -> None:
    """What S1's destination-backed runs' STATUS says about their evidence.

    THE EXPECTATION MOVED WITH THE PRODUCT, AND THE ROW SAYS SO. dest-a's and
    dest-b's `evidenceRead` is a `SecretKeys` grant — a credential only a pod
    may hold. Until EVIDENCE-FETCH-JOB-UNBUILT closed (`claude/evidence-fetch`,
    D2 §3.9) the controller had no way to read it and published `NotAttempted`
    with a sentence saying so; this row asserted exactly that, and it was right
    for that build. D2 §3.9's evidence-fetch Job is that way: the controller
    creates `lwc-ev-<sha256(uid:attempt)[:20]>` holding ONLY the read grant,
    reads the relayed bytes, checks them against the RUNNER's receipt digest
    and verifies the DSSE sidecar itself. So a `SecretKeys` destination now
    reaches `Valid`, and `NotAttempted` belongs to a destination the Job
    cannot use either — which `S1.notAttempted` (phase `s1c`) keeps asserting.

    The premise clause stays: the installation policy allowlists no
    controller-identity location, so a `Valid` here can only have come through
    the Job, never through the controller's own read-only handle.
    """
    with Scenario("S1.statusVerification",
                  "status.evidence.verification on a destination-backed Backup") as sc:
        policy = get_opt("configmap", "weirkeeper-policy", LAB_NS)
        identity_locations = None
        if policy and "policy.json" in policy.get("data", {}):
            identity_locations = json.loads(policy["data"]["policy.json"]).get(
                "evidence", {}).get("controllerIdentityLocations")
        sc.detail["policyControllerIdentityLocations"] = identity_locations
        failed_all: list[str] = []
        for name in ("bk-a", "bk-b"):
            obj = get_opt("backup", name)
            check(obj is not None, f"Backup/{name} is absent; run S1 first")
            # BOUNDED, AND LONG ENOUGH FOR THE JOB: a fetch is a pod start plus
            # the plan's 120 s budget plus a requeue, so the 45 s the old
            # settle allowed would have read `Pending` and called it a verdict.
            obj = settle_reached_verdict("backup", name)
            verification = (obj.get("status", {}).get("evidence", {}) or {}).get("verification")
            criteria = fetched_verdict_is_valid(obj, identity_locations)
            sc.detail.setdefault("backups", {})[name] = {
                "observedVerificationField": verification,
                "observation": ((obj.get("status") or {}).get("evidence") or {}).get(
                    "observation"),
                "windowCovered": (obj.get("status") or {}).get("windowCovered"),
                "criteria": criteria,
            }
            failed_all += [f"{name}: {k}" for k, ok in criteria.items() if not ok]
        artifact("objects/s1/status-verification.json", sc.detail)
        check(not failed_all,
              "S1.statusVerification: " + "; ".join(failed_all)
              + ". A SecretKeys destination is read by D2 §3.9's evidence-fetch Job on this "
              "build; `NotAttempted` here is EVIDENCE-FETCH-JOB-UNBUILT reproduced, and "
              "`Pending` after the bounded wait is a fetch that never finished")
        # The receipt itself is verified from its stored bytes in S1; this row
        # is only about what the STATUS says.


# --------------------------------------------------------------------------
# D2 §3.9 — the evidence-fetch Job (EVIDENCE-FETCH-JOB-UNBUILT, PLAT-08.1)
#
# lab-refresh-8's rows for `claude/evidence-fetch`. Every row judges a pure
# predicate below over what a live object said, and `e2e/k8s/d2/
# test_evidence_fetch_rows.py` drives each predicate over the shape the fixed
# controller writes AND a planted-wrong shape it must refuse.
# --------------------------------------------------------------------------

#: `check/job.rs::evidence_fetch_job_name` — `lwc-ev-<first 20 hex of
#: sha256(owner uid + ":" + attempt)>`. The unit twin reads the Rust source.
EVIDENCE_FETCH_PREFIX = "lwc-ev-"
EVIDENCE_FETCH_KIND = "evidenceFetch"
#: `check::job::TTL_SECONDS`, patched on only after the verdict commits.
EVIDENCE_FETCH_TTL_SECONDS = 600
#: `evidence_fetch::RETRY_DELAYS_SECONDS[0]` — attempt 2 starts this long after
#: attempt 1's `NotAttempted`.
EVIDENCE_FETCH_FIRST_RETRY_SECONDS = 60
#: THE BOUNDED WAIT FOR A REACHED VERDICT. A fetch is an image-present pod
#: start, the plan's `FETCH_TIMEOUT_SECONDS` (120) and one requeue (15 s); the
#: Job's own deadline is that plus `DEADLINE_MARGIN_SECONDS` (90). 240 s
#: covers the whole budget of one attempt, and not a retry.
VERDICT_SETTLE_SECONDS = 240
#: The verdicts that END a wait. `Pending` is not one: it is "the Job is
#: reading", and a wait that stopped on it would call a fetch in flight a
#: verdict.
REACHED_VERDICTS = frozenset({"Valid", "Invalid", "Untrusted", "NotAttempted"})
#: The codes an unusable `evidenceRead` grant surfaces as — the kubelet's
#: `CreateContainerConfigError` classified (`check/waiting.rs`), or the store's
#: own denial relayed by the runner.
BROKEN_GRANT_CODES = ("CredentialSecretKeyMissing", "CredentialSecretNotFound", "AccessDenied")


def evidence_fetch_job_name(owner_uid: str, attempt: int) -> str:
    """The Job name D2 §3.9 step 2 derives, for one attempt."""
    digest_hex = hashlib.sha256(f"{owner_uid}:{attempt}".encode()).hexdigest()
    return EVIDENCE_FETCH_PREFIX + digest_hex[:20]


def verdict_of(obj: dict[str, Any] | None) -> str | None:
    return ((((obj or {}).get("status") or {}).get("evidence") or {})
            .get("verification") or {}).get("result")


def settle_reached_verdict(kind: str, name: str, *, seconds: int = VERDICT_SETTLE_SECONDS,
                           namespace: str = "") -> dict[str, Any]:
    """Wait a BOUNDED time for a reached verdict and return the object either way.

    The row then judges what is there: a `Pending` still standing at the bound
    is a fetch that did not finish, and the predicate says so by name.
    """
    deadline = time.monotonic() + seconds
    obj = get(kind, name, namespace)
    while time.monotonic() < deadline and verdict_of(obj) not in REACHED_VERDICTS:
        time.sleep(3)
        obj = get(kind, name, namespace)
    return obj


def _ms_window(window: Any) -> bool:
    w = window if isinstance(window, dict) else {}
    lo, hi = w.get("fromMs"), w.get("toMs")
    return isinstance(lo, int) and isinstance(hi, int) and 0 < lo <= hi


def _controller_ref(obj: dict[str, Any] | None, kind: str, uid: str | None) -> bool:
    """Whether `obj` is CONTROLLED by (kind, uid) — the ownerReference with
    `controller: true`, which is the only one the controller reads."""
    return bool(uid) and any(
        ref.get("kind") == kind and ref.get("uid") == uid and ref.get("controller") is True
        for ref in (((obj or {}).get("metadata") or {}).get("ownerReferences") or []))


def _condition(obj: dict[str, Any] | None, kind: str) -> dict[str, Any]:
    for c in (((obj or {}).get("status") or {}).get("conditions") or []):
        if c.get("type") == kind:
            return c
    return {}


def fetched_verdict_is_valid(obj: dict[str, Any],
                             identity_locations: Any) -> dict[str, bool]:
    """A `Backup` whose `SecretKeys`-read evidence the Job relayed and the
    controller verified — each clause on its own.

    The window, records and capture are the receipt's facts, projected only
    when the digest and the binding hold (`controllers/backup.rs`), so their
    presence is what separates a verified receipt from a verdict word.
    """
    meta = obj.get("metadata") or {}
    status = obj.get("status") or {}
    evidence = status.get("evidence") or {}
    ver = evidence.get("verification") or {}
    obs = evidence.get("observation") or {}
    attempt = obs.get("attempt") if isinstance(obs.get("attempt"), int) else 0
    records = status.get("records")
    return {
        "the verdict is Valid": ver.get("result") == "Valid",
        "a trusted key matched": bool(ver.get("matchedKeyId")),
        "the observation names the Job of the attempt it records":
            attempt >= 1 and (obs.get("jobRef") or {}).get("name")
            == evidence_fetch_job_name(meta.get("uid") or "", attempt),
        "the grant the Job read with is a pod-only SecretKeys grant":
            obs.get("mode") == "SecretKeys",
        "the relay read both objects whole (presence Complete)":
            obs.get("presence") == "Complete",
        "no retry is pending on a reached verdict": not obs.get("retryAfter"),
        "windowCovered is projected from the verified receipt":
            _ms_window(status.get("windowCovered")),
        "records are projected": records not in (None, {}, [], 0),
        "capture is projected": bool((status.get("capture") or {}).get("startedAt")),
        "Verified=True": _condition(obj, "Verified").get("status") == "True",
        "the runner's receipt digest is published": str(
            evidence.get("receiptSha256") or "").startswith("sha256:"),
        "no controller-identity location is allowlisted, so the Job and not the "
        "controller's own handle read it": not identity_locations,
    }


# --- the verdict trail: every resourceVersion a watch saw ----------------------


def parse_watch_stream(text: str) -> list[dict[str, Any]]:
    """`kubectl get … --watch --output-watch-events -o json` prints one JSON
    document per event, pretty-printed and back to back. A watch stopped by
    the harness can leave a truncated LAST document; it is dropped, never
    guessed at."""
    decoder = json.JSONDecoder()
    out: list[dict[str, Any]] = []
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            doc, end = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        if isinstance(doc, dict):
            out.append(doc)
        i = end
    return out


def verdict_trail(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One entry per resourceVersion, in the order the API server sent them.

    A merge patch is one resourceVersion, so `Pending` — written in its own
    patch between the terminal patch and the verdict — is one entry here even
    when it lived for a second, which a poll can miss and a watch cannot.
    """
    trail: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        obj = event.get("object") if isinstance(event.get("object"), dict) else event
        meta = obj.get("metadata") or {}
        rv = str(meta.get("resourceVersion") or "")
        if not rv or rv in seen:
            continue
        seen.add(rv)
        status = obj.get("status") or {}
        evidence = status.get("evidence") or {}
        ver = evidence.get("verification") or {}
        obs = evidence.get("observation") or {}
        trail.append({
            "type": event.get("type"),
            "resourceVersion": rv,
            "uid": meta.get("uid"),
            "phase": status.get("phase"),
            "result": ver.get("result"),
            "detail": ver.get("detail"),
            "verifiedAt": ver.get("verifiedAt"),
            "attempt": obs.get("attempt"),
            "jobRef": (obs.get("jobRef") or {}).get("name"),
            "jobUid": (obs.get("jobRef") or {}).get("uid"),
            "mode": obs.get("mode"),
            "presence": obs.get("presence"),
            "retryAfter": obs.get("retryAfter"),
            "windowCovered": status.get("windowCovered"),
        })
    return trail


LWTIMEOUT = pathlib.Path("/tmp/lwtimeout")


class ObjectWatch:
    """A `kubectl --watch` of ONE object by name, from before it exists.

    `--field-selector metadata.name=` rather than `get <kind> <name>`, because
    the latter refuses an object that does not exist yet and the first
    `Pending` can be written seconds after the create. The child is bounded
    twice: `/tmp/lwtimeout` (WORKER-RULES) when the host has it, and `stop()`
    — always called from a `finally` — terminates and then kills it.
    """

    def __init__(self, plural: str, name: str, *, seconds: int = 1800,
                 namespace: str = "") -> None:
        self.plural, self.name = plural, name
        self.path = OUT / f"watch-{plural}-{name}-{int(time.time())}.json"
        args = CTX + ["-n", namespace or NS, "get", plural,
                      "--field-selector", f"metadata.name={name}",
                      "--watch", "--output-watch-events", "-o", "json"]
        if LWTIMEOUT.exists():
            args = [str(LWTIMEOUT), str(seconds)] + args
        self.handle = self.path.open("w")
        self.proc = subprocess.Popen(args, stdout=self.handle, stderr=subprocess.DEVNULL,
                                     cwd=ROOT)
        self.deadline = time.monotonic() + seconds

    def trail(self) -> list[dict[str, Any]]:
        self.handle.flush()
        return verdict_trail(parse_watch_stream(self.path.read_text()))

    def stop(self) -> list[dict[str, Any]]:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=15)
        self.handle.close()
        return verdict_trail(parse_watch_stream(self.path.read_text()))


def trail_went_pending_then(trail: list[dict[str, Any]], uid: str, final: str,
                            *, allowed: frozenset[str] = frozenset({"Pending", "Valid"}),
                            ) -> dict[str, bool]:
    """What a watch saw on the way to `final`, each clause on its own.

    `Pending` is written in the same pass that creates attempt 1's Job and
    names it (D2 §3.9 step 3); the verdict replaces it. A trail with no
    `Pending` is a verdict that did not come through the Job — the pre-fetch
    build's `NotAttempted`, or a controller-identity read.
    """
    results = [t.get("result") for t in trail]
    pending = [i for i, r in enumerate(results) if r == "Pending"]
    finals = [i for i, r in enumerate(results) if r == final]
    return {
        "the verdict was observed Pending while the fetch ran": bool(pending),
        "the Pending block named attempt 1's Job": any(
            trail[i].get("attempt") == 1
            and trail[i].get("jobRef") == evidence_fetch_job_name(uid, 1)
            for i in pending),
        f"Pending preceded {final} and never followed it":
            bool(pending) and bool(finals) and pending[0] < finals[0]
            and not any(i > finals[0] for i in pending),
        f"no verdict outside {sorted(allowed)} was written on the way":
            {r for r in results if r} <= set(allowed),
    }


def fetch_reached_valid(trail: list[dict[str, Any]], final: dict[str, Any],
                        identity_locations: Any) -> dict[str, bool]:
    """EVF-1: Pending → Valid through the Job, with the receipt's facts."""
    uid = (final.get("metadata") or {}).get("uid") or ""
    clauses = trail_went_pending_then(trail, uid, "Valid")
    clauses.update(fetched_verdict_is_valid(final, identity_locations))
    return clauses


def evidence_job_holds_only_the_read_grant(
    job: dict[str, Any] | None, pod: dict[str, Any] | None,
    plan: dict[str, Any] | None, owner: dict[str, Any], *,
    owner_kind: str, read_secret: str, forbidden_secrets: set[str],
) -> dict[str, bool]:
    """EVF-2: the Job holds EXACTLY the `evidenceRead` grant and nothing else.

    `forbidden_secrets` is what an `ArchiveReadGrant` must never resolve to —
    the archive-WRITE Secret (the one D2 §3.4 would fall back to if the grant
    were resolved as `archiveWrite`) and the signing key.
    """
    job = job or {}
    meta = job.get("metadata") or {}
    template = ((job.get("spec") or {}).get("template") or {}).get("spec") or {}
    containers = template.get("containers") or []
    env = [e for c in containers for e in (c.get("env") or [])]
    refs = {e.get("name"): ((e.get("valueFrom") or {}).get("secretKeyRef") or {}).get("name")
            for e in env if ((e.get("valueFrom") or {}).get("secretKeyRef"))}
    volumes = template.get("volumes") or []
    named_secrets = set(refs.values())
    named_secrets |= {(v.get("secret") or {}).get("secretName") for v in volumes
                      if v.get("secret")}
    named_secrets |= {((f.get("secretRef") or {}).get("name")) for c in containers
                      for f in (c.get("envFrom") or []) if f.get("secretRef")}
    check_plan = next((v for v in volumes if v.get("name") == "check-plan"), {})
    owner_uid = (owner.get("metadata") or {}).get("uid") or ""
    attempt = ((((owner.get("status") or {}).get("evidence") or {}).get("observation") or {})
               .get("attempt"))
    return {
        "the Job is the lwc-ev- name of the attempt the status records":
            isinstance(attempt, int)
            and meta.get("name") == evidence_fetch_job_name(owner_uid, attempt),
        "labelled logweir.dev/check-kind=evidenceFetch":
            (meta.get("labels") or {}).get("logweir.dev/check-kind") == EVIDENCE_FETCH_KIND,
        f"controlled by this {owner_kind} (kind, uid, controller: true)":
            _controller_ref(job, owner_kind, owner_uid),
        "its credential env is exactly AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY":
            set(refs) - {"AWS_SESSION_TOKEN"} == {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"},
        f"both from the evidenceRead grant's own Secret {read_secret}":
            bool(refs) and set(refs.values()) == {read_secret},
        "no Secret the fetch must not hold is named anywhere in the pod":
            not (named_secrets & set(forbidden_secrets)),
        "no evidence-WRITE variable (LOGWEIR_EVIDENCE_AWS_*)":
            not any(str(e.get("name", "")).startswith("LOGWEIR_EVIDENCE_AWS_") for e in env),
        "no envFrom at all": not any(c.get("envFrom") for c in containers),
        "automountServiceAccountToken is false":
            template.get("automountServiceAccountToken") is False,
        "it runs as logweir-runner": template.get("serviceAccountName") == "logweir-runner",
        "its volumes are exactly check-plan and work":
            sorted(v.get("name") for v in volumes) == ["check-plan", "work"],
        "no volume is a Secret or a projected token":
            not any(v.get("secret") or v.get("projected") for v in volumes),
        "check-plan mounts this Job's own plan ConfigMap, <job>-plan":
            (check_plan.get("configMap") or {}).get("name") == f"{meta.get('name')}-plan",
        f"the plan ConfigMap is immutable and controlled by this {owner_kind}":
            (plan or {}).get("immutable") is True
            and _controller_ref(plan, owner_kind, owner_uid),
        "the real pod is controlled by this Job's UID (SEC-PODLOG)":
            _controller_ref(pod, "Job", meta.get("uid")),
        "ttlSecondsAfterFinished is 600 once the verdict is committed":
            (job.get("spec") or {}).get("ttlSecondsAfterFinished")
            == EVIDENCE_FETCH_TTL_SECONDS,
    }


def _seconds_between(earlier: str | None, later: str | None) -> float | None:
    try:
        return (rfc3339(str(later)) - rfc3339(str(earlier))).total_seconds()
    except (TypeError, ValueError):
        return None


def broken_grant_retries(trail: list[dict[str, Any]], final: dict[str, Any],
                         attempt_two: dict[str, Any] | None) -> dict[str, bool]:
    """EVF-4: an unusable grant is an honest `NotAttempted`, and it is retried.

    A Job failure (the kubelet could not project the Secret's key) is not an
    answer about the evidence, so D2 §3.9 step 5 schedules attempt 2 at +1 m
    with a NEW Job name; nothing is verified and no window is projected.
    """
    uid = (final.get("metadata") or {}).get("uid") or ""
    first = next((t for t in trail
                  if t.get("result") == "NotAttempted" and t.get("attempt") == 1), {})
    detail = str(first.get("detail") or "")
    gap = _seconds_between(first.get("verifiedAt"), first.get("retryAfter"))
    status = final.get("status") or {}
    return {
        "attempt 1 ended NotAttempted": bool(first),
        "its detail names attempt 1's Job": evidence_fetch_job_name(uid, 1) in detail,
        "and the grant's failure by its code": any(c in detail for c in BROKEN_GRANT_CODES),
        "and says when attempt 2 starts": "attempt 2 starts at" in detail,
        "a retry is scheduled about +60 s after that verdict":
            gap is not None and 45 <= gap <= 90,
        "the verdict was never Valid":
            "Valid" not in {t.get("result") for t in trail} and verdict_of(final) != "Valid",
        "no window was ever projected":
            all(t.get("windowCovered") is None for t in trail)
            and status.get("windowCovered") is None,
        "attempt 2's own Job lwc-ev-<sha256(uid:2)> was created":
            ((attempt_two or {}).get("metadata") or {}).get("name")
            == evidence_fetch_job_name(uid, 2),
        "controlled by the same Backup": _controller_ref(attempt_two, "Backup", uid),
        "and the status moved on to attempt 2": any(t.get("attempt") == 2 for t in trail),
    }


def restart_resumed_the_fetch(trail: list[dict[str, Any]], final: dict[str, Any],
                              jobs: list[dict[str, Any]], *, down_at: str, up_at: str | None,
                              pending_before_down: bool) -> dict[str, bool]:
    """EVF-5: a controller that went away while the Job ran finds THAT Job again.

    The Job's name is a pure function of the Backup's UID and the attempt, so
    the controller that comes back recomputes it, GETs it and reads its relay
    — one attempt, one Job, never two. The verdict must be written AFTER the
    new controller started, or the row measured the old one finishing.
    """
    uid = (final.get("metadata") or {}).get("uid") or ""
    names = sorted((j.get("metadata") or {}).get("name") for j in jobs)
    obs = (((final.get("status") or {}).get("evidence") or {}).get("observation") or {})
    ver = (((final.get("status") or {}).get("evidence") or {}).get("verification") or {})
    first = next((j for j in jobs if (j.get("metadata") or {}).get("name")
                  == evidence_fetch_job_name(uid, 1)), {})
    created = (first.get("metadata") or {}).get("creationTimestamp")
    after_up = _seconds_between(up_at, ver.get("verifiedAt"))
    before_down = _seconds_between(created, down_at)
    return {
        "the verdict was Pending, naming attempt 1's Job, when the controller went down":
            pending_before_down,
        "attempt 1's Job was created before the controller went down":
            before_down is not None and before_down >= 0,
        "exactly one evidence-fetch Job exists for this Backup, and it is attempt 1's":
            names == [evidence_fetch_job_name(uid, 1)],
        "the verdict is Valid": ver.get("result") == "Valid",
        "and it names the Job the old controller created":
            bool(obs.get("jobRef")) and (obs.get("jobRef") or {}).get("uid")
            == (first.get("metadata") or {}).get("uid"),
        "the verdict was written after the new controller started":
            after_up is not None and after_up >= 0,
        "the watch saw nothing but Pending and Valid":
            {t.get("result") for t in trail if t.get("result")} <= {"Pending", "Valid"},
    }


def restore_fetch_reached_valid(trail: list[dict[str, Any]], final: dict[str, Any],
                                job: dict[str, Any] | None) -> dict[str, bool]:
    """EVF-6: a destination-backed `Restore`'s scorecard, Pending → Valid.

    The facts are copied from the relayed bytes only when the scorecard's
    `run_id` is the run its printed key names (`controllers/restore.rs`), and
    `Verified=True` requires `outcome: pass` as well as `Valid`.
    """
    meta = final.get("metadata") or {}
    uid = meta.get("uid") or ""
    status = final.get("status") or {}
    evidence = status.get("evidence") or {}
    ver = evidence.get("verification") or {}
    obs = evidence.get("observation") or {}
    verified = _condition(final, "Verified").get("status")
    clauses = trail_went_pending_then(trail, uid, "Valid")
    clauses.update({
        "the verdict is Valid": ver.get("result") == "Valid",
        "a trusted key matched": bool(ver.get("matchedKeyId")),
        "the observation names attempt's Job and a SecretKeys grant":
            isinstance(obs.get("attempt"), int)
            and (obs.get("jobRef") or {}).get("name")
            == evidence_fetch_job_name(uid, obs.get("attempt") or 0)
            and obs.get("mode") == "SecretKeys",
        "the relay read both objects whole (presence Complete)":
            obs.get("presence") == "Complete",
        "outcome is copied from the relayed scorecard": bool(status.get("outcome")),
        "objectives are copied from it": bool(status.get("objectives")),
        "the scorecard digest over the relayed bytes is published":
            str(evidence.get("scorecardSha256") or "").startswith("sha256:"),
        "Verified=True exactly when outcome is pass":
            (verified == "True") == (status.get("outcome") == "pass"),
        "the Job is controlled by this Restore": _controller_ref(job, "Restore", uid),
    })
    return clauses


def no_fetch_for_a_grant_no_job_may_use(obj: dict[str, Any], trail: list[dict[str, Any]],
                                        fetch_jobs: list[dict[str, Any]],
                                        identity_locations: Any) -> dict[str, bool]:
    """S1.notAttempted: a destination the Job cannot read for either.

    No `evidenceRead` at all, or `ControllerIdentity` at a location the
    installation does not allowlist: the controller has no grant a pod may
    hold and none it may use itself, so it creates NO Job, writes no
    observation and says `NotAttempted` with its reason — the distinction the
    verdict exists for. Silence would read as "nobody wrote anything".
    """
    verification = (((obj.get("status") or {}).get("evidence") or {}).get("verification"))
    clauses = not_attempted_is_honest(verification, identity_locations)
    clauses.update({
        "no evidence-fetch Job was created for it": not fetch_jobs,
        "no observation block is written":
            not (((obj.get("status") or {}).get("evidence") or {}).get("observation")),
        "the verdict never went through Pending":
            "Pending" not in {t.get("result") for t in trail},
        "no window is projected from a receipt nobody read":
            (obj.get("status") or {}).get("windowCovered") is None,
    })
    return clauses


# --- the live phases -----------------------------------------------------------


def identity_locations_now() -> Any:
    policy = get_opt("configmap", "weirkeeper-policy", LAB_NS)
    if policy and "policy.json" in policy.get("data", {}):
        return json.loads(policy["data"]["policy.json"]).get("evidence", {}).get(
            "controllerIdentityLocations")
    return None


def fetch_jobs_for(uid: str) -> list[dict[str, Any]]:
    """Every evidence-fetch Job in this namespace whose owner-uid LABEL is `uid`.

    The label is an index, not an identity (`check/job.rs`), so the rows that
    care about ownership also read `ownerReferences`."""
    return get_list("jobs", selector=f"logweir.dev/check-kind={EVIDENCE_FETCH_KIND},"
                                     f"logweir.dev/check-owner-uid={uid}")


def owned_pod_of(job: dict[str, Any] | None) -> dict[str, Any] | None:
    if not job:
        return None
    pods = get_list("pods", selector=f"batch.kubernetes.io/job-name={job['metadata']['name']}")
    owned_pods = [p for p in pods if _controller_ref(p, "Job", job["metadata"].get("uid"))]
    return (owned_pods or pods or [None])[0]


def watched_backup(name: str, obj: dict[str, Any], *, seconds: int = 1800,
                   settle: int = VERDICT_SETTLE_SECONDS,
                   until: Callable[[dict[str, Any]], bool] | None = None,
                   ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Create a Backup under a watch, wait for its terminal phase and then for
    `until` (default: a reached verdict), and return (final object, trail)."""
    watch = ObjectWatch("backups", name, seconds=seconds)
    try:
        apply(obj)
        wait_backup(name, timeout=720)
        done = until or (lambda o: verdict_of(o) in REACHED_VERDICTS)
        deadline = time.monotonic() + settle
        final = get("backup", name)
        while time.monotonic() < deadline and not done(final):
            time.sleep(3)
            final = get("backup", name)
        time.sleep(2)  # let the watch deliver the event the last read already saw
    finally:
        trail = watch.stop()
    return final, trail


def ensure_archive_read_principal() -> None:
    """dest-arg's `archiveRead` grant: its OWN principal, read-only on both the
    archive prefix and `logweir/`, so an `ArchiveReadGrant` evidence read can
    succeed — and so a Job that resolved the grant as `archiveWrite` would name
    a DIFFERENT Secret, which EVF-2 refuses."""
    if state.get("archiveReadPrincipal"):
        return
    literal_secret("a-archread", {"access-key-id": "a-archread",
                                  "secret-access-key": SECRETS["a-archread"]})
    minio_user("a", "a-archread", "d2w14-a-archread",
               _policy("lw-a", write=False, prefixes=[ARCHIVE_PREFIX + "/", "logweir/"]))
    state["archiveReadPrincipal"] = True
    save()


def wait_destination(name: str) -> dict[str, Any]:
    return wait_for(
        "backupdestination", name,
        lambda o: any(c.get("type") == "Valid"
                      for c in o.get("status", {}).get("conditions", [])),
        timeout=180, what="a Valid condition")


def evf() -> None:
    """EVF-1 and EVF-2: an `ArchiveReadGrant` destination, Pending → Valid, and
    the Job that read it holds exactly that grant."""
    ensure_archive_read_principal()
    apply(destination("dest-arg", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="a-writer", archive_read="a-archread",
                      evidence_write="a-writer", evidence_read_mode="ArchiveReadGrant",
                      description="evidenceRead: ArchiveReadGrant over a separate read "
                                  "principal (EVF-1)"))
    artifact("objects/evf/backupdestination-dest-arg.json", wait_destination("dest-arg"))
    name = f"bk-evf1-{attempt('evf1Attempt')}"
    identity = identity_locations_now()
    with Scenario("EVF-1", "an ArchiveReadGrant destination goes Pending -> Valid through "
                           "the evidence-fetch Job") as sc:
        final, trail = watched_backup(
            name, backup(name, source="source-admin", topics=["orders"],
                         destination_ref="dest-arg"))
        artifact(f"objects/evf/backup-{name}.json", final)
        artifact(f"objects/evf/trail-{name}.json", trail)
        clauses = fetch_reached_valid(trail, final, identity)
        sc.detail.update({"backup": name, "uid": final["metadata"]["uid"],
                          "trail": trail, "clauses": clauses,
                          "policyControllerIdentityLocations": identity})
        failed = sorted(k for k, ok in clauses.items() if not ok)
        check(not failed, "EVF-1: " + "; ".join(failed))
    with Scenario("EVF-2", "the evidence-fetch Job holds only the read grant; its pod is "
                           "controlled by the Job's UID") as sc:
        final = get("backup", name)
        obs = ((final.get("status") or {}).get("evidence") or {}).get("observation") or {}
        job_name = (obs.get("jobRef") or {}).get("name") or evidence_fetch_job_name(
            final["metadata"]["uid"], 1)
        # THE TTL IS PATCHED AFTER THE VERDICT COMMITS — a second or two later.
        job = None
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            job = get_opt("job", job_name)
            if job is None or (job.get("spec") or {}).get("ttlSecondsAfterFinished"):
                break
            time.sleep(2)
        pod = owned_pod_of(job)
        plan = get_opt("configmap", f"{job_name}-plan")
        for label, body in (("job", job), ("pod", pod), ("plan", plan)):
            artifact(f"objects/evf/{label}-{job_name}.json", body or {})
        clauses = evidence_job_holds_only_the_read_grant(
            job, pod, plan, final, owner_kind="Backup", read_secret="a-archread",
            forbidden_secrets={"a-writer", "logweir-signing-key"})
        sc.detail.update({"job": job_name, "jobUid": ((job or {}).get("metadata") or {}).get("uid"),
                          "podOwnerReferences": ((pod or {}).get("metadata") or {}).get(
                              "ownerReferences"),
                          "clauses": clauses})
        failed = sorted(k for k, ok in clauses.items() if not ok)
        check(job is not None, f"the evidence-fetch Job {job_name} was not found at all")
        check(not failed, "EVF-2: " + "; ".join(failed))
    with Scenario("EVF-1.ttl", "the finished evidence-fetch Job is collected by its TTL") as sc:
        # ttlSecondsAfterFinished 600, plus the TTL controller's own lag.
        gone = False
        deadline = time.monotonic() + EVIDENCE_FETCH_TTL_SECONDS + 120
        while time.monotonic() < deadline:
            if get_opt("job", job_name) is None:
                gone = True
                break
            time.sleep(15)
        sc.detail.update({"job": job_name, "gone": gone})
        check(gone, f"{job_name} still exists {EVIDENCE_FETCH_TTL_SECONDS + 120}s after its "
                    "verdict; the TTL patch never landed or never took effect")


def evf4() -> None:
    """EVF-4: a broken `evidenceRead` grant is `NotAttempted`, and retried."""
    literal_secret("ev-broken", {"access-key-id": "a-evidence-ro"})  # no secret-access-key
    apply(destination("dest-evbroken", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="a-writer", archive_read="a-reader",
                      evidence_write="a-writer", evidence_read="ev-broken",
                      description="evidenceRead names a Secret with no secret-access-key "
                                  "(EVF-4)"))
    artifact("objects/evf4/backupdestination-dest-evbroken.json",
             wait_destination("dest-evbroken"))
    name = f"bk-evf4-{attempt('evf4Attempt')}"
    with Scenario("EVF-4", "a broken evidenceRead grant is NotAttempted with a retry, and "
                           "attempt 2 gets its own Job") as sc:
        seen_two = lambda o: (((o.get("status") or {}).get("evidence") or {})  # noqa: E731
                              .get("observation") or {}).get("attempt") == 2
        final, trail = watched_backup(
            name, backup(name, source="source-admin", topics=["orders"],
                         destination_ref="dest-evbroken"),
            settle=VERDICT_SETTLE_SECONDS + EVIDENCE_FETCH_FIRST_RETRY_SECONDS + 60,
            until=seen_two)
        uid = final["metadata"]["uid"]
        attempt_two = get_opt("job", evidence_fetch_job_name(uid, 2))
        artifact(f"objects/evf4/backup-{name}.json", final)
        artifact(f"objects/evf4/trail-{name}.json", trail)
        artifact(f"objects/evf4/attempt-2-job-{name}.json", attempt_two or {})
        clauses = broken_grant_retries(trail, final, attempt_two)
        sc.detail.update({"backup": name, "uid": uid, "trail": trail, "clauses": clauses})
        failed = sorted(k for k, ok in clauses.items() if not ok)
        check(not failed, "EVF-4: " + "; ".join(failed))


def evf5() -> None:
    """EVF-5: the controller goes away while the fetch Job runs and comes back.

    CLUSTER LOCK. The shared controller is scaled to zero right after the
    `Pending` block names attempt 1's Job, the Job is left to finish with no
    controller at all, and the original replica count is restored in a
    `finally` whatever happens. That is a restart with the widest possible gap:
    the controller that writes the verdict is not the one that created the Job.

    `notRun`, never `pass`, when the fetch beat the harness: a verdict written
    before the old controller's pods were gone measured nothing about a restart.
    """
    title = ("a controller restart while the fetch Job runs creates no second Job, and the "
             "verdict still arrives")
    deploy = get("deployment", "weirkeeper", LAB_NS)
    original = int((deploy.get("spec") or {}).get("replicas") or 1)
    name = f"bk-evf5-{attempt('evf5Attempt')}"
    watch = ObjectWatch("backups", name, seconds=2400)
    try:
        apply(backup(name, source="source-admin", topics=["orders"], destination_ref="dest-a"))
        obj = wait_for("backup", name,
                       lambda o: verdict_of(o) == "Pending" or verdict_of(o) in REACHED_VERDICTS,
                       timeout=900, what="a Pending verdict or any reached one")
        if verdict_of(obj) != "Pending":
            record("EVF-5", title, "notRun",
                   reason=f"the fetch finished ({verdict_of(obj)}) before a Pending read; "
                          "re-run the phase",
                   detail={"backup": name, "trail": watch.trail()})
            return
        uid = obj["metadata"]["uid"]
        job_name = evidence_fetch_job_name(uid, 1)
        down_at = now()
        run(CTX + ["-n", LAB_NS, "scale", "deployment", "weirkeeper", "--replicas=0"],
            timeout=60)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline and get_list(
                "pods", LAB_NS, "app.kubernetes.io/component=control-plane"):
            time.sleep(3)
        gone_at = now()
        while_down = get("backup", name)
        if verdict_of(while_down) != "Pending":
            record("EVF-5", title, "notRun",
                   reason=f"the old controller wrote {verdict_of(while_down)} before its pods "
                          "were gone; nothing about a restart was measured — re-run the phase",
                   detail={"backup": name, "downAt": down_at, "oldPodsGoneAt": gone_at})
            return
        with Scenario("EVF-5", title) as sc:
            wait_for("job", job_name,
                     lambda j: bool((j.get("status") or {}).get("succeeded")
                                    or (j.get("status") or {}).get("failed")),
                     timeout=300, what="the Job to finish with no controller running")
            while_down = get("backup", name)
            run(CTX + ["-n", LAB_NS, "scale", "deployment", "weirkeeper",
                       f"--replicas={original}"], timeout=60)
            run(CTX + ["-n", LAB_NS, "rollout", "status", "deployment/weirkeeper",
                       "--timeout=240s"], timeout=270)
            pods = get_list("pods", LAB_NS, "app.kubernetes.io/component=control-plane")
            starts = [(p.get("status") or {}).get("startTime") for p in pods]
            up_at = min((t for t in starts if t), default=None)
            final = settle_reached_verdict("backup", name)
            time.sleep(2)
            trail = watch.trail()
            jobs = fetch_jobs_for(uid)
            artifact(f"objects/evf5/backup-{name}.json", final)
            artifact(f"objects/evf5/trail-{name}.json", trail)
            artifact(f"objects/evf5/jobs-{name}.json", jobs)
            clauses = restart_resumed_the_fetch(trail, final, jobs, down_at=down_at,
                                                up_at=up_at, pending_before_down=True)
            clauses["the verdict was still Pending while no controller ran"] = (
                verdict_of(while_down) == "Pending")
            sc.detail.update({"backup": name, "uid": uid, "downAt": down_at,
                              "oldPodsGoneAt": gone_at, "upAt": up_at,
                              "originalReplicas": original, "clauses": clauses})
            failed = sorted(k for k, ok in clauses.items() if not ok)
            check(not failed, "EVF-5: " + "; ".join(failed))
    finally:
        watch.stop()
        current = int((get("deployment", "weirkeeper", LAB_NS).get("spec") or {}).get(
            "replicas") or 0)
        if current != original:
            run(CTX + ["-n", LAB_NS, "scale", "deployment", "weirkeeper",
                       f"--replicas={original}"], timeout=60)
            run(CTX + ["-n", LAB_NS, "rollout", "status", "deployment/weirkeeper",
                       "--timeout=240s"], timeout=270, check=False)
        artifact("objects/evf5/controller-restored.json",
                 {"originalReplicas": original,
                  "replicasNow": (get("deployment", "weirkeeper", LAB_NS).get("spec") or {})
                  .get("replicas")})


def evf6() -> None:
    """EVF-6: a destination-backed Restore's scorecard, fetched by dest-b's
    `SecretKeys` `evidenceRead` grant, goes Pending -> Valid."""
    facts = backup_facts("bk-a", "a", "lw-a")
    backup_id, finished = facts["backupId"], facts["pointInTime"]
    target_id = get("kafkacluster", "target")["status"]["clusterId"]
    source_id = get("kafkacluster", "source-admin")["status"]["clusterId"]
    index = attempt("evf6Attempt")
    restore_name, approval_name = f"rs-evf6-{index}", f"rs-evf6-approval-{index}"
    prefix = f"d2w14-evf6-{index}-"
    with Scenario("EVF-6", "a Restore's scorecard goes Pending -> Valid through the "
                           "evidence-fetch Job") as sc:
        plan = restore_plan(backup_id, finished, prefix)
        minted = mint_approval(approval_name, restore_name, plan, ticket="EVF6")
        approval_bundle("logweir-approval-bundle", minted, [target_id], source_id)
        watch = ObjectWatch("restores", restore_name, seconds=2400)
        try:
            created = apply({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                "metadata": owned(restore_name),
                "spec": {
                    "sourceDestinationRef": {"name": "dest-a"},
                    "evidenceDestinationRef": {"name": "dest-b"},
                    "sourceArchive": {"url": "logweir-destination://dest-a"},
                    "backupSetRef": backup_id,
                    "pointInTime": finished,
                    "planBytes": minted["planBytes"],
                    "approvalRef": {"name": approval_name},
                    "deadlineSeconds": 900,
                    "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                               "topicNaming": {"prefix": prefix}},
                },
            })
            record_approval(minted)
            wait_for("restore", restore_name, terminal_phase, timeout=900,
                     what="a terminal phase")
            final = settle_reached_verdict("restore", restore_name)
            time.sleep(2)
        finally:
            trail = watch.stop()
        uid = created["metadata"]["uid"]
        obs = ((final.get("status") or {}).get("evidence") or {}).get("observation") or {}
        job = get_opt("job", (obs.get("jobRef") or {}).get("name")
                      or evidence_fetch_job_name(uid, 1))
        artifact(f"objects/evf6/restore-{restore_name}.json", final)
        artifact(f"objects/evf6/trail-{restore_name}.json", trail)
        artifact(f"objects/evf6/job-{restore_name}.json", job or {})
        clauses = restore_fetch_reached_valid(trail, final, job)
        sc.detail.update({"restore": restore_name, "uid": uid, "trail": trail,
                          "clauses": clauses})
        failed = sorted(k for k, ok in clauses.items() if not ok)
        check(not failed, "EVF-6: " + "; ".join(failed))


def s1c() -> None:
    """S1.notAttempted: the two destinations the evidence-fetch Job must NOT read.

    THE NEGATIVE CONTROL for EVF-1. A controller that created a fetch Job for
    every destination-backed run — or one that wrote `Valid` from somewhere
    else — fails here: dest-noread declares no `evidenceRead` at all, and
    dest-ci asks for `ControllerIdentity` at a location the lab's installation
    policy does not allowlist.
    """
    apply(destination("dest-noread", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="a-writer", archive_read="a-reader",
                      evidence_write="a-writer",
                      description="no evidenceRead grant at all (S1.notAttempted)"))
    apply(destination("dest-ci", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="a-writer", archive_read="a-reader",
                      evidence_write="a-writer", evidence_read_mode="ControllerIdentity",
                      description="ControllerIdentity at a location nobody allowlisted "
                                  "(S1.notAttempted)"))
    for dest in ("dest-noread", "dest-ci"):
        artifact(f"objects/s1c/backupdestination-{dest}.json", wait_destination(dest))
    identity = identity_locations_now()
    index = attempt("s1cAttempt")
    with Scenario("S1.notAttempted", "no evidence-fetch Job, and an honest NotAttempted, for a "
                                     "grant neither a pod nor the controller may use") as sc:
        failed_all: list[str] = []
        for dest in ("dest-noread", "dest-ci"):
            name = f"bk-{dest.removeprefix('dest-')}-{index}"
            final, trail = watched_backup(
                name, backup(name, source="source-admin", topics=["orders"],
                             destination_ref=dest))
            jobs = fetch_jobs_for(final["metadata"]["uid"])
            clauses = no_fetch_for_a_grant_no_job_may_use(final, trail, jobs, identity)
            artifact(f"objects/s1c/backup-{name}.json", final)
            sc.detail.setdefault("runs", {})[dest] = {
                "backup": name, "trail": trail, "fetchJobs": [j["metadata"]["name"] for j in jobs],
                "verification": ((final.get("status") or {}).get("evidence") or {}).get(
                    "verification"),
                "clauses": clauses}
            failed_all += [f"{dest}: {k}" for k, ok in clauses.items() if not ok]
        sc.detail["policyControllerIdentityLocations"] = identity
        check(not failed_all, "S1.notAttempted: " + "; ".join(failed_all))


# --------------------------------------------------------------------------
# S2 — transport and addressing validation
# --------------------------------------------------------------------------


def s2() -> None:
    with Scenario("S2", "transport, addressing and malformed-URL validation") as sc:
        cases = {
            "dest-bad1": destination(
                "dest-bad1", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                endpoint=f"http://minio-a.{NS}.svc:9000", security="TLS",
                archive_write="a-writer"),
            "dest-bad2": destination(
                "dest-bad2", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                endpoint=f"https://minio-tls.{NS}.svc:9000", security="InsecureHTTP",
                archive_write="a-writer"),
            "dest-bad3": destination(
                "dest-bad3", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                endpoint=None, security="InsecureHTTP", archive_write="a-writer"),
        }
        refusals = {}
        for name, obj in cases.items():
            stderr = apply_expect_failure(obj)
            refusals[name] = stderr.strip()
            check("transport.security must match the endpoint scheme" in stderr,
                  f"{name}: refusal did not name the transport/scheme rule: {stderr[:300]}")
            check(get_opt("backupdestination", name) is None,
                  f"{name}: a refused destination exists anyway")
        malformed = {
            "dest-badurl-path": f"http://minio-a.{NS}.svc:9000/bucket/deep",
            "dest-badurl-query": f"http://minio-a.{NS}.svc:9000?x=1",
            "dest-badurl-userinfo": f"http://user:pw@minio-a.{NS}.svc:9000",
        }
        for name, endpoint in malformed.items():
            stderr = apply_expect_failure(
                destination(name, bucket="lw-a", prefix=ARCHIVE_PREFIX, endpoint=endpoint,
                            security="InsecureHTTP", archive_write="a-writer")
            )
            refusals[name] = stderr.strip()
            check("storage.endpoint must be an http(s) origin" in stderr,
                  f"{name}: refusal did not name the origin rule: {stderr[:300]}")
            check(get_opt("backupdestination", name) is None, f"{name}: exists anyway")
        bad_prefix = apply_expect_failure(
            destination("dest-badprefix", bucket="lw-a", prefix="logweir/evidence",
                        endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                        archive_write="a-writer")
        )
        refusals["dest-badprefix"] = bad_prefix.strip()
        check("reserved evidence root" in bad_prefix,
              "the reserved evidence-root prefix was not refused")

        # VirtualHosted is applied, and the controller says the engine cannot use it
        # VirtualHosted is only unhonourable WITH a custom endpoint (engine
        # 0.21.0 forces path-style whenever one is set), so the fixture sets
        # one; CEL admits it because the scheme matches the TLS transport.
        apply(destination("dest-vh", bucket="lw-a", prefix=ARCHIVE_PREFIX,
                          endpoint=f"https://minio-tls.{NS}.svc:9000",
                          security="TLS", addressing="VirtualHosted",
                          archive_write="a-writer"))
        vh = wait_for(
            "backupdestination", "dest-vh",
            lambda o: o.get("status", {}).get("observedGeneration") ==
            o["metadata"]["generation"]
            and any(c.get("type") == "Valid" for c in o.get("status", {}).get("conditions", [])),
            timeout=180, what="a Valid condition for the current generation")
        artifact("objects/s2/backupdestination-dest-vh.json", vh)
        valid = [c for c in vh["status"]["conditions"] if c["type"] == "Valid"][0]
        sc.detail["destVhCondition"] = valid
        check(valid.get("reason") == "AddressingUnsupportedByEngine",
              f"dest-vh Valid reason is {valid.get('reason')!r}, "
              "expected AddressingUnsupportedByEngine")

        # transport.security is immutable in place
        patch = run(
            K + ["patch", "backupdestination", "dest-a", "--type", "merge", "-p",
                 json.dumps({"spec": {"transport": {"security": "TLS"}}})],
            check=False, timeout=60,
        )
        refusals["dest-a transport patch"] = (patch.stdout + patch.stderr).strip()
        check(patch.returncode != 0, "transport.security was changed in place")
        check("transport can never be changed in place" in (patch.stdout + patch.stderr),
              f"the refusal did not name the immutability rule: {(patch.stdout+patch.stderr)[:300]}")
        # storage is immutable too
        storage_patch = run(
            K + ["patch", "backupdestination", "dest-a", "--type", "merge", "-p",
                 json.dumps({"spec": {"storage": {"bucket": "lw-b"}}})],
            check=False, timeout=60,
        )
        refusals["dest-a storage patch"] = (storage_patch.stdout + storage_patch.stderr).strip()
        check(storage_patch.returncode != 0, "storage was changed in place")
        artifact("objects/s2/refusals.json", refusals)
        sc.detail["refusals"] = refusals


def s2b() -> None:
    """The U1 measurement itself cannot run here; its REFUSAL half can, and
    does, because that is the contract this build actually ships."""
    policy = get_opt("configmap", "weirkeeper-policy", LAB_NS)
    reason = (
        "S2b asks for the shared installation policy to be run with "
        "engine.allowUnverifiedCustomCa: true. The lab's weirkeeper-policy "
        "ConfigMap has it false (lab-refresh-2 §6.1), and changing it is a "
        "change to the shared release under the cluster lock that would also "
        "change behaviour for the two other live waves running concurrently. "
        "The engine's SSL_CERT_FILE behaviour on a recorded engine digest is "
        "therefore NOT measured here and ENGINE_CUSTOM_CA_VERIFIED stays false."
    )
    record("S2b", "private-CA HTTPS through the engine (U1)", "notRun", reason=reason,
           detail={"policyConfigMapPresent": policy is not None,
                   "policyKey": "engine.allowUnverifiedCustomCa",
                   "observedValue": json.loads(policy["data"]["policy.json"]).get("engine")
                   if policy and "policy.json" in policy.get("data", {}) else None})

    with Scenario("E1", "a caBundle destination is refused for an engine-driven Backup") as sc:
        created = apply(backup("bk-tls", source="source-admin", topics=["orders"],
                               destination_ref="dest-tls", deadline=300))
        sc.detail["uid"] = created["metadata"]["uid"]
        obj = wait_for(
            "backup", "bk-tls", terminal_phase,
            timeout=420, what="a terminal phase")
        artifact("objects/s2b/backup-bk-tls.json", obj)
        status = obj["status"]
        # The refusal is on the terminal condition and on `status.progress`, not
        # on a top-level `status.reason` — recorded here because D2 §14 wrote it
        # as `{.status.reason}` and this build does not publish it there.
        failed = [c for c in status.get("conditions", []) if c["type"] == "Failed"]
        reasons = {c.get("reason") for c in failed} | {
            status.get("progress", {}).get("reason")}
        sc.detail["status"] = {k: status.get(k) for k in
                               ("phase", "reason", "exitCode", "jobRef")}
        sc.detail["terminalConditionReasons"] = sorted(r for r in reasons if r)
        sc.detail["message"] = (failed[0]["message"] if failed else "")
        check(status.get("phase") == "Failed", f"bk-tls is {status.get('phase')!r}")
        check("CaBundleUnsupportedByEngine" in reasons,
              f"the refusal reason is {sorted(r for r in reasons if r)}, expected "
              "CaBundleUnsupportedByEngine")
        check(status.get("jobRef") is None,
              "a refused destination still produced a Job")
        message = failed[0]["message"] if failed else ""
        check("engine.allowUnverifiedCustomCa" in message,
              "the refusal does not name the administrator's escape hatch")

    with Scenario("E2", "a TLS destination with no CA bundle renders AWS_ALLOW_HTTP=false") as sc:
        created = apply(backup("bk-tls-noca", source="source-admin", topics=["orders"],
                               destination_ref="dest-tls-noca", deadline=300))
        sc.detail["uid"] = created["metadata"]["uid"]
        obj = wait_for("backup", "bk-tls-noca",
                       lambda o: o.get("status", {}).get("jobRef", {}).get("name") is not None
                       or terminal_phase(o),
                       timeout=420, what="a Job or a terminal phase")
        job_name = obj.get("status", {}).get("jobRef", {}).get("name")
        check(job_name is not None,
              f"no Job was rendered: {obj.get('status', {}).get('reason')}")
        facts = job_facts(job_name)
        artifact("objects/s2b/job-bk-tls-noca.json", facts)
        env = facts["env"]
        sc.detail["env"] = {k: v for k, v in env.items() if k.startswith("AWS_") or
                            k.startswith("LOGWEIR_STORE")}
        check(env.get("AWS_ALLOW_HTTP") == "false",
              f"AWS_ALLOW_HTTP is {env.get('AWS_ALLOW_HTTP')!r} for a TLS destination")
        check("AWS_ENDPOINT_URL" not in env, "AWS_ENDPOINT_URL is present")
        check(secret_ref_name(env, "AWS_ACCESS_KEY_ID") == "tls-writer",
              "the archive credential is not the destination's own")
        final = wait_backup("bk-tls-noca", timeout=600)
        artifact("objects/s2b/backup-bk-tls-noca.json", final)
        sc.detail["outcome"] = {k: final["status"].get(k) for k in
                                ("phase", "exitCode", "reason", "message")}


def wait_preflight(name: str, *, timeout: int = 420) -> dict[str, Any]:
    return wait_for("preflight", name,
                    lambda o: o.get("status", {}).get("result", {}).get("state") is not None
                    or terminal_phase(o),
                    timeout=timeout, what="a result or a terminal phase")


# --------------------------------------------------------------------------
# S3 — a denied location, named as denied and not as broken
# --------------------------------------------------------------------------


def s3() -> None:
    with Scenario("S3", "a denied location is refused by name, with no secret in the message") as sc:
        apply(preflight("pf-denied", {
            "operation": "DestinationAccess",
            "destinationAccess": {"destinationRef": {"name": "dest-denied"},
                                  "roles": ["ArchiveRead"]},
        }, timeout_seconds=120))
        obj = wait_preflight("pf-denied", timeout=420)
        artifact("objects/s3/preflight-pf-denied.json", obj)
        result = obj.get("status", {}).get("result", {})
        sc.detail["state"] = result.get("state")
        checks = checks_by_id(obj)
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code"),
                                   "gating": v.get("gating")} for k, v in checks.items()}
        check(result.get("state") == "notReady",
              f"the overall state is {result.get('state')!r}, expected notReady")
        listable = checks.get("destination.archiveListable")
        check(listable is not None,
              f"no destination.archiveListable check ran; ids: {sorted(checks)}")
        check(listable["state"] == "notReady",
              f"destination.archiveListable is {listable['state']!r}")
        check(listable["code"] == "AccessDenied",
              f"destination.archiveListable code is {listable['code']!r}, expected AccessDenied")
        blob = json.dumps(obj)
        for label, value in SECRETS.items():
            check(value not in blob, f"the Preflight object carries the {label} credential")

        created = apply(backup("bk-denied", source="source-admin", topics=["orders"],
                               destination_ref="dest-denied", deadline=300))
        sc.detail["backupUid"] = created["metadata"]["uid"]
        run_obj = wait_backup("bk-denied", timeout=600)
        artifact("objects/s3/backup-bk-denied.json", run_obj)
        status = run_obj["status"]
        sc.detail["backup"] = {k: status.get(k) for k in
                               ("phase", "exitCode", "exitReason", "reason", "message")}
        check(status.get("phase") == "Failed",
              f"bk-denied phase is {status.get('phase')!r}, expected Failed")
        check(status.get("exitCode") == 1,
              f"bk-denied exitCode is {status.get('exitCode')!r}, expected 1 "
              "(execution is authoritative)")
        logs = pod_logs_for_job(status["jobRef"]["name"]) if status.get("jobRef") else ""
        artifact("objects/s3/bk-denied-pod.log", logs)
        for label, value in SECRETS.items():
            check(value not in logs, f"the runner log carries the {label} credential")


# --------------------------------------------------------------------------
# S4 — a reference never leaves its namespace
# --------------------------------------------------------------------------


def s4() -> None:
    with Scenario("S4", "a destinationRef does not resolve across namespaces") as sc:
        created = apply(
            backup("bk-cross", source="source-admin", topics=["orders"],
                   destination_ref="dest-a", deadline=120, namespace=NS_B),
            namespace=NS_B,
        )
        sc.detail["uid"] = created["metadata"]["uid"]
        sc.detail["namespace"] = NS_B
        def reason_of(obj: dict[str, Any]) -> str | None:
            status = obj.get("status", {})
            for condition in status.get("conditions", []):
                if condition.get("reason"):
                    return condition["reason"]
            return status.get("reason") or status.get("progress", {}).get("reason")

        held = wait_for("backup", "bk-cross", lambda o: reason_of(o) is not None,
                        timeout=120, namespace=NS_B, what="a refusal reason")
        artifact("objects/s4/backup-bk-cross-holding.json", held)
        sc.detail["holdingReason"] = reason_of(held)
        sc.detail["holdingCondition"] = held["status"].get("conditions")
        check(reason_of(held) == "DestinationNotFound",
              f"the reason is {reason_of(held)!r}, expected DestinationNotFound")
        check(NS in json.dumps(held["status"].get("conditions")) or
              "namespace-local" in json.dumps(held["status"].get("conditions")),
              "the refusal does not say that a destinationRef is namespace-local")
        # Nothing is created WHILE HOLDING. The namespace also holds the probe
        # Job of its own saved connection, which is not this Backup's, so the
        # assertion is by owner UID and by name rather than by namespace.
        uid = created["metadata"]["uid"]

        def belongs(obj: dict[str, Any]) -> bool:
            owners = obj["metadata"].get("ownerReferences", [])
            return any(o.get("uid") == uid for o in owners) or                 obj["metadata"]["name"].startswith("bk-cross")

        jobs = [j for j in get_list("jobs", namespace=NS_B) if belongs(j)]
        cms = [c for c in get_list("configmaps", namespace=NS_B) if belongs(c)]
        sc.detail["allJobsInNamespaceB"] = [j["metadata"]["name"]
                                            for j in get_list("jobs", namespace=NS_B)]
        sc.detail["jobsOwnedByTheHeldBackup"] = [j["metadata"]["name"] for j in jobs]
        sc.detail["configMapsOwnedByTheHeldBackup"] = [c["metadata"]["name"] for c in cms]
        check(not jobs, f"the held Backup created a Job: {sc.detail['jobsOwnedByTheHeldBackup']}")
        check(not cms,
              f"the held Backup created a ConfigMap: {sc.detail['configMapsOwnedByTheHeldBackup']}")
        # and the destination really does exist next door, under the same name
        sibling = get("backupdestination", "dest-a", NS)
        sc.detail["siblingDestination"] = {
            "namespace": NS, "name": "dest-a", "uid": sibling["metadata"]["uid"]
        }
        final = wait_for("backup", "bk-cross", terminal_phase, timeout=300,
                         namespace=NS_B, what="a terminal phase")
        artifact("objects/s4/backup-bk-cross-final.json", final)
        sc.detail["final"] = {k: final["status"].get(k) for k in
                              ("phase", "reason", "message", "exitCode")}
        check(final["status"].get("phase") == "Failed",
              f"the run ended {final['status'].get('phase')!r}, expected Failed")


# --------------------------------------------------------------------------
# S5 — archive and evidence in two different stores
# --------------------------------------------------------------------------


def _facts_from_manifest(backup_id: str, manifest: dict[str, Any]) -> dict[str, Any]:
    """A backup's coverage, read from the manifest it wrote.

    `status.capture` and `status.windowCovered` are not published on this
    build, so the covered window comes from the document the restore itself
    reads. The point in time is the NEWEST covered record timestamp: the
    manifest's `created_at` is when the run finished uploading, which is after
    coverage ends and which `archive.coverage` correctly refuses."""
    segments = [
        segment
        for topic in manifest.get("topics", [])
        for partition in topic.get("partitions", [])
        for segment in partition.get("segments", [])
    ]
    newest = max((s["end_timestamp"] for s in segments), default=manifest["created_at"])
    oldest = min((s["start_timestamp"] for s in segments), default=manifest["created_at"])
    return {
        "backupId": backup_id,
        "createdAtMs": manifest["created_at"],
        "oldestCoveredMs": oldest,
        "newestCoveredMs": newest,
        # MILLISECONDS, not whole seconds: truncating 1789701831681 to the
        # second lands at …831000, which is BEFORE the oldest covered record,
        # and the window then selects no segment at all.
        "pointInTime": dt.datetime.fromtimestamp(
            newest / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "segmentKeys": [s["key"] for s in segments],
        "manifestTopics": [t["name"] for t in manifest.get("topics", [])],
    }


def backup_facts_flat(name: str, alias: str, bucket: str) -> dict[str, Any]:
    """`backup_facts` for a destination whose `storage.prefix` is empty."""
    backup_id = get("backup", name)["status"]["backupId"]
    return _facts_from_manifest(
        backup_id, json.loads(mc_get(f"{alias}/{bucket}/{backup_id}/manifest.json")))


def backup_facts(name: str, alias: str, bucket: str) -> dict[str, Any]:
    backup_id = get("backup", name)["status"]["backupId"]
    return _facts_from_manifest(
        backup_id,
        json.loads(mc_get(f"{alias}/{bucket}/{ARCHIVE_PREFIX}/{backup_id}/manifest.json")))


def restore_plan(backup_id: str, point_in_time: str, prefix: str,
                 topics: list[str] | None = None) -> dict[str, Any]:
    """`topics` defaults to the `orders` topic most rows back up. A row that
    restores a Backup of OTHER topics must pass that Backup's own
    `manifestTopics`: a plan naming a topic outside the backup set is refused
    by the preflight's blocking `archive.coverage` row (`TopicNotInBackupSet`),
    which is a verdict about the fixture and not about the row (measured on
    lab-refresh-8: S22 read `notReady` because `bk-a2` backs up `payments`)."""
    return {
        "source": {
            "storage": {
                "backend": "s3", "bucket": "lw-a", "prefix": ARCHIVE_PREFIX,
                "region": "us-east-1",
                "endpoint": f"http://minio-a.{NS}.svc:9000",
                "path_style": True, "allow_http": True,
            },
            "backup": backup_id,
            "topics": list(topics) if topics is not None else ["orders"],
        },
        "target": {
            "bootstrap_servers": [f"kafka-target.{NS}.svc.cluster.local:9096"],
            "auth": {"mode": "scramSha512", "username": "admin", "tls": False},
            "mode": "newTopic",
            "topic_naming": {"prefix": prefix},
            "topic_mapping_prefix": "logweir-scratch-",
            "marker_topic": "logweir.scratch",
            "default_replication_factor": 1,
            "teardown": "delete",
        },
        "restore": {"point_in_time": point_in_time},
        "sample": {
            "window_start": "2026-09-01T00:00:00Z",
            "window_end": point_in_time,
            "records_per_partition": 25,
            "anchor": "head",
        },
        "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
        "evidence": {
            "backend": "s3", "bucket": "lw-b", "prefix": "logweir/",
            "region": "us-east-1",
            "endpoint": f"http://minio-b.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        },
        "notifications": {"webhooks": []},
    }


def attempt(key: str) -> int:
    """A per-scenario attempt counter, so a re-run makes NEW objects.

    A `Restore`, its `Approval` and the topics it creates are all sealed or
    already there after one run; re-using the names would measure the first
    attempt's leftovers instead of this one."""
    value = int(state.get(key, 0)) + 1
    state[key] = value
    save()
    return value


def mint_approval(name: str, subject: str, plan: dict[str, Any], *, ticket: str = "D2W14") -> dict[str, Any]:
    """Sign the exact plan bytes with the roster's own approver key and record
    the Approval. The private key never leaves KEYS and never enters an
    artifact."""
    keys = approver_material()
    work = KEYS / name
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    plan_bytes = json.dumps(plan, indent=2) + "\n"
    (work / "plan.json").write_text(plan_bytes)
    run([logweir_cli(), "drill", "approve",
         "--spec", str(work / "plan.json"),
         "--key", str(keys["approver"]),
         "--approver", "d2w14",
         "--ticket", ticket,
         "--subject-kind", "Restore",
         "--out", str(work / "approval.json")], timeout=120)
    return {"planBytes": plan_bytes, "planHash": digest(plan_bytes),
            "approvalPath": str(work / "approval.json"),
            "approvalBytes": (work / "approval.json").read_text(),
            "sidecarBytes": (work / "approval.sig").read_text(),
            "name": name, "subject": subject,
            "approverPub": keys["approverPub"]}


def record_approval(minted: dict[str, Any]) -> dict[str, Any]:
    """Create the `Approval` AFTER its subject exists: the reconciler resolves
    `spec.subjectRef` and refuses a referent that is not there yet."""
    return apply({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
        "metadata": owned(minted["name"]),
        "spec": {
            "approvalBytes": minted["approvalBytes"],
            "sidecarBytes": minted["sidecarBytes"],
            "planHash": minted["planHash"],
            "subjectRef": {"kind": "Restore", "name": minted["subject"]},
        },
    })


def approval_bundle(name: str, minted: dict[str, Any], allowed: list[str], source_id: str) -> None:
    keys = approver_material()
    work = pathlib.Path(minted["approvalPath"]).parent
    apply({
        "apiVersion": "v1", "kind": "Secret", "metadata": owned(name), "type": "Opaque",
        "data": {
            "approval.json": base64.b64encode((work / "approval.json").read_bytes()).decode(),
            "approval.sig": base64.b64encode((work / "approval.sig").read_bytes()).decode(),
            "approver.pub.pem": base64.b64encode(keys["approverPub"].read_bytes()).decode(),
            "allowed-clusters.json": base64.b64encode(json.dumps({
                "allowed_cluster_ids": allowed,
                "source_cluster_id": source_id,
            }).encode()).decode(),
        },
    })


def s5() -> None:
    with Scenario("S5", "archive and evidence resolve to two different destinations") as sc:
        facts = backup_facts("bk-a", "a", "lw-a")
        backup_id = facts["backupId"]
        finished = facts["pointInTime"]
        sc.detail["sourceBackup"] = facts
        target = get("kafkacluster", "target")
        source = get("kafkacluster", "source-admin")
        target_id = target["status"]["clusterId"]
        source_id = source["status"]["clusterId"]
        check(target_id != source_id, "the target cluster id equals the source's")
        run_index = attempt("s5Attempt")
        restore_name = f"rs-split-{run_index}"
        approval_name = f"rs-approval-{run_index}"
        prefix = f"d2w14-restored-{run_index}-"
        sc.detail["restoreName"] = restore_name
        plan = restore_plan(backup_id, finished, prefix)
        minted = mint_approval(approval_name, restore_name, plan)
        approval_bundle("logweir-approval-bundle", minted, [target_id], source_id)
        sc.detail["planHash"] = minted["planHash"]
        sc.detail["clusterIds"] = {"source": source_id, "target": target_id}
        created = apply({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": owned(restore_name),
            "spec": {
                "sourceDestinationRef": {"name": "dest-a"},
                "evidenceDestinationRef": {"name": "dest-b"},
                "sourceArchive": {"url": "logweir-destination://dest-a"},
                "backupSetRef": backup_id,
                "pointInTime": finished,
                "planBytes": minted["planBytes"],
                "approvalRef": {"name": approval_name},
                "deadlineSeconds": 900,
                "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                           "topicNaming": {"prefix": prefix}},
            },
        })
        sc.detail["uid"] = created["metadata"]["uid"]
        record_approval(minted)
        approval = wait_for("approval", approval_name,
                            lambda o: o.get("status", {}).get("verified") is not None,
                            timeout=180, what="a verification verdict")
        artifact(f"objects/s5/approval-{approval_name}.json", approval)
        check(approval["status"].get("verified") is True,
              "the approval did not verify: "
              + json.dumps(approval["status"].get("conditions", [])))
        obj = wait_for("restore", restore_name, terminal_phase, timeout=900,
                       what="a terminal phase")
        artifact(f"objects/s5/restore-{restore_name}.json", obj)
        status = obj["status"]
        sc.detail["restore"] = {k: status.get(k) for k in
                                ("phase", "exitCode", "exitReason", "outcome", "reason",
                                 "message", "newTopics")}
        if status.get("jobRef"):
            facts = job_facts(status["jobRef"]["name"])
            artifact(f"objects/s5/job-{restore_name}.json", facts)
            env = facts["env"]
            sc.detail["jobCredentials"] = {
                "AWS_ACCESS_KEY_ID": secret_ref_name(env, "AWS_ACCESS_KEY_ID"),
                "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID":
                    secret_ref_name(env, "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID"),
                "LOGWEIR_EVIDENCE_CREDENTIALS": env.get("LOGWEIR_EVIDENCE_CREDENTIALS"),
                "LOGWEIR_ARCHIVE_CREDENTIALS": env.get("LOGWEIR_ARCHIVE_CREDENTIALS"),
                "AWS_ENDPOINT_URL": env.get("AWS_ENDPOINT_URL"),
            }
            check("AWS_ENDPOINT_URL" not in env,
                  "the restore Job carries AWS_ENDPOINT_URL")
            check(secret_ref_name(env, "AWS_ACCESS_KEY_ID") == "a-reader",
                  "the archive credential is not dest-a's archiveRead grant: "
                  f"{secret_ref_name(env, 'AWS_ACCESS_KEY_ID')!r}")
            check(secret_ref_name(env, "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID") == "b-writer",
                  "the evidence credential is not dest-b's evidenceWrite grant: "
                  f"{secret_ref_name(env, 'LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID')!r}")
            logs = pod_logs_for_job(status["jobRef"]["name"])
            artifact(f"objects/s5/{restore_name}-pod.log", logs)
        check(status.get("exitCode") == 0,
              f"exitCode {status.get('exitCode')} ({status.get('exitReason')}): "
              f"{status.get('message')}")
        evidence = status.get("evidence", {})
        sc.detail["evidenceKeys"] = evidence
        check(evidence.get("scorecardKey") and evidence.get("sidecarKey"),
              f"no scorecard evidence was recorded: {evidence}")
        # The status's own verdict is S1.statusVerification's subject; here the
        # signed document is verified independently, from the bytes dest-b
        # stores, so this scenario does not rest on the controller's answer.
        keys = approver_material()
        payload = mc_get(f"b/lw-b/{evidence['scorecardKey']}")
        sidecar = mc_get(f"b/lw-b/{evidence['sidecarKey']}")
        outcome = verify_document(payload, sidecar, keys["signingPub"], "scorecard")
        artifact("objects/s5/scorecard.json", payload.decode())
        artifact("objects/s5/scorecard-verify.json", outcome)
        sc.detail["independentVerification"] = outcome
        sc.detail["statusVerificationField"] = evidence.get("verification")
        check(outcome["exitCode"] == 0,
              "the stored scorecard did not verify against the roster's signing key: "
              + outcome["stderr"])
        listing = {"b-evidence": mc_ls("b/lw-b/logweir/"), "a-evidence": mc_ls("a/lw-a/logweir/")}
        artifact("objects/s5/evidence-listing.json", listing)
        sc.detail["evidenceListing"] = listing
        run_key = status.get("evidence", {}).get("scorecardKey", "")
        check(any(evidence["scorecardKey"].split("/")[-1] in line
                  for line in listing["b-evidence"]),
              "the scorecard is not under lw-b/logweir/drills/")
        check(not any("drills" in line for line in listing["a-evidence"]),
              "drill evidence landed in lw-a, which is the ARCHIVE destination")
        sc.detail["scorecardKey"] = run_key
        # a-reader really is read-only where it matters
        denied = mc("cp", "/etc/hosts", f"a/lw-a/{ARCHIVE_PREFIX}/d2w14-probe", check=False)
        sc.detail["aReaderWriteProbe"] = (denied.stdout + denied.stderr).strip()[:400]


# --------------------------------------------------------------------------
# S6 — the controller's own configuration reaches no destination-backed run
# --------------------------------------------------------------------------


def s6() -> None:
    with Scenario("S6", "no global-configuration leakage into a destination-backed run") as sc:
        controller_env = json.loads(
            run(CTX + ["-n", LAB_NS, "get", "deploy", "weirkeeper", "-o",
                       "jsonpath={.spec.template.spec.containers[0].env}"]).stdout
        )
        names = {e["name"]: e.get("value") for e in controller_env}
        sc.detail["controllerGlobalAddressing"] = {
            k: names.get(k) for k in
            ("LOGWEIR_ARCHIVE_URL", "AWS_ENDPOINT_URL", "AWS_ALLOW_HTTP",
             "AWS_REGION", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST")
        }
        check(names.get("AWS_ENDPOINT_URL", "").startswith("http://minio."),
              "the controller has no legacy global endpoint set, so this scenario "
              "would prove nothing")
        created = apply(backup("bk-a2", source="source-admin", topics=["payments"],
                               destination_ref="dest-a", deadline=600))
        sc.detail["uid"] = created["metadata"]["uid"]
        obj = wait_backup("bk-a2", timeout=720)
        artifact("objects/s6/backup-bk-a2.json", obj)
        status = obj["status"]
        check(status.get("phase") == "Succeeded",
              f"bk-a2 is {status.get('phase')!r}: {status.get('message')}")
        facts = job_facts(status["jobRef"]["name"])
        artifact("objects/s6/job-bk-a2.json", facts)
        env = facts["env"]
        sc.detail["jobEnv"] = {k: v for k, v in env.items() if isinstance(v, str)}
        check("AWS_ENDPOINT_URL" not in env,
              "the controller's AWS_ENDPOINT_URL reached the Job")
        for key in ("LOGWEIR_ARCHIVE_URL",):
            check(key not in env, f"the controller's {key} reached the Job")
        check(env.get("AWS_REGION") == "us-east-1",
              f"AWS_REGION is {env.get('AWS_REGION')!r} (the destination's own region)")
        plan = plan_config_map_for(status["jobRef"]["name"])
        if plan is not None:
            artifact("objects/s6/plan-bk-a2.json", plan)
            blob = json.dumps(plan.get("data", {}))
            check("kafka-backups" not in blob,
                  "the frozen plan names the controller's global bucket")
            check(LAB_NS not in blob,
                  "the frozen plan names the controller's global endpoint host")
            sc.detail["planNamesGlobalBucket"] = "kafka-backups" in blob
        listing = mc_ls(f"a/lw-a/{ARCHIVE_PREFIX}/")
        check(any(status["backupId"] in line for line in listing),
              "bk-a2 did not land in its own destination's bucket")

    record(
        "S6.legacy",
        "a legacy inline Backup still succeeds through the controller's global handle",
        "notRun",
        reason=(
            "The legacy inline path takes its addressing from the controller's own "
            "environment and from the installation policy's legacyArchiveAddressing, "
            "both of which point at the SHARED lab MinIO in "
            + LAB_NS
            + ". WORKER-RULES permits that fixture read-only, and repointing the "
            "controller is a shared-release change. The half of S6 that this wave "
            "can prove without writing to a shared store — that a destination-backed "
            "run takes NOTHING from that global configuration while it is set — is "
            "scenario S6 above, and it passed."
        ),
    )



# --------------------------------------------------------------------------
# the large catalog, created over the wire
# --------------------------------------------------------------------------

BULK_COUNT = 5000
BULK_PREFIX = "bulk-"


def free_port() -> int:
    import socket as _socket

    with _socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PortForward:
    """`kubectl port-forward` with a ceiling on every wait, so a broker that
    never answers ends the phase instead of the wave."""

    def __init__(self, pod: str, remote: int) -> None:
        self.pod = pod
        self.remote = remote
        self.local = free_port()
        self.proc: subprocess.Popen[str] | None = None

    def __enter__(self) -> "PortForward":
        self.proc = subprocess.Popen(
            K + ["port-forward", f"pod/{self.pod}", f"{self.local}:{self.remote}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        import socket as _socket

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("port-forward exited: " + (self.proc.stdout.read() if self.proc.stdout else ""))
            try:
                with _socket.create_connection(("127.0.0.1", self.local), timeout=2):
                    return self
            except OSError:
                time.sleep(1)
        raise TimeoutError("port-forward never accepted a connection within 60 s")

    def __exit__(self, *exc: Any) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def kafka_acl_pod() -> str:
    pods = [p for p in get_list("pods", selector="app=kafka-acl")
            if p["status"]["phase"] == "Running"]
    if not pods:
        raise RuntimeError("no running kafka-acl pod")
    return pods[0]["metadata"]["name"]


def bulk() -> None:
    """Create the 5,000 `bulk-*` topics S7 needs, then record the broker's own
    list as the set every later assertion is compared against."""
    helper = pathlib.Path(__file__).resolve().parent / "bulk_topics.py"
    with PortForward(kafka_acl_pod(), 9092) as forward:
        proc = run(
            [sys.executable, str(helper), "--port", str(forward.local),
             "--prefix", BULK_PREFIX, "--count", str(BULK_COUNT), "--batch", "500"],
            timeout=1800,
        )
        artifact("fixtures/bulk-topics.log", proc.stdout + proc.stderr)
        # THE BASELINE IS ONLY A BASELINE ONCE THE BROKER HAS STOPPED MOVING.
        # `CreateTopics` returning 0 for five thousand names does not mean five
        # thousand topics are in metadata: the previous snapshot, taken in this
        # same port-forward, recorded 503 of 5,000 and a minute later the
        # controller's own discovery listed 3,004. S7 asserts
        # `returned == len(this file)`, so a mid-convergence snapshot is not a
        # slightly-wrong baseline — it is one that guarantees the row fails
        # (lab-refresh-5 §7.2). `--list` now holds until the count is still, and
        # exits 2 rather than printing a listing somebody would save.
        listing = run(
            [sys.executable, str(helper), "--port", str(forward.local), "--list",
             "--stable-for", "25", "--converge-timeout", "900"],
            timeout=1200,
            check=False,
        )
    settle = [ln for ln in listing.stderr.splitlines() if ln.startswith("#")]
    artifact("fixtures/broker-topics-convergence.txt", "\n".join(settle))
    if listing.returncode != 0:
        raise RuntimeError(
            "the broker's topic count never settled, so no baseline was recorded: "
            + " ".join(settle)
        )
    rows = [line.split("\t") for line in listing.stdout.splitlines() if line.strip()]
    names = sorted(row[0] for row in rows)
    user_topics = sorted(row[0] for row in rows if len(row) > 1 and row[1] == "user")
    artifact("fixtures/broker-topics-all.tsv", listing.stdout)
    artifact("fixtures/broker-topics-user.txt", "\n".join(user_topics))
    state["brokerTopics"] = {"all": len(names), "user": len(user_topics)}
    state["userTopicsSha256"] = digest("\n".join(user_topics))
    state["brokerConvergence"] = settle
    save()
    record(
        "S7.baseline",
        "the broker's topic list is recorded only after it stops moving",
        "pass" if len(user_topics) >= BULK_COUNT else "fail",
        detail={"userTopics": len(user_topics), "allTopics": len(names),
                "requested": BULK_COUNT, "convergence": settle},
        reason="" if len(user_topics) >= BULK_COUNT else
        f"the converged broker holds {len(user_topics)} user topics and this run asked for "
        f"{BULK_COUNT}: the baseline is settled but short, so S7 would compare the "
        f"discovery against a smaller catalog than the one it was asked to build",
    )
    log(f"bulk: {len(user_topics)} user topics, {len(names) - len(user_topics)} internal; "
        f"{' '.join(settle)}")


def broker_user_topics() -> list[str]:
    path = ART / "fixtures/broker-topics-user.txt"
    if not path.is_file():
        raise RuntimeError("run the `bulk` phase first: no recorded broker topic list")
    return [line for line in path.read_text().splitlines() if line]


def relayed_check_result(job_name: str, tag: str) -> Any:
    """The runner's OWN verdict, decoded from the frames it printed.

    The controller projects a `reason` onto the object; when the two disagree
    the disagreement is the finding, so both are recorded."""
    logs = pod_logs_for_job(job_name)
    artifact(f"objects/{tag}/pod.log", logs)
    for line in logs.splitlines():
        if line.startswith("logweir-check-part=result:"):
            try:
                return json.loads(base64.b64decode(line.split(":", 2)[2]).decode())
            except Exception:  # noqa: BLE001 - this is evidence, not control flow
                return line[:400]
    return None


def relayed_codes(result: Any) -> list[str]:
    if not isinstance(result, dict):
        return []
    return [c.get("code") for c in result.get("checks", []) if c.get("code")]


def wait_discovery(name: str, *, timeout: int = 300) -> dict[str, Any]:
    return wait_for("topicdiscovery", name, terminal_phase, timeout=timeout,
                    what="a terminal phase")


# --------------------------------------------------------------------------
# S7 — a large catalog, chunked, indexed and checkable
# --------------------------------------------------------------------------


def s7() -> None:
    with Scenario("S7", "a 5,003-topic catalog is returned in verified immutable chunks") as sc:
        expected = broker_user_topics()
        created = apply(topic_discovery("td-full", "source-admin", max_topics=20000,
                                        timeout_seconds=120))
        sc.detail["uid"] = created["metadata"]["uid"]
        started = time.monotonic()
        obj = wait_discovery("td-full", timeout=420)
        sc.detail["secondsToTerminal"] = round(time.monotonic() - started, 1)
        artifact("objects/s7/topicdiscovery-td-full.json", obj)
        status = obj["status"]
        check(status.get("phase") == "Succeeded",
              f"td-full is {status.get('phase')!r} ({status.get('reason')}): "
              f"{status.get('message')}")
        result = status["result"]
        counts = result["counts"]
        sc.detail["counts"] = counts
        sc.detail["brokerUserTopics"] = len(expected)
        check(counts["returned"] == len(expected),
              f"returned {counts['returned']}, the broker has {len(expected)} user topics")
        check(counts["returned"] == BULK_COUNT + 3,
              f"returned {counts['returned']}, expected {BULK_COUNT + 3} "
              "(5,000 bulk plus orders, payments and audit)")
        chunks = result["chunks"]
        sc.detail["chunkIndex"] = chunks
        check(len(chunks) == 3, f"{len(chunks)} chunks, expected 3 at 2,500 lines each")
        td_uid = obj["metadata"]["uid"]
        names: list[str] = []
        for index, entry in enumerate(chunks):
            cm = get("configmap", entry["name"])
            artifact(f"objects/s7/chunk-{entry['name']}.json",
                     {k: v for k, v in cm.items() if k != "data"})
            check(cm.get("immutable") is True, f"{entry['name']} is not immutable")
            owners = cm["metadata"].get("ownerReferences", [])
            check(any(o.get("uid") == td_uid for o in owners),
                  f"{entry['name']} is not owned by the TopicDiscovery ({owners})")
            data = cm["data"]["topics.tsv"]
            actual = hashlib.sha256(data.encode()).hexdigest()
            annotation = cm["metadata"]["annotations"]["logweir.dev/result-sha256"]
            check(annotation.endswith(actual),
                  f"{entry['name']}: annotation {annotation} != sha256 {actual}")
            check(entry["sha256"].endswith(actual),
                  f"{entry['name']}: status sha256 {entry['sha256']} != {actual}")
            check(cm["metadata"]["annotations"]["logweir.dev/chunk"] ==
                  f"{index + 1}/{len(chunks)}",
                  f"{entry['name']}: chunk annotation is "
                  f"{cm['metadata']['annotations']['logweir.dev/chunk']!r}")
            rows = [line.split("\t")[0] for line in data.splitlines() if line.strip()]
            check(len(rows) == entry["count"],
                  f"{entry['name']}: {len(rows)} rows, index says {entry['count']}")
            names += rows
        sc.detail["namesReturned"] = len(names)
        check(len(names) == len(set(names)), "a topic name appears in more than one chunk")
        check(sorted(names) == sorted(expected),
              "the union of the chunks is not the broker's own user-topic set "
              f"(missing {sorted(set(expected) - set(names))[:5]}, "
              f"extra {sorted(set(names) - set(expected))[:5]})")
        artifact("objects/s7/td-full-topics.txt", "\n".join(sorted(names)))
        sc.detail["topicsSha256"] = result.get("topicsSha256")
        sc.detail["visibility"] = result.get("visibility")
        sc.detail["freshUntil"] = status.get("freshUntil")
        sc.detail["binding"] = status.get("binding")


# --------------------------------------------------------------------------
# S8 — internal topics are excluded by default and namable on request
# --------------------------------------------------------------------------


def s8() -> None:
    with Scenario("S8", "internal topics are excluded by default and flagged when asked for") as sc:
        full = get("topicdiscovery", "td-full")
        counts = full["status"]["result"]["counts"]
        sc.detail["internalExcluded"] = counts.get("internalExcluded")
        check(counts.get("internalExcluded", 0) >= 1,
              f"internalExcluded is {counts.get('internalExcluded')}, expected at least 1")
        rows = topics_from_chunks(full)
        names = {r["name"] for r in rows}
        check("__consumer_offsets" not in names,
              "__consumer_offsets is in the default discovery's chunks")
        check(not any(n.startswith("__") for n in names),
              "an internal topic is in the default discovery's chunks")

        apply(topic_discovery("td-internal", "source-admin", max_topics=20000,
                              include_internal=True, timeout_seconds=120))
        obj = wait_discovery("td-internal", timeout=420)
        artifact("objects/s8/topicdiscovery-td-internal.json", obj)
        check(obj["status"]["phase"] == "Succeeded",
              f"td-internal is {obj['status']['phase']!r}: {obj['status'].get('message')}")
        rows = topics_from_chunks(obj)
        by_name = {r["name"]: r["fields"] for r in rows}
        sc.detail["internalCounts"] = obj["status"]["result"]["counts"]
        check("__consumer_offsets" in by_name,
              f"__consumer_offsets is absent from includeInternal: true "
              f"({len(by_name)} topics)")
        fields = by_name["__consumer_offsets"]
        sc.detail["consumerOffsetsRow"] = fields
        check(any("internal" in f for f in fields),
              f"__consumer_offsets carries no internal flag: {fields}")


# --------------------------------------------------------------------------
# S9 — an ACL-limited principal never claims completeness
# --------------------------------------------------------------------------


def s9() -> None:
    with Scenario("S9", "an ACL-limited principal is reported limited, not complete") as sc:
        apply(topic_discovery("td-limited", "source-limited",
                              expected=["orders", "payments"], timeout_seconds=120))
        obj = wait_discovery("td-limited", timeout=420)
        artifact("objects/s9/topicdiscovery-td-limited.json", obj)
        status = obj["status"]
        check(status["phase"] == "Succeeded",
              f"td-limited is {status['phase']!r}: {status.get('message')}")
        result = status["result"]
        names = sorted(r["name"] for r in topics_from_chunks(obj))
        sc.detail["visibleTopics"] = names
        sc.detail["visibility"] = result.get("visibility")
        sc.detail["expected"] = result.get("expected")
        check(names == ["orders"],
              f"the limited principal saw {names}, expected exactly ['orders']")
        visibility = result.get("visibility", {})
        check(visibility.get("state") == "limited",
              f"visibility state is {visibility.get('state')!r}, expected limited")
        basis = visibility.get("basis", [])
        check(any("expectedTopicNotAuthorized" in b for b in basis),
              f"basis does not carry expectedTopicNotAuthorized: {basis}")
        check(result["expected"]["notAuthorized"] == 1,
              f"expected.notAuthorized is {result['expected']['notAuthorized']}, expected 1")

        apply(topic_discovery("td-limited-noexpect", "source-limited", timeout_seconds=120))
        second = wait_discovery("td-limited-noexpect", timeout=420)
        artifact("objects/s9/topicdiscovery-td-limited-noexpect.json", second)
        vis = second["status"]["result"].get("visibility", {})
        sc.detail["noExpectVisibility"] = vis
        check(second["status"]["phase"] == "Succeeded",
              f"td-limited-noexpect is {second['status']['phase']!r}")
        check(vis.get("state") == "unknown",
              f"with nothing expected the state is {vis.get('state')!r}, expected unknown")
        check(vis.get("basis") == ["listingOnly"],
              f"basis is {vis.get('basis')!r}, expected ['listingOnly']")

    policy = get_opt("configmap", "weirkeeper-policy", LAB_NS)
    attestations = None
    if policy and "policy.json" in policy.get("data", {}):
        attestations = json.loads(policy["data"]["policy.json"]).get(
            "discovery", {}).get("visibilityAttestations")
    record(
        "S9.attested", "attestedComplete under a policy attestation, and its expiry",
        "notRun",
        reason=(
            "`attestedComplete` requires an administrator-governed attestation in the "
            "SHARED installation policy ConfigMap `weirkeeper-policy` in " + LAB_NS + ". "
            "lab-refresh-2 §6.1 left `discovery.visibilityAttestations` empty on purpose "
            "so the lab stays honest, and adding one is a change to the shared release "
            "that would also apply to the two other live waves. Observed value: "
            + json.dumps(attestations)
        ),
        detail={"visibilityAttestations": attestations},
    )


# --------------------------------------------------------------------------
# S10 — an empty cluster is empty, not unknown-and-empty-looking
# --------------------------------------------------------------------------


def s10() -> None:
    with Scenario("S10", "an empty cluster returns zero topics and claims nothing") as sc:
        apply(topic_discovery("td-empty", "empty", timeout_seconds=120))
        obj = wait_discovery("td-empty", timeout=420)
        artifact("objects/s10/topicdiscovery-td-empty.json", obj)
        status = obj["status"]
        check(status["phase"] == "Succeeded",
              f"td-empty is {status['phase']!r} ({status.get('reason')}): "
              f"{status.get('message')}")
        result = status["result"]
        sc.detail["counts"] = result["counts"]
        sc.detail["visibility"] = result.get("visibility")
        sc.detail["chunks"] = result.get("chunks")
        check(result["counts"]["returned"] == 0,
              f"returned {result['counts']['returned']} topics from an empty cluster")
        check(result.get("visibility", {}).get("state") == "unknown",
              f"visibility is {result.get('visibility', {}).get('state')!r}, expected unknown")
        check(not result.get("chunks"),
              f"an empty result wrote {len(result.get('chunks', []))} chunk ConfigMaps")


# --------------------------------------------------------------------------
# S11 — a broker that never answers is a failure with a name
# --------------------------------------------------------------------------


def s11() -> None:
    with Scenario("S11", "an unreachable broker fails by name and writes no chunk") as sc:
        before = {c["metadata"]["name"] for c in get_list("configmaps")}
        apply(topic_discovery("td-timeout", "blackhole", timeout_seconds=10))
        started = time.monotonic()
        obj = wait_discovery("td-timeout", timeout=300)
        sc.detail["secondsToTerminal"] = round(time.monotonic() - started, 1)
        artifact("objects/s11/topicdiscovery-td-timeout.json", obj)
        status = obj["status"]
        sc.detail["status"] = {k: status.get(k) for k in
                               ("phase", "reason", "message", "jobRef")}
        after = {c["metadata"]["name"] for c in get_list("configmaps")}
        new_chunks = sorted(n for n in after - before if "-r0" in n)
        sc.detail["newChunkConfigMaps"] = new_chunks
        job_conditions: list[dict[str, Any]] = []
        relayed: Any = None
        if status.get("jobRef"):
            job = get_opt("job", status["jobRef"]["name"])
            if job is not None:
                job_conditions = job.get("status", {}).get("conditions", [])
            logs = pod_logs_for_job(status["jobRef"]["name"])
            artifact("objects/s11/td-timeout-pod.log", logs)
            # The relayed frame is the runner's own verdict, base64 on one line.
            for line in logs.splitlines():
                if line.startswith("logweir-check-part=result:"):
                    try:
                        relayed = json.loads(
                            base64.b64decode(line.split(":", 2)[2]).decode())
                    except Exception:  # noqa: BLE001 - evidence, not control flow
                        relayed = line[:200]
        sc.detail["jobConditions"] = job_conditions
        sc.detail["relayedCheckResult"] = relayed
        artifact("objects/s11/td-timeout-relayed.json", relayed)
        # THE FIFTH CRITERION, AND WHY IT IS RESTATED RATHER THAN DROPPED.
        # D2 §14.4's S11 line ends "…; no chunk `ConfigMap`s; Job `Failed`." The
        # requirements table two sections up splits the case in two: a Job
        # `DeadlineExceeded` becomes `Failed/DeadlineExceeded`, and a KAFKA
        # timeout becomes `Failed/BrokerUnreachable` via
        # `check_cli::metadata_timeout_code`. This scenario is the second path,
        # where the runner classifies the failure ITSELF — it relays a readable
        # frame naming `BrokerUnreachable` and exits 0, so its Job completes.
        # Requiring `Job Failed` here required the first path's shape from the
        # second path's scenario, and D2-RESULTUNREADABLE being fixed is exactly
        # what made the frame readable and the Job succeed (lab-refresh-3 §8.4).
        # What the contract now says is asserted instead: the Job ENDED, and the
        # reason on the object came from the runner's own blocking check rather
        # than from the controller guessing at a dead Job. Asserting only "the
        # Job ended" would accept a Job that merely finished, so the frame and
        # the projection are required to agree.
        #
        # NO DEVIATION ANY MORE. The wording was amended on main at `ce69be4`;
        # what travels with `results.json` is now a CITATION of the criterion
        # this row asserts, so the next reader is not told to fix a document
        # that is already correct (review L-3).
        blocking = [c for c in ((relayed or {}).get("checks") or [])
                    if isinstance(c, dict) and c.get("gating") == "blocking"
                    and c.get("state") == "notReady"]
        sc.detail["jobTerminalConditions"] = [
            c["type"] for c in job_conditions
            if c.get("type") in {"Complete", "Failed"} and c.get("status") == "True"
        ]
        sc.detail["jobIsComplete"] = "Complete" in sc.detail["jobTerminalConditions"]
        sc.detail["blockingNotReadyCodes"] = [c.get("code") for c in blocking]
        sc.detail["contractCitation"] = (
            "D2 §14.4 S11 as amended 2026-09-18 (main `ce69be4`, D2 W14 / lab-refresh-3): "
            "\"the check Job is `Complete` — the runner exits 0 after relaying its "
            "`notReady` result and the failure is carried in the projected reason\". This "
            "row asserts exactly that: `Complete`, and a projected reason equal to the code "
            "of the runner's own blocking `notReady` check. `Failed` is the deadline path "
            "and not this fixture."
        )
        criteria = s11_criteria(status, job_conditions, relayed, new_chunks)
        sc.detail["criteria"] = criteria
        sc.detail["observedReason"] = status.get("reason")
        sc.detail["observedMessage"] = status.get("message")
        failed = sorted(k for k, ok in criteria.items() if not ok)
        check(not failed,
              "D2 §14.4 S11 criteria not met: " + "; ".join(failed)
              + f". Observed status.reason={status.get('reason')!r} "
              f"({status.get('message')!r}); the runner's own relayed check said "
              f"code={((relayed or {}).get('checks') or [{}])[0].get('code')!r}.")


# --------------------------------------------------------------------------
# S12 — a rotated credential, and what a stale success still says
# --------------------------------------------------------------------------


def s12() -> None:
    with Scenario("S12", "credential rotation: a failure that names itself, then a refresh") as sc:
        # START FROM A STATE THIS PHASE ASSERTS. A previous attempt may have
        # moved the broker's password without moving the Secret; re-running
        # then measures that leftover rather than the rotation.
        set_scram_credential("kafka-acl", "rotating", SECRETS["kafka-rotating-1"])
        literal_secret("kafka-rotating", {"password": SECRETS["kafka-rotating-1"]})
        time.sleep(5)
        apply(topic_discovery("td-rot0", "source-rotating", timeout_seconds=120))
        first = wait_discovery("td-rot0", timeout=420)
        artifact("objects/s12/topicdiscovery-td-rot0.json", first)
        if first["status"]["phase"] != "Succeeded":
            sc.detail["baselineRelayed"] = relayed_check_result(
                (first["status"].get("jobRef") or {}).get("name", ""), "s12-baseline")
        check(first["status"]["phase"] == "Succeeded",
              f"the baseline discovery is {first['status']['phase']!r}: "
              f"{first['status'].get('message')}")
        sc.detail["baselineFreshUntil"] = first["status"].get("freshUntil")
        sc.detail["baselineReturned"] = first["status"]["result"]["counts"]["returned"]

        # The BROKER's credential changes; the Secret does not.
        set_scram_credential("kafka-acl", "rotating", SECRETS["kafka-rotating-2"])
        apply(topic_discovery("td-rot1", "source-rotating", timeout_seconds=60))
        failed = wait_discovery("td-rot1", timeout=420)
        artifact("objects/s12/topicdiscovery-td-rot1.json", failed)
        sc.detail["afterBrokerRotation"] = {
            k: failed["status"].get(k) for k in ("phase", "reason", "message")
        }
        relayed = relayed_check_result(
            (failed["status"].get("jobRef") or {}).get("name", ""), "s12") \
            if failed["status"].get("jobRef") else None
        sc.detail["relayedCheckResult"] = relayed
        sc.detail["relayedCodes"] = relayed_codes(relayed)
        criteria = {
            "phase == Failed": failed["status"]["phase"] == "Failed",
            "status.reason == AuthenticationFailed":
                failed["status"].get("reason") == "AuthenticationFailed",
        }
        sc.detail["criteria"] = criteria
        still = get("topicdiscovery", "td-rot0")
        check(still["status"]["phase"] == "Succeeded",
              "the earlier success stopped being readable after a later failure")
        sc.detail["earlierSuccessStillReadable"] = True

        # Now the Secret catches up.
        literal_secret("kafka-rotating", {"password": SECRETS["kafka-rotating-2"]})
        apply(topic_discovery("td-rot2", "source-rotating", timeout_seconds=120))
        healed = wait_discovery("td-rot2", timeout=420)
        artifact("objects/s12/topicdiscovery-td-rot2.json", healed)
        sc.detail["afterSecretRotation"] = {
            k: healed["status"].get(k) for k in ("phase", "reason", "message")
        }
        check(healed["status"]["phase"] == "Succeeded",
              f"td-rot2 is {healed['status']['phase']!r} after the Secret was updated: "
              f"{healed['status'].get('message')}")
        check(healed["status"]["result"]["counts"]["returned"] ==
              first["status"]["result"]["counts"]["returned"],
              "the refreshed discovery returned a different number of topics")
        # The whole rotation story is proved above; this is the one criterion
        # this build does not meet, judged last so nothing else is lost to it.
        unmet = sorted(k for k, ok in criteria.items() if not ok)
        check(not unmet,
              "D2 §14.4 S12 criteria not met: " + "; ".join(unmet)
              + f". Observed status.reason={failed['status'].get('reason')!r} "
              f"({failed['status'].get('message')!r}); the runner's own relayed check "
              f"said {relayed_codes(relayed)}.")

    with Scenario("S12.gc", "at most five terminal discoveries survive per connection") as sc:
        for index in range(3, 7):
            apply(topic_discovery(f"td-rot{index}", "source-rotating", timeout_seconds=120))
            wait_discovery(f"td-rot{index}", timeout=420)
        time.sleep(30)
        remaining = [
            d for d in get_list("topicdiscoveries")
            if d["spec"]["request"]["connectionRef"]["name"] == "source-rotating"
        ]
        names = sorted(d["metadata"]["name"] for d in remaining)
        sc.detail["remaining"] = names
        sc.detail["count"] = len(names)
        artifact("objects/s12/rotating-discoveries.json", names)
        check(len(names) <= 5,
              f"{len(names)} terminal discoveries survive for one connection: {names}. "
              "D2 §5.8 keeps the last five.")


# --------------------------------------------------------------------------
# S13 — a cancellation is one wish, and it is honoured
# --------------------------------------------------------------------------


def s13() -> None:
    with Scenario("S13", "a running discovery is cancelled, and the wish cannot be unwished") as sc:
        apply(topic_discovery("td-cancel", "blackhole", timeout_seconds=300))
        running = wait_for("topicdiscovery", "td-cancel",
                           lambda o: o.get("status", {}).get("phase") == "Running",
                           timeout=240, what="phase Running")
        sc.detail["jobRef"] = running["status"].get("jobRef")
        started = time.monotonic()
        run(K + ["patch", "topicdiscovery", "td-cancel", "--type", "merge", "-p",
                 json.dumps({"spec": {"cancelRequested": True}})], timeout=60)
        cancelled = wait_for("topicdiscovery", "td-cancel",
                             lambda o: o.get("status", {}).get("phase") == "Cancelled",
                             timeout=120, what="phase Cancelled")
        sc.detail["secondsToCancelled"] = round(time.monotonic() - started, 1)
        artifact("objects/s13/topicdiscovery-td-cancel.json", cancelled)
        check(sc.detail["secondsToCancelled"] <= 60,
              f"cancellation took {sc.detail['secondsToCancelled']}s")
        job_name = (cancelled["status"].get("jobRef") or {}).get("name")
        if job_name:
            # U3: "patching a running Job's activeDeadlineSeconds terminates it
            # as DeadlineExceeded". `FailureTarget` is the 1.31+ interim
            # condition that becomes `Failed`; both carry the same reason, and
            # either is the measurement U3 asks for.
            def terminated(job: dict[str, Any]) -> bool:
                return job["spec"].get("suspend") is True or any(
                    c["type"] in {"Failed", "FailureTarget"} and c["status"] == "True"
                    for c in job.get("status", {}).get("conditions", [])
                )

            job = wait_for("job", job_name, terminated, timeout=120,
                           what="a terminal Job condition or a suspension")
            conditions = job.get("status", {}).get("conditions", [])
            sc.detail["jobConditions"] = conditions
            sc.detail["jobSuspended"] = job["spec"].get("suspend")
            sc.detail["jobActiveDeadlineSeconds"] = job["spec"].get("activeDeadlineSeconds")
            reasons = {c.get("reason") for c in conditions
                       if c["type"] in {"Failed", "FailureTarget"}}
            sc.detail["jobTerminationReasons"] = sorted(r for r in reasons if r)
            check("DeadlineExceeded" in reasons or job["spec"].get("suspend") is True,
                  f"the Job neither ended DeadlineExceeded nor was suspended: {conditions}")
            sc.detail["u3Verified"] = "DeadlineExceeded" in reasons
        deadline = time.monotonic() + 90
        pods = []
        while time.monotonic() < deadline:
            pods = [p for p in get_list("pods", selector=f"batch.kubernetes.io/job-name={job_name}")
                    if p["status"]["phase"] in {"Running", "Pending"}]
            if not pods:
                break
            time.sleep(5)
        sc.detail["livePodsAfterCancel"] = [p["metadata"]["name"] for p in pods]
        check(not pods, f"a pod is still alive after the cancellation: {sc.detail['livePodsAfterCancel']}")
        undo = run(K + ["patch", "topicdiscovery", "td-cancel", "--type", "merge", "-p",
                        json.dumps({"spec": {"cancelRequested": False}})],
                   check=False, timeout=60)
        sc.detail["unwish"] = (undo.stdout + undo.stderr).strip()[:300]
        check(undo.returncode != 0, "cancelRequested was set back to false")
        check("may only change from false to true" in (undo.stdout + undo.stderr),
              f"the refusal did not name the CEL rule: {sc.detail['unwish']}")



# --------------------------------------------------------------------------
# S14 — every way a Backup is not ready, named with its remedy
# --------------------------------------------------------------------------


def backup_preflight(name: str, source: str, *, topics: list[str] = ["orders"],
                     destination: str = "dest-a", timeout_seconds: int = 90) -> dict[str, Any]:
    return preflight(name, {
        "operation": "Backup",
        "backup": {
            "sourceRef": {"name": source},
            "topics": topics,
            "destinationRef": {"name": destination},
        },
    }, timeout_seconds=timeout_seconds)


def s14() -> None:
    cases = [
        ("S14a", "a missing credential Secret", "pf-missing-secret", "missing-secret",
         "connection.credentialProjected", "CredentialSecretNotFound", "missing-secret"),
        ("S14b", "a Secret without the named key", "pf-missing-key", "missing-key",
         "connection.credentialProjected", "CredentialSecretKeyMissing", ""),
        ("S14c", "a wrong password", "pf-wrong-password", "wrong-password",
         "connection.authenticated", "AuthenticationFailed", ""),
    ]
    for sid, title, obj_name, connection, check_id, code, needle in cases:
        with Scenario(sid, f"Backup readiness names {title}") as sc:
            apply(backup_preflight(obj_name, connection, timeout_seconds=90))
            started = time.monotonic()
            obj = wait_preflight(obj_name, timeout=420)
            sc.detail["seconds"] = round(time.monotonic() - started, 1)
            artifact(f"objects/s14/preflight-{obj_name}.json", obj)
            result = obj.get("status", {}).get("result", {})
            checks = checks_by_id(obj)
            sc.detail["state"] = result.get("state")
            sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                                   for k, v in checks.items()}
            entry = checks.get(check_id)
            check(entry is not None,
                  f"{check_id} did not run; ids: {sorted(checks)}")
            sc.detail["check"] = entry
            check(entry["state"] == "notReady",
                  f"{check_id} is {entry['state']!r}: {entry.get('message')}")
            check(entry["code"] == code,
                  f"{check_id} code is {entry['code']!r}, expected {code}")
            check(entry.get("remedy"), f"{check_id} carries no remedy")
            check(entry.get("observedAt"), f"{check_id} carries no check time")
            check(sc.detail["seconds"] <= 90,
                  f"the result took {sc.detail['seconds']}s; D2 §14.4 S14 allows 90 s")
            # `scope` is judged as its own scenario (E3) because D2 §14.4's S14
            # criteria do not name it and the tracker's acceptance sentence does.
            sc.detail["scopePresent"] = {k: bool(v.get("scope")) for k, v in checks.items()}
            if needle:
                check(needle in json.dumps(entry),
                      f"{check_id} does not name {needle!r}: {entry}")
            check(result.get("state") == "notReady",
                  f"the overall state is {result.get('state')!r}")

    with Scenario("S14f", "an unreachable broker blocks its dependents by name") as sc:
        apply(backup_preflight("pf-blackhole", "blackhole", timeout_seconds=60))
        obj = wait_preflight("pf-blackhole", timeout=420)
        artifact("objects/s14/preflight-pf-blackhole.json", obj)
        checks = checks_by_id(obj)
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                               for k, v in checks.items()}
        auth = checks.get("connection.authenticated")
        check(auth is not None, f"connection.authenticated did not run: {sorted(checks)}")
        check(auth["code"] in {"BrokerUnreachable", "MetadataTimeout"},
              f"connection.authenticated code is {auth['code']!r}")
        blocked = [k for k, v in checks.items() if v.get("code") == "BlockedByPrerequisite"]
        sc.detail["blockedByPrerequisite"] = blocked
        check(blocked, "nothing was reported BlockedByPrerequisite behind an "
                       "unreachable broker")

    with Scenario("E3", "every notReady prerequisite carries remedy, check time AND scope") as sc:
        rows: dict[str, dict[str, Any]] = {}
        for name in ("pf-missing-secret", "pf-missing-key", "pf-wrong-password",
                     "pf-blackhole"):
            obj = get_opt("preflight", name)
            if obj is None:
                continue
            for check_id, entry in checks_by_id(obj).items():
                if entry.get("state") != "notReady":
                    continue
                rows[f"{name}/{check_id}"] = {
                    "authority": entry.get("authority"),
                    "code": entry.get("code"),
                    "remedy": bool(entry.get("remedy")),
                    "observedAt": bool(entry.get("observedAt")),
                    "scope": entry.get("scope"),
                }
        sc.detail["notReadyRows"] = rows
        artifact("objects/s14/notready-rows.json", rows)
        missing = sorted(k for k, v in rows.items() if not v["scope"])
        sc.detail["rowsWithoutScope"] = missing
        sc.detail["authoritiesWithoutScope"] = sorted(
            {rows[k]["authority"] for k in missing})
        check(rows, "no notReady prerequisite was found to judge")
        check(not missing,
              "PLAT-03.1's acceptance asks for check time AND SCOPE on each failed "
              f"prerequisite; these carry no scope: {missing} "
              f"(authorities: {sc.detail['authoritiesWithoutScope']})")

    with Scenario("S14.redaction", "no fixture credential appears in any Preflight or log") as sc:
        listing = run(K + ["get", "preflights", "-o", "yaml"], timeout=120).stdout
        artifact("objects/s14/preflights-all.yaml", listing)
        log_text = controller_log("60m")
        artifact("logs/controller-during-s14.log", log_text)
        leaks = []
        for label, value in SECRETS.items():
            if not value:
                continue
            encoded = base64.b64encode(value.encode()).decode()
            for where, text in (("preflights", listing), ("controller-log", log_text)):
                if value in text or encoded in text:
                    leaks.append({"marker": label, "where": where})
        sc.detail["leaks"] = leaks
        sc.detail["markersChecked"] = len(SECRETS)
        check(not leaks, f"credential values leaked: {leaks}")

    record(
        "S14e", "a runner image that is not on the node (RunnerImageNotPresent)", "notRun",
        reason=(
            "D2 §14.4 S14(e) sets the CONTROLLER's LOGWEIR_RUNNER_IMAGE to a tag that "
            "does not exist. The controller is the shared lab Deployment in " + LAB_NS
            + "; that env var is release-wide, and two other live waves were running "
            "against it throughout this run. Changing it would have changed every "
            "Job those waves rendered. It is a cluster-lock operation with a "
            "documented restore (scripts/test-plat07-live.py's lab-swap/lab-restore) "
            "and is left for a wave that owns the cluster alone."
        ),
    )


# --------------------------------------------------------------------------
# S15 — a plan edit makes a green preflight stale, and a green preview cannot
#        bypass a collision at execution
# --------------------------------------------------------------------------


def restore_preflight_request(plan_bytes: str, *, target: str = "target",
                              source_destination: str = "dest-a",
                              evidence_destination: str = "dest-b",
                              restore_ref: str | None = None) -> dict[str, Any]:
    restore: dict[str, Any] = {
        "planBytes": plan_bytes,
        "planHash": digest(plan_bytes),
        "targetRef": {"name": target},
        "sourceDestinationRef": {"name": source_destination},
        "evidenceDestinationRef": {"name": evidence_destination},
    }
    if restore_ref:
        restore["restoreRef"] = {"name": restore_ref}
    return {"operation": "Restore", "restore": restore}


def s15() -> None:
    with Scenario("S15", "a plan edit makes a green restore preflight stale") as sc:
        facts = backup_facts("bk-a2", "a", "lw-a")
        backup_id = facts["backupId"]
        finished = facts["pointInTime"]
        sc.detail["sourceBackup"] = facts
        p1 = json.dumps(restore_plan(backup_id, finished, "d2w14-p1-"), indent=2) + "\n"
        p2 = json.dumps(restore_plan(backup_id, finished, "d2w14-p2-"), indent=2) + "\n"
        p1["source"]["topics"] if False else None
        sc.detail["planHashes"] = {"p1": digest(p1), "p2": digest(p2)}
        check(digest(p1) != digest(p2), "two different plans hashed the same")
        apply(preflight("pf-plan1", restore_preflight_request(p1), timeout_seconds=180))
        obj = wait_preflight("pf-plan1", timeout=600)
        artifact("objects/s15/preflight-pf-plan1.json", obj)
        status = obj["status"]
        result = status.get("result", {})
        checks = checks_by_id(obj)
        sc.detail["state"] = result.get("state")
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                               for k, v in checks.items()}
        sc.detail["binding"] = status.get("binding")
        check(status.get("binding", {}).get("planHash") == digest(p1),
              f"the Preflight bound plan hash {status.get('binding', {}).get('planHash')!r}, "
              f"not {digest(p1)}")
        approval = checks.get("approval.state")
        sc.detail["approvalCheck"] = approval
        check(result.get("state") in {"ready", "notReady", "unknown"},
              f"unexpected state {result.get('state')!r}")
        # The draft's binding is to P1's bytes; P2 is a different binding, and
        # the object itself is sealed so it cannot be edited into agreement.
        edit = run(K + ["patch", "preflight", "pf-plan1", "--type", "merge", "-p",
                        json.dumps({"spec": {"request": {"restore": {"planHash": digest(p2)}}}})],
                   check=False, timeout=60)
        sc.detail["planEditRefusal"] = (edit.stdout + edit.stderr).strip()[:400]
        check(edit.returncode != 0, "the bound plan hash was edited in place")
        check("immutable" in (edit.stdout + edit.stderr),
              f"the refusal did not name immutability: {sc.detail['planEditRefusal']}")
        apply(preflight("pf-plan2", restore_preflight_request(p2), timeout_seconds=180))
        second = wait_preflight("pf-plan2", timeout=600)
        artifact("objects/s15/preflight-pf-plan2.json", second)
        sc.detail["secondBinding"] = second["status"].get("binding")
        check(second["status"].get("binding", {}).get("planHash") == digest(p2),
              "the recomputed preflight did not bind the edited plan's hash")
        check(second["status"]["binding"]["planHash"] !=
              obj["status"]["binding"]["planHash"],
              "the two plans produced the same binding, so an edit would not be visible")
        sc.detail["inputsDigests"] = {
            "p1": obj["status"]["binding"].get("inputsDigest"),
            "p2": second["status"]["binding"].get("inputsDigest"),
        }
        check(sc.detail["inputsDigests"]["p1"] != sc.detail["inputsDigests"]["p2"],
              "the inputs digest did not move when the plan did")


def s15b() -> None:
    with Scenario("S15b", "a green preflight, a new collision, and exitCode 3 at execution") as sc:
        facts = backup_facts("bk-a2", "a", "lw-a")
        backup_id = facts["backupId"]
        finished = facts["pointInTime"]
        sc.detail["sourceBackup"] = facts
        run_index = attempt("s15bAttempt")
        restore_name = f"rs-race-{run_index}"
        approval_name = f"race-approval-{run_index}"
        prefix = f"d2w14-race-{run_index}-"
        sc.detail["restoreName"] = restore_name
        plan = restore_plan(backup_id, finished, prefix)
        plan["source"]["topics"] = ["payments"]
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        # 1. the preflight BEFORE the collision exists
        apply(preflight(f"pf-race-before-{run_index}",
                        restore_preflight_request(plan_bytes), timeout_seconds=180))
        before = wait_preflight(f"pf-race-before-{run_index}", timeout=600)
        artifact(f"objects/s15b/preflight-before-{run_index}.json", before)
        checks = checks_by_id(before)
        mapped = checks.get("target.mappedTopics")
        sc.detail["beforeState"] = before["status"].get("result", {}).get("state")
        sc.detail["beforeMappedTopics"] = mapped
        check(mapped is not None, f"target.mappedTopics did not run: {sorted(checks)}")
        check(mapped["state"] == "ready" and mapped["code"] == "MappedTopicsAbsent",
              f"before the collision target.mappedTopics is {mapped['state']}/{mapped['code']}")
        # 2. somebody creates the very topic the plan will map to
        collision = f"{prefix}payments"
        broker_exec("kafka-target", [
            f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
            "--create", "--topic", collision, "--partitions", "1",
            "--replication-factor", "1",
        ])
        sc.detail["collisionTopic"] = collision
        # 3. the Restore runs anyway, and the runner's guard refuses it
        minted = mint_approval(approval_name, restore_name, plan)
        target_id = get("kafkacluster", "target")["status"]["clusterId"]
        source_id = get("kafkacluster", "source-admin")["status"]["clusterId"]
        approval_bundle("logweir-approval-bundle", minted, [target_id], source_id)
        apply({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": owned(restore_name),
            "spec": {
                "sourceDestinationRef": {"name": "dest-a"},
                "evidenceDestinationRef": {"name": "dest-b"},
                "sourceArchive": {"url": "logweir-destination://dest-a"},
                "backupSetRef": backup_id,
                "pointInTime": finished,
                "planBytes": plan_bytes,
                "approvalRef": {"name": approval_name},
                "deadlineSeconds": 900,
                "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                           "topicNaming": {"prefix": prefix}},
            },
        })
        record_approval(minted)
        verified = wait_for("approval", approval_name,
                            lambda o: o.get("status", {}).get("verified") is not None,
                            timeout=180, what="a verification verdict")
        artifact("objects/s15b/approval-race.json", verified)
        check(verified["status"].get("verified") is True,
              "the approval did not verify: "
              + json.dumps(verified["status"].get("conditions", [])))
        obj = wait_for("restore", restore_name, terminal_phase, timeout=900,
                       what="a terminal phase")
        artifact(f"objects/s15b/restore-{restore_name}.json", obj)
        status = obj["status"]
        sc.detail["restore"] = {k: status.get(k) for k in
                                ("phase", "exitCode", "exitReason", "reason", "message")}
        logs = pod_logs_for_job(status["jobRef"]["name"]) if status.get("jobRef") else ""
        artifact(f"objects/s15b/{restore_name}-pod.log", logs)
        check(status.get("exitCode") == 3,
              f"exitCode is {status.get('exitCode')!r}, expected 3 (GuardRefused)")
        check(status.get("exitReason") == "GuardRefused",
              f"exitReason is {status.get('exitReason')!r}")
        check("already exists" in logs,
              "the runner log does not say the topic already exists on the cluster")
        # 4. and a re-run of the preflight now says so too
        apply(preflight(f"pf-race-after-{run_index}",
                        restore_preflight_request(plan_bytes), timeout_seconds=180))
        after = wait_preflight(f"pf-race-after-{run_index}", timeout=600)
        artifact(f"objects/s15b/preflight-after-{run_index}.json", after)
        mapped_after = checks_by_id(after).get("target.mappedTopics")
        sc.detail["afterMappedTopics"] = mapped_after
        check(mapped_after is not None, "target.mappedTopics did not run the second time")
        check(mapped_after["state"] == "notReady",
              f"after the collision target.mappedTopics is {mapped_after['state']!r}")
        check(mapped_after["code"] == "MappedTopicExists",
              f"code is {mapped_after['code']!r}, expected MappedTopicExists")
        check(collision in json.dumps(mapped_after),
              f"the check does not name the colliding topic: {mapped_after}")


# --------------------------------------------------------------------------
# S16 / S17 — a missing segment and a denied archive read
# --------------------------------------------------------------------------


def s16() -> None:
    with Scenario("S16", "a segment removed from the archive is named as missing") as sc:
        # dest-c, whose `storage.prefix` is empty, because E4 proved the restore
        # preflight cannot read a manifest under a NON-empty prefix at all;
        # running S16 against a prefixed destination would only re-measure that.
        facts = backup_facts_flat("bk-c", "a", "lw-c")
        backup_id = facts["backupId"]
        finished = facts["pointInTime"]
        sc.detail["sourceBackup"] = facts
        segment_keys = facts["segmentKeys"]
        sc.detail["segmentKeys"] = segment_keys
        check(segment_keys, "bk-c's manifest names no segments")
        removed = segment_keys[0]
        mc("rm", f"a/lw-c/{removed}", check=False)
        sc.detail["removedKey"] = removed
        sc.detail["removedLeaf"] = removed.split("/")[-1]
        plan = restore_plan(backup_id, finished, "d2w14-seg-")
        plan["source"]["topics"] = ["payments"]
        plan["source"]["storage"] = {
            "backend": "s3", "bucket": "lw-c", "prefix": "",
            "region": "us-east-1", "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        }
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        apply(preflight("pf-segment", restore_preflight_request(
            plan_bytes, source_destination="dest-c", evidence_destination="dest-c"),
            timeout_seconds=180))
        obj = wait_preflight("pf-segment", timeout=600)
        artifact("objects/s16/preflight-pf-segment.json", obj)
        checks = checks_by_id(obj)
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                               for k, v in checks.items()}
        entry = checks.get("archive.segments")
        check(entry is not None, f"archive.segments did not run: {sorted(checks)}")
        sc.detail["segmentsCheck"] = entry
        details_ref = obj["status"]["result"].get("detailsRef")
        details = ""
        if details_ref:
            cm = get_opt("configmap", details_ref["name"])
            if cm is not None:
                details = cm["data"].get("details.jsonl", "")
                artifact("objects/s16/details.jsonl", details)
        sc.detail["detailsRef"] = details_ref
        sc.detail["details"] = details
        leaf = removed.split("/")[-1]
        criteria = {
            "archive.segments is notReady": entry["state"] == "notReady",
            "code is SegmentMissing": entry["code"] == "SegmentMissing",
            "detail.count == 1":
                '"count":1' in json.dumps(entry).replace(" ", "").replace('\\"', '"'),
            "the sample names the removed key": leaf in json.dumps(entry),
            "the details document names the removed key": leaf in details,
        }
        sc.detail["criteria"] = criteria
        unmet = sorted(k for k, ok in criteria.items() if not ok)
        check(not unmet,
              "D2 §14.4 S16 criteria not met: " + "; ".join(unmet)
              + f". The removed object is `{removed}`; the check reported "
              f"{entry.get('message')!r} and the details document reads "
              f"{details.strip()!r}.")


def s17() -> None:
    with Scenario("S17", "a restore whose archive read is denied says so, not 'broken'") as sc:
        # dest-c-denied names the SAME prefix-less location as dest-c with a
        # principal that holds no policy at all, so the refusal is about the
        # grant and not about the key the check composes (E4).
        facts = backup_facts_flat("bk-c", "a", "lw-c")
        backup_id = facts["backupId"]
        finished = facts["pointInTime"]
        sc.detail["sourceBackup"] = facts
        plan = restore_plan(backup_id, finished, "d2w14-denied-")
        plan["source"]["topics"] = ["payments"]
        plan["source"]["storage"] = {
            "backend": "s3", "bucket": "lw-c", "prefix": "",
            "region": "us-east-1", "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        }
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        apply(preflight("pf-archive-denied", restore_preflight_request(
            plan_bytes, source_destination="dest-c-denied", evidence_destination="dest-c"),
            timeout_seconds=180))
        obj = wait_preflight("pf-archive-denied", timeout=600)
        artifact("objects/s17/preflight-pf-archive-denied.json", obj)
        checks = checks_by_id(obj)
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                               for k, v in checks.items()}
        entry = checks.get("archive.backupSet")
        check(entry is not None, f"archive.backupSet did not run: {sorted(checks)}")
        sc.detail["backupSetCheck"] = entry
        check(entry["state"] == "notReady", f"archive.backupSet is {entry['state']!r}")
        check(entry["code"] == "AccessDenied",
              f"archive.backupSet code is {entry['code']!r}, expected AccessDenied")
        for label, value in SECRETS.items():
            check(value not in json.dumps(obj), f"the Preflight carries the {label} credential")


# S18 CANNOT RUN AGAINST A CONTROLLER OLDER THAN THIS COMMIT.
#
# Before `fix(preflight): the approval row is the Approval's verdict, not the
# roster's`, `approval.state` recomputed the expiry from the CLUSTER-SCOPED
# `TrustRoster`, so a namespace-scoped `TrustPolicy` — the only fixture that
# does not edit shared state — could not reach the row at all. Running this row
# against an older image would record a FAIL that is about the build and not
# about the product, which is the class of mistake four previous waves spent
# their reports correcting.
PREFLIGHT_FIX_COMMIT = "c694fcd"


def controller_revision() -> str:
    """The commit the RUNNING controller was built from, off its image label."""
    pods = get_list("pods", namespace=LAB_NS,
                    selector="app.kubernetes.io/component=control-plane")
    if not pods:
        return ""
    image = ((pods[0].get("spec") or {}).get("containers") or [{}])[0].get("image", "")
    if not image:
        return ""
    out = run(["docker", "image", "inspect", image, "--format",
               '{{index .Config.Labels "org.opencontainers.image.revision"}}'],
              check=False, timeout=120)
    return out.stdout.strip()


def controller_carries(commit: str) -> bool:
    """Whether the running controller's build CONTAINS a commit.

    An ancestry question, not a string compare: the lab is refreshed to
    whatever main is at the time, and every build after the fix carries it.
    """
    revision = controller_revision()
    if not revision or revision == "<no value>":
        return False
    return run(["git", "merge-base", "--is-ancestor", commit, revision],
               check=False, timeout=60).returncode == 0


# The approver key's usage, and the one the CRD's G8 rule keeps separate from
# evidence signing: "a key that both attests and authorises is a key whose
# holder can approve their own work".
APPROVER_USAGE = "GovernedApproval"


def policy_key(key_id: str, spki_pem: str, *, usages: list[str], state: str = "Active",
               not_before: str = "2026-01-01T00:00:00Z",
               not_after: str = "2027-01-01T00:00:00Z",
               subject: str = "approver@scram-local.invalid",
               display: str = "the lab approver key", **over: Any) -> dict[str, Any]:
    """One `TrustPolicy.spec.keys[]` entry, with its own window and usage.

    PER-KEY WINDOWS ARE THE POINT. This row needs a key that is valid when the
    Approval is signed and expired when the Preflight reads it, and `notAfter`
    is per key — G2 allows it to be brought FORWARD, which is exactly the move
    an expiry is.
    """
    entry = {
        "keyId": key_id,
        "spkiPem": spki_pem,
        "algorithm": "p256",
        "principal": {"id": subject, "display": display},
        "usages": usages,
        "state": state,
        "notBefore": not_before,
        "notAfter": not_after,
    }
    entry.update(over)
    return entry


def trust_policy(name: str, namespaces: list[str], keys: list[dict[str, Any]]) -> dict[str, Any]:
    """A cluster-scoped `TrustPolicy` governing named namespaces only.

    CLUSTER-SCOPED, SO IT RUNS UNDER THE LOCK — but it touches nothing shared:
    `spec.namespaces` is an exact list and this one names only this run's
    namespace, so the shared release's objects keep resolving through
    `TrustRoster/default` exactly as before.
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicy",
        "metadata": {"name": name, "labels": {OWNER_LABEL_KEY: OWNER}},
        "spec": {"namespaces": namespaces, "keys": keys},
    }


def roster_approver_key() -> dict[str, Any]:
    roster = json.loads(run(CTX + ["get", "trustroster", "default", "-o", "json"]).stdout)
    return roster["spec"]["approverKeys"][0]


def approval_facts(approval: dict[str, Any]) -> dict[str, Any]:
    """The Approval's verdict, and the reason from where it actually lives.

    `status.verified` is a bool and the REASON is on the `Verified` condition,
    not beside it — reading `status.reason` returns `None` however expired the
    key is, which is how the first live run of this row lost its second clause.
    """
    status = approval.get("status") or {}
    condition = next((c for c in status.get("conditions", [])
                      if c.get("type") == "Verified"), {})
    return {
        "verified": status.get("verified"),
        "reason": condition.get("reason"),
        "message": (condition.get("message") or "")[:200],
        "matchedKeyId": status.get("matchedKeyId"),
        # APPROVAL-KEY-WINDOW-UNPUBLISHED. `None` for a controller image that
        # predates the field and for an approval that matched no key; S22/S23
        # are the rows that make the difference matter.
        "approverKeyWindow": status.get("approverKeyWindow"),
    }


def approval_expiry_is_relayed(green: dict[str, Any], expired: dict[str, Any],
                               approval_after: dict[str, Any],
                               restore_jobs: list[str],
                               admitted: dict[str, Any]) -> dict[str, bool]:
    """D2 §14.4 S18, as the fixed preflight answers it.

    The chain is three objects long and each link has to hold: a
    namespace-scoped `TrustPolicy` expires the approver key, the **Approval
    controller** re-derives `Verified=False` with `KeyIdExpired`, and the
    Preflight's `approval.state` RELAYS that as `notReady/ApprovalExpired`
    rather than recomputing it from the roster — which is what
    `fix-preflight-approval` changed and what makes this row possible at all.

    The first clause is the one that makes the rest mean something: the same
    preview was GREEN before the key expired. Without it, a row could pass on a
    preflight that was never able to approve anything.
    """
    return {
        "the preview was green before the key expired": (
            green.get("state") == "ready" and green.get("code") == "ApprovalVerified"
        ),
        "the Approval's own verdict went to Verified=False/KeyIdExpired": (
            approval_after.get("verified") is False
            and approval_after.get("reason") == "KeyIdExpired"
        ),
        "the preflight relays it as notReady/ApprovalExpired": (
            expired.get("state") == "notReady" and expired.get("code") == "ApprovalExpired"
        ),
        "and says which key, in words": bool((expired.get("message") or "").strip()),
        "the check is blocking, so the aggregate cannot be ready":
            expired.get("gating") == "blocking",
        "no restore Job was created for the previously green plan": not restore_jobs,
        "and the Restore is not admitted": admitted.get("status") == "False",
    }


def s18() -> None:
    """PLAT-03.2's `expired approval`, as the tracker means it.

    THE FIXTURE IS A NAMESPACE-SCOPED `TrustPolicy`, NOT A ROSTER EDIT.
    `TrustRoster/default` is cluster-scoped, shared, and its spec is immutable
    (`spec is immutable; create a new object instead`), and its one approver key
    has no `notAfter` at all — which is why this row was `notRun` for four
    waves. A `TrustPolicy` naming ONLY this namespace expires the key here and
    nowhere else, and since `fix-preflight-approval` the preflight's
    `approval.state` consumes the Approval's own verdict, so the expiry reaches
    the row.

    The window is brought FORWARD rather than set in the past from the start:
    the Approval has to be Verified while the key is valid, or the row would be
    about an approval that never worked.
    """
    with Scenario("S18", "an expired approver key makes an approval expired") as sc:
        controller = controller_revision()
        sc.detail["controllerRevision"] = controller
        sc.detail["carriesPreflightFix"] = controller_carries(PREFLIGHT_FIX_COMMIT)
        if not sc.detail["carriesPreflightFix"]:
            record("S18", "an expired approver key makes an approval expired", "notRun",
                   reason=("the running controller does not carry the preflight fix "
                           f"({controller}); before it, `approval.state` recomputed the "
                           "expiry from the cluster-scoped roster and a namespace-scoped "
                           "TrustPolicy could not reach this row"),
                   detail=sc.detail)
            return
        approver = roster_approver_key()
        policy_name = f"{OWNER}-{STAMP}-approver"
        sc.detail["approverKeyId"] = approver["keyId"]
        sc.detail["trustPolicy"] = policy_name
        facts = backup_facts("bk-a2", "a", "lw-a")
        plan = restore_plan(facts["backupId"], facts["pointInTime"], "d2w14-expired-",
                        topics=facts["manifestTopics"])
        restore_name = "rs-expired"
        approval_name = "ap-expired"
        try:
            # RE-RUNNABLE. A `Restore`'s spec is immutable, so a leftover from an
            # earlier attempt cannot be applied over; and an Approval bound to a
            # deleted subject is a different refusal from the one under test.
            for kind, name in (("restore", restore_name), ("approval", approval_name),
                               ("preflight", "pf-expired-green"),
                               ("preflight", "pf-expired")):
                run(K + ["delete", kind, name, "--ignore-not-found=true", "--wait=true"],
                    check=False, timeout=120)
            # 1. the key is valid for the next ten minutes, HERE only
            valid_until = (dt.datetime.now(dt.timezone.utc)
                           + dt.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            apply(trust_policy(policy_name, [NS], [policy_key(
                approver["keyId"], approver["spkiPem"], usages=[APPROVER_USAGE],
                not_after=valid_until)]))
            sc.detail["validUntil"] = valid_until

            # 2. an Approval signed and Verified while it is valid
            minted = mint_approval(approval_name, restore_name, plan)
            apply({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                "metadata": owned(restore_name),
                "spec": {
                    "sourceDestinationRef": {"name": "dest-a"},
                    "evidenceDestinationRef": {"name": "dest-b"},
                    "sourceArchive": {"url": "logweir-destination://dest-a"},
                    "backupSetRef": facts["backupId"],
                    "pointInTime": facts["pointInTime"],
                    "planBytes": minted["planBytes"],
                    "approvalRef": {"name": approval_name},
                    "deadlineSeconds": 900,
                    "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                               "topicNaming": {"prefix": "d2w14-expired-"}},
                },
            })
            record_approval(minted)
            verified = wait_for(
                "approval", approval_name,
                lambda o: (o.get("status") or {}).get("verified") is True,
                timeout=300, what="the Approval to verify while the key is valid")
            sc.detail["approvalWhileValid"] = approval_facts(verified)
            artifact("objects/s18/approval-verified.json", verified)

            # 3. the green preview
            # `restoreRef` ALONE. The CRD's own rule is "set exactly one of
            # planBytes (a draft) or restoreRef (an existing Restore)", and a
            # DRAFT preflight answers `approval.state`
            # `skipped/SubjectNotCreated` — "about a draft plan, which no
            # approver has been asked to sign yet" — which is a true statement
            # about a draft and no statement at all about an expired approval.
            # The Restore carries its own plan, target and destinations.
            apply(preflight("pf-expired-green",
                            {"operation": "Restore",
                             "restore": {"restoreRef": {"name": restore_name},
                                         "sourceDestinationRef": {"name": "dest-a"},
                                         "evidenceDestinationRef": {"name": "dest-b"},
                                         "targetRef": {"name": "target"}}},
                            timeout_seconds=180))
            green_pf = wait_preflight("pf-expired-green", timeout=600)
            artifact("objects/s18/preflight-green.json", green_pf)
            green = checks_by_id(green_pf).get("approval.state") or {}
            sc.detail["greenApprovalCheck"] = green

            # 4. the key expires — BROUGHT FORWARD, which G2 allows
            expired_at = (dt.datetime.now(dt.timezone.utc)
                          - dt.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            apply(trust_policy(policy_name, [NS], [policy_key(
                approver["keyId"], approver["spkiPem"], usages=[APPROVER_USAGE],
                not_after=expired_at)]))
            sc.detail["expiredAt"] = expired_at
            after = wait_for(
                "approval", approval_name,
                lambda o: (o.get("status") or {}).get("verified") is False,
                timeout=300, what="the Approval to lose its verdict when the key expires")
            approval_after = approval_facts(after)
            sc.detail["approvalAfterExpiry"] = approval_after
            artifact("objects/s18/approval-expired.json", after)

            # 5. the preview a previously green plan gets now
            apply(preflight("pf-expired",
                            {"operation": "Restore",
                             "restore": {"restoreRef": {"name": restore_name},
                                         "sourceDestinationRef": {"name": "dest-a"},
                                         "evidenceDestinationRef": {"name": "dest-b"},
                                         "targetRef": {"name": "target"}}},
                            timeout_seconds=180))
            expired_pf = wait_preflight("pf-expired", timeout=600)
            artifact("objects/s18/preflight-expired.json", expired_pf)
            expired = checks_by_id(expired_pf).get("approval.state") or {}
            sc.detail["expiredApprovalCheck"] = expired
            sc.detail["aggregate"] = (expired_pf["status"].get("result") or {}).get("state")

            # 6. and the Restore cannot start
            restore = get("restore", restore_name)
            jobs = [j["metadata"]["name"] for j in get_list("jobs")
                    if any(o.get("uid") == restore["metadata"]["uid"]
                           for o in (j["metadata"].get("ownerReferences") or []))]
            admitted = next((c for c in (restore.get("status") or {}).get("conditions", [])
                             if c.get("type") == "Admitted"), {})
            sc.detail["restore"] = {"phase": (restore.get("status") or {}).get("phase"),
                                    "admitted": admitted, "jobs": jobs,
                                    "jobRef": (restore.get("status") or {}).get("jobRef")}
            artifact("objects/s18/restore.json", restore)

            clauses = approval_expiry_is_relayed(green, expired, approval_after, jobs, admitted)
            sc.detail["criteria"] = clauses
            unmet = sorted(k for k, ok in clauses.items() if not ok)
            check(not unmet,
                  "D2 §14.4 S18 criteria not met: " + "; ".join(unmet)
                  + f". The preview was {green.get('state')}/{green.get('code')} while the "
                  f"key was valid to {valid_until}; after the key expired at {expired_at} "
                  f"the Approval reads verified={approval_after.get('verified')}/"
                  f"{approval_after.get('reason')} and the preflight's approval.state is "
                  f"{expired.get('state')}/{expired.get('code')} "
                  f"({(expired.get('message') or '')[:120]}), aggregate "
                  f"{sc.detail['aggregate']}; the Restore is Admitted="
                  f"{admitted.get('status')}/{admitted.get('reason')} with {len(jobs)} Job(s)")
        finally:
            run(CTX + ["delete", "trustpolicy", policy_name, "--ignore-not-found=true",
                       "--wait=true"], check=False, timeout=120)
            left = [t["metadata"]["name"] for t in json.loads(
                run(CTX + ["get", "trustpolicies", "-o", "json"]).stdout)["items"]
                if t["metadata"]["name"].startswith(f"{OWNER}-{STAMP}")]
            sc.detail["trustPoliciesLeft"] = left
            check(not left,
                  f"the cluster-scoped TrustPolicy this row created is deleted before it "
                  f"returns; remaining: {left}. It governs a namespace by exact name, so a "
                  f"leftover would silently re-judge a later run's evidence")


# --------------------------------------------------------------------------
# S22 / S23 — APPROVAL-KEY-WINDOW-UNPUBLISHED
# --------------------------------------------------------------------------

# The re-check ceiling D2 §6.3 gives both `approval.*` rows, in seconds. The
# published `expiresAt` is `min(this, the key's notAfter)`.
APPROVAL_RECHECK_CEILING_SECONDS = 10 * 60


def crd_publishes_status_field(plural: str, field: str) -> bool:
    """Whether the INSTALLED CRD's `status` carries `field`.

    THE BUILD QUESTION, ASKED OF THE CLUSTER. S18 gates itself on a commit
    ancestry because its fix left no schema trace; this one does, and a schema
    probe survives a rebase that renumbers the commit. It answers "could this
    controller publish the window at all", which is a fact about the image —
    and never "did it", which is what S22 asserts.
    """
    out = run(CTX + ["get", "crd", plural, "-o", "json"], check=False, timeout=120)
    if out.returncode != 0:
        return False
    versions = (json.loads(out.stdout).get("spec") or {}).get("versions") or []
    for version in versions:
        properties = (((version.get("schema") or {}).get("openAPIV3Schema") or {})
                      .get("properties") or {}).get("status") or {}
        if field in (properties.get("properties") or {}):
            return True
    return False


def rfc3339(value: str) -> dt.datetime:
    """An RFC 3339 instant the API server wrote, as an aware datetime."""
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def approver_key_window_is_warned(approval: dict[str, Any], validity: dict[str, Any],
                                  state: dict[str, Any], deadline_seconds: int,
                                  aggregate: str) -> dict[str, bool]:
    """D2 §6.3's `ApproverKeyExpiresBeforeDeadline`, as the fixed product answers it.

    The chain: a namespace-scoped `TrustPolicy` narrows the approver key's
    `notAfter` to inside the restore's own `deadlineSeconds`; the **Approval
    controller** republishes `status.approverKeyWindow` for the key that
    verified it; the Preflight's `approval.keyValidity` row reads that window
    back and warns.

    THE LAST TWO CLAUSES ARE WHAT MAKE IT A WARNING AND NOT A REFUSAL. D2 §6.3
    gates this row `A` and §6.4 says "Advisory `notReady` appears as warnings".
    An approval that is valid RIGHT NOW is not refused because its key closes
    before a deadline the restore may never reach — so `approval.state` is
    still `ready` and the aggregate is not `notReady`.
    """
    window = approval.get("approverKeyWindow") or {}
    not_after = window.get("notAfter")
    return {
        "the Approval publishes a window for the key that verified it": (
            bool(not_after)
            and bool(approval.get("matchedKeyId"))
            and window.get("keyId") == approval.get("matchedKeyId")
        ),
        "the window closes inside the restore's own deadline": bool(not_after) and (
            rfc3339(not_after)
            < dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=deadline_seconds)
        ),
        "the preflight warns with ApproverKeyExpiresBeforeDeadline": (
            validity.get("state") == "notReady"
            and validity.get("code") == "ApproverKeyExpiresBeforeDeadline"
        ),
        "and names the key and both instants, in words": (
            bool((validity.get("message") or "").strip())
            and str(window.get("keyId") or "\x00") in (validity.get("message") or "")
        ),
        "the warning is ADVISORY, so it cannot refuse": validity.get("gating") == "advisory",
        "the blocking approval row is still ready": (
            state.get("state") == "ready" and state.get("code") == "ApprovalVerified"
        ),
        "so the verdict is not notReady": aggregate != "notReady",
    }


def approval_rows_recheck_is_capped(rows: dict[str, Any], not_after: str,
                                    ceiling_seconds: int = APPROVAL_RECHECK_CEILING_SECONDS
                                    ) -> dict[str, bool]:
    """D2 §6.3's `min(10 m, notAfter)` on BOTH `approval.*` rows.

    A verdict about a key must not outlive the key. The ceiling clause alone
    would pass against a product that does nothing — every row is already
    capped at ten minutes — so the row is run with a `notAfter` INSIDE the
    ceiling and the last clause requires the cap to have actually bitten.
    """
    ids = ("approval.state", "approval.keyValidity")
    present = [rows.get(i) or {} for i in ids]
    expiries: list[dt.datetime] = []
    observed: list[dt.datetime] = []
    for row in present:
        if row.get("expiresAt") and row.get("observedAt"):
            expiries.append(rfc3339(row["expiresAt"]))
            observed.append(rfc3339(row["observedAt"]))
    closes = rfc3339(not_after) if not_after else None
    return {
        "both approval rows ran and both publish an expiresAt": len(expiries) == len(ids),
        "neither is re-checked after the key's notAfter": bool(closes) and len(expiries) == len(ids)
        and all(e <= closes for e in expiries),
        "and neither outlives the ten-minute ceiling": len(expiries) == len(ids) and all(
            e <= o + dt.timedelta(seconds=ceiling_seconds)
            for e, o in zip(expiries, observed)
        ),
        "the cap BIT — the key closes sooner than the ceiling": bool(closes)
        and len(expiries) == len(ids)
        and all(
            closes < o + dt.timedelta(seconds=ceiling_seconds) and e < o + dt.timedelta(
                seconds=ceiling_seconds)
            for e, o in zip(expiries, observed)
        ),
    }


S22_ID = "S22"
S22_TITLE = "an approver key closing before the restore's deadline is warned about ahead of time"
S23_ID = "S23"
S23_TITLE = "both approval rows re-check inside min(10 m, the approver key's notAfter)"


def s22() -> None:
    """Two rows for APPROVAL-KEY-WINDOW-UNPUBLISHED, off ONE fixture.

    S18 proves the WITHDRAWN verdict reaches the blocking row. These two prove
    the other half: the approver key's WINDOW reaches the advisory row and the
    re-check cap, which between 2026-09-19 and 2026-09-21 nothing published
    anywhere.

    THE FIXTURE IS THE SAME NAMESPACE-SCOPED `TrustPolicy` S18 uses, for the
    same reason: `TrustRoster/default` is cluster-scoped, shared and immutable,
    and its one approver key has no `notAfter` at all. The policy names ONLY
    this run's namespace, and it is deleted in a `finally` — it is
    cluster-scoped, so a leftover would silently re-judge a later run.

    THE WINDOW IS NARROWED AFTER THE APPROVAL VERIFIED, not before: the
    Approval has to be `Verified=True` under a wide window first, or S22 would
    be a row about an approval that never worked, and the narrowing is itself
    the live proof that the published window is RE-DERIVED on a policy edit
    rather than stamped once.
    """
    deadline_seconds = 3600
    # NINE MINUTES, NOT TEN. The cap is `min(10 m, notAfter)`; a window exactly
    # at the ceiling would leave both bounds equal and the row could not tell
    # a capped `expiresAt` from an uncapped one. Nine leaves the whole window
    # for the preflight to finish in, and the preflight's `observedAt` is later
    # than this instant, so `notAfter - observedAt < 9 m` however fast it runs.
    window_minutes = 9
    controller = controller_revision()
    publishes = crd_publishes_status_field("approvals.logweir.dev", "approverKeyWindow")
    if not publishes:
        for sid, title in ((S22_ID, S22_TITLE), (S23_ID, S23_TITLE)):
            record(sid, title, "notRun",
                   reason=("the installed Approval CRD has no status.approverKeyWindow "
                           f"(controller {controller or 'unknown'}); before "
                           "APPROVAL-KEY-WINDOW-UNPUBLISHED's fix no window was published "
                           "anywhere, so neither row can reach the behaviour it is about"),
                   detail={"controllerRevision": controller})
        return

    approver = roster_approver_key()
    policy_name = f"{OWNER}-{STAMP}-window"
    facts = backup_facts("bk-a2", "a", "lw-a")
    plan = restore_plan(facts["backupId"], facts["pointInTime"], "d2w14-window-",
                        topics=facts["manifestTopics"])
    restore_name = "rs-window"
    approval_name = "ap-window"
    detail: dict[str, Any] = {"controllerRevision": controller, "trustPolicy": policy_name,
                              "approverKeyId": approver["keyId"],
                              "deadlineSeconds": deadline_seconds}
    try:
        for kind, name in (("restore", restore_name), ("approval", approval_name),
                           ("preflight", "pf-window")):
            run(K + ["delete", kind, name, "--ignore-not-found=true", "--wait=true"],
                check=False, timeout=120)

        # 1. a WIDE window, here only
        wide = (dt.datetime.now(dt.timezone.utc)
                + dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        apply(trust_policy(policy_name, [NS], [policy_key(
            approver["keyId"], approver["spkiPem"], usages=[APPROVER_USAGE],
            not_after=wide)]))
        detail["wideNotAfter"] = wide

        # 2. an Approval that verifies under it
        minted = mint_approval(approval_name, restore_name, plan)
        apply({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": owned(restore_name),
            "spec": {
                "sourceDestinationRef": {"name": "dest-a"},
                "evidenceDestinationRef": {"name": "dest-b"},
                "sourceArchive": {"url": "logweir-destination://dest-a"},
                "backupSetRef": facts["backupId"],
                "pointInTime": facts["pointInTime"],
                "planBytes": minted["planBytes"],
                "approvalRef": {"name": approval_name},
                "deadlineSeconds": deadline_seconds,
                "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                           "topicNaming": {"prefix": "d2w14-window-"}},
            },
        })
        record_approval(minted)
        verified = wait_for(
            "approval", approval_name,
            lambda o: (o.get("status") or {}).get("verified") is True,
            timeout=300, what="the Approval to verify under the wide window")
        detail["approvalUnderWideWindow"] = approval_facts(verified)
        artifact("objects/s22/approval-wide.json", verified)

        # 3. the window is NARROWED, and the published window follows
        closes = (dt.datetime.now(dt.timezone.utc)
                  + dt.timedelta(minutes=window_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
        apply(trust_policy(policy_name, [NS], [policy_key(
            approver["keyId"], approver["spkiPem"], usages=[APPROVER_USAGE],
            not_after=closes)]))
        detail["narrowedNotAfter"] = closes
        # AND THE APPROVAL IS NUDGED, so the re-derivation does not have to wait
        # out a heartbeat inside the nine minutes this row is measuring.
        #
        # The Approval controller now WATCHES `TrustPolicy`, so the edit above
        # should already have enqueued this object within seconds. The nudge is
        # deliberately kept anyway and is deliberately not a `spec` edit: an
        # `Approval`'s spec is sealed by CEL (`self == oldSelf`) and its
        # METADATA is not, so an annotation is a write the watch sees and the
        # verdict cannot depend on. It makes this row measure the WINDOW rather
        # than the wake-up path — and it still holds if the row is ever run
        # against an image built before the watch, where a verified approval is
        # requeued only every `min(300 s, notAfter - now)`.
        run(K + ["annotate", "approval", approval_name,
                 "logweir.dev/test-nudge="
                 + str(int(dt.datetime.now(dt.timezone.utc).timestamp())),
                 "--overwrite"], check=False, timeout=120)
        narrowed = wait_for(
            "approval", approval_name,
            lambda o: (((o.get("status") or {}).get("approverKeyWindow") or {})
                       .get("notAfter") or "").startswith(closes[:16]),
            # 360 s, NOT 300. The heartbeat IS 300 s, so a 300 s budget is
            # exactly the worst case and can time out on a correct controller.
            timeout=360,
            what="the published approverKeyWindow to follow the TrustPolicy edit")
        detail["secondsLeftInWindow"] = round(
            (rfc3339(closes) - dt.datetime.now(dt.timezone.utc)).total_seconds())
        approval_now = approval_facts(narrowed)
        detail["approvalAfterNarrowing"] = approval_now
        artifact("objects/s22/approval-narrowed.json", narrowed)

        # 3b. THE ROW'S OWN RESTORE HAS ALREADY RUN. Its Approval verified at
        # step 2, so the controller admitted it and its runner created the
        # mapped topics on this namespace's target broker within seconds. A
        # preflight taken now answers `target.topicCreate` notReady /
        # `MappedTopicExists` about THOSE topics — correctly, and blocking — so
        # the aggregate this row reads would be a verdict about the row's own
        # leftovers (measured on lab-refresh-8). The Restore is waited to a
        # terminal phase and the topics IT created, in this run's own broker,
        # are removed before the preflight; both are recorded.
        ran = wait_for(
            "restore", restore_name,
            lambda o: (o.get("status") or {}).get("phase") in {"Succeeded", "Failed"},
            timeout=900, what="the row's own Restore to finish before the preflight")
        restored_topics = [f"d2w14-window-{t}" for t in facts["manifestTopics"]]
        for topic in restored_topics:
            broker_exec("kafka-target", [
                f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                "--delete", "--if-exists", "--topic", topic,
            ], check=False)
        left: list[str] = restored_topics
        for _ in range(30):
            listed = set(broker_exec("kafka-target", [
                f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                "--list", "--exclude-internal",
            ], check=False).stdout.split())
            left = [t for t in restored_topics if t in listed]
            if not left:
                break
            time.sleep(2)
        if left:
            raise RuntimeError(f"the row's own restored topics are still present: {left}")
        detail["ownRestoreBeforePreflight"] = {
            "phase": (ran.get("status") or {}).get("phase"),
            "uid": ran["metadata"]["uid"],
            "restoredTopicsRemoved": restored_topics,
        }

        # 4. one preflight over the existing Restore, read by both rows
        apply(preflight("pf-window",
                        {"operation": "Restore",
                         "restore": {"restoreRef": {"name": restore_name},
                                     "sourceDestinationRef": {"name": "dest-a"},
                                     "evidenceDestinationRef": {"name": "dest-b"},
                                     "targetRef": {"name": "target"}}},
                        timeout_seconds=180))
        obj = wait_preflight("pf-window", timeout=420)
        artifact("objects/s22/preflight-window.json", obj)
        rows = checks_by_id(obj)
        aggregate = (obj["status"].get("result") or {}).get("state")
        detail["aggregate"] = aggregate
        detail["approvalRows"] = {k: rows.get(k) for k in
                                  ("approval.state", "approval.keyValidity")}

        published = ((narrowed.get("status") or {}).get("approverKeyWindow") or {})
        with Scenario(S22_ID, S22_TITLE) as sc:
            sc.detail.update(detail)
            clauses = approver_key_window_is_warned(
                approval_now, rows.get("approval.keyValidity") or {},
                rows.get("approval.state") or {}, deadline_seconds, aggregate)
            sc.detail["criteria"] = clauses
            unmet = sorted(k for k, ok in clauses.items() if not ok)
            check(not unmet,
                  "D2 §6.3 approval.keyValidity criteria not met: " + "; ".join(unmet)
                  + f". The key closes at {published.get('notAfter')} and the restore's "
                  f"deadline is {deadline_seconds}s out; the row reads "
                  f"{(rows.get('approval.keyValidity') or {}).get('state')}/"
                  f"{(rows.get('approval.keyValidity') or {}).get('code')} "
                  f"({((rows.get('approval.keyValidity') or {}).get('message') or '')[:160]}), "
                  f"approval.state is {(rows.get('approval.state') or {}).get('state')}/"
                  f"{(rows.get('approval.state') or {}).get('code')}, aggregate {aggregate}")

        with Scenario(S23_ID, S23_TITLE) as sc:
            sc.detail.update(detail)
            clauses = approval_rows_recheck_is_capped(rows, published.get("notAfter") or "")
            sc.detail["criteria"] = clauses
            unmet = sorted(k for k, ok in clauses.items() if not ok)
            check(not unmet,
                  "D2 §6.3 min(10 m, notAfter) criteria not met: " + "; ".join(unmet)
                  + f". The key closes at {published.get('notAfter')}; the rows publish "
                  + json.dumps({k: {"observedAt": (rows.get(k) or {}).get("observedAt"),
                                    "expiresAt": (rows.get(k) or {}).get("expiresAt")}
                                for k in ("approval.state", "approval.keyValidity")},
                               sort_keys=True))
    finally:
        run(CTX + ["delete", "trustpolicy", policy_name, "--ignore-not-found=true",
                   "--wait=true"], check=False, timeout=120)
        left = [t["metadata"]["name"] for t in json.loads(
            run(CTX + ["get", "trustpolicies", "-o", "json"]).stdout)["items"]
            if t["metadata"]["name"] == policy_name]
        if left:
            record(S23_ID, S23_TITLE, "fail",
                   reason=f"the cluster-scoped TrustPolicy {policy_name} outlived this row; "
                          "it governs a namespace by exact name and would re-judge a later run")


# --- S24 / S25: the namespace's TrustPolicy is what readiness reads ------------
#
# rehearsal-catalog-trust's "extra rows for the next lab refresh" (32306a5,
# 5dda65b): once a TrustPolicy governs the namespace, a Backup check's
# `signer.rostered` is judged against THAT policy's keys and says so in its
# scope, and a scratch restore check's `target.clusterIdentity` reads the
# policy's `spec.allowedTargetClusterIds` — never TrustRoster's list. S25 is a
# DIFFERENTIAL on one policy: the same plan is refused `TargetNotAllowlisted`
# while the allowlist names another cluster, and allowed once it names the
# target, so a build that read the roster (or no list) fails one of the arms.

S24_ID, S24_TITLE = "S24", "signer.rostered is judged by the namespace's TrustPolicy and says so"
S25_ID, S25_TITLE = ("S25", "a scratch restore's target allowlist is the governing TrustPolicy's "
                            "allowedTargetClusterIds")


def signer_rostered_by_policy(row: dict[str, Any], policy: str) -> dict[str, bool]:
    scope = row.get("scope") or {}
    return {
        "signer.rostered is ready/SignerRostered": row.get("state") == "ready"
        and row.get("code") == "SignerRostered",
        f"and its scope is TrustPolicy/{policy}, not the roster":
            scope.get("kind") == "TrustPolicy" and scope.get("name") == policy,
    }


def allowlist_is_the_policys(denied: dict[str, Any], allowed: dict[str, Any],
                             target_id: str) -> dict[str, bool]:
    return {
        "with the policy's allowlist naming another cluster: notReady/TargetNotAllowlisted":
            denied.get("state") == "notReady" and denied.get("code") == "TargetNotAllowlisted",
        "about the observed target cluster id":
            bool(target_id) and target_id in str(denied.get("message") or ""),
        "and naming the governing TrustPolicy's list as the source":
            "allowedTargetClusterIds" in str(denied.get("message") or ""),
        "with the allowlist naming the target: ready/TargetAllowed": allowed.get("state") == "ready"
        and allowed.get("code") == "TargetAllowed",
    }


def s24() -> None:
    """S24 and S25 (CLUSTER LOCK: a TrustPolicy). Needs `setup` and `s6`
    (`bk-a2`)."""
    roster = json.loads(run(CTX + ["get", "trustroster", "default", "-o", "json"]).stdout)
    signer = roster["spec"]["signingKeys"][0]
    target_id = ((get("kafkacluster", "target").get("status") or {}).get("clusterId") or "")
    policy_name = f"{OWNER}-{STAMP}-scope"
    facts = backup_facts("bk-a2", "a", "lw-a")
    doc = restore_plan(facts["backupId"], facts["pointInTime"], "d2w14-scope-",
                       topics=facts["manifestTopics"])
    doc["target"]["mode"] = "scratch"
    plan = json.dumps(doc, indent=2) + "\n"

    def body(allowed: list[str]) -> dict[str, Any]:
        obj = trust_policy(policy_name, [NS], [policy_key(
            signer["keyId"], signer["spkiPem"], usages=["EvidenceSigning"],
            subject=signer.get("subject") or "signing@scram-local.invalid",
            display="the lab signing key")])
        obj["spec"]["allowedTargetClusterIds"] = allowed
        return obj

    try:
        for name in ("pf-scope-backup", "pf-scope-deny", "pf-scope-allow"):
            run(K + ["delete", "preflight", name, "--ignore-not-found=true", "--wait=true"],
                check=False, timeout=120)
        apply(body(["lr9-not-this-cluster"]))
        time.sleep(5)
        apply(backup_preflight("pf-scope-backup", "source-admin", topics=["payments"],
                               timeout_seconds=120))
        backup_obj = wait_preflight("pf-scope-backup", timeout=420)
        artifact("objects/s24/preflight-backup.json", backup_obj)
        apply(preflight("pf-scope-deny", restore_preflight_request(plan), timeout_seconds=180))
        deny_obj = wait_preflight("pf-scope-deny", timeout=600)
        artifact("objects/s24/preflight-deny.json", deny_obj)
        apply(body([target_id]))
        time.sleep(5)
        apply(preflight("pf-scope-allow", restore_preflight_request(plan), timeout_seconds=180))
        allow_obj = wait_preflight("pf-scope-allow", timeout=600)
        artifact("objects/s24/preflight-allow.json", allow_obj)
        live = json.loads(run(CTX + ["get", "trustpolicy", policy_name, "-o", "json"]).stdout)
        with Scenario(S24_ID, S24_TITLE) as sc:
            row = checks_by_id(backup_obj).get("signer.rostered") or {}
            clauses = signer_rostered_by_policy(row, policy_name)
            sc.detail.update({"policy": policy_name, "policyUid": live["metadata"]["uid"],
                              "signerKeyId": signer["keyId"], "row": row,
                              "clauses": clauses})
            check(all(clauses.values()), f"signer.rostered under {policy_name}: {row}")
        with Scenario(S25_ID, S25_TITLE) as sc:
            denied = checks_by_id(deny_obj).get("target.clusterIdentity") or {}
            allowed = checks_by_id(allow_obj).get("target.clusterIdentity") or {}
            clauses = allowlist_is_the_policys(denied, allowed, target_id)
            sc.detail.update({"policy": policy_name, "targetClusterId": target_id,
                              "denied": denied, "allowed": allowed, "clauses": clauses})
            check(all(clauses.values()), f"target.clusterIdentity: denied={denied} "
                  f"allowed={allowed}")
    finally:
        seen = run(CTX + ["get", "trustpolicy", policy_name, "-o", "json"], check=False)
        if seen.returncode == 0:
            owner = (json.loads(seen.stdout)["metadata"].get("labels") or {}).get(OWNER_LABEL_KEY)
            if owner == OWNER:
                run(CTX + ["delete", "trustpolicy", policy_name, "--wait=true"], check=False,
                    timeout=120)


def s19() -> None:
    with Scenario("S19", "a stale inventory does not stand in for a live topic check") as sc:
        full = get("topicdiscovery", "td-full")
        names = {r["name"] for r in topics_from_chunks(full)}
        check("audit" in names, "the recorded inventory does not contain `audit`")
        sc.detail["inventoryObservedAt"] = full["status"].get("observedAt")
        broker_exec("kafka-acl", [
            f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
            "--delete", "--topic", "audit",
        ])
        sc.detail["deletedTopic"] = "audit"
        time.sleep(10)
        apply(backup_preflight("pf-stale", "source-admin", topics=["audit"],
                               timeout_seconds=120))
        obj = wait_preflight("pf-stale", timeout=600)
        artifact("objects/s19/preflight-pf-stale.json", obj)
        checks = checks_by_id(obj)
        entry = checks.get("connection.topicsDescribable")
        sc.detail["checks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                               for k, v in checks.items()}
        check(entry is not None, f"connection.topicsDescribable did not run: {sorted(checks)}")
        sc.detail["topicsDescribable"] = entry
        check(entry["state"] == "notReady", f"the check is {entry['state']!r}")
        check(entry["code"] == "TopicNotFound",
              f"code is {entry['code']!r}, expected TopicNotFound")
        still = {r["name"] for r in topics_from_chunks(get("topicdiscovery", "td-full"))}
        sc.detail["inventoryStillNamesAudit"] = "audit" in still
        check("audit" in still,
              "the stored inventory changed, so the preflight could have used it")


# --------------------------------------------------------------------------
# a negative control: the harness must be able to fail
# --------------------------------------------------------------------------


def negative_control() -> None:
    """A deliberately false assertion about a real object, so the reader can
    see this harness report a failure rather than assume it would."""
    with Scenario("NC", "negative control: a true fact asserted false") as sc:
        obj = get("backupdestination", "dest-a")
        sc.detail["observedBucket"] = obj["spec"]["storage"]["bucket"]
        check(obj["spec"]["storage"]["bucket"] == "this-bucket-does-not-exist",
              "NEGATIVE CONTROL: dest-a's bucket is lw-a, and this assertion says it "
              "is 'this-bucket-does-not-exist'. A run in which this line does not "
              "produce a FAIL is a run whose passes mean nothing.")



# --------------------------------------------------------------------------
# A prefix-less destination, so the restore-preflight archive checks can be
# judged on their own — the control beside E4's prefixed destination, not a way
# around a defect: D2-PREFLIGHT-PREFIX is closed and E4 asserts the join.
# --------------------------------------------------------------------------


def s16prep() -> None:
    """Bucket `lw-c` on minio-a with a full-bucket principal, a destination
    whose `storage.prefix` is EMPTY, and one backup written through it."""
    load_secrets()
    for user in ("c-writer", "c-reader"):
        if user not in SECRETS:
            raise RuntimeError("credentials.json has no " + user)
        literal_secret(user, {"access-key-id": user, "secret-access-key": SECRETS[user]})
    mc("mb", "a/lw-c", check=False)
    minio_user("a", "c-writer", "d2w14-c-writer", _policy("lw-c", write=True, prefixes=[""]))
    minio_user("a", "c-reader", "d2w14-c-reader", _policy("lw-c", write=False, prefixes=[""]))
    apply(destination("dest-c", bucket="lw-c", prefix="",
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="c-writer", archive_read="c-reader",
                      evidence_write="c-writer", evidence_read="c-reader",
                      description="the same store with an EMPTY storage.prefix"))
    apply(destination("dest-c-denied", bucket="lw-c", prefix="",
                      endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                      archive_write="a-denied", archive_read="a-denied",
                      evidence_write="a-denied", evidence_read="a-denied",
                      description="the prefix-less location with a principal that has no policy"))
    for name in ("dest-c", "dest-c-denied"):
        wait_for("backupdestination", name,
                 lambda o: any(c.get("type") == "Valid"
                               for c in o.get("status", {}).get("conditions", [])),
                 timeout=180, what="a Valid condition")
    if get_opt("backup", "bk-c") is None:
        apply(backup("bk-c", source="source-admin", topics=["payments"],
                     destination_ref="dest-c", deadline=600))
    obj = wait_backup("bk-c", timeout=720)
    artifact("objects/s16/backup-bk-c.json", obj)
    if obj["status"].get("phase") != "Succeeded":
        raise RuntimeError("bk-c did not succeed: " + json.dumps(obj["status"])[:600])
    log("s16prep: dest-c (empty prefix) and bk-c ready")


def e4() -> None:
    with Scenario("E4", "the plan names a relative manifest key and the product joins the "
                        "destination's prefix at the read") as sc:
        prefixed = backup_facts("bk-a2", "a", "lw-a")
        flat = backup_facts_flat("bk-c", "a", "lw-c")
        sc.detail["prefixedBackup"] = prefixed
        sc.detail["prefixlessBackup"] = flat
        plan = restore_plan(flat["backupId"], flat["pointInTime"], "d2w14-flat-")
        plan["source"]["storage"] = {
            "backend": "s3", "bucket": "lw-c", "prefix": "",
            "region": "us-east-1", "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        }
        plan["source"]["topics"] = ["payments"]
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        apply(preflight("pf-flat", restore_preflight_request(
            plan_bytes, source_destination="dest-c", evidence_destination="dest-c"),
            timeout_seconds=180))
        flat_pf = wait_preflight("pf-flat", timeout=600)
        artifact("objects/s16/preflight-pf-flat.json", flat_pf)
        flat_checks = checks_by_id(flat_pf)
        sc.detail["prefixlessChecks"] = {
            k: {"state": v.get("state"), "code": v.get("code")}
            for k, v in flat_checks.items() if k.startswith("archive")}
        prefixed_pf = get_opt("preflight", "pf-plan1")
        if prefixed_pf is not None:
            prefixed_checks = checks_by_id(prefixed_pf)
            sc.detail["prefixedChecks"] = {
                k: {"state": v.get("state"), "code": v.get("code")}
                for k, v in prefixed_checks.items() if k.startswith("archive")}
            job = (prefixed_pf.get("status", {}).get("jobRef") or {}).get("name")
            if job:
                cm = get_opt("configmap", job + "-plan")
                if cm is not None:
                    plan_doc = json.loads(cm["data"]["check-plan.json"])
                    sc.detail["prefixedManifestKey"] = plan_doc["request"][
                        "restorePreflight"].get("manifestKey")
                    sc.detail["prefixedLocationPrefix"] = plan_doc["request"][
                        "restorePreflight"]["sourceDestination"]["location"]["prefix"]
        # THIS ROW WAS INVERTED AND ITS ASSERTIONS WERE NOT (review F-3). Its
        # title said the preflight *"reads the manifest WITHOUT the
        # destination's storage.prefix"*, which reads like D2-PREFLIGHT-PREFIX
        # reproducing; what it actually asserted — a RELATIVE `manifestKey` in
        # the plan — is the post-fix contract, because
        # `crates/logweir/src/check/kinds/restore.rs` does
        # `access.qualify(&req.manifest_key)` under "THE PREFIX IS JOINED HERE,
        # ONCE". The proof that the join works was in the row's own `detail` and
        # asserted nowhere. It is asserted now.
        entry = flat_checks.get("archive.backupSet")
        check(entry is not None, f"archive.backupSet did not run: {sorted(flat_checks)}")
        check(entry["state"] == "ready" and entry["code"] == "ManifestReadable",
              "with an EMPTY storage.prefix the restore preflight cannot read the "
              f"manifest: {entry['state']}/{entry['code']} — {entry.get('message')}")
        check(sc.detail.get("prefixedManifestKey", "").count("/") == 1,
              "the check plan must name a RELATIVE manifestKey — the archive's own "
              "convention — because the product joins the destination's prefix once, at "
              f"the read: {sc.detail.get('prefixedManifestKey')!r}")
        prefixed_checks = sc.detail.get("prefixedChecks") or {}
        check(bool(prefixed_checks),
              "the prefixed destination's preflight (pf-plan1) was not available, so this "
              "row cannot show the join working")
        manifest = prefixed_checks.get("archive.backupSet") or {}
        segments = prefixed_checks.get("archive.segments") or {}
        sc.detail["prefixJoinedAtTheRead"] = {
            "storagePrefix": sc.detail.get("prefixedLocationPrefix"),
            "planManifestKey": sc.detail.get("prefixedManifestKey"),
            "archive.backupSet": manifest, "archive.segments": segments,
        }
        check(manifest.get("state") == "ready" and manifest.get("code") == "ManifestReadable",
              f"on the PREFIXED destination (storage.prefix "
              f"{sc.detail.get('prefixedLocationPrefix')!r}) the manifest is "
              f"{manifest.get('state')}/{manifest.get('code')} — D2-PREFLIGHT-PREFIX is "
              f"reproducing, not closed")
        check(segments.get("state") == "ready" and segments.get("code") == "SegmentsPresent",
              f"on the PREFIXED destination the segments are "
              f"{segments.get('state')}/{segments.get('code')}")



def s21prep() -> None:
    """A connection whose only job is to be deleted and recreated, plus the
    saved spec the UI journey recreates it from, plus one discovery bound to
    its first UID."""
    servers = [f"kafka-empty.{NS}.svc.cluster.local:9096"]
    obj = kafka_cluster("stale-probe", servers=servers, username="admin",
                        secret="kafka-empty-admin")
    apply(obj)
    config_map("stale-probe-spec", {"spec.json": json.dumps(obj)})
    wait_for("kafkacluster", "stale-probe",
             lambda o: o.get("status", {}).get("reachable") is True,
             timeout=240, what="reachable=true")
    # ALWAYS a fresh one: a discovery left over from an earlier run is bound to
    # an earlier UID and is already stale, which is the very state the journey
    # has to create for itself if the measurement is to mean anything.
    run(K + ["delete", "topicdiscovery", "td-stale", "--ignore-not-found"], timeout=120)
    apply(topic_discovery("td-stale", "stale-probe", timeout_seconds=120))
    found = wait_discovery("td-stale", timeout=420)
    artifact("objects/s21/topicdiscovery-td-stale.json", found)
    if found["status"]["phase"] != "Succeeded":
        raise RuntimeError("td-stale did not succeed: " + json.dumps(found["status"])[:400])
    log("s21prep: stale-probe and td-stale ready")


def s21() -> None:
    with Scenario("S21", "editing a destination makes a bound preflight stale without "
                         "touching the plan") as sc:
        facts = backup_facts_flat("bk-c", "a", "lw-c")
        plan = restore_plan(facts["backupId"], facts["pointInTime"], "d2w14-edit-")
        plan["source"]["topics"] = ["payments"]
        plan["source"]["storage"] = {
            "backend": "s3", "bucket": "lw-c", "prefix": "",
            "region": "us-east-1", "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        }
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        plan_hash = digest(plan_bytes)
        apply(preflight("pf-edit", restore_preflight_request(
            plan_bytes, source_destination="dest-c", evidence_destination="dest-c"),
            timeout_seconds=180))
        obj = wait_preflight("pf-edit", timeout=600)
        artifact("objects/s21/preflight-pf-edit-before.json", obj)
        referents = {f"{r['kind']}/{r['name']}": r
                     for r in obj["status"].get("binding", {}).get("referents", [])}
        sc.detail["referentsBefore"] = referents
        key = "BackupDestination/dest-c"
        check(key in referents,
              f"the preflight records no referent for dest-c: {sorted(referents)}")
        before_generation = referents[key].get("generation")
        before = get("backupdestination", "dest-c")
        # A CA bundle is the one mutable part of a destination (D2 §3.1: "a CA
        # rotates"), so this edits `transport.caBundle` rather than the sealed
        # location. The plan names a LOCATION, and the location does not move.
        run(K + ["patch", "backupdestination", "dest-c", "--type", "merge", "-p",
                 json.dumps({"spec": {"description": "edited at " + now()}})], timeout=60)
        after = get("backupdestination", "dest-c")
        sc.detail["generation"] = {
            "destinationBefore": before["metadata"]["generation"],
            "destinationAfter": after["metadata"]["generation"],
            "preflightBoundGeneration": before_generation,
        }
        check(after["metadata"]["generation"] > before["metadata"]["generation"],
              "editing the destination did not increment its generation")
        check(before_generation is not None and
              after["metadata"]["generation"] != before_generation,
              "the preflight's bound referent generation still equals the live one, so "
              "no reader could tell the destination had changed")
        # and the plan the approver signed did not move
        still = get("preflight", "pf-edit")
        artifact("objects/s21/preflight-pf-edit-after.json", still)
        sc.detail["planHash"] = {
            "computed": plan_hash,
            "boundBefore": obj["status"]["binding"].get("planHash"),
            "boundAfter": still["status"]["binding"].get("planHash"),
        }
        check(still["status"]["binding"].get("planHash") ==
              obj["status"]["binding"].get("planHash") == plan_hash,
              "the bound plan hash moved when only the destination was edited")
        sc.detail["locationDigest"] = {
            "before": before["status"].get("locationDigest"),
            "after": after["status"].get("locationDigest"),
        }
        check(before["status"].get("locationDigest") == after["status"].get("locationDigest"),
              "the destination's location digest moved on a description edit")


def s20() -> None:
    record(
        "S20", "a pod spoofing a check Job's label is ignored (G10)", "notRun",
        reason=(
            "D2 §14.4 marks S20 optional. The controller reads a check's frames only "
            "through the Job's owner UID, and `ForeignPodIgnored` is the code it "
            "records; proving it live means creating a bare pod carrying another "
            "object's `batch.kubernetes.io/job-name` label while that Job is running, "
            "which is a race this wave did not have the time budget to make "
            "deterministic. The controller-side rule is covered by "
            "`crates/weirkeeper/tests/` (D2 W8's S6 row) and is NOT claimed live here."
        ),
    )



def e5() -> None:
    with Scenario("E5", "a signing key the cluster's roster lists is reported rostered") as sc:
        keys = approver_material()
        roster = json.loads(
            run(CTX + ["get", "trustroster", "default", "-o", "json"], timeout=60).stdout)
        rostered = {entry["keyId"] for entry in roster["spec"].get("signingKeys", [])}
        # binary output, so this reads bytes rather than text
        der = subprocess.run(
            ["openssl", "pkey", "-pubin", "-in", str(keys["signingPub"]), "-outform", "DER"],
            capture_output=True, timeout=60, check=True,
        ).stdout
        projected = hashlib.sha256(der).hexdigest()
        sc.detail["rosterSigningKeyIds"] = sorted(rostered)
        sc.detail["projectedSigningKeyId"] = projected
        sc.detail["secretProjectedIntoCheckJobs"] = "logweir-signing-key/signing.pem"
        check(projected in rostered,
              "the key this namespace projects is genuinely absent from the roster, so "
              "SignerNotRostered would be correct")
        obj = get_opt("preflight", "pf-wrong-password")
        check(obj is not None, "run the s14 phase first: pf-wrong-password does not exist")
        entry = checks_by_id(obj).get("signer.rostered")
        check(entry is not None, "signer.rostered did not run")
        sc.detail["signerRostered"] = entry
        usable = checks_by_id(obj).get("signer.privateKeyUsable")
        sc.detail["signerPrivateKeyUsable"] = usable
        check(entry["state"] == "ready" and entry["code"] == "SignerRostered",
              "the projected signing key IS on TrustRoster/default (key id "
              f"{projected}), the check Job parsed it and signed a probe with it "
              f"(signer.privateKeyUsable = {usable.get('code') if usable else None}), and "
              f"signer.rostered still answers {entry['state']}/{entry['code']}: "
              f"{entry.get('message')!r}. The fact the roster is matched against reaches "
              "the controller REDACTED, so the comparison is made against the literal "
              "string `[redacted]`.")



def e6() -> None:
    """The closest a draft restore preflight gets to green on this build."""
    with Scenario("E6", "a restore preflight whose every prerequisite is satisfied") as sc:
        # ITS OWN BACKUP: S16 deletes a segment of `bk-c` on purpose, and a
        # preflight over a knowingly-damaged set is not the question here.
        if get_opt("backup", "bk-c2") is None:
            apply(backup("bk-c2", source="source-admin", topics=["payments"],
                         destination_ref="dest-c", deadline=600))
        made = wait_backup("bk-c2", timeout=720)
        check(made["status"].get("phase") == "Succeeded",
              f"bk-c2 is {made['status'].get('phase')!r}")
        facts = backup_facts_flat("bk-c2", "a", "lw-c")
        sc.detail["sourceBackup"] = facts
        plan = restore_plan(facts["backupId"], facts["pointInTime"], "d2w14-green-")
        plan["source"]["topics"] = ["payments"]
        flat = {
            "backend": "s3", "bucket": "lw-c", "prefix": "",
            "region": "us-east-1", "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        }
        plan["source"]["storage"] = flat
        plan["evidence"] = dict(flat, prefix="logweir/")
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        name = f"pf-green-{attempt('e6Attempt')}"
        apply(preflight(name, restore_preflight_request(
            plan_bytes, source_destination="dest-c", evidence_destination="dest-c"),
            timeout_seconds=180))
        obj = wait_preflight(name, timeout=600)
        artifact(f"objects/e6/preflight-{name}.json", obj)
        result = obj["status"]["result"]
        checks = checks_by_id(obj)
        blocking_not_ready = {
            k: {"state": v.get("state"), "code": v.get("code"), "message": v.get("message")}
            for k, v in checks.items()
            if v.get("gating") == "blocking" and v.get("state") != "ready"
        }
        sc.detail["overallState"] = result.get("state")
        sc.detail["blockingRowsNotReady"] = blocking_not_ready
        sc.detail["allChecks"] = {k: {"state": v.get("state"), "code": v.get("code")}
                                  for k, v in checks.items()}
        # D2 §6.3: a DRAFT's `approval.state` is `skipped (SubjectNotCreated)`, and
        # §6.4 says a skipped blocking check keeps the overall state `unknown` —
        # never `ready`. So the acceptance question is whether anything ELSE is
        # blocking, not whether the aggregate reads green.
        unexpected = {k: v for k, v in blocking_not_ready.items() if k != "approval.state"}
        check(not unexpected,
              "a restore preflight with every referent satisfied still has blocking rows "
              f"that are not ready: {json.dumps(unexpected, indent=1)[:1500]}")
        check(checks.get("approval.state", {}).get("state") == "skipped",
              "the draft's approval row is not `skipped`: "
              f"{checks.get('approval.state')}")
        check(result.get("state") in {"unknown", "ready"},
              f"with only the draft's approval row skipped the aggregate is "
              f"{result.get('state')!r}; D2 §6.4 says a skipped blocking check keeps it "
              "`unknown`")


# ==========================================================================
# U6 — the per-role minimal object-storage permission set, MEASURED
# ==========================================================================
#
# D2 §15 U6 is the last open item of PLAT-08.1: §3.11's per-role table was
# RECORDED from the policies that happened to suffice, never bisected. This
# phase measures it.
#
# The method, per role:
#
#   1. attach the RECORDED starting set to that role's MinIO principal and run
#      the role's own operation — it must succeed, or there is nothing to
#      bisect;
#   2. for every grant unit in the set, re-attach the set WITHOUT that unit and
#      run the operation again. A unit whose removal still succeeds was never
#      needed; a unit whose removal makes the operation fail is in the minimal
#      set, and the failure must be the PRODUCT's own classified answer —
#      `AccessDenied` on a check row, a nonzero `exitCode` on a run — and never
#      a timeout, a crash or an unrelated code;
#   3. attach exactly the minimal set and run once more, so the table's row is
#      a set that was executed and not a set that was inferred.
#
# A "grant unit" is one S3 action at one resource scope, because scope is half
# the answer: `s3:ListBucket` is authorised on the BUCKET arn and `s3:GetObject`
# on an OBJECT arn, and a table that named only actions would be unusable.

U6_BUCKET = "lw-u6"
U6_ARCHIVE_PREFIX = "team/u6"
U6_EVIDENCE_PREFIX = "logweir"

#: The resource scopes a unit can name. `bucket:*` are `s3:ListBucket`'s
#: prefix-conditioned forms; MinIO refuses an `s3:prefix` condition on
#: `s3:GetBucketLocation`, which is why `bucket` exists unconditioned.
#: `bucket:both` is retained for the record — a policy document may legally
#: carry both prefix legs in ONE condition — but no starting set uses it any
#: more: a compound unit withdrawn whole shows only that ONE of its legs was
#: needed, so a minimal set containing it is asserted at the other leg
#: (reviewer finding **F1**). `readiness` is the narrower object scope D2 §3.11
#: records for the write probe, `<bucket>/logweir/readiness/*`.
U6_SCOPES = ("bucket", "bucket:archive", "bucket:evidence", "bucket:both",
             "archive", "evidence", "readiness")

#: The codes the product uses for "the backend evaluated this request and
#: refused it". A removal that fails with anything else is NOT a permission
#: measurement — it is a broken fixture, and `u6_row_is_proved` says so.
U6_DENIAL_CODES = {"AccessDenied", "InvalidCredentials"}

#: The retention enforcer's own per-point codes for "the delete credential was
#: refused a call the enforcer needs" (`retention-point=… state=Kept
#: code=<code>`). Since ctl-batch-2 (`62ef1a1`) the enforcer HEADs every key
#: before deleting it and, when the credential lacks `s3:GetObject`, keeps the
#: point with `VersionProbeRefused` — the product's own name for that refusal,
#: which carries no `AccessDenied` text of its own. `VersionedBucket` is NOT
#: here: it is a refusal about the bucket, not about the grant, and a removal
#: that ended in it measured nothing about the unit removed.
U6_RETENTION_DENIAL_CODES = {"VersionProbeRefused"}

#: The substrings a denied object-store call leaves in a runner's own output.
U6_DENIAL_TEXT = ("AccessDenied", "Access Denied", "access denied",
                  "InvalidAccessKeyId", "SignatureDoesNotMatch", "403 Forbidden")


def u6_statement(unit: str) -> dict[str, Any]:
    """One grant unit — `<action>@<scope>` — as a MinIO policy statement."""
    action, _, scope = unit.partition("@")
    if scope not in U6_SCOPES:
        raise ValueError(f"unknown scope {scope!r} in unit {unit!r}")
    bucket_arn = f"arn:aws:s3:::{U6_BUCKET}"
    prefixes = {
        "bucket:archive": [f"{U6_ARCHIVE_PREFIX}/*"],
        "bucket:evidence": [f"{U6_EVIDENCE_PREFIX}/*"],
        "bucket:both": [f"{U6_ARCHIVE_PREFIX}/*", f"{U6_EVIDENCE_PREFIX}/*"],
    }
    if scope == "bucket":
        return {"Effect": "Allow", "Action": [action], "Resource": [bucket_arn]}
    if scope in prefixes:
        return {
            "Effect": "Allow",
            "Action": [action],
            "Resource": [bucket_arn],
            "Condition": {"StringLike": {"s3:prefix": prefixes[scope]}},
        }
    root = {
        "archive": U6_ARCHIVE_PREFIX,
        "evidence": U6_EVIDENCE_PREFIX,
        "readiness": f"{U6_EVIDENCE_PREFIX}/readiness",
    }[scope]
    return {
        "Effect": "Allow",
        "Action": [action],
        "Resource": [f"arn:aws:s3:::{U6_BUCKET}/{root}/*"],
    }


def u6_policy(units: Iterable[str]) -> dict[str, Any]:
    return {"Version": "2012-10-17",
            "Statement": [u6_statement(u) for u in units]}


def u6_attach(user: str, units: list[str], tag: str) -> str:
    """Replace `user`'s MinIO policy with EXACTLY `units`, and return the name.

    A fresh policy name every time: `mc admin policy create` over an existing
    name is version-dependent, and a bisection that silently kept the previous
    document would measure the wrong set.
    """
    seq = int(state.get("u6PolicySeq", 0)) + 1
    state["u6PolicySeq"] = seq
    name = f"u6-{tag}-{seq}"[:60]
    if not units:
        # MinIO refuses a policy document with no statement, and "no policy
        # attached" is the honest spelling of an empty grant anyway.
        previous = (state.get("u6Attached") or {}).get(user)
        if previous:
            mc("admin", "policy", "detach", "a", previous, "--user", user, check=False)
        attached = dict(state.get("u6Attached") or {})
        attached.pop(user, None)
        state["u6Attached"] = attached
        save()
        artifact(f"u6/policies/{name}.json", {"note": "no policy attached", "units": []})
        return "(no policy attached)"
    body = json.dumps(u6_policy(units))
    for attempt_no in range(3):
        proc = run(K + ["exec", "-i", "mc", "--", "sh", "-c", f"cat > /tmp/{name}.json"],
                   data=body, check=False, timeout=60)
        if proc.returncode == 0:
            break
        time.sleep(2 * (attempt_no + 1))
    mc("admin", "policy", "create", "a", name, f"/tmp/{name}.json")
    mc("admin", "policy", "attach", "a", name, "--user", user)
    previous = (state.get("u6Attached") or {}).get(user)
    if previous and previous != name:
        mc("admin", "policy", "detach", "a", previous, "--user", user, check=False)
    attached = dict(state.get("u6Attached") or {})
    attached[user] = name
    state["u6Attached"] = attached
    save()
    artifact(f"u6/policies/{name}.json", u6_policy(units))
    return name


#: A `RecoveryCatalog` never publishes `AccessDenied`. A walk whose points
#: would not open lands `Synced=False/PartialScan` — "a permission or transport
#: failure, which is NOT the same as absent" — and one whose first listing was
#: refused relays no body and lands `ResultUnreadable`. In a bisection the only
#: variable is the MinIO policy, so these are that failure classified; the
#: operation records WHICH of them it saw as `deniedBy`, and never infers a
#: denial from a sync that merely did not finish.
U6_CATALOG_REFUSAL_REASONS = ("PartialScan", "ResultUnreadable")


def u6_denied(classified: Any) -> bool:
    """Did the PRODUCT say `denied` — as a check code, a recorded refusal, or
    in its own output?"""
    if isinstance(classified, dict):
        # An operation whose product vocabulary is not the check contract's
        # records the sentence it did say, and records nothing when it did not.
        if classified.get("deniedBy"):
            return True
        if any(str(c).split(":")[0] in U6_RETENTION_DENIAL_CODES
               for c in (classified.get("pointCodes") or [])):
            return True
        for key in ("code", "reason", "exitReason"):
            if classified.get(key) in U6_DENIAL_CODES:
                return True
        blob = json.dumps(classified, default=str)
    else:
        blob = str(classified)
    return any(marker in blob for marker in U6_DENIAL_TEXT)


def u6_row_is_proved(
    role: str,
    baseline: dict[str, Any],
    removals: list[dict[str, Any]],
    minimal: list[str],
    confirm: dict[str, Any] | None,
) -> dict[str, bool]:
    """Is one role's measured row actually PROVED by what was run?

    Pure: every argument is a recorded outcome, so `test_rows.py` can make each
    clause say False. A row that cannot be made to say False is not a row.
    """
    removed = [r["unit"] for r in removals]
    required = [r["unit"] for r in removals if not r["ok"]]
    return {
        "the role is named": bool(role),
        "the starting set ran and succeeded": bool(baseline.get("ok")),
        "every unit of the starting set was removed exactly once":
            len(removed) == len(set(removed)) and len(removed) > 0,
        "the minimal set is exactly the units whose removal failed":
            sorted(minimal) == sorted(required),
        "at least one unit is required":
            len(required) > 0,
        "every required unit's failure is the product's own denial":
            all(u6_denied(r.get("classified")) for r in removals if not r["ok"]),
        "no required unit failed for an unclassified reason":
            all(not r.get("unclassified") for r in removals if not r["ok"]),
        "every unit outside the minimal set really did succeed without it":
            all(r["ok"] for r in removals if r["unit"] not in minimal),
        "the minimal set itself was executed and succeeded":
            bool((confirm or baseline).get("ok"))
            and (sorted(minimal) == sorted(removed) or confirm is not None),
    }


def u6_bisect(
    sc: "Scenario",
    *,
    role: str,
    user: str,
    starting: list[str],
    operation: Callable[[str], dict[str, Any]],
    note: str = "",
    before: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Measure one role's minimal set. See the module comment for the method.

    `before` runs ahead of every variant. Two roles need it: the readiness
    marker is create-only, so a second probe answers `MarkerAlreadyPresent`
    about the FIRST probe's object, and the enforcer deletes the very objects
    its next run would delete. A bisection without it measures leftovers.
    """
    log(f"U6[{role}]: starting set of {len(starting)} units on principal {user}")
    if before:
        before()
    u6_attach(user, starting, f"{role}-full")
    baseline = operation(f"{role}-full")
    baseline["units"] = list(starting)
    if not baseline.get("ok"):
        raise AssertionError(
            f"U6[{role}]: the RECORDED starting set does not even work: "
            + redact(json.dumps(baseline.get("classified"), default=str))[:600]
        )
    removals: list[dict[str, Any]] = []
    for index, unit in enumerate(starting):
        reduced = [u for u in starting if u != unit]
        if before:
            before()
        u6_attach(user, reduced, f"{role}-no{index}")
        outcome = operation(f"{role}-no{index}")
        outcome["unit"] = unit
        outcome["remaining"] = reduced
        removals.append(outcome)
        verdict = "still works (NOT required)" if outcome["ok"] else "FAILS (required)"
        log(f"U6[{role}]: without `{unit}` — {verdict}")
        # A partial result is evidence: write the table after every row so a
        # killed worker resumes from what it measured.
        artifact(f"u6/{role}.json",
                 {"role": role, "user": user, "starting": starting,
                  "baseline": baseline, "removals": removals})
    minimal = [r["unit"] for r in removals if not r["ok"]]
    confirm: dict[str, Any] | None = None
    if sorted(minimal) != sorted(starting):
        if before:
            before()
        u6_attach(user, minimal, f"{role}-min")
        confirm = operation(f"{role}-min")
        confirm["units"] = list(minimal)
        if not confirm.get("ok"):
            raise AssertionError(
                f"U6[{role}]: the measured minimal set does not work as a whole: "
                + redact(json.dumps(confirm.get("classified"), default=str))[:600]
            )
    proof = u6_row_is_proved(role, baseline, removals, minimal, confirm)
    row = {
        "role": role,
        "principal": user,
        "note": note,
        "startingSet": starting,
        "minimalSet": minimal,
        "notRequired": [u for u in starting if u not in minimal],
        "baseline": baseline,
        "removals": removals,
        "minimalConfirmed": confirm,
        "proof": proof,
    }
    artifact(f"u6/{role}.json", row)
    table = dict(state.get("u6Table") or {})
    table[role] = row
    state["u6Table"] = table
    save()
    sc.detail.setdefault("roles", {})[role] = {
        "minimalSet": minimal,
        "notRequired": row["notRequired"],
        "proof": proof,
    }
    check(all(proof.values()),
          f"U6[{role}]: the measured row is not proved: "
          + json.dumps({k: v for k, v in proof.items() if not v}))
    return row


def u6_seq() -> int:
    """A monotonic object counter, so every run of every variant is its OWN
    object and no assertion can read a previous attempt's status."""
    n = int(state.get("u6ObjSeq", 0)) + 1
    state["u6ObjSeq"] = n
    save()
    return n


#: The display name of each role, WITH its own markup: a title is wrapped in
#: nothing by the renderer, because two of these carry backticks of their own
#: and nesting them produces `` `retention enforcer (`logweir-retention`)` ``.
#: Every role's RECORDED starting set, in one place, so `test_rows.py` can
#: assert that none of them carries a compound unit (reviewer finding **F1**)
#: and a reader can see what was withdrawn without reading seven phases.
U6_STARTING_SETS: dict[str, list[str]] = {
    "archive-write": [
        "s3:ListBucket@bucket:archive", "s3:ListBucket@bucket:evidence",
        "s3:GetBucketLocation@bucket",
        "s3:GetObject@archive", "s3:PutObject@archive",
        "s3:AbortMultipartUpload@archive", "s3:DeleteObject@archive",
        "s3:GetObject@evidence", "s3:PutObject@evidence",
        "s3:AbortMultipartUpload@evidence",
    ],
    "archive-read": [
        "s3:ListBucket@bucket:archive", "s3:GetObject@archive",
        "s3:GetBucketLocation@bucket",
    ],
    "evidence-write": [
        "s3:PutObject@evidence", "s3:GetObject@evidence",
        "s3:ListBucket@bucket:evidence", "s3:GetBucketLocation@bucket",
    ],
    "evidence-read": [
        "s3:GetObject@evidence", "s3:ListBucket@bucket:evidence",
        "s3:GetBucketLocation@bucket",
    ],
    "write-probe": [
        "s3:PutObject@readiness", "s3:GetObject@evidence",
        "s3:ListBucket@bucket:evidence", "s3:GetBucketLocation@bucket",
        "s3:ListBucket@bucket:archive", "s3:GetObject@archive",
    ],
    "catalog-sync": [
        "s3:ListBucket@bucket:archive", "s3:ListBucket@bucket:evidence",
        "s3:GetBucketLocation@bucket",
        "s3:GetObject@archive", "s3:GetObject@evidence",
    ],
    "retention-enforcer": [
        "s3:ListBucket@bucket:archive", "s3:GetBucketLocation@bucket",
        "s3:GetObject@archive", "s3:DeleteObject@archive",
    ],
}


U6_ROLES = {
    "archive-write": "`archiveWrite`",
    "archive-read": "`archiveRead`",
    "evidence-write": "`evidenceWrite`",
    "evidence-read": "`evidenceRead`",
    "retention-enforcer": "retention enforcer (`logweir-retention`)",
    "catalog-sync": "`catalogSync` reader",
    "write-probe": "write probe (`readiness.writeProbe: CreateOnlyMarker`)",
}

U6_PRINCIPALS = {
    "archive-write": "u6-writer",
    "archive-read": "u6-reader",
    "evidence-write": "u6-evwriter",
    "evidence-read": "u6-evreader",
    "retention-enforcer": "u6-deleter",
    "catalog-sync": "u6-catreader",
    "write-probe": "u6-writer",
}


def u6_mc_pod() -> dict[str, Any]:
    """One `mc` with a single alias. Not `mc_pod()`: that one waits three
    minutes per MinIO this phase does not deploy, and needs the private CA the
    TLS fixture builds."""
    endpoint = f"http://minio-a.{NS}.svc:9000"
    script = "; ".join([
        "set -e",
        "for i in $(seq 1 90); do "
        f"  mc alias set a {endpoint} {MINIO_ROOT_USER} \"$ROOT\" >/dev/null 2>&1 && break; "
        "  sleep 2; done",
        f"mc alias set a {endpoint} {MINIO_ROOT_USER} \"$ROOT\" >/dev/null",
        "touch /tmp/ready",
        "sleep 21600",
    ])
    return {
        "apiVersion": "v1", "kind": "Pod", "metadata": owned("mc"),
        "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False,
            "containers": [{
                "name": "mc", "image": MC_IMAGE, "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c"], "args": [script],
                "env": [
                    {"name": "MC_CONFIG_DIR", "value": "/tmp/mcconfig"},
                    {"name": "ROOT", "valueFrom": {
                        "secretKeyRef": {"name": "minio-root", "key": "password"}}},
                ],
                "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]},
                                   "periodSeconds": 1},
            }],
        },
    }


def u6_signing_key() -> None:
    """The signing key this run's receipts are signed with.

    `approver_material()` reads `/tmp/logweir-scram-e2e`, which this host no
    longer has (the lab was rebuilt since D2 W14). The key the cluster-scoped
    `TrustRoster/default` already trusts lives in the shared release's own
    Secret, so it is COPIED — through a pipe, never printed, never written to
    an artifact — into this run's namespace. The shared release is not
    modified: this is a read of one Secret. U6 needs no approver key, because
    every restore preflight it runs is a draft.
    """
    if get_opt("secret", "logweir-signing-key") is not None:
        return
    lab = json.loads(run(CTX + ["-n", LAB_NS, "get", "secret", "logweir-signing-key",
                                "-o", "json"], timeout=60).stdout)
    apply({"apiVersion": "v1", "kind": "Secret",
           "metadata": owned("logweir-signing-key"),
           "type": lab.get("type", "Opaque"),
           "data": {"signing.pem": lab["data"]["signing.pem"]}})
    log("u6setup: the roster's signing key is present in this namespace")


def u6setup() -> None:
    """The U6 fixture: ONE MinIO, TWO brokers, one bucket, one principal per
    role, and one destination per readiness shape. Deliberately smaller than
    `setup()`: U6 measures object-storage permissions and needs neither TLS,
    nor a second store, nor an ACL-limited broker."""
    load_secrets()
    log(f"u6setup: namespace {NS}")
    ns = create_namespace(NS)
    state["namespaceUid"] = ns["metadata"]["uid"]
    save()

    literal_secret("minio-root", {"password": SECRETS["minio-root"]})
    for user in U6_PRINCIPALS.values():
        literal_secret(user, {"access-key-id": user,
                              "secret-access-key": SECRETS[user]})
    literal_secret("kafka-admin", {"password": SECRETS["kafka-admin"]})
    literal_secret("kafka-target-admin", {"password": SECRETS["kafka-target-admin"]})
    u6_signing_key()
    apply({"apiVersion": "v1", "kind": "ServiceAccount",
           "metadata": owned("logweir-runner"), "automountServiceAccountToken": False})
    # `retention_policy.rs::SERVICE_ACCOUNT` — an enforcement Job names it and
    # the kubelet refuses the pod without it. Absent, the Job exists, no pod is
    # ever created, `status.failed` stays 0 so the Job never looks terminal,
    # and three runs later the policy is `EnforcementDegraded` with "the last
    # run produced no exit code (its pod is gone or was never readable)".
    apply({"apiVersion": "v1", "kind": "ServiceAccount",
           "metadata": owned("logweir-retention"), "automountServiceAccountToken": False})

    objects: list[dict[str, Any]] = []
    objects += kafka_objects("kafka-acl", "U6D2AclAAAAAAAAAAAAAAA",
                             heap="-Xmx700M -Xms256M", memory="1200Mi")
    objects += kafka_objects("kafka-target", "U6D2TgtAAAAAAAAAAAAAAA",
                             heap="-Xmx512M -Xms256M", memory="1Gi")
    objects += minio_objects("minio-a", tls=False)
    for obj in objects:
        apply(obj)
    for app in ("kafka-acl", "kafka-target", "minio-a"):
        wait_pod_ready(f"app={app}", timeout=420)
        log(f"u6setup: {app} ready")
    run(K + ["delete", "pod", "mc", "--ignore-not-found", "--wait=true"], timeout=120)
    apply(u6_mc_pod())
    run(K + ["wait", "--for=condition=Ready", "pod/mc", "--timeout=300s"], timeout=330)
    log("u6setup: mc ready")

    mc("mb", f"a/{U6_BUCKET}", check=False)
    for user in U6_PRINCIPALS.values():
        run(K + ["exec", "-i", "mc", "--", "sh", "-c",
                 f"mc admin user add a {user} \"$(cat)\" >/dev/null"],
            data=SECRETS[user], timeout=120)
    log("u6setup: one MinIO principal per role, each with NO policy yet")

    for user, key in (("admin", "kafka-admin"), ):
        set_scram_credential("kafka-acl", user, SECRETS[key])
    set_scram_credential("kafka-target", "admin", SECRETS["kafka-target-admin"])
    for broker in ("kafka-acl", "kafka-target"):
        broker_exec(broker, [
            f"{KAFKA_BIN}/kafka-acls.sh", "--bootstrap-server", "localhost:9092",
            "--add", "--allow-principal", "User:admin", "--operation", "All",
            "--cluster", "--topic", "*", "--group", "*",
        ], check=False)
    broker_exec("kafka-acl", [
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
        "--create", "--topic", "orders", "--partitions", "1",
        "--replication-factor", "1",
    ], check=False)
    if not state.get("u6Seeded"):
        payload = "\n".join(f"orders-record-{i:04d}" for i in range(120)) + "\n"
        pods = get_list("pods", selector="app=kafka-acl")
        pod = [p for p in pods if p["status"]["phase"] == "Running"][0]["metadata"]["name"]
        run(K + ["exec", "-i", pod, "--", f"{KAFKA_BIN}/kafka-console-producer.sh",
                 "--bootstrap-server", "localhost:9092", "--topic", "orders"],
            data=payload, timeout=180)
        state["u6Seeded"] = True
        save()
        log("u6setup: `orders` seeded with 120 records")

    apply(kafka_cluster("source", servers=[f"kafka-acl.{NS}.svc.cluster.local:9096"],
                        username="admin", secret="kafka-admin", role="source"))
    apply(kafka_cluster("target", servers=[f"kafka-target.{NS}.svc.cluster.local:9096"],
                        username="admin", secret="kafka-target-admin", role="target"))

    common = dict(bucket=U6_BUCKET, prefix=U6_ARCHIVE_PREFIX,
                  endpoint=f"http://minio-a.{NS}.svc:9000", security="InsecureHTTP",
                  addressing="PathStyle")
    apply(destination(
        "u6-dest", archive_write="u6-writer", archive_read="u6-reader",
        evidence_write="u6-evwriter", evidence_read="u6-evreader",
        description="U6: one principal per role, so a policy change isolates one role",
        **common))
    # `writeProbe: CreateOnlyMarker` is read from the OBJECT and only for a
    # Backup/Restore readiness plan — `preflight.rs:3692` skips it for a
    # `DestinationAccess` request, which names its own roles. So the one
    # operation that probes an evidence-write grant is a Backup preflight on a
    # destination that opted in, and this one carries no other separated grant
    # so the probed row is about `evidenceWrite` alone.
    apply(destination(
        "u6-dest-probe", archive_write="u6-writer", evidence_write="u6-evwriter",
        write_probe=True,
        description="U6: the same location with the create-only readiness write probe on",
        **common))
    apply(destination(
        "u6-dest-write", archive_write="u6-writer",
        description="U6: `evidenceWrite` and `archiveRead` ABSENT, so both fall back to "
                    "`archiveWrite` \u2014 the shape D2 §3.11's archiveWrite row is about "
                    "(\"Backup Job (engine and receipt)\")",
        **common))
    apply(destination(
        "u6-dest-cat", archive_write="u6-writer", archive_read="u6-catreader",
        evidence_write="u6-evwriter", evidence_read="u6-evreader",
        description="U6: catalogSync reads through `archiveRead`; this one isolates it",
        **common))
    for name in ("u6-dest", "u6-dest-probe", "u6-dest-cat", "u6-dest-write"):
        obj = wait_for("backupdestination", name,
                       lambda o: o.get("status", {}).get("reason") is not None,
                       timeout=180, what="a Valid verdict")
        artifact(f"u6/objects/{name}.json", obj)
        check(obj["status"].get("reason") == "Valid",
              f"{name} is not Valid: {obj['status'].get('reason')}")
    log("u6setup: four destinations Valid")


def u6_backup_manifest_facts(backup_id: str) -> dict[str, Any]:
    return _facts_from_manifest(
        backup_id,
        json.loads(mc_get(f"a/{U6_BUCKET}/{U6_ARCHIVE_PREFIX}/{backup_id}/manifest.json")))


def u6_restore_plan(backup_id: str, point_in_time: str, prefix: str) -> dict[str, Any]:
    return {
        "source": {
            "storage": {
                "backend": "s3", "bucket": U6_BUCKET, "prefix": U6_ARCHIVE_PREFIX,
                "region": "us-east-1",
                "endpoint": f"http://minio-a.{NS}.svc:9000",
                "path_style": True, "allow_http": True,
            },
            "backup": backup_id,
            "topics": ["orders"],
        },
        "target": {
            "bootstrap_servers": [f"kafka-target.{NS}.svc.cluster.local:9096"],
            "auth": {"mode": "scramSha512", "username": "admin", "tls": False},
            "mode": "newTopic",
            "topic_naming": {"prefix": prefix},
            "topic_mapping_prefix": "logweir-scratch-",
            "marker_topic": "logweir.scratch",
            "default_replication_factor": 1,
            "teardown": "delete",
        },
        "restore": {"point_in_time": point_in_time},
        "sample": {
            "window_start": "2026-09-01T00:00:00Z",
            "window_end": point_in_time,
            "records_per_partition": 25,
            "anchor": "head",
        },
        "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
        "evidence": {
            "backend": "s3", "bucket": U6_BUCKET, "prefix": "logweir/",
            "region": "us-east-1",
            "endpoint": f"http://minio-a.{NS}.svc:9000",
            "path_style": True, "allow_http": True,
        },
        "notifications": {"webhooks": []},
    }


def u6_rows(obj: dict[str, Any], rows: list[str]) -> dict[str, Any]:
    checks = checks_by_id(obj)
    return {r: {"state": checks.get(r, {}).get("state"),
                "code": checks.get(r, {}).get("code"),
                "gating": checks.get(r, {}).get("gating"),
                "message": (checks.get(r, {}).get("message") or "")[:400]}
            for r in rows}


def u6_unanswered(picked: dict[str, Any], ok: bool) -> bool:
    """Did this check FAIL without answering anything it was asked?

    Not "did every row answer". A blocking row that failed puts the rows after
    it at `unknown/BlockedByPrerequisite` — "a prerequisite of this check did
    not pass, so it did not run" — which is the product being honest about a
    row it did not reach, downstream of a verdict it did reach. What would make
    a removal useless as a measurement is a result with NO `notReady` verdict
    at all: every row `unknown/PodNotStarted`, a check that timed out, a pod
    that never ran. That is a fact about the harness, not about the grant.
    """
    if ok:
        return False
    return not any(
        row.get("state") == "notReady" and row.get("code") for row in picked.values()
    )


def u6_check_outcome(obj: dict[str, Any], name: str, rows: list[str]) -> dict[str, Any]:
    picked = u6_rows(obj, rows)
    ok = all(picked[r]["state"] == "ready" for r in rows)
    unclassified = u6_unanswered(picked, ok)
    return {
        "ok": ok, "object": name, "kind": "Preflight",
        "overall": obj.get("status", {}).get("result", {}).get("state"),
        "classified": picked, "unclassified": unclassified,
    }


def u6_destination_access_op(dest: str, roles: list[str], rows: list[str]):
    def op(tag: str) -> dict[str, Any]:
        name = f"u6-da-{u6_seq():03d}"
        apply(preflight(name, {"operation": "DestinationAccess",
                               "destinationAccess": {"destinationRef": {"name": dest},
                                                     "roles": roles}},
                        timeout_seconds=150))
        obj = wait_preflight(name, timeout=600)
        artifact(f"u6/objects/{name}.json", obj)
        out = u6_check_outcome(obj, name, rows)
        out["variant"] = tag
        return out
    return op


def u6_restore_preflight_op(rows: list[str]):
    def op(tag: str) -> dict[str, Any]:
        facts = state["u6BackupFacts"]
        plan = json.dumps(
            u6_restore_plan(facts["backupId"], facts["pointInTime"], "u6-r-"), indent=2) + "\n"
        name = f"u6-rp-{u6_seq():03d}"
        apply(preflight(name, {
            "operation": "Restore",
            "restore": {"planBytes": plan, "planHash": digest(plan),
                        "targetRef": {"name": "target"},
                        "sourceDestinationRef": {"name": "u6-dest"},
                        "evidenceDestinationRef": {"name": "u6-dest"}},
        }, timeout_seconds=180))
        obj = wait_preflight(name, timeout=600)
        artifact(f"u6/objects/{name}.json", obj)
        out = u6_check_outcome(obj, name, rows)
        out["variant"] = tag
        return out
    return op


def u6_backup_preflight_op(dest: str, rows: list[str]):
    """A Backup readiness plan. The ONLY operation on this build that probes an
    `evidenceWrite` grant, because `writeProbe` is read from the destination
    object and only for a Backup or Restore plan."""
    def op(tag: str) -> dict[str, Any]:
        name = f"u6-bp-{u6_seq():03d}"
        apply(preflight(name, {
            "operation": "Backup",
            "backup": {"sourceRef": {"name": "source"},
                       "destinationRef": {"name": dest},
                       "topics": ["orders"]},
        }, timeout_seconds=180))
        obj = wait_preflight(name, timeout=600)
        artifact(f"u6/objects/{name}.json", obj)
        out = u6_check_outcome(obj, name, rows)
        out["variant"] = tag
        return out
    return op


def u6_backup_op(dest: str = "u6-dest"):
    def op(tag: str) -> dict[str, Any]:
        name = f"u6-bk-{u6_seq():03d}"
        apply(backup(name, source="source", topics=["orders"],
                     destination_ref=dest, deadline=420))
        obj = wait_backup(name, timeout=900)
        artifact(f"u6/objects/{name}.json", obj)
        status = obj.get("status", {})
        ok = status.get("phase") == "Succeeded" and status.get("exitCode") == 0
        tail = ""
        job = (status.get("jobRef") or {}).get("name")
        if job and not ok:
            try:
                tail = redact(pod_logs_for_job(job, tail=80))[-2500:]
            except Exception as exc:  # a pod already swept is not a measurement
                tail = f"[runner log unavailable: {type(exc).__name__}]"
        classified = {
            "phase": status.get("phase"), "exitCode": status.get("exitCode"),
            "reason": status.get("reason"), "exitReason": status.get("exitReason"),
            "message": (status.get("message") or "")[:400],
            "runnerLogTail": tail,
        }
        return {"ok": ok, "object": name, "kind": "Backup", "variant": tag,
                "backupId": status.get("backupId"),
                "evidence": status.get("evidence"),
                "classified": classified,
                "unclassified": (not ok) and status.get("exitCode") is None}
    return op


def u6_clear_markers() -> None:
    """Remove the create-only readiness markers a previous probe wrote.

    `put_marker` answers `MarkerAlreadyPresent` — a READY row — when the key is
    already there, on D2 §4.2's `[VERIFY U7]` premise that a backend authorises
    before it evaluates `If-None-Match`. `u6f` measures that premise. Whatever
    the answer, a bisection has to make each variant attempt a real create.
    """
    mc("rm", "--recursive", "--force", f"a/{U6_BUCKET}/logweir/readiness/",
       check=False, timeout=120)


def u6f() -> None:
    """The optional create-only write probe — D2 §3.11's fifth row — and the
    two facts this build makes about it.

    **The probe holds the ARCHIVE credential.** A check Job carries ONE
    credential for its destination (`preflight.rs`'s own comment), projected as
    the unprefixed `AWS_ACCESS_KEY_ID`, and `store::open_evidence_write` builds
    its handle from those same options with only the URL changed. So on a
    destination that SEPARATES `evidenceWrite`, `destination.evidenceWritable`
    is a statement about the archive principal's authority under `logweir/*`
    and not about the `evidenceWrite` grant at all.

    **And `MarkerAlreadyPresent` may not be a grant.** `put_marker` reads
    `AlreadyExists` as authorised, on D2 §4.2's `[VERIFY U7]` premise that a
    backend authorises before it evaluates `If-None-Match`. Measured here.
    """
    with Scenario("U6.writeProbe",
                  "the minimal grant under which the create-only readiness marker writes") as sc:
        row = u6_bisect(
            sc, role="write-probe", user="u6-writer",
            # `s3:PutObject` at `<bucket>/logweir/readiness/*`, not at
            # `<bucket>/logweir/*`: D2 §3.11 and `docs/install.md` both record
            # the narrower scope for this row and nothing had tested it, so the
            # narrower one is what the starting set carries. A baseline that
            # succeeds with it IS the proof that it suffices.
            starting=U6_STARTING_SETS["write-probe"],
            operation=u6_backup_preflight_op("u6-dest-probe",
                                             ["destination.evidenceWritable"]),
            before=u6_clear_markers,
            note="`writeProbe: CreateOnlyMarker` is what makes this row probed at all, and a "
                 "Backup readiness plan is the only request that reads it",
        )
        sc.detail["probeCredential"] = (
            "the check Job projects ONE credential — the destination's archive grant — as "
            "`AWS_ACCESS_KEY_ID`, with no `LOGWEIR_EVIDENCE_AWS_*`, so this row is about "
            "that principal's authority under the evidence root")
        # THE SECOND PROOF, READ AND NOT ASSERTED (reviewer finding **F4**: the
        # first landing declared a `job_env_before` nothing ever assigned, so
        # the artifact carried `null` under a report that said "confirmed
        # twice"). The check Job's own env is read back here, by name: which
        # Secret `AWS_ACCESS_KEY_ID` comes from, and whether any
        # `LOGWEIR_EVIDENCE_AWS_*` exists beside it. Values never appear — a
        # `secretKeyRef` is a name and a key.
        job = (get("preflight", row["baseline"]["object"])
               .get("status", {}).get("jobRef", {}) or {}).get("name")
        if job and get_opt("job", job) is not None:
            env = job_env(job)
            sc.detail["probeJobEnv"] = {
                "AWS_ACCESS_KEY_ID": env.get("AWS_ACCESS_KEY_ID"),
                "evidenceVariablesPresent": sorted(
                    k for k in env if k.startswith("LOGWEIR_EVIDENCE_AWS_")),
                "job": job,
            }
        else:
            sc.detail["probeJobEnv"] = (
                f"the check Job {job!r} is gone (its TTL expired), so this leg of the "
                "proof is the bisection alone")

    with Scenario("U6.markerPrecedence",
                  "whether `MarkerAlreadyPresent` is evidence of a write grant") as sc:
        probe = u6_backup_preflight_op("u6-dest-probe", ["destination.evidenceWritable"])
        u6_clear_markers()
        u6_attach("u6-writer", U6_WIDE_WRITE, "u7-write")
        first = probe("u7-write")
        sc.detail["withGrantOnAnEmptyRoot"] = first["classified"]
        check(first["ok"], "the marker was not written by a principal that may write it")
        # The marker now EXISTS. Take the write away and ask again.
        without = [u for u in U6_WIDE_WRITE if u != "s3:PutObject@evidence"]
        u6_attach("u6-writer", without, "u7-nowrite")
        second = probe("u7-nowrite")
        sc.detail["withoutGrantOnAPresentMarker"] = second["classified"]
        # With the marker gone the SAME policy must be refused, which is what
        # makes the answer above a statement about the KEY and not the policy.
        u6_clear_markers()
        third = probe("u7-nowrite-clean")
        sc.detail["withoutGrantOnAnEmptyRoot"] = third["classified"]
        check(not third["ok"],
              "a principal with no `s3:PutObject` under `logweir/*` wrote the marker anyway, "
              "so this scenario is measuring the wrong credential")
        sc.detail["u7Holds"] = not second["ok"]
        sc.detail["finding"] = (
            "U7 HOLDS on MinIO: an unauthorised principal is refused before the "
            "precondition, so `MarkerAlreadyPresent` really does prove the grant"
            if not second["ok"] else
            "U7 DOES NOT HOLD on MinIO: `If-None-Match` is evaluated BEFORE authorisation, "
            "so `MarkerAlreadyPresent` is a fact about the KEY and not about the grant, and "
            "`destination.evidenceWritable` answers `ready` on every check after the first "
            "for a principal that cannot write there")
        artifact("u6/marker-precedence.json", sc.detail)
        log("U6[U7]: " + sc.detail["finding"])


def u6a() -> None:
    """The two evidence-root roles, measured with the checks that probe them."""
    with Scenario("U6.evidenceRead",
                  "the minimal grant under which `destination.evidenceReadable` answers") as sc:
        u6_bisect(
            sc, role="evidence-read", user="u6-evreader",
            starting=U6_STARTING_SETS["evidence-read"],
            operation=u6_destination_access_op(
                "u6-dest", ["EvidenceRead"], ["destination.evidenceReadable"]),
            note="D2 §3.11 recorded `s3:GetObject`, with `s3:ListBucket` optional "
                 "— measured here as optional indeed",
        )
    with Scenario("U6.evidenceWrite",
                  "the minimal grant under which a run writes its signed receipt") as sc:
        # NOT the readiness probe: `u6f` measures that the marker probe holds
        # the ARCHIVE credential, so it cannot say anything about a separated
        # `evidenceWrite`. What exercises this grant is an EXECUTION — the
        # receipt a Backup writes under `logweir/` with
        # `LOGWEIR_EVIDENCE_AWS_*`. `u6-dest` separates all four principals.
        u6_attach("u6-writer", U6_WIDE_WRITE, "evw-archive")
        u6_bisect(
            sc, role="evidence-write", user="u6-evwriter",
            starting=U6_STARTING_SETS["evidence-write"],
            operation=u6_backup_op("u6-dest"),
            note="a destination-backed Backup writes its archive with `archiveWrite` and its "
                 "signed receipt with `evidenceWrite`; only the second is varied here",
        )




def u6_composite(*parts: Callable[[str], dict[str, Any]]):
    """One role, two product operations, one verdict.

    `archiveRead` is listed AND read on this build — the check framework's
    bounded list and the restore preflight's manifest read — and a minimal set
    measured against only one of them would be minimal for half the role.
    """
    def op(tag: str) -> dict[str, Any]:
        outcomes = [part(tag) for part in parts]
        ok = all(o["ok"] for o in outcomes)
        failing = [o for o in outcomes if not o["ok"]]
        return {
            "ok": ok, "variant": tag,
            "kind": "+".join(sorted({o["kind"] for o in outcomes})),
            "object": "+".join(o["object"] for o in outcomes),
            "classified": {o["object"]: o["classified"] for o in outcomes},
            "unclassified": bool(failing) and any(o["unclassified"] for o in failing),
        }
    return op


def u6b() -> None:
    """`archiveRead`: the bounded listing AND the manifest/segment reads."""
    with Scenario("U6.archiveRead",
                  "the minimal grant under which a destination's archive is listable "
                  "and its recovery point readable") as sc:
        check("u6BackupFacts" in state,
              "U6.archiveRead needs a recovery point to read; run `u6c` (or `u6point`) first")
        u6_bisect(
            sc, role="archive-read", user="u6-reader",
            starting=U6_STARTING_SETS["archive-read"],
            operation=u6_composite(
                u6_destination_access_op("u6-dest", ["ArchiveRead"],
                                         ["destination.archiveListable"]),
                u6_restore_preflight_op(["archive.backupSet", "archive.segments"]),
            ),
            note="D2 §3.11 recorded `s3:ListBucket` (prefix-conditioned) plus `s3:GetObject`",
        )


U6_WIDE_WRITE = ["s3:ListBucket@bucket:both", "s3:GetBucketLocation@bucket",
                 "s3:GetObject@archive", "s3:PutObject@archive",
                 "s3:AbortMultipartUpload@archive",
                 "s3:GetObject@evidence", "s3:PutObject@evidence",
                 "s3:AbortMultipartUpload@evidence"]


def u6point() -> None:
    """One recovery point, written with a grant wide enough that WRITING it is
    not the thing under measurement. Every later phase reads this point."""
    with Scenario(f"U6.point{len(state.get('u6Points') or []) + 1}",
                  "a recovery point to measure the read roles against") as sc:
        u6_attach("u6-writer", U6_WIDE_WRITE, "point")
        out = u6_backup_op("u6-dest-write")("point")
        check(out["ok"], f"the seed backup did not succeed: {json.dumps(out['classified'])[:600]}")
        facts = u6_backup_manifest_facts(out["backupId"])
        state["u6BackupFacts"] = facts
        state.setdefault("u6Points", []).append(out["backupId"])
        save()
        sc.detail["backup"] = {"object": out["object"], "backupId": out["backupId"],
                               "evidence": out["evidence"]}
        sc.detail["facts"] = {k: v for k, v in facts.items() if k != "segmentKeys"}
        sc.detail["segmentCount"] = len(facts["segmentKeys"])
        artifact("u6/point.json", sc.detail)


def u6c() -> None:
    """`archiveWrite`: a real `Backup`, which is the only thing that writes an
    archive. D2 §14.3's recorded policy is the starting set VERBATIM, including
    the `s3:DeleteObject` it carried and §3.11's table did not — so the
    discrepancy between the two is settled by measurement."""
    with Scenario("U6.archiveWrite",
                  "the minimal grant under which a destination-backed Backup succeeds") as sc:
        u6_bisect(
            sc, role="archive-write", user="u6-writer",
            # The two `s3:ListBucket` prefix legs are SEPARATE units. Withdrawn
            # together they show only that one of them was needed, and the
            # minimal set then asserts the other (reviewer finding **F1**).
            starting=U6_STARTING_SETS["archive-write"],
            operation=u6_backup_op("u6-dest-write"),
            note="the run writes the archive through the engine AND its own signed "
                 "receipt under `logweir/`, with ONE grant",
        )


def u6_relayed_checks(log_text: str) -> list[dict[str, Any]]:
    """The check rows a check Job relayed, decoded from its own stdout.

    A check Job frames its result as `logweir-check-part=result:<i>/<n>:<b64>`
    lines, and the `RecoveryCatalog` controller publishes a VERDICT from them
    without republishing the rows. So the sentence that names the refusal —
    `destination.archiveListable notReady AccessDenied` — exists only in the
    Job's output, and a permission measurement that never decodes it is
    reading the controller's summary instead of the store's answer.

    Returns `[]` for anything that is not a complete, decodable `result`
    stream: a partial frame set is not a verdict.
    """
    parts: dict[int, str] = {}
    total: int | None = None
    for line in log_text.splitlines():
        match = re.match(r"logweir-check-part=(\w+):(\d+)/(\d+):(.*)$", line.strip())
        if not match or match.group(1) != "result":
            continue
        parts[int(match.group(2))] = match.group(4)
        total = int(match.group(3))
    if not parts or total is None or sorted(parts) != list(range(1, total + 1)):
        return []
    try:
        body = json.loads(base64.b64decode("".join(parts[k] for k in sorted(parts))))
    except Exception:
        return []
    return [
        {"id": c.get("id"), "state": c.get("state"), "code": c.get("code"),
         "message": (c.get("message") or "")[:300]}
        for c in body.get("checks", []) or []
    ]


#: The two condition types whose `False` is a SYNC VERDICT. `Stale=False`
#: (`ViewFresh`) and `TrustAvailable=False` are not: the first is good news and
#: the second is about the roster. A predicate that read any `False` condition
#: as "the sync has answered" returned nine seconds after the Job was created,
#: with no view and no failure, and the row was about nothing.
U6_CATALOG_VERDICT_TYPES = ("Synced", "Ready")


def u6_catalog_settled(status: dict[str, Any], since: str, token: str) -> bool:
    """Has THIS sync finished — with a view, or with a verdict that it failed?"""
    if status.get("observedSyncRequest") != token:
        return False
    if status.get("pages") and (status.get("syncedAt") or "") >= since:
        return True
    return any(
        c.get("type") in U6_CATALOG_VERDICT_TYPES
        and c.get("status") == "False"
        and (c.get("lastTransitionTime") or "") >= since
        for c in status.get("conditions", []) or []
    )


def u6_catalog_op(dest: str = "u6-dest-cat"):
    """One `RecoveryCatalog` sync per variant — a FRESH object every time,
    because a `syncRequest` bump on this build starts a Job whose result is
    never harvested (`catalog-resync-is-not-harvested`, D3 W14)."""
    def op(tag: str) -> dict[str, Any]:
        name = f"u6-cat-{u6_seq():03d}"
        since = now()
        apply({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
            "metadata": owned(name),
            "spec": {"destinationRef": {"name": dest},
                     "sync": {"intervalSeconds": 300, "mode": "Index",
                              "deepCheck": "ManifestDigest"},
                     "syncRequest": "u6"},
        })

        def settled(o: dict[str, Any]) -> bool:
            return u6_catalog_settled(o.get("status") or {}, since, "u6")

        try:
            obj = wait_for("recoverycatalog", name, settled, timeout=540,
                           what="a published view or a settled sync failure")
            timed_out = False
        except TimeoutError:
            obj = get("recoverycatalog", name)
            timed_out = True
        artifact(f"u6/objects/{name}.json", obj)
        status = obj.get("status") or {}
        counts = status.get("counts") or {}
        ok = bool(status.get("pages")) and (counts.get("available") or 0) >= 1
        conditions = [{"type": c.get("type"), "status": c.get("status"),
                       "reason": c.get("reason"), "message": (c.get("message") or "")[:300]}
                      for c in status.get("conditions", []) or []]
        # THE OBJECT DOES NOT SAY `AccessDenied` HERE, AND THAT IS DELIBERATE.
        # A walk that could not read its points publishes `Synced=False` with
        # `PartialScan` — "a permission or transport failure, which is NOT the
        # same as absent" — and a walk whose first listing was refused relays
        # no body at all and lands `ResultUnreadable`. Both are the product
        # classifying the failure; neither names the refusal. The refusal is in
        # the sync Job's own output, which is where the store answered, so the
        # Job's log is read into the record and is what makes this a permission
        # measurement rather than a report that something went wrong.
        job = (status.get("lastSyncJob") or {}).get("name")
        job_log = ""
        if job and not ok:
            try:
                job_log = redact(pod_logs_for_job(job, tail=60))[-2000:]
            except Exception as exc:
                job_log = f"[sync Job log unavailable: {type(exc).__name__}]"
        relayed = u6_relayed_checks(job_log)
        refused = [c for c in relayed
                   if c["state"] == "notReady" and c["code"] in U6_DENIAL_CODES]
        verdict = [c for c in conditions
                   if c["type"] == "Synced" and c["status"] == "False"
                   and c["reason"] in U6_CATALOG_REFUSAL_REASONS]
        denied_by = ""
        if refused:
            denied_by = "the sync Job relayed `%s` %s" % (refused[0]["id"], refused[0]["code"])
        elif verdict:
            denied_by = "`Synced=False/%s`, with %s of %s points unreadable" % (
                verdict[0]["reason"], counts.get("unreadable"), counts.get("total"))
        classified = {"counts": counts, "pages": len(status.get("pages") or []),
                      "conditions": conditions, "timedOut": timed_out,
                      "syncJob": job, "relayedChecks": relayed,
                      "deniedBy": denied_by if not ok else "",
                      "syncJobLogTail": job_log[-600:]}
        return {"ok": ok, "object": name, "kind": "RecoveryCatalog", "variant": tag,
                "classified": classified,
                "unclassified": (not ok) and not denied_by}
    return op


def u6d() -> None:
    """The `catalogSync` reader. It uses the destination's `archiveRead` grant
    (`check/kinds/catalog_sync.rs` opens `DestinationRole::ArchiveRead`) but it
    reads the catalog log, the receipts and the sidecars under `logweir/` as
    well as the manifests under the archive prefix — so its minimal set is a
    claim about a DIFFERENT set of keys than `archiveRead`'s own."""
    with Scenario("U6.catalogSync",
                  "the minimal grant under which a RecoveryCatalog publishes a view") as sc:
        u6_bisect(
            sc, role="catalog-sync", user="u6-catreader",
            starting=U6_STARTING_SETS["catalog-sync"],
            operation=u6_catalog_op(),
            note="the reader walks `logweir/catalog/v1/log/`, then reads each point's "
                 "receipt, sidecar and manifest",
        )


# --- the retention enforcer ------------------------------------------------
#
# The enforcer is the one role whose operation DESTROYS what it measures, so a
# bisection has to put the objects back between variants. The archive prefix is
# snapshotted to a sibling prefix before the first run and copied back before
# each variant, and every variant re-runs the CONTROLLER'S OWN Job — the same
# image, command, argv, plan `ConfigMap` and projected credentials the
# controller rendered — under a fresh `LOGWEIR_RETENTION_RUN_ID`, because the
# tombstones it writes are create-only.

U6_SNAPSHOT_PREFIX = "u6-snapshot"


def u6_snapshot_archive() -> None:
    """`mirror`, not `cp --recursive`: mirror's contract is "make the
    destination look like the source", with no argument about whether the
    source's last path segment is appended."""
    mc("rm", "--recursive", "--force", f"a/{U6_BUCKET}/{U6_SNAPSHOT_PREFIX}/", check=False)
    mc("mirror", "--overwrite", "--quiet", f"a/{U6_BUCKET}/{U6_ARCHIVE_PREFIX}/",
       f"a/{U6_BUCKET}/{U6_SNAPSHOT_PREFIX}/", timeout=300)
    listed = mc("ls", "--recursive", f"a/{U6_BUCKET}/{U6_SNAPSHOT_PREFIX}/").stdout
    if not listed.strip():
        raise RuntimeError("the archive snapshot is empty, so no variant could be re-run")


def u6_restore_archive() -> None:
    mc("mirror", "--overwrite", "--quiet", f"a/{U6_BUCKET}/{U6_SNAPSHOT_PREFIX}/",
       f"a/{U6_BUCKET}/{U6_ARCHIVE_PREFIX}/", timeout=300)


def u6_retention_job_op():
    """Re-run the controller's own enforcement Job, one variant at a time."""
    def op(tag: str) -> dict[str, Any]:
        template = json.loads(json.dumps(state["u6RetentionJob"]))
        name = f"u6-ret-{u6_seq():03d}"
        run_id = f"{name}-{secrets.token_hex(4)}"
        u6_restore_archive()
        spec = template["spec"]
        spec.pop("selector", None)
        spec["template"]["metadata"].pop("labels", None)
        spec["template"]["metadata"].pop("creationTimestamp", None)
        spec.pop("completionMode", None)
        spec.pop("suspend", None)
        for env in spec["template"]["spec"]["containers"][0].get("env", []):
            if env.get("name") == "LOGWEIR_RETENTION_RUN_ID":
                env["value"] = run_id
        job = {"apiVersion": "batch/v1", "kind": "Job",
               "metadata": owned(name), "spec": spec}
        apply(job)
        finished = wait_for("job", name, u6_job_is_terminal,
                            timeout=420, what="a terminal Job")
        logs = redact(pod_logs_for_job(name, tail=120))[-3000:]
        artifact(f"u6/objects/{name}.json", finished)
        artifact(f"u6/objects/{name}.log", logs)
        status = finished.get("status") or {}
        ok = bool(status.get("succeeded")) and "retention-point=" in logs
        deleted = logs.count("state=Deleted")
        classified = {"jobSucceeded": bool(status.get("succeeded")),
                      "jobFailed": status.get("failed"),
                      "pointsDeleted": deleted,
                      "pointCodes": re.findall(r"retention-point=\S+ state=\S+ objects=\d+ code=(\S+)",
                                               logs),
                      "runnerLogTail": logs[-2000:]}
        return {"ok": ok and deleted >= 1, "object": name, "kind": "Job", "variant": tag,
                "classified": classified,
                "unclassified": (not (ok and deleted >= 1)) and not u6_denied(classified)}
    return op


def u6_job_is_terminal(job: dict[str, Any]) -> bool:
    """Has this Job finished, one way or the other?

    `status.succeeded`/`status.failed` count PODS, so a Job the kubelet refuses
    to give a pod at all — a missing ServiceAccount, for one — sits at zero of
    both forever while the API server records `FailedCreate` every few minutes.
    The `Complete` and `Failed` conditions are what say the Job is over.
    """
    status = job.get("status") or {}
    if status.get("succeeded") or status.get("failed"):
        return True
    return any(
        c.get("type") in ("Complete", "Failed") and c.get("status") == "True"
        for c in status.get("conditions", []) or []
    )


def u6_retention_policy(name: str, *, catalog: str, generation_note: str = "") -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RetentionPolicy",
        "metadata": owned(name),
        "spec": {
            "destinationRef": {"name": "u6-dest"},
            "catalogRef": {"name": catalog},
            "scope": {"prefix": U6_ARCHIVE_PREFIX},
            "rules": {"keepLast": 1, "minUsablePoints": 1},
            "mode": "Enforce",
            "enforcement": {
                "credentialSecretRef": {"name": "u6-deleter"},
                "schedule": "* * * * *",
                "requireApprovedPlan": False,
                "deadlineSeconds": 240,
                "maxDeletionsPerRun": 1,
                "maxObjectsPerRun": 500,
            },
        },
    }


def u6e() -> None:
    """The retention enforcer's delete grant.

    The baseline is the CONTROLLER's own `Enforce` run, so the role's minimal
    set is anchored to a `RetentionPolicy` that actually enforced. The per-unit
    rows then re-run that same Job — the controller's image, command, argv,
    plan `ConfigMap` and projected credentials, with only the MinIO policy and
    the run id changed — because the operation destroys the objects it needs
    and a fresh controller run per unit costs eight minutes each.
    """
    with Scenario("U6.retentionEnforcer",
                  "the minimal delete grant under which an Enforce run removes a point") as sc:
        starting = U6_STARTING_SETS["retention-enforcer"]
        # D3 §6.5 and `docs/kubernetes.md` §7f both say the record and the
        # tombstones are written with the destination's own `evidenceWrite`
        # grant, and since the fix for **RET-EVIDENCE-GRANT-IS-ARCHIVEREAD**
        # they are. This phase used to pin `s3:PutObject@evidence` onto
        # `u6-reader` as well, because the build projected the `archiveRead`
        # grant onto `LOGWEIR_EVIDENCE_AWS_*` and the run could not write its
        # tombstone otherwise. THAT PIN IS GONE, and its removal is what gives
        # this phase a failure mode: `u6-reader` can no longer write under
        # `logweir/*`, so a build that went back to projecting `archiveRead`
        # would have its first tombstone refused `403` and every row here would
        # fail rather than quietly pass on a workaround.
        u6_attach("u6-evwriter", ["s3:PutObject@evidence", "s3:GetObject@evidence"],
                  "ret-evidence")
        u6_attach("u6-deleter", starting, "ret-baseline")
        # The enforcer reads its candidates from a catalog VIEW, and the view
        # is walked with the destination's `archiveRead` grant — which `u6b`
        # has just reduced to its own minimal set, and which `u6d` measured to
        # be too narrow for a catalog walk (the walk reads the record log and
        # the receipts under `logweir/` as well as the manifests). Pin it to
        # the catalog reader's measured minimum, or the enforcer has no plan
        # for reasons that have nothing to do with the delete grant.
        # The catalog reader's measured minimum and NOTHING MORE. No
        # `s3:PutObject@evidence`: the record is not this principal's to write.
        u6_attach("u6-reader", ["s3:ListBucket@bucket:both", "s3:GetObject@archive",
                                "s3:GetObject@evidence"],
                  "ret-view")
        catalog = f"u6-retcat-{u6_seq():03d}"
        since = now()
        apply({"apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
               "metadata": owned(catalog),
               "spec": {"destinationRef": {"name": "u6-dest"},
                        "sync": {"intervalSeconds": 300, "mode": "Index",
                                 "deepCheck": "ManifestDigest"},
                        "syncRequest": "u6"}})
        view = wait_for("recoverycatalog", catalog,
                        lambda o: bool((o.get("status") or {}).get("pages"))
                        and ((o.get("status") or {}).get("syncedAt") or "") >= since,
                        timeout=540, what="a published view for the enforcer")
        counts = view["status"].get("counts") or {}
        sc.detail["catalog"] = {"name": catalog, "counts": counts}
        # `keepLast: 1` needs at least two usable points for one to be a
        # candidate, and the view is the only thing that knows how many there
        # are: every successful Backup of every earlier U6 phase left one.
        check((counts.get("available") or 0) >= 2,
              f"the enforcer needs two usable points to have a candidate; the view has "
              f"{counts.get('available')} of {counts.get('total')}")
        u6_snapshot_archive()

        # TWO POLICIES FOR ONE DESTINATION IS A REFUSAL, not a race: the second
        # lands `Ready=False/Conflict` — "neither evaluates and neither
        # enforces until one is removed" — so a re-run of this phase must take
        # the previous attempt's policy with it or measure nothing.
        for old_policy in get_list("retentionpolicies"):
            name_ = old_policy["metadata"]["name"]
            if name_.startswith("u6-ret-policy-"):
                run(K + ["delete", "retentionpolicy", name_, "--wait=true"],
                    check=False, timeout=120)
                log(f"U6[retention]: removed the previous attempt's policy {name_}")
        policy_name = f"u6-ret-policy-{u6_seq():03d}"
        created = apply(u6_retention_policy(policy_name, catalog=catalog))
        policy_uid = created["metadata"]["uid"]
        window = int(os.environ.get("U6_RETENTION_WINDOW", "1500"))
        deadline = time.monotonic() + window
        job_obj: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            jobs = [j for j in get_list("jobs")
                    if any(o.get("uid") == policy_uid
                           for o in (j["metadata"].get("ownerReferences") or []))]
            terminal = [j for j in jobs if u6_job_is_terminal(j)]
            if terminal:
                job_obj = terminal[0]
                break
            time.sleep(10)
        policy = get("retentionpolicy", policy_name)
        artifact(f"u6/objects/{policy_name}.json", policy)
        if job_obj is None:
            events = run(K + ["get", "events", "--field-selector",
                              "reason=FailedCreate", "--sort-by=.lastTimestamp"],
                         check=False, timeout=60).stdout[-800:]
            sc.detail["failedCreateEvents"] = redact(events)
        check(job_obj is not None,
              f"no enforcement Job for {policy_name} within {window}s; "
              f"status: {redact(json.dumps(policy.get('status'), sort_keys=True))[:600]}")
        assert job_obj is not None
        job_name = job_obj["metadata"]["name"]
        logs = redact(pod_logs_for_job(job_name, tail=120))[-3000:]
        artifact(f"u6/objects/{job_name}.json", job_obj)
        artifact(f"u6/objects/{job_name}.log", logs)
        # THE CONTRACT, ASSERTED — not the defect, recorded. `u6-reader` no
        # longer holds `s3:PutObject` under `logweir/*`, so a build that
        # projected `archiveRead` here would also fail the delete rows below;
        # this row names the principal so the failure says WHY.
        rendered = job_env(job_name)
        evidence_ref = ((rendered.get("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID") or {})
                        .get("secretKeyRef") or {})
        delete_ref = ((rendered.get("AWS_ACCESS_KEY_ID") or {})
                      .get("secretKeyRef") or {})
        sc.detail["evidenceCredential"] = {
            "id": "RET-EVIDENCE-GRANT-IS-ARCHIVEREAD",
            "expected": "spec.access.evidenceWrite (D3 §6.5, docs/kubernetes.md §7f)",
            "recordSecret": evidence_ref.get("name"),
            "deleteSecret": delete_ref.get("name"),
            "observed": redact(json.dumps(rendered, sort_keys=True))[:900],
        }
        check(evidence_ref.get("name") == "u6-evwriter",
              "the enforcement Job's LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID must name the "
              "evidenceWrite Secret u6-evwriter and never the archiveRead Secret u6-reader "
              f"(RET-EVIDENCE-GRANT-IS-ARCHIVEREAD): {json.dumps(sc.detail['evidenceCredential']['recordSecret'])}")
        check(delete_ref.get("name") == "u6-deleter",
              "and AWS_ACCESS_KEY_ID must name spec.enforcement.credentialSecretRef's "
              f"Secret u6-deleter: {json.dumps(sc.detail['evidenceCredential']['deleteSecret'])}")
        sc.detail["controllerBaseline"] = {
            "policy": policy_name,
            "job": job_name,
            "jobSucceeded": bool((job_obj.get("status") or {}).get("succeeded")),
            "pointsDeleted": logs.count("state=Deleted"),
            "lastEnforcement": (policy.get("status") or {}).get("lastEnforcement"),
            "conditions": [{"type": c.get("type"), "status": c.get("status"),
                            "reason": c.get("reason")}
                           for c in (policy.get("status") or {}).get("conditions", [])],
        }
        check(bool((job_obj.get("status") or {}).get("succeeded")) and "state=Deleted" in logs,
              "the controller's own Enforce run did not delete a point with the recorded "
              f"grant: {json.dumps(sc.detail['controllerBaseline'])[:600]}")
        # The Job spec the controller rendered IS the fixture every unit row runs.
        state["u6RetentionJob"] = {"spec": job_obj["spec"]}
        save()
        artifact("u6/retention-job-template.json",
                 json.loads(redact(json.dumps(job_obj["spec"], sort_keys=True))))
        u6_bisect(
            sc, role="retention-enforcer", user="u6-deleter",
            starting=starting, operation=u6_retention_job_op(),
            note="D3 §6.5 and `docs/kubernetes.md` §7f record `s3:ListBucket` with a prefix "
                 "condition plus `s3:GetObject`/`s3:DeleteObject` on `<prefix>/*`",
        )




U6_UNIT_PROSE = {
    "s3:ListBucket@bucket": "`s3:ListBucket` on `arn:aws:s3:::<bucket>`, unconditioned",
    "s3:ListBucket@bucket:archive":
        "`s3:ListBucket` on `arn:aws:s3:::<bucket>` (`s3:prefix` in `<prefix>/*`)",
    "s3:ListBucket@bucket:evidence":
        "`s3:ListBucket` on `arn:aws:s3:::<bucket>` (`s3:prefix` in `logweir/*`)",
    "s3:ListBucket@bucket:both":
        "`s3:ListBucket` on `arn:aws:s3:::<bucket>` (`s3:prefix` in `<prefix>/*`, `logweir/*`)",
    "s3:GetBucketLocation@bucket": "`s3:GetBucketLocation` on `arn:aws:s3:::<bucket>`",
    "s3:GetObject@archive": "`s3:GetObject` on `<bucket>/<prefix>/*`",
    "s3:PutObject@archive": "`s3:PutObject` on `<bucket>/<prefix>/*`",
    "s3:AbortMultipartUpload@archive": "`s3:AbortMultipartUpload` on `<bucket>/<prefix>/*`",
    "s3:DeleteObject@archive": "`s3:DeleteObject` on `<bucket>/<prefix>/*`",
    "s3:GetObject@evidence": "`s3:GetObject` on `<bucket>/logweir/*`",
    "s3:PutObject@evidence": "`s3:PutObject` on `<bucket>/logweir/*`",
    "s3:AbortMultipartUpload@evidence": "`s3:AbortMultipartUpload` on `<bucket>/logweir/*`",
}


def u6_unit_prose(unit: str) -> str:
    return U6_UNIT_PROSE.get(unit, f"`{unit}`")


def u6table() -> None:
    """Render what U6 measured: the per-role minimal set, and one bisection row
    per unit of every starting set, naming the object that proves it."""
    table = state.get("u6Table") or {}
    if not table:
        raise RuntimeError("nothing measured yet; run the u6 phases first")
    lines = [
        "# D2 §15 U6 — the measured per-role minimal object-storage permission set",
        "",
        f"Cluster `{CONTEXT}`, namespace `{NS}`, owner `{OWNER}`, run `{STAMP}`.",
        "",
        "## The table",
        "",
        "| Role | Minimal S3 actions, with resource scope | Measured on | Harness row |",
        "|---|---|---|---|",
    ]
    for role, row in sorted(table.items()):
        minimal = "; ".join(u6_unit_prose(u) for u in row["minimalSet"]) or "(none)"
        proved_by = row.get("minimalConfirmed") or row["baseline"]
        lines.append(
            f"| {U6_ROLES.get(role, role)} | {minimal} | {row['baseline']['kind']} "
            f"`{proved_by['object']}` | `U6/{role}` |"
        )
    lines += ["", "## Bisection rows — removing one unit at a time", "",
              "| Role | Unit removed | Operation | Verdict | The product's own answer |",
              "|---|---|---|---|---|"]
    for role, row in sorted(table.items()):
        for removal in row["removals"]:
            verdict = "still succeeds — **not required**" if removal["ok"] else "**FAILS**"
            answer = json.dumps(removal["classified"], sort_keys=True)
            answer = re.sub(r"\s+", " ", answer)[:260].replace("|", "\\|")
            lines.append(
                f"| {U6_ROLES.get(role, role)} | {u6_unit_prose(removal['unit'])} | "
                f"{removal['kind']} `{removal['object']}` | {verdict} | `{answer}` |")
    lines += ["", "## Units the recorded starting sets carried and the product does not need",
              ""]
    for role, row in sorted(table.items()):
        if row["notRequired"]:
            lines.append(f"- {U6_ROLES.get(role, role)}: "
                         + ", ".join(u6_unit_prose(u) for u in row["notRequired"]))
    lines += ["", "## Proof clauses, per role", ""]
    for role, row in sorted(table.items()):
        failed = [k for k, v in row["proof"].items() if not v]
        lines.append(f"- {U6_ROLES.get(role, role)}: "
                     + ("every clause of `u6_row_is_proved` holds"
                        if not failed else "NOT PROVED: " + "; ".join(failed)))
    body = "\n".join(lines) + "\n"
    artifact("u6/TABLE.md", body)
    print(body)


def phase_table() -> dict[str, Callable[[], None]]:
    """Every scenario is a module-level function named exactly as its phase, so
    the list of runnable phases is the list of things this file defines and
    cannot drift from it."""
    table: dict[str, Callable[[], None]] = {
        "setup": setup,
        "cleanup": cleanup,
        "report": report,
        "revision": revision_guard,
        "sweep": lambda: print(json.dumps(sweep(), indent=2)),
    }
    for name, value in sorted(globals().items()):
        if re.fullmatch(
            r"(s\d+[a-z]*|e\d+|u\d+[a-z]*|evf\d*|bulk|negative_control|ui|api_probe)", name
        ) and callable(value):
            table[name] = value
    return table


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phases", nargs="+")
    args = parser.parse_args()
    # EVERY phase, not just `setup`: `redact()` and every credential assertion
    # walk SECRETS, and an empty SECRETS would make both a silent no-op — a
    # redaction that redacts nothing and a leak check that checks nothing.
    load_secrets()
    if not SECRETS:
        raise RuntimeError("no credential values were loaded; redaction would be a no-op")
    known = phase_table()
    for phase in args.phases:
        if phase not in known:
            print(f"unknown phase {phase!r}; known: {sorted(known)}", file=sys.stderr)
            return 2
    for phase in args.phases:
        log(f"==== phase {phase}")
        known[phase]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
