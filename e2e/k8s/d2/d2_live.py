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
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py report
    $LOGWEIR_PYTHON e2e/k8s/d2/d2_live.py cleanup

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
OWNER = "d2w14"
OWNER_LABEL_KEY = "logweir.dev/test-owner"
NAMESPACE_PREFIX = "lw-d2w14-"
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
        "a-denied": "X" + secrets.token_urlsafe(20),
        "b-writer": "B" + secrets.token_urlsafe(20),
        "b-evidence-ro": "N" + secrets.token_urlsafe(20),
        "c-writer": "C" + secrets.token_urlsafe(20),
        "c-reader": "F" + secrets.token_urlsafe(20),
        "tls-writer": "T" + secrets.token_urlsafe(20),
        "tls-reader": "U" + secrets.token_urlsafe(20),
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
            {"name": "MINIO_ROOT_USER", "value": "d2w14root"},
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
        "  mc alias set %s %s d2w14root \"$ROOT\" >/dev/null 2>&1 && break; sleep 2; "
        "done; mc alias set %s %s d2w14root \"$ROOT\" >/dev/null"
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
    return run(K + ["exec", "mc", "--", "mc", *args], check=check, timeout=timeout)


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
    proof: list[str] = ["# d2w14 cleanup proof", ""]
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
    dirty = [ln[3:].strip() for ln in
             run(["git", "status", "--porcelain"], timeout=120).stdout.splitlines()
             if ln.strip()]
    product = sorted({p for p in changed + dirty if not p.startswith(NON_IMAGE_PATHS)})
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
                           "changedSinceImage": changed, "uncommitted": dirty},
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


def approver_material() -> dict[str, pathlib.Path]:
    """The lab roster's own approver and signing key pair.

    The cluster-scoped `TrustRoster/default` is SHARED with two other live
    waves, so this harness never edits it. It signs with the key the roster
    already carries, whose private half `scripts/test-k8s-scram.py` wrote when
    the lab was built."""
    base = pathlib.Path(os.environ.get("LOGWEIR_SCRAM_OUT", "/tmp/logweir-scram-e2e"))
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
    """Each clause S1.statusVerification rests on, judged on its own.

    `NotAttempted` exists as a verdict distinct from `Invalid` precisely so an
    operator can tell "we did not check" from "it did not verify"; a block that
    is absent altogether says neither, which is the defect. The premise is a
    clause too: the verdict is only predictable while the installation policy
    allows the controller's own identity nowhere.
    """
    block = verification if isinstance(verification, dict) else {}
    return {
        "a verification block exists": verification is not None,
        "the verdict is one of the three published":
            block.get("result") in {"Valid", "Invalid", "NotAttempted"},
        "no controller identity location is allowed": not identity_locations,
        "the verdict is NotAttempted": block.get("result") == "NotAttempted",
        "NotAttempted says why": bool((block.get("detail") or "").strip()),
        "nothing was verified, so no key matched": block.get("matchedKeyId") is None,
    }


def s1b() -> None:
    """The half of S1 the build could not reach, now that it reaches it.

    D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN was that NO verification block was
    written at all for a destination whose `evidenceRead` is `SecretKeys`: the
    controller answered `EvidenceSource::NotAttempted`, but with
    `receipt_sha256` absent the second status patch was skipped, so an operator
    saw no verification field rather than an explicit `NotAttempted` with its
    sentence. Silence and "we did not check" look identical to a console, which
    is the whole reason `NotAttempted` exists as a verdict distinct from
    `Invalid`.

    This row hard-coded `notRun` and recorded the measurement beside it, so a
    closed defect could not move its verdict — the block became real and the
    row still said `notRun` (lab-refresh-3 §8.4). It asserts now.
    """
    with Scenario("S1.statusVerification",
                  "status.evidence.verification on a destination-backed Backup") as sc:
        obj = get_opt("backup", "bk-a")
        check(obj is not None, "Backup/bk-a is absent; run S1 first")
        verification = (obj.get("status", {}).get("evidence", {}) or {}).get("verification")
        policy = get_opt("configmap", "weirkeeper-policy", LAB_NS)
        identity_locations = None
        if policy and "policy.json" in policy.get("data", {}):
            identity_locations = json.loads(policy["data"]["policy.json"]).get(
                "evidence", {}).get("controllerIdentityLocations")
        sc.detail["observedVerificationField"] = verification
        sc.detail["policyControllerIdentityLocations"] = identity_locations
        artifact("objects/s1/status-verification.json",
                 {"verification": verification,
                  "policyControllerIdentityLocations": identity_locations,
                  "destinationEvidenceRead": "SecretKeys"})
        # WHY `NotAttempted` IS THE RIGHT ANSWER HERE, and not a failure to
        # verify: dest-a's `evidenceRead` is `SecretKeys`, a grant only a pod may
        # hold, and the only other route — `ControllerIdentity` — needs an entry
        # in the shared installation policy's
        # `evidence.controllerIdentityLocations`, which the lab deliberately
        # leaves empty. The premise is a clause of its own, so a policy that
        # started listing one fails this row instead of quietly changing what it
        # means.
        criteria = not_attempted_is_honest(verification, identity_locations)
        sc.detail["criteria"] = criteria
        sc.detail["notAttemptedDetail"] = (verification or {}).get("detail")
        failed = sorted(name for name, ok in criteria.items() if not ok)
        check(not failed,
              "S1.statusVerification: " + "; ".join(failed)
              + f". Observed {json.dumps(verification)} with "
              f"controllerIdentityLocations={identity_locations!r}. An absent block is the "
              "silence D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN was about: the operator cannot "
              "tell 'we did not check' from 'nobody wrote anything'")
        # The receipt itself is verified from its stored bytes in S1; this row
        # is only about what the STATUS says.


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


def restore_plan(backup_id: str, point_in_time: str, prefix: str) -> dict[str, Any]:
    return {
        "source": {
            "storage": {
                "backend": "s3", "bucket": "lw-a", "prefix": ARCHIVE_PREFIX,
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
        listing = run(
            [sys.executable, str(helper), "--port", str(forward.local), "--list"],
            timeout=300,
        )
    rows = [line.split("\t") for line in listing.stdout.splitlines() if line.strip()]
    names = sorted(row[0] for row in rows)
    user_topics = sorted(row[0] for row in rows if len(row) > 1 and row[1] == "user")
    artifact("fixtures/broker-topics-all.tsv", listing.stdout)
    artifact("fixtures/broker-topics-user.txt", "\n".join(user_topics))
    state["brokerTopics"] = {"all": len(names), "user": len(user_topics)}
    state["userTopicsSha256"] = digest("\n".join(user_topics))
    save()
    log(f"bulk: {len(user_topics)} user topics, {len(names) - len(user_topics)} internal")


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


def s18() -> None:
    record(
        "S18", "an expired approver key makes an approval expired", "notRun",
        reason=(
            "D2 §14.4 S18 asks for a roster approver key whose `notAfter` is two "
            "minutes ahead. `TrustRoster` and `TrustPolicy` are CLUSTER-SCOPED "
            "(verified: `kubectl get crd -o custom-columns=…SCOPE`), there is exactly "
            "one `TrustRoster/default` in this cluster, and lab-refresh-2 §8 already "
            "showed that changing the resolved trust policy re-derives the verdict of "
            "every pre-existing object. Two other live waves were reconciling against "
            "that roster throughout this run, so this harness signs with the key the "
            "roster ALREADY carries and never edits it. `logweir drill approve` has no "
            "expiry flag (`--help` lists spec/key/approver/ticket/out/subject-kind "
            "only), so an approval that expires cannot be minted without the roster "
            "edit either."
        ),
    )


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
# judged on their own rather than through the prefix defect E4 isolates.
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
    with Scenario("E4", "a restore preflight reads the manifest without the destination's "
                        "storage.prefix") as sc:
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
        entry = flat_checks.get("archive.backupSet")
        check(entry is not None, f"archive.backupSet did not run: {sorted(flat_checks)}")
        check(entry["state"] == "ready" and entry["code"] == "ManifestReadable",
              "even with an EMPTY storage.prefix the restore preflight cannot read the "
              f"manifest: {entry['state']}/{entry['code']} — {entry.get('message')}")
        check(sc.detail.get("prefixedManifestKey", "").count("/") == 1,
              "the prefixed destination's check plan names a manifestKey that already "
              f"carries a prefix: {sc.detail.get('prefixedManifestKey')!r}")



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
        if re.fullmatch(r"(s\d+[a-z]*|e\d+|bulk|negative_control|ui|api_probe)", name) and callable(value):
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
