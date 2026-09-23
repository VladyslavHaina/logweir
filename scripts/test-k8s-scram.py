#!/usr/bin/env python3
"""Authenticated Kubernetes end-to-end checks, exclusively on docker-desktop.

Build current local images first (native controller architecture):
  docker build --load -f Dockerfile.weirkeeper -t weirkeeper:scram-local .
  docker build --platform linux/amd64 --load -t logweir:scram-local .
  cargo build -p logweir

Run phases in order with the same LOGWEIR_SCRAM_OUT directory:
  python3 scripts/test-k8s-scram.py setup
  python3 scripts/test-k8s-scram.py install
  python3 scripts/test-k8s-scram.py positives
  python3 scripts/test-k8s-scram.py negatives
  python3 scripts/test-k8s-scram.py rotation
  python3 scripts/test-k8s-scram.py restore
  python3 scripts/test-k8s-scram.py report

Requires Docker Desktop Kubernetes, kubectl, Helm, openssl, cargo-built CLI,
and a Python interpreter with cryptography for the independent verifier.
Set LOGWEIR_PYTHON to that interpreter. No resources are automatically deleted.
The fixture uses SASL_PLAINTEXT (not TLS), disposable Kafka data and MinIO root
credentials: this checks authentication and record integrity, not IAM isolation.
"""

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
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = pathlib.Path(os.environ.get("LOGWEIR_SCRAM_OUT", "/tmp/logweir-scram-e2e"))
NS = "logweir-scram-local"
REL = "scram-local"
OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
OUT.chmod(0o700)
CTX = ["kubectl", "--context", "docker-desktop"]
K = CTX + ["-n", NS]
STATE = OUT / "state.json"
state = (
    json.loads(STATE.read_text())
    if STATE.exists()
    else {
        "password": secrets.token_hex(16),
        "rotated": secrets.token_hex(16),
        "created": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
)
STATE.write_text(json.dumps(state))
STATE.chmod(0o600)


def save():
    STATE.write_text(json.dumps(state))
    STATE.chmod(0o600)


def redact(value):
    text = str(value)
    for key in ("password", "rotated"):
        secret_value = state.get(key, "")
        if secret_value:
            text = text.replace(secret_value, "[REDACTED]")
            text = text.replace(
                base64.b64encode(secret_value.encode()).decode(), "[REDACTED]"
            )
    for pair in ("signing", "approver"):
        path = OUT / (pair + ".pem")
        if path.exists():
            pem = path.read_text()
            for encoded in (
                pem,
                json.dumps(pem)[1:-1],
                base64.b64encode(pem.encode()).decode(),
            ):
                text = text.replace(encoded, "[REDACTED PRIVATE KEY]")
    return re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.DOTALL,
    )


def cmd(args, body=None, check=True, timeout=180):
    r = subprocess.run(
        args,
        input=body,
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=timeout,
        check=False,
    )
    if check and r.returncode:
        raise RuntimeError(
            redact(f"{args[:7]} rc={r.returncode}: {r.stderr} {r.stdout}")[-3600:]
        )
    return r


def apply(obj):
    return cmd(K + ["apply", "-f", "-"], json.dumps(obj))


def resource(kind, name, spec=None, **extra):
    o = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": kind,
        "metadata": {"name": name, "namespace": NS},
        **extra,
    }
    if spec is not None:
        o["spec"] = spec
    return o


def get(kind, name):
    return json.loads(cmd(K + ["get", kind, name, "-o", "json"]).stdout)


def wait(kind, name, predicate, seconds=360):
    deadline = time.time() + seconds
    while time.time() < deadline:
        o = get(kind, name)
        if predicate(o):
            (OUT / f"{kind}-{name}.json").write_text(json.dumps(o, indent=2))
            return o
        time.sleep(3)
    raise RuntimeError(f"Timeout {kind}/{name}: {json.dumps(o.get('status'))}")


def secret(name, data):
    o = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NS},
        "type": "Opaque",
        "stringData": data,
    }
    return apply(o)


def log(message):
    print(dt.datetime.now(dt.timezone.utc).isoformat(), redact(message), flush=True)


def brokerexec(role, script, timeout=180):
    return cmd(
        K + ["exec", "-i", "deploy/kafka-" + role, "--", "bash", "-se"],
        script,
        timeout=timeout,
    ).stdout


#: The source broker's seed topics. The shared lab (`scram-local`) is built once
#: and then read by every live harness for weeks, so the seed must outlive the
#: broker's default 7-day `retention.ms`: with it, `orders`/`payments` emptied a
#: week after the 2026-09-22 lab build and every Backup of them captured nothing
#: (LAB-SEED-TOPIC-RETENTION). `retention.ms=-1` keeps the 100 records per topic
#: for the life of the broker; lab-refresh-8 re-seeded the live lab that way.
SEED_TOPICS = ("orders", "payments")
SEED_RECORDS_PER_TOPIC = 100


def seed_topic_script(topic):
    """The broker shell script that creates one seed topic and writes its records.

    Pure (no cluster access), so `scripts/test_k8s_scram_seed.py` can check it."""
    script = (
        "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
        f" --create --topic {topic} --partitions 1 --replication-factor 1"
        " --config retention.ms=-1\n"
    )
    script += (
        "printf '%s\\n' "
        + " ".join(
            f"'{topic}-record-{n:03}'" for n in range(1, SEED_RECORDS_PER_TOPIC + 1)
        )
        + f" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic {topic}\n"
    )
    return script


def setup():
    if cmd(CTX + ["get", "ns", NS], check=False).returncode == 0:
        raise RuntimeError("Namespace exists; refusing setup overwrite")
    cmd(CTX + ["create", "ns", NS])
    cmd(CTX + ["label", "ns", NS, "app.kubernetes.io/managed-by=logweir-scram-e2e"])
    log("created isolated namespace " + NS)
    for role in ["source", "target"]:
        host = f"kafka-{role}.{NS}.svc.cluster.local"
        env = {
            "CLUSTER_ID": base64.urlsafe_b64encode(uuid.uuid4().bytes)
            .decode()
            .rstrip("="),
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SASL://0.0.0.0:9096",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:9092,SASL://{host}:9096",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT",
            "KAFKA_SASL_ENABLED_MECHANISMS": "SCRAM-SHA-512",
            "KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required;",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
            "KAFKA_LOG_DIRS": "/tmp/kraft-combined-logs",
            "KAFKA_HEAP_OPTS": "-Xmx512M -Xms256M",
        }
        labels = {"app": "kafka-" + role}
        apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "kafka-" + role, "namespace": NS},
                "spec": {
                    "selector": labels,
                    "ports": [{"name": "sasl", "port": 9096, "targetPort": 9096}],
                },
            }
        )
        apply(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "kafka-" + role, "namespace": NS},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": labels},
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "automountServiceAccountToken": False,
                            "containers": [
                                {
                                    "name": "kafka",
                                    "image": "apache/kafka:3.7.1",
                                    "imagePullPolicy": "IfNotPresent",
                                    "env": [
                                        {"name": k, "value": v} for k, v in env.items()
                                    ],
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "512Mi"},
                                        "limits": {"memory": "1Gi"},
                                    },
                                    "readinessProbe": {
                                        "tcpSocket": {"port": 9096},
                                        "initialDelaySeconds": 8,
                                        "periodSeconds": 3,
                                    },
                                }
                            ],
                        },
                    },
                },
            }
        )
    labels = {"app": "scram-minio"}
    apply(
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "minio", "namespace": NS},
            "spec": {"selector": labels, "ports": [{"port": 9000, "targetPort": 9000}]},
        }
    )
    secret("minio-root", {"user": "minioadmin", "password": state["password"]})
    apply(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "minio", "namespace": NS},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "minio",
                                "image": "minio/minio:latest",
                                "imagePullPolicy": "Never",
                                "args": ["server", "/data"],
                                "env": [
                                    {
                                        "name": "MINIO_ROOT_USER",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "minio-root",
                                                "key": "user",
                                            }
                                        },
                                    },
                                    {
                                        "name": "MINIO_ROOT_PASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "minio-root",
                                                "key": "password",
                                            }
                                        },
                                    },
                                ],
                                "ports": [{"containerPort": 9000}],
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/data"}
                                ],
                            }
                        ],
                        "volumes": [{"name": "data", "emptyDir": {}}],
                    },
                },
            },
        }
    )
    for name in ["kafka-source", "kafka-target", "minio"]:
        cmd(K + ["rollout", "status", "deploy/" + name, "--timeout=240s"], timeout=260)
    for role in ["source", "target"]:
        brokerexec(
            role,
            f'/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config "SCRAM-SHA-512=[password={state["password"]}]" --entity-type users --entity-name scram-user\n',
        )
    for topic in SEED_TOPICS:
        brokerexec("source", seed_topic_script(topic))
    brokerexec(
        "target",
        "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic logweir.scratch --partitions 1 --replication-factor 1\n",
    )
    secret("source-scram", {"password": state["password"]})
    secret("target-scram", {"password": state["password"]})
    secret(
        "logweir-s3",
        {"access-key-id": "minioadmin", "secret-access-key": state["password"]},
    )
    secret(
        "logweir-evidence-ro",
        {"access-key-id": "minioadmin", "secret-access-key": state["password"]},
    )
    secret("mc-host", {"url": f"http://minioadmin:{state['password']}@minio:9000"})
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "mc", "namespace": NS},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "mc",
                    "image": "minio/mc:latest",
                    "imagePullPolicy": "Never",
                    "command": ["/bin/sh", "-c", "sleep 7200"],
                    "env": [
                        {
                            "name": "MC_HOST_local",
                            "valueFrom": {
                                "secretKeyRef": {"name": "mc-host", "key": "url"}
                            },
                        }
                    ],
                }
            ],
        },
    }
    apply(pod)
    cmd(K + ["wait", "--for=condition=Ready", "pod/mc", "--timeout=90s"])
    cmd(K + ["exec", "mc", "--", "mc", "mb", "local/kafka-backups"])
    log("SCRAM brokers and archive ready; 100 records each in orders/payments")
    for pair in ["signing", "approver"]:
        cmd(
            [
                "openssl",
                "genpkey",
                "-algorithm",
                "EC",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-out",
                str(OUT / f"{pair}.pem"),
            ]
        )
        cmd(
            [
                "openssl",
                "pkey",
                "-in",
                str(OUT / f"{pair}.pem"),
                "-pubout",
                "-out",
                str(OUT / f"{pair}.pub.pem"),
            ]
        )
        (OUT / f"{pair}.pem").chmod(0o600)
    secret("logweir-signing-key", {"signing.pem": (OUT / "signing.pem").read_text()})
    state["setup"] = True
    save()


def install():
    if not state.get("setup"):
        raise RuntimeError("Run setup first")
    if cmd(CTX + ["get", "trustroster", "default"], check=False).returncode == 0:
        raise RuntimeError("Refusing existing roster overwrite")
    for image in ["logweir:scram-local", "weirkeeper:scram-local"]:
        cmd(["docker", "image", "inspect", image])
    cmd(
        [
            "helm",
            "install",
            REL,
            "charts/logweir",
            "--kube-context",
            "docker-desktop",
            "-n",
            NS,
            "--set",
            "controllerImage=weirkeeper:scram-local",
            "--set",
            "runnerImage=logweir:scram-local",
            "--set",
            "imagePullPolicy=Never",
            "--set",
            "runnerImagePullPolicy=Never",
            "--set",
            "archive.url=s3://kafka-backups/scram-local",
            "--set",
            f"archive.s3.endpoint=http://minio.{NS}.svc.cluster.local:9000",
            "--set",
            "archive.s3.region=us-east-1",
            "--set",
            "archive.s3.allowHttp=true",
            "--wait",
            "--timeout",
            "180s",
        ],
        timeout=200,
    )
    entries = {}
    for pair in ["approver", "signing"]:
        der = subprocess.check_output(
            [
                "openssl",
                "pkey",
                "-pubin",
                "-in",
                str(OUT / f"{pair}.pub.pem"),
                "-outform",
                "DER",
            ]
        )
        entries[pair] = {
            "keyId": hashlib.sha256(der).hexdigest(),
            "subject": pair + "@scram-local.invalid",
            "spkiPem": (OUT / f"{pair}.pub.pem").read_text(),
        }
    o = resource(
        "TrustRoster",
        "default",
        {
            "allowedClusterIds": [],
            "approverKeys": [entries["approver"]],
            "signingKeys": [entries["signing"]],
        },
    )
    o["metadata"].pop("namespace")
    cmd(CTX + ["create", "-f", "-"], json.dumps(o))
    for role in ["source", "target"]:
        create_cluster(role, role, role + "-scram")
    for role in ["source", "target"]:
        o = wait(
            "kafkacluster", role, lambda o: o.get("status", {}).get("reachable") is True
        )
        state[role + "_cluster_id"] = o["status"]["clusterId"]
        log(role + " authenticated probe passed: " + o["status"]["clusterId"])
    assert state["source_cluster_id"] != state["target_cluster_id"]
    save()


def create_cluster(name, role, secret_name):
    auth = {"mode": "scramSha512", "username": "scram-user", "tls": False}
    if secret_name is not None:
        auth["secretRef"] = {"name": secret_name}
    apply(
        resource(
            "KafkaCluster",
            name,
            {
                "bootstrapServers": [f"kafka-{role}.{NS}.svc.cluster.local:9096"],
                "auth": auth,
                "role": role,
            },
        )
    )


def backup_argv():
    # The current controller requires this scheduler annotation for manual CRs.
    # Omit --backup-id-override: the rendered plan uses the manual object's UID.
    return [
        "backup",
        "run",
        "--spec",
        "/plan/backup.yaml",
        "--allowed-clusters",
        "/plan/allowed-clusters.json",
        "--signing-key",
        "/signing/key.pem",
        "--receipt-out",
        "/work/receipt.json",
        "--triggered-by",
        "manual",
    ]


def backup(name, source="source", deadline=150):
    obj = resource(
        "Backup",
        name,
        {
            "sourceRef": {"name": source},
            "topics": ["orders", "payments"],
            "archive": {
                "url": "s3://kafka-backups/scram-local",
                "secretRef": {"name": "logweir-s3"},
            },
            "triggeredBy": "manual",
            "deadlineSeconds": deadline,
        },
    )
    obj["metadata"]["annotations"] = {
        "logweir.dev/runner-argv": json.dumps(backup_argv())
    }
    apply(obj)
    return wait(
        "backup",
        name,
        lambda o: o.get("status", {}).get("phase") in ["Succeeded", "Failed"],
        seconds=deadline + 120,
    )


def verify_backup(o):
    name = o["metadata"]["name"]
    assert o["status"].get("exitCode") == 0, o["status"]
    o = wait(
        "backup",
        name,
        lambda o: (
            o.get("status", {})
            .get("evidence", {})
            .get("verification", {})
            .get("result")
            == "Valid"
        ),
        seconds=90,
    )
    j = get("job", name)
    env = j["spec"]["template"]["spec"]["containers"][0]["env"]
    src = [e for e in env if e["name"] == "LOGWEIR_SOURCE_PASSWORD"]
    assert src[0]["valueFrom"]["secretKeyRef"] == {
        "key": "password",
        "name": "source-scram",
    }, src
    cm = get("configmap", name + "-plan")
    assert state["password"] not in json.dumps(cm)
    assert state["rotated"] not in json.dumps(cm)
    state.setdefault("backups", []).append(name)
    save()
    log("backup passed " + name + " id=" + o["status"]["backupId"])
    return o


def positives():
    apply(
        resource(
            "BackupSchedule",
            "scram-schedule",
            {
                "schedule": "* * * * *",
                "sourceRef": {"name": "source"},
                "topics": ["orders", "payments"],
                "archive": {
                    "url": "s3://kafka-backups/scram-local",
                    "secretRef": {"name": "logweir-s3"},
                },
                "suspend": False,
            },
        )
    )
    deadline = time.time() + 100
    name = None
    while time.time() < deadline:
        items = json.loads(cmd(K + ["get", "backups", "-o", "json"]).stdout)["items"]
        matches = [
            o
            for o in items
            if o["spec"].get("scheduleRef", {}).get("name") == "scram-schedule"
        ]
        if matches:
            name = matches[0]["metadata"]["name"]
            break
        time.sleep(3)
    assert name, "schedule did not fire"
    cmd(
        K
        + [
            "patch",
            "backupschedule",
            "scram-schedule",
            "--type=merge",
            "-p",
            '{"spec":{"suspend":true}}',
        ]
    )
    verify_backup(
        wait(
            "backup",
            name,
            lambda o: o.get("status", {}).get("phase") in ["Succeeded", "Failed"],
        )
    )
    o = verify_backup(backup("shared-secret-repeat"))
    state["restore_backup_id"] = o["status"]["backupId"]
    save()
    # Reapplying the same immutable Backup is a retry of reconciliation, not a new archive.
    before = get("job", "shared-secret-repeat")["metadata"]["uid"]
    apply(
        {k: v for k, v in o.items() if k in ["apiVersion", "kind", "spec", "metadata"]}
    )
    assert get("job", "shared-secret-repeat")["metadata"]["uid"] == before
    log("repeat reconciliation retains same Job UID")


def negatives():
    create_cluster("missing-reference", "source", None)
    o = wait(
        "kafkacluster",
        "missing-reference",
        lambda o: o.get("status", {}).get("reachable") is False,
        seconds=90,
    )
    log("missing auth.secretRef rejected: " + json.dumps(o["status"]))
    o = backup("missing-reference-backup", "missing-reference")
    assert o["status"].get("exitCode") != 0
    assert (
        cmd(K + ["get", "job", "missing-reference-backup"], check=False).returncode != 0
    )
    log("missing auth.secretRef backup refused without Job")
    secret("bad-scram", {"password": "wrong-local-fixture-password"})
    create_cluster("wrong-password", "source", "bad-scram")
    o = wait(
        "kafkacluster",
        "wrong-password",
        lambda o: o.get("status", {}).get("reachable") is False,
        seconds=180,
    )
    log("wrong password probe failed (no plaintext fallback)")
    o = backup("wrong-password-backup", "wrong-password", 60)
    assert o["status"].get("exitCode") != 0
    log("wrong password backup failed: " + json.dumps(o["status"]))
    # A secret object missing its password key is a kubelet configuration failure.
    secret("no-password-key", {"other": "not-a-password"})
    create_cluster("missing-key", "source", "no-password-key")
    deadline = time.time() + 45
    found = False
    while time.time() < deadline:
        pods = json.loads(cmd(K + ["get", "pods", "-o", "json"]).stdout)["items"]
        for pod in pods:
            refs = pod["spec"]["containers"][0].get("env", [])
            if any(
                e.get("valueFrom", {}).get("secretKeyRef", {}).get("name")
                == "no-password-key"
                for e in refs
            ):
                statuses = pod.get("status", {}).get("containerStatuses", [])
                if any(
                    c.get("state", {}).get("waiting", {}).get("reason")
                    == "CreateContainerConfigError"
                    for c in statuses
                ):
                    found = True
                    (OUT / "missing-key-pod.json").write_text(json.dumps(pod, indent=2))
                    break
        if found:
            break
        time.sleep(3)
    assert found, "missing key did not surface kubelet config error"
    log("missing Secret data key surfaced CreateContainerConfigError")


def rotation():
    brokerexec(
        "source",
        f'/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config "SCRAM-SHA-512=[password={state["rotated"]}]" --entity-type users --entity-name scram-user\n',
    )
    o = backup("stale-secret-after-rotation", deadline=60)
    assert o["status"].get("exitCode") != 0
    log("broker rotation invalidated old Secret")
    secret("source-scram", {"password": state["rotated"]})
    o = verify_backup(backup("rotated-secret-backup"))
    state["restore_backup_id"] = o["status"]["backupId"]
    save()
    log(
        "same KafkaCluster and same source Secret name resumed backups after Secret update"
    )


def restore():
    # Disconnect the registered source endpoint without destroying ephemeral
    # fixture data. A fresh restore must read only the archive and target.
    selector = get("service", "kafka-source")["spec"]["selector"]

    def select(value):
        cmd(
            K + [
                "patch", "service", "kafka-source", "--type=json", "-p",
                json.dumps([
                    {"op": "replace", "path": "/spec/selector", "value": value}
                ]),
            ]
        )

    select({"app": "offline-" + uuid.uuid4().hex})
    try:
        wait("endpoints", "kafka-source", lambda obj: not obj.get("subsets"), seconds=30)
        restore_from_archive()
        state["source_unavailable_restore"] = True
        save()
    finally:
        select(selector)
        wait("endpoints", "kafka-source", lambda obj: bool(obj.get("subsets")), seconds=30)


def restore_from_archive():
    now = dt.datetime.now(dt.timezone.utc)
    suffix = uuid.uuid4().hex[:8]
    name = "scram-record-restore-" + suffix
    approval = "scram-record-approval-" + suffix
    topic_prefix = "scram-restored-" + suffix + "-"
    pit = (now + dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (now - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    endpoint = f"http://minio.{NS}.svc.cluster.local:9000"
    storage = {
        "backend": "s3",
        "bucket": "kafka-backups",
        "prefix": "scram-local",
        "region": "us-east-1",
        "endpoint": endpoint,
        "path_style": True,
        "allow_http": True,
    }
    plan = {
        "source": {
            "storage": storage,
            "backup": state["restore_backup_id"],
            "topics": ["orders", "payments"],
        },
        "target": {
            "bootstrap_servers": [f"kafka-target.{NS}.svc.cluster.local:9096"],
            "auth": {"mode": "scramSha512", "username": "scram-user", "tls": False},
            "mode": "newTopic",
            "topic_naming": {"prefix": topic_prefix},
            "topic_mapping_prefix": "logweir-scratch-",
            "marker_topic": "logweir.scratch",
            "default_replication_factor": 1,
            "teardown": "delete",
        },
        "restore": {"point_in_time": pit},
        "sample": {
            "window_start": start,
            "window_end": pit,
            "records_per_partition": 25,
            "anchor": "head",
        },
        "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
        "evidence": {**storage, "prefix": "logweir/"},
        "notifications": {"webhooks": []},
    }
    plan_bytes = json.dumps(plan, indent=2) + "\n"
    (OUT / "restore-plan.json").write_text(plan_bytes)
    plan_hash = "sha256:" + hashlib.sha256(plan_bytes.encode()).hexdigest()
    cmd(
        [
            str(ROOT / "target/debug/logweir"),
            "drill",
            "approve",
            "--spec",
            str(OUT / "restore-plan.json"),
            "--key",
            str(OUT / "approver.pem"),
            "--approver",
            "scram-local-test",
            "--ticket",
            "LOCAL-SCRAM",
            "--subject-kind",
            "Restore",
            "--out",
            str(OUT / "approval.json"),
        ]
    )
    allowed = json.dumps(
        {
            "allowed_cluster_ids": [state["target_cluster_id"]],
            "source_cluster_id": state["source_cluster_id"],
        }
    )
    secret(
        "logweir-approval-bundle",
        {
            "approval.json": (OUT / "approval.json").read_text(),
            "approval.sig": (OUT / "approval.sig").read_text(),
            "approver.pub.pem": (OUT / "approver.pub.pem").read_text(),
            "allowed-clusters.json": allowed,
        },
    )
    apply(
        resource(
            "Restore",
            name,
            {
                "sourceArchive": {
                    "url": "s3://kafka-backups/scram-local",
                    "secretRef": {"name": "logweir-s3"},
                },
                "backupSetRef": state["restore_backup_id"],
                "pointInTime": pit,
                "target": {
                    "clusterRef": {"name": "target"},
                    "mode": "newTopic",
                    "topicNaming": {"prefix": topic_prefix},
                },
                "approvalRef": {"name": approval},
                "planBytes": plan_bytes,
                "deadlineSeconds": 600,
            },
        )
    )
    apply(
        resource(
            "Approval",
            approval,
            {
                "subjectRef": {"kind": "Restore", "name": name},
                "planHash": plan_hash,
                "approvalBytes": (OUT / "approval.json").read_text(),
                "sidecarBytes": (OUT / "approval.sig").read_text(),
            },
        )
    )
    o = wait(
        "restore",
        name,
        lambda o: o.get("status", {}).get("phase") in ["Succeeded", "Failed"],
        seconds=700,
    )
    assert o["status"].get("exitCode") == 0, o["status"]
    assert o["status"].get("outcome") == "pass", o["status"]
    o = wait(
        "restore",
        name,
        lambda o: (
            o.get("status", {})
            .get("evidence", {})
            .get("verification", {})
            .get("result")
            == "Valid"
        ),
        seconds=90,
    )
    for topic in ["orders", "payments"]:
        offsets = brokerexec(
            "target",
            f"/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic {topic_prefix}{topic} --time -1\n",
        ).strip().splitlines()
        assert offsets == [f"{topic_prefix}{topic}:0:100"], (
            "unexpected trailing records or partitions",
            offsets,
        )
        data = brokerexec(
            "target",
            f"/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic_prefix}{topic} --from-beginning --max-messages 100 --timeout-ms 15000\n",
        )
        lines = data.strip().splitlines()
        assert sorted(lines) == [f"{topic}-record-{n:03}" for n in range(1, 101)], (
            topic,
            len(lines),
            lines[:3],
        )
        (OUT / f"restored-{topic}.txt").write_text(data)
    ev = o["status"]["evidence"]
    for key, filename in [
        ("scorecardKey", "scorecard.json"),
        ("sidecarKey", "scorecard.sig"),
    ]:
        r = cmd(K + ["exec", "mc", "--", "mc", "cat", "local/kafka-backups/" + ev[key]])
        (OUT / filename).write_text(r.stdout)
    sc = json.loads((OUT / "scorecard.json").read_text())
    assert sc["target"]["auth"] == {"mode": "scramSha512", "username": "scram-user"}, (
        sc["target"].get("auth")
    )
    cmd(
        [
            str(ROOT / "target/debug/logweir"),
            "drill",
            "verify",
            "--scorecard",
            str(OUT / "scorecard.json"),
            "--signature",
            str(OUT / "scorecard.sig"),
            "--public-key",
            str(OUT / "signing.pub.pem"),
        ]
    )
    cmd(
        [
            os.environ.get("LOGWEIR_PYTHON", sys.executable),
            "docs/verify_scorecard.py",
            str(OUT / "scorecard.json"),
            str(OUT / "scorecard.sig"),
            str(OUT / "signing.pub.pem"),
        ]
    )
    log(
        "SCRAM target restore passed; all 200 records exactly match; scorecard target.auth and both verifiers passed"
    )
    state["passed"] = True
    save()


def report():
    result = {
        "context": "docker-desktop",
        "namespace": NS,
        "completed": all(
            phase in state.get("phases", {})
            for phase in [
                "setup",
                "install",
                "positives",
                "negatives",
                "rotation",
                "restore",
            ]
        ),
        "phases": state.get("phases", {}),
        "source_unavailable_restore": state.get("source_unavailable_restore", False),
        "images": {},
        "checks": {},
        "artifacts": str(OUT),
        "limitations": [
            "SASL_PLAINTEXT; TLS not exercised",
            "MinIO root credential; IAM isolation not exercised",
            "No automatic cleanup",
        ],
    }
    deployed_controller = get("deployment", "weirkeeper")["spec"]["template"]["spec"][
        "containers"
    ][0]["image"]
    for tag in ["logweir:scram-local", deployed_controller]:
        result["images"][tag] = cmd(
            ["docker", "image", "inspect", tag, "--format", "{{.Id}}"]
        ).stdout.strip()
    for kind in ["kafkaclusters", "backups", "restores"]:
        result["checks"][kind] = [
            {"name": o["metadata"]["name"], "status": o.get("status", {})}
            for o in json.loads(cmd(K + ["get", kind, "-o", "json"]).stdout)["items"]
        ]
    result["record_comparison"] = (
        {"orders": 100, "payments": 100} if state.get("passed") else None
    )
    (OUT / "report.json").write_text(redact(json.dumps(result, indent=2)) + "\n")
    log("report written to " + str(OUT / "report.json"))


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("Do not use python -O: this test harness requires assertions")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "phase",
        choices=[
            "setup",
            "install",
            "positives",
            "negatives",
            "rotation",
            "restore",
            "report",
        ],
    )
    args = parser.parse_args()
    if args.phase != "setup":
        ns = json.loads(cmd(CTX + ["get", "ns", NS, "-o", "json"]).stdout)
        if (
            ns["metadata"].get("labels", {}).get("app.kubernetes.io/managed-by")
            != "logweir-scram-e2e"
        ):
            raise SystemExit("Refusing namespace not owned by this test harness")
    try:
        globals()[args.phase]()
        if args.phase != "report":
            state.setdefault("phases", {})[args.phase] = dt.datetime.now(
                dt.timezone.utc
            ).isoformat()
            save()
    except Exception as exc:  # noqa: BLE001 -- redact all terminal failures before printing
        raise SystemExit(redact(f"{type(exc).__name__}: {exc}")) from None
