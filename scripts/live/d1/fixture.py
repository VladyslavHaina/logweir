#!/usr/bin/env python3
"""The docker-desktop fixture the D1 live acceptance harness runs against.

One namespace holds everything a D1 scenario needs and nothing it does not:

* a single-node KRaft Kafka on PLAINTEXT (`apache/kafka:3.7.1`, the image the
  shared lab already runs, so nothing is pulled), seeded with named topics and
  a few records each — D1 §13.1 asks W8 to bring its own broker rather than
  write to the shared lab's;
* a MinIO the harness owns, so that L-04-4 may scale it to zero and back
  without touching a shared object;
* a `BackupDestination` naming that MinIO, because the archive endpoint is an
  installation setting the controller renders into the Job (the shared release
  points at the lab's MinIO), and a saved destination is the ONLY way a run in
  this namespace writes to the broker-local store;
* the three Secrets the runner needs, copied by reference from the lab fixture
  so that no credential value is ever read, printed or written down here.

Nothing in this module asserts anything about Logweir's behaviour: it is
scaffolding, and the scenarios in `run.py` are the measurements.
"""

from __future__ import annotations

from typing import Any

KAFKA_IMAGE = "apache/kafka:3.7.1"
MINIO_IMAGE = "docker.io/vladyslavhaina/minio-mirror@sha256:a707398148b545774fc98264d16e76307f1b3727f77b1cc49c67ccde8998709d"
MC_IMAGE = "docker.io/vladyslavhaina/mc-mirror@sha256:4824f9b00fd4ca9e3b7d61f66211450cd5d63f5f99d3170654af49568869b77b"
FIXTURE_NS = "logweir-scram-local"
BUCKET = "d1-archive"
CLUSTER_ID = "d1w8AcceptLabQQ"  # 22 chars, base64url; distinct from the lab's


def kafka_manifests(ns: str, labels: dict[str, str]) -> list[dict[str, Any]]:
    """A PLAINTEXT single-node KRaft broker and the Service it advertises.

    PLAINTEXT and not SCRAM on purpose: D1's scenarios measure scheduling,
    revisions, ownership and topic selection. SCRAM is PLAT-07's subject and
    `scripts/test-plat07-live.py` already proves it live; carrying it here
    would add a credential to a harness that otherwise handles none.
    """
    host = f"kafka.{ns}.svc.cluster.local"
    env = {
        "CLUSTER_ID": CLUSTER_ID,
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{host}:9092",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        # The scenarios create every topic they name. Auto-creation would make
        # L-09-4 ("exclude every user topic") unfalsifiable, because a consumer
        # probe could conjure the topic it was meant to find missing.
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
        "KAFKA_LOG_DIRS": "/tmp/kraft-combined-logs",
        "KAFKA_HEAP_OPTS": "-Xmx512M -Xms256M",
    }
    return [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "kafka", "namespace": ns, "labels": labels},
            "spec": {
                "selector": {"app": "d1-kafka"},
                "ports": [{"name": "kafka", "port": 9092, "targetPort": 9092}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "kafka", "namespace": ns, "labels": labels},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "d1-kafka"}},
                "template": {
                    "metadata": {"labels": dict(labels, app="d1-kafka")},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "kafka",
                                "image": KAFKA_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "env": [{"name": k, "value": v} for k, v in env.items()],
                                "ports": [{"containerPort": 9092}],
                                "readinessProbe": {
                                    "tcpSocket": {"port": 9092},
                                    "initialDelaySeconds": 8,
                                    "periodSeconds": 3,
                                },
                                "resources": {
                                    "requests": {"cpu": "100m", "memory": "512Mi"},
                                    "limits": {"memory": "1Gi"},
                                },
                            }
                        ],
                    },
                },
            },
        },
    ]


def minio_manifests(ns: str, labels: dict[str, str]) -> list[dict[str, Any]]:
    """MinIO with the lab's own root credential, by reference only.

    The Secret is copied key-for-key by the harness (`copy_secret`), so this
    module never sees a value; `logweir-s3` carries the same pair, which is
    why a destination built on it can write here.
    """
    return [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "minio", "namespace": ns, "labels": labels},
            "spec": {
                "selector": {"app": "d1-minio"},
                "ports": [{"name": "s3", "port": 9000, "targetPort": 9000}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "minio", "namespace": ns, "labels": labels},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "d1-minio"}},
                "template": {
                    "metadata": {"labels": dict(labels, app="d1-minio")},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "minio",
                                "image": MINIO_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "args": ["server", "/data"],
                                "env": [
                                    {
                                        "name": "MINIO_ROOT_USER",
                                        "valueFrom": {
                                            "secretKeyRef": {"name": "minio-root", "key": "user"}
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
                                "readinessProbe": {
                                    "tcpSocket": {"port": 9000},
                                    "initialDelaySeconds": 3,
                                    "periodSeconds": 3,
                                },
                                "resources": {
                                    "requests": {"cpu": "50m", "memory": "256Mi"},
                                    "limits": {"memory": "1Gi"},
                                },
                            }
                        ],
                        "volumes": [],
                    },
                },
            },
        },
    ]


def mc_pod(ns: str, labels: dict[str, str]) -> dict[str, Any]:
    """A long-lived `mc` shell, so the harness can read the archive it wrote."""
    endpoint = f"http://minio.{ns}.svc.cluster.local:9000"
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "d1-mc", "namespace": ns, "labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "mc",
                    "image": MC_IMAGE,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        "until mc alias set local "
                        f"{endpoint} "
                        '"$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null 2>&1; '
                        "do sleep 2; done; "
                        f"mc mb --ignore-existing local/{BUCKET} >/dev/null 2>&1; "
                        "touch /tmp/ready && sleep 10800"
                    ],
                    "env": [
                        {
                            "name": "AWS_ACCESS_KEY_ID",
                            "valueFrom": {
                                "secretKeyRef": {"name": "logweir-s3", "key": "access-key-id"}
                            },
                        },
                        {
                            "name": "AWS_SECRET_ACCESS_KEY",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "logweir-s3",
                                    "key": "secret-access-key",
                                }
                            },
                        },
                    ],
                    "readinessProbe": {
                        "exec": {"command": ["test", "-f", "/tmp/ready"]},
                        "periodSeconds": 2,
                    },
                }
            ],
        },
    }


def destination(ns: str, name: str, labels: dict[str, str], prefix: str) -> dict[str, Any]:
    """The saved location every run in this namespace writes to."""
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupDestination",
        "metadata": {"name": name, "namespace": ns, "labels": labels},
        "spec": {
            "description": "the D1 W8 acceptance MinIO, in this namespace",
            "storage": {
                "provider": "S3",
                "bucket": BUCKET,
                "prefix": prefix,
                "addressing": "PathStyle",
                "endpoint": f"http://minio.{ns}.svc.cluster.local:9000",
                "region": "us-east-1",
            },
            # InsecureHTTP is the honest spelling for a plaintext in-cluster
            # MinIO. Docker Desktop does not enforce NetworkPolicy, so this
            # harness claims nothing about transport confinement either way.
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {
                    "mode": "SecretKeys",
                    "secret": {"name": "logweir-s3"},
                }
            },
        },
    }


def kafka_cluster(ns: str, name: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": name, "namespace": ns, "labels": labels},
        "spec": {
            "bootstrapServers": [f"kafka.{ns}.svc.cluster.local:9092"],
            "auth": {"mode": "plaintext", "tls": False},
            "role": "source",
        },
    }
