#!/usr/bin/env python3
"""D1 L-09-5's broker: KRaft, `StandardAuthorizer`, and one topic the backup
principal may not describe.

L-09-5 is the only D1 row that is a statement about **authorization**, and the
lab's shared `kafka-source` cannot make it: it runs SCRAM with no authorizer at
all, so every authenticated principal sees every topic. This module builds the
broker the row needs, in the row's own namespace.

# Why the credentials are created at `kafka-storage format` time

There is a bootstrap order problem and it is the reason PLAT-09.2's residue
names this step explicitly. `kafka-configs.sh --alter --add-config
'SCRAM-SHA-512=[…]'` is the usual way to create a SCRAM user, and it needs a
connection to a broker that is already up. With `StandardAuthorizer` and
`allow.everyone.if.no.acl.found=false` there is no principal that may make that
connection until a credential exists — and no credential exists until someone
makes it. `kafka-storage format --add-scram` writes the credential into the
metadata log **before the broker starts**, which is the only order that closes
the loop. `apache/kafka:3.7.1`'s own entrypoint (`/etc/kafka/docker/run`)
formats storage without that flag, so this fixture replaces the entrypoint
rather than configuring it.

# The two principals, and which one the row measures

* `User:ANONYMOUS` is a super user. That is not a convenience: KRaft's own
  controller traffic (`Vote`, `BeginQuorumEpoch`, `BrokerRegistration`, the
  metadata-log `Fetch`) is authorized as `ClusterAction` on `Cluster`, and on a
  PLAINTEXT `CONTROLLER` listener its principal is `ANONYMOUS`. Without the
  super-user entry a single-node KRaft broker with an authorizer never finishes
  starting. The same entry is what lets the harness create topics and ACLs from
  **inside the pod** over `localhost:9092` with no credential on any command
  line — which is why no password is ever an argument of a recorded command.
* `User:BACKUP_USER` is the principal the row measures. It exists only in SCRAM,
  reaches the broker only over the `SASL_PLAINTEXT` listener the `Service`
  publishes, and gets ACLs for every topic except one.

The PLAINTEXT listener is deliberately NOT published by the `Service`: it is
advertised as `localhost:9092`, so the super-user path exists inside the pod and
nowhere else.

# What is never printed

The password is generated per run by the caller (`secrets.token_urlsafe`), put
in a `Secret` under the key `password` — which `run.py::redact` blanks in every
artifact — and reaches the broker only as an environment variable read by the
entrypoint script. This module never receives it and never returns it; the
manifests below carry a `secretKeyRef` and nothing else.
"""

from __future__ import annotations

from typing import Any

KAFKA_IMAGE = "apache/kafka:3.7.1"

# 22 characters, base64url, distinct from `fixture.CLUSTER_ID` and from the
# lab's: two brokers in one namespace with one cluster id would make
# `SourceChangedDuringResolution`'s cluster-id clause unfalsifiable.
CLUSTER_ID = "d1P092AclAuthzLabQQQQQ"

APP = "d1-acl-kafka"
SERVICE = "acl-kafka"
DEPLOYMENT = "acl-kafka"
CONFIGMAP = "acl-kafka-entrypoint"
SECRET = "acl-source-scram"
SECRET_KEY = "password"
BACKUP_USER = "d1backup"
SASL_PORT = 9096

#: The topic the backup principal is never granted `Describe` on.
DENIED_TOPIC = "secret-t"
#: The topics it is granted `Describe` and `Read` on.
ALLOWED_TOPICS = ("acl-a", "acl-b")


def entrypoint(ns: str) -> str:
    """The container's whole command: write the config, format, start.

    `set -eu` and an `exec` at the end, so a failure to format is a crashing
    container the harness can see rather than a broker that starts without its
    credentials and then refuses every connection for a reason nobody can find.
    """
    host = f"{SERVICE}.{ns}.svc.cluster.local"
    properties = "\n".join(
        [
            "node.id=1",
            "process.roles=broker,controller",
            "controller.quorum.voters=1@localhost:9093",
            "listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
            f",SASL://0.0.0.0:{SASL_PORT}",
            f"advertised.listeners=PLAINTEXT://localhost:9092,SASL://{host}:{SASL_PORT}",
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            ",SASL:SASL_PLAINTEXT",
            "controller.listener.names=CONTROLLER",
            "inter.broker.listener.name=PLAINTEXT",
            "sasl.enabled.mechanisms=SCRAM-SHA-512",
            "listener.name.sasl.scram-sha-512.sasl.jaas.config="
            "org.apache.kafka.common.security.scram.ScramLoginModule required;",
            # THE AUTHORIZER IS THE POINT OF THIS FIXTURE.
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "super.users=User:ANONYMOUS",
            # Explicit, although it is the default: a reader of this fixture has
            # to be able to see that an absent ACL denies, because the row's
            # whole measurement is one absent ACL.
            "allow.everyone.if.no.acl.found=false",
            "log.dirs=/tmp/d1-acl-logs",
            "offsets.topic.replication.factor=1",
            "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1",
            "group.initial.rebalance.delay.ms=0",
            # The row creates every topic it names; auto-creation would let a
            # probe conjure the very topic the ACL is meant to hide.
            "auto.create.topics.enable=false",
            "num.partitions=1",
            "default.replication.factor=1",
        ]
    )
    return f"""set -eu
CFG=/tmp/d1-acl-server.properties
cat > "$CFG" <<'PROPS'
{properties}
PROPS
if [ ! -f /tmp/d1-acl-logs/meta.properties ]; then
  # The password is expanded here, inside the container, from the Secret-backed
  # environment variable. It is on no manifest and on no recorded command line.
  # stdout is dropped because `kafka-storage format` echoes the directory it
  # wrote; stderr is kept so a real failure is visible in `kubectl logs`.
  /opt/kafka/bin/kafka-storage.sh format \\
    -t "$D1_CLUSTER_ID" -c "$CFG" \\
    --add-scram "SCRAM-SHA-512=[name={BACKUP_USER},password=$D1_BACKUP_PASSWORD]" \\
    >/dev/null
fi
exec /opt/kafka/bin/kafka-server-start.sh "$CFG"
"""


def manifests(ns: str, labels: dict[str, str]) -> list[dict[str, Any]]:
    """The ConfigMap, the Service and the Deployment — in apply order.

    Only the SASL listener is published. A `Service` for 9092 would put a
    super-user PLAINTEXT port on the namespace network, and the row would then
    be measuring a broker that is not the broker it describes.
    """
    return [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": CONFIGMAP, "namespace": ns, "labels": labels},
            "data": {"entrypoint.sh": entrypoint(ns)},
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": SERVICE, "namespace": ns, "labels": labels},
            "spec": {
                "selector": {"app": APP},
                "ports": [{"name": "sasl", "port": SASL_PORT, "targetPort": SASL_PORT}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": DEPLOYMENT, "namespace": ns, "labels": labels},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": APP}},
                "template": {
                    "metadata": {"labels": dict(labels, app=APP)},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "volumes": [
                            {
                                "name": "entrypoint",
                                "configMap": {"name": CONFIGMAP, "defaultMode": 0o555},
                            }
                        ],
                        "containers": [
                            {
                                "name": "kafka",
                                "image": KAFKA_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["/bin/sh", "/d1/entrypoint.sh"],
                                "volumeMounts": [
                                    {"name": "entrypoint", "mountPath": "/d1"}
                                ],
                                "env": [
                                    {"name": "D1_CLUSTER_ID", "value": CLUSTER_ID},
                                    {"name": "KAFKA_HEAP_OPTS", "value": "-Xmx512M -Xms256M"},
                                    {
                                        "name": "D1_BACKUP_PASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": SECRET,
                                                "key": SECRET_KEY,
                                            }
                                        },
                                    },
                                ],
                                "ports": [{"containerPort": SASL_PORT}],
                                "readinessProbe": {
                                    "tcpSocket": {"port": SASL_PORT},
                                    "initialDelaySeconds": 10,
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


def kafka_cluster(ns: str, name: str, labels: dict[str, str]) -> dict[str, Any]:
    """The saved connection the measured principal dials.

    SCRAM-SHA-512 over the published listener, password by reference. Nothing
    here names a super user: the only principal Logweir ever authenticates as
    against this broker is the one the ACLs constrain.
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": name, "namespace": ns, "labels": labels},
        "spec": {
            "bootstrapServers": [f"{SERVICE}.{ns}.svc.cluster.local:{SASL_PORT}"],
            "auth": {
                "mode": "scramSha512",
                "username": BACKUP_USER,
                "secretRef": {"name": SECRET},
                "tls": False,
            },
            "role": "source",
        },
    }


def acl_commands(*, allowed: tuple[str, ...] = ALLOWED_TOPICS) -> list[list[str]]:
    """Every `kafka-acls.sh` invocation, as argv, for `User:BACKUP_USER`.

    Run through the pod-local PLAINTEXT listener as the `ANONYMOUS` super user,
    so no credential appears in any argv this harness records.

    What is granted, and what deliberately is not:

    * `Describe` and `Read` on each allowed topic, literal — `Read` because the
      `BackUpVisibleTopics` run has to actually copy the records, and a row that
      only proved the topic was listed would not have proved a backup;
    * `Read` on group `*`, because the engine's consumer needs a group id even
      though this run never commits;
    * `Describe` on `Cluster`, which `DescribeCluster` needs and which says
      nothing about any topic;
    * **nothing at all on `DENIED_TOPIC`.** With
      `allow.everyone.if.no.acl.found=false` the absence IS the denial, and the
      absence is what the row measures. There is no "deny" ACL here on purpose:
      a deny rule and a missing allow rule are the same answer to the client
      and only the second is the ordinary operator mistake.
    """
    base = ["/opt/kafka/bin/kafka-acls.sh", "--bootstrap-server", "localhost:9092", "--add",
            "--allow-principal", f"User:{BACKUP_USER}"]
    commands = [
        base + ["--operation", "Describe", "--operation", "Read", "--topic", topic]
        for topic in allowed
    ]
    commands.append(base + ["--operation", "Read", "--group", "*"])
    commands.append(base + ["--operation", "Describe", "--cluster"])
    return commands
