#!/usr/bin/env python3
"""Live docker-desktop acceptance for PLAT-06.1 — the Backup execution contract.

Every case runs in one owned namespace against the shared `logweir-scram-local`
fixture's SCRAM Kafka and MinIO, with the lab controller pointed at images built
from this branch (the caller does that, under the cluster lock, and restores it).

    python3 scripts/test-plat06-live.py setup
    python3 scripts/test-plat06-live.py case-a      # manual Backup, no annotation
    python3 scripts/test-plat06-live.py case-b      # hostile annotation, ignored
    python3 scripts/test-plat06-live.py case-c      # scheduled Backup
    python3 scripts/test-plat06-live.py case-d      # controller restart mid-run
    python3 scripts/test-plat06-live.py case-e      # deleted Job under Forbid (both claim arms)
    python3 scripts/test-plat06-live.py case-f      # configuration snapshot equality
    python3 scripts/test-plat06-live.py case-g      # duplicate create is AlreadyExists
    python3 scripts/test-plat06-live.py report
    python3 scripts/test-plat06-live.py cleanup

Nothing here deletes anything it did not create: every object carries
`logweir.dev/test-owner=plat06` and the namespace is checked for that label
before anything is removed. Secret VALUES are never printed; pod logs are
redacted before they are saved.
"""

from __future__ import annotations

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

ROOT = pathlib.Path(__file__).resolve().parents[1]
STAMP = os.environ.get("LOGWEIR_PLAT06_STAMP", "20260916t0000z")
NS = f"lw-plat06-{STAMP}"
FIXTURE_NS = "logweir-scram-local"
OWNER = "plat06"
OUT = pathlib.Path(
    os.environ.get("LOGWEIR_PLAT06_OUT", "/tmp/logweir-roadmap-run/claude/artifacts/plat06")
)
STATE_PATH = OUT / "state.json"
K = ["kubectl", "--context", "docker-desktop"]
KN = K + ["-n", NS]
ARCHIVE_URL = os.environ.get("LOGWEIR_PLAT06_ARCHIVE", "s3://kafka-backups/scram-local")
ARCHIVE_BUCKET = "kafka-backups"
ARCHIVE_PREFIX = ARCHIVE_URL.split("//", 1)[1].split("/", 1)[1]
TOPICS = ["orders", "payments"]

OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE: dict[str, Any] = (
    json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {"namespace": NS, "cases": {}}
)


def save() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def run(args: list[str], *, data: str | None = None, check: bool = True, timeout: int = 120):
    result = subprocess.run(
        args, input=data, text=True, capture_output=True, timeout=timeout, cwd=ROOT
    )
    if check and result.returncode:
        raise RuntimeError(
            f"rc={result.returncode}: {args[:6]}\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )
    return result


def apply(obj: dict[str, Any]) -> dict[str, Any]:
    return json.loads(run(K + ["apply", "-f", "-", "-o", "json"], data=json.dumps(obj)).stdout)


def create(obj: dict[str, Any]) -> dict[str, Any]:
    return json.loads(run(K + ["create", "-f", "-", "-o", "json"], data=json.dumps(obj)).stdout)


def get(kind: str, name: str, namespace: str = NS) -> dict[str, Any]:
    return json.loads(run(K + ["-n", namespace, "get", kind, name, "-o", "json"]).stdout)


def get_opt(kind: str, name: str, namespace: str = NS) -> dict[str, Any] | None:
    result = run(K + ["-n", namespace, "get", kind, name, "-o", "json"], check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def artifact(name: str, body: Any) -> pathlib.Path:
    path = OUT / name
    path.write_text(body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True))
    return path


def owned_metadata(name: str) -> dict[str, Any]:
    return {"name": name, "namespace": NS, "labels": {"logweir.dev/test-owner": OWNER}}


def wait_for(
    kind: str,
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    seconds: int = 300,
    namespace: str = NS,
) -> dict[str, Any]:
    deadline = time.time() + seconds
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        last = get_opt(kind, name, namespace)
        if last is not None and predicate(last):
            return last
        time.sleep(3)
    raise RuntimeError(f"timeout waiting for {kind}/{name}: {json.dumps((last or {}).get('status'))}")


def terminal(o: dict[str, Any]) -> bool:
    return o.get("status", {}).get("phase") in {"Succeeded", "Failed", "Refused"}


def redact(text: str) -> str:
    """Pod logs and object dumps, with anything credential-shaped removed."""
    text = re.sub(r"(?i)(password|secret|access[-_]key)[\"'= :]+[^\s\"',}]+", r"\1=[REDACTED]", text)
    return re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.DOTALL,
    )


def pod_log(job: str) -> str:
    pods = json.loads(
        run(KN + ["get", "pods", "-l", f"batch.kubernetes.io/job-name={job}", "-o", "json"]).stdout
    )["items"]
    if not pods:
        return "<no pod>"
    name = pods[0]["metadata"]["name"]
    return redact(run(KN + ["logs", name, "--tail=40"], check=False).stdout)


def mc(*args: str, check: bool = True) -> str:
    return run(KN + ["exec", "plat06-mc", "--", "mc", *args], check=check, timeout=120).stdout


def archive_objects(path: str) -> list[dict[str, Any]]:
    """Every object under one bucket path, with size and etag, as `mc` reports it."""
    out = mc("ls", "--recursive", "--json", f"local/{ARCHIVE_BUCKET}/{path}", check=False)
    found = []
    for line in out.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("status") != "success" or "key" not in entry:
            continue
        found.append(
            {"key": entry["key"], "size": entry.get("size"), "etag": entry.get("etag")}
        )
    return sorted(found, key=lambda e: e["key"])


def archive_keys(execution_id: str) -> dict[str, list[dict[str, Any]]]:
    """The data set and the signed evidence one run identity owns."""
    return {
        "data": archive_objects(f"{ARCHIVE_PREFIX}/{execution_id}"),
        "evidence": archive_objects(f"logweir/backups/{execution_id}"),
    }


def evidence_bytes(key: str) -> bytes:
    """One evidence object, read back through the archive itself."""
    text = mc("cat", f"local/{ARCHIVE_BUCKET}/{key}")
    return text.encode()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# RBAC: what the SHIPPED ClusterRole actually grants, and how to ask
# ---------------------------------------------------------------------------

CONTROLLER_SA = f"system:serviceaccount:{FIXTURE_NS}:weirkeeper"
STATUS_KINDS = ["backupschedules", "backups", "kafkaclusters", "restores", "approvals"]
STATUS_VERBS = ["update", "patch", "create", "get", "list", "watch", "delete"]


def can_i(args: list[str]) -> str:
    """One `auth can-i` answer. `check=False`: `no` is exit 1, which is an
    ANSWER and not a failure, and the return code is read from the completed
    process rather than through a pipe (gate_lint)."""
    return run(K + ["auth", "can-i", *args, "--as=" + CONTROLLER_SA], check=False).stdout.strip()


def can_i_matrix() -> dict[str, Any]:
    """The controller's status-subresource grants, asked BOTH WAYS.

    `artifacts/w0-reservation/can-i-matrix-before.txt` answers `no` for every
    verb of every kind, which cannot describe a controller that demonstrably
    lists and watches its own resources. Two things produce that file and
    neither is the shipped role:

    * `auth can-i <verb> <resource>/status` is NOT a subresource query.
      kubectl parses `VERB TYPE[/NAME]`, so `status` is read as the object
      NAME and the answer describes the BASE resource — `patch` on
      `backupschedules` is `no` even though `backupschedules/status` is
      granted. Only `--subresource=status` asks the question meant.
    * The capture predates the installed role: the ClusterRole the fixture
      runs under today was created three minutes after that file's own
      timestamp, so the subject it asked about had no grants at all.

    Both forms are recorded here so the difference is evidence, not a claim.
    """
    matrix: dict[str, Any] = {
        "subject": CONTROLLER_SA,
        "namespace": FIXTURE_NS,
        "clusterRoleUid": run(
            K + ["get", "clusterrole", "weirkeeper", "-o", "jsonpath={.metadata.uid}"]
        ).stdout.strip(),
        "clusterRoleCreated": run(
            K
            + ["get", "clusterrole", "weirkeeper", "-o", "jsonpath={.metadata.creationTimestamp}"]
        ).stdout.strip(),
        "recordedAt": now(),
        "subresourceFlagForm": {},
        "pathForm": {},
        "baseResource": {},
    }
    for kind in STATUS_KINDS:
        matrix["subresourceFlagForm"][kind] = {
            verb: can_i([verb, kind, "--subresource=status", "-n", FIXTURE_NS])
            for verb in STATUS_VERBS
        }
        matrix["pathForm"][kind] = {
            verb: can_i([verb, f"{kind}/status", "-n", FIXTURE_NS]) for verb in STATUS_VERBS
        }
        matrix["baseResource"][kind] = {
            verb: can_i([verb, kind, "-n", FIXTURE_NS]) for verb in STATUS_VERBS
        }
    return matrix


def render_can_i_matrix(matrix: dict[str, Any]) -> str:
    lines = [
        "# `kubectl auth can-i` against the SHIPPED weirkeeper ClusterRole",
        f"# recorded(UTC): {matrix['recordedAt']}",
        "# context: docker-desktop",
        f"# subject: {matrix['subject']}",
        f"# namespace: {matrix['namespace']}",
        f"# ClusterRole weirkeeper uid: {matrix['clusterRoleUid']}"
        f"  created: {matrix['clusterRoleCreated']}",
        "#",
        "# FORM A  `can-i <verb> <kind> --subresource=status`   <- the status subresource",
        "# FORM B  `can-i <verb> <kind>/status`                 <- kind + object NAME 'status'",
        "# FORM C  `can-i <verb> <kind>`                        <- the base resource",
        "# FORM B == FORM C by construction; it never asks about a subresource.",
        "",
        f"{'VERB':<8} {'KIND':<16} {'A/subres':<10} {'B/path':<10} {'C/base':<10}",
    ]
    for kind in STATUS_KINDS:
        for verb in STATUS_VERBS:
            lines.append(
                f"{verb:<8} {kind:<16} "
                f"{matrix['subresourceFlagForm'][kind][verb]:<10} "
                f"{matrix['pathForm'][kind][verb]:<10} "
                f"{matrix['baseResource'][kind][verb]:<10}"
            )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Independent verification of a signed receipt
# ---------------------------------------------------------------------------

def _lab_signing_pub() -> str:
    """`$HOME/.logweir-lab/scram-e2e` first — the lab's durable home since
    2026-09-22 — and the `/tmp/logweir-scram-e2e` symlink to it second, because
    macOS tidies `/tmp` after three untouched days."""
    for base in (pathlib.Path.home() / ".logweir-lab" / "scram-e2e",
                 pathlib.Path("/tmp/logweir-scram-e2e")):
        if (base / "signing.pub.pem").is_file():
            return str(base / "signing.pub.pem")
    return "/tmp/logweir-scram-e2e/signing.pub.pem"


FIXTURE_PUBLIC_KEY = pathlib.Path(
    os.environ.get("LOGWEIR_PLAT06_PUBKEY") or _lab_signing_pub()
)


def verify_independently(tag: str, receipt: bytes, sidecar: bytes) -> dict[str, Any]:
    """`docs/verify_scorecard.py` — a reader that is not the controller and not
    the Rust engine — over the receipt bytes read back out of the archive."""
    if not FIXTURE_PUBLIC_KEY.is_file():
        raise RuntimeError(f"missing retained fixture public key {FIXTURE_PUBLIC_KEY}")
    doc = OUT / f"{tag}-verify-receipt.json"
    sig = OUT / f"{tag}-verify-receipt.sig"
    doc.write_bytes(receipt)
    sig.write_bytes(sidecar)
    proc = run(
        [
            os.environ.get("LOGWEIR_PYTHON", sys.executable),
            "docs/verify_scorecard.py",
            "--payload-type",
            "backup-receipt",
            str(doc),
            str(sig),
            str(FIXTURE_PUBLIC_KEY),
        ],
        check=False,
        timeout=120,
    )
    verdict = {
        "verifier": "docs/verify_scorecard.py",
        "interpreter": os.environ.get("LOGWEIR_PYTHON", sys.executable),
        "publicKeySha256": hashlib.sha256(FIXTURE_PUBLIC_KEY.read_bytes()).hexdigest(),
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip()[-400:],
    }
    artifact(f"{tag}-independent-verify.txt", json.dumps(verdict, indent=2, sort_keys=True))
    if proc.returncode != 0 or "VALID" not in proc.stdout:
        raise RuntimeError(f"{tag}: the independent verifier refused the receipt: {verdict}")
    return verdict


def backup_object(name: str, annotations: dict[str, str] | None = None) -> dict[str, Any]:
    metadata = owned_metadata(name)
    if annotations:
        metadata["annotations"] = annotations
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": metadata,
        "spec": {
            "sourceRef": {"name": "source"},
            "topics": TOPICS,
            "archive": {"url": ARCHIVE_URL, "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "deadlineSeconds": 600,
        },
    }


def schedule_object(name: str, policy: str) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": owned_metadata(name),
        "spec": {
            "schedule": "* * * * *",
            "sourceRef": {"name": "source"},
            "topics": TOPICS,
            "archive": {"url": ARCHIVE_URL, "secretRef": {"name": "logweir-s3"}},
            "concurrencyPolicy": policy,
            "suspend": False,
        },
    }


def copy_secret(name: str) -> None:
    source = get("secret", name, namespace=FIXTURE_NS)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned_metadata(name),
            "type": source.get("type", "Opaque"),
            "data": source["data"],
        }
    )


def setup() -> None:
    existing = get_opt("namespace", NS, namespace="default")
    if existing is None:
        run(K + ["create", "namespace", NS])
        run(K + ["label", "namespace", NS, f"logweir.dev/test-owner={OWNER}"])
    else:
        labels = existing["metadata"].get("labels", {})
        if labels.get("logweir.dev/test-owner") != OWNER:
            raise RuntimeError(f"refusing to reuse unowned namespace {NS}")
    STATE["namespace_uid"] = get("namespace", NS, namespace="default")["metadata"]["uid"]
    apply(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": owned_metadata("logweir-runner"),
            "automountServiceAccountToken": False,
        }
    )
    for secret in ["source-scram", "logweir-s3", "logweir-signing-key"]:
        copy_secret(secret)
    apply(
        {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "KafkaCluster",
            "metadata": owned_metadata("source"),
            "spec": {
                "bootstrapServers": [f"kafka-source.{FIXTURE_NS}.svc.cluster.local:9096"],
                "auth": {
                    "mode": "scramSha512",
                    "username": "scram-user",
                    "secretRef": {"name": "source-scram"},
                    "tls": False,
                },
                "role": "source",
            },
        }
    )
    if get_opt("pod", "plat06-mc") is None:
        apply(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": owned_metadata("plat06-mc"),
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "name": "mc",
                            "image": "minio/mc:latest",
                            "imagePullPolicy": "Never",
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                "mc alias set local "
                                f"http://minio.{FIXTURE_NS}.svc.cluster.local:9000 "
                                '"$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null '
                                "&& touch /tmp/ready && sleep 7200"
                            ],
                            "env": [
                                {
                                    "name": "AWS_ACCESS_KEY_ID",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "logweir-s3",
                                            "key": "access-key-id",
                                        }
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
                                "periodSeconds": 1,
                            },
                        }
                    ],
                },
            }
        )
        run(KN + ["wait", "--for=condition=Ready", "pod/plat06-mc", "--timeout=120s"])
    # NO RBAC COMPENSATION. An earlier revision of this harness granted the
    # controller `update` on `backupschedules/status` inside this namespace so
    # the Forbid cases could run while P0-RESERVE was open. The merged
    # `claude/w0-reservation` fix reserves a slot with a resourceVersion-
    # conditional merge PATCH, which the SHIPPED ClusterRole already
    # authorises (D-SEAMS S7), so the grant is gone: every scheduled case below
    # runs under the unmodified shipped role, which is what makes case-c the
    # P0-RESERVE proof rather than a rehearsal of it.
    STATE["canIMatrix"] = can_i_matrix()
    artifact("can-i-matrix.txt", render_can_i_matrix(STATE["canIMatrix"]))
    shipped = STATE["canIMatrix"]["subresourceFlagForm"]
    if shipped["backupschedules"]["patch"] != "yes" or shipped["backupschedules"]["update"] != "no":
        raise RuntimeError(f"the shipped role is not patch-only on status: {shipped}")
    cluster = wait_for(
        "kafkacluster", "source", lambda o: o.get("status", {}).get("reachable") is True
    )
    STATE["source_cluster_id"] = cluster["status"]["clusterId"]
    STATE["cases"]["setup"] = "passed"
    save()
    log(f"namespace {NS} ready; source clusterId={STATE['source_cluster_id']}")


# ---------------------------------------------------------------------------
# Shared assertions
# ---------------------------------------------------------------------------


def frozen_inputs(backup: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """The recorded execution and the ConfigMap it names, verified against it."""
    name = backup["metadata"]["name"]
    execution = backup["status"]["execution"]
    cm = get("configmap", execution["inputsRef"]["name"])
    snapshot = cm["data"]["execution-inputs.json"]
    digest = "sha256:" + hashlib.sha256(snapshot.encode()).hexdigest()
    if execution["inputsSha256"] != digest:
        raise RuntimeError(f"{name}: status digest {execution['inputsSha256']} != {digest}")
    if cm["metadata"]["annotations"]["logweir.dev/execution-inputs-sha256"] != digest:
        raise RuntimeError(f"{name}: the ConfigMap annotation is not the snapshot digest")
    if cm.get("immutable") is not True:
        raise RuntimeError(f"{name}: the plan ConfigMap is not immutable")
    if sorted(cm["data"]) != ["allowed-clusters.json", "backup.yaml", "execution-inputs.json"]:
        raise RuntimeError(f"{name}: unexpected plan keys {sorted(cm['data'])}")
    owners = cm["metadata"]["ownerReferences"]
    if len(owners) != 1 or owners[0]["uid"] != backup["metadata"]["uid"]:
        raise RuntimeError(f"{name}: the plan is not owned by exactly this Backup")
    body = json.dumps(cm)
    for forbidden in ["source-scram", "logweir-s3", "password", "access-key-id"]:
        if forbidden in body:
            raise RuntimeError(f"{name}: the plan names `{forbidden}`")
    return execution, cm


def job_facts(name: str) -> dict[str, Any]:
    job = get("job", name)
    container = job["spec"]["template"]["spec"]["containers"][0]
    return {
        "uid": job["metadata"]["uid"],
        "args": container["args"],
        "image": container["image"],
        "annotations": job["metadata"].get("annotations", {}),
        "templateAnnotations": job["spec"]["template"]["metadata"].get("annotations", {}),
        "volumes": [v["name"] for v in job["spec"]["template"]["spec"]["volumes"]],
        "planConfigMap": next(
            v["configMap"]["name"]
            for v in job["spec"]["template"]["spec"]["volumes"]
            if v["name"] == "plan"
        ),
        "env": {e["name"]: e.get("value", json.dumps(e.get("valueFrom"))) for e in container["env"]},
    }


def derived_argv(execution_id: str, trigger: str) -> list[str]:
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
        trigger,
        "--backup-id-override",
        execution_id,
    ]


def condition(o: dict[str, Any], kind: str) -> dict[str, Any] | None:
    for c in o.get("status", {}).get("conditions", []) or []:
        if c["type"] == kind:
            return c
    return None


def verify_succeeded(name: str, trigger: str, tag: str) -> dict[str, Any]:
    """The whole success contract for one run, recorded as artifacts."""
    backup = wait_for("backup", name, terminal, seconds=600)
    artifact(f"{tag}-backup.json", backup)
    status = backup["status"]
    if status.get("exitCode") != 0 or status.get("phase") != "Succeeded":
        artifact(f"{tag}-failed-log.txt", pod_log(name))
        raise RuntimeError(f"{name}: {json.dumps(status)}")
    execution, cm = frozen_inputs(backup)
    artifact(f"{tag}-plan-configmap.json", cm)
    snapshot = json.loads(cm["data"]["execution-inputs.json"])
    if snapshot["execution"]["trigger"] != trigger:
        raise RuntimeError(f"{name}: snapshot trigger {snapshot['execution']['trigger']}")
    if snapshot["execution"]["backup"]["uid"] != backup["metadata"]["uid"]:
        raise RuntimeError(f"{name}: snapshot is not bound to this object's UID")
    facts = job_facts(name)
    artifact(f"{tag}-job.json", facts)
    if facts["args"] != derived_argv(execution["id"], trigger):
        raise RuntimeError(f"{name}: Job args are not the derived argv: {facts['args']}")
    if facts["annotations"].get("logweir.dev/execution-inputs-sha256") != execution["inputsSha256"]:
        raise RuntimeError(f"{name}: the Job does not carry the recorded inputs digest")
    if status.get("backupId") != execution["id"]:
        raise RuntimeError(f"{name}: status.backupId {status.get('backupId')}")
    # The evidence, read back out of the archive itself.
    evidence = status["evidence"]
    receipt = evidence_bytes(evidence["receiptKey"])
    digest = "sha256:" + hashlib.sha256(receipt).hexdigest()
    if digest != evidence["receiptSha256"]:
        raise RuntimeError(f"{name}: receipt digest {digest} != {evidence['receiptSha256']}")
    document = json.loads(receipt)
    if document["backup_id"] != execution["id"]:
        raise RuntimeError(f"{name}: the receipt names backup_id {document['backup_id']}")
    if document["triggered_by"] != trigger:
        raise RuntimeError(f"{name}: the receipt names triggered_by {document['triggered_by']}")
    sidecar = evidence_bytes(evidence["sidecarKey"])
    artifact(f"{tag}-receipt.json", receipt.decode())
    artifact(f"{tag}-receipt.sig", sidecar.decode())
    artifact(f"{tag}-pod-log.txt", pod_log(name))
    independent = verify_independently(tag, receipt, sidecar)
    wait_for(
        "backup",
        name,
        lambda o: o["status"].get("evidence", {}).get("verification", {}).get("result") == "Valid",
        seconds=180,
    )
    record = {
        "namespace": NS,
        "backupUid": backup["metadata"]["uid"],
        "execution": execution,
        "configMapUid": cm["metadata"]["uid"],
        "configMapImmutable": cm.get("immutable"),
        "configMapKeys": sorted(cm["data"]),
        "jobUid": facts["uid"],
        "jobArgs": facts["args"],
        "jobImage": facts["image"],
        "podExitCode": status.get("exitCode"),
        "phase": status.get("phase"),
        "records": status.get("records"),
        "evidence": {k: evidence.get(k) for k in ["receiptKey", "sidecarKey", "receiptSha256"]},
        "independentVerification": independent,
        "receiptRecords": document.get("records"),
        "sourceClusterId": document["source"]["cluster_id"],
        "artifacts": [
            f"{tag}-backup.json",
            f"{tag}-plan-configmap.json",
            f"{tag}-job.json",
            f"{tag}-receipt.json",
            f"{tag}-receipt.sig",
            f"{tag}-pod-log.txt",
            f"{tag}-independent-verify.txt",
        ],
    }
    STATE["cases"][tag] = record
    save()
    log(f"{tag}: {name} succeeded as execution {execution['id']}")
    return backup


# ---------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------


def case_a() -> None:
    """A manual Backup with NO annotations runs, and its inputs are frozen."""
    name = "plat06-manual"
    created = create(backup_object(name))
    if created["metadata"].get("annotations", {}).get("logweir.dev/runner-argv"):
        raise RuntimeError("the fixture must carry no runner-argv annotation")
    backup = verify_succeeded(name, "manual", "case-a")
    if backup["status"]["execution"]["id"] != created["metadata"]["uid"]:
        raise RuntimeError("a manual run's identity is its object UID")
    if condition(backup, "RunnerArgvAnnotationIgnored") is not None:
        raise RuntimeError("an unannotated Backup raises no annotation condition")
    # Re-applying the same object is idempotent: same UID, same run, one Job.
    again = apply(backup_object(name))
    if again["metadata"]["uid"] != created["metadata"]["uid"]:
        raise RuntimeError("re-applying the same name must not mint a new object")
    jobs_for_name = [
        j["metadata"]["uid"]
        for j in json.loads(run(KN + ["get", "jobs", "-o", "json"]).stdout)["items"]
        if j["metadata"]["name"] == name
    ]
    if len(jobs_for_name) != 1:
        raise RuntimeError(f"re-applying the same name produced {len(jobs_for_name)} Jobs")

    # A DELIBERATE NEW NAME IS A NEW RUN — the contract PLAT-06.2's button needs.
    second = "plat06-manual-again"
    other = create(backup_object(second))
    other_backup = verify_succeeded(second, "manual", "case-a-second")
    if other_backup["status"]["execution"]["id"] == backup["status"]["execution"]["id"]:
        raise RuntimeError("two Backups shared one run identity")
    STATE["cases"]["case-a-idempotence"] = {
        "reappliedUid": again["metadata"]["uid"],
        "jobsForName": jobs_for_name,
        "firstIdentity": backup["status"]["execution"]["id"],
        "secondName": second,
        "secondUid": other["metadata"]["uid"],
        "secondIdentity": other_backup["status"]["execution"]["id"],
    }
    save()


HOSTILE_ARGV = [
    "backup",
    "run",
    "--spec",
    "/tmp/attacker-spec.yaml",
    "--allowed-clusters",
    "/tmp/attacker-allowed.json",
    "--signing-key",
    "/tmp/attacker-key.pem",
    "--receipt-out",
    "/work/receipt.json",
    "--triggered-by",
    "schedule",
    "--backup-id-override",
    "attacker-controlled-prefix",
]


def case_b() -> None:
    """A hostile runner-argv annotation is ignored, surfaced, and not executed."""
    name = "plat06-hostile-annotation"
    annotation = json.dumps(HOSTILE_ARGV, separators=(",", ":"))
    created = create(
        backup_object(name, annotations={"logweir.dev/runner-argv": annotation})
    )
    backup = verify_succeeded(name, "manual", "case-b")
    facts = job_facts(name)
    for token in ["attacker-spec.yaml", "attacker-key.pem", "attacker-controlled-prefix", "/tmp/"]:
        if any(token in arg for arg in facts["args"]):
            raise RuntimeError(f"the hostile annotation reached the argv: {facts['args']}")
    if backup["status"]["execution"]["id"] != created["metadata"]["uid"]:
        raise RuntimeError("the hostile backup id override was executed")
    surfaced = condition(backup, "RunnerArgvAnnotationIgnored")
    if surfaced is None or surfaced["status"] != "True":
        raise RuntimeError(f"the annotation was not surfaced: {json.dumps(backup['status'])}")
    digest = "sha256:" + hashlib.sha256(annotation.encode()).hexdigest()
    if digest not in surfaced["message"] or "DIFFERS" not in surfaced["message"]:
        raise RuntimeError(f"the condition does not name the annotation: {surfaced['message']}")
    for token in ["attacker", "/tmp/"]:
        if token in surfaced["message"]:
            raise RuntimeError("the annotation's content was echoed into the status")
    keys = archive_keys(backup["status"]["execution"]["id"])
    everything = archive_objects("")
    if any("attacker-controlled-prefix" in o["key"] for o in everything):
        raise RuntimeError("the hostile archive prefix exists in the bucket")
    STATE["cases"]["case-b-detail"] = {
        "annotationSha256": digest,
        "condition": surfaced,
        "archiveKeys": keys,
    }
    save()


def scheduled_child(schedule: str, seconds: int = 180) -> str:
    deadline = time.time() + seconds
    while time.time() < deadline:
        items = json.loads(run(KN + ["get", "backups", "-o", "json"]).stdout)["items"]
        owned = [
            o
            for o in items
            if o["spec"].get("scheduleRef", {}).get("name") == schedule
        ]
        if owned:
            return sorted(o["metadata"]["name"] for o in owned)[-1]
        time.sleep(3)
    raise RuntimeError(f"the schedule {schedule} fired no Backup")


class ScheduleWatch:
    """Every `BackupSchedule` revision the API SERVER recorded, not a sample.

    The reservation is the P0-RESERVE contract made observable: a `Forbid`
    admission writes `status.pendingBackupRef` BEFORE it creates the Backup and
    clears it after, with a resourceVersion-conditional merge PATCH that the
    shipped ClusterRole authorises (it grants `patch`, never `update`). That
    window is as short as the controller can make it, so POLLING MISSES IT —
    the first revision of this harness sampled at 200 ms and saw nothing, which
    proves only that the poll was slower than the window. A watch stream is
    delivered every revision, so `pendingBackupRef` being set and then cleared
    is read off the object's own history rather than inferred from its end
    state.
    """

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.handle = path.open("w")
        self.proc = subprocess.Popen(
            KN + ["get", "backupschedules", "--watch", "--output-watch-events", "-o", "json"],
            stdout=self.handle,
            stderr=subprocess.DEVNULL,
            text=True,
            cwd=ROOT,
        )
        time.sleep(2)  # let the initial LIST land before the caller creates anything

    def stop(self, name: str) -> list[dict[str, Any]]:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=30)
        self.handle.close()
        revisions: list[dict[str, Any]] = []
        decoder = json.JSONDecoder()
        text = self.path.read_text()
        index = 0
        while index < len(text):
            while index < len(text) and text[index] in " \t\r\n":
                index += 1
            if index >= len(text):
                break
            event, index = decoder.raw_decode(text, index)
            obj = event.get("object", {})
            if obj.get("metadata", {}).get("name") != name:
                continue
            status = obj.get("status", {}) or {}
            revisions.append(
                {
                    "type": event.get("type"),
                    "resourceVersion": obj["metadata"]["resourceVersion"],
                    "pendingBackupRef": status.get("pendingBackupRef"),
                    "activeBackupRef": status.get("activeBackupRef"),
                    "lastScheduledSlot": status.get("lastScheduledSlot"),
                }
            )
        return revisions


#: How long case-c waits for `status.pendingBackupRef` to clear after the
#: reservation was observed. The cycle takes ~1.5 s; sixty is forty times that,
#: so a congested node is absorbed and a reservation that never clears still
#: fails the case inside a minute.
RESERVATION_CLEAR_SECONDS = 60


def case_c() -> None:
    """A scheduled Backup under the SHIPPED role: the slot is reserved, exactly
    one Backup is created per slot, the reservation clears, and the identity is
    the schedule UID and slot. This is also the P0-RESERVE live proof."""
    policy = os.environ.get("LOGWEIR_PLAT06_POLICY", "Forbid")
    name = os.environ.get("LOGWEIR_PLAT06_SCHEDULE", "plat06-schedule")

    # FIRST, the grants the reservation runs under — no namespace-local Role
    # exists, so this is the shipped ClusterRole and nothing else.
    matrix = can_i_matrix()
    artifact("case-c-can-i-matrix.txt", render_can_i_matrix(matrix))
    shipped = matrix["subresourceFlagForm"]["backupschedules"]
    if shipped["patch"] != "yes":
        raise RuntimeError(f"the shipped role does not grant patch on status: {shipped}")
    if shipped["update"] != "no":
        raise RuntimeError(f"the shipped role grants update on status: {shipped}")
    local_roles = json.loads(run(KN + ["get", "rolebindings", "-o", "json"]).stdout)["items"]
    granting = [
        b["metadata"]["name"]
        for b in local_roles
        if any(s.get("name") == "weirkeeper" for s in b.get("subjects") or [])
    ]
    if granting:
        raise RuntimeError(f"this namespace grants the controller extra rights: {granting}")

    watch = ScheduleWatch(OUT / f"case-c-{name}-watch.json")
    schedule = apply(schedule_object(name, policy))
    child = scheduled_child(name)
    reservation = watch.stop(name)
    run(
        KN
        + [
            "patch",
            "backupschedule",
            name,
            "--type=merge",
            "-p",
            json.dumps({"spec": {"suspend": True}}),
        ]
    )
    reserved = [s for s in reservation if s["pendingBackupRef"]]
    if not reserved:
        raise RuntimeError(
            "no watched revision of the schedule carried status.pendingBackupRef; "
            f"{len(reservation)} revisions seen"
        )
    # WAITED FOR, NOT READ ONCE. The reservation is written, the Backup is
    # created, and the reservation is cleared by a LATER status write — about
    # 1.5 s end to end on lab-refresh-7's build (reservation seen at +0.2 s,
    # cleared at +1.4 s after it appeared). A single read taken the instant
    # after the suspend patch lands inside that window and blamed the product
    # for the harness's timing (lab-refresh-7, 03:40Z). It still FAILS when the
    # reservation is never cleared: a `pendingBackupRef` still set at the
    # deadline is exactly the defect this clause exists to catch.
    clear_deadline = time.time() + RESERVATION_CLEAR_SECONDS
    cleared = get("backupschedule", name)["status"].get("pendingBackupRef")
    while cleared and time.time() < clear_deadline:
        time.sleep(0.5)
        cleared = get("backupschedule", name)["status"].get("pendingBackupRef")
    if cleared:
        raise RuntimeError(
            f"the reservation was never cleared within {RESERVATION_CLEAR_SECONDS}s: {cleared}"
        )
    # The reservation NAMES the Backup the very next revision creates, and the
    # slot it reserves is the slot that Backup carries.
    reserved_names = sorted({json.dumps(s["pendingBackupRef"], sort_keys=True) for s in reserved})

    # EXACTLY ONE BACKUP PER SLOT, counted over every child the schedule owns.
    children = [
        o
        for o in json.loads(run(KN + ["get", "backups", "-o", "json"]).stdout)["items"]
        if o["spec"].get("scheduleRef", {}).get("name") == name
    ]
    per_slot: dict[str, list[str]] = {}
    for o in children:
        per_slot.setdefault(o["spec"]["slot"], []).append(o["metadata"]["name"])
    duplicated = {slot: names for slot, names in per_slot.items() if len(names) != 1}
    if duplicated:
        raise RuntimeError(f"a slot produced more than one Backup: {duplicated}")

    created = get("backup", child)
    if created["metadata"].get("annotations", {}).get("logweir.dev/runner-argv"):
        raise RuntimeError("a scheduled Backup must carry no runner-argv annotation")
    backup = verify_succeeded(child, "schedule", "case-c")
    expected = f"{schedule['metadata']['uid']}-{created['spec']['slot']}"
    if backup["status"]["execution"]["id"] != expected:
        raise RuntimeError(f"the scheduled identity is {backup['status']['execution']['id']}")
    STATE["cases"]["case-c-detail"] = {
        "policy": policy,
        "scheduleUid": schedule["metadata"]["uid"],
        "child": child,
        "childUid": created["metadata"]["uid"],
        "slot": created["spec"]["slot"],
        "expectedIdentity": expected,
        "scheduleName": name,
        "watchedRevisions": reservation,
        "reservationObserved": True,
        "reservationsSeen": reserved_names,
        "reservationRevisions": [s["resourceVersion"] for s in reserved],
        "reservationCleared": True,
        "watchArtifact": f"case-c-{name}-watch.json",
        "backupsPerSlot": per_slot,
        "canIMatrixArtifact": "case-c-can-i-matrix.txt",
        "shippedRoleStatusGrants": shipped,
        "namespaceRoleBindingsForController": granting,
        "scheduleStatus": get("backupschedule", name)["status"],
    }
    save()
    log(
        f"case-c[{name}]: {len(children)} child(ren) over {len(per_slot)} slot(s); "
        f"watched revisions={len(reservation)}, reservations observed={len(reserved)}"
    )


def controller_pod() -> str:
    pods = json.loads(
        run(
            K
            + ["-n", FIXTURE_NS, "get", "pods", "-l", "app.kubernetes.io/name=logweir", "-o", "json"]
        ).stdout
    )["items"]
    running = [
        p["metadata"]["name"]
        for p in pods
        if p["metadata"]["name"].startswith("weirkeeper") and p["status"]["phase"] == "Running"
    ]
    if len(running) != 1:
        raise RuntimeError(f"expected exactly one controller pod, saw {running}")
    return running[0]


def case_d() -> None:
    """A controller restart MID-RUN creates no second Job and changes no input.

    A run against this fixture finishes in about ten seconds, so "delete the
    controller pod and hope" would restart it against a Backup that had already
    gone terminal and would prove nothing. The namespace is held at zero pods
    first: the Job exists and is owned and its inputs are frozen, but its pod
    cannot be admitted, so the Backup is DEMONSTRABLY nonterminal across the
    whole restart. The hold is released afterwards and the same Job runs.
    """
    name = "plat06-restart"
    apply(
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": owned_metadata("plat06-restart-hold"),
            "spec": {"hard": {"pods": "0"}},
        }
    )
    create(backup_object(name))
    backup = wait_for(
        "backup",
        name,
        lambda o: o.get("status", {}).get("jobRef") is not None
        and o.get("status", {}).get("execution") is not None,
        seconds=300,
    )
    if terminal(backup):
        raise RuntimeError("the Backup went terminal before the restart; the hold did not hold")
    before = {
        "execution": backup["status"]["execution"],
        "phase": backup["status"].get("phase"),
        "job": job_facts(name),
        "configMap": get("configmap", f"{name}-plan"),
        "controllerPod": controller_pod(),
    }
    before["configMapUid"] = before["configMap"]["metadata"]["uid"]
    run(K + ["-n", FIXTURE_NS, "delete", "pod", before["controllerPod"], "--wait=false"])
    log(f"deleted controller pod {before['controllerPod']} mid-run")
    run(
        K
        + ["-n", FIXTURE_NS, "rollout", "status", "deployment/weirkeeper", "--timeout=240s"],
        timeout=260,
    )
    restarted = controller_pod()
    if restarted == before["controllerPod"]:
        raise RuntimeError("the controller pod did not actually change")

    # STILL MID-RUN, under a controller that has never seen this object before:
    # give it a reconcile window, then read what it did with the frozen inputs.
    time.sleep(30)
    during = get("backup", name)
    if terminal(during):
        raise RuntimeError(f"the Backup went terminal while held: {json.dumps(during['status'])}")
    mid_job = job_facts(name)
    mid_cm = get("configmap", f"{name}-plan")
    if mid_job["uid"] != before["job"]["uid"]:
        raise RuntimeError("the restarted controller replaced the Job")
    if mid_cm["metadata"]["uid"] != before["configMapUid"] or mid_cm["data"] != before[
        "configMap"
    ]["data"]:
        raise RuntimeError("the restarted controller rewrote the frozen inputs")
    if during["status"]["execution"] != before["execution"]:
        raise RuntimeError("the restarted controller rewrote status.execution")

    run(KN + ["delete", "resourcequota", "plat06-restart-hold"])
    backup = verify_succeeded(name, "manual", "case-d")
    after = {
        "execution": backup["status"]["execution"],
        "job": job_facts(name),
        "configMapUid": get("configmap", f"{name}-plan")["metadata"]["uid"],
        "controllerPod": controller_pod(),
    }
    jobs = json.loads(run(KN + ["get", "jobs", "-o", "json"]).stdout)["items"]
    mine = [j for j in jobs if j["metadata"]["name"] == name]
    if len(mine) != 1 or after["job"]["uid"] != before["job"]["uid"]:
        raise RuntimeError("the restart created a second Job")
    for field in ["execution", "configMapUid"]:
        if before[field] != after[field]:
            raise RuntimeError(f"the restart changed {field}: {before[field]} -> {after[field]}")
    if before["job"]["args"] != after["job"]["args"]:
        raise RuntimeError("the restart changed the Job args")
    before.pop("configMap")
    STATE["cases"]["case-d-detail"] = {
        "heldAtZeroPodsAcrossRestart": True,
        "before": before,
        "duringPhase": during["status"].get("phase"),
        "duringJobUid": mid_job["uid"],
        "after": after,
        "controllerPodBefore": before["controllerPod"],
        "controllerPodAfter": after["controllerPod"],
    }
    save()
    log(
        f"case-d: controller {before['controllerPod']} -> {after['controllerPod']}, "
        f"Job {before['job']['uid']} unchanged across the restart"
    )


# ---------------------------------------------------------------------------
# Case e — a deleted Job under Forbid, and RECEIPT-DUP's execution claim
# ---------------------------------------------------------------------------
#
# SINCE RECEIPT-DUP (2026-09-23) a re-created Job never runs the engine a second
# time over one execution. Every runner claims its execution id with a
# create-only `logweir/backups/<id>/execution.claim.json` before its engine
# starts, so:
#
# * CLAIMED arm — the deleted Job's pod reached the claim (here: deleted with
#   `--cascade=orphan`, so it runs to completion and signs receipt 1). The
#   re-created Job exits 1 `ExecutionAlreadyClaimed`, the Backup ends `Failed`
#   with that `exitReason`, and the execution holds exactly ONE receipt, which
#   verifies and whose manifest still hashes to the digest it attests.
# * UNCLAIMED arm — the deleted Job's pod never existed (a `pods: 0` quota was
#   in place before the schedule fired). The re-created Job runs normally and
#   the Backup ends `Succeeded` with one receipt, as before.
#
# The two judges below are PURE, so `scripts/test_plat06_case_e_rows.py` runs
# them — and their negative controls, including the pre-fix outcome — with no
# cluster.

CLAIM_FILE = "execution.claim.json"


def _receipt_failures(f: dict[str, Any]) -> list[str]:
    out: list[str] = []
    receipts = f.get("receipts") or []
    if len(receipts) != 1:
        out.append(f"expected exactly ONE receipt for the execution, found {len(receipts)}")
        return out
    r = receipts[0]
    if not r.get("verifierValid"):
        out.append(f"the independent verifier did not say VALID for {r.get('key')}")
    if not r.get("manifestSha256Attested") or (
        r.get("manifestSha256Attested") != r.get("manifestSha256Actual")
    ):
        out.append(
            f"the manifest hashes to {r.get('manifestSha256Actual')}, the receipt attests "
            f"{r.get('manifestSha256Attested')}"
        )
    claim = f.get("claim")
    if not claim:
        out.append(f"no {CLAIM_FILE} for the execution")
    elif claim.get("run_id") != r.get("runId"):
        out.append(
            f"the claim names run {claim.get('run_id')}, the receipt is run {r.get('runId')}"
        )
    return out


def judge_case_e_claimed(f: dict[str, Any]) -> list[str]:
    """Every way the CLAIMED arm can be wrong; empty means it held."""
    out: list[str] = []
    if f.get("phase") != "Failed" or f.get("exitCode") != 1:
        out.append(f"the Backup is {f.get('phase')}/exit {f.get('exitCode')}, not Failed/exit 1")
    if f.get("exitReason") != "ExecutionAlreadyClaimed":
        out.append(f"status.exitReason is {f.get('exitReason')!r}, not ExecutionAlreadyClaimed")
    rerun = f.get("rerunLog") or ""
    if "failure-reason=ExecutionAlreadyClaimed" not in rerun:
        out.append("the re-created pod's log does not name ExecutionAlreadyClaimed")
    if "progress-phase=-1:engine" in rerun:
        out.append("the re-created pod STARTED THE ENGINE over a claimed execution")
    out.extend(_receipt_failures(f))
    return out


def judge_case_e_unclaimed(f: dict[str, Any]) -> list[str]:
    """Every way the UNCLAIMED arm can be wrong; empty means it held."""
    out: list[str] = []
    if f.get("phase") != "Succeeded" or f.get("exitCode") != 0:
        out.append(f"the Backup is {f.get('phase')}/exit {f.get('exitCode')}, not Succeeded/exit 0")
    out.extend(_receipt_failures(f))
    return out


def pods_of_job(job_uid: str, job_name: str) -> list[dict[str, Any]]:
    """The pods one Job UID owns — never an orphaned pod of an earlier Job
    that still carries the same job-name label."""
    pods = json.loads(
        run(KN + ["get", "pods", "-l", f"batch.kubernetes.io/job-name={job_name}", "-o", "json"]).stdout
    )["items"]
    return [
        p
        for p in pods
        if any(o.get("uid") == job_uid for o in p["metadata"].get("ownerReferences") or [])
    ]


def orphaned_pods(job_name: str) -> list[dict[str, Any]]:
    pods = json.loads(
        run(KN + ["get", "pods", "-l", f"batch.kubernetes.io/job-name={job_name}", "-o", "json"]).stdout
    )["items"]
    return [p for p in pods if not p["metadata"].get("ownerReferences")]


def execution_evidence(execution_id: str, tag: str) -> dict[str, Any]:
    """Every receipt and the claim under `logweir/backups/<id>/`, each receipt
    verified independently and its manifest re-hashed out of the archive."""
    keys = [e["key"] for e in archive_objects(f"logweir/backups/{execution_id}")]
    base = f"logweir/backups/{execution_id}/"
    claim = None
    if any(k.endswith(CLAIM_FILE) for k in keys):
        claim = json.loads(evidence_bytes(base + CLAIM_FILE))
    receipts = []
    for k in sorted(k for k in keys if k.endswith(".receipt.json")):
        key = base + k.rsplit("/", 1)[-1]
        body = evidence_bytes(key)
        sidecar = evidence_bytes(key[: -len(".json")] + ".sig")
        doc = json.loads(body)
        try:
            verify_independently(f"{tag}-{doc['run_id']}", body, sidecar)
            valid = True
        except RuntimeError:
            valid = False
        manifest_key = doc["archive"]["manifest_key"]
        manifest = mc("cat", f"local/{ARCHIVE_BUCKET}/{manifest_key}", check=False).encode()
        receipts.append(
            {
                "key": key,
                "runId": doc["run_id"],
                "verifierValid": valid,
                "manifestKey": manifest_key,
                "manifestSha256Attested": doc["archive"]["manifest_sha256"],
                "manifestSha256Actual": "sha256:" + hashlib.sha256(manifest).hexdigest(),
            }
        )
    return {"evidenceKeys": keys, "claim": claim, "receipts": receipts}


def case_e() -> None:
    """Under Forbid, a deleted Job is re-created from the SAME frozen inputs and
    the schedule blocks the next slot meanwhile. RECEIPT-DUP: the re-created Job
    runs the engine only when the deleted Job's pod never claimed the execution.
    Both arms run; each is judged by its pure judge."""
    case_e_unclaimed()
    case_e_claimed()


def case_e_claimed() -> None:
    name = "plat06-forbid"
    schedule = apply(schedule_object(name, "Forbid"))
    child = scheduled_child(name)
    wait_for(
        "backup",
        child,
        lambda o: o.get("status", {}).get("jobRef") is not None
        and o.get("status", {}).get("execution") is not None,
        seconds=300,
    )
    backup = get("backup", child)
    execution_id = backup["status"]["execution"]["id"]
    first = job_facts(child)
    # The first pod must EXIST before the hold, or this is the unclaimed arm.
    deadline = time.time() + 180
    while not pods_of_job(first["uid"], child):
        if time.time() > deadline:
            raise RuntimeError("the first Job never created its pod")
        time.sleep(2)
    cm = get("configmap", f"{child}-plan")
    before_keys = archive_keys(execution_id)
    schedule_before = get("backupschedule", name)["status"]

    # Hold the namespace at zero NEW pods so the re-created Job cannot start
    # while a slot comes due: the Backup stays nonterminal and Forbid is
    # observable. The first pod already exists, and `--cascade=orphan` keeps it
    # running to completion — it claims the execution and signs receipt 1.
    apply(
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": owned_metadata("plat06-hold"),
            "spec": {"hard": {"pods": "0"}},
        }
    )
    run(KN + ["delete", "job", child, "--cascade=orphan"])
    log(f"deleted the running Job {child} (uid {first['uid']}) and orphaned its pod")
    second_job = wait_for(
        "job",
        child,
        lambda o: o["metadata"]["uid"] != first["uid"],
        seconds=120,
    )
    blocked = wait_for(
        "backupschedule",
        name,
        lambda o: (condition(o, "Ready") or {}).get("reason") == "ConcurrencyBlocked",
        seconds=180,
    )
    children_while_blocked = sorted(
        o["metadata"]["name"]
        for o in json.loads(run(KN + ["get", "backups", "-o", "json"]).stdout)["items"]
        if o["spec"].get("scheduleRef", {}).get("name") == name
    )
    if children_while_blocked != [child]:
        raise RuntimeError(f"Forbid admitted a second child: {children_while_blocked}")
    # The orphaned first pod finishes on its own.
    deadline = time.time() + 600
    orphan: dict[str, Any] | None = None
    while time.time() < deadline:
        pods = orphaned_pods(child)
        orphan = pods[0] if pods else None
        if orphan and orphan["status"].get("phase") in {"Succeeded", "Failed"}:
            break
        time.sleep(3)
    if not orphan or orphan["status"].get("phase") != "Succeeded":
        raise RuntimeError(f"the orphaned first pod did not succeed: {json.dumps((orphan or {}).get('status'))}")
    artifact("case-e-claimed-first-pod-log.txt",
             redact(run(KN + ["logs", orphan["metadata"]["name"], "--tail=40"], check=False).stdout))
    mid = get("backup", child)
    if terminal(mid):
        raise RuntimeError("the Backup went terminal while its re-created Job was held")

    run(KN + ["delete", "resourcequota", "plat06-hold"])
    backup = wait_for("backup", child, terminal, seconds=600)
    artifact("case-e-claimed-backup.json", backup)
    second = job_facts(child)
    rerun_pods = pods_of_job(second["uid"], child)
    rerun_log = (
        redact(run(KN + ["logs", rerun_pods[0]["metadata"]["name"], "--tail=40"], check=False).stdout)
        if rerun_pods
        else ""
    )
    artifact("case-e-claimed-rerun-pod-log.txt", rerun_log)
    evidence = execution_evidence(execution_id, "case-e-claimed")
    status = backup["status"]
    facts = {
        "phase": status.get("phase"),
        "exitCode": status.get("exitCode"),
        "exitReason": status.get("exitReason"),
        "conditionMessage": (condition(backup, "Failed") or {}).get("message"),
        "rerunLog": rerun_log,
        **evidence,
    }
    artifact("case-e-claimed-facts.json", facts)
    failures = judge_case_e_claimed(facts)
    if failures:
        raise RuntimeError("case-e claimed arm: " + "; ".join(failures))

    cm_after = get("configmap", f"{child}-plan")
    if cm_after["metadata"]["uid"] != cm["metadata"]["uid"] or cm_after["data"] != cm["data"]:
        raise RuntimeError("the frozen inputs ConfigMap was replaced or changed")
    if second["args"] != first["args"] or second["volumes"] != first["volumes"]:
        raise RuntimeError("the re-created Job is not the same execution")
    if second["annotations"] != first["annotations"]:
        raise RuntimeError("the re-created Job carries different execution annotations")

    # After the run is terminal the schedule admits the next slot again.
    next_child = None
    deadline = time.time() + 180
    while time.time() < deadline:
        names = sorted(
            o["metadata"]["name"]
            for o in json.loads(run(KN + ["get", "backups", "-o", "json"]).stdout)["items"]
            if o["spec"].get("scheduleRef", {}).get("name") == name
        )
        if len(names) > 1:
            next_child = [n for n in names if n != child][-1]
            break
        time.sleep(3)
    if next_child is None:
        raise RuntimeError("the schedule admitted no next slot after the run was terminal")
    schedule_after = get("backupschedule", name)["status"]
    run(KN + ["patch", "backupschedule", name, "--type=merge", "-p",
              json.dumps({"spec": {"suspend": True}})])
    STATE["cases"]["case-e-claimed"] = {
        "scheduleUid": schedule["metadata"]["uid"],
        "backupUid": backup["metadata"]["uid"],
        "execution": status.get("execution"),
        "configMapUid": cm["metadata"]["uid"],
        "firstJob": first,
        "secondJob": second,
        "secondJobUid": second_job["metadata"]["uid"],
        "orphanedFirstPod": orphan["metadata"]["name"],
        "scheduleBefore": schedule_before,
        "scheduleBlocked": blocked["status"],
        "scheduleAfter": schedule_after,
        "nextChild": next_child,
        "archiveKeysBeforeDelete": before_keys,
        "archiveKeysAfterRerun": archive_keys(execution_id),
        "facts": {k: v for k, v in facts.items() if k != "rerunLog"},
    }
    save()
    log(f"case-e claimed: {child} Failed/ExecutionAlreadyClaimed; one receipt, verified")


def case_e_unclaimed() -> None:
    name = "plat06-forbid-unclaimed"
    # The hold goes in FIRST: the first Job's pod is never created, so it
    # never reaches the claim.
    apply(
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": owned_metadata("plat06-hold-early"),
            "spec": {"hard": {"pods": "0"}},
        }
    )
    apply(schedule_object(name, "Forbid"))
    child = scheduled_child(name)
    wait_for(
        "backup",
        child,
        lambda o: o.get("status", {}).get("jobRef") is not None
        and o.get("status", {}).get("execution") is not None,
        seconds=300,
    )
    first = job_facts(child)
    if pods_of_job(first["uid"], child):
        raise RuntimeError("the hold did not stop the first Job's pod; this is not the unclaimed arm")
    run(KN + ["delete", "job", child, "--cascade=background"])
    wait_for("job", child, lambda o: o["metadata"]["uid"] != first["uid"], seconds=120)
    run(KN + ["delete", "resourcequota", "plat06-hold-early"])
    backup = verify_succeeded(child, "schedule", "case-e-unclaimed")
    execution_id = backup["status"]["execution"]["id"]
    evidence = execution_evidence(execution_id, "case-e-unclaimed")
    status = backup["status"]
    facts = {"phase": status.get("phase"), "exitCode": status.get("exitCode"), **evidence}
    artifact("case-e-unclaimed-facts.json", facts)
    failures = judge_case_e_unclaimed(facts)
    if failures:
        raise RuntimeError("case-e unclaimed arm: " + "; ".join(failures))
    run(KN + ["patch", "backupschedule", name, "--type=merge", "-p",
              json.dumps({"spec": {"suspend": True}})])
    STATE["cases"]["case-e-unclaimed"] = {"backupUid": backup["metadata"]["uid"], "facts": facts}
    save()
    log(f"case-e unclaimed: {child} Succeeded after a Job deleted before its pod existed")


def case_f() -> None:
    """Configuration snapshot equality: two Backups with identical inputs freeze
    byte-identical `execution-inputs.json` APART FROM IDENTITY."""
    names = ["plat06-snapshot-one", "plat06-snapshot-two"]
    frozen: dict[str, Any] = {}
    for name in names:
        create(backup_object(name))
        backup = wait_for(
            "backup",
            name,
            lambda o: o.get("status", {}).get("execution") is not None,
            seconds=300,
        )
        execution, cm = frozen_inputs(backup)
        frozen[name] = {
            "uid": backup["metadata"]["uid"],
            "execution": execution,
            "configMapUid": cm["metadata"]["uid"],
            "snapshot": cm["data"]["execution-inputs.json"],
            "backupYaml": cm["data"]["backup.yaml"],
            "allowedClusters": cm["data"]["allowed-clusters.json"],
        }
    one, two = (frozen[n] for n in names)

    # The two snapshots are the same bytes once identity is removed, and the
    # identity is the ONLY thing that differs. Three places carry it and all
    # three are DERIVED from the same one value: `execution.id`,
    # `execution.backup` (the object's name and UID), and the
    # `--backup-id-override` token of the argv the controller derives. Every
    # other field — source connection, topics, archive, runner settings and
    # deadline — must be equal, and anything else differing is real drift.
    def strip_identity(raw: str) -> dict[str, Any]:
        doc = json.loads(raw)
        identity = doc["execution"]["id"]
        doc["execution"]["id"] = "<identity>"
        doc["execution"]["backup"] = "<identity>"
        args = doc["runner"]["args"]
        if identity not in args:
            raise RuntimeError("the derived argv does not carry the run identity")
        doc["runner"]["args"] = ["<identity>" if a == identity else a for a in args]
        return doc

    stripped_one = strip_identity(one["snapshot"])
    stripped_two = strip_identity(two["snapshot"])
    if stripped_one != stripped_two:
        differing = sorted(
            k for k in set(stripped_one) | set(stripped_two)
            if stripped_one.get(k) != stripped_two.get(k)
        )
        artifact("case-f-snapshot-one.json", one["snapshot"])
        artifact("case-f-snapshot-two.json", two["snapshot"])
        raise RuntimeError(f"identical inputs froze different snapshots: {differing}")
    if one["snapshot"] == two["snapshot"]:
        raise RuntimeError("two distinct runs froze byte-identical snapshots INCLUDING identity")
    if one["execution"]["inputsSha256"] == two["execution"]["inputsSha256"]:
        raise RuntimeError("two distinct runs share an inputs digest")
    canonical_one = json.dumps(stripped_one, sort_keys=True, separators=(",", ":"))
    canonical_two = json.dumps(stripped_two, sort_keys=True, separators=(",", ":"))
    identity_free_digest = hashlib.sha256(canonical_one.encode()).hexdigest()
    if hashlib.sha256(canonical_two.encode()).hexdigest() != identity_free_digest:
        raise RuntimeError("the identity-free canonical forms disagree")

    # The two rendered documents differ ONLY where the identity appears.
    for key in ["allowedClusters"]:
        if one[key] != two[key]:
            raise RuntimeError(f"identical inputs rendered different {key}")

    artifact("case-f-snapshot-one.json", one["snapshot"])
    artifact("case-f-snapshot-two.json", two["snapshot"])
    for name in names:
        verify_succeeded(name, "manual", f"case-f-{name.rsplit('-', 1)[1]}")
    STATE["cases"]["case-f-detail"] = {
        "names": names,
        "uids": {n: frozen[n]["uid"] for n in names},
        "executions": {n: frozen[n]["execution"] for n in names},
        "configMapUids": {n: frozen[n]["configMapUid"] for n in names},
        "identityFreeSnapshotSha256": identity_free_digest,
        "snapshotBytesEqualAfterIdentityRemoval": True,
        "allowedClustersEqual": True,
        "artifacts": ["case-f-snapshot-one.json", "case-f-snapshot-two.json"],
    }
    save()
    log(f"case-f: identity-free snapshot sha256={identity_free_digest}")


def case_g() -> None:
    """Creating the same Backup name twice is the API server's `AlreadyExists`,
    not a second run and not a controller decision."""
    name = "plat06-duplicate"
    first = create(backup_object(name))
    second = run(
        K + ["create", "-f", "-", "-o", "json"],
        data=json.dumps(backup_object(name)),
        check=False,
    )
    if second.returncode == 0:
        raise RuntimeError("the second create succeeded")
    message = (second.stderr + second.stdout).strip()
    if "AlreadyExists" not in message and "already exists" not in message:
        raise RuntimeError(f"the second create failed for another reason: {message}")
    # It is the API SERVER's refusal: the status is `AlreadyExists`/409 and the
    # controller never saw a second object.
    raw = run(
        K
        + [
            "create",
            "--raw",
            f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups",
            "-f",
            "-",
        ],
        data=json.dumps(backup_object(name)),
        check=False,
    )
    api_body = (raw.stdout + raw.stderr).strip()
    if '"reason":"AlreadyExists"' not in api_body and "AlreadyExists" not in api_body:
        raise RuntimeError(f"the raw API create did not answer AlreadyExists: {api_body[:400]}")
    after = json.loads(run(KN + ["get", "backups", "-o", "json"]).stdout)["items"]
    same_name = [o for o in after if o["metadata"]["name"] == name]
    if len(same_name) != 1 or same_name[0]["metadata"]["uid"] != first["metadata"]["uid"]:
        raise RuntimeError("the duplicate create changed the object")
    backup = verify_succeeded(name, "manual", "case-g")
    jobs = [
        j["metadata"]["uid"]
        for j in json.loads(run(KN + ["get", "jobs", "-o", "json"]).stdout)["items"]
        if j["metadata"]["name"] == name
    ]
    if len(jobs) != 1:
        raise RuntimeError(f"the duplicate create produced {len(jobs)} Jobs")
    artifact("case-g-alreadyexists.txt", redact(message + "\n---\n" + api_body))
    STATE["cases"]["case-g-detail"] = {
        "name": name,
        "uid": first["metadata"]["uid"],
        "execution": backup["status"]["execution"],
        "secondCreateReturnCode": second.returncode,
        "rawApiReturnCode": raw.returncode,
        "jobsForName": jobs,
        "artifacts": ["case-g-alreadyexists.txt"],
    }
    save()
    log(f"case-g: duplicate create refused by the API server (rc={second.returncode})")


def report() -> None:
    artifact("report.json", STATE)
    print(json.dumps(STATE, indent=2, sort_keys=True))


def cleanup() -> None:
    ns = get_opt("namespace", NS, namespace="default")
    if ns is None:
        log("namespace already gone")
        return
    if ns["metadata"].get("labels", {}).get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError("refusing to delete a namespace this run does not own")
    if ns["metadata"]["uid"] != STATE.get("namespace_uid"):
        raise RuntimeError("the namespace UID does not match this run's state")
    run(K + ["delete", "namespace", NS, "--wait=true"], timeout=300)
    STATE["cleanup"] = {"deletedAt": now(), "namespaceUid": ns["metadata"]["uid"]}
    save()
    log(f"deleted namespace {NS}")


PHASES = {
    "setup": setup,
    "case-a": case_a,
    "case-b": case_b,
    "case-c": case_c,
    "case-d": case_d,
    "case-e": case_e,
    # Each arm alone (lab-refresh-10): the receipt-dup negative control runs the
    # CLAIMED arm under the pre-fix runner, and `case-e` stops at the first arm
    # that fails, which under that runner is the unclaimed one.
    "case-e-claimed": case_e_claimed,
    "case-e-unclaimed": case_e_unclaimed,
    "case-f": case_f,
    "case-g": case_g,
    "report": report,
    "cleanup": cleanup,
}


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in PHASES:
        print(f"usage: {sys.argv[0]} [{'|'.join(PHASES)}]", file=sys.stderr)
        raise SystemExit(2)
    PHASES[sys.argv[1]]()
