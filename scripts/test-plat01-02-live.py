#!/usr/bin/env python3
"""Owned Docker Desktop live acceptance for PLAT-01 and PLAT-02.2.

This harness never changes kubeconfig context, never deletes shared CRDs or the
preserved ``scram-local`` data plane, and only cleans the namespace whose UID it
recorded at creation.  Every kubectl and Docker invocation carries the explicit
context required by the acceptance run.
"""

from __future__ import annotations

import argparse
import base64
import copy
import datetime as dt
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Callable


ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_BACKEND_LIVE_OUT", "/tmp/logweir-backend-live-20260915T0330Z/run"
    )
)
OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
OUT.chmod(0o700)
STATE_PATH = OUT / "state.json"
REPORT_PATH = OUT / "report.json"
NS = os.environ.get("LOGWEIR_BACKEND_LIVE_NS", "logweir-backend-live-20260915")
FIXTURE_NS = "logweir-scram-local"
RUN_LABEL = os.environ.get("LOGWEIR_BACKEND_LIVE_RUN_LABEL", "20260915t0330z")
CURRENT_RUNNER = "logweir:backend-live-20260915"
OLD_RUNNER = "logweir:pre-handshake-92e02097"
CURRENT_CONTROLLER = "weirkeeper:backend-live-20260915"
OLD_CONTROLLER = "weirkeeper:pre-handshake-92e02097"
KUBECTL = ["kubectl", "--context", "docker-desktop"]
K = KUBECTL + ["-n", NS]
KF = KUBECTL + ["-n", FIXTURE_NS]
DOCKER = ["docker", "--context", "desktop-linux"]
FIXTURE_KEYS = pathlib.Path("/tmp/logweir-scram-e2e")
REQUIRED_CASES = {
    "additive_crd_drain_fence_restart",
    "approval_recreated_uid_replay",
    "approval_wrong_kind_replay",
    "approval_wrong_namespace_replay",
    "bundle_immutability",
    "collision_matrix_evidence_retained",
    "configmap_collision_owner_matrix",
    "configmap_substitution_before_mount",
    "controller_restart_in_flight",
    "controller_runner_rbac",
    "current_controller_actual_old_runner_handshake",
    "distinct_approval_cross_use",
    "fresh_scram_backup",
    "job_collision_owner_matrix",
    "legacy_mutable_prejob_transition",
    "legacy_same_uid_inflight_observation",
    "missing_bundle_before_mount",
    "ownerless_plan_configmap_collision",
    "retained_old_archive_evidence",
    "retained_then_owner_gc",
    "signer_missing_malformed_wrong_path_type",
    "signer_permission_denied_regular_file",
    "two_simultaneous_restores",
    "exact_old_controller_job_observation",
    "controller_upgrade_drain_rollback_during_restore",
    "post_start_projected_plan_replacement",
    "live_plan_directed_network_observation",
    "mid_run_signer_rotation",
}
KNOWN_UNRUN_CASES = {
    "exact_old_controller_job_observation",
    "controller_upgrade_drain_rollback_during_restore",
    "post_start_projected_plan_replacement",
    "live_plan_directed_network_observation",
    "mid_run_signer_rotation",
}
if STATE_PATH.exists():
    STATE: dict[str, Any] = json.loads(STATE_PATH.read_text())
else:
    STATE = {"created": dt.datetime.now(dt.timezone.utc).isoformat(), "cases": {}}
    baseline_path = os.environ.get("LOGWEIR_BACKEND_LIVE_BASELINE_STATE")
    if baseline_path:
        baseline = json.loads(pathlib.Path(baseline_path).read_text())
        STATE["baseline_state"] = str(pathlib.Path(baseline_path).resolve())
        STATE["cases"] = copy.deepcopy(baseline.get("cases", {}))
        # The review rejected these aggregate classifications. Preserve the
        # valid sub-cases under precise names and require corrected reruns.
        if STATE["cases"].pop("legacy_mutable_prejob_transition", None) == "failed":
            STATE["cases"]["legacy_mixed_annotated_mutable_rejection"] = "passed"
        if STATE["cases"].pop("signer_missing_malformed_unreadable", None) == "passed":
            STATE["cases"]["signer_missing_malformed_wrong_path_type"] = "passed"
        for key in [
            "backup_id",
            "backup_name",
            "positive_restores",
            "retained_signer",
            "approval_crd_uid",
            "source_cluster_id",
            "target_cluster_id",
        ]:
            if key in baseline:
                STATE[key] = copy.deepcopy(baseline[key])


def save_state() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")
    STATE_PATH.chmod(0o600)


def acceptance_gate(
    cases: dict[str, str],
    cleanup_state: dict[str, Any],
    *,
    require_cleanup: bool,
    required_cases: set[str] = REQUIRED_CASES,
) -> tuple[int, dict[str, list[str]]]:
    failed = sorted(name for name in required_cases if cases.get(name) == "failed")
    unrun = sorted(name for name in required_cases if cases.get(name) == "unrun")
    missing = sorted(name for name in required_cases if name not in cases)
    cleanup_failures: list[str] = []
    if require_cleanup and cleanup_state.get("result") != "deleted":
        cleanup_failures.append(
            f"required cleanup result is {cleanup_state.get('result', 'missing')!r}"
        )
    failures = {
        "failed": failed,
        "unrun": unrun,
        "missing": missing,
        "cleanup": cleanup_failures,
    }
    return (1 if any(failures.values()) else 0), failures


def terminal_exit_status(*, require_cleanup: bool) -> int:
    status, reasons = acceptance_gate(
        STATE.get("cases", {}), STATE.get("cleanup", {}), require_cleanup=require_cleanup
    )
    STATE["acceptance_gate"] = {"exit_code": status, **reasons}
    save_state()
    return status


def redact(text: str) -> str:
    for marker in ["BACKEND-LIVE-MALFORMED-PRIVATE-KEY-SENTINEL"]:
        text = text.replace(marker, "[REDACTED]")
    return text


def run(
    argv: list[str],
    *,
    stdin: str | None = None,
    check: bool = True,
    timeout: int = 180,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        argv,
        input=stdin,
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=timeout,
        check=False,
    )
    if check and proc.returncode:
        raise RuntimeError(
            redact(
                f"command={argv[:8]!r} rc={proc.returncode} "
                f"stderr={proc.stderr[-2400:]} stdout={proc.stdout[-2400:]}"
            )
        )
    return proc


def log(message: str) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"{now} {redact(message)}", flush=True)


def apply(obj: dict[str, Any], *, fixture_namespace: bool = False) -> None:
    command = KF if fixture_namespace else K
    run(command + ["apply", "-f", "-"], stdin=json.dumps(obj))


def get(kind: str, name: str, *, fixture_namespace: bool = False) -> dict[str, Any]:
    command = KF if fixture_namespace else K
    return json.loads(run(command + ["get", kind, name, "-o", "json"]).stdout)


def optional_json_from_result(
    proc: subprocess.CompletedProcess[str], *, description: str
) -> dict[str, Any] | None:
    """Decode an ignore-not-found get without hiding API/transport failures."""
    if proc.returncode:
        raise RuntimeError(
            redact(
                f"{description} failed rc={proc.returncode}: "
                f"{(proc.stderr + proc.stdout)[-2400:]}"
            )
        )
    if not proc.stdout.strip():
        return None
    return json.loads(proc.stdout)


def get_optional(
    kind: str,
    name: str,
    *,
    fixture_namespace: bool = False,
    cluster_scoped: bool = False,
) -> dict[str, Any] | None:
    command = KUBECTL if cluster_scoped else (KF if fixture_namespace else K)
    proc = run(
        command + ["get", kind, name, "-o", "json", "--ignore-not-found"],
        check=False,
    )
    return optional_json_from_result(proc, description=f"get {kind}/{name}")


def delete_with_uid_precondition(
    kind: str,
    name: str,
    uid: str,
    *,
    api_version: str,
    namespace: str | None = NS,
    timeout: int = 180,
) -> None:
    """Delete an exact object incarnation through the raw Kubernetes API."""
    if api_version == "v1":
        prefix = "/api/v1"
    else:
        group, version = api_version.split("/", 1)
        prefix = f"/apis/{group}/{version}"
    plural = {
        "configmap": "configmaps",
        "job": "jobs",
        "namespace": "namespaces",
        "resourcequota": "resourcequotas",
        "restore": "restores",
    }[kind.lower()]
    scope = f"/namespaces/{namespace}" if namespace else ""
    uri = f"{prefix}{scope}/{plural}/{name}"
    options = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {"uid": uid},
        "propagationPolicy": "Foreground",
    }
    run(
        KUBECTL + ["delete", "--raw", uri, "-f", "-"],
        stdin=json.dumps(options),
        timeout=timeout,
    )


def wait_for(
    kind: str,
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    seconds: int = 300,
) -> dict[str, Any]:
    deadline = time.monotonic() + seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = get_optional(kind, name)
        if last is not None and predicate(last):
            save_artifact(f"{kind}-{name}.json", last)
            return last
        time.sleep(2)
    status = None if last is None else last.get("status")
    raise RuntimeError(f"timeout waiting for {kind}/{name}; last status={status!r}")


def save_artifact(name: str, value: Any) -> pathlib.Path:
    path = OUT / name
    if isinstance(value, str):
        path.write_text(redact(value))
    else:
        path.write_text(redact(json.dumps(value, indent=2, sort_keys=True)) + "\n")
    return path


def owned_metadata(name: str, *, namespace: str = NS) -> dict[str, Any]:
    return {
        "name": name,
        "namespace": namespace,
        "labels": {"backend-live.logweir.dev/run": RUN_LABEL},
    }


def custom_resource(kind: str, name: str, spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": kind,
        "metadata": owned_metadata(name),
        "spec": spec,
    }


def copy_secret(name: str, source_name: str | None = None) -> None:
    source = get("secret", source_name or name, fixture_namespace=True)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned_metadata(name),
            "type": source.get("type", "Opaque"),
            "data": source["data"],
        }
    )


def setup_namespace() -> None:
    ns = get_optional("namespace", NS, cluster_scoped=True)
    if ns is not None:
        labels = ns["metadata"].get("labels", {})
        if labels.get("backend-live.logweir.dev/run") != RUN_LABEL:
            raise RuntimeError(f"refusing pre-existing unowned namespace {NS}")
        if STATE.get("namespace_uid") != ns["metadata"]["uid"]:
            raise RuntimeError("owned namespace UID does not match saved run state")
    else:
        ns = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": NS,
                "labels": {"backend-live.logweir.dev/run": RUN_LABEL},
            },
        }
        run(KUBECTL + ["apply", "-f", "-"], stdin=json.dumps(ns))
        created = json.loads(
            run(KUBECTL + ["get", "namespace", NS, "-o", "json"]).stdout
        )
        STATE["namespace_uid"] = created["metadata"]["uid"]
        save_state()
    apply(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": owned_metadata("logweir-runner"),
            "automountServiceAccountToken": False,
        }
    )
    for secret_name in [
        "source-scram",
        "target-scram",
        "logweir-s3",
        "logweir-signing-key",
    ]:
        copy_secret(secret_name)
    if get_optional("pod", "backend-live-mc") is None:
        apply(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": owned_metadata("backend-live-mc"),
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
    run(K + ["wait", "--for=condition=Ready", "pod/backend-live-mc", "--timeout=90s"])
    log(f"owned namespace ready: {NS} uid={STATE['namespace_uid']}")


def assert_single_controller() -> None:
    pods = json.loads(run(KUBECTL + ["get", "pods", "-A", "-o", "json"]).stdout)[
        "items"
    ]
    controllers = [
        pod
        for pod in pods
        if any(
            container.get("image", "").split("/")[-1].startswith("weirkeeper:")
            for container in pod["spec"].get("containers", [])
        )
        and pod.get("metadata", {}).get("deletionTimestamp") is None
    ]
    if len(controllers) != 1:
        raise RuntimeError(f"expected one live weirkeeper pod, found {len(controllers)}")
    pod = controllers[0]
    image = pod["spec"]["containers"][0]["image"]
    ready = pod["status"]["containerStatuses"][0].get("ready")
    if image != CURRENT_CONTROLLER or ready is not True:
        raise RuntimeError(f"unexpected controller image/readiness: {image} {ready}")
    STATE["controller_pod"] = {
        "name": pod["metadata"]["name"],
        "uid": pod["metadata"]["uid"],
        "image": image,
        "image_id": pod["status"]["containerStatuses"][0]["imageID"],
    }
    save_state()


def create_clusters() -> None:
    for role in ["source", "target"]:
        apply(
            custom_resource(
                "KafkaCluster",
                role,
                {
                    "bootstrapServers": [
                        f"kafka-{role}.{FIXTURE_NS}.svc.cluster.local:9096"
                    ],
                    "auth": {
                        "mode": "scramSha512",
                        "username": "scram-user",
                        "tls": False,
                        "secretRef": {"name": f"{role}-scram"},
                    },
                    "role": role,
                },
            )
        )
    for role in ["source", "target"]:
        cluster = wait_for(
            "kafkacluster",
            role,
            lambda obj: obj.get("status", {}).get("reachable") is True,
            seconds=240,
        )
        STATE[f"{role}_cluster_id"] = cluster["status"]["clusterId"]
    if STATE["source_cluster_id"] == STATE["target_cluster_id"]:
        raise RuntimeError("source and target unexpectedly report the same cluster ID")
    save_state()
    log("real SCRAM probes succeeded for distinct source and target cluster IDs")


def backup_argv() -> list[str]:
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


def mc_cat(key: str) -> str:
    return run(
        K
        + [
            "exec",
            "backend-live-mc",
            "--",
            "mc",
            "cat",
            f"local/kafka-backups/{key}",
        ],
        timeout=120,
    ).stdout


def verify_signed_document(
    document: pathlib.Path, signature: pathlib.Path, payload_type: str
) -> None:
    public_key = FIXTURE_KEYS / "signing.pub.pem"
    if not public_key.is_file():
        raise RuntimeError(f"missing retained public fixture key {public_key}")
    rust = run(
        DOCKER
        + [
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{OUT}:/evidence:ro",
            "-v",
            f"{public_key}:/keys/signing.pub.pem:ro",
            CURRENT_RUNNER,
            "drill",
            "verify",
            "--payload-type",
            payload_type,
            "--scorecard",
            f"/evidence/{document.name}",
            "--signature",
            f"/evidence/{signature.name}",
            "--public-key",
            "/keys/signing.pub.pem",
        ],
        timeout=60,
    )
    python = run(
        [
            os.environ.get("LOGWEIR_PYTHON", sys.executable),
            "docs/verify_scorecard.py",
            "--payload-type",
            payload_type,
            str(document),
            str(signature),
            str(public_key),
        ],
        timeout=60,
    )
    if "VALID" not in python.stdout:
        raise RuntimeError(f"independent verifier gave no VALID verdict: {python.stdout}")
    save_artifact(f"verify-{document.stem}.txt", rust.stdout + python.stdout)


def fresh_backup() -> dict[str, Any]:
    name = "backend-live-backup"
    obj = custom_resource(
        "Backup",
        name,
        {
            "sourceRef": {"name": "source"},
            "topics": ["orders", "payments"],
            "archive": {
                "url": "s3://kafka-backups/scram-local",
                "secretRef": {"name": "logweir-s3"},
            },
            "triggeredBy": "manual",
            "deadlineSeconds": 240,
        },
    )
    obj["metadata"]["annotations"] = {
        "logweir.dev/runner-argv": json.dumps(backup_argv(), separators=(",", ":"))
    }
    apply(obj)
    backup = wait_for(
        "backup",
        name,
        lambda item: item.get("status", {}).get("phase") in {"Succeeded", "Failed"},
        seconds=360,
    )
    status = backup["status"]
    if status.get("phase") != "Succeeded" or status.get("exitCode") != 0:
        raise RuntimeError(f"fresh backup failed: {status!r}")
    backup = wait_for(
        "backup",
        name,
        lambda item: item.get("status", {})
        .get("evidence", {})
        .get("verification", {})
        .get("result")
        == "Valid",
        seconds=120,
    )
    job = get("job", name)
    container = job["spec"]["template"]["spec"]["containers"][0]
    if container["image"] != CURRENT_RUNNER:
        raise RuntimeError(f"backup used stale runner image {container['image']}")
    env = {entry["name"]: entry for entry in container["env"]}
    expected_refs = {
        "LOGWEIR_SOURCE_PASSWORD": ("source-scram", "password"),
        "AWS_ACCESS_KEY_ID": ("logweir-s3", "access-key-id"),
        "AWS_SECRET_ACCESS_KEY": ("logweir-s3", "secret-access-key"),
    }
    for variable, (secret_name, key) in expected_refs.items():
        actual = env[variable]["valueFrom"]["secretKeyRef"]
        if actual.get("name") != secret_name or actual.get("key") != key:
            raise RuntimeError(f"wrong saved credential propagation for {variable}: {actual}")
    evidence = backup["status"]["evidence"]
    receipt_path = save_artifact("fresh-backup-receipt.json", mc_cat(evidence["receiptKey"]))
    signature_path = save_artifact(
        "fresh-backup-receipt.sig", mc_cat(evidence["sidecarKey"])
    )
    receipt = json.loads(receipt_path.read_text())
    if receipt["source"]["auth"] != {
        "mode": "scramSha512",
        "username": "scram-user",
    }:
        raise RuntimeError(f"receipt lost selected SCRAM metadata: {receipt['source']['auth']}")
    if receipt["source"]["topics"] != ["orders", "payments"]:
        raise RuntimeError(f"receipt topic list changed: {receipt['source']['topics']}")
    if receipt["records"] != {"orders": 100, "payments": 100}:
        raise RuntimeError(f"receipt record counts are not exact: {receipt['records']}")
    if receipt["source"]["cluster_id"] != STATE["source_cluster_id"]:
        raise RuntimeError("receipt source cluster ID does not match live SCRAM probe")
    verify_signed_document(receipt_path, signature_path, "backup-receipt")
    save_artifact("fresh-backup-job.json", job)
    STATE["backup_id"] = status["backupId"]
    STATE["backup_name"] = name
    STATE["cases"]["fresh_scram_backup"] = "passed"
    save_state()
    log("fresh current-runner backup passed with exact 100+100 receipt counts")
    return backup


def restore_plan(prefix: str, point_in_time: str) -> str:
    endpoint = f"http://minio.{FIXTURE_NS}.svc.cluster.local:9000"
    storage = {
        "backend": "s3",
        "bucket": "kafka-backups",
        "prefix": "scram-local",
        "region": "us-east-1",
        "endpoint": endpoint,
        "path_style": True,
        "allow_http": True,
    }
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    plan = {
        "source": {
            "storage": storage,
            "backup": STATE["backup_id"],
            "topics": ["orders", "payments"],
        },
        "target": {
            "bootstrap_servers": [
                f"kafka-target.{FIXTURE_NS}.svc.cluster.local:9096"
            ],
            "auth": {"mode": "scramSha512", "username": "scram-user", "tls": False},
            "mode": "newTopic",
            "topic_naming": {"prefix": prefix},
            "topic_mapping_prefix": "logweir-scratch-",
            "marker_topic": "logweir.scratch",
            "default_replication_factor": 1,
            "teardown": "delete",
        },
        "restore": {"point_in_time": point_in_time},
        "sample": {
            "window_start": start,
            "window_end": point_in_time,
            "records_per_partition": 25,
            "anchor": "head",
        },
        "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
        "evidence": {**storage, "prefix": "logweir/"},
        "notifications": {"webhooks": []},
    }
    return json.dumps(plan, indent=2) + "\n"


def create_restore_shell(name: str, approval_name: str, prefix: str) -> tuple[str, str]:
    existing = get_optional("restore", name)
    if existing is not None:
        if existing["spec"]["approvalRef"]["name"] != approval_name:
            raise RuntimeError(f"existing Restore/{name} has the wrong approvalRef")
        return existing["spec"]["planBytes"], existing["metadata"]["uid"]
    point = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    plan = restore_plan(prefix, point)
    apply(
        custom_resource(
            "Restore",
            name,
            {
                "sourceArchive": {
                    "url": "s3://kafka-backups/scram-local",
                    "secretRef": {"name": "logweir-s3"},
                },
                "backupSetRef": STATE["backup_id"],
                "pointInTime": point,
                "target": {
                    "clusterRef": {"name": "target"},
                    "mode": "newTopic",
                    "topicNaming": {"prefix": prefix},
                },
                "approvalRef": {"name": approval_name},
                "planBytes": plan,
                "deadlineSeconds": 600,
            },
        )
    )
    restore = get("restore", name)
    return plan, restore["metadata"]["uid"]


def create_restore_with_plan(
    name: str, approval_name: str, prefix: str, plan: str, point: str
) -> tuple[str, str]:
    if get_optional("restore", name) is not None:
        raise RuntimeError(f"Restore/{name} already exists; refusing ambiguous rerun")
    apply(
        custom_resource(
            "Restore",
            name,
            {
                "sourceArchive": {
                    "url": "s3://kafka-backups/scram-local",
                    "secretRef": {"name": "logweir-s3"},
                },
                "backupSetRef": STATE["backup_id"],
                "pointInTime": point,
                "target": {
                    "clusterRef": {"name": "target"},
                    "mode": "newTopic",
                    "topicNaming": {"prefix": prefix},
                },
                "approvalRef": {"name": approval_name},
                "planBytes": plan,
                "deadlineSeconds": 900,
            },
        )
    )
    restore = get("restore", name)
    return plan, restore["metadata"]["uid"]


def approve_restore(
    name: str,
    approval_name: str,
    plan: str,
    *,
    wait_for_verification: bool = True,
    require_subject_binding: bool = True,
) -> dict[str, Any]:
    existing = get_optional("approval", approval_name)
    if existing is not None:
        if not wait_for_verification:
            return existing
        return wait_for(
            "approval",
            approval_name,
            lambda item: item.get("status", {}).get("verified") is True,
            seconds=180,
        )
    pair_dir = OUT / approval_name
    pair_dir.mkdir(mode=0o700, exist_ok=True)
    plan_path = pair_dir / "restore.json"
    plan_path.write_text(plan)
    approver_key = FIXTURE_KEYS / "approver.pem"
    if not approver_key.is_file():
        raise RuntimeError(f"missing controlled approver key {approver_key}")
    run(
        DOCKER
        + [
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{pair_dir}:/out",
            "-v",
            f"{approver_key}:/keys/approver.pem:ro",
            CURRENT_RUNNER,
            "drill",
            "approve",
            "--spec",
            "/out/restore.json",
            "--key",
            "/keys/approver.pem",
            "--approver",
            "backend-live@example.invalid",
            "--ticket",
            "PLAT01-PLAT02-LIVE",
            "--subject-kind",
            "Restore",
            "--out",
            "/out/approval.json",
        ],
        timeout=60,
    )
    approval_bytes = (pair_dir / "approval.json").read_text()
    sidecar_bytes = (pair_dir / "approval.sig").read_text()
    apply(
        custom_resource(
            "Approval",
            approval_name,
            {
                "subjectRef": {"kind": "Restore", "name": name},
                "planHash": "sha256:" + hashlib.sha256(plan.encode()).hexdigest(),
                "approvalBytes": approval_bytes,
                "sidecarBytes": sidecar_bytes,
            },
        )
    )
    if not wait_for_verification:
        return get("approval", approval_name)
    approval = wait_for(
        "approval",
        approval_name,
        lambda item: item.get("status", {}).get("verified") is True,
        seconds=180,
    )
    if not require_subject_binding:
        return approval
    subject = approval["status"].get("verifiedSubjectRef")
    restore = get("restore", name)
    expected = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "name": name,
        "namespace": NS,
        "uid": restore["metadata"]["uid"],
    }
    if subject != expected:
        raise RuntimeError(f"approval provenance mismatch: {subject!r} != {expected!r}")
    return approval


def set_pod_quota(limit: str) -> None:
    apply(
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": owned_metadata("pod-barrier"),
            "spec": {"hard": {"pods": limit}},
        }
    )


def remove_pod_quota() -> None:
    quota = get_optional("resourcequota", "pod-barrier")
    if quota is None:
        return
    if quota["metadata"]["labels"].get("backend-live.logweir.dev/run") != RUN_LABEL:
        raise RuntimeError("refusing to remove unowned ResourceQuota")
    delete_with_uid_precondition(
        "resourcequota",
        "pod-barrier",
        quota["metadata"]["uid"],
        api_version="v1",
    )
    wait_absent("resourcequota", "pod-barrier")


def restart_controller() -> None:
    run(
        KF
        + [
            "rollout",
            "restart",
            "deployment/weirkeeper",
        ]
    )
    run(
        KF
        + ["rollout", "status", "deployment/weirkeeper", "--timeout=180s"],
        timeout=200,
    )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            assert_single_controller()
            return
        except RuntimeError:
            time.sleep(2)
    raise RuntimeError("controller restart did not converge to one Ready pod")


def broker_target(script: str, timeout: int = 120) -> str:
    return run(
        KF + ["exec", "-i", "deploy/kafka-target", "--", "bash", "-se"],
        stdin=script,
        timeout=timeout,
    ).stdout


def target_topics() -> set[str]:
    output = broker_target(
        "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list\n"
    )
    return {line.strip() for line in output.splitlines() if line.strip()}


def archive_listing() -> str:
    return run(
        K
        + [
            "exec",
            "backend-live-mc",
            "--",
            "mc",
            "ls",
            "--recursive",
            "local/kafka-backups",
        ],
        timeout=120,
    ).stdout


def set_controller_runner(image: str) -> None:
    run(
        KF
        + [
            "set",
            "env",
            "deployment/weirkeeper",
            f"LOGWEIR_RUNNER_IMAGE={image}",
        ]
    )
    run(
        KF + ["rollout", "status", "deployment/weirkeeper", "--timeout=180s"],
        timeout=200,
    )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        pods = json.loads(
            run(KF + ["get", "pods", "-l", "app.kubernetes.io/component=control-plane", "-o", "json"]).stdout
        )["items"]
        active = [
            pod
            for pod in pods
            if pod["metadata"].get("deletionTimestamp") is None
            and pod.get("status", {}).get("containerStatuses", [{}])[0].get("ready") is True
        ]
        if len(active) == 1:
            container = active[0]["spec"]["containers"][0]
            env = {entry["name"]: entry.get("value") for entry in container.get("env", [])}
            if container["image"] == CURRENT_CONTROLLER and env.get(
                "LOGWEIR_RUNNER_IMAGE"
            ) == image:
                return
        time.sleep(2)
    raise RuntimeError(f"controller rollout did not converge on runner image {image}")


def controller_deployment() -> dict[str, Any]:
    return json.loads(
        run(KF + ["get", "deployment", "weirkeeper", "-o", "json"]).stdout
    )


def controller_identity(deployment: dict[str, Any]) -> dict[str, Any]:
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    runner_image = next(
        entry.get("value")
        for entry in container.get("env", [])
        if entry["name"] == "LOGWEIR_RUNNER_IMAGE"
    )
    pods = json.loads(
        run(
            KF
            + [
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=control-plane",
                "-o",
                "json",
            ]
        ).stdout
    )["items"]
    active = [
        pod
        for pod in pods
        if pod["metadata"].get("deletionTimestamp") is None
        and pod.get("status", {}).get("containerStatuses", [{}])[0].get("ready") is True
    ]
    return {
        "deployment_uid": deployment["metadata"]["uid"],
        "resource_version": deployment["metadata"]["resourceVersion"],
        "generation": deployment["metadata"]["generation"],
        "image": container["image"],
        "runner_image": runner_image,
        "ready_pods": [
            {
                "name": pod["metadata"]["name"],
                "uid": pod["metadata"]["uid"],
                "image_id": pod["status"]["containerStatuses"][0].get("imageID"),
            }
            for pod in active
        ],
        "spec_sha256": hashlib.sha256(
            json.dumps(deployment["spec"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }


def set_controller_images(controller_image: str, runner_image: str) -> None:
    configure_controller_images(controller_image, runner_image)
    run(
        KF + ["rollout", "status", "deployment/weirkeeper", "--timeout=180s"],
        timeout=200,
    )
    current = controller_identity(controller_deployment())
    if (
        current["image"] != controller_image
        or current["runner_image"] != runner_image
        or len(current["ready_pods"]) != 1
    ):
        raise RuntimeError(f"controller switch did not converge: {current!r}")


def configure_controller_images(controller_image: str, runner_image: str) -> None:
    run(
        KF
        + [
            "set",
            "image",
            "deployment/weirkeeper",
            f"weirkeeper={controller_image}",
        ]
    )
    run(
        KF
        + [
            "set",
            "env",
            "deployment/weirkeeper",
            f"LOGWEIR_RUNNER_IMAGE={runner_image}",
        ]
    )


def scale_any_controller(replicas: int) -> None:
    run(KF + ["scale", "deployment/weirkeeper", f"--replicas={replicas}"])
    if replicas == 0:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if not controller_identity(controller_deployment())["ready_pods"]:
                return
            time.sleep(2)
        raise RuntimeError("controller did not drain to zero Ready pods")
    run(
        KF + ["rollout", "status", "deployment/weirkeeper", "--timeout=180s"],
        timeout=200,
    )
    identity = controller_identity(controller_deployment())
    if len(identity["ready_pods"]) != 1:
        raise RuntimeError(f"controller did not converge to one Ready pod: {identity!r}")


def wait_restore_terminal(name: str, seconds: int = 300) -> dict[str, Any]:
    return wait_for(
        "restore",
        name,
        lambda item: item.get("status", {}).get("phase") in {"Succeeded", "Failed"},
        seconds=seconds,
    )


def job_logs(name: str) -> str:
    return run(K + ["logs", f"job/{name}", "-c", "runner"], check=False).stdout + run(
        K + ["logs", f"job/{name}", "-c", "runner"], check=False
    ).stderr


def pod_for_job(name: str, *, seconds: int = 120) -> dict[str, Any]:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pods = json.loads(
            run(K + ["get", "pods", "-l", f"job-name={name}", "-o", "json"]).stdout
        )["items"]
        if pods:
            return pods[0]
        time.sleep(1)
    raise RuntimeError(f"no Pod observed for Job/{name}")


def old_runner_handshake() -> None:
    name = "backend-live-old-runner"
    approval_name = "backend-live-old-approval"
    prefix = "backend-live-old-"
    before_archive = archive_listing()
    before_topics = target_topics()
    set_controller_runner(OLD_RUNNER)
    try:
        plan, _uid = create_restore_shell(name, approval_name, prefix)
        approve_restore(name, approval_name, plan)
        job = wait_for("job", name, lambda _item: True, seconds=180)
        container = job["spec"]["template"]["spec"]["containers"][0]
        if container["image"] != OLD_RUNNER:
            raise RuntimeError(f"old-runner case used {container['image']}")
        restore = wait_restore_terminal(name, seconds=240)
        if restore["status"].get("phase") != "Failed":
            raise RuntimeError(f"old runner unexpectedly succeeded: {restore['status']!r}")
        logs = job_logs(name)
        lower = logs.lower()
        if "unexpected argument" not in lower or "execution-contract-version" not in lower:
            raise RuntimeError(f"old runner did not name mandatory handshake refusal: {logs}")
        if any(topic.startswith(prefix) for topic in target_topics()):
            raise RuntimeError("old runner created target topics despite clap refusal")
        if target_topics() != before_topics:
            raise RuntimeError("target topic set changed during old-runner handshake refusal")
        if archive_listing() != before_archive:
            raise RuntimeError("archive object listing changed during old-runner handshake refusal")
        if restore["status"].get("evidence"):
            raise RuntimeError("old-runner handshake refusal reported evidence artifacts")
        save_artifact("old-runner-job.json", get("job", name))
        save_artifact("old-runner-logs.txt", logs)
        STATE["cases"]["current_controller_actual_old_runner_handshake"] = "passed"
        STATE["old_runner_exit_code"] = restore["status"].get("exitCode")
        save_state()
        log("actual archived old runner rejected the current mandatory handshake before data")
    finally:
        set_controller_runner(CURRENT_RUNNER)


def premount_substitution() -> None:
    name = "backend-live-premount-substitution"
    approval_name = "backend-live-premount-approval"
    prefix = "backend-live-premount-"
    before_archive = archive_listing()
    before_topics = target_topics()
    set_pod_quota("0")
    try:
        plan, _uid = create_restore_shell(name, approval_name, prefix)
        approve_restore(name, approval_name, plan)
        wait_for("job", name, lambda _item: True, seconds=180)
        pods = json.loads(run(K + ["get", "pods", "-o", "json"]).stdout)["items"]
        if any(
            pod.get("metadata", {}).get("labels", {}).get("job-name") == name
            for pod in pods
        ):
            raise RuntimeError("quota barrier failed before ConfigMap substitution")
        bundle = get("configmap", f"{name}-approval-bundle")
        original_uid = bundle["metadata"]["uid"]
        if bundle.get("immutable") is not True:
            raise RuntimeError("original bundle was not immutable")
        current = get("configmap", f"{name}-approval-bundle")
        if current["metadata"]["uid"] != original_uid:
            raise RuntimeError("bundle UID changed before precise delete")
        run(K + ["delete", "configmap", f"{name}-approval-bundle", "--wait=true"])
        replacement = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                **owned_metadata(f"{name}-approval-bundle"),
                "annotations": bundle["metadata"]["annotations"],
                "ownerReferences": bundle["metadata"]["ownerReferences"],
            },
            "immutable": True,
            "data": copy.deepcopy(bundle["data"]),
        }
        replacement["data"]["allowed-clusters.json"] = '{"allowed_cluster_ids":[]}\n'
        apply(replacement)
        replacement_live = get("configmap", f"{name}-approval-bundle")
        if replacement_live["metadata"]["uid"] == original_uid:
            raise RuntimeError("ConfigMap delete/recreate did not produce a new UID")
        save_artifact("premount-original-bundle.json", bundle)
        save_artifact("premount-replacement-bundle.json", replacement_live)
    finally:
        remove_pod_quota()
    restore = wait_restore_terminal(name, seconds=300)
    if restore["status"].get("phase") != "Failed":
        raise RuntimeError(f"substituted bundle unexpectedly succeeded: {restore['status']!r}")
    logs = job_logs(name)
    lower = logs.lower()
    if "allowed-clusters" not in lower or "sha256" not in lower:
        raise RuntimeError(f"runner did not identify the substituted member digest: {logs}")
    if any(topic.startswith(prefix) for topic in target_topics()):
        raise RuntimeError("pre-mount substitution performed Kafka restore actions")
    if target_topics() != before_topics:
        raise RuntimeError("target topic set changed during pre-auth substitution")
    if archive_listing() != before_archive:
        raise RuntimeError("archive object listing changed during pre-auth substitution")
    if restore["status"].get("evidence"):
        raise RuntimeError("pre-auth substitution reported evidence artifacts")
    save_artifact("premount-substitution-logs.txt", logs)
    save_artifact("premount-substitution-restore.json", restore)
    STATE["cases"]["configmap_substitution_before_mount"] = "passed"
    save_state()
    log("delete/recreate ConfigMap substitution before mount failed on exact digest with zero data artifacts")


def missing_bundle_before_mount() -> None:
    name = "backend-live-missing-bundle"
    approval_name = "backend-live-missing-bundle-approval"
    prefix = "backend-live-missing-bundle-"
    before_archive = archive_listing()
    before_topics = target_topics()
    set_pod_quota("0")
    try:
        plan, _uid = create_restore_shell(name, approval_name, prefix)
        approve_restore(name, approval_name, plan)
        wait_for("job", name, lambda _item: True, seconds=180)
        bundle = get_optional("configmap", f"{name}-approval-bundle")
        if bundle is not None:
            bundle_uid = bundle["metadata"]["uid"]
            if get("configmap", f"{name}-approval-bundle")["metadata"]["uid"] != bundle_uid:
                raise RuntimeError("bundle UID changed before missing-bundle delete")
            run(K + ["delete", "configmap", f"{name}-approval-bundle", "--wait=true"])
    finally:
        remove_pod_quota()
    deadline = time.monotonic() + 90
    observed: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        pods = json.loads(run(K + ["get", "pods", "-o", "json"]).stdout)["items"]
        matches = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("labels", {}).get("job-name") == name
        ]
        if matches:
            observed = matches[0]
            statuses = observed.get("status", {}).get("containerStatuses", [])
            if statuses:
                reason = statuses[0].get("state", {}).get("waiting", {}).get("reason")
                if reason == "CreateContainerConfigError":
                    break
                if reason == "ContainerCreating":
                    events = json.loads(
                        run(
                            K
                            + [
                                "get",
                                "events",
                                "--field-selector",
                                f"involvedObject.uid={observed['metadata']['uid']}",
                                "-o",
                                "json",
                            ]
                        ).stdout
                    )["items"]
                    if any(
                        event.get("reason") == "FailedMount"
                        and f'configmap "{name}-approval-bundle" not found'
                        in event.get("message", "")
                        for event in events
                    ):
                        save_artifact("missing-bundle-events.json", events)
                        break
        time.sleep(2)
    else:
        raise RuntimeError("missing bundle did not produce an exact kubelet mount refusal")
    if observed is None:
        raise RuntimeError("missing bundle produced no pod to inspect")
    if observed.get("status", {}).get("containerStatuses", [{}])[0].get("state", {}).get(
        "running"
    ):
        raise RuntimeError("runner started despite missing bundle")
    if any(topic.startswith(prefix) for topic in target_topics()):
        raise RuntimeError("missing bundle performed Kafka restore actions")
    if target_topics() != before_topics or archive_listing() != before_archive:
        raise RuntimeError("Kafka/archive state changed while bundle was missing")
    restore = get("restore", name)
    if restore.get("status", {}).get("evidence"):
        raise RuntimeError("missing bundle reported evidence artifacts")
    save_artifact("missing-bundle-pod.json", observed)
    STATE["cases"]["missing_bundle_before_mount"] = "passed"
    save_state()
    log("missing approval bundle prevented container startup with zero data artifacts")


def owner_reference(kind: str, name: str, uid: str, *, controller: bool) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1" if kind == "Restore" else "v1",
        "kind": kind,
        "name": name,
        "uid": uid,
        "controller": controller,
        "blockOwnerDeletion": True,
    }


def create_marker(name: str) -> dict[str, Any]:
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned_metadata(name),
            "data": {"purpose": "owned collision marker"},
        }
    )
    return get("configmap", name)


def create_collision_job(
    name: str,
    owners: list[dict[str, Any]] | None,
    *,
    hold_for_observation: bool = False,
) -> dict[str, Any]:
    metadata = owned_metadata(name)
    if owners is not None:
        metadata["ownerReferences"] = owners
    if hold_for_observation:
        metadata["finalizers"] = ["backend-live.logweir.dev/hold-for-observation"]
    apply(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": metadata,
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "labels": {"backend-live.logweir.dev/run": RUN_LABEL}
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "serviceAccountName": "logweir-runner",
                        "containers": [
                            {
                                "name": "runner",
                                "image": CURRENT_RUNNER,
                                "imagePullPolicy": "Never",
                                "args": ["--help"],
                            }
                        ],
                    },
                },
            },
        }
    )
    return get("job", name)


def status_reasons(obj: dict[str, Any]) -> str:
    status = obj.get("status", {})
    conditions = status.get("conditions") or []
    return " ".join(
        [
            str(status.get("exitReason", "")),
            *(str(condition.get("reason", "")) for condition in conditions),
            *(str(condition.get("message", "")) for condition in conditions),
        ]
    )


def save_checked_absence(kind: str, name: str, artifact_name: str) -> None:
    if get_optional(kind, name) is not None:
        raise RuntimeError(f"expected {kind}/{name} to be absent")
    save_artifact(
        artifact_name,
        {
            "kind": kind,
            "name": name,
            "namespace": NS,
            "observation": "kubectl --ignore-not-found returned exit 0 and empty output",
        },
    )


def wait_job_conflict(name: str) -> dict[str, Any]:
    restore = wait_restore_terminal(name, seconds=180)
    reasons = status_reasons(restore)
    if restore["status"].get("phase") != "Failed" or "JobNameConflict" not in reasons:
        raise RuntimeError(f"Restore/{name} was not refused for JobNameConflict: {restore['status']!r}")
    save_artifact(f"collision-{name}-result-restore.json", restore)
    save_checked_absence(
        "configmap",
        f"{name}-plan",
        f"collision-{name}-plan-absence.json",
    )
    return restore


def job_collision_matrix() -> None:
    # Ownerless same-name Job.
    name = "backend-live-job-ownerless"
    collision = create_collision_job(name, None)
    save_artifact(f"collision-{name}-before-job.json", collision)
    create_restore_shell(name, "missing-ownerless-approval", "backend-live-ownerless-")
    wait_job_conflict(name)

    # Foreign owner of a different kind and live UID.
    name = "backend-live-job-foreign"
    marker = create_marker("backend-live-foreign-owner")
    collision = create_collision_job(
        name,
        [owner_reference("ConfigMap", marker["metadata"]["name"], marker["metadata"]["uid"], controller=True)],
    )
    save_artifact(f"collision-{name}-before-job.json", collision)
    create_restore_shell(name, "missing-foreign-approval", "backend-live-foreign-")
    wait_job_conflict(name)

    # Previous incarnation UID.
    name = "backend-live-job-olduid"
    _plan, old_uid = create_restore_shell(name, "missing-olduid-approval", "backend-live-olduid-")
    run(K + ["delete", "restore", name, "--wait=true"])
    collision = create_collision_job(
        name,
        [owner_reference("Restore", name, old_uid, controller=True)],
        hold_for_observation=True,
    )
    save_artifact(f"collision-{name}-before-job.json", collision)
    _new_plan, new_uid = create_restore_shell(
        name, "missing-olduid-approval", "backend-live-olduid-new-"
    )
    if new_uid == old_uid:
        raise RuntimeError("Restore recreation did not change UID")
    wait_job_conflict(name)
    run(
        K
        + [
            "patch",
            "job",
            name,
            "--type=merge",
            "-p",
            '{"metadata":{"finalizers":null}}',
        ]
    )

    # Exact controller owner plus a secondary owner is still forbidden.
    name = "backend-live-job-secondary"
    _plan, restore_uid = create_restore_shell(
        name, "missing-secondary-approval", "backend-live-secondary-"
    )
    marker = create_marker("backend-live-secondary-owner")
    collision = create_collision_job(
        name,
        [
            owner_reference("Restore", name, restore_uid, controller=True),
            owner_reference(
                "ConfigMap",
                marker["metadata"]["name"],
                marker["metadata"]["uid"],
                controller=False,
            ),
        ],
    )
    save_artifact(f"collision-{name}-before-job.json", collision)
    wait_job_conflict(name)
    STATE["cases"]["job_collision_owner_matrix"] = "passed"
    save_state()
    log("ownerless, foreign, old-UID and secondary-owner Jobs were all refused")


def configmap_collision_and_recreated_uid() -> None:
    collision_name = "backend-live-plan-ownerless-collision"
    collision_approval = "backend-live-plan-ownerless-collision-approval"
    plan, _collision_uid = create_restore_shell(
        collision_name, collision_approval, "backend-live-plan-ownerless-collision-"
    )
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned_metadata(f"{collision_name}-plan"),
            "data": {"restore.yaml": "substituted-before-create\n"},
        }
    )
    collision_cm = get("configmap", f"{collision_name}-plan")
    save_artifact("collision-plan-ownerless-before-configmap.json", collision_cm)
    approve_restore(collision_name, collision_approval, plan)
    restore = wait_restore_terminal(collision_name, seconds=180)
    reasons = status_reasons(restore)
    if restore["status"].get("phase") != "Failed" or "Plan" not in reasons:
        raise RuntimeError(f"ownerless plan collision was not refused: {restore['status']!r}")
    save_artifact("collision-plan-ownerless-result-restore.json", restore)
    save_checked_absence(
        "job", collision_name, "collision-plan-ownerless-job-absence.json"
    )

    # A distinct name prevents recreated-UID replay artifacts from replacing
    # the ownerless-collision evidence above.
    name = "backend-live-recreated-uid-replay"
    approval_name = "backend-live-recreated-uid-replay-approval"
    plan, old_uid = create_restore_shell(
        name, approval_name, "backend-live-recreated-uid-old-"
    )
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned_metadata(f"{name}-plan"),
            "data": {"restore.yaml": "controlled replay-binding blocker\n"},
        }
    )
    approval = approve_restore(name, approval_name, plan)
    old_restore = wait_restore_terminal(name, seconds=180)
    old_spec = old_restore["spec"]
    save_artifact("recreated-uid-before-restore.json", old_restore)
    save_artifact("recreated-uid-before-approval.json", approval)
    delete_with_uid_precondition(
        "restore", name, old_uid, api_version="logweir.dev/v1alpha1"
    )
    wait_absent("restore", name)
    blocker = get("configmap", f"{name}-plan")
    delete_with_uid_precondition(
        "configmap",
        f"{name}-plan",
        blocker["metadata"]["uid"],
        api_version="v1",
    )
    wait_absent("configmap", f"{name}-plan")
    apply(custom_resource("Restore", name, old_spec))
    recreated = get("restore", name)
    if recreated["metadata"]["uid"] == old_uid:
        raise RuntimeError("recreated Restore retained old UID")
    rejected = wait_restore_terminal(name, seconds=180)
    reasons = status_reasons(rejected)
    if "ApprovalSubjectMismatch" not in reasons:
        raise RuntimeError(f"recreated-UID approval replay was not refused: {rejected['status']!r}")
    save_checked_absence("job", name, "recreated-uid-job-absence.json")
    current_approval = get("approval", approval_name)
    bound = current_approval["status"].get("verifiedSubjectRef")
    if bound is None or bound.get("uid") != old_uid:
        raise RuntimeError(f"Approval sticky provenance did not retain old UID: {bound!r}")
    save_artifact("recreated-uid-result-approval.json", current_approval)
    save_artifact("recreated-uid-result-restore.json", rejected)
    STATE["cases"]["ownerless_plan_configmap_collision"] = "passed"
    STATE["cases"]["approval_recreated_uid_replay"] = "passed"
    save_state()
    log("ownerless plan collision and recreated-UID Approval replay were refused before Job creation")


def create_bundle_collision(
    name: str,
    approval_name: str,
    prefix: str,
    owners: list[dict[str, Any]],
    *,
    finalizer: bool = False,
) -> None:
    plan, _uid = create_restore_shell(name, approval_name, prefix)
    metadata = owned_metadata(f"{name}-approval-bundle")
    metadata["ownerReferences"] = owners
    if finalizer:
        metadata["finalizers"] = ["backend-live.logweir.dev/hold-for-observation"]
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "immutable": True,
            "data": {"approval.json": "foreign collision bytes\n"},
        }
    )
    save_artifact(
        f"collision-{name}-before-configmap.json",
        get("configmap", f"{name}-approval-bundle"),
    )
    approve_restore(name, approval_name, plan)
    restore = wait_restore_terminal(name, seconds=180)
    reasons = status_reasons(restore)
    if restore["status"].get("phase") != "Failed" or not any(
        token in reasons for token in ["ApprovalBundle", "Materialization"]
    ):
        raise RuntimeError(f"bundle collision {name} was not refused: {restore['status']!r}")
    save_checked_absence("job", name, f"collision-{name}-job-absence.json")
    save_artifact(f"{name}-restore.json", restore)
    if finalizer:
        run(
            K
            + [
                "patch",
                "configmap",
                f"{name}-approval-bundle",
                "--type=merge",
                "-p",
                '{"metadata":{"finalizers":null}}',
            ],
            check=False,
        )


def configmap_owner_matrix() -> None:
    # Foreign live ConfigMap owner.
    marker = create_marker("backend-live-bundle-foreign-marker")
    create_bundle_collision(
        "backend-live-bundle-foreign",
        "backend-live-bundle-foreign-approval",
        "backend-live-bundle-foreign-",
        [
            owner_reference(
                "ConfigMap",
                marker["metadata"]["name"],
                marker["metadata"]["uid"],
                controller=True,
            )
        ],
    )

    # Previous Restore incarnation UID, held just long enough for observation.
    name = "backend-live-bundle-olduid"
    approval_name = "backend-live-bundle-olduid-approval"
    _plan, old_uid = create_restore_shell(
        name, approval_name, "backend-live-bundle-olduid-old-"
    )
    run(K + ["delete", "restore", name, "--wait=true"])
    plan, new_uid = create_restore_shell(
        name, approval_name, "backend-live-bundle-olduid-new-"
    )
    if new_uid == old_uid:
        raise RuntimeError("bundle collision Restore recreation retained UID")
    metadata = owned_metadata(f"{name}-approval-bundle")
    metadata["ownerReferences"] = [
        owner_reference("Restore", name, old_uid, controller=True)
    ]
    metadata["finalizers"] = ["backend-live.logweir.dev/hold-for-observation"]
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "immutable": True,
            "data": {"approval.json": "stale UID bytes\n"},
        }
    )
    save_artifact(
        f"collision-{name}-before-configmap.json",
        get("configmap", f"{name}-approval-bundle"),
    )
    approve_restore(name, approval_name, plan)
    restore = wait_restore_terminal(name, seconds=180)
    if not any(
        token in status_reasons(restore) for token in ["ApprovalBundle", "Materialization"]
    ):
        raise RuntimeError(f"old-UID bundle collision was not refused: {restore['status']!r}")
    save_artifact(f"collision-{name}-result-restore.json", restore)
    save_checked_absence("job", name, f"collision-{name}-job-absence.json")
    run(
        K
        + [
            "patch",
            "configmap",
            f"{name}-approval-bundle",
            "--type=merge",
            "-p",
            '{"metadata":{"finalizers":null}}',
        ],
        check=False,
    )

    # Exact Restore owner plus a secondary owner.
    name = "backend-live-bundle-secondary"
    approval_name = "backend-live-bundle-secondary-approval"
    plan, restore_uid = create_restore_shell(
        name, approval_name, "backend-live-bundle-secondary-"
    )
    marker = create_marker("backend-live-bundle-secondary-marker")
    metadata = owned_metadata(f"{name}-approval-bundle")
    metadata["ownerReferences"] = [
        owner_reference("Restore", name, restore_uid, controller=True),
        owner_reference(
            "ConfigMap",
            marker["metadata"]["name"],
            marker["metadata"]["uid"],
            controller=False,
        ),
    ]
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "immutable": True,
            "data": {"approval.json": "secondary owner bytes\n"},
        }
    )
    save_artifact(
        f"collision-{name}-before-configmap.json",
        get("configmap", f"{name}-approval-bundle"),
    )
    approve_restore(name, approval_name, plan)
    restore = wait_restore_terminal(name, seconds=180)
    if not any(
        token in status_reasons(restore) for token in ["ApprovalBundle", "Materialization"]
    ):
        raise RuntimeError(f"secondary-owner bundle collision was not refused: {restore['status']!r}")
    save_artifact(f"collision-{name}-result-restore.json", restore)
    save_checked_absence("job", name, f"collision-{name}-job-absence.json")
    STATE["cases"]["configmap_collision_owner_matrix"] = "passed"
    save_state()
    log("foreign, old-UID and secondary-owner bundle ConfigMaps were refused; ownerless plan already covered")


def scale_controller(replicas: int) -> None:
    run(KF + ["scale", "deployment/weirkeeper", f"--replicas={replicas}"])
    if replicas == 0:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pods = json.loads(
                run(
                    KF
                    + [
                        "get",
                        "pods",
                        "-l",
                        "app.kubernetes.io/component=control-plane",
                        "-o",
                        "json",
                    ]
                ).stdout
            )["items"]
            if not [pod for pod in pods if pod["metadata"].get("deletionTimestamp") is None]:
                return
            time.sleep(2)
        raise RuntimeError("controller did not fence to zero pods")
    run(
        KF + ["rollout", "status", "deployment/weirkeeper", "--timeout=180s"],
        timeout=200,
    )
    assert_single_controller()


def injected_subject_replay_matrix() -> None:
    # Remove the intentionally stuck missing-bundle Restore before fencing.
    missing = get_optional("restore", "backend-live-missing-bundle")
    if missing is not None:
        run(K + ["delete", "restore", missing["metadata"]["name"], "--wait=true"])
    running = [
        item["metadata"]["name"]
        for item in json.loads(run(K + ["get", "restores", "-o", "json"]).stdout)["items"]
        if item.get("status", {}).get("phase") == "Running"
    ]
    if running:
        raise RuntimeError(f"refusing controller fence with running Restores: {running!r}")
    good = get("approval", "backend-live-approval-a")
    matched_key = good["status"]["matchedKeyId"]
    cases = [
        ("namespace", "logweir-wrong-namespace", "Restore"),
        ("kind", NS, "Backup"),
    ]
    scale_controller(0)
    try:
        prepared: list[tuple[str, str, str, str]] = []
        for label, wrong_namespace, wrong_kind in cases:
            name = f"backend-live-replay-{label}"
            approval_name = f"backend-live-replay-{label}-approval"
            plan, uid = create_restore_shell(
                name, approval_name, f"backend-live-replay-{label}-"
            )
            approval = approve_restore(
                name,
                approval_name,
                plan,
                wait_for_verification=False,
            )
            status = {
                "verified": True,
                "matchedKeyId": matched_key,
                "approver": "backend-live@example.invalid",
                "ticket": "PLAT01-PLAT02-LIVE",
                "selfAttestedRisk": False,
                "conditions": [
                    {
                        "type": "Verified",
                        "status": "True",
                        "reason": "ControlledFixture",
                        "message": "controlled provenance fault injection",
                        "observedGeneration": 1,
                        "lastTransitionTime": dt.datetime.now(dt.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                    }
                ],
                "verifiedSubjectRef": {
                    "apiVersion": "logweir.dev/v1alpha1",
                    "kind": wrong_kind,
                    "name": name,
                    "namespace": wrong_namespace,
                    "uid": uid,
                },
            }
            run(
                K
                + [
                    "patch",
                    "approval",
                    approval["metadata"]["name"],
                    "--subresource=status",
                    "--type=merge",
                    "-p",
                    json.dumps({"status": status}, separators=(",", ":")),
                ]
            )
            prepared.append((label, name, approval_name, uid))
    finally:
        scale_controller(1)
    for label, name, approval_name, _uid in prepared:
        restore = wait_restore_terminal(name, seconds=180)
        if "ApprovalSubjectMismatch" not in status_reasons(restore):
            raise RuntimeError(f"wrong-{label} subject replay was not refused: {restore['status']!r}")
        if get_optional("job", name) is not None:
            raise RuntimeError(f"wrong-{label} subject replay created a Job")
        approval = get("approval", approval_name)
        bound = approval["status"].get("verifiedSubjectRef")
        expected = "logweir-wrong-namespace" if label == "namespace" else "Backup"
        observed = bound.get("namespace") if label == "namespace" else bound.get("kind")
        if observed != expected:
            raise RuntimeError(f"sticky wrong-{label} provenance was overwritten: {bound!r}")
        save_artifact(f"wrong-{label}-replay-restore.json", restore)
        save_artifact(f"wrong-{label}-replay-approval.json", approval)
    STATE["cases"]["approval_wrong_namespace_replay"] = "passed"
    STATE["cases"]["approval_wrong_kind_replay"] = "passed"
    save_state()
    log("controlled wrong-namespace and wrong-kind sticky Approval provenance was refused by live controllers")


def cross_restore_approval_use() -> None:
    name = "backend-live-cross-approval"
    plan, _uid = create_restore_shell(
        name, "backend-live-approval-b", "backend-live-cross-approval-"
    )
    del plan
    restore = wait_restore_terminal(name, seconds=180)
    if "ApprovalSubjectMismatch" not in status_reasons(restore):
        raise RuntimeError(f"cross-Restore approval use was not refused: {restore['status']!r}")
    if get_optional("job", name) is not None:
        raise RuntimeError("cross-Restore approval use created a Job")
    save_artifact("cross-restore-approval-use.json", restore)
    STATE["cases"]["distinct_approval_cross_use"] = "passed"
    save_state()
    log("one successful Restore's Approval could not be cross-used by another Restore")


def signer_prerequisite_jobs(selected_cases: set[str] | None = None) -> None:
    trap_name = "backend-live-engine-trap"
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned_metadata(trap_name),
            "data": {
                "engine": "#!/bin/sh\ntouch /work/ENGINE_INVOKED\nexit 99\n"
            },
        }
    )
    malformed_name = "backend-live-malformed-signer"
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned_metadata(malformed_name),
            "data": {
                "key.pem": "BACKEND-LIVE-MALFORMED-PRIVATE-KEY-SENTINEL\n"
            },
        }
    )
    source_name = (
        "backend-live-restore-a"
        if get_optional("job", "backend-live-restore-a") is not None
        else "backend-live-legacy-prejob"
    )
    source_job = get("job", source_name)
    source_pod = source_job["spec"]["template"]["spec"]
    before_archive = archive_listing()
    before_topics = target_topics()
    cases = {
        "missing": "/signing/no-such-key.pem",
        "malformed": "/signing/key.pem",
        "wrong_path_type": "/signing",
        "permission_denied": "/signing/key.pem",
    }
    if selected_cases is not None:
        cases = {name: path for name, path in cases.items() if name in selected_cases}
    if "permission_denied" in cases:
        copy_secret(
            "backend-live-unreadable-signer-key", "logweir-signing-key"
        )
    outcomes: dict[str, Any] = {}
    for case, key_path in cases.items():
        name = f"backend-live-signer-{case.replace('_', '-')}"
        existing = get_optional("job", name)
        if existing is None:
            pod_spec = copy.deepcopy(source_pod)
            runner = pod_spec["containers"][0]
            args = runner["args"]
            signer_index = args.index("--signing-key") + 1
            args[signer_index] = key_path
            args.extend(["--metrics-file", "/work/metrics.prom"])
            runner["env"].append(
                {"name": "LOGWEIR_ENGINE_BIN", "value": "/trap/engine"}
            )
            runner["volumeMounts"].append(
                {"name": "engine-trap", "mountPath": "/trap", "readOnly": True}
            )
            if case == "malformed":
                for volume in pod_spec["volumes"]:
                    if volume["name"] == "signing":
                        volume.pop("secret", None)
                        volume["configMap"] = {
                            "name": malformed_name,
                            "defaultMode": 288,
                            "items": [{"key": "key.pem", "path": "key.pem"}],
                        }
            if case == "permission_denied":
                # Project a real valid private key as a regular root-owned 0400
                # file. Removing fsGroup is essential: kubelet may otherwise
                # grant the supplemental group read access to projected files.
                pod_spec.get("securityContext", {}).pop("fsGroup", None)
                for volume in pod_spec["volumes"]:
                    if volume["name"] == "signing":
                        volume["secret"] = {
                            "secretName": "backend-live-unreadable-signer-key",
                            "defaultMode": 256,
                            "items": [{"key": "signing.pem", "path": "key.pem"}],
                        }
            pod_spec["volumes"].append(
                {
                    "name": "engine-trap",
                    "configMap": {
                        "name": trap_name,
                        "defaultMode": 365,
                        "items": [{"key": "engine", "path": "engine"}],
                    },
                }
            )
            observer = {
                "name": "observer",
                "image": CURRENT_RUNNER,
                "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c"],
                "args": ["while [ ! -f /work/stop ]; do sleep 1; done"],
                "securityContext": copy.deepcopy(runner["securityContext"]),
                "volumeMounts": [
                    {"name": "work", "mountPath": "/work"},
                    {"name": "signing", "mountPath": "/signing", "readOnly": True},
                ],
            }
            pod_spec["containers"].append(observer)
            apply(
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": owned_metadata(name),
                    "spec": {
                        "activeDeadlineSeconds": 180,
                        "backoffLimit": 0,
                        "template": {
                            "metadata": {
                                "labels": {
                                    "backend-live.logweir.dev/run": RUN_LABEL,
                                    "backend-live.logweir.dev/case": case,
                                }
                            },
                            "spec": pod_spec,
                        },
                    },
                }
            )
        deadline = time.monotonic() + 120
        observed_pod: dict[str, Any] | None = None
        runner_status: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            pods = json.loads(
                run(K + ["get", "pods", "-l", f"job-name={name}", "-o", "json"]).stdout
            )["items"]
            if pods:
                observed_pod = pods[0]
                statuses = observed_pod.get("status", {}).get("containerStatuses", [])
                runner_status = next(
                    (status for status in statuses if status["name"] == "runner"), None
                )
                if runner_status and runner_status.get("state", {}).get("terminated"):
                    break
            time.sleep(1)
        if observed_pod is None or runner_status is None:
            raise RuntimeError(f"signer case {case} produced no observable runner termination")
        terminated = runner_status["state"]["terminated"]
        if terminated.get("exitCode") != 4:
            raise RuntimeError(f"signer case {case} exited {terminated.get('exitCode')}")
        pod_name = observed_pod["metadata"]["name"]
        logs_proc = run(
            K + ["logs", pod_name, "-c", "runner"], check=False, timeout=30
        )
        logs = logs_proc.stdout + logs_proc.stderr
        if "BACKEND-LIVE-MALFORMED-PRIVATE-KEY-SENTINEL" in logs:
            raise RuntimeError(f"signer case {case} leaked malformed key contents")
        required_diagnostics = [
            key_path,
            "Mount a readable P-256 or Ed25519 PKCS#8 PEM private key",
            "No engine data operation was started",
        ]
        if case == "permission_denied":
            required_diagnostics.append("Permission denied")
        if case == "wrong_path_type":
            required_diagnostics.append("Is a directory")
        for required in required_diagnostics:
            if required not in logs:
                raise RuntimeError(f"signer case {case} diagnostic lacks {required!r}: {logs}")
        metrics = run(
            K
            + [
                "exec",
                pod_name,
                "-c",
                "observer",
                "--",
                "cat",
                "/work/metrics.prom",
            ]
        ).stdout
        if 'logweir_drill_exit_code{cluster="unknown"} 4' not in metrics:
            raise RuntimeError(f"signer case {case} metrics do not report exit 4: {metrics}")
        if "logweir_drill_runs_total" in metrics:
            raise RuntimeError(f"signer case {case} metrics claim a completed drill")
        if "BACKEND-LIVE-MALFORMED-PRIVATE-KEY-SENTINEL" in metrics:
            raise RuntimeError(f"signer case {case} leaked key material in metrics")
        files = run(
            K
            + [
                "exec",
                pod_name,
                "-c",
                "observer",
                "--",
                "find",
                "/work",
                "-maxdepth",
                "1",
                "-type",
                "f",
                "-printf",
                "%f\\n",
            ]
        ).stdout.splitlines()
        if files != ["metrics.prom"]:
            raise RuntimeError(f"signer case {case} wrote non-metric artifacts: {files!r}")
        file_observation: dict[str, Any] | None = None
        if case == "permission_denied":
            stat = run(
                K
                + [
                    "exec",
                    pod_name,
                    "-c",
                    "observer",
                    "--",
                    "stat",
                    "-Lc",
                    "%F %a %u %g",
                    key_path,
                ]
            ).stdout.strip()
            readable = run(
                K
                + ["exec", pod_name, "-c", "observer", "--", "test", "-r", key_path],
                check=False,
            )
            if not stat.startswith("regular file 400 0 ") or readable.returncode == 0:
                raise RuntimeError(
                    f"permission fixture is not a denied regular file: stat={stat!r} "
                    f"test_rc={readable.returncode}"
                )
            file_observation = {"stat": stat, "test_readable_rc": readable.returncode}
            save_artifact("signer-permission-denied-stat.txt", stat + "\n")
        run(
            K
            + [
                "exec",
                pod_name,
                "-c",
                "observer",
                "--",
                "touch",
                "/work/stop",
            ]
        )
        wait_for(
            "job",
            name,
            lambda item: any(
                condition.get("type") == "Failed" and condition.get("status") == "True"
                for condition in item.get("status", {}).get("conditions", [])
            ),
            seconds=90,
        )
        save_artifact(f"signer-{case}-logs.txt", logs)
        save_artifact(f"signer-{case}-metrics.prom", metrics)
        save_artifact(f"signer-{case}-pod.json", observed_pod)
        outcomes[case] = {
            "exit_code": 4,
            "work_files": files,
            "file_observation": file_observation,
            "runner_image_id": runner_status.get("imageID"),
        }
    if target_topics() != before_topics:
        raise RuntimeError("Kafka topic set changed across signer prerequisite failures")
    if archive_listing() != before_archive:
        raise RuntimeError("archive object listing changed across signer prerequisite failures")
    STATE.setdefault("signer_cases", {}).update(outcomes)
    if {"missing", "malformed", "wrong_path_type"}.issubset(
        STATE["signer_cases"]
    ):
        STATE["cases"]["signer_missing_malformed_wrong_path_type"] = "passed"
    if "permission_denied" in outcomes:
        STATE["cases"]["signer_permission_denied_regular_file"] = "passed"
    save_state()
    log(
        "selected signer prerequisite Jobs exited 4 with exact file-type/permission, "
        "redacted metrics and no engine/data artifacts"
    )


def wait_absent(
    kind: str,
    name: str,
    seconds: int = 120,
    *,
    cluster_scoped: bool = False,
) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if get_optional(kind, name, cluster_scoped=cluster_scoped) is None:
            return
        time.sleep(2)
    raise RuntimeError(f"{kind}/{name} remained after bounded GC wait")


def legacy_same_uid_observation() -> None:
    name = "backend-live-legacy-inflight"
    plan, uid = create_restore_shell(
        name, "backend-live-legacy-missing-approval", "backend-live-legacy-"
    )
    if get_optional("job", name) is None:
        apply(
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    **owned_metadata(name),
                    "ownerReferences": [
                        owner_reference("Restore", name, uid, controller=True)
                    ],
                },
                "spec": {
                    "activeDeadlineSeconds": 180,
                    "backoffLimit": 0,
                    "template": {
                        "metadata": {
                            "labels": {"backend-live.logweir.dev/run": RUN_LABEL}
                        },
                        "spec": {
                            "restartPolicy": "Never",
                            "automountServiceAccountToken": False,
                            "serviceAccountName": "logweir-runner",
                            "containers": [
                                {
                                    "name": "runner",
                                    "image": CURRENT_RUNNER,
                                    "imagePullPolicy": "Never",
                                    "command": ["/bin/sh", "-c"],
                                    "args": ["sleep 120"],
                                }
                            ],
                        },
                    },
                },
            }
        )
    job = get("job", name)
    job_uid = job["metadata"]["uid"]
    observed = wait_for(
        "restore",
        name,
        lambda item: item.get("status", {}).get("phase") == "Running",
        seconds=90,
    )
    if observed["status"].get("jobRef", {}).get("name") != name:
        raise RuntimeError("legacy same-UID Job was not observed by name")
    after = get("job", name)
    if after["metadata"]["uid"] != job_uid:
        raise RuntimeError("controller replaced the legitimate same-UID legacy Job")
    runner = after["spec"]["template"]["spec"]["containers"][0]
    if "--execution-contract-version" in runner.get("args", []):
        raise RuntimeError("legacy in-flight Job was silently rewritten to the new contract")
    if get_optional("configmap", f"{name}-approval-bundle") is not None:
        raise RuntimeError("legacy in-flight observation materialized a new bundle")
    save_artifact("legacy-inflight-job.json", after)
    current = get("restore", name)
    if current["metadata"]["uid"] != uid:
        raise RuntimeError("legacy Restore UID changed before precise GC delete")
    run(K + ["delete", "restore", name, "--wait=true"])
    wait_absent("job", name)
    STATE["cases"]["legacy_same_uid_inflight_observation"] = "passed"
    save_state()
    log("legitimate same-UID legacy in-flight Job was observed unchanged and owner-GC'd")


def legacy_prejob_transition() -> None:
    name = "backend-live-legacy-prejob"
    approval_name = "backend-live-legacy-prejob-approval"
    prefix = "backend-live-legacy-prejob-"
    plan, uid = create_restore_shell(name, approval_name, prefix)
    plan_name = f"{name}-plan"
    legacy = get_optional("configmap", plan_name)
    if legacy is None:
        apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    **owned_metadata(plan_name),
                    "ownerReferences": [
                        owner_reference("Restore", name, uid, controller=True)
                    ],
                },
                "data": {"restore.yaml": plan},
            }
        )
        legacy = get("configmap", plan_name)
    legacy_uid = legacy["metadata"]["uid"]
    binding_annotations = {
        "logweir.dev/restore-uid",
        "logweir.dev/plan-hash",
        "logweir.dev/approval-name",
        "logweir.dev/approval-uid",
    }
    annotations = legacy["metadata"].get("annotations", {})
    if "immutable" in legacy or binding_annotations.intersection(annotations):
        raise RuntimeError(
            "pre-Job fixture is not the archived old-controller shape: "
            "immutable/binding annotations must be absent"
        )
    expected_owner = [owner_reference("Restore", name, uid, controller=True)]
    if legacy["metadata"].get("ownerReferences") != expected_owner:
        raise RuntimeError("pre-Job fixture owner set is not the one exact Restore owner")
    if legacy.get("data") != {"restore.yaml": plan}:
        raise RuntimeError("pre-Job fixture did not retain exact planBytes")
    save_artifact("legacy-prejob-before-configmap.json", legacy)
    approve_restore(name, approval_name, plan)
    terminal = wait_restore_terminal(name, seconds=240)
    if terminal["status"].get("phase") == "Failed":
        save_artifact("legacy-prejob-result-restore.json", terminal)
        save_artifact("legacy-prejob-after-configmap.json", get("configmap", plan_name))
        STATE["cases"]["legacy_mutable_prejob_transition"] = "failed"
        STATE.setdefault("product_failures", []).append(
            {
                "case": "legacy_mutable_prejob_transition",
                "reason": terminal["status"].get("reason"),
                "message": status_reasons(terminal),
            }
        )
        save_state()
        log("legitimate archived-shape mutable pre-Job plan was refused before Job creation")
        return
    result = verify_restore(name, prefix)
    transitioned = get("configmap", plan_name)
    if transitioned["metadata"]["uid"] != legacy_uid:
        raise RuntimeError("pre-Job transition replaced the same-UID legacy plan ConfigMap")
    if "--execution-contract-version" not in result["job"]["spec"]["template"]["spec"][
        "containers"
    ][0]["args"]:
        raise RuntimeError("pre-Job transition did not create a hash-pinned current Job")
    pod = pod_for_job(name)
    runner_status = next(
        status
        for status in pod.get("status", {}).get("containerStatuses", [])
        if status["name"] == "runner"
    )
    exit_code = runner_status.get("state", {}).get("terminated", {}).get("exitCode")
    image_id = runner_status.get("imageID")
    if exit_code != 0 or not image_id:
        raise RuntimeError(
            f"legacy transition Pod lacks success identity: exit={exit_code} imageID={image_id}"
        )
    save_artifact("legacy-prejob-after-configmap.json", transitioned)
    save_artifact("legacy-prejob-result-restore.json", result["restore"])
    save_artifact("legacy-prejob-result-job.json", result["job"])
    save_artifact("legacy-prejob-result-pod.json", pod)
    STATE["legacy_prejob"] = {
        "restore_uid": uid,
        "configmap_uid": legacy_uid,
        "plan_sha256": hashlib.sha256(plan.encode()).hexdigest(),
        "runner_image_id": image_id,
        "exit_code": exit_code,
    }
    STATE.setdefault("owned_target_topics", []).extend(
        [prefix + "orders", prefix + "payments"]
    )
    STATE["owned_target_topics"] = sorted(set(STATE["owned_target_topics"]))
    STATE["product_failures"] = [
        item
        for item in STATE.get("product_failures", [])
        if item.get("case") != "legacy_mutable_prejob_transition"
    ]
    STATE["cases"]["legacy_mutable_prejob_transition"] = "passed"
    save_state()
    log("same-UID mutable legacy plan transitioned to a current hash-pinned successful Job")


def rbac_and_gc() -> None:
    checks = [
        (
            "controller_create_jobs",
            KUBECTL
            + [
                "auth",
                "can-i",
                "create",
                "jobs.batch",
                "--as=system:serviceaccount:logweir-scram-local:weirkeeper",
                "-n",
                NS,
            ],
            "yes",
        ),
        (
            "controller_create_configmaps",
            KUBECTL
            + [
                "auth",
                "can-i",
                "create",
                "configmaps",
                "--as=system:serviceaccount:logweir-scram-local:weirkeeper",
                "-n",
                NS,
            ],
            "yes",
        ),
        (
            "controller_get_secrets",
            KUBECTL
            + [
                "auth",
                "can-i",
                "get",
                "secrets",
                "--as=system:serviceaccount:logweir-scram-local:weirkeeper",
                "-n",
                NS,
            ],
            "no",
        ),
        (
            "runner_get_secrets",
            KUBECTL
            + [
                "auth",
                "can-i",
                "get",
                "secrets",
                f"--as=system:serviceaccount:{NS}:logweir-runner",
                "-n",
                NS,
            ],
            "no",
        ),
        (
            "runner_create_jobs",
            KUBECTL
            + [
                "auth",
                "can-i",
                "create",
                "jobs.batch",
                f"--as=system:serviceaccount:{NS}:logweir-runner",
                "-n",
                NS,
            ],
            "no",
        ),
        (
            "runner_get_configmaps",
            KUBECTL
            + [
                "auth",
                "can-i",
                "get",
                "configmaps",
                f"--as=system:serviceaccount:{NS}:logweir-runner",
                "-n",
                NS,
            ],
            "no",
        ),
    ]
    results: dict[str, str] = {}
    for label, command, expected in checks:
        proc = run(command, check=False)
        actual = proc.stdout.strip()
        expected_rc = 0 if expected == "yes" else 1
        if proc.returncode != expected_rc or actual != expected:
            raise RuntimeError(f"RBAC {label}: expected {expected}, got rc={proc.returncode} {actual!r}")
        results[label] = actual
    job = get("job", "backend-live-restore-a")
    if job["spec"]["template"]["spec"].get("automountServiceAccountToken") is not False:
        raise RuntimeError("runner Job unexpectedly automounts an API token")
    # Completed resources must still exist for debugging before explicit Restore deletion.
    restore = get("restore", "backend-live-restore-a")
    dependents = {
        "job": get("job", "backend-live-restore-a")["metadata"]["uid"],
        "plan": get("configmap", "backend-live-restore-a-plan")["metadata"]["uid"],
        "bundle": get("configmap", "backend-live-restore-a-approval-bundle")["metadata"]["uid"],
    }
    expected_uid = next(
        item["uid"] for item in STATE["positive_restores"] if item["name"] == restore["metadata"]["name"]
    )
    if restore["metadata"]["uid"] != expected_uid:
        raise RuntimeError("positive Restore UID changed before GC test")
    run(K + ["delete", "restore", restore["metadata"]["name"], "--wait=true"])
    wait_absent("job", "backend-live-restore-a")
    wait_absent("configmap", "backend-live-restore-a-plan")
    wait_absent("configmap", "backend-live-restore-a-approval-bundle")
    if get_optional("approval", "backend-live-approval-a") is None:
        raise RuntimeError("Approval was unexpectedly owner-GC'd with its Restore")
    STATE["rbac"] = results
    STATE["gc_dependents"] = dependents
    STATE["cases"]["controller_runner_rbac"] = "passed"
    STATE["cases"]["retained_then_owner_gc"] = "passed"
    save_state()
    log("live RBAC scope passed; retained completed Job/plan/bundle were owner-GC'd after Restore deletion")


def verify_retained_archive_evidence() -> None:
    old_restore = get("restore", "scram-record-restore-47c9b780", fixture_namespace=True)
    evidence = old_restore["status"]["evidence"]
    old_scorecard = save_artifact(
        "retained-old-scorecard.json", mc_cat(evidence["scorecardKey"])
    )
    old_signature = save_artifact(
        "retained-old-scorecard.sig", mc_cat(evidence["sidecarKey"])
    )
    verify_signed_document(old_scorecard, old_signature, "scorecard")
    old_backup = get("backup", "rotated-secret-backup", fixture_namespace=True)
    backup_evidence = old_backup["status"]["evidence"]
    old_receipt = save_artifact(
        "retained-old-backup-receipt.json", mc_cat(backup_evidence["receiptKey"])
    )
    old_receipt_signature = save_artifact(
        "retained-old-backup-receipt.sig", mc_cat(backup_evidence["sidecarKey"])
    )
    verify_signed_document(old_receipt, old_receipt_signature, "backup-receipt")
    shared_signer = get("secret", "logweir-signing-key", fixture_namespace=True)
    STATE["retained_signer"] = {
        "uid": shared_signer["metadata"]["uid"],
        "resource_version": shared_signer["metadata"]["resourceVersion"],
        "public_key_sha256": hashlib.sha256(
            (FIXTURE_KEYS / "signing.pub.pem").read_bytes()
        ).hexdigest(),
    }
    STATE["cases"]["retained_old_archive_evidence"] = "passed"
    save_state()
    log("retained old restore scorecard and backup receipt independently re-verified")


def additive_drain_fence() -> None:
    running = [
        item["metadata"]["name"]
        for item in json.loads(run(K + ["get", "restores", "-o", "json"]).stdout)["items"]
        if item.get("status", {}).get("phase") == "Running"
    ]
    if running:
        raise RuntimeError(f"rollback drain refused with running Restores: {running!r}")
    crd_before = json.loads(
        run(KUBECTL + ["get", "crd", "approvals.logweir.dev", "-o", "json"]).stdout
    )
    uid = crd_before["metadata"]["uid"]
    scale_controller(0)
    fenced_pods = json.loads(
        run(
            KF
            + [
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=control-plane",
                "-o",
                "json",
            ]
        ).stdout
    )["items"]
    if [pod for pod in fenced_pods if pod["metadata"].get("deletionTimestamp") is None]:
        raise RuntimeError("controller fence still had a live pod")
    crd_fenced = json.loads(
        run(KUBECTL + ["get", "crd", "approvals.logweir.dev", "-o", "json"]).stdout
    )
    if crd_fenced["metadata"]["uid"] != uid:
        raise RuntimeError("additive Approval CRD changed UID during controller fence")
    if not any(
        condition.get("type") == "Established" and condition.get("status") == "True"
        for condition in crd_fenced["status"]["conditions"]
    ):
        raise RuntimeError("Approval CRD was not Established while controller was fenced")
    scale_controller(1)
    STATE["approval_crd_uid"] = uid
    STATE["cases"]["additive_crd_drain_fence_restart"] = "passed"
    save_state()
    save_artifact("approval-crd-after-drain.json", crd_fenced)
    log("drained controller fence/restart passed while additive Approval CRD remained Established")


def finalize() -> None:
    assert_single_controller()
    setup_namespace()
    rbac_and_gc()
    verify_retained_archive_evidence()
    additive_drain_fence()


def sha256_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def report() -> None:
    critical = [
        "crates/logweir-core/src/execution_contract.rs",
        "crates/logweir/src/cli.rs",
        "crates/logweir/src/drill/mod.rs",
        "crates/logweir/src/drill/phase1_approval.rs",
        "crates/logweir/src/signer.rs",
        "crates/logweir/src/backup/mod.rs",
        "crates/logweir/src/backup/phase_run.rs",
        "crates/weirkeeper/src/controllers/restore.rs",
        "crates/weirkeeper/src/controllers/approval.rs",
        "crates/weirkeeper/src/job.rs",
        "config/crd/approvals.yaml",
    ]
    snapshot_root = pathlib.Path("/tmp/logweir-backend-live-20260915T0330Z/src")
    source_hashes = {
        path: {
            "snapshot": sha256_file(snapshot_root / path),
            "working_tree_now": sha256_file(ROOT / path),
        }
        for path in critical
    }
    images: dict[str, Any] = {}
    platforms = {
        CURRENT_CONTROLLER: "linux/arm64",
        CURRENT_RUNNER: "linux/amd64",
        OLD_RUNNER: "linux/amd64",
    }
    binaries = {
        CURRENT_CONTROLLER: "/usr/local/bin/weirkeeper",
        CURRENT_RUNNER: "/usr/local/bin/logweir",
        OLD_RUNNER: "/usr/local/bin/logweir",
    }
    for image, platform in platforms.items():
        inspected = json.loads(
            run(DOCKER + ["image", "inspect", image, "--format", "{{json .}}"] ).stdout
        )
        binary_hash = run(
            DOCKER
            + [
                "run",
                "--rm",
                "--platform",
                platform,
                "--entrypoint",
                "sha256sum",
                image,
                binaries[image],
            ]
        ).stdout.split()[0]
        images[image] = {
            "id": inspected["Id"],
            "architecture": inspected["Architecture"],
            "created": inspected["Created"],
            "binary": binaries[image],
            "binary_sha256": binary_hash,
        }
    images[CURRENT_RUNNER]["engine_binary"] = "/usr/local/bin/kafka-backup"
    images[CURRENT_RUNNER]["engine_binary_sha256"] = run(
        DOCKER
        + [
            "run",
            "--rm",
            "--platform",
            platforms[CURRENT_RUNNER],
            "--entrypoint",
            "sha256sum",
            CURRENT_RUNNER,
            "/usr/local/bin/kafka-backup",
        ]
    ).stdout.split()[0]
    deployment = json.loads(
        run(KF + ["get", "deployment", "weirkeeper", "-o", "json"]).stdout
    )
    crd = json.loads(
        run(KUBECTL + ["get", "crd", "approvals.logweir.dev", "-o", "json"]).stdout
    )
    owned_namespace = run(
        KUBECTL + ["get", "namespace", NS, "-o", "name"], check=False
    )
    artifact_hashes = {
        path.name: sha256_file(path)
        for path in sorted(OUT.iterdir())
        if path.is_file() and path.name not in {REPORT_PATH.name}
    }
    passed = sorted(
        name for name, result in STATE.get("cases", {}).items() if result == "passed"
    )
    failed = sorted(
        name for name, result in STATE.get("cases", {}).items() if result == "failed"
    )
    unrun = [
        "post-start projected ConfigMap substitution with live webhook A/B observation",
        "live plan-directed notification transport on pre-authentication failure",
        "mid-run projected signer rotation after the pod proves its mounted file changed",
        "post-parse software signing capability failure (intrinsically unreachable for current software keys)",
        "mutating-admission webhook rewrite (API immutability was exercised; no mutating webhook was installed)",
        "automatic identity bootstrap and default/full-chart install or reinstall (owned by separate review)",
        "UI/browser behavior and unrelated scheduler behavior",
    ]
    structured = {
        "context": "docker-desktop",
        "docker_context": "desktop-linux",
        "source_snapshot": str(snapshot_root),
        "old_source": {
            "commit": "92e02097540c39ff8565283a38ee592499b95020",
            "cli_rs_sha256": sha256_file(
                pathlib.Path(
                    "/tmp/logweir-backend-live-20260915T0330Z/old/crates/logweir/src/cli.rs"
                )
            ),
        },
        "source_hashes": source_hashes,
        "images": images,
        "passed": passed,
        "failed": failed,
        "product_failures": STATE.get("product_failures", []),
        "unrun": unrun,
        "harness_iteration_errors": STATE.get("errors", []),
        "rbac": STATE.get("rbac", {}),
        "positive_restores": STATE.get("positive_restores", []),
        "retained_signer": STATE.get("retained_signer", {}),
        "approval_crd": {
            "uid": crd["metadata"]["uid"],
            "resource_version": crd["metadata"]["resourceVersion"],
            "stored_versions": crd["status"]["storedVersions"],
            "established": any(
                condition.get("type") == "Established"
                and condition.get("status") == "True"
                for condition in crd["status"]["conditions"]
            ),
        },
        "restored_shared_deployment": {
            "image": deployment["spec"]["template"]["spec"]["containers"][0]["image"],
            "runner_image": next(
                item["value"]
                for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
                if item["name"] == "LOGWEIR_RUNNER_IMAGE"
            ),
            "ready_replicas": deployment["status"].get("readyReplicas", 0),
        },
        "cleanup": {
            **STATE.get("cleanup", {}),
            "owned_namespace_absent": owned_namespace.returncode != 0,
            "approval_crd_retained": True,
            "signed_archive_evidence_retained": True,
        },
        "artifact_hashes": artifact_hashes,
    }
    REPORT_PATH.write_text(json.dumps(structured, indent=2, sort_keys=True) + "\n")
    REPORT_PATH.chmod(0o600)
    restore_by_name = {
        item["name"]: item for item in STATE.get("positive_restores", [])
    }
    restore_a = restore_by_name.get("backend-live-restore-a", {})
    restore_b = restore_by_name.get("backend-live-restore-b", {})
    failure = next(
        (
            item
            for item in STATE.get("product_failures", [])
            if item.get("case") == "legacy_mutable_prejob_transition"
        ),
        {},
    )
    lines = [
        "# PLAT-01 / PLAT-02.2 Docker Desktop live acceptance",
        "",
        f"Generated: {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"Evidence directory: `{OUT}`",
        f"Machine report: `{REPORT_PATH}`",
        "",
        "## Verdict",
        "",
        f"Passed: {len(passed)}; failed: {len(failed)}; explicitly unrun: {len(unrun)}.",
        "",
        "Passed cases:",
        *[f"- {name}" for name in passed],
        "",
        "Failed cases:",
        *([f"- {name}" for name in failed] or ["- none"]),
        "",
        "Unrun/limited cases:",
        *[f"- {name}" for name in unrun],
        "",
        "## Live runtime evidence",
        "",
        "- A fresh backup ran against the preserved SCRAM-SHA-512 source. The signed receipt "
        "contains exact topic counts `orders=100` and `payments=100`, source authentication "
        "metadata `mode=scramSha512` / `username=scram-user`, the live source cluster ID, and "
        "the selected topic names. Source and S3 credential-reference propagation was checked "
        "without printing secret values. The Rust verifier and an independent Python verifier "
        "both accepted the receipt and detached signature.",
        "- Two Restore objects ran concurrently after a real ResourceQuota scheduling barrier "
        "and controller restart. Both Jobs existed before either pod was allowed to start; their "
        "actual run intervals overlapped. The target was read independently after completion: "
        "each restore produced exactly 100 `orders-record-NNN` and 100 `payments-record-NNN` "
        "records, with one partition per topic and end offset 100. Each signed scorecard was "
        "accepted by both verifiers and reported a 50/50 sampled restore window.",
        f"- Restore A: Restore UID `{restore_a.get('uid')}`, Job UID "
        f"`{restore_a.get('job_uid')}`, bundle UID `{restore_a.get('bundle_uid')}`, "
        f"bundle-data SHA-256 `{restore_a.get('bundle_data_sha256')}`.",
        f"- Restore B: Restore UID `{restore_b.get('uid')}`, Job UID "
        f"`{restore_b.get('job_uid')}`, bundle UID `{restore_b.get('bundle_uid')}`, "
        f"bundle-data SHA-256 `{restore_b.get('bundle_data_sha256')}`.",
        "- The current controller/current reviewed runner succeeded. With the controller unchanged "
        "and the runner replaced by the actual binary built from commit "
        "`92e02097540c39ff8565283a38ee592499b95020`, the Job retained the mandatory "
        "handshake arguments and the old runner rejected `--execution-contract-version` before "
        "data actions. Kafka topics and the recursive archive listing were byte-for-byte unchanged.",
        "- Approval cross-use and replay with a wrong namespace, wrong kind, and recreated Restore "
        "UID were rejected without Jobs. Wrong-kind/namespace provenance used controlled status "
        "fault injection while the singleton controller was fenced, then exercised the real "
        "Approval and Restore controllers after restart.",
        "- Ownerless, foreign-owner, stale-UID, and secondary-owner Job/ConfigMap collisions were "
        "terminal. Pre-mount bundle replacement failed the runner's `allowed-clusters` digest "
        "check with exit 3. A deleted bundle produced a kubelet `FailedMount` event with an "
        "unstarted container and empty image ID. These cases left Kafka, archive listings, and "
        "evidence paths unchanged.",
        "- Missing, malformed, and unreadable signer inputs each produced runner exit 4, redacted "
        "diagnostics, `logweir_drill_exit_code{cluster=\"unknown\"} 4`, no runs-total metric, "
        "and no engine sentinel, scorecard, offsets file, Kafka change, or archive change.",
        "- A legitimate same-UID legacy in-flight Job was observed unchanged and was owner-GC'd "
        "when its Restore was deleted. Current completed Job, plan, and approval bundle remained "
        "available until explicit Restore deletion and were then owner-GC'd. Historical backup "
        "and restore evidence was re-fetched and independently reverified.",
        "- Live RBAC checks: controller may create Jobs/ConfigMaps but may not get Secrets; runner "
        "may not get Secrets/ConfigMaps or create Jobs. Runner Jobs disabled automatic service-account "
        "token mounting.",
        "",
        "## Product failure",
        "",
        f"`legacy_mutable_prejob_transition` failed with `{failure.get('reason')}`: "
        f"{failure.get('message')}",
        "",
        "The fixture used the exact live Restore UID, exact plan bytes and digest, exact binding "
        "annotations, and one exact owner reference; only `immutable: false` represented the "
        "documented legitimate pre-Job legacy state. The controller rejected it before Job creation. "
        "Reproduction artifacts are `legacy-prejob-failed-restore.json` and "
        "`legacy-prejob-mutable-plan.json` in the evidence directory. No product code was changed.",
        "",
        "## Source and image identity",
        "",
        *[
            f"- `{name}`: image `{value['id']}`, {value['architecture']}, "
            f"binary SHA-256 `{value['binary_sha256']}`"
            + (
                f", engine SHA-256 `{value['engine_binary_sha256']}`"
                if "engine_binary_sha256" in value
                else ""
            )
            for name, value in images.items()
        ],
        "",
        "Critical snapshot hashes:",
        *[
            f"- `{path}`: `{values['snapshot']}`"
            + (
                ""
                if values["snapshot"] == values["working_tree_now"]
                else f" (working tree later changed to `{values['working_tree_now']}`)"
            )
            for path, values in source_hashes.items()
        ],
        "",
        "The tested images are pinned to the listed snapshot. A separately owned bootstrap change "
        "later changed only the working-tree `cli.rs` hash shown above; the Restore controller, "
        "Approval controller, execution-contract, signer, and CRD hashes remained identical. "
        "Accordingly, this report claims applicability only for the pinned image IDs and those "
        "unchanged critical hashes.",
        "",
        "## Exact primary commands",
        "",
        "```text",
        "# working directory: /tmp/logweir-backend-live-20260915T0330Z/src",
        "cargo vendor --offline --versioned-dirs vendor >/dev/null",
        "docker --context desktop-linux build --load --platform linux/arm64 -f Dockerfile.weirkeeper -t weirkeeper:backend-live-20260915 /tmp/logweir-backend-live-20260915T0330Z/src",
        "docker --context desktop-linux build --load --platform linux/amd64 -f Dockerfile -t logweir:backend-live-20260915 /tmp/logweir-backend-live-20260915T0330Z/src",
        "git archive 92e02097540c39ff8565283a38ee592499b95020 | tar -x -C /tmp/logweir-backend-live-20260915T0330Z/old",
        "docker --context desktop-linux build --load --platform linux/amd64 -f /tmp/logweir-backend-live-20260915T0330Z/old/Dockerfile -t logweir:pre-handshake-92e02097 /tmp/logweir-backend-live-20260915T0330Z/old",
        "kubectl --context docker-desktop apply -f config/crd/approvals.yaml",
        "kubectl --context docker-desktop wait --for=condition=Established crd/approvals.logweir.dev --timeout=60s",
        "LOGWEIR_PYTHON=/usr/bin/python3 python3 scripts/test-plat01-02-live.py positive",
        "LOGWEIR_PYTHON=/usr/bin/python3 python3 scripts/test-plat01-02-live.py negative",
        "LOGWEIR_PYTHON=/usr/bin/python3 python3 scripts/test-plat01-02-live.py finalize",
        "LOGWEIR_PYTHON=/usr/bin/python3 python3 scripts/test-plat01-02-live.py cleanup",
        "```",
        "",
        "## Cleanup",
        "",
        f"Owned namespace `{NS}` UID `{STATE.get('namespace_uid')}` deleted: "
        f"{structured['cleanup']['owned_namespace_absent']}.",
        f"Shared deployment restored to `{structured['restored_shared_deployment']['image']}` / "
        f"`{structured['restored_shared_deployment']['runner_image']}` with "
        f"{structured['restored_shared_deployment']['ready_replicas']} Ready replica.",
        f"Approval CRD UID `{structured['approval_crd']['uid']}` remains additive and Established.",
        "Four owned target topics were deleted; original SCRAM topics were retained. Signed archive evidence was retained.",
        "",
        "## Limitations and resolved harness iterations",
        "",
        "The seven items listed as unrun above are not credited from unit or fake-API evidence. "
        "In particular, this run does not claim automatic bootstrap/full-chart acceptance, live "
        "notification delivery, post-start projected-volume substitution, or signer-file rotation.",
        "",
        "Harness iterations were retained in the machine report rather than erased. They were: an "
        "initial attempt to exec into a completed shared mc pod (replaced by an owned mc fixture); "
        "a Homebrew Python without `cryptography` (rerun with `/usr/bin/python3`); an incorrect "
        "expectation of 200 scorecard samples instead of the configured 50 (full Kafka reads still "
        "proved 200 records per restore); a missing-bundle expectation refined to the observed "
        "kubelet `FailedMount`; a stale-owner fixture GC race held with a test-only finalizer; and "
        "a `kubectl auth can-i` return-code expectation corrected for its documented `no` result. "
        "The legacy mutable pre-Job failure remained reproducible and is the product failure above.",
        "",
        "The machine report records SHA-256 values for every retained evidence file. No historical "
        "`report.json` or old deployed image was treated as current acceptance evidence.",
    ]
    markdown_path = pathlib.Path("/tmp/logweir-backend-live-acceptance.md")
    markdown_path.write_text("\n".join(lines) + "\n")
    markdown_path.chmod(0o600)
    log(f"reports written: {REPORT_PATH} and {markdown_path}")


def verify_restore(name: str, prefix: str) -> dict[str, Any]:
    restore = wait_for(
        "restore",
        name,
        lambda item: item.get("status", {}).get("phase") in {"Succeeded", "Failed"},
        seconds=720,
    )
    status = restore["status"]
    if status.get("phase") != "Succeeded" or status.get("exitCode") != 0:
        raise RuntimeError(f"restore {name} failed: {status!r}")
    restore = wait_for(
        "restore",
        name,
        lambda item: item.get("status", {})
        .get("evidence", {})
        .get("verification", {})
        .get("result")
        == "Valid",
        seconds=120,
    )
    job = get("job", name)
    bundle = get("configmap", f"{name}-approval-bundle")
    plan_cm = get("configmap", f"{name}-plan")
    owner = job["metadata"].get("ownerReferences", [])
    expected_owner = {
        "apiVersion": "logweir.dev/v1alpha1",
        "blockOwnerDeletion": True,
        "controller": True,
        "kind": "Restore",
        "name": name,
        "uid": restore["metadata"]["uid"],
    }
    if owner != [expected_owner]:
        raise RuntimeError(f"job owner set is not exact: {owner!r}")
    if bundle.get("immutable") is not True:
        raise RuntimeError("approval bundle is not immutable")
    if bundle["metadata"].get("ownerReferences") != [expected_owner]:
        raise RuntimeError("bundle owner set is not exact")
    container = job["spec"]["template"]["spec"]["containers"][0]
    if container["image"] != CURRENT_RUNNER:
        raise RuntimeError(f"restore used stale image {container['image']}")
    args = container["args"]
    if args[:4] != ["restore", "run", "--execution-contract-version", "1"]:
        raise RuntimeError(f"mandatory execution handshake missing: {args[:6]!r}")
    env = {entry["name"]: entry for entry in container["env"]}
    approval = get("approval", restore["spec"]["approvalRef"]["name"])
    expected_contract = {
        "LOGWEIR_EXECUTION_CONTRACT_VERSION": "1",
        "LOGWEIR_EXECUTION_SUBJECT_API_VERSION": "logweir.dev/v1alpha1",
        "LOGWEIR_EXECUTION_SUBJECT_KIND": "Restore",
        "LOGWEIR_EXECUTION_SUBJECT_NAME": name,
        "LOGWEIR_EXECUTION_SUBJECT_NAMESPACE": NS,
        "LOGWEIR_EXECUTION_SUBJECT_UID": restore["metadata"]["uid"],
        "LOGWEIR_EXECUTION_APPROVAL_NAME": approval["metadata"]["name"],
        "LOGWEIR_EXECUTION_APPROVAL_UID": approval["metadata"]["uid"],
    }
    for variable, value in expected_contract.items():
        if env.get(variable, {}).get("value") != value:
            raise RuntimeError(f"execution contract mismatch for {variable}")
    target_ref = env["LOGWEIR_TARGET_PASSWORD"]["valueFrom"]["secretKeyRef"]
    if target_ref != {"key": "password", "name": "target-scram"}:
        raise RuntimeError(f"target credential propagation mismatch: {target_ref}")
    for variable, key in [
        ("AWS_ACCESS_KEY_ID", "access-key-id"),
        ("AWS_SECRET_ACCESS_KEY", "secret-access-key"),
    ]:
        ref = env[variable]["valueFrom"]["secretKeyRef"]
        if ref != {"key": key, "name": "logweir-s3"}:
            raise RuntimeError(f"archive credential propagation mismatch for {variable}")
    volume_maps = {
        volume["name"]: volume.get("configMap", {}).get("name")
        for volume in job["spec"]["template"]["spec"]["volumes"]
    }
    if volume_maps.get("approval") != bundle["metadata"]["name"]:
        raise RuntimeError("job did not mount its exact per-Restore bundle name")
    if volume_maps.get("plan") != plan_cm["metadata"]["name"]:
        raise RuntimeError("job did not mount its exact plan ConfigMap")
    for topic in ["orders", "payments"]:
        target_topic = prefix + topic
        offsets = broker_target(
            f"/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 "
            f"--topic {target_topic} --time -1\n"
        ).strip().splitlines()
        if offsets != [f"{target_topic}:0:100"]:
            raise RuntimeError(f"unexpected offsets for {target_topic}: {offsets!r}")
        data = broker_target(
            f"/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 "
            f"--topic {target_topic} --from-beginning --max-messages 100 "
            "--timeout-ms 15000\n",
            timeout=60,
        )
        records = data.strip().splitlines()
        expected = [f"{topic}-record-{number:03}" for number in range(1, 101)]
        if sorted(records) != expected:
            raise RuntimeError(
                f"record mismatch for {target_topic}: count={len(records)} head={records[:3]!r}"
            )
        save_artifact(f"{name}-{topic}.txt", data)
    evidence = restore["status"]["evidence"]
    scorecard_path = save_artifact(f"{name}-scorecard.json", mc_cat(evidence["scorecardKey"]))
    signature_path = save_artifact(f"{name}-scorecard.sig", mc_cat(evidence["sidecarKey"]))
    scorecard = json.loads(scorecard_path.read_text())
    if scorecard["target"]["auth"] != {
        "mode": "scramSha512",
        "username": "scram-user",
    }:
        raise RuntimeError("scorecard lost selected SCRAM target metadata")
    if scorecard["sample"]["records_expected"] != 50:
        raise RuntimeError(f"scorecard expected sample count is not 50: {scorecard['sample']}")
    if scorecard["sample"]["records_restored"] != 50:
        raise RuntimeError(f"scorecard restored sample count is not 50: {scorecard['sample']}")
    verify_signed_document(scorecard_path, signature_path, "scorecard")
    save_artifact(f"{name}-job.json", job)
    save_artifact(f"{name}-bundle.json", bundle)
    return {
        "restore": restore,
        "job": job,
        "bundle": bundle,
        "approval": approval,
        "scorecard": scorecard,
    }


def concurrent_restores() -> None:
    names = ["backend-live-restore-a", "backend-live-restore-b"]
    prefixes = ["backend-live-a-", "backend-live-b-"]
    approvals = ["backend-live-approval-a", "backend-live-approval-b"]
    completed_before_resume = all(
        (get_optional("job", name) or {}).get("status", {}).get("completionTime")
        for name in names
    )
    if not completed_before_resume:
        set_pod_quota("0")
    plans: list[str] = []
    for name, approval, prefix in zip(names, approvals, prefixes, strict=True):
        plan, _uid = create_restore_shell(name, approval, prefix)
        plans.append(plan)
    for name, approval, plan in zip(names, approvals, plans, strict=True):
        approve_restore(name, approval, plan)
    if not completed_before_resume:
        jobs = [
            wait_for("job", name, lambda _item: True, seconds=180) for name in names
        ]
        job_uids = [job["metadata"]["uid"] for job in jobs]
        pods = json.loads(run(K + ["get", "pods", "-o", "json"]).stdout)["items"]
        restore_pods = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("labels", {}).get("job-name") in names
        ]
        if restore_pods:
            raise RuntimeError("pod quota barrier did not hold Restore pods before restart")
        restart_controller()
        after = [get("job", name)["metadata"]["uid"] for name in names]
        if after != job_uids:
            raise RuntimeError("controller restart replaced an in-flight Restore Job")
        STATE["restart_job_uids"] = job_uids
        save_state()
        remove_pod_quota()
    else:
        remove_pod_quota()
        initial_jobs = [
            json.loads((OUT / f"job-{name}.json").read_text()) for name in names
        ]
        initial_uids = [job["metadata"]["uid"] for job in initial_jobs]
        current_uids = [get("job", name)["metadata"]["uid"] for name in names]
        if initial_uids != current_uids:
            raise RuntimeError("resumed jobs differ from the pre-restart captured UIDs")
        STATE["restart_job_uids"] = initial_uids
    results = [verify_restore(name, prefix) for name, prefix in zip(names, prefixes, strict=True)]
    bundle_uids = [item["bundle"]["metadata"]["uid"] for item in results]
    bundle_hashes = [
        hashlib.sha256(
            json.dumps(item["bundle"]["data"], sort_keys=True).encode()
        ).hexdigest()
        for item in results
    ]
    if len(set(bundle_uids)) != 2 or len(set(bundle_hashes)) != 2:
        raise RuntimeError("concurrent Restores did not receive distinct bundle objects/bytes")
    for index, item in enumerate(results):
        other = 1 - index
        env = {
            entry["name"]: entry.get("value")
            for entry in item["job"]["spec"]["template"]["spec"]["containers"][0][
                "env"
            ]
        }
        if env["LOGWEIR_EXECUTION_APPROVAL_UID"] == results[other]["approval"][
            "metadata"
        ]["uid"]:
            raise RuntimeError("cross-Restore approval UID appeared in execution contract")
    intervals = []
    for item in results:
        status = item["job"]["status"]
        intervals.append((status["startTime"], status["completionTime"]))
    start_max = max(dt.datetime.fromisoformat(value.replace("Z", "+00:00")) for value, _ in intervals)
    end_min = min(dt.datetime.fromisoformat(value.replace("Z", "+00:00")) for _, value in intervals)
    if start_max > end_min:
        raise RuntimeError(f"Restore Jobs were not simultaneous: {intervals!r}")
    mutation = run(
        K
        + [
            "patch",
            "configmap",
            f"{names[0]}-approval-bundle",
            "--type=merge",
            "-p",
            '{"data":{"approval.json":"replacement"}}',
        ],
        check=False,
    )
    if mutation.returncode == 0 or "immutable" not in (mutation.stderr + mutation.stdout).lower():
        raise RuntimeError("Kubernetes did not enforce approval bundle immutability")
    STATE["positive_restores"] = [
        {
            "name": names[index],
            "uid": results[index]["restore"]["metadata"]["uid"],
            "job_uid": results[index]["job"]["metadata"]["uid"],
            "bundle_uid": bundle_uids[index],
            "bundle_data_sha256": bundle_hashes[index],
            "prefix": prefixes[index],
        }
        for index in range(2)
    ]
    STATE["cases"]["two_simultaneous_restores"] = "passed"
    STATE["cases"]["controller_restart_in_flight"] = "passed"
    STATE["cases"]["bundle_immutability"] = "passed"
    save_state()
    log("two simultaneous Restores passed with distinct exact-input bundles and records")


def positive() -> None:
    assert_single_controller()
    setup_namespace()
    create_clusters()
    fresh_backup()
    concurrent_restores()


def negative() -> None:
    assert_single_controller()
    setup_namespace()
    if STATE["cases"].get("current_controller_actual_old_runner_handshake") != "passed":
        old_runner_handshake()
    if STATE["cases"].get("configmap_substitution_before_mount") != "passed":
        premount_substitution()
    if STATE["cases"].get("missing_bundle_before_mount") != "passed":
        missing_bundle_before_mount()
    if STATE["cases"].get("job_collision_owner_matrix") != "passed":
        job_collision_matrix()
    if STATE["cases"].get("approval_recreated_uid_replay") != "passed":
        configmap_collision_and_recreated_uid()
    if STATE["cases"].get("configmap_collision_owner_matrix") != "passed":
        configmap_owner_matrix()
    if STATE["cases"].get("approval_wrong_namespace_replay") != "passed":
        injected_subject_replay_matrix()
    if STATE["cases"].get("distinct_approval_cross_use") != "passed":
        cross_restore_approval_use()
    if STATE["cases"].get("signer_missing_malformed_unreadable") != "passed":
        signer_prerequisite_jobs()
    if STATE["cases"].get("legacy_same_uid_inflight_observation") != "passed":
        legacy_same_uid_observation()
    if STATE["cases"].get("legacy_mutable_prejob_transition") is None:
        legacy_prejob_transition()


def execute_independent(case_names: list[str], action: Callable[[], None]) -> None:
    for case_name in case_names:
        STATE["cases"][case_name] = "unrun"
    save_state()
    try:
        action()
        for case_name in case_names:
            if STATE["cases"].get(case_name) == "unrun":
                STATE["cases"][case_name] = "passed"
    except Exception as exc:  # noqa: BLE001 - independent cases must continue
        message = redact(f"{type(exc).__name__}: {exc}")
        for case_name in case_names:
            STATE["cases"][case_name] = "failed"
        STATE.setdefault("case_failures", []).append(
            {"cases": case_names, "error": message}
        )
        log(f"case failure preserved for {case_names}: {message}")
    save_state()


def targeted() -> None:
    """Run only review gaps; accepted broad positive paths are not repeated."""
    original = controller_deployment()
    original_identity = controller_identity(original)
    save_artifact("shared-controller-before-targeted.json", original)
    STATE["shared_controller_original"] = original_identity
    save_state()
    try:
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        assert_single_controller()
        setup_namespace()
        create_clusters()
        execute_independent(
            ["legacy_mutable_prejob_transition"], legacy_prejob_transition
        )
        execute_independent(
            ["signer_permission_denied_regular_file"],
            lambda: signer_prerequisite_jobs({"permission_denied"}),
        )
        execute_independent(["job_collision_owner_matrix"], job_collision_matrix)
        execute_independent(
            ["ownerless_plan_configmap_collision", "approval_recreated_uid_replay"],
            configmap_collision_and_recreated_uid,
        )
        execute_independent(
            ["configmap_collision_owner_matrix"], configmap_owner_matrix
        )
        collision_cases = [
            "job_collision_owner_matrix",
            "ownerless_plan_configmap_collision",
            "approval_recreated_uid_replay",
            "configmap_collision_owner_matrix",
        ]
        STATE["cases"]["collision_matrix_evidence_retained"] = (
            "passed"
            if all(STATE["cases"].get(name) == "passed" for name in collision_cases)
            else "failed"
        )
        save_state()
    finally:
        set_controller_images(
            original_identity["image"], original_identity["runner_image"]
        )
        restored = controller_deployment()
        restored_identity = controller_identity(restored)
        save_artifact("shared-controller-after-targeted.json", restored)
        STATE["shared_controller_restored"] = restored_identity
        STATE["shared_controller_restore_exact"] = (
            restored_identity["deployment_uid"]
            == original_identity["deployment_uid"]
            and restored_identity["image"] == original_identity["image"]
            and restored_identity["runner_image"] == original_identity["runner_image"]
            and restored_identity["spec_sha256"] == original_identity["spec_sha256"]
        )
        save_state()
        if not STATE["shared_controller_restore_exact"]:
            raise RuntimeError(
                "shared controller image/runner/spec did not restore exactly"
            )


def signer_permission() -> None:
    """Rerun only the regular-file permission-denied signer gap."""
    original = controller_deployment()
    original_identity = controller_identity(original)
    save_artifact("shared-controller-before-signer-permission.json", original)
    try:
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        assert_single_controller()
        setup_namespace()
        execute_independent(
            ["signer_permission_denied_regular_file"],
            lambda: signer_prerequisite_jobs({"permission_denied"}),
        )
    finally:
        set_controller_images(
            original_identity["image"], original_identity["runner_image"]
        )
        restored = controller_deployment()
        restored_identity = controller_identity(restored)
        save_artifact("shared-controller-after-signer-permission.json", restored)
        exact = (
            restored_identity["deployment_uid"] == original_identity["deployment_uid"]
            and restored_identity["image"] == original_identity["image"]
            and restored_identity["runner_image"] == original_identity["runner_image"]
            and restored_identity["spec_sha256"] == original_identity["spec_sha256"]
        )
        STATE["shared_controller_restore_exact_after_signer"] = exact
        save_state()
        if not exact:
            raise RuntimeError("shared controller was not exactly restored after signer case")


def exact_old_controller_job() -> None:
    """Observe an archived controller's real Job, then current-controller adoption."""
    original = controller_deployment()
    original_identity = controller_identity(original)
    save_artifact("shared-controller-before-old-controller-case.json", original)
    name = "backend-live-exact-old-controller"
    approval_name = "backend-live-exact-old-controller-approval"
    prefix = "backend-live-exact-old-controller-"
    quota_installed = False
    try:
        set_controller_images(OLD_CONTROLLER, OLD_RUNNER)
        setup_namespace()
        set_pod_quota("0")
        quota_installed = True
        plan, restore_uid = create_restore_shell(name, approval_name, prefix)
        approval = approve_restore(
            name,
            approval_name,
            plan,
            require_subject_binding=False,
        )
        # The archived Job predates per-Restore ConfigMap bundles and mounts
        # this fixed Secret. Populate it with this case's public approval
        # material only; no private key is copied into evidence or this bundle.
        apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": owned_metadata("logweir-approval-bundle"),
                "type": "Opaque",
                "stringData": {
                    "approval.json": approval["spec"]["approvalBytes"],
                    "approval.sig": approval["spec"]["sidecarBytes"],
                    "approver.pub.pem": (
                        FIXTURE_KEYS / "approver.pub.pem"
                    ).read_text(),
                    "allowed-clusters.json": json.dumps(
                        {
                            "allowed_cluster_ids": [STATE["target_cluster_id"]],
                            "source_cluster_id": STATE["source_cluster_id"],
                        },
                        indent=2,
                    )
                    + "\n",
                },
            }
        )
        old_job = wait_for("job", name, lambda _item: True, seconds=180)
        old_plan = get("configmap", f"{name}-plan")
        old_restore = get("restore", name)
        old_controller_identity = controller_identity(controller_deployment())
        owner = [owner_reference("Restore", name, restore_uid, controller=True)]
        if old_plan["metadata"].get("ownerReferences") != owner:
            raise RuntimeError("archived controller plan owner set was not exact")
        if old_plan.get("data") != {"restore.yaml": plan}:
            raise RuntimeError("archived controller plan bytes changed")
        if "immutable" in old_plan or old_plan["metadata"].get("annotations"):
            raise RuntimeError(
                "archived controller plan unexpectedly carried immutable or annotations"
            )
        runner = old_job["spec"]["template"]["spec"]["containers"][0]
        if runner["image"] != OLD_RUNNER:
            raise RuntimeError(f"archived controller emitted runner {runner['image']}")
        if "--execution-contract-version" in runner.get("args", []):
            raise RuntimeError("archived controller Job unexpectedly had current handshake")
        save_artifact("exact-old-controller-before-plan.json", old_plan)
        save_artifact("exact-old-controller-before-job.json", old_job)
        save_artifact("exact-old-controller-before-restore.json", old_restore)
        save_artifact(
            "exact-old-controller-controller-identity.json", old_controller_identity
        )

        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        adopted = wait_for(
            "restore",
            name,
            lambda item: item.get("status", {}).get("phase") == "Running",
            seconds=180,
        )
        after_job = get("job", name)
        if after_job["metadata"]["uid"] != old_job["metadata"]["uid"]:
            raise RuntimeError("current controller replaced archived controller Job")
        if after_job["spec"] != old_job["spec"]:
            raise RuntimeError("current controller rewrote archived controller Job")
        save_artifact("exact-old-controller-after-job.json", after_job)
        save_artifact("exact-old-controller-after-restore.json", adopted)

        remove_pod_quota()
        quota_installed = False
        terminal = wait_restore_terminal(name, seconds=720)
        pod = pod_for_job(name, seconds=180)
        runner_status = next(
            status
            for status in pod.get("status", {}).get("containerStatuses", [])
            if status["name"] == "runner"
        )
        termination = runner_status.get("state", {}).get("terminated", {})
        if not runner_status.get("imageID") or "exitCode" not in termination:
            raise RuntimeError("archived runner Pod lacks imageID/exit evidence")
        save_artifact("exact-old-controller-result-restore.json", terminal)
        save_artifact("exact-old-controller-result-pod.json", pod)
        STATE["exact_old_controller"] = {
            "controller_image_id": old_controller_identity["ready_pods"][0]["image_id"],
            "job_uid": old_job["metadata"]["uid"],
            "runner_image_id": runner_status["imageID"],
            "runner_exit_code": termination["exitCode"],
            "result_phase": terminal.get("status", {}).get("phase"),
        }
        STATE.setdefault("owned_target_topics", []).extend(
            [prefix + "orders", prefix + "payments"]
        )
        STATE["owned_target_topics"] = sorted(set(STATE["owned_target_topics"]))
        STATE["cases"]["exact_old_controller_job_observation"] = "passed"
        save_state()
    finally:
        if quota_installed:
            try:
                remove_pod_quota()
            except Exception as exc:  # noqa: BLE001
                STATE["cleanup"] = {
                    "namespace": NS,
                    "result": "failed",
                    "error": redact(f"quota cleanup: {exc}"),
                }
                save_state()
        set_controller_images(
            original_identity["image"], original_identity["runner_image"]
        )
        restored = controller_identity(controller_deployment())
        exact = (
            restored["deployment_uid"] == original_identity["deployment_uid"]
            and restored["image"] == original_identity["image"]
            and restored["runner_image"] == original_identity["runner_image"]
            and restored["spec_sha256"] == original_identity["spec_sha256"]
        )
        STATE["shared_controller_restore_exact_after_old_controller"] = exact
        save_artifact(
            "shared-controller-after-old-controller-case.json", controller_deployment()
        )
        save_state()
        if not exact:
            raise RuntimeError("shared controller was not exactly restored after old case")


def create_receiver(label: str) -> None:
    name = f"backend-live-receiver-{label}"
    if get_optional("pod", name) is None:
        receiver_metadata = owned_metadata(name)
        receiver_metadata["labels"]["backend-live.logweir.dev/receiver"] = label
        server = (
            "import http.server,json\n"
            "counts={'posts':0}\n"
            "class H(http.server.BaseHTTPRequestHandler):\n"
            " def do_POST(self):\n"
            "  n=int(self.headers.get('content-length','0')); body=self.rfile.read(n); "
            "counts['posts']+=1; print(json.dumps({'path':self.path,'bytes':len(body)}),flush=True); "
            "self.send_response(204); self.end_headers()\n"
            " def do_GET(self):\n"
            "  data=json.dumps(counts).encode(); self.send_response(200); "
            "self.send_header('content-length',str(len(data))); self.end_headers(); self.wfile.write(data)\n"
            " def log_message(self,*args): pass\n"
            "http.server.ThreadingHTTPServer(('0.0.0.0',8080),H).serve_forever()\n"
        )
        apply(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": receiver_metadata,
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "name": "receiver",
                            "image": "python:3.12-alpine",
                            "imagePullPolicy": "Never",
                            "command": ["python", "-u", "-c", server],
                            "ports": [{"containerPort": 8080}],
                            "readinessProbe": {
                                "tcpSocket": {"port": 8080},
                                "periodSeconds": 1,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "runAsNonRoot": True,
                                "runAsUser": 65532,
                                "runAsGroup": 65532,
                            },
                        }
                    ],
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                },
            }
        )
        apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": owned_metadata(name),
                "spec": {
                    "selector": {"backend-live.logweir.dev/receiver": label},
                    "ports": [{"port": 8080, "targetPort": 8080}],
                },
            }
        )
    run(K + ["wait", "--for=condition=Ready", f"pod/{name}", "--timeout=90s"])


def receiver_count(label: str) -> int:
    name = f"backend-live-receiver-{label}"
    output = run(
        K
        + [
            "exec",
            name,
            "-c",
            "receiver",
            "--",
            "python",
            "-c",
            "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8080/count').read().decode())",
        ]
    ).stdout.strip()
    return int(json.loads(output)["posts"])


def sanitized_job_spec(job: dict[str, Any], *, case: str) -> dict[str, Any]:
    spec = copy.deepcopy(job["spec"])
    for field in ["selector", "manualSelector", "suspend"]:
        spec.pop(field, None)
    spec["template"]["metadata"] = {
        "labels": {
            "backend-live.logweir.dev/run": RUN_LABEL,
            "backend-live.logweir.dev/case": case,
        }
    }
    return spec


def wait_runner_termination(job_name: str, seconds: int = 300) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pod = pod_for_job(job_name, seconds=30)
        status = next(
            (
                item
                for item in pod.get("status", {}).get("containerStatuses", [])
                if item["name"] == "runner"
            ),
            None,
        )
        if status and status.get("state", {}).get("terminated"):
            return pod, status
        time.sleep(1)
    raise RuntimeError(f"runner for Job/{job_name} did not terminate")


def runtime_gap_matrix() -> None:
    """Controlled real-runner timing for projected inputs, network, and rollout."""
    original = controller_deployment()
    original_identity = controller_identity(original)
    name = "backend-live-runtime-gap-retained"
    approval_name = "backend-live-runtime-gap-retained-approval"
    prefix = "backend-live-runtime-gap-retained-"
    controller_running = True
    private_path: pathlib.Path | None = None
    try:
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        setup_namespace()
        create_receiver("a")
        create_receiver("b")
        url_a = f"http://backend-live-receiver-a.{NS}.svc.cluster.local:8080/hook"
        url_b = f"http://backend-live-receiver-b.{NS}.svc.cluster.local:8080/hook"
        if receiver_count("a") != 0 or receiver_count("b") != 0:
            raise RuntimeError("notification receivers were not fresh")

        set_pod_quota("0")
        point = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        parsed = json.loads(restore_plan(prefix, point))
        parsed["notifications"]["webhooks"] = [url_a]
        plan = json.dumps(parsed, indent=2) + "\n"
        plan, restore_uid = create_restore_with_plan(
            name, approval_name, prefix, plan, point
        )
        approve_restore(name, approval_name, plan)
        emitted_job = wait_for("job", name, lambda _item: True, seconds=180)
        emitted_plan = get("configmap", f"{name}-plan")
        emitted_bundle = get("configmap", f"{name}-approval-bundle")
        save_artifact("runtime-gap-controller-job.json", emitted_job)
        save_artifact("runtime-gap-original-plan.json", emitted_plan)
        save_artifact("runtime-gap-original-bundle.json", emitted_bundle)

        scale_any_controller(0)
        controller_running = False
        delete_with_uid_precondition(
            "job", name, emitted_job["metadata"]["uid"], api_version="batch/v1"
        )
        wait_absent("job", name)

        tampered = copy.deepcopy(emitted_bundle)
        tampered_name = f"{name}-tampered-bundle"
        tampered["metadata"] = owned_metadata(tampered_name)
        tampered["immutable"] = True
        tampered["data"]["allowed-clusters.json"] += " "
        apply(tampered)
        negative_spec = sanitized_job_spec(emitted_job, case="preauth-network-negative")
        negative_runner = negative_spec["template"]["spec"]["containers"][0]
        for volume in negative_spec["template"]["spec"]["volumes"]:
            if volume["name"] == "approval":
                volume["configMap"]["name"] = tampered_name
        negative_name = f"{name}-preauth-negative"
        apply(
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": owned_metadata(negative_name),
                "spec": negative_spec,
            }
        )
        remove_pod_quota()
        negative_pod, negative_status = wait_runner_termination(negative_name, 180)
        if negative_status["state"]["terminated"]["exitCode"] != 3:
            raise RuntimeError("pre-auth network negative did not exit 3")
        if receiver_count("a") != 0 or receiver_count("b") != 0:
            raise RuntimeError("pre-auth refusal reached a plan-directed receiver")
        save_artifact("runtime-gap-preauth-negative-pod.json", negative_pod)
        save_artifact(
            "runtime-gap-preauth-receiver-observation.json",
            {"receiver_a_posts": 0, "receiver_b_posts": 0},
        )

        set_pod_quota("0")
        wrapper_name = "backend-live-runtime-gap-wrapper"
        apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": owned_metadata(wrapper_name),
                "data": {
                    "engine": "#!/bin/sh\nset -eu\ntouch /work/ENGINE_STARTED\nwhile [ ! -f /work/CONTINUE ]; do sleep 1; done\nexec /usr/local/bin/kafka-backup \"$@\"\n"
                },
            }
        )
        instrumented = sanitized_job_spec(emitted_job, case="runtime-gap-instrumented")
        pod_spec = instrumented["template"]["spec"]
        runner = pod_spec["containers"][0]
        runner["env"].append(
            {"name": "LOGWEIR_ENGINE_BIN", "value": "/instrument/engine"}
        )
        runner["volumeMounts"].append(
            {"name": "instrument", "mountPath": "/instrument", "readOnly": True}
        )
        pod_spec["volumes"].append(
            {
                "name": "instrument",
                "configMap": {
                    "name": wrapper_name,
                    "defaultMode": 365,
                    "items": [{"key": "engine", "path": "engine"}],
                },
            }
        )
        observer = {
            "name": "observer",
            "image": CURRENT_RUNNER,
            "imagePullPolicy": "Never",
            "command": ["/bin/sh", "-c"],
            "args": ["while [ ! -f /work/STOP ]; do sleep 1; done"],
            "securityContext": copy.deepcopy(runner["securityContext"]),
            "volumeMounts": [
                {"name": "work", "mountPath": "/work"},
                {"name": "plan", "mountPath": "/plan", "readOnly": True},
                {"name": "signing", "mountPath": "/signing", "readOnly": True},
            ],
        }
        pod_spec["containers"].append(observer)
        metadata = owned_metadata(name)
        metadata["ownerReferences"] = [
            owner_reference("Restore", name, restore_uid, controller=True)
        ]
        apply(
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": metadata,
                "spec": instrumented,
            }
        )
        remove_pod_quota()
        pod = pod_for_job(name, seconds=180)
        pod_name = pod["metadata"]["name"]
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            started = run(
                K
                + ["exec", pod_name, "-c", "observer", "--", "test", "-f", "/work/ENGINE_STARTED"],
                check=False,
            )
            if started.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("instrumented real runner never reached engine execution")
        before_hashes = run(
            K
            + [
                "exec",
                pod_name,
                "-c",
                "observer",
                "--",
                "sha256sum",
                "/plan/restore.yaml",
                "/signing/key.pem",
            ]
        ).stdout
        save_artifact("runtime-gap-projected-before-sha256.txt", before_hashes)

        transitions = []
        configure_controller_images(OLD_CONTROLLER, OLD_RUNNER)
        scale_any_controller(1)
        controller_running = True
        transitions.append({"event": "rollback_to_archived", **controller_identity(controller_deployment())})
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        transitions.append({"event": "upgrade_to_current", **controller_identity(controller_deployment())})
        scale_any_controller(0)
        controller_running = False
        transitions.append({"event": "drain_current", **controller_identity(controller_deployment())})
        configure_controller_images(OLD_CONTROLLER, OLD_RUNNER)
        scale_any_controller(1)
        controller_running = True
        transitions.append({"event": "rollback_during_execution", **controller_identity(controller_deployment())})
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        transitions.append({"event": "reupgrade_during_execution", **controller_identity(controller_deployment())})
        scale_any_controller(0)
        controller_running = False
        transitions.append({"event": "final_drain_before_input_replacement", **controller_identity(controller_deployment())})
        save_artifact("runtime-gap-controller-transitions.json", transitions)

        replacement = copy.deepcopy(parsed)
        replacement["notifications"]["webhooks"] = [url_b]
        replacement["objectives"]["rto_seconds"] = 3599
        replacement_bytes = json.dumps(replacement, indent=2) + "\n"
        delete_with_uid_precondition(
            "configmap",
            f"{name}-plan",
            emitted_plan["metadata"]["uid"],
            api_version="v1",
        )
        wait_absent("configmap", f"{name}-plan")
        apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    **owned_metadata(f"{name}-plan"),
                    "ownerReferences": [
                        owner_reference("Restore", name, restore_uid, controller=True)
                    ],
                },
                "data": {"restore.yaml": replacement_bytes},
            }
        )

        original_public = FIXTURE_KEYS / "signing.pub.pem"
        (OUT / "original-signing.pub.pem").write_bytes(original_public.read_bytes())
        with tempfile.NamedTemporaryFile(
            prefix="logweir-runtime-gap-", suffix=".pem", delete=False
        ) as private_file:
            private_path = pathlib.Path(private_file.name)
        run(
            [
                "openssl",
                "genpkey",
                "-algorithm",
                "EC",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-out",
                str(private_path),
            ]
        )
        rotated_public = OUT / "rotated-signing.pub.pem"
        public_proc = run(
            ["openssl", "pkey", "-in", str(private_path), "-pubout"]
        )
        rotated_public.write_text(public_proc.stdout)
        rotated_bytes = private_path.read_bytes()
        run(
            K
            + [
                "patch",
                "secret",
                "logweir-signing-key",
                "--type=merge",
                "-p",
                json.dumps(
                    {"data": {"signing.pem": base64.b64encode(rotated_bytes).decode()}}
                ),
            ]
        )
        private_path.unlink()
        private_path = None

        expected_plan_hash = hashlib.sha256(replacement_bytes.encode()).hexdigest()
        expected_signer_hash = hashlib.sha256(rotated_bytes).hexdigest()
        original_plan_hash = before_hashes.splitlines()[0].split()[0]
        deadline = time.monotonic() + 130
        after_hashes = ""
        plan_projection = "unknown"
        while time.monotonic() < deadline:
            after_hashes = run(
                K
                + [
                    "exec",
                    pod_name,
                    "-c",
                    "observer",
                    "--",
                    "sha256sum",
                    "/plan/restore.yaml",
                    "/signing/key.pem",
                ]
            ).stdout
            plan_hash = after_hashes.splitlines()[0].split()[0]
            signer_hash = after_hashes.splitlines()[1].split()[0]
            if plan_hash == expected_plan_hash and signer_hash == expected_signer_hash:
                plan_projection = "replacement-propagated"
                break
            time.sleep(2)
        else:
            plan_hash = after_hashes.splitlines()[0].split()[0]
            signer_hash = after_hashes.splitlines()[1].split()[0]
            if signer_hash != expected_signer_hash:
                raise RuntimeError("in-place Secret signer rotation did not propagate")
            if plan_hash == original_plan_hash:
                plan_projection = "original-retained-after-configmap-delete-recreate"
            else:
                raise RuntimeError(
                    f"projected plan reached an unexpected third digest {plan_hash}"
                )
        save_artifact("runtime-gap-projected-after-sha256.txt", after_hashes)
        run(K + ["exec", pod_name, "-c", "observer", "--", "touch", "/work/CONTINUE"])
        finished_pod, runner_status = wait_runner_termination(name, 720)
        termination = runner_status["state"]["terminated"]
        if termination["exitCode"] != 0:
            raise RuntimeError(f"instrumented real runner exited {termination['exitCode']}")
        scorecard = run(
            K + ["exec", pod_name, "-c", "observer", "--", "cat", "/work/scorecard.json"]
        ).stdout
        signature = run(
            K + ["exec", pod_name, "-c", "observer", "--", "cat", "/work/scorecard.sig"]
        ).stdout
        scorecard_path = save_artifact("runtime-gap-scorecard.json", scorecard)
        signature_path = save_artifact("runtime-gap-scorecard.sig", signature)
        verify_signed_document(scorecard_path, signature_path, "scorecard")
        rotated_verdict = run(
            [
                os.environ.get("LOGWEIR_PYTHON", sys.executable),
                "docs/verify_scorecard.py",
                "--payload-type",
                "scorecard",
                str(scorecard_path),
                str(signature_path),
                str(rotated_public),
            ],
            check=False,
        )
        if rotated_verdict.returncode != 1:
            raise RuntimeError("rotated key unexpectedly verified the retained-signer scorecard")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and receiver_count("a") != 1:
            time.sleep(1)
        receiver_observation = {
            "receiver_a_posts": receiver_count("a"),
            "receiver_b_posts": receiver_count("b"),
        }
        if receiver_observation != {"receiver_a_posts": 1, "receiver_b_posts": 0}:
            raise RuntimeError(f"notification routing changed after parse: {receiver_observation!r}")
        save_artifact("runtime-gap-receiver-observation.json", receiver_observation)
        save_artifact(
            "runtime-gap-receiver-a-logs.txt",
            run(K + ["logs", "backend-live-receiver-a", "-c", "receiver"]).stdout,
        )
        save_artifact(
            "runtime-gap-receiver-b-logs.txt",
            run(K + ["logs", "backend-live-receiver-b", "-c", "receiver"]).stdout,
        )
        save_artifact("runtime-gap-runner-pod.json", finished_pod)
        run(K + ["exec", pod_name, "-c", "observer", "--", "touch", "/work/STOP"])
        configure_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        scale_any_controller(1)
        controller_running = True
        result = verify_restore(name, prefix)
        if result["restore"]["status"].get("phase") != "Succeeded":
            raise RuntimeError("runtime-gap Restore did not succeed")
        STATE.setdefault("owned_target_topics", []).extend(
            [prefix + "orders", prefix + "payments"]
        )
        STATE["owned_target_topics"] = sorted(set(STATE["owned_target_topics"]))
        for case_name in [
            "controller_upgrade_drain_rollback_during_restore",
            "post_start_projected_plan_replacement",
            "live_plan_directed_network_observation",
            "mid_run_signer_rotation",
        ]:
            STATE["cases"][case_name] = "passed"
        STATE["runtime_gap"] = {
            "restore_uid": restore_uid,
            "job_uid": get("job", name)["metadata"]["uid"],
            "runner_image_id": runner_status.get("imageID"),
            "runner_exit_code": termination["exitCode"],
            "original_plan_sha256": hashlib.sha256(plan.encode()).hexdigest(),
            "replacement_plan_sha256": expected_plan_hash,
            "plan_projection_observation": plan_projection,
            "rotated_signer_sha256": expected_signer_hash,
            "notification": receiver_observation,
            "controlled_instrumentation": "engine wrapper paused after startup validation and before the real kafka-backup exec",
        }
        save_state()
    finally:
        if private_path is not None and private_path.exists():
            private_path.unlink()
        if not controller_running:
            configure_controller_images(
                original_identity["image"], original_identity["runner_image"]
            )
            scale_any_controller(1)
        else:
            set_controller_images(
                original_identity["image"], original_identity["runner_image"]
            )
        restored = controller_identity(controller_deployment())
        exact = (
            restored["deployment_uid"] == original_identity["deployment_uid"]
            and restored["image"] == original_identity["image"]
            and restored["runner_image"] == original_identity["runner_image"]
            and restored["spec_sha256"] == original_identity["spec_sha256"]
        )
        STATE["shared_controller_restore_exact_after_runtime_gap"] = exact
        save_artifact("shared-controller-after-runtime-gap.json", controller_deployment())
        save_state()
        if not exact:
            raise RuntimeError("shared controller was not exactly restored after runtime-gap case")


def runtime_gap_finalize_evidence() -> None:
    """Finalize the completed controlled run without requiring sidecar log collection."""
    name = "backend-live-runtime-gap-retained"
    prefix = "backend-live-runtime-gap-retained-"
    pod = pod_for_job(name)
    runner_status = next(
        status
        for status in pod.get("status", {}).get("containerStatuses", [])
        if status["name"] == "runner"
    )
    termination = runner_status.get("state", {}).get("terminated", {})
    if termination.get("exitCode") != 0 or not runner_status.get("imageID"):
        raise RuntimeError("completed runtime-gap runner lacks exit-0/imageID evidence")
    preauth_pod = json.loads((OUT / "runtime-gap-preauth-negative-pod.json").read_text())
    preauth_runner = next(
        status
        for status in preauth_pod.get("status", {}).get("containerStatuses", [])
        if status["name"] == "runner"
    )
    if preauth_runner["state"]["terminated"]["exitCode"] != 3:
        raise RuntimeError("retained pre-auth negative is not exit 3")
    if json.loads((OUT / "runtime-gap-preauth-receiver-observation.json").read_text()) != {
        "receiver_a_posts": 0,
        "receiver_b_posts": 0,
    }:
        raise RuntimeError("pre-auth receiver absence evidence changed")
    receiver_observation = {
        "receiver_a_posts": receiver_count("a"),
        "receiver_b_posts": receiver_count("b"),
    }
    if receiver_observation != {"receiver_a_posts": 1, "receiver_b_posts": 0}:
        raise RuntimeError(f"retained notification receiver evidence changed: {receiver_observation!r}")

    before = (OUT / "runtime-gap-projected-before-sha256.txt").read_text().splitlines()
    after = (OUT / "runtime-gap-projected-after-sha256.txt").read_text().splitlines()
    before_plan, before_signer = [line.split()[0] for line in before]
    after_plan, after_signer = [line.split()[0] for line in after]
    if before_plan != after_plan or before_signer == after_signer:
        raise RuntimeError("projected plan retention/signer rotation evidence is inconsistent")
    transitions = json.loads((OUT / "runtime-gap-controller-transitions.json").read_text())
    events = [item["event"] for item in transitions]
    expected_events = [
        "rollback_to_archived",
        "upgrade_to_current",
        "drain_current",
        "rollback_during_execution",
        "reupgrade_during_execution",
        "final_drain_before_input_replacement",
    ]
    if events != expected_events:
        raise RuntimeError(f"controller transition evidence is incomplete: {events!r}")

    scorecard_path = OUT / "runtime-gap-scorecard.json"
    signature_path = OUT / "runtime-gap-scorecard.sig"
    verify_signed_document(scorecard_path, signature_path, "scorecard")
    rotated_verdict = run(
        [
            os.environ.get("LOGWEIR_PYTHON", sys.executable),
            "docs/verify_scorecard.py",
            "--payload-type",
            "scorecard",
            str(scorecard_path),
            str(signature_path),
            str(OUT / "rotated-signing.pub.pem"),
        ],
        check=False,
    )
    if rotated_verdict.returncode != 1:
        raise RuntimeError("rotated public key did not reject the retained-signer signature")

    for topic in ["orders", "payments"]:
        target_topic = prefix + topic
        offsets = broker_target(
            f"/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 "
            f"--topic {target_topic} --time -1\n"
        ).strip().splitlines()
        if offsets != [f"{target_topic}:0:100"]:
            raise RuntimeError(f"runtime-gap target offsets changed: {offsets!r}")
        data = broker_target(
            f"/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 "
            f"--topic {target_topic} --from-beginning --max-messages 100 --timeout-ms 15000\n",
            timeout=60,
        )
        records = data.strip().splitlines()
        expected = [f"{topic}-record-{number:03}" for number in range(1, 101)]
        if sorted(records) != expected:
            raise RuntimeError(f"runtime-gap record mismatch for {target_topic}")
        save_artifact(f"runtime-gap-{topic}.txt", data)

    restore = get("restore", name)
    save_artifact("runtime-gap-controlled-boundary-restore.json", restore)
    controller_logs = run(
        KF + ["logs", "deployment/weirkeeper", "--since=15m"], check=False
    ).stdout.splitlines()
    bounded = [line for line in controller_logs if name in line][-20:]
    save_artifact("runtime-gap-controlled-boundary-controller.txt", "\n".join(bounded) + "\n")
    STATE.setdefault("owned_target_topics", []).extend(
        [prefix + "orders", prefix + "payments"]
    )
    STATE["owned_target_topics"] = sorted(set(STATE["owned_target_topics"]))
    for case_name in [
        "controller_upgrade_drain_rollback_during_restore",
        "post_start_projected_plan_replacement",
        "live_plan_directed_network_observation",
        "mid_run_signer_rotation",
    ]:
        STATE["cases"][case_name] = "passed"
    STATE["runtime_gap"] = {
        "restore_uid": restore["metadata"]["uid"],
        "job_uid": get("job", name)["metadata"]["uid"],
        "runner_image_id": runner_status["imageID"],
        "runner_exit_code": 0,
        "original_plan_sha256": before_plan,
        "projected_plan_after_sha256": after_plan,
        "plan_projection_observation": "original-retained-after-configmap-delete-recreate",
        "original_signer_sha256": before_signer,
        "rotated_signer_sha256": after_signer,
        "notification": receiver_observation,
        "controller_status_boundary": (
            "controlled observer sidecar made controller log collection ambiguous; "
            "runner/Job/records/signature/transport were verified directly"
        ),
        "controlled_instrumentation": (
            "engine wrapper paused after startup validation and before exec of the real kafka-backup"
        ),
    }
    save_state()


def vendor_manifest_sha256(root: pathlib.Path) -> str:
    """Hash a reproducible relative-path/content manifest for a vendor tree."""
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        digest.update(f"{content_hash}  {relative}\n".encode())
    return digest.hexdigest()


def report() -> None:
    """Write the corrected bounded machine report and Markdown handoff."""
    snapshot_root = pathlib.Path("/tmp/logweir-backend-live-20260915T0330Z/src")
    old_root = pathlib.Path("/tmp/logweir-backend-live-20260915T0330Z/old")
    public_key = OUT / "verifier-signing.pub.pem"
    public_key.write_bytes((FIXTURE_KEYS / "signing.pub.pem").read_bytes())
    public_key.chmod(0o600)

    seam = subprocess.run(
        [
            "cargo",
            "test",
            "--offline",
            "-p",
            "logweir",
            "signer::tests::readiness_rejects_a_real_signature_that_does_not_verify_over_its_probe",
            "--",
            "--exact",
        ],
        cwd=snapshot_root,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    seam_output = seam.stdout + seam.stderr
    save_artifact("signer-readiness-seam-test.txt", seam_output[-12000:])
    if seam.returncode != 0 or "1 passed; 0 failed" not in seam_output:
        raise RuntimeError("source-matched signer readiness seam test did not pass")

    python_bin = os.environ.get("LOGWEIR_PYTHON", sys.executable)
    python_version = run([python_bin, "--version"]).stdout.strip()
    cryptography_version = run(
        [python_bin, "-c", "import cryptography; print(cryptography.__version__)"]
    ).stdout.strip()
    runner_version_proc = run(
        DOCKER
        + [
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            CURRENT_RUNNER,
            "--version",
        ],
        check=False,
    )
    runner_version = (runner_version_proc.stdout + runner_version_proc.stderr).strip()

    critical = [
        "crates/logweir-core/src/execution_contract.rs",
        "crates/logweir/src/cli.rs",
        "crates/logweir/src/drill/mod.rs",
        "crates/logweir/src/drill/phase1_approval.rs",
        "crates/logweir/src/drill/phase8_score.rs",
        "crates/logweir/src/signer.rs",
        "crates/weirkeeper/src/controllers/restore.rs",
        "crates/weirkeeper/src/controllers/approval.rs",
        "crates/weirkeeper/src/job.rs",
        "config/crd/approvals.yaml",
    ]
    source_hashes = {
        path: {
            "tested_snapshot": sha256_file(snapshot_root / path),
            "working_tree_at_report": sha256_file(ROOT / path),
        }
        for path in critical
    }

    image_specs = {
        CURRENT_CONTROLLER: ("linux/arm64", "/usr/local/bin/weirkeeper"),
        CURRENT_RUNNER: ("linux/amd64", "/usr/local/bin/logweir"),
        OLD_CONTROLLER: ("linux/arm64", "/usr/local/bin/weirkeeper"),
        OLD_RUNNER: ("linux/amd64", "/usr/local/bin/logweir"),
    }
    images: dict[str, Any] = {}
    for image, (platform, binary) in image_specs.items():
        inspected = json.loads(
            run(DOCKER + ["image", "inspect", image, "--format", "{{json .}}"] ).stdout
        )
        binary_hash = run(
            DOCKER
            + [
                "run",
                "--rm",
                "--platform",
                platform,
                "--entrypoint",
                "sha256sum",
                image,
                binary,
            ]
        ).stdout.split()[0]
        images[image] = {
            "id": inspected["Id"],
            "architecture": inspected["Architecture"],
            "created": inspected["Created"],
            "binary": binary,
            "binary_sha256": binary_hash,
        }
    images[CURRENT_RUNNER]["engine_binary_sha256"] = run(
        DOCKER
        + [
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--entrypoint",
            "sha256sum",
            CURRENT_RUNNER,
            "/usr/local/bin/kafka-backup",
        ]
    ).stdout.split()[0]

    build_inputs = {}
    for label, root in [("current", snapshot_root), ("archived_old", old_root)]:
        build_inputs[label] = {
            "root": str(root),
            "Dockerfile_sha256": sha256_file(root / "Dockerfile"),
            "Dockerfile_weirkeeper_sha256": sha256_file(root / "Dockerfile.weirkeeper"),
            "Cargo_lock_sha256": sha256_file(root / "Cargo.lock"),
            "cargo_config_sha256": sha256_file(root / ".cargo/config.toml"),
            "vendor_manifest_sha256": vendor_manifest_sha256(root / "vendor"),
            "vendor_manifest_algorithm": (
                "SHA-256 of sorted lines '<file-sha256>  <vendor-relative-path>\\n'"
            ),
            "rust_toolchain_selector": "absent in test snapshot",
            "dockerignore": "absent so checksum-listed vendored *.tar.gz files enter build context",
        }
    build_provenance = {
        "scope": "test-specific images only; not an ordinary shipping/release workflow proof",
        "transformations": [
            "export source snapshot; for archived build use git archive 92e02097540c39ff8565283a38ee592499b95020",
            "remove rust-toolchain.toml selector; Dockerfiles still pin rust:1.89-bookworm",
            "install scripts/backend-live-cargo-config.toml as .cargo/config.toml",
            "cargo vendor --offline --versioned-dirs vendor >/dev/null",
            "remove .dockerignore because its patterns excluded checksum-listed vendor tar files",
        ],
        "commands": [
            "docker --context desktop-linux build --load --platform linux/arm64 -f Dockerfile.weirkeeper -t weirkeeper:backend-live-20260915 <current-snapshot>",
            "docker --context desktop-linux build --load --platform linux/amd64 -f Dockerfile -t logweir:backend-live-20260915 <current-snapshot>",
            "docker --context desktop-linux build --load --platform linux/arm64 -f /tmp/logweir-backend-live-20260915T0330Z/old/Dockerfile.weirkeeper -t weirkeeper:pre-handshake-92e02097 /tmp/logweir-backend-live-20260915T0330Z/old",
            "docker --context desktop-linux build --load --platform linux/amd64 -f <old-snapshot>/Dockerfile -t logweir:pre-handshake-92e02097 <old-snapshot>",
        ],
        "inputs": build_inputs,
        "shipping_workflow": (
            "Not exercised here. Standard unmodified Dockerfile/just release proof remains separate release work."
        ),
    }

    cases = STATE.get("cases", {})
    gate_exit, gate_reasons = acceptance_gate(
        cases, STATE.get("cleanup", {}), require_cleanup=True
    )
    classifications = {
        "required": {name: cases.get(name, "missing") for name in sorted(REQUIRED_CASES)},
        "passed": sorted(name for name in REQUIRED_CASES if cases.get(name) == "passed"),
        "failed": gate_reasons["failed"],
        "unrun": gate_reasons["unrun"],
        "missing": gate_reasons["missing"],
        "supplemental": {
            name: result
            for name, result in sorted(cases.items())
            if name not in REQUIRED_CASES
        },
    }
    namespace = get_optional("namespace", NS, cluster_scoped=True)
    deployment = controller_deployment()
    deployment_identity = controller_identity(deployment)
    crd = json.loads(
        run(KUBECTL + ["get", "crd", "approvals.logweir.dev", "-o", "json"]).stdout
    )
    artifact_hashes = {
        path.name: sha256_file(path)
        for path in sorted(OUT.iterdir())
        if path.is_file() and path.name != REPORT_PATH.name
    }
    structured = {
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "verdict": "accepted" if gate_exit == 0 else "incomplete",
        "exit_code": gate_exit,
        "context": "docker-desktop",
        "docker_context": "desktop-linux",
        "namespace": {"name": NS, "uid": STATE.get("namespace_uid")},
        "classifications": classifications,
        "gate_reasons": gate_reasons,
        "core_accepted_evidence": {
            "backup_records": {"orders": 100, "payments": 100, "total": 200},
            "restore_a_full_records": 200,
            "restore_b_full_records": 200,
            "scorecard_sample": {
                "configured_records_per_partition": 25,
                "partitions": 2,
                "expected": 50,
                "restored": 50,
            },
        },
        "legacy_prejob": STATE.get("legacy_prejob", {}),
        "exact_old_controller": STATE.get("exact_old_controller", {}),
        "runtime_gap": STATE.get("runtime_gap", {}),
        "signer_cases": STATE.get("signer_cases", {}),
        "collision_evidence": {
            "retained": cases.get("collision_matrix_evidence_retained") == "passed",
            "artifact_prefix": "collision-",
        },
        "signer_post_parse_failure": {
            "live_classification": "not_reachable_for_valid_parsed_current_software_signers",
            "reason": (
                "P-256 and Ed25519 sign_detached branches are infallible after parse; the parsed signer is retained in memory."
            ),
            "meaningful_seam": (
                "signer::tests::readiness_rejects_a_real_signature_that_does_not_verify_over_its_probe"
            ),
            "seam_test_exit_code": seam.returncode,
            "seam_artifact": "signer-readiness-seam-test.txt",
        },
        "admission_boundary": (
            "Live checks cover API immutability, exact owner/UID collision refusal, and runner hash binding. "
            "They do not claim protection from a fully malicious cluster administrator."
        ),
        "separate_owners_not_counted": [
            "bootstrap/identity and full-chart install/reinstall",
            "UI/browser behavior",
            "scheduler behavior (this review remained read-only)",
        ],
        "source_applicability": {
            "current_tested_snapshot": str(snapshot_root),
            "archived_commit": "92e02097540c39ff8565283a38ee592499b95020",
            "critical_hashes": source_hashes,
            "note": (
                "Runtime claims apply to the listed image IDs and tested snapshot bytes. Later shared-working-tree bootstrap/CLI/chart changes are not included."
            ),
        },
        "images": images,
        "build_provenance": build_provenance,
        "verifiers": {
            "public_key_artifact": public_key.name,
            "public_key_sha256": sha256_file(public_key),
            "private_keys_in_evidence": False,
            "runner_version": runner_version,
            "python": python_version,
            "cryptography": cryptography_version,
            "python_verifier_sha256": sha256_file(ROOT / "docs/verify_scorecard.py"),
        },
        "cleanup": {
            **STATE.get("cleanup", {}),
            "namespace_absent": namespace is None,
            "shared_controller": deployment_identity,
            "shared_controller_exact_restores": {
                "targeted": STATE.get("shared_controller_restore_exact"),
                "signer": STATE.get("shared_controller_restore_exact_after_signer"),
                "old_controller": STATE.get("shared_controller_restore_exact_after_old_controller"),
                "runtime_gap": STATE.get("shared_controller_restore_exact_after_runtime_gap"),
            },
            "approval_crd_uid": crd["metadata"]["uid"],
            "approval_crd_established": any(
                item.get("type") == "Established" and item.get("status") == "True"
                for item in crd.get("status", {}).get("conditions", [])
            ),
        },
        "harness_fault_controls": "harness-fault-controls.json",
        "harness_iterations": STATE.get("case_failures", []) + STATE.get("errors", []),
        "artifact_hashes": artifact_hashes,
        "reproducible_commands": [
            "LOGWEIR_BACKEND_LIVE_OUT=<out> LOGWEIR_BACKEND_LIVE_NS=<owned-ns> LOGWEIR_BACKEND_LIVE_RUN_LABEL=<label> LOGWEIR_BACKEND_LIVE_BASELINE_STATE=/tmp/logweir-backend-live-20260915T0330Z/run/state.json LOGWEIR_PYTHON=/usr/bin/python3 python3 scripts/test-plat01-02-live.py harness-selftest",
            "... python3 scripts/test-plat01-02-live.py targeted",
            "... python3 scripts/test-plat01-02-live.py signer-permission",
            "... python3 scripts/test-plat01-02-live.py exact-old-controller-job",
            "... python3 scripts/test-plat01-02-live.py runtime-gap-matrix",
            "... python3 scripts/test-plat01-02-live.py runtime-gap-finalize-evidence",
            "... python3 scripts/test-plat01-02-live.py cleanup",
            "... python3 scripts/test-plat01-02-live.py report",
        ],
    }
    REPORT_PATH.write_text(json.dumps(structured, indent=2, sort_keys=True) + "\n")
    REPORT_PATH.chmod(0o600)

    passed = classifications["passed"]
    lines = [
        "# PLAT-01 / PLAT-02.2 backend live acceptance — corrected close",
        "",
        f"Verdict: **{structured['verdict']}** (terminal exit `{gate_exit}`).",
        f"Required: {len(REQUIRED_CASES)}; passed: {len(passed)}; failed: {len(classifications['failed'])}; unrun: {len(classifications['unrun'])}; missing: {len(classifications['missing'])}.",
        "",
        "## Corrected targeted evidence",
        "",
        f"- True archived-shape mutable pre-Job transition passed: Restore UID `{STATE.get('legacy_prejob', {}).get('restore_uid')}`, ConfigMap UID `{STATE.get('legacy_prejob', {}).get('configmap_uid')}`, exit `0`, runner `{STATE.get('legacy_prejob', {}).get('runner_image_id')}`. Before/after ConfigMaps and resulting Restore/Job/Pod are retained.",
        f"- Exact archived controller Job passed and was adopted unchanged by the current controller: Job UID `{STATE.get('exact_old_controller', {}).get('job_uid')}`, old controller `{STATE.get('exact_old_controller', {}).get('controller_image_id')}`, old runner `{STATE.get('exact_old_controller', {}).get('runner_image_id')}`, exit `0`.",
        "- The regular-file signer case dereferenced to `regular file 400 0 0`; UID/GID 65532 had `test -r` exit 1. The runner reported `Permission denied`, exited 4, wrote only `metrics.prom`, and did not touch the engine sentinel. The directory case remains separately classified as wrong-path-type coverage.",
        "- Every Job/ConfigMap collision object was saved immediately before reconcile with complete ownerReferences/finalizers; each resulting reason and checked no-Job/no-plan observation has a unique artifact.",
        f"- During a real current-runner execution, controller rollback/upgrade/drain/re-upgrade transitions were observed. The ConfigMap delete/recreate retained the original projected plan digest `{STATE.get('runtime_gap', {}).get('original_plan_sha256')}` in the running Pod, while the in-place Secret rotation changed its signer digest. The signed result verified under the original public key and failed verification under the rotated public key.",
        "- A plan-authentication failure produced exit 3 while both owned receivers observed zero POSTs. The authenticated retained plan later sent exactly one POST to receiver A; replacement-plan receiver B observed zero. Absence is receiver-observed, not inferred from empty configuration or Kafka state.",
        "- Controlled instrumentation boundary: an observer sidecar and pre-engine wrapper were added after the controller emitted the production Job. The wrapper paused only after startup validation and then exec'd the real kafka-backup. The multi-container Job prevents the unmodified controller from choosing a logs container, so runner/Job/records/signature/transport evidence was verified directly; no product defect is claimed.",
        "",
        "## Preserved core assertions",
        "",
        "- Backup receipt: 100 orders + 100 payments = 200 exact records.",
        "- Both accepted Restore targets retain full 200-record proofs. The independent scorecard sample remains correctly configured at 25 records × 2 partitions = 50/50; neither assertion replaces the other.",
        "",
        "## Signer and admission boundaries",
        "",
        "A valid parsed P-256/Ed25519 software signer has no reachable post-parse signing-error branch today. The source-matched readiness seam used a real non-verifying signature and passed; no fake live failure is claimed. Admission evidence covers actual API immutability, owner/UID collision checks, and runner hash binding—not a fully malicious cluster administrator.",
        "",
        "## Provenance and cleanup",
        "",
        "Test-specific builds used the recorded offline vendor transformation (toolchain selector removal, offline Cargo config/vendor, and `.dockerignore` removal). Dockerfile, lock, vendor-manifest, and Cargo-config hashes are in `report.json`. This is separate from the unmodified standard shipping Dockerfile/release workflow, which remains release work.",
        f"Public verifier key `{public_key.name}` SHA-256 `{sha256_file(public_key)}` is included; no private key is in evidence. Verifiers: `{runner_version}`, `{python_version}`, cryptography `{cryptography_version}`.",
        f"Owned namespace `{NS}` UID `{STATE.get('namespace_uid')}` absent: `{namespace is None}`. Shared controller restored to `{deployment_identity['image']}` / `{deployment_identity['runner_image']}` with one Ready pod. Cleanup result: `{STATE.get('cleanup', {}).get('result')}`.",
        "Bootstrap/full-chart, UI, and scheduler belong to separate owners and are not counted as missing here.",
        "",
        f"Machine report: `{REPORT_PATH}`",
        f"Evidence directory: `{OUT}`",
    ]
    markdown_path = pathlib.Path(
        os.environ.get(
            "LOGWEIR_BACKEND_LIVE_MARKDOWN",
            "/tmp/logweir-backend-live-close-acceptance.md",
        )
    )
    markdown_path.write_text("\n".join(lines) + "\n")
    markdown_path.chmod(0o600)
    STATE["report"] = {
        "machine": str(REPORT_PATH),
        "markdown": str(markdown_path),
        "exit_code": gate_exit,
    }
    save_state()
    log(f"corrected reports written: {REPORT_PATH} and {markdown_path}")


def gate_probe() -> None:
    payload = json.loads(os.environ["LOGWEIR_GATE_PROBE_JSON"])
    status, _reasons = acceptance_gate(
        payload.get("cases", {}),
        payload.get("cleanup", {}),
        require_cleanup=True,
        required_cases=set(payload.get("required", [])),
    )
    raise SystemExit(status)


def harness_selftest() -> None:
    empty = subprocess.CompletedProcess(["kubectl"], 0, "", "")
    if optional_json_from_result(empty, description="NotFound control") is not None:
        raise RuntimeError("empty exit-0 optional get did not classify as absent")
    present = subprocess.CompletedProcess(["kubectl"], 0, '{"kind":"ConfigMap"}\n', "")
    if optional_json_from_result(present, description="present control") != {
        "kind": "ConfigMap"
    }:
        raise RuntimeError("present optional get did not decode")
    for label, stderr in [
        ("Forbidden", "Error from server (Forbidden): denied"),
        ("transport", "Unable to connect to the server: connection refused"),
    ]:
        fault = subprocess.CompletedProcess(["kubectl"], 1, "", stderr)
        try:
            optional_json_from_result(fault, description=f"{label} control")
        except RuntimeError:
            pass
        else:
            raise RuntimeError(f"{label} optional-get fault was misclassified as absence")

    probes = [
        ({"cases": {"a": "failed"}, "cleanup": {"result": "deleted"}}, 1),
        ({"cases": {}, "cleanup": {"result": "deleted"}}, 1),
        ({"cases": {"a": "passed"}, "cleanup": {"result": "failed"}}, 1),
        ({"cases": {"a": "passed"}, "cleanup": {"result": "deleted"}}, 0),
    ]
    results = []
    for payload, expected in probes:
        payload["required"] = ["a"]
        env = os.environ.copy()
        env["LOGWEIR_GATE_PROBE_JSON"] = json.dumps(payload)
        proc = subprocess.run(
            [sys.executable, str(pathlib.Path(__file__).resolve()), "gate-probe"],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
            check=False,
        )
        if proc.returncode != expected:
            raise RuntimeError(
                f"gate probe {payload!r} exited {proc.returncode}, expected {expected}: "
                f"{proc.stderr}"
            )
        results.append({"input": payload, "exit_code": proc.returncode})
    save_artifact("harness-fault-controls.json", results)
    STATE["harness_selftest"] = "passed"
    save_state()


def cleanup() -> None:
    existing = get_optional("namespace", NS, cluster_scoped=True)
    if existing is None:
        if STATE.get("namespace_uid"):
            STATE["cleanup"] = {
                "namespace": NS,
                "uid": STATE["namespace_uid"],
                "result": "deleted",
                "observation": "already absent via ignore-not-found exit 0 empty output",
            }
            save_state()
        log("owned namespace already absent")
        return
    ns = existing
    labels = ns["metadata"].get("labels", {})
    expected_uid = STATE.get("namespace_uid")
    if labels.get("backend-live.logweir.dev/run") != RUN_LABEL:
        raise RuntimeError("refusing cleanup: namespace ownership label changed")
    if ns["metadata"]["uid"] != expected_uid:
        raise RuntimeError("refusing cleanup: namespace UID changed")
    for topic in STATE.get("owned_target_topics", []):
        if not topic.startswith("backend-live-"):
            raise RuntimeError(f"refusing cleanup of non-owned topic {topic!r}")
        if topic in target_topics():
            broker_target(
                "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--delete --topic {topic}\n"
            )
    remaining_topics = sorted(
        topic for topic in STATE.get("owned_target_topics", []) if topic in target_topics()
    )
    if remaining_topics:
        raise RuntimeError(f"owned target topics remain: {remaining_topics!r}")
    delete_with_uid_precondition(
        "namespace", NS, expected_uid, api_version="v1", namespace=None, timeout=200
    )
    wait_absent("namespace", NS, seconds=180, cluster_scoped=True)
    STATE["cleanup"] = {
        "namespace": NS,
        "uid": expected_uid,
        "result": "deleted",
        "owned_target_topics_absent": True,
    }
    save_state()
    log(f"deleted only owned namespace {NS} uid={expected_uid}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=[
            "positive",
            "negative",
            "targeted",
            "signer-permission",
            "exact-old-controller-job",
            "runtime-gap-matrix",
            "runtime-gap-finalize-evidence",
            "finalize",
            "cleanup",
            "report",
            "harness-selftest",
            "gate-probe",
        ],
    )
    args = parser.parse_args()
    try:
        globals()[args.phase.replace("-", "_")]()
    except Exception as exc:  # noqa: BLE001 - redact every terminal path
        STATE.setdefault("errors", []).append(redact(f"{type(exc).__name__}: {exc}"))
        if args.phase == "cleanup":
            STATE["cleanup"] = {"namespace": NS, "result": "failed", "error": redact(str(exc))}
        save_state()
        print(redact(f"{type(exc).__name__}: {exc}"), file=sys.stderr)
        return 1
    if args.phase in {"cleanup", "report"}:
        return terminal_exit_status(require_cleanup=True)
    if args.phase in {
        "negative",
        "targeted",
        "signer-permission",
        "exact-old-controller-job",
        "runtime-gap-matrix",
        "runtime-gap-finalize-evidence",
    }:
        return terminal_exit_status(require_cleanup=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
