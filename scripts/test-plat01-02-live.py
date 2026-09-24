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
# Every image and source root is a parameter so evidence is regenerated against
# the exact images built for the commit under test, never a stale default tag.
CURRENT_RUNNER = os.environ.get(
    "LOGWEIR_BACKEND_LIVE_CURRENT_RUNNER", "logweir:backend-live-20260915"
)
OLD_RUNNER = os.environ.get(
    "LOGWEIR_BACKEND_LIVE_OLD_RUNNER", "logweir:pre-handshake-92e02097"
)
CURRENT_CONTROLLER = os.environ.get(
    "LOGWEIR_BACKEND_LIVE_CURRENT_CONTROLLER", "weirkeeper:backend-live-20260915"
)
OLD_CONTROLLER = os.environ.get(
    "LOGWEIR_BACKEND_LIVE_OLD_CONTROLLER", "weirkeeper:pre-handshake-92e02097"
)
SOURCE_ROOT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_BACKEND_LIVE_SOURCE_ROOT", "/tmp/logweir-backend-live-20260915T0330Z/src"
    )
)
OLD_SOURCE_ROOT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_BACKEND_LIVE_OLD_SOURCE_ROOT", "/tmp/logweir-backend-live-20260915T0330Z/old"
    )
)
SOURCE_COMMIT = os.environ.get("LOGWEIR_BACKEND_LIVE_SOURCE_COMMIT", "")
OLD_COMMIT = os.environ.get(
    "LOGWEIR_BACKEND_LIVE_OLD_COMMIT", "92e02097540c39ff8565283a38ee592499b95020"
)
# The worker-rules ownership label, carried in addition to the run label.
TEST_OWNER = os.environ.get("LOGWEIR_BACKEND_LIVE_TEST_OWNER", "")
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
    "runner_refuses_missing_bundle_member",
    "runner_refuses_tampered_bundle_member",
    "restores_mount_only_their_own_bundles",
}
# Cases that cannot be exercised live and are therefore classified with a
# justification instead of being counted as passed or silently omitted.
UNSUPPORTED_LIVE_CASES = {
    "post_parse_software_signing_failure": (
        "A parsed P-256/Ed25519 software signer has no reachable post-parse signing error; "
        "the readiness seam is proven by the source-matched unit test named in the report."
    ),
    "mutating_admission_webhook_rewrite": (
        "No mutating webhook is installed in the shared lab; API immutability, exact owner/UID "
        "collision refusal and runner digest binding are exercised instead. This does not claim "
        "defence against a malicious cluster administrator."
    ),
    "networkpolicy_enforcement": (
        "Docker Desktop does not enforce NetworkPolicy; notification absence is proven by owned "
        "receivers, never by policy."
    ),
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
    lab_state: dict[str, Any] | None = None,
) -> tuple[int, dict[str, list[str]]]:
    failed = sorted(name for name in required_cases if cases.get(name) == "failed")
    unrun = sorted(name for name in required_cases if cases.get(name) == "unrun")
    missing = sorted(name for name in required_cases if name not in cases)
    unknown = sorted(
        name
        for name in required_cases
        if name in cases and cases[name] not in {"passed", "failed", "unrun"}
    )
    cleanup_failures: list[str] = []
    if require_cleanup and cleanup_state.get("result") != "deleted":
        cleanup_failures.append(
            f"required cleanup result is {cleanup_state.get('result', 'missing')!r}"
        )
    if lab_state and lab_state.get("recorded") and lab_state.get("restore_exact") is not True:
        cleanup_failures.append(
            f"shared lab controller restore_exact is {lab_state.get('restore_exact')!r}"
        )
    failures = {
        "failed": failed + unknown,
        "unrun": unrun,
        "missing": missing,
        "cleanup": cleanup_failures,
    }
    return (1 if any(failures.values()) else 0), failures


def lab_gate_state() -> dict[str, Any]:
    return {
        "recorded": STATE.get("lab_controller_original") is not None,
        "restore_exact": STATE.get("lab_controller_restore_exact"),
    }


def terminal_exit_status(*, require_cleanup: bool) -> int:
    status, reasons = acceptance_gate(
        STATE.get("cases", {}),
        STATE.get("cleanup", {}),
        require_cleanup=require_cleanup,
        lab_state=lab_gate_state() if require_cleanup else None,
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
    propagation: str = "Foreground",
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
        "propagationPolicy": propagation,
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


def namespace_labels() -> dict[str, str]:
    labels = {"backend-live.logweir.dev/run": RUN_LABEL}
    if TEST_OWNER:
        labels["logweir.dev/test-owner"] = TEST_OWNER
    return labels


def setup_namespace() -> None:
    ns = get_optional("namespace", NS, cluster_scoped=True)
    if ns is not None:
        labels = ns["metadata"].get("labels", {})
        if any(labels.get(key) != value for key, value in namespace_labels().items()):
            raise RuntimeError(f"refusing pre-existing unowned namespace {NS}")
        if STATE.get("namespace_uid") != ns["metadata"]["uid"]:
            raise RuntimeError("owned namespace UID does not match saved run state")
    else:
        ns = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": NS,
                "labels": namespace_labels(),
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


#: The controller's exec probes (chart gap G5, `weirkeeper --probe live|ready`).
#: A controller image that predates G5 has no `--probe`, so under the lab's
#: probes its pod is never Ready and is restarted by liveness: "chart and
#: images move together" (release notes). The OLD controller therefore runs
#: WITHOUT them, and every switch back to another image puts back exactly the
#: probes the lab Deployment had when this run recorded it.
PROBE_FIELDS = ("livenessProbe", "readinessProbe")


def probe_patch(
    container: dict[str, Any], controller_image: str, original_probes: dict[str, Any]
) -> list[dict[str, Any]]:
    """JSON-patch operations that make container 0's probes right for the image."""
    base = "/spec/template/spec/containers/0/"
    if controller_image == OLD_CONTROLLER:
        return [{"op": "remove", "path": base + field}
                for field in PROBE_FIELDS if field in container]
    return [{"op": "add", "path": base + field, "value": value}
            for field, value in original_probes.items() if container.get(field) != value]


def original_probes() -> dict[str, Any]:
    original = STATE.get(LAB_ORIGINAL) or {}
    containers = original.get("containers") or [{}]
    return {field: containers[0][field] for field in PROBE_FIELDS if field in containers[0]}


def configure_controller_images(controller_image: str, runner_image: str) -> None:
    patch = probe_patch(
        controller_deployment()["spec"]["template"]["spec"]["containers"][0],
        controller_image,
        original_probes(),
    )
    if patch:
        run(KF + ["patch", "deployment", "weirkeeper", "--type=json", "-p", json.dumps(patch)])
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


LAB_ORIGINAL = "lab_controller_original"


def lab_switch() -> None:
    """Record the shared lab controller exactly once, then run the images under test."""
    deployment = controller_deployment()
    if STATE.get(LAB_ORIGINAL) is None:
        save_artifact("lab-controller-original-deployment.json", deployment)
        STATE[LAB_ORIGINAL] = {
            **controller_identity(deployment),
            "replicas": deployment["spec"].get("replicas"),
            "template_annotations": deployment["spec"]["template"]["metadata"].get(
                "annotations"
            ),
            "containers": deployment["spec"]["template"]["spec"]["containers"],
        }
        save_state()
    set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
    assert_single_controller()
    STATE["lab_controller_under_test"] = controller_identity(controller_deployment())
    save_state()
    log(f"lab controller switched to {CURRENT_CONTROLLER} / {CURRENT_RUNNER}")


def lab_restore() -> None:
    """Restore the recorded lab controller spec, replicas and pod-template annotations."""
    original = STATE.get(LAB_ORIGINAL)
    if original is None:
        raise RuntimeError("refusing lab restore: no recorded original controller spec")
    current = controller_deployment()
    if current["metadata"]["uid"] != original["deployment_uid"]:
        raise RuntimeError("refusing lab restore: controller Deployment UID changed")
    patch: list[dict[str, Any]] = [
        {"op": "replace", "path": "/spec/replicas", "value": original["replicas"]},
        {
            "op": "replace",
            "path": "/spec/template/spec/containers",
            "value": original["containers"],
        },
    ]
    if original["template_annotations"] is None:
        if current["spec"]["template"]["metadata"].get("annotations") is not None:
            patch.append({"op": "remove", "path": "/spec/template/metadata/annotations"})
    else:
        patch.append(
            {
                "op": "add",
                "path": "/spec/template/metadata/annotations",
                "value": original["template_annotations"],
            }
        )
    run(KF + ["patch", "deployment", "weirkeeper", "--type=json", "-p", json.dumps(patch)])
    run(
        KF + ["rollout", "status", "deployment/weirkeeper", "--timeout=240s"],
        timeout=260,
    )
    deadline = time.monotonic() + 120
    restored = controller_identity(controller_deployment())
    while time.monotonic() < deadline and len(restored["ready_pods"]) != original["replicas"]:
        time.sleep(2)
        restored = controller_identity(controller_deployment())
    exact = (
        restored["deployment_uid"] == original["deployment_uid"]
        and restored["image"] == original["image"]
        and restored["runner_image"] == original["runner_image"]
        and restored["spec_sha256"] == original["spec_sha256"]
        and len(restored["ready_pods"]) == original["replicas"]
    )
    STATE["lab_controller_restored"] = restored
    STATE["lab_controller_restore_exact"] = exact
    save_artifact("lab-controller-restored-deployment.json", controller_deployment())
    save_state()
    if not exact:
        raise RuntimeError(f"lab controller did not restore exactly: {restored!r}")
    log("lab controller restored to its recorded spec, replicas and pod-template annotations")


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
        delete_with_uid_precondition(
            "configmap", f"{name}-approval-bundle", original_uid, api_version="v1"
        )
        wait_absent("configmap", f"{name}-approval-bundle")
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
            delete_with_uid_precondition(
                "configmap", f"{name}-approval-bundle", bundle_uid, api_version="v1"
            )
            wait_absent("configmap", f"{name}-approval-bundle")
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
    delete_with_uid_precondition("restore", name, old_uid, api_version="logweir.dev/v1alpha1")
    wait_absent("restore", name)
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
    delete_with_uid_precondition("restore", name, old_uid, api_version="logweir.dev/v1alpha1")
    wait_absent("restore", name)
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
        delete_with_uid_precondition(
            "restore",
            missing["metadata"]["name"],
            missing["metadata"]["uid"],
            api_version="logweir.dev/v1alpha1",
        )
        wait_absent("restore", missing["metadata"]["name"])
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
    delete_with_uid_precondition(
        "restore", name, uid, api_version="logweir.dev/v1alpha1", propagation="Background"
    )
    wait_absent("restore", name)
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
    result = verify_restore(name, prefix, plan_state="legacy")
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
    delete_with_uid_precondition(
        "restore",
        restore["metadata"]["name"],
        expected_uid,
        api_version="logweir.dev/v1alpha1",
        propagation="Background",
    )
    wait_absent("restore", restore["metadata"]["name"])
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


def sha256_prefixed(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def assert_only_own_inputs(
    name: str,
    restore: dict[str, Any],
    approval: dict[str, Any],
    job: dict[str, Any],
    bundle: dict[str, Any],
    plan_cm: dict[str, Any],
    *,
    plan_state: str = "immutable",
    instrumentation_volumes: frozenset[tuple[str, str, str]] = frozenset(),
) -> None:
    """The Job mounts exactly its own plan/bundle and pins their exact digests."""
    pod_spec = job["spec"]["template"]["spec"]
    volumes = []
    for volume in pod_spec["volumes"]:
        if "configMap" in volume:
            volumes.append((volume["name"], "configMap", volume["configMap"]["name"]))
        elif "secret" in volume:
            volumes.append((volume["name"], "secret", volume["secret"]["secretName"]))
        elif "emptyDir" in volume:
            volumes.append((volume["name"], "emptyDir", ""))
        else:
            volumes.append((volume["name"], "other", json.dumps(volume, sort_keys=True)))
    expected_volumes = sorted(
        [
            ("approval", "configMap", f"{name}-approval-bundle"),
            ("plan", "configMap", f"{name}-plan"),
            ("signing", "secret", "logweir-signing-key"),
            ("work", "emptyDir", ""),
            *instrumentation_volumes,
        ]
    )
    if sorted(volumes) != expected_volumes:
        raise RuntimeError(f"Job/{name} volumes are not exactly its own inputs: {volumes!r}")
    for container in pod_spec["containers"] + pod_spec.get("initContainers", []):
        if container.get("envFrom"):
            raise RuntimeError(f"Job/{name} unexpectedly imports environment wholesale")
    runner_env = {
        entry["name"]: entry.get("value") for entry in pod_spec["containers"][0]["env"]
    }
    data = bundle["data"]
    pinned = {
        "LOGWEIR_EXECUTION_PLAN_SHA256": sha256_prefixed(restore["spec"]["planBytes"]),
        "LOGWEIR_EXECUTION_APPROVAL_SHA256": sha256_prefixed(data["approval.json"]),
        "LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256": sha256_prefixed(data["approval.sig"]),
        "LOGWEIR_EXECUTION_APPROVER_KEY_SHA256": sha256_prefixed(data["approver.pub.pem"]),
        "LOGWEIR_EXECUTION_ALLOWED_CLUSTERS_SHA256": sha256_prefixed(
            data["allowed-clusters.json"]
        ),
    }
    for variable, digest in pinned.items():
        if runner_env.get(variable) != digest:
            raise RuntimeError(f"Job/{name} {variable} does not pin its own mounted bytes")
    if data["approval.json"] != approval["spec"]["approvalBytes"]:
        raise RuntimeError(f"bundle for {name} does not carry its own Approval bytes")
    if data["approval.sig"] != approval["spec"]["sidecarBytes"]:
        raise RuntimeError(f"bundle for {name} does not carry its own Approval sidecar")
    annotations = bundle["metadata"].get("annotations", {})
    expected_annotations = {
        "logweir.dev/restore-uid": restore["metadata"]["uid"],
        "logweir.dev/plan-hash": sha256_prefixed(restore["spec"]["planBytes"]),
        "logweir.dev/approval-name": approval["metadata"]["name"],
        "logweir.dev/approval-uid": approval["metadata"]["uid"],
    }
    for key, value in expected_annotations.items():
        if annotations.get(key) != value:
            raise RuntimeError(f"bundle for {name} annotation {key} is not bound to it")
    if plan_state == "substituted-after-start":
        # The harness deliberately replaced this object after the runner had
        # captured its bytes; the pinned digest above is the binding that
        # matters, and the replacement is asserted by the calling case.
        return
    if plan_cm.get("data") != {"restore.yaml": restore["spec"]["planBytes"]}:
        raise RuntimeError(f"plan ConfigMap for {name} does not carry exact planBytes")
    if plan_state == "legacy":
        # The adopted pre-PLAT-01 plan stays mutable and unannotated; the new
        # Job pins its exact digest instead (asserted above), so a later
        # replacement is refused by the runner before any client exists.
        if plan_cm.get("immutable") is True or {
            "logweir.dev/restore-uid",
            "logweir.dev/plan-hash",
        }.intersection(plan_cm["metadata"].get("annotations") or {}):
            raise RuntimeError(f"legacy plan ConfigMap for {name} was rewritten")
    elif plan_state != "immutable":
        raise RuntimeError(f"unknown plan_state {plan_state!r}")
    elif plan_cm.get("immutable") is not True:
        raise RuntimeError(f"plan ConfigMap for {name} is not immutable")


def verify_restore(
    name: str,
    prefix: str,
    *,
    plan_state: str = "immutable",
    instrumentation_volumes: frozenset[tuple[str, str, str]] = frozenset(),
) -> dict[str, Any]:
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
    assert_only_own_inputs(
        name,
        restore,
        approval,
        job,
        bundle,
        plan_cm,
        plan_state=plan_state,
        instrumentation_volumes=instrumentation_volumes,
    )
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
        other_env = {
            entry["name"]: entry.get("value")
            for entry in results[other]["job"]["spec"]["template"]["spec"]["containers"][0][
                "env"
            ]
        }
        for variable in [
            "LOGWEIR_EXECUTION_PLAN_SHA256",
            "LOGWEIR_EXECUTION_APPROVAL_SHA256",
            "LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256",
        ]:
            if env[variable] == other_env[variable]:
                raise RuntimeError(f"concurrent Restores pinned the same {variable}")
    save_artifact(
        "concurrent-restores-mounted-input-binding.json",
        [
            {
                "restore": item["restore"]["metadata"]["name"],
                "restore_uid": item["restore"]["metadata"]["uid"],
                "approval_uid": item["approval"]["metadata"]["uid"],
                "bundle": item["bundle"]["metadata"]["name"],
                "bundle_uid": item["bundle"]["metadata"]["uid"],
                "pinned_digests": {
                    entry["name"]: entry.get("value")
                    for entry in item["job"]["spec"]["template"]["spec"]["containers"][0]["env"]
                    if entry["name"].startswith("LOGWEIR_EXECUTION_")
                },
                "volumes": item["job"]["spec"]["template"]["spec"]["volumes"],
            }
            for item in results
        ],
    )
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
    STATE["cases"]["restores_mount_only_their_own_bundles"] = "passed"
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
    # The signer cases were renamed when the directory case stopped being
    # counted as an unreadable file; rerun unless both current names passed.
    if (
        STATE["cases"].get("signer_missing_malformed_wrong_path_type") != "passed"
        or STATE["cases"].get("signer_permission_denied_regular_file") != "passed"
    ):
        signer_prerequisite_jobs()
    if STATE["cases"].get("legacy_same_uid_inflight_observation") != "passed":
        legacy_same_uid_observation()
    if STATE["cases"].get("legacy_mutable_prejob_transition") != "passed":
        legacy_prejob_transition()


class PrerequisiteUnavailable(RuntimeError):
    """A case could not start because a case it depends on did not pass."""


def requires(*case_names: str) -> None:
    missing = [name for name in case_names if STATE["cases"].get(name) != "passed"]
    if missing:
        raise PrerequisiteUnavailable(f"prerequisite case(s) did not pass: {missing}")


def execute_independent(case_names: list[str], action: Callable[[], None]) -> None:
    """Run one case group, preserving its failure and continuing the matrix.

    A group must classify every case it owns. Returning without doing so is a
    failure, never an implicit pass; a missing prerequisite leaves the cases
    `unrun` with a recorded reason, which the terminal gate still rejects.
    """
    for case_name in case_names:
        STATE["cases"][case_name] = "unrun"
    save_state()
    try:
        action()
        unclassified = [name for name in case_names if STATE["cases"].get(name) == "unrun"]
        if unclassified:
            raise RuntimeError(f"case group returned without classifying {unclassified}")
    except PrerequisiteUnavailable as exc:
        message = redact(str(exc))
        STATE.setdefault("unrun_reasons", {}).update({name: message for name in case_names})
        log(f"cases left unrun for {case_names}: {message}")
    except Exception as exc:  # noqa: BLE001 - independent cases must continue
        message = redact(f"{type(exc).__name__}: {exc}")
        for case_name in case_names:
            if STATE["cases"].get(case_name) != "passed":
                STATE["cases"][case_name] = "failed"
        STATE.setdefault("case_failures", []).append(
            {"cases": case_names, "error": message}
        )
        log(f"case failure preserved for {case_names}: {message}")
    save_state()


def matrix() -> None:
    """One authoritative, ordered run of every PLAT-01 / PLAT-02.2 live case."""
    STATE["matrix_started"] = dt.datetime.now(dt.timezone.utc).isoformat()
    STATE["images_under_test"] = {
        "current_runner": CURRENT_RUNNER,
        "current_controller": CURRENT_CONTROLLER,
        "old_runner": OLD_RUNNER,
        "old_controller": OLD_CONTROLLER,
        "source_commit": SOURCE_COMMIT,
        "old_commit": OLD_COMMIT,
    }
    save_state()
    lab_switch()
    try:
        setup_namespace()
        if "baseline_target_topics" not in STATE:
            STATE["baseline_target_topics"] = sorted(target_topics())
            save_state()
        save_artifact("baseline-target-topics.json", STATE["baseline_target_topics"])
        create_clusters()
        backup = "fresh_scram_backup"
        restores = "two_simultaneous_restores"
        execute_independent([backup], fresh_backup)

        def after(prerequisites: list[str], action: Callable[[], None]) -> Callable[[], None]:
            def guarded() -> None:
                requires(*prerequisites)
                action()

            return guarded

        execute_independent(
            [
                restores,
                "controller_restart_in_flight",
                "bundle_immutability",
                "restores_mount_only_their_own_bundles",
            ],
            after([backup], concurrent_restores),
        )
        execute_independent(
            ["current_controller_actual_old_runner_handshake"],
            after([backup], old_runner_handshake),
        )
        execute_independent(
            ["configmap_substitution_before_mount"], after([backup], premount_substitution)
        )
        execute_independent(
            ["missing_bundle_before_mount"], after([backup], missing_bundle_before_mount)
        )
        execute_independent(["job_collision_owner_matrix"], after([backup], job_collision_matrix))
        execute_independent(
            ["ownerless_plan_configmap_collision", "approval_recreated_uid_replay"],
            after([backup], configmap_collision_and_recreated_uid),
        )
        execute_independent(
            ["configmap_collision_owner_matrix"], after([backup], configmap_owner_matrix)
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
        execute_independent(
            ["approval_wrong_namespace_replay", "approval_wrong_kind_replay"],
            after([restores], injected_subject_replay_matrix),
        )
        execute_independent(
            ["distinct_approval_cross_use"], after([restores], cross_restore_approval_use)
        )
        execute_independent(
            ["signer_missing_malformed_wrong_path_type", "signer_permission_denied_regular_file"],
            after([restores], signer_prerequisite_jobs),
        )
        execute_independent(
            ["legacy_same_uid_inflight_observation"],
            after([backup], legacy_same_uid_observation),
        )
        execute_independent(
            ["legacy_mutable_prejob_transition"], after([backup], legacy_prejob_transition)
        )
        execute_independent(
            ["controller_runner_rbac", "retained_then_owner_gc"], after([restores], rbac_and_gc)
        )
        execute_independent(["retained_old_archive_evidence"], verify_retained_archive_evidence)
        execute_independent(["additive_crd_drain_fence_restart"], additive_drain_fence)
        execute_independent(
            ["exact_old_controller_job_observation"], after([backup], exact_old_controller_job)
        )
        execute_independent(
            [
                "runner_refuses_tampered_bundle_member",
                "runner_refuses_missing_bundle_member",
                "controller_upgrade_drain_rollback_during_restore",
                "post_start_projected_plan_replacement",
                "live_plan_directed_network_observation",
                "mid_run_signer_rotation",
            ],
            after([backup], runtime_gap_matrix),
        )
    finally:
        STATE["matrix_finished"] = dt.datetime.now(dt.timezone.utc).isoformat()
        save_state()
        lab_restore()


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


def exec_runner(pod_name: str, argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Observe the live single-container runner Pod without adding a sidecar."""
    return run(K + ["exec", pod_name, "-c", "runner", "--"] + argv, check=check)


def assert_job_uid_unchanged(name: str, uid: str, event: str) -> dict[str, Any]:
    job = get("job", name)
    if job["metadata"]["uid"] != uid:
        raise RuntimeError(f"{event}: Job/{name} was replaced during execution")
    return {"event": event, "job_uid": job["metadata"]["uid"], **controller_identity(controller_deployment())}


def preauth_negative_job(
    name: str,
    emitted_job: dict[str, Any],
    emitted_bundle: dict[str, Any],
    *,
    label: str,
    mutate: Callable[[dict[str, Any]], None],
    remove_item: str | None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Run the controller-emitted runner spec against a controlled bundle copy."""
    bundle_name = f"{name}-{label}-bundle"
    bundle = copy.deepcopy(emitted_bundle)
    bundle["metadata"] = owned_metadata(bundle_name)
    bundle["immutable"] = True
    mutate(bundle["data"])
    apply(bundle)
    spec = sanitized_job_spec(emitted_job, case=f"preauth-{label}")
    for volume in spec["template"]["spec"]["volumes"]:
        if volume["name"] == "approval":
            volume["configMap"]["name"] = bundle_name
            if remove_item is not None:
                volume["configMap"]["items"] = [
                    item for item in volume["configMap"]["items"] if item["key"] != remove_item
                ]
    job_name = f"{name}-preauth-{label}"
    apply(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": owned_metadata(job_name),
            "spec": spec,
        }
    )
    pod, status = wait_runner_termination(job_name, 240)
    logs_proc = run(K + ["logs", pod["metadata"]["name"], "-c", "runner"], check=False, timeout=60)
    logs = logs_proc.stdout + logs_proc.stderr
    save_artifact(f"runtime-gap-preauth-{label}-pod.json", pod)
    save_artifact(f"runtime-gap-preauth-{label}-bundle.json", get("configmap", bundle_name))
    save_artifact(f"runtime-gap-preauth-{label}-logs.txt", logs)
    return pod, status, logs


def runtime_gap_matrix() -> None:
    """Real-runner timing for projected inputs, network, rollout and revalidation.

    The instrumented Job keeps the controller's single-container contract so
    the unmodified controller still finalizes the Restore from `pods/log`.
    Observations use `kubectl exec` into the paused runner container instead of
    a sidecar (a second container makes the controller's log read ambiguous).
    """
    original = controller_deployment()
    original_identity = controller_identity(original)
    name = "backend-live-runtime-gap-retained"
    approval_name = "backend-live-runtime-gap-retained-approval"
    prefix = "backend-live-runtime-gap-retained-"
    wrapper_name = "backend-live-runtime-gap-wrapper"
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
        before_topics = target_topics()
        before_archive = archive_listing()

        set_pod_quota("0")
        point = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        parsed = json.loads(restore_plan(prefix, point))
        parsed["notifications"]["webhooks"] = [url_a]
        plan = json.dumps(parsed, indent=2) + "\n"
        plan, restore_uid = create_restore_with_plan(name, approval_name, prefix, plan, point)
        approve_restore(name, approval_name, plan)
        emitted_job = wait_for("job", name, lambda _item: True, seconds=180)
        emitted_plan = get("configmap", f"{name}-plan")
        emitted_bundle = get("configmap", f"{name}-approval-bundle")
        save_artifact("runtime-gap-controller-job.json", emitted_job)
        save_artifact("runtime-gap-original-plan.json", emitted_plan)
        save_artifact("runtime-gap-original-bundle.json", emitted_bundle)
        if len(emitted_job["spec"]["template"]["spec"]["containers"]) != 1:
            raise RuntimeError("controller-emitted Restore Job is not single-container")

        # Fence the controller so the emitted Job can be replaced by an
        # instrumented copy of the same spec; no Pod ran for the original.
        scale_any_controller(0)
        controller_running = False
        delete_with_uid_precondition(
            "job", name, emitted_job["metadata"]["uid"], api_version="batch/v1"
        )
        wait_absent("job", name)
        remove_pod_quota()

        # Runner revalidation before any client: a substituted member and a
        # missing member, each with owned receivers watching for traffic.
        _pod, tampered_status, tampered_logs = preauth_negative_job(
            name,
            emitted_job,
            emitted_bundle,
            label="tampered-member",
            mutate=lambda data: data.__setitem__(
                "allowed-clusters.json", data["allowed-clusters.json"] + " "
            ),
            remove_item=None,
        )
        tampered_exit = tampered_status["state"]["terminated"]["exitCode"]
        if tampered_exit != 3 or "allowed-clusters bytes hash to" not in tampered_logs or (
            "no data operation was started" not in tampered_logs
        ):
            raise RuntimeError(
                f"tampered bundle member was not refused by digest: exit={tampered_exit}"
            )
        _pod, missing_status, missing_logs = preauth_negative_job(
            name,
            emitted_job,
            emitted_bundle,
            label="missing-member",
            mutate=lambda data: data.pop("approval.sig"),
            remove_item="approval.sig",
        )
        missing_exit = missing_status["state"]["terminated"]["exitCode"]
        if missing_exit == 0 or "approval sidecar" not in missing_logs or (
            "No such file or directory" not in missing_logs
        ):
            raise RuntimeError(
                f"missing bundle member was not refused before data work: exit={missing_exit}"
            )
        preauth_observation = {
            "receiver_a_posts": receiver_count("a"),
            "receiver_b_posts": receiver_count("b"),
            "tampered_member_exit": tampered_exit,
            "missing_member_exit": missing_exit,
            "target_topics_unchanged": target_topics() == before_topics,
            "archive_listing_unchanged": archive_listing() == before_archive,
        }
        save_artifact("runtime-gap-preauth-receiver-observation.json", preauth_observation)
        if (
            preauth_observation["receiver_a_posts"] != 0
            or preauth_observation["receiver_b_posts"] != 0
            or not preauth_observation["target_topics_unchanged"]
            or not preauth_observation["archive_listing_unchanged"]
        ):
            raise RuntimeError(f"pre-authentication refusal had side effects: {preauth_observation!r}")
        STATE["cases"]["runner_refuses_tampered_bundle_member"] = "passed"
        STATE["cases"]["runner_refuses_missing_bundle_member"] = "passed"
        save_state()

        set_pod_quota("0")
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
        runner["env"].append({"name": "LOGWEIR_ENGINE_BIN", "value": "/instrument/engine"})
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
        metadata = owned_metadata(name)
        metadata["ownerReferences"] = [
            owner_reference("Restore", name, restore_uid, controller=True)
        ]
        apply({"apiVersion": "batch/v1", "kind": "Job", "metadata": metadata, "spec": instrumented})
        instrumented_job = get("job", name)
        job_uid = instrumented_job["metadata"]["uid"]
        save_artifact("runtime-gap-instrumented-job.json", instrumented_job)
        remove_pod_quota()
        pod = pod_for_job(name, seconds=180)
        pod_name = pod["metadata"]["name"]
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            if exec_runner(pod_name, ["test", "-f", "/work/ENGINE_STARTED"], check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("instrumented real runner never reached engine execution")
        before_hashes = exec_runner(
            pod_name, ["sha256sum", "/plan/restore.yaml", "/signing/key.pem"]
        ).stdout
        save_artifact("runtime-gap-projected-before-sha256.txt", before_hashes)

        transitions = []
        configure_controller_images(OLD_CONTROLLER, OLD_RUNNER)
        scale_any_controller(1)
        controller_running = True
        transitions.append(assert_job_uid_unchanged(name, job_uid, "rollback_to_archived"))
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        transitions.append(assert_job_uid_unchanged(name, job_uid, "upgrade_to_current"))
        scale_any_controller(0)
        controller_running = False
        transitions.append(assert_job_uid_unchanged(name, job_uid, "drain_current"))
        configure_controller_images(OLD_CONTROLLER, OLD_RUNNER)
        scale_any_controller(1)
        controller_running = True
        transitions.append(assert_job_uid_unchanged(name, job_uid, "rollback_during_execution"))
        set_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        transitions.append(assert_job_uid_unchanged(name, job_uid, "reupgrade_during_execution"))
        scale_any_controller(0)
        controller_running = False
        transitions.append(
            assert_job_uid_unchanged(name, job_uid, "final_drain_before_input_replacement")
        )
        if exec_runner(pod_name, ["test", "-f", "/work/ENGINE_STARTED"], check=False).returncode:
            raise RuntimeError("runner Pod did not survive the controller transitions")
        save_artifact("runtime-gap-controller-transitions.json", transitions)

        replacement = copy.deepcopy(parsed)
        replacement["notifications"]["webhooks"] = [url_b]
        replacement["objectives"]["rto_seconds"] = 3599
        replacement_bytes = json.dumps(replacement, indent=2) + "\n"
        delete_with_uid_precondition(
            "configmap", f"{name}-plan", emitted_plan["metadata"]["uid"], api_version="v1"
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
        save_artifact("runtime-gap-replacement-plan.json", get("configmap", f"{name}-plan"))

        original_public = FIXTURE_KEYS / "signing.pub.pem"
        (OUT / "original-signing.pub.pem").write_bytes(original_public.read_bytes())
        with tempfile.NamedTemporaryFile(
            prefix="logweir-runtime-gap-", suffix=".pem", delete=False
        ) as private_file:
            private_path = pathlib.Path(private_file.name)
        private_path.chmod(0o600)
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
        rotated_public.write_text(run(["openssl", "pkey", "-in", str(private_path), "-pubout"]).stdout)
        rotated_bytes = private_path.read_bytes()
        private_path.unlink()
        private_path = None
        run(
            K
            + [
                "patch",
                "secret",
                "logweir-signing-key",
                "--type=merge",
                "-p",
                json.dumps({"data": {"signing.pem": base64.b64encode(rotated_bytes).decode()}}),
            ]
        )
        expected_signer_hash = hashlib.sha256(rotated_bytes).hexdigest()
        del rotated_bytes
        original_plan_hash, original_signer_hash = [
            line.split()[0] for line in before_hashes.splitlines()
        ]
        deadline = time.monotonic() + 180
        after_hashes = ""
        while time.monotonic() < deadline:
            after_hashes = exec_runner(
                pod_name, ["sha256sum", "/plan/restore.yaml", "/signing/key.pem"]
            ).stdout
            signer_now = after_hashes.splitlines()[1].split()[0]
            if signer_now == expected_signer_hash:
                break
            time.sleep(3)
        else:
            raise RuntimeError("in-place Secret signer rotation did not reach the running Pod")
        plan_now = after_hashes.splitlines()[0].split()[0]
        replacement_hash = hashlib.sha256(replacement_bytes.encode()).hexdigest()
        if plan_now == original_plan_hash:
            plan_projection = "original-retained-after-configmap-delete-recreate"
        elif plan_now == replacement_hash:
            plan_projection = "replacement-propagated-to-mount"
        else:
            raise RuntimeError(f"projected plan reached an unexpected third digest {plan_now}")
        save_artifact("runtime-gap-projected-after-sha256.txt", after_hashes)

        # Restore the current controller before release so it observes and
        # finalizes the single-container Job exactly as in production.
        configure_controller_images(CURRENT_CONTROLLER, CURRENT_RUNNER)
        scale_any_controller(1)
        controller_running = True
        transitions.append(assert_job_uid_unchanged(name, job_uid, "current_controller_before_release"))
        save_artifact("runtime-gap-controller-transitions.json", transitions)
        exec_runner(pod_name, ["touch", "/work/CONTINUE"])
        finished_pod, runner_status = wait_runner_termination(name, 720)
        termination = runner_status["state"]["terminated"]
        if termination["exitCode"] != 0:
            raise RuntimeError(f"instrumented real runner exited {termination['exitCode']}")
        result = verify_restore(
            name,
            prefix,
            plan_state="substituted-after-start",
            instrumentation_volumes=frozenset({("instrument", "configMap", wrapper_name)}),
        )
        if get("job", name)["metadata"]["uid"] != job_uid:
            raise RuntimeError("controller replaced the instrumented Job before finalizing")
        scorecard_path = OUT / f"{name}-scorecard.json"
        signature_path = OUT / f"{name}-scorecard.sig"
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
        save_artifact(
            "runtime-gap-rotated-key-verdict.txt",
            f"rc={rotated_verdict.returncode}\n{rotated_verdict.stdout}{rotated_verdict.stderr}",
        )
        if rotated_verdict.returncode != 1:
            raise RuntimeError("rotated key unexpectedly verified the retained-signer scorecard")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and receiver_count("a") < 1:
            time.sleep(1)
        receiver_observation = {
            "receiver_a_posts": receiver_count("a"),
            "receiver_b_posts": receiver_count("b"),
        }
        save_artifact("runtime-gap-receiver-observation.json", receiver_observation)
        save_artifact(
            "runtime-gap-receiver-a-logs.txt",
            run(K + ["logs", "backend-live-receiver-a", "-c", "receiver"]).stdout,
        )
        save_artifact(
            "runtime-gap-receiver-b-logs.txt",
            run(K + ["logs", "backend-live-receiver-b", "-c", "receiver"]).stdout,
        )
        if receiver_observation != {"receiver_a_posts": 1, "receiver_b_posts": 0}:
            raise RuntimeError(f"notification routing changed after parse: {receiver_observation!r}")
        save_artifact("runtime-gap-runner-pod.json", finished_pod)
        STATE.setdefault("owned_target_topics", []).extend([prefix + "orders", prefix + "payments"])
        STATE["owned_target_topics"] = sorted(set(STATE["owned_target_topics"]))
        STATE["runtime_gap"] = {
            "restore_uid": restore_uid,
            "job_uid": job_uid,
            "runner_image_id": runner_status.get("imageID"),
            "runner_exit_code": termination["exitCode"],
            "restore_phase": result["restore"]["status"].get("phase"),
            "restore_verification": result["restore"]["status"]
            .get("evidence", {})
            .get("verification", {})
            .get("result"),
            "original_plan_sha256": original_plan_hash,
            "replacement_plan_sha256": replacement_hash,
            "plan_projection_observation": plan_projection,
            "original_signer_sha256": original_signer_hash,
            "rotated_signer_sha256": expected_signer_hash,
            "notification": receiver_observation,
            "preauth": preauth_observation,
            "controller_transitions": [item["event"] for item in transitions],
            "controlled_instrumentation": (
                "controller-emitted Job replaced by the same single-container spec plus an engine "
                "wrapper that pauses after startup validation and then execs the real kafka-backup"
            ),
        }
        for case_name in [
            "controller_upgrade_drain_rollback_during_restore",
            "post_start_projected_plan_replacement",
            "live_plan_directed_network_observation",
            "mid_run_signer_rotation",
        ]:
            STATE["cases"][case_name] = "passed"
        save_state()
        log("runtime gap matrix passed with controller finalization of the instrumented Job")
    finally:
        if private_path is not None and private_path.exists():
            private_path.unlink()
        if not controller_running:
            configure_controller_images(
                original_identity["image"], original_identity["runner_image"]
            )
            scale_any_controller(1)
        else:
            set_controller_images(original_identity["image"], original_identity["runner_image"])
        restored = controller_identity(controller_deployment())
        exact = (
            restored["deployment_uid"] == original_identity["deployment_uid"]
            and restored["image"] == original_identity["image"]
            and restored["runner_image"] == original_identity["runner_image"]
            and restored["spec_sha256"] == original_identity["spec_sha256"]
        )
        STATE["shared_controller_restore_exact_after_runtime_gap"] = exact
        save_state()
        if not exact:
            raise RuntimeError("controller under test was not restored after runtime-gap case")


def report() -> None:
    """Write the machine report and Markdown handoff for the authoritative run."""
    public_key = OUT / "verifier-signing.pub.pem"
    public_key.write_bytes((FIXTURE_KEYS / "signing.pub.pem").read_bytes())
    public_key.chmod(0o600)

    # The live-unreachable post-parse signing failure is covered by the
    # source-matched seam test and the binary subprocess suite, run from the
    # checkout whose crates are byte-identical to the tested source commit.
    head = run(["git", "rev-parse", "HEAD"]).stdout.strip()
    # Name what the working tree changed since the tested commit rather than
    # answering yes/no: a harness or test edit is not a runtime difference, and
    # the critical-file hashes below say so file by file.
    changed_crate_files = sorted(
        line.strip()
        for line in run(
            ["git", "diff", "--name-only", SOURCE_COMMIT or "HEAD", "--", "crates", "Cargo.lock", "Cargo.toml"],
            check=False,
        ).stdout.splitlines()
        if line.strip()
    )
    seam_tests: dict[str, Any] = {}
    for label, argv in [
        (
            "signer_readiness_seam",
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "logweir",
                "--lib",
                "signer::tests::readiness_rejects_a_real_signature_that_does_not_verify_over_its_probe",
                "--",
                "--exact",
            ],
        ),
        ("signing_startup_subprocess", ["cargo", "test", "--locked", "-p", "logweir", "--test", "signing_startup"]),
    ]:
        proc = subprocess.run(
            argv, cwd=ROOT, capture_output=True, text=True, timeout=1800, check=False
        )
        output = proc.stdout + proc.stderr
        save_artifact(f"{label}-test.txt", output[-16000:])
        summary = [line for line in output.splitlines() if line.startswith("test result:")]
        seam_tests[label] = {
            "command": " ".join(argv),
            "exit_code": proc.returncode,
            "summary": summary,
        }
    if seam_tests["signer_readiness_seam"]["exit_code"] != 0 or not any(
        "1 passed; 0 failed" in line for line in seam_tests["signer_readiness_seam"]["summary"]
    ):
        STATE["cases"]["signer_unit_seam_for_unreachable_live_failure"] = "failed"
    else:
        STATE["cases"]["signer_unit_seam_for_unreachable_live_failure"] = "passed"
    save_state()

    python_bin = os.environ.get("LOGWEIR_PYTHON", sys.executable)
    python_version = run([python_bin, "--version"]).stdout.strip()
    cryptography_version = run(
        [python_bin, "-c", "import cryptography; print(cryptography.__version__)"]
    ).stdout.strip()
    runner_version_proc = run(
        DOCKER + ["run", "--rm", "--platform", "linux/amd64", CURRENT_RUNNER, "--version"],
        check=False,
    )
    runner_version = (runner_version_proc.stdout + runner_version_proc.stderr).strip()

    critical = [
        "crates/logweir-core/src/execution_contract.rs",
        "crates/logweir/src/cli.rs",
        "crates/logweir/src/identity.rs",
        "crates/logweir/src/drill/mod.rs",
        "crates/logweir/src/drill/phase1_approval.rs",
        "crates/logweir/src/drill/phase8_score.rs",
        "crates/logweir/src/signer.rs",
        "crates/weirkeeper/src/controllers/restore.rs",
        "crates/weirkeeper/src/controllers/approval.rs",
        "crates/weirkeeper/src/job.rs",
        "config/crd/approvals.yaml",
        "config/crd/restores.yaml",
        "Dockerfile",
        "Dockerfile.weirkeeper",
        "Cargo.lock",
    ]
    source_hashes = {
        path: {
            "tested_source_root": sha256_file(SOURCE_ROOT / path),
            "worktree_at_report": sha256_file(ROOT / path),
            "old_source_root": sha256_file(OLD_SOURCE_ROOT / path)
            if (OLD_SOURCE_ROOT / path).is_file()
            else None,
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
            run(DOCKER + ["image", "inspect", image, "--format", "{{json .}}"]).stdout
        )
        binary_hash = run(
            DOCKER
            + ["run", "--rm", "--platform", platform, "--entrypoint", "sha256sum", image, binary]
        ).stdout.split()[0]
        images[image] = {
            "id": inspected["Id"],
            "architecture": inspected["Architecture"],
            "created": inspected["Created"],
            "revision_label": (inspected.get("Config", {}).get("Labels") or {}).get(
                "org.opencontainers.image.revision"
            ),
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
    build_record_path = os.environ.get("LOGWEIR_BACKEND_LIVE_BUILD_RECORD", "")
    build_record = (
        json.loads(pathlib.Path(build_record_path).read_text()) if build_record_path else {}
    )

    cases = STATE.get("cases", {})
    gate_exit, gate_reasons = acceptance_gate(
        cases,
        STATE.get("cleanup", {}),
        require_cleanup=True,
        lab_state=lab_gate_state(),
    )
    classifications = {
        "required": {name: cases.get(name, "missing") for name in sorted(REQUIRED_CASES)},
        "passed": sorted(name for name in REQUIRED_CASES if cases.get(name) == "passed"),
        "failed": gate_reasons["failed"],
        "unrun": gate_reasons["unrun"],
        "missing": gate_reasons["missing"],
        "unrun_reasons": STATE.get("unrun_reasons", {}),
        "unsupported_live": UNSUPPORTED_LIVE_CASES,
        "supplemental": {
            name: result for name, result in sorted(cases.items()) if name not in REQUIRED_CASES
        },
    }
    namespace = get_optional("namespace", NS, cluster_scoped=True)
    deployment_identity = controller_identity(controller_deployment())
    crd = json.loads(run(KUBECTL + ["get", "crd", "approvals.logweir.dev", "-o", "json"]).stdout)
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
        "matrix_window": {
            "started": STATE.get("matrix_started"),
            "finished": STATE.get("matrix_finished"),
        },
        "classifications": classifications,
        "gate_reasons": gate_reasons,
        "evidence": {
            "backup_id": STATE.get("backup_id"),
            "positive_restores": STATE.get("positive_restores", []),
            "restart_job_uids": STATE.get("restart_job_uids"),
            "legacy_prejob": STATE.get("legacy_prejob", {}),
            "exact_old_controller": STATE.get("exact_old_controller", {}),
            "runtime_gap": STATE.get("runtime_gap", {}),
            "signer_cases": STATE.get("signer_cases", {}),
            "rbac": STATE.get("rbac", {}),
            "gc_dependents": STATE.get("gc_dependents", {}),
            "retained_signer": STATE.get("retained_signer", {}),
            "old_runner_exit_code": STATE.get("old_runner_exit_code"),
            "scorecard_sample": {
                "configured_records_per_partition": 25,
                "partitions": 2,
                "expected": 50,
            },
        },
        "signer_post_parse_failure": {
            "live_classification": "unsupported_live",
            "reason": UNSUPPORTED_LIVE_CASES["post_parse_software_signing_failure"],
            "tests": seam_tests,
        },
        "source_applicability": {
            "source_commit": SOURCE_COMMIT,
            "old_commit": OLD_COMMIT,
            "source_root": str(SOURCE_ROOT),
            "old_source_root": str(OLD_SOURCE_ROOT),
            "worktree_head": head,
            "worktree_changed_crate_files_since_source_commit": changed_crate_files,
            "worktree_changed_runtime_sources": [
                path
                for path in changed_crate_files
                if "/tests/" not in path and not path.endswith("Cargo.lock")
            ],
            "critical_hashes": source_hashes,
        },
        "images": images,
        "build_record": build_record,
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
            "lab_controller_original": {
                key: value
                for key, value in (STATE.get(LAB_ORIGINAL) or {}).items()
                if key != "containers"
            },
            "lab_controller_restored": STATE.get("lab_controller_restored"),
            "lab_controller_restore_exact": STATE.get("lab_controller_restore_exact"),
            "lab_controller_now": deployment_identity,
            "approval_crd_uid": crd["metadata"]["uid"],
            "approval_crd_established": any(
                item.get("type") == "Established" and item.get("status") == "True"
                for item in crd.get("status", {}).get("conditions", [])
            ),
        },
        "harness_iterations": STATE.get("case_failures", []) + STATE.get("errors", []),
        "artifact_hashes": artifact_hashes,
    }
    REPORT_PATH.write_text(json.dumps(structured, indent=2, sort_keys=True) + "\n")
    REPORT_PATH.chmod(0o600)

    passed = classifications["passed"]
    runtime = STATE.get("runtime_gap", {})
    old = STATE.get("exact_old_controller", {})
    legacy = STATE.get("legacy_prejob", {})
    lines = [
        "# PLAT-01 / PLAT-02.2 live matrix",
        "",
        f"Verdict: **{structured['verdict']}** (terminal exit `{gate_exit}`).",
        f"Required: {len(REQUIRED_CASES)}; passed: {len(passed)}; failed: "
        f"{len(classifications['failed'])}; unrun: {len(classifications['unrun'])}; "
        f"missing: {len(classifications['missing'])}.",
        f"Source `{SOURCE_COMMIT}`; old `{OLD_COMMIT}`; images: "
        + ", ".join(f"`{name}`=`{value['id']}`" for name, value in images.items()),
        "",
        "## Cases",
        "",
        *[f"- `{name}`: {result}" for name, result in classifications["required"].items()],
        "",
        "Unsupported live (classified, not counted as passed):",
        *[f"- `{name}`: {reason}" for name, reason in UNSUPPORTED_LIVE_CASES.items()],
        "",
        "## Key observations",
        "",
        f"- Legacy mutable pre-Job plan: Restore `{legacy.get('restore_uid')}`, ConfigMap "
        f"`{legacy.get('configmap_uid')}` retained, runner exit `{legacy.get('exit_code')}`.",
        f"- Archived controller Job `{old.get('job_uid')}` adopted unchanged; old runner exit "
        f"`{old.get('runner_exit_code')}`, result `{old.get('result_phase')}`.",
        f"- Runtime gap: transitions `{runtime.get('controller_transitions')}`, plan projection "
        f"`{runtime.get('plan_projection_observation')}`, receivers `{runtime.get('notification')}`, "
        f"pre-auth `{runtime.get('preauth')}`, controller verification "
        f"`{runtime.get('restore_verification')}`.",
        "",
        f"Cleanup: namespace absent `{namespace is None}`; lab controller restore exact "
        f"`{STATE.get('lab_controller_restore_exact')}`; cleanup result "
        f"`{STATE.get('cleanup', {}).get('result')}`.",
        "",
        f"Machine report: `{REPORT_PATH}`",
    ]
    markdown_path = pathlib.Path(
        os.environ.get("LOGWEIR_BACKEND_LIVE_MARKDOWN", str(OUT / "report.md"))
    )
    markdown_path.write_text("\n".join(lines) + "\n")
    markdown_path.chmod(0o600)
    STATE["report"] = {
        "machine": str(REPORT_PATH),
        "markdown": str(markdown_path),
        "exit_code": gate_exit,
    }
    save_state()
    log(f"reports written: {REPORT_PATH} and {markdown_path}")


def gate_probe() -> None:
    payload = json.loads(os.environ["LOGWEIR_GATE_PROBE_JSON"])
    status, _reasons = acceptance_gate(
        payload.get("cases", {}),
        payload.get("cleanup", {}),
        require_cleanup=True,
        required_cases=set(payload.get("required", [])),
        lab_state=payload.get("lab"),
    )
    raise SystemExit(status)


def harness_selftest() -> None:
    """Bounded local controls proving the harness can fail; no cluster access."""
    probes = {"livenessProbe": {"exec": {"command": ["/usr/local/bin/weirkeeper", "--probe", "live"]}},
              "readinessProbe": {"exec": {"command": ["/usr/local/bin/weirkeeper", "--probe", "ready"]}}}
    if [op["op"] for op in probe_patch(dict(probes), OLD_CONTROLLER, probes)] != ["remove", "remove"]:
        raise RuntimeError("the old controller would keep probes its binary cannot answer")
    if probe_patch({}, CURRENT_CONTROLLER, probes) != [
        {"op": "add", "path": "/spec/template/spec/containers/0/" + k, "value": v}
        for k, v in probes.items()
    ]:
        raise RuntimeError("a switch back would not restore the recorded probes")
    if probe_patch(dict(probes), CURRENT_CONTROLLER, probes) != []:
        raise RuntimeError("an unchanged current controller would be patched")
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
        ({"cases": {"a": "unrun"}, "cleanup": {"result": "deleted"}}, 1),
        ({"cases": {"a": "skipped"}, "cleanup": {"result": "deleted"}}, 1),
        ({"cases": {"a": "passed"}, "cleanup": {"result": "failed"}}, 1),
        (
            {
                "cases": {"a": "passed"},
                "cleanup": {"result": "deleted"},
                "lab": {"recorded": True, "restore_exact": False},
            },
            1,
        ),
        (
            {
                "cases": {"a": "passed"},
                "cleanup": {"result": "deleted"},
                "lab": {"recorded": True, "restore_exact": True},
            },
            0,
        ),
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

    # execute_independent must never turn an unclassified or blocked group into a pass.
    saved_cases = copy.deepcopy(STATE.get("cases", {}))
    saved_failures = copy.deepcopy(STATE.get("case_failures", []))
    saved_unrun = copy.deepcopy(STATE.get("unrun_reasons", {}))
    try:
        execute_independent(["selftest_silent_return"], lambda: None)
        silent = STATE["cases"].get("selftest_silent_return")
        execute_independent(
            ["selftest_blocked"], lambda: requires("selftest_prerequisite_never_passed")
        )
        blocked = STATE["cases"].get("selftest_blocked")

        def raising() -> None:
            raise RuntimeError("controlled failure")

        execute_independent(["selftest_raises"], raising)
        raised = STATE["cases"].get("selftest_raises")
    finally:
        STATE["cases"] = saved_cases
        STATE["case_failures"] = saved_failures
        STATE["unrun_reasons"] = saved_unrun
        save_state()
    if (silent, blocked, raised) != ("failed", "unrun", "failed"):
        raise RuntimeError(
            f"execute_independent classification controls wrong: {(silent, blocked, raised)!r}"
        )
    results.append(
        {
            "execute_independent": {
                "silent_return": silent,
                "missing_prerequisite": blocked,
                "raises": raised,
            }
        }
    )
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
    if any(labels.get(key) != value for key, value in namespace_labels().items()):
        raise RuntimeError("refusing cleanup: namespace ownership label changed")
    if ns["metadata"]["uid"] != expected_uid:
        raise RuntimeError("refusing cleanup: namespace UID changed")
    # Topics this run created: recorded names plus any harness-prefixed topic
    # that was absent from the baseline listing taken before the first case.
    baseline_topics = STATE.get("baseline_target_topics")
    if baseline_topics is not None:
        discovered = sorted(
            topic
            for topic in target_topics()
            if topic.startswith("backend-live-") and topic not in set(baseline_topics)
        )
        STATE["owned_target_topics"] = sorted(
            set(STATE.get("owned_target_topics", [])) | set(discovered)
        )
        save_state()
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
            "matrix",
            "lab-switch",
            "lab-restore",
            "positive",
            "negative",
            "exact-old-controller-job",
            "runtime-gap-matrix",
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
        "matrix",
        "negative",
        "exact-old-controller-job",
        "runtime-gap-matrix",
    }:
        return terminal_exit_status(require_cleanup=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
