#!/usr/bin/env python3
"""Live docker-desktop acceptance for the reviewed PLAT04.1 scheduler.

This is intentionally a controller/API acceptance harness, not a Kafka backup
test.  Backup status writes and runner Job creation are withheld from the test
controller Role so scheduled Backup CRs remain nonterminal until the harness
drains them.  One explicit busybox Job is used as a bounded holding/drain
fixture; backend-live owns Kafka record and recovery proof.
"""

from __future__ import annotations

import copy
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
STAMP = os.environ.get("LOGWEIR_PLAT04_RUN_ID", "20260915t0419z")
SELECTION = os.environ.get("LOGWEIR_PLAT04_SELECTION", "broad")
NS = f"logweir-plat04-live-{STAMP}"
RUN_LABEL = STAMP
POLICY = f"plat04-isolate-{STAMP}"
BINDING = f"plat04-isolate-{STAMP}"
SA = "plat04-scheduler"
CONTROLLERS = ("plat04-controller-a", "plat04-controller-b")
SHARED_CONTROLLER_USER = "system:serviceaccount:logweir-scram-local:weirkeeper"
IMAGE_TAG = "weirkeeper:backend-live-20260915"
IMAGE_ID = "sha256:c829479470548bd1954ab675f2e0d2bcb6c7af9c08e70bffb24b8324fd50d860"
IMAGE_REF = "weirkeeper@sha256:c829479470548bd1954ab675f2e0d2bcb6c7af9c08e70bffb24b8324fd50d860"
SNAPSHOT = pathlib.Path("/tmp/logweir-backend-live-20260915T0330Z/src")
OUT = pathlib.Path(os.environ.get("LOGWEIR_PLAT04_LIVE_OUT", f"/tmp/{NS}"))
STATE_PATH = OUT / "state.json"
REPORT_PATH = OUT / "report.json"
K = ["kubectl", "--context", "docker-desktop"]
D = ["docker", "--context", "desktop-linux"]

EXPECTED_HASHES = {
    "crates/weirkeeper/src/controllers/backup_schedule.rs": "07c18579484ba30ce18c28e0c74c94a74a2c826b528a2746aba2656477c8ebe9",
    "crates/weirkeeper/src/crds/backup_schedule.rs": "ea62f4702379b69138c7d7d407f525cf852c5cb926c776fdcccbf147680db6b1",
    "crates/weirkeeper/tests/schedule_controller.rs": "f2d364a20ddda3c4119195f9561c8c8ecec3545460954685fbbd5dbed962de0a",
    "crates/weirkeeper/tests/crd_shape.rs": "549b2f3f8490219df89aa9dcb844bde95896231473c0cdbac783926b42e8c668",
    "config/crd/backupschedules.yaml": "14f57268d6da1483278c85263c581c99eace6fb0eb85dbad9541292d27f0a053",
}

STATE: dict[str, Any] = {
    "namespace": NS,
    "runLabel": RUN_LABEL,
    "selection": SELECTION,
    "cases": {},
    "uids": {},
    "provenance": {},
    "apiOutcomes": [],
    "cleanup": {"status": "not-run", "errors": []},
    "limitations": [
        "controlled nonterminal Backup CRs do not claim Kafka data recovery",
        "the beyond-horizon reservation is API-seeded in accepted deterministic form",
    ],
}


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def note(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def save() -> None:
    OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")


def run(
    args: list[str],
    *,
    data: str | None = None,
    check: bool = True,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        input=data,
        text=True,
        capture_output=True,
        timeout=timeout,
        cwd=ROOT,
    )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed rc={result.returncode}: {args!r}\n"
            f"stdout={result.stdout[-2000:]}\nstderr={result.stderr[-2000:]}"
        )
    return result


def kubectl(*args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run(K + list(args), check=check, timeout=timeout)


def apply(obj: dict[str, Any]) -> dict[str, Any]:
    result = run(K + ["apply", "-f", "-", "-o", "json"], data=json.dumps(obj), timeout=90)
    return json.loads(result.stdout)


def create(obj: dict[str, Any]) -> dict[str, Any]:
    result = run(K + ["create", "-f", "-", "-o", "json"], data=json.dumps(obj), timeout=60)
    return json.loads(result.stdout)


def get(kind: str, name: str, *, namespace: str | None = NS) -> dict[str, Any]:
    args = []
    if namespace is not None:
        args += ["-n", namespace]
    args += ["get", kind, name, "-o", "json"]
    return json.loads(kubectl(*args).stdout)


def list_items(kind: str, *, selector: str | None = None) -> list[dict[str, Any]]:
    args = ["-n", NS, "get", kind]
    if selector:
        args += ["-l", selector]
    args += ["-o", "json"]
    return json.loads(kubectl(*args).stdout)["items"]


def patch(kind: str, name: str, body: dict[str, Any], *, subresource: str | None = None) -> dict[str, Any]:
    args = ["-n", NS, "patch", kind, name, "--type=merge", "-p", json.dumps(body), "-o", "json"]
    if subresource:
        args += [f"--subresource={subresource}"]
    return json.loads(kubectl(*args).stdout)


def suspend_at_observed_resource_version(schedule: dict[str, Any]) -> dict[str, Any]:
    return patch(
        "backupschedule",
        schedule["metadata"]["name"],
        {
            "metadata": {"resourceVersion": schedule["metadata"]["resourceVersion"]},
            "spec": {"suspend": True},
        },
    )


def wait_for(label: str, predicate: Callable[[], Any], timeout: int = 60, interval: float = 0.5) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
            last = repr(error)
        time.sleep(interval)
    raise RuntimeError(f"timeout waiting for {label}; last={last!r}")


def when(value: Any, predicate: Callable[[Any], bool]) -> Any:
    return value if predicate(value) else None


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def record_uid(key: str, obj: dict[str, Any]) -> str:
    uid = obj["metadata"]["uid"]
    STATE["uids"][key] = uid
    save()
    note(f"created {key} uid={uid}")
    return uid


def schedule_obj(name: str, *, policy: str = "Forbid", suspend: bool = False, omit_policy: bool = False) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "schedule": "* * * * *",
        "sourceRef": {"name": "unused-source"},
        "topics": ["plat04-live-topic"],
        "archive": {"url": "s3://plat04-live/controlled"},
        "suspend": suspend,
    }
    if not omit_policy:
        spec["concurrencyPolicy"] = policy
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": {
            "name": name,
            "namespace": NS,
            "labels": {"plat04.logweir.dev/run": RUN_LABEL},
        },
        "spec": spec,
    }


def ready_reason(schedule: dict[str, Any]) -> str | None:
    for condition in (schedule.get("status") or {}).get("conditions") or []:
        if condition.get("type") == "Ready":
            return condition.get("reason")
    return None


def owned_backups(schedule: dict[str, Any]) -> list[dict[str, Any]]:
    name = schedule["metadata"]["name"]
    uid = schedule["metadata"]["uid"]
    result = []
    for backup in list_items("backups.logweir.dev", selector=f"logweir.dev/schedule={name}"):
        owners = backup["metadata"].get("ownerReferences") or []
        if any(
            owner.get("apiVersion") == "logweir.dev/v1alpha1"
            and owner.get("kind") == "BackupSchedule"
            and owner.get("name") == name
            and owner.get("uid") == uid
            and owner.get("controller") is True
            for owner in owners
        ):
            result.append(backup)
    return result


def scale(name: str, replicas: int) -> None:
    kubectl("-n", NS, "scale", "deployment", name, f"--replicas={replicas}")
    if replicas:
        kubectl("-n", NS, "rollout", "status", f"deployment/{name}", "--timeout=90s", timeout=100)
        wait_for(f"proxy {name}", lambda: proxy_state(name), timeout=30)
    else:
        wait_for(
            f"{name} scaled to zero",
            lambda: not list_items("pods", selector=f"app={name}"),
            timeout=45,
        )


def pod_for(controller: str) -> dict[str, Any]:
    pods = list_items("pods", selector=f"app={controller}")
    running = [pod for pod in pods if pod.get("status", {}).get("phase") == "Running"]
    if len(running) != 1:
        raise RuntimeError(f"expected one Running pod for {controller}, got {len(running)}")
    return running[0]


def proxy_call(controller: str, path: str) -> dict[str, Any]:
    pod = pod_for(controller)["metadata"]["name"]
    result = kubectl(
        "-n",
        NS,
        "exec",
        pod,
        "-c",
        "scope-proxy",
        "--",
        "wget",
        "-qO-",
        f"http://127.0.0.1:8080{path}",
        timeout=30,
    )
    return json.loads(result.stdout)


def proxy_state(controller: str) -> dict[str, Any]:
    return proxy_call(controller, "/__state")


def arm(controller: str, kind: str, name: str, mode: str = "pause") -> None:
    query = f"/__arm?kind={kind}&name={name}&mode={mode}"
    proxy_call(controller, query)


def release(controller: str) -> None:
    proxy_call(controller, "/__release")


def capture_for(controller: str, kind: str, name: str, *, paused: bool = False, response: int | None = None) -> dict[str, Any] | None:
    state = proxy_state(controller)
    for item in reversed(state["captures"]):
        matches_name = name in {item.get("name"), item.get("schedule")}
        if item.get("kind") != kind or not matches_name:
            continue
        if paused and not item.get("paused"):
            continue
        if response is not None and item.get("response") != response:
            continue
        return item
    return None


def controller_log_event(
    controller: str,
    schedule: str,
    *,
    error_contains: str,
) -> dict[str, Any] | None:
    pod = pod_for(controller)["metadata"]["name"]
    result = kubectl("-n", NS, "logs", pod, "-c", "weirkeeper", "--tail=500", check=False, timeout=30)
    if result.returncode:
        raise RuntimeError(f"controller log read failed: {result.stderr[-500:]}")
    for line in reversed(result.stdout.splitlines()):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = event.get("fields") or {}
        if (
            fields.get("schedule") == schedule
            and fields.get("message") == "backup schedule reconcile failed; requeueing"
            and error_contains in str(fields.get("error") or "")
        ):
            return {
                "timestamp": event.get("timestamp"),
                "message": fields["message"],
                "error": fields["error"],
            }
    return None


def deployment(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": NS,
            "labels": {"plat04.logweir.dev/run": RUN_LABEL, "app": name},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"plat04.logweir.dev/run": RUN_LABEL, "app": name}},
                "spec": {
                    "serviceAccountName": SA,
                    "containers": [
                        {
                            "name": "weirkeeper",
                            "image": IMAGE_REF,
                            "imagePullPolicy": "Never",
                            "env": [
                                {"name": "KUBERNETES_SERVICE_HOST", "value": ""},
                                {"name": "KUBERNETES_SERVICE_PORT", "value": ""},
                                {"name": "KUBERNETES_SERVICE_PORT_HTTPS", "value": ""},
                                {"name": "KUBECONFIG", "value": "/kube/config"},
                                {"name": "RUST_LOG", "value": "weirkeeper=debug,info"},
                                {"name": "LOGWEIR_ARCHIVE_URL", "value": ""},
                                {"name": "LOGWEIR_RUNNER_IMAGE", "value": "unused-by-scheduler"},
                                {"name": "LOGWEIR_RUNNER_PULL_POLICY", "value": "Never"},
                            ],
                            "volumeMounts": [{"name": "kubeconfig", "mountPath": "/kube", "readOnly": True}],
                        },
                        {
                            "name": "scope-proxy",
                            "image": "python:3.12-alpine",
                            "imagePullPolicy": "Never",
                            "command": ["python3", "/proxy/plat04_scope_proxy.py"],
                            "env": [{"name": "PLAT04_NAMESPACE", "value": NS}],
                            "ports": [{"name": "proxy", "containerPort": 8080}],
                            "volumeMounts": [{"name": "proxy", "mountPath": "/proxy", "readOnly": True}],
                        },
                    ],
                    "volumes": [
                        {"name": "kubeconfig", "configMap": {"name": "plat04-kubeconfig"}},
                        {"name": "proxy", "configMap": {"name": "plat04-scope-proxy"}},
                    ],
                },
            },
        },
    }


def preflight() -> None:
    if SELECTION not in {"broad", "focused"}:
        raise RuntimeError(f"unknown LOGWEIR_PLAT04_SELECTION={SELECTION!r}; expected broad or focused")
    if OUT.exists():
        raise RuntimeError(f"refusing to reuse output directory {OUT}")
    OUT.mkdir(mode=0o700, parents=True)
    current_hashes: dict[str, str] = {}
    snapshot_hashes: dict[str, str] = {}
    for relative, expected in EXPECTED_HASHES.items():
        current = sha256(ROOT / relative)
        snap = sha256(SNAPSHOT / relative)
        if current != expected or snap != expected:
            raise RuntimeError(f"reviewed hash mismatch for {relative}: current={current} snapshot={snap}")
        current_hashes[relative] = current
        snapshot_hashes[relative] = snap
    inspected = json.loads(run(D + ["inspect", IMAGE_TAG]).stdout)[0]
    if inspected["Id"] != IMAGE_ID:
        raise RuntimeError(f"backend-live tag moved: {inspected['Id']} != {IMAGE_ID}")
    STATE["provenance"] = {
        "reviewedHashes": current_hashes,
        "snapshotHashes": snapshot_hashes,
        "imageTag": IMAGE_TAG,
        "imageRefUsed": IMAGE_REF,
        "imageId": inspected["Id"],
        "binarySha256": "c14accca58189ff259b074fe7fb89ab15a178795c9213a676439804e2622754a",
        "buildTransformation": "backend-live source snapshot omitted rust-toolchain.toml and used cargo vendor --offline --versioned-dirs; Rust 1.89 base",
        "validationClass": "source-matched test build, not unmodified published-image validation",
    }
    existing = json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]
    STATE["preservedControllerPodsBefore"] = [
        {
            "namespace": pod["metadata"]["namespace"],
            "name": pod["metadata"]["name"],
            "uid": pod["metadata"]["uid"],
            "images": [container["image"] for container in pod["spec"]["containers"]],
        }
        for pod in existing
        if any("weirkeeper" in container["image"] for container in pod["spec"]["containers"])
    ]
    crd = get("crd", "backupschedules.logweir.dev", namespace=None)
    (OUT / "backupschedules-crd-before.json").write_text(json.dumps(crd, indent=2) + "\n")
    STATE["sharedCrdBefore"] = {
        "uid": crd["metadata"]["uid"],
        "resourceVersion": crd["metadata"]["resourceVersion"],
        "generation": crd["metadata"]["generation"],
        "concurrencyPolicyDefault": crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        ["properties"]["spec"]["properties"].get("concurrencyPolicy", {}).get("default"),
    }
    save()


def setup() -> None:
    namespace = create(
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}},
        }
    )
    record_uid("namespace", namespace)

    policy = create(
        {
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {"name": POLICY, "labels": {"plat04.logweir.dev/run": RUN_LABEL}},
            "spec": {
                "failurePolicy": "Fail",
                "matchConstraints": {
                    "resourceRules": [
                        {
                            "apiGroups": ["logweir.dev"],
                            "apiVersions": ["v1alpha1"],
                            "operations": ["CREATE", "UPDATE", "DELETE"],
                            "resources": ["*/*"],
                        },
                        {
                            "apiGroups": ["batch"],
                            "apiVersions": ["v1"],
                            "operations": ["CREATE", "UPDATE", "DELETE"],
                            "resources": ["jobs", "jobs/*"],
                        },
                        {
                            "apiGroups": [""],
                            "apiVersions": ["v1"],
                            "operations": ["CREATE", "UPDATE", "DELETE"],
                            "resources": ["configmaps"],
                        },
                    ]
                },
                "validations": [
                    {
                        "expression": f"request.userInfo.username != '{SHARED_CONTROLLER_USER}'",
                        "message": "PLAT04 owned namespace rejects the preserved shared controller",
                    }
                ],
            },
        }
    )
    record_uid("validatingAdmissionPolicy", policy)
    binding = create(
        {
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {"name": BINDING, "labels": {"plat04.logweir.dev/run": RUN_LABEL}},
            "spec": {
                "policyName": POLICY,
                "validationActions": ["Deny"],
                "matchResources": {"namespaceSelector": {"matchLabels": {"plat04.logweir.dev/run": RUN_LABEL}}},
            },
        }
    )
    record_uid("validatingAdmissionPolicyBinding", binding)
    time.sleep(2)
    denied = kubectl(
        "--as",
        SHARED_CONTROLLER_USER,
        "-n",
        NS,
        "create",
        "configmap",
        "plat04-admission-probe",
        "--from-literal=x=y",
        check=False,
    )
    if denied.returncode == 0 or "PLAT04 owned namespace" not in denied.stderr:
        raise RuntimeError(f"admission isolation negative control did not deny for intended reason: {denied.stderr}")
    reason_match = re.search(r"Error from server \(([^)]+)\)", denied.stderr)
    STATE["apiOutcomes"].append(
        {
            "case": "shared-controller-isolation",
            "kubectlReturnCode": denied.returncode,
            "observedReason": reason_match.group(1) if reason_match else None,
            "stderr": denied.stderr.strip(),
            "messageMatched": True,
        }
    )
    save()

    if SELECTION == "focused":
        setup_controllers()
        return

    old = create(schedule_obj("old-omitted-policy", suspend=True, omit_policy=True))
    record_uid("schedule/old-omitted-policy", old)
    stored_old = get("backupschedule", "old-omitted-policy")
    prior_upgrade = pathlib.Path("/tmp/logweir-plat04-live-20260915t0419z")
    if STATE["sharedCrdBefore"]["concurrencyPolicyDefault"] is None:
        if "concurrencyPolicy" in stored_old["spec"]:
            raise RuntimeError("old-schema fixture unexpectedly stored concurrencyPolicy before upgrade")
        (OUT / "old-omitted-before-upgrade.json").write_text(json.dumps(stored_old, indent=2) + "\n")
        applied = run(K + ["apply", "-f", str(ROOT / "config/crd/backupschedules.yaml"), "-o", "json"], timeout=90)
        upgraded_crd = json.loads(applied.stdout)
        if upgraded_crd["metadata"]["uid"] != STATE["sharedCrdBefore"]["uid"]:
            raise RuntimeError("additive CRD apply replaced the shared CRD UID")
        kubectl("wait", "--for=condition=Established", "crd/backupschedules.logweir.dev", "--timeout=60s")
        upgrade_evidence: dict[str, Any] = {"performedThisRun": True}
    else:
        if stored_old["spec"].get("concurrencyPolicy") != "Forbid":
            raise RuntimeError("current CRD did not default an omitted concurrencyPolicy")
        prior_report = json.loads((prior_upgrade / "report.json").read_text())
        prior_before = json.loads((prior_upgrade / "old-omitted-before-upgrade.json").read_text())
        prior_after = json.loads((prior_upgrade / "old-omitted-after-upgrade.json").read_text())
        if (
            prior_report["cases"].get("old_omitted_policy_crd_upgrade_cel_suspend") != "passed"
            or "concurrencyPolicy" in prior_before["spec"]
            or prior_after["spec"].get("concurrencyPolicy") != "Forbid"
            or prior_after["spec"].get("suspend") is not True
            or prior_report["sharedCrdBefore"]["uid"] != STATE["sharedCrdBefore"]["uid"]
            or prior_report["sharedCrdAfter"]["generation"] != STATE["sharedCrdBefore"]["generation"]
        ):
            raise RuntimeError("preserved old-schema upgrade evidence is incomplete or for another CRD")
        upgraded_crd = get("crd", "backupschedules.logweir.dev", namespace=None)
        upgrade_evidence = {
            "performedThisRun": False,
            "reason": "shared CRD was already upgraded by the earlier nonzero run; no downgrade performed",
            "priorRun": str(prior_upgrade),
            "priorBeforeSha256": sha256(prior_upgrade / "old-omitted-before-upgrade.json"),
            "priorAfterSha256": sha256(prior_upgrade / "old-omitted-after-upgrade.json"),
            "priorReportSha256": sha256(prior_upgrade / "report.json"),
            "priorGenerationBefore": prior_report["sharedCrdBefore"]["generation"],
            "priorGenerationAfter": prior_report["sharedCrdAfter"]["generation"],
        }
    after = get("backupschedule", "old-omitted-policy")
    if after["spec"].get("concurrencyPolicy") != "Forbid":
        raise RuntimeError(f"CRD default was not applied to old object read: {after['spec']}")
    patch("backupschedule", "old-omitted-policy", {"spec": {"suspend": False}})
    updated = patch("backupschedule", "old-omitted-policy", {"spec": {"suspend": True}})
    if updated["spec"].get("suspend") is not True or updated["spec"].get("concurrencyPolicy") != "Forbid":
        raise RuntimeError("post-upgrade suspend/default update did not persist")
    STATE["cases"]["old_omitted_policy_crd_upgrade_cel_suspend"] = {
        "status": "passed",
        **upgrade_evidence,
        "currentOmittedCreateDefault": after["spec"]["concurrencyPolicy"],
        "currentSuspendUpdate": updated["spec"]["suspend"],
    }
    STATE["sharedCrdAfter"] = {
        "uid": upgraded_crd["metadata"]["uid"],
        "resourceVersion": upgraded_crd["metadata"]["resourceVersion"],
        "generation": upgraded_crd["metadata"]["generation"],
    }
    (OUT / "old-omitted-after-upgrade.json").write_text(json.dumps(updated, indent=2) + "\n")
    save()

    setup_controllers()


def setup_controllers() -> None:
    proxy_source = (ROOT / "scripts/fixtures/plat04_scope_proxy.py").read_text()
    kubeconfig = f"""apiVersion: v1
kind: Config
clusters:
- name: scoped-real-api
  cluster:
    server: http://127.0.0.1:8080
contexts:
- name: scoped
  context:
    cluster: scoped-real-api
    user: anonymous-to-local-proxy
    namespace: {NS}
current-context: scoped
users:
- name: anonymous-to-local-proxy
  user: {{}}
"""
    manifests = [
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": SA, "namespace": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}}},
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": SA, "namespace": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}},
            "rules": [
                {"apiGroups": ["logweir.dev"], "resources": ["approvals", "backups", "backupschedules", "kafkaclusters", "restores"], "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["logweir.dev"], "resources": ["backups"], "verbs": ["create"]},
                {"apiGroups": ["logweir.dev"], "resources": ["backupschedules/status"], "verbs": ["get", "patch", "update"]},
                {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch"]},
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": SA, "namespace": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": SA},
            "subjects": [{"kind": "ServiceAccount", "name": SA, "namespace": NS}],
        },
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "plat04-scope-proxy", "namespace": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}}, "data": {"plat04_scope_proxy.py": proxy_source}},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "plat04-kubeconfig", "namespace": NS, "labels": {"plat04.logweir.dev/run": RUN_LABEL}}, "data": {"config": kubeconfig}},
    ]
    for manifest in manifests:
        obj = create(manifest)
        record_uid(f"{obj['kind']}/{obj['metadata']['name']}", obj)
    for name in CONTROLLERS:
        obj = create(deployment(name))
        record_uid(f"Deployment/{name}", obj)
    for name in CONTROLLERS:
        kubectl("-n", NS, "rollout", "status", f"deployment/{name}", "--timeout=120s", timeout=130)
        pod = pod_for(name)
        record_uid(f"Pod/{name}/initial", pod)
        status = pod["status"]["containerStatuses"]
        controller = next(item for item in status if item["name"] == "weirkeeper")
        if controller.get("imageID") != f"docker-pullable://{IMAGE_REF}":
            raise RuntimeError(f"controller pod imageID mismatch: {controller.get('imageID')}")
        proxy_state(name)
    readiness = {
        "status": "passed",
        "replicas": list(CONTROLLERS),
        "imageRef": IMAGE_REF,
    }
    if SELECTION == "broad":
        STATE["cases"]["two_isolated_real_controller_replicas_ready"] = "passed"
    else:
        STATE["infrastructure"] = {"twoIsolatedRealControllerReplicasReady": readiness}
    save()


def wait_next_minute(slot: str | None = None) -> None:
    if slot:
        due = dt.datetime.strptime(slot, "%Y%m%d-%H%M%S").replace(tzinfo=dt.timezone.utc)
        target = due + dt.timedelta(minutes=1, seconds=2)
    else:
        current = dt.datetime.now(dt.timezone.utc)
        target = current.replace(second=0, microsecond=0) + dt.timedelta(minutes=1, seconds=2)
    delay = max(0.0, (target - dt.datetime.now(dt.timezone.utc)).total_seconds())
    note(f"bounded wait {delay:.1f}s for a distinct cron slot")
    time.sleep(delay)


def create_holding_job(backup: dict[str, Any]) -> dict[str, Any]:
    name = "hold-" + backup["metadata"]["name"][-45:]
    return create(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": NS,
                "labels": {"plat04.logweir.dev/run": RUN_LABEL, "plat04.logweir.dev/fixture": "holding-job"},
                "ownerReferences": [
                    {
                        "apiVersion": "logweir.dev/v1alpha1",
                        "kind": "Backup",
                        "name": backup["metadata"]["name"],
                        "uid": backup["metadata"]["uid"],
                        "controller": True,
                        "blockOwnerDeletion": True,
                    }
                ],
            },
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": {"plat04.logweir.dev/run": RUN_LABEL, "job-name": name}},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "holder",
                                "image": "busybox:latest",
                                "imagePullPolicy": "Never",
                                "command": ["sh", "-c", "sleep 100"],
                            }
                        ],
                    },
                },
            },
        }
    )


def overlap_cases() -> None:
    forbid = create(schedule_obj("forbid-span"))
    allow = create(schedule_obj("allow-overlap", policy="Allow"))
    record_uid("schedule/forbid-span", forbid)
    record_uid("schedule/allow-overlap", allow)
    forbid_first = wait_for(
        "initial Forbid child",
        lambda: (items := owned_backups(get("backupschedule", "forbid-span"))) and items[0],
        timeout=50,
    )
    allow_first = wait_for(
        "initial Allow child",
        lambda: (items := owned_backups(get("backupschedule", "allow-overlap"))) and items[0],
        timeout=50,
    )
    record_uid("backup/forbid-span/first", forbid_first)
    record_uid("backup/allow-overlap/first", allow_first)
    holding = create_holding_job(forbid_first)
    record_uid("job/forbid-span-holder", holding)
    wait_for(
        "holding Job Active",
        lambda: when(
            get("job", holding["metadata"]["name"]),
            lambda job: (job.get("status") or {}).get("active") == 1,
        ),
        timeout=35,
    )
    slot = forbid_first["spec"]["slot"]
    wait_next_minute(slot)
    for name in ("forbid-span", "allow-overlap"):
        patch("backupschedule", name, {"spec": {"suspend": True}})
        wait_for(f"{name} suspended condition", lambda n=name: ready_reason(get("backupschedule", n)) == "Suspended", timeout=30)
        patch("backupschedule", name, {"spec": {"suspend": False}})
    blocked = wait_for(
        "ConcurrencyBlocked",
        lambda: when(
            get("backupschedule", "forbid-span"),
            lambda obj: ready_reason(obj) == "ConcurrencyBlocked",
        ),
        timeout=45,
    )
    forbid_children = owned_backups(blocked)
    if len(forbid_children) != 1 or forbid_children[0]["metadata"]["uid"] != forbid_first["metadata"]["uid"]:
        raise RuntimeError("Forbid created or adopted a second child across the due slot")
    if (forbid_children[0].get("status") or {}).get("phase") in {"Succeeded", "Failed", "Refused"}:
        raise RuntimeError("Forbid fixture was unexpectedly terminal")
    job = get("job", holding["metadata"]["name"])
    if (job.get("status") or {}).get("active") != 1:
        raise RuntimeError(f"holding Job was not Active across due slot: {job.get('status')}")
    allow_children = wait_for(
        "second Allow child",
        lambda: when(
            owned_backups(get("backupschedule", "allow-overlap")),
            lambda items: len(items) >= 2,
        ),
        timeout=45,
    )
    if len({item["metadata"]["uid"] for item in allow_children}) != 2:
        raise RuntimeError("Allow overlap did not produce exactly two distinct children")
    if any((item.get("status") or {}).get("phase") in {"Succeeded", "Failed", "Refused"} for item in allow_children):
        raise RuntimeError("Allow overlap children did not remain nonterminal")
    STATE["cases"]["forbid_two_replicas_nonterminal_next_slot"] = {
        "status": "passed",
        "scheduleUid": blocked["metadata"]["uid"],
        "childUid": forbid_first["metadata"]["uid"],
        "childCount": 1,
        "reason": ready_reason(blocked),
        "lastMissedSlot": blocked["status"]["lastMissedSlot"],
        "jobUid": job["metadata"]["uid"],
        "jobState": "Active",
    }
    STATE["cases"]["allow_explicit_overlap"] = {
        "status": "passed",
        "scheduleUid": allow["metadata"]["uid"],
        "childUids": [item["metadata"]["uid"] for item in allow_children],
        "childCount": 2,
    }
    save()


def different_slot_rv_race() -> None:
    scale(CONTROLLERS[1], 0)
    race = create(schedule_obj("rv-different-slots", suspend=True))
    record_uid("schedule/rv-different-slots", race)
    current = dt.datetime.now(dt.timezone.utc)
    if current.second > 42:
        wait_next_minute()
    arm(CONTROLLERS[0], "reservation", "rv-different-slots")
    patch("backupschedule", "rv-different-slots", {"spec": {"suspend": False}})
    old_request = wait_for(
        "A paused old-slot reservation",
        lambda: capture_for(CONTROLLERS[0], "reservation", "rv-different-slots", paused=True),
        timeout=25,
    )
    old_name = old_request["body"]["status"]["pendingBackupRef"]["name"]
    old_slot = old_name.rsplit("-", 2)[-2] + "-" + old_name.rsplit("-", 1)[-1]
    wait_next_minute(old_slot)
    scale(CONTROLLERS[1], 1)
    winner_schedule = wait_for(
        "B new-slot final status",
        lambda: when(
            get("backupschedule", "rv-different-slots"),
            lambda obj: bool((obj.get("status") or {}).get("activeBackupRef")),
        ),
        timeout=50,
    )
    winner_name = winner_schedule["status"]["activeBackupRef"]["name"]
    if winner_name == old_name:
        raise RuntimeError("race did not use different cron slots")
    release(CONTROLLERS[0])
    stale = wait_for(
        "A reservation 409",
        lambda: capture_for(CONTROLLERS[0], "reservation", "rv-different-slots", response=409),
        timeout=25,
    )
    children = owned_backups(get("backupschedule", "rv-different-slots"))
    if len(children) != 1 or children[0]["metadata"]["name"] != winner_name:
        raise RuntimeError("different-slot race did not leave exactly the API winner")
    STATE["cases"]["different_slot_real_resource_version_exclusion"] = {
        "status": "passed",
        "oldRequestedChild": old_name,
        "winningChild": winner_name,
        "winningChildUid": children[0]["metadata"]["uid"],
        "staleApiCode": stale["response"],
        "harness": "controller A reservation PUT paused before forwarding; controller B admitted the next minute",
    }
    save()
    capture_controller_artifacts(CONTROLLERS[0], "different-slot-race")
    capture_controller_artifacts(CONTROLLERS[1], "different-slot-race")
    scale(CONTROLLERS[1], 0)


def restart_before_create() -> None:
    name = "restart-before-create"
    arm(CONTROLLERS[0], "backup_create", name)
    schedule = create(schedule_obj(name))
    record_uid(f"schedule/{name}", schedule)
    request = wait_for(
        "Backup POST paused before create",
        lambda: capture_for(CONTROLLERS[0], "backup_create", name, paused=True),
        timeout=45,
    )
    pending = get("backupschedule", name)
    pending_name = pending["status"]["pendingBackupRef"]["name"]
    if request["name"] != pending_name or list_items("backups.logweir.dev", selector=f"logweir.dev/schedule={name}"):
        raise RuntimeError("pre-create crash barrier did not preserve a childless reservation")
    old_pod = pod_for(CONTROLLERS[0])["metadata"]["uid"]
    scale(CONTROLLERS[0], 0)
    scale(CONTROLLERS[0], 1)
    final = wait_for(
        "reservation resumed after restart",
        lambda: when(
            get("backupschedule", name),
            lambda obj: (obj.get("status") or {}).get("activeBackupRef", {}).get("name") == pending_name
            and not (obj.get("status") or {}).get("pendingBackupRef"),
        ),
        timeout=50,
    )
    children = owned_backups(final)
    new_pod = pod_for(CONTROLLERS[0])["metadata"]["uid"]
    if len(children) != 1 or children[0]["metadata"]["name"] != pending_name or new_pod == old_pod:
        raise RuntimeError("pre-create restart lost or duplicated the accepted reservation")
    STATE["cases"]["restart_after_reservation_before_child"] = {
        "status": "passed",
        "oldControllerPodUid": old_pod,
        "newControllerPodUid": new_pod,
        "reservation": pending_name,
        "childUid": children[0]["metadata"]["uid"],
        "childCount": 1,
    }
    save()


def horizon_resume() -> None:
    scale(CONTROLLERS[0], 0)
    name = "resume-beyond-horizon"
    schedule = create(schedule_obj(name, suspend=True))
    record_uid(f"schedule/{name}", schedule)
    old_due = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2, minutes=5)).replace(second=0, microsecond=0)
    slot = old_due.strftime("%Y%m%d-%H%M%S")
    pending_name = f"logweir-backup-{name}-{slot}"
    seeded = patch(
        "backupschedule",
        name,
        {
            "status": {
                "pendingBackupRef": {"name": pending_name},
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True",
                        "observedGeneration": schedule["metadata"]["generation"],
                        "reason": "Scheduled",
                        "message": "harness-seeded accepted deterministic reservation beyond horizon",
                    }
                ],
            }
        },
        subresource="status",
    )
    STATE["uids"][f"reservation/{name}/resourceVersion"] = seeded["metadata"]["resourceVersion"]
    save()
    scale(CONTROLLERS[0], 1)
    final = wait_for(
        "beyond-horizon pending reservation resumed on restart",
        lambda: when(
            get("backupschedule", name),
            lambda obj: (obj.get("status") or {}).get("activeBackupRef", {}).get("name") == pending_name
            and not (obj.get("status") or {}).get("pendingBackupRef"),
        ),
        timeout=50,
    )
    children = owned_backups(final)
    if len(children) != 1 or children[0]["spec"].get("slot") != slot:
        raise RuntimeError("beyond-horizon accepted-format reservation was lost or renamed")
    STATE["cases"]["restart_resume_accepted_shape_beyond_horizon"] = {
        "status": "passed",
        "harnessDriven": True,
        "ageSeconds": int((dt.datetime.now(dt.timezone.utc) - old_due).total_seconds()),
        "reservation": pending_name,
        "childUid": children[0]["metadata"]["uid"],
        "scheduleRemainedSuspended": final["spec"]["suspend"],
    }
    save()


def restart_after_create_before_final() -> None:
    name = "restart-after-create"
    arm(CONTROLLERS[0], "schedule_final", name)
    schedule = create(schedule_obj(name))
    record_uid(f"schedule/{name}", schedule)
    final_request = wait_for(
        "final status PATCH paused after child create",
        lambda: capture_for(CONTROLLERS[0], "schedule_final", name, paused=True),
        timeout=45,
    )
    pending = get("backupschedule", name)
    pending_name = pending["status"]["pendingBackupRef"]["name"]
    child = get("backup", pending_name)
    child_uid = child["metadata"]["uid"]
    if final_request["body"]["metadata"]["resourceVersion"] != pending["metadata"]["resourceVersion"]:
        raise RuntimeError("paused finalizer was not based on reservation response resourceVersion")
    old_pod = pod_for(CONTROLLERS[0])["metadata"]["uid"]
    STATE["capturesBeforeRestartAfterCreate"] = final_request
    save()
    scale(CONTROLLERS[0], 0)
    scale(CONTROLLERS[0], 1)
    final = wait_for(
        "post-create reservation finalized after restart",
        lambda: when(
            get("backupschedule", name),
            lambda obj: (obj.get("status") or {}).get("activeBackupRef", {}).get("name") == pending_name
            and not (obj.get("status") or {}).get("pendingBackupRef"),
        ),
        timeout=50,
    )
    children = owned_backups(final)
    new_pod = pod_for(CONTROLLERS[0])["metadata"]["uid"]
    if len(children) != 1 or children[0]["metadata"]["uid"] != child_uid or new_pod == old_pod:
        raise RuntimeError("post-create restart lost or duplicated the child")
    STATE["cases"]["restart_after_child_before_final_status"] = {
        "status": "passed",
        "oldControllerPodUid": old_pod,
        "newControllerPodUid": new_pod,
        "childName": pending_name,
        "childUid": child_uid,
        "childCount": 1,
    }
    save()


def stale_finalizer_conflict() -> None:
    name = "stale-finalizer"
    arm(CONTROLLERS[0], "schedule_final", name)
    schedule = create(schedule_obj(name))
    record_uid(f"schedule/{name}", schedule)
    request = wait_for(
        "stale finalizer paused",
        lambda: capture_for(CONTROLLERS[0], "schedule_final", name, paused=True),
        timeout=45,
    )
    current = get("backupschedule", name)
    old_rv = request["body"]["metadata"]["resourceVersion"]
    if current["metadata"]["resourceVersion"] != old_rv:
        raise RuntimeError("finalizer was not paused against current reservation RV")
    newer_due = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1)).replace(second=0, microsecond=0)
    newer_name = f"logweir-backup-{name}-{newer_due.strftime('%Y%m%d-%H%M%S')}"
    newer = patch(
        "backupschedule",
        name,
        {
            "status": {
                "pendingBackupRef": {"name": newer_name},
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True",
                        "observedGeneration": current["metadata"]["generation"],
                        "reason": "Scheduled",
                        "message": "controlled newer reservation B",
                    }
                ],
            }
        },
        subresource="status",
    )
    if newer["metadata"]["resourceVersion"] == old_rv:
        raise RuntimeError("newer reservation did not advance resourceVersion")
    release(CONTROLLERS[0])
    outcome = wait_for(
        "actual stale controller final PATCH 409",
        lambda: capture_for(CONTROLLERS[0], "schedule_final", name, response=409),
        timeout=25,
    )
    preserved = get("backupschedule", name)
    if (preserved.get("status") or {}).get("pendingBackupRef", {}).get("name") != newer_name:
        raise RuntimeError("stale finalizer cleared the newer reservation")
    if ready_reason(preserved) != "Scheduled" or preserved["status"]["conditions"][0].get("message") != "controlled newer reservation B":
        raise RuntimeError("stale finalizer overwrote newer scheduling status")
    capture_controller_artifacts(CONTROLLERS[0], "stale-finalizer-before-restart")
    status_path = OUT / "stale-finalizer-resulting-schedule-before-restart.json"
    status_path.write_text(json.dumps(preserved, indent=2, sort_keys=True) + "\n")
    capture_path = OUT / f"{CONTROLLERS[0]}-stale-finalizer-before-restart-proxy-state.json"
    STATE["cases"]["stale_finalizer_real_merge_patch_409"] = {
        "status": "passed",
        "harnessDrivenInterleaving": True,
        "actualControllerPatchBodySha256": outcome["bodySha256"],
        "staleResourceVersion": old_rv,
        "newResourceVersion": newer["metadata"]["resourceVersion"],
        "apiCode": 409,
        "preservedPending": newer_name,
        "preservedMessage": "controlled newer reservation B",
        "captureBeforeProxyRestart": str(capture_path),
        "captureBeforeProxyRestartSha256": sha256(capture_path),
        "resultingStateBeforeProxyRestart": str(status_path),
        "resultingStateBeforeProxyRestartSha256": sha256(status_path),
    }
    save()
    scale(CONTROLLERS[0], 0)
    scale(CONTROLLERS[0], 1)


def collision_case(case: str, mutation: str) -> dict[str, Any]:
    name = f"winner-{case}"
    if mutation == "transient404":
        arm(CONTROLLERS[0], "backup_create", name, mode="conflict404")
        schedule = create(schedule_obj(name, policy="Allow"))
        record_uid(f"schedule/{name}", schedule)
        post = wait_for(
            f"{case} synthetic POST 409",
            lambda: capture_for(CONTROLLERS[0], "backup_create", name, response=409),
            timeout=40,
        )
        got = wait_for(
            f"{case} synthetic GET 404",
            lambda: next(
                (
                    item
                    for item in reversed(proxy_state(CONTROLLERS[0])["captures"])
                    if item.get("kind") == "backup_get" and item.get("name") == post["name"] and item.get("response") == 404
                ),
                None,
            ),
            timeout=20,
        )
        requeue = wait_for(
            f"{case} controller-observable 404 requeue",
            lambda: controller_log_event(CONTROLLERS[0], name, error_contains="not found: NotFound"),
            timeout=25,
        )
        current = get("backupschedule", name)
        children = owned_backups(current)
        if children or (current.get("status") or {}).get("lastFireTime"):
            raise RuntimeError("409->404 was not conservatively non-successful when the requeue was observed")
        suspended = suspend_at_observed_resource_version(current)
        return {
            "status": "passed",
            "injectedPostCode": post["response"],
            "injectedGetCode": got["response"],
            "requeueObservedBeforeSuspension": requeue,
            "claim": "observed-conservative-non-success-only",
            "retryObserved": False,
            "reportedScheduledAtObservation": False,
            "childCountAtObservation": len(children),
            "observedResourceVersion": current["metadata"]["resourceVersion"],
            "suspensionResourceVersion": suspended["metadata"]["resourceVersion"],
            "lateWriteCouldFalsePass": False,
        }

    arm(CONTROLLERS[0], "backup_create", name)
    schedule = create(schedule_obj(name, policy="Allow"))
    record_uid(f"schedule/{name}", schedule)
    post = wait_for(
        f"{case} Backup POST pause",
        lambda: capture_for(CONTROLLERS[0], "backup_create", name, paused=True),
        timeout=40,
    )
    winner = copy.deepcopy(post["body"])
    if mutation == "ownerless":
        winner["metadata"].pop("ownerReferences", None)
    elif mutation == "olduid":
        winner["metadata"]["ownerReferences"][0]["uid"] = "00000000-0000-4000-8000-000000000004"
        # Kubernetes GC otherwise removes a dangling old-UID owner reference
        # before the paused controller POST is released.  This test-only
        # finalizer keeps that exact API object available for the real 409/GET
        # ownership check; teardown removes it by recorded UID.
        winner["metadata"]["finalizers"] = ["plat04.logweir.dev/hold-old-uid-winner"]
        winner["metadata"].setdefault("labels", {})["plat04.logweir.dev/run"] = RUN_LABEL
    elif mutation == "foreign":
        foreign_name = "foreign-owner-schedule"
        try:
            foreign = get("backupschedule", foreign_name)
        except RuntimeError:
            foreign = create(schedule_obj(foreign_name, suspend=True))
            record_uid(f"schedule/{foreign_name}", foreign)
        owner = winner["metadata"]["ownerReferences"][0]
        owner["name"] = foreign_name
        owner["uid"] = foreign["metadata"]["uid"]
    elif mutation != "owned":
        raise RuntimeError(f"unknown collision mutation {mutation}")
    created = create(winner)
    record_uid(f"backup/{name}/winner", created)
    stored_winner = get("backup", created["metadata"]["name"])
    if stored_winner["metadata"]["uid"] != created["metadata"]["uid"]:
        raise RuntimeError(f"{mutation} collision object changed before controller release")
    release(CONTROLLERS[0])
    api_post = wait_for(
        f"{case} real POST 409",
        lambda: capture_for(CONTROLLERS[0], "backup_create", name, response=409),
        timeout=20,
    )
    api_get = wait_for(
        f"{case} real winner GET 200",
        lambda: next(
            (
                item
                for item in reversed(proxy_state(CONTROLLERS[0])["captures"])
                if item.get("kind") == "backup_get" and item.get("name") == created["metadata"]["name"] and item.get("response") == 200
            ),
            None,
        ),
        timeout=20,
    )
    if mutation == "owned":
        current = wait_for(
            "owned collision final success",
            lambda: when(
                get("backupschedule", name),
                lambda obj: bool((obj.get("status") or {}).get("lastFireTime")),
            ),
            timeout=20,
        )
        success = True
        rejection = None
    else:
        rejection = wait_for(
            f"{case} controller-observable ownership rejection",
            lambda: controller_log_event(
                CONTROLLERS[0],
                name,
                error_contains="does not carry the complete current BackupSchedule controller identity",
            ),
            timeout=25,
        )
        current = get("backupschedule", name)
        success = bool((current.get("status") or {}).get("lastFireTime"))
        if success:
            raise RuntimeError(f"{mutation} winner was falsely accepted after observed rejection")
    if mutation == "owned":
        patch("backupschedule", name, {"spec": {"suspend": True}})
        suspension_rv = None
    else:
        suspended = suspend_at_observed_resource_version(current)
        suspension_rv = suspended["metadata"]["resourceVersion"]
    return {
        "status": "passed",
        "postCode": api_post["response"],
        "getCode": api_get["response"],
        "winnerUid": created["metadata"]["uid"],
        "reportedScheduled": success,
        "ownerMode": mutation,
        "rejectionObservedBeforeSuspension": rejection,
        "observedResourceVersion": current["metadata"]["resourceVersion"] if mutation != "owned" else None,
        "suspensionResourceVersion": suspension_rv,
        "lateWriteCouldFalsePass": False if mutation != "owned" else None,
    }


def winner_matrix() -> None:
    results = {}
    for case, mutation in (
        ("owned", "owned"),
        ("foreign", "foreign"),
        ("ownerless", "ownerless"),
        ("olduid", "olduid"),
        ("transient404", "transient404"),
    ):
        results[case] = collision_case(case, mutation)
    STATE["cases"]["allow_409_winner_matrix"] = results
    save()


def safe_replacement() -> None:
    old = patch("backupschedule", "forbid-span", {"spec": {"suspend": True}})
    wait_for("old schedule suspended", lambda: ready_reason(get("backupschedule", "forbid-span")) == "Suspended", timeout=35)
    old_uid = old["metadata"]["uid"]
    children = owned_backups(old)
    if not children:
        raise RuntimeError("safe replacement has no retained old history")
    holding_name = get("job", next(item for item in list_items("jobs", selector="plat04.logweir.dev/fixture=holding-job"))["metadata"]["name"])["metadata"]["name"]
    completed = wait_for(
        "holding Job Complete before drain",
        lambda: when(
            get("job", holding_name),
            lambda job: (job.get("status") or {}).get("succeeded") == 1,
        ),
        timeout=150,
        interval=2,
    )
    for child in children:
        patch("backup", child["metadata"]["name"], {"status": {"phase": "Succeeded"}}, subresource="status")
    drained = wait_for(
        "all old UID-owned children terminal",
        lambda: when(
            owned_backups(get("backupschedule", "forbid-span")),
            lambda items: bool(items)
            and all((item.get("status") or {}).get("phase") in {"Succeeded", "Failed", "Refused"} for item in items),
        ),
        timeout=30,
    )
    replacement = create(schedule_obj("forbid-span-v2", policy="Allow", suspend=True))
    record_uid("schedule/forbid-span-v2", replacement)
    retained = get("backupschedule", "forbid-span")
    retained_children = owned_backups(retained)
    if retained["metadata"]["uid"] != old_uid or retained["spec"]["suspend"] is not True:
        raise RuntimeError("old schedule was not retained suspended")
    if [item["metadata"]["uid"] for item in retained_children] != [item["metadata"]["uid"] for item in drained]:
        raise RuntimeError("old Backup history was deleted or replaced")
    if replacement["metadata"]["name"] == retained["metadata"]["name"]:
        raise RuntimeError("replacement reused old schedule name")
    STATE["cases"]["safe_replacement_after_drain"] = {
        "status": "passed",
        "oldScheduleName": retained["metadata"]["name"],
        "oldScheduleUid": retained["metadata"]["uid"],
        "oldRetained": True,
        "oldSuspended": True,
        "historyChildUids": [item["metadata"]["uid"] for item in retained_children],
        "historyPhases": [(item.get("status") or {}).get("phase") for item in retained_children],
        "holdingJobUid": completed["metadata"]["uid"],
        "holdingJobState": "Complete",
        "replacementName": replacement["metadata"]["name"],
        "replacementUid": replacement["metadata"]["uid"],
        "replacementPolicy": replacement["spec"]["concurrencyPolicy"],
        "deletionUsedForMigration": False,
    }
    save()


def collect() -> None:
    for controller in CONTROLLERS:
        try:
            pod = pod_for(controller)["metadata"]["name"]
            for container in ("weirkeeper", "scope-proxy"):
                result = kubectl("-n", NS, "logs", pod, "-c", container, "--tail=500", check=False, timeout=30)
                (OUT / f"{controller}-{container}.log").write_text(result.stdout + result.stderr)
            try:
                (OUT / f"{controller}-proxy-state.json").write_text(json.dumps(proxy_state(controller), indent=2) + "\n")
            except Exception as error:  # collection must not hide primary failure
                (OUT / f"{controller}-proxy-state.error.txt").write_text(repr(error) + "\n")
        except Exception as error:
            (OUT / f"{controller}-collection.error.txt").write_text(repr(error) + "\n")
    for kinds, filename in (
        ("backupschedules.logweir.dev,backups.logweir.dev,jobs", "owned-final-resources.json"),
        ("deployments,pods,serviceaccounts,roles,rolebindings", "owned-final-infra.json"),
    ):
        result = kubectl("-n", NS, "get", kinds, "-o", "json", check=False, timeout=45)
        (OUT / filename).write_text(result.stdout if result.returncode == 0 else result.stderr)
    after = json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]
    STATE["preservedControllerPodsAfterTests"] = [
        {"namespace": p["metadata"]["namespace"], "name": p["metadata"]["name"], "uid": p["metadata"]["uid"]}
        for p in after
        if p["metadata"].get("namespace") in {"logweir-scram-local", "logweir-backend-live-20260915"}
        and any("weirkeeper" in c["image"] for c in p["spec"]["containers"])
    ]
    save()


def capture_controller_artifacts(controller: str, suffix: str) -> None:
    pod = pod_for(controller)["metadata"]["name"]
    for container in ("weirkeeper", "scope-proxy"):
        result = kubectl("-n", NS, "logs", pod, "-c", container, "--tail=300", check=False, timeout=30)
        (OUT / f"{controller}-{suffix}-{container}.log").write_text(result.stdout + result.stderr)
    (OUT / f"{controller}-{suffix}-proxy-state.json").write_text(
        json.dumps(proxy_state(controller), indent=2) + "\n"
    )


def delete_checked(kind: str, name: str, uid_key: str, *, namespace: str | None = None) -> None:
    expected = STATE["uids"].get(uid_key)
    if not expected:
        STATE["cleanup"].setdefault("notCreated", []).append(f"{kind}/{name}")
        return
    args = []
    if namespace:
        args += ["-n", namespace]
    probe = kubectl(*(args + ["get", kind, name, "-o", "json"]), check=False)
    if probe.returncode:
        if "NotFound" in probe.stderr:
            STATE["cleanup"]["errors"].append(f"{kind}/{name}: unexpectedly NotFound before cleanup")
            return
        STATE["cleanup"]["errors"].append(f"{kind}/{name}: get failed: {probe.stderr[-500:]}")
        return
    obj = json.loads(probe.stdout)
    if obj["metadata"]["uid"] != expected:
        STATE["cleanup"]["errors"].append(f"{kind}/{name}: UID changed; refused delete")
        return
    deleted = kubectl(*(args + ["delete", kind, name, "--wait=false"]), check=False)
    if deleted.returncode:
        STATE["cleanup"]["errors"].append(f"{kind}/{name}: delete failed: {deleted.stderr[-500:]}")


def poll_exact_not_found(
    kind: str,
    name: str,
    uid_key: str,
    *,
    namespace: str | None = None,
    timeout: int = 45,
) -> None:
    args = ["-n", namespace] if namespace else []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        probe = kubectl(*(args + ["get", kind, name, "-o", "json"]), check=False)
        if probe.returncode == 0:
            time.sleep(1)
            continue
        stderr = probe.stderr.strip()
        if "Error from server (NotFound):" in stderr and f'"{name}" not found' in stderr:
            STATE["cleanup"].setdefault("notFound", []).append(
                {
                    "kind": kind,
                    "name": name,
                    "expectedUid": STATE["uids"].get(uid_key),
                    "kubectlReturnCode": probe.returncode,
                    "observed": stderr,
                }
            )
            save()
            return
        raise RuntimeError(f"{kind}/{name}: NotFound poll failed with a different API error: {stderr[-500:]}")
    raise RuntimeError(f"{kind}/{name}: not deleted within {timeout}s")


def cleanup() -> None:
    STATE["cleanup"]["status"] = "running"
    save()
    old_uid_key = "backup/winner-olduid/winner"
    old_uid = STATE["uids"].get(old_uid_key)
    if old_uid:
        probe = kubectl(
            "-n", NS, "get", "backups.logweir.dev",
            "-l", f"plat04.logweir.dev/run={RUN_LABEL}", "-o", "json", check=False
        )
        if probe.returncode == 0:
            matches = [item for item in json.loads(probe.stdout)["items"] if item["metadata"]["uid"] == old_uid]
            if len(matches) == 1:
                result = kubectl(
                    "-n", NS, "patch", "backup", matches[0]["metadata"]["name"],
                    "--type=merge", "-p", '{"metadata":{"finalizers":null}}', check=False
                )
                if result.returncode:
                    STATE["cleanup"]["errors"].append(
                        f"old-UID winner finalizer removal failed: {result.stderr[-500:]}"
                    )
                else:
                    STATE["cleanup"]["oldUidWinnerFinalizer"] = "removed-by-recorded-uid"
            elif len(matches) == 0:
                STATE["cleanup"]["errors"].append("old-UID winner unexpectedly NotFound before finalizer cleanup")
            else:
                STATE["cleanup"]["errors"].append("multiple objects matched recorded old-UID winner UID")
        elif "NotFound" not in probe.stderr:
            STATE["cleanup"]["errors"].append(f"old-UID winner lookup failed: {probe.stderr[-500:]}")
    delete_checked("namespace", NS, "namespace")
    poll_exact_not_found("namespace", NS, "namespace", timeout=90)
    STATE["cleanup"]["namespace"] = "deleted"
    delete_checked("validatingadmissionpolicybinding", BINDING, "validatingAdmissionPolicyBinding")
    poll_exact_not_found(
        "validatingadmissionpolicybinding",
        BINDING,
        "validatingAdmissionPolicyBinding",
    )
    delete_checked("validatingadmissionpolicy", POLICY, "validatingAdmissionPolicy")
    poll_exact_not_found("validatingadmissionpolicy", POLICY, "validatingAdmissionPolicy")
    STATE["cleanup"]["status"] = "passed" if not STATE["cleanup"]["errors"] else "failed"
    save()


def main() -> int:
    primary_error: str | None = None
    try:
        preflight()
        setup()
        if SELECTION == "focused":
            scale(CONTROLLERS[1], 0)
            STATE["infrastructure"]["focusedExecutionController"] = {
                "active": CONTROLLERS[0],
                "scaledToZero": CONTROLLERS[1],
                "reason": "remove unrelated replica interleavings from the focused stale-finalizer and winner assertions",
            }
            save()
            stale_finalizer_conflict()
            winner_matrix()
        else:
            overlap_cases()
            different_slot_rv_race()
            restart_before_create()
            horizon_resume()
            restart_after_create_before_final()
            stale_finalizer_conflict()
            winner_matrix()
            safe_replacement()
        STATE["result"] = "passed"
    except Exception as error:
        primary_error = f"{type(error).__name__}: {error}"
        STATE["result"] = "failed"
        STATE["primaryError"] = primary_error
        note(primary_error)
    finally:
        try:
            if OUT.exists():
                collect()
        except Exception as error:
            STATE.setdefault("collectionErrors", []).append(f"{type(error).__name__}: {error}")
            save()
        if STATE.get("uids", {}).get("namespace"):
            try:
                cleanup()
            except Exception as error:
                STATE["cleanup"]["status"] = "failed"
                STATE["cleanup"]["errors"].append(f"cleanup exception: {type(error).__name__}: {error}")
                save()
        STATE["finishedAt"] = now()
        REPORT_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")
    if primary_error:
        return 1
    if STATE["cleanup"]["status"] != "passed":
        return 2
    note(f"PLAT04 live acceptance passed; report={REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
