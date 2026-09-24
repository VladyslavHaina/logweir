#!/usr/bin/env python3
"""Owned Docker Desktop full-chart acceptance for PLAT-02.1 (installation identity).

Run with a Python that has PyYAML and cryptography (macOS: /usr/bin/python3).

Every kubectl call names ``--context docker-desktop`` and every helm call names
``--kube-context docker-desktop``. The chart's controller and its cluster RBAC
are cluster singletons, so the run requires the shared cluster lock, scales the
lab release's controller to zero, records the lab release's cluster-scoped RBAC
objects that a second release of this chart must adopt with
``--take-ownership`` (whatever ``helm template`` renders cluster-scoped, not a
fixed list), and re-creates them exactly after every test release is gone. The
CRDs the chart ships are recorded too: Helm 4 re-applies ``crds/`` on every
install and a CRD cannot be re-created without deleting its objects, so the
cleanup proof requires each to keep its uid and generation. Private key bytes
exist only in this process's memory: evidence records SHA-256 digests, public
key ids and public verification material.

Environment: ``LOGWEIR_CHART_LIVE_OUT`` (evidence directory, holds
``state.json``), ``LOGWEIR_CHART_LIVE_TS`` (namespace suffix; pin it when
phases run as separate processes), ``LOGWEIR_CHART_LIVE_OWNER`` (must equal the
owner that holds ``k8s-lock.sh``; also the namespaces' test-owner label).
Phases: ``selftest``, ``full`` (runs ``report``), ``report``, ``lab-restore``.
Offline rows: ``scripts/test_plat02_chart_live_rows.py``.
"""

from __future__ import annotations

import argparse
import base64
import copy
import datetime as dt
import gzip
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "logweir"
OUT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_CHART_LIVE_OUT", "/tmp/logweir-roadmap-run/claude/artifacts/plat01-02-live/chart"
    )
)
OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE_PATH = OUT / "state.json"
REPORT_PATH = OUT / "report.json"
TS = os.environ.get("LOGWEIR_CHART_LIVE_TS", dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dt%H%Mz"))
OWNER = os.environ.get("LOGWEIR_CHART_LIVE_OWNER", "plat01-02-live")
LOCK_OWNER_FILE = pathlib.Path("/tmp/logweir-roadmap-run/k8s.lock/owner")
KUBECTL = ["kubectl", "--context", "docker-desktop"]
HELM = ["helm", "--kube-context", "docker-desktop"]
LAB_NS = "logweir-scram-local"
LAB_DEPLOYMENT = "weirkeeper"
LAB_RELEASE = "scram-local"
# The cluster-scoped objects a test release of this chart adopts with
# `--take-ownership` and deletes on uninstall are whatever the chart renders:
# `chart_cluster_objects` reads them from `helm template` at run time. A fixed
# list here drifted once already (the chart grew `logweir-trust-admin` and
# `logweir-retention-admin`, which the lab's restore then silently lost).
CLUSTER_SCOPED_KINDS = {
    "ClusterRole": "clusterrole",
    "ClusterRoleBinding": "clusterrolebinding",
    "ValidatingAdmissionPolicy": "validatingadmissionpolicy",
    "ValidatingAdmissionPolicyBinding": "validatingadmissionpolicybinding",
}
# The value sets the phases install with, so every cluster-scoped object any
# phase can render is recorded before the lab is touched.
RENDER_VARIANTS = [
    [],
    ["--set-string", "identity.authorizedRunnerNamespaces[0]=lw-render-runner"],
    ["--set", "identity.externalSecret.name=render-signer", "--set", "identity.externalSecret.key=identity.pem"],
]
SINGLETON = "logweir-identity-singleton"
SECRET = "logweir-signing-key"
PUBLIC = "logweir-signing-trust"
TRUST_REFERENCE = "logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys"
PUBLIC_KEYS = {"algorithm", "key-id", "signing.pub.pem", "trust-reference"}
RUN_LABEL_KEY = "plat02-chart.logweir.dev/run"
HOOK_SELECTOR = "logweir.dev/identity-authority=bootstrap"
INSTALL_FLAGS = ["--take-ownership", "--force-conflicts"]
NAMES = {
    "primary": (f"lw-plat0102-chart-{TS}", "lwchart"),
    "runner": (f"lw-plat0102-runner-{TS}", None),
    "adopt": (f"lw-plat0102-adopt-{TS}", "lwadopt"),
    "manual": (f"lw-plat0102-manual-{TS}", "lwmanual"),
    "unavailable": (f"lw-plat0102-unavail-{TS}", "lwunavail"),
    "deny": (f"lw-plat0102-deny-{TS}", "lwdeny"),
    "race": (f"lw-plat0102-race-{TS}", "lwrace"),
}
REQUIRED_CASES = [
    "chart_fresh_default_install_bootstraps_identity",
    "chart_bootstrap_rbac_is_resource_scoped",
    "chart_helm_state_carries_no_private_material",
    "chart_upgrade_retains_identity",
    "chart_distributes_same_identity_to_authorized_namespace",
    "chart_controller_restart_retains_identity",
    "chart_rollback_retains_identity",
    "chart_uninstall_reinstall_retains_identity",
    "chart_lost_private_key_refused_without_rotation",
    "chart_restore_first_recovery_retains_identity",
    "chart_external_key_adoption",
    "chart_external_mismatch_never_rotates",
    "chart_existing_managed_secret_adopted",
    "chart_external_key_unavailable_no_fallback",
    "chart_denied_secret_write_fails_closed",
    "chart_recovers_after_denial_removed",
    "chart_concurrent_bootstrap_converges",
    "chart_owned_cleanup_and_lab_restore",
]
PEM_BLOCK = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----", re.S)

if STATE_PATH.exists():
    STATE: Dict[str, Any] = json.loads(STATE_PATH.read_text())
else:
    STATE = {"created": dt.datetime.now(dt.timezone.utc).isoformat(), "ts": TS, "cases": {}}


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def save_state() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")
    STATE_PATH.chmod(0o600)


def redact(text: str) -> str:
    return PEM_BLOCK.sub("[REDACTED PRIVATE KEY]", text)


def log(message: str) -> None:
    print(f"{now()} {redact(message)}", flush=True)


def run(
    argv: List[str],
    *,
    stdin: Optional[str] = None,
    check: bool = True,
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        argv, input=stdin, text=True, capture_output=True, cwd=ROOT, timeout=timeout, check=False
    )
    if check and proc.returncode:
        raise RuntimeError(
            redact(
                f"command={argv[:9]!r} rc={proc.returncode} "
                f"stderr={proc.stderr[-2000:]} stdout={proc.stdout[-1000:]}"
            )
        )
    return proc


def k(*args: str) -> List[str]:
    return KUBECTL + list(args)


def save(name: str, value: Any) -> pathlib.Path:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    text = value if isinstance(value, str) else json.dumps(value, indent=2, sort_keys=True) + "\n"
    if "PRIVATE KEY-----" in text and "[REDACTED" not in text:
        raise RuntimeError(f"refusing to write private key material into evidence {name}")
    path.write_text(redact(text))
    return path


def get_optional(kind: str, name: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
    argv = k("get", kind, name, "-o", "json", "--ignore-not-found")
    if namespace:
        argv = k("-n", namespace, "get", kind, name, "-o", "json", "--ignore-not-found")
    proc = run(argv, check=False)
    if proc.returncode:
        raise RuntimeError(f"get {kind}/{name} failed (not NotFound): {proc.stderr[-500:]}")
    return json.loads(proc.stdout) if proc.stdout.strip() else None


def get(kind: str, name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
    obj = get_optional(kind, name, namespace)
    if obj is None:
        raise RuntimeError(f"expected {kind}/{name} in {namespace or 'cluster'} to exist")
    return obj


def wait_absent(kind: str, name: str, namespace: Optional[str] = None, seconds: int = 240) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if get_optional(kind, name, namespace) is None:
            return
        time.sleep(2)
    raise RuntimeError(f"{kind}/{name} still present after {seconds}s")


API_PATHS = {
    "secret": ("/api/v1", "secrets"),
    "configmap": ("/api/v1", "configmaps"),
    "namespace": ("/api/v1", "namespaces"),
    "job": ("/apis/batch/v1", "jobs"),
    "clusterrole": ("/apis/rbac.authorization.k8s.io/v1", "clusterroles"),
    "clusterrolebinding": ("/apis/rbac.authorization.k8s.io/v1", "clusterrolebindings"),
    "validatingadmissionpolicy": ("/apis/admissionregistration.k8s.io/v1", "validatingadmissionpolicies"),
    "validatingadmissionpolicybinding": (
        "/apis/admissionregistration.k8s.io/v1",
        "validatingadmissionpolicybindings",
    ),
}


def create_verbatim(kind: str, obj: Dict[str, Any]) -> None:
    """POST a cluster-scoped object exactly as recorded.

    `kubectl create -f` rewrites `kubectl.kubernetes.io/last-applied-configuration`
    when the object already carries it (the lab's refresh-applied RBAC does), so
    a restore through it can never equal the record; a raw POST sends the bytes.
    """
    prefix, plural = API_PATHS[kind]
    run(k("create", "--raw", f"{prefix}/{plural}", "-f", "-"), stdin=json.dumps(obj))


def delete_exact(kind: str, name: str, uid: str, namespace: Optional[str] = None) -> None:
    plural = API_PATHS[kind]
    scope = f"/namespaces/{namespace}" if namespace else ""
    options = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {"uid": uid},
        "propagationPolicy": "Background",
    }
    run(k("delete", "--raw", f"{plural[0]}{scope}/{plural[1]}/{name}", "-f", "-"), stdin=json.dumps(options))
    wait_absent(kind, name, namespace)


# --------------------------------------------------------------------- keys


def key_info(pem: bytes) -> Dict[str, str]:
    key = serialization.load_pem_private_key(pem, password=None)
    public = key.public_key()
    der = public.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    if isinstance(key, ec.EllipticCurvePrivateKey) and key.curve.name == "secp256r1":
        algorithm = "ecdsa-p256-sha256"
    elif isinstance(key, ed25519.Ed25519PrivateKey):
        algorithm = "ed25519"
    else:
        algorithm = type(key).__name__
    return {
        "key_id": hashlib.sha256(der).hexdigest(),
        "algorithm": algorithm,
        "private_sha256": hashlib.sha256(pem).hexdigest(),
        "spki_der_sha256": hashlib.sha256(der).hexdigest(),
    }


def public_key_id(spki_pem: str) -> str:
    public = serialization.load_pem_public_key(spki_pem.encode())
    der = public.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()


def generate_key(algorithm: str) -> bytes:
    key = ed25519.Ed25519PrivateKey.generate() if algorithm == "ed25519" else ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def secret_summary(secret: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if secret is None:
        return None
    meta = secret["metadata"]
    data = secret.get("data") or {}
    return {
        "name": meta["name"],
        "namespace": meta["namespace"],
        "uid": meta["uid"],
        "resourceVersion": meta["resourceVersion"],
        "annotations": meta.get("annotations"),
        "labels": meta.get("labels"),
        "ownerReferences": meta.get("ownerReferences"),
        "type": secret.get("type"),
        "data_keys": sorted(data),
        "data_value_sha256": {key: hashlib.sha256(value.encode()).hexdigest() for key, value in data.items()},
    }


def private_pem(secret: Dict[str, Any], key: str = "signing.pem") -> bytes:
    data = secret.get("data") or {}
    if key not in data:
        raise RuntimeError(f"Secret {secret['metadata']['name']} has no {key}")
    return base64.b64decode(data[key])


def identity(namespace: str, *, expect: str) -> Dict[str, Any]:
    """Read the retained identity objects; expect 'established' or 'uninitialized'."""
    secret = get_optional("secret", SECRET, namespace)
    public = get_optional("configmap", PUBLIC, namespace)
    snapshot: Dict[str, Any] = {
        "namespace": namespace,
        "secret": secret_summary(secret),
        "configmap": public,
    }
    if expect == "established":
        if secret is None or public is None:
            raise RuntimeError(f"{namespace}: identity objects missing")
        if (secret["metadata"].get("annotations") or {}).get("logweir.dev/identity-state") != "established":
            raise RuntimeError(f"{namespace}: Secret is not annotated established")
        if sorted((secret.get("data") or {})) != ["signing.pem"]:
            raise RuntimeError(f"{namespace}: Secret data keys are not exactly signing.pem")
        info = key_info(private_pem(secret))
        data = public.get("data") or {}
        if set(data) != PUBLIC_KEYS:
            raise RuntimeError(f"{namespace}: public ConfigMap keys are {sorted(data)!r}")
        if (public["metadata"].get("annotations") or {}).get("logweir.dev/identity-state") != "established":
            raise RuntimeError(f"{namespace}: public ConfigMap is not annotated established")
        if data["key-id"] != info["key_id"] or public_key_id(data["signing.pub.pem"]) != info["key_id"]:
            raise RuntimeError(f"{namespace}: public record does not match the private key")
        if data["algorithm"] != info["algorithm"] or data["trust-reference"] != TRUST_REFERENCE:
            raise RuntimeError(f"{namespace}: public algorithm/trust-reference mismatch")
        snapshot["private"] = info
    elif expect == "uninitialized":
        if secret is not None and (secret.get("data") or {}):
            raise RuntimeError(f"{namespace}: Secret unexpectedly carries data")
        if secret is not None and (secret["metadata"].get("annotations") or {}).get(
            "logweir.dev/identity-state"
        ) == "established":
            raise RuntimeError(f"{namespace}: Secret unexpectedly established")
        if public is not None and (public.get("data") or {}):
            raise RuntimeError(f"{namespace}: public ConfigMap unexpectedly carries data")
    else:
        raise ValueError(expect)
    return snapshot


def distributed_identity(namespace: str) -> Dict[str, Any]:
    secret = get("secret", SECRET, namespace)
    if (secret["metadata"].get("annotations") or {}).get("logweir.dev/identity-state") != "established":
        raise RuntimeError(f"{namespace}: distributed Secret is not established")
    return {"secret": secret_summary(secret), "private": key_info(private_pem(secret))}


def same_identity(before: Dict[str, Any], after: Dict[str, Any], label: str) -> Dict[str, Any]:
    checks = {
        "secret_uid": (before["secret"]["uid"], after["secret"]["uid"]),
        "secret_resource_version": (before["secret"]["resourceVersion"], after["secret"]["resourceVersion"]),
        "private_sha256": (before["private"]["private_sha256"], after["private"]["private_sha256"]),
        "key_id": (before["private"]["key_id"], after["private"]["key_id"]),
        "configmap_uid": (before["configmap"]["metadata"]["uid"], after["configmap"]["metadata"]["uid"]),
        "configmap_resource_version": (
            before["configmap"]["metadata"]["resourceVersion"],
            after["configmap"]["metadata"]["resourceVersion"],
        ),
        "public_data": (before["configmap"]["data"], after["configmap"]["data"]),
    }
    differing = [name for name, (a, b) in checks.items() if a != b]
    if differing:
        raise RuntimeError(f"{label}: identity changed in {differing}")
    return {name: a for name, (a, _b) in checks.items() if name != "public_data"}


# ------------------------------------------------------------------- hooks


class HookWatcher(threading.Thread):
    """Follow identity hook Pods so their logs survive Helm's hook-succeeded deletion."""

    def __init__(self, namespaces: List[str], label: str) -> None:
        super().__init__(daemon=True)
        self.namespaces = namespaces
        self.label = label
        self.stop_event = threading.Event()
        self.followers: Dict[str, Tuple[subprocess.Popen, pathlib.Path, str, str]] = {}
        self.pods: Dict[str, Dict[str, Any]] = {}

    def run(self) -> None:
        while not self.stop_event.is_set():
            self.poll()
            time.sleep(0.5)
        self.poll()

    def poll(self) -> None:
        for namespace in self.namespaces:
            proc = subprocess.run(
                k("-n", namespace, "get", "pods", "-l", HOOK_SELECTOR, "-o", "json"),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if proc.returncode:
                continue
            for pod in json.loads(proc.stdout).get("items", []):
                uid = pod["metadata"]["uid"]
                self.pods[uid] = pod
                statuses = pod.get("status", {}).get("containerStatuses") or []
                started = any(
                    "running" in status.get("state", {}) or "terminated" in status.get("state", {})
                    for status in statuses
                )
                if started and uid not in self.followers:
                    path = OUT / "hook-logs" / self.label / f"{namespace}--{pod['metadata']['name']}.log"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    handle = path.open("w")
                    follower = subprocess.Popen(
                        k("-n", namespace, "logs", "-f", pod["metadata"]["name"], "--all-containers"),
                        stdout=handle,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    self.followers[uid] = (follower, path, namespace, pod["metadata"]["name"])

    def stop(self) -> List[Dict[str, Any]]:
        self.stop_event.set()
        self.join(timeout=30)
        deadline = time.monotonic() + 30
        results = []
        for uid, (follower, path, namespace, name) in self.followers.items():
            try:
                follower.wait(timeout=max(1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                follower.kill()
            text = path.read_text()
            if "PRIVATE KEY" in text:
                raise RuntimeError(f"hook log {path.name} contains private key material")
            pod = self.pods.get(uid, {})
            terminated = [
                {
                    "container": status["name"],
                    "exitCode": status.get("state", {}).get("terminated", {}).get("exitCode"),
                    "imageID": status.get("imageID"),
                }
                for status in pod.get("status", {}).get("containerStatuses") or []
            ]
            results.append(
                {
                    "namespace": namespace,
                    "pod": name,
                    "uid": uid,
                    "job": pod.get("metadata", {}).get("labels", {}).get("job-name"),
                    "component": pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component"),
                    "containers": terminated,
                    "log_file": str(path.relative_to(OUT)),
                    "log": text.strip()[-2000:],
                }
            )
        return results


def helm_command(
    label: str,
    argv: List[str],
    *,
    watch: List[str],
    expect_success: bool,
    timeout: int = 1200,
) -> Dict[str, Any]:
    watcher = HookWatcher(watch, label)
    watcher.start()
    started = time.monotonic()
    proc = run(HELM + argv, check=False, timeout=timeout)
    hooks = watcher.stop()
    record = {
        "label": label,
        "argv": ["helm", "--kube-context", "docker-desktop"] + argv,
        "rc": proc.returncode,
        "seconds": round(time.monotonic() - started, 1),
        "stdout": redact(proc.stdout)[-4000:],
        "stderr": redact(proc.stderr)[-4000:],
        "hook_pods": hooks,
    }
    save(f"helm/{label}.json", record)
    STATE.setdefault("helm_commands", []).append({"label": label, "rc": proc.returncode})
    save_state()
    if expect_success and proc.returncode != 0:
        raise RuntimeError(f"{label}: helm failed rc={proc.returncode}: {redact(proc.stderr)[-1500:]}")
    if not expect_success and proc.returncode == 0:
        raise RuntimeError(f"{label}: helm unexpectedly succeeded")
    return record


def hook_lines(record: Dict[str, Any], component: str) -> List[str]:
    return [
        line
        for pod in record["hook_pods"]
        if pod["component"] == component
        for line in pod["log"].splitlines()
    ]


def require_hook_line(record: Dict[str, Any], component: str, *needles: str) -> str:
    for line in hook_lines(record, component):
        if all(needle in line for needle in needles):
            return line
    raise RuntimeError(
        f"{record['label']}: no {component} hook log line with {needles!r}; captured="
        f"{[pod['log'][-300:] for pod in record['hook_pods']]!r}"
    )


def release_state(release: str, namespace: str, label: str, *, private: Optional[bytes] = None) -> Dict[str, Any]:
    """Save Helm's view of a release and prove it carries no private key bytes."""
    outputs = {}
    for part in ["manifest", "hooks", "values"]:
        proc = run(HELM + ["get", part, release, "-n", namespace, "--all"] if part == "values" else HELM + ["get", part, release, "-n", namespace], check=False)
        outputs[part] = proc.stdout
    history = run(HELM + ["history", release, "-n", namespace, "-o", "json"], check=False).stdout
    secrets = json.loads(
        run(k("-n", namespace, "get", "secrets", "-l", f"owner=helm,name={release}", "-o", "json")).stdout
    )["items"]
    decoded = []
    for item in secrets:
        raw = base64.b64decode(item["data"]["release"])
        try:
            payload = gzip.decompress(base64.b64decode(raw))
        except Exception:  # noqa: BLE001 - Helm encodings vary by version
            payload = raw
        decoded.append({"name": item["metadata"]["name"], "bytes": len(payload), "text": payload.decode("utf-8", "replace")})
    haystacks = list(outputs.values()) + [item["text"] for item in decoded]
    leaks = [index for index, text in enumerate(haystacks) if "PRIVATE KEY" in text]
    if private is not None:
        body = b"".join(line for line in private.splitlines() if not line.startswith(b"-----")).decode()
        encoded = base64.b64encode(private).decode()
        leaks += [
            index for index, text in enumerate(haystacks) if body[:48] in text or encoded[:48] in text
        ]
    save(f"helm/{label}-manifest.yaml", outputs["manifest"])
    save(f"helm/{label}-hooks.yaml", outputs["hooks"])
    save(f"helm/{label}-values.yaml", outputs["values"])
    save(f"helm/{label}-history.json", history)
    result = {
        "release_secrets": [{"name": item["name"], "decoded_bytes": item["bytes"]} for item in decoded],
        "private_material_found": bool(leaks),
    }
    if leaks:
        raise RuntimeError(f"{label}: Helm release state carries private key material")
    return result


# -------------------------------------------------------------- namespaces


def create_namespace(key: str) -> str:
    name = NAMES[key][0]
    existing = get_optional("namespace", name)
    labels = {"logweir.dev/test-owner": OWNER, RUN_LABEL_KEY: TS}
    if existing is not None:
        if any((existing["metadata"].get("labels") or {}).get(a) != b for a, b in labels.items()):
            raise RuntimeError(f"refusing unowned pre-existing namespace {name}")
        return name
    run(
        k("apply", "-f", "-"),
        stdin=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name, "labels": labels}}),
    )
    created = get("namespace", name)
    STATE.setdefault("namespaces", {})[name] = created["metadata"]["uid"]
    save_state()
    return name


def delete_owned_namespace(name: str) -> None:
    existing = get_optional("namespace", name)
    if existing is None:
        return
    labels = existing["metadata"].get("labels") or {}
    if labels.get("logweir.dev/test-owner") != OWNER or labels.get(RUN_LABEL_KEY) != TS:
        raise RuntimeError(f"refusing to delete namespace {name} without this run's labels")
    if STATE.get("namespaces", {}).get(name) != existing["metadata"]["uid"]:
        raise RuntimeError(f"refusing to delete namespace {name}: UID differs from the recorded one")
    delete_exact("namespace", name, existing["metadata"]["uid"])


def delete_retained_identity(namespace: str, release: str) -> Dict[str, Any]:
    """Remove this run's retained identity objects after verifying they are this release's."""
    removed = {}
    for kind, name in [("secret", SECRET), ("configmap", PUBLIC)]:
        obj = get_optional(kind, name, namespace)
        if obj is None:
            continue
        labels = obj["metadata"].get("labels") or {}
        instance = labels.get("app.kubernetes.io/instance")
        if instance not in {release, None}:
            raise RuntimeError(f"refusing to delete {kind}/{name} in {namespace} labelled for {instance}")
        delete_exact(kind, name, obj["metadata"]["uid"], namespace)
        removed[f"{kind}/{name}"] = obj["metadata"]["uid"]
    return removed


def delete_singleton(release: str, namespace: str) -> Optional[str]:
    singleton = get_optional("clusterrole", SINGLETON)
    if singleton is None:
        return None
    annotations = singleton["metadata"].get("annotations") or {}
    if annotations.get("logweir.dev/release-name") != release or annotations.get(
        "logweir.dev/release-namespace"
    ) != namespace:
        raise RuntimeError(f"refusing to delete {SINGLETON}: owned by {annotations!r}")
    delete_exact("clusterrole", SINGLETON, singleton["metadata"]["uid"])
    return singleton["metadata"]["uid"]


def uninstall(release: str, namespace: str, label: str) -> Dict[str, Any]:
    status = run(HELM + ["status", release, "-n", namespace], check=False)
    if status.returncode:
        return {"release": release, "present": False}
    return helm_command(
        label, ["uninstall", release, "-n", namespace, "--wait", "--timeout", "10m"], watch=[namespace], expect_success=True
    )


def teardown_release(key: str, *, extra_namespaces: Optional[List[str]] = None) -> Dict[str, Any]:
    namespace, release = NAMES[key]
    record: Dict[str, Any] = {"release": release, "namespace": namespace}
    if get_optional("namespace", namespace) is None:
        return record
    record["uninstall"] = uninstall(release, namespace, f"{key}-teardown-uninstall").get("rc")
    record["retained_removed"] = delete_retained_identity(namespace, release)
    for other in extra_namespaces or []:
        if get_optional("namespace", other) is not None:
            record.setdefault("retained_removed_other", {})[other] = delete_retained_identity(other, release)
    record["singleton_removed_uid"] = delete_singleton(release, namespace)
    for other in extra_namespaces or []:
        delete_owned_namespace(other)
    delete_owned_namespace(namespace)
    STATE.setdefault("teardown", {})[key] = record
    save_state()
    return record


# ------------------------------------------------------------------ lab


def lock_held() -> None:
    owner = LOCK_OWNER_FILE.read_text().split()[0] if LOCK_OWNER_FILE.exists() else ""
    if owner != OWNER:
        raise RuntimeError(f"cluster lock is held by {owner!r}, not {OWNER!r}")


def deployment_identity(deployment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "uid": deployment["metadata"]["uid"],
        "replicas": deployment["spec"].get("replicas"),
        "image": deployment["spec"]["template"]["spec"]["containers"][0]["image"],
        "spec_sha256": hashlib.sha256(
            json.dumps(deployment["spec"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "ready_replicas": deployment.get("status", {}).get("readyReplicas", 0),
    }


def stripped(obj: Dict[str, Any]) -> Dict[str, Any]:
    clean = copy.deepcopy(obj)
    for field in ["uid", "resourceVersion", "creationTimestamp", "managedFields", "generation"]:
        clean["metadata"].pop(field, None)
    clean.pop("status", None)
    return clean


def comparable(obj: Dict[str, Any]) -> Dict[str, Any]:
    clean = stripped(obj)
    return {
        "labels": clean["metadata"].get("labels"),
        "annotations": clean["metadata"].get("annotations"),
        "rules": clean.get("rules"),
        "subjects": clean.get("subjects"),
        "roleRef": clean.get("roleRef"),
        "aggregationRule": clean.get("aggregationRule"),
    }


def chart_cluster_objects(docs: List[Any]) -> List[Tuple[str, str]]:
    """The non-hook cluster-scoped objects in a rendered chart, as (kubectl kind, name).

    Hook objects are excluded: the only one, the identity singleton marker, is
    `keep` and is removed by `delete_singleton` after its release's teardown.
    A document without a namespace whose kind is not a known cluster-scoped
    kind is refused, so a new cluster-scoped kind cannot slip past the record.
    """
    found = set()
    for doc in docs:
        if not doc:
            continue
        metadata = doc.get("metadata") or {}
        if "helm.sh/hook" in (metadata.get("annotations") or {}):
            continue
        kind = doc.get("kind")
        if kind in CLUSTER_SCOPED_KINDS:
            found.add((CLUSTER_SCOPED_KINDS[kind], metadata["name"]))
        elif not metadata.get("namespace"):
            raise RuntimeError(f"rendered {kind}/{metadata.get('name')} has no namespace and no known cluster scope")
    return sorted(found)


def render_cluster_objects() -> List[Tuple[str, str]]:
    found = set()
    for variant in RENDER_VARIANTS:
        text = run(HELM + ["template", LAB_RELEASE, str(CHART), "-n", LAB_NS, *variant]).stdout
        found.update(chart_cluster_objects(list(yaml.safe_load_all(text))))
    return sorted(found)


def chart_crd_names() -> List[str]:
    names = []
    for path in sorted((CHART / "crds").glob("*.yaml")):
        names += [doc["metadata"]["name"] for doc in yaml.safe_load_all(path.read_text()) if doc]
    return sorted(names)


def crd_record(crd: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if crd is None:
        return {"absent": True}
    return {"uid": crd["metadata"]["uid"], "generation": crd["metadata"].get("generation")}


def crds_as_found(recorded: Dict[str, Dict[str, Any]], live: Dict[str, Dict[str, Any]]) -> List[str]:
    """Problems with the chart's CRDs after the run, compared with the record.

    Helm 4 server-side applies `crds/` on every install, even over existing
    CRDs, and never deletes them. A CRD cannot be restored by re-creating it
    (deleting one deletes every object of its kind), so the harness can only
    prove the run left them as found: the same object (uid) and no spec change
    (`metadata.generation` moves on every spec change; resourceVersion also
    moves when only a field manager is added, so it is not the signal). A CRD
    the run added is reported, never deleted.
    """
    problems = []
    for name, before in sorted(recorded.items()):
        after = live.get(name, {"absent": True})
        if before.get("absent") and not after.get("absent"):
            problems.append(f"{name}: added by the run")
        elif not before.get("absent") and after.get("absent"):
            problems.append(f"{name}: removed by the run")
        elif not before.get("absent") and before["uid"] != after["uid"]:
            problems.append(f"{name}: replaced (uid {before['uid']} -> {after['uid']})")
        elif not before.get("absent") and before["generation"] != after["generation"]:
            problems.append(f"{name}: spec changed (generation {before['generation']} -> {after['generation']})")
    return problems


def live_crds(names: List[str]) -> Dict[str, Dict[str, Any]]:
    return {name: crd_record(get_optional("customresourcedefinition", name)) for name in names}


def release_annotation(obj: Dict[str, Any]) -> Optional[str]:
    return (obj["metadata"].get("annotations") or {}).get("meta.helm.sh/release-name")


def lab_owned(obj: Dict[str, Any]) -> bool:
    """True for the lab release's own incarnation of a cluster-scoped object.

    Helm-installed objects carry the release annotation. Objects a lab refresh
    applied with `kubectl apply` from a later chart render carry no Helm
    annotation, only the chart's `app.kubernetes.io/instance` label; those are
    the lab's too. Any other Helm release's annotation wins over the label.
    """
    annotation = release_annotation(obj)
    if annotation is not None:
        return annotation == LAB_RELEASE
    return (obj["metadata"].get("labels") or {}).get("app.kubernetes.io/instance") == LAB_RELEASE


def test_release_names() -> set:
    return {release for _namespace, release in NAMES.values() if release}


def restore_action(recorded: Dict[str, Any], live: Optional[Dict[str, Any]]) -> str:
    """What lab-restore must do with one recorded cluster-scoped object.

    `recorded` is the lab_prepare record ({"absent": True} or a present
    object's uid/comparable). Anything neither the lab's nor a test release's
    is refused, never deleted.
    """
    key = recorded.get("key", "object")
    if recorded.get("absent"):
        if live is None:
            return "absent"
        if release_annotation(live) in test_release_names():
            return "delete"
        raise RuntimeError(f"{key} was absent before the run and is now owned by {release_annotation(live)!r}")
    if live is None:
        return "recreate"
    if lab_owned(live):
        return "present"
    if release_annotation(live) in test_release_names():
        return "reclaim"
    raise RuntimeError(f"{key} is owned by {release_annotation(live)!r}; refusing to touch it")


def lab_prepare() -> None:
    lock_held()
    if STATE.get("lab") is None:
        deployment = get("deployment", LAB_DEPLOYMENT, LAB_NS)
        rendered = render_cluster_objects()
        objects = {}
        absent = []
        for kind, name in rendered:
            obj = get_optional(kind, name)
            if obj is None:
                absent.append(f"{kind}/{name}")
                continue
            if not lab_owned(obj):
                raise RuntimeError(f"{kind}/{name} is not owned by the lab release; refusing adoption")
            objects[f"{kind}/{name}"] = obj
        if get_optional("clusterrole", SINGLETON) is not None:
            raise RuntimeError(f"{SINGLETON} already exists; refusing to claim cluster identity")
        STATE["lab"] = {
            "deployment": deployment_identity(deployment),
            "crds": live_crds(chart_crd_names()),
            "rendered_cluster_objects": [f"{kind}/{name}" for kind, name in rendered],
            "cluster_objects": {
                **{key: {"uid": obj["metadata"]["uid"], "comparable": comparable(obj)} for key, obj in objects.items()},
                **{key: {"absent": True} for key in absent},
            },
        }
        save("lab/deployment-before.json", deployment)
        for key, obj in objects.items():
            save(f"lab/{key.replace('/', '-')}-before.json", obj)
        save_state()
    run(k("-n", LAB_NS, "scale", f"deployment/{LAB_DEPLOYMENT}", "--replicas=0"))
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        pods = json.loads(
            run(k("-n", LAB_NS, "get", "pods", "-l", "app.kubernetes.io/component=control-plane", "-o", "json")).stdout
        )["items"]
        if not pods:
            break
        time.sleep(2)
    else:
        raise RuntimeError("lab controller did not scale to zero")
    STATE["lab_scaled_to_zero"] = now()
    save_state()
    log("lab controller scaled to zero; cluster RBAC recorded")


def lab_restore() -> Dict[str, Any]:
    lab = STATE["lab"]
    result: Dict[str, Any] = {"cluster_objects": {}}
    for key, recorded in lab["cluster_objects"].items():
        kind, name = key.split("/", 1)
        live = get_optional(kind, name)
        action = restore_action({"key": key, **recorded}, live)
        if recorded.get("absent"):
            if action == "delete":
                # A test release created it and its teardown did not run.
                delete_exact(kind, name, live["metadata"]["uid"])
                action = "deleted"
            if get_optional(kind, name) is not None:
                raise RuntimeError(f"{key} was absent before the run and is still present")
            result["cluster_objects"][key] = {"action": action, "absent_as_found": True}
            continue
        original = json.loads((OUT / f"lab/{kind}-{name}-before.json").read_text())
        if action == "reclaim":
            # A test release still holds the adopted object (its teardown did
            # not run); remove that incarnation and restore the lab's.
            delete_exact(kind, name, live["metadata"]["uid"])
            create_verbatim(kind, stripped(original))
            action = "reclaimed"
        elif action == "recreate":
            create_verbatim(kind, stripped(original))
            action = "recreated"
        now_obj = get(kind, name)
        equal = comparable(now_obj) == recorded["comparable"]
        result["cluster_objects"][key] = {
            "action": action,
            "original_uid": recorded["uid"],
            "uid_now": now_obj["metadata"]["uid"],
            "spec_labels_annotations_equal": equal,
        }
        if not equal:
            raise RuntimeError(f"{key} restored with differing content")
    replicas = lab["deployment"]["replicas"]
    run(k("-n", LAB_NS, "scale", f"deployment/{LAB_DEPLOYMENT}", f"--replicas={replicas}"))
    run(k("-n", LAB_NS, "rollout", "status", f"deployment/{LAB_DEPLOYMENT}", "--timeout=240s"), timeout=260)
    deadline = time.monotonic() + 120
    identity_now = deployment_identity(get("deployment", LAB_DEPLOYMENT, LAB_NS))
    while time.monotonic() < deadline and identity_now["ready_replicas"] != replicas:
        time.sleep(2)
        identity_now = deployment_identity(get("deployment", LAB_DEPLOYMENT, LAB_NS))
    result["deployment"] = identity_now
    result["deployment_exact"] = (
        identity_now["uid"] == lab["deployment"]["uid"]
        and identity_now["spec_sha256"] == lab["deployment"]["spec_sha256"]
        and identity_now["ready_replicas"] == replicas
    )
    if not result["deployment_exact"]:
        raise RuntimeError(f"lab controller not restored exactly: {identity_now!r}")
    save("lab/restore.json", result)
    return result


# ------------------------------------------------------------------ cases


def set_case(name: str, value: str = "passed") -> None:
    STATE["cases"][name] = value
    save_state()


def execute(cases: List[str], action: Callable[[], None]) -> None:
    for case in cases:
        STATE["cases"][case] = "unrun"
    save_state()
    try:
        action()
        missing = [case for case in cases if STATE["cases"].get(case) == "unrun"]
        if missing:
            raise RuntimeError(f"group returned without classifying {missing}")
    except Exception as exc:  # noqa: BLE001 - later phases still run and teardown still happens
        message = redact(f"{type(exc).__name__}: {exc}")
        for case in cases:
            if STATE["cases"].get(case) != "passed":
                STATE["cases"][case] = "failed"
        STATE.setdefault("failures", []).append({"cases": cases, "error": message})
        save_state()
        log(f"case failure preserved for {cases}: {message}")


def auth_can_i(verb: str, resource: str, namespace: str, user: str) -> str:
    proc = run(k("auth", "can-i", verb, resource, f"--as={user}", "-n", namespace), check=False)
    return proc.stdout.strip()


def phase_primary() -> None:
    namespace, release = NAMES["primary"]
    runner_ns = NAMES["runner"][0]
    values_bootstrap = yaml.safe_load((CHART / "values.yaml").read_text())["identity"]["bootstrapImage"]
    STATE["pinned_bootstrap_image"] = values_bootstrap
    save_state()
    evidence: Dict[str, Any] = {}

    def fresh() -> None:
        create_namespace("primary")
        create_namespace("runner")
        install = helm_command(
            "primary-01-fresh-default-install",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m"],
            watch=[namespace],
            expect_success=True,
        )
        established = identity(namespace, expect="established")
        if established["private"]["algorithm"] != "ecdsa-p256-sha256":
            raise RuntimeError("generated installation identity is not P-256")
        line = require_hook_line(
            install, "identity-bootstrap", "identity-ready", established["private"]["key_id"], "source=generated"
        )
        bootstrap_images = {
            container["imageID"]
            for pod in install["hook_pods"]
            if pod["component"] == "identity-bootstrap"
            for container in pod["containers"]
        }
        digest = values_bootstrap.split("@", 1)[1]
        if not bootstrap_images or not all(digest in (image or "") for image in bootstrap_images):
            raise RuntimeError(f"bootstrap Pod did not run the pinned digest: {bootstrap_images!r}")
        hooks = [doc for doc in yaml.safe_load_all(run(HELM + ["get", "hooks", release, "-n", namespace]).stdout) if doc]
        job = next(doc for doc in hooks if doc["kind"] == "Job" and doc["metadata"]["name"] == f"{release}-identity-bootstrap")
        container = job["spec"]["template"]["spec"]["containers"][0]
        if container["image"] != values_bootstrap or container["imagePullPolicy"] != "IfNotPresent":
            raise RuntimeError(f"rendered bootstrap Job is not the pinned default: {container['image']}")
        singleton = get("clusterrole", SINGLETON)
        annotations = singleton["metadata"].get("annotations") or {}
        if (
            annotations.get("logweir.dev/release-name") != release
            or annotations.get("logweir.dev/release-namespace") != namespace
            or singleton.get("rules")
        ):
            raise RuntimeError(f"singleton marker is not this release's authority-free claim: {annotations!r}")
        deployment = get("deployment", "weirkeeper", namespace)
        pods = json.loads(
            run(k("-n", namespace, "get", "pods", "-l", "app.kubernetes.io/component=control-plane", "-o", "json")).stdout
        )["items"]
        controller = [
            {"pod": pod["metadata"]["name"], "imageID": status.get("imageID"), "ready": status.get("ready")}
            for pod in pods
            for status in pod.get("status", {}).get("containerStatuses") or []
        ]
        if deployment.get("status", {}).get("readyReplicas") != 1 or not all(item["ready"] for item in controller):
            raise RuntimeError("chart controller is not Ready after --wait")
        evidence["fresh"] = {
            "identity": {key: value for key, value in established.items() if key != "configmap"},
            "public_configmap": established["configmap"],
            "bootstrap_log_line": line,
            "bootstrap_image_ids": sorted(bootstrap_images),
            "rendered_bootstrap_image": container["image"],
            "singleton": {"uid": singleton["metadata"]["uid"], "annotations": annotations},
            "controller_image": deployment["spec"]["template"]["spec"]["containers"][0]["image"],
            "controller_pods": controller,
            "local_key_generation_by_harness": False,
            "helm_state": release_state(release, namespace, "primary-01", private=private_pem(get("secret", SECRET, namespace))),
        }
        STATE["primary_identity"] = established["private"]
        save("primary/01-fresh.json", evidence["fresh"])
        set_case("chart_fresh_default_install_bootstraps_identity")
        set_case("chart_helm_state_carries_no_private_material")

        bootstrap_user = f"system:serviceaccount:{namespace}:{release}-identity-bootstrap"
        matrix = {
            "bootstrap_get_signing_secret": (auth_can_i("get", f"secret/{SECRET}", namespace, bootstrap_user), "yes"),
            "bootstrap_patch_signing_secret": (auth_can_i("patch", f"secret/{SECRET}", namespace, bootstrap_user), "yes"),
            "bootstrap_list_secrets": (auth_can_i("list", "secrets", namespace, bootstrap_user), "no"),
            "bootstrap_get_other_secret": (auth_can_i("get", "secret/logweir-s3", namespace, bootstrap_user), "no"),
            "bootstrap_create_secrets": (auth_can_i("create", "secrets", namespace, bootstrap_user), "no"),
            "bootstrap_delete_signing_secret": (auth_can_i("delete", f"secret/{SECRET}", namespace, bootstrap_user), "no"),
            "bootstrap_patch_public_configmap": (auth_can_i("patch", f"configmap/{PUBLIC}", namespace, bootstrap_user), "yes"),
            "bootstrap_patch_other_configmap": (auth_can_i("patch", "configmap/other", namespace, bootstrap_user), "no"),
            "controller_get_secrets": (
                auth_can_i("get", "secrets", namespace, f"system:serviceaccount:{namespace}:weirkeeper"),
                "no",
            ),
            "runner_get_secrets": (
                auth_can_i("get", "secrets", namespace, f"system:serviceaccount:{namespace}:logweir-runner"),
                "no",
            ),
        }
        save("primary/01-rbac-matrix.json", {name: {"actual": a, "expected": b} for name, (a, b) in matrix.items()})
        wrong = {name: pair for name, pair in matrix.items() if pair[0] != pair[1]}
        if wrong:
            raise RuntimeError(f"bootstrap RBAC matrix mismatch: {wrong!r}")
        set_case("chart_bootstrap_rbac_is_resource_scoped")

    execute(
        [
            "chart_fresh_default_install_bootstraps_identity",
            "chart_helm_state_carries_no_private_material",
            "chart_bootstrap_rbac_is_resource_scoped",
        ],
        fresh,
    )

    def upgrade() -> None:
        if STATE["cases"].get("chart_fresh_default_install_bootstraps_identity") != "passed":
            raise RuntimeError("fresh install did not pass")
        before = identity(namespace, expect="established")
        record = helm_command(
            "primary-02-upgrade-distribute",
            [
                "upgrade", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m",
                "--set-string", f"identity.authorizedRunnerNamespaces[0]={runner_ns}",
            ],
            watch=[namespace, runner_ns],
            expect_success=True,
        )
        after = identity(namespace, expect="established")
        unchanged = same_identity(before, after, "upgrade")
        require_hook_line(record, "identity-bootstrap", "identity-ready", before["private"]["key_id"], "source=existing")
        distributed = distributed_identity(runner_ns)
        if distributed["private"]["private_sha256"] != before["private"]["private_sha256"]:
            raise RuntimeError("authorized runner namespace received a different signer")
        dist_line = require_hook_line(
            record, "identity-distribution", "identity-distributed", before["private"]["key_id"], "source=adopted"
        )
        runner_sa = get("serviceaccount", "logweir-runner", runner_ns)
        policies = json.loads(run(k("-n", runner_ns, "get", "networkpolicies", "-o", "json")).stdout)["items"]
        evidence["upgrade"] = {
            "unchanged": unchanged,
            "distributed": distributed,
            "distribution_log_line": dist_line,
            "runner_serviceaccount_uid": runner_sa["metadata"]["uid"],
            "runner_networkpolicies": sorted(item["metadata"]["name"] for item in policies),
            "helm_state": release_state(release, namespace, "primary-02", private=private_pem(get("secret", SECRET, namespace))),
        }
        STATE["runner_distributed"] = distributed
        save("primary/02-upgrade.json", evidence["upgrade"])
        set_case("chart_upgrade_retains_identity")
        set_case("chart_distributes_same_identity_to_authorized_namespace")

    execute(["chart_upgrade_retains_identity", "chart_distributes_same_identity_to_authorized_namespace"], upgrade)

    def restart() -> None:
        before = identity(namespace, expect="established")
        run(k("-n", namespace, "rollout", "restart", "deployment/weirkeeper"))
        run(k("-n", namespace, "rollout", "status", "deployment/weirkeeper", "--timeout=300s"), timeout=320)
        after = identity(namespace, expect="established")
        save("primary/03-restart.json", same_identity(before, after, "controller restart"))
        set_case("chart_controller_restart_retains_identity")

    execute(["chart_controller_restart_retains_identity"], restart)

    def rollback() -> None:
        before = identity(namespace, expect="established")
        distributed_before = distributed_identity(runner_ns)
        record = helm_command(
            "primary-04-rollback-to-1",
            ["rollback", release, "1", "-n", namespace, "--wait", "--timeout", "15m"],
            watch=[namespace, runner_ns],
            expect_success=True,
        )
        after = identity(namespace, expect="established")
        unchanged = same_identity(before, after, "rollback")
        line = require_hook_line(record, "identity-bootstrap", "identity-ready", before["private"]["key_id"], "source=existing")
        distributed_after = distributed_identity(runner_ns)
        if distributed_after["secret"] != distributed_before["secret"]:
            raise RuntimeError("rollback changed the retained distributed Secret")
        save("primary/04-rollback.json", {"unchanged": unchanged, "bootstrap_log_line": line, "distributed_retained": distributed_after["secret"]})
        set_case("chart_rollback_retains_identity")

    execute(["chart_rollback_retains_identity"], rollback)

    def uninstall_reinstall() -> None:
        before = identity(namespace, expect="established")
        distributed_before = distributed_identity(runner_ns)
        singleton_before = get("clusterrole", SINGLETON)["metadata"]["uid"]
        helm_command("primary-05-uninstall", ["uninstall", release, "-n", namespace, "--wait", "--timeout", "10m"], watch=[namespace], expect_success=True)
        retained = identity(namespace, expect="established")
        same_identity(before, retained, "after uninstall")
        if distributed_identity(runner_ns)["secret"] != distributed_before["secret"]:
            raise RuntimeError("uninstall changed the distributed Secret")
        if get("clusterrole", SINGLETON)["metadata"]["uid"] != singleton_before:
            raise RuntimeError("uninstall replaced the singleton marker")
        gone = {
            "deployment/weirkeeper": get_optional("deployment", "weirkeeper", namespace) is None,
            "role/bootstrap": get_optional("role", f"{release}-identity-bootstrap", namespace) is None,
            "serviceaccount/bootstrap": get_optional("serviceaccount", f"{release}-identity-bootstrap", namespace) is None,
        }
        if not all(gone.values()):
            raise RuntimeError(f"uninstall left ordinary release resources: {gone!r}")
        record = helm_command(
            "primary-06-reinstall",
            [
                "install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m",
                "--set-string", f"identity.authorizedRunnerNamespaces[0]={runner_ns}",
            ],
            watch=[namespace, runner_ns],
            expect_success=True,
        )
        after = identity(namespace, expect="established")
        unchanged = same_identity(before, after, "reinstall")
        line = require_hook_line(record, "identity-bootstrap", "identity-ready", before["private"]["key_id"], "source=existing")
        dist_line = require_hook_line(record, "identity-distribution", "identity-distributed", before["private"]["key_id"], "source=existing")
        if distributed_identity(runner_ns)["secret"] != distributed_before["secret"]:
            raise RuntimeError("reinstall changed the distributed Secret")
        save(
            "primary/05-uninstall-reinstall.json",
            {
                "unchanged": unchanged,
                "ordinary_resources_removed_by_uninstall": gone,
                "singleton_uid": singleton_before,
                "bootstrap_log_line": line,
                "distribution_log_line": dist_line,
                "helm_state": release_state(release, namespace, "primary-06", private=private_pem(get("secret", SECRET, namespace))),
            },
        )
        set_case("chart_uninstall_reinstall_retains_identity")

    execute(["chart_uninstall_reinstall_retains_identity"], uninstall_reinstall)

    def lost_key() -> None:
        before = identity(namespace, expect="established")
        original = get("secret", SECRET, namespace)
        retained_data = copy.deepcopy(original.get("data"))
        retained_meta = {
            "annotations": copy.deepcopy(original["metadata"].get("annotations")),
            "labels": copy.deepcopy(original["metadata"].get("labels")),
        }
        delete_exact("secret", SECRET, original["metadata"]["uid"], namespace)
        refused = helm_command(
            "primary-07-upgrade-after-private-key-loss",
            [
                "upgrade", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "4m",
                "--set-string", f"identity.authorizedRunnerNamespaces[0]={runner_ns}",
            ],
            watch=[namespace, runner_ns],
            expect_success=False,
        )
        refusal = require_hook_line(refused, "identity-bootstrap", "retained private key is absent")
        placeholder = get("secret", SECRET, namespace)
        if placeholder.get("data"):
            raise RuntimeError("bootstrap wrote a new private key after the retained key was lost")
        public_after = get("configmap", PUBLIC, namespace)
        if public_after["metadata"]["uid"] != before["configmap"]["metadata"]["uid"] or public_after.get("data") != before["configmap"]["data"]:
            raise RuntimeError("public trust record changed after private key loss")
        exit_codes = sorted(
            {container["exitCode"] for pod in refused["hook_pods"] if pod["component"] == "identity-bootstrap" for container in pod["containers"]}
        )
        save(
            "primary/07-lost-key.json",
            {
                "deleted_secret_uid": original["metadata"]["uid"],
                "refusal_log_line": refusal,
                "bootstrap_exit_codes": exit_codes,
                "placeholder": secret_summary(placeholder),
                "public_unchanged": {"uid": public_after["metadata"]["uid"], "resourceVersion": public_after["metadata"]["resourceVersion"]},
                "helm_rc": refused["rc"],
            },
        )
        set_case("chart_lost_private_key_refused_without_rotation")

        # Restore-first recovery: replace the empty placeholder with the backed-up bytes.
        delete_exact("secret", SECRET, placeholder["metadata"]["uid"], namespace)
        restored = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": SECRET, "namespace": namespace, **{key: value for key, value in retained_meta.items() if value}},
            "data": retained_data,
        }
        run(k("create", "-f", "-"), stdin=json.dumps(restored))
        del retained_data
        recovered = helm_command(
            "primary-08-upgrade-after-restore-first-recovery",
            [
                "upgrade", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m",
                "--set-string", f"identity.authorizedRunnerNamespaces[0]={runner_ns}",
            ],
            watch=[namespace, runner_ns],
            expect_success=True,
        )
        after = identity(namespace, expect="established")
        if after["private"] != before["private"]:
            raise RuntimeError("recovered identity differs from the backed-up signer")
        line = require_hook_line(recovered, "identity-bootstrap", "identity-ready", before["private"]["key_id"], "source=existing")
        save(
            "primary/08-restore-first-recovery.json",
            {"private": after["private"], "secret": after["secret"], "bootstrap_log_line": line},
        )
        set_case("chart_restore_first_recovery_retains_identity")

    execute(["chart_lost_private_key_refused_without_rotation", "chart_restore_first_recovery_retains_identity"], lost_key)
    STATE.setdefault("teardown", {})["primary"] = teardown_release("primary", extra_namespaces=[runner_ns])
    save_state()


def phase_adoption() -> None:
    namespace, release = NAMES["adopt"]
    source = "company-logweir-signer"

    def adopt() -> None:
        create_namespace("adopt")
        pem = generate_key("ed25519")
        external = key_info(pem)
        run(
            k("create", "-f", "-"),
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "type": "Opaque",
                    "metadata": {"name": source, "namespace": namespace, "labels": {"logweir.dev/test-owner": OWNER}},
                    "data": {"identity.pem": base64.b64encode(pem).decode()},
                }
            ),
        )
        source_before = secret_summary(get("secret", source, namespace))
        values = ["--set", f"identity.externalSecret.name={source}", "--set", "identity.externalSecret.key=identity.pem"]
        record = helm_command(
            "adopt-01-install-external",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m", *values],
            watch=[namespace],
            expect_success=True,
        )
        established = identity(namespace, expect="established")
        if established["private"] != external:
            raise RuntimeError("managed Secret does not hold the exact external key")
        line = require_hook_line(record, "identity-bootstrap", "identity-ready", external["key_id"], "source=adopted")
        if secret_summary(get("secret", source, namespace)) != source_before:
            raise RuntimeError("bootstrap modified the get-only external source Secret")
        save(
            "adopt/01-external-adoption.json",
            {
                "external": external,
                "managed": established["private"],
                "public": established["configmap"],
                "source_secret_unchanged": source_before,
                "bootstrap_log_line": line,
                "helm_state": release_state(release, namespace, "adopt-01", private=pem),
            },
        )
        set_case("chart_external_key_adoption")

        other = generate_key("ed25519")
        other_info = key_info(other)
        source_obj = get("secret", source, namespace)
        source_obj["data"] = {"identity.pem": base64.b64encode(other).decode()}
        run(k("replace", "-f", "-"), stdin=json.dumps(source_obj))
        del other
        refused = helm_command(
            "adopt-02-upgrade-with-different-external-key",
            ["upgrade", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "4m", *values],
            watch=[namespace],
            expect_success=False,
        )
        refusal = require_hook_line(refused, "identity-bootstrap", "does not match the established installation identity")
        after = identity(namespace, expect="established")
        same_identity(established, after, "external mismatch")
        source_obj = get("secret", source, namespace)
        source_obj["data"] = {"identity.pem": base64.b64encode(pem).decode()}
        run(k("replace", "-f", "-"), stdin=json.dumps(source_obj))
        del pem
        recovered = helm_command(
            "adopt-03-upgrade-with-original-external-key",
            ["upgrade", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m", *values],
            watch=[namespace],
            expect_success=True,
        )
        line_existing = require_hook_line(recovered, "identity-bootstrap", "identity-ready", external["key_id"], "source=existing")
        final = identity(namespace, expect="established")
        same_identity(established, final, "external restored")
        save(
            "adopt/02-external-mismatch.json",
            {"different_key_id": other_info["key_id"], "refusal_log_line": refusal, "identity_unchanged": final["private"], "recovered_log_line": line_existing},
        )
        set_case("chart_external_mismatch_never_rotates")

    execute(["chart_external_key_adoption", "chart_external_mismatch_never_rotates"], adopt)
    teardown_release("adopt")


def phase_manual() -> None:
    namespace, release = NAMES["manual"]

    def manual() -> None:
        create_namespace("manual")
        pem = generate_key("p256")
        info = key_info(pem)
        run(
            k("create", "-f", "-"),
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "type": "Opaque",
                    "metadata": {"name": SECRET, "namespace": namespace},
                    "data": {"signing.pem": base64.b64encode(pem).decode()},
                }
            ),
        )
        before = get("secret", SECRET, namespace)
        record = helm_command(
            "manual-01-install-over-existing-managed-secret",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m"],
            watch=[namespace],
            expect_success=True,
        )
        after = get("secret", SECRET, namespace)
        if after["metadata"]["uid"] != before["metadata"]["uid"] or key_info(private_pem(after)) != info:
            raise RuntimeError("pre-provisioned managed Secret was replaced or rotated")
        public = get("configmap", PUBLIC, namespace)
        if (public.get("data") or {}).get("key-id") != info["key_id"] or set(public.get("data") or {}) != PUBLIC_KEYS:
            raise RuntimeError("public record does not publish the pre-provisioned key")
        line = require_hook_line(record, "identity-bootstrap", "identity-ready", info["key_id"], "source=existing")
        save(
            "manual/01-existing-managed-secret.json",
            {
                "secret_before": secret_summary(before),
                "secret_after": secret_summary(after),
                "private": info,
                "public": public,
                "bootstrap_log_line": line,
                "helm_state": release_state(release, namespace, "manual-01", private=pem),
            },
        )
        del pem
        set_case("chart_existing_managed_secret_adopted")

    execute(["chart_existing_managed_secret_adopted"], manual)
    teardown_release("manual")


def phase_unavailable() -> None:
    namespace, release = NAMES["unavailable"]
    source = "absent-logweir-signer"
    values = ["--set", f"identity.externalSecret.name={source}", "--set", "identity.externalSecret.key=identity.pem"]

    def unavailable() -> None:
        create_namespace("unavailable")
        refused = helm_command(
            "unavailable-01-install-absent-external",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "4m", *values],
            watch=[namespace],
            expect_success=False,
        )
        absent = require_hook_line(refused, "identity-bootstrap", f"configured external signing Secret {source}/identity.pem is unavailable", "HTTP 404")
        first = identity(namespace, expect="uninitialized")
        helm_command("unavailable-02-uninstall-failed", ["uninstall", release, "-n", namespace, "--wait", "--timeout", "10m"], watch=[namespace], expect_success=True)
        run(
            k("create", "-f", "-"),
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "type": "Opaque",
                    "metadata": {"name": source, "namespace": namespace, "labels": {"logweir.dev/test-owner": OWNER}},
                    "data": {"other.pem": base64.b64encode(b"not the configured key\n").decode()},
                }
            ),
        )
        refused_key = helm_command(
            "unavailable-03-install-external-without-key",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "4m", *values],
            watch=[namespace],
            expect_success=False,
        )
        missing_key = require_hook_line(refused_key, "identity-bootstrap", "is unavailable", "required data key is absent")
        second = identity(namespace, expect="uninitialized")
        save(
            "unavailable/01-external-unavailable.json",
            {"absent_log_line": absent, "missing_key_log_line": missing_key, "after_absent": first, "after_missing_key": second},
        )
        set_case("chart_external_key_unavailable_no_fallback")

    execute(["chart_external_key_unavailable_no_fallback"], unavailable)
    teardown_release("unavailable")


def phase_denied() -> None:
    namespace, release = NAMES["deny"]
    policy = f"lw-plat0102-deny-signing-write-{TS}"

    def denied() -> None:
        create_namespace("deny")
        user = f"system:serviceaccount:{namespace}:{release}-identity-bootstrap"
        labels = {"logweir.dev/test-owner": OWNER, RUN_LABEL_KEY: TS}
        run(
            k("create", "-f", "-"),
            stdin=json.dumps(
                {
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicy",
                    "metadata": {"name": policy, "labels": labels},
                    "spec": {
                        "failurePolicy": "Fail",
                        "matchConstraints": {
                            "resourceRules": [
                                {
                                    "apiGroups": [""],
                                    "apiVersions": ["v1"],
                                    "operations": ["UPDATE"],
                                    "resources": ["secrets"],
                                    "resourceNames": [SECRET],
                                }
                            ],
                            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
                        },
                        "validations": [
                            {
                                "expression": f"request.userInfo.username != '{user}'",
                                "reason": "Forbidden",
                                "message": "plat01-02-live controlled denial of the identity Secret write",
                            }
                        ],
                    },
                }
            ),
        )
        run(
            k("create", "-f", "-"),
            stdin=json.dumps(
                {
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": "ValidatingAdmissionPolicyBinding",
                    "metadata": {"name": policy, "labels": labels},
                    "spec": {"policyName": policy, "validationActions": ["Deny"]},
                }
            ),
        )
        STATE["vap"] = {
            "policy_uid": get("validatingadmissionpolicy", policy)["metadata"]["uid"],
            "binding_uid": get("validatingadmissionpolicybinding", policy)["metadata"]["uid"],
            "name": policy,
        }
        save_state()
        save("deny/00-policy.json", get("validatingadmissionpolicy", policy))
        time.sleep(10)
        refused = helm_command(
            "deny-01-install-with-denied-secret-write",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "4m"],
            watch=[namespace],
            expect_success=False,
        )
        denial = require_hook_line(refused, "identity-bootstrap", f"Kubernetes patch secrets {SECRET} returned HTTP 403")
        blocked = identity(namespace, expect="uninitialized")
        save("deny/01-denied.json", {"denial_log_line": denial, "identity_after_denial": blocked, "helm_rc": refused["rc"]})
        set_case("chart_denied_secret_write_fails_closed")

        delete_exact("validatingadmissionpolicybinding", policy, STATE["vap"]["binding_uid"])
        delete_exact("validatingadmissionpolicy", policy, STATE["vap"]["policy_uid"])
        STATE["vap"]["removed"] = now()
        save_state()
        time.sleep(10)
        helm_command("deny-02-uninstall-failed", ["uninstall", release, "-n", namespace, "--wait", "--timeout", "10m"], watch=[namespace], expect_success=True)
        recovered = helm_command(
            "deny-03-install-after-policy-removed",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--wait", "--timeout", "15m"],
            watch=[namespace],
            expect_success=True,
        )
        established = identity(namespace, expect="established")
        line = require_hook_line(recovered, "identity-bootstrap", "identity-ready", established["private"]["key_id"], "source=generated")
        save("deny/02-recovered.json", {"identity": {key: value for key, value in established.items() if key != "configmap"}, "bootstrap_log_line": line})
        set_case("chart_recovers_after_denial_removed")

    try:
        execute(["chart_denied_secret_write_fails_closed", "chart_recovers_after_denial_removed"], denied)
    finally:
        for kind in ["validatingadmissionpolicybinding", "validatingadmissionpolicy"]:
            live = get_optional(kind, policy)
            if live is not None:
                if (live["metadata"].get("labels") or {}).get("logweir.dev/test-owner") != OWNER:
                    raise RuntimeError(f"refusing to delete unowned {kind}/{policy}")
                delete_exact(kind, policy, live["metadata"]["uid"])
        teardown_release("deny")


def phase_race() -> None:
    namespace, release = NAMES["race"]

    def race() -> None:
        create_namespace("race")
        helm_command(
            "race-01-install-no-hooks",
            ["install", release, str(CHART), "-n", namespace, *INSTALL_FLAGS, "--no-hooks", "--wait", "--timeout", "15m"],
            watch=[namespace],
            expect_success=True,
        )
        hooks_text = run(HELM + ["get", "hooks", release, "-n", namespace]).stdout
        hooks = [doc for doc in yaml.safe_load_all(hooks_text) if doc]
        if not hooks:
            # `--no-hooks` releases may record none; render the same chart and
            # keep the documents Helm marks as hooks.
            hooks_text = run(HELM + ["template", release, str(CHART), "-n", namespace]).stdout
            hooks = [
                doc
                for doc in yaml.safe_load_all(hooks_text)
                if doc and "helm.sh/hook" in ((doc.get("metadata") or {}).get("annotations") or {})
            ]
        by_kind = {(doc["kind"], doc["metadata"]["name"]): doc for doc in hooks}
        secret_doc = by_kind[("Secret", SECRET)]
        public_doc = by_kind[("ConfigMap", PUBLIC)]
        singleton_doc = by_kind.get(("ClusterRole", SINGLETON))
        job_doc = by_kind[("Job", f"{release}-identity-bootstrap")]
        save("race/00-rendered-hooks.yaml", hooks_text)
        if singleton_doc is not None and get_optional("clusterrole", SINGLETON) is None:
            run(k("create", "-f", "-"), stdin=json.dumps(singleton_doc))
        attempts = []
        contention = None
        for attempt in range(1, 6):
            for kind, name in [("secret", SECRET), ("configmap", PUBLIC)]:
                live = get_optional(kind, name, namespace)
                if live is not None:
                    delete_exact(kind, name, live["metadata"]["uid"], namespace)
            run(k("-n", namespace, "create", "-f", "-"), stdin=json.dumps(secret_doc))
            run(k("-n", namespace, "create", "-f", "-"), stdin=json.dumps(public_doc))
            start_ns = time.time_ns() + 30_000_000_000
            jobs = []
            for side in ["a", "b"]:
                job = copy.deepcopy(job_doc)
                job["metadata"] = {
                    "name": f"{release}-bootstrap-race-{side}{attempt}",
                    "namespace": namespace,
                    "labels": {"logweir.dev/test-owner": OWNER, RUN_LABEL_KEY: TS},
                }
                job["spec"]["backoffLimit"] = 0
                container = job["spec"]["template"]["spec"]["containers"][0]
                original_args = container["args"]
                container["command"] = ["/bin/sh", "-c"]
                container["args"] = [
                    f"T={start_ns}; while [ \"$(date +%s%N)\" -lt \"$T\" ]; do :; done; "
                    "exec /usr/local/bin/logweir \"$@\"",
                    "synchronized-start",
                    *original_args,
                ]
                jobs.append(job)
            for job in jobs:
                run(k("-n", namespace, "create", "-f", "-"), stdin=json.dumps(job))
            outcomes = {}
            deadline = time.monotonic() + 240
            while time.monotonic() < deadline and len(outcomes) < 2:
                for job in jobs:
                    name = job["metadata"]["name"]
                    if name in outcomes:
                        continue
                    pods = json.loads(run(k("-n", namespace, "get", "pods", "-l", f"job-name={name}", "-o", "json")).stdout)["items"]
                    for pod in pods:
                        status = (pod.get("status", {}).get("containerStatuses") or [{}])[0]
                        terminated = status.get("state", {}).get("terminated")
                        if terminated:
                            text = run(k("-n", namespace, "logs", pod["metadata"]["name"]), check=False).stdout
                            if "PRIVATE KEY" in text:
                                raise RuntimeError("race pod log carries private key material")
                            outcomes[name] = {"exitCode": terminated.get("exitCode"), "log": text.strip(), "imageID": status.get("imageID")}
                time.sleep(1)
            if len(outcomes) < 2:
                raise RuntimeError(f"race attempt {attempt} did not finish: {outcomes!r}")
            established = identity(namespace, expect="established")
            key_id = established["private"]["key_id"]
            sources = []
            for outcome in outcomes.values():
                if outcome["exitCode"] != 0 or f"key-id={key_id}" not in outcome["log"]:
                    raise RuntimeError(f"race attempt {attempt} did not converge: {outcomes!r}")
                match = re.search(r"source=(\S+)(?: contention-http=(\d+))?", outcome["log"])
                sources.append((match.group(1), match.group(2)) if match else ("?", None))
            attempts.append({"attempt": attempt, "outcomes": outcomes, "sources": sources, "key_id": key_id, "private_sha256": established["private"]["private_sha256"]})
            save("race/attempts.json", attempts)
            kinds = sorted(source for source, _status in sources)
            if kinds == ["concurrent-existing", "generated"]:
                contention = attempts[-1]
                break
            if kinds not in (["existing", "generated"],):
                raise RuntimeError(f"unexpected race sources {sources!r}")
        if contention is None:
            raise RuntimeError("no attempt produced real API contention between the two bootstrap clients")
        save("race/01-converged.json", {"contention_attempt": contention, "attempts": len(attempts)})
        set_case("chart_concurrent_bootstrap_converges")

    execute(["chart_concurrent_bootstrap_converges"], race)
    teardown_release("race")


def full() -> None:
    STATE["started"] = now()
    save_state()
    lab_prepare()
    try:
        phase_primary()
        phase_adoption()
        phase_manual()
        phase_unavailable()
        phase_denied()
        phase_race()
    finally:
        leftovers = []
        for key, (namespace, _release) in NAMES.items():
            if get_optional("namespace", namespace) is not None:
                leftovers.append(namespace)
        STATE["leftover_namespaces_before_lab_restore"] = leftovers
        save_state()
        restore = lab_restore()
        STATE["lab_restore"] = restore
        recorded_crds = STATE["lab"].get("crds")
        crd_problems = (
            crds_as_found(recorded_crds, live_crds(sorted(recorded_crds)))
            if recorded_crds is not None
            else ["no CRD record: lab_prepare predates the CRD check"]
        )
        STATE["crd_problems"] = crd_problems
        checks = {
            "no_test_namespaces": all(get_optional("namespace", ns) is None for ns, _ in NAMES.values()),
            "no_singleton": get_optional("clusterrole", SINGLETON) is None,
            "no_policy": not run(k("get", "validatingadmissionpolicies", "-o", "name")).stdout.strip(),
            "no_test_releases": not [
                item for item in json.loads(run(HELM + ["list", "-A", "-o", "json"]).stdout) if item["name"] in {r for _, r in NAMES.values() if r}
            ],
            "lab_release_deployed": json.loads(run(HELM + ["status", LAB_RELEASE, "-n", LAB_NS, "-o", "json"]).stdout)["info"]["status"] == "deployed",
            "lab_controller_exact": restore["deployment_exact"],
            "lab_cluster_rbac_equal": all(
                item.get("spec_labels_annotations_equal") is True or item.get("absent_as_found") is True
                for item in restore["cluster_objects"].values()
            ),
            "crds_as_found": not crd_problems,
        }
        STATE["cleanup_checks"] = checks
        STATE["cases"]["chart_owned_cleanup_and_lab_restore"] = "passed" if all(checks.values()) else "failed"
        STATE["finished"] = now()
        save_state()
        save(
            "cleanup-proof.json",
            {"checks": checks, "restore": restore, "teardown": STATE.get("teardown"), "crd_problems": crd_problems},
        )


def report() -> int:
    cases = STATE.get("cases", {})
    classification = {name: cases.get(name, "missing") for name in REQUIRED_CASES}
    failed = [name for name, value in classification.items() if value != "passed"]
    artifact_hashes = {
        str(path.relative_to(OUT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(OUT.rglob("*"))
        if path.is_file() and path.name not in {REPORT_PATH.name}
    }
    report_doc = {
        "generated": now(),
        "verdict": "accepted" if not failed else "incomplete",
        "exit_code": 1 if failed else 0,
        "context": "docker-desktop",
        "helm": run(HELM[:1] + ["version", "--short"]).stdout.strip(),
        "chart_commit": run(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "chart_values_sha256": hashlib.sha256((CHART / "values.yaml").read_bytes()).hexdigest(),
        "pinned_bootstrap_image": STATE.get("pinned_bootstrap_image"),
        "namespaces": STATE.get("namespaces"),
        "cases": classification,
        "failures": STATE.get("failures", []),
        "cleanup_checks": STATE.get("cleanup_checks"),
        "lab_restore": STATE.get("lab_restore"),
        "window": {"started": STATE.get("started"), "finished": STATE.get("finished")},
        "artifact_hashes": artifact_hashes,
    }
    REPORT_PATH.write_text(json.dumps(report_doc, indent=2, sort_keys=True) + "\n")
    lines = [
        "# PLAT-02.1 full-chart acceptance (docker-desktop)",
        "",
        f"Verdict: **{report_doc['verdict']}** (exit {report_doc['exit_code']}); pinned bootstrap `{report_doc['pinned_bootstrap_image']}`.",
        "",
        *[f"- `{name}`: {value}" for name, value in classification.items()],
        "",
        f"Cleanup checks: `{STATE.get('cleanup_checks')}`",
    ]
    (OUT / "report.md").write_text("\n".join(lines) + "\n")
    return report_doc["exit_code"]


def selftest() -> None:
    """Local controls: evidence writer and case execution must be able to fail."""
    try:
        save("selftest-private.txt", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
    except RuntimeError:
        pass
    else:
        raise RuntimeError("evidence writer accepted private key material")
    saved = copy.deepcopy(STATE.get("cases", {}))
    try:
        execute(["selftest_silent"], lambda: None)
        silent = STATE["cases"]["selftest_silent"]

        def raising() -> None:
            raise RuntimeError("controlled")

        execute(["selftest_raises"], raising)
        raised = STATE["cases"]["selftest_raises"]
    finally:
        STATE["cases"] = saved
        STATE.pop("failures", None)
        save_state()
    if (silent, raised) != ("failed", "failed"):
        raise RuntimeError(f"execute classification controls wrong: {(silent, raised)!r}")
    info = key_info(generate_key("ed25519"))
    if info["algorithm"] != "ed25519" or len(info["key_id"]) != 64:
        raise RuntimeError("key id derivation control failed")
    save("selftest.json", {"private_writer_refused": True, "silent": silent, "raised": raised})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["full", "report", "selftest", "lab-restore"])
    args = parser.parse_args()
    try:
        if args.phase == "full":
            full()
            return report()
        if args.phase == "report":
            return report()
        if args.phase == "lab-restore":
            STATE["lab_restore"] = lab_restore()
            save_state()
            return 0
        selftest()
        return 0
    except Exception as exc:  # noqa: BLE001 - terminal path records a redacted error
        STATE.setdefault("errors", []).append(redact(f"{type(exc).__name__}: {exc}"))
        save_state()
        print(redact(f"{type(exc).__name__}: {exc}"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
