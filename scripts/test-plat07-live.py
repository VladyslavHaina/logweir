#!/usr/bin/env python3
"""Live docker-desktop acceptance for the saved-connection contract over frozen inputs.

PLAT-07.1 (`connection::resolve` — one saved connection per runner Job, a
non-default SASL `passwordKey`, a projected private CA) MERGED INTO PLAT-06.1
(an immutable per-run plan ConfigMap whose `execution-inputs.json` is the
executable snapshot). The two contracts were accepted separately; this harness
exercises the ONE path they became.

    python3 scripts/test-plat07-live.py lab-baseline   # record the shared lab, byte for byte
    python3 scripts/test-plat07-live.py lab-swap       # point the lab at this branch's images
    python3 scripts/test-plat07-live.py setup          # namespace, CA, TLS broker, connections
    python3 scripts/test-plat07-live.py case-a         # plat06 case (a), unchanged
    python3 scripts/test-plat07-live.py case-b         # a non-default passwordKey, merged path
    python3 scripts/test-plat07-live.py case-c         # TLS + private CA, and the wrong-CA control
    python3 scripts/test-plat07-live.py case-d         # the joint conflict
    python3 scripts/test-plat07-live.py case-e         # rotation across a frozen plan
    python3 scripts/test-plat07-live.py case-f         # redaction sweep
    python3 scripts/test-plat07-live.py report
    python3 scripts/test-plat07-live.py cleanup        # namespace + this run's archive objects
    python3 scripts/test-plat07-live.py lab-restore    # the lab, byte-equal, and proved

`lab-baseline`, `lab-swap` and `lab-restore` change SHARED state and require the
cluster lock (`/tmp/logweir-roadmap-run/claude/k8s-lock.sh`); the caller holds
it across the whole run. Everything else lives in one owned namespace.

NOTHING HERE PRINTS OR STORES A SECRET VALUE. Passwords and the CA private key
are generated into a directory outside the artifact tree, referenced by digest
in every record, and used only as `kubectl` input or as a needle the redaction
sweep counts occurrences of. Pod logs are redacted before they are saved. Every
object carries `logweir.dev/test-owner=plat07-live` and the namespace's label
and UID are checked before anything is deleted.
"""

from __future__ import annotations

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
from typing import Any, Callable

ROOT = pathlib.Path(__file__).resolve().parents[1]
STAMP = os.environ.get("LOGWEIR_PLAT07_STAMP", "20260916t0000z")
NS = f"lw-plat07live-{STAMP}"
FIXTURE_NS = "logweir-scram-local"
OWNER = "plat07-live"
OUT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_PLAT07_OUT", "/tmp/logweir-roadmap-run/claude/artifacts/plat07-live"
    )
)
# PRIVATE MATERIAL LIVES HERE AND NOWHERE ELSE. Never under OUT: the artifact
# tree is read by reviewers and copied around, and a CA private key in it would
# be exactly the leak case (f) exists to disprove.
KEYS = pathlib.Path(os.environ.get("LOGWEIR_PLAT07_KEYS", "/tmp/logweir-plat07live/keys"))
STATE_PATH = OUT / "state.json"
K = ["kubectl", "--context", "docker-desktop"]
KN = K + ["-n", NS]
ARCHIVE_BUCKET = "kafka-backups"
ARCHIVE_PREFIX = f"plat07live-{STAMP}"
EVIDENCE_PREFIX = "logweir/backups"
LAB_TOPICS = ["orders", "payments"]
TLS_TOPIC = "orders"
TLS_RECORDS = 12
TLS_HOST = f"kafka-tls.{NS}.svc.cluster.local"
BROKER = "deployment/kafka-tls"
KAFKA_BIN = "/opt/kafka/bin"
SOURCE_PASSWORD_ENV = "LOGWEIR_SOURCE_PASSWORD"
SOURCE_TLS_CA_FILE_ENV = "LOGWEIR_SOURCE_TLS_CA_FILE"
CA_MOUNT = "/connection/source-ca"
CA_VOLUME = "source-ca"
CA_FILE_NAME = "ca.crt"
INPUTS_KEY = "execution-inputs.json"
INPUTS_SHA256_ANNOTATION = "logweir.dev/execution-inputs-sha256"
PLAN_KEYS = ["allowed-clusters.json", "backup.yaml", INPUTS_KEY]
TERMINAL_PLAN_CONFLICT = "PlanConfigMapConflict"
LAB_DEPLOYMENT = "weirkeeper"

OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
KEYS.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE: dict[str, Any] = (
    json.loads(STATE_PATH.read_text())
    if STATE_PATH.exists()
    else {"namespace": NS, "owner": OWNER, "cases": {}}
)


def save() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def run(args: list[str], *, data: str | None = None, check: bool = True, timeout: int = 180):
    """One subprocess, ALWAYS with a timeout, and the exit code read from the
    completed process rather than through a pipe (STANDING RULE 20)."""
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


def ensure(obj: dict[str, Any]) -> dict[str, Any]:
    """Create the object, or return the one already there. A phase is re-runnable
    against a namespace it has already written into; `create` is still the call,
    so a name that exists is never silently overwritten with a different spec."""
    result = run(K + ["create", "-f", "-", "-o", "json"], data=json.dumps(obj), check=False)
    if result.returncode == 0:
        return json.loads(result.stdout)
    if "AlreadyExists" not in result.stderr:
        raise RuntimeError(f"rc={result.returncode}: {result.stderr[-2000:]}")
    return get(obj["kind"].lower(), obj["metadata"]["name"])


def get(kind: str, name: str, namespace: str = NS) -> dict[str, Any]:
    return json.loads(run(K + ["-n", namespace, "get", kind, name, "-o", "json"]).stdout)


def get_opt(kind: str, name: str, namespace: str = NS) -> dict[str, Any] | None:
    result = run(K + ["-n", namespace, "get", kind, name, "-o", "json"], check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def get_list(kind: str, namespace: str = NS) -> list[dict[str, Any]]:
    result = run(K + ["-n", namespace, "get", kind, "-o", "json"], check=False)
    return json.loads(result.stdout)["items"] if result.returncode == 0 else []


def artifact(name: str, body: Any) -> pathlib.Path:
    path = OUT / name
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True))
    return path


def digest(text: str | bytes) -> str:
    raw = text.encode() if isinstance(text, str) else text
    return "sha256:" + hashlib.sha256(raw).hexdigest()


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
    raise RuntimeError(
        f"timeout waiting for {kind}/{name}: {json.dumps((last or {}).get('status'))}"
    )


def terminal(o: dict[str, Any]) -> bool:
    return o.get("status", {}).get("phase") in {"Succeeded", "Failed", "Refused"}


def condition(o: dict[str, Any], kind: str) -> dict[str, Any] | None:
    for c in o.get("status", {}).get("conditions", []) or []:
        if c["type"] == kind:
            return c
    return None


def redact(text: str) -> str:
    """Pod logs and object dumps, with anything credential-shaped removed AND
    every value this run generated replaced by name. The generic pattern alone
    would not catch a bare password echoed without its field name."""
    for label, value in sorted(SECRETS_IN_MEMORY.items()):
        if value:
            text = text.replace(value, f"[REDACTED {label}]")
            text = text.replace(base64.b64encode(value.encode()).decode(), f"[REDACTED {label}/b64]")
    text = re.sub(r"(?i)(password|secret|access[-_]key)[\"'= :]+[^\s\"',}]+", r"\1=[REDACTED]", text)
    return re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.DOTALL,
    )


# Filled by `load_secrets`; never serialised, never printed, never written.
SECRETS_IN_MEMORY: dict[str, str] = {}


def pod_log(job: str, *, tail: int = 60) -> str:
    pods = get_list("pods")
    names = [
        p["metadata"]["name"]
        for p in pods
        if p["metadata"].get("labels", {}).get("batch.kubernetes.io/job-name") == job
    ]
    if not names:
        return "<no pod>"
    return redact(run(KN + ["logs", names[0], f"--tail={tail}"], check=False).stdout)


def raw_pod_logs() -> dict[str, str]:
    """Every pod log in the namespace, UNREDACTED, for the sweep to count in.
    The caller stores counts, never this text."""
    logs: dict[str, str] = {}
    for pod in get_list("pods"):
        name = pod["metadata"]["name"]
        result = run(KN + ["logs", name, "--all-containers=true", "--tail=-1"], check=False)
        logs[name] = result.stdout + result.stderr
    return logs


# ---------------------------------------------------------------------------
# The shared archive, through a client that is not the controller
# ---------------------------------------------------------------------------


def mc(*args: str, check: bool = True) -> str:
    return run(KN + ["exec", "plat07live-mc", "--", "mc", *args], check=check, timeout=180).stdout


def archive_objects(path: str) -> list[dict[str, Any]]:
    """Every object under one bucket path, with its FULL key.

    `mc ls --recursive --json` reports `key` RELATIVE to the path it was given,
    so the prefix is put back here. A cleanup that deleted the relative key
    would address a different object, silently delete nothing, and still be
    able to claim it had run — which is exactly what the before/after listing
    is for."""
    prefix = path.rstrip("/")
    out = mc("ls", "--recursive", "--json", f"local/{ARCHIVE_BUCKET}/{path}", check=False)
    found = []
    for line in out.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("status") != "success" or "key" not in entry:
            continue
        key = entry["key"].lstrip("/")
        full = key if key.startswith(f"{prefix}/") else f"{prefix}/{key}"
        found.append({"key": full, "size": entry.get("size")})
    return sorted(found, key=lambda e: e["key"])


def evidence_bytes(key: str) -> bytes:
    return mc("cat", f"local/{ARCHIVE_BUCKET}/{key}").encode()


FIXTURE_PUBLIC_KEY = pathlib.Path(
    os.environ.get("LOGWEIR_PLAT07_PUBKEY", "/tmp/logweir-scram-e2e/signing.pub.pem")
)


def verify_independently(tag: str, receipt: bytes, sidecar: bytes) -> dict[str, Any]:
    """`docs/verify_scorecard.py` — neither the controller nor the Rust engine —
    over the receipt bytes read back out of the archive."""
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
        timeout=180,
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


# ---------------------------------------------------------------------------
# The shared lab: record it, swap it, put it back byte-equal
# ---------------------------------------------------------------------------

LAB_DIR = "lab-baseline"
SERVER_MANAGED = [
    "resourceVersion",
    "generation",
    "managedFields",
    "creationTimestamp",
    "uid",
    "selfLink",
]


def normalised_deployment(deployment: dict[str, Any]) -> dict[str, Any]:
    """The Deployment with only the fields a server owns removed. Everything a
    client wrote — image, every env entry including both `secretKeyRef`s, pull
    policies, securityContext, probes, serviceAccountName, replicas, strategy —
    stays in the comparison."""
    view = json.loads(json.dumps(deployment))
    view.pop("status", None)
    metadata = view.get("metadata", {})
    for field in SERVER_MANAGED:
        metadata.pop(field, None)
    annotations = metadata.get("annotations", {})
    annotations.pop("deployment.kubernetes.io/revision", None)
    annotations.pop("kubectl.kubernetes.io/last-applied-configuration", None)
    if not annotations:
        metadata.pop("annotations", None)
    return view


def lab_controller_pods() -> list[dict[str, Any]]:
    return [
        pod
        for pod in get_list("pods", namespace=FIXTURE_NS)
        if any(
            c["name"] == LAB_DEPLOYMENT for c in pod["spec"].get("containers", [])
        )
        and pod["metadata"].get("deletionTimestamp") is None
    ]


def lab_controller_image_ids() -> list[dict[str, Any]]:
    """THE RUNNING CONTAINER'S IMAGE ID, not the tag the Deployment names. A tag
    is a mutable pointer: the lab's `weirkeeper:scram-reviewed` may have been
    rebuilt since an earlier run recorded it, and the only honest baseline is
    what the kubelet actually has running right now."""
    found = []
    for pod in lab_controller_pods():
        for status in pod.get("status", {}).get("containerStatuses", []) or []:
            found.append(
                {
                    "pod": pod["metadata"]["name"],
                    "podUid": pod["metadata"]["uid"],
                    "container": status["name"],
                    "image": status.get("image"),
                    "imageID": status.get("imageID"),
                    "ready": status.get("ready"),
                    "restartCount": status.get("restartCount"),
                }
            )
    return found


def crd_summary() -> dict[str, Any]:
    crds: dict[str, Any] = {}
    listing = run(K + ["get", "crd", "-o", "json"], check=False)
    for item in json.loads(listing.stdout or '{"items": []}')["items"]:
        name = item["metadata"]["name"]
        if not name.endswith(".logweir.dev"):
            continue
        crds[name] = {
            "uid": item["metadata"]["uid"],
            "generation": item["metadata"].get("generation"),
            "resourceVersion": item["metadata"]["resourceVersion"],
            "specSha256": digest(json.dumps(item["spec"], sort_keys=True)),
        }
    return crds


def lab_baseline() -> None:
    """Record the shared lab BEFORE anything is touched. Requires the lock."""
    deployment = get("deployment", LAB_DEPLOYMENT, namespace=FIXTURE_NS)
    artifact(f"{LAB_DIR}/deploy-weirkeeper-before.json", deployment)
    normalised = normalised_deployment(deployment)
    artifact(f"{LAB_DIR}/deploy-weirkeeper-before-normalised.json", normalised)
    clusterrole = json.loads(run(K + ["get", "clusterrole", "weirkeeper", "-o", "json"]).stdout)
    artifact(f"{LAB_DIR}/clusterrole-weirkeeper.json", clusterrole)
    images = lab_controller_image_ids()
    crds = crd_summary()
    artifact(f"{LAB_DIR}/crd-summary-before.json", crds)
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in container.get("env", []) if "value" in e}
    baseline = {
        "recordedAt": now(),
        "context": "docker-desktop",
        "namespace": FIXTURE_NS,
        "deploymentUid": deployment["metadata"]["uid"],
        "deploymentResourceVersion": deployment["metadata"]["resourceVersion"],
        "containers": [c["name"] for c in deployment["spec"]["template"]["spec"]["containers"]],
        "controllerImage": container["image"],
        "controllerImagePullPolicy": container.get("imagePullPolicy"),
        "runnerImageEnv": env.get("LOGWEIR_RUNNER_IMAGE"),
        "runnerPullPolicyEnv": env.get("LOGWEIR_RUNNER_PULL_POLICY"),
        "runningContainerImageIds": images,
        "normalisedSpecSha256": digest(json.dumps(normalised, sort_keys=True)),
        "clusterRoleUid": clusterrole["metadata"]["uid"],
        "clusterRoleRulesSha256": digest(json.dumps(clusterrole["rules"], sort_keys=True)),
        "crds": crds,
    }
    STATE["labBaseline"] = baseline
    save()
    artifact(f"{LAB_DIR}/BEFORE.json", baseline)
    log(f"lab baseline recorded: image {baseline['controllerImage']} ids {images}")


CONTROLLER_IMAGE = os.environ.get("LOGWEIR_PLAT07_CONTROLLER_IMAGE", "weirkeeper:plat07live-64e4bb8")
RUNNER_IMAGE = os.environ.get("LOGWEIR_PLAT07_RUNNER_IMAGE", "logweir:plat07live-64e4bb8")


def replace_lab_containers(containers: list[dict[str, Any]]) -> None:
    """A JSON `replace` of the WHOLE containers array, never a strategic merge.

    plat06 §5.6: a strategic-merge patch keys containers by NAME, so a patch
    that guesses the name (`manager`, from `config/manager/deployment.yaml`)
    ADDS a second container instead of editing the one that is there, and two
    controllers reconcile the shared namespace at once. The array is replaced
    whole, built from what the baseline recorded, so the count cannot change.
    """
    patch = json.dumps(
        [{"op": "replace", "path": "/spec/template/spec/containers", "value": containers}]
    )
    run(
        K
        + [
            "-n",
            FIXTURE_NS,
            "patch",
            "deployment",
            LAB_DEPLOYMENT,
            "--type=json",
            "-p",
            patch,
        ]
    )
    run(
        K
        + [
            "-n",
            FIXTURE_NS,
            "rollout",
            "status",
            f"deployment/{LAB_DEPLOYMENT}",
            "--timeout=240s",
        ],
        timeout=300,
    )


def lab_swap() -> None:
    """Apply this branch's CRDs and point the lab at this branch's images."""
    baseline = STATE.get("labBaseline")
    if not baseline:
        raise RuntimeError("run lab-baseline first")
    before = {name: crd_summary().get(name) for name in ["kafkaclusters.logweir.dev"]}
    applied = run(K + ["apply", "-f", "config/crd/kafkaclusters.yaml"]).stdout.strip()
    after = {name: crd_summary().get(name) for name in ["kafkaclusters.logweir.dev"]}
    STATE["crdApply"] = {"at": now(), "output": applied, "before": before, "after": after}
    save()

    deployment = get("deployment", LAB_DEPLOYMENT, namespace=FIXTURE_NS)
    containers = json.loads(json.dumps(deployment["spec"]["template"]["spec"]["containers"]))
    if len(containers) != 1 or containers[0]["name"] != LAB_DEPLOYMENT:
        raise RuntimeError(f"unexpected lab containers: {[c['name'] for c in containers]}")
    containers[0]["image"] = CONTROLLER_IMAGE
    containers[0]["imagePullPolicy"] = "Never"
    env = containers[0].setdefault("env", [])
    seen = {e["name"] for e in env}
    for name, value in [
        ("LOGWEIR_RUNNER_IMAGE", RUNNER_IMAGE),
        ("LOGWEIR_RUNNER_PULL_POLICY", "Never"),
    ]:
        if name in seen:
            for entry in env:
                if entry["name"] == name:
                    entry.pop("valueFrom", None)
                    entry["value"] = value
        else:
            env.append({"name": name, "value": value})
    replace_lab_containers(containers)

    pods = lab_controller_pods()
    if len(pods) != 1:
        raise RuntimeError(f"expected exactly one controller pod, found {[p['metadata']['name'] for p in pods]}")
    live = get("deployment", LAB_DEPLOYMENT, namespace=FIXTURE_NS)
    live_containers = live["spec"]["template"]["spec"]["containers"]
    if len(live_containers) != 1:
        raise RuntimeError(f"the Deployment carries {len(live_containers)} containers after the swap")
    STATE["labSwap"] = {
        "at": now(),
        "controllerImage": CONTROLLER_IMAGE,
        "runnerImage": RUNNER_IMAGE,
        "podCount": len(pods),
        "runningContainerImageIds": lab_controller_image_ids(),
    }
    save()
    artifact(f"{LAB_DIR}/deploy-weirkeeper-swapped.json", live)
    artifact(f"{LAB_DIR}/AFTER-SWAP.json", STATE["labSwap"])
    log(f"lab controller swapped to {CONTROLLER_IMAGE}; one pod: {pods[0]['metadata']['name']}")


def lab_restore() -> None:
    """Put the containers array back exactly as the baseline recorded it, and
    prove byte-equality of the normalised spec."""
    baseline = STATE.get("labBaseline")
    if not baseline:
        raise RuntimeError("no recorded baseline to restore from")
    recorded = json.loads((OUT / f"{LAB_DIR}/deploy-weirkeeper-before.json").read_text())
    replace_lab_containers(recorded["spec"]["template"]["spec"]["containers"])
    live = get("deployment", LAB_DEPLOYMENT, namespace=FIXTURE_NS)
    artifact(f"{LAB_DIR}/deploy-weirkeeper-restored.json", live)
    restored = normalised_deployment(live)
    artifact(f"{LAB_DIR}/deploy-weirkeeper-restored-normalised.json", restored)
    restored_sha = digest(json.dumps(restored, sort_keys=True))
    pods = lab_controller_pods()
    proof = {
        "at": now(),
        "baselineNormalisedSpecSha256": baseline["normalisedSpecSha256"],
        "restoredNormalisedSpecSha256": restored_sha,
        "byteEqualAfterNormalisation": restored_sha == baseline["normalisedSpecSha256"],
        "deploymentUidUnchanged": live["metadata"]["uid"] == baseline["deploymentUid"],
        "baselineRunningImageIds": baseline["runningContainerImageIds"],
        "restoredRunningImageIds": lab_controller_image_ids(),
        "controllerPodCount": len(pods),
        "crdsAfter": crd_summary(),
    }
    STATE["labRestore"] = proof
    save()
    artifact(f"{LAB_DIR}/RESTORE-PROOF.json", proof)
    if not proof["byteEqualAfterNormalisation"]:
        raise RuntimeError(f"the lab Deployment is NOT byte-equal to its baseline: {proof}")
    log("lab restored byte-equal to the recorded baseline")


def lab_observe(label: str) -> dict[str, Any]:
    """Every object the lab owns, with the fields a restore must not disturb."""
    snapshot: dict[str, Any] = {"at": now(), "label": label}
    for kind in ["kafkacluster", "backup", "backupschedule", "restore", "approval"]:
        rows = []
        for item in get_list(kind, namespace=FIXTURE_NS):
            status = item.get("status") or {}
            rows.append(
                {
                    "name": item["metadata"]["name"],
                    "uid": item["metadata"]["uid"],
                    "phase": status.get("phase"),
                    "reason": status.get("reason"),
                    "exitCode": status.get("exitCode"),
                    "reachable": status.get("reachable"),
                    "clusterId": status.get("clusterId"),
                    "verification": (status.get("evidence") or {}).get("verification"),
                }
            )
        snapshot[kind] = sorted(rows, key=lambda r: r["name"])
    artifact(f"{LAB_DIR}/lab-objects-{label}.json", snapshot)
    return snapshot


def lab_controller_log(since: str = "10m") -> str:
    pods = lab_controller_pods()
    if not pods:
        return "<no controller pod>"
    name = pods[0]["metadata"]["name"]
    result = run(
        K + ["-n", FIXTURE_NS, "logs", name, f"--since={since}", "--tail=-1"], check=False
    )
    return result.stdout + result.stderr


# ---------------------------------------------------------------------------
# A private CA, a broker certificate, and two SCRAM passwords
# ---------------------------------------------------------------------------


def openssl(*args: str, timeout: int = 120) -> None:
    run(["openssl", *args], timeout=timeout)


def generate_ca_and_broker_certificate() -> dict[str, Any]:
    """TWO CAs ON PURPOSE. `ca.crt` signs the broker; `wrong-ca.crt` signs
    nothing and exists so the negative control is a real TRUST failure and not
    a missing file. Both private keys stay under KEYS and reach no Kubernetes
    object and no artifact."""
    ca_crt = KEYS / "ca.crt"
    if not ca_crt.is_file():
        openssl(
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-keyout", str(KEYS / "ca.key"), "-out", str(ca_crt),
            "-subj", "/CN=logweir-plat07live-ca",
        )
        openssl(
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-keyout", str(KEYS / "wrong-ca.key"), "-out", str(KEYS / "wrong-ca.crt"),
            "-subj", "/CN=logweir-plat07live-wrong-ca",
        )
        openssl(
            "req", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(KEYS / "server.key"), "-out", str(KEYS / "server.csr"),
            "-subj", f"/CN={TLS_HOST}",
        )
        (KEYS / "san.cnf").write_text(
            f"subjectAltName = DNS:{TLS_HOST}, DNS:kafka-tls.{NS}.svc, DNS:kafka-tls\n"
            "extendedKeyUsage = serverAuth\n"
        )
        openssl(
            "x509", "-req", "-in", str(KEYS / "server.csr"),
            "-CA", str(ca_crt), "-CAkey", str(KEYS / "ca.key"), "-CAcreateserial",
            "-out", str(KEYS / "server.crt"), "-days", "2",
            "-extfile", str(KEYS / "san.cnf"),
        )
        # The apache/kafka image's `configure` wrapper reads a keystore from
        # /etc/kafka/secrets with two credential files beside it; it has no PEM
        # path. The keystore holds the SAME certificate this run's CA signed.
        openssl(
            "pkcs12", "-export", "-in", str(KEYS / "server.crt"),
            "-inkey", str(KEYS / "server.key"), "-name", "kafka",
            "-out", str(KEYS / "server.p12"),
            "-passout", f"pass:{SECRETS_IN_MEMORY['keystore']}",
        )
        for name in ["ca.key", "wrong-ca.key", "server.key", "server.p12"]:
            (KEYS / name).chmod(0o600)
    # ONLY the public certificates are copied into the artifact tree.
    artifact("certs/ca.crt", ca_crt.read_text())
    artifact("certs/wrong-ca.crt", (KEYS / "wrong-ca.crt").read_text())
    artifact("certs/server.crt", (KEYS / "server.crt").read_text())
    subject = run(
        ["openssl", "x509", "-in", str(KEYS / "server.crt"), "-noout", "-subject", "-issuer",
         "-ext", "subjectAltName"]
    ).stdout
    artifact("certs/server-certificate.txt", subject)
    return {
        "caCertSha256": digest(ca_crt.read_bytes()),
        "wrongCaCertSha256": digest((KEYS / "wrong-ca.crt").read_bytes()),
        "serverCertSha256": digest((KEYS / "server.crt").read_bytes()),
        "brokerCertificate": subject.strip().splitlines(),
        "privateKeysLiveIn": str(KEYS),
    }


PASSWORD_FILE = KEYS / "passwords.json"


def load_secrets() -> None:
    """Generate (once) and load this run's credential values into memory.

    The file lives beside the private keys, mode 0600, outside the artifact
    tree. `STATE` records only the digest of each value."""
    if PASSWORD_FILE.is_file():
        SECRETS_IN_MEMORY.update(json.loads(PASSWORD_FILE.read_text()))
        return
    SECRETS_IN_MEMORY.update(
        {
            "tls-password-1": "P" + secrets.token_urlsafe(18),
            "tls-password-2": "R" + secrets.token_urlsafe(18),
            "keystore": secrets.token_urlsafe(18),
        }
    )
    PASSWORD_FILE.write_text(json.dumps(SECRETS_IN_MEMORY, indent=2, sort_keys=True))
    PASSWORD_FILE.chmod(0o600)


def secret_digests() -> dict[str, str]:
    return {label: digest(value) for label, value in sorted(SECRETS_IN_MEMORY.items())}


def broker_exec(args: list[str], *, check: bool = True, timeout: int = 240):
    return run(KN + ["exec", BROKER, "--", *args], check=check, timeout=timeout)


def set_scram_credential(user: str, password: str) -> None:
    """Set a SCRAM-SHA-512 credential on THIS RUN'S OWN broker, over its
    in-pod PLAINTEXT listener. Nothing is printed."""
    broker_exec(
        [
            f"{KAFKA_BIN}/kafka-configs.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--alter",
            "--add-config",
            f"SCRAM-SHA-512=[password={password}]",
            "--entity-type",
            "users",
            "--entity-name",
            user,
        ]
    )


def copy_secret(name: str, *, rename: str | None = None, rekey: dict[str, str] | None = None) -> dict[str, Any]:
    """Copy a lab Secret into this namespace, optionally under a different
    object name and different DATA KEYS. The base64 payload is moved without
    being decoded, printed or stored."""
    source = get("secret", name, namespace=FIXTURE_NS)
    data = dict(source["data"])
    if rekey:
        data = {new: data[old] for old, new in rekey.items()}
    created = apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned_metadata(rename or name),
            "type": source.get("type", "Opaque"),
            "data": data,
        }
    )
    return {
        "name": created["metadata"]["name"],
        "uid": created["metadata"]["uid"],
        "keys": sorted(data),
        "copiedFrom": f"{FIXTURE_NS}/{name}",
    }


def literal_secret(name: str, values: dict[str, str]) -> dict[str, Any]:
    created = apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned_metadata(name),
            "type": "Opaque",
            "data": {k: base64.b64encode(v.encode()).decode() for k, v in values.items()},
        }
    )
    return {"name": created["metadata"]["name"], "uid": created["metadata"]["uid"], "keys": sorted(values)}


def binary_secret(name: str, files: dict[str, pathlib.Path], values: dict[str, str]) -> dict[str, Any]:
    data = {k: base64.b64encode(p.read_bytes()).decode() for k, p in files.items()}
    data.update({k: base64.b64encode(v.encode()).decode() for k, v in values.items()})
    created = apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned_metadata(name),
            "type": "Opaque",
            "data": data,
        }
    )
    return {"name": created["metadata"]["name"], "uid": created["metadata"]["uid"], "keys": sorted(data)}


# ---------------------------------------------------------------------------
# Fixtures: the TLS broker, the archive client, and the saved connections
# ---------------------------------------------------------------------------


def tls_broker_objects() -> list[dict[str, Any]]:
    """A SASL_SSL/SCRAM-SHA-512 broker in THIS namespace, serving a certificate
    signed by the CA this run generated.

    In its own namespace and not the shared lab's: the lab broker has no TLS
    listener, and adding one would change a shared fixture for every other
    worker. The in-pod PLAINTEXT listener is advertised as `localhost` so only
    the pod itself can use it — the credential and topic setup below — while
    every dial from outside is SASL_SSL.
    """
    return [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": owned_metadata("kafka-tls"),
            "spec": {
                "selector": {"app": "kafka-tls"},
                "ports": [{"name": "sasl-ssl", "port": 9096, "targetPort": 9096}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": owned_metadata("kafka-tls"),
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "kafka-tls"}},
                "template": {
                    "metadata": {
                        "labels": {"app": "kafka-tls", "logweir.dev/test-owner": OWNER}
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "volumes": [
                            {
                                "name": "certs",
                                "secret": {"secretName": "kafka-tls-broker", "defaultMode": 0o444},
                            }
                        ],
                        "containers": [
                            {
                                "name": "kafka",
                                "image": "apache/kafka:3.7.1",
                                "imagePullPolicy": "IfNotPresent",
                                "volumeMounts": [
                                    {
                                        "name": "certs",
                                        "mountPath": "/etc/kafka/secrets",
                                        "readOnly": True,
                                    }
                                ],
                                "env": [
                                    {"name": k, "value": v}
                                    for k, v in [
                                        ("CLUSTER_ID", "PLAT07LiveTlsAAAAAAAAA"),
                                        ("KAFKA_NODE_ID", "1"),
                                        ("KAFKA_PROCESS_ROLES", "broker,controller"),
                                        (
                                            "KAFKA_LISTENERS",
                                            "PLAINTEXT://0.0.0.0:9092,"
                                            "CONTROLLER://0.0.0.0:9093,SASLSSL://0.0.0.0:9096",
                                        ),
                                        (
                                            "KAFKA_ADVERTISED_LISTENERS",
                                            "PLAINTEXT://localhost:9092,"
                                            f"SASLSSL://{TLS_HOST}:9096",
                                        ),
                                        ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
                                        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                                        ("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
                                        (
                                            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,"
                                            "SASLSSL:SASL_SSL",
                                        ),
                                        ("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512"),
                                        (
                                            "KAFKA_LISTENER_NAME_SASLSSL_SCRAM___SHA___512"
                                            "_SASL_JAAS_CONFIG",
                                            "org.apache.kafka.common.security.scram."
                                            "ScramLoginModule required;",
                                        ),
                                        ("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"),
                                        ("KAFKA_SSL_KEYSTORE_FILENAME", "server.p12"),
                                        ("KAFKA_SSL_KEYSTORE_CREDENTIALS", "keystore_creds"),
                                        ("KAFKA_SSL_KEY_CREDENTIALS", "key_creds"),
                                        ("KAFKA_SSL_CLIENT_AUTH", "none"),
                                        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                                        (
                                            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                                            "1",
                                        ),
                                        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                                        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                                        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
                                        ("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs"),
                                        ("KAFKA_HEAP_OPTS", "-Xmx512M -Xms256M"),
                                    ]
                                ],
                                "readinessProbe": {
                                    "tcpSocket": {"port": 9096},
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


def mc_pod() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": owned_metadata("plat07live-mc"),
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "mc",
                    "image": "docker.io/vladyslavhaina/mc-mirror@sha256:9c7cbc3f47b092d52b73124fb9ab12f3266534c23c283b2e984d07408c9ff381",
                    "imagePullPolicy": "IfNotPresent",
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
                                "secretKeyRef": {"name": "logweir-s3", "key": "access-key-id"}
                            },
                        },
                        {
                            "name": "AWS_SECRET_ACCESS_KEY",
                            "valueFrom": {
                                "secretKeyRef": {"name": "logweir-s3", "key": "secret-access-key"}
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


def kafka_cluster(
    name: str,
    *,
    servers: list[str],
    mode: str,
    username: str | None = None,
    secret: dict[str, str] | None = None,
    tls: bool = False,
    tls_ca: dict[str, Any] | None = None,
) -> dict[str, Any]:
    auth: dict[str, Any] = {"mode": mode, "tls": tls}
    if username:
        auth["username"] = username
    if secret:
        auth["secretRef"] = secret
    if tls_ca:
        auth["tlsCa"] = tls_ca
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": owned_metadata(name),
        "spec": {"bootstrapServers": servers, "auth": auth, "role": "source"},
    }


def backup_object(
    name: str,
    source: str,
    *,
    topics: list[str],
    archive_path: str,
    deadline: int = 600,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    metadata = owned_metadata(name)
    if annotations:
        metadata["annotations"] = annotations
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": metadata,
        "spec": {
            "sourceRef": {"name": source},
            "topics": topics,
            "archive": {
                "url": f"s3://{ARCHIVE_BUCKET}/{ARCHIVE_PREFIX}/{archive_path}",
                "secretRef": {"name": "logweir-s3"},
            },
            "triggeredBy": "manual",
            "deadlineSeconds": deadline,
        },
    }


LAB_SOURCE = [f"kafka-source.{FIXTURE_NS}.svc.cluster.local:9096"]
TLS_SOURCE = [f"{TLS_HOST}:9096"]
CA_FROM_CONFIG_MAP = {"configMapKeyRef": {"name": "kafka-ca", "key": CA_FILE_NAME}}
WRONG_CA_FROM_CONFIG_MAP = {"configMapKeyRef": {"name": "kafka-wrong-ca", "key": CA_FILE_NAME}}


def setup() -> None:
    load_secrets()
    existing = get_opt("namespace", NS, namespace="default")
    if existing is None:
        run(K + ["create", "namespace", NS])
        run(K + ["label", "namespace", NS, f"logweir.dev/test-owner={OWNER}"])
    elif existing["metadata"].get("labels", {}).get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError(f"refusing to reuse unowned namespace {NS}")
    STATE["namespaceUid"] = get("namespace", NS, namespace="default")["metadata"]["uid"]
    STATE["secretDigests"] = secret_digests()
    save()

    apply(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": owned_metadata("logweir-runner"),
            "automountServiceAccountToken": False,
        }
    )
    secrets_created = {
        # case (a) reads the lab's own Secret under its DEFAULT key `password`.
        "source-scram": copy_secret("source-scram"),
        # case (b) reads the SAME value under a NON-DEFAULT key name.
        "lab-sasl": copy_secret("source-scram", rename="lab-sasl", rekey={"password": "sasl-password"}),
        "logweir-s3": copy_secret("logweir-s3"),
        "logweir-signing-key": copy_secret("logweir-signing-key"),
        "tls-sasl": literal_secret("tls-sasl", {"sasl-password": SECRETS_IN_MEMORY["tls-password-1"]}),
    }

    certs = generate_ca_and_broker_certificate()
    secrets_created["kafka-tls-broker"] = binary_secret(
        "kafka-tls-broker",
        {"server.p12": KEYS / "server.p12"},
        {
            "keystore_creds": SECRETS_IN_MEMORY["keystore"],
            "key_creds": SECRETS_IN_MEMORY["keystore"],
        },
    )
    for cm_name, source in [("kafka-ca", KEYS / "ca.crt"), ("kafka-wrong-ca", KEYS / "wrong-ca.crt")]:
        apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": owned_metadata(cm_name),
                "data": {CA_FILE_NAME: source.read_text()},
            }
        )
    STATE["secrets"] = secrets_created
    STATE["certs"] = certs
    save()

    for obj in tls_broker_objects():
        apply(obj)
    run(
        KN + ["rollout", "status", "deployment/kafka-tls", "--timeout=240s"],
        timeout=300,
    )
    set_scram_credential("tls-user", SECRETS_IN_MEMORY["tls-password-1"])
    topics = broker_exec(
        [f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]
    ).stdout.split()
    if TLS_TOPIC not in topics:
        broker_exec(
            [
                f"{KAFKA_BIN}/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--create",
                "--topic",
                TLS_TOPIC,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ]
        )
        payload = "".join(f"plat07live-{i}\n" for i in range(TLS_RECORDS))
        run(
            KN
            + [
                "exec",
                "-i",
                BROKER,
                "--",
                "/bin/sh",
                "-c",
                f"{KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server localhost:9092 "
                f"--topic {TLS_TOPIC}",
            ],
            data=payload,
            timeout=240,
        )
    STATE["tlsBroker"] = {
        "topic": TLS_TOPIC,
        "records": TLS_RECORDS,
        "bootstrap": TLS_SOURCE,
        "scramUser": "tls-user",
    }
    save()

    if get_opt("pod", "plat07live-mc") is None:
        apply(mc_pod())
        run(KN + ["wait", "--for=condition=Ready", "pod/plat07live-mc", "--timeout=180s"])

    connections = [
        # (a) the lab's SCRAM listener under the DEFAULT password key.
        kafka_cluster(
            "lab-default",
            servers=LAB_SOURCE,
            mode="scramSha512",
            username="scram-user",
            secret={"name": "source-scram"},
        ),
        # (b) the same broker and the same value under a NON-DEFAULT key.
        kafka_cluster(
            "lab-scram-key",
            servers=LAB_SOURCE,
            mode="scramSha512",
            username="scram-user",
            secret={"name": "lab-sasl", "passwordKey": "sasl-password"},
        ),
        # (c) TLS with this run's private CA, and the wrong-CA control.
        kafka_cluster(
            "tls-ca",
            servers=TLS_SOURCE,
            mode="scramSha512",
            username="tls-user",
            secret={"name": "tls-sasl", "passwordKey": "sasl-password"},
            tls=True,
            tls_ca=CA_FROM_CONFIG_MAP,
        ),
        kafka_cluster(
            "tls-wrong-ca",
            servers=TLS_SOURCE,
            mode="scramSha512",
            username="tls-user",
            secret={"name": "tls-sasl", "passwordKey": "sasl-password"},
            tls=True,
            tls_ca=WRONG_CA_FROM_CONFIG_MAP,
        ),
        # (d) the connection the joint conflict deletes and recreates.
        kafka_cluster(
            "joint-ca",
            servers=TLS_SOURCE,
            mode="scramSha512",
            username="tls-user",
            secret={"name": "tls-sasl", "passwordKey": "sasl-password"},
            tls=True,
            tls_ca=CA_FROM_CONFIG_MAP,
        ),
    ]
    created = {}
    for obj in connections:
        body = apply(obj)
        created[body["metadata"]["name"]] = {
            "uid": body["metadata"]["uid"],
            "generation": body["metadata"]["generation"],
            "resourceVersion": body["metadata"]["resourceVersion"],
        }
    STATE["connections"] = created
    save()

    reachable = {}
    for name in ["lab-default", "lab-scram-key", "tls-ca"]:
        cluster = wait_for(
            "kafkacluster", name, lambda o: o.get("status", {}).get("reachable") is True, seconds=300
        )
        reachable[name] = cluster["status"].get("clusterId")
    STATE["reachable"] = reachable
    STATE["cases"]["setup"] = "passed"
    save()
    artifact("setup.json", {k: STATE[k] for k in ["connections", "reachable", "tlsBroker", "certs"]})
    log(f"namespace {NS} ready; reachable {reachable}")


# ---------------------------------------------------------------------------
# Shared assertions
# ---------------------------------------------------------------------------

# Names that must never appear anywhere in a plan ConfigMap. The CA's ConfigMap
# name `kafka-ca` is deliberately NOT here: the frozen snapshot records the CA
# REFERENCE, because a changed trust anchor must be detectable, and a CA
# certificate is public material. Every CREDENTIAL name and key still is.
FORBIDDEN_IN_PLAN = [
    "source-scram",
    "lab-sasl",
    "tls-sasl",
    "logweir-s3",
    "logweir-signing-key",
    "sasl-password",
    "password",
    "access-key-id",
    "secret-access-key",
]


def frozen_inputs(backup: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """The recorded execution, the ConfigMap it names verified against it, and
    the parsed snapshot."""
    name = backup["metadata"]["name"]
    execution = backup["status"]["execution"]
    cm = get("configmap", execution["inputsRef"]["name"])
    stored = cm["data"][INPUTS_KEY]
    sha = digest(stored)
    if execution["inputsSha256"] != sha:
        raise RuntimeError(f"{name}: status digest {execution['inputsSha256']} != {sha}")
    if cm["metadata"]["annotations"][INPUTS_SHA256_ANNOTATION] != sha:
        raise RuntimeError(f"{name}: the ConfigMap annotation is not the snapshot digest")
    if cm.get("immutable") is not True:
        raise RuntimeError(f"{name}: the plan ConfigMap is not immutable")
    if sorted(cm["data"]) != PLAN_KEYS:
        raise RuntimeError(f"{name}: unexpected plan keys {sorted(cm['data'])}")
    owners = cm["metadata"]["ownerReferences"]
    if len(owners) != 1 or owners[0]["uid"] != backup["metadata"]["uid"]:
        raise RuntimeError(f"{name}: the plan is not owned by exactly this Backup")
    body = json.dumps(cm)
    named = [token for token in FORBIDDEN_IN_PLAN if token in body]
    if named:
        raise RuntimeError(f"{name}: the plan names {named}")
    return execution, cm, json.loads(stored)


def job_facts(name: str) -> dict[str, Any]:
    job = get("job", name)
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    return {
        "uid": job["metadata"]["uid"],
        "args": container["args"],
        "image": container["image"],
        "annotations": job["metadata"].get("annotations", {}),
        "serviceAccountName": pod.get("serviceAccountName"),
        "automountServiceAccountToken": pod.get("automountServiceAccountToken"),
        "volumes": {
            v["name"]: {
                "configMap": v.get("configMap"),
                "secret": {k: val for k, val in (v.get("secret") or {}).items() if k != "defaultMode"}
                or None,
            }
            for v in pod.get("volumes", [])
        },
        "volumeMounts": {
            m["name"]: {"mountPath": m["mountPath"], "readOnly": m.get("readOnly")}
            for m in container.get("volumeMounts", [])
        },
        "env": {e["name"]: e.get("value") for e in container["env"] if "value" in e},
        "envFrom": {
            e["name"]: e["valueFrom"]["secretKeyRef"]
            for e in container["env"]
            if "valueFrom" in e and "secretKeyRef" in e["valueFrom"]
        },
    }


def derived_argv(execution_id: str, trigger: str = "manual") -> list[str]:
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


def jobs_named(name: str) -> list[str]:
    return [j["metadata"]["uid"] for j in get_list("jobs") if j["metadata"]["name"] == name]


def plan_fingerprint(cm_name: str) -> dict[str, Any] | None:
    cm = get_opt("configmap", cm_name)
    if cm is None:
        return None
    return {
        "name": cm_name,
        "uid": cm["metadata"]["uid"],
        "resourceVersion": cm["metadata"]["resourceVersion"],
        "immutable": cm.get("immutable"),
        "annotations": cm["metadata"].get("annotations", {}),
        "dataSha256": {k: digest(v) for k, v in sorted(cm.get("data", {}).items())},
    }


def identity_free(snapshot: dict[str, Any]) -> dict[str, Any]:
    """The snapshot with IDENTITY and nothing else removed.

    Identity reaches the document in exactly three derived places, all from one
    value: `execution.id`, `execution.backup` (name + uid) and the
    `--backup-id-override` token of `runner.args`. Everything else — source,
    topics, archive, runner settings, deadline, version — is what two runs of
    the same spec must agree on. plat06 case (f) learned this the hard way: a
    normaliser that strips `execution` but not the argv reports a difference
    that is identity wearing another hat."""
    view = json.loads(json.dumps(snapshot))
    view["execution"]["id"] = "<identity>"
    view["execution"]["backup"]["name"] = "<identity>"
    view["execution"]["backup"]["uid"] = "<identity>"
    args = view["runner"]["args"]
    if "--backup-id-override" in args:
        args[args.index("--backup-id-override") + 1] = "<identity>"
    return view


def verify_succeeded(name: str, tag: str, *, trigger: str = "manual") -> dict[str, Any]:
    """The whole success contract for one run, recorded as artifacts."""
    backup = wait_for("backup", name, terminal, seconds=900)
    artifact(f"{tag}-backup.json", backup)
    status = backup["status"]
    if status.get("exitCode") != 0 or status.get("phase") != "Succeeded":
        artifact(f"{tag}-failed-log.txt", pod_log(name))
        raise RuntimeError(f"{name}: {json.dumps(status)}")
    execution, cm, snapshot = frozen_inputs(backup)
    artifact(f"{tag}-plan-configmap.json", cm)
    artifact(f"{tag}-snapshot.json", snapshot)
    if snapshot["execution"]["trigger"] != trigger:
        raise RuntimeError(f"{name}: snapshot trigger {snapshot['execution']['trigger']}")
    if snapshot["execution"]["backup"]["uid"] != backup["metadata"]["uid"]:
        raise RuntimeError(f"{name}: snapshot is not bound to this object's UID")
    facts = job_facts(name)
    artifact(f"{tag}-job.json", facts)
    if facts["args"] != derived_argv(execution["id"], trigger):
        raise RuntimeError(f"{name}: Job args are not the derived argv: {facts['args']}")
    if facts["annotations"].get(INPUTS_SHA256_ANNOTATION) != execution["inputsSha256"]:
        raise RuntimeError(f"{name}: the Job does not carry the recorded inputs digest")
    if status.get("backupId") != execution["id"]:
        raise RuntimeError(f"{name}: status.backupId {status.get('backupId')}")
    evidence = status["evidence"]
    receipt = evidence_bytes(evidence["receiptKey"])
    if digest(receipt) != evidence["receiptSha256"]:
        raise RuntimeError(f"{name}: receipt digest {digest(receipt)} != {evidence['receiptSha256']}")
    document = json.loads(receipt)
    if document["backup_id"] != execution["id"]:
        raise RuntimeError(f"{name}: the receipt names backup_id {document['backup_id']}")
    sidecar = evidence_bytes(evidence["sidecarKey"])
    artifact(f"{tag}-receipt.json", receipt.decode())
    artifact(f"{tag}-receipt.sig", sidecar.decode())
    artifact(f"{tag}-pod-log.txt", pod_log(name))
    independent = verify_independently(tag, receipt, sidecar)
    wait_for(
        "backup",
        name,
        lambda o: o["status"].get("evidence", {}).get("verification", {}).get("result") == "Valid",
        seconds=300,
    )
    record = {
        "namespace": NS,
        "backup": name,
        "backupUid": backup["metadata"]["uid"],
        "execution": execution,
        "configMapUid": cm["metadata"]["uid"],
        "configMapImmutable": cm.get("immutable"),
        "configMapKeys": sorted(cm["data"]),
        "planNamesNoCredential": True,
        "jobUid": facts["uid"],
        "jobArgs": facts["args"],
        "jobImage": facts["image"],
        "jobServiceAccount": facts["serviceAccountName"],
        "jobEnvFromSecret": facts["envFrom"],
        "jobCaEnv": facts["env"].get(SOURCE_TLS_CA_FILE_ENV),
        "podExitCode": status.get("exitCode"),
        "phase": status.get("phase"),
        "records": status.get("records"),
        "evidence": {k: evidence.get(k) for k in ["receiptKey", "sidecarKey", "receiptSha256"]},
        "independentVerification": independent,
        "sourceClusterId": document["source"]["cluster_id"],
        "snapshotSource": snapshot["source"],
        "identityFreeSha256": digest(json.dumps(identity_free(snapshot), sort_keys=True)),
        "artifacts": [
            f"{tag}-backup.json",
            f"{tag}-plan-configmap.json",
            f"{tag}-snapshot.json",
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
    return record


def expect_failed(name: str, tag: str) -> dict[str, Any]:
    """A run that MUST fail, with its runner log kept as the proof of WHERE."""
    backup = wait_for("backup", name, terminal, seconds=900)
    artifact(f"{tag}-backup.json", backup)
    status = backup["status"]
    raw = pod_log(name, tail=200)
    artifact(f"{tag}-pod-log.txt", raw)
    record = {
        "backup": name,
        "phase": status.get("phase"),
        "reason": status.get("reason"),
        "exitCode": status.get("exitCode"),
        "conditions": [
            {k: c.get(k) for k in ["type", "status", "reason"]}
            for c in status.get("conditions", []) or []
        ],
        "logSha256": digest(raw),
    }
    if status.get("phase") != "Failed":
        raise RuntimeError(f"{name} did not fail: {json.dumps(status)}")
    STATE["cases"][tag] = record
    save()
    return record


# ---------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------


def case_a() -> None:
    """(a) plat06 case (a), unchanged by the merge: a manual Backup with NO
    annotation freezes its inputs, runs, and produces a verified receipt."""
    name = "plat07live-manual"
    created = ensure(
        backup_object(name, "lab-default", topics=LAB_TOPICS, archive_path="manual")
    )
    if created["metadata"].get("annotations", {}).get("logweir.dev/runner-argv"):
        raise RuntimeError("the fixture must carry no runner-argv annotation")
    record = verify_succeeded(name, "case-a")
    backup = get("backup", name)
    if backup["status"]["execution"]["id"] != created["metadata"]["uid"]:
        raise RuntimeError("a manual run's identity is its object UID")
    if condition(backup, "RunnerArgvAnnotationIgnored") is not None:
        raise RuntimeError("an unannotated Backup raises no annotation condition")
    # Re-applying the identical object is not a new run.
    again = apply(backup_object(name, "lab-default", topics=LAB_TOPICS, archive_path="manual"))
    jobs = jobs_named(name)
    if again["metadata"]["uid"] != created["metadata"]["uid"] or len(jobs) != 1:
        raise RuntimeError(f"re-applying minted a new object or a second Job: {jobs}")
    # The connection contract did NOT change what a no-CA snapshot carries.
    if "tlsCa" in record["snapshotSource"]:
        raise RuntimeError("a connection that names no CA must freeze no tlsCa key")
    STATE["cases"]["case-a-idempotence"] = {
        "reappliedUid": again["metadata"]["uid"],
        "jobsForName": jobs,
        "snapshotHasNoTlsCa": True,
        "passwordEnv": record["jobEnvFromSecret"].get(SOURCE_PASSWORD_ENV),
    }
    save()


def case_b() -> None:
    """(b) A SCRAM backup whose password lives under a NON-DEFAULT data key,
    through the merged freeze path."""
    name = "plat07live-scram-key"
    ensure(backup_object(name, "lab-scram-key", topics=LAB_TOPICS, archive_path="scram-key"))
    record = verify_succeeded(name, "case-b")
    reference = record["jobEnvFromSecret"].get(SOURCE_PASSWORD_ENV)
    if reference != {"name": "lab-sasl", "key": "sasl-password"}:
        raise RuntimeError(f"{name}: the password reference is {reference}")
    cm = get("configmap", f"{name}-plan")
    snapshot = json.loads(cm["data"][INPUTS_KEY])
    # WHAT THE PLAN NAMES, recorded rather than asserted away.
    named = {
        "source": snapshot["source"],
        "topics": snapshot["topics"],
        "archiveUrl": snapshot["archive"]["url"],
        "archiveStorageKeys": sorted(snapshot["archive"]["storage"]),
        "addressingEnvNames": [e["name"] for e in snapshot["archive"].get("addressingEnv", [])],
        "runnerKeys": sorted(snapshot["runner"]),
        "planKeys": sorted(cm["data"]),
        "backupYamlSha256": digest(cm["data"]["backup.yaml"]),
    }
    artifact("case-b-plan-names.json", named)
    for token in FORBIDDEN_IN_PLAN:
        if token in json.dumps(cm):
            raise RuntimeError(f"{name}: the plan names `{token}`")
    if "auth" not in snapshot["source"] or "username" not in snapshot["source"]["auth"]:
        raise RuntimeError("the snapshot must still name the public SCRAM identity")
    STATE["cases"]["case-b-reference"] = {
        "passwordEnv": reference,
        "planNames": named,
        "planNamesNoPasswordKeyName": "sasl-password" not in json.dumps(cm),
        "snapshotAuth": snapshot["source"]["auth"],
    }
    save()
    log(f"case-b: {SOURCE_PASSWORD_ENV} <- secretKeyRef {reference['name']}/{reference['key']}")


def case_c() -> None:
    """(c) TLS with a private CA: the run succeeds with `tlsCa`, and the
    wrong-CA control fails at the TLS HANDSHAKE on a `sasl_ssl://` connection —
    refused, never dialled in the clear."""
    name = "plat07live-tls-ca"
    ensure(backup_object(name, "tls-ca", topics=[TLS_TOPIC], archive_path="tls-ca"))
    record = verify_succeeded(name, "case-c")
    source = record["snapshotSource"]
    expected_ca = {"kind": "configMap", "name": "kafka-ca", "key": CA_FILE_NAME}
    if source.get("tlsCa") != expected_ca:
        raise RuntimeError(f"{name}: the snapshot's tlsCa is {source.get('tlsCa')}")
    if source["auth"].get("tls") is not True:
        raise RuntimeError(f"{name}: the snapshot does not record a TLS transport")
    facts = job_facts(name)
    if facts["env"].get(SOURCE_TLS_CA_FILE_ENV) != f"{CA_MOUNT}/{CA_FILE_NAME}":
        raise RuntimeError(f"{name}: the CA path env is {facts['env'].get(SOURCE_TLS_CA_FILE_ENV)}")
    volume = facts["volumes"].get(CA_VOLUME, {}).get("configMap") or {}
    if volume.get("name") != "kafka-ca" or volume.get("items") != [
        {"key": CA_FILE_NAME, "path": CA_FILE_NAME}
    ]:
        raise RuntimeError(f"{name}: the CA volume is {volume}")
    if volume.get("optional") is True:
        raise RuntimeError(f"{name}: the CA volume is optional; a missing CA must not be silent")
    mount = facts["volumeMounts"].get(CA_VOLUME)
    if mount != {"mountPath": CA_MOUNT, "readOnly": True}:
        raise RuntimeError(f"{name}: the CA mount is {mount}")

    # THE NEGATIVE CONTROL. A CA that signed nothing in this run — a real trust
    # failure, not a missing file.
    wrong = "plat07live-tls-wrong-ca"
    ensure(
        backup_object(wrong, "tls-wrong-ca", topics=[TLS_TOPIC], archive_path="tls-wrong-ca", deadline=180)
    )
    failure = expect_failed(wrong, "case-c-wrong-ca")
    raw = (OUT / "case-c-wrong-ca-pod-log.txt").read_text()
    proof = [
        line
        for line in raw.splitlines()
        if "sasl_ssl://" in line and ("SSL_HANDSHAKE" in line or "certificate verify failed" in line)
    ]
    plaintext = [line for line in raw.splitlines() if "plaintext://" in line.lower()]
    if not proof:
        raise RuntimeError(f"{wrong}: no sasl_ssl:// handshake failure in the runner log")
    if plaintext:
        raise RuntimeError(f"{wrong}: the runner dialled in the clear: {plaintext[:2]}")
    artifact("case-c-wrong-ca-scheme-proof.txt", "\n".join(proof) + "\n")
    wrong_cluster = get("kafkacluster", "tls-wrong-ca")
    STATE["cases"]["case-c-wrong-ca"] = {
        **failure,
        "handshakeProofLines": len(proof),
        "plaintextDialLines": len(plaintext),
        "firstProofLine": proof[0][:400],
        "probeReachable": wrong_cluster.get("status", {}).get("reachable"),
        "probeReason": wrong_cluster.get("status", {}).get("reason"),
        "artifacts": ["case-c-wrong-ca-pod-log.txt", "case-c-wrong-ca-scheme-proof.txt"],
    }
    save()
    log(f"case-c: {len(proof)} sasl_ssl handshake-failure lines, {len(plaintext)} plaintext dials")


# ---------------------------------------------------------------------------
# Holding a Backup between the freeze and the Job
# ---------------------------------------------------------------------------

JOB_QUOTA = "plat07live-no-jobs"


def hold_jobs() -> None:
    """Refuse every Job `POST` in this namespace, so a `Backup` stops between
    the freeze (the plan ConfigMap and `status.execution`, both written before
    the Job) and the Job itself. That gap is where a connection can change
    under a plan, and it is the only place the merged refusal is reachable."""
    apply(
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": owned_metadata(JOB_QUOTA),
            "spec": {"hard": {"count/jobs.batch": "0"}},
        }
    )
    wait_for(
        "resourcequota",
        JOB_QUOTA,
        lambda o: (o.get("status", {}).get("hard") or {}).get("count/jobs.batch") == "0",
        seconds=120,
    )


def release_jobs() -> None:
    if get_opt("resourcequota", JOB_QUOTA) is not None:
        run(KN + ["delete", "resourcequota", JOB_QUOTA, "--wait=true"], timeout=120)


def wait_frozen_without_job(name: str) -> dict[str, Any]:
    backup = wait_for(
        "backup",
        name,
        lambda o: (o.get("status") or {}).get("execution") is not None,
        seconds=300,
    )
    if jobs_named(name):
        raise RuntimeError(f"{name}: a Job exists although Job creation is quota-refused")
    if terminal(backup):
        raise RuntimeError(f"{name}: went terminal during the hold: {json.dumps(backup['status'])}")
    return backup


def nudge(name: str) -> None:
    """Make the controller reconcile now, without touching spec or status."""
    run(
        KN
        + [
            "annotate",
            "backup",
            name,
            f"logweir.dev/test-nudge={dt.datetime.now(dt.timezone.utc).timestamp()}",
            "--overwrite",
        ]
    )


def case_d() -> None:
    """(d) THE JOINT CONFLICT — the one thing neither prior run exercised.

    Freeze a `Backup` against a CA-bearing `KafkaCluster`, delete and recreate
    that `KafkaCluster` under the SAME NAME naming a DIFFERENT CA, and
    reconcile with the old plan present: terminal `PlanConfigMapConflict`, no
    Job, and the plan ConfigMap untouched. `KafkaCluster.spec` is CEL-immutable,
    so delete-and-recreate is the only route — which also changes the pinned
    UID, and the snapshot pins both.
    """
    name = "plat07live-joint"
    hold_jobs()
    before_cluster = get("kafkacluster", "joint-ca")
    ensure(backup_object(name, "joint-ca", topics=[TLS_TOPIC], archive_path="joint", deadline=600))
    held = wait_frozen_without_job(name)
    plan_before = plan_fingerprint(f"{name}-plan")
    artifact("case-d-plan-before.json", plan_before)
    snapshot_before = json.loads(get("configmap", f"{name}-plan")["data"][INPUTS_KEY])
    artifact("case-d-snapshot-before.json", snapshot_before)
    if snapshot_before["source"]["tlsCa"] != {
        "kind": "configMap",
        "name": "kafka-ca",
        "key": CA_FILE_NAME,
    }:
        raise RuntimeError(f"case-d: the frozen CA is {snapshot_before['source'].get('tlsCa')}")

    # The connection changes under the frozen plan: same NAME, different CA.
    run(KN + ["delete", "kafkacluster", "joint-ca", "--wait=true"], timeout=180)
    recreated = apply(
        kafka_cluster(
            "joint-ca",
            servers=TLS_SOURCE,
            mode="scramSha512",
            username="tls-user",
            secret={"name": "tls-sasl", "passwordKey": "sasl-password"},
            tls=True,
            tls_ca=WRONG_CA_FROM_CONFIG_MAP,
        )
    )
    nudge(name)
    conflicted = wait_for(
        "backup",
        name,
        lambda o: (o.get("status") or {}).get("phase") in {"Failed", "Refused"},
        seconds=300,
    )
    artifact("case-d-backup-conflicted.json", conflicted)
    failed = condition(conflicted, "Failed") or {}
    if failed.get("reason") != TERMINAL_PLAN_CONFLICT:
        raise RuntimeError(f"case-d: the refusal is {failed.get('reason')}: {failed.get('message')}")

    # THE QUOTA IS REMOVED BEFORE "no Job" IS CLAIMED, so the claim is about
    # the controller's refusal and not about an admission plugin.
    release_jobs()
    nudge(name)
    time.sleep(60)
    after = get("backup", name)
    plan_after = plan_fingerprint(f"{name}-plan")
    artifact("case-d-plan-after.json", plan_after)
    jobs = jobs_named(name)
    if jobs:
        raise RuntimeError(f"case-d: a Job was created after the conflict: {jobs}")
    if plan_after != plan_before:
        raise RuntimeError(f"case-d: the plan ConfigMap changed:\n{plan_before}\n{plan_after}")
    if (after.get("status") or {}).get("execution") != (held.get("status") or {}).get("execution"):
        raise RuntimeError("case-d: status.execution was rewritten by the conflicting pass")

    # THE CONTROL — this guard can fail. The SAME hold, released with the
    # connection UNCHANGED, produces the Job and a verified run. Without it,
    # "no Job" would be indistinguishable from "Jobs never start here".
    control = "plat07live-joint-control"
    hold_jobs()
    ensure(backup_object(control, "tls-ca", topics=[TLS_TOPIC], archive_path="joint-control"))
    wait_frozen_without_job(control)
    control_plan = plan_fingerprint(f"{control}-plan")
    release_jobs()
    control_record = verify_succeeded(control, "case-d-control")
    if plan_fingerprint(f"{control}-plan") != control_plan:
        raise RuntimeError("case-d control: the plan was rewritten when the hold was released")

    STATE["cases"]["case-d"] = {
        "backup": name,
        "backupUid": conflicted["metadata"]["uid"],
        "clusterUidBefore": before_cluster["metadata"]["uid"],
        "clusterUidAfter": recreated["metadata"]["uid"],
        "caBefore": snapshot_before["source"]["tlsCa"],
        "caAfter": recreated["spec"]["auth"]["tlsCa"],
        "phase": conflicted["status"].get("phase"),
        "reason": failed.get("reason"),
        "message": failed.get("message"),
        "jobsForName": jobs,
        "planBefore": plan_before,
        "planAfter": plan_after,
        "planUnmodified": plan_after == plan_before,
        "quotaRemovedBeforeTheNoJobClaim": True,
        "control": {
            "backup": control,
            "jobUid": control_record["jobUid"],
            "phase": control_record["phase"],
            "planUnmodified": True,
        },
        "artifacts": [
            "case-d-plan-before.json",
            "case-d-plan-after.json",
            "case-d-snapshot-before.json",
            "case-d-backup-conflicted.json",
        ],
    }
    save()
    log(f"case-d: {name} is terminal {failed.get('reason')} with {len(jobs)} Jobs")


def case_e() -> None:
    """(e) ROTATION. A password is a REFERENCE, not an input: a plan frozen
    before a rotation runs after it, its digest unchanged, and a new Backup of
    the same spec freezes the same inputs."""
    cluster_before = get("kafkacluster", "tls-ca")
    name = "plat07live-rot-hold"
    hold_jobs()
    ensure(backup_object(name, "tls-ca", topics=[TLS_TOPIC], archive_path="rotation"))
    held = wait_frozen_without_job(name)
    frozen_digest = held["status"]["execution"]["inputsSha256"]
    plan_before = plan_fingerprint(f"{name}-plan")
    snapshot_before = json.loads(get("configmap", f"{name}-plan")["data"][INPUTS_KEY])

    # ROTATE: the broker credential AND the Secret, with NO KafkaCluster edit.
    set_scram_credential("tls-user", SECRETS_IN_MEMORY["tls-password-2"])
    literal_secret("tls-sasl", {"sasl-password": SECRETS_IN_MEMORY["tls-password-2"]})
    # A Secret that still holds the PRE-rotation value, so "the rotation
    # happened" is measured and not assumed.
    literal_secret("tls-sasl-stale", {"sasl-password": SECRETS_IN_MEMORY["tls-password-1"]})
    apply(
        kafka_cluster(
            "tls-stale",
            servers=TLS_SOURCE,
            mode="scramSha512",
            username="tls-user",
            secret={"name": "tls-sasl-stale", "passwordKey": "sasl-password"},
            tls=True,
            tls_ca=CA_FROM_CONFIG_MAP,
        )
    )
    cluster_after = get("kafkacluster", "tls-ca")
    # NO EDIT means the SPEC did not change, and `metadata.generation` is the
    # API server's own counter for exactly that. `resourceVersion` is NOT the
    # test: the KafkaCluster controller writes its own status (probe result,
    # observed cluster id) and every such write bumps it without anyone having
    # edited the connection. Both are recorded; only generation is asserted.
    if cluster_after["metadata"]["generation"] != cluster_before["metadata"]["generation"]:
        raise RuntimeError("case-e: the KafkaCluster spec was edited; a rotation must need no edit")
    if cluster_after["spec"] != cluster_before["spec"]:
        raise RuntimeError("case-e: the KafkaCluster spec changed across the rotation")

    release_jobs()
    held_record = verify_succeeded(name, "case-e-held")
    after = get("backup", name)
    if after["status"]["execution"]["inputsSha256"] != frozen_digest:
        raise RuntimeError(
            f"case-e: the frozen digest changed across the rotation: {frozen_digest} -> "
            f"{after['status']['execution']['inputsSha256']}"
        )
    if plan_fingerprint(f"{name}-plan") != plan_before:
        raise RuntimeError("case-e: the plan ConfigMap was rewritten across the rotation")

    # A NEW Backup of the SAME spec, resolved entirely after the rotation.
    fresh = "plat07live-rot-after"
    ensure(backup_object(fresh, "tls-ca", topics=[TLS_TOPIC], archive_path="rotation"))
    fresh_record = verify_succeeded(fresh, "case-e-after")
    snapshot_after = json.loads(get("configmap", f"{fresh}-plan")["data"][INPUTS_KEY])
    if identity_free(snapshot_before) != identity_free(snapshot_after):
        artifact("case-e-snapshot-diff.json", {"before": identity_free(snapshot_before), "after": identity_free(snapshot_after)})
        raise RuntimeError("case-e: a rotated password changed the frozen inputs")
    if snapshot_before == snapshot_after:
        raise RuntimeError("case-e: two runs must not share one identity")

    # THE MUTANT: the pre-rotation value must no longer authenticate.
    stale = "plat07live-rot-stale"
    ensure(
        backup_object(stale, "tls-stale", topics=[TLS_TOPIC], archive_path="rotation-stale", deadline=180)
    )
    stale_record = expect_failed(stale, "case-e-stale")
    stale_log = (OUT / "case-e-stale-pod-log.txt").read_text()
    credential_failure = [
        line for line in stale_log.splitlines() if "SASL authentication error" in line
    ]
    handshake_failure = [line for line in stale_log.splitlines() if "SSL_HANDSHAKE" in line]
    if not credential_failure or handshake_failure:
        raise RuntimeError(
            "case-e: the stale-secret run must fail on the CREDENTIAL after a successful "
            f"handshake ({len(credential_failure)} auth / {len(handshake_failure)} handshake lines)"
        )

    STATE["cases"]["case-e"] = {
        "heldBackup": name,
        "frozenDigestBefore": frozen_digest,
        "frozenDigestAfter": after["status"]["execution"]["inputsSha256"],
        "frozenDigestUnchanged": True,
        "planUnmodifiedAcrossRotation": True,
        "kafkaClusterGeneration": cluster_after["metadata"]["generation"],
        "kafkaClusterSpecUnchanged": True,
        "kafkaClusterResourceVersionBefore": cluster_before["metadata"]["resourceVersion"],
        "kafkaClusterResourceVersionAfter": cluster_after["metadata"]["resourceVersion"],
        "heldRunPhase": held_record["phase"],
        "heldRunExit": held_record["podExitCode"],
        "freshBackup": fresh,
        "freshRunPhase": fresh_record["phase"],
        "identityFreeSha256Before": digest(json.dumps(identity_free(snapshot_before), sort_keys=True)),
        "identityFreeSha256After": digest(json.dumps(identity_free(snapshot_after), sort_keys=True)),
        "staleSecretRun": {
            **stale_record,
            "saslAuthErrorLines": len(credential_failure),
            "sslHandshakeErrorLines": len(handshake_failure),
            "firstAuthLine": credential_failure[0][:400],
        },
        "passwordDigests": {
            "before": digest(SECRETS_IN_MEMORY["tls-password-1"]),
            "after": digest(SECRETS_IN_MEMORY["tls-password-2"]),
        },
    }
    save()
    log(f"case-e: the frozen digest {frozen_digest} survived the rotation")


# ---------------------------------------------------------------------------
# (f) The redaction sweep
# ---------------------------------------------------------------------------

SECRET_NAMES = [
    "source-scram",
    "lab-sasl",
    "tls-sasl",
    "tls-sasl-stale",
    "logweir-s3",
    "logweir-signing-key",
    "kafka-tls-broker",
]
CA_OBJECT_NAMES = ["kafka-ca", "kafka-wrong-ca"]


def secret_value(name: str, key: str, namespace: str = NS) -> str:
    """One Secret value, decoded IN MEMORY so the sweep has a needle. It is
    never printed, never stored and never written to an artifact."""
    encoded = get("secret", name, namespace=namespace)["data"][key]
    return base64.b64decode(encoded).decode("utf-8", "replace")


def haystack() -> dict[str, str]:
    """Every place a credential could surface. Secrets themselves are NOT here:
    a Secret holding its own value is the point of a Secret. Everything a
    reader, an operator or an exported bundle would see is."""
    pile: dict[str, str] = {}
    for kind in ["backup", "kafkacluster", "backupschedule", "restore", "approval"]:
        for item in get_list(kind):
            pile[f"cr/{kind}/{item['metadata']['name']}"] = json.dumps(item)
    for cm in get_list("configmap"):
        pile[f"configmap/{cm['metadata']['name']}"] = json.dumps(cm)
    for job in get_list("job"):
        pile[f"job/{job['metadata']['name']}"] = json.dumps(job)
    for name, text in raw_pod_logs().items():
        pile[f"podlog/{name}"] = text
    pile["events"] = json.dumps(get_list("event"))
    pile["labControllerLog"] = lab_controller_log("30m")
    return pile


def case_f() -> None:
    """(f) No password, no CA private key and no Secret VALUE reaches any
    custom resource, ConfigMap, Job, pod log, event or the lab controller's
    own log. Secret NAMES may appear; where they do is recorded."""
    needles = {
        "tlsPasswordBeforeRotation": SECRETS_IN_MEMORY["tls-password-1"],
        "tlsPasswordAfterRotation": SECRETS_IN_MEMORY["tls-password-2"],
        "brokerKeystorePassword": SECRETS_IN_MEMORY["keystore"],
        "labScramPassword": secret_value("source-scram", "password"),
        "archiveAccessKeyId": secret_value("logweir-s3", "access-key-id"),
        "archiveSecretAccessKey": secret_value("logweir-s3", "secret-access-key"),
    }
    ca_key = (KEYS / "ca.key").read_text()
    ca_key_body = "".join(ca_key.splitlines()[1:-1])
    needles["caPrivateKeyWhole"] = ca_key
    needles["caPrivateKeyBody64"] = ca_key_body[:64]
    needles["caPrivateKeyBodyTail64"] = ca_key_body[-64:]
    needles["signingKeyPem"] = secret_value("logweir-signing-key", "signing.pem")

    pile = haystack()
    total_bytes = sum(len(v) for v in pile.values())
    hits: dict[str, list[str]] = {}
    for label, needle in needles.items():
        found = []
        variants = [needle, base64.b64encode(needle.encode()).decode()]
        for source, text in pile.items():
            if any(v and v in text for v in variants):
                found.append(source)
        hits[label] = sorted(found)
    marker_hits = [
        source
        for source, text in pile.items()
        if "BEGIN RSA PRIVATE KEY" in text or "BEGIN PRIVATE KEY" in text
    ]
    # WHAT LOGWEIR PRODUCED, AND WHAT A THIRD-PARTY FIXTURE DID. The broker and
    # the `mc` client in this namespace are stock upstream images this run stood
    # up; what THEY print about their own configuration is not evidence about
    # Logweir's handling. Every custom resource, every ConfigMap, every Job
    # manifest, every runner pod log, the events and the lab controller's log
    # ARE, and a hit in any of them fails this case.
    fixture_sources = {
        source
        for source in pile
        if source.startswith("podlog/kafka-tls") or source.startswith("podlog/plat07live-mc")
    }
    product_hits = {
        label: [s for s in sources if s not in fixture_sources]
        for label, sources in hits.items()
    }
    fixture_hits = {
        label: [s for s in sources if s in fixture_sources]
        for label, sources in hits.items()
    }
    product_marker_hits = [s for s in marker_hits if s not in fixture_sources]
    # A BARE SUBSTRING, NOT A QUOTED JSON TOKEN. A plan ConfigMap carries its
    # snapshot as a STRING, so every quote inside it is escaped and a `"name"`
    # search would report a clean ConfigMap that in fact names the object. This
    # search over-reports rather than under-reports, which is the only safe
    # direction for a leak check.
    name_map = {
        name: sorted(source for source, text in pile.items() if name in text)
        for name in SECRET_NAMES + CA_OBJECT_NAMES
    }
    # THE INVARIANT, ASSERTED SEPARATELY FROM THE SURVEY. No CREDENTIAL object's
    # name may appear in any ConfigMap. The CA objects are deliberately exempt:
    # the frozen snapshot records the CA REFERENCE so a changed trust anchor is
    # a conflict, and a CA certificate is public material.
    credential_names_in_config_maps = {
        name: [s for s in sources if s.startswith("configmap/")]
        for name, sources in name_map.items()
        if name in SECRET_NAMES
    }
    offending = {k: v for k, v in credential_names_in_config_maps.items() if v}
    ca_certificate_present = sorted(
        source for source, text in pile.items() if "BEGIN CERTIFICATE" in text
    )
    result = {
        "sources": sorted(pile),
        "sourceCount": len(pile),
        "bytesScanned": total_bytes,
        "needlesSha256": {k: digest(v) for k, v in needles.items()},
        "hits": hits,
        "hitsInLogweirProducedSources": {k: v for k, v in product_hits.items() if v},
        "hitsInThirdPartyFixtureLogs": {k: v for k, v in fixture_hits.items() if v},
        "thirdPartyFixtureSources": sorted(fixture_sources),
        "privateKeyMarkerHits": marker_hits,
        "privateKeyMarkerHitsInLogweirProducedSources": product_marker_hits,
        "secretNamesAppearIn": name_map,
        "credentialSecretNamesInConfigMaps": credential_names_in_config_maps,
        "caObjectNamesInConfigMaps": {
            name: [s for s in name_map[name] if s.startswith("configmap/")]
            for name in CA_OBJECT_NAMES
        },
        "caCertificateAppearsIn": ca_certificate_present,
        "secretsExcludedFromHaystack": "Secret objects themselves — a Secret holds its own value",
    }
    artifact("case-f-redaction.json", result)
    leaked = {k: v for k, v in product_hits.items() if v}
    if leaked or product_marker_hits:
        raise RuntimeError(
            f"case-f: credential material surfaced: {leaked} {product_marker_hits}"
        )
    if offending:
        raise RuntimeError(f"case-f: a credential Secret is named in a ConfigMap: {offending}")
    STATE["cases"]["case-f"] = result
    save()
    log(f"case-f: {len(needles)} needles, 0 hits across {len(pile)} sources / {total_bytes} bytes")


# ---------------------------------------------------------------------------
# Report and cleanup
# ---------------------------------------------------------------------------


def report() -> None:
    artifact("report.json", STATE)
    print(json.dumps(STATE, indent=2, sort_keys=True))


def archive_inventory(label: str) -> dict[str, Any]:
    ids = [
        record["execution"]["id"]
        for record in STATE["cases"].values()
        if isinstance(record, dict) and isinstance(record.get("execution"), dict)
    ]
    inventory = {
        "at": now(),
        "label": label,
        "runPrefix": archive_objects(f"{ARCHIVE_PREFIX}/"),
        "evidence": {i: archive_objects(f"{EVIDENCE_PREFIX}/{i}/") for i in sorted(set(ids))},
        "labPrefixes": {
            prefix: len(archive_objects(f"{prefix}/"))
            for prefix in ["logweir", "scram-local"]
        },
    }
    artifact(f"archive-{label}.json", inventory)
    return inventory


def cleanup() -> None:
    """Delete ONLY what this run created: its namespace (checked by label AND
    UID) and the archive objects its own runs wrote."""
    before = archive_inventory("before-cleanup") if get_opt("pod", "plat07live-mc") else None
    if before is not None:
        for entry in before["runPrefix"]:
            mc("rm", f"local/{ARCHIVE_BUCKET}/{entry['key']}", check=False)
        for execution_id, entries in before["evidence"].items():
            for entry in entries:
                mc("rm", f"local/{ARCHIVE_BUCKET}/{entry['key']}", check=False)
        after = archive_inventory("after-cleanup")
        remaining = len(after["runPrefix"]) + sum(len(v) for v in after["evidence"].values())
        # THE EVIDENCE PREFIX IS SHARED. `logweir/backups/<id>/` holds every
        # run's receipt, this run's among them, so its object count MUST fall by
        # exactly what this run wrote and by nothing else. `scram-local/` is the
        # lab's own data and must not move at all. Comparing the two counts for
        # equality would be the wrong check and would pass while deleting a
        # stranger's receipt.
        mine = sum(len(v) for v in before["evidence"].values())
        expected_evidence = before["labPrefixes"]["logweir"] - mine
        arithmetic = {
            "evidenceObjectsThisRunWrote": mine,
            "sharedEvidencePrefixBefore": before["labPrefixes"]["logweir"],
            "sharedEvidencePrefixExpectedAfter": expected_evidence,
            "sharedEvidencePrefixAfter": after["labPrefixes"]["logweir"],
            "labDataPrefixBefore": before["labPrefixes"]["scram-local"],
            "labDataPrefixAfter": after["labPrefixes"]["scram-local"],
        }
        STATE["archiveCleanup"] = {
            "before": before,
            "after": after,
            "remainingObjects": remaining,
            "arithmetic": arithmetic,
            "onlyThisRunsObjectsWereDeleted": (
                after["labPrefixes"]["logweir"] == expected_evidence
                and after["labPrefixes"]["scram-local"] == before["labPrefixes"]["scram-local"]
            ),
        }
        save()
        if remaining:
            raise RuntimeError(f"archive cleanup left {remaining} objects behind")
        if not STATE["archiveCleanup"]["onlyThisRunsObjectsWereDeleted"]:
            raise RuntimeError(f"archive cleanup touched objects this run did not write: {arithmetic}")

    ns = get_opt("namespace", NS, namespace="default")
    if ns is None:
        log("namespace already gone")
        return
    if ns["metadata"].get("labels", {}).get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError("refusing to delete a namespace this run does not own")
    if ns["metadata"]["uid"] != STATE.get("namespaceUid"):
        raise RuntimeError("the namespace UID does not match this run's state")
    run(K + ["delete", "namespace", NS, "--wait=true"], timeout=600)
    leftovers = run(
        K
        + [
            "get",
            "all,secret,configmap,resourcequota,kafkacluster,backup",
            "-A",
            "-l",
            f"logweir.dev/test-owner={OWNER}",
        ],
        check=False,
    )
    STATE["cleanup"] = {
        "deletedAt": now(),
        "namespaceUid": ns["metadata"]["uid"],
        "ownerLabelSurvey": (leftovers.stdout + leftovers.stderr).strip(),
    }
    save()
    artifact("cleanup-proof.txt", json.dumps(STATE["cleanup"], indent=2, sort_keys=True))
    log(f"deleted namespace {NS}")


PHASES = {
    "lab-baseline": lab_baseline,
    "lab-observe-before": lambda: lab_observe("before-swap"),
    "lab-observe-after": lambda: lab_observe("after-restore"),
    "lab-swap": lab_swap,
    "lab-restore": lab_restore,
    "setup": setup,
    "case-a": case_a,
    "case-b": case_b,
    "case-c": case_c,
    "case-d": case_d,
    "case-e": case_e,
    "case-f": case_f,
    "report": report,
    "cleanup": cleanup,
}


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in PHASES:
        print(f"usage: {sys.argv[0]} [{'|'.join(PHASES)}]", file=sys.stderr)
        raise SystemExit(2)
    load_secrets()
    PHASES[sys.argv[1]]()
