#!/usr/bin/env python3
"""Live docker-desktop acceptance for D3's catalog, retention and trust tasks.

This is the harness D3 §15 asks for (PLAT-15.1, PLAT-16.1, PLAT-16.2, with
evidence toward PLAT-14.2 and PLAT-19.1). Every phase runs in one owned
namespace against the shared `logweir-scram-local` fixture's SCRAM Kafka and
MinIO, and writes its own buckets — never the fixture's `kafka-backups`.
`README.md` beside this file carries the full order and its three ordering
constraints; `--help` prints the phase list.

    python3 e2e/k8s/d3/d3_live.py setup
    python3 e2e/k8s/d3/d3_live.py catalog        # PLAT-15.1
    python3 e2e/k8s/d3/d3_live.py retention      # PLAT-16.1
    python3 e2e/k8s/d3/d3_live.py enforce --retention-image <ref>    # PLAT-16.2
    python3 e2e/k8s/d3/d3_live.py trust          # PLAT-19.1 (cluster lock: TrustPolicy)
    python3 e2e/k8s/d3/d3_live.py notify         # PLAT-14.2
    python3 e2e/k8s/d3/d3_live.py control        # the negative control
    python3 e2e/k8s/d3/d3_live.py report
    python3 e2e/k8s/d3/d3_live.py cleanup

NOTHING HERE CHANGES THE SHARED RELEASE. The enforcement phases run
`logweir-retention` as Jobs in this namespace, exactly as `docs/kubernetes.md`
§7f prescribes for the preview, out of an image named by `--retention-image`
(no image this repository builds carries that binary — defect RET-NOIMAGE, which
the `packaging` phase proves live). The controller's `LOGWEIR_RUNNER_IMAGE` is
read and never written. The only object created outside this namespace is the
cluster-scoped `TrustPolicy` the trust phases need, which binds this namespace
alone and which `cleanup` removes.

Rules this file enforces rather than documents: every `kubectl` carries
`--context docker-desktop`; every object it creates carries
`logweir.dev/test-owner=d3w14`; `cleanup` refuses a namespace or a bucket that
does not; no Secret value, token or private key is ever printed or written to
an artifact, and the ONE credential this harness mints is generated per run and
scrubbed from argv and output before either is recorded (`redact` over every
captured stream, `sweep` over every artifact at `report` time, and
`sweep_selftest` planting a probe first so the sweep is a guard that can fail).
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

ROOT = pathlib.Path(__file__).resolve().parents[3]
STAMP = os.environ.get("LOGWEIR_D3_STAMP", "20260918t0000z")
OWNER = "d3w14"
NS = f"{OWNER}-{STAMP}"
FIXTURE_NS = "logweir-scram-local"
OUT = pathlib.Path(
    os.environ.get("LOGWEIR_D3_OUT", f"/tmp/logweir-roadmap-run/claude/artifacts/d3-live/{STAMP}")
)
STATE_PATH = OUT / "state.json"
K = ["kubectl", "--context", "docker-desktop"]
KN = K + ["-n", NS]
BUCKET_A = f"{OWNER}-{STAMP}-a"
BUCKET_B = f"{OWNER}-{STAMP}-b"
DEST_PREFIX = "archive"
MINIO_ENDPOINT = f"http://minio.{FIXTURE_NS}.svc.cluster.local:9000"
TOPICS = ["orders", "payments"]
LABEL = {"logweir.dev/test-owner": OWNER}

OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE: dict[str, Any] = (
    json.loads(STATE_PATH.read_text())
    if STATE_PATH.exists()
    else {"namespace": NS, "stamp": STAMP, "scenarios": {}, "commands": []}
)


# ---------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------


def save() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


# EVERY SECRET-SHAPED VALUE THIS RUN ITSELF CREATED.
#
# The harness mints exactly one credential of its own (the read-only MinIO user
# the denied-deletion case needs). A pattern-matching sweep cannot be trusted to
# catch it: the first version of this file passed a hardcoded password as a
# POSITIONAL argument to `mc admin user add`, which no `key=value` pattern
# matches, and `run()` recorded that argv into `state.json` — so the sweep
# reported zero hits over the one credential the run produced. Membership is
# exact where a pattern is a guess, so every minted value is registered here,
# scrubbed out of argv and output before either is recorded, and searched for
# literally by `sweep`.
MINTED: set[str] = set()


def mint(nbytes: int = 24) -> str:
    """A fresh value, registered so it can never reach an artifact."""
    value = secrets.token_urlsafe(nbytes)
    MINTED.add(value)
    return value


def redact(text: str) -> str:
    """Anything credential-shaped, removed before it can reach an artifact."""
    for value in MINTED:
        text = text.replace(value, "[REDACTED MINTED VALUE]")
    text = re.sub(
        r"(?i)(password|secret[-_]?access[-_]?key|access[-_]key[-_]?id|routing[-_]key|token)"
        r"[\"'= :]+[^\s\"',}]+",
        r"\1=[REDACTED]",
        text,
    )
    return re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.DOTALL,
    )


def run(args: list[str], *, data: str | None = None, check: bool = True, timeout: int = 180):
    """One subprocess, always with a timeout (WORKER-RULES: a hung child blocks
    the worker). The return code is read from the completed process and never
    through a pipe."""
    started = time.time()
    result = subprocess.run(
        args, input=data, text=True, capture_output=True, timeout=timeout, cwd=ROOT
    )
    STATE["commands"].append(
        {
            "at": now(),
            # ARGV IS RECORDED, SO ARGV IS REDACTED. `state.json` and
            # `results.json` carry this list; a minted value passed
            # positionally would otherwise land in both.
            "argv": [redact(a) for a in args[:12]],
            "rc": result.returncode,
            "seconds": round(time.time() - started, 2),
        }
    )
    if check and result.returncode:
        raise RuntimeError(
            f"rc={result.returncode}: {args[:8]}\n"
            f"{redact(result.stdout[-3000:])}\n{redact(result.stderr[-3000:])}"
        )
    return result


def apply(obj: dict[str, Any]) -> dict[str, Any]:
    return json.loads(run(K + ["apply", "-f", "-", "-o", "json"], data=json.dumps(obj)).stdout)


def create(obj: dict[str, Any], check: bool = True):
    result = run(K + ["create", "-f", "-", "-o", "json"], data=json.dumps(obj), check=check)
    return json.loads(result.stdout) if result.returncode == 0 else result


def get(kind: str, name: str, namespace: str = NS) -> dict[str, Any]:
    return json.loads(run(K + ["-n", namespace, "get", kind, name, "-o", "json"]).stdout)


def get_opt(kind: str, name: str, namespace: str = NS) -> dict[str, Any] | None:
    result = run(K + ["-n", namespace, "get", kind, name, "-o", "json"], check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def lst(kind: str, namespace: str = NS, selector: str | None = None) -> list[dict[str, Any]]:
    args = K + ["-n", namespace, "get", kind, "-o", "json"]
    if selector:
        args += ["-l", selector]
    return json.loads(run(args).stdout)["items"]


def artifact(name: str, body: Any) -> str:
    path = OUT / name
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    text = body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True, default=str)
    path.write_text(redact(text))
    return str(path)


def owned(name: str, namespace: str | None = NS) -> dict[str, Any]:
    meta: dict[str, Any] = {"name": name, "labels": dict(LABEL)}
    if namespace:
        meta["namespace"] = namespace
    return meta


def wait_for(
    kind: str,
    name: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    seconds: int = 300,
    namespace: str = NS,
    what: str = "",
) -> dict[str, Any]:
    deadline = time.time() + seconds
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        last = get_opt(kind, name, namespace)
        if last is not None and predicate(last):
            return last
        time.sleep(3)
    dump = artifact(f"timeout-{kind}-{name}.json", last or {})
    raise RuntimeError(f"timeout waiting for {kind}/{name} {what}: dump {dump}")


def condition(obj: dict[str, Any], kind: str) -> dict[str, Any]:
    for c in obj.get("status", {}).get("conditions", []) or []:
        if c.get("type") == kind:
            return c
    return {}


def terminal(o: dict[str, Any]) -> bool:
    return o.get("status", {}).get("phase") in {"Succeeded", "Failed", "Refused"}


# ---------------------------------------------------------------------------
# Scenario bookkeeping
# ---------------------------------------------------------------------------


def record(
    scenario: str,
    task: str,
    verdict: str,
    detail: str,
    evidence: list[str] | None = None,
    **extra: Any,
) -> None:
    entry = {
        "scenario": scenario,
        "task": task,
        "verdict": verdict,
        "detail": detail,
        "evidence": evidence or [],
        "at": now(),
        **extra,
    }
    STATE["scenarios"][scenario] = entry
    save()
    log(f"[{verdict}] {scenario} — {detail}")


def check(scenario: str, task: str, ok: bool, detail: str, evidence: list[str] | None = None):
    record(scenario, task, "PASS" if ok else "FAIL", detail, evidence)
    return ok


# ---------------------------------------------------------------------------
# MinIO, through an `mc` pod this namespace owns
# ---------------------------------------------------------------------------

MC_POD = "d3w14-mc"


def mc(*args: str, check_rc: bool = True, timeout: int = 180) -> str:
    result = run(KN + ["exec", MC_POD, "--", "mc", *args], check=check_rc, timeout=timeout)
    return result.stdout


def mc_admin(*args: str, check_rc: bool = True) -> str:
    """`mc admin …` against the alias that holds the ROOT credential. Used only
    to mint the scoped users the denied-deletion cases need."""
    result = run(KN + ["exec", MC_POD, "--", "mc", "admin", *args], check=check_rc, timeout=180)
    return result.stdout


def objects(bucket: str, path: str = "") -> list[dict[str, Any]]:
    out = mc("ls", "--recursive", "--json", f"local/{bucket}/{path}", check_rc=False)
    found = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("status") != "success" or "key" not in entry:
            continue
        # `mc ls --recursive` reports a key RELATIVE to the path it was given,
        # so a listing of a prefix must be re-rooted or every key it yields is
        # a key that does not exist.
        found.append(
            {"key": path + entry["key"], "size": entry.get("size"), "etag": entry.get("etag")}
        )
    return sorted(found, key=lambda e: e["key"])


def cat(bucket: str, key: str) -> bytes:
    return mc("cat", f"local/{bucket}/{key}").encode()


def put(bucket: str, key: str, body: bytes) -> None:
    b64 = base64.b64encode(body).decode()
    run(
        KN
        + [
            "exec",
            MC_POD,
            "--",
            "/bin/sh",
            "-c",
            f"echo {b64} | base64 -d > /tmp/put.bin && mc cp --quiet /tmp/put.bin "
            f"local/{bucket}/{key} >/dev/null && rm -f /tmp/put.bin",
        ],
        timeout=120,
    )


def rm(bucket: str, key: str) -> None:
    mc("rm", f"local/{bucket}/{key}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def copy_secret(name: str, as_name: str | None = None) -> None:
    """A Secret copied from the shared fixture into this namespace. The VALUES
    are never read into this process's own output — `data` is moved as opaque
    base64 and nothing prints it."""
    source = get("secret", name, namespace=FIXTURE_NS)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned(as_name or name),
            "type": source.get("type", "Opaque"),
            "data": source["data"],
        }
    )


def destination(name: str, bucket: str, *, write_secret: str = "logweir-s3") -> dict[str, Any]:
    def grant(secret: str) -> dict[str, Any]:
        return {
            "mode": "SecretKeys",
            "secret": {
                "name": secret,
                "accessKeyIdKey": "access-key-id",
                "secretAccessKeyKey": "secret-access-key",
            },
        }

    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupDestination",
        "metadata": owned(name),
        "spec": {
            "description": f"d3w14 live acceptance, bucket {bucket}",
            "storage": {
                "provider": "S3",
                "bucket": bucket,
                "prefix": DEST_PREFIX,
                "endpoint": MINIO_ENDPOINT,
                "region": "us-east-1",
                "addressing": "PathStyle",
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": grant(write_secret),
                "archiveRead": grant(write_secret),
                "evidenceWrite": grant(write_secret),
                "evidenceRead": grant(write_secret),
            },
            "readiness": {"writeProbe": "Disabled"},
        },
    }


def backup_object(name: str, dest: str, topics: list[str] | None = None) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": owned(name),
        "spec": {
            "sourceRef": {"name": "source"},
            "destinationRef": {"name": dest},
            "topics": topics or TOPICS,
            "archive": {"url": f"logweir-destination://{dest}"},
            "triggeredBy": "manual",
            "deadlineSeconds": 600,
        },
    }


def mc_pod() -> dict[str, Any]:
    secret_env = lambda var, secret, key: {  # noqa: E731 - a table, not a statement
        "name": var,
        "valueFrom": {"secretKeyRef": {"name": secret, "key": key}},
    }
    script = (
        f'mc alias set local {MINIO_ENDPOINT} "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" '
        ">/dev/null && "
        f'mc alias set adm {MINIO_ENDPOINT} "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null '
        "&& touch /tmp/ready && sleep 10800"
    )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": owned(MC_POD),
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "mc",
                    "image": "minio/mc:latest",
                    "imagePullPolicy": "Never",
                    "command": ["/bin/sh", "-c"],
                    "args": [script],
                    "env": [
                        secret_env("AWS_ACCESS_KEY_ID", "logweir-s3", "access-key-id"),
                        secret_env("AWS_SECRET_ACCESS_KEY", "logweir-s3", "secret-access-key"),
                        secret_env("MINIO_ROOT_USER", "minio-root", "user"),
                        secret_env("MINIO_ROOT_PASSWORD", "minio-root", "password"),
                    ],
                    "readinessProbe": {
                        "exec": {"command": ["test", "-f", "/tmp/ready"]},
                        "periodSeconds": 1,
                    },
                }
            ],
        },
    }


def setup() -> None:
    existing = get_opt("namespace", NS, namespace="default")
    if existing is None:
        run(K + ["create", "namespace", NS])
        run(K + ["label", "namespace", NS, f"logweir.dev/test-owner={OWNER}"])
    elif existing["metadata"].get("labels", {}).get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError(f"refusing to reuse unowned namespace {NS}")
    STATE["namespaceUid"] = get("namespace", NS, namespace="default")["metadata"]["uid"]
    STATE["controller"] = controller_facts()
    for sa in ["logweir-runner", "logweir-retention"]:
        # `logweir-retention` is the SA every retention Job requests and the
        # chart does not create yet (docs/kubernetes.md §7f). It is a namespaced
        # object this namespace owns, so the harness creates it here.
        apply(
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": owned(sa),
                "automountServiceAccountToken": False,
            }
        )
    for secret in ["source-scram", "logweir-s3", "logweir-signing-key", "minio-root"]:
        copy_secret(secret)
    apply(
        {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "KafkaCluster",
            "metadata": owned("source"),
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
    if get_opt("pod", MC_POD) is None:
        apply(mc_pod())
        run(KN + ["wait", "--for=condition=Ready", f"pod/{MC_POD}", "--timeout=180s"])
    for bucket in [BUCKET_A, BUCKET_B]:
        mc("mb", "--ignore-existing", f"local/{bucket}")
    STATE["buckets"] = [BUCKET_A, BUCKET_B]
    for name, bucket in [("dest-a", BUCKET_A), ("dest-b", BUCKET_B)]:
        apply(destination(name, bucket))
        ready = wait_for(
            "backupdestination",
            name,
            lambda o: condition(o, "Valid").get("status") == "True",
            seconds=180,
            what="Valid=True",
        )
        STATE.setdefault("destinations", {})[name] = {
            "uid": ready["metadata"]["uid"],
            "bucket": bucket,
            "valid": condition(ready, "Valid"),
        }
    cluster = wait_for(
        "kafkacluster", "source", lambda o: o.get("status", {}).get("reachable") is True
    )
    STATE["sourceClusterId"] = cluster["status"]["clusterId"]
    artifact("setup/destinations.json", STATE["destinations"])
    record(
        "setup",
        "-",
        "PASS",
        f"namespace {NS} uid {STATE['namespaceUid']}; buckets {BUCKET_A}, {BUCKET_B}; "
        f"source clusterId {STATE['sourceClusterId']}",
        [artifact("setup/state-after-setup.json", STATE)],
    )


def controller_facts() -> dict[str, Any]:
    """The controller this run is judging, by pod, imageID and revision label.
    A live claim about a build is worth nothing without it."""
    pods = lst("pods", namespace=FIXTURE_NS, selector="app.kubernetes.io/component=control-plane")
    pod = pods[0] if pods else {}
    deploy = get("deployment", "weirkeeper", namespace=FIXTURE_NS)
    env = {
        e["name"]: e.get("value", "<fieldRef/secretRef>")
        for e in deploy["spec"]["template"]["spec"]["containers"][0].get("env", [])
    }
    return {
        "pod": pod.get("metadata", {}).get("name"),
        "imageID": (pod.get("status", {}).get("containerStatuses") or [{}])[0].get("imageID"),
        "image": (pod.get("spec", {}).get("containers") or [{}])[0].get("image"),
        "runnerImage": env.get("LOGWEIR_RUNNER_IMAGE"),
        "policyConfigMap": env.get("LOGWEIR_POLICY_CONFIGMAP"),
        "startedAt": pod.get("status", {}).get("startTime"),
        "recordedAt": now(),
    }


# ---------------------------------------------------------------------------
# PLAT-15.1 — the catalog reconstructs history after the CRs are gone
# ---------------------------------------------------------------------------

CATALOG_PREFIX = "logweir/catalog/v1"


VERDICTS = {"Valid", "Invalid", "Untrusted", "NotAttempted"}


def verdict_of(o: dict[str, Any]) -> str | None:
    return o.get("status", {}).get("evidence", {}).get("verification", {}).get("result")


def settle_verdict(name: str, seconds: int = 45) -> dict[str, Any]:
    """A Backup is `Succeeded` before its evidence verdict is written, and
    `status.records` comes from the VERIFIED receipt (D3 W2). This waits a
    BOUNDED time for a verdict and returns whatever the object has: on a
    destination-backed run whose `evidenceRead` is a `SecretKeys` grant this
    build creates no evidence-fetch Job, so `verification` never appears at all
    (see `backup.rs`'s EVIDENCE_READ_NOT_CONFIGURED text and the
    `SecretKeys`/`WorkloadIdentity` arm). Waiting for one forever would be
    waiting for something this build does not write."""
    deadline = time.time() + seconds
    obj = get("backup", name)
    while time.time() < deadline and verdict_of(obj) not in VERDICTS:
        time.sleep(3)
        obj = get("backup", name)
    return obj


def run_backup(
    name: str, dest: str, topics: list[str] | None = None, settle: int = 12
) -> dict[str, Any]:
    create(backup_object(name, dest, topics))
    wait_for("backup", name, terminal, seconds=600, what="a terminal phase")
    done = settle_verdict(name, settle)
    if done["status"].get("phase") != "Succeeded":
        artifact(f"catalog/backup-{name}-failed.json", done)
        raise RuntimeError(f"Backup/{name} did not succeed: {done['status'].get('reason')}")
    return done


def backup_facts(obj: dict[str, Any]) -> dict[str, Any]:
    status = obj.get("status", {})
    return {
        "name": obj["metadata"]["name"],
        "uid": obj["metadata"]["uid"],
        "backupId": status.get("backupId"),
        "phase": status.get("phase"),
        "exitCode": status.get("exitCode"),
        "records": status.get("records"),
        "capture": status.get("capture"),
        "execution": status.get("execution"),
        "evidence": status.get("evidence"),
        "destination": status.get("destination"),
    }


def catalog_object(name: str, dest: str, token: str, **sync: Any) -> dict[str, Any]:
    # 300 s is the CRD's floor and it is load-bearing here: a manual-only
    # catalog (`intervalSeconds: 0`) publishes its FIRST view and then never
    # harvests another sync — see the `catalog-resync-harvest` finding.
    body = {"intervalSeconds": 300, "mode": "Index", "deepCheck": "ManifestDigest"}
    body.update(sync)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "RecoveryCatalog",
        "metadata": owned(name),
        "spec": {"destinationRef": {"name": dest}, "sync": body, "syncRequest": token},
    }


def settled(token: str, since: str = ""):
    """A view published AFTER `since` for THIS token.

    Neither half is redundant. `observedSyncRequest` is written when a sync
    STARTS, so a predicate that stops there reads the previous view; and the
    `Synced` condition is not usable as a completion signal at all on this
    build — a published view is routinely followed by a write that puts the
    condition back to `Unknown/PodNotStarted` (see
    `catalog-resync-is-not-harvested`). `status.syncedAt` moving forward is the
    one signal that means the pages in `status.pages` are this walk's.
    """

    def predicate(o: dict[str, Any]) -> bool:
        status = o.get("status", {})
        return (
            status.get("observedSyncRequest") == token
            and bool(status.get("pages"))
            and bool(status.get("syncedAt"))
            and status.get("syncedAt") >= since
        )

    return predicate


def await_sync(name: str, token: str, *, seconds: int = 600, since: str = "") -> dict[str, Any]:
    return wait_for(
        "recoverycatalog", name, settled(token, since), seconds=seconds,
        what=f"a view published after {since or '<any>'} for token {token}",
    )


def fresh_catalog(name: str, dest: str, *, seconds: int = 420, **sync: Any) -> dict[str, Any]:
    """A NEW `RecoveryCatalog` object, synced once.

    Not a `syncRequest` bump, and the reason is a defect this harness measures
    rather than works around silently: a catalog's FIRST sync publishes within
    seconds, and a later `syncRequest` starts a Job that completes and is never
    harvested (`catalog-resync-is-not-harvested`). A fresh object per archive
    generation is the only way to read the archive as it is NOW.
    """
    if get_opt("recoverycatalog", name) is not None:
        run(KN + ["delete", "recoverycatalog", name, "--wait=true"])
    since = now()
    apply(catalog_object(name, dest, "s1", **sync))
    return await_sync(name, "s1", seconds=seconds, since=since)


def sync_now(name: str, token: str, *, seconds: int = 600) -> dict[str, Any]:
    """Bump the one mutable field and wait for the controller to record that it
    acted on THAT token — never on a previous sync's result."""
    since = now()
    run(
        KN
        + [
            "patch",
            "recoverycatalog",
            name,
            "--type=merge",
            "-p",
            json.dumps({"spec": {"syncRequest": token}}),
        ]
    )
    return await_sync(name, token, seconds=seconds, since=since)


def view_entries(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    """Every entry of the materialised view, read from the page ConfigMaps and
    checked against the digest the status recorded for each page."""
    entries: list[dict[str, Any]] = []
    for page in catalog.get("status", {}).get("pages", []) or []:
        cm = get("configmap", page["configMapName"])
        body = cm["data"]["entries.jsonl"]
        digest = hashlib.sha256(body.encode()).hexdigest()
        if digest != page["sha256"].removeprefix("sha256:"):
            raise RuntimeError(
                f"page {page['configMapName']} does not match its recorded digest "
                f"{page['sha256']} (computed sha256:{digest})"
            )
        for line in body.splitlines():
            if line.strip():
                entries.append(json.loads(line))
    return entries


def catalog_summary(catalog: dict[str, Any]) -> dict[str, Any]:
    status = catalog.get("status", {})
    return {
        "counts": status.get("counts"),
        "cursor": status.get("cursor"),
        "truncated": status.get("truncated"),
        "pages": status.get("pages"),
        "signers": status.get("signers"),
        "conditions": {c["type"]: {k: c[k] for k in ("status", "reason", "message")}
                       for c in status.get("conditions", []) or []},
        "lastSyncJob": status.get("lastSyncJob"),
        "viewExpiresAt": status.get("viewExpiresAt"),
        "observedSyncRequest": status.get("observedSyncRequest"),
    }


def catalog() -> None:
    evidence: list[str] = []
    # A phase that starts from the bucket it left behind cannot assert a count.
    # This bucket is one this namespace created at `setup`, and nothing else
    # writes it.
    mc("rm", "--recursive", "--force", f"local/{BUCKET_A}/", check_rc=False)
    for kind, name in [("recoverycatalog", "primary")]:
        if get_opt(kind, name) is not None:
            run(KN + ["delete", kind, name, "--wait=true"])
    for job in lst("jobs", selector="app.kubernetes.io/managed-by=weirkeeper"):
        run(KN + ["delete", "job", job["metadata"]["name"], "--wait=false"], check=False)
    for b in lst("backups"):
        if (b["spec"].get("destinationRef") or {}).get("name") == "dest-a":
            run(KN + ["delete", "backup", b["metadata"]["name"], "--wait=true"])

    # --- real backups, into a destination this namespace created -------------
    points: dict[str, Any] = {}
    for name in ["point-a", "point-b", "point-c"]:
        obj = run_backup(name, "dest-a")
        points[name] = backup_facts(obj)
        log(
            f"{name}: backupId={points[name]['backupId']} records={points[name]['records']} "
            f"verification={verdict_of(obj)}"
        )
    STATE["points"] = points
    evidence.append(artifact("catalog/backups-before-deletion.json", points))

    archive_before = objects(BUCKET_A)
    evidence.append(artifact("catalog/archive-objects-before.json", archive_before))
    records = [o for o in archive_before if o["key"].startswith(f"{CATALOG_PREFIX}/points/")]
    check(
        "catalog-records-written",
        "PLAT-15.1",
        len([r for r in records if r["key"].endswith("record.json")]) == 3
        and len([r for r in records if r["key"].endswith("record.sig")]) == 3,
        f"the backup runner wrote 3 signed catalog point records under {CATALOG_PREFIX}/points/ "
        f"({len(records)} objects)",
        evidence,
    )

    # --- CR LOSS ------------------------------------------------------------
    for name in points:
        run(KN + ["delete", "backup", name, "--wait=true"])
    remaining = [
        b["metadata"]["name"]
        for b in lst("backups")
        if (b["spec"].get("destinationRef") or {}).get("name") == "dest-a"
    ]
    if remaining:
        raise RuntimeError(f"CR loss did not happen: {remaining}")
    evidence.append(
        artifact(
            "catalog/backups-after-deletion.txt",
            "every Backup CR that named dest-a is deleted; `kubectl get backups -n "
            f"{NS}` lists "
            + (", ".join(b["metadata"]["name"] for b in lst("backups")) or "<none>")
            + "\n",
        )
    )

    # --- the view, reconstructed from storage alone -------------------------
    since_primary = now()
    apply(catalog_object("primary", "dest-a", "t1"))
    cat_obj = await_sync("primary", "t1", since=since_primary)
    entries = view_entries(cat_obj)
    evidence.append(artifact("catalog/view-entries-after-cr-loss.json", entries))
    evidence.append(artifact("catalog/catalog-after-cr-loss.json", cat_obj))
    by_backup = {e.get("backupId"): e for e in entries}
    expected = {p["backupId"] for p in points.values()}
    ok = (
        len(entries) == 3
        and expected <= set(by_backup)
        and all(e.get("availability") == "Available" for e in entries)
        and all(e.get("verification") == "Verified" for e in entries)
        and all(e.get("selectable") is True for e in entries)
    )
    check(
        "catalog-reconstruction-after-cr-loss",
        "PLAT-15.1",
        ok,
        f"with zero Backup CRs the view lists {len(entries)} points "
        f"({sorted(set(by_backup))}) — availability "
        f"{sorted({e.get('availability') for e in entries})}, verification "
        f"{sorted({e.get('verification') for e in entries})}, selectable "
        f"{sorted({e.get('selectable') for e in entries})}",
        evidence,
    )
    STATE["viewEntriesAfterCrLoss"] = entries
    STATE["catalogSummary"] = catalog_summary(cat_obj)
    save()

    # --- the sync really was a `catalogSync` check Job -----------------------
    job_name = cat_obj["status"]["lastSyncJob"]["name"]
    job = get("job", job_name)
    plan_cm = next(
        (
            vol["configMap"]["name"]
            for vol in job["spec"]["template"]["spec"].get("volumes", []) or []
            if vol.get("name") == "check-plan" and vol.get("configMap")
        ),
        None,
    )
    plan = get("configmap", plan_cm) if plan_cm else {}
    plan_doc = json.loads(plan.get("data", {}).get("check-plan.json", "{}"))
    argv = job["spec"]["template"]["spec"]["containers"][0].get("args", [])
    evidence.append(artifact("catalog/sync-job.json", {"job": job, "plan": plan_doc}))
    check(
        "catalog-sync-is-the-one-check-runner",
        "PLAT-15.1",
        list(plan_doc.get("request", {}).keys()) == ["catalogSync"]
        and argv[:2] == ["check", "run"]
        and plan_doc.get("contract") == "logweir.dev/check-plan/v1",
        f"the sync ran through the one check runner: Job {job_name}, argv {argv}, "
        f"plan kind {list(plan_doc.get('request', {}).keys())}, contract "
        f"{plan_doc.get('contract')!r}",
        evidence,
    )


# ---------------------------------------------------------------------------
# PLAT-15.1 — the cases that make the two axes worth reading
# ---------------------------------------------------------------------------


def copy_config_map(name: str, as_name: str) -> str:
    """A plan ConfigMap survives its Backup only if something else owns a copy:
    the original is owned by the Backup and garbage-collected with it."""
    source = get("configmap", name)
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned(as_name),
            "data": source.get("data", {}),
        }
    )
    return as_name


def clone_runner_job(source: str, clone: str, plan_copy: str | None = None) -> str:
    """The SAME frozen inputs, run a second time.

    PLAT-06.1 case e and the RECEIPT-DUP defect: a Job re-created from a
    Backup's immutable plan ConfigMap writes a SECOND run-id receipt under the
    SAME execution id. That is how a duplicate identity is produced honestly —
    two real signed receipts for one `backupId` — rather than by hand-editing
    an archive.
    """
    job = get("job", source)
    spec = json.loads(json.dumps(job["spec"]["template"]["spec"]))
    if plan_copy:
        for vol in spec.get("volumes", []) or []:
            cm = vol.get("configMap")
            if cm and cm.get("name") == f"{source}-plan":
                cm["name"] = plan_copy
    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": owned(clone),
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 3600,
            "template": {"metadata": {"labels": dict(LABEL)}, "spec": spec},
        },
    }
    create(body)
    run(KN + ["wait", "--for=condition=Complete", f"job/{clone}", "--timeout=300s"])
    return clone


def index_entries(bucket: str) -> list[str]:
    return [o["key"] for o in objects(bucket, f"{CATALOG_PREFIX}/log/")]


def catalog_cases() -> None:
    entries_before = STATE.get("viewEntriesAfterCrLoss") or []
    if not entries_before:
        raise RuntimeError("run the `catalog` phase first")
    evidence: list[str] = []

    # --- duplicate identity: two receipts, one backupId ----------------------
    for leftover in ["point-d", "point-d-again"]:
        if get_opt("backup", leftover) is not None:
            run(KN + ["delete", "backup", leftover, "--wait=true"])
        if get_opt("job", leftover) is not None:
            run(KN + ["delete", "job", leftover, "--wait=true"])
    dup = run_backup("point-d", "dest-a")
    dup_backup_id = dup["status"]["backupId"]
    plan_copy = copy_config_map("point-d-plan", "point-d-plan-kept")
    clone_runner_job("point-d", "point-d-again", plan_copy)
    run(KN + ["delete", "backup", "point-d", "--wait=true"])
    after_dup = fresh_catalog("case-duplicate", "dest-a")
    dup_entries = view_entries(after_dup)
    same_id = [e for e in dup_entries if e["backupId"] == dup_backup_id]
    evidence.append(artifact("catalog/duplicate-identity-entries.json", dup_entries))
    evidence.append(artifact("catalog/duplicate-identity-status.json", catalog_summary(after_dup)))
    check(
        "catalog-duplicate-identity",
        "PLAT-15.1",
        len(same_id) == 2
        and len({e["pointId"] for e in same_id}) == 2
        and len({e["receiptSha256"] for e in same_id}) == 2,
        f"one backupId {dup_backup_id}, two real signed receipts from the SAME frozen "
        f"inputs, is TWO points: {sorted(e['pointId'] for e in same_id)} "
        f"(total entries {len(dup_entries)}, counts {after_dup['status']['counts']})",
        evidence,
    )
    STATE["duplicateBackupId"] = dup_backup_id

    # --- a deleted manifest is Missing; a deleted record is counted, not listed
    victim = [e for e in dup_entries if e["backupId"] != dup_backup_id][0]
    rm(BUCKET_A, victim["manifestKey"])
    ghost = [e for e in dup_entries if e["pointId"] not in {victim["pointId"]} and
             e["backupId"] != dup_backup_id][0]
    rm(BUCKET_A, f"{CATALOG_PREFIX}/points/{ghost['pointId']}/record.json")
    after_missing = fresh_catalog("case-missing", "dest-a")
    missing_entries = view_entries(after_missing)
    listed = {e["pointId"]: e for e in missing_entries}
    counts = after_missing["status"]["counts"]
    evidence.append(artifact("catalog/missing-manifest-entries.json", missing_entries))
    evidence.append(artifact("catalog/missing-manifest-status.json", catalog_summary(after_missing)))
    check(
        "catalog-missing-manifest",
        "PLAT-15.1",
        listed.get(victim["pointId"], {}).get("availability") == "Missing"
        and listed[victim["pointId"]]["selectable"] is False
        and counts.get("missing", 0) >= 1,
        f"the point whose manifest was deleted is availability="
        f"{listed.get(victim['pointId'], {}).get('availability')}, selectable="
        f"{listed.get(victim['pointId'], {}).get('selectable')}; counts {counts}",
        evidence,
    )
    check(
        "catalog-stale-index-entry",
        "PLAT-15.1",
        ghost["pointId"] not in listed
        and counts["total"] > len(missing_entries)
        and bool(after_missing["status"].get("pages")),
        f"a log index entry whose record object is gone is counted and NOT listed "
        f"({ghost['pointId']} absent from the view; counts.total={counts['total']}, "
        f"entries listed={len(missing_entries)}) and the walk still published a view "
        f"— the stale entry is never fatal. (The `Synced` condition read "
        f"{condition(after_missing, 'Synced').get('status')}/"
        f"{condition(after_missing, 'Synced').get('reason')} at this read; see "
        f"`catalog-resync-is-not-harvested` for why that condition is not a completion "
        f"signal on this build.)",
        evidence,
    )

    # --- a record from a future major is per-entry, never fatal --------------
    template_point = [e for e in missing_entries if e["availability"] == "Available"][0]
    record_key = f"{CATALOG_PREFIX}/points/{template_point['pointId']}/record.json"
    template_record = json.loads(cat(BUCKET_A, record_key).decode())
    future_point = "lwp1-" + "f" * 32
    future = dict(template_record)
    future["pointId"] = future_point
    future["formatVersion"] = "2.0.0"
    put(BUCKET_A, f"{CATALOG_PREFIX}/points/{future_point}/record.json",
        json.dumps(future).encode())
    put(BUCKET_A, f"{CATALOG_PREFIX}/points/{future_point}/record.sig",
        cat(BUCKET_A, f"{CATALOG_PREFIX}/points/{template_point['pointId']}/record.sig"))
    sample_index = index_entries(BUCKET_A)[0]
    index_body = cat(BUCKET_A, sample_index)
    day = sample_index.rsplit("/", 1)[0]
    new_index = json.loads(index_body.decode())
    new_index["pointId"] = future_point
    put(BUCKET_A, f"{day}/9999999999999-{future_point}.json", json.dumps(new_index).encode())
    after_future = fresh_catalog("case-format", "dest-a")
    future_entries = view_entries(after_future)
    fcounts = after_future["status"]["counts"]
    evidence.append(artifact("catalog/unsupported-format-status.json",
                             catalog_summary(after_future)))
    evidence.append(artifact("catalog/unsupported-format-entries.json", future_entries))
    listed_future = {e["pointId"]: e for e in future_entries}.get(future_point, {})
    check(
        "catalog-schema-version-compatibility",
        "PLAT-15.1",
        bool(after_future["status"].get("pages"))
        and fcounts["total"] > counts["total"]
        and listed_future.get("selectable") is not True,
        f"a record carrying `formatVersion: 2.0.0` is counted "
        f"(counts.total {counts['total']} -> {fcounts['total']}), is NEVER offered "
        f"(entry {listed_future or '<counted, not listed>'}), and does not stop the walk: "
        f"the view is still published with {len(after_future['status']['pages'])} page(s). "
        f"counts {fcounts}. LIMITATION, stated rather than smoothed over: "
        f"`counts.unsupportedFormat` is {fcounts.get('unsupportedFormat')} because a record "
        f"edited outside the runner no longer verifies under the signing key, and a record "
        f"that is BOTH validly signed AND of a future major cannot be produced without the "
        f"installation's private signing key — so this proves `never fatal` and `never "
        f"offered`, not the `UnsupportedFormat` classification itself",
        evidence,
    )
    # --- and the measurement that forced `fresh_catalog` on this harness ----
    started = time.time()
    probe_since = now()
    run(KN + ["patch", "recoverycatalog", "primary", "--type=merge", "-p",
              json.dumps({"spec": {"syncRequest": "resync-probe"}})])
    harvested = None
    while time.time() - started < 480:
        obj = get("recoverycatalog", "primary")
        if settled("resync-probe", probe_since)(obj):
            harvested = round(time.time() - started, 1)
            break
        time.sleep(10)
    obj = get("recoverycatalog", "primary")
    jobs = [j["metadata"]["name"] for j in lst("jobs")
            if j["metadata"]["name"].startswith("lwc-cs-")]
    completed = [
        j["metadata"]["name"]
        for j in lst("jobs")
        if j["metadata"]["name"].startswith("lwc-cs-")
        and (j.get("status", {}) or {}).get("succeeded")
    ]
    path = artifact(
        "catalog/resync-probe.json",
        {"harvestedAfterSeconds": harvested, "status": catalog_summary(obj),
         "syncJobs": jobs, "completedSyncJobs": completed},
    )
    record(
        "catalog-resync-is-not-harvested",
        "PLAT-15.1",
        "FAIL" if harvested is None else "PASS",
        f"`spec.syncRequest` is documented as 'change it to ask for a sync now'. On the "
        f"SECOND request this catalog's sync Job ran and completed "
        f"({len(completed)} of {len(jobs)} sync Jobs are Complete) and the controller never "
        f"harvested it: after 480 s the object still reads Synced="
        f"{condition(obj, 'Synced').get('status')}/{condition(obj, 'Synced').get('reason')} "
        f"with observedSyncRequest={obj['status'].get('observedSyncRequest')!r} and the "
        f"PREVIOUS view still published. harvestedAfterSeconds={harvested}",
        [path],
    )
    STATE["catalogCasesDone"] = True
    save()


# ---------------------------------------------------------------------------
# PLAT-16.1 — two destinations, and a report that names only its own
# ---------------------------------------------------------------------------


def retention_policy(
    name: str,
    dest: str,
    catalog: str,
    *,
    mode: str = "Report",
    rules: dict[str, Any] | None = None,
    enforcement: dict[str, Any] | None = None,
    external: dict[str, Any] | None = None,
    holds: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "destinationRef": {"name": dest},
        "catalogRef": {"name": catalog},
        "scope": {"prefix": DEST_PREFIX},
        "rules": rules or {"keepLast": 2, "minUsablePoints": 1},
        "mode": mode,
    }
    if enforcement:
        spec["enforcement"] = enforcement
    if external:
        spec["externalLifecycle"] = external
    if holds:
        spec["holds"] = holds
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "RetentionPolicy",
        "metadata": owned(name),
        "spec": spec,
    }


def wait_evaluated(name: str, *, seconds: int = 300, generation: int | None = None):
    def ready(o: dict[str, Any]) -> bool:
        ev = condition(o, "Evaluated")
        if ev.get("status") not in {"True", "False"}:
            return False
        if generation is not None and o.get("status", {}).get("observedGeneration") != generation:
            return False
        return True

    return wait_for("retentionpolicy", name, ready, seconds=seconds, what="an Evaluated verdict")


def evaluation_backup_ids(policy: dict[str, Any]) -> dict[str, set[str]]:
    ev = policy.get("status", {}).get("lastEvaluation", {}) or {}
    out: dict[str, set[str]] = {}
    for bucket in ["candidates", "protected", "skipped", "kept"]:
        rows = ev.get(bucket) or []
        out[bucket] = {
            r.get("backupId") for r in rows if isinstance(r, dict) and r.get("backupId")
        }
    return out


def schedule_object(name: str, dest: str) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": owned(name),
        "spec": {
            "schedule": "* * * * *",
            "sourceRef": {"name": "source"},
            "destinationRef": {"name": dest},
            "topics": TOPICS,
            "archive": {"url": f"logweir-destination://{dest}"},
            "concurrencyPolicy": "Forbid",
            "suspend": False,
        },
    }


def retention() -> None:
    evidence: list[str] = []
    names = ["b-1", "b-2", "b-3", "b-4", "b-5", "b-6"]
    have = [n for n in names
            if (get_opt("backup", n) or {}).get("status", {}).get("phase") == "Succeeded"]
    if len(have) != len(names) or os.environ.get("LOGWEIR_D3_FRESH_B"):
        mc("rm", "--recursive", "--force", f"local/{BUCKET_B}/", check_rc=False)
        for name in names:
            if get_opt("backup", name) is not None:
                run(KN + ["delete", "backup", name, "--wait=true"])
            run_backup(name, "dest-b")
            time.sleep(1)
    secondary = fresh_catalog("secondary", "dest-b")
    b_entries = view_entries(secondary)
    evidence.append(artifact("retention/dest-b-view.json", b_entries))
    STATE["bEntries"] = b_entries
    save()

    # A schedule that keeps firing on the OTHER destination for the whole phase.
    apply(schedule_object("keeps-running", "dest-a"))
    started = now()

    apply(retention_policy("keep-a", "dest-a", "primary", rules={"keepLast": 1,
                                                                "minUsablePoints": 1}))
    apply(retention_policy("keep-b", "dest-b", "secondary", rules={"keepLast": 2,
                                                                  "minUsablePoints": 3}))
    pol_a = wait_evaluated("keep-a")
    pol_b = wait_evaluated("keep-b")
    evidence.append(artifact("retention/keep-a.json", pol_a))
    evidence.append(artifact("retention/keep-b.json", pol_b))
    ev_a = (pol_a.get("status", {}) or {}).get("lastEvaluation") or {}
    ev_b = (pol_b.get("status", {}) or {}).get("lastEvaluation") or {}

    # --- the defect that decides everything below ---------------------------
    unreadable = [
        (name, condition(pol, "Evaluated"))
        for name, pol in [("keep-a", pol_a), ("keep-b", pol_b)]
        if condition(pol, "Evaluated").get("reason") == "ViewUnreadable"
    ]
    if unreadable:
        pages = {
            name: [
                {"configMapName": pg["configMapName"], "publishedSha256": pg["sha256"]}
                for pg in (get("recoverycatalog", cat)["status"].get("pages") or [])
            ]
            for name, cat in [("keep-a", "primary"), ("keep-b", "secondary")]
        }
        path = artifact(
            "retention/view-unreadable.json",
            {"conditions": {n: c for n, c in unreadable}, "catalogPages": pages},
        )
        record(
            "retention-view-digest-prefix-defect",
            "PLAT-16.1",
            "FAIL",
            "EVERY RetentionPolicy in this namespace lands `Ready=False/CatalogUnusable` + "
            "`Evaluated=False/ViewUnreadable`, and the controller's own message prints the "
            "two digests side by side as EQUAL apart from a prefix: "
            + unreadable[0][1].get("message", "")
            + ". `weirkeeper::catalog_view::page_digest` returns bare hex "
            "(`logweir_core::ids::sha256_hex`) while `status.pages[].sha256` is published "
            "`sha256:`-prefixed, and `controllers/retention_policy.rs:1345` compares the two "
            "with `!=`. No plan can be rendered for any destination on this build, so "
            "PLAT-16.1's report and PLAT-16.2's controller-driven enforcement are both "
            "unreachable through the controller.",
            [path],
        )

    a_ids = evaluation_backup_ids(pol_a)
    b_ids = evaluation_backup_ids(pol_b)
    a_universe = {e["backupId"] for e in (STATE.get("viewEntriesAfterCrLoss") or [])}
    b_universe = {e["backupId"] for e in b_entries}
    a_seen = set().union(*a_ids.values()) if a_ids else set()
    b_seen = set().union(*b_ids.values()) if b_ids else set()
    check(
        "retention-two-destinations",
        "PLAT-16.1",
        bool(ev_a) and bool(ev_b)
        and b_seen <= b_universe
        and not (b_seen & a_universe)
        and not (a_seen & b_universe)
        and ev_b.get("pointsEvaluated") == len(b_entries),
        f"keep-a (dest-a) names {len(a_seen)} backupIds and keep-b (dest-b) names "
        f"{len(b_seen)}; dest-a holds {len(a_universe)} and dest-b {len(b_universe)}. "
        f"keep-b pointsEvaluated={ev_b.get('pointsEvaluated')}. "
        + ("NO EVALUATION EXISTS: see `retention-view-digest-prefix-defect`."
           if not (ev_a and ev_b) else "No id crosses between the two reports."),
        evidence,
    )

    check(
        "retention-overlapping-keep-rules",
        "PLAT-16.2",
        len(ev_b.get("candidates") or []) == 3
        and len(ev_b.get("protected") or []) == 3
        and all(p.get("reason") == "MinUsablePoints" for p in (ev_b.get("protected") or [])),
        f"6 points, keepLast=2, minUsablePoints=3: {len(ev_b.get('candidates') or [])} "
        f"candidates and {len(ev_b.get('protected') or [])} protected with reasons "
        f"{sorted({p.get('reason') for p in (ev_b.get('protected') or [])})}"
        + ("" if ev_b else " — no evaluation was produced at all"),
        evidence,
    )

    skipped_states = {s.get("state") for s in (ev_a.get("skipped") or [])}
    check(
        "retention-unreadable-point-never-a-candidate",
        "PLAT-16.1",
        bool(ev_a.get("skipped"))
        and not ({s.get("backupId") for s in (ev_a.get("skipped") or [])}
                 & {c.get("backupId") for c in (ev_a.get("candidates") or [])}),
        f"dest-a's degraded points ({len(ev_a.get('skipped') or [])} rows, states "
        f"{sorted(skipped_states)}) are skipped and none is a candidate"
        + ("" if ev_a else " — no evaluation was produced at all"),
        evidence,
    )

    guarantees = (pol_b.get("status", {}) or {}).get("guarantees") or {}
    check(
        "retention-guarantees-never-flatter",
        "PLAT-16.1",
        guarantees.get("sharedSegments") == "NotEnforced"
        and guarantees.get("legalHold") != "LogweirEnforced",
        f"status.guarantees on the Report policy: {guarantees or '<absent>'}",
        evidence,
    )

    # --- two policies over one destination: BOTH refuse ---------------------
    apply(
        retention_policy(
            "declared-c", "dest-b", "secondary", mode="ExternalLifecycle",
            rules={"keepDays": 30, "minUsablePoints": 1},
            external={"provider": "s3", "prefix": DEST_PREFIX, "ruleId": "d3w14-rule",
                      "expirationDays": 7},
        )
    )
    contested = wait_for(
        "retentionpolicy", "declared-c",
        lambda o: bool(condition(o, "Ready").get("reason")),
        seconds=180, what="a Ready verdict while two policies claim dest-b",
    )
    other = get("retentionpolicy", "keep-b")
    evidence.append(artifact("retention/two-policies-one-destination.json",
                             {"declared-c": contested, "keep-b": other}))
    check(
        "retention-two-policies-one-destination",
        "PLAT-16.1",
        condition(contested, "Ready").get("reason") == "Conflict"
        and condition(contested, "Enforced").get("status") == "False",
        f"a second policy over dest-b puts it in Ready=False/"
        f"{condition(contested, 'Ready').get('reason')} — "
        f"{condition(contested, 'Ready').get('message', '')[:150]} — and neither enforces",
        evidence,
    )
    run(KN + ["delete", "retentionpolicy", "keep-b", "--wait=true"])

    # --- ExternalLifecycle is a DECLARATION, and the bucket wins -------------
    generation = get("retentionpolicy", "declared-c")["metadata"]["generation"]
    run(KN + ["patch", "retentionpolicy", "declared-c", "--type=merge", "-p",
              json.dumps({"metadata": {"annotations": {"logweir.dev/d3w14-nudge": now()}}})])
    apply(
        retention_policy(
            "declared-c", "dest-b", "secondary", mode="ExternalLifecycle",
            rules={"keepDays": 30, "minUsablePoints": 1},
            external={"provider": "s3", "prefix": DEST_PREFIX, "ruleId": "d3w14-rule",
                      "expirationDays": 7},
        )
    )
    declared = wait_for(
        "retentionpolicy",
        "declared-c",
        lambda o: bool(o.get("status", {}).get("conditions")),
        seconds=180,
        what="a verdict on the declared lifecycle",
    )
    evidence.append(artifact("retention/external-lifecycle.json", declared))
    conflict = condition(declared, "ExternalLifecycleConflict")
    dg = (declared.get("status", {}) or {}).get("guarantees") or {}
    check(
        "retention-external-lifecycle-is-declared-not-enforced",
        "PLAT-16.1",
        conflict.get("status") == "True"
        and dg.get("minUsablePoints") == "NotEnforced"
        and dg.get("activeRestoreProtection") == "NotEnforced"
        and dg.get("ageExpiry") == "ProviderEnforcedUnverified",
        f"a declared bucket rule expiring at 7 days under a policy that asks for 30 reports "
        f"ExternalLifecycleConflict={conflict.get('status')}/{conflict.get('reason')} "
        f"({conflict.get('message', '')[:120]}); guarantees {dg} — the bucket wins and "
        f"Logweir claims nothing it cannot enforce",
        evidence,
    )
    run(KN + ["delete", "retentionpolicy", "declared-c", "--wait=true"])
    apply(retention_policy("keep-b", "dest-b", "secondary",
                           rules={"keepLast": 2, "minUsablePoints": 3}))
    wait_evaluated("keep-b", seconds=240)

    # scheduled backups continue throughout
    children = [
        b for b in lst("backups")
        if b["metadata"]["name"].startswith("logweir-backup-keeps-running")
    ]
    finished = [b for b in children if b.get("status", {}).get("phase") == "Succeeded"]
    evidence.append(
        artifact(
            "retention/scheduled-during-evaluation.json",
            {"since": started, "children": [b["metadata"]["name"] for b in children],
             "succeeded": [b["metadata"]["name"] for b in finished]},
        )
    )
    check(
        "retention-scheduled-backups-continue",
        "PLAT-16.1",
        len(finished) >= 1,
        f"a `* * * * *` BackupSchedule on dest-a ran through the whole evaluation window: "
        f"{len(children)} children, {len(finished)} Succeeded — a retention verdict, "
        f"including a refused one, blocks no backup",
        evidence,
    )
    run(KN + ["patch", "backupschedule", "keeps-running", "--type=merge",
              "-p", json.dumps({"spec": {"suspend": True}})])


def plan_document(policy: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """The immutable plan the controller rendered, read out of its ConfigMap."""
    ref = policy["status"]["lastEvaluation"]["planRef"]["name"]
    cm = get("configmap", ref)
    key = next(iter(cm["data"]))
    return ref, json.loads(cm["data"][key])


PLAN_MEDIA_TYPE = "application/vnd.logweir.retention-plan+json;version=1.0.0"

# THE IMAGE THAT CARRIES THE ENFORCER. `Dockerfile` builds `-p logweir` only, so
# no image this repository ships contains `/usr/local/bin/logweir-retention`;
# this is an image built from the SAME commit with that one binary added. See
# the README, and the `retention-enforcer-ships-in-no-image` row.
# THE ENFORCER'S IMAGE — A REQUIRED INPUT WITH NO DEFAULT (defect RET-NOIMAGE).
#
# `retention_policy.rs` renders every enforcement Job's command as
# `logweir-retention` out of the controller's `LOGWEIR_RUNNER_IMAGE`, and NO
# image this repository builds contains that binary (`packaging` proves it live:
# exit 127, executable not found). Every scenario below that runs the enforcer
# therefore needs an image this tree does not produce, and `Dockerfile.retention`
# beside this file is the recipe for one.
#
# A DEFAULT HERE WOULD BE A LIE. It would make the phases look runnable from a
# clean checkout and fail with `ErrImageNeverPull` instead of saying why, so the
# value comes from `--retention-image <ref>` or `LOGWEIR_D3_RETENTION_IMAGE` and
# from nowhere else; without it the enforcement scenarios record NOT-RUN naming
# the defect, which is the honest verdict and not a skip.
RETENTION_IMAGE_ENV = "LOGWEIR_D3_RETENTION_IMAGE"
RETENTION_IMAGE: str | None = os.environ.get(RETENTION_IMAGE_ENV)

RET_NOIMAGE = (
    "NOT RUN: no image in this tree ships `logweir-retention` (defect RET-NOIMAGE). "
    "`Dockerfile` builds `-p logweir` and copies one binary, there is no product "
    "`Dockerfile.retention`, `.github/workflows/images.yml` builds three images and none "
    "is a retention image, and `charts/logweir/values.yaml` carries no retention image "
    "value — while `retention_policy.rs` names `logweir-retention` as every enforcement "
    "Job's command. Build one with `e2e/k8s/d3/Dockerfile.retention` (see its header) and "
    f"pass `--retention-image <ref>` or {RETENTION_IMAGE_ENV}=<ref> to run this scenario."
)


def retention_image(scenario: str, task: str = "PLAT-16.2") -> str | None:
    """The image, or a recorded NOT-RUN and `None`."""
    if RETENTION_IMAGE:
        return RETENTION_IMAGE
    record(scenario, task, "NOT-RUN", RET_NOIMAGE, [])
    return None


def dest_location(name: str) -> dict[str, Any]:
    dest = get("backupdestination", name)["spec"]["storage"]
    return {
        "provider": dest["provider"],
        "bucket": dest["bucket"],
        "prefix": dest.get("prefix", ""),
        "region": dest.get("region"),
        "endpoint": dest.get("endpoint"),
        "addressing": dest["addressing"],
        "transport": get("backupdestination", name)["spec"]["transport"]["security"],
    }


def build_plan(policy_name: str, dest: str, entries: list[dict[str, Any]], *,
               keep_last: int, min_usable: int) -> dict[str, Any]:
    """A plan document in the product's own format.

    HAND-BUILT, AND THE REASON IS RECORDED: the controller cannot render one on
    this build (`retention-view-digest-prefix-defect`). Every field is the one
    `weirkeeper::retention_plan::PlanDocument` and `logweir_reaper::Plan` agree
    on — both `deny_unknown_fields`, so a wrong shape is exit 3 and nothing is
    deleted, which is itself a check on this construction.
    """
    pol = get("retentionpolicy", policy_name)
    ordered = sorted(entries, key=lambda e: e["recoveryPointAtMs"], reverse=True)
    keep = max(keep_last, min_usable)
    candidates = ordered[keep:]
    lines = []
    for entry in candidates:
        lines.append(
            {
                "point_id": entry["pointId"],
                "backup_id": entry["backupId"],
                "reason": "KeepLast",
                "recovery_point_at_ms": entry["recoveryPointAtMs"],
                "manifest_key": entry["manifestKey"],
                "set_prefix": f"{DEST_PREFIX}/{entry['backupId']}/",
                "enumerate_set": True,
                "object_keys": [entry["manifestKey"]],
            }
        )
    return {
        "format": PLAN_MEDIA_TYPE,
        "format_version": "1.0.0",
        "policy_namespace": NS,
        "policy_name": policy_name,
        "policy_uid": pol["metadata"]["uid"],
        "location_id": get("backupdestination", dest)["status"]["canonicalUrl"],
        "scope_prefix": DEST_PREFIX,
        "keep_last": keep_last,
        "keep_days": None,
        "min_usable_points": min_usable,
        "lines": lines,
    }


def write_plan(cm_name: str, plan: dict[str, Any]) -> tuple[str, str]:
    body = json.dumps(plan, sort_keys=True, separators=(",", ":"))
    digest = "sha256:" + hashlib.sha256(body.encode()).hexdigest()
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": owned(cm_name),
            "data": {"plan.json": body},
        }
    )
    return digest, body


def secret_env(var: str, secret: str, key: str) -> dict[str, Any]:
    return {"name": var, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


def retention_job(
    name: str,
    plan_cm: str,
    *,
    dry_run: bool,
    digest: str,
    image: str,
    dest: str = "dest-b",
    delete_secret: str = "logweir-s3",
    evidence_secret: str | None = "logweir-s3",
    run_id: str | None = None,
    approver: str = "d3w14-live-acceptance",
    max_deletions: int = 50,
):
    """One `logweir-retention` run, as the operator Job docs/kubernetes.md §7f
    describes. The whole binding travels in the environment, never in argv."""
    args = ["run", "--plan", "/plan/plan.json", "--retention-contract-version", "1"]
    if dry_run:
        args.append("--dry-run")
    env = [
        {"name": "LOGWEIR_RETENTION_PLAN_SHA256", "value": digest},
        {"name": "LOGWEIR_RETENTION_POLICY_UID",
         "value": get("retentionpolicy", "keep-b")["metadata"]["uid"]},
        {"name": "LOGWEIR_RETENTION_POLICY_GENERATION", "value": "1"},
        {"name": "LOGWEIR_RETENTION_SCOPE_PREFIX", "value": DEST_PREFIX},
        {"name": "LOGWEIR_RETENTION_RUN_ID", "value": run_id or f"d3w14-{int(time.time())}"},
        {"name": "LOGWEIR_RETENTION_APPROVER", "value": approver},
        {"name": "LOGWEIR_RETENTION_MAX_DELETIONS", "value": str(max_deletions)},
        {"name": "LOGWEIR_RETENTION_MAX_OBJECTS", "value": "20000"},
        {"name": "LOGWEIR_RETENTION_LOCATION", "value": json.dumps(dest_location(dest))},
        secret_env("AWS_ACCESS_KEY_ID", delete_secret, "access-key-id"),
        secret_env("AWS_SECRET_ACCESS_KEY", delete_secret, "secret-access-key"),
        {"name": "AWS_ALLOW_HTTP", "value": "true"},
        {"name": "RUST_LOG", "value": "info"},
    ]
    if evidence_secret:
        env += [
            secret_env("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID", evidence_secret, "access-key-id"),
            secret_env(
                "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY", evidence_secret, "secret-access-key"
            ),
        ]
    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": owned(name),
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": dict(LABEL)},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": "logweir-retention",
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "name": "runner",
                            "image": image,
                            "imagePullPolicy": "Never",
                            "command": ["logweir-retention"],
                            "args": args,
                            "env": env,
                            "volumeMounts": [{"name": "plan", "mountPath": "/plan"}],
                        }
                    ],
                    "volumes": [{"name": "plan", "configMap": {"name": plan_cm}}],
                },
            },
        },
    }
    if get_opt("job", name) is not None:
        run(KN + ["delete", "job", name, "--wait=true"])
    create(body)
    deadline = time.time() + 300
    while time.time() < deadline:
        job = get("job", name)
        st = job.get("status", {}) or {}
        if st.get("succeeded") or st.get("failed"):
            break
        time.sleep(3)
    job = get("job", name)
    pods = lst("pods", selector=f"batch.kubernetes.io/job-name={name}")
    logs = ""
    exit_code = None
    if pods:
        logs = redact(run(KN + ["logs", pods[0]["metadata"]["name"]], check=False).stdout)
        for cs in (pods[0].get("status", {}).get("containerStatuses") or []):
            exit_code = (cs.get("state", {}).get("terminated") or {}).get("exitCode")
    return job, logs, exit_code


def key_lines(logs: str, prefix: str) -> list[dict[str, str]]:
    out = []
    for line in logs.splitlines():
        if line.startswith(prefix):
            out.append(dict(part.split("=", 1) for part in line.split() if "=" in part))
    return out


def preview() -> None:
    """The dry preview: real per-candidate object counts, and nothing deleted."""
    evidence: list[str] = []
    image = retention_image("retention-dry-preview")
    if image is None:
        return
    entries = STATE.get("bEntries") or []
    if not entries:
        raise RuntimeError("run the `retention` phase first")
    plan = build_plan("keep-b", "dest-b", entries, keep_last=2, min_usable=3)
    digest, body = write_plan("d3w14-plan", plan)
    STATE["plan"] = {"sha256": digest, "lines": len(plan["lines"]),
                     "pointIds": [ln["point_id"] for ln in plan["lines"]]}
    save()
    evidence.append(artifact("enforce/plan-document.json", plan))
    before = objects(BUCKET_B)
    job, logs, code = retention_job(
        "d3w14-preview", "d3w14-plan", dry_run=True, digest=digest, image=image
    )
    after = objects(BUCKET_B)
    evidence.append(artifact("enforce/preview-log.txt", logs))
    evidence.append(artifact("enforce/preview-objects-before.json", before))
    evidence.append(artifact("enforce/preview-objects-after.json", after))
    points = key_lines(logs, "retention-point=")
    counts = {p.get("retention-point"): p for p in points}
    check(
        "retention-dry-preview",
        "PLAT-16.2",
        code == 0
        and len(points) == len(plan["lines"])
        and all(p.get("state") == "Kept" and p.get("code") == "DryRun" for p in points)
        and all(int(p.get("objects", "0")) > 0 for p in points)
        and before == after,
        f"`logweir-retention run --dry-run` over the approved plan exited {code} and printed "
        f"{len(points)} per-point lines with REAL object counts "
        f"{ {k: v.get('objects') for k, v in counts.items()} }, every one "
        f"state=Kept code=DryRun; the archive listing is byte-identical "
        f"({len(before)} objects before and after: {before == after})",
        evidence,
    )
    STATE["previewCounts"] = {k: v.get("objects") for k, v in counts.items()}
    save()


def enforce() -> None:
    """One real enforced pass, and the record that attributes every deletion."""
    evidence: list[str] = []
    image = retention_image("enforce-record-verified-by-digest")
    if image is None:
        record("enforce-every-deletion-attributable", "PLAT-16.2", "NOT-RUN", RET_NOIMAGE, [])
        record("enforce-policy-change-invalidates-the-plan", "PLAT-16.2", "NOT-RUN",
               RET_NOIMAGE, [])
        return
    plan = json.loads((OUT / "enforce/plan-document.json").read_text())
    digest = STATE["plan"]["sha256"]
    before = objects(BUCKET_B)
    planned_sets = {ln["set_prefix"] for ln in plan["lines"]}
    expected_gone = {
        o["key"] for o in before
        if any(o["key"].startswith(sp) for sp in planned_sets)
    }
    job, logs, code = retention_job(
        "d3w14-enforce", "d3w14-plan", dry_run=False, digest=digest, image=image
    )
    after = objects(BUCKET_B)
    gone = {o["key"] for o in before} - {o["key"] for o in after}
    added = {o["key"] for o in after} - {o["key"] for o in before}
    evidence.append(artifact("enforce/enforce-log.txt", logs))
    evidence.append(artifact("enforce/objects-before-run.json", before))
    evidence.append(artifact("enforce/objects-after-run.json", after))

    record_line = key_lines(logs, "retention-record=")
    record_key = record_line[0]["retention-record"] if record_line else None
    reported = record_line[0].get("sha256") if record_line else None
    doc: dict[str, Any] = {}
    computed = None
    sidecar: list[str] = []
    if record_key:
        raw = cat(BUCKET_B, record_key)
        computed = "sha256:" + hashlib.sha256(raw).hexdigest()
        doc = json.loads(raw.decode())
        sidecar = [o["key"] for o in after if o["key"].startswith(record_key.rsplit(".", 1)[0])
                   and o["key"].endswith(".sig")]
        # THE RAW BYTES, UNTOUCHED, BESIDE THE PRETTY ONE. `computed` is over
        # what MinIO returned, so a reviewer who only has the pretty-printed
        # artifact cannot re-derive the digest from it. This file is written
        # byte for byte — no re-serialisation, no redaction pass — which is the
        # whole point; `sweep` still reads it like every other artifact.
        raw_path = OUT / "enforce/enforcement-record.raw.json"
        raw_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        raw_path.write_bytes(raw)
        evidence.append(str(raw_path))
        evidence.append(artifact("enforce/enforcement-record.json", doc))
        evidence.append(
            artifact(
                "enforce/record-digest.txt",
                f"recordKey            {record_key}\n"
                f"worker key line      {reported}\n"
                f"sha256(bytes read)   {computed}\n"
                f"match                {computed == reported}\n"
                f"signature sidecar    {sidecar or '<none: the record is UNSIGNED in this '
                f'build and is verified BY DIGEST, not by signature>'}\n",
            )
        )
    check(
        "enforce-record-verified-by-digest",
        "PLAT-16.2",
        bool(record_key) and computed == reported and not sidecar,
        f"the create-only enforcement record at {record_key} digests to {computed}; the "
        f"worker's own `retention-record=` key line reports {reported} — equal: "
        f"{computed == reported}. It has NO signature sidecar ({sidecar or 'none'}): this "
        f"build's record is UNSIGNED and this row verifies it BY DIGEST, not by signature",
        evidence,
    )

    attributed_points = {p.get("point_id") or p.get("pointId")
                         for p in (doc.get("points") or [])}
    attributed_keys: set[str] = set()
    for p in doc.get("points") or []:
        for k in (p.get("deleted_keys") or p.get("deletedKeys") or []):
            attributed_keys.add(k)
    logweir_before = {o["key"] for o in before if o["key"].startswith("logweir/")}
    logweir_after = {o["key"] for o in after if o["key"].startswith("logweir/")}
    check(
        "enforce-every-deletion-attributable",
        "PLAT-16.2",
        code == 0
        and bool(gone)
        and gone == expected_gone
        and all(k.startswith(f"{DEST_PREFIX}/") for k in gone)
        and logweir_before <= logweir_after
        and attributed_points == set(STATE["plan"]["pointIds"]),
        f"exit {code}; {len(gone)} objects removed and every one is a key the plan's set "
        f"bounds named ({gone == expected_gone}); every key is under {DEST_PREFIX}/ and no "
        f"`logweir/` object was removed ({len(logweir_before)} before, {len(logweir_after)} "
        f"after, added {sorted(added)[:3]}); the record attributes "
        f"{len(attributed_points)} point(s) {sorted(attributed_points)} against the plan's "
        f"{STATE['plan']['pointIds']}",
        evidence,
    )
    STATE["enforcement"] = {"recordKey": record_key, "digest": computed, "gone": sorted(gone),
                            "exitCode": code}
    save()

    # --- the same approved digest against CHANGED bytes -> nothing deleted ---
    tampered = json.loads(json.dumps(plan))
    tampered["min_usable_points"] = 1
    write_plan("d3w14-plan-tampered", tampered)
    before2 = objects(BUCKET_B)
    _job, logs2, code2 = retention_job(
        "d3w14-stale-digest", "d3w14-plan-tampered", dry_run=False, digest=digest, image=image
    )
    after2 = objects(BUCKET_B)
    evidence.append(artifact("enforce/stale-digest-log.txt", logs2))
    check(
        "enforce-policy-change-invalidates-the-plan",
        "PLAT-16.2",
        code2 == 3 and before2 == after2,
        f"a plan edited after approval (a `minUsablePoints` change, the shape a policy edit "
        f"produces) is refused before anything runs: exit {code2} (3 = REFUSED, zero "
        f"deleted), the archive listing is byte-identical ({before2 == after2}), and the "
        f"worker said "
        f"{[ln for ln in logs2.splitlines() if 'digest' in ln.lower()][:1]}",
        evidence,
    )


def packaging() -> None:
    """The enforcer binary is in no image this repository builds, and the
    retention controller names it out of `LOGWEIR_RUNNER_IMAGE`. This is the
    live proof, not an argument from the Dockerfile."""
    evidence: list[str] = []
    facts = controller_facts()
    shipped = facts["runnerImage"]
    name = "d3w14-shipped-image-probe"
    if get_opt("job", name) is not None:
        run(KN + ["delete", "job", name, "--wait=true"])
    create(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": owned(name),
            "spec": {
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": 3600,
                "template": {
                    "metadata": {"labels": dict(LABEL)},
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "runner",
                                "image": shipped,
                                "imagePullPolicy": "Never",
                                "command": ["logweir-retention"],
                                "args": ["run", "--retention-contract-version", "1"],
                            }
                        ],
                    },
                },
            },
        }
    )
    deadline = time.time() + 180
    state: dict[str, Any] = {}
    while time.time() < deadline:
        pods = lst("pods", selector=f"batch.kubernetes.io/job-name={name}")
        if pods:
            statuses = pods[0].get("status", {}).get("containerStatuses") or []
            if statuses:
                state = statuses[0].get("state", {}) or {}
                waiting = state.get("waiting") or {}
                terminated = state.get("terminated") or {}
                if waiting.get("reason") in {"CreateContainerError", "RunContainerError"} or (
                    terminated
                ):
                    break
        time.sleep(4)
    evidence.append(
        artifact("enforce/shipped-image-probe.json",
                 {"runnerImage": shipped, "containerState": state, "controller": facts})
    )
    message = json.dumps(state)
    check(
        "retention-enforcer-ships-in-no-image",
        "PLAT-16.2",
        "not found" in message or "no such file" in message.lower(),
        f"a Job running `logweir-retention` out of the image the shared controller names "
        f"({shipped}) cannot start: {message[:260]}. `Dockerfile` builds `-p logweir` and "
        f"copies one binary; there is no `Dockerfile.retention` and no retention image value "
        f"in `charts/logweir/values.yaml`, while `retention_policy.rs` renders every "
        f"enforcement Job's command as `logweir-retention` out of `LOGWEIR_RUNNER_IMAGE`. "
        f"Every enforcement row in this run was produced with an image built from THIS "
        f"commit with that binary added ({RETENTION_IMAGE or '<--retention-image not given>'}"
        f"), run as the operator Job "
        f"docs/kubernetes.md §7f prescribes",
        evidence,
    )


def wrong_prefix() -> None:
    """The worker re-derives every key bound from `scope_prefix` and refuses the
    WHOLE plan, deleting nothing, on the first key outside it."""
    evidence: list[str] = []
    image = retention_image("retention-wrong-prefix-refused")
    if image is None:
        return
    plan = json.loads((OUT / "enforce/plan-document.json").read_text())
    entries = STATE.get("bEntries") or []
    survivor = sorted(entries, key=lambda e: e["recoveryPointAtMs"], reverse=True)[0]
    tampered = json.loads(json.dumps(plan))
    tampered["lines"] = [
        {
            "point_id": survivor["pointId"],
            "backup_id": survivor["backupId"],
            "reason": "KeepLast",
            "recovery_point_at_ms": survivor["recoveryPointAtMs"],
            "manifest_key": f"elsewhere/{survivor['backupId']}/manifest.json",
            "set_prefix": f"elsewhere/{survivor['backupId']}/",
            "enumerate_set": True,
            "object_keys": [f"elsewhere/{survivor['backupId']}/manifest.json"],
        }
    ]
    digest, _ = write_plan("d3w14-plan-wrong-prefix", tampered)
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        "d3w14-wrong-prefix", "d3w14-plan-wrong-prefix", dry_run=False, digest=digest,
        image=image,
    )
    after = objects(BUCKET_B)
    evidence.append(artifact("enforce/wrong-prefix-log.txt", logs))
    evidence.append(artifact("enforce/wrong-prefix-plan.json", tampered))
    check(
        "retention-wrong-prefix-refused",
        "PLAT-16.2",
        code == 3 and before == after,
        f"a plan line whose key bound leaves `{plan['scope_prefix']}/<backupId>/` is refused "
        f"BEFORE anything runs: exit {code} (3 = REFUSED), the archive listing is "
        f"byte-identical ({before == after}), and the worker said "
        f"{[ln for ln in logs.splitlines() if 'scope' in ln.lower() or 'prefix' in ln.lower()][:1]}",
        evidence,
    )


def denied_deletion() -> None:
    """A credential that cannot delete. The run must record the refusal and
    remove nothing — the deletion authority is the credential's, not Logweir's."""
    evidence: list[str] = []
    image = retention_image("retention-denied-deletion")
    if image is None:
        return
    entries = STATE.get("bEntries") or []
    remaining = [
        e for e in entries
        if e["pointId"] not in set(STATE.get("plan", {}).get("pointIds") or [])
    ]
    if len(remaining) < 2:
        record("retention-denied-deletion", "PLAT-16.2", "NOT-RUN",
               "no point is left outside the enforced plan to aim a read-only run at", [])
        return
    victim = sorted(remaining, key=lambda e: e["recoveryPointAtMs"])[0]
    plan = json.loads((OUT / "enforce/plan-document.json").read_text())
    readonly = json.loads(json.dumps(plan))
    readonly["lines"] = [
        {
            "point_id": victim["pointId"],
            "backup_id": victim["backupId"],
            "reason": "KeepLast",
            "recovery_point_at_ms": victim["recoveryPointAtMs"],
            "manifest_key": victim["manifestKey"],
            "set_prefix": f"{DEST_PREFIX}/{victim['backupId']}/",
            "enumerate_set": True,
            "object_keys": [victim["manifestKey"]],
        }
    ]
    digest, _ = write_plan("d3w14-plan-readonly", readonly)
    # a MinIO user that may read and list and may NOT delete
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
                 "Resource": [f"arn:aws:s3:::{BUCKET_B}", f"arn:aws:s3:::{BUCKET_B}/*"]}
            ],
        }
    )
    # MINTED, NOT WRITTEN DOWN. The value is generated here, registered in
    # `MINTED`, and therefore scrubbed out of every recorded argv, every captured
    # stream and every artifact — and searched for literally by `sweep`. The
    # earlier hardcoded literal reached `state.json` through this very argv.
    reader_secret = mint()
    run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
              f"printf '%s' '{policy}' > /tmp/ro.json && "
              f"mc admin policy create adm d3w14-ro /tmp/ro.json >/dev/null 2>&1; "
              f"mc admin user add adm d3w14reader {reader_secret} >/dev/null 2>&1; "
              f"mc admin policy attach adm d3w14-ro --user d3w14reader >/dev/null 2>&1; echo done"],
        check=False, timeout=120)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned("d3w14-readonly-s3"),
            "stringData": {"access-key-id": "d3w14reader",
                           "secret-access-key": reader_secret},
        }
    )
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        "d3w14-denied", "d3w14-plan-readonly", dry_run=False, digest=digest, image=image,
        delete_secret="d3w14-readonly-s3",
    )
    after = objects(BUCKET_B)
    evidence.append(artifact("enforce/denied-deletion-log.txt", logs))
    points = key_lines(logs, "retention-point=")
    # The comparison is over the DATA prefix. A denied run still writes its
    # create-only intent tombstone under `logweir/` — that is the design (the
    # tombstone goes down before the delete is attempted), so the bucket
    # listing legitimately GROWS while nothing is removed.
    data_before = {o["key"] for o in before if o["key"].startswith(f"{DEST_PREFIX}/")}
    data_after = {o["key"] for o in after if o["key"].startswith(f"{DEST_PREFIX}/")}
    check(
        "retention-denied-deletion",
        "PLAT-16.2",
        data_before == data_after
        and code in {1, 3}
        and bool(points)
        and all(p.get("state") != "Deleted" for p in points),
        f"with a list/read-only credential the run exits {code} and removes NOTHING: the "
        f"{len(data_before)} objects under {DEST_PREFIX}/ are unchanged "
        f"({data_before == data_after}); the per-point lines read "
        f"{[{k: v for k, v in p.items() if k != 'objects'} for p in points]}. The listing "
        f"grew from {len(before)} to {len(after)} because the create-only intent tombstone "
        f"under `logweir/` is written BEFORE the delete is attempted — a denial leaves an "
        f"attributable trace and no data loss",
        evidence,
    )
    run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
              "mc admin user remove adm d3w14reader >/dev/null 2>&1; "
              "mc admin policy rm adm d3w14-ro >/dev/null 2>&1; echo done"],
        check=False, timeout=120)


def no_evidence_credential() -> None:
    """No `evidenceWrite` grant means exit 3 having deleted nothing: a deletion
    that cannot be attributed is not performed."""
    evidence: list[str] = []
    image = retention_image("retention-unattributable-deletion-refused")
    if image is None:
        return
    digest = STATE.get("plan", {}).get("sha256")
    if not digest:
        record("retention-unattributable-deletion-refused", "PLAT-16.2", "NOT-RUN",
               "no approved plan exists", [])
        return
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        "d3w14-no-evidence", "d3w14-plan", dry_run=False, digest=digest, image=image,
        evidence_secret=None,
    )
    after = objects(BUCKET_B)
    evidence.append(artifact("enforce/no-evidence-log.txt", logs))
    check(
        "retention-unattributable-deletion-refused",
        "PLAT-16.2",
        code == 3 and before == after,
        f"with no `evidenceWrite` credential projected the run exits {code} (3 = REFUSED) "
        f"having deleted nothing ({before == after}); the credential that deletes cannot "
        f"write the record that attributes the deletion, and neither alone is enough. "
        f"Worker said {[ln for ln in logs.splitlines() if 'evidence' in ln.lower()][:1]}",
        evidence,
    )


def legal_hold() -> None:
    """`spec.holds[]` names a point that may not be removed whatever the rules
    say. The evaluation must move it out of the candidate list."""
    evidence: list[str] = []
    pol = wait_evaluated("keep-b")
    candidates = ((pol.get("status", {}) or {}).get("lastEvaluation") or {}).get(
        "candidates"
    ) or []
    if not candidates:
        record(
            "retention-legal-hold",
            "PLAT-16.2",
            "NOT-RUN",
            "`spec.holds[]` is applied by the controller's EVALUATION, and this build renders "
            "no evaluation for any destination: Evaluated=False/"
            f"{condition(pol, 'Evaluated').get('reason')} (see "
            "`retention-view-digest-prefix-defect`). There is no candidate list for a hold to "
            "remove a point from, so the hold cannot be observed either way — it is not "
            "reported as working and not reported as broken.",
            [artifact("enforce/legal-hold-not-run.json", pol)],
        )
        return
    held = candidates[0]["pointId"]
    generation = get("retentionpolicy", "keep-b")["metadata"]["generation"] + 1
    patch_policy(
        "keep-b",
        {"holds": [{"pointId": held, "reason": "d3w14 live acceptance: a legal hold"}]},
    )
    after = wait_evaluated("keep-b", generation=generation, seconds=300)
    ev = after["status"]["lastEvaluation"]
    protected = {p["pointId"]: p for p in (ev.get("protected") or [])}
    evidence.append(artifact("enforce/legal-hold.json", after))
    check(
        "retention-legal-hold",
        "PLAT-16.2",
        held not in {c["pointId"] for c in (ev.get("candidates") or [])}
        and held in protected,
        f"the held point {held} left the candidate list and is reported protected with "
        f"reason {protected.get(held, {}).get('reason')!r}; guarantees.legalHold is "
        f"{after['status']['guarantees'].get('legalHold')} (never `LogweirEnforced`: "
        f"object_store 0.14 exposes no WORM readback)",
        evidence,
    )
    patch_policy("keep-b", {"holds": []})


# ---------------------------------------------------------------------------
# PLAT-19.1 — trust lifecycle against a terminal, already-verified Backup
# ---------------------------------------------------------------------------

LEGACY_ARCHIVE = f"s3://kafka-backups/{OWNER}-{STAMP}"
TRUST_POLICY = f"{OWNER}-{STAMP}"


def legacy_backup(name: str, topics: list[str] | None = None) -> dict[str, Any]:
    """An inline-`archive` Backup against the fixture's own bucket.

    THIS IS NOT A PREFERENCE. A destination-backed run's evidence verdict is
    never written on this build unless `evidenceRead.mode` is
    `ControllerIdentity` AND the installation policy allowlists the location —
    the lab's `weirkeeper-policy` deliberately allowlists nothing, and
    `backup.rs`'s `SecretKeys` arm says in so many words that this build creates
    no evidence-fetch Job. The controller's ONE global read-only handle is
    rooted at the fixture's archive, so a Backup-level verdict is only
    reachable there. Retention enforcement never touches this bucket.
    """
    body = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": owned(name),
        "spec": {
            "sourceRef": {"name": "source"},
            "topics": topics or ["orders"],
            "archive": {"url": LEGACY_ARCHIVE, "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "deadlineSeconds": 600,
        },
    }
    if get_opt("backup", name) is not None:
        run(KN + ["delete", "backup", name, "--wait=true"])
    create(body)
    wait_for("backup", name, terminal, seconds=600, what="a terminal phase")
    return wait_for(
        "backup", name, lambda o: verdict_of(o) in VERDICTS, seconds=240,
        what="an evidence verdict",
    )


def roster_signing_key() -> dict[str, Any]:
    roster = get("trustroster", "default", namespace="default")
    return roster["spec"]["signingKeys"][0]


def trust_policy(state: str, **over: Any) -> dict[str, Any]:
    key = roster_signing_key()
    entry = {
        "keyId": key["keyId"],
        "spkiPem": key["spkiPem"],
        "algorithm": "p256",
        "principal": {"id": key.get("subject", "signing@scram-local.invalid"),
                      "display": "the lab signing key"},
        "usages": ["EvidenceSigning"],
        "state": state,
        "notBefore": "2026-01-01T00:00:00Z",
        "notAfter": "2027-01-01T00:00:00Z",
    }
    entry.update(over)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicy",
        "metadata": owned(TRUST_POLICY, namespace=None),
        "spec": {"namespaces": [NS], "keys": [entry]},
    }


def await_trust(name: str, predicate: Callable[[dict[str, Any]], bool], *, seconds: int = 240,
                what: str = "") -> dict[str, Any]:
    return wait_for(
        "backup", name, lambda o: predicate(
            o.get("status", {}).get("evidence", {}).get("verification", {}) or {}
        ),
        seconds=seconds, what=what,
    )


def trust() -> None:
    evidence: list[str] = []
    fresh = legacy_backup("trust-subject")
    v = fresh["status"]["evidence"]["verification"]
    before_fields = {
        "phase": fresh["status"]["phase"],
        "exitCode": fresh["status"]["exitCode"],
        "complete": condition(fresh, "Complete"),
        "records": fresh["status"].get("records"),
    }
    evidence.append(artifact("trust/fresh-object-verdict.json", v))
    check(
        "trust-fresh-object-records-signedAt",
        "PLAT-19.1",
        v.get("result") == "Valid"
        and bool(v.get("signedAt"))
        and v.get("trust", {}).get("basis") == "Current"
        and v.get("trust", {}).get("policy", {}).get("name") == "legacy-roster-v1",
        f"a run on THIS build records signedAt={v.get('signedAt')} and comes back "
        f"{v.get('result')} under {v.get('trust', {}).get('policy', {}).get('name')} "
        f"(basis {v.get('trust', {}).get('basis')}, keyState "
        f"{v.get('trust', {}).get('keyState')}); status.records={before_fields['records']}",
        evidence,
    )

    apply(trust_policy("Active"))
    active = await_trust(
        "trust-subject",
        lambda x: x.get("trust", {}).get("policy", {}).get("name") == TRUST_POLICY,
        what="the explicit TrustPolicy to take over",
    )
    va = active["status"]["evidence"]["verification"]
    evidence.append(artifact("trust/verdict-under-active-policy.json", va))
    check(
        "trust-explicit-policy-keeps-evidence-valid",
        "PLAT-19.1",
        va.get("result") == "Valid" and va.get("trust", {}).get("keyState") == "Active",
        f"binding an explicit TrustPolicy that lists the key Active leaves the terminal "
        f"Backup {va.get('result')} (basis {va.get('trust', {}).get('basis')}, keyState "
        f"{va.get('trust', {}).get('keyState')}, policy "
        f"{va.get('trust', {}).get('policy', {}).get('name')})",
        evidence,
    )

    apply(trust_policy("Retired", retiredAt=now()))
    retired = await_trust(
        "trust-subject",
        lambda x: x.get("trust", {}).get("keyState") == "Retired",
        what="the retirement to be re-derived",
    )
    vr = retired["status"]["evidence"]["verification"]
    evidence.append(artifact("trust/verdict-under-retired-key.json", vr))
    check(
        "trust-rotation-old-evidence-still-verified",
        "PLAT-19.1",
        vr.get("result") == "Valid" and vr.get("trust", {}).get("basis") == "Historical",
        f"retiring the signing key keeps the old evidence verified: result "
        f"{vr.get('result')}, basis {vr.get('trust', {}).get('basis')}, keyState "
        f"{vr.get('trust', {}).get('keyState')} — a rotation is not a trust gap",
        evidence,
    )

    apply(
        trust_policy(
            "Revoked",
            retiredAt=vr.get("trust", {}).get("retiredAt") or now(),
            revokedAt=now(),
            revocationEffectiveFrom=now(),
            revocationReason="KeyCompromise",
        )
    )
    revoked = await_trust(
        "trust-subject",
        lambda x: x.get("result") == "Untrusted"
        or x.get("trust", {}).get("keyState") == "Revoked",
        what="the revocation to reach the terminal object",
    )
    vv = revoked["status"]["evidence"]["verification"]
    after_fields = {
        "phase": revoked["status"]["phase"],
        "exitCode": revoked["status"]["exitCode"],
        "complete": condition(revoked, "Complete"),
        "records": revoked["status"].get("records"),
    }
    evidence.append(artifact("trust/verdict-after-revocation.json",
                             {"verification": vv, "before": before_fields,
                              "after": after_fields}))
    check(
        "trust-revocation-flips-a-terminal-badge",
        "PLAT-19.1",
        vv.get("result") == "Untrusted"
        and before_fields["phase"] == after_fields["phase"]
        and before_fields["exitCode"] == after_fields["exitCode"]
        and before_fields["complete"].get("reason") == after_fields["complete"].get("reason"),
        f"revoking the key for KeyCompromise flips the TERMINAL Backup's badge to "
        f"{vv.get('result')} ({(vv.get('reason') or vv.get('detail') or '')[:110]}) on the "
        f"next policy event, while phase={after_fields['phase']}, exitCode="
        f"{after_fields['exitCode']} and conditions[Complete]="
        f"{after_fields['complete'].get('reason')} are unchanged",
        evidence,
    )


# ---------------------------------------------------------------------------
# The §8 upgrade defect, measured once more on a FRESH object
# ---------------------------------------------------------------------------


def signed_at_probe() -> None:
    """`lab-refresh-2` §8: five 2026-09-14 fixture objects whose status predates
    `status.evidence.verification.signedAt` were re-derived Valid -> Untrusted
    (`SignedOutsideValidity`, "carries no signing-time field"). This measures the
    same thing on an object THIS run created:

    1. a fresh run records `signedAt` and comes back `Valid`;
    2. with `signedAt` removed from the stored status — which is exactly the
       shape of a pre-`signedAt` object — the next policy event re-derives the
       SAME receipt to `Untrusted`;
    3. and then: does any path restore it? The re-trust pass reads the stored
       claim and performs no fetch, so the only candidate an operator has is
       clearing the whole `verification` block and hoping for a full re-verify
       that reads the archive. This tries exactly that and records the answer.
    """
    evidence: list[str] = []

    # --- first, the CEL lifecycle rules, on the policy `trust` left Revoked --
    illegal = run(
        K + ["apply", "-f", "-", "-o", "json"],
        data=json.dumps(trust_policy("Active")),
        check=False,
    )
    refusal = redact(illegal.stderr.strip())
    evidence.append(artifact("trust/monotonic-refusal.txt", refusal + "\n"))
    check(
        "trust-lifecycle-is-monotonic",
        "PLAT-19.1",
        illegal.returncode != 0 and "never backwards" in refusal,
        f"walking the key back from Revoked to Active is refused by the CRD's own CEL rules, "
        f"with the rules' messages: {refusal.splitlines()[1:] if refusal else '<none>'}",
        evidence,
    )
    # a NEW policy object: the lifecycle rules are per-object history, and this
    # probe needs a policy event it can repeat.
    if get_opt("trustpolicy", TRUST_POLICY, namespace="default") is not None:
        run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"])
    apply(trust_policy("Active"))

    fresh = legacy_backup("signedat-subject")
    v0 = fresh["status"]["evidence"]["verification"]
    evidence.append(artifact("trust/signedat-1-fresh.json", v0))
    fresh_ok = v0.get("result") == "Valid" and bool(v0.get("signedAt"))

    run(
        KN
        + [
            "patch", "backup", "signedat-subject", "--subresource=status", "--type=merge",
            "-p", json.dumps({"status": {"evidence": {"verification": {"signedAt": None}}}}),
        ]
    )
    stripped = get("backup", "signedat-subject")["status"]["evidence"]["verification"]
    evidence.append(artifact("trust/signedat-2-stripped.json", stripped))
    run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"])
    apply(trust_policy("Active", notAfter="2027-06-01T00:00:00Z"))
    deadline = time.time() + 240
    v2: dict[str, Any] = stripped
    while time.time() < deadline:
        v2 = (
            get("backup", "signedat-subject").get("status", {}).get("evidence", {})
            .get("verification", {}) or {}
        )
        if v2.get("result") != "Valid" or v2.get("signedAt"):
            break
        time.sleep(5)
    evidence.append(artifact("trust/signedat-3-rederived.json", v2))

    run(
        KN
        + [
            "patch", "backup", "signedat-subject", "--subresource=status", "--type=merge",
            "-p", json.dumps({"status": {"evidence": {"verification": None}}}),
        ]
    )
    run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"])
    apply(trust_policy("Active", notAfter="2027-07-01T00:00:00Z"))
    deadline = time.time() + 150
    restored: dict[str, Any] = {}
    while time.time() < deadline:
        restored = (
            get("backup", "signedat-subject").get("status", {}).get("evidence", {})
            .get("verification", {}) or {}
        )
        if restored.get("result") == "Valid" and restored.get("signedAt"):
            break
        time.sleep(5)
    evidence.append(artifact("trust/signedat-4-after-clearing-the-block.json", restored))
    recovered = restored.get("result") == "Valid" and bool(restored.get("signedAt"))
    record(
        "trust-signedat-upgrade-defect",
        "PLAT-19.1",
        "PASS" if fresh_ok and v2.get("result") == "Untrusted" else "FAIL",
        f"fresh run: result={v0.get('result')} signedAt={v0.get('signedAt')}. The SAME "
        f"receipt with `signedAt` absent from its stored status re-derives to result="
        f"{v2.get('result')} "
        f"({(v2.get('reason') or v2.get('detail') or '')[:130]}). Clearing "
        f"`status.evidence.verification` entirely does NOT trigger a re-verify that reads "
        f"the archive: after 150 s the block is {restored or '<still absent>'} "
        f"(restored={recovered}). No documented path re-reads the archive for a TERMINAL "
        f"object — the re-trust pass re-derives from the stored matchedKeyId/signedAt/"
        f"verifiedAt and performs no fetch — so a pre-`signedAt` object cannot be repaired "
        f"in place by any operator action this build offers",
        evidence,
        freshVerification=v0,
        rederivedVerification=v2,
        afterClearing=restored,
    )


# ---------------------------------------------------------------------------
# PLAT-14.2 — a stale point alerts once, and a failed delivery rewrites nothing
# ---------------------------------------------------------------------------

SINK_POD = "d3w14-sink"
SINK_PORT = 8080


def sink_pod() -> dict[str, Any]:
    script = (
        "touch /tmp/posts.log; while true; do "
        "printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 3\\r\\nConnection: close\\r\\n\\r\\nok\\n' "
        f"| nc -l -p {SINK_PORT} >> /tmp/posts.log 2>/dev/null; done"
    )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": SINK_POD, "namespace": NS,
                     "labels": dict(LABEL, **{"app": "d3w14-sink"})},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "sink",
                    "image": "busybox:latest",
                    "imagePullPolicy": "Never",
                    "command": ["/bin/sh", "-c"],
                    "args": [script],
                    "ports": [{"containerPort": SINK_PORT}],
                }
            ],
        },
    }


def sink_posts() -> int:
    out = run(KN + ["exec", SINK_POD, "--", "/bin/sh", "-c",
                    "grep -c '^POST' /tmp/posts.log 2>/dev/null || echo 0"], check=False).stdout
    try:
        return int(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def protection_policy(name: str, *, max_age: int) -> dict[str, Any]:
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "ProtectionPolicy",
        "metadata": owned(name),
        "spec": {
            "protects": {
                "sourceRef": {"name": "source"},
                "destinationRef": {"name": "dest-a"},
                "catalogRef": {"name": "primary"},
                "scheduleRefs": [{"name": "keeps-running"}],
                "topics": TOPICS,
            },
            "objectives": {
                "maxRecoveryPointAgeSeconds": max_age,
                "requireVerifiedEvidence": True,
                "requireCatalogAvailability": True,
            },
            "evaluationIntervalSeconds": 60,
            "notifications": {
                "kinds": ["Staleness", "BackupFailure", "ArchiveUnavailable"],
                "sendResolved": True,
                "routes": [
                    {
                        "name": "local-sink",
                        "webhook": {"urlSecretRef": {"name": "d3w14-sink-url", "key": "url"}},
                    }
                ],
            },
        },
    }


def notify() -> None:
    evidence: list[str] = []
    if get_opt("pod", SINK_POD) is None:
        apply(sink_pod())
        run(KN + ["wait", "--for=condition=Ready", f"pod/{SINK_POD}", "--timeout=120s"])
    sink_ip = get("pod", SINK_POD)["status"]["podIP"]
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned("d3w14-sink-url"),
            "stringData": {"url": f"http://{sink_ip}:{SINK_PORT}/alerts"},
        }
    )
    if get_opt("backupschedule", "keeps-running") is not None:
        run(KN + ["patch", "backupschedule", "keeps-running", "--type=merge",
                  "-p", json.dumps({"spec": {"suspend": True}})])
    posts_before = sink_posts()
    backups_before = {b["metadata"]["name"]: b["metadata"]["resourceVersion"]
                      for b in lst("backups")}
    apply(protection_policy("protect-a", max_age=300))
    wait_for(
        "protectionpolicy",
        "protect-a",
        lambda o: bool((o.get("status", {}) or {}).get("health")),
        seconds=300,
        what="a health verdict",
    )
    # three evaluation intervals' worth of wall clock, to prove the alert does
    # not re-fire: `renotifyAfterSeconds` is unset, so one transition is one
    # alert however many times the policy is reconciled.
    time.sleep(200)
    settled_obj = get("protectionpolicy", "protect-a")
    status = settled_obj["status"]
    alerts = status.get("alerts") or []
    # BY OWNER, not by a name guess: a delivery Job is owned by the policy.
    policy_uid = settled_obj["metadata"]["uid"]
    jobs = [
        j for j in lst("jobs")
        if any(o.get("uid") == policy_uid for o in (j["metadata"].get("ownerReferences") or []))
    ]
    transitions = {j["metadata"]["name"].rsplit("-", 1)[0] for j in jobs}
    evidence.append(artifact("notify/protection-policy.json", settled_obj))
    evidence.append(artifact("notify/delivery-jobs.json",
                             [{"name": j["metadata"]["name"], "status": j.get("status", {})}
                              for j in jobs]))
    logs = {}
    for j in jobs:
        pods = lst("pods", selector=f"batch.kubernetes.io/job-name={j['metadata']['name']}")
        if pods:
            logs[j["metadata"]["name"]] = redact(
                run(KN + ["logs", pods[0]["metadata"]["name"]], check=False).stdout
            )
    evidence.append(artifact("notify/delivery-logs.json", logs))
    posts_after = sink_posts()
    backups_after = {b["metadata"]["name"]: b["metadata"]["resourceVersion"]
                     for b in lst("backups")}
    delivery = (alerts[0].get("delivery") if alerts else {}) or {}
    check(
        "notify-stale-point-alerts-exactly-once",
        "PLAT-14.2",
        len(alerts) == 1
        and len(transitions) == 1
        and delivery.get("attempts", 0) <= 3
        and status.get("health") in {"Stale", "Unprotected", "Unknown"},
        f"health={status.get('health')}; over ~3 evaluation intervals the policy holds "
        f"{len(alerts)} open alert and every delivery Job belongs to ONE transition "
        f"({sorted(transitions)}, {len(jobs)} Job(s) = the bounded retry, "
        f"attempts={delivery.get('attempts')} of at most 3). One transition is one alert "
        f"however many times the policy is reconciled. Alert rows: "
        f"{[{k: a.get(k) for k in ('kind', 'state', 'delivery')} for a in alerts]}",
        evidence,
    )
    unchanged = {n: r for n, r in backups_before.items() if backups_after.get(n) == r}
    check(
        "notify-failure-never-rewrites-a-backup",
        "PLAT-14.2",
        posts_after == posts_before
        and len(unchanged) == len(backups_before),
        f"the plaintext sink received {posts_after - posts_before} POST(s) — `logweir notify "
        f"deliver` refuses a non-https webhook before it dials (NOTIFY_ALLOW_INSECURE_SINKS "
        f"is the documented escape hatch and nothing in the ProtectionPolicy or the delivery "
        f"Job spec sets it), so the failure is visible on ProtectionPolicy.status and "
        f"nowhere else — and every one of the {len(backups_before)} Backups in this "
        f"namespace still carries the resourceVersion it had before the protection "
        f"controller ran ({len(unchanged)} unchanged)",
        evidence,
    )


# ---------------------------------------------------------------------------
# The negative control, the report and the cleanup
# ---------------------------------------------------------------------------


def control() -> None:
    """A harness that cannot fail proves nothing. This asserts something that is
    false about the same live objects the passing scenarios read, through the
    same `check` path, and records it as the FAIL it is."""
    # A LIVE READ, NOT THE CACHE. Asserting over `STATE` would prove that the
    # catalog phase read the cluster, which is the thing under test, not the
    # evidence for it. This re-reads the catalog's page ConfigMaps now, through
    # the same digest-checked path every catalog row uses.
    live = get_opt("recoverycatalog", "primary")
    source = "a live read of RecoveryCatalog/primary"
    if live is None or not live.get("status", {}).get("pages"):
        entries = STATE.get("viewEntriesAfterCrLoss") or []
        source = "the recorded view (RecoveryCatalog/primary is gone — run `control` before "
        source += "`cleanup` for the live form)"
    else:
        entries = view_entries(live)
    ok = check(
        "negative-control-must-fail",
        "-",
        bool(entries) and all(e.get("availability") == "Missing" for e in entries),
        "DELIBERATE: asserts that every reconstructed catalog entry is `Missing` when "
        f"{source} says {sorted({e.get('availability') for e in entries})}. This row is "
        "expected to FAIL; a run in which it PASSES means the harness is not reading the "
        "cluster.",
    )
    STATE["scenarios"]["negative-control-must-fail"]["expected"] = "FAIL"
    save()
    if ok:
        raise RuntimeError("the negative control passed, so no other result here is evidence")


CREDENTIAL_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)\baws_secret_access_key\b\s*[:=]\s*[\"']?[A-Za-z0-9+/=_-]{8,}"),
    re.compile(r"(?i)\bpassword\b\s*[\"']?\s*[:=]\s*[\"']?(?![\[<])[A-Za-z0-9+/=_-]{8,}"),
    # A POSITIONAL SECRET. `mc admin user add <alias> <user> <secret>` puts the
    # value where no `key=value` pattern can see it, which is exactly how the
    # first version of this sweep reported zero hits over the one credential the
    # run had minted.
    re.compile(
        r"(?i)\b(?:admin\s+user\s+add|user\s+add)\s+\S+\s+\S+\s+(?!\[REDACTED)(\S{8,})"
    ),
]


def sweep(extra: list[str] | None = None) -> dict[str, Any]:
    """Every artifact this run wrote, read back and searched for anything
    credential-shaped — and for every value this run itself minted, by exact
    match. A redaction that is never checked is a hope, so `sweep_selftest`
    proves this function can fail before `report` trusts it not to."""
    needles = sorted(MINTED | set(extra or []))
    hits: list[dict[str, Any]] = []
    scanned = 0
    for path in sorted(OUT.rglob("*")):
        if not path.is_file():
            continue
        scanned += 1
        text = path.read_text(errors="replace")
        for pattern in CREDENTIAL_PATTERNS:
            for match in pattern.finditer(text):
                hits.append({"file": str(path.relative_to(OUT)), "pattern": pattern.pattern,
                             "at": match.start(), "sample": "<not reproduced>"})
        for needle in needles:
            at = text.find(needle)
            if at >= 0:
                hits.append({"file": str(path.relative_to(OUT)), "pattern": "<minted value>",
                             "at": at, "sample": "<not reproduced>"})
    return {"filesScanned": scanned, "hits": hits, "mintedValues": len(MINTED),
            "patterns": len(CREDENTIAL_PATTERNS)}


def sweep_selftest() -> dict[str, Any]:
    """THE MUTANT, PLANTED AND KILLED IN ONE STEP.

    A sweep that reports zero hits is worthless unless it is known to be able to
    report one. This writes a file carrying both shapes the sweep exists for — a
    minted value and a positional `mc admin user add` secret — asserts the sweep
    finds both, and removes it. If either is missed the run fails here rather
    than printing a clean sweep that means nothing.
    """
    probe = mint()
    positional = "kubectl exec pod -- mc admin user add adm probeuser " + "P" * 20
    path = OUT / "sweep-selftest.tmp"
    path.write_text(f"{probe}\n{positional}\n")
    try:
        result = sweep()
        found = {h["pattern"] for h in result["hits"] if h["file"] == "sweep-selftest.tmp"}
        if "<minted value>" not in found or len(found) < 2:
            raise RuntimeError(
                f"the credential sweep cannot fail: the planted probe produced {found}"
            )
    finally:
        path.unlink(missing_ok=True)
        MINTED.discard(probe)
    return {"plantedPatterns": sorted(found), "killed": True}


def report() -> None:
    STATE["controllerAtReport"] = controller_facts()
    STATE["revision"] = run(["git", "rev-parse", "HEAD"]).stdout.strip()
    STATE["originMain"] = run(["git", "rev-parse", "origin/main"], check=False).stdout.strip()
    rows = list(STATE["scenarios"].values())
    verdicts: dict[str, int] = {}
    for row in rows:
        key = "negative-control" if row.get("expected") == "FAIL" else row["verdict"]
        verdicts[key] = verdicts.get(key, 0) + 1
    STATE["summary"] = verdicts
    STATE["credentialSweepSelfTest"] = sweep_selftest()
    sweep_result = sweep()
    STATE["credentialSweep"] = sweep_result
    save()
    artifact("results.json", STATE)
    lines = [
        "# d3w14 — live docker-desktop acceptance results",
        f"context: docker-desktop   namespace: {NS}   uid: {STATE.get('namespaceUid')}",
        f"revision: {STATE.get('revision')}   origin/main: {STATE.get('originMain')}",
        f"controller: {json.dumps(STATE.get('controllerAtReport'))}",
        f"buckets: {STATE.get('buckets')}",
        "",
        f"{'SCENARIO':<52} {'TASK':<12} VERDICT",
    ]
    for row in sorted(rows, key=lambda r: r["scenario"]):
        lines.append(f"{row['scenario']:<52} {row['task']:<12} {row['verdict']}")
    lines += ["", f"summary: {json.dumps(verdicts)}",
              f"credential sweep: {sweep_result['filesScanned']} files, "
              f"{sweep_result['patterns']} patterns, {sweep_result['mintedValues']} minted "
              f"value(s), {len(sweep_result['hits'])} hits "
              f"(self-test: {STATE['credentialSweepSelfTest']})"]
    print("\n".join(lines))
    artifact("results.txt", "\n".join(lines) + "\n")
    if sweep_result["hits"]:
        raise RuntimeError(f"credential sweep found {len(sweep_result['hits'])} hits")


def render_cleanup(proof: dict[str, Any]) -> str:
    lines = [
        "# d3w14 cleanup proof",
        "",
        f"- recorded (UTC): {proof['at']}",
        "- context: docker-desktop (every command; no other context was named)",
        f"- namespace: `{NS}`",
        f"- namespace uid at setup: `{STATE.get('namespaceUid')}`",
        f"- namespace uid at cleanup: `{proof.get('namespaceUid')}`",
        f"- owner label checked before deletion: `{proof.get('ownerLabel')}`",
        f"- namespace after deletion: **{proof.get('namespaceAfter')}**",
        "",
        "## Buckets this run created in the shared MinIO, and removed",
        "",
    ]
    for row in proof.get("buckets", []):
        lines.append(f"- `{row['bucket']}` — {row['objectsRemoved']} objects, bucket removed")
    lines += [
        "",
        "## Cluster-scoped state",
        "",
        f"- {proof.get('clusterScopedLeft')}"
        + (f" (uid `{proof.get('trustPolicy', {}).get('uid')}`, namespaces "
           f"{proof.get('trustPolicy', {}).get('namespaces')})"
           if proof.get("trustPolicy") else ""),
        "",
        "## What is deliberately left behind",
        "",
        f"- {proof.get('sharedArchiveLeftBehind')}",
        "",
    ]
    return "\n".join(lines)


def cleanup() -> None:
    """Deletes ONLY what this run created, after checking the owner label and
    the UID recorded at setup."""
    proof: dict[str, Any] = {"at": now(), "namespace": NS}
    ns = get_opt("namespace", NS, namespace="default")
    if ns is None:
        proof["namespaceAfter"] = "NotFound (already absent)"
    else:
        labels = ns["metadata"].get("labels", {})
        uid = ns["metadata"]["uid"]
        proof["namespaceUid"] = uid
        proof["ownerLabel"] = labels.get("logweir.dev/test-owner")
        if labels.get("logweir.dev/test-owner") != OWNER:
            raise RuntimeError(f"refusing to delete {NS}: owner label is {labels}")
        if STATE.get("namespaceUid") and uid != STATE["namespaceUid"]:
            raise RuntimeError(f"refusing to delete {NS}: uid {uid} is not {STATE['namespaceUid']}")
    policy = get_opt("trustpolicy", TRUST_POLICY, namespace="default")
    if policy is not None:
        if policy["metadata"].get("labels", {}).get("logweir.dev/test-owner") != OWNER:
            raise RuntimeError(f"refusing to delete TrustPolicy/{TRUST_POLICY}: not ours")
        proof["trustPolicy"] = {"uid": policy["metadata"]["uid"],
                                "namespaces": policy["spec"].get("namespaces")}
        run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"])
    buckets: list[dict[str, Any]] = []
    for bucket in STATE.get("buckets", []):
        if not bucket.startswith(f"{OWNER}-"):
            raise RuntimeError(f"refusing to remove bucket {bucket}: not this run's")
        listing = objects(bucket)
        mc("rb", "--force", f"local/{bucket}", check_rc=False)
        buckets.append({"bucket": bucket, "objectsRemoved": len(listing)})
    proof["buckets"] = buckets
    if ns is not None:
        run(K + ["delete", "namespace", NS, "--wait=true"], timeout=600)
        proof["namespaceAfter"] = (
            "NotFound" if get_opt("namespace", NS, namespace="default") is None
            else "STILL PRESENT"
        )
    proof["clusterScopedLeft"] = (
        "the one TrustPolicy this run created was deleted" if policy is not None
        else "none created"
    )
    proof["sharedArchiveLeftBehind"] = (
        f"objects under s3://kafka-backups/{OWNER}-{STAMP}/ and the matching "
        f"logweir/backups/<execution-id>/ evidence written by the legacy-archive Backups are "
        f"NOT deleted: this harness never deletes from the shared fixture's bucket."
    )
    artifact("cleanup.md", render_cleanup(proof))
    print(json.dumps(proof, indent=2))


PHASES = [
    "setup", "catalog", "catalog_cases", "retention", "legal_hold", "packaging", "preview",
    "enforce", "wrong_prefix", "denied_deletion", "no_evidence_credential", "trust",
    "signed_at_probe", "notify", "control", "report", "cleanup",
]


USAGE = """usage: d3_live.py <phase> [--retention-image <ref>]

phases: {phases}

--retention-image <ref>   the image carrying `logweir-retention`, which NO image
                          this repository builds contains (defect RET-NOIMAGE).
                          Required by preview, enforce, wrong-prefix,
                          denied-deletion and no-evidence-credential; those
                          scenarios record NOT-RUN without it. Build one with
                          e2e/k8s/d3/Dockerfile.retention. May also be given as
                          {env}.
"""


def main() -> int:
    global RETENTION_IMAGE
    argv = sys.argv[1:]
    if "--retention-image" in argv:
        at = argv.index("--retention-image")
        if at + 1 >= len(argv):
            print(USAGE.format(phases=", ".join(p.replace("_", "-") for p in PHASES),
                               env=RETENTION_IMAGE_ENV))
            return 2
        RETENTION_IMAGE = argv[at + 1]
        del argv[at:at + 2]
    if len(argv) != 1 or argv[0].replace("-", "_") not in PHASES:
        print(USAGE.format(phases=", ".join(p.replace("_", "-") for p in PHASES),
                           env=RETENTION_IMAGE_ENV))
        return 2
    globals()[argv[0].replace("-", "_")]()
    save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
