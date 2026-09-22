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
`logweir.dev/test-owner=<OWNER>` (`LOGWEIR_D3_OWNER`, default `d3w14`);
`cleanup` refuses a namespace or a bucket that
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
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable

ROOT = pathlib.Path(__file__).resolve().parents[3]
STAMP = os.environ.get("LOGWEIR_D3_STAMP", "20260918t0000z")
# THE OWNER IS THE RUN'S, NOT THE FILE'S. Every object this harness creates
# carries `logweir.dev/test-owner=<OWNER>`, `cleanup` refuses a namespace or a
# bucket that does not, and the shared MinIO's users and policies are named
# from it — so two workers running this harness at once must not share it.
# `d3w14` stays the default because it is what the recorded evidence of
# 2026-09-18 was produced under; a later worker sets its own and gets its own
# namespace, buckets and MinIO identities with no edit to this file.
OWNER = os.environ.get("LOGWEIR_D3_OWNER", "d3w14")
NS = os.environ.get("LOGWEIR_D3_NS", f"{OWNER}-{STAMP}")
# MinIO user names are not DNS labels: a hyphen is legal but an owner like
# `d3-rows` reads better as one token beside `mc admin user add`.
OWNER_TAG = re.sub(r"[^a-z0-9]", "", OWNER)
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
# THE SHARED MinIO'S OWN NAMESPACE HAS NO NAMESPACES. A user and a policy
# created here outlive this run's Kubernetes namespace and are visible to every
# other worker, so both carry the owner and both are removed by the phase that
# minted them.
RO_USER = f"{OWNER_TAG}reader"
RO_POLICY = f"{OWNER_TAG}-ro"
RO_SECRET = f"{OWNER}-readonly-s3"
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

MC_POD = f"{OWNER}-mc"


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


def destination(name: str, bucket: str, *, write_secret: str = "logweir-s3",
                prefix: str = DEST_PREFIX) -> dict[str, Any]:
    """A `BackupDestination` this namespace owns.

    `prefix` IS A PARAMETER because one destination has to name an archive this
    harness did not write through a destination at all: the legacy inline
    archive the trust and protection-verdict rows use lives at
    `s3://kafka-backups/<owner>-<stamp>`, and a `RecoveryCatalog` can only be
    pointed at a `destinationRef`. Declaring that root as a destination is how
    a catalog reads points the controller verified itself.
    """
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
            "description": f"{OWNER} live acceptance, bucket {bucket}",
            "storage": {
                "provider": "S3",
                "bucket": bucket,
                "prefix": prefix,
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


def backup_object(name: str, dest: str, topics: list[str] | None = None,
                  schedule: dict[str, str] | None = None) -> dict[str, Any]:
    """A manual `Backup`, optionally a manual run OF a schedule.

    # `schedule` IS NOT DECORATION — it is what makes a point SELECTABLE

    A `ProtectionPolicy` carrying `spec.protects.scheduleRefs` counts a run as
    history only through `identity::is_run_of_schedule`, which accepts
    `spec.scheduleRef.{name,uid}`, the legacy controller `ownerReference` or
    PLAT-05.2's retention annotation — and therefore "counts a manual run of
    the schedule as history"
    (`controllers/protection_policy.rs::is_member`). D3 §3.1 says the same from
    the other side: `scheduleRefs: [{name: nightly}]` means "points produced by
    these schedules count", and §3.2 names the authority in as many words —
    **"the `spec.scheduleRef.uid` field is the authority and the label is the
    index"**.

    A manual `Backup` with none of those three is NOT a member. Every
    protection row in this file measured exactly that shape against a policy
    naming a schedule, so the policies had an EMPTY candidate set and
    `Unprotected` was an answer about nothing. Measured live on 2026-09-22
    (`verdicts/probe-schedulerefs-membership.json`): `status.lastAttempt: null`
    — no run counted at all.

    Only `uid` and `name` are set. `runPolicySha256` is deliberately omitted:
    `identity::check_run_policy_digest` returns `Ok` when it is absent and
    compares it to the spec's own recomputed digest when it is present, so
    writing one here would be inventing a control-plane fact. No
    `ownerReference` is set either — this run is a manual run OF the schedule,
    not a run the schedule created and may garbage-collect.
    """
    spec: dict[str, Any] = {
        "sourceRef": {"name": "source"},
        "destinationRef": {"name": dest},
        "topics": topics or TOPICS,
        "archive": {"url": f"logweir-destination://{dest}"},
        "triggeredBy": "manual",
        "deadlineSeconds": 600,
    }
    if schedule:
        spec["scheduleRef"] = {"name": schedule["name"], "uid": schedule["uid"]}
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": owned(name),
        "spec": spec,
    }


def schedule_ref(name: str) -> dict[str, str]:
    """`{name, uid}` for a `BackupSchedule` that exists, read from the object.

    The UID is read rather than assumed because "a schedule deleted and
    recreated under the same name is a different schedule and must not adopt
    this run" (`crds/backup.rs::ScheduleRef::uid`) — and a stale UID here would
    silently un-select every point the row depends on.
    """
    return {"name": name, "uid": get("backupschedule", name)["metadata"]["uid"]}


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


def image_revision(image: str | None) -> str | None:
    """`org.opencontainers.image.revision` off a local image, or `None`."""
    if not image:
        return None
    probe = run(
        ["docker", "image", "inspect", "--format",
         '{{index .Config.Labels "org.opencontainers.image.revision"}}', image],
        check=False, timeout=60,
    )
    revision = probe.stdout.strip()
    return revision if probe.returncode == 0 and revision and revision != "<no value>" else None


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
    image = (pod.get("spec", {}).get("containers") or [{}])[0].get("image")
    return {
        "pod": pod.get("metadata", {}).get("name"),
        "imageID": (pod.get("status", {}).get("containerStatuses") or [{}])[0].get("imageID"),
        "image": image,
        # WHICH COMMIT THE BUILD CAME FROM, recorded rather than taken on trust.
        # An imageID is a content digest and says nothing about a revision; CI
        # (and WORKER-RULES, for a hand-built image) sets
        # `org.opencontainers.image.revision`, and a live claim about a build is
        # worth less without it. Best-effort: the label is read off the local
        # daemon and an absent docker, image or label records `None`.
        "imageRevision": image_revision(image),
        "runnerImage": env.get("LOGWEIR_RUNNER_IMAGE"),
        "policyConfigMap": env.get("LOGWEIR_POLICY_CONFIGMAP"),
        # The documented escape hatch for a non-https webhook. It decides
        # whether a plaintext sink is dialled at all, so a notification row that
        # did not read it would be asserting against an unread configuration.
        "allowInsecureSinks": env.get("LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS"),
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
    name: str, dest: str, topics: list[str] | None = None, settle: int = 12,
    schedule: dict[str, str] | None = None,
) -> dict[str, Any]:
    create(backup_object(name, dest, topics, schedule))
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
    # catalog (`intervalSeconds: 0`) publishes its FIRST view and then has no
    # interval to publish another on. The SECOND-REQUEST half of that finding —
    # CATALOG-RESYNC-NOT-HARVESTED, where a bump ran a Job to Complete that the
    # controller never harvested — is CLOSED on this build; see
    # `catalog-resync-harvest`.
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
    `Synced` condition was not usable as a completion signal at all on the
    2026-09-18 build — a published view was routinely followed by a write that
    put the condition back to `Unknown/PodNotStarted`
    (`catalog-resync-is-not-harvested`, CLOSED on `af64073`, where the same
    read finds `True/Succeeded`). `status.syncedAt` moving forward is still the
    one signal that means the pages in `status.pages` are this walk's, and
    reading the fact rather than the condition costs nothing.
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

    Not a `syncRequest` bump — but NO LONGER because a bump is not harvested.
    That was CATALOG-RESYNC-NOT-HARVESTED, and it is CLOSED on this build:
    `catalog-resync-harvest` measures the harvest every run and read 10.5 s on
    `af64073`, where 480 s was not enough on 2026-09-18, and `refresh_view`
    bumps `syncRequest` because that is what the product documents.

    A fresh object per archive generation stays for a smaller reason: these
    phases assert exact counts over an archive they have just mutated, and a
    new object's first view is unambiguously about the archive as it is now,
    with no previous generation for a reader of the evidence to confuse it
    with.
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


RECEIPT_KEY_PREFIX = "logweir/backups"
ULID_LEN = 26


def reconstructed_view_ok(entries: list[dict[str, Any]],
                          expected_backup_ids: set[str],
                          want: int) -> dict[str, bool]:
    """The view rebuilt from storage alone, and every field a restore binds to.

    THE LAST THREE CLAUSES ARE CATALOG-RECEIPTKEY-REDACTED. This row used to
    ask only for the count and the two axes, so it passed through a refresh in
    which every point published `receiptKey: "[redacted].receipt.json"` — the
    run id in the key is a 26-character ULID and `check::redact_path`'s
    free-component budget was 24, so the redactor ate the key while
    `receiptSha256` and the location beside it survived. D3 §5.5 step 4 builds
    a restore plan's `source.point {point_id, receipt_key, receipt_sha256,
    manifest_sha256}` from these entries, so the field the console needs as a
    plan binding was the one field it could not use, and no live row could
    fail. The key is derived in exactly one place
    (`logweir::backup::phase_run::receipt_keys`), so the row can name it.
    """
    by_backup = {e.get("backupId"): e for e in entries}
    keys = [(e.get("receiptKey") or "") for e in entries]
    return {
        "the view lists every point with zero Backup CRs":
            len(entries) == want and expected_backup_ids <= set(by_backup),
        "every point is Available": bool(entries) and all(
            e.get("availability") == "Available" for e in entries),
        "every point is Verified": bool(entries) and all(
            e.get("verification") == "Verified" for e in entries),
        "every point is selectable": bool(entries) and all(
            e.get("selectable") is True for e in entries),
        "no published receiptKey carries the redaction marker":
            bool(keys) and all("[redacted]" not in k for k in keys),
        "every receiptKey is the key the backup runner wrote":
            bool(entries) and all(
                e.get("receiptKey")
                == f"{RECEIPT_KEY_PREFIX}/{e.get('backupId')}/{e.get('runId')}.receipt.json"
                for e in entries),
        "and its run id is a 26-character ULID, which is what made the key redactable":
            bool(entries) and all(
                len(e.get("runId") or "") == ULID_LEN for e in entries),
    }


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
    clauses = reconstructed_view_ok(entries, expected, 3)
    check(
        "catalog-reconstruction-after-cr-loss",
        "PLAT-15.1",
        all(clauses.values()),
        f"with zero Backup CRs the view lists {len(entries)} points "
        f"({sorted(set(by_backup))}) — availability "
        f"{sorted({e.get('availability') for e in entries})}, verification "
        f"{sorted({e.get('verification') for e in entries})}, selectable "
        f"{sorted({e.get('selectable') for e in entries})}, receiptKey "
        f"{sorted({e.get('receiptKey') for e in entries})}. Clauses {clauses}",
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


def record_of(bucket: str, point_id: str) -> dict[str, Any]:
    """One point's signed record, as it is stored."""
    return json.loads(cat(bucket, f"{CATALOG_PREFIX}/points/{point_id}/record.json").decode())


def index_entries(bucket: str) -> list[str]:
    return [o["key"] for o in objects(bucket, f"{CATALOG_PREFIX}/log/")]


def future_major_document(template: dict[str, Any], planted: str) -> dict[str, Any]:
    """A record this build cannot interpret, from a real one.

    SNAKE_CASE, AND THAT IS THE WHOLE POINT. `CatalogPoint` is serialised with
    `format_version` and `point_id`; a probe that sets `formatVersion` adds an
    UNKNOWN FIELD, which major 1 ignores by design (`catalog/reader.rs` rule 2),
    leaving a major-1 record carrying the template's own identity. The first
    version of the schema-version row did exactly that, read
    `counts.unsupportedFormat == 0`, and concluded that the classification
    needs the installation's private signing key. It needs no signature at all:
    `examine` classifies from these bytes and returns before it fetches the
    receipt (`check/kinds/catalog_sync.rs:1191`).
    """
    doc = dict(template)
    doc["format_version"] = "2.0.0"
    doc["point_id"] = planted
    doc.pop("formatVersion", None)
    doc.pop("pointId", None)
    return doc


def future_major_index_entry(template: dict[str, Any], planted: str) -> dict[str, Any]:
    """The index row that makes the planted record reachable.

    Its `record_key` must be the one `point_id` implies or the reader drops the
    row as `Inconsistent` (review finding F6) — which would leave the record
    unlisted for a reason that has nothing to do with its format.
    """
    entry = dict(template)
    entry["point_id"] = planted
    entry["record_key"] = f"{CATALOG_PREFIX}/points/{planted}/record.json"
    entry.pop("pointId", None)
    entry.pop("recordKey", None)
    return entry


def plant_future_major(bucket: str, template_point: str, planted: str) -> dict[str, Any]:
    """The record, its index row, and the keys both went to."""
    record = future_major_document(record_of(bucket, template_point), planted)
    record_key = f"{CATALOG_PREFIX}/points/{planted}/record.json"
    put(bucket, record_key, json.dumps(record).encode())
    sample = sorted(index_entries(bucket))[-1]
    entry = future_major_index_entry(json.loads(cat(bucket, sample).decode()), planted)
    entry_key = f"{sample.rsplit('/', 1)[0]}/{int(time.time() * 1000):013d}-{planted}.json"
    put(bucket, entry_key, json.dumps(entry).encode())
    return {"pointId": planted, "recordKey": record_key, "indexKey": entry_key,
            "formatVersion": record["format_version"]}


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
    future_point = "lwp1-" + hashlib.sha256(f"{OWNER}{STAMP}cases".encode()).hexdigest()[:32]
    planted_here = plant_future_major(BUCKET_A, template_point["pointId"], future_point)
    evidence.append(artifact("catalog/planted-future-record.json", planted_here))
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
        and fcounts.get("unsupportedFormat", 0) >= 1
        and listed_future.get("selectable") is not True
        and future_point not in {e["pointId"] for e in future_entries},
        f"a record carrying `formatVersion: 2.0.0` is counted "
        f"(counts.total {counts['total']} -> {fcounts['total']}), is NEVER offered "
        f"(entry {listed_future or '<counted, not listed>'}), and does not stop the walk: "
        f"the view is still published with {len(after_future['status']['pages'])} page(s). "
        f"counts {fcounts}, of which unsupportedFormat="
        f"{fcounts.get('unsupportedFormat')}. THE PREVIOUS VERSION OF THIS ROW PLANTED "
        f"`formatVersion` (camelCase) into a document whose field is `format_version`, which "
        f"major 1 ignores as an unknown field: it measured an unsigned edit to a major-1 "
        f"record, read `counts.unsupportedFormat == 0`, and concluded the classification "
        f"needed the installation's private signing key. It needs no signature at all — "
        f"`examine` classifies from the record's own bytes and returns before it fetches the "
        f"receipt or the sidecar (`crates/logweir/src/check/kinds/catalog_sync.rs:1191`)",
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
        "catalog-resync-harvest",
        "PLAT-15.1",
        "FAIL" if harvested is None else "PASS",
        (
            f"`spec.syncRequest` is documented as 'change it to ask for a sync now', and on "
            f"the SECOND request this catalog published a view for that token after "
            f"{harvested}s ({len(completed)} of {len(jobs)} sync Jobs Complete; Synced="
            f"{condition(obj, 'Synced').get('status')}/"
            f"{condition(obj, 'Synced').get('reason')}, observedSyncRequest="
            f"{obj['status'].get('observedSyncRequest')!r}). CATALOG-RESYNC-NOT-HARVESTED — "
            f"the 2026-09-18 blocker, where the Job ran to Complete and the controller never "
            f"harvested it, 480s was not enough, and `Synced` flipped back to "
            f"`Unknown/PodNotStarted` after a publish — IS CLOSED on this build. The row was "
            f"named `catalog-resync-is-not-harvested` while that was true; it asserts the "
            f"same measurement either way and is named for the measurement now."
            if harvested is not None else
            f"`spec.syncRequest` is documented as 'change it to ask for a sync now'. On the "
            f"SECOND request this catalog's sync Job ran and completed "
            f"({len(completed)} of {len(jobs)} sync Jobs are Complete) and the controller "
            f"never harvested it: after 480 s the object still reads Synced="
            f"{condition(obj, 'Synced').get('status')}/"
            f"{condition(obj, 'Synced').get('reason')} with observedSyncRequest="
            f"{obj['status'].get('observedSyncRequest')!r} and the PREVIOUS view still "
            f"published — CATALOG-RESYNC-NOT-HARVESTED, open on this build."
        ),
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
    """Wait for an Evaluated verdict, PREFERRING a settled `True`.

    Any verdict used to do, because under RET-DIGEST-PREFIX no policy ever
    reached `True` and a harness that insisted on one would have hung instead of
    measuring the defect. With the defect closed that tolerance became a race:
    re-running this phase in an existing namespace recreates the catalog, a
    policy reconciled inside that window lands
    `Evaluated=False/ViewUnreadable` naming the absent catalog, and the harness
    read that half-second as the answer (observed 2026-09-18). So `True` is
    waited for first, and a settled `False` is accepted only once most of the
    window is gone — a policy that really cannot evaluate is still observed,
    and still reported, just not mistaken for one that is mid-reconcile.
    """
    def settled(o: dict[str, Any], want_true: bool) -> bool:
        if generation is not None and o.get("status", {}).get("observedGeneration") != generation:
            return False
        state = condition(o, "Evaluated").get("status")
        return state == "True" if want_true else state in {"True", "False"}

    deadline = time.time() + seconds
    try:
        return wait_for("retentionpolicy", name, lambda o: settled(o, True),
                        seconds=max(10, int(seconds * 0.6)), what="an Evaluated=True verdict")
    except RuntimeError:
        pass
    return wait_for("retentionpolicy", name, lambda o: settled(o, False),
                    seconds=max(10, int(deadline - time.time())),
                    what="any settled Evaluated verdict")


# `SkippedEntry.reason`'s closed vocabulary
# (`crates/weirkeeper/src/crds/retention_policy.rs`). A row that only counted
# entries would not notice a controller inventing a reason.
SKIPPED_REASONS = {"Unreadable", "UnsupportedFormat", "Conflict"}


def evaluation_point_ids(policy: dict[str, Any]) -> dict[str, set[str]]:
    """Every point id the evaluation names, per bucket.

    IT IS `pointId`, AND `kept` IS A LIST OF BARE IDS. This read used to ask
    each row for a `backupId` no bucket has ever carried, so every set came back
    empty and `retention-two-destinations` asserted set relations between four
    empty sets — a row that could not fail, passing on the first evaluation the
    controller was ever able to produce (lab-refresh-3 §8.2). `kept` is
    `Vec<String>`; the other three are structs keyed by `pointId`.
    """
    ev = policy.get("status", {}).get("lastEvaluation", {}) or {}
    out: dict[str, set[str]] = {}
    for bucket in ["candidates", "protected", "skipped", "kept"]:
        rows = ev.get(bucket) or []
        out[bucket] = {
            row if isinstance(row, str) else row.get("pointId")
            for row in rows
            if (row if isinstance(row, str) else row.get("pointId"))
        }
    return out


# ---------------------------------------------------------------------------
# The row decisions, as named functions
#
# Each of these is the boolean a `check()` below rests on, lifted out of the
# call site so it can be fed a recorded object without a cluster —
# `e2e/k8s/d3/test_rows.py` runs every one of them against the shape the
# product publishes today AND against the shape it published before the defect
# was fixed, and requires the second to be refused. A row whose decision cannot
# be made to say False is not a row.
# ---------------------------------------------------------------------------


def reports_are_disjoint(ev_a: dict[str, Any], ev_b: dict[str, Any],
                         a_seen: set[str], b_seen: set[str],
                         a_universe: set[str], b_universe: set[str],
                         b_points: int) -> bool:
    """Two destinations, two reports, no id in common — and neither empty.

    The emptiness clauses are the point: without them this is a disjointness
    claim that an evaluation naming nothing at all satisfies. dest-a is not
    required to be inside its recorded universe, because a schedule keeps
    adding points to it after the view was captured; dest-b is closed.
    """
    return (
        bool(ev_a) and bool(ev_b)
        and bool(a_seen) and bool(b_seen)
        and b_seen <= b_universe
        and not (b_seen & a_universe)
        and not (a_seen & b_universe)
        and ev_b.get("pointsEvaluated") == b_points
    )


def keep_rule_expectation(points: int, keep_last: int, min_usable: int) -> dict[str, int]:
    """What `keepLast` and `minUsablePoints` together mean for one evaluation.

    The newest `max(keepLast, minUsablePoints)` points stay; everything beyond
    the keep rule is a candidate; and `protected` is only the OVERRIDE — the
    points `minUsablePoints` pulled back out of the keep rule's reach, which is
    what D3 L9 means by "`protected` lists the `minUsablePoints` overrides".
    """
    kept = max(keep_last, min_usable)
    return {"kept": kept, "candidates": points - kept, "protected": min_usable - keep_last}


def overlapping_keep_rules_ok(ev: dict[str, Any], ids: dict[str, set[str]],
                              want: dict[str, int], points: int) -> bool:
    candidates = ev.get("candidates") or []
    kept = ev.get("kept") or []
    protected = ev.get("protected") or []
    return (
        ev.get("pointsEvaluated") == points
        and len(candidates) == want["candidates"]
        and {c.get("reason") for c in candidates} == {"BeyondKeepLast"}
        and len(kept) == want["kept"]
        and len(protected) == want["protected"]
        and {p.get("reason") for p in protected} == {"MinUsablePoints"}
        and ids["protected"] <= ids["kept"]
        and not (ids["candidates"] & ids["kept"])
    )


def skipped_never_a_candidate(ev: dict[str, Any], ids: dict[str, set[str]]) -> bool:
    """A point the evaluation could not classify is named, explained, and safe.

    `SkippedEntry` is `{pointId | key, reason}`. Requiring the identity as well
    as the reason is what stops an unreadable point being dropped silently:
    a row with neither is an object nobody can go and look at.
    """
    skipped = ev.get("skipped") or []
    return (
        bool(skipped)
        and all(row.get("pointId") or row.get("key") for row in skipped)
        and {row.get("reason") for row in skipped} <= SKIPPED_REASONS
        and not (ids["skipped"] & ids["candidates"])
    )


DIGEST_HEX = re.compile(r"(sha256:)?([0-9a-f]{64})")


def digest_prefix_signature(message: str) -> bool:
    """The fingerprint RET-DIGEST-PREFIX left on every `ViewUnreadable` message.

    `weirkeeper::catalog_view::page_digest` returned bare hex while
    `status.pages[].sha256` is published `sha256:`-prefixed, and the two were
    compared with `!=`. So the controller printed one digest TWICE — once bare,
    once prefixed — and called the two different. That is the whole fingerprint,
    and it is narrower than "two equal digests": a message printing
    `expected sha256:X got sha256:X` is a genuine equality bug in something
    else, and reporting it as this defect would be the mis-attribution this
    function exists to end (review L-1). One body, both spellings, or it is not
    this.
    """
    spellings: dict[str, set[bool]] = {}
    for prefix, body in DIGEST_HEX.findall(message or ""):
        spellings.setdefault(body, set()).add(bool(prefix))
    return any(seen == {True, False} for seen in spellings.values())


def enforcer_is_in_the_image(state: dict[str, Any]) -> bool:
    """What a PRESENT `logweir-retention` looks like from a probe Job.

    A container that started and exited with the enforcer's own refusal. An
    ABSENT one is 127, or a kubelet `waiting` state with no exit code at all,
    and the `not found` text the pre-fix row required is required to be gone.
    """
    terminated = state.get("terminated") or {}
    waiting = state.get("waiting") or {}
    message = json.dumps(state).lower()
    return (
        bool(terminated)
        and not waiting
        and "not found" not in message
        and "no such file" not in message
        and terminated.get("exitCode") == RETENTION_EXIT_REFUSED
    )


def hatch_is_open(value: Any) -> bool:
    """`LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS` as the controller reads it."""
    return str(value).strip().lower() in {"1", "true", "yes"}


def expected_posts(hatch_open: bool, new_transitions: int) -> int:
    """How many POSTs this window should have produced.

    ONE PER NEWLY NOTIFIED TRANSITION, and none at all while the escape hatch is
    shut. Not "one, always": `renotifyAfterSeconds` is unset, so one transition
    is one alert however many times the policy is reconciled, and re-running
    this phase against a policy whose alert is already open must see ZERO — the
    same rule `notify-stale-point-alerts-exactly-once` asserts from the other
    side. A flat expectation of 1 made the row pass only on a fresh namespace
    and call a correct re-run a failure (observed 2026-09-18).
    """
    return new_transitions if hatch_open else 0


def selector_matched_a_run(status: dict[str, Any]) -> dict[str, bool]:
    """Whether `spec.protects` selected ANY run at all.

    THE CLAUSE THAT SEPARATES A VERDICT FROM A VACUUM. `Unprotected` /
    `NoAvailablePoint` is what a policy says when the points it covers are all
    unusable AND what it says when it covers no points at all, and the two look
    identical in `status.health`. `status.lastAttempt` is built from
    `input.slots.first()` and `slots` is the MEMBER list
    (`controllers/protection_policy.rs:380`), so a policy naming a run has
    selected at least one.

    Measured live on 2026-09-22 before this was a clause: every protection row
    in this file named `scheduleRefs` while its points were manual `Backup`s
    with no `spec.scheduleRef`, and `lastAttempt` read `null` — the rows were
    green (or red) over an empty candidate set
    (`verdicts/probe-schedulerefs-membership.json`).
    """
    named = ((status.get("lastAttempt") or {}).get("backupRef") or {}).get("name")
    return {
        "the policy's selector matched at least one run — status.lastAttempt names it":
            bool(named),
    }


def notify_delivery_ok(hatch_open: bool, posts: int, want_posts: int, delivered: bool,
                       unchanged: int, total: int) -> bool:
    """What the controller's own environment implies, and the invariant under it.

    The POST count is a consequence of the escape hatch and of how many
    transitions this window opened; the Backups' resourceVersions are the
    contract, whichever way delivery went.
    """
    return posts == want_posts and delivered == hatch_open and unchanged == total


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
            # SUSPENDED AT BIRTH. Every caller patches it suspended one call
            # later, and `* * * * *` means the gap between the two is a whole
            # slot: a stray scheduled point in dest-a shifts the "newest point"
            # the protection rows measure against.
            "suspend": True,
        },
    }


def retention() -> None:
    evidence: list[str] = []
    names = ["b-1", "b-2", "b-3", "b-4", "b-5", "b-6"]
    # IDEMPOTENT BY THE DESTINATION'S POINTS, NOT BY THE BACKUP NAMES.
    #
    # The re-seed used to skip whenever all six `b-*` Backups still existed and
    # had Succeeded — which says nothing about what is AT the destination. Every
    # other phase writes there: `lifecycle` runs a Backup to dest-b to show an
    # unreadable-lifecycle policy blocks nothing, and the enforcement rows
    # delete points out of it. So a second `retention` skipped its seeding and
    # measured a destination holding seven points with the arithmetic row still
    # asserting six: PASS clean, FAIL on the re-run, for a reason that is the
    # harness's and looks like the product's (lab-refresh-5 §8.3/§8.4).
    #
    # What the keep-rule arithmetic needs is a destination holding EXACTLY these
    # six points, so that is what is reconciled and then asserted. The manifest
    # count is the destination's own answer — the catalog view is built from the
    # archive, so wiping the prefix retires every stray point whatever CR wrote
    # it — and re-seeding is driven by that count rather than by the CR names.
    def points_at_destination() -> list[str]:
        # The whole bucket and then a filter: `objects()` re-roots a prefixed
        # listing by concatenation, and this is the spelling every other
        # enforcement row already uses.
        return sorted(o["key"] for o in objects(BUCKET_B)
                      if o["key"].startswith(f"{DEST_PREFIX}/")
                      and o["key"].endswith("/manifest.json"))

    have = [n for n in names
            if (get_opt("backup", n) or {}).get("status", {}).get("phase") == "Succeeded"]
    before_seed = points_at_destination()
    stale = len(before_seed) != len(names)
    if len(have) != len(names) or stale or os.environ.get("LOGWEIR_D3_FRESH_B"):
        mc("rm", "--recursive", "--force", f"local/{BUCKET_B}/", check_rc=False)
        for name in names:
            if get_opt("backup", name) is not None:
                run(KN + ["delete", "backup", name, "--wait=true"])
            run_backup(name, "dest-b")
            time.sleep(1)
    after_seed = points_at_destination()
    evidence.append(artifact("retention/seed.json",
                             {"names": names, "succeededBackups": have,
                              "pointsBeforeSeed": before_seed, "pointsAfterSeed": after_seed,
                              "reseeded": len(have) != len(names) or stale}))
    check(
        "retention-seed-is-idempotent",
        "PLAT-16.1",
        len(after_seed) == len(names),
        f"this phase leaves dest-b holding exactly the {len(names)} points the keep-rule "
        f"arithmetic is written against: {len(before_seed)} before and {len(after_seed)} "
        f"after (re-seeded: {len(have) != len(names) or stale}). A phase whose second run "
        f"measures a different destination from its first cannot tell a harness leak from a "
        f"product change",
        evidence,
    )
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
    # THE DEFECT ROW, RE-POINTED AND NO LONGER A CATCH-ALL. It recorded a FAIL
    # naming RET-DIGEST-PREFIX for ANY policy landing
    # `Evaluated=False/ViewUnreadable`, which was right while that defect made
    # every view unreadable and wrong the moment it was fixed: a re-run of this
    # phase recreates the catalog, a policy reconciled inside that window lands
    # `ViewUnreadable` naming the ABSENT catalog, and the row reported the fixed
    # defect as back (observed 2026-09-18). The defect has a fingerprint no
    # other cause produces — two digests printed side by side that are equal
    # once the `sha256:` prefix is off — so that is what is looked for, and any
    # other unreadable view is recorded as the different thing it is.
    unreadable = [
        (name, condition(pol, "Evaluated"))
        for name, pol in [("keep-a", pol_a), ("keep-b", pol_b)]
        if condition(pol, "Evaluated").get("reason") == "ViewUnreadable"
    ]
    prefix_defect = [(n, c) for n, c in unreadable
                     if digest_prefix_signature(c.get("message", ""))]
    if unreadable:
        pages = {
            name: [
                {"configMapName": pg["configMapName"], "publishedSha256": pg["sha256"]}
                for pg in ((get_opt("recoverycatalog", cat) or {}).get("status", {}).get("pages")
                           or [])
            ]
            for name, cat in [("keep-a", "primary"), ("keep-b", "secondary")]
        }
        evidence.append(artifact(
            "retention/view-unreadable.json",
            {"conditions": {n: c for n, c in unreadable}, "catalogPages": pages,
             "carriesTheDigestPrefixSignature": [n for n, _ in prefix_defect]},
        ))
    check(
        "retention-view-digest-is-compared-without-its-prefix",
        "PLAT-16.1",
        not prefix_defect,
        (
            "`weirkeeper::catalog_view::page_digest` returns bare hex "
            "(`logweir_core::ids::sha256_hex`) and `status.pages[].sha256` is published "
            "`sha256:`-prefixed; RET-DIGEST-PREFIX was `retention_policy.rs` comparing the "
            "two with `!=`, which made EVERY view unreadable and printed the two digests "
            "side by side as equal apart from the prefix. "
            + (
                "THAT IS BACK: " + prefix_defect[0][1].get("message", "")
                if prefix_defect
                else "No policy's view is unreadable for that reason."
            )
            + (
                f" (Other unreadable views, which are NOT this defect: "
                f"{[(n, c.get('message', '')[:120]) for n, c in unreadable]})"
                if unreadable and not prefix_defect else ""
            )
        ),
        evidence,
    )

    a_ids = evaluation_point_ids(pol_a)
    b_ids = evaluation_point_ids(pol_b)
    a_universe = {e["pointId"] for e in (STATE.get("viewEntriesAfterCrLoss") or [])}
    b_universe = {e["pointId"] for e in b_entries}
    a_seen = set().union(*a_ids.values()) if a_ids else set()
    b_seen = set().union(*b_ids.values()) if b_ids else set()
    # BOTH SIDES MUST NAME SOMETHING. Without the two emptiness clauses this is
    # a set-disjointness assertion that an evaluation naming nothing at all
    # satisfies, which is exactly how it passed while reading a key no bucket
    # carries. dest-a is NOT required to be a subset of its recorded universe:
    # the `keeps-running` schedule keeps adding points to it after the view was
    # captured. dest-b is closed, so it is.
    check(
        "retention-two-destinations",
        "PLAT-16.1",
        reports_are_disjoint(ev_a, ev_b, a_seen, b_seen, a_universe, b_universe,
                             len(b_entries)),
        f"keep-a (dest-a) names {len(a_seen)} pointIds and keep-b (dest-b) names "
        f"{len(b_seen)}; dest-a's recorded view holds {len(a_universe)} and dest-b "
        f"{len(b_universe)}. keep-b pointsEvaluated={ev_b.get('pointsEvaluated')}. "
        + ("NO EVALUATION EXISTS: see `retention-view-digest-prefix-defect`."
           if not (ev_a and ev_b)
           else "Neither report is empty and no id crosses between them."),
        evidence,
    )

    # WHAT `protected` COUNTS. D3 L9 asks for "exactly 3 candidates and
    # `protected` lists the `minUsablePoints` OVERRIDES" — the points a
    # guarantee pulled back out of the rules' reach, not the whole retained set.
    # With `keepLast: 2` and `minUsablePoints: 3` over 6 points, 4 are beyond the
    # keep rule and exactly one of them is pulled back to make the third usable
    # point: `candidates` 3, `kept` 3, `protected` 1. This row demanded
    # `protected == 3`, the other reading, and failed on the first evaluation the
    # controller was ever able to produce (lab-refresh-3 §8.2). The arithmetic is
    # written out rather than hard-coded so the row says why 3 and 1.
    keep_last, min_usable = 2, 3
    b_points = len(b_entries)
    want = keep_rule_expectation(b_points, keep_last, min_usable)
    b_candidates = ev_b.get("candidates") or []
    b_kept = ev_b.get("kept") or []
    b_protected = ev_b.get("protected") or []
    check(
        "retention-overlapping-keep-rules",
        "PLAT-16.2",
        overlapping_keep_rules_ok(ev_b, b_ids, want, b_points),
        f"{b_points} points, keepLast={keep_last}, minUsablePoints={min_usable}: "
        f"{len(b_candidates)} candidates (expected {want['candidates']}, reasons "
        f"{sorted({c.get('reason') for c in b_candidates})}), {len(b_kept)} kept (expected "
        f"{want['kept']}) and {len(b_protected)} protected (expected {want['protected']}, reasons "
        f"{sorted({p.get('reason') for p in b_protected})}). `protected` is the subset of "
        f"`kept` a guarantee saved beyond the keep rule, not the retained set; every "
        f"protected id is kept ({b_ids['protected'] <= b_ids['kept']}) and no candidate is "
        f"({not (b_ids['candidates'] & b_ids['kept'])})"
        + ("" if ev_b else " — no evaluation was produced at all"),
        evidence,
    )

    # `SkippedEntry` is `{pointId | key, reason}` — `reason`, not `state`, and
    # `pointId`, not `backupId`. Reading two absent keys made both sides `{None}`
    # and the intersection non-empty, so this failed on exactly the evidence that
    # proves it (lab-refresh-3 §8.2). It now also requires every skipped row to
    # identify what it skipped and to give a reason from the published
    # vocabulary: a row that could not be classified AND could not be named
    # would be an unreadable point silently dropped.
    skipped = ev_a.get("skipped") or []
    skipped_reasons = {row.get("reason") for row in skipped}
    check(
        "retention-unreadable-point-never-a-candidate",
        "PLAT-16.1",
        skipped_never_a_candidate(ev_a, a_ids),
        f"dest-a's degraded points ({len(skipped)} rows, reasons {sorted(skipped_reasons)}, "
        f"ids {sorted(a_ids['skipped'])}) are skipped and none is a candidate "
        f"({sorted(a_ids['candidates'])}); {len(a_ids['skipped'] - set(ev_a.get('kept') or []))}"
        f" of them is outside `kept` too"
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

    # AN EXPLICIT WINDOW, NOT A TIMEOUT'S SHADOW. This row used to count
    # whatever the `* * * * *` schedule had produced by the time the line above
    # returned. Before RET-DIGEST-PREFIX was fixed that call sat out its whole
    # 240 s — a policy stuck at `Evaluated=False/ViewUnreadable` never settles —
    # so the schedule had four minutes and the row saw 25 children. The fixed
    # controller evaluates in seconds, the borrowed window collapsed to about a
    # minute, the single child created had not finished yet, and the row failed
    # on its own clock rather than on anything a retention verdict did
    # (lab-refresh-3 §8.2). The window is now stated and waited for: three slots
    # of a one-minute schedule plus a run's worth of slack, ended early by the
    # first Succeeded child.
    scheduled_window_seconds = 240
    window_deadline = time.time() + scheduled_window_seconds
    children: list[dict[str, Any]] = []
    finished: list[dict[str, Any]] = []
    while True:
        children = [
            b for b in lst("backups")
            if b["metadata"]["name"].startswith("logweir-backup-keeps-running")
        ]
        finished = [b for b in children if b.get("status", {}).get("phase") == "Succeeded"]
        if finished or time.time() >= window_deadline:
            break
        time.sleep(5)
    evidence.append(
        artifact(
            "retention/scheduled-during-evaluation.json",
            {"since": started, "until": now(), "windowSeconds": scheduled_window_seconds,
             "children": [b["metadata"]["name"] for b in children],
             "succeeded": [b["metadata"]["name"] for b in finished],
             "phases": {b["metadata"]["name"]: b.get("status", {}).get("phase")
                        for b in children}},
        )
    )
    check(
        "retention-scheduled-backups-continue",
        "PLAT-16.1",
        len(finished) >= 1,
        f"a `* * * * *` BackupSchedule on dest-a ran through the evaluation and an explicit "
        f"{scheduled_window_seconds}s window after it: {len(children)} children, "
        f"{len(finished)} Succeeded — a retention verdict, including a refused one, blocks "
        f"no backup",
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

# THE IMAGE THAT CARRIES THE ENFORCER.
#
# `retention_policy.rs` renders every enforcement Job's command as
# `logweir-retention` out of the controller's `LOGWEIR_RUNNER_IMAGE`. RET-NOIMAGE
# was that no image this repository built contained that binary, so the
# enforcement rows needed one built by hand from the same commit
# (`Dockerfile.retention` beside this file) and a default here would have been a
# lie — it would have made the phases look runnable and then failed with
# `ErrImageNeverPull` instead of saying why.
#
# THAT DEFECT IS CLOSED. `Dockerfile` builds `logweir-retention` beside
# `logweir`, and `scripts/check-image.sh` check 7 refuses a runner image that
# does not carry it (lab-refresh-3 §2.3 ran the gate on the published image, and
# §8.3 ran the binary out of it). So the default is now the runner image the
# controller ITSELF names — the image every enforcement Job would really use,
# read off the live Deployment rather than assumed. `--retention-image <ref>` or
# LOGWEIR_D3_RETENTION_IMAGE still override it, which is how a build the lab is
# not running gets measured. Only when neither can be resolved does a scenario
# record NOT-RUN, which is still an honest verdict and not a skip.
RETENTION_IMAGE_ENV = "LOGWEIR_D3_RETENTION_IMAGE"
RETENTION_IMAGE: str | None = os.environ.get(RETENTION_IMAGE_ENV)

# `logweir_retention::EXIT_REFUSED` (crates/logweir-retention/src/lib.rs). The
# enforcer's own refusal: it parsed its arguments, found no usable plan, and
# said so. A binary that is not there cannot produce it — that is 127, or a
# kubelet `CreateContainerError` before any exit code exists at all.
RETENTION_EXIT_REFUSED = 3

RET_NOIMAGE = (
    "NOT RUN: no enforcer image could be resolved. `retention_policy.rs` names "
    "`logweir-retention` as every enforcement Job's command, out of the controller's "
    "`LOGWEIR_RUNNER_IMAGE`; this run could not read that value off the shared "
    "Deployment and no override was given. Pass `--retention-image <ref>` or "
    f"{RETENTION_IMAGE_ENV}=<ref>."
)

_RESOLVED_RETENTION_IMAGE: list[str] = []


def retention_image(scenario: str, task: str = "PLAT-16.2") -> str | None:
    """The image every enforcement row runs, or a recorded NOT-RUN and `None`."""
    if RETENTION_IMAGE:
        return RETENTION_IMAGE
    if not _RESOLVED_RETENTION_IMAGE:
        shipped = controller_facts().get("runnerImage")
        if shipped:
            _RESOLVED_RETENTION_IMAGE.append(shipped)
    if _RESOLVED_RETENTION_IMAGE:
        return _RESOLVED_RETENTION_IMAGE[0]
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
    generation: int | None = None,
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
        # THE LIVE GENERATION, not a constant. It goes into the record
        # (`lib.rs`: "the generation, for the record"), and a record that named
        # generation 1 for a plan rendered at generation 3 — which is what a
        # `spec.holds[]` patch produces — would misattribute the run.
        {"name": "LOGWEIR_RETENTION_POLICY_GENERATION",
         "value": str(generation if generation is not None
                      else get("retentionpolicy", "keep-b")["metadata"]["generation"])},
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
    """The enforcer binary IS in the image the retention controller names out of
    `LOGWEIR_RUNNER_IMAGE`. This is the live proof, not an argument from the
    Dockerfile — the same probe that used to prove the opposite."""
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
    terminated = state.get("terminated") or {}
    # THE ROW IS INVERTED, AND THE INVERSION IS THE PROOF. It used to require
    # the container state to say `not found` — the shape of RET-NOIMAGE — and it
    # now requires the opposite, because the defect is closed: `Dockerfile`
    # builds `logweir-retention` and `scripts/check-image.sh` check 7 refuses a
    # runner image without it. What a present binary looks like from here is a
    # container that STARTED and then exited with the enforcer's own refusal
    # (`EXIT_REFUSED`, no plan and no credentials were given to this probe). What
    # an absent one looks like is 127, or a kubelet `CreateContainerError` with
    # no exit code at all — and the old assertion is kept as the thing that must
    # now be false, so a regression to a runner built `-p logweir` fails here
    # first.
    check(
        "retention-enforcer-ships-in-the-runner-image",
        "PLAT-16.2",
        enforcer_is_in_the_image(state),
        f"a Job running `logweir-retention` out of the image the shared controller names "
        f"({shipped}) starts and runs: {message[:260]}. Exit "
        f"{terminated.get('exitCode')!r} is the enforcer's own refusal "
        f"(logweir_retention::EXIT_REFUSED = {RETENTION_EXIT_REFUSED}; this probe gives it "
        f"no plan), which only a binary that exists and resolved by bare name through $PATH "
        f"can produce — an absent one is 127 or a CreateContainerError with no exit code. "
        f"Formerly `retention-enforcer-ships-in-no-image`, which asserted `not found` here "
        f"and is the defect RET-NOIMAGE closed. The enforcement rows in this run used "
        f"{RETENTION_IMAGE or shipped}, run as the operator Job docs/kubernetes.md §7f "
        f"prescribes",
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
              f"mc admin policy create adm {RO_POLICY} /tmp/ro.json >/dev/null 2>&1; "
              f"mc admin user add adm {RO_USER} {reader_secret} >/dev/null 2>&1; "
              f"mc admin policy attach adm {RO_POLICY} --user {RO_USER} >/dev/null 2>&1; echo done"],
        check=False, timeout=120)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned(RO_SECRET),
            "stringData": {"access-key-id": RO_USER,
                           "secret-access-key": reader_secret},
        }
    )
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        "d3w14-denied", "d3w14-plan-readonly", dry_run=False, digest=digest, image=image,
        delete_secret=RO_SECRET,
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
              f"mc admin user remove adm {RO_USER} >/dev/null 2>&1; "
              f"mc admin policy rm adm {RO_POLICY} >/dev/null 2>&1; echo done"],
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


# ---------------------------------------------------------------------------
# PLAT-16.2's two controller-side guards: `spec.holds[]` and an in-flight Restore
# ---------------------------------------------------------------------------


def patch_policy(name: str, spec_patch: dict[str, Any]) -> dict[str, Any]:
    """Merge-patch a `RetentionPolicy`'s spec and return the patched object.

    THIS FUNCTION NEVER EXISTED. `legal_hold` called it twice and
    `git log -S "def patch_policy"` finds no commit that ever defined it, so
    the phase raised `NameError` on its first live line and PLAT-16.2's legal
    hold / lock test has never been exercised by this harness (lab-refresh-4
    §9.3). A merge patch is what `spec.holds[]` wants: the field is mutable
    because, as the CRD says, "a legal hold arrives on a Tuesday".
    """
    return json.loads(
        run(
            KN + ["patch", "retentionpolicy", name, "--type=merge",
                  "-p", json.dumps({"spec": spec_patch}), "-o", "json"]
        ).stdout
    )


def awaiting_approval_restore(name: str, backup_id: str, dest: str = "dest-b") -> dict[str, Any]:
    """A `Restore` that is nonterminal, creates nothing, and reads nothing.

    D3 §6.4 step 4 protects every point whose SET a nonterminal `Restore`
    names, and `retention_policy.rs::active_restore_sets` skips only restores
    whose phase is `Succeeded`, `Failed` or `Refused`. So the fixture has to be
    a restore that is genuinely in flight and genuinely harmless.

    `spec.approvalRef` naming an `Approval` that does not exist is exactly
    that: `restore.rs::admit` step 2 answers `ApprovalNotVerified`, which
    `is_terminal()` returns **false** for — the object holds at
    `phase: Pending` with one `Admitted=False` condition, is requeued every
    `ADMISSION_REQUEUE_SECS`, and **no Job is created until the approval is
    verified**. An EMPTY `approvalRef` would be `ApprovalNotReceived` and
    terminal, which is why the name is present and simply unfulfilled.

    Nothing here is a shortcut around authorisation: the restore never runs.
    It exists so the retention controller can see a set that is being read.
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": owned(name),
        "spec": {
            "approvalRef": {"name": f"{name}-approval-never-minted"},
            "sourceDestinationRef": {"name": dest},
            "evidenceDestinationRef": {"name": dest},
            "sourceArchive": {"url": f"logweir-destination://{dest}"},
            "backupSetRef": backup_id,
            "planBytes": json.dumps(
                {"note": "a plan nobody approved; this Restore never runs",
                 "backupSetRef": backup_id},
                sort_keys=True,
            ),
            "pointInTime": now(),
            "deadlineSeconds": 600,
            "target": {
                "clusterRef": {"name": "source"},
                "mode": "scratch",
                "topicNaming": {"prefix": f"{OWNER}-hold-drill-"},
            },
        },
    }


def protection_verdict(ev: dict[str, Any], point_id: str) -> dict[str, Any]:
    """Where one point landed in an evaluation, as the three buckets say it."""
    protected = {p.get("pointId"): p for p in (ev.get("protected") or [])}
    return {
        "pointId": point_id,
        "isCandidate": point_id in {c.get("pointId") for c in (ev.get("candidates") or [])},
        "isKept": point_id in set(ev.get("kept") or []),
        "protectReason": protected.get(point_id, {}).get("reason"),
    }


def point_is_protected(verdict: dict[str, Any], reason: str) -> bool:
    """A guard held this point: named, kept, reasoned, and never a candidate.

    All four, and the first is what makes the other three worth reading — a
    point the evaluation does not mention at all is not protected, it is
    absent. `Kept` without a reason is a retention decision nobody can audit,
    and a point that is BOTH kept and a candidate is a plan that contradicts
    itself.
    """
    return (
        verdict.get("protectReason") == reason
        and verdict.get("isKept") is True
        and verdict.get("isCandidate") is False
    )


def plan_omits(plan: dict[str, Any], point_ids: set[str]) -> bool:
    """No protected point reached the plan the enforcer is given."""
    return not ({line.get("point_id") for line in (plan.get("lines") or [])} & point_ids)


def survived_enforcement(before: list[dict[str, Any]], after: list[dict[str, Any]],
                         prefixes: set[str]) -> bool:
    """Every object under a protected point's set prefix is still there."""
    keys_before = {o["key"] for o in before if any(o["key"].startswith(p) for p in prefixes)}
    keys_after = {o["key"] for o in after}
    return bool(keys_before) and keys_before <= keys_after


def plan_from_evaluation(ev: dict[str, Any], policy_name: str, dest: str,
                         by_point: dict[str, dict[str, Any]], cm_name: str,
                         *, keep_last: int, min_usable: int) -> tuple[str, dict[str, Any]]:
    """A plan whose lines are exactly the CONTROLLER's own candidate list.

    NOT the controller's own plan DOCUMENT, and the reason is worth recording.
    It USED TO BE that `mode: Report` published `planSha256` and **no
    `planRef`** at all: the plan `ConfigMap` was rendered only on the
    enforcement path, behind a lease that refuses outright while a nonterminal
    `Restore` reads the destination (`StartOutcome::ActiveRestore`), which is
    the very fixture this phase installs.

    THAT CHANGED ON `claude/status-sweep` (merged to main as `561a21f`): the
    controller now writes `planRef` on EVERY evaluation, Report mode included,
    so `status.lastEvaluation.planRef` becomes a name where this harness
    recorded `null` and `plan_document()` becomes reachable for a Report-mode
    policy. **No row's predicate reads `planRef`**, so nothing here passes or
    fails differently; what changes is the recorded evidence
    (`controllerPlanRef`) and the sentences that explained the absence. On a
    lab image that predates that merge the field is still `null`, and this
    phase's evidence will say so until the batch refresh.

    Either way this builds the plan from the VERDICT and not from the
    document, which is the point below.

    What IS retrievable is the verdict, and the verdict is what a guard test
    needs: the lines are the controller's candidates, one for one, and nothing
    else. A protected point cannot reach the enforcer because the controller
    did not call it a candidate — which is exactly the claim.
    """
    pol = get("retentionpolicy", policy_name)
    lines = []
    for candidate in ev.get("candidates") or []:
        entry = by_point[candidate["pointId"]]
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
    plan = {
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
    digest, _ = write_plan(cm_name, plan)
    return digest, plan


def legal_hold() -> None:
    """PLAT-16.2's two controller-side guards, and the enforced pass that
    proves they hold.

    `spec.holds[]` names a point that may not be removed whatever the rules
    say, and a nonterminal `Restore` names a SET that is being read. Both must
    leave the candidate list, both must be reported kept with their reason, and
    an enforced pass over the controller's own plan must leave every one of
    their objects where it is.
    """
    evidence: list[str] = []
    entries = STATE.get("bEntries") or []
    if not entries:
        raise RuntimeError("run the `retention` phase first")
    by_point = {e["pointId"]: e for e in entries}

    pol = wait_evaluated("keep-b")
    candidates = ((pol.get("status", {}) or {}).get("lastEvaluation") or {}).get(
        "candidates"
    ) or []
    if len(candidates) < 3:
        record(
            "retention-legal-hold",
            "PLAT-16.2",
            "NOT-RUN",
            "`spec.holds[]` and `ActiveRestore` are applied by the controller's EVALUATION, "
            f"and this run has {len(candidates)} candidate(s) — fewer than the three the "
            "fixture needs to hold one, restore one and still leave a plan with something "
            "in it. Evaluated="
            f"{condition(pol, 'Evaluated').get('status')}/"
            f"{condition(pol, 'Evaluated').get('reason')}. Neither guard can be observed "
            "either way, so neither is reported as working and neither as broken.",
            [artifact("enforce/legal-hold-not-run.json", pol)],
        )
        record("retention-active-restore-protection", "PLAT-16.2", "NOT-RUN",
               "see `retention-legal-hold`: the same evaluation is the input.", [])
        record("retention-protected-points-survive-enforcement", "PLAT-16.2", "NOT-RUN",
               "see `retention-legal-hold`: the same evaluation is the input.", [])
        return

    held = candidates[0]["pointId"]
    restored = candidates[-1]["pointId"]
    restored_set = by_point[restored]["backupId"]
    evidence.append(artifact("enforce/guards-input.json", {
        "candidatesBefore": [c.get("pointId") for c in candidates],
        "heldPoint": held, "activeRestorePoint": restored,
        "activeRestoreBackupSet": restored_set,
    }))

    # --- the in-flight Restore, created BEFORE the hold so one re-evaluation
    #     settles both and the two guards are read from one object ----------
    restore_name = f"{OWNER}-hold-drill"
    if get_opt("restore", restore_name) is not None:
        run(KN + ["delete", "restore", restore_name, "--wait=true"])
    created = apply(awaiting_approval_restore(restore_name, restored_set))
    in_flight = wait_for(
        "restore", restore_name,
        lambda o: (o.get("status", {}) or {}).get("phase") is not None,
        seconds=180, what="a phase on the awaiting-approval Restore",
    )
    restore_phase = in_flight["status"].get("phase")
    evidence.append(artifact("enforce/active-restore-object.json", in_flight))
    check(
        "retention-active-restore-is-really-in-flight",
        "PLAT-16.2",
        restore_phase not in {"Succeeded", "Failed", "Refused"}
        and not [
            j for j in lst("jobs")
            if any(o.get("uid") == created["metadata"]["uid"]
                   for o in (j["metadata"].get("ownerReferences") or []))
        ]
        and (in_flight["status"].get("jobRef") is None)
        and (condition(in_flight, "Admitted").get("status") == "False"),
        f"the fixture Restore {restore_name} (uid {created['metadata']['uid']}) is "
        f"phase={restore_phase!r} with Admitted="
        f"{condition(in_flight, 'Admitted').get('status')}/"
        f"{condition(in_flight, 'Admitted').get('reason')} — nonterminal, so the retention "
        f"controller must see it, and no Job exists because the approval it names was never "
        f"minted. A row that protected a point with a TERMINAL restore would be proving "
        f"nothing",
        evidence,
    )

    generation = get("retentionpolicy", "keep-b")["metadata"]["generation"] + 1
    patch_policy(
        "keep-b",
        {"holds": [{"pointId": held, "reason": "d3w14 live acceptance: a legal hold"}]},
    )
    after = wait_evaluated("keep-b", generation=generation, seconds=300)
    ev = after["status"]["lastEvaluation"]
    evidence.append(artifact("enforce/legal-hold.json", after))
    held_verdict = protection_verdict(ev, held)
    restored_verdict = protection_verdict(ev, restored)
    evidence.append(artifact("enforce/guard-verdicts.json",
                             {"hold": held_verdict, "activeRestore": restored_verdict,
                              "candidatesAfter": [c.get("pointId")
                                                  for c in (ev.get("candidates") or [])],
                              "protected": ev.get("protected"), "kept": ev.get("kept")}))
    guarantees = after["status"].get("guarantees") or {}
    check(
        "retention-legal-hold",
        "PLAT-16.2",
        point_is_protected(held_verdict, "Hold"),
        f"the held point {held} left the candidate list and is reported kept and protected "
        f"with reason {held_verdict['protectReason']!r} (`spec.holds[]` is `Hold`; "
        f"`LegalHold` is the code a PROVIDER refusal records). Verdict {held_verdict}; "
        f"guarantees.legalHold is {guarantees.get('legalHold')} — never `LogweirEnforced`, "
        f"because object_store 0.14 exposes no WORM readback",
        evidence,
    )
    check(
        "retention-active-restore-protection",
        "PLAT-16.2",
        point_is_protected(restored_verdict, "ActiveRestore"),
        f"the point {restored}, whose set {restored_set} a nonterminal Restore names, left "
        f"the candidate list and is reported kept and protected with reason "
        f"{restored_verdict['protectReason']!r}. Verdict {restored_verdict}; "
        f"guarantees.activeRestoreProtection is "
        f"{guarantees.get('activeRestoreProtection')}",
        evidence,
    )

    # --- and the enforced pass, over the CONTROLLER's own plan -------------
    image = retention_image("retention-protected-points-survive-enforcement")
    if image is None:
        patch_policy("keep-b", {"holds": []})
        return
    digest, plan = plan_from_evaluation(ev, "keep-b", "dest-b", by_point,
                                       f"{OWNER}-guard-plan", keep_last=2, min_usable=3)
    protected_ids = {held, restored}
    protected_prefixes = {
        f"{DEST_PREFIX}/{by_point[pid]['backupId']}/" for pid in protected_ids
    }
    candidate_ids = {c.get("pointId") for c in (ev.get("candidates") or [])}
    evidence.append(artifact("enforce/guard-plan.json",
                             {"plan": plan, "sha256": digest,
                              "controllerCandidates": sorted(candidate_ids),
                              "controllerPlanSha256": ev.get("planSha256"),
                              "controllerPlanRef": ev.get("planRef"),
                              "protectedPrefixes": sorted(protected_prefixes)}))
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        f"{OWNER}-guard-enforce", f"{OWNER}-guard-plan", dry_run=False, digest=digest,
        image=image, generation=get("retentionpolicy", "keep-b")["metadata"]["generation"],
    )
    after_objects = objects(BUCKET_B)
    evidence.append(artifact("enforce/guard-enforce-log.txt", logs))
    evidence.append(artifact("enforce/guard-objects-before.json", before))
    evidence.append(artifact("enforce/guard-objects-after.json", after_objects))
    record_line = key_lines(logs, "retention-record=")
    record_key = record_line[0]["retention-record"] if record_line else None
    doc: dict[str, Any] = {}
    if record_key:
        raw = cat(BUCKET_B, record_key)
        doc = json.loads(raw.decode())
        evidence.append(artifact("enforce/guard-enforcement-record.json", doc))
    attributed = {pt.get("point_id") or pt.get("pointId") for pt in (doc.get("points") or [])}
    gone = {o["key"] for o in before} - {o["key"] for o in after_objects}
    check(
        "retention-protected-points-survive-enforcement",
        "PLAT-16.2",
        {ln["point_id"] for ln in plan["lines"]} == candidate_ids
        and bool(candidate_ids)
        and plan_omits(plan, protected_ids)
        and code == 0
        and survived_enforcement(before, after_objects, protected_prefixes)
        and not (attributed & protected_ids),
        f"a plan whose lines ARE the controller's candidate list ({sorted(candidate_ids)}; "
        f"digest {digest}) names neither protected point "
        f"({plan_omits(plan, protected_ids)}) — the controller published "
        f"planSha256={ev.get('planSha256')} and planRef={ev.get('planRef')!r} "
        f"(`null` on an image predating `561a21f`, where Report mode rendered no plan "
        f"ConfigMap; a name on one that carries it — no predicate here reads the field "
        f"either way); one enforced pass exited {code}, "
        f"removed {len(gone)} object(s), and every object under the held and "
        f"actively-restored sets {sorted(protected_prefixes)} is still present "
        f"({survived_enforcement(before, after_objects, protected_prefixes)}); the "
        f"enforcement record at {record_key} attributes {sorted(attributed)} and neither "
        f"protected point",
        evidence,
    )
    STATE["guards"] = {"held": held_verdict, "activeRestore": restored_verdict,
                       "planPointIds": [ln.get("point_id") for ln in (plan.get("lines") or [])],
                       "recordKey": record_key, "gone": sorted(gone), "exitCode": code}
    save()
    patch_policy("keep-b", {"holds": []})
    # AND THE FIXTURE RESTORE GOES. It is permanently nonterminal by design —
    # that is what makes it an in-flight restore — and
    # `retention_policy.rs::start_decision` refuses on the WHOLE destination
    # while one exists: "a nonterminal Restore reads this destination; no
    # retention Job is created". Leaving it alive wedged every later phase's
    # enforcement silently, and cost the bounded-retry row a 1800 s window that
    # recorded zero Jobs (review H-1). The row cleans up what it planted.
    run(KN + ["delete", "restore", restore_name, "--wait=true"], check=False)
    remaining_restores = [r["metadata"]["name"] for r in lst("restores")]
    evidence.append(artifact("enforce/guard-restore-cleanup.json",
                             {"deleted": restore_name, "restoresLeft": remaining_restores}))
    check(
        "retention-active-restore-fixture-is-cleaned-up",
        "PLAT-16.2",
        not remaining_restores,
        f"the in-flight Restore this row planted is deleted before the phase returns "
        f"({restore_name}); restores left in the namespace: {remaining_restores}. A "
        f"permanently nonterminal Restore blocks every later enforcement run at this "
        f"destination, so leaving one behind makes the next phase measure the leak instead "
        f"of the product",
        evidence,
    )


# ---------------------------------------------------------------------------
# PLAT-16.1's two remaining tests: a credential that cannot read the bucket
# lifecycle, and an evaluation that cannot complete
# ---------------------------------------------------------------------------


def external_lifecycle_is_unknown(policy: dict[str, Any]) -> dict[str, bool]:
    """What a declared bucket rule may and may not claim, clause by clause.

    D3 §6.6 and `retention_policy.rs::declare_external` are explicit: **Logweir
    reads no lifecycle configuration** — `object_store` 0.14 exposes no such
    API — so `ageExpiry` and `legalHold` are `ProviderEnforcedUnverified`,
    which is the UNKNOWN answer, never `LogweirEnforced`; the three guarantees
    a bucket rule cannot express are `NotEnforced` outright; and the policy
    produces NO evaluation at all, because there is nothing to report about
    what a rule nobody read would remove.

    The clause that matters for "missing lifecycle permissions" is the last
    one: a policy that produced an EMPTY evaluation would be claiming to know
    that nothing will be deleted. `Evaluated=Unknown/NeverEvaluated` with no
    `lastEvaluation` block is the honest shape, and it is the same shape
    whether the credential could have read the rule or not — which is exactly
    what makes a missing permission harmless here.
    """
    status = policy.get("status") or {}
    guarantees = status.get("guarantees") or {}
    evaluated = condition(policy, "Evaluated")
    return {
        "ageExpiry is provider-claimed and UNVERIFIED, never LogweirEnforced":
            guarantees.get("ageExpiry") == "ProviderEnforcedUnverified",
        "legalHold is provider-claimed and unverified":
            guarantees.get("legalHold") == "ProviderEnforcedUnverified",
        "the three guarantees a bucket rule cannot express are NotEnforced":
            {guarantees.get(k) for k in
             ("minUsablePoints", "activeRestoreProtection", "sharedSegments")} == {"NotEnforced"},
        "Evaluated is Unknown/NeverEvaluated": (
            evaluated.get("status") == "Unknown"
            and evaluated.get("reason") == "NeverEvaluated"
        ),
        "no evaluation block claims to know what would be removed":
            status.get("lastEvaluation") is None,
        "Logweir deletes nothing under this policy":
            condition(policy, "Enforced").get("status") == "False",
    }


def evaluation_failure_is_distinguishable(policy: dict[str, Any]) -> dict[str, bool]:
    """An evaluation that cannot complete says so, and says nothing else.

    THE POINT OF THE ROW IS THE LAST TWO CLAUSES. A policy that could not read
    its view and answered `candidates: []` would be indistinguishable from one
    that read it and found nothing to delete — the same status, opposite
    meanings, and an operator acting on the wrong one deletes nothing while
    believing the archive is under control. So the failure must carry its own
    reason AND must not publish an evaluation at all.
    """
    status = policy.get("status") or {}
    evaluated = condition(policy, "Evaluated")
    ready = condition(policy, "Ready")
    return {
        "Evaluated is False with a named reason": (
            evaluated.get("status") == "False" and bool(evaluated.get("reason"))
        ),
        "the reason is not a success reason":
            evaluated.get("reason") not in {"EvaluationComplete", "NeverEvaluated"},
        "the message says what could not be read":
            bool((evaluated.get("message") or "").strip()),
        "Ready is False, so a console cannot show this policy as working":
            ready.get("status") == "False",
        "no candidate list at all — not an empty one":
            (status.get("lastEvaluation") or {}).get("candidates") is None,
        "and no count that would read as `nothing to delete`":
            (status.get("lastEvaluation") or {}).get("candidateCount") is None,
    }


def lifecycle() -> None:
    """PLAT-16.1: a destination whose credential cannot read the bucket
    lifecycle, and an evaluation that cannot complete."""
    evidence: list[str] = []
    # RE-RUNNABLE. Both halves of this phase put a policy on the same
    # destination, one after the other, because two at once is a third refusal
    # (`Ready=False/Conflict`) that neither row is about. A leftover from an
    # earlier attempt would make the first half read that refusal instead.
    for leftover in (f"{OWNER}-broken", f"{OWNER}-nolife"):
        if get_opt("retentionpolicy", leftover) is not None:
            run(KN + ["delete", "retentionpolicy", leftover, "--wait=true"], check=False)

    # --- a credential that genuinely cannot read a bucket lifecycle --------
    policy_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow",
                 "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                 "Resource": [f"arn:aws:s3:::{BUCKET_B}", f"arn:aws:s3:::{BUCKET_B}/*"]},
            ],
        }
    )
    nolife_secret = mint()
    run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
              f"printf '%s' '{policy_doc}' > /tmp/nolife.json && "
              f"mc admin policy create adm {OWNER}-nolife /tmp/nolife.json >/dev/null 2>&1; "
              f"mc admin user add adm {OWNER}nolife {nolife_secret} >/dev/null 2>&1; "
              f"mc admin policy attach adm {OWNER}-nolife --user {OWNER}nolife "
              f">/dev/null 2>&1; echo done"],
        check=False, timeout=120)
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned(f"{OWNER}-nolife-s3"),
           "stringData": {"access-key-id": f"{OWNER}nolife",
                          "secret-access-key": nolife_secret}})
    # PROVE THE DENIAL rather than assume it. `mc ilm rule list` is the bucket
    # lifecycle read, and this credential's policy grants no `s3:GetLifecycle*`.
    probe = run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
                      f"mc alias set nolife {MINIO_ENDPOINT} {OWNER}nolife {nolife_secret} "
                      f">/dev/null 2>&1; mc ilm rule list nolife/{BUCKET_B} 2>&1 | head -5"],
                check=False, timeout=120)
    denial = redact(probe.stdout.strip())
    evidence.append(artifact("lifecycle/credential-probe.txt", denial))
    check(
        "retention-lifecycle-read-is-really-denied",
        "PLAT-16.1",
        "denied" in denial.lower() or "not allowed" in denial.lower()
        or "no lifecycle" in denial.lower() or "unable" in denial.lower(),
        f"the destination's credential cannot read the bucket lifecycle: "
        f"`mc ilm rule list` answers {denial[:200]!r}. A row that asserted a missing "
        f"permission without demonstrating it would be asserting its own fixture",
        evidence,
    )

    dest = destination(f"{OWNER}-dest-nolife", BUCKET_B, write_secret=f"{OWNER}-nolife-s3")
    dest["spec"]["storage"]["prefix"] = f"{DEST_PREFIX}-nolife"
    apply(dest)
    wait_for("backupdestination", f"{OWNER}-dest-nolife",
             lambda o: condition(o, "Valid").get("status") is not None,
             seconds=180, what="a Valid verdict on the lifecycle destination")
    apply(
        retention_policy(
            f"{OWNER}-nolife", f"{OWNER}-dest-nolife", "secondary",
            mode="ExternalLifecycle",
            rules={"keepDays": 30, "minUsablePoints": 1},
            external={"provider": "s3", "prefix": f"{DEST_PREFIX}-nolife",
                      "ruleId": f"{OWNER}-nolife-rule", "expirationDays": 7},
        )
    )
    declared = wait_for(
        "retentionpolicy", f"{OWNER}-nolife",
        lambda o: bool(condition(o, "Evaluated").get("status")),
        seconds=240, what="a verdict on the lifecycle policy",
    )
    evidence.append(artifact("lifecycle/policy-nolife.json", declared))
    clauses = external_lifecycle_is_unknown(declared)
    evidence.append(artifact("lifecycle/unknown-clauses.json", clauses))
    check(
        "retention-missing-lifecycle-permission",
        "PLAT-16.1",
        all(clauses.values()),
        "with a credential that cannot read the bucket lifecycle the report says the rule "
        "is UNKNOWN and enforced by nobody Logweir can see: "
        + "; ".join(f"{k}={v}" for k, v in clauses.items())
        + f". guarantees={((declared.get('status') or {}).get('guarantees'))}; Evaluated="
        f"{condition(declared, 'Evaluated').get('status')}/"
        f"{condition(declared, 'Evaluated').get('reason')}. `declare_external` reads no "
        f"lifecycle configuration at all — object_store 0.14 exposes no such API — so a "
        f"missing permission changes nothing about what is claimed, which is the property "
        f"this row exists to fix in place",
        evidence,
    )

    # --- unrelated backups continue ---------------------------------------
    started = now()
    survivor = f"{OWNER}-lifecycle-survivor"
    if get_opt("backup", survivor) is not None:
        run(KN + ["delete", "backup", survivor, "--wait=true"])
    # NOT `run_backup`: it RAISES on a phase that is not Succeeded, and a row
    # that crashes where it should record a FAIL is a row that cannot fail.
    create(backup_object(survivor, "dest-b"))
    survived = wait_for("backup", survivor, terminal, seconds=600,
                        what="a terminal phase on the unrelated Backup")
    evidence.append(artifact("lifecycle/unrelated-backup.json", survived))
    check(
        "retention-lifecycle-policy-blocks-no-backup",
        "PLAT-16.1",
        survived.get("status", {}).get("phase") == "Succeeded",
        f"a Backup to dest-b started at {started}, while the unreadable-lifecycle policy "
        f"stood beside it, reached {survived.get('status', {}).get('phase')!r}: a policy "
        f"that can report nothing blocks nothing",
        evidence,
    )

    # --- an evaluation that cannot complete --------------------------------
    # THE LIFECYCLE POLICY GOES FIRST. Two policies over one destination land
    # `Ready=False/Conflict` and the controller writes NO `Evaluated` condition
    # at all — a different refusal, and one this row must not be reading by
    # accident. Removing it leaves the destination to a single policy whose
    # catalog does not exist, which is the failure under test.
    run(KN + ["delete", "retentionpolicy", f"{OWNER}-nolife", "--wait=true"], check=False)
    # AND THE SCOPE MUST NARROW ITS OWN DESTINATION, or the refusal is
    # `Ready=False/DestinationUnusable` — a THIRD not-ready state, beside
    # `Conflict`, that this row would otherwise read by accident. That three
    # distinct refusals are distinguishable at all is the property under test;
    # the one this row is about is the evaluation that could not complete.
    broken_policy = retention_policy(
        f"{OWNER}-broken", f"{OWNER}-dest-nolife", f"{OWNER}-catalog-that-does-not-exist",
        rules={"keepLast": 1, "minUsablePoints": 1},
    )
    broken_policy["spec"]["scope"] = {"prefix": f"{DEST_PREFIX}-nolife"}
    apply(broken_policy)
    broken = wait_for(
        "retentionpolicy", f"{OWNER}-broken",
        lambda o: condition(o, "Evaluated").get("status") == "False",
        seconds=240, what="an evaluation failure",
    )
    evidence.append(artifact("lifecycle/policy-broken.json", broken))
    failure = evaluation_failure_is_distinguishable(broken)
    evidence.append(artifact("lifecycle/evaluation-failure-clauses.json", failure))
    check(
        "retention-evaluation-failure-is-distinguishable",
        "PLAT-16.1",
        all(failure.values()),
        "an evaluation that cannot complete is its own state and never an empty "
        "`nothing to delete`: "
        + "; ".join(f"{k}={v}" for k, v in failure.items())
        + f". Evaluated={condition(broken, 'Evaluated').get('status')}/"
        f"{condition(broken, 'Evaluated').get('reason')} "
        f"({(condition(broken, 'Evaluated').get('message') or '')[:160]}); Ready="
        f"{condition(broken, 'Ready').get('status')}/"
        f"{condition(broken, 'Ready').get('reason')}; lastEvaluation="
        f"{(broken.get('status') or {}).get('lastEvaluation')}",
        evidence,
    )
    run(KN + ["delete", "retentionpolicy", f"{OWNER}-broken", "--wait=true"], check=False)


# ---------------------------------------------------------------------------
# PLAT-16.2's remaining tests: shared segment, partial failure, bounded retry,
# and the lock nobody can read
# ---------------------------------------------------------------------------


def shared_segment_contract(entries: list[dict[str, Any]], ev: dict[str, Any],
                            guarantees: dict[str, Any]) -> dict[str, bool]:
    """`sharedSegments` reports what is TRUE of the view it was computed from.

    `retention_plan::evaluate` step 4 protects a candidate whose segment keys
    appear in a RETAINED point's key set, with `SharedSegment`. It can only do
    that when the point CARRIES its segment keys, and `point_facts` cannot
    supply them: a catalog view entry has no segment field at all
    (`retention_policy.rs`, "the catalog view entry has no segment field …
    so on every view this build reads the honest answer is `NotEnforced`").

    So the guarantee is DERIVED from the points rather than written as a
    constant — the first landing wrote `LogweirEnforced` unconditionally, which
    is the withdrawn-guarantee defect class on a status field. This row asserts
    the derivation in both directions: no segment keys in the view means
    `NotEnforced` and no `SharedSegment` protection; segment keys in the view
    would mean `LogweirEnforced`. The day an entry carries them the row starts
    testing the other branch with no edit.
    """
    visible = any(e.get("segmentKeys") for e in entries)
    reasons = {p.get("reason") for p in (ev.get("protected") or [])}
    return {
        "the guarantee matches what the view can support": (
            guarantees.get("sharedSegments") == ("LogweirEnforced" if visible else "NotEnforced")
        ),
        "no point is SharedSegment-protected unless the view carries segment keys":
            visible or "SharedSegment" not in reasons,
        "the guarantee is never a bare claim": (
            guarantees.get("sharedSegments") in {"LogweirEnforced", "NotEnforced"}
        ),
    }


def partial_failure_is_attributable(points: list[dict[str, str]], exit_code: int | None,
                                    gone: set[str], remaining: set[str],
                                    allowed_prefix: str, denied_prefix: str,
                                    doc: dict[str, Any]) -> dict[str, bool]:
    """One deletion denied mid-run: what the pass must still be able to say.

    `logweir_retention`'s exit contract is explicit — **1** is "at least one
    point did not complete: `Orphaned` or `Kept`, with its closed code" — and
    `execute` walks every line of the plan and emits one `retention-point=` per
    point, so the run KEEPS GOING and the denial is one point's verdict rather
    than the run's.

    The record then has to attribute exactly what happened, in its own
    vocabulary: a total, a per-point count, the closed code on the point that
    did not complete, and `remaining_keys` — which `logweir_reaper` documents
    as "the keys that remain, if any — exactly what the next plan must name".
    A partial failure that over-claims is worse than one that fails, and a
    partial failure that cannot say what is left behind cannot be resumed.
    """
    reported = {p.get("retention-point"): p for p in points}
    deleted = {k for k, v in reported.items() if v.get("state") == "Deleted"}
    kept = {k for k, v in reported.items() if v.get("state") != "Deleted" and v.get("code")}
    by_point = {pt.get("point_id"): pt for pt in (doc.get("points") or [])}
    deleted_rows = [pt for pt in by_point.values() if pt.get("state") == "Deleted"]
    kept_rows = [pt for pt in by_point.values() if pt.get("state") != "Deleted"]
    leftovers = {k for pt in kept_rows for k in (pt.get("remaining_keys") or [])}
    return {
        "the run reported every point, not just the first": len(points) >= 2,
        "exactly one point completed and one did not": len(deleted) == 1 and len(kept) == 1,
        "the point that did not complete carries a closed code":
            bool(kept_rows) and all(pt.get("code") for pt in kept_rows),
        "exit 1 — work remains, and the run says so": exit_code == 1,
        "only the permitted set's keys are gone": (
            bool(gone) and all(k.startswith(allowed_prefix) for k in gone)
        ),
        "nothing under the denied set was removed":
            not any(k.startswith(denied_prefix) for k in gone),
        "the record's total is what actually went": doc.get("objects_deleted") == len(gone),
        "the completed point's own count is what actually went":
            len(deleted_rows) == 1 and deleted_rows[0].get("objects_deleted") == len(gone),
        "the denied point claims to have removed nothing":
            all(pt.get("objects_deleted") == 0 for pt in kept_rows),
        "and names the leftovers exactly, for the next plan": (
            bool(leftovers)
            and all(k.startswith(denied_prefix) for k in leftovers)
            and leftovers <= remaining
        ),
        "the record carries the run's own exit code": doc.get("exit_code") == exit_code,
    }


def bounded_retry_degrades(policy: dict[str, Any], failed_runs: int, counted: int,
                           jobs_while_degraded: int) -> dict[str, bool]:
    """D3 §6.5: after the retry budget, say so and stop until the spec changes.

    `DEGRADED_AFTER_FAILURES` is 3 and a run that stopped on its own ceiling
    (`BudgetExhausted`) deliberately does NOT count, so three ordinary bounded
    runs on a large archive cannot degrade a healthy policy. What must degrade
    it is three consecutive runs that genuinely failed.

    `failed_runs` IS COUNTED FROM THE POLICY'S OWN JOBS, not from
    `status.consecutiveRunFailures`, and the two are separate clauses on
    purpose. The first landing took both numbers from the status, so a build
    that fails to RECORD a failure looked to it like a build that had not run —
    and it reported NOT-RUN over a window in which five enforcement Jobs had
    failed (review **H-1**). A harness that reads the same broken field twice
    cannot see the break. The Jobs are the ground truth; the counter is a claim
    about them, and "the policy counted them" is the clause that fails when the
    claim is wrong.
    """
    degraded = condition(policy, "EnforcementDegraded")
    return {
        "the budget was actually spent — three runs genuinely failed": failed_runs >= 3,
        "the policy COUNTED the failures its own Jobs recorded": counted >= 3,
        "EnforcementDegraded is True with a reason": (
            degraded.get("status") == "True" and bool(degraded.get("reason"))
        ),
        "and it says why in words": bool((degraded.get("message") or "").strip()),
        "no further retention Job is created while degraded": jobs_while_degraded == 0,
    }


def enforce_guards() -> None:
    """PLAT-16.2: shared segment, partial failure, bounded retry, and lock."""
    evidence: list[str] = []
    entries = STATE.get("bEntries") or []
    if not entries:
        raise RuntimeError("run the `retention` phase first")
    by_point = {e["pointId"]: e for e in entries}

    # --- lock: recorded as unprovable, with the reason -----------------------
    record(
        "retention-legal-lock",
        "PLAT-16.2",
        "NOT-RUN",
        "UNPROVABLE ON THIS PROVIDER, and not for want of a fixture. The `lock` half of "
        "PLAT-16.2's `legal hold / lock` test asks that a provider object-lock be respected "
        "and shown to be in force. `object_store` 0.14 exposes NO WORM readback, which is "
        "why `retention_policy.rs` writes `legalHold: ProviderEnforcedUnverified` and never "
        "`LogweirEnforced`: \"a provider refusal is authoritative and recorded\", never "
        "\"Logweir knows the hold exists\" (D3 §16). A row asserting a lock is in force "
        "would be asserting something no code in this build can observe. The HOLD half is "
        "`retention-legal-hold`, which passes; the provider-lock half needs either an "
        "object_store release with a lock readback or a provider probe outside Logweir.",
        [],
    )

    # --- shared segment ------------------------------------------------------
    pol = wait_evaluated("keep-b")
    ev = (pol.get("status", {}) or {}).get("lastEvaluation") or {}
    guarantees = (pol.get("status", {}) or {}).get("guarantees") or {}
    segment_fields = sorted({k for e in entries for k in e})
    clauses = shared_segment_contract(entries, ev, guarantees)
    evidence.append(artifact("enforce/shared-segment.json", {
        "viewEntryFields": segment_fields,
        "entriesCarryingSegmentKeys": [e["pointId"] for e in entries if e.get("segmentKeys")],
        "guarantees": guarantees, "protected": ev.get("protected"), "clauses": clauses,
    }))
    check(
        "retention-shared-segment-guarantee-is-derived",
        "PLAT-16.2",
        all(clauses.values()),
        "`sharedSegments` is computed from the view rather than claimed: the view's entries "
        f"carry the fields {segment_fields} and "
        f"{len([e for e in entries if e.get('segmentKeys')])} of {len(entries)} carry segment "
        f"keys, so the guarantee reads {guarantees.get('sharedSegments')!r} and no point is "
        f"protected with `SharedSegment` "
        f"({sorted({p.get('reason') for p in (ev.get('protected') or [])})}). "
        + "; ".join(f"{k}={v}" for k, v in clauses.items()),
        evidence,
    )
    record(
        "retention-shared-segment",
        "PLAT-16.2",
        "NOT-RUN",
        "UNREACHABLE THROUGH THE CONTROLLER on this build. `evaluate` protects a candidate "
        "whose segments a retained point also names, with `SharedSegment` — but only when "
        "the point carries its segment keys, and `point_facts` cannot supply them because a "
        "catalog view entry has no segment field at all. The observable half is "
        "`retention-shared-segment-guarantee-is-derived`, which PASSES and pins the honest "
        "`NotEnforced` to the view rather than to a constant; the day a view entry carries "
        "its segment keys that row tests the other branch with no edit and this test becomes "
        "runnable. It is a product gap, not a harness one.",
        [],
    )

    # --- partial failure -----------------------------------------------------
    image = retention_image("retention-partial-failure")
    if image is None:
        return
    # CANDIDATES WHOSE OBJECTS ARE STILL THERE. The catalog can legitimately
    # still call a point a candidate after an earlier phase deleted its objects
    # — the view is rebuilt on a cadence — and a "denied" deletion of a key
    # that does not exist still answers AccessDenied, which would make the
    # record's `remaining_keys` name objects the bucket no longer has. The
    # fixture needs two sets that are really present.
    live_keys = {o["key"] for o in objects(BUCKET_B)}
    candidates = [
        c["pointId"] for c in (ev.get("candidates") or [])
        if c.get("pointId") in by_point
        and sum(1 for k in live_keys
                if k.startswith(f"{DEST_PREFIX}/{by_point[c['pointId']]['backupId']}/")) >= 2
    ]
    if len(candidates) < 2:
        record("retention-partial-failure", "PLAT-16.2", "NOT-RUN",
               f"needs two candidates whose objects are still in the bucket, and this "
               f"evaluation has {len(candidates)} of "
               f"{len(ev.get('candidates') or [])} candidate(s).",
               evidence)
        return
    allowed_set = by_point[candidates[0]]["backupId"]
    denied_set = by_point[candidates[1]]["backupId"]
    allowed_prefix = f"{DEST_PREFIX}/{allowed_set}/"
    denied_prefix = f"{DEST_PREFIX}/{denied_set}/"
    policy_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow",
                 "Action": ["s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
                 "Resource": [f"arn:aws:s3:::{BUCKET_B}", f"arn:aws:s3:::{BUCKET_B}/*"]},
                {"Effect": "Deny", "Action": ["s3:DeleteObject"],
                 "Resource": [f"arn:aws:s3:::{BUCKET_B}/{denied_prefix}*"]},
            ],
        }
    )
    partial_secret = mint()
    run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
              f"printf '%s' '{policy_doc}' > /tmp/partial.json && "
              f"mc admin policy create adm {OWNER}-partial /tmp/partial.json >/dev/null 2>&1; "
              f"mc admin user add adm {OWNER}partial {partial_secret} >/dev/null 2>&1; "
              f"mc admin policy attach adm {OWNER}-partial --user {OWNER}partial "
              f">/dev/null 2>&1; echo done"],
        check=False, timeout=120)
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned(f"{OWNER}-partial-s3"),
           "stringData": {"access-key-id": f"{OWNER}partial",
                          "secret-access-key": partial_secret}})
    digest, plan = plan_from_evaluation(
        {"candidates": [{"pointId": p} for p in candidates[:2]]},
        "keep-b", "dest-b", by_point, f"{OWNER}-partial-plan", keep_last=2, min_usable=3)
    evidence.append(artifact("enforce/partial-plan.json",
                             {"plan": plan, "sha256": digest,
                              "allowedPrefix": allowed_prefix, "deniedPrefix": denied_prefix}))
    before = objects(BUCKET_B)
    _job, logs, code = retention_job(
        f"{OWNER}-partial", f"{OWNER}-partial-plan", dry_run=False, digest=digest, image=image,
        delete_secret=f"{OWNER}-partial-s3",
    )
    after = objects(BUCKET_B)
    gone = {o["key"] for o in before} - {o["key"] for o in after}
    points = key_lines(logs, "retention-point=")
    record_line = key_lines(logs, "retention-record=")
    record_key = record_line[0]["retention-record"] if record_line else None
    doc: dict[str, Any] = {}
    if record_key:
        doc = json.loads(cat(BUCKET_B, record_key).decode())
        evidence.append(artifact("enforce/partial-record.json", doc))
    remaining = {o["key"] for o in after}
    evidence.append(artifact("enforce/partial-log.txt", logs))
    evidence.append(artifact("enforce/partial-objects.json",
                             {"gone": sorted(gone), "points": points,
                              "deniedSetStillPresent": sorted(
                                  k for k in remaining if k.startswith(denied_prefix))}))
    partial = partial_failure_is_attributable(points, code, gone, remaining, allowed_prefix,
                                              denied_prefix, doc)
    check(
        "retention-partial-failure",
        "PLAT-16.2",
        all(partial.values()),
        f"one plan, two points, and a credential denied `s3:DeleteObject` under "
        f"{denied_prefix}: the run exited {code} and reported {len(points)} per-point lines "
        f"{[{k: v for k, v in p.items() if k != 'objects'} for p in points]}; "
        f"{len(gone)} key(s) removed, all under {allowed_prefix} "
        f"({all(k.startswith(allowed_prefix) for k in gone) if gone else False}), none under "
        f"{denied_prefix}; the record at {record_key} totals "
        f"{doc.get('objects_deleted')} deleted and names the denied point's leftovers "
        f"{[pt.get('remaining_keys') for pt in (doc.get('points') or []) if pt.get('code')]}. "
        + "; ".join(f"{k}={v}" for k, v in partial.items()),
        evidence,
    )
    STATE["partialFailure"] = {"exitCode": code, "gone": sorted(gone),
                               "recordKey": record_key, "clauses": partial}
    save()



def bounded_retry() -> None:
    """PLAT-16.2's bounded retry, in its own phase because it is slow.

    D3 §6.5 counts CONSECUTIVE failed runs, and a run that stopped on its own
    ceiling (`BudgetExhausted`) deliberately does not count — three ordinary
    bounded runs on a large archive must not degrade a healthy policy. So the
    fixture is an `Enforce` policy on a cadence with a credential that can read
    and list and cannot delete: every run genuinely fails.

    IT IS SLOW, AND THE WINDOW IS STATED RATHER THAN GUESSED. The cron is
    `* * * * *`, but an enforcement run is gated by the evaluation, the lease
    and the plan, and the first observed run started about eight minutes after
    the policy was created. Three of them is the budget, so the window is
    thirty minutes and the row records how many it actually saw.
    """
    evidence: list[str] = []
    window = int(os.environ.get("LOGWEIR_D3_DEGRADE_WINDOW", "1800"))
    for leftover in ("keep-b", f"{OWNER}-degrade"):
        if get_opt("retentionpolicy", leftover) is not None:
            run(KN + ["delete", "retentionpolicy", leftover, "--wait=true"], check=False)
    if get_opt("secret", RO_SECRET) is None:
        ro_policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
                 "Resource": [f"arn:aws:s3:::{BUCKET_B}", f"arn:aws:s3:::{BUCKET_B}/*"]}
            ],
        })
        ro_secret = mint()
        run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
                  f"printf '%s' '{ro_policy}' > /tmp/ro.json && "
                  f"mc admin policy create adm {RO_POLICY} /tmp/ro.json >/dev/null 2>&1; "
                  f"mc admin user add adm {RO_USER} {ro_secret} >/dev/null 2>&1; "
                  f"mc admin policy attach adm {RO_POLICY} --user {RO_USER} >/dev/null 2>&1; "
                  f"echo done"], check=False, timeout=120)
        apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned(RO_SECRET),
               "stringData": {"access-key-id": RO_USER,
                              "secret-access-key": ro_secret}})
    created = apply(retention_policy(
        f"{OWNER}-degrade", "dest-b", "secondary", mode="Enforce",
        rules={"keepLast": 1, "minUsablePoints": 1},
        enforcement={
            "credentialSecretRef": {"name": RO_SECRET},
            "schedule": "* * * * *",
            "requireApprovedPlan": False,
            "deadlineSeconds": 120,
            "maxDeletionsPerRun": 2,
            "maxObjectsPerRun": 200,
        },
    ))
    policy_uid = created["metadata"]["uid"]

    def owned_jobs() -> set[str]:
        """The policy's OWN enforcement Jobs, by ownerReference.

        Not every Job in the namespace: catalog syncs and backup runners are
        created by other objects on their own cadence, and counting them would
        make `no further Job while degraded` impossible to satisfy for reasons
        that have nothing to do with retention.
        """
        return {
            j["metadata"]["name"] for j in lst("jobs")
            if any(o.get("uid") == policy_uid
                   for o in (j["metadata"].get("ownerReferences") or []))
        }

    # THE JOBS ARE THE GROUND TRUTH. `status.lastEnforcement.runId` is the field
    # the defect below freezes, so a loop that counted runs from it would
    # under-report by construction — which is exactly how a window containing
    # five failed enforcement Jobs was reported as "no run at all" (review
    # **H-1**). Runs are counted from the policy's OWN Jobs, by ownerReference,
    # and the status counter is read beside them as a separate claim.
    deadline = time.time() + window
    policy: dict[str, Any] = {}
    failures = 0
    runs: dict[str, str] = {}
    while time.time() < deadline:
        policy = get("retentionpolicy", f"{OWNER}-degrade")
        status = policy.get("status") or {}
        failures = status.get("consecutiveRunFailures") or 0
        for job in lst("jobs"):
            if not any(o.get("uid") == policy_uid
                       for o in (job["metadata"].get("ownerReferences") or [])):
                continue
            js = job.get("status") or {}
            if js.get("failed"):
                runs[job["metadata"]["name"]] = "failed"
            elif js.get("succeeded"):
                runs[job["metadata"]["name"]] = "succeeded"
            else:
                runs.setdefault(job["metadata"]["name"], "running")
        if condition(policy, "EnforcementDegraded").get("status") == "True":
            break
        # ENOUGH FAILED RUNS AND A COUNTER THAT DID NOT MOVE is the defect, and
        # waiting out the rest of the window only delays reporting it.
        if sum(1 for v in runs.values() if v == "failed") >= 3 and failures < 3:
            time.sleep(60)
            policy = get("retentionpolicy", f"{OWNER}-degrade")
            failures = (policy.get("status") or {}).get("consecutiveRunFailures") or 0
            if failures < 3:
                break
        time.sleep(15)
    failed_runs = sum(1 for v in runs.values() if v == "failed")
    degraded_at = now()
    jobs_at_degrade = owned_jobs()
    time.sleep(180)
    new_jobs = sorted(owned_jobs() - jobs_at_degrade)
    evidence.append(artifact("enforce/bounded-retry-policy.json", policy))
    evidence.append(artifact("enforce/bounded-retry-runs.json",
                             {"windowSeconds": window, "observedAt": degraded_at,
                              "consecutiveRunFailures": failures,
                              "ownJobsAndOutcomes": runs, "failedRuns": failed_runs,
                              "lastEnforcement": (policy.get("status") or {})
                              .get("lastEnforcement"),
                              "ownedJobsAtObservation": sorted(jobs_at_degrade),
                              "ownedJobsAfter": new_jobs}))
    retry = bounded_retry_degrades(policy, failed_runs, failures, len(new_jobs))
    uncounted = failed_runs >= 3 and failures < 3
    check(
        "retention-bounded-retry",
        "PLAT-16.2",
        all(retry.values()),
        f"an Enforce policy on a `* * * * *` cadence with a credential that cannot delete "
        f"ran {len(runs)} enforcement Job(s) of its own in {window}s, {failed_runs} of them "
        f"FAILED ({runs}), and the policy's own counter reads "
        f"consecutiveRunFailures={failures} with EnforcementDegraded="
        f"{condition(policy, 'EnforcementDegraded').get('status')}/"
        f"{condition(policy, 'EnforcementDegraded').get('reason')}; it created "
        f"{len(new_jobs)} further Job(s) {new_jobs} afterwards. "
        + (
            "DEFECT RET-DEGRADED-UNREACHABLE: the runs failed and the policy did not count "
            "them. `retention_policy.rs` logs \"RetentionPolicy carries no "
            "metadata.resourceVersion, which a /status compare-and-set needs (D-SEAMS S7); "
            "no patch is sent\", so the status patch never lands, the failure is never "
            "recorded, consecutiveRunFailures cannot reach DEGRADED_AFTER_FAILURES = 3 and "
            "`EnforcementDegraded` can NEVER fire — D3 §6.5's bounded retry is unreachable "
            "on this build, while Ready/Evaluated/Enforced all read True and a console sees "
            "a healthy policy. This row FAILS on the product and must not be recorded as "
            "unrun: the failure is the finding. "
            if uncounted else ""
        )
        + "; ".join(f"{k}={v}" for k, v in retry.items()),
        evidence,
    )
    # AND IT RESUMES ON A SPEC CHANGE, the other half of "until the spec
    # changes": the stop is keyed on `observedGeneration == generation`.
    before_gen = get("retentionpolicy", f"{OWNER}-degrade")["metadata"]["generation"]
    patch_policy(f"{OWNER}-degrade", {"rules": {"keepLast": 2, "minUsablePoints": 1}})
    resumed = wait_for(
        "retentionpolicy", f"{OWNER}-degrade",
        lambda o: o["metadata"]["generation"] > before_gen
        and (o.get("status") or {}).get("observedGeneration") == o["metadata"]["generation"],
        seconds=300, what="the policy to observe its new generation",
    )
    evidence.append(artifact("enforce/bounded-retry-after-spec-change.json", resumed))
    check(
        "retention-bounded-retry-resumes-on-a-spec-change",
        "PLAT-16.2",
        (resumed.get("status") or {}).get("observedGeneration")
        == resumed["metadata"]["generation"]
        and condition(resumed, "Evaluated").get("status") in {"True", "False"},
        f"the stop is keyed on the observed generation, so an edit ends it: generation "
        f"{before_gen} -> {resumed['metadata']['generation']}, observedGeneration "
        f"{(resumed.get('status') or {}).get('observedGeneration')}, Evaluated="
        f"{condition(resumed, 'Evaluated').get('status')}/"
        f"{condition(resumed, 'Evaluated').get('reason')}, EnforcementDegraded="
        f"{condition(resumed, 'EnforcementDegraded').get('status')}",
        evidence,
    )
    run(KN + ["delete", "retentionpolicy", f"{OWNER}-degrade", "--wait=true"], check=False)


# ---------------------------------------------------------------------------
# PLAT-19.1 — trust lifecycle against a terminal, already-verified Backup
# ---------------------------------------------------------------------------

LEGACY_ARCHIVE = f"s3://kafka-backups/{OWNER}-{STAMP}"
TRUST_POLICY = f"{OWNER}-{STAMP}"


def legacy_backup(name: str, topics: list[str] | None = None,
                  namespace: str | None = None) -> dict[str, Any]:
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
    # THE NAMESPACE IS A PARAMETER because `multiple_namespaces` needs the SAME
    # run in two of them: one archive, one signing key, two policies, and the
    # verdicts have to differ for the resolution and for nothing else.
    ns = namespace or NS
    body = {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": ns, "labels": dict(LABEL)},
        "spec": {
            "sourceRef": {"name": "source"},
            "topics": topics or ["orders"],
            "archive": {"url": LEGACY_ARCHIVE, "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "deadlineSeconds": 600,
        },
    }
    if get_opt("backup", name, namespace=ns) is not None:
        run(K + ["-n", ns, "delete", "backup", name, "--wait=true"])
    apply(body)
    wait_for("backup", name, terminal, seconds=600, namespace=ns, what="a terminal phase")
    return wait_for(
        "backup", name, lambda o: verdict_of(o) in VERDICTS, seconds=240, namespace=ns,
        what="an evidence verdict",
    )


def roster_signing_key() -> dict[str, Any]:
    roster = get("trustroster", "default", namespace="default")
    return roster["spec"]["signingKeys"][0]


def policy_key(key_id: str, spki_pem: str, state: str, *, display: str,
               subject: str = "signing@scram-local.invalid", **over: Any) -> dict[str, Any]:
    """One `spec.keys[]` entry, with its own validity window.

    PER-KEY WINDOWS ARE THE POINT of taking a list: `old archive` needs two
    keys whose states differ, and a retirement is a change to ONE entry's
    `state`/`retiredAt` while the other's window stays where it was.
    """
    entry = {
        "keyId": key_id,
        "spkiPem": spki_pem,
        "algorithm": "p256",
        "principal": {"id": subject, "display": display},
        "usages": ["EvidenceSigning"],
        "state": state,
        "notBefore": "2026-01-01T00:00:00Z",
        "notAfter": "2027-01-01T00:00:00Z",
    }
    entry.update(over)
    return entry


def trust_policy(state: str, *, keys: list[dict[str, Any]] | None = None,
                 namespaces: list[str] | None = None, name: str | None = None,
                 **over: Any) -> dict[str, Any]:
    key = roster_signing_key()
    entry = policy_key(key["keyId"], key["spkiPem"], state,
                       display="the lab signing key",
                       subject=key.get("subject", "signing@scram-local.invalid"))
    entry.update(over)
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicy",
        "metadata": owned(name or TRUST_POLICY, namespace=None),
        "spec": {"namespaces": namespaces or [NS], "keys": keys or [entry]},
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

    # --- the tracker's `overlap`, under its own name ----------------------
    # A DECLARED ALIAS OF THE TWO VERDICTS ABOVE, not a new fixture: `v` is the
    # same terminal Backup judged under the SYNTHESISED `legacy-roster-v1` and
    # `va` is the same object judged under the explicit policy that now governs
    # this namespace. The tracker's test is that both are accepted while they
    # overlap, and that is what these two verdicts are — the row exists because
    # the review found no row under the tracker's NAME, and a test nobody can
    # find is a test nobody counts.
    overlap = {
        "the synthesised policy accepted it": (
            v.get("result") == "Valid"
            and v.get("trust", {}).get("policy", {}).get("name") == "legacy-roster-v1"
        ),
        "the explicit policy accepts the same object": (
            va.get("result") == "Valid"
            and va.get("trust", {}).get("policy", {}).get("name") == TRUST_POLICY
        ),
        "against the same key, so it is one object judged twice": (
            va.get("matchedKeyId") == v.get("matchedKeyId")
        ),
        "and the basis did not weaken across the handover": (
            va.get("trust", {}).get("basis") == v.get("trust", {}).get("basis") == "Current"
        ),
    }
    evidence.append(artifact("trust/overlap.json", {
        "underSynthesised": v, "underExplicit": va, "clauses": overlap}))
    check(
        "trust-overlap",
        "PLAT-19.1",
        all(overlap.values()),
        f"the same terminal Backup is accepted by BOTH policies while they overlap: "
        f"{v.get('result')} under `legacy-roster-v1` (the synthesised policy an installation "
        f"starts with) and {va.get('result')} under the explicit `{TRUST_POLICY}` that now "
        f"names this namespace, same key {str(v.get('matchedKeyId'))[:16]}…, basis "
        f"{va.get('trust', {}).get('basis')} either way. Declared alias: the verdicts are "
        f"`trust-fresh-object-records-signedAt`'s and "
        f"`trust-explicit-policy-keeps-evidence-valid`'s; this row is the tracker's test "
        f"under the tracker's name. "
        + "; ".join(f"{k}={x}" for k, x in overlap.items()),
        evidence,
    )
    # --- and `upgrade from the default roster`, likewise -------------------
    upgrade = {
        "an installation with no explicit policy still judges": bool(v.get("result")),
        "under the SYNTHESISED legacy-roster-v1":
            v.get("trust", {}).get("policy", {}).get("name") == "legacy-roster-v1",
        "and the verdict survives the upgrade to an explicit policy":
            va.get("result") == v.get("result") == "Valid",
        "which is a different policy by name":
            va.get("trust", {}).get("policy", {}).get("name") != "legacy-roster-v1",
    }
    evidence.append(artifact("trust/upgrade-from-default-roster.json",
                             {"before": v, "after": va, "clauses": upgrade}))
    check(
        "trust-upgrade-from-default-roster",
        "PLAT-19.1",
        all(upgrade.values()),
        f"an installation that has never written a TrustPolicy judges under the synthesised "
        f"`legacy-roster-v1` ({v.get('result')}), and applying an explicit policy is an "
        f"UPGRADE rather than a reset: the same object reads {va.get('result')} under "
        f"`{TRUST_POLICY}`. Declared alias of the same two verdicts as `trust-overlap`, "
        f"named for the tracker's test. The pre-`signedAt` half — the five 2026-09-14 "
        f"objects healing — is `trust-signedat-upgrade-heals` and lab-refresh-6 §6. "
        + "; ".join(f"{k}={x}" for k, x in upgrade.items()),
        evidence,
    )
    record(
        "trust-unknown-stale-expiry",
        "PLAT-19.1",
        "NOT-RUN",
        "IT IS A KEYS-VIEW RENDERING, and D3 §7.7 says so in as many words. The EVALUATION "
        "column reads `unknown` when `status` is absent, when "
        "`status.observedGeneration != metadata.generation`, or when `evaluatedAt` is older "
        "than 15 minutes measured against a SERVER clock — and \"`valid` and `expired` are "
        "only rendered for a fresh evaluation\". None of those three is a verdict this "
        "harness can read off an object: they are statements about how a page renders a "
        "roster whose evaluation is missing or stale, and the replacement they describe is "
        "`ui/pages/keys.js`, which is D3 W12's work. No console journey touches a keys view, "
        "so this test has no row anywhere. What is provable from the API is the neighbouring "
        "rule — a key absent from the resolved policy, or signing after its retirement, "
        "never reads Valid — and that is "
        "`trust-two-namespaces-resolve-their-own-policies` (Invalid where the key is not "
        "listed) and `trust-old-archive-survives-its-signer-retiring` (Untrusted for a "
        "signature after retirement). Named here rather than aliased, because neither is the "
        "`unknown` rendering §7.7 defines.",
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
    # WAITING FOR THE HEALED SHAPE, not for the defect. The old loop broke as
    # soon as the verdict stopped being `Valid` OR `signedAt` came back — which
    # is "wait until something happens", and it was written when the something
    # was the re-derivation to `Untrusted`. The fix restores the field, so the
    # loop waits for the restoration and stops early on the verdict that would
    # mean the defect is back.
    deadline = time.time() + 240
    v2: dict[str, Any] = stripped
    while time.time() < deadline:
        v2 = (
            get("backup", "signedat-subject").get("status", {}).get("evidence", {})
            .get("verification", {}) or {}
        )
        if v2.get("result") == "Valid" and v2.get("signedAt"):
            break
        if v2.get("result") not in {"Valid", None}:
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
    heals = signedat_heals(v0, v2)
    evidence.append(artifact("trust/signedat-heal-clauses.json", heals))
    check(
        "trust-signedat-upgrade-heals",
        "PLAT-19.1",
        all(heals.values()),
        f"a fresh run records result={v0.get('result')} signedAt={v0.get('signedAt')}; the "
        f"SAME receipt with `signedAt` removed from its stored status — the shape of every "
        f"pre-`signedAt` object — re-derives to result={v2.get('result')} signedAt="
        f"{v2.get('signedAt')} after one read, with matchedKeyId "
        f"{'unchanged' if v2.get('matchedKeyId') == v0.get('matchedKeyId') else 'MOVED'}. "
        f"This row used to assert the opposite: it was "
        f"`trust-signedat-upgrade-defect`, and it passed only when the re-derivation came "
        f"back `Untrusted` — an inverted guard that outlived TRUST-UPGRADE-SIGNEDAT's fix "
        f"and failed on the build that fixed it (lab-refresh-5 §8.2). "
        + "; ".join(f"{k}={v}" for k, v in heals.items()),
        evidence,
    )
    cleared = cleared_block_is_not_re_read(restored)
    # `check()` takes no extra keywords — `record()` did, and this row used to be
    # one. The three observations travel in the artifact instead, which is where
    # a reader looks for them anyway.
    evidence.append(artifact("trust/cleared-block-clauses.json",
                             {"clauses": cleared, "freshVerification": v0,
                              "rederivedVerification": v2, "afterClearing": restored}))
    check(
        "trust-cleared-verification-is-not-re-read",
        "PLAT-19.1",
        all(cleared.values()),
        f"clearing `status.evidence.verification` entirely triggers NO archive re-read: "
        f"after 150 s and a policy event the block is {restored or '<still absent>'}. This "
        f"is the still-true half of the old defect row and it is a product statement, not a "
        f"regression — the re-trust pass re-derives from the STORED matchedKeyId, signedAt "
        f"and verifiedAt and performs no fetch, so an operator who clears the block gets "
        f"nothing back. It is kept because it bounds what the fix above does: `signedAt` is "
        f"restored by re-derivation from what is still stored, never by going to the "
        f"archive. "
        + "; ".join(f"{k}={v}" for k, v in cleared.items()),
        evidence,
    )


def signedat_heals(fresh: dict[str, Any], rederived: dict[str, Any]) -> dict[str, bool]:
    """TRUST-UPGRADE-SIGNEDAT's fix, asserted as the fix rather than the defect.

    A pre-`signedAt` object is a stored verification block with a verdict and a
    matched key and no signing time. The defect re-derived that to `Untrusted`
    (`SignedOutsideValidity`, "carries no signing-time field"); the fix reads
    the document's own latest pre-signature timestamp and restores the field,
    so the SAME receipt comes back `Valid` WITH `signedAt` after one read.

    The last clause is what keeps this from being a row about nothing: a pass
    that restored `signedAt` by changing which key it matched would not be a
    repair, it would be a different verdict wearing the same word.
    """
    return {
        "the fresh run recorded a verdict and a signing time": (
            fresh.get("result") == "Valid" and bool(fresh.get("signedAt"))
        ),
        "the stripped object re-derives Valid, not Untrusted":
            rederived.get("result") == "Valid",
        "and its signing time is back": bool(rederived.get("signedAt")),
        "against the same signing key": (
            rederived.get("matchedKeyId") == fresh.get("matchedKeyId")
        ),
    }


def cleared_block_is_not_re_read(after: dict[str, Any]) -> dict[str, bool]:
    """The half of the old defect row that is still true, and still a bound.

    Clearing `status.evidence.verification` outright is the only move an
    operator has left, and it produces nothing: the re-trust pass re-derives
    from what is STORED and performs no archive fetch, so with nothing stored
    there is nothing to re-derive. Keeping it beside the healing row is what
    stops that row being read as "the controller re-reads the archive".
    """
    return {
        "no verdict came back from an archive read": not after.get("result"),
        "and no signing time with it": not after.get("signedAt"),
    }


# ---------------------------------------------------------------------------
# PLAT-19.1's `old archive` — a second signer, then retired
# ---------------------------------------------------------------------------


def old_archive_survives_retirement(before: dict[str, Any], after: dict[str, Any],
                                    fresh: dict[str, Any]) -> dict[str, bool]:
    """D3 §7.4: a retired key's OLD evidence stays valid; its NEW evidence does not.

    Retirement is not revocation. `Retired` means "may no longer sign", and the
    whole point of distinguishing it from `Revoked` is that everything the key
    signed while it was `Active` keeps its verdict — with `basis: Historical`
    rather than `Current`, so a console can say "verified against a retired
    key" instead of pretending nothing happened. A build that answered `Valid`
    with `basis: Current` after a retirement would be hiding the retirement;
    one that answered `Untrusted` would be revocation wearing retirement's
    name, and would invalidate every archive an operator still needs.

    The third clause is what keeps the first two honest: if a run signed AFTER
    the retirement also came back `Valid`, "retired" would mean nothing at all.
    """
    return {
        "the archive verified Valid while the key was Active":
            before.get("result") == "Valid",
        "and on the CURRENT basis, since the key was live when it signed":
            (before.get("trust") or {}).get("basis") == "Current",
        "after the retirement the same archive is STILL Valid":
            after.get("result") == "Valid",
        "on the HISTORICAL basis, so the retirement is visible":
            (after.get("trust") or {}).get("basis") == "Historical",
        "against the same key, so this is the same evidence re-judged":
            after.get("matchedKeyId") == before.get("matchedKeyId"),
        "and a run signed AFTER the retirement is NOT Valid":
            fresh.get("result") != "Valid",
    }


def mint_signing_key(tag: str) -> dict[str, Any]:
    """A second P-256 signing keypair, private half on disk and nowhere else.

    THE PRIVATE HALF NEVER ENTERS AN OBJECT OR AN ARTIFACT. It is written 0600
    inside a 0700 directory, projected into ONE Secret this namespace owns, and
    deleted in the caller's `finally`. What is recorded is the public SPKI and
    the key id, which is what a roster carries and what every verdict names.

    `keyId` is the sha256 of the DER SPKI, lowercase hex — the same recipe the
    lab's own key satisfies, which the caller re-computes against the roster
    entry before trusting this function at all.
    """
    work = pathlib.Path(tempfile.mkdtemp(prefix=f"{tag}-", dir="/tmp"))
    work.chmod(0o700)
    private = work / "signing.pem"
    sec1 = work / "sec1.pem"
    run(["openssl", "ecparam", "-name", "prime256v1", "-genkey", "-noout",
         "-out", str(sec1)], timeout=60)
    sec1.chmod(0o600)
    # PKCS#8, NOT SEC1. `logweir_evidence::keys` parses with `from_pkcs8_pem`,
    # and `openssl ecparam -genkey` writes `BEGIN EC PRIVATE KEY` (SEC1), which
    # that parser refuses — the runner then exits 4 with no evidence at all,
    # which is exactly how this row first failed.
    run(["openssl", "pkcs8", "-topk8", "-nocrypt", "-in", str(sec1),
         "-out", str(private)], timeout=60)
    private.chmod(0o600)
    sec1.unlink(missing_ok=True)
    spki_pem = run(["openssl", "ec", "-in", str(private), "-pubout"],
                   timeout=60).stdout
    der = subprocess.run(["openssl", "pkey", "-pubin", "-outform", "DER"],
                         input=spki_pem.encode(), capture_output=True, timeout=60).stdout
    return {"dir": work, "private": private, "spkiPem": spki_pem,
            "keyId": hashlib.sha256(der).hexdigest()}


def roster_approver_key() -> dict[str, Any]:
    return get("trustroster", "default", namespace="default")["spec"]["approverKeys"][0]


def approver_material() -> dict[str, pathlib.Path]:
    """The lab roster's approver keypair, written when the lab was built.

    The PRIVATE half stays where `scripts/test-k8s-scram.py` put it and is
    passed to the CLI by path; nothing here reads or records its bytes.
    """
    base = pathlib.Path(os.environ.get("LOGWEIR_SCRAM_OUT", "/tmp/logweir-scram-e2e"))
    needed = {"approver": base / "approver.pem", "approverPub": base / "approver.pub.pem"}
    missing = [str(v) for v in needed.values() if not v.is_file()]
    if missing:
        raise RuntimeError("lab approver material is absent: " + ", ".join(missing))
    return needed


def logweir_cli() -> str:
    override = os.environ.get("LOGWEIR_BIN")
    if override and pathlib.Path(override).is_file():
        return override
    for candidate in (ROOT / "target/debug/logweir", ROOT / "target/release/logweir"):
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError("no `logweir` binary: set LOGWEIR_BIN or build one")


def legacy_restore_plan(backup_id: str, point_in_time: str, prefix: str) -> dict[str, Any]:
    """A restore plan over the LEGACY inline archive this namespace writes to."""
    return {
        "source": {
            "storage": {"backend": "s3", "bucket": "kafka-backups",
                        "prefix": f"{OWNER}-{STAMP}", "region": "us-east-1",
                        "endpoint": MINIO_ENDPOINT, "path_style": True, "allow_http": True},
            "backup": backup_id,
            "topics": TOPICS[:1],
        },
        "target": {
            "bootstrap_servers": [f"kafka-source.{FIXTURE_NS}.svc.cluster.local:9096"],
            "auth": {"mode": "scramSha512", "username": "scram-user", "tls": False},
            "mode": "scratch",
            "topic_naming": {"prefix": prefix},
            "topic_mapping_prefix": "logweir-scratch-",
            "marker_topic": "logweir.scratch",
            "default_replication_factor": 1,
            "teardown": "delete",
        },
        "restore": {"point_in_time": point_in_time},
        "objectives": {"rto_seconds": 3600, "rpo_seconds": 86400, "pass_rate": 1.0},
        "evidence": {"backend": "s3", "bucket": "kafka-backups",
                     "prefix": "logweir/", "region": "us-east-1",
                     "endpoint": MINIO_ENDPOINT, "path_style": True, "allow_http": True},
        "notifications": {"webhooks": []},
    }


def historical_archive_still_restores(verdict: dict[str, Any], admitted: dict[str, Any],
                                      job: str | None, phase: str | None,
                                      fresh: dict[str, Any]) -> dict[str, bool]:
    """A retired key's archive is still READABLE, and still unsignable.

    D3 §7.4's green rule is `Valid ∧ (basis Current|Historical)`, and the point
    of `Historical` is that a retirement must not strand the archives the key
    signed: an operator has to be able to RESTORE from them. So the read side
    has to proceed — the Restore admitted and its Job created — while the write
    side does not: a run signed after the retirement is refused.

    The last clause is what stops this being a row about a restore that happened
    to work: if a NEW signature were also accepted, "retired" would mean nothing
    and the read half would be proving no rule at all.
    """
    # ADMISSION IS A CONDITION, AND THIS ROW ASKS FOR IT. The comment here used
    # to say the opposite — that on `Ok` the controller "creates the Job instead
    # of stamping `Admitted=True`", and that a first draft asking for
    # `Admitted=True` had been "asserting something the product never writes",
    # so the clause was weakened to `!= "False"`.
    #
    # That was a DEFECT OBSERVED LIVE AND MIS-DIAGNOSED. The controller does
    # write `Admitted=True`; the next reconcile of the same running object then
    # dropped it, because `diagnostics::apply` replaced the condition array
    # instead of upserting into it. That is RESTORE-ADMITTED-DROPPED, already
    # recorded and fixed on `claude/status-sweep` (review
    # `claude/status-sweep.review.md`, LOW-1, which named this very comment as
    # the defect's strongest live corroboration).
    #
    # The clause is therefore tightened back to `== "True"`. ON A LAB BUILD
    # THAT PREDATES THAT FIX THIS ROW FAILS, and that is the honest reading: an
    # auditor looking at the Restore cannot tell that it was approved. It is
    # expected to pass at the first batch refresh that carries the fix.
    return {
        "the archive verifies on the historical basis": (
            verdict.get("result") == "Valid"
            and (verdict.get("trust") or {}).get("basis") == "Historical"
        ),
        "the Restore says it was admitted (`Admitted=True`)":
            admitted.get("status") == "True",
        "its runner Job exists and it is running — the read PROCEEDED": (
            bool(job) and phase in {"Running", "Succeeded"}
        ),
        "while a run signed after the retirement is refused":
            fresh.get("result") != "Valid",
    }


def old_archive() -> None:
    """A Backup signed by a SECOND key, then that key retired.

    The signing Secret is looked up by the fixed name `logweir-signing-key` in
    the RUN'S OWN namespace (`controllers/backup.rs::SIGNING_KEY_SECRET`), so a
    second signer needs no change to anything shared: this namespace's copy is
    replaced with a keypair minted here, and the lab's own Secret is untouched.
    """
    evidence: list[str] = []
    # A FRESH POLICY, BECAUSE `spec.keys` IS APPEND-ONLY. The CRD refuses to
    # drop a keyId — "old archives still need the public material that signed
    # them", which is the same rule this row exists to demonstrate — and every
    # run of this phase mints a DIFFERENT second key, so applying over a
    # previous run's policy is refused. The policy is this run's own, named with
    # the stamp, and is removed rather than edited.
    if get_opt("trustpolicy", TRUST_POLICY, namespace="default") is not None:
        run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"], check=False)
    key = mint_signing_key(f"{OWNER}-signer2")
    original = get("secret", "logweir-signing-key")
    try:
        # the recipe, checked against the lab's own key before it is relied on
        lab = roster_signing_key()
        lab_der = subprocess.run(["openssl", "pkey", "-pubin", "-outform", "DER"],
                                 input=lab["spkiPem"].encode(), capture_output=True,
                                 timeout=60).stdout
        check(
            "trust-key-id-recipe-matches-the-roster",
            "PLAT-19.1",
            hashlib.sha256(lab_der).hexdigest() == lab["keyId"],
            f"the keyId this row computes for a minted key — sha256 of the DER SPKI, "
            f"lowercase hex — reproduces the roster's own recorded keyId for the lab key "
            f"({lab['keyId'][:16]}…). A second signer identified by a recipe nobody checked "
            f"would be a key the policy never matches, and every verdict below would be "
            f"about nothing",
            evidence,
        )
        run(KN + ["delete", "secret", "logweir-signing-key", "--wait=true"], check=False)
        run(KN + ["create", "secret", "generic", "logweir-signing-key",
                  f"--from-file=signing.pem={key['private']}"], timeout=120)
        # THE APPROVER KEY GOES IN TOO. An explicit policy REPLACES the roster
        # for this namespace, so a policy listing only signing keys leaves the
        # restore's Approval with no approver key to verify against — and the
        # restore half below would fail for a reason that has nothing to do
        # with the retirement it is about.
        approver = roster_approver_key()
        apply(trust_policy(
            "Active",
            keys=[policy_key(lab["keyId"], lab["spkiPem"], "Active",
                             display="the lab signing key"),
                  policy_key(key["keyId"], key["spkiPem"], "Active",
                             display=f"{OWNER}'s second signer",
                             subject=f"{OWNER}-signer2@logweir.invalid"),
                  policy_key(approver["keyId"], approver["spkiPem"], "Active",
                             display="the lab approver key",
                             subject="approver@scram-local.invalid",
                             usages=["GovernedApproval"])],
        ))
        old = legacy_backup(f"{OWNER}-old-archive")
        before = old["status"]["evidence"]["verification"]
        evidence.append(artifact("trust/old-archive-1-signed.json", before))
        check(
            "trust-second-signer-is-trusted-while-active",
            "PLAT-19.1",
            before.get("result") == "Valid" and before.get("matchedKeyId") == key["keyId"],
            f"a Backup signed by a SECOND key this run minted verifies "
            f"{before.get('result')} against {str(before.get('matchedKeyId'))[:16]}… "
            f"(the minted key is {key['keyId'][:16]}…), basis "
            f"{(before.get('trust') or {}).get('basis')}. The signing Secret is resolved by "
            f"a fixed name in the run's own namespace, so nothing shared was touched",
            evidence,
        )
        # --- and now it retires ------------------------------------------
        retired_at = now()
        # THE RETIREMENT IS AN EDIT TO ONE ENTRY, not a new policy: `state` and
        # `retiredAt` move while both keyIds stay, which is what append-only
        # allows and what a real retirement looks like.
        apply(trust_policy(
            "Active",
            keys=[policy_key(lab["keyId"], lab["spkiPem"], "Active",
                             display="the lab signing key"),
                  policy_key(key["keyId"], key["spkiPem"], "Retired",
                             display=f"{OWNER}'s second signer",
                             subject=f"{OWNER}-signer2@logweir.invalid",
                             retiredAt=retired_at),
                  policy_key(approver["keyId"], approver["spkiPem"], "Active",
                             display="the lab approver key",
                             subject="approver@scram-local.invalid",
                             usages=["GovernedApproval"])],
        ))
        after = await_trust(
            f"{OWNER}-old-archive",
            lambda v: (v.get("trust") or {}).get("basis") == "Historical"
            or v.get("result") != "Valid",
            seconds=300, what="the old archive to be re-judged against the retired key",
        )["status"]["evidence"]["verification"]
        evidence.append(artifact("trust/old-archive-2-after-retirement.json", after))
        fresh = legacy_backup(f"{OWNER}-after-retirement")["status"]["evidence"][
            "verification"]
        evidence.append(artifact("trust/old-archive-3-new-run.json", fresh))
        clauses = old_archive_survives_retirement(before, after, fresh)
        evidence.append(artifact("trust/old-archive-clauses.json",
                                 {"clauses": clauses, "retiredAt": retired_at,
                                  "mintedKeyId": key["keyId"]}))
        check(
            "trust-old-archive-survives-its-signer-retiring",
            "PLAT-19.1",
            all(clauses.values()),
            f"the archive signed at {before.get('signedAt')} by the key retired at "
            f"{retired_at} still verifies {after.get('result')} on basis "
            f"{(after.get('trust') or {}).get('basis')} against the same key; a run made "
            f"AFTER the retirement verifies {fresh.get('result')} "
            f"({(fresh.get('trust') or {}).get('basis')}). Retirement is not revocation: "
            f"what the key signed while Active keeps its verdict, and the console can say "
            f"'verified against a retired key' rather than pretending nothing happened. "
            + "; ".join(f"{k}={v}" for k, v in clauses.items()),
            evidence,
        )
        # --- and the READ still proceeds -------------------------------
        # THE POINT OF `Historical`. A retirement must not strand the archives
        # the key signed, so an operator has to be able to RESTORE from them —
        # which is the half harness-rows-6 §5 and harness-rows-7 §5 owed, and
        # which only works over a LEGACY inline archive: a destination-backed
        # run's evidence verdict is `NotAttempted` on this build (D2's own
        # `S1.statusVerification` asserts it), so it has no `Valid` to become
        # `Historical` in the first place. This archive is the one
        # `legacy_backup` wrote and the controller verified itself.
        restore_name = f"{OWNER}-historical-restore"
        approval_name = f"{OWNER}-historical-approval"
        for kind, name in (("restore", restore_name), ("approval", approval_name)):
            run(KN + ["delete", kind, name, "--ignore-not-found=true", "--wait=true"],
                check=False, timeout=120)
        subject = get("backup", f"{OWNER}-old-archive")
        plan = legacy_restore_plan(subject["status"]["backupId"],
                                   subject["status"].get("capture", {}).get("finishedAt")
                                   or now(), f"{OWNER}-hist-")
        plan_bytes = json.dumps(plan, indent=2) + "\n"
        work = pathlib.Path(tempfile.mkdtemp(prefix=f"{OWNER}-hist-", dir="/tmp"))
        work.chmod(0o700)
        try:
            (work / "plan.json").write_text(plan_bytes)
            run([logweir_cli(), "drill", "approve", "--spec", str(work / "plan.json"),
                 "--key", str(approver_material()["approver"]), "--approver", OWNER,
                 "--ticket", "HR7", "--subject-kind", "Restore",
                 "--out", str(work / "approval.json")], timeout=120)
            apply({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                "metadata": owned(restore_name),
                "spec": {
                    "sourceArchive": {"url": LEGACY_ARCHIVE,
                                      "secretRef": {"name": "logweir-s3"}},
                    "backupSetRef": subject["status"]["backupId"],
                    "pointInTime": plan["restore"]["point_in_time"],
                    "planBytes": plan_bytes,
                    "approvalRef": {"name": approval_name},
                    "deadlineSeconds": 900,
                    "target": {"clusterRef": {"name": "source"}, "mode": "scratch",
                               "topicNaming": {"prefix": f"{OWNER}-hist-"}},
                },
            })
            apply({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
                "metadata": owned(approval_name),
                "spec": {
                    "approvalBytes": (work / "approval.json").read_text(),
                    "sidecarBytes": (work / "approval.sig").read_text(),
                    "planHash": "sha256:" + hashlib.sha256(plan_bytes.encode()).hexdigest(),
                    "subjectRef": {"kind": "Restore", "name": restore_name},
                },
            })
        finally:
            shutil.rmtree(work, ignore_errors=True)
        restored = wait_for(
            "restore", restore_name,
            lambda o: (condition(o, "Admitted").get("status") == "True"
                       or (o.get("status") or {}).get("jobRef")
                       or (o.get("status") or {}).get("phase") in
                       {"Running", "Succeeded", "Failed", "Refused"}),
            seconds=420, what="the restore from the retired key's archive to be admitted",
        )
        job = ((restored.get("status") or {}).get("jobRef") or {}).get("name")
        admitted = condition(restored, "Admitted")
        evidence.append(artifact("trust/historical-restore.json", restored))
        phase = (restored.get("status") or {}).get("phase")
        clauses = historical_archive_still_restores(after, admitted, job, phase, fresh)
        evidence.append(artifact("trust/historical-restore-clauses.json", clauses))
        check(
            "trust-old-archive-still-restores",
            "PLAT-19.1",
            all(clauses.values()),
            f"the archive signed by the key retired at {retired_at} verifies "
            f"{after.get('result')} on basis {(after.get('trust') or {}).get('basis')}, and a "
            f"Restore from it PROCEEDS: nothing holds it at admission "
            f"(Admitted={admitted.get('status')}/{admitted.get('reason')}), phase "
            f"{phase!r}, runner Job {job!r}. Meanwhile a "
            f"Backup signed by that same key AFTER the retirement verifies "
            f"{fresh.get('result')} — the archive stays readable and the key stays unusable, "
            f"which is what `Historical` is for. "
            + "; ".join(f"{k}={v}" for k, v in clauses.items()),
            evidence,
        )
        STATE["oldArchive"] = {"mintedKeyId": key["keyId"], "retiredAt": retired_at,
                               "before": before, "after": after, "fresh": fresh,
                               "restore": {"name": restore_name, "job": job,
                                           "admitted": admitted}}
        save()
    finally:
        # THE PRIVATE HALF GOES, whatever happened above.
        for path in (key["private"],):
            path.unlink(missing_ok=True)
        shutil.rmtree(key["dir"], ignore_errors=True)
        run(KN + ["delete", "secret", "logweir-signing-key", "--wait=true"], check=False)
        apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned("logweir-signing-key"),
               "data": original.get("data", {}), "type": original.get("type", "Opaque")})
        evidence.append(artifact("trust/old-archive-key-cleanup.json", {
            "privateKeyFileExists": key["private"].exists(),
            "workDirExists": key["dir"].exists(),
            "labSecretRestored": get_opt("secret", "logweir-signing-key") is not None,
        }))
        check(
            "trust-minted-private-key-never-outlives-the-row",
            "PLAT-19.1",
            not key["private"].exists() and not key["dir"].exists(),
            f"the private half minted for this row is gone from disk "
            f"(file {key['private'].exists()}, dir {key['dir'].exists()}) and never entered "
            f"an artifact: what is recorded is the public SPKI and the key id. The "
            f"namespace's `logweir-signing-key` is restored to the lab's own copy",
            evidence,
        )


# ---------------------------------------------------------------------------
# PLAT-19.1's `multiple namespaces` — two policies, two namespaces, real objects
# ---------------------------------------------------------------------------


def verdicts_differ_by_namespace(governed: dict[str, Any], ungoverned: dict[str, Any],
                                 key_id: str) -> dict[str, bool]:
    """Two namespaces, two explicit policies, one key trusted in one of them.

    D3's `multiple namespaces` is about RESOLUTION: `spec.namespaces` is an
    exact list — "never a pattern: a pattern is how a new namespace silently
    inherits a trust decision" — so two policies over two namespaces must
    resolve independently, and the SAME signing key must be trusted in one and
    not in the other.

    The clause that makes this a resolution test rather than two unrelated
    observations is the last one: both verdicts name the same key. If the
    receipts had been signed by different keys, the differing verdicts would
    say nothing about which policy governed which namespace.
    """
    return {
        "the governed namespace trusts the key": governed.get("result") == "Valid",
        "on the current basis": (governed.get("trust") or {}).get("basis") == "Current",
        "the other namespace does NOT": ungoverned.get("result") != "Valid",
        "and says so with a verdict rather than silence": bool(ungoverned.get("result")),
        "both are about the SAME signing key": (
            governed.get("matchedKeyId") == key_id
            and ungoverned.get("matchedKeyId") in (key_id, None)
        ),
    }


def trust_namespace(namespace: str) -> None:
    """The smallest namespace a trust verdict can be measured in.

    NOT `setup()` PARAMETERISED, AND THE DIFFERENCE IS DELIBERATE. `setup`
    builds buckets, an `mc` pod, destinations and a catalog, none of which a
    `legacy_backup` needs: it writes to the SHARED fixture's archive through an
    inline `archive.url` and is verified by the controller's own read-only
    handle. What a second namespace needs is its own name, the three Secrets
    that Backup projects, and a `KafkaCluster` to read. Building the rest would
    be a second fixture to keep in step with the first, for nothing this row
    reads.
    """
    def apply_there(obj: dict[str, Any]) -> dict[str, Any]:
        """`apply()` is bound to this run's own namespace; the second one needs
        its own target or kubectl refuses the mismatch."""
        return json.loads(run(K + ["-n", namespace, "apply", "-f", "-", "-o", "json"],
                              data=json.dumps(obj)).stdout)

    existing = run(K + ["get", "namespace", namespace, "-o", "name"], check=False, timeout=60)
    if existing.returncode != 0:
        run(K + ["create", "namespace", namespace])
        run(K + ["label", "namespace", namespace, f"logweir.dev/test-owner={OWNER}"])
    # THE RUNNER'S ServiceAccounts, which `setup` creates and a probe Job needs:
    # without `logweir-runner` the reachability probe's pod never starts, the
    # Job finishes with no terminated container, and `reachable` is left unset —
    # "rather than invented", as the controller says.
    for sa in ("logweir-runner", "logweir-retention"):
        apply_there({"apiVersion": "v1", "kind": "ServiceAccount",
                     "metadata": {"name": sa, "namespace": namespace,
                                  "labels": dict(LABEL)}})
    for name in ("source-scram", "logweir-s3", "logweir-signing-key"):
        source = get("secret", name, namespace=FIXTURE_NS)
        apply_there({"apiVersion": "v1", "kind": "Secret",
                     "metadata": {"name": name, "namespace": namespace,
                                  "labels": dict(LABEL)},
                     "type": source.get("type", "Opaque"), "data": source["data"]})
    apply_there({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": "source", "namespace": namespace, "labels": dict(LABEL)},
        "spec": {
            "bootstrapServers": [f"kafka-source.{FIXTURE_NS}.svc.cluster.local:9096"],
            # THE SAME SHAPE `setup` USES. The CRD decodes strictly, so an
            # invented `spec.security` is a BadRequest rather than a default.
            "auth": {"mode": "scramSha512", "username": "scram-user",
                     "secretRef": {"name": "source-scram"}, "tls": False},
            "role": "source",
        },
    })
    wait_for("kafkacluster", "source", lambda o: (o.get("status") or {}).get("reachable")
             is True, seconds=240, namespace=namespace, what="the second namespace's source")


def multiple_namespaces() -> None:
    """Two explicit TrustPolicies, two namespaces, one key trusted in one."""
    evidence: list[str] = []
    second = f"{NS}-b"
    first_policy = f"{OWNER}-{STAMP}-ns-a"
    second_policy = f"{OWNER}-{STAMP}-ns-b"
    lab = roster_signing_key()
    # A key the SECOND namespace trusts INSTEAD of the lab's: a policy with no
    # usable key at all would be a policy that failed to load, and this row
    # needs a policy that loaded and decided.
    other = mint_signing_key(f"{OWNER}-otherkey")
    try:
        trust_namespace(second)
        for name in (first_policy, second_policy):
            if get_opt("trustpolicy", name, namespace="default") is not None:
                run(K + ["delete", "trustpolicy", name, "--wait=true"], check=False)
        apply(trust_policy("Active", name=first_policy, namespaces=[NS],
                           keys=[policy_key(lab["keyId"], lab["spkiPem"], "Active",
                                            display="the lab signing key")]))
        apply(trust_policy("Active", name=second_policy, namespaces=[second],
                           keys=[policy_key(other["keyId"], other["spkiPem"], "Active",
                                            display=f"{OWNER}'s unrelated key",
                                            subject=f"{OWNER}-other@logweir.invalid")]))
        here = legacy_backup(f"{OWNER}-ns-a")["status"]["evidence"]["verification"]
        there_obj = legacy_backup(f"{OWNER}-ns-b", namespace=second)
        there = there_obj["status"]["evidence"]["verification"]
        evidence.append(artifact("trust/multi-ns-governed.json", here))
        evidence.append(artifact("trust/multi-ns-other.json", there))
        clauses = verdicts_differ_by_namespace(here, there, lab["keyId"])
        evidence.append(artifact("trust/multi-ns-clauses.json", {
            "clauses": clauses, "governedNamespace": NS, "otherNamespace": second,
            "policies": {first_policy: [lab["keyId"]], second_policy: [other["keyId"]]},
        }))
        check(
            "trust-two-namespaces-resolve-their-own-policies",
            "PLAT-19.1",
            all(clauses.values()),
            f"two explicit TrustPolicies — {first_policy} over {NS} carrying the lab signing "
            f"key, {second_policy} over {second} carrying a different key — resolve "
            f"independently: the SAME signing key's receipt verifies "
            f"{here.get('result')}/{(here.get('trust') or {}).get('basis')} in the governed "
            f"namespace and {there.get('result')} in the other, both naming "
            f"{str(here.get('matchedKeyId'))[:16]}…. `spec.namespaces` is an exact list, "
            f"never a pattern, which is how a new namespace is stopped from silently "
            f"inheriting a trust decision. "
            + "; ".join(f"{k}={v}" for k, v in clauses.items()),
            evidence,
        )
    finally:
        for path in (other["private"],):
            path.unlink(missing_ok=True)
        shutil.rmtree(other["dir"], ignore_errors=True)
        for name in (first_policy, second_policy):
            run(K + ["delete", "trustpolicy", name, "--ignore-not-found=true", "--wait=true"],
                check=False)
        run(K + ["delete", "namespace", second, "--ignore-not-found=true", "--wait=true"],
            check=False, timeout=300)
        left = [t["metadata"]["name"] for t in json.loads(
            run(K + ["get", "trustpolicies", "-o", "json"]).stdout)["items"]
            if t["metadata"]["name"].startswith(f"{OWNER}-{STAMP}-ns-")]
        evidence.append(artifact("trust/multi-ns-cleanup.json",
                                 {"policiesLeft": left,
                                  "secondNamespaceGone": run(
                                      K + ["get", "namespace", second, "-o", "name"],
                                      check=False, timeout=60).returncode != 0,
                                  "privateKeyGone": not other["private"].exists()}))
        check(
            "trust-multi-namespace-fixture-is-cleaned-up",
            "PLAT-19.1",
            not left and not other["private"].exists(),
            f"the two cluster-scoped policies and the second namespace this row created are "
            f"gone (policies left: {left}); the unrelated key's private half is deleted "
            f"({not other['private'].exists()}). A policy naming a namespace by exact name "
            f"would silently re-judge a later run's evidence",
            evidence,
        )


# ---------------------------------------------------------------------------
# PLAT-19.1's `unauthorized update`, as an RBAC result
# ---------------------------------------------------------------------------

# The roles the chart ships (`charts/logweir/templates/human-roles.yaml`).
# `logweir-viewer` reads `trustpolicies`; `logweir-operator` creates backups and
# patches schedules; only `logweir-trust-admin` may write a `trustpolicy`. A
# realistic non-admin holds the first two.
SHIPPED_NON_ADMIN_ROLES = ("logweir-viewer", "logweir-operator")
TRUST_ADMIN_ROLE = "logweir-trust-admin"


def rbac_refused_the_update(can_patch: str, can_read: str, refusal: str,
                            admin_can_patch: str) -> dict[str, bool]:
    """An edit refused by RBAC, and distinguishable from the other refusals.

    PLAT-19.1's `unauthorized update` is about AUTHORIZATION, and the refusal
    already proven — `trust-lifecycle-is-monotonic` — is the CRD's own CEL,
    which fires for a cluster-admin too. The two look nothing alike and a row
    that accepted either would prove neither, so this asserts the shape of the
    RBAC one: the API server's `is forbidden` naming the subject and the verb,
    with none of CEL's rule text in it.

    The two control clauses are what stop it passing for the wrong reason. A
    subject with no access at all would be refused for reading as well, and a
    cluster where NOBODY may write a `trustpolicy` would refuse the admin too —
    in either case the refusal would say nothing about this subject's authority.
    """
    return {
        "the non-admin may not patch a TrustPolicy": can_patch.strip() == "no",
        "the refusal is the API server's, naming the subject and the verb": (
            "is forbidden" in refusal
            and "cannot patch resource" in refusal
            and "trustpolicies" in refusal
        ),
        "and it is NOT the CRD's CEL refusal": (
            "never backwards" not in refusal and "Invalid value" not in refusal
        ),
        "the same subject MAY read one, so this is about the verb": can_read.strip() == "yes",
        "and the shipped trust-admin role MAY patch one": admin_can_patch.strip() == "yes",
    }


def trust_rbac() -> None:
    """A non-admin bound only to the shipped roles cannot edit trust.

    CLUSTER-SCOPED, so it runs under the cluster lock: `trustpolicies` are
    cluster-scoped and a ClusterRoleBinding is the only way to grant a subject
    the shipped roles as an installation really would. Both bindings are this
    run's own, named with the stamp, and deleted before the phase returns.
    """
    evidence: list[str] = []
    sa = f"{OWNER}-nonadmin"
    admin_sa = f"{OWNER}-trustadmin"
    subject = f"system:serviceaccount:{NS}:{sa}"
    admin_subject = f"system:serviceaccount:{NS}:{admin_sa}"
    for name in (sa, admin_sa):
        apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": owned(name)})
    created: list[str] = []
    for role in SHIPPED_NON_ADMIN_ROLES:
        binding = f"{OWNER}-{STAMP}-{role}"
        apply({
            "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
            "metadata": {"name": binding, "labels": dict(LABEL)},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                        "name": role},
            "subjects": [{"kind": "ServiceAccount", "name": sa, "namespace": NS}],
        })
        created.append(binding)
    admin_binding = f"{OWNER}-{STAMP}-{TRUST_ADMIN_ROLE}"
    apply({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": admin_binding, "labels": dict(LABEL)},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                    "name": TRUST_ADMIN_ROLE},
        "subjects": [{"kind": "ServiceAccount", "name": admin_sa, "namespace": NS}],
    })
    created.append(admin_binding)
    try:
        # A policy of this run's own to aim at, so nothing shared is even the
        # target of a refused write.
        if get_opt("trustpolicy", TRUST_POLICY, namespace="default") is None:
            apply(trust_policy("Active"))
        can_patch = run(K + ["auth", "can-i", "patch", "trustpolicies", "--as", subject],
                        check=False).stdout
        can_read = run(K + ["auth", "can-i", "get", "trustpolicies", "--as", subject],
                       check=False).stdout
        admin_can_patch = run(
            K + ["auth", "can-i", "patch", "trustpolicies", "--as", admin_subject],
            check=False).stdout
        attempt = run(
            K + ["patch", "trustpolicy", TRUST_POLICY, "--type=merge", "--as", subject,
                 "-p", json.dumps({"metadata": {"annotations": {
                     f"{OWNER}.logweir.dev/unauthorized-probe": "this write must be refused"}}})],
            check=False)
        refusal = redact((attempt.stdout + attempt.stderr).strip())
        evidence.append(artifact("trust/rbac-unauthorized-update.json", {
            "subject": subject, "boundTo": list(SHIPPED_NON_ADMIN_ROLES),
            "adminSubject": admin_subject, "adminBoundTo": TRUST_ADMIN_ROLE,
            "canPatch": can_patch.strip(), "canRead": can_read.strip(),
            "adminCanPatch": admin_can_patch.strip(),
            "exitCode": attempt.returncode, "refusal": refusal,
            "clusterRoleBindings": created,
        }))
        clauses = rbac_refused_the_update(can_patch, can_read, refusal, admin_can_patch)
        check(
            "trust-unauthorized-update-is-refused-by-rbac",
            "PLAT-19.1",
            all(clauses.values()) and attempt.returncode != 0,
            f"a ServiceAccount bound to the SHIPPED roles {list(SHIPPED_NON_ADMIN_ROLES)} and "
            f"nothing else cannot edit a TrustPolicy: `auth can-i patch` says "
            f"{can_patch.strip()!r}, the write exits {attempt.returncode} with "
            f"{refusal[:220]!r}. It MAY read one ({can_read.strip()!r}), and a subject bound "
            f"to {TRUST_ADMIN_ROLE} MAY patch one ({admin_can_patch.strip()!r}), so the "
            f"refusal is about this subject's authority and not about the cluster. This is "
            f"the AUTHORIZATION boundary: `trust-lifecycle-is-monotonic` proves the CRD's "
            f"CEL refusal, which fires for a cluster-admin too. "
            + "; ".join(f"{k}={v}" for k, v in clauses.items()),
            evidence,
        )
    finally:
        for binding in created:
            run(K + ["delete", "clusterrolebinding", binding, "--ignore-not-found=true",
                     "--wait=true"], check=False)
        left = [b["metadata"]["name"] for b in json.loads(
            run(K + ["get", "clusterrolebindings", "-o", "json"]).stdout)["items"]
            if b["metadata"]["name"].startswith(f"{OWNER}-{STAMP}-")]
        evidence.append(artifact("trust/rbac-cleanup.json",
                                 {"deleted": created, "remaining": left}))
        check(
            "trust-rbac-fixture-is-cleaned-up",
            "PLAT-19.1",
            not left,
            f"the {len(created)} ClusterRoleBindings this row created are deleted before it "
            f"returns; remaining: {left}. They are cluster-scoped, so leaving one behind "
            f"would hand a later run's ServiceAccount an authority nobody granted it",
            evidence,
        )


# ---------------------------------------------------------------------------
# PLAT-14.2 — a stale point alerts once, and a failed delivery rewrites nothing
# ---------------------------------------------------------------------------

SINK_POD = f"{OWNER}-sink"
SINK_URL_SECRET = f"{OWNER}-sink-url"
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
                     "labels": dict(LABEL, **{"app": SINK_POD})},
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


def sink_post_count(log: str) -> int:
    """POSTs in a raw `nc` capture — OCCURRENCES, not lines.

    `grep -c '^POST'` counted 1 forever. An HTTP request ends
    `\r\n\r\n<body>` with no trailing newline, so the NEXT request's `POST`
    is appended to the tail of the previous request's last line and never
    begins a line again: after the first delivery the counter froze at 1, and a
    row asking for "exactly one new POST" measured 0 however many arrived. It
    survived two runs because the only assertion over it was
    `posts_after == posts_before` while the insecure-sink hatch was shut and
    the true count really was zero. A counter that cannot go up is not a
    counter.
    """
    # THE FACT IS `POST `, NOT THE ROUTE. Keying on "POST /alerts" would zero
    # this counter again the day the sink's path changes, in the same silent way
    # the line-anchored version did.
    return log.count("POST ")


def sink_posts() -> int:
    out = run(KN + ["exec", SINK_POD, "--", "/bin/sh", "-c",
                    "cat /tmp/posts.log 2>/dev/null || true"], check=False).stdout
    return sink_post_count(out)


def protection_policy(name: str, *, max_age: int,
                      schedule: str = "keeps-running") -> dict[str, Any]:
    """The `ProtectionPolicy` shape D3 §3.1 documents, over one schedule.

    `schedule` is a parameter because two phases need DIFFERENT ones and the
    reason is the rows': a policy selects "points produced by these schedules"
    (§3.1), so two policies sharing one schedule share one candidate set, and
    `protection_cases` — which needs its incident to OPEN over an empty set
    before its own fresh point closes it — would instead find `notify`'s point
    already sitting inside its objective and never open an incident at all.
    Each phase's policy names a schedule only its own runs reference.
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "ProtectionPolicy",
        "metadata": owned(name),
        "spec": {
            "protects": {
                "sourceRef": {"name": "source"},
                "destinationRef": {"name": "dest-a"},
                "catalogRef": {"name": "primary"},
                "scheduleRefs": [{"name": schedule}],
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
                        "webhook": {"urlSecretRef": {"name": SINK_URL_SECRET, "key": "url"}},
                    }
                ],
            },
        },
    }


# The CRD's own floor for `maxRecoveryPointAgeSeconds` (`>= 300`), so the wait
# that ages the point past it is as short as the schema allows.
NOTIFY_MAX_AGE = 300
NOTIFY_POINT = "notify-point"


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
            "metadata": owned(SINK_URL_SECRET),
            "stringData": {"url": f"http://{sink_ip}:{SINK_PORT}/alerts"},
        }
    )
    # THE SCHEDULE THIS POLICY NAMES HAS TO EXIST, and this phase used to
    # inherit it from `retention` without declaring it. A `ProtectionPolicy`
    # whose `scheduleRefs` names nothing resolvable evaluates to
    # `health: Unknown` / `Protected=Unknown/ScheduleMissing` — no alert opens,
    # no transition is owed, and every row here then measures an evaluation
    # that never happened. Measured on 2026-09-21, running `notify` without
    # `retention`.
    if get_opt("backupschedule", "keeps-running") is None:
        apply(schedule_object("keeps-running", "dest-a"))
    run(KN + ["patch", "backupschedule", "keeps-running", "--type=merge",
              "-p", json.dumps({"spec": {"suspend": True}})])

    # A POINT THE POLICY CAN ACTUALLY SELECT, AND OLD ENOUGH TO BE STALE.
    #
    # `catalog` deletes every dest-a `Backup` CR to prove reconstruction, so
    # this namespace reaches `notify` with none — and the manual runs the other
    # phases make carry no `spec.scheduleRef`, so even where one survived it was
    # not a member of a policy naming `scheduleRefs` (see `backup_object`, and
    # `verdicts/probe-schedulerefs-membership.json`). Both together are why the
    # `Stale` arm of the row below had never been measured: the policy was
    # `Unprotected` over an EMPTY candidate set, and the alert it opened was
    # about nothing.
    #
    # The point is made here, as a manual run OF `keeps-running`, and the view
    # is refreshed so `requireCatalogAvailability` can answer for it. The wait
    # below then does double duty: three evaluation intervals for the dedup
    # claim, and past `maxRecoveryPointAgeSeconds` (the CRD's floor, 300 s) so
    # the newest available point is genuinely past the objective.
    if get_opt("backup", NOTIFY_POINT) is not None:
        run(KN + ["delete", "backup", NOTIFY_POINT, "--wait=true"])
    member = run_backup(NOTIFY_POINT, "dest-a", schedule=schedule_ref("keeps-running"))
    point_written = time.time()
    refresh_view("primary", f"notify-{int(point_written)}")
    evidence.append(artifact("notify/member-point.json",
                             {"backup": backup_facts(member),
                              "scheduleRef": member["spec"].get("scheduleRef"),
                              "pointFactsThePolicyNeeds":
                                  point_facts_the_policy_needs(member.get("status") or {})}))
    posts_before = sink_posts()
    backups_before = {b["metadata"]["name"]: b["metadata"]["resourceVersion"]
                      for b in lst("backups")}
    # HOW MANY TRANSITIONS WERE ALREADY NOTIFIED. A re-run against a policy
    # whose alert is already open opens no new transition and must therefore see
    # no new POST; only the transitions this window opens are owed one.
    existing = get_opt("protectionpolicy", "protect-a") or {}
    notified_before = sum(a.get("notifiedTransition") or 0
                          for a in ((existing.get("status") or {}).get("alerts") or []))
    apply(protection_policy("protect-a", max_age=NOTIFY_MAX_AGE))
    wait_for(
        "protectionpolicy",
        "protect-a",
        lambda o: bool((o.get("status", {}) or {}).get("health")),
        seconds=300,
        what="a health verdict",
    )
    # Three evaluation intervals' worth of wall clock, to prove the alert does
    # not re-fire: `renotifyAfterSeconds` is unset, so one transition is one
    # alert however many times the policy is reconciled. AND past the
    # objective, measured from the point's own capture rather than from here,
    # so the newest available point is older than `maxRecoveryPointAgeSeconds`
    # and `Stale` is reachable at all — bounded, because a wait with no bound
    # is how a worker hangs.
    stale_at = point_written + NOTIFY_MAX_AGE + 40
    time.sleep(max(200.0, min(stale_at - time.time(), 600.0)))
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
    selector = selector_matched_a_run(status)
    check(
        "notify-stale-point-alerts-exactly-once",
        "PLAT-14.2",
        len(alerts) == 1
        and len(transitions) == 1
        and delivery.get("attempts", 0) <= 3
        and status.get("health") == "Stale"
        and all(selector.values()),
        f"selector {selector} — `{NOTIFY_POINT}` is a manual run OF `keeps-running` "
        f"(`spec.scheduleRef.uid`, D3 §3.2's authority), written "
        f"{int(time.time() - point_written)}s before this read and past the objective of "
        f"{NOTIFY_MAX_AGE}s, and status.lastAttempt names "
        f"{((status.get('lastAttempt') or {}).get('backupRef') or {}).get('name')!r}: this "
        f"row is a measurement of a candidate set and not of an empty one. "
        f"health={status.get('health')}; over ~3 evaluation intervals the policy holds "
        f"{len(alerts)} open alert and every delivery Job belongs to ONE transition "
        f"({sorted(transitions)}, {len(jobs)} Job(s) = the bounded retry, "
        f"attempts={delivery.get('attempts')} of at most 3). One transition is one alert "
        f"however many times the policy is reconciled. Alert rows: "
        f"{[{k: a.get(k) for k in ('kind', 'state', 'delivery')} for a in alerts]}",
        evidence,
    )
    unchanged = {n: r for n, r in backups_before.items() if backups_after.get(n) == r}
    # THE HATCH DECIDES THE POST COUNT, AND THE ROW READS IT. This asserted
    # `posts_after == posts_before` — zero POSTs, because `logweir notify
    # deliver` refuses a non-https webhook before it dials. That is still the
    # DEFAULT, but it is a configuration and not an invariant: with
    # `LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS` set on the controller — the
    # documented escape hatch, which the lab now sets — the sink is dialled and
    # D3 L5's "an in-cluster echo sink records exactly 1 POST" becomes
    # observable. The row asserted the unconfigured half as if it were the whole
    # contract and failed the moment the hatch was opened (lab-refresh-3 §8.3).
    # It now asserts the delivery outcome the controller's own environment
    # implies, in both configurations, and keeps the clause that IS the
    # invariant: whatever a notification does, it never rewrites a Backup.
    facts = controller_facts()
    hatch = facts.get("allowInsecureSinks")
    hatch_open = hatch_is_open(hatch)
    posts = posts_after - posts_before
    delivered = delivery.get("state") == "Delivered"
    notified_after = sum(a.get("notifiedTransition") or 0 for a in alerts)
    new_transitions = max(0, notified_after - notified_before)
    want_posts = expected_posts(hatch_open, new_transitions)
    # THE ROW'S HONEST WEAK CASE, NAMED IN ITS OWN MESSAGE. On a re-used
    # namespace whose alert is already open this window owes no POST, so the
    # POST half of the row reduces to `0 == 0` and observes no delivery at all
    # (review L-6). The resourceVersion invariant and
    # `notify-stale-point-alerts-exactly-once` still hold, but a green row here
    # must not read as "a delivery was seen".
    nothing_to_deliver = (
        "" if new_transitions else
        " NOTHING WAS DELIVERED IN THIS WINDOW: the policy's alert was already open, so no "
        "transition and no POST were owed and none was observed. This run proves the "
        "resourceVersion invariant and nothing about delivery — run `notify` on a fresh "
        "namespace for a delivery observation."
    )
    evidence.append(artifact("notify/controller-facts.json",
                             dict(facts, newTransitions=new_transitions,
                                  deliveryObserved=bool(new_transitions))))
    check(
        "notify-delivery-never-rewrites-a-backup",
        "PLAT-14.2",
        notify_delivery_ok(hatch_open, posts, want_posts, delivered, len(unchanged),
                           len(backups_before)),
        f"LOGWEIR_NOTIFY_ALLOW_INSECURE_SINKS={hatch!r} on the controller, so a plaintext "
        f"sink is {'dialled' if hatch_open else 'refused before the dial'}; this window "
        f"opened {new_transitions} new notified transition(s) (notifiedTransition "
        f"{notified_before} -> {notified_after}), so {want_posts} POST(s) are owed: the sink "
        f"received {posts}, the alert records delivery {delivery.get('state')!r} after "
        f"{delivery.get('attempts')} attempt(s) — and every one of the "
        f"{len(backups_before)} Backups in this namespace still carries the resourceVersion "
        f"it had before the protection controller ran ({len(unchanged)} unchanged). "
        f"Formerly `notify-failure-never-rewrites-a-backup`, which required zero POSTs and "
        f"so encoded the hatch being shut as if it were the contract." + nothing_to_deliver,
        evidence,
    )


# ---------------------------------------------------------------------------
# PLAT-15.1 — a catalog with more real points than the view may hold
# ---------------------------------------------------------------------------

BUCKET_C = f"{OWNER}-{STAMP}-c"

# THE CRD'S OWN FLOOR, AND NOTHING HERE MOVES IT. `sync.viewLimit` is
# `100..5000` (D3 §5.3) and `maxObjectsPerRun` bottoms out at 1000, so the only
# honest way to see `status.truncated` is to put more than a hundred REAL
# signed points in an archive. Editing the floor to meet a smaller fixture
# would be a row about a CRD this build does not ship.
VIEW_LIMIT_FLOOR = 100
SCALE_POINTS = int(os.environ.get("LOGWEIR_D3_SCALE_POINTS", "104"))
SCALE_BATCH = int(os.environ.get("LOGWEIR_D3_SCALE_BATCH", "10"))
CLI_POD = f"{OWNER}-cli"


def run_backups(names: list[str], dest: str, *, batch: int = SCALE_BATCH) -> dict[str, str]:
    """N REAL Backups, `batch` of them in flight at a time.

    D2's `bulk_topics.py` is the shape: a fixture that makes the product do the
    expensive thing for real and then REPORTS WHAT IT ACTUALLY GOT, rather than
    asserting the number it asked for. A point in this archive is a run of the
    shipped runner against the lab's Kafka that wrote an archive, a DSSE-signed
    receipt and the catalog record that receipt implies — this harness knows no
    other way to make a signed point, and it does not invent one.
    """
    phases: dict[str, str] = {}
    for start in range(0, len(names), batch):
        group = names[start:start + batch]
        for name in group:
            if get_opt("backup", name) is None:
                create(backup_object(name, dest), check=False)
        for name in group:
            try:
                obj = wait_for("backup", name, terminal, seconds=900, what="a terminal phase")
                phases[name] = obj.get("status", {}).get("phase", "Unknown")
            except RuntimeError:
                phases[name] = "Timeout"
        log(f"bulk: {len(phases)}/{len(names)} terminal, "
            f"{sum(1 for v in phases.values() if v == 'Succeeded')} Succeeded")
    return phases


def cli_run(name: str, args: list[str], *, seconds: int = 600) -> tuple[int | None, str]:
    """`logweir` out of the SHIPPED runner image, run the way an operator runs
    it: its own read credential from a Secret, no API token, no service account
    token mounted, and the exit code and stdout recorded."""
    image = (STATE.get("controller") or {}).get("runnerImage") or controller_facts()["runnerImage"]
    if get_opt("pod", name) is not None:
        run(KN + ["delete", "pod", name, "--wait=true"])
    create(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": owned(name),
            "spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "cli",
                        "image": image,
                        "imagePullPolicy": "Never",
                        "command": ["logweir"],
                        "args": args,
                        "env": [
                            secret_env("AWS_ACCESS_KEY_ID", "logweir-s3", "access-key-id"),
                            secret_env("AWS_SECRET_ACCESS_KEY", "logweir-s3", "secret-access-key"),
                            {"name": "AWS_REGION", "value": "us-east-1"},
                        ],
                    }
                ],
            },
        }
    )
    pod = wait_for(
        "pod", name,
        lambda o: o.get("status", {}).get("phase") in {"Succeeded", "Failed"},
        seconds=seconds, what="a terminal phase",
    )
    logs = redact(run(KN + ["logs", name], check=False).stdout)
    state = ((pod["status"].get("containerStatuses") or [{}])[0].get("state") or {})
    code = (state.get("terminated") or {}).get("exitCode")
    return code, logs


def parse_list(logs: str) -> dict[str, Any]:
    """`logweir catalog list`'s printed page, as the CLI prints it.

    The counts it always prints — `catalog-unsupported-format`,
    `catalog-unreadable`, `catalog-inconsistent` — are read too, because a page
    that dropped rows it could not read without saying so would let a short
    listing read as "these are all the points" (`catalog/cli.rs`'s `print_list`).
    """
    rows = [line.split()[0] for line in logs.splitlines() if line.startswith("lwp1-")]
    fields: dict[str, str] = {}
    for line in logs.splitlines():
        if line.startswith("catalog-") and "=" in line:
            key, _, value = line.partition("=")
            fields[key] = value
    return {
        "rows": rows,
        "listed": int(fields.get("catalog-listed", -1)),
        "truncated": fields.get("catalog-truncated") == "true",
        "unsupportedFormat": int(fields.get("catalog-unsupported-format", -1)),
        "unreadable": int(fields.get("catalog-unreadable", -1)),
        "inconsistent": int(fields.get("catalog-inconsistent", -1)),
        "searchedDays": int(fields.get("catalog-searched-days", -1)),
    }


def cli_list(bucket: str, *, name: str, max_rows: int, since: str | None = None) -> dict[str, Any]:
    args = [
        "catalog", "list", "--url", f"s3://{bucket}", "--endpoint", MINIO_ENDPOINT,
        "--region", "us-east-1", "--path-style", "--allow-http", "--max", str(max_rows),
    ]
    if since:
        args += ["--since", since]
    code, logs = cli_run(name, args)
    page = parse_list(logs)
    page["exitCode"] = code
    page["evidence"] = artifact(f"scale/{name}.txt", logs)
    return page


def view_truncates_honestly(records: int, counts: dict[str, Any], truncated: Any,
                            entries: int, pages: int, cursor: dict[str, Any],
                            newest_in_archive: list[str],
                            listed_ids: list[str]) -> dict[str, bool]:
    """D3 §5.3's three honest signals: the FLAG, the COUNT and the CURSOR.

    Each clause can fail on its own, and the one that matters most is the
    count: a view that silently listed `viewLimit` points and reported
    `total: viewLimit` would look exactly like this one from Kubernetes and
    would have lost a hundred recovery points.
    """
    return {
        "the archive holds more real signed points than the view may list":
            records > VIEW_LIMIT_FLOOR,
        "the flag says so": truncated is True,
        "the count is the archive's, not the page's": counts.get("total") == records,
        "the view materialises exactly `viewLimit` entries": entries == VIEW_LIMIT_FLOOR,
        "in at most the 8 page ConfigMaps the CRD allows": 1 <= pages <= 8,
        "a cursor is published": bool(cursor),
        "and the entries are the NEWEST points, not an arbitrary hundred":
            sorted(listed_ids) == sorted(newest_in_archive),
    }


def catalog_scale() -> None:
    """More REAL points than `viewLimit`, and what each surface then says."""
    evidence: list[str] = []
    if BUCKET_C not in (STATE.get("buckets") or []):
        mc("mb", "--ignore-existing", f"local/{BUCKET_C}")
        STATE.setdefault("buckets", []).append(BUCKET_C)
        save()
    if get_opt("backupdestination", "dest-c") is None:
        apply(destination("dest-c", BUCKET_C))
        wait_for("backupdestination", "dest-c",
                 lambda o: condition(o, "Valid").get("status") == "True",
                 seconds=180, what="Valid=True")
    names = [f"bulk-{i:03d}" for i in range(SCALE_POINTS)]
    phases = run_backups(names, "dest-c")
    evidence.append(artifact("scale/backup-phases.json", phases))

    # THE NUMBER THIS ROW USES IS THE ARCHIVE'S, NOT THE ONE IT ASKED FOR.
    # `bulk_topics.py`'s lesson: a fixture that asserts its own request has
    # measured nothing. Every record.json here is a signed point the runner
    # wrote.
    record_keys = [o["key"] for o in objects(BUCKET_C, f"{CATALOG_PREFIX}/points/")
                   if o["key"].endswith("record.json")]
    sig_keys = [o["key"] for o in objects(BUCKET_C, f"{CATALOG_PREFIX}/points/")
                if o["key"].endswith("record.sig")]
    index_keys = sorted(index_entries(BUCKET_C))
    archive_ids = [k.rsplit("/", 1)[-1].removesuffix(".json").split("-", 1)[1] for k in index_keys]
    newest = archive_ids[-VIEW_LIMIT_FLOOR:]
    evidence.append(artifact("scale/archive-index.json",
                             {"records": len(record_keys), "sidecars": len(sig_keys),
                              "indexEntries": len(index_keys),
                              "succeeded": sum(1 for v in phases.values() if v == "Succeeded")}))

    view = fresh_catalog("scale", "dest-c", seconds=900,
                         viewLimit=VIEW_LIMIT_FLOOR, maxObjectsPerRun=100000)
    entries = view_entries(view)
    status = view["status"]
    counts = status.get("counts") or {}
    evidence.append(artifact("scale/view-status.json", catalog_summary(view)))
    evidence.append(artifact("scale/view-entries.json", entries))
    clauses = view_truncates_honestly(
        len(record_keys), counts, status.get("truncated"), len(entries),
        len(status.get("pages") or []), status.get("cursor") or {},
        newest, [e["pointId"] for e in entries],
    )
    check(
        "catalog-large-view-truncates-honestly",
        "PLAT-15.1",
        all(clauses.values()),
        f"{len(record_keys)} REAL signed point records (and {len(sig_keys)} sidecars) written "
        f"by {sum(1 for v in phases.values() if v == 'Succeeded')} Succeeded Backups against "
        f"the lab's Kafka, read by a catalog asking for the CRD's smallest view "
        f"(viewLimit={VIEW_LIMIT_FLOOR}, untouched in the CRD): truncated="
        f"{status.get('truncated')}, counts={counts}, entries={len(entries)}, pages="
        f"{len(status.get('pages') or [])}, cursor={status.get('cursor')}. Clauses {clauses}",
        evidence,
    )
    STATE["scaleRecords"] = len(record_keys)
    STATE["scaleOmitted"] = sorted(set(archive_ids) - {e["pointId"] for e in entries})
    save()

    # --- and what the operator CLI can still reach --------------------------
    full = cli_list(BUCKET_C, name=f"{OWNER}-cli-full", max_rows=SCALE_POINTS + 50)
    page = cli_list(BUCKET_C, name=f"{OWNER}-cli-page", max_rows=50)
    newest_key = index_keys[-1] if index_keys else ""
    after = cli_list(BUCKET_C, name=f"{OWNER}-cli-after", max_rows=50, since=newest_key)
    evidence += [full["evidence"], page["evidence"], after["evidence"]]
    omitted = STATE["scaleOmitted"]
    cli_clauses = {
        "the CLI reaches every point in the archive, including the ones the view omits":
            full["listed"] == len(record_keys) and set(omitted) <= set(full["rows"]),
        "a bounded page returns exactly what was asked for": page["listed"] == 50,
        "and says it is a page": page["truncated"] is True,
        "the full listing does not claim to be truncated": full["truncated"] is False,
        "the cursor means `what arrived after this`, and nothing has":
            after["listed"] == 0 and after["exitCode"] == 0,
        "no row was dropped unreported": full["unreadable"] == 0 and full["inconsistent"] == 0,
    }
    check(
        "catalog-list-reaches-past-the-view",
        "PLAT-15.1",
        all(cli_clauses.values()),
        f"`logweir catalog list` out of the shipped runner image lists {full['listed']} points "
        f"where the Kubernetes view materialised {len(entries)}: the {len(omitted)} point(s) "
        f"beyond `viewLimit` ({omitted}) are reachable there and nowhere in the view. "
        f"--max 50 returns {page['listed']} rows with catalog-truncated={page['truncated']}; "
        f"--since <newest index key> returns {after['listed']} rows (exit {after['exitCode']}), "
        f"which is the advertised cursor — a windowed query over OLDER points is an absent "
        f"capability (D3 §5.3) and this row does not pretend otherwise. Clauses {cli_clauses}",
        evidence,
    )


# ---------------------------------------------------------------------------
# PLAT-15.1 — partial access, a corrupt manifest, and a record from the future
# ---------------------------------------------------------------------------

BUCKET_D = f"{OWNER}-{STAMP}-d"
ACCESS_POINTS = ["acc-1", "acc-2", "acc-3", "acc-4"]
SCOPED_USER = f"{OWNER_TAG}scoped"
SCOPED_POLICY = f"{OWNER_TAG}-scoped"
SCOPED_SECRET = f"{OWNER}-scoped-s3"


def scoped_policy_document(bucket: str, deny_keys: list[str]) -> str:
    """A credential that may read this archive EXCEPT these objects.

    Key-scoped, not bucket-scoped: the row it exists for is "one prefix and not
    another", and a credential denied the whole bucket would make every entry
    unreadable and prove only that a broken credential breaks everything.
    """
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
                 "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]},
                {"Effect": "Deny", "Action": ["s3:GetObject"],
                 "Resource": [f"arn:aws:s3:::{bucket}/{key}" for key in deny_keys]},
            ],
        }
    )


def partial_access_ok(entries: dict[str, dict[str, Any]], counts: dict[str, Any],
                      denied: list[str], readable: list[str]) -> dict[str, bool]:
    """403 is "could not tell", and it is NEVER "your backup is gone".

    The clause that carries the row is the last one: `Missing` is a definite
    `NotFound` (D3 §5.4), and a credential that cannot read an object has
    established nothing about whether the object is there.
    """
    return {
        "the points whose objects the credential may not read are Unreadable":
            bool(denied) and all(entries.get(p, {}).get("availability") == "Unreadable"
                                 for p in denied),
        "and are not selectable":
            all(entries.get(p, {}).get("selectable") is False for p in denied),
        "the points it may read are Available":
            bool(readable) and all(entries.get(p, {}).get("availability") == "Available"
                                   for p in readable),
        "the count of unreadable points is exact":
            counts.get("unreadable") == len(denied),
        "and nothing was called Missing": counts.get("missing", 0) == 0,
    }


def corrupt_is_not_missing(corrupt: dict[str, Any], missing: dict[str, Any],
                           counts: dict[str, Any]) -> dict[str, bool]:
    """Three different answers about one archive, in one view."""
    return {
        "the point whose manifest was DELETED is Missing":
            missing.get("availability") == "Missing",
        "the point whose manifest was CORRUPTED is not":
            corrupt.get("availability") not in {"Missing", None},
        "it is not Available either":
            corrupt.get("availability") != "Available",
        "and it is not offered": corrupt.get("selectable") is False,
        "each state is counted once":
            counts.get("missing") == 1
            and counts.get("conflict", 0) + counts.get("unreadable", 0) == 1,
        "and the corrupt point carries a remedy sentence": bool(corrupt.get("remedy")),
    }


def unsupported_format_ok(counts: dict[str, Any], listed: dict[str, dict[str, Any]],
                          planted: str, pages: int, others: int) -> dict[str, bool]:
    return {
        "the record from a future major is counted": counts.get("unsupportedFormat") == 1,
        "it is never offered — it is not in the view at all": planted not in listed,
        "the walk still published a view": pages >= 1,
        "and every other point is still listed": others == len(ACCESS_POINTS),
    }


def catalog_access() -> None:
    evidence: list[str] = []
    if BUCKET_D not in (STATE.get("buckets") or []):
        mc("mb", "--ignore-existing", f"local/{BUCKET_D}")
        STATE.setdefault("buckets", []).append(BUCKET_D)
        save()
    if get_opt("backupdestination", "dest-d") is None:
        apply(destination("dest-d", BUCKET_D))
        wait_for("backupdestination", "dest-d",
                 lambda o: condition(o, "Valid").get("status") == "True",
                 seconds=180, what="Valid=True")
    phases = run_backups(ACCESS_POINTS, "dest-d", batch=4)
    if sorted(n for n, p in phases.items() if p == "Succeeded") != sorted(ACCESS_POINTS):
        raise RuntimeError(f"the access fixture needs four real points: {phases}")
    base = fresh_catalog("case-access", "dest-d")
    base_entries = sorted(view_entries(base), key=lambda e: e["recoveryPointAtMs"])
    if len(base_entries) != len(ACCESS_POINTS):
        raise RuntimeError(f"dest-d holds {len(base_entries)} points, not {len(ACCESS_POINTS)}")
    evidence.append(artifact("access/baseline-entries.json", base_entries))
    p1, p2, p3, p4 = (e["pointId"] for e in base_entries)

    # --- a credential that may read one object and not another --------------
    #
    # The keys come from the RECORD in the bucket and not from the view. That
    # WAS because the view's `receiptKey` came back `[redacted].receipt.json`
    # and a deny built from a redacted path would deny nothing, so the row
    # would pass on an unrestricted credential. It was never "by design": it
    # was CATALOG-RECEIPTKEY-REDACTED, and the view now publishes the whole key
    # (`catalog-reconstruction-after-cr-loss` asserts it). The record stays the
    # source here anyway — it is the right source for a deny policy either way,
    # and it does not depend on the view being reachable.
    denied_receipt = record_of(BUCKET_D, p1)["receipt"]["key"]
    denied_manifest = record_of(BUCKET_D, p2)["archive"]["manifest_key"]
    document = scoped_policy_document(BUCKET_D, [denied_receipt, denied_manifest])
    scoped_secret = mint()
    run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
              f"printf '%s' '{document}' > /tmp/scoped.json && "
              f"mc admin policy create adm {SCOPED_POLICY} /tmp/scoped.json >/dev/null 2>&1; "
              f"mc admin user add adm {SCOPED_USER} {scoped_secret} >/dev/null 2>&1; "
              f"mc admin policy attach adm {SCOPED_POLICY} --user {SCOPED_USER} "
              ">/dev/null 2>&1; echo done"],
        check=False, timeout=120)
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": owned(SCOPED_SECRET),
            "stringData": {"access-key-id": SCOPED_USER, "secret-access-key": scoped_secret},
        }
    )
    apply(destination("dest-ro", BUCKET_D, write_secret=SCOPED_SECRET))
    wait_for("backupdestination", "dest-ro",
             lambda o: condition(o, "Valid").get("status") == "True",
             seconds=180, what="Valid=True")
    try:
        partial = fresh_catalog("case-partial", "dest-ro")
        partial_entries = {e["pointId"]: e for e in view_entries(partial)}
        counts = partial["status"].get("counts") or {}
        evidence.append(artifact("access/partial-status.json", catalog_summary(partial)))
        evidence.append(artifact("access/partial-entries.json", list(partial_entries.values())))
        clauses = partial_access_ok(partial_entries, counts, [p1, p2], [p3, p4])
        check(
            "catalog-partial-access-is-unreadable-not-missing",
            "PLAT-15.1",
            all(clauses.values()),
            f"a SECOND destination over the same archive with a key-scoped MinIO credential — "
            f"allowed to list and read the bucket, denied `s3:GetObject` on exactly one "
            f"point's receipt and one point's manifest — syncs to "
            f"{ {k: v['availability'] for k, v in partial_entries.items()} } with counts "
            f"{counts} and Synced="
            f"{condition(partial, 'Synced').get('status')}/"
            f"{condition(partial, 'Synced').get('reason')}. The denied points' remedy reads "
            f"{[partial_entries.get(p, {}).get('remedy') for p in (p1, p2)]}. A 403 is "
            f"'could not tell'; `Missing` is a definite NotFound and NOTHING here claimed "
            f"one. Clauses {clauses}",
            evidence,
        )
    finally:
        run(KN + ["exec", MC_POD, "--", "/bin/sh", "-c",
                  f"mc admin user remove adm {SCOPED_USER} >/dev/null 2>&1; "
                  f"mc admin policy rm adm {SCOPED_POLICY} >/dev/null 2>&1; echo done"],
            check=False, timeout=120)

    # --- a deleted manifest, a corrupted one, and a record from the future ---
    deleted_manifest = base_entries[2]["manifestKey"]
    corrupt_manifest = base_entries[3]["manifestKey"]
    rm(BUCKET_D, deleted_manifest)
    # NOT a digest edit: the bytes are replaced with something that is not a
    # manifest at all, which is what a truncated or half-overwritten object in
    # a bucket looks like.
    put(BUCKET_D, corrupt_manifest, b'{"this is not a manifest": true, "truncat')
    planted = plant_future_major(
        BUCKET_D, p1, "lwp1-" + hashlib.sha256(f"{OWNER}{STAMP}future".encode()).hexdigest()[:32])
    planted_id = planted["pointId"]
    evidence.append(artifact("access/planted-future-record.json", planted))

    after = fresh_catalog("case-corrupt", "dest-d")
    listed = {e["pointId"]: e for e in view_entries(after)}
    acounts = after["status"].get("counts") or {}
    evidence.append(artifact("access/corrupt-status.json", catalog_summary(after)))
    evidence.append(artifact("access/corrupt-entries.json", list(listed.values())))
    corrupt_clauses = corrupt_is_not_missing(listed.get(p4, {}), listed.get(p3, {}), acounts)
    check(
        "catalog-corrupt-manifest-is-not-missing",
        "PLAT-15.1",
        all(corrupt_clauses.values()),
        f"one view, three answers about three points of one archive: the manifest DELETED is "
        f"{listed.get(p3, {}).get('availability')}, the manifest OVERWRITTEN with bytes that "
        f"are not a manifest is {listed.get(p4, {}).get('availability')} "
        f"(remedy {listed.get(p4, {}).get('remedy')!r}), and the untouched points are "
        f"{sorted({listed.get(p, {}).get('availability') for p in (p1, p2)})}. counts "
        f"{acounts}. TWO DISTINCT FINDINGS, NOT ONE: `Missing` is reserved for a definite "
        f"NotFound, and bytes that do not hash to the digest the signed receipt names are a "
        f"CONTRADICTION about the point, which D3 §5.4 calls `Conflict` and "
        f"`check/kinds/catalog_sync.rs:1264-1270` argues for in its own comment. D3 §5.4's "
        f"'could not tell' gloss belongs to `Unreadable` — a read that FAILED — and that is "
        f"proven separately by `catalog-partial-access-is-unreadable-not-missing`, whose 403 "
        f"carries the remedy sentence 'this is could not tell, not is not there'. Clauses "
        f"{corrupt_clauses}",
        evidence,
    )
    format_clauses = unsupported_format_ok(
        acounts, listed, planted_id, len(after["status"].get("pages") or []),
        len([p for p in (p1, p2, p3, p4) if p in listed]),
    )
    check(
        "catalog-unsupported-format-is-counted-and-never-offered",
        "PLAT-15.1",
        all(format_clauses.values()),
        f"a record declaring `format_version: 2.0.0` under a well-formed point id, with a "
        f"self-consistent major-1 index entry pointing at it ({planted}), is counted "
        f"(counts.unsupportedFormat={acounts.get('unsupportedFormat')}), is absent from the "
        f"view entirely ({planted_id not in listed}) and does not stop the walk: "
        f"{len(after['status'].get('pages') or [])} page(s) published and "
        f"{len([p for p in (p1, p2, p3, p4) if p in listed])} of the four real points still "
        f"listed. THE 2026-09-18 RECORD SAID THIS NEEDED THE INSTALLATION'S PRIVATE KEY. It "
        f"does not, and the reason is in the code: `examine` classifies from the record's own "
        f"bytes and returns BEFORE it fetches the receipt or the sidecar "
        f"(`crates/logweir/src/check/kinds/catalog_sync.rs:1191`, "
        f"`crates/logweir/src/catalog/reader.rs:54`), so no signature is consulted for a "
        f"major this build does not implement. The earlier probe wrote `formatVersion` "
        f"(camelCase) into a document whose field is `format_version`, which major 1 ignores "
        f"as an unknown field — it measured an unsigned edit, not a future major. SO THE "
        f"TRACKER'S 'validly signed AND of a future major' IS UNREACHABLE BY CONSTRUCTION, "
        f"not merely unbuilt: no signature is consulted for a future major on either side — "
        f"this planting writes no `record.sig` at all and the walk would not read one. A "
        f"future build that read the sidecar BEFORE classifying the format would silently "
        f"change what this row means, which is the one thing to watch. Clauses "
        f"{format_clauses}",
        evidence,
    )


# ---------------------------------------------------------------------------
# PLAT-14.2 — the recovery notification, the unavailable archive, and the word
# that is never `complete`
# ---------------------------------------------------------------------------

RECOVERY_POLICY = "protect-recovery"
RECOVERY_MAX_AGE = 600
# THIS PHASE'S OWN SCHEDULE, and not `notify`'s. A policy selects "points
# produced by these schedules" (D3 §3.1), so sharing `keeps-running` would mean
# sharing `notify`'s member point — which is minutes old and therefore INSIDE
# this policy's 600 s objective, so the incident this phase needs to open
# before its own fresh point closes it would never open at all. Suspended at
# birth like every schedule here; nothing fires from it.
RECOVERY_SCHEDULE = "recovery-runs"


def alert_of(alerts: list[dict[str, Any]], kind: str) -> dict[str, Any] | None:
    for alert in alerts or []:
        if alert.get("kind") == kind:
            return alert
    return None


def notified_total(alerts: list[dict[str, Any]]) -> int:
    return sum(a.get("notifiedTransition") or 0 for a in alerts or [])


def policy_alerts(name: str) -> list[dict[str, Any]]:
    return ((get_opt("protectionpolicy", name) or {}).get("status") or {}).get("alerts") or []


def policy_events(name: str) -> list[dict[str, Any]]:
    """Every event document the controller wrote for this policy.

    The ConfigMaps are `<policy>-ev-<sha8>` and immutable, so this reads what
    was actually delivered rather than re-deriving what should have been.
    """
    events = []
    for cm in lst("configmaps"):
        if not cm["metadata"]["name"].startswith(f"{name}-ev-"):
            continue
        body = (cm.get("data") or {}).get("event.json")
        if body:
            events.append(json.loads(body))
    return events


def resolve_delivered_once(before: dict[str, Any] | None, after: dict[str, Any] | None,
                           posts: int, new_transitions: int,
                           point_before: str | None, point_after: str | None) -> dict[str, bool]:
    """A FRESH POINT closed the alert, and the close was delivered once.

    The point clause is not decoration: an alert that resolved because the
    objective was widened, or because an old point was re-read, would satisfy
    every other clause here and would prove nothing about recovery.
    """
    return {
        "the staleness alert was open before": (before or {}).get("state") == "Open",
        "a NEW recovery point reached the catalog view":
            bool(point_after) and point_after != point_before,
        "the alert is Resolved after it": (after or {}).get("state") == "Resolved",
        "which is exactly one new notified transition":
            (after or {}).get("notifiedTransition", 0)
            == (before or {}).get("notifiedTransition", 0) + 1,
        "one POST per transition this window opened, and no more":
            posts == new_transitions and new_transitions == 1,
        "and the delivery says Delivered, not merely attempted":
            ((after or {}).get("delivery") or {}).get("state") == "Delivered",
    }


def archive_unavailable_opened(entry_before: dict[str, Any], entry_after: dict[str, Any],
                               before: list[dict[str, Any]], after: list[dict[str, Any]],
                               posts: int, new_transitions: int) -> dict[str, bool]:
    opened = alert_of(after, "ArchiveUnavailable")
    return {
        "the newest point was Available and selectable in the view":
            entry_before.get("availability") == "Available"
            and entry_before.get("selectable") is True,
        "breaking its archive flipped that entry":
            entry_after.get("availability") in {"Missing", "Unreadable", "Deleted", "Conflict"},
        "no ArchiveUnavailable alert was open before":
            alert_of(before, "ArchiveUnavailable") is None,
        "exactly one is open after": (opened or {}).get("state") == "Open",
        "one POST per transition this window opened":
            posts == new_transitions and new_transitions >= 1,
        "and the open transition was delivered":
            ((opened or {}).get("delivery") or {}).get("state") == "Delivered",
    }


def scope_is_never_complete(events: list[dict[str, Any]]) -> dict[str, bool]:
    scopes = {e.get("verification_scope") for e in events}
    bodies = json.dumps(events)
    return {
        "every event labels its verification scope":
            bool(events) and all(e.get("verification_scope") for e in events),
        "with one of the three honest words": scopes <= {"sampled", "degraded", "none"},
        "and never `complete`": "complete" not in scopes,
        "and no body claims an exhaustive comparison":
            not re.search(r"complete(ly)?\s+verif|exhaustive|byte-for-byte\s+complete",
                          bodies, re.I),
    }


def refresh_view(name: str, token: str) -> dict[str, Any]:
    """The view, as it is NOW.

    `sync_now` is tried first because a `syncRequest` bump is what the product
    documents; `fresh_catalog` is the fallback the 2026-09-18 run had to use for
    every refresh (CATALOG-RESYNC-NOT-HARVESTED). Which one answered is
    recorded, because that difference is a product fact and not a harness
    detail.
    """
    try:
        view = sync_now(name, token, seconds=420)
        STATE.setdefault("viewRefresh", {})[token] = "syncRequest"
    except RuntimeError:
        view = fresh_catalog(name, "dest-a")
        STATE.setdefault("viewRefresh", {})[token] = "recreated (syncRequest was not harvested)"
    save()
    return view


def settle(kind: str, name: str, predicate: Callable[[dict[str, Any]], bool], *,
           seconds: int, what: str) -> dict[str, Any] | None:
    """`wait_for`, for a condition the product may honestly never reach.

    A row that ends in a timeout EXCEPTION records nothing, and "the harness
    crashed" is not a verdict anybody can act on. Where the thing waited for is
    the thing under test, the wait returns `None` and the row writes the FAIL
    with the objects in it.
    """
    try:
        return wait_for(kind, name, predicate, seconds=seconds, what=what)
    except RuntimeError:
        log(f"NOT REACHED in {seconds}s: {kind}/{name} {what}")
        return None


def point_facts_the_policy_needs(status: dict[str, Any]) -> dict[str, bool]:
    """The three facts a `ProtectionPolicy` reads off a `Backup` to place its
    point in TIME and in the CATALOG.

    `controllers/protection_policy.rs:855-882` builds each `PointCandidate`
    from `status.capture.startedAt` (D3 §3.2's `recoveryPointAt`) and from
    `status.evidence.receiptSha256`, whose first 128 bits ARE the point id
    (`protection.rs:641`). Both come from the backup receipt, which the
    controller reads only when the destination's `evidenceRead` grant is one it
    can use itself.
    """
    evidence = status.get("evidence") or {}
    verification = evidence.get("verification") or {}
    result = verification.get("result")
    return {
        "capture.startedAt — D3 §3.2's recoveryPointAt":
            bool((status.get("capture") or {}).get("startedAt")),
        "evidence.receiptSha256 — the point id is its first 128 bits":
            bool(evidence.get("receiptSha256")),
        # WRITTEN, AND NOT THE SAME QUESTION AS SATISFIED. The verdict block IS
        # published on this path — `NotAttempted` with a sentence naming the
        # grant, which is what D2-EVIDENCE-NOTATTEMPTED-UNWRITTEN's fix landed
        # for (`controllers/backup.rs:4050`). Reading its mere presence as the
        # objective being met is the error this pair of clauses exists to stop.
        "evidence.verification.result is written at all": bool(result),
        "…and it satisfies requireVerifiedEvidence (Valid/ValidHistorical)":
            result in {"Valid", "ValidHistorical"},
    }


def protection_cases() -> None:
    evidence: list[str] = []
    if get_opt("pod", SINK_POD) is None:
        apply(sink_pod())
        run(KN + ["wait", "--for=condition=Ready", f"pod/{SINK_POD}", "--timeout=120s"])
    sink_ip = get("pod", SINK_POD)["status"]["podIP"]
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned(SINK_URL_SECRET),
           "stringData": {"url": f"http://{sink_ip}:{SINK_PORT}/alerts"}})
    for schedule in ("keeps-running", RECOVERY_SCHEDULE):
        if get_opt("backupschedule", schedule) is None:
            apply(schedule_object(schedule, "dest-a"))
        run(KN + ["patch", "backupschedule", schedule, "--type=merge",
                  "-p", json.dumps({"spec": {"suspend": True}})])
    if get_opt("protectionpolicy", RECOVERY_POLICY) is not None:
        run(KN + ["delete", "protectionpolicy", RECOVERY_POLICY, "--wait=true"])

    # --- a policy over an archive whose newest point is old -----------------
    view = refresh_view("primary", f"protect-1-{int(time.time())}")
    before_entries = {e["pointId"]: e for e in view_entries(view)}
    newest_before = max(before_entries.values(), key=lambda e: e["recoveryPointAtMs"],
                        default={})
    apply(protection_policy(RECOVERY_POLICY, max_age=RECOVERY_MAX_AGE,
                            schedule=RECOVERY_SCHEDULE))
    opened = settle(
        "protectionpolicy", RECOVERY_POLICY,
        lambda o: alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness") is not None,
        seconds=420, what="a Staleness alert",
    )
    if opened is None:
        raise RuntimeError("no Staleness alert opened at all; the rows below measure nothing")
    settle(
        "protectionpolicy", RECOVERY_POLICY,
        lambda o: (((alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness") or {})
                    .get("delivery") or {}).get("state") in {"Delivered", "Failed",
                                                             "Suppressed"}),
        seconds=300, what="the open transition to finish delivering",
    )
    stale = get("protectionpolicy", RECOVERY_POLICY)
    alerts_before = (stale.get("status") or {}).get("alerts") or []
    evidence.append(artifact("protect/policy-stale.json", stale))

    # --- (a) a fresh recovery point, and ONE delivery -----------------------
    posts_mark = sink_posts()
    notified_mark = notified_total(alerts_before)
    # A RE-RUN'S POINT HAS TO BE FRESH TOO. Reusing the previous attempt's
    # Backup would date the "fresh" point to the previous attempt and measure a
    # resolve that a point already in the view is supposed to have caused.
    if get_opt("backup", "recovery-point") is not None:
        run(KN + ["delete", "backup", "recovery-point", "--wait=true"])
    # A MANUAL RUN OF THIS POLICY'S SCHEDULE. Without `spec.scheduleRef` the
    # point is not a member (`identity::is_run_of_schedule`), the policy's
    # candidate set stays empty, and the resolve this row measures cannot
    # happen on ANY build — the row would then fail at the batch refresh for a
    # harness reason that looks exactly like the product defect it exists to
    # confirm.
    fresh = run_backup("recovery-point", "dest-a",
                       schedule=schedule_ref(RECOVERY_SCHEDULE))
    fresh_status = fresh.get("status") or {}
    fresh_view = refresh_view("primary", f"protect-2-{int(time.time())}")
    after_entries = {e["pointId"]: e for e in view_entries(fresh_view)}
    new_ids = sorted(set(after_entries) - set(before_entries))
    newest_after = max(after_entries.values(), key=lambda e: e["recoveryPointAtMs"], default={})
    resolved = settle(
        "protectionpolicy", RECOVERY_POLICY,
        lambda o: (alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness") or {})
        .get("state") == "Resolved",
        seconds=420, what="the Staleness alert to resolve",
    )
    if resolved is not None:
        settle(
            "protectionpolicy", RECOVERY_POLICY,
            lambda o: (((alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness")
                         or {}).get("delivery") or {}).get("state") in {"Delivered", "Failed"}),
            seconds=300, what="the resolve transition to finish delivering",
        )
    policy_now = get("protectionpolicy", RECOVERY_POLICY)
    alerts_after = (policy_now.get("status") or {}).get("alerts") or []
    posts = sink_posts() - posts_mark
    new_transitions = notified_total(alerts_after) - notified_mark
    # WHY THE POLICY CANNOT SEE THE POINT THE CATALOG CAN, if it cannot.
    missing_facts = point_facts_the_policy_needs(fresh_status)
    evidence.append(artifact("protect/policy-after-fresh-point.json", policy_now))
    evidence.append(artifact(
        "protect/fresh-point.json",
        {"backupStatusKeys": sorted(fresh_status.keys()),
         "pointFactsThePolicyNeeds": missing_facts,
         "backup": backup_facts(fresh),
         "newPointIdsInTheView": new_ids,
         "theViewsVerdictOnTheFreshPoint": after_entries.get(newest_after.get("pointId", "")),
         "newestBefore": newest_before.get("pointId"),
         "newestAfter": newest_after.get("pointId"),
         "viewRefresh": STATE.get("viewRefresh")}))
    clauses = resolve_delivered_once(
        alert_of(alerts_before, "Staleness"), alert_of(alerts_after, "Staleness"),
        posts, new_transitions, newest_before.get("pointId"), newest_after.get("pointId"),
    )
    # AND THE POLICY SELECTED IT. Everything above is about what the policy
    # DECIDED; this is whether it had anything to decide about.
    clauses["the policy selected the point this row created"] = (
        (((policy_now.get("status") or {}).get("lastAttempt") or {}).get("backupRef") or {})
        .get("name") == "recovery-point")
    blind = [name for name, present in missing_facts.items() if not present]
    verdict_written = missing_facts["evidence.verification.result is written at all"]
    entry_now = after_entries.get(newest_after.get("pointId", ""), {})
    diagnosis = (
        "" if all(clauses.values()) else
        f" WHY, as far as this row can see. The fresh Backup Succeeded (exit "
        f"{fresh_status.get('exitCode')}) and the catalog's own view calls its point "
        f"{entry_now.get('availability')}/{entry_now.get('verification')} with "
        f"selectable={entry_now.get('selectable')}. `Backup.status` carries none of "
        f"{blind or 'the facts this row tracks'} — which is EXPECTED on this destination and "
        f"is not the failure: its `evidenceRead` is a `SecretKeys` grant, whose read is D2 "
        f"§3.9's evidence-fetch Job, and `controllers/backup.rs` says in its own words that "
        f"this build does not create it. The refusal is reported honestly — the verdict block "
        f"IS written (present: {verdict_written}, value "
        f"{((fresh_status.get('evidence') or {}).get('verification') or {}).get('result')!r}). "
        f"SINCE THE FIX FOR PROTECTION-SECRETKEYS-UNPROTECTED, none of that is fatal: "
        f"`protection::evidence_objective_met` lets the catalog entry's own verification axis "
        f"answer `requireVerifiedEvidence` where the controller reached NO verdict, "
        f"`protection::entries_for` joins on the archive set id when the point has no "
        f"receipt-derived identity, "
        f"`controllers::protection_policy::with_catalog_facts` fills the capture time and the "
        f"point id from that row, and `protection::is_unplaceable` reports a point nothing "
        f"can place as `Unknown`/`PointFactsUnread` rather than `Unprotected`. So a failure "
        f"here is a controller image that predates that fix (check the image's "
        f"`org.opencontainers.image.revision` label) or a view that has not harvested the "
        f"point (viewRefresh {STATE.get('viewRefresh')!r}) — NOT the old defect being "
        f"re-observed, and this sentence must not be quoted as if it were. A verdict of "
        f"`Untrusted` or `Invalid` is a different case again: that verdict was REACHED, and a "
        f"refused point is `Unprotected` by design at every setting of "
        f"`requireVerifiedEvidence`"
    )
    check(
        "protection-recovery-notification-delivers-exactly-once",
        "PLAT-14.2",
        all(clauses.values()),
        f"a real Backup wrote a new point and the refreshed view lists it "
        f"({len(new_ids)} new point id(s) {new_ids}; the view's newest was "
        f"{newest_before.get('pointId')} and is now {newest_after.get('pointId')}), the view "
        f"was refreshed ({STATE.get('viewRefresh')}), and the policy's Staleness alert went "
        f"{(alert_of(alerts_before, 'Staleness') or {}).get('state')} -> "
        f"{(alert_of(alerts_after, 'Staleness') or {}).get('state')} with health "
        f"{(policy_now.get('status') or {}).get('health')}/"
        f"{(policy_now.get('status') or {}).get('availabilityBasis')}: {new_transitions} new "
        f"notified transition(s) and {posts} POST(s) at the in-cluster echo sink, delivery "
        f"{((alert_of(alerts_after, 'Staleness') or {}).get('delivery') or {})}. Clauses "
        f"{clauses}.{diagnosis}",
        evidence,
    )

    # --- (c) what the delivered documents say about verification ------------
    events = policy_events(RECOVERY_POLICY)
    evidence.append(artifact("protect/event-documents.json", events))
    scope_clauses = scope_is_never_complete(events)
    refusal = mutate_event_and_deliver(events)
    if refusal:
        evidence.append(refusal["evidence"])
    check(
        "protection-verification-scope-is-never-complete",
        "PLAT-14.2",
        all(scope_clauses.values()) and (refusal is None or refusal["refused"]),
        f"the {len(events)} event document(s) this policy delivered carry verification_scope "
        f"{sorted({e.get('verification_scope') for e in events})}, with health "
        f"{sorted({e.get('health') for e in events})} and last_available_point "
        f"{[e.get('last_available_point') for e in events]}. WHICH OF THE THREE WORDS IS NOT "
        f"THIS ROW'S SUBJECT and the row does not claim `sampled`: the controller writes "
        f"`sampled` only for a point whose evidence verdict is `Valid` "
        f"(`controllers::protection_policy::verification_scope`), which is unreachable on this "
        f"evidence-read path — the GRANT's own limit, not the fixed defect "
        f"PROTECTION-SECRETKEYS-UNPROTECTED, which never governed this word — so a "
        f"`NotAttempted` point "
        f"is labelled `none` — honest, and still not `complete`. The vocabulary has three "
        f"words and `complete` is not one of them "
        f"(`weirkeeper::protection::VerificationScope`, whose own doc comment calls a fourth "
        f"variant 'the product's one unrecoverable lie'). LIVE MUTANT: "
        + (f"the delivery Job's own image, argv and mount, run against a copy of a real event "
           f"whose verification_scope reads \"complete\", exits {refusal['exitCode']} and "
           f"delivers nothing — {refusal['reason']}" if refusal else
           "NOT RUN — no delivery Job was left to clone") +
        f". Clauses {scope_clauses}",
        evidence,
    )

    # --- (b) an archive that can no longer serve its newest point -----------
    posts_mark = sink_posts()
    alerts_before_break = policy_alerts(RECOVERY_POLICY)
    notified_mark = notified_total(alerts_before_break)
    victim = newest_after
    rm(BUCKET_A, victim["manifestKey"])
    broken_view = refresh_view("primary", f"protect-3-{int(time.time())}")
    broken_entries = {e["pointId"]: e for e in view_entries(broken_view)}
    degraded = settle(
        "protectionpolicy", RECOVERY_POLICY,
        lambda o: alert_of(((o.get("status") or {}).get("alerts") or []),
                           "ArchiveUnavailable") is not None,
        seconds=420, what="an ArchiveUnavailable alert",
    )
    if degraded is not None:
        settle(
            "protectionpolicy", RECOVERY_POLICY,
            lambda o: all(((a.get("delivery") or {}).get("state")
                           in {"Delivered", "Failed", "Suppressed"})
                          for a in ((o.get("status") or {}).get("alerts") or [])),
            seconds=300, what="every transition to finish delivering",
        )
    policy_broken = get("protectionpolicy", RECOVERY_POLICY)
    alerts_after_break = (policy_broken.get("status") or {}).get("alerts") or []
    posts = sink_posts() - posts_mark
    new_transitions = notified_total(alerts_after_break) - notified_mark
    evidence.append(artifact("protect/policy-archive-unavailable.json", policy_broken))
    evidence.append(artifact("protect/broken-entry.json",
                             {"before": victim, "after": broken_entries.get(victim["pointId"]),
                              "manifestRemoved": victim.get("manifestKey"),
                              "backupStatusStillSays": {
                                  k: (fresh_status.get(k))
                                  for k in ("phase", "exitCode", "backupId")}}))
    break_clauses = archive_unavailable_opened(
        victim, broken_entries.get(victim["pointId"], {}), alerts_before_break,
        alerts_after_break, posts, new_transitions,
    )
    break_clauses.update(selector_matched_a_run((policy_broken.get("status") or {})))
    check(
        "protection-unavailable-archive-raises-the-alert",
        "PLAT-14.2",
        all(break_clauses.values()),
        f"the newest point {victim.get('pointId')} was catalogued "
        f"{victim.get('availability')}/selectable={victim.get('selectable')}; with its "
        f"manifest removed from the bucket the refreshed view reads "
        f"{broken_entries.get(victim.get('pointId', ''), {}).get('availability')} "
        f"(remedy {broken_entries.get(victim.get('pointId', ''), {}).get('remedy')!r}) while "
        f"the Backup's own status still says {fresh_status.get('phase')}/exit "
        f"{fresh_status.get('exitCode')} — which is the whole point of the row — and the "
        f"policy holds "
        f"{[a.get('kind') for a in alerts_after_break if a.get('state') == 'Open']} open at "
        f"health {(policy_broken.get('status') or {}).get('health')}: {new_transitions} new "
        f"notified transition(s), {posts} POST(s) at the sink. Clauses {break_clauses}."
        + diagnosis.replace("WHY, AND IT IS THE PRODUCT'S:",
                            "AND WHY NO ALERT OF THAT KIND CAN OPEN HERE:", 1),
        evidence,
    )


def mutate_event_and_deliver(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The delivery Job's own command, against an event that says `complete`.

    The enum makes the word unwritable by the controller; this is the other
    half — that the runner REFUSES it rather than forwarding it — and it is run
    with the same image, the same argv and the same mount as the real delivery
    Job, so a refusal here is the refusal a real sink would get.
    """
    names = [j["metadata"]["name"] for j in lst("jobs")
             if j["metadata"]["name"].startswith(f"{RECOVERY_POLICY}-n-")]
    jobs = sorted(names) or sorted(j["metadata"]["name"] for j in lst("jobs")
                                   if "-n-" in j["metadata"]["name"])
    if not events or not jobs:
        return None
    job = get("job", jobs[-1])
    spec = json.loads(json.dumps(job["spec"]["template"]["spec"]))
    mutated = dict(events[0])
    mutated["verification_scope"] = "complete"
    name = f"{OWNER}-scope-mutant"
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": owned(f"{name}-event"),
           "data": {"event.json": json.dumps(mutated)}})
    for volume in spec.get("volumes", []) or []:
        if volume.get("configMap"):
            volume["configMap"]["name"] = f"{name}-event"
    spec["restartPolicy"] = "Never"
    if get_opt("pod", name) is not None:
        run(KN + ["delete", "pod", name, "--wait=true"])
    create({"apiVersion": "v1", "kind": "Pod", "metadata": owned(name), "spec": spec})
    pod = wait_for("pod", name,
                   lambda o: o.get("status", {}).get("phase") in {"Succeeded", "Failed"},
                   seconds=300, what="a terminal phase")
    logs = redact(run(KN + ["logs", name], check=False).stdout)
    state = ((pod["status"].get("containerStatuses") or [{}])[0].get("state") or {})
    code = (state.get("terminated") or {}).get("exitCode")
    return {
        "exitCode": code,
        "refused": code not in {0, None},
        "reason": (logs.strip().splitlines() or ["<no output>"])[-1],
        "evidence": artifact("protect/scope-mutant.txt",
                             f"exit={code}\nevent.verification_scope=complete\n\n{logs}"),
    }


# ---------------------------------------------------------------------------
# PLAT-14.2 — the three arms the fix-protection reviews left unwritten:
# `PointFactsUnread`, a REFUSED signature, and D3 §3.3's resolve column
# ---------------------------------------------------------------------------
#
# WHY THESE THREE EXIST AND WHY THEY ARE HERE. `claude/fix-protection.result.md`
# §6 and `claude/fix-protection.review-2.md` §6 both end with the same list:
# three behaviours the controller fix landed and NO harness row asserts, so the
# batch refresh cannot confirm them. `rg PointFactsUnread e2e/` found nothing;
# no row anywhere drives a verdict the controller REACHED and refused; and
# nothing measures the transition D3 §3.3's resolve column governs.
#
# THE LAB THESE RUN AGAINST DOES NOT CARRY THE FIX. The shared release runs the
# image labelled `af64073`, which predates it (`controller.imageRevision` in
# every artifact below records which build answered). The rows are written to
# assert the CORRECT — fixed — behaviour, so on `af64073` the ones that depend
# on the fix FAIL, and that failure IS the defect's live reproduction: it is
# what `PROTECTION-SECRETKEYS-UNPROTECTED` says happens. They pass at the next
# lab refresh or the fix did not land. Each row's `detail` says which of the two
# it is, so a reader of the artifact never has to guess.
#
# THE ONE ARM THIS LAB CANNOT REACH, stated rather than faked: a destination
# whose `evidenceRead` grant is `ControllerIdentity`. It needs the installation
# policy ConfigMap of the shared release to allowlist the archive location, and
# that is a change to a shared fixture behind the cluster lock. Where the brief
# asks for a `ControllerIdentity` destination the rows below use the two live
# facts that are reachable instead — the controller's own global read-only
# handle over the LEGACY inline archive, which reaches a real verdict, and a
# point the catalog CAN place — and the planted-fixture half is a unit row in
# `test_rows.py`. Nothing here pretends a grant it did not configure.

UNREAD_POLICY = "protect-unread"
REFUSED_POLICY = "protect-refused"
STAYS_POLICY = "protect-stays"
LEGACY_DEST = "dest-legacy"
LEGACY_CATALOG = "legacy-cat"
UNREAD_CATALOG = "verdict-cat"
# 86 400 s, so that a point the policy CAN place is `Healthy` rather than
# `Stale`: these rows are about whether a point can be placed and counted at
# all, and an objective tight enough to age it out would answer a different
# question.
VERDICT_MAX_AGE = 86_400
VERDICT_KINDS = ["Staleness", "BackupFailure", "ArchiveUnavailable"]


def verdict_policy(name: str, *, subject: dict[str, Any], catalog: str | None,
                   require_verified: bool = True,
                   require_catalog_availability: bool = True,
                   max_age: int = VERDICT_MAX_AGE) -> dict[str, Any]:
    """One `ProtectionPolicy`, with the two axes the reviews probed as
    parameters.

    NO `scheduleRefs`, AND THAT IS NOT A SHORTCUT. `is_member`
    (`controllers/protection_policy.rs:747`) requires a run to be a run OF one
    of the named schedules — `identity::is_run_of_schedule` over the schedule
    UID — and every Backup these rows create is a manual one. A policy naming
    `keeps-running` would therefore have no candidates at all, and every row
    below would be measuring an evaluation that never looked at a point.
    Without `scheduleRefs` the membership rule is D3 §3.2's other half, which
    the CRD states in as many words: "every run of the named source that writes
    to the named destination".

    `maxConsecutiveFailedRuns: 0` disables the failure axis (the controller
    requires `> 0` for both `AtRisk` and `BackupFailure`), so `Healthy` and
    `Staleness` mean what these rows say they mean and not "and no run failed".
    """
    protects: dict[str, Any] = {"sourceRef": {"name": "source"}, "topics": TOPICS}
    protects.update(subject)
    if catalog:
        protects["catalogRef"] = {"name": catalog}
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "ProtectionPolicy",
        "metadata": owned(name),
        "spec": {
            "protects": protects,
            "objectives": {
                "maxRecoveryPointAgeSeconds": max_age,
                "maxConsecutiveFailedRuns": 0,
                "requireVerifiedEvidence": require_verified,
                "requireCatalogAvailability": require_catalog_availability,
            },
            "evaluationIntervalSeconds": 60,
            "notifications": {
                "kinds": VERDICT_KINDS,
                "sendResolved": True,
                "routes": [
                    {"name": "local-sink",
                     "webhook": {"urlSecretRef": {"name": SINK_URL_SECRET, "key": "url"}}}
                ],
            },
        },
    }


def policy_view(obj: dict[str, Any]) -> dict[str, Any]:
    """Everything the rows below decide from, in one flat dict.

    A flat dict is what makes these predicates unit-testable without a cluster
    (`test_rows.py` plants wrong ones), and it is what the artifacts carry, so
    the value a clause read and the value a reader sees are the same value.
    """
    status = obj.get("status") or {}
    protected = condition(obj, "Protected")
    return {
        "health": status.get("health"),
        "availabilityBasis": status.get("availabilityBasis"),
        "lastAvailablePoint": status.get("lastAvailablePoint"),
        # WHICH RUN THE POLICY COUNTED. `status.lastAttempt` is built from
        # `input.slots.first()` and `slots` is built from the MEMBERS
        # (`controllers/protection_policy.rs:380`), so this naming the Backup
        # under test is the only proof from the API that the policy looked at
        # it at all — see `counted_this_run`.
        "lastAttempt": status.get("lastAttempt"),
        "protectedStatus": protected.get("status"),
        "protectedReason": protected.get("reason"),
        "protectedMessage": protected.get("message"),
        "alerts": status.get("alerts") or [],
        "observedGeneration": status.get("observedGeneration"),
        "generation": (obj.get("metadata") or {}).get("generation"),
        "evaluatedAt": status.get("evaluatedAt"),
    }


def rfc3339_ms(value: str | None) -> int | None:
    """`2026-09-21T18:00:00Z` — or `2026-09-22T02:28:38.193596887Z` — as epoch
    milliseconds, or `None`.

    THE FRACTION IS NOT OPTIONAL TO HANDLE. `logweir.dev`'s `Time` serializes
    with nanosecond precision wherever the controller wrote one directly
    (`status.evaluatedAt` in this run's own artifacts reads
    `2026-09-22T02:28:38.193596887Z`), so a parser that accepted only whole
    seconds would answer `None` for a real capture time and the clause that
    compares it to the catalog row would fail against a product that was
    right. Parsing is in one place for the same reason.
    """
    if not value:
        return None
    body = value.strip().removesuffix("Z")
    fraction = "000000"
    if "." in body:
        body, fraction = body.split(".", 1)
        fraction = fraction[:6].ljust(6, "0")
    if not fraction.isdigit():
        return None
    try:
        base = (dt.datetime.strptime(body, "%Y-%m-%dT%H:%M:%S")
                .replace(tzinfo=dt.timezone.utc))
    except ValueError:
        return None
    return int(base.timestamp() * 1000) + int(fraction) // 1000


def moved_past(value: str | None, mark: str) -> bool:
    """Whether an RFC3339 instant is later than `mark`.

    NOT `>` ON THE STRINGS. `2026-09-22T02:28:38.193596887Z` sorts BEFORE
    `2026-09-22T02:28:38Z` byte-wise — `.` is 0x2E and `Z` is 0x5A — so the
    string comparison this replaced said a controller that HAD re-evaluated
    had not, and `verdict_after`'s second wait would have run its full timeout
    on every call and then reported the previous posture's answer.
    """
    at = rfc3339_ms(value)
    since = rfc3339_ms(mark)
    return at is not None and since is not None and at > since


# ---- the predicates, one per behaviour, all pure -------------------------


def counted_this_run(view: dict[str, Any], backup: str) -> bool:
    """Whether the policy's verdict is ABOUT the run this row created.

    THE ROW THAT TAUGHT US TO ASK. The first live run of this phase put the
    refused-signature policy's `protects.topics` at both topics while the
    legacy Backups covered one, so `topics_covered` excluded every candidate,
    the policy had NO points, and `Unprotected` came back for a reason that had
    nothing to do with the signature: four cells green over an empty set. The
    negative control is what caught it — it demanded `Protected` from the same
    fixture and could not get it — and this clause is so that the next reader
    does not need the control to notice.

    `status.lastAttempt.backupRef` is built from `input.slots.first()`, and
    `slots` is the MEMBER list (`controllers/protection_policy.rs:380`: source,
    destination or archive URL, and the schedule membership rule), so a policy
    that names this Backup has counted this run.
    """
    return ((view.get("lastAttempt") or {}).get("backupRef") or {}).get("name") == backup


def unread_point_is_unknown(view: dict[str, Any], backup: str) -> dict[str, bool]:
    """D3 §3.2's `Unknown` row — *"evaluation impossible"* — for a point the
    controller could not PLACE.

    The destination's `evidenceRead` grant is `SecretKeys`, so the controller
    holds no Secret verb for it, verifies nothing itself and writes
    `evidence.verification.result: NotAttempted` with no `capture` and no
    `receiptSha256` (`docs/kubernetes.md`, "A point whose receipt the
    controller could not read"). With no `catalogRef` there is nothing left
    that could supply the capture time, so the point can be neither aged
    against the objective nor named.

    D3 §3.2's health table gives that `Unknown`, not `Unprotected`:
    `Unprotected` is "no available point at all", which PAGES, and saying it
    about an archive whose own catalog entry reads `Available`/`Verified` is
    defect `PROTECTION-SECRETKEYS-UNPROTECTED`. The message clause is the one
    that stops this passing for a policy that is `Unknown` for some OTHER
    reason — `CatalogStale`, `SourceMissing`, `ScheduleMissing` all land on
    `Unknown` too, and only this one names the read.
    """
    return {
        "the policy counted this run — status.lastAttempt names it":
            counted_this_run(view, backup),
        "health is Unknown — D3 §3.2's `evaluation impossible`, never `Unprotected`":
            view["health"] == "Unknown",
        "the Protected condition mirrors it (`Unknown` for `Unknown`)":
            view["protectedStatus"] == "Unknown",
        "with reason PointFactsUnread, and not another Unknown cause":
            view["protectedReason"] == "PointFactsUnread",
        "and a message naming the READ rather than a missing object":
            "read no verification verdict" in (view["protectedMessage"] or ""),
        "nothing is published as the newest available point":
            not (view["lastAvailablePoint"] or {}),
    }


def placed_point_is_protected(view: dict[str, Any], entry: dict[str, Any],
                              backup: str) -> dict[str, bool]:
    """THE NEGATIVE CONTROL for `unread_point_is_unknown`, and what it refuses.

    The same policy, the same `SecretKeys` destination, the same unverified
    point — with a `catalogRef` whose view holds ONE row for it. D3 §3.2's
    availability rule is then satisfiable: the capture time and the identity
    are read off that row (`recoveryPointAtMs` IS the receipt's `started_at`
    carried through the view) and the entry's own verification axis answers
    `requireVerifiedEvidence`, so the point counts and the policy is
    `Healthy`/`Protected=True`.

    A controller that answered `PointFactsUnread` for every unverified point —
    the cheapest way to make the row above pass — fails this one. A controller
    that rewrote the verdict to make it fit fails the last clause: the evidence
    still reads `NotAttempted`, because THIS controller still did not read that
    receipt, and D3 §5.4 keeps availability and verification on separate axes.
    """
    point = view["lastAvailablePoint"] or {}
    captured = rfc3339_ms(point.get("recoveryPointAt"))
    return {
        "the policy counted this run — status.lastAttempt names it":
            counted_this_run(view, backup),
        "health is Healthy — the point counts": view["health"] == "Healthy",
        "Protected=True": view["protectedStatus"] == "True",
        "the reason is NOT PointFactsUnread": view["protectedReason"] != "PointFactsUnread",
        "a point is published with a capture time": captured is not None,
        "which is the catalog row's own recoveryPointAtMs, to the second":
            captured is not None
            and entry.get("recoveryPointAtMs") is not None
            and abs(captured - int(entry["recoveryPointAtMs"])) < 1000,
        "and the catalog row's own point id": bool(point.get("pointId"))
            and point.get("pointId") == entry.get("pointId"),
        "availability was decided by the catalog, and says so":
            view["availabilityBasis"] == "Catalog",
        "the verdict is NOT rewritten — it still reads NotAttempted":
            point.get("evidence") == "NotAttempted",
    }


def refused_signature_is_unprotected(view: dict[str, Any], verdict: str | None,
                                     rescue: dict[str, Any] | None,
                                     backup: str) -> dict[str, bool]:
    """A verdict the controller REACHED and refused is `Unprotected` and PAGES
    — at every setting of `requireVerifiedEvidence` and with or without a
    `catalogRef`.

    `docs/kubernetes.md`: *"Such a point is `Unprotected` and pages, with or
    without a capture time, and at every setting of `requireVerifiedEvidence`:
    that objective governs whether an UNVERIFIED point may count as protection,
    never whether a REFUSED one may."* `Untrusted` is a signature this
    installation will not accept and `Invalid` is a document that is not what
    it claims to be; neither is "the controller did not look", and neither may
    be overruled by a catalog row — otherwise `TrustPolicy` would be
    decorative.

    The `rescue` clause is what makes the two `catalogRef` cells mean
    something. Without it the cell would pass on a view that simply held no row
    for the point, which proves nothing about whether a catalog row can rescue
    a refused verdict; with it the cell asserts that a row WAS there, carrying
    a capture time the policy could have filled from, and the policy refused
    the point anyway.

    The message clause is the HIGH-1b inversion, in one line: a refused point
    must not be described with `PointFactsUnread`'s sentence, which says the
    controller read no verdict. It read one.
    """
    staleness = alert_of(view["alerts"], "Staleness") or {}
    clauses = {
        "the policy counted this run — status.lastAttempt names it":
            counted_this_run(view, backup),
        "the controller REACHED a verdict, and it refuses the signature":
            verdict in {"Untrusted", "Invalid"},
        "health is Unprotected — D3 §3.2's `no available point at all`":
            view["health"] == "Unprotected",
        "Protected=False": view["protectedStatus"] == "False",
        "reason NoAvailablePoint": view["protectedReason"] == "NoAvailablePoint",
        "the Staleness incident is Open — this PAGES": staleness.get("state") == "Open",
        "nothing is published as the newest available point":
            not (view["lastAvailablePoint"] or {}),
        "and the message does NOT say the controller read no verdict — it read one":
            "read no verification verdict" not in (view["protectedMessage"] or ""),
    }
    if rescue is not None:
        clauses["the catalog held ONE row for this point, so it COULD have placed it"] = (
            rescue.get("recoveryPointAtMs") is not None
            and rescue.get("availability") == "Available"
        )
    return clauses


def valid_signature_is_protected(view: dict[str, Any], verdict: str | None,
                                 backup: str) -> dict[str, bool]:
    """THE NEGATIVE CONTROL for the refused-signature row: the same policy, the
    same archive, the same four postures — a VALID signature.

    It flips nothing in the product: no objective changes, no grant changes, no
    trust object is written. The only difference is which key signed the
    receipt the controller reads. A controller that answered `Unprotected` for
    every point on this archive — the cheapest way to make the four cells above
    pass — fails every clause here.
    """
    staleness = alert_of(view["alerts"], "Staleness") or {}
    point = view["lastAvailablePoint"] or {}
    return {
        "the policy counted this run — status.lastAttempt names it":
            counted_this_run(view, backup),
        "the same archive under a trusted signer verifies Valid": verdict == "Valid",
        "health is Healthy": view["health"] == "Healthy",
        "Protected=True": view["protectedStatus"] == "True",
        "a point is published, with a capture time": bool(point.get("recoveryPointAt")),
        "carrying the verdict the controller reached": point.get("evidence") in
            {"Valid", "ValidHistorical"},
        "and the Staleness incident is not Open": staleness.get("state") != "Open",
    }


def incident_resolves_exactly_once(before: dict[str, Any], after: dict[str, Any],
                                   health: str | None, posts: int,
                                   new_transitions: int) -> dict[str, bool]:
    """D3 §3.3's `Staleness` resolve column, verbatim: **"`health` back to
    `Healthy`/`AtRisk`"** — and exactly one transition for it.

    `protection::resolves_alerts` is those two values and nothing else. This is
    the arm where the condition DID clear, so the incident must close, once,
    and the close must be delivered once: one POST per transition this window
    opened is D3 §3.3's dedup rule ("one open alert per `(policy, kind)`;
    PagerDuty gets `trigger` on Open and `resolve` on Resolved under that
    key").

    The POST clause counts transitions across the whole ledger rather than this
    incident's alone, because a second incident may legitimately close in the
    same window (an `ArchiveUnavailable` opened over the refused point resolves
    when a sound point arrives). What is asserted is the RATIO — one delivery
    per transition, and no delivery without one.
    """
    return {
        "the Staleness incident was Open before": before.get("state") == "Open",
        "health came back to Healthy/AtRisk — D3 §3.3's resolve column":
            health in {"Healthy", "AtRisk"},
        "the incident is Resolved after it": after.get("state") == "Resolved",
        "which is exactly one new notified transition on this incident":
            (after.get("notifiedTransition") or 0) == (before.get("notifiedTransition") or 0) + 1,
        "one POST per transition this window opened, and no more":
            posts == new_transitions and new_transitions >= 1,
        "and the delivery says Delivered, not merely attempted":
            ((after.get("delivery") or {}).get("state")) == "Delivered",
    }


def incident_stays_open_on_unknown(before: dict[str, Any], after: dict[str, Any],
                                   view: dict[str, Any], posts: int) -> dict[str, bool]:
    """The other side of the same column, and the one a resolve-on-anything
    implementation gets wrong.

    D3 §3.3 resolves `Staleness` on "`health` back to `Healthy`/`AtRisk`" —
    `Unknown` is neither. `docs/kubernetes.md` says what that means for an
    operator: *"A policy that lands on `Unknown`/`PointFactsUnread` keeps its
    incident open and un-renotified until someone gives it a `catalogRef` or a
    readable receipt: Logweir does not claim a condition cleared because it
    stopped being able to look."*

    So this is a REQUIRED refusal, not a recorded one: the incident that was
    open stays open, its transition counter does not move, and nothing is
    delivered. A controller that treated "no longer Stale" as "resolved" would
    close a real incident on the strength of having lost the ability to measure
    it, and every clause here would fail.
    """
    return {
        "the Staleness incident was Open before": before.get("state") == "Open",
        "the policy is now Unknown/PointFactsUnread": (
            view["health"] == "Unknown" and view["protectedReason"] == "PointFactsUnread"
        ),
        "the incident is STILL Open — `Unknown` is not back to Healthy/AtRisk":
            after.get("state") == "Open",
        "no new transition was recorded":
            (after.get("transition") or 0) == (before.get("transition") or 0),
        "it was not re-notified":
            (after.get("notifiedTransition") or 0) == (before.get("notifiedTransition") or 0),
        "and nothing was delivered for it": posts == 0,
    }


def alert_ledger_unchanged(before: list[dict[str, Any]],
                           after: list[dict[str, Any]]) -> dict[str, bool]:
    """Nothing opened, nothing closed, nothing paged.

    D3 §3.2 gives `Unknown` no alert of its own — `open_alert_kinds` opens
    `Staleness` for `Stale | Unprotected` only — so a policy that became
    unmeasurable must not page, and must not un-page either.
    """
    kinds_before = {a.get("kind"): a for a in before or []}
    kinds_after = {a.get("kind"): a for a in after or []}
    open_before = {k for k, v in kinds_before.items() if v.get("state") == "Open"}
    open_after = {k for k, v in kinds_after.items() if v.get("state") == "Open"}
    return {
        "no alert kind opened that was not open before": open_after <= open_before,
        "nothing that was open was resolved": not any(
            kinds_before.get(k, {}).get("state") == "Open" and v.get("state") == "Resolved"
            for k, v in kinds_after.items()
        ),
        "and no transition was recorded at all":
            sum(a.get("transition") or 0 for a in after or [])
            == sum(a.get("transition") or 0 for a in before or []),
    }


# ---- the live plumbing ---------------------------------------------------


def settled_policy(name: str, *, seconds: int = 420) -> dict[str, Any]:
    """The verdict for the spec AS IT IS NOW.

    `status.observedGeneration` is the only thing that distinguishes "the
    controller answered my patch" from "the controller has not looked yet and
    this is the previous posture's answer" — which, on a row whose whole
    subject is a posture, is the difference between a measurement and a
    coincidence.
    """
    want = get("protectionpolicy", name)["metadata"]["generation"]
    obj = settle(
        "protectionpolicy", name,
        lambda o: ((o.get("status") or {}).get("observedGeneration") == want
                   and bool((o.get("status") or {}).get("health"))),
        seconds=seconds, what=f"a verdict for generation {want}",
    )
    return obj if obj is not None else get("protectionpolicy", name)


def verdict_after(name: str, mark: str, predicate: Callable[[dict[str, Any]], bool], *,
                  seconds: int = 300, what: str = "") -> dict[str, Any]:
    """Wait for the state the row expects; if it never arrives, wait instead
    for PROOF THAT THE CONTROLLER LOOKED, and return what it decided.

    `status.evaluatedAt` is "rewritten only on change, or when older than half
    the interval", so on a 60 s interval a timestamp that moved past `mark` is
    the controller having re-evaluated after the change this row made. Without
    that second wait a row that fails would be indistinguishable from a row
    whose controller had not got to it yet, and the FAIL would be worthless.
    """
    obj = settle("protectionpolicy", name, predicate, seconds=seconds, what=what)
    if obj is not None:
        return obj
    looked = settle(
        "protectionpolicy", name,
        lambda o: moved_past((o.get("status") or {}).get("evaluatedAt"), mark),
        seconds=150, what=f"any evaluation after {mark}",
    )
    return looked if looked is not None else get("protectionpolicy", name)


def patch_protection_policy(name: str, patch: dict[str, Any]) -> None:
    """Merge-patch a `ProtectionPolicy` with a whole-object patch.

    ITS OWN NAME, AND THAT IS THE FIX. This was `patch_policy` too, and Python
    binds a module's `def`s in file order, so it silently replaced the
    `RetentionPolicy` helper of the same name ~4 000 lines up: `legal_hold` and
    `bounded_retry` then patched a `ProtectionPolicy` called `keep-b` that does
    not exist (lab-refresh-7, 03:55Z, proved by `inspect.getsourcelines`).
    `test_rows.py::test_no_harness_module_defines_a_top_level_name_twice` is
    the guard for the class.
    """
    run(KN + ["patch", "protectionpolicy", name, "--type=merge", "-p", json.dumps(patch)])


def sink_ready() -> None:
    if get_opt("pod", SINK_POD) is None:
        apply(sink_pod())
        run(KN + ["wait", "--for=condition=Ready", f"pod/{SINK_POD}", "--timeout=120s"])
    sink_ip = get("pod", SINK_POD)["status"]["podIP"]
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned(SINK_URL_SECRET),
           "stringData": {"url": f"http://{sink_ip}:{SINK_PORT}/alerts"}})


def entry_for_backup(entries: list[dict[str, Any]], backup: dict[str, Any]) -> dict[str, Any]:
    """The ONE view row for this Backup's archive set.

    The join is the controller's own (`protection::entries_for`): the point id
    where the controller has one — the first 128 bits of the receipt digest —
    and the ARCHIVE SET ID otherwise, which is what a point with no
    receipt-derived identity has left. Ambiguity is refused rather than
    resolved: two rows for one set are two points, and reading either one's
    capture time would put a number on a recovery point that is not this one.
    """
    backup_id = (backup.get("status") or {}).get("backupId")
    matched = [e for e in entries if backup_id and e.get("backupId") == backup_id]
    return matched[0] if len(matched) == 1 else {}


def protection_verdicts() -> None:
    """The three owed arms, live, in one namespace.

    ORDER IS THE FIXTURES' OWN. Row 2 must measure an archive whose ONLY point
    is the refused one, and its control then adds a sound point to that same
    archive — which is exactly the transition row 3's flip is about, so the
    control and the flip are one action measured twice rather than two archives
    that happen to agree.
    """
    evidence: list[str] = []
    sink_ready()
    STATE["controller"] = controller_facts()
    save()
    evidence.append(artifact("verdicts/controller.json", STATE["controller"]))

    # =====================================================================
    # ROW 1 — `PointFactsUnread`: a point on a `SecretKeys` grant
    # =====================================================================
    if get_opt("backup", "unread-point") is not None:
        run(KN + ["delete", "backup", "unread-point", "--wait=true"])
    unread = run_backup("unread-point", "dest-a")
    unread_status = unread.get("status") or {}
    facts = point_facts_the_policy_needs(unread_status)
    evidence.append(artifact("verdicts/1-unread-backup.json",
                             {"backup": backup_facts(unread),
                              "pointFactsThePolicyNeeds": facts}))
    view = fresh_catalog(UNREAD_CATALOG, "dest-a")
    entries = view_entries(view)
    entry = entry_for_backup(entries, unread)
    evidence.append(artifact("verdicts/1-catalog-entry.json",
                             {"entryForThePoint": entry, "entriesInView": len(entries)}))

    # --- (1a) THE CONTROL: with a `catalogRef`, the point IS placed -------
    if get_opt("protectionpolicy", UNREAD_POLICY) is not None:
        run(KN + ["delete", "protectionpolicy", UNREAD_POLICY, "--wait=true"])
    apply(verdict_policy(UNREAD_POLICY,
                         subject={"destinationRef": {"name": "dest-a"}},
                         catalog=UNREAD_CATALOG))
    placed = policy_view(settled_policy(UNREAD_POLICY))
    evidence.append(artifact("verdicts/1a-with-catalogref.json",
                             {"view": placed, "catalogEntry": entry}))
    placed_clauses = placed_point_is_protected(placed, entry, "unread-point")
    unread_verdict = verdict_of(unread)
    unread_capture = unread_status.get("capture")
    unread_receipt = bool((unread_status.get("evidence") or {}).get("receiptSha256"))
    check(
        "protection-catalog-places-an-unverified-point",
        "PLAT-14.2",
        all(placed_clauses.values()),
        f"NEGATIVE CONTROL for `protection-unplaceable-point-is-unknown`, and the arm that "
        f"proves the row distinguishes. The `Backup` on a `SecretKeys` `evidenceRead` grant "
        f"carries verification {unread_verdict!r}, capture {unread_capture!r} and "
        f"receiptSha256 present={unread_receipt} — the controller "
        f"read nothing. With `catalogRef: {UNREAD_CATALOG}`, whose view holds "
        f"{'one row' if entry else 'NO row'} for this point "
        f"({entry.get('availability')}/{entry.get('verification')}, selectable="
        f"{entry.get('selectable')}, recoveryPointAtMs={entry.get('recoveryPointAtMs')}), the "
        f"policy reads health {placed['health']}/{placed['availabilityBasis']}, "
        f"Protected={placed['protectedStatus']}/{placed['protectedReason']}, point "
        f"{placed['lastAvailablePoint']}. D3 §3.2's availability rule is satisfiable here and "
        f"§5.4 keeps the two axes apart, so the verdict must still read `NotAttempted`. "
        f"Clauses {placed_clauses}. ON `af64073` THIS FAILS: the pre-fix join refuses a "
        f"candidate with no point id the moment a catalogRef is consulted, which is defect "
        f"PROTECTION-SECRETKEYS-UNPROTECTED and what this row reproduces.",
        evidence,
    )

    # --- (1b) with `catalogRef` REMOVED, nothing can place it -------------
    before_alerts = policy_alerts(UNREAD_POLICY)
    posts_mark = sink_posts()
    mark = now()
    patch_protection_policy(UNREAD_POLICY, {"spec": {"protects": {"catalogRef": None}}})
    want = get("protectionpolicy", UNREAD_POLICY)["metadata"]["generation"]
    unknown = policy_view(verdict_after(
        UNREAD_POLICY, mark,
        lambda o: ((o.get("status") or {}).get("observedGeneration") == want
                   and bool((o.get("status") or {}).get("health"))),
        seconds=300, what="a verdict with no catalogRef",
    ))
    after_alerts = policy_alerts(UNREAD_POLICY)
    posts = sink_posts() - posts_mark
    unknown_clauses = unread_point_is_unknown(unknown, "unread-point")
    ledger = alert_ledger_unchanged(before_alerts, after_alerts)
    evidence.append(artifact("verdicts/1b-no-catalogref.json",
                             {"view": unknown, "alertsBefore": before_alerts,
                              "alertsAfter": after_alerts, "posts": posts,
                              "clauses": unknown_clauses, "ledger": ledger}))
    check(
        "protection-unplaceable-point-is-unknown",
        "PLAT-14.2",
        all(unknown_clauses.values()) and all(ledger.values()),
        f"D3 §3.2: `Unknown` is for an evaluation that is impossible; `Unprotected` is "
        f"\"no available point at all\" and it PAGES. A run this policy covers succeeded "
        f"(`unread-point`, exit {unread_status.get('exitCode')}) and the controller could not "
        f"place its point in time — a `SecretKeys` `evidenceRead` grant, so no capture time "
        f"and no receipt-derived identity — and with `catalogRef` removed nothing else can. "
        f"The policy reads health {unknown['health']}, Protected="
        f"{unknown['protectedStatus']}/{unknown['protectedReason']}, message "
        f"{(unknown['protectedMessage'] or '')[:160]!r}; {posts} POST(s) at the sink. "
        f"Clauses {unknown_clauses}. Ledger {ledger} — `Unknown` opens no alert "
        f"(`open_alert_kinds` opens `Staleness` for `Stale | Unprotected` only). THE "
        f"NON-VACUOUS half of \"and clears none\" is `protection-unknown-keeps-its-incident-"
        f"open`, which starts from an incident that IS open; this row only asserts that "
        f"nothing opened or closed here. ON `af64073` THIS FAILS with "
        f"`Unprotected`/`NoAvailablePoint`, which is the defect.",
        evidence,
    )

    # =====================================================================
    # ROW 2 — a signature this installation REFUSES, at four postures
    # =====================================================================
    # THE ARCHIVE IS THE LEGACY INLINE ONE, and that is the only way to reach a
    # verdict at all on this lab: a destination-backed run's receipt is read by
    # the controller only where `evidenceRead.mode` is `ControllerIdentity` AND
    # the installation policy allowlists the location, and the lab's
    # `weirkeeper-policy` allowlists nothing. The controller's one global
    # read-only handle is rooted at the fixture's archive, so a Backup-level
    # verdict — `Valid`, `Untrusted` or `Invalid` — is reachable there and
    # nowhere else. `spec.protects.legacyArchive` is the CRD's own way to point
    # a policy at it (CEL rule H1: a saved destination XOR an inline archive).
    subject = {"legacyArchive": {"url": LEGACY_ARCHIVE, "secretRef": {"name": "logweir-s3"}}}
    # A PREVIOUS RUN'S SOUND POINT IS THIS RUN'S SILENT PASS. `valid-signature`
    # is created LATER in this phase, so on a second run of it the four cells
    # below would be measured against an archive that already holds a point
    # the installation accepts — and `Unprotected` would then be the wrong
    # answer for the right reason, or the right answer for the wrong one.
    # Every fixture this phase measures against is created by this run.
    if get_opt("backup", "valid-signature") is not None:
        run(KN + ["delete", "backup", "valid-signature", "--wait=true"])
    key = mint_signing_key(f"{OWNER}-untrusted")
    original = get("secret", "logweir-signing-key")
    refused_backup: dict[str, Any] = {}
    try:
        # NO TrustPolicy IS WRITTEN. `spec.keys` of the synthesised
        # `legacy-roster-v1` is the lab's own `TrustRoster`, and a key that is
        # not in the resolved policy is `UntrustedSigner` — "exactly what a
        # revocation-by-deletion would look like" (`trust.rs:233`). Producing
        # the refusal by SIGNING with an unknown key rather than by REVOKING a
        # known one keeps every object this row writes namespaced: no
        # cluster-scoped change, no cluster lock, nothing shared touched.
        run(KN + ["delete", "secret", "logweir-signing-key", "--wait=true"], check=False)
        run(KN + ["create", "secret", "generic", "logweir-signing-key",
                  f"--from-file=signing.pem={key['private']}"], timeout=120)
        # TOPICS, NOT `legacy_backup`'s DEFAULT. `matches_policy` applies
        # `topics_covered` (D3 §3.2's `topics ⊆ point topics`) to every
        # candidate, and this policy protects both topics: a point covering
        # `orders` alone is not a candidate at all, the policy has an EMPTY
        # candidate set, and `Unprotected` comes back for a reason that has
        # nothing to do with the signature. That is exactly how the first live
        # run of this phase made four cells green over nothing.
        refused_backup = legacy_backup("refused-signature", topics=TOPICS)
    finally:
        run(KN + ["delete", "secret", "logweir-signing-key", "--wait=true"], check=False)
        apply({"apiVersion": "v1", "kind": "Secret", "metadata": owned("logweir-signing-key"),
               "data": original.get("data", {}), "type": original.get("type", "Opaque")})
        key["private"].unlink(missing_ok=True)
        shutil.rmtree(key["dir"], ignore_errors=True)
    refused_verification = ((refused_backup.get("status") or {}).get("evidence") or {}).get(
        "verification") or {}
    refused_verdict = refused_verification.get("result")
    evidence.append(artifact("verdicts/2-refused-backup.json",
                             {"backup": backup_facts(refused_backup),
                              "verification": refused_verification,
                              "mintedKeyId": key["keyId"],
                              "privateKeyFileExists": key["private"].exists()}))
    check(
        "protection-untrusted-signer-fixture-is-real",
        "PLAT-14.2",
        refused_verdict in {"Untrusted", "Invalid"}
        and not key["private"].exists() and not key["dir"].exists(),
        f"the fixture the four cells below are about: a Backup signed by a key this "
        f"installation has never heard of ({key['keyId'][:16]}…, minted for this row) "
        f"verifies {refused_verdict!r} — a verdict the controller REACHED, which is the whole "
        f"difference from row 1's `NotAttempted`. matchedKeyId "
        f"{str(refused_verification.get('matchedKeyId'))[:16]!r}, trust "
        f"{refused_verification.get('trust')}. The private half is gone from disk "
        f"(file {key['private'].exists()}, dir {key['dir'].exists()}) and never entered an "
        f"artifact; the namespace's `logweir-signing-key` is restored to the lab's own copy. "
        f"A row whose fixture did not refuse would make all four cells below vacuous.",
        evidence,
    )
    # THE CATALOG NEEDS A DESTINATION, and a `RecoveryCatalog` takes only a
    # `destinationRef` — so the archive the legacy Backups write into is
    # declared as one: the shared fixture's bucket, under THIS RUN'S OWN prefix,
    # which is the same root `LEGACY_ARCHIVE` names. Nothing is written outside
    # that prefix and nothing is ever deleted from that bucket (see `cleanup`).
    apply(destination(LEGACY_DEST, "kafka-backups", prefix=f"{OWNER}-{STAMP}"))
    wait_for("backupdestination", LEGACY_DEST,
             lambda o: condition(o, "Valid").get("status") == "True",
             seconds=180, what="Valid=True")
    legacy_view = fresh_catalog(LEGACY_CATALOG, LEGACY_DEST)
    legacy_entries = view_entries(legacy_view)
    rescue = entry_for_backup(legacy_entries, refused_backup)
    evidence.append(artifact("verdicts/2-catalog-rescue.json",
                             {"entryForTheRefusedPoint": rescue,
                              "entriesInView": len(legacy_entries)}))

    postures = [(True, True), (True, False), (False, True), (False, False)]
    if get_opt("protectionpolicy", REFUSED_POLICY) is not None:
        run(KN + ["delete", "protectionpolicy", REFUSED_POLICY, "--wait=true"])
    apply(verdict_policy(REFUSED_POLICY, subject=subject, catalog=LEGACY_CATALOG))
    cells: dict[str, dict[str, bool]] = {}
    views: dict[str, dict[str, Any]] = {}
    for verified, with_catalog in postures:
        label = f"requireVerifiedEvidence={verified} catalogRef={'present' if with_catalog else 'absent'}"
        patch_protection_policy(REFUSED_POLICY, {
            "spec": {
                "protects": {"catalogRef": {"name": LEGACY_CATALOG} if with_catalog else None},
                "objectives": {"requireVerifiedEvidence": verified},
            }
        })
        cell = policy_view(settled_policy(REFUSED_POLICY))
        views[label] = cell
        cells[label] = refused_signature_is_unprotected(
            cell, refused_verdict, rescue if with_catalog else None, "refused-signature")
    evidence.append(artifact("verdicts/2-four-postures.json",
                             {"cells": cells, "views": views, "verdict": refused_verdict}))
    check(
        "protection-refused-signature-is-unprotected-at-every-posture",
        "PLAT-14.2",
        all(all(c.values()) for c in cells.values()),
        f"a receipt signed by a key this installation refuses verifies {refused_verdict!r} — "
        f"REACHED, not skipped — and D3 §3.2 gives such a point `Unprotected` at every "
        f"setting of the evidence objective: that objective governs whether an UNVERIFIED "
        f"point may count, never whether a REFUSED one may. Four cells, each asserted: "
        + "; ".join(
            f"[{label}] health {views[label]['health']}/"
            f"{views[label]['protectedReason']} alerts "
            f"{[(a.get('kind'), a.get('state')) for a in views[label]['alerts']]} "
            f"{'ALL PASS' if all(c.values()) else 'FAILED ' + str([k for k, v in c.items() if not v])}"
            for label, c in cells.items())
        + f". The catalog's own row for the refused point is {rescue.get('availability')}/"
        f"{rescue.get('verification')} with recoveryPointAtMs "
        f"{rescue.get('recoveryPointAtMs')} — present, so the two `catalogRef` cells assert "
        f"that a row which COULD have placed the point did not rescue it.",
        evidence,
    )

    # --- (2c) THE CONTROL: the same four postures, a VALID signature ------
    # It plants a sound signature and changes nothing else — no objective, no
    # grant, no trust object. This is also row 3's flip: the archive whose only
    # point was refused now holds a sound one, which is the condition D3 §3.3
    # resolves on.
    staleness_before = alert_of(policy_alerts(REFUSED_POLICY), "Staleness") or {}
    alerts_before_flip = policy_alerts(REFUSED_POLICY)
    posts_mark = sink_posts()
    sound = legacy_backup("valid-signature", topics=TOPICS)
    sound_verdict = (((sound.get("status") or {}).get("evidence") or {})
                     .get("verification") or {}).get("result")
    # `fresh_catalog`, not `refresh_view`: the latter's fallback recreates the
    # object over `dest-a`, which is not this catalog's destination.
    fresh_legacy = fresh_catalog(LEGACY_CATALOG, LEGACY_DEST)
    evidence.append(artifact("verdicts/2c-valid-backup.json",
                             {"backup": backup_facts(sound), "verdict": sound_verdict,
                              "entriesInView": len(view_entries(fresh_legacy))}))
    control_cells: dict[str, dict[str, bool]] = {}
    control_views: dict[str, dict[str, Any]] = {}
    for verified, with_catalog in postures:
        label = f"requireVerifiedEvidence={verified} catalogRef={'present' if with_catalog else 'absent'}"
        patch_protection_policy(REFUSED_POLICY, {
            "spec": {
                "protects": {"catalogRef": {"name": LEGACY_CATALOG} if with_catalog else None},
                "objectives": {"requireVerifiedEvidence": verified},
            }
        })
        cell = policy_view(settled_policy(REFUSED_POLICY))
        control_views[label] = cell
        control_cells[label] = valid_signature_is_protected(
            cell, sound_verdict, "valid-signature")
    evidence.append(artifact("verdicts/2c-control-four-postures.json",
                             {"cells": control_cells, "views": control_views}))
    check(
        "protection-valid-signature-is-protected-at-every-posture",
        "PLAT-14.2",
        all(all(c.values()) for c in control_cells.values()),
        f"NEGATIVE CONTROL for `protection-refused-signature-is-unprotected-at-every-posture`. "
        f"The same policy, the same archive, the same four postures, and ONE difference: the "
        f"receipt is signed by the lab's own key, so it verifies {sound_verdict!r}. Nothing in "
        f"the product was flipped to make it pass. "
        + "; ".join(
            f"[{label}] health {control_views[label]['health']}/"
            f"{control_views[label]['protectedReason']} point "
            f"{(control_views[label]['lastAvailablePoint'] or {}).get('recoveryPointAt')} "
            f"{'ALL PASS' if all(c.values()) else 'FAILED ' + str([k for k, v in c.items() if not v])}"
            for label, c in control_cells.items())
        + ". A controller that answered `Unprotected` for every point on this archive would "
        "pass all four cells of the row above and fail all four of these.",
        evidence,
    )

    # =====================================================================
    # ROW 3 — D3 §3.3's resolve column, both directions
    # =====================================================================
    alerts_after_flip = policy_alerts(REFUSED_POLICY)
    staleness_after = alert_of(alerts_after_flip, "Staleness") or {}
    if staleness_after.get("state") == "Resolved":
        settle(
            "protectionpolicy", REFUSED_POLICY,
            lambda o: all(((a.get("delivery") or {}).get("state")
                           in {"Delivered", "Failed", "Suppressed"})
                          for a in ((o.get("status") or {}).get("alerts") or [])),
            seconds=300, what="every transition to finish delivering",
        )
        alerts_after_flip = policy_alerts(REFUSED_POLICY)
        staleness_after = alert_of(alerts_after_flip, "Staleness") or {}
    posts = sink_posts() - posts_mark
    new_transitions = (sum(a.get("transition") or 0 for a in alerts_after_flip)
                       - sum(a.get("transition") or 0 for a in alerts_before_flip))
    final = policy_view(get("protectionpolicy", REFUSED_POLICY))
    flip = incident_resolves_exactly_once(
        staleness_before, staleness_after, final["health"], posts, new_transitions)
    evidence.append(artifact("verdicts/3a-flip.json",
                             {"before": staleness_before, "after": staleness_after,
                              "alertsBefore": alerts_before_flip,
                              "alertsAfter": alerts_after_flip, "posts": posts,
                              "newTransitions": new_transitions, "view": final,
                              "clauses": flip}))
    check(
        "protection-refused-to-sound-resolves-exactly-once",
        "PLAT-14.2",
        all(flip.values()),
        f"D3 §3.3's resolve column for `Staleness`, verbatim: \"`health` back to "
        f"`Healthy`/`AtRisk`\". The policy's only point was one the installation refuses, so "
        f"the incident was {staleness_before.get('state')!r} at transition "
        f"{staleness_before.get('transition')}; a sound point arrived in the same archive "
        f"(`valid-signature`, verdict {sound_verdict!r}), health came back to "
        f"{final['health']}, and the incident is {staleness_after.get('state')!r} at "
        f"transition {staleness_after.get('transition')} / notified "
        f"{staleness_after.get('notifiedTransition')}, delivery "
        f"{(staleness_after.get('delivery') or {}).get('state')!r}. {new_transitions} "
        f"transition(s) across the ledger in this window and {posts} POST(s) at the "
        f"in-cluster sink — D3 §3.3's dedup rule is one message per transition. "
        f"Clauses {flip}.",
        evidence,
    )

    # --- (3b) the refusal: `Unknown` does not clear an incident -----------
    # THE OTHER SIDE OF THE SAME COLUMN, and a REQUIRED refusal rather than a
    # recorded one. dest-b, because it holds no points: the policy therefore
    # opens `Staleness` honestly (`Unprotected` — nothing to recover from), and
    # the point that arrives next is one the controller cannot place.
    if get_opt("protectionpolicy", STAYS_POLICY) is not None:
        run(KN + ["delete", "protectionpolicy", STAYS_POLICY, "--wait=true"])
    # THE POINT ARRIVES AFTER THE INCIDENT, and on a re-run that means deleting
    # the previous run's `unplaceable-point` FIRST. The incident has to open
    # over a destination with nothing in it — `Unprotected`, "no available
    # recovery point for this policy at all" — or the transition this row
    # measures would already have happened before the row started.
    if get_opt("backup", "unplaceable-point") is not None:
        run(KN + ["delete", "backup", "unplaceable-point", "--wait=true"])
    apply(verdict_policy(STAYS_POLICY,
                         subject={"destinationRef": {"name": "dest-b"}}, catalog=None))
    opened = settle(
        "protectionpolicy", STAYS_POLICY,
        lambda o: (alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness") or {})
        .get("state") == "Open",
        seconds=420, what="a Staleness incident over an empty destination",
    )
    if opened is None:
        record("protection-unknown-keeps-its-incident-open", "PLAT-14.2", "FAIL",
               "no Staleness incident opened over an empty destination at all, so the "
               "refusal this row is about could not be measured; the objects are in "
               + artifact("verdicts/3b-no-incident.json",
                          policy_view(get("protectionpolicy", STAYS_POLICY))),
               evidence)
    else:
        settle(
            "protectionpolicy", STAYS_POLICY,
            lambda o: (((alert_of(((o.get("status") or {}).get("alerts") or []), "Staleness")
                         or {}).get("delivery") or {}).get("state")
                       in {"Delivered", "Failed", "Suppressed"}),
            seconds=300, what="the open transition to finish delivering",
        )
        stays_before = alert_of(policy_alerts(STAYS_POLICY), "Staleness") or {}
        posts_mark = sink_posts()
        mark = now()
        stays_backup = run_backup("unplaceable-point", "dest-b")
        stays_view = policy_view(verdict_after(
            STAYS_POLICY, mark,
            lambda o: condition(o, "Protected").get("reason") == "PointFactsUnread",
            seconds=300, what="the policy to become Unknown/PointFactsUnread",
        ))
        stays_after = alert_of(policy_alerts(STAYS_POLICY), "Staleness") or {}
        posts = sink_posts() - posts_mark
        stays = incident_stays_open_on_unknown(stays_before, stays_after, stays_view, posts)
        evidence.append(artifact("verdicts/3b-stays-open.json",
                                 {"before": stays_before, "after": stays_after,
                                  "view": stays_view, "posts": posts, "clauses": stays,
                                  "backup": backup_facts(stays_backup)}))
        check(
            "protection-unknown-keeps-its-incident-open",
            "PLAT-14.2",
            all(stays.values()),
            f"THE REFUSAL D3 §3.3's resolve column requires, and the one a "
            f"resolve-on-anything implementation gets wrong. A policy over an empty "
            f"destination opened `Staleness` at transition {stays_before.get('transition')} "
            f"(delivery {(stays_before.get('delivery') or {}).get('state')!r}); a run then "
            f"succeeded into that destination (`unplaceable-point`, exit "
            f"{(stays_backup.get('status') or {}).get('exitCode')}) whose receipt the "
            f"controller cannot read — a `SecretKeys` grant, and no `catalogRef` — so the "
            f"policy is {stays_view['health']}/{stays_view['protectedReason']}. `Unknown` is "
            f"NOT \"back to `Healthy`/`AtRisk`\", so the incident must stay "
            f"{stays_after.get('state')!r} at transition {stays_after.get('transition')}, "
            f"notified {stays_after.get('notifiedTransition')}, with {posts} POST(s) in the "
            f"window: Logweir does not claim a condition cleared because it stopped being "
            f"able to look. Clauses {stays}. ON `af64073` THIS FAILS on the health clause "
            f"only — the incident stays open there because the policy stays `Unprotected`, "
            f"which is the defect, not the rule.",
            evidence,
        )


# ---------------------------------------------------------------------------
# PLAT-14.3 / D3 §15 L6 — a rehearsal executes end to end
# ---------------------------------------------------------------------------
#
# THE CONTRACT IS `claude/plat14-3b.review.md` §4, ten steps, and every clause
# below is named after that specification's own words. It is written against
# the CORRECT behaviour — the behaviour PLAT-14.3b landed — and NOT against the
# build the lab happens to run: a clause softened to make a row green today is
# a row that will still be green when the defect comes back.
#
# What each step proves, and why it is here rather than in a unit test:
#
#   1. setup            the standing `Approval` really verifies in a cluster
#   2. the Restore      `spec.authorization` UNBLOCKS — the 14.3b defect
#   3. the Job          the standing mount, the five-member bundle, the env
#   4. the scorecard    signed evidence naming the schedule, not a person
#   5. the schedule     `rehearsalLast*`, `RehearsalHealthy`, the cleared active ref
#   6. the topics       owned, torn down, and the unrelated one survives
#   7. concurrency      a second slot while the first runs is `ConcurrencyBlocked`
#   8. the leftover     a pre-created mapped name refuses and is untouched
#   9. retention        the evidence outlives the Job's TTL
#  10. the refused arm  a NEGATIVE CONTROL that REQUIRES the refusal
#
# Steps 2–9 are a chain: each needs the one before it to have produced an
# object. A step whose input never existed is recorded **NOT-REACHED**, never
# PASS and never FAIL, because "the assertion did not run" is a third answer and
# writing it as either of the other two is how a harness lies.

#: The schedule D3 §15's L6 is about. Ten minutes, as the scenario says.
REHEARSAL_SCHEDULE = "l6-rehearsal"
#: D3 §15 L6 and review §4 step 1: "a 10-minute cron".
REHEARSAL_CRON = "*/10 * * * *"
#: Review §4 step 7's arm. Its own schedule, and see `REHEARSAL_FAST_CRON`.
REHEARSAL_CONCURRENCY_SCHEDULE = "l6-concurrency"
#: Review §4 step 8's arm.
REHEARSAL_LEFTOVER_SCHEDULE = "l6-leftover"
#: Review §4 step 10's arm — the negative control.
REHEARSAL_REFUSED_SCHEDULE = "l6-refused"
#: A ONE-MINUTE CADENCE FOR THE TWO ARMS THAT NEED A SECOND SLOT.
#
# Steps 7 and 8 are about what `decide` does when a NEW slot arrives while the
# previous rehearsal is still running, and about what a pre-created mapped name
# does to the slot after it. Neither clause is about the cadence — `decide`
# takes the same branch whatever the cron says — and at ten minutes the arrival
# of the second slot inside a run that takes two or three minutes is a coin
# toss. The schedule L6 names keeps its ten-minute cron; these two arms use
# their own objects at one minute so the second slot is a certainty rather than
# a wait the row cannot bound.
REHEARSAL_FAST_CRON = "* * * * *"
#: The `KafkaCluster` in THIS namespace naming the lab's scratch broker.
REHEARSAL_TARGET = "rehearsal-target"
#: D3 §15 L6, by name: "a pre-created unrelated topic `rehearsal-not-ours`".
REHEARSAL_UNRELATED_TOPIC = "rehearsal-not-ours"
#: The `BackupSchedule` whose manual run becomes the qualifying point.
REHEARSAL_POINT_SCHEDULE = "l6-points"
#: The catalog whose view makes that point selectable.
REHEARSAL_CATALOG = "l6-cat"
#: The one topic every rehearsal here restores. One, because the scope signs the
#: list and a second name buys the row nothing.
REHEARSAL_TOPIC = "orders"
#: The lab's scratch broker, in the shared fixture namespace.
TARGET_DEPLOY = "kafka-target"
KAFKA_BIN = "/opt/kafka/bin"
#: The bundle a standing-authorized `Restore` projects — review §4 step 3's
#: "exactly five keys", spelled as `controllers/restore.rs` spells them.
STANDING_BUNDLE_KEYS = {
    "standing-authorization.json",
    "standing-authorization.sig",
    "authorization-keys.json",
    "allowed-clusters.json",
    "approver.pub.pem",
}
#: The two per-run digests a standing Job must NOT carry (PLAT-14.3b).
APPROVAL_DIGEST_ENV = (
    "LOGWEIR_EXECUTION_APPROVAL_SHA256",
    "LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256",
)


def target_exec(args: list[str], *, check_rc: bool = True, timeout: int = 120):
    """One command inside the lab's scratch broker, over its PLAINTEXT listener.

    THE SHARED FIXTURE'S KAFKA, USED AS A KAFKA. WORKER-RULES lets a run use
    `logweir-scram-local`'s Kafka and MinIO from its own namespace; what it
    forbids is changing the RELEASE — its controller image, its env, its CRDs.
    Creating and deleting topics under this run's own `rehearsal-` prefix is
    the same use the drill path makes of the same broker, and every name this
    function creates is removed in `rehearsal`'s own `finally`.
    """
    return run(
        K + ["-n", FIXTURE_NS, "exec", f"deploy/{TARGET_DEPLOY}", "--", *args],
        check=check_rc, timeout=timeout,
    )


def target_topics() -> set[str]:
    """Every non-internal topic the scratch broker holds, now."""
    out = target_exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
        "--list", "--exclude-internal",
    ]).stdout
    return {line.strip() for line in out.splitlines() if line.strip()}


def target_topic_create(name: str) -> None:
    target_exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
        "--create", "--if-not-exists", "--topic", name,
        "--partitions", "1", "--replication-factor", "1",
    ], check_rc=False)


def target_topic_delete(name: str) -> None:
    target_exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
        "--delete", "--topic", name,
    ], check_rc=False)


def rendered_prefix(uid: str) -> str:
    """D3 §4.4's rendered prefix, `<spec.target.topicPrefix><uid[..8]>-`.

    The eight characters are `rehearsal::UID_PREFIX_LEN`, and this function is
    the harness's only copy of that arithmetic: the signed scope carries the
    RENDERED value, so a prefix computed differently here would mint a document
    the controller refuses for a reason that is the harness's.
    """
    return f"rehearsal-{uid[:8]}-"


def mapped_topic(uid: str, topic: str) -> str:
    return f"{rendered_prefix(uid)}{topic}"


# --- the ten predicates, each one pure and each one with a planted mutant ----


def standing_approval_is_verified(approval: dict[str, Any],
                                  schedule_uid: str) -> dict[str, bool]:
    """Review §4 step 1: `Verified=True` with a non-empty `status.matchedKeyId`
    and `status.verifiedSubjectRef.uid`.

    THE UID CLAUSE IS THE ONE THAT MATTERS. `Verified=True` alone says a
    signature checked out; what makes the verdict about THIS schedule is the
    referent identity the `Approval` controller recorded, and a schedule
    deleted and recreated under the same name reuses the name and not the UID.
    """
    status = approval.get("status") or {}
    subject = status.get("verifiedSubjectRef") or {}
    return {
        "the Approval is Verified=True":
            condition(approval, "Verified").get("status") == "True",
        "status.matchedKeyId names the key it verified under":
            bool(status.get("matchedKeyId")),
        "status.verifiedSubjectRef.uid is this RehearsalSchedule's own uid": (
            bool(subject.get("uid")) and subject.get("uid") == schedule_uid
        ),
        "and the recorded referent kind is RehearsalSchedule":
            subject.get("kind") == "RehearsalSchedule",
    }


def restore_is_created_on_the_standing_authorization(
    restore: dict[str, Any], schedule: str, approval: str
) -> dict[str, bool]:
    """Review §4 step 2 — **the 14.3b unblocking**.

    "A `Restore` labelled `logweir.dev/rehearsal-schedule` exists, carries
    `spec.authorization` (`kind: Standing`, `approvalRef`,
    `rehearsalScheduleRef`) and **no** `approvalRef`, and reaches
    `status.reason != ApprovalNotReceived` — this is the 14.3b unblocking, and
    before the branch it held here forever."

    `ApprovalNotReceived` is the terminal refusal an OLDER controller writes
    for a `Restore` whose `spec.approvalRef` is empty, which is exactly the
    documented rollback behaviour (`docs/kubernetes.md` §7g) — so on a build
    that predates PLAT-14.3b the last clause is FALSE, and that failure is the
    defect reproduced live rather than a harness fault.
    """
    spec = restore.get("spec") or {}
    authorization = spec.get("authorization") or {}
    labels = (restore.get("metadata") or {}).get("labels") or {}
    reason = (restore.get("status") or {}).get("reason")
    return {
        "the Restore is labelled logweir.dev/rehearsal-schedule with this schedule":
            labels.get("logweir.dev/rehearsal-schedule") == schedule,
        "spec.authorization.kind is Standing":
            authorization.get("kind") == "Standing",
        "spec.authorization.approvalRef names the standing Approval":
            (authorization.get("approvalRef") or {}).get("name") == approval,
        "spec.authorization.rehearsalScheduleRef names the schedule":
            (authorization.get("rehearsalScheduleRef") or {}).get("name") == schedule,
        "and it carries NO spec.approvalRef":
            not (spec.get("approvalRef") or {}).get("name"),
        "status.reason is not ApprovalNotReceived":
            reason != "ApprovalNotReceived",
    }


def the_job_carries_the_standing_mount(
    argv: list[str], env: dict[str, str], bundle_keys: set[str],
    schedule: str, slot: str, schedule_uid: str,
) -> dict[str, bool]:
    """Review §4 step 3: the argv, the five-member bundle and the env.

    Every clause is a NEGATIVE as well as a positive: `--approval` absent, the
    bundle exactly five members with neither `approval.json` nor
    `approval.sig`, and NEITHER per-run approval digest in the environment. A
    standing run that also carried the per-run slot would be the pre-14.3b
    "sits beside" shape the runner now refuses by name, and a row that only
    checked the additions would not notice it.
    """
    pairs = {argv[i]: argv[i + 1] for i in range(len(argv) - 1)}
    return {
        "argv carries --standing-authorization …/standing-authorization.json":
            pairs.get("--standing-authorization") == "/approval/standing-authorization.json",
        "argv carries --authorization-keys …/authorization-keys.json":
            pairs.get("--authorization-keys") == "/approval/authorization-keys.json",
        "argv carries --triggered-by rehearsal/<schedule>/<slot>":
            pairs.get("--triggered-by") == f"rehearsal/{schedule}/{slot}",
        "argv carries NO --approval":
            "--approval" not in argv,
        "the bundle ConfigMap has exactly the five standing members":
            bundle_keys == STANDING_BUNDLE_KEYS,
        "and neither approval.json nor approval.sig":
            not ({"approval.json", "approval.sig"} & bundle_keys),
        "LOGWEIR_EXECUTION_AUTHORIZATION_KIND is standing":
            env.get("LOGWEIR_EXECUTION_AUTHORIZATION_KIND") == "standing",
        "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID is the schedule's uid":
            env.get("LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID") == schedule_uid,
        "and neither approval sha env is set":
            not any(name in env for name in APPROVAL_DIGEST_ENV),
    }


def the_scorecard_names_the_schedule_and_the_slot(
    restore: dict[str, Any], scorecard: dict[str, Any], schedule: str, slot: str
) -> dict[str, bool]:
    """Review §4 step 4: `outcome=pass`, `evidence.verification.result=Valid`,
    and a signed scorecard whose `approval.approver` is
    `standing-authorization/<schedule>` and whose `triggered_by` is
    `rehearsal/<schedule>/<slot>`.

    THE APPROVER CLAUSE IS THE PRODUCT'S HONESTY, checked from the signed bytes
    rather than from a status field: v1.0.0 of the standing document carries no
    approver, and a scorecard naming a person would tell its reader that a
    human approved THIS run when what a human approved was a schedule.
    """
    status = restore.get("status") or {}
    verification = ((status.get("evidence") or {}).get("verification") or {})
    return {
        "the Restore's status.outcome is pass": status.get("outcome") == "pass",
        "status.evidence.verification.result is Valid":
            verification.get("result") == "Valid",
        "the signed scorecard was fetched from the evidence destination":
            bool(scorecard.get("run_id")),
        "its approval.approver is standing-authorization/<schedule>":
            (scorecard.get("approval") or {}).get("approver")
            == f"standing-authorization/{schedule}",
        "its approval.approver names no person (the ticket is empty)":
            (scorecard.get("approval") or {}).get("ticket") == "",
        "and its triggered_by is rehearsal/<schedule>/<slot>":
            scorecard.get("triggered_by") == f"rehearsal/{schedule}/{slot}",
    }


def the_schedule_records_the_pass(schedule: dict[str, Any],
                                  restore_name: str) -> dict[str, bool]:
    """Review §4 step 5: `status.lastSucceeded` with `restoreRef`, `at`,
    `evidence` and `rtoSeconds`; `RehearsalHealthy=True/Passed`;
    `activeRestoreRef` cleared.

    The cleared `activeRestoreRef` is not bookkeeping: `decide` reads it to
    answer the concurrency question, and a schedule that never released it
    would skip every subsequent slot with `ConcurrencyBlocked` for a rehearsal
    that finished.
    """
    status = schedule.get("status") or {}
    last = status.get("lastSucceeded") or {}
    healthy = condition(schedule, "RehearsalHealthy")
    return {
        "status.lastSucceeded.restoreRef names the rehearsal that ran":
            (last.get("restoreRef") or {}).get("name") == restore_name,
        "status.lastSucceeded.at is recorded": bool(last.get("at")),
        "status.lastSucceeded.evidence carries the verdict": bool(last.get("evidence")),
        "status.lastSucceeded.rtoSeconds is the measured recovery time":
            isinstance(last.get("rtoSeconds"), int),
        "RehearsalHealthy is True with reason Passed": (
            healthy.get("status") == "True" and healthy.get("reason") == "Passed"
        ),
        "and status.activeRestoreRef is cleared":
            not (status.get("activeRestoreRef") or {}).get("name"),
    }


def the_target_holds_exactly_the_mapped_topics(
    during: set[str], after: set[str], mapped: set[str], prefix: str, unrelated: str
) -> dict[str, bool]:
    """Review §4 step 6: "during the run the target holds exactly
    `rehearsal-<uid8>-<topic>`; after teardown it holds none of them, and
    `rehearsal-not-ours` still exists".

    "Exactly" is scoped to THIS SCHEDULE'S RENDERED PREFIX, and that is the
    only reading the fixture allows: the scratch broker is shared and carries
    the lab's own topics, so a row demanding an empty broker would fail for a
    reason that is the lab's and not the product's. Inside the prefix the
    claim is exact in both directions — every mapped name present during the
    run, and no other name under the prefix, which is what makes "the runner
    created these and only these" mean something.

    `rehearsal-not-ours` does not carry the rendered prefix (it has no
    `<uid8>-` segment), so it is outside the deleter's guard by construction —
    which is exactly why it is the right witness for "cleanup never touches an
    unrelated topic".
    """
    prefixed_during = {t for t in during if t.startswith(prefix)}
    prefixed_after = {t for t in after if t.startswith(prefix)}
    return {
        "during the run the target holds every mapped rehearsal-<uid8>-<topic>":
            bool(mapped) and mapped <= during,
        "and no other topic under this schedule's rendered prefix":
            prefixed_during == mapped,
        "after teardown it holds none of them": not prefixed_after,
        "the unrelated topic rehearsal-not-ours existed during the run":
            unrelated in during,
        "and it still exists afterwards — teardown never touched it":
            unrelated in after,
    }


def the_second_slot_is_concurrency_blocked(
    schedule: dict[str, Any], restores: list[str], first_restore: str
) -> dict[str, bool]:
    """Review §4 step 7: "a second slot while the first is active records
    `status.lastSkipped.reason=ConcurrencyBlocked` and creates no `Restore`".

    The control REQUIRES the skip: a schedule that recorded nothing, or that
    skipped for `TargetBusy` or `LeftoverTopics`, fails here. And it requires
    the second `Restore` to be ABSENT — "created no Restore" is the half that
    makes the reason mean something, because a skip recorded beside a second
    running rehearsal would be a status field disagreeing with the cluster.
    """
    status = schedule.get("status") or {}
    skipped = status.get("lastSkipped") or {}
    return {
        "status.lastSkipped.reason is ConcurrencyBlocked":
            skipped.get("reason") == "ConcurrencyBlocked",
        "the skip names the slot it refused": bool(skipped.get("slot")),
        "the first rehearsal is the one that was running":
            first_restore in restores,
        "and the second slot created no Restore": len(restores) == 1,
    }


def the_leftover_guard_refuses_and_keeps_the_topic(
    schedule: dict[str, Any], restore: dict[str, Any] | None,
    planted: str, topics_after: set[str],
) -> dict[str, bool]:
    """Review §4 step 8: "with a mapped name pre-created, the next slot records
    `LeftoverTopics` / `GuardRefused` and the pre-created topic is untouched".

    TWO ARMS, BOTH OF WHICH ARE REFUSALS, and the row accepts either because
    which one fires depends on where the leftover was seen from
    (`docs/kubernetes.md` §7g says so in as many words):

    * the CONTROLLER's, when a previous teardown ATTESTED the failure — the
      slot is skipped with `LeftoverTopics` and no `Restore` is created;
    * the RUNNER's phase 0, when nothing attested it — the `Restore` exists and
      is refused terminally with `GuardRefused`, because phase 0 refuses a
      mapped target topic that already exists.

    What the row does NOT accept is a rehearsal that ran: a `Restore` that
    reached `Succeeded`, or a pre-created topic that is gone, fails here. The
    controller deletes no topic ever, and the runner's prefix-scoped deleter
    must not adopt a name it did not create.
    """
    status = schedule.get("status") or {}
    skipped = (status.get("lastSkipped") or {}).get("reason")
    failed = (status.get("lastFailed") or {}).get("reason")
    restore_status = (restore or {}).get("status") or {}
    controller_arm = skipped == "LeftoverTopics" and restore is None
    runner_arm = (
        restore is not None
        and restore_status.get("phase") in {"Refused", "Failed"}
        and "GuardRefused" in {restore_status.get("reason"),
                               restore_status.get("exitReason"), failed}
    )
    return {
        "the slot is refused — LeftoverTopics on the schedule, or GuardRefused "
        "on its Restore": controller_arm or runner_arm,
        "no rehearsal succeeded against the pre-created name":
            restore_status.get("phase") != "Succeeded",
        "and the pre-created topic is untouched — it still exists":
            planted in topics_after,
    }


def the_evidence_outlives_the_job(
    job_gone: bool, fetched: dict[str, bool]
) -> dict[str, bool]:
    """Review §4 step 9: "scorecard, sidecar, offset report and teardown objects
    are still fetchable after the Job TTL".

    `job_gone` is the precondition the clause is about — asserting that four
    objects are readable while the Job is still there would prove nothing about
    retention — so it is a clause and not a guard.
    """
    clauses = {"the runner Job is gone after its TTL": job_gone}
    for key, ok in sorted(fetched.items()):
        clauses[f"`{key}` is still fetchable from the evidence destination"] = ok
    return clauses


def the_refused_arm_reaches_no_job(
    schedule: dict[str, Any], restores: list[dict[str, Any]],
    jobs: list[str], bundles: list[str],
) -> dict[str, bool]:
    """Review §4 step 10, the NEGATIVE CONTROL — and **it must REQUIRE the
    refusal**.

    "A second schedule whose standing `Approval` is expired (or whose key was
    retired between slots): the slot records a refusal, `RehearsalHealthy=False`,
    `lastFailed.reason` is `AuthorizationInvalid` (schedule-side) or the
    `Restore` reaches `StandingAuthorizationRefused` (reconciler-side), and
    **zero** Jobs and **zero** bundle ConfigMaps exist for it. A row that passes
    when the product does nothing is not a row."

    So the first clause is the one that cannot be satisfied by silence: a
    schedule that recorded no skip, no failure and no refused `Restore` fails
    here even though its Job count is zero — which is precisely the state a
    build that never reconciles this kind at all would leave behind, and
    precisely the state that made "zero Jobs" worthless as evidence.

    `RehearsalHealthy` is read as "not True": the condition is about the last
    FINISHED rehearsal, and an arm that was refused before any rehearsal ran
    has none, so `False` and an absent condition are both honest and `True`
    is not.
    """
    status = schedule.get("status") or {}
    skipped = (status.get("lastSkipped") or {}).get("reason")
    failed = (status.get("lastFailed") or {}).get("reason")
    named = {"AuthorizationInvalid", "AuthorizationExpired"}
    reconciler_side = any(
        ((r.get("status") or {}).get("reason") == "StandingAuthorizationRefused")
        for r in restores
    )
    return {
        "the refusal is RECORDED and NAMED — AuthorizationInvalid/Expired on "
        "the schedule, or StandingAuthorizationRefused on its Restore": (
            skipped in named or failed in named or reconciler_side
        ),
        "RehearsalHealthy does not claim a passing rehearsal":
            condition(schedule, "RehearsalHealthy").get("status") != "True",
        "zero runner Jobs exist for it": not jobs,
        "and zero approval-bundle ConfigMaps exist for it": not bundles,
    }


# --- the fixtures the ten steps run against ---------------------------------


def rehearsal_target_cluster() -> dict[str, Any]:
    """The lab's scratch broker, as a `KafkaCluster` in THIS namespace.

    A SECOND OBJECT AND NOT THE `source` ONE. D3 §4.4 requires the rehearsal
    target to differ from the point's own source cluster, and `decide` compares
    the target's REPORTED `status.clusterId` against the signed scope — never a
    `spec.role`, which is free-form and is not authority
    (`crds/kafka_cluster.rs:104`).
    """
    # ITS OWN CREDENTIAL. The scratch broker has its own SCRAM database, and
    # the lab's `target-scram` Secret is the one that authenticates against it —
    # `source-scram` is a different password and the cluster would simply report
    # `reachable: false`, which this phase would then read as D3 §4.4's
    # `TargetUnavailable` and blame the product for.
    copy_secret("target-scram")
    apply({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": owned(REHEARSAL_TARGET),
        "spec": {
            "bootstrapServers": [f"{TARGET_DEPLOY}.{FIXTURE_NS}.svc.cluster.local:9096"],
            "auth": {"mode": "scramSha512", "username": "scram-user",
                     "secretRef": {"name": "target-scram"}, "tls": False},
            "role": "target",
        },
    })
    return wait_for(
        "kafkacluster", REHEARSAL_TARGET,
        lambda o: (o.get("status") or {}).get("reachable") is True
        and bool((o.get("status") or {}).get("clusterId")),
        seconds=240, what="reachable: true with a clusterId",
    )


def rehearsal_trust(target_cluster_id: str, approver_key: dict[str, Any],
                    refused_key: dict[str, Any]) -> dict[str, Any]:
    """This namespace's `TrustPolicy`, rebuilt for the rehearsal.

    # Why it is DELETED and recreated rather than patched

    `trust` leaves the lab signing key **Revoked** on this object — that is its
    last row's whole point — and `spec.keys` is append-only with no
    Revoked→Active transition, so a rehearsal that inherited it would fail at
    the evidence verdict for a reason belonging to the previous phase. Deleting
    and recreating is the same remedy `old_archive` uses two phases earlier and
    for the same CRD rule, and the object is this run's own (`{OWNER}-{STAMP}`,
    binding only this namespace).

    # CLUSTER-SCOPED — take the orchestration lock around this phase

    `TrustPolicy` is cluster-scoped. It binds only `spec.namespaces: [NS]`, so
    it governs nothing outside this run, but creating it is still a
    cluster-scoped write and the README lists `rehearsal` beside `trust`,
    `signed-at-probe` and `cleanup` for that reason.

    # The three keys, and why the third is Retired

    * the lab SIGNING key, Active/`EvidenceSigning` — the catalog sync Job
      verifies the point's receipt under it, and an unverified point is not
      selectable, so without this entry step 1 fails at the point and not at
      the authorization;
    * an APPROVER key minted by this run, Active/`GovernedApproval` — what the
      standing document is signed with. `EvidenceSigning` is refused for this
      role by D3 §7.3, so the usage is not decoration. **It is minted and not
      the lab roster's own approver key**, because signing needs the PRIVATE
      half and the lab's lives under `/tmp/logweir-scram-e2e`, which macOS
      deletes after three untouched days (WORKER-RULES, host notes) — measured
      on 2026-09-22, when only `approver.pub.pem` was left. A row that cannot
      run because the operating system tidied a fixture is a row that proves
      nothing, and minting costs one `openssl` call;
    * a SECOND approver key minted by this run, **Retired**/`GovernedApproval`
      — review §4 step 10's "whose key was retired between slots". It is a
      separate key so the refused arm costs the passing arm nothing: retiring
      the one the passing arm signs with would refuse every schedule in the
      namespace and the control would prove only that the harness broke its own
      fixture.
    """
    if get_opt("trustpolicy", TRUST_POLICY, namespace="default") is not None:
        run(K + ["delete", "trustpolicy", TRUST_POLICY, "--wait=true"], check=False)
    signing = roster_signing_key()
    body = trust_policy(
        "Active",
        keys=[
            policy_key(signing["keyId"], signing["spkiPem"], "Active",
                       display="the lab signing key"),
            policy_key(approver_key["keyId"], approver_key["spkiPem"], "Active",
                       display=f"{OWNER}'s rehearsal approver key",
                       subject=f"{OWNER}-approver@logweir.invalid",
                       usages=["GovernedApproval"]),
            policy_key(refused_key["keyId"], refused_key["spkiPem"], "Retired",
                       display=f"{OWNER}'s retired approver key",
                       subject=f"{OWNER}-retired-approver@logweir.invalid",
                       usages=["GovernedApproval"],
                       retiredAt=now()),
        ],
    )
    # D3 §4.4: "the target cluster id must be in the bound TrustPolicy's
    # allowedTargetClusterIds". EXACTLY the one id, so the field is a bound and
    # not a formality.
    body["spec"]["allowedTargetClusterIds"] = [target_cluster_id]
    return apply(body)


def rehearsal_point() -> dict[str, Any]:
    """One qualifying recovery point, and the view that makes it selectable.

    BOTH HALVES ARE REQUIRED, and neither is enough on its own
    (`controllers/rehearsal_schedule.rs::candidates`):

    * the `Backup` supplies the point's TOPIC SET — a catalog entry records
      none, and `select_point` refuses a candidate whose own topic set nobody
      recorded rather than assuming the subset claim;
    * the CATALOG decides SELECTABILITY, because availability and the signature
      verdict are what it actually measured by listing the archive.

    The `Backup` is a manual run OF a `BackupSchedule` (`spec.scheduleRef`),
    because `spec.point.scheduleRefs` is how the schedule names its candidates
    and a manual `Backup` with no such reference is in nobody's candidate set.
    """
    apply(schedule_object(REHEARSAL_POINT_SCHEDULE, "dest-a"))
    ref = schedule_ref(REHEARSAL_POINT_SCHEDULE)
    backup = run_backup("l6-point", "dest-a", topics=[REHEARSAL_TOPIC], schedule=ref)
    catalog = fresh_catalog(REHEARSAL_CATALOG, "dest-a")
    entries = view_entries(catalog)
    point_id = None
    receipt = ((backup.get("status") or {}).get("evidence") or {}).get("receiptSha256") or ""
    if receipt.startswith("sha256:") and len(receipt) >= 39:
        point_id = f"lwp1-{receipt[len('sha256:'):][:32]}"
    entry = next((e for e in entries if e.get("pointId") == point_id), None)
    return {"backup": backup_facts(backup), "pointId": point_id,
            "entry": entry, "entries": len(entries),
            "selectable": bool(entry and entry.get("selectable") is True)}


def rehearsal_schedule_object(name: str, *, cron: str, approval: str,
                              suspend: bool = True) -> dict[str, Any]:
    """D3 §4.1's `RehearsalSchedule`, created SUSPENDED.

    SUSPENDED AT BIRTH, AND THAT IS THE ONLY ORDER THAT WORKS. The standing
    document binds `subjectRef.uid` and `scope.templateDigest`, and neither
    exists until the object does — the runbook's own `--schedule-uid
    $(kubectl … -o jsonpath='{.metadata.uid}')` says so. `suspend` is the one
    mutable field precisely so this is legal: the digest covers `spec` minus
    `suspend`, so unsuspending later does not invalidate the document.
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "RehearsalSchedule",
        "metadata": owned(name),
        "spec": {
            "schedule": cron,
            "suspend": suspend,
            "point": {
                "scheduleRefs": [{"name": REHEARSAL_POINT_SCHEDULE}],
                "catalogRef": {"name": REHEARSAL_CATALOG},
                "selection": "NewestAvailable",
                "minAgeSeconds": 0,
                "topics": [REHEARSAL_TOPIC],
                "requireVerifiedEvidence": True,
            },
            "target": {
                "clusterRef": {"name": REHEARSAL_TARGET},
                "topicPrefix": "rehearsal-",
                "markerTopic": "logweir.scratch",
                "replicationFactor": 1,
            },
            "bounds": {
                "concurrencyPolicy": "Forbid",
                "deadlineSeconds": 900,
                "startingDeadlineSeconds": 3600,
                "recordsPerPartition": 25,
                "maxPartitions": 200,
            },
            "objectives": {"rtoSeconds": 1800, "passRate": 1.0},
            "authorization": {"standingApprovalRef": {"name": approval}},
        },
    }


def mint_standing(work: pathlib.Path, key: pathlib.Path, schedule: dict[str, Any],
                  *, target_cluster_id: str, valid_days: int = 30) -> tuple[str, str]:
    """`logweir drill approve --standing`, the shipped signer, over a scope file.

    NEVER SIGNED IN PYTHON. PLAT-14.3b's fix round 1 closed P0 — that nothing
    in the product minted a `StandingRehearsalAuthorization` — with this exact
    command, and a harness that hand-rolled DSSE here would prove the cluster
    accepts bytes the harness can make rather than bytes the product makes.
    The flags are the runbook's (`docs/kubernetes.md` §7g).

    The scope is a FILE because that is what the signature covers, and the
    values are read back off the live objects: the rendered prefix carries the
    schedule's own uid, `templateDigest` is the digest the controller
    published, and the target cluster id is the one the `KafkaCluster`
    reported.
    """
    uid = schedule["metadata"]["uid"]
    spec = schedule["spec"]
    scope = {
        "templateDigest": schedule["status"]["templateDigest"],
        "targetClusterId": target_cluster_id,
        "topicPrefix": rendered_prefix(uid),
        "topics": spec["point"]["topics"],
        "maxPartitions": spec["bounds"]["maxPartitions"],
        "recordsPerPartition": spec["bounds"]["recordsPerPartition"],
        "deadlineSeconds": spec["bounds"]["deadlineSeconds"],
        "modes": ["scratch"],
    }
    scope_path = work / f"scope-{schedule['metadata']['name']}.json"
    scope_path.write_text(json.dumps(scope, indent=2, sort_keys=True) + "\n")
    out = work / f"standing-{schedule['metadata']['name']}.json"
    run([logweir_cli(), "drill", "approve", "--standing",
         "--key", str(key),
         "--schedule-namespace", NS,
         "--schedule-name", schedule["metadata"]["name"],
         "--schedule-uid", uid,
         "--scope", str(scope_path),
         "--valid-days", str(valid_days),
         "--out", str(out)], timeout=180)
    return out.read_text(), out.with_suffix(".sig").read_text()


def standing_approval_object(name: str, schedule: dict[str, Any],
                             envelope: str, sidecar: str) -> dict[str, Any]:
    """The immutable `Approval` that transports the signed document.

    `spec.planHash` is the schedule's `status.templateDigest` (D3 §4.3's
    "transport" paragraph), and `spec.subjectRef.kind` is `RehearsalSchedule`
    — the enum value D3 §4.5 added. The envelope and sidecar are DOCUMENT TEXT
    and never base64 (`docs/kubernetes.md` §8).
    """
    return {
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": owned(name),
        "spec": {
            "approvalBytes": envelope,
            "sidecarBytes": sidecar,
            "planHash": schedule["status"]["templateDigest"],
            "subjectRef": {"kind": "RehearsalSchedule",
                           "name": schedule["metadata"]["name"]},
        },
    }


def rehearsal_restores(schedule: str) -> list[dict[str, Any]]:
    """Every `Restore` this schedule created, by its own label."""
    return sorted(
        lst("restores", selector=f"logweir.dev/rehearsal-schedule={schedule}"),
        key=lambda o: o["metadata"]["name"],
    )


def rehearsal_bundles(restores: list[dict[str, Any]]) -> list[str]:
    """The approval-bundle ConfigMaps that exist for these Restores."""
    return [
        f"{r['metadata']['name']}-approval-bundle"
        for r in restores
        if get_opt("configmap", f"{r['metadata']['name']}-approval-bundle") is not None
    ]


def job_facts(restore: dict[str, Any]) -> dict[str, Any]:
    """The runner Job's argv and env, and the bundle's key set.

    READ FROM THE JOB AND FROM THE POD. The Job is what the controller POSTed;
    the pod is what the kubelet ran. A row that read only one of them could not
    tell a template from an execution, and D3 §15's evidence list asks for the
    pod's argv and env by name.
    """
    name = ((restore.get("status") or {}).get("jobRef") or {}).get("name")
    if not name:
        return {"job": None}
    job = get_opt("job", name)
    container = (((job or {}).get("spec") or {}).get("template") or {}).get(
        "spec", {}).get("containers", [{}])[0]
    argv = list(container.get("command") or []) + list(container.get("args") or [])
    env = {e["name"]: e.get("value", "<fieldRef/secretRef>")
           for e in container.get("env") or []}
    pods = lst("pods", selector=f"batch.kubernetes.io/job-name={name}")
    pod_container = ((pods[0].get("spec") if pods else {}) or {}).get(
        "containers", [{}])[0] if pods else {}
    pod_argv = list(pod_container.get("command") or []) + list(pod_container.get("args") or [])
    pod_env = {e["name"]: e.get("value", "<fieldRef/secretRef>")
               for e in pod_container.get("env") or []}
    bundle = get_opt("configmap", f"{restore['metadata']['name']}-approval-bundle")
    keys = set((bundle or {}).get("data", {}) or {}) | set(
        (bundle or {}).get("binaryData", {}) or {})
    return {"job": name, "argv": argv, "env": env, "podArgv": pod_argv, "podEnv": pod_env,
            "bundleKeys": sorted(keys), "pod": pods[0]["metadata"]["name"] if pods else None}


def fetchable(bucket: str, key: str) -> bool:
    """Whether one object can still be READ, not merely listed.

    `mc cat`, because D3 §15 L6 says *fetchable*: a listing answers "the index
    says it is there", and after a retention run those are different answers.
    """
    if not key:
        return False
    out = mc("cat", f"local/{bucket}/{key}", check_rc=False)
    return bool(out.strip())


def rehearsal_first_restore(schedule: str, *, seconds: int) -> dict[str, Any]:
    """The `Restore` a schedule's first slot creates, waited for by its label."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        found = rehearsal_restores(schedule)
        if found:
            return found[0]
        time.sleep(5)
    dump = artifact(f"rehearsal/no-restore-{schedule}.json",
                    get_opt("rehearsalschedule", schedule) or {})
    raise RuntimeError(f"no rehearsal Restore for {schedule} within {seconds}s: {dump}")


def rehearsal() -> None:
    """D3 §15's **L6**, as ten rows over one live rehearsal.

    Read `claude/plat14-3b.review.md` §4 beside this: every clause is that
    specification's own sentence, and none of them was weakened to suit the
    build the lab is running. Steps 2–9 form a chain, so a step whose input the
    previous step never produced is recorded **NOT-REACHED**.

    CLUSTER LOCK: this phase writes the cluster-scoped `TrustPolicy` (see
    `rehearsal_trust`). It creates and deletes topics under its own
    `rehearsal-` names on the lab's scratch broker and changes nothing else
    about the shared release.
    """
    evidence: list[str] = []
    reached: dict[str, Any] = {}
    work = pathlib.Path(tempfile.mkdtemp(prefix=f"{OWNER}-l6-", dir="/tmp"))
    work.chmod(0o700)
    approver_key = mint_signing_key(f"{OWNER}-l6-approver")
    refused_key = mint_signing_key(f"{OWNER}-l6-retired")
    planted: list[str] = []

    def unreached(step: str, task: str, why: str) -> None:
        record(step, task, "NOT-REACHED", why, evidence)

    try:
        # ---- fixtures ----------------------------------------------------
        target = rehearsal_target_cluster()
        target_cluster_id = target["status"]["clusterId"]
        rehearsal_trust(target_cluster_id, approver_key, refused_key)
        point = rehearsal_point()
        target_topic_create(REHEARSAL_UNRELATED_TOPIC)
        planted.append(REHEARSAL_UNRELATED_TOPIC)
        evidence.append(artifact("rehearsal/00-fixtures.json", {
            "targetClusterId": target_cluster_id,
            "sourceClusterId": STATE.get("sourceClusterId"),
            "point": point,
            "trustPolicy": TRUST_POLICY,
            "approverKeyId": approver_key["keyId"],
            "retiredApproverKeyId": refused_key["keyId"],
            "unrelatedTopic": REHEARSAL_UNRELATED_TOPIC,
        }))

        # ---- step 1: setup, and the standing Approval verifies -------------
        approval_name = f"{REHEARSAL_SCHEDULE}-standing"
        apply(rehearsal_schedule_object(REHEARSAL_SCHEDULE, cron=REHEARSAL_CRON,
                                        approval=approval_name))
        schedule = wait_for(
            "rehearsalschedule", REHEARSAL_SCHEDULE,
            lambda o: bool((o.get("status") or {}).get("templateDigest")),
            seconds=300, what="status.templateDigest, which the document signs",
        )
        schedule_uid = schedule["metadata"]["uid"]
        envelope, sidecar = mint_standing(work, approver_key["private"], schedule,
                                          target_cluster_id=target_cluster_id)
        apply(standing_approval_object(approval_name, schedule, envelope, sidecar))
        approval = wait_for(
            "approval", approval_name,
            lambda o: bool(condition(o, "Verified").get("status")),
            seconds=300, what="the Verified condition to be decided",
        )
        setup = standing_approval_is_verified(approval, schedule_uid)
        setup["a qualifying catalog point is selectable"] = point["selectable"]
        setup[f"the unrelated topic {REHEARSAL_UNRELATED_TOPIC} is pre-created on the target"] = (
            REHEARSAL_UNRELATED_TOPIC in target_topics()
        )
        evidence.append(artifact("rehearsal/01-approval.json",
                                 {"approval": approval, "schedule": schedule,
                                  "clauses": setup}))
        setup_ok = check(
            "rehearsal-1-setup-standing-approval-verifies",
            "PLAT-14.3",
            all(setup.values()),
            f"a RehearsalSchedule on a {REHEARSAL_CRON} cron (uid {schedule_uid}), a qualifying "
            f"catalog point ({point['pointId']}, selectable={point['selectable']}), and an "
            f"Approval whose spec.subjectRef is that schedule carrying the standing envelope "
            f"minted by `logweir drill approve --standing` plus its sidecar: Verified="
            f"{condition(approval, 'Verified').get('status')}/"
            f"{condition(approval, 'Verified').get('reason')}, matchedKeyId "
            f"{str((approval.get('status') or {}).get('matchedKeyId'))[:16]}…, "
            f"verifiedSubjectRef.uid "
            f"{((approval.get('status') or {}).get('verifiedSubjectRef') or {}).get('uid')}. "
            + "; ".join(f"{k}={v}" for k, v in setup.items()),
            evidence,
        )

        # ---- step 2: the Restore is created on `spec.authorization` --------
        first_name = None
        step2_ok = False
        restore: dict[str, Any] = {}
        if not setup_ok:
            unreached("rehearsal-2-restore-on-spec-authorization", "PLAT-14.3",
                      "step 1 did not produce a verified standing Approval, so no slot could "
                      "fire and there is no Restore to read; this is NOT a pass and NOT a "
                      "failure of the clause")
        else:
            run(KN + ["patch", "rehearsalschedule", REHEARSAL_SCHEDULE, "--type=merge",
                      "-p", json.dumps({"spec": {"suspend": False}})])
            first = rehearsal_first_restore(REHEARSAL_SCHEDULE, seconds=900)
            first_name = first["metadata"]["name"]
            restore = wait_for(
                "restore", first_name,
                lambda o: bool(((o.get("status") or {}).get("jobRef") or {}).get("name"))
                or bool((o.get("status") or {}).get("reason"))
                or (o.get("status") or {}).get("phase") in {"Running", "Succeeded",
                                                            "Failed", "Refused"},
                seconds=420, what="admission to be decided (a Job, a reason or a phase)",
            )
            clauses = restore_is_created_on_the_standing_authorization(
                restore, REHEARSAL_SCHEDULE, approval_name)
            evidence.append(artifact("rehearsal/02-restore.json",
                                     {"restore": restore, "clauses": clauses}))
            step2_ok = check(
                "rehearsal-2-restore-on-spec-authorization",
                "PLAT-14.3",
                all(clauses.values()),
                f"the schedule created {first_name} on spec.authorization with no approvalRef; "
                f"phase {(restore.get('status') or {}).get('phase')!r}, reason "
                f"{(restore.get('status') or {}).get('reason')!r}, jobRef "
                f"{((restore.get('status') or {}).get('jobRef') or {}).get('name')!r}. THIS IS "
                f"THE PLAT-14.3b UNBLOCKING: on a controller that predates it the object holds "
                f"at ApprovalNotReceived — the documented fail-closed rollback for an older "
                f"controller reading a standing-authorized Restore — and this row FAILS, which "
                f"is the defect reproduced live. "
                + "; ".join(f"{k}={v}" for k, v in clauses.items()),
                evidence,
            )
        reached["step2"] = step2_ok

        # ---- steps 3-6 and 9: the Job, the scorecard, the schedule, topics -
        chain = ["rehearsal-3-job-carries-the-standing-mount",
                 "rehearsal-4-scorecard-names-the-schedule",
                 "rehearsal-5-schedule-records-the-pass",
                 "rehearsal-6-topics-owned-torn-down-unrelated-survives",
                 "rehearsal-9-evidence-outlives-the-job-ttl"]
        if not step2_ok:
            for step in chain:
                unreached(step, "PLAT-14.3",
                          "the rehearsal Restore was never admitted (step 2), so no Job, no "
                          "scorecard, no rehearsalLast* and no mapped topic exists to read")
        else:
            slot = (restore["metadata"].get("labels") or {}).get("logweir.dev/rehearsal-slot", "")
            during: set[str] = set(target_topics())
            facts: dict[str, Any] = {"job": None}

            def watch(obj: dict[str, Any]) -> bool:
                during.update(target_topics())
                if facts.get("job") is None:
                    facts.update(job_facts(obj))
                return terminal(obj)

            final = wait_for("restore", first_name, watch, seconds=1500,
                             what="the rehearsal to reach a terminal phase")
            evidence.append(artifact("rehearsal/03-job.json", facts))
            job_clauses = the_job_carries_the_standing_mount(
                facts.get("argv") or [], facts.get("env") or {},
                set(facts.get("bundleKeys") or []), REHEARSAL_SCHEDULE, slot, schedule_uid)
            check(
                "rehearsal-3-job-carries-the-standing-mount",
                "PLAT-14.3",
                all(job_clauses.values()),
                f"Job {facts.get('job')!r} (pod {facts.get('pod')!r}) for slot {slot}: argv "
                f"carries --standing-authorization and --authorization-keys and no --approval, "
                f"the bundle ConfigMap {first_name}-approval-bundle holds "
                f"{facts.get('bundleKeys')}, and the env sets "
                f"LOGWEIR_EXECUTION_AUTHORIZATION_KIND="
                f"{(facts.get('env') or {}).get('LOGWEIR_EXECUTION_AUTHORIZATION_KIND')!r} with "
                f"neither per-run approval digest. "
                + "; ".join(f"{k}={v}" for k, v in job_clauses.items()),
                evidence,
            )

            # step 4 — the signed scorecard, read back from the destination
            ev = (final.get("status") or {}).get("evidence") or {}
            scorecard_key = ev.get("scorecardKey") or ""
            run_id = scorecard_key.rsplit("/", 1)[-1].removesuffix(".json")
            scorecard: dict[str, Any] = {}
            if scorecard_key:
                try:
                    scorecard = json.loads(cat(BUCKET_A, scorecard_key))
                except Exception:  # noqa: BLE001 - recorded as an empty scorecard
                    scorecard = {}
            evidence.append(artifact("rehearsal/04-scorecard.json",
                                     {"key": scorecard_key, "scorecard": scorecard,
                                      "restoreStatus": final.get("status")}))
            card = the_scorecard_names_the_schedule_and_the_slot(
                final, scorecard, REHEARSAL_SCHEDULE, slot)
            check(
                "rehearsal-4-scorecard-names-the-schedule",
                "PLAT-14.3",
                all(card.values()),
                f"outcome {(final.get('status') or {}).get('outcome')!r}, verification "
                f"{((ev.get('verification') or {}).get('result'))!r}, scorecard {scorecard_key!r} "
                f"with approval.approver "
                f"{(scorecard.get('approval') or {}).get('approver')!r} and triggered_by "
                f"{scorecard.get('triggered_by')!r}: what a human signed was a SCHEDULE, and the "
                f"signed evidence says so instead of naming a person. "
                + "; ".join(f"{k}={v}" for k, v in card.items()),
                evidence,
            )

            # step 5 — `rehearsalLast*` on the schedule
            after_schedule = wait_for(
                "rehearsalschedule", REHEARSAL_SCHEDULE,
                lambda o: bool(((o.get("status") or {}).get("lastSucceeded") or {}).get("at"))
                or bool(((o.get("status") or {}).get("lastFailed") or {}).get("at")),
                seconds=420, what="the schedule to record the finished rehearsal",
            )
            last = the_schedule_records_the_pass(after_schedule, first_name)
            evidence.append(artifact("rehearsal/05-schedule-status.json",
                                     {"status": after_schedule.get("status"),
                                      "clauses": last}))
            check(
                "rehearsal-5-schedule-records-the-pass",
                "PLAT-14.3",
                all(last.values()),
                f"status.lastSucceeded="
                f"{json.dumps((after_schedule.get('status') or {}).get('lastSucceeded'))}, "
                f"RehearsalHealthy="
                f"{condition(after_schedule, 'RehearsalHealthy').get('status')}/"
                f"{condition(after_schedule, 'RehearsalHealthy').get('reason')}, "
                f"activeRestoreRef="
                f"{json.dumps((after_schedule.get('status') or {}).get('activeRestoreRef'))}. "
                + "; ".join(f"{k}={v}" for k, v in last.items()),
                evidence,
            )

            # step 6 — the topics, during and after
            after_topics = target_topics()
            prefix = rendered_prefix(schedule_uid)
            mapped = {mapped_topic(schedule_uid, REHEARSAL_TOPIC)}
            topics = the_target_holds_exactly_the_mapped_topics(
                during, after_topics, mapped, prefix, REHEARSAL_UNRELATED_TOPIC)
            evidence.append(artifact("rehearsal/06-topics.json", {
                "prefix": prefix, "mapped": sorted(mapped),
                "during": sorted(during), "after": sorted(after_topics),
                "clauses": topics,
            }))
            check(
                "rehearsal-6-topics-owned-torn-down-unrelated-survives",
                "PLAT-14.3",
                all(topics.values()),
                f"the rendered prefix is {prefix} (unique per schedule object, "
                f"<prefix><uid[..8]>-); during the run the target held "
                f"{sorted(t for t in during if t.startswith(prefix))} and afterwards "
                f"{sorted(t for t in after_topics if t.startswith(prefix))}; "
                f"{REHEARSAL_UNRELATED_TOPIC} survives. The controller deletes no topic ever — "
                f"teardown is the runner's phase 9 inside its prefix-scoped guard. "
                + "; ".join(f"{k}={v}" for k, v in topics.items()),
                evidence,
            )

            # step 9 — the evidence outlives the Job
            job_name = facts.get("job")
            job_gone = False
            deadline = time.time() + 600
            while time.time() < deadline:
                if job_name and get_opt("job", job_name) is None:
                    job_gone = True
                    break
                time.sleep(10)
            keys = {
                "scorecard": scorecard_key,
                "sidecar": f"logweir/drills/{run_id}.sig" if run_id else "",
                "offset report": ev.get("offsetReportKey") or "",
                "teardown attestation": f"logweir/drills/{run_id}.teardown.json" if run_id else "",
            }
            fetched = {name: fetchable(BUCKET_A, key) for name, key in keys.items()}
            retention = the_evidence_outlives_the_job(job_gone, fetched)
            evidence.append(artifact("rehearsal/09-retention.json",
                                     {"job": job_name, "jobGone": job_gone, "keys": keys,
                                      "fetched": fetched, "clauses": retention}))
            check(
                "rehearsal-9-evidence-outlives-the-job-ttl",
                "PLAT-14.3",
                all(retention.values()),
                f"after Job {job_name!r} was collected by its TTL (gone={job_gone}) the four "
                f"signed objects under logweir/drills/ are still fetchable from "
                f"{BUCKET_A}: {json.dumps(fetched)}. Rehearsal evidence is never deleted "
                f"(D3 §4.4). "
                + "; ".join(f"{k}={v}" for k, v in retention.items()),
                evidence,
            )

        # ---- step 7: a second slot while the first is active ---------------
        if not step2_ok:
            unreached("rehearsal-7-second-slot-is-concurrency-blocked", "PLAT-14.3",
                      "a rehearsal never occupied the schedule (step 2), so no second slot "
                      "could find one active and ConcurrencyBlocked cannot be observed")
        else:
            conc_ok = rehearsal_arm_concurrency(work, target_cluster_id,
                                                approver_key, evidence)
            reached["step7"] = conc_ok

        # ---- step 8: the leftover guard ------------------------------------
        if not step2_ok:
            unreached("rehearsal-8-leftover-guard-keeps-the-pre-created-topic", "PLAT-14.3",
                      "no rehearsal reached a target (step 2), so a pre-created mapped name "
                      "has nothing to refuse")
        else:
            rehearsal_arm_leftover(work, target_cluster_id, approver_key,
                                   planted, evidence)

        # ---- step 10: the refused arm — the negative control ---------------
        rehearsal_arm_refused(work, target_cluster_id, refused_key, evidence)

        STATE["rehearsal"] = {
            "schedule": REHEARSAL_SCHEDULE, "scheduleUid": schedule_uid,
            "approval": approval_name, "restore": first_name,
            "targetClusterId": target_cluster_id, "point": point,
            "reached": reached, "at": now(),
        }
        save()
    finally:
        for topic in planted:
            target_topic_delete(topic)
        for key in (approver_key, refused_key):
            key["private"].unlink(missing_ok=True)
            shutil.rmtree(key["dir"], ignore_errors=True)
        shutil.rmtree(work, ignore_errors=True)
        gone = not any(k["private"].exists() or k["dir"].exists()
                       for k in (approver_key, refused_key))
        evidence.append(artifact("rehearsal/99-cleanup.json", {
            "topicsDeleted": planted,
            "topicsOnTargetAfter": sorted(target_topics()),
            "approverKeyId": approver_key["keyId"],
            "retiredApproverKeyId": refused_key["keyId"],
            "privateKeyFilesExist": [k["private"].exists()
                                     for k in (approver_key, refused_key)],
            "workDirExists": work.exists(),
        }))
        check(
            "rehearsal-minted-private-keys-never-outlive-the-row",
            "PLAT-14.3",
            gone and not work.exists(),
            f"both approver keys this phase minted — the Active one it signs with and the "
            f"Retired one the refused arm signs with — are gone from disk, the scope/envelope "
            f"working directory is gone ({work.exists()}), and every topic this phase created "
            f"on the shared scratch broker is deleted: {planted}. What is recorded is the "
            f"public SPKI and the key id",
            evidence,
        )


def rehearsal_arm(name: str, *, cron: str, key: pathlib.Path, work: pathlib.Path,
                  target_cluster_id: str) -> tuple[dict[str, Any], str]:
    """One arm's `RehearsalSchedule` and its standing `Approval`, both created
    and the `Approval` waited on until its `Verified` condition is DECIDED.

    Decided, not True: step 10's arm is signed by a RETIRED key and the whole
    point is that the cluster refuses it, so waiting for `True` there would be
    waiting for the control to fail.
    """
    approval = f"{name}-standing"
    apply(rehearsal_schedule_object(name, cron=cron, approval=approval))
    schedule = wait_for(
        "rehearsalschedule", name,
        lambda o: bool((o.get("status") or {}).get("templateDigest")),
        seconds=300, what="status.templateDigest",
    )
    envelope, sidecar = mint_standing(work, key, schedule,
                                      target_cluster_id=target_cluster_id)
    apply(standing_approval_object(approval, schedule, envelope, sidecar))
    wait_for("approval", approval,
             lambda o: bool(condition(o, "Verified").get("status")),
             seconds=300, what="the Verified condition to be decided")
    return schedule, approval


def unsuspend(kind: str, name: str) -> None:
    run(KN + ["patch", kind, name, "--type=merge", "-p",
              json.dumps({"spec": {"suspend": False}})])


def rehearsal_arm_concurrency(work: pathlib.Path, target_cluster_id: str,
                              approver_key: dict[str, Any],
                              evidence: list[str]) -> bool:
    """Review §4 step 7 — a second slot arriving while the first is active.

    ITS OWN SCHEDULE, AT A ONE-MINUTE CADENCE, and `REHEARSAL_FAST_CRON` says
    why: the clause is about `decide`'s concurrency branch, which does not read
    the cron, and at ten minutes the second slot's arrival inside a run that
    takes two or three minutes is a coin toss rather than a row.
    """
    name = REHEARSAL_CONCURRENCY_SCHEDULE
    schedule, _ = rehearsal_arm(name, cron=REHEARSAL_FAST_CRON,
                                key=approver_key["private"], work=work,
                                target_cluster_id=target_cluster_id)
    unsuspend("rehearsalschedule", name)
    first = rehearsal_first_restore(name, seconds=420)["metadata"]["name"]
    skipped: dict[str, Any] = {}
    names: list[str] = [first]
    live: dict[str, Any] = {}
    deadline = time.time() + 480
    while time.time() < deadline:
        live = get("rehearsalschedule", name)
        skipped = (live.get("status") or {}).get("lastSkipped") or {}
        names = [r["metadata"]["name"] for r in rehearsal_restores(name)]
        if skipped.get("reason"):
            break
        if terminal(get("restore", first)):
            # THE PRECONDITION NEVER HELD. The first rehearsal finished before a
            # second slot arrived, so "a second slot WHILE the first is active"
            # was never observed. That is NOT-REACHED: recording it as a pass
            # would credit the product for a branch nothing entered, and as a
            # failure would blame it for the harness's timing.
            record("rehearsal-7-second-slot-is-concurrency-blocked", "PLAT-14.3",
                   "NOT-REACHED",
                   f"the rehearsal {first} on {name} reached a terminal phase before a second "
                   f"slot arrived, so no slot ever found it active; the ConcurrencyBlocked "
                   f"branch was not entered and this run observed nothing about it",
                   evidence)
            return False
        time.sleep(5)
    clauses = the_second_slot_is_concurrency_blocked(live or schedule, names, first)
    evidence.append(artifact("rehearsal/07-concurrency.json",
                             {"schedule": (live or schedule).get("status"),
                              "restores": names, "clauses": clauses}))
    return check(
        "rehearsal-7-second-slot-is-concurrency-blocked",
        "PLAT-14.3",
        all(clauses.values()),
        f"with {first} still running, the next slot on {name} recorded "
        f"status.lastSkipped={json.dumps(skipped)} and the schedule still has exactly "
        f"{len(names)} Restore(s) ({names}). spec.bounds.concurrencyPolicy is Forbid and the "
        f"reservation protocol is what makes the skip and the cluster agree. "
        + "; ".join(f"{k}={v}" for k, v in clauses.items()),
        evidence,
    )


def rehearsal_arm_leftover(work: pathlib.Path, target_cluster_id: str,
                           approver_key: dict[str, Any],
                           planted: list[str], evidence: list[str]) -> bool:
    """Review §4 step 8 — a mapped name pre-created, and it must be untouched.

    The topic is created BEFORE the schedule is unsuspended, so the first slot
    this arm ever runs meets it. Which refusal fires — the controller's
    `LeftoverTopics` or the runner's phase-0 `GuardRefused` — depends on
    whether a previous teardown ATTESTED the leftover, and
    `the_leftover_guard_refuses_and_keeps_the_topic` accepts either; what it
    does not accept is a rehearsal that ran, or a pre-created topic that is
    gone.
    """
    name = REHEARSAL_LEFTOVER_SCHEDULE
    schedule, _ = rehearsal_arm(name, cron=REHEARSAL_FAST_CRON,
                                key=approver_key["private"], work=work,
                                target_cluster_id=target_cluster_id)
    uid = schedule["metadata"]["uid"]
    topic = mapped_topic(uid, REHEARSAL_TOPIC)
    target_topic_create(topic)
    planted.append(topic)
    unsuspend("rehearsalschedule", name)
    live: dict[str, Any] = schedule
    restore: dict[str, Any] | None = None
    deadline = time.time() + 600
    while time.time() < deadline:
        live = get("rehearsalschedule", name)
        status = live.get("status") or {}
        found = rehearsal_restores(name)
        restore = found[0] if found else None
        if (status.get("lastSkipped") or {}).get("reason") == "LeftoverTopics":
            break
        if restore is not None and terminal(restore):
            break
        time.sleep(5)
    after = target_topics()
    clauses = the_leftover_guard_refuses_and_keeps_the_topic(live, restore, topic, after)
    evidence.append(artifact("rehearsal/08-leftover.json", {
        "plantedTopic": topic, "schedule": live.get("status"),
        "restore": restore, "topicsAfter": sorted(after), "clauses": clauses,
    }))
    return check(
        "rehearsal-8-leftover-guard-keeps-the-pre-created-topic",
        "PLAT-14.3",
        all(clauses.values()),
        f"with the mapped name {topic} pre-created on the target, {name}'s slot recorded "
        f"lastSkipped={json.dumps((live.get('status') or {}).get('lastSkipped'))} / "
        f"lastFailed={json.dumps((live.get('status') or {}).get('lastFailed'))} and its "
        f"Restore {((restore or {}).get('metadata') or {}).get('name')!r} is phase "
        f"{((restore or {}).get('status') or {}).get('phase')!r} / reason "
        f"{((restore or {}).get('status') or {}).get('reason')!r}; the pre-created topic is "
        f"{'still present' if topic in after else 'GONE'}. "
        + "; ".join(f"{k}={v}" for k, v in clauses.items()),
        evidence,
    )


def rehearsal_arm_refused(work: pathlib.Path, target_cluster_id: str,
                          refused_key: dict[str, Any], evidence: list[str]) -> bool:
    """Review §4 step 10 — the negative control, **and it REQUIRES the refusal**.

    The document is genuinely signed, by a key this namespace's `TrustPolicy`
    carries as **Retired** with `GovernedApproval` — "whose key was retired
    between slots". So the signature verifies and the AUTHORITY does not, which
    is the only shape that tests the rule rather than the parser: a garbage
    signature would be refused by the crypto layer and would prove nothing
    about `may_sign_new_for`.

    `--valid-days` is the ordinary 30 rather than a past window, because the
    shipped signer refuses to mint an already-expired document
    (`plat14-3b.result.md` fix round 2, INFO) — it compares `issuedAt` against
    `Utc::now()` — so "expired" is not a document this harness can produce with
    the product's own command, and the review's own alternative is taken.
    """
    name = REHEARSAL_REFUSED_SCHEDULE
    _, approval_name = rehearsal_arm(name, cron=REHEARSAL_FAST_CRON,
                                     key=refused_key["private"], work=work,
                                     target_cluster_id=target_cluster_id)
    unsuspend("rehearsalschedule", name)
    live: dict[str, Any] = {}
    restores: list[dict[str, Any]] = []
    deadline = time.time() + 420
    while time.time() < deadline:
        live = get("rehearsalschedule", name)
        status = live.get("status") or {}
        restores = rehearsal_restores(name)
        recorded = bool((status.get("lastSkipped") or {}).get("reason")
                        or (status.get("lastFailed") or {}).get("reason"))
        if recorded or any(terminal(r) for r in restores):
            break
        time.sleep(5)
    jobs = [((r.get("status") or {}).get("jobRef") or {}).get("name")
            for r in restores]
    jobs = [j for j in jobs if j and get_opt("job", j) is not None]
    bundles = rehearsal_bundles(restores)
    clauses = the_refused_arm_reaches_no_job(live, restores, jobs, bundles)
    # WHICH CHECK REFUSED IT, not just that something did. The schedule only
    # ever says `the Approval is not Verified=True`; the Approval's own
    # condition is where `KeyIdNotInRoster` / a signing refusal / a document
    # mismatch are distinguished, and a reader of this row cannot tell the
    # retired-key mechanism from any other refusal without it.
    refused_approval = get_opt("approval", approval_name) or {}
    evidence.append(artifact("rehearsal/10-refused.json", {
        "retiredApproverKeyId": refused_key["keyId"],
        "schedule": live.get("status"), "restores": restores,
        "approval": approval_name,
        "approvalConditions": (refused_approval.get("status") or {}).get("conditions"),
        "jobs": jobs, "bundles": bundles, "clauses": clauses,
    }))
    return check(
        "rehearsal-10-refused-arm-reaches-no-job",
        "PLAT-14.3",
        all(clauses.values()),
        f"NEGATIVE CONTROL: {name}'s standing document is signed by the approver key this "
        f"run minted and this namespace's TrustPolicy carries as Retired "
        f"({refused_key['keyId'][:16]}…), so the signature verifies and the key may authorise "
        f"nothing new. The slot recorded "
        f"lastSkipped={json.dumps((live.get('status') or {}).get('lastSkipped'))} / "
        f"lastFailed={json.dumps((live.get('status') or {}).get('lastFailed'))}, "
        f"RehearsalHealthy={condition(live, 'RehearsalHealthy').get('status')}, with "
        f"{len(jobs)} Job(s) and {len(bundles)} bundle ConfigMap(s). The first clause is the "
        f"one that cannot be satisfied by silence: zero Jobs is also what a build that never "
        f"reconciles this kind leaves behind. "
        + "; ".join(f"{k}={v}" for k, v in clauses.items()),
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
    violations = phase_order_violations(PHASES)
    check(
        "d3-phase-order-satisfies-its-own-preconditions",
        "harness",
        not violations,
        f"the declared PHASES order satisfies every precondition the phases have of each "
        f"other ({len(PHASE_PRECONDITIONS)} declared): {violations or 'none violated'}. "
        f"`preview` writes the plan document `enforce`, `wrong-prefix`, `denied-deletion` "
        f"and `no-evidence-credential` all read, and `bounded_retry` deletes the `keep-b` "
        f"policy `preview` builds from — which is why it now runs last of the dest-b phases. "
        f"A declared order nobody checks is a comment",
        [],
    )
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
        f"# {OWNER} — live docker-desktop acceptance results",
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
        f"# {OWNER} cleanup proof",
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


# What each phase needs to have run before it, declared rather than implied.
# `preview` writes `enforce/plan-document.json` and `STATE["plan"]`; `keep-b` is
# `retention`'s policy and `bounded_retry` deletes it.
PHASE_PRECONDITIONS: dict[str, tuple[str, ...]] = {
    "catalog_cases": ("catalog",),
    # Both write their own bucket and their own destination, so they need only
    # `setup` — declared rather than implied, because the reason `catalog_access`
    # does NOT use dest-b is that `retention` asserts dest-b holds exactly its
    # six points, and a corrupt manifest planted there would fail a phase that
    # never asked for one.
    "catalog_scale": ("setup",),
    "catalog_access": ("setup",),
    # `notify` opens this namespace's first Staleness alert and needs the view
    # `catalog` publishes; `protection_cases` refreshes that view, adds a point
    # to dest-a and then breaks it, so it runs after every phase that reads
    # dest-a expecting it whole.
    "notify": ("catalog",),
    "protection_cases": ("catalog", "notify"),
    # ONLY `setup`. Every fixture it needs — its three policies, its two
    # catalogs, the legacy destination, the sink and every Backup it measures —
    # it creates itself, because its rows assert exact alert ledgers and an
    # incident another phase opened on the same policy would make every count
    # in them somebody else's. It runs after `protection_cases` in `PHASES`
    # only because that phase breaks a manifest in dest-a, and a broken
    # manifest is not what these rows are about.
    "protection_verdicts": ("setup",),
    "retention": ("catalog",),
    "legal_hold": ("retention",),
    "lifecycle": ("retention",),
    "enforce_guards": ("retention",),
    "preview": ("retention",),
    "enforce": ("preview",),
    "wrong_prefix": ("preview",),
    "denied_deletion": ("preview",),
    "no_evidence_credential": ("preview",),
    # LAST OF THE dest-b PHASES, because it deletes the policy they read — and
    # after `legal_hold`, which plants a permanently nonterminal Restore at
    # dest-b. `start_decision` refuses on the WHOLE destination while one
    # exists ("a nonterminal Restore reads this destination; no retention Job
    # is created"), so a `bounded_retry` that ran first would count zero
    # enforcement runs and report the retry budget unexercised — which is
    # exactly what it did before harness-rows-2 made `legal_hold` clean up
    # after itself. The cleanup landed; the ORDER it depends on was never
    # declared, and an undeclared dependency is one edit from being a bug
    # again.
    "bounded_retry": ("preview", "enforce", "wrong_prefix", "denied_deletion",
                      "no_evidence_credential", "legal_hold"),
    "signed_at_probe": ("trust",),
    # D3 §15 L6. `catalog` is where dest-a's archive and its first view exist, and
    # this phase restores a point out of that destination and writes the rehearsal's
    # evidence back into it. `trust` is an ORDERING and not a convenience: its last
    # row leaves the lab signing key REVOKED on this namespace's TrustPolicy, and a
    # rehearsal that inherited that would fail at the point's evidence verdict for a
    # reason belonging to the previous phase. `rehearsal_trust` rebuilds the object
    # (the CRD's keys are append-only with no Revoked->Active transition, so it is
    # deleted and recreated, exactly as `old_archive` does), which is only correct
    # AFTER the phase whose rows are about that revocation.
    "rehearsal": ("catalog", "trust"),
}


def phase_order_violations(phases: list[str]) -> list[str]:
    """Every phase that runs before something it needs.

    A DECLARED ORDER, CHECKED. The list used to put `bounded_retry` before
    `preview`, and since `bounded_retry` DELETES `keep-b` to put its own Enforce
    policy on dest-b, `preview` then died on `retentionpolicy "keep-b" not
    found` and took `enforce`, `wrong-prefix` and `denied-deletion` down with it
    — four phases failing for a reason that is the list's and looks like the
    product's (lab-refresh-5 §8.4, again in lab-refresh-6 §16). A declared order
    nobody checks is a comment.
    """
    position = {name: index for index, name in enumerate(phases)}
    out: list[str] = []
    for phase, needs in PHASE_PRECONDITIONS.items():
        if phase not in position:
            continue
        for need in needs:
            if need in position and position[need] > position[phase]:
                out.append(f"{phase} runs before {need}")
    return sorted(out)


# THE ORDER IS THE PHASES' OWN PRECONDITIONS, not a preference.
#
# `preview` is what writes `enforce/plan-document.json` and `STATE["plan"]`, and
# `enforce`, `wrong_prefix`, `denied_deletion` and `no_evidence_credential` all
# read them — so `preview` comes before every one of them. `bounded_retry`
# DELETES `keep-b` to put its own Enforce policy on dest-b, and `preview` builds
# its plan from `keep-b`'s evaluation, so running `bounded_retry` first left
# `preview` dying on `retentionpolicy "keep-b" not found` and took the other
# four down with it (lab-refresh-5 §8.4, confirmed again in lab-refresh-6 §16).
# `bounded_retry` therefore runs after the phases that need `keep-b`.
PHASES = [
    "setup", "catalog", "catalog_cases", "catalog_scale", "catalog_access", "retention", "legal_hold", "lifecycle",
    "enforce_guards", "packaging", "preview",
    "enforce", "wrong_prefix", "denied_deletion", "no_evidence_credential",
    "bounded_retry", "trust",
    "signed_at_probe", "trust_rbac", "old_archive", "multiple_namespaces", "notify", "protection_cases",
    "protection_verdicts",
    "rehearsal",
    "control", "report", "cleanup",
]


USAGE = """usage: d3_live.py <phase> [--retention-image <ref>]

phases: {phases}

--retention-image <ref>   the image carrying `logweir-retention`, used by
                          preview, enforce, wrong-prefix, denied-deletion and
                          no-evidence-credential. OPTIONAL: the product runner
                          image carries that binary now (RET-NOIMAGE closed), so
                          the default is the controller's own
                          LOGWEIR_RUNNER_IMAGE. Give this to measure a build the
                          lab is not running; e2e/k8s/d3/Dockerfile.retention
                          builds one. May also be given as {env}.
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
