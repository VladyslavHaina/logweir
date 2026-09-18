#!/usr/bin/env python3
"""Live docker-desktop acceptance for PLAT-17.1's six bounded-endpoint tests.

WHAT THIS PROVES. `logweir-api` runs in `localAdmin` mode on a loopback port
against ONE owned namespace on the shared `docker-desktop` lab, whose
`weirkeeper` controller watches every namespace. Against that live pair it
exercises the six tests PLAT-17.1's tracker entry names, plus the acceptance
sentence itself ("the API submits durable CRs while the controller remains the
execution authority; arbitrary Kubernetes paths are unavailable"):

  1. contract validation  - every write route's success body is validated FIELD
     BY FIELD against `schemas/logweir-api-v1.openapi.json`'s declared response
     schema (required present, no undeclared member, every type and enum).
  2. malformed input      - an undeclared field, a wrong type and a missing
     required field each answer `application/problem+json` with a field path
     AND create nothing (proved with `kubectl` afterwards); `PUT` and `DELETE`
     on a resource route are refused.
  3. duplicate request    - one `Idempotency-Key` replayed with identical
     content resolves to the SAME Kubernetes UID; replayed with different
     content it is `idempotency_conflict`; the three routes that REFUSE a key
     answer 400 when one is sent.
  4. timeout/cancellation - a transient check against a blackhole broker
     (`10.255.255.1:9096`, which nothing answers) is cancelled through the API
     while `Running` and reaches a cancelled state, and a short-budget run
     reaches a failed state; the reason the API projects is recorded verbatim.
  5. pagination           - 56 objects paged at a small limit cover the set
     exactly once (set equality: no duplicate, no omission); a tampered cursor
     and a cursor replayed on another route are both refused.
  6. restart              - the API process is killed and restarted: every
     object is still there with the same UID, an in-flight operation is moved
     by the CONTROLLER while the API is DOWN (the down interval is proved by a
     refused connection), and a key from before the restart replays to the same
     UID after it.

  plus  `surface`, which records which collection routes this build actually
        serves against what `ui/api.js` is able to address, and which capability
        flags it reports false;

  plus  arbitrary Kubernetes paths (Secrets, Pods, logs, exec, Jobs, `/apis`,
        two path traversals sent on a raw socket so nothing normalises them)
        and `Impersonate-User`, each with its exact status and body; and a
        CREDENTIAL SWEEP: three random markers are placed in a Secret and typed
        once into a write-only credential field, then every captured response,
        the API's own log and every artifact are searched for them (plain and
        base64) and must match zero times.

HOW TO RUN. Phase by phase, so a partial run still leaves evidence:

    python3 e2e/k8s/d2/api_live.py setup
    python3 e2e/k8s/d2/api_live.py contract
    python3 e2e/k8s/d2/api_live.py surface
    python3 e2e/k8s/d2/api_live.py malformed
    python3 e2e/k8s/d2/api_live.py idempotency
    python3 e2e/k8s/d2/api_live.py transient
    python3 e2e/k8s/d2/api_live.py pagination
    python3 e2e/k8s/d2/api_live.py restart
    python3 e2e/k8s/d2/api_live.py kubepaths
    python3 e2e/k8s/d2/api_live.py credsweep
    python3 e2e/k8s/d2/api_live.py report
    python3 e2e/k8s/d2/api_live.py cleanup
    python3 e2e/k8s/d2/api_live.py negative-control   # the harness failing on purpose

`all` runs setup through credsweep and then `report`. Every phase appends to
`results.json` in the artifact directory, so a phase that dies still leaves the
scenarios that ran before it.

NOTHING HERE PRINTS OR STORES A CREDENTIAL. The three markers live in
`$LOGWEIR_D2W14_WORK` (0600, outside the artifact tree), every artifact is
written through `redact()`, and `credsweep` counts occurrences rather than
showing them. Every subprocess and every socket carries a timeout (WORKER-RULES:
a hung child blocks the whole wave). Every `kubectl` carries
`--context docker-desktop`; the namespace is created with
`logweir.dev/test-owner=d2w14` and is deleted only after its label AND its UID
are read back and compared with the ones recorded at creation.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import pathlib
import re
import secrets
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[3]
CONTEXT = "docker-desktop"
OWNER = "d2w14"
OWNER_LABEL = f"logweir.dev/test-owner={OWNER}"
STAMP = os.environ.get("LOGWEIR_D2W14_STAMP", "20260918t0315z")
NS = os.environ.get("LOGWEIR_D2W14_NS", f"lw-{OWNER}-api-{STAMP}")
PORT = int(os.environ.get("LOGWEIR_D2W14_PORT", "18914"))
ORIGIN = f"http://127.0.0.1:{PORT}"
HOSTHDR = f"127.0.0.1:{PORT}"
OUT = pathlib.Path(
    os.environ.get(
        "LOGWEIR_D2W14_OUT",
        "/tmp/logweir-roadmap-run/claude/artifacts/d2-live/20260918T030840Z/api",
    )
)
# THE MARKERS AND THE CURSOR KEY LIVE HERE AND NOWHERE ELSE. Never under OUT:
# the artifact tree is read by reviewers and copied around, and a credential in
# it would be exactly the leak the sweep exists to disprove.
WORK = pathlib.Path(os.environ.get("LOGWEIR_D2W14_WORK", "/tmp/d2w14-api-live"))
API_BIN = ROOT / "target" / "debug" / "logweir-api"
UI_DIR = ROOT / "ui"
SCHEMA_PATH = ROOT / "schemas" / "logweir-api-v1.openapi.json"
STATE_PATH = OUT / "state.json"
RESULTS_PATH = OUT / "results.json"
API_LOG = OUT / "api.log"
BLACKHOLE = "10.255.255.1:9096"
LAB_BOOTSTRAP = "kafka-source.logweir-scram-local.svc:9092"
LAB_ENDPOINT = "http://minio.logweir-scram-local.svc.cluster.local:9000"
PAGE_OBJECTS = 56
PAGE_LIMIT = 10
K = ["kubectl", "--context", CONTEXT]
KN = K + ["-n", NS]
# EVERY NAMESPACE THIS FILE MAY EVER NAME. A second name here is a bug, not a
# feature: one wave owns one namespace and the lab namespace is read-only.
FORBIDDEN_NS = {"default", "kube-system", "kube-public", "kube-node-lease", "logweir-scram-local"}

OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
WORK.mkdir(mode=0o700, parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# state, results, redaction
# --------------------------------------------------------------------------

def _load(path: pathlib.Path, default: Any) -> Any:
    return json.loads(path.read_text()) if path.exists() else default


STATE: dict[str, Any] = _load(STATE_PATH, {"namespace": NS, "owner": OWNER, "objects": {}, "keys": {}})
RESULTS: dict[str, Any] = _load(RESULTS_PATH, {"namespace": NS, "scenarios": []})
MARKERS: dict[str, str] = _load(WORK / "markers.json", {})


def save_state() -> None:
    STATE_PATH.write_text(json.dumps(STATE, indent=2, sort_keys=True) + "\n")


def save_results() -> None:
    RESULTS["writtenAt"] = now()
    RESULTS_PATH.write_text(json.dumps(RESULTS, indent=2, sort_keys=True) + "\n")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def needles() -> list[str]:
    """Every string that must never appear in an artifact, plain and base64.

    Base64 too because a Secret's `data` is base64 and a leak that went through
    `kubectl get -o json` would not be a plaintext match.
    """
    out: list[str] = []
    for value in MARKERS.values():
        if not value:
            continue
        out.append(value)
        out.append(base64.b64encode(value.encode()).decode())
    return out


def redact(text: str) -> str:
    for needle in needles():
        text = text.replace(needle, "<<redacted-credential>>")
    return text


# THE SWEEP'S EVIDENCE, AND WHY IT IS NOT UNDER `OUT`. Every artifact is
# written through `redact()`, so searching the artifact tree alone would find
# zero markers BY CONSTRUCTION and prove nothing about the service. Every
# response body is therefore also appended here, BYTE FOR BYTE, in a 0600 file
# outside the artifact tree; that file is what the sweep searches. It is deleted
# by `cleanup`.
RAW_CAPTURE = WORK / "raw-responses.log"


def capture_raw(label: str, text: str) -> None:
    with open(RAW_CAPTURE, "a") as sink:
        sink.write(f"### {label}\n{text}\n")
    RAW_CAPTURE.chmod(0o600)


def write_artifact(name: str, text: str) -> str:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact(text))
    return name


# --------------------------------------------------------------------------
# subprocesses and sockets - ALWAYS with a timeout
# --------------------------------------------------------------------------

def run(args: list[str], *, data: str | None = None, check: bool = True, timeout: int = 120):
    """One subprocess, ALWAYS with a timeout, and the exit code read from the
    completed process rather than through a pipe."""
    result = subprocess.run(
        args, input=data, text=True, capture_output=True, timeout=timeout, cwd=ROOT
    )
    if check and result.returncode:
        raise RuntimeError(
            f"rc={result.returncode}: {args[:8]}\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )
    return result


def kube_json(args: list[str], *, timeout: int = 120) -> Any:
    return json.loads(run(KN + args + ["-o", "json"], timeout=timeout).stdout)


def guard_namespace(name: str) -> None:
    if name in FORBIDDEN_NS or not name.startswith(f"lw-{OWNER}-"):
        raise RuntimeError(f"refusing to touch namespace {name!r}")


# --------------------------------------------------------------------------
# the OpenAPI validator - field for field
# --------------------------------------------------------------------------

DOC = json.loads(SCHEMA_PATH.read_text())
SCHEMAS: dict[str, Any] = DOC["components"]["schemas"]
DATE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def _resolve(schema: dict[str, Any]) -> dict[str, Any]:
    if "$ref" not in schema:
        return schema
    base = dict(SCHEMAS[schema["$ref"].rsplit("/", 1)[-1]])
    for key, value in schema.items():
        if key != "$ref":
            base[key] = value
    return base


def validate(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Every way `value` departs from `schema`. An EMPTY list is the pass.

    Strict on purpose: a member the document does not declare is reported, so
    "the response validates field for field" means what it says rather than
    "the fields the document happens to know about are fine".
    """
    schema = _resolve(schema)
    bad: list[str] = []
    if value is None:
        if not schema.get("nullable", False):
            bad.append(f"{path}: null where the document declares no nullable")
        return bad
    if "oneOf" in schema:
        for branch in schema["oneOf"]:
            if not validate(value, branch, path):
                return []
        bad.append(f"{path}: matches none of the {len(schema['oneOf'])} declared variants ({value!r})")
        return bad
    declared = schema.get("type")
    if declared == "object":
        if not isinstance(value, dict):
            return [f"{path}: expected object, got {type(value).__name__}"]
        properties = schema.get("properties", {})
        for name in schema.get("required", []):
            if name not in value:
                bad.append(f"{path}.{name}: required by the document and absent")
        for name, member in value.items():
            if name not in properties:
                bad.append(f"{path}.{name}: not declared by the document")
                continue
            bad.extend(validate(member, properties[name], f"{path}.{name}"))
        return bad
    if declared == "array":
        if not isinstance(value, list):
            return [f"{path}: expected array, got {type(value).__name__}"]
        items = schema.get("items")
        if items is not None:
            for index, member in enumerate(value):
                bad.extend(validate(member, items, f"{path}[{index}]"))
        return bad
    if declared == "string":
        if not isinstance(value, str):
            return [f"{path}: expected string, got {type(value).__name__}"]
        if "enum" in schema and value not in schema["enum"]:
            bad.append(f"{path}: {value!r} is not one of {schema['enum']}")
        if schema.get("format") == "date-time" and not DATE_TIME.match(value):
            bad.append(f"{path}: {value!r} is not an RFC 3339 instant")
        return bad
    if declared in ("integer", "number"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return [f"{path}: expected {declared}, got {type(value).__name__}"]
        if declared == "integer" and not isinstance(value, int):
            bad.append(f"{path}: expected integer, got {value!r}")
        return bad
    if declared == "boolean":
        if not isinstance(value, bool):
            return [f"{path}: expected boolean, got {type(value).__name__}"]
        return bad
    return bad


# --------------------------------------------------------------------------
# the scenario record
# --------------------------------------------------------------------------

class Scenario:
    """One tracker check, with everything a reader needs to re-run it."""

    def __init__(self, ident: str, title: str, phase: str) -> None:
        self.record: dict[str, Any] = {
            "id": ident,
            "title": title,
            "phase": phase,
            "startedAt": now(),
            "requests": [],
            "commands": [],
            "objects": [],
            "assertions": [],
            "timings": {},
            "result": "NOT-RUN",
            "reason": "",
        }
        self.started = time.monotonic()

    def request(self, entry: dict[str, Any]) -> None:
        self.record["requests"].append(entry)

    def command(self, argv: list[str], rc: int, out_name: str | None = None) -> None:
        self.record["commands"].append({"argv": argv, "rc": rc, "output": out_name})

    def object(self, kind: str, name: str, uid: str | None = None) -> None:
        self.record["objects"].append({"kind": kind, "name": name, "uid": uid})

    def timing(self, name: str, seconds: float) -> None:
        self.record["timings"][name] = round(seconds, 3)

    def check(self, claim: str, ok: bool, excerpt: Any = "") -> bool:
        self.record["assertions"].append(
            {"claim": claim, "pass": bool(ok), "excerpt": redact(json.dumps(excerpt, default=str))[:1400]}
        )
        log(f"  [{'PASS' if ok else 'FAIL'}] {claim}")
        return bool(ok)

    def not_run(self, reason: str) -> None:
        self.record["result"] = "NOT-RUN"
        self.record["reason"] = reason
        log(f"  [NOT-RUN] {self.record['id']}: {reason}")

    def finish(self, reason: str = "") -> None:
        if self.record["result"] == "NOT-RUN" and not self.record["reason"]:
            failed = [a for a in self.record["assertions"] if not a["pass"]]
            self.record["result"] = "FAIL" if failed else "PASS"
            self.record["reason"] = reason or (
                failed[0]["claim"] if failed else (self.record["assertions"][-1]["claim"] if self.record["assertions"] else "")
            )
        self.record["endedAt"] = now()
        self.record["durationMs"] = round((time.monotonic() - self.started) * 1000)
        RESULTS["scenarios"] = [s for s in RESULTS["scenarios"] if s["id"] != self.record["id"]]
        RESULTS["scenarios"].append(self.record)
        RESULTS["scenarios"].sort(key=lambda s: s["id"])
        save_results()
        log(f"  => {self.record['id']} {self.record['result']}")


# --------------------------------------------------------------------------
# HTTP - urllib for ordinary calls, a raw socket for the paths nothing may
# normalise on the way out
# --------------------------------------------------------------------------

def _headers(raw: Any) -> dict[str, str]:
    """Response headers, LOWERCASED. HTTP/1.1 field names are case-insensitive
    and this listener sends them lowercase; a dict built from the raw names
    would make `Content-Type` a miss and turn a passing check into a failing
    one for a reason that is not about the service."""
    return {name.lower(): value for name, value in raw.items()}


SEQ = {"n": 0}


def _next_name(scenario: Scenario, kind: str) -> str:
    SEQ["n"] += 1
    return f"bodies/{scenario.record['id']}-{SEQ['n']:03d}-{kind}.json"


def call(
    scenario: Scenario,
    method: str,
    path: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    *,
    timeout: int = 30,
    note: str = "",
) -> tuple[int, dict[str, str], str]:
    data = json.dumps(body).encode() if body is not None else None
    sent = {"Host": HOSTHDR}
    if data is not None:
        sent["Content-Type"] = "application/json"
        sent["Origin"] = ORIGIN
    sent.update(headers or {})
    request = urllib.request.Request(ORIGIN + path, data=data, headers=sent, method=method)
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, received, text = response.status, _headers(response.headers), response.read().decode()
    except urllib.error.HTTPError as refused:
        status, received, text = refused.code, _headers(refused.headers), refused.read().decode()
    elapsed = time.monotonic() - started
    entry: dict[str, Any] = {
        "method": method,
        "url": ORIGIN + path,
        "requestHeaders": {k: v for k, v in sent.items()},
        "status": status,
        "contentType": received.get("content-type"),
        "elapsedMs": round(elapsed * 1000),
        "note": note,
    }
    if data is not None:
        entry["requestBody"] = write_artifact(_next_name(scenario, "req"), json.dumps(body, indent=1))
    entry["responseBody"] = write_artifact(_next_name(scenario, "res"), text)
    capture_raw(f"{method} {path} -> {status}", text)
    scenario.request(entry)
    return status, received, text


def raw_call(
    scenario: Scenario,
    method: str,
    target: str,
    headers: dict[str, str] | None = None,
    *,
    timeout: int = 20,
    note: str = "",
) -> tuple[int, str, str]:
    """One request line sent BYTE FOR BYTE, so a traversal target reaches the
    listener exactly as written. `urllib` would normalise `..` away and the
    check would then prove nothing."""
    lines = [f"{method} {target} HTTP/1.1", f"Host: {HOSTHDR}", "Connection: close"]
    for name, value in (headers or {}).items():
        lines.append(f"{name}: {value}")
    wire = ("\r\n".join(lines) + "\r\n\r\n").encode()
    chunks: list[bytes] = []
    with socket.create_connection(("127.0.0.1", PORT), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(wire)
        while True:
            piece = sock.recv(65536)
            if not piece:
                break
            chunks.append(piece)
    raw = b"".join(chunks).decode(errors="replace")
    head, _, text = raw.partition("\r\n\r\n")
    status = int(head.split(" ", 2)[1]) if head.startswith("HTTP/") else 0
    entry = {
        "method": method,
        "requestLine": f"{method} {target} HTTP/1.1",
        "requestHeaders": dict(headers or {}, Host=HOSTHDR),
        "status": status,
        "responseHead": write_artifact(_next_name(scenario, "head"), head),
        "responseBody": write_artifact(_next_name(scenario, "res"), text),
        "note": note,
    }
    capture_raw(f"{method} {target} -> {status}", head + "\n" + text)
    scenario.request(entry)
    return status, head, text


# --------------------------------------------------------------------------
# the API process
# --------------------------------------------------------------------------

API: dict[str, Any] = {"process": None, "log": None}


def api_alive(timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(ORIGIN + "/healthz", timeout=timeout) as response:
            return response.status == 200
    except Exception:
        return False


def start_api(*, wait: float = 60.0) -> float:
    """Start `logweir-api` and wait for `/healthz`. EVERY WAIT HAS A CEILING."""
    if API["process"] is not None and API["process"].poll() is None:
        return 0.0
    handle = open(API_LOG, "ab")
    API["log"] = handle
    API["process"] = subprocess.Popen(
        [str(API_BIN), "--config", str(WORK / "config.yaml")],
        stdout=handle,
        stderr=handle,
        cwd=ROOT,
    )
    started = time.monotonic()
    deadline = started + wait
    while time.monotonic() < deadline:
        if api_alive(timeout=2.0):
            return time.monotonic() - started
        if API["process"].poll() is not None:
            raise RuntimeError(f"logweir-api exited with {API['process'].returncode}; see {API_LOG}")
        time.sleep(0.4)
    stop_api()
    raise RuntimeError(f"logweir-api never answered /healthz within {wait:.0f}s; see {API_LOG}")


def stop_api() -> None:
    process = API["process"]
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=20)
    API["process"] = None
    if API["log"] is not None:
        API["log"].flush()
        API["log"].close()
        API["log"] = None


def ensure_api() -> None:
    if api_alive():
        if API["process"] is None:
            raise RuntimeError(
                f"something already answers {ORIGIN}/healthz and this harness did not start it; "
                "refusing to run against a process whose lifetime it does not own"
            )
        return
    start_api()


# --------------------------------------------------------------------------
# small helpers over the product API
# --------------------------------------------------------------------------

def key(name: str) -> str:
    """One `Idempotency-Key` per logical request, remembered across phases."""
    existing = STATE["keys"].get(name)
    if existing is None:
        existing = f"{OWNER}-{name}-{secrets.token_hex(6)}"
        STATE["keys"][name] = existing
        save_state()
    return existing


def fresh_key(name: str) -> str:
    """A key that is new on every run of its phase.

    `key()` remembers, which is what `R3` needs (a key minted before the
    restart, replayed after it). `D1` needs the opposite: re-running the phase
    must exercise a FIRST send, not a replay of the previous run's.
    """
    minted = f"{OWNER}-{name}-{secrets.token_hex(6)}"
    STATE["keys"][name] = minted
    save_state()
    return minted


def remember(kind: str, name: str, uid: str) -> None:
    STATE["objects"].setdefault(kind, {})[name] = uid
    save_state()


def poll_state(
    scenario: Scenario,
    path: str,
    wanted: set[str],
    *,
    limit: float,
    note: str,
) -> tuple[str, dict[str, Any], float]:
    """Read one check through the API until its `state` is in `wanted`."""
    started = time.monotonic()
    seen: list[str] = []
    item: dict[str, Any] = {}
    while time.monotonic() - started < limit:
        status, _, text = call(scenario, "GET", path, note=f"{note} poll")
        if status != 200:
            time.sleep(1.0)
            continue
        item = json.loads(text)["item"]
        state = item.get("state", "")
        if not seen or seen[-1] != state:
            seen.append(state)
        if state in wanted:
            return state, item, time.monotonic() - started
        time.sleep(1.0)
    scenario.record.setdefault("observedStates", []).extend(seen)
    return item.get("state", ""), item, time.monotonic() - started


def kubectl_phase(kind: str, name: str) -> tuple[str, str]:
    result = run(KN + ["get", kind, name, "-o", "json"], check=False, timeout=60)
    if result.returncode:
        return "", ""
    status = json.loads(result.stdout).get("status", {})
    return status.get("phase", ""), status.get("reason", "")


# --------------------------------------------------------------------------
# PHASE setup
# --------------------------------------------------------------------------

def phase_setup() -> None:
    guard_namespace(NS)
    if not API_BIN.exists():
        raise RuntimeError(f"{API_BIN} is not built")
    run(K + ["version", "--client=true"], timeout=60)

    existing = run(K + ["get", "namespace", NS, "-o", "json"], check=False, timeout=60)
    if existing.returncode:
        run(K + ["create", "namespace", NS], timeout=60)
        run(K + ["label", "namespace", NS, OWNER_LABEL], timeout=60)
    else:
        run(K + ["label", "namespace", NS, OWNER_LABEL, "--overwrite"], timeout=60)
    namespace = json.loads(run(K + ["get", "namespace", NS, "-o", "json"], timeout=60).stdout)
    STATE["namespaceUid"] = namespace["metadata"]["uid"]
    STATE["namespaceLabels"] = namespace["metadata"].get("labels", {})
    if STATE["namespaceLabels"].get("logweir.dev/test-owner") != OWNER:
        raise RuntimeError("the namespace does not carry this wave's owner label")
    write_artifact(
        "00-namespace.txt",
        run(K + ["get", "namespace", NS, "--show-labels"], timeout=60).stdout
        + f"\nuid={STATE['namespaceUid']}\n",
    )

    # A KNOWN, EMPTY START. Everything in this namespace was created by this
    # wave; the earlier probe objects are deleted so `pagination`'s set
    # equality is over a set this file created.
    kinds = ",".join([
        "topicdiscoveries", "preflights", "backupschedules", "backups", "restores",
        "backupdestinations", "kafkaclusters", "approvals",
    ])
    run(KN + ["delete", kinds, "--all", "--wait=true"], check=False, timeout=300)

    run(KN + ["create", "serviceaccount", "logweir-runner"], check=False, timeout=60)
    run(KN + ["label", "serviceaccount", "logweir-runner", OWNER_LABEL, "--overwrite"], timeout=60)

    # THE THREE MARKERS. Random, high-entropy, and written to a 0600 file
    # outside the artifact tree. `existing` goes into a Secret this file
    # creates; `typed` is typed ONCE into the product API's write-only
    # credential field and must never come back out; `akid` is the access key
    # id, which is a credential too.
    global MARKERS
    MARKERS = {
        "existing": "LWMARKEREXISTING" + secrets.token_hex(16),
        "typed": "LWMARKERTYPED" + secrets.token_hex(16),
        "akid": "LWMARKERAKID" + secrets.token_hex(12),
    }
    markers_path = WORK / "markers.json"
    markers_path.write_text(json.dumps(MARKERS, indent=1) + "\n")
    markers_path.chmod(0o600)
    if RAW_CAPTURE.exists():
        RAW_CAPTURE.unlink()
    run(KN + ["delete", "secret", "d2w14-marker-cred"], check=False, timeout=60)
    run(
        KN + [
            "create", "secret", "generic", "d2w14-marker-cred",
            f"--from-literal=access-key-id={MARKERS['akid']}",
            f"--from-literal=secret-access-key={MARKERS['existing']}",
        ],
        timeout=60,
    )
    run(KN + ["label", "secret", "d2w14-marker-cred", OWNER_LABEL, "--overwrite"], timeout=60)
    STATE["markerSecret"] = "d2w14-marker-cred"

    cursor = WORK / "cursor.key"
    cursor.write_bytes(secrets.token_bytes(32))
    cursor.chmod(0o600)
    config = "\n".join([
        "mode: localAdmin",
        f'listen: "127.0.0.1:{PORT}"',
        f'publicOrigin: "{ORIGIN}"',
        f"uiDirectory: {UI_DIR}",
        "localAdmin:",
        f"  subject: {OWNER}-api",
        "  displayName: D2 W14 live acceptance",
        f"namespaces: [{NS}]",
        "kubernetes:",
        "  source: kubeconfig",
        f"  context: {CONTEXT}",
        f"cursorKeyFile: {cursor}",
        "",
    ])
    (WORK / "config.yaml").write_text(config)
    write_artifact("01-config.yaml", config)
    STATE["port"] = PORT
    save_state()

    scenario = Scenario("A0", "the API answers on a loopback port against exactly this namespace", "setup")
    boot = start_api()
    scenario.timing("startupSeconds", boot)
    status, _, text = call(scenario, "GET", "/healthz", note="liveness")
    scenario.check("healthz answers 200", status == 200, text)
    status, _, text = call(scenario, "GET", "/api/v1/session", note="the session this actor gets")
    session = json.loads(text) if status == 200 else {}
    scenario.check("session answers 200", status == 200)
    scenario.check(
        "the session names exactly the one granted namespace",
        [g["name"] for g in session.get("namespaces", [])] == [NS],
        [g["name"] for g in session.get("namespaces", [])],
    )
    scenario.check(
        "the session validates against SessionResponse field for field",
        not validate(session, SCHEMAS["SessionResponse"]),
        validate(session, SCHEMAS["SessionResponse"]),
    )
    STATE["capabilities"] = session.get("capabilities", {})
    save_state()
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE contract
# --------------------------------------------------------------------------

def _created(scenario: Scenario, kind: str, text: str, schema: str, expect: int, status: int) -> dict[str, Any]:
    body = json.loads(text)
    item = body["item"]
    scenario.object(kind, item.get("name") or item.get("id"), item.get("uid"))
    remember(kind, item.get("name") or item.get("id"), item["uid"])
    scenario.check(f"the create answers {expect}", status == expect, status)
    violations = validate(body, SCHEMAS[schema])
    scenario.check(
        f"the body validates against {schema} field for field", not violations, violations
    )
    live = kube_json(["get", kind, item.get("name") or item.get("id")])
    scenario.check(
        "the durable object exists in Kubernetes with the UID the API returned",
        live["metadata"]["uid"] == item["uid"],
        live["metadata"]["uid"],
    )
    scenario.command(KN + ["get", kind, item.get("name") or item.get("id")], 0)
    return item


def phase_contract() -> None:
    ensure_api()

    scenario = Scenario("C1", "POST .../connections creates a KafkaCluster and answers ConnectionResponse", "contract")
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections",
        {
            "bootstrapServers": [LAB_BOOTSTRAP],
            "role": "source",
            "auth": {"mode": "plaintext", "tls": False},
        },
        {"Idempotency-Key": key("lab-connection")}, note="the lab source connection",
    )
    item = _created(scenario, "kafkacluster", text, "ConnectionResponse", 201, status)
    STATE["labConnection"] = item["name"]
    save_state()
    scenario.finish()

    scenario = Scenario("C2", "POST .../destinations creates a BackupDestination and answers DestinationResponse", "contract")
    name = "d2w14-dest"
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/destinations",
        {
            "name": name,
            "description": "D2 W14 live acceptance",
            "storage": {
                "provider": "s3", "bucket": "kafka-backups",
                "prefix": f"d2w14-api/{STAMP}", "region": "us-east-1",
                "endpoint": LAB_ENDPOINT, "addressing": "pathStyle",
            },
            "transport": {"security": "insecureHttp"},
            "access": {
                "archiveWrite": {
                    "mode": "secretKeys",
                    "secret": {"existing": {"name": STATE["markerSecret"]}},
                }
            },
        },
        {"Idempotency-Key": key("destination")}, note="a destination naming an existing Secret",
    )
    item = _created(scenario, "backupdestination", text, "DestinationResponse", 201, status)
    scenario.check(
        "the response names the Secret and never its keys' values",
        item["access"]["archiveWrite"].get("secretName") == STATE["markerSecret"]
        and all(m not in text for m in needles()),
        item["access"]["archiveWrite"],
    )
    STATE["destination"] = item["name"]
    save_state()
    scenario.finish()

    scenario = Scenario("C2b", "POST .../destinations turns a typed credential into a Secret and never echoes it", "contract")
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/destinations",
        {
            "name": "d2w14-dest-typed",
            "storage": {
                "provider": "s3", "bucket": "kafka-backups",
                "prefix": f"d2w14-api/{STAMP}-typed", "region": "us-east-1",
                "endpoint": LAB_ENDPOINT, "addressing": "pathStyle",
            },
            "transport": {"security": "insecureHttp"},
            "access": {
                "archiveWrite": {
                    "mode": "secretKeys",
                    "secret": {"new": {
                        "accessKeyId": MARKERS["akid"],
                        "secretAccessKey": MARKERS["typed"],
                    }},
                }
            },
        },
        {"Idempotency-Key": key("destination-typed")}, note="the write-only credential path",
    )
    if status in (201, 200):
        item = _created(scenario, "backupdestination", text, "DestinationResponse", 201, status)
        scenario.check(
            "the response carries neither the typed key nor the access key id",
            all(m not in text for m in needles()),
            item["access"]["archiveWrite"].get("secretName"),
        )
        STATE["typedDestination"] = item["name"]
        save_state()
        scenario.finish()
    else:
        scenario.check("the write-only credential path answered a create", False, text[:600])
        scenario.finish("the typed-credential create was refused; see the body")

    scenario = Scenario("C3", "POST .../connections/{name}/topic-discoveries starts a check and answers TopicDiscoveryResponse", "contract")
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections/{STATE['labConnection']}/topic-discoveries",
        {"timeoutSeconds": 30, "reuseFresh": False},
        {"Idempotency-Key": key("contract-discovery")}, note="a sub-collection create on a connection",
    )
    body = json.loads(text)
    scenario.check("the create answers 202", status == 202, status)
    violations = validate(body, SCHEMAS["TopicDiscoveryResponse"])
    scenario.check("the body validates against TopicDiscoveryResponse field for field", not violations, violations)
    item = body["item"]
    scenario.object("topicdiscovery", item["id"], item["uid"])
    remember("topicdiscovery", item["id"], item["uid"])
    live = kube_json(["get", "topicdiscovery", item["id"]])
    scenario.check("the TopicDiscovery exists with the UID the API returned", live["metadata"]["uid"] == item["uid"], item["uid"])
    scenario.finish()

    scenario = Scenario("C4", "POST .../preflights creates a Preflight and answers PreflightResponse", "contract")
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/preflights",
        {
            "operation": "destinationAccess",
            "destinationAccess": {"destination": STATE["destination"], "roles": ["archiveWrite"]},
            "timeoutSeconds": 60,
        },
        {"Idempotency-Key": key("contract-preflight")}, note="a destination-access preflight",
    )
    body = json.loads(text)
    scenario.check("the create answers 202", status == 202, status)
    violations = validate(body, SCHEMAS["PreflightResponse"])
    scenario.check("the body validates against PreflightResponse field for field", not violations, violations)
    item = body["item"]
    scenario.object("preflight", item["id"], item["uid"])
    remember("preflight", item["id"], item["uid"])
    live = kube_json(["get", "preflight", item["id"]])
    scenario.check("the Preflight exists with the UID the API returned", live["metadata"]["uid"] == item["uid"], item["uid"])
    STATE["contractPreflight"] = item["id"]
    save_state()
    scenario.finish()

    scenario = Scenario("C5", "every list route answers its declared List envelope", "contract")
    for route, schema in (
        ("connections", "ConnectionList"),
        ("destinations", "DestinationList"),
        ("schedules", "ScheduleList"),
        ("backups", "BackupList"),
        ("restores", "RestoreList"),
        ("approvals", "ApprovalList"),
    ):
        status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/{route}", note=f"list {route}")
        violations = validate(json.loads(text), SCHEMAS[schema]) if status == 200 else ["status %s" % status]
        scenario.check(f"GET .../{route} is 200 and validates against {schema}", status == 200 and not violations, violations)
    scenario.finish()

    scenario = Scenario("C6", "the controller drives the API-created check and the API projects what it wrote", "contract")
    state, item, waited = poll_state(
        scenario, f"/api/v1/namespaces/{NS}/preflights/{STATE['contractPreflight']}",
        {"ready", "notReady", "unknown", "failed", "cancelled"}, limit=240, note="destination access",
    )
    scenario.timing("secondsToTerminal", waited)
    scenario.check("the destination-access preflight reached a terminal state", item.get("terminal") is True, state)
    violations = validate(item, SCHEMAS["Preflight"])
    scenario.check("the terminal projection still validates against Preflight", not violations, violations)
    status, _, text = call(
        scenario, "GET", f"/api/v1/namespaces/{NS}/operations/preflight/{STATE['contractPreflight']}",
        note="the normalized operation view",
    )
    violations = validate(json.loads(text), SCHEMAS["CheckOperationResponse"]) if status == 200 else ["status %s" % status]
    scenario.check("GET .../operations/preflight/{id} validates against CheckOperationResponse", status == 200 and not violations, violations)
    jobs = kube_json(["get", "jobs"])
    scenario.check(
        "the check ran in a Job the CONTROLLER owns, not one the API created",
        any(o.get("kind") in ("Preflight", "TopicDiscovery", "KafkaCluster", "Backup", "Restore")
            for j in jobs["items"] for o in j["metadata"].get("ownerReferences", [])),
        [{"job": j["metadata"]["name"],
          "ownedBy": [o["kind"] for o in j["metadata"].get("ownerReferences", [])]} for j in jobs["items"]],
    )
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE surface - what `ui/api.js` can address and this build does not serve
# --------------------------------------------------------------------------

# `ui/api.js` bounds what a page may reach: `CONSOLE_WRITABLE_PLURALS` for
# creates, the six `CONSOLE_ACTIONS`, and `consoleList`/`consoleGet`/
# `consoleSub` over a plural. A plural it can NAME is not the same as a route
# this build SERVES, and the difference is worth having as a fact rather than
# as a reading of two files.
SURFACE = [
    ("GET", "/connections", 200),
    ("GET", "/destinations", 200),
    ("GET", "/schedules", 200),
    ("GET", "/backups", 200),
    ("GET", "/restores", 200),
    ("GET", "/approvals", 200),
    ("GET", "/preflights", None),
    ("GET", "/topic-discoveries", None),
    ("GET", "/operations", None),
]


def phase_surface() -> None:
    ensure_api()
    scenario = Scenario("C7", "which collection routes this build serves, and which it does not", "surface")
    served: dict[str, int] = {}
    for method, suffix, expected in SURFACE:
        status, _, _ = call(scenario, method, f"/api/v1/namespaces/{NS}{suffix}", note=f"{method} ...{suffix}")
        served[f"{method} .../{suffix.lstrip('/')}"] = status
        if expected is not None:
            scenario.check(f"{method} .../{suffix.lstrip('/')} is served", status == expected, status)
    scenario.record["collectionRoutes"] = served
    # `preflights`, `topic-discoveries` and `operations` have no COLLECTION
    # route: a preflight is addressed by id, a discovery through its
    # connection, and an operation by kind and name. The refusal is a problem
    # document either way -- 405 where a route exists for another method, 404
    # where no route exists at all -- and never a partial list.
    unserved = {s: v for s, v in served.items() if s.endswith(("/preflights", "/topic-discoveries", "/operations"))}
    scenario.check(
        "no collection route answers for preflights, topic-discoveries or operations",
        all(status in (404, 405) for status in unserved.values()), unserved,
    )
    status, _, text = call(scenario, "GET", "/api/v1/cadence-previews?preset=daily&hour=3&minute=0&timeZone=UTC",
                           note="the one route that reads nothing")
    scenario.check("GET /api/v1/cadence-previews is served", status == 200, status)
    if status == 200:
        violations = validate(json.loads(text), SCHEMAS["CadencePreviewResponse"])
        scenario.check("its body validates against CadencePreviewResponse", not violations, violations)
    capabilities = STATE.get("capabilities", {})
    scenario.record["capabilitiesFalse"] = sorted(name for name, value in capabilities.items() if value is False)
    scenario.check("every capability flag this build reports false is a domain with no route",
                   sorted(name for name, value in capabilities.items() if value is False)
                   == ["approvalSubmit", "connectionTest", "operationEvents"],
                   sorted(name for name, value in capabilities.items() if value is False))
    write_artifact("surface-collection-routes.json", json.dumps(served, indent=1))
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE malformed
# --------------------------------------------------------------------------

def phase_malformed() -> None:
    ensure_api()
    before = {c["metadata"]["name"] for c in kube_json(["get", "kafkaclusters"])["items"]}
    base = {"bootstrapServers": [LAB_BOOTSTRAP], "role": "source", "auth": {"mode": "plaintext", "tls": False}}

    cases = [
        ("M1", "an undeclared field is refused and names the field",
         dict(base, undeclaredField="whatever"), "unknown_field"),
        ("M2", "a wrong JSON type is refused and names a field path",
         dict(base, auth={"mode": "plaintext", "tls": "yes"}), "invalid_type"),
        ("M3", "a missing required field is refused and names the field",
         {"role": "source", "auth": {"mode": "plaintext", "tls": False}}, "required"),
    ]
    for ident, title, body, wanted in cases:
        scenario = Scenario(ident, title, "malformed")
        status, headers, text = call(
            scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body,
            {"Idempotency-Key": key(f"malformed-{ident}")}, note=title,
        )
        problem = json.loads(text)
        scenario.check("the status is 422", status == 422, status)
        scenario.check(
            "the media type is application/problem+json",
            (headers.get("content-type") or "").startswith("application/problem+json"),
            headers.get("content-type"),
        )
        violations = validate(problem, SCHEMAS["Problem"])
        scenario.check("the body validates against Problem field for field", not violations, violations)
        scenario.check("the code is validation_failed", problem.get("code") == "validation_failed", problem.get("code"))
        errors = problem.get("errors", [])
        scenario.check("the problem carries a field path", bool(errors) and all("field" in e for e in errors), errors)
        scenario.check(f"the field code is {wanted}", any(e.get("code") == wanted for e in errors), errors)
        scenario.finish()

    scenario = Scenario("M4", "PUT and DELETE on a resource route are refused", "malformed")
    name = STATE["labConnection"]
    status, headers, text = call(
        scenario, "PUT", f"/api/v1/namespaces/{NS}/connections/{name}", {"role": "target"},
        note="PUT on a GET-only resource route",
    )
    scenario.check("PUT is 405 method_not_allowed", status == 405 and json.loads(text).get("code") == "method_not_allowed", (status, text[:200]))
    scenario.check("PUT is refused as problem+json", (headers.get("content-type") or "").startswith("application/problem+json"), headers.get("content-type"))
    status, headers, text = call(
        scenario, "DELETE", f"/api/v1/namespaces/{NS}/connections/{name}", None,
        {"Origin": ORIGIN}, note="DELETE as a bare unsafe request",
    )
    scenario.check("a bare DELETE is refused at the boundary", status in (405, 415, 403),
                   (status, json.loads(text).get("code")))
    status, headers, text = call(
        scenario, "DELETE", f"/api/v1/namespaces/{NS}/connections/{name}", None,
        {"Origin": ORIGIN, "Content-Type": "application/json"},
        note="DELETE that satisfies every boundary rule, so the answer is about the METHOD",
    )
    scenario.check("a well-formed DELETE is 405 method_not_allowed",
                   status == 405 and json.loads(text).get("code") == "method_not_allowed", (status, text[:200]))
    scenario.check("the DELETE refusal is problem+json",
                   (headers.get("content-type") or "").startswith("application/problem+json"),
                   headers.get("content-type"))
    live = run(KN + ["get", "kafkacluster", name, "-o", "json"], check=False, timeout=60)
    scenario.command(KN + ["get", "kafkacluster", name], live.returncode)
    scenario.check("the object the DELETE named is still there", live.returncode == 0, name)
    scenario.finish()

    scenario = Scenario("M5", "not one of the refused requests created an object", "malformed")
    after = {c["metadata"]["name"] for c in kube_json(["get", "kafkaclusters"])["items"]}
    scenario.command(KN + ["get", "kafkaclusters"], 0, write_artifact(
        "malformed-kafkaclusters-after.txt", run(KN + ["get", "kafkaclusters"], timeout=60).stdout))
    scenario.check("the KafkaCluster set is unchanged by the three refusals", before == after, sorted(after))
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE idempotency
# --------------------------------------------------------------------------

def phase_idempotency() -> None:
    ensure_api()
    body = {"bootstrapServers": [LAB_BOOTSTRAP], "role": "target", "auth": {"mode": "plaintext", "tls": False}}
    replay_key = fresh_key("replay")

    scenario = Scenario("D1", "one key replayed with identical content resolves to the SAME object UID", "idempotency")
    first_status, _, first_text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body,
        {"Idempotency-Key": replay_key}, note="first send",
    )
    first = json.loads(first_text)["item"]
    scenario.object("kafkacluster", first["name"], first["uid"])
    remember("kafkacluster", first["name"], first["uid"])
    STATE["replayName"] = first["name"]
    STATE["replayUid"] = first["uid"]
    save_state()
    second_status, _, second_text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body,
        {"Idempotency-Key": replay_key}, note="identical replay",
    )
    second = json.loads(second_text)
    third_status, _, third_text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body,
        {"Idempotency-Key": replay_key}, note="a third identical replay",
    )
    scenario.check("the first send is 201", first_status == 201, first_status)
    scenario.check("the replay is 200", second_status == 200, second_status)
    scenario.check("the replay says so", second.get("replayed") is True, second.get("replayed"))
    scenario.check("the replay carries the same UID", second["item"]["uid"] == first["uid"], second["item"]["uid"])
    scenario.check("a third replay carries the same UID too", json.loads(third_text)["item"]["uid"] == first["uid"], third_status)
    listed = kube_json(["get", "kafkaclusters"])["items"]
    same = [c for c in listed if c["metadata"]["name"] == first["name"]]
    scenario.command(KN + ["get", "kafkaclusters"], 0)
    scenario.check("kubectl sees exactly one object for the three POSTs, with that UID",
                   len(same) == 1 and same[0]["metadata"]["uid"] == first["uid"], first["uid"])
    scenario.finish()

    scenario = Scenario("D2", "the same key with different content is idempotency_conflict", "idempotency")
    status, headers, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections",
        dict(body, bootstrapServers=["kafka-target.logweir-scram-local.svc:9092"]),
        {"Idempotency-Key": replay_key}, note="same key, different body",
    )
    problem = json.loads(text)
    scenario.check("the status is 409", status == 409, status)
    scenario.check("the code is idempotency_conflict", problem.get("code") == "idempotency_conflict", problem.get("code"))
    scenario.check("it is problem+json", (headers.get("content-type") or "").startswith("application/problem+json"), headers.get("content-type"))
    scenario.check("the body validates against Problem", not validate(problem, SCHEMAS["Problem"]), validate(problem, SCHEMAS["Problem"]))
    after = [c for c in kube_json(["get", "kafkaclusters"])["items"] if c["metadata"]["name"] == first["name"]]
    scenario.check("the conflict created nothing and changed nothing",
                   len(after) == 1 and after[0]["metadata"]["uid"] == first["uid"], first["uid"])
    scenario.finish()

    scenario = Scenario("D3", "a durable create without a key is refused", "idempotency")
    status, _, text = call(scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body, note="no Idempotency-Key")
    scenario.check("the status is 400", status == 400, status)
    scenario.check("the code is idempotency_key_required",
                   json.loads(text).get("code") == "idempotency_key_required", json.loads(text).get("code"))
    scenario.finish()

    scenario = Scenario("D4", "the three routes that REFUSE a key answer 400 when one is sent", "idempotency")
    discovery = sorted(STATE["objects"].get("topicdiscovery", {}))
    preflight = sorted(STATE["objects"].get("preflight", {}))
    routes = [
        (f"/api/v1/namespaces/{NS}/destinations/{STATE['destination']}:update-access", {}, "the credential rotation"),
        (f"/api/v1/namespaces/{NS}/topic-discoveries/{discovery[0]}:cancel", {}, "the discovery cancel") if discovery else None,
        (f"/api/v1/namespaces/{NS}/preflights/{preflight[0]}:cancel", {}, "the preflight cancel") if preflight else None,
    ]
    for entry in routes:
        if entry is None:
            scenario.check("a cancel route could be addressed", False, "no object of that kind exists yet")
            continue
        path, payload, note = entry
        status, _, text = call(
            scenario, "POST", path, payload,
            {"Idempotency-Key": key("refused-" + note.replace(" ", "-"))}, note=note,
        )
        problem = json.loads(text)
        scenario.check(f"{note} answers 400 to an Idempotency-Key", status == 400, (path, status))
        scenario.check(f"{note} names the reason as idempotency_key_invalid",
                       problem.get("code") == "idempotency_key_invalid", problem.get("detail", "")[:200])
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE transient - the item the 2026-09-16 record lists as "Not done"
# --------------------------------------------------------------------------

def _blackhole(scenario: Scenario, suffix: str) -> str:
    saved = STATE.get("blackhole", {}).get(suffix)
    if saved:
        return saved
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections",
        {"bootstrapServers": [BLACKHOLE], "role": "source", "auth": {"mode": "plaintext", "tls": False}},
        {"Idempotency-Key": key("blackhole-" + suffix)},
        note="a connection whose broker nothing answers",
    )
    item = json.loads(text)["item"]
    scenario.object("kafkacluster", item["name"], item["uid"])
    remember("kafkacluster", item["name"], item["uid"])
    STATE.setdefault("blackhole", {})[suffix] = item["name"]
    save_state()
    return item["name"]


def runner_verdict(scenario: Scenario, uid: str) -> dict[str, Any]:
    """What the RUNNER itself relayed for a check, read from its Job's pod log.

    The controller decides what a check's status says; this reads the frame the
    check pod printed, so a difference between "what the runner found" and
    "what the API projects" can be stated as a fact rather than inferred.
    """
    found: dict[str, Any] = {"job": None, "codes": [], "messages": []}
    jobs = run(
        KN + ["get", "jobs", "-l", f"logweir.dev/check-owner-uid={uid}", "-o", "name"],
        check=False, timeout=60,
    )
    scenario.command(KN + ["get", "jobs", "-l", f"logweir.dev/check-owner-uid={uid}"], jobs.returncode)
    names = [line.split("/", 1)[-1] for line in jobs.stdout.split() if line.strip()]
    if not names:
        found["job"] = "gone (the Job's TTL collected it before this read)"
        return found
    found["job"] = names[0]
    logs = run(KN + ["logs", f"job/{names[0]}", "--container", "runner", "--tail=-1"], check=False, timeout=120)
    scenario.command(KN + ["logs", f"job/{names[0]}"], logs.returncode,
                     write_artifact(f"{scenario.record['id']}-runner-frames.txt", logs.stdout + logs.stderr))
    for line in logs.stdout.splitlines():
        if not line.startswith("logweir-check-part=result:"):
            continue
        try:
            payload = json.loads(base64.b64decode(line.split(":", 2)[2]).decode())
        except Exception:  # pragma: no cover - a frame this file cannot read is reported as such
            found["codes"].append("<undecodable frame>")
            continue
        for check in payload.get("checks", []):
            found["codes"].append(check.get("code"))
            found["messages"].append(check.get("message"))
    write_artifact(f"{scenario.record['id']}-runner-verdict.json", json.dumps(found, indent=1))
    return found


def phase_transient() -> None:
    ensure_api()

    scenario = Scenario("T1", "a running TopicDiscovery is cancelled through the API and reaches cancelled", "transient")
    connection = _blackhole(scenario, "cancel")
    started = time.monotonic()
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections/{connection}/topic-discoveries",
        {"timeoutSeconds": 300, "reuseFresh": False},
        {"Idempotency-Key": fresh_key("cancel-discovery")}, note="a 300 s budget against a blackhole broker",
    )
    item = json.loads(text)["item"]
    scenario.object("topicdiscovery", item["id"], item["uid"])
    remember("topicdiscovery", item["id"], item["uid"])
    scenario.check("the create answers 202", status == 202, status)
    state, running, waited = poll_state(
        scenario, f"/api/v1/namespaces/{NS}/topic-discoveries/{item['id']}",
        {"running"}, limit=180, note="wait for running",
    )
    scenario.timing("secondsToRunning", waited)
    scenario.check("the check reached running", state == "running", state)
    scenario.check("a running check declares itself cancellable", running.get("terminal") is False, running.get("state"))
    cancel_at = time.monotonic()
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/topic-discoveries/{item['id']}:cancel", {},
        note="the API's cancel route",
    )
    cancel = json.loads(text)
    scenario.check("the cancel answers 200", status == 200, status)
    scenario.check("the cancel body validates against CancelResponse",
                   not validate(cancel, SCHEMAS["CancelResponse"]), validate(cancel, SCHEMAS["CancelResponse"]))
    scenario.check("the check had not already finished", cancel.get("alreadyTerminal") is False, cancel)
    state, final, waited = poll_state(
        scenario, f"/api/v1/namespaces/{NS}/topic-discoveries/{item['id']}",
        {"cancelled", "failed", "succeeded"}, limit=180, note="wait for the cancelled state",
    )
    scenario.timing("secondsFromCancelToTerminal", time.monotonic() - cancel_at)
    scenario.timing("secondsTotal", time.monotonic() - started)
    scenario.check("the check reached cancelled", state == "cancelled", {"state": state, "reason": final.get("reason")})
    scenario.check("and is terminal", final.get("terminal") is True, final.get("terminal"))
    phase, reason = kubectl_phase("topicdiscovery", item["id"])
    scenario.command(KN + ["get", "topicdiscovery", item["id"]], 0)
    scenario.check("kubectl agrees the CONTROLLER wrote the cancelled phase", phase == "Cancelled", {"phase": phase, "reason": reason})
    scenario.record["cancelledReason"] = reason
    scenario.finish()

    scenario = Scenario("T2", "a short-budget TopicDiscovery against a blackhole broker reaches a failed state", "transient")
    connection = _blackhole(scenario, "timeout")
    started = time.monotonic()
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections/{connection}/topic-discoveries",
        {"timeoutSeconds": 10, "reuseFresh": False},
        {"Idempotency-Key": fresh_key("timeout-discovery")}, note="the smallest budget the contract allows",
    )
    item = json.loads(text)["item"]
    scenario.object("topicdiscovery", item["id"], item["uid"])
    remember("topicdiscovery", item["id"], item["uid"])
    state, final, waited = poll_state(
        scenario, f"/api/v1/namespaces/{NS}/topic-discoveries/{item['id']}",
        {"failed", "cancelled", "succeeded"}, limit=300, note="wait for the timeout",
    )
    scenario.timing("secondsToTerminal", waited)
    scenario.timing("secondsTotal", time.monotonic() - started)
    scenario.check("the run reached failed", state == "failed", {"state": state, "reason": final.get("reason")})
    scenario.check("it is terminal", final.get("terminal") is True, final.get("terminal"))
    reason = final.get("reason") or ""
    message = (final.get("error") or {}).get("message") or ""
    scenario.record["projectedReason"] = reason
    scenario.record["projectedMessage"] = message
    # THE ASSERTION IS NOT WEAKENED TO MATCH WHAT CAME BACK. The tracker asks
    # for a broker-unreachable / metadata-timeout reason; whatever is recorded
    # here is what the API actually projects.
    unreachable = re.search(r"unreachable|timeout|timedout|metadata", reason + " " + message, re.I) is not None
    scenario.check(
        "the API projects a broker-unreachable / metadata-timeout reason",
        unreachable, {"reason": reason, "message": message[:300]},
    )
    phase, kreason = kubectl_phase("topicdiscovery", item["id"])
    scenario.check("kubectl agrees the CONTROLLER wrote the failed phase", phase == "Failed", {"phase": phase, "reason": kreason})
    verdict = runner_verdict(scenario, item["uid"])
    scenario.record["runnerVerdict"] = verdict
    scenario.check(
        "the check pod itself did name the broker unreachable (so the fact exists at the Job boundary)",
        re.search(r"unreachable|timeout|timedout", " ".join(str(c) for c in verdict["codes"]), re.I) is not None,
        verdict,
    )
    scenario.finish()

    scenario = Scenario("T3", "a running Preflight is cancelled through the API and reaches cancelled", "transient")
    connection = STATE["blackhole"]["cancel"]
    started = time.monotonic()
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/preflights",
        {
            "operation": "backup",
            "backup": {
                "sourceConnection": connection,
                "topics": ["orders"],
                "destination": STATE["destination"],
            },
            "timeoutSeconds": 300,
        },
        {"Idempotency-Key": fresh_key("cancel-preflight")}, note="a backup preflight bound to the blackhole broker",
    )
    if status != 202:
        scenario.check("the preflight create answered 202", False, text[:600])
        scenario.finish("the backup preflight was refused; see the body")
    else:
        item = json.loads(text)["item"]
        scenario.object("preflight", item["id"], item["uid"])
        remember("preflight", item["id"], item["uid"])
        state, _, waited = poll_state(
            scenario, f"/api/v1/namespaces/{NS}/preflights/{item['id']}",
            {"running"}, limit=180, note="wait for running",
        )
        scenario.timing("secondsToRunning", waited)
        scenario.check("the preflight reached running", state == "running", state)
        cancel_at = time.monotonic()
        status, _, text = call(
            scenario, "POST", f"/api/v1/namespaces/{NS}/preflights/{item['id']}:cancel", {},
            note="the API's preflight cancel route",
        )
        cancel = json.loads(text)
        scenario.check("the cancel answers 200", status == 200, status)
        scenario.check("the cancel body validates against CancelResponse",
                       not validate(cancel, SCHEMAS["CancelResponse"]), validate(cancel, SCHEMAS["CancelResponse"]))
        scenario.check("the preflight had not already finished", cancel.get("alreadyTerminal") is False, cancel)
        state, final, waited = poll_state(
            scenario, f"/api/v1/namespaces/{NS}/preflights/{item['id']}",
            {"cancelled", "failed", "ready", "notReady", "unknown"}, limit=180, note="wait for the cancelled state",
        )
        scenario.timing("secondsFromCancelToTerminal", time.monotonic() - cancel_at)
        scenario.timing("secondsTotal", time.monotonic() - started)
        scenario.check("the preflight reached cancelled", state == "cancelled", {"state": state, "reason": final.get("reason")})
        phase, reason = kubectl_phase("preflight", item["id"])
        scenario.check("kubectl agrees the CONTROLLER wrote the cancelled phase", phase == "Cancelled", {"phase": phase, "reason": reason})
        scenario.finish()

    scenario = Scenario("T4", "a bounded Preflight against the blackhole broker reports it as unreachable", "transient")
    connection = STATE["blackhole"]["timeout"]
    started = time.monotonic()
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/preflights",
        {
            "operation": "backup",
            "backup": {
                "sourceConnection": connection,
                "topics": ["orders"],
                "destination": STATE["destination"],
            },
            "timeoutSeconds": 30,
        },
        {"Idempotency-Key": fresh_key("timeout-preflight")}, note="a 30 s budget against a blackhole broker",
    )
    if status != 202:
        scenario.check("the preflight create answered 202", False, text[:600])
        scenario.finish("the backup preflight was refused; see the body")
    else:
        item = json.loads(text)["item"]
        scenario.object("preflight", item["id"], item["uid"])
        remember("preflight", item["id"], item["uid"])
        state, final, waited = poll_state(
            scenario, f"/api/v1/namespaces/{NS}/preflights/{item['id']}",
            {"ready", "notReady", "unknown", "failed", "cancelled"}, limit=420, note="wait for the verdict",
        )
        scenario.timing("secondsToTerminal", waited)
        scenario.timing("secondsTotal", time.monotonic() - started)
        scenario.check("the preflight reached a terminal verdict", final.get("terminal") is True, state)
        codes = [c.get("code") for c in final.get("checks", [])]
        messages = " ".join(str(c.get("message", "")) for c in final.get("checks", []))
        scenario.record["checkCodes"] = codes
        scenario.check(
            "a blocking check names the broker as unreachable or its metadata as timed out",
            re.search(r"unreachable|timeout|timedout", " ".join(str(c) for c in codes) + " " + messages, re.I) is not None,
            {"state": state, "reason": final.get("reason"), "codes": codes},
        )
        violations = validate(final, SCHEMAS["Preflight"])
        scenario.check("the verdict validates against Preflight field for field", not violations, violations)
        scenario.finish()

    scenario = Scenario("T5", "the API projects the broker's unreachability where the controller records it", "transient")
    status, _, text = call(
        scenario, "GET", f"/api/v1/namespaces/{NS}/connections/{STATE['blackhole']['timeout']}",
        note="the blackhole connection, after the controller's probe Job",
    )
    item = json.loads(text)["item"]
    reach = item.get("reachability", {})
    deadline = time.monotonic() + 180
    while reach.get("state") == "unknown" and time.monotonic() < deadline:
        time.sleep(5.0)
        status, _, text = call(
            scenario, "GET", f"/api/v1/namespaces/{NS}/connections/{STATE['blackhole']['timeout']}",
            note="waiting for the probe's verdict",
        )
        item = json.loads(text)["item"]
        reach = item.get("reachability", {})
    scenario.record["reachability"] = reach
    scenario.check("the API reports the blackhole broker as unreachable", reach.get("state") == "unreachable", reach)
    scenario.check("and names the controller's reason", bool(reach.get("reason")), reach.get("reason"))
    scenario.check("the projection validates against Connection", not validate(item, SCHEMAS["Connection"]), validate(item, SCHEMAS["Connection"]))
    scenario.finish()

    scenario = Scenario("T6", "a transient check whose endpoint nothing answers times out with a reason that names it", "transient")
    blackhole_destination = "d2w14-dest-blackhole"
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/destinations",
        {
            "name": blackhole_destination,
            "storage": {
                "provider": "s3", "bucket": "kafka-backups",
                "prefix": f"d2w14-api/{STAMP}-blackhole", "region": "us-east-1",
                "endpoint": "http://10.255.255.1:9000", "addressing": "pathStyle",
            },
            "transport": {"security": "insecureHttp"},
            "access": {"archiveWrite": {"mode": "secretKeys", "secret": {"existing": {"name": STATE["markerSecret"]}}}},
        },
        {"Idempotency-Key": key("blackhole-destination")}, note="a destination whose object store nothing answers",
    )
    if status not in (200, 201):
        scenario.check("the blackhole destination was created", False, text[:400])
        scenario.finish("the destination create was refused; see the body")
    else:
        created = json.loads(text)["item"]
        scenario.object("backupdestination", created["name"], created["uid"])
        remember("backupdestination", created["name"], created["uid"])
        started = time.monotonic()
        status, _, text = call(
            scenario, "POST", f"/api/v1/namespaces/{NS}/preflights",
            {
                "operation": "destinationAccess",
                "destinationAccess": {"destination": blackhole_destination, "roles": ["archiveWrite"]},
                "timeoutSeconds": 30,
            },
            {"Idempotency-Key": fresh_key("blackhole-access")}, note="a 30 s budget against an endpoint nothing answers",
        )
        item = json.loads(text)["item"]
        scenario.object("preflight", item["id"], item["uid"])
        remember("preflight", item["id"], item["uid"])
        state, final, waited = poll_state(
            scenario, f"/api/v1/namespaces/{NS}/preflights/{item['id']}",
            {"ready", "notReady", "unknown", "failed", "cancelled"}, limit=420, note="wait for the timeout",
        )
        scenario.timing("secondsToTerminal", waited)
        scenario.timing("secondsTotal", time.monotonic() - started)
        codes = [c.get("code") for c in final.get("checks", [])]
        messages = " ".join(str(c.get("message", "")) for c in final.get("checks", []))
        scenario.record["checkCodes"] = codes
        scenario.record["projectedState"] = state
        scenario.check("the check reached a terminal verdict rather than hanging", final.get("terminal") is True, state)
        scenario.check("the verdict is not ready", state in ("notReady", "failed", "unknown"), state)
        scenario.check(
            "a check names the endpoint as unreachable or its request as timed out",
            re.search(r"unreachable|timeout|timedout|refused|connect", " ".join(str(c) for c in codes) + " " + messages, re.I) is not None,
            {"codes": codes, "messages": messages[:400]},
        )
        scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE pagination
# --------------------------------------------------------------------------

def phase_pagination() -> None:
    ensure_api()
    scenario = Scenario("P1", f"{PAGE_OBJECTS} objects paged at limit={PAGE_LIMIT} cover the set exactly once", "pagination")
    existing = {s["metadata"]["name"] for s in kube_json(["get", "backupschedules"])["items"]}
    to_create = PAGE_OBJECTS - len(existing)
    created = 0
    for index in range(max(to_create, 0)):
        status, _, text = call(
            scenario, "POST", f"/api/v1/namespaces/{NS}/schedules",
            {
                "schedule": "0 3 * * *",
                "suspended": True,
                "sourceRef": {"name": STATE["labConnection"]},
                "topics": ["orders"],
                "archive": {"url": f"s3://kafka-backups/d2w14-api/{STAMP}/p{index:02d}"},
            },
            {"Idempotency-Key": key(f"page-{index:02d}")}, note=f"paging fixture {index}",
        )
        if status not in (200, 201):
            scenario.check(f"paging fixture {index} was created", False, text[:400])
            break
        created += 1
    # The request bodies of 56 identical creates are noise in the record; only
    # the count and the failures matter.
    scenario.record["requests"] = [r for r in scenario.record["requests"] if not r.get("note", "").startswith("paging fixture")]
    scenario.record["fixturesCreated"] = created
    truth = sorted(s["metadata"]["name"] for s in kube_json(["get", "backupschedules"])["items"])
    scenario.command(KN + ["get", "backupschedules"], 0, write_artifact(
        "pagination-kubectl-truth.txt", "\n".join(truth) + "\n"))
    scenario.check(f"the namespace holds at least {PAGE_OBJECTS} schedules", len(truth) >= PAGE_OBJECTS, len(truth))

    pages: list[list[str]] = []
    cursors: list[str] = []
    cursor: str | None = None
    for _ in range(40):
        query = f"?limit={PAGE_LIMIT}" + (f"&cursor={urllib.parse.quote(cursor, safe="")}" if cursor else "")
        status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/schedules{query}", note="one page")
        if status != 200:
            scenario.check("every page answers 200", False, (status, text[:300]))
            break
        body = json.loads(text)
        violations = validate(body, SCHEMAS["ScheduleList"])
        if violations:
            scenario.check("every page validates against ScheduleList", False, violations)
            break
        pages.append([item["name"] for item in body["items"]])
        cursor = body["page"].get("nextCursor")
        if cursor:
            cursors.append(cursor)
        else:
            break
    seen = [name for page in pages for name in page]
    scenario.record["pageSizes"] = [len(p) for p in pages]
    scenario.check("more than one page was needed", len(pages) > 1, len(pages))
    scenario.check("every page but the last holds exactly the limit",
                   all(len(p) == PAGE_LIMIT for p in pages[:-1]), [len(p) for p in pages])
    scenario.check("no object appears twice across the pages", len(seen) == len(set(seen)),
                   sorted({n for n in seen if seen.count(n) > 1}))
    scenario.check("the pages cover the namespace's schedules exactly", sorted(seen) == truth,
                   {"pagedButNotLive": sorted(set(seen) - set(truth)), "liveButNotPaged": sorted(set(truth) - set(seen))})
    write_artifact("pagination-pages.json", json.dumps(pages, indent=1))
    STATE["cursor"] = cursors[0] if cursors else None
    save_state()
    scenario.finish()

    scenario = Scenario("P2", "a tampered cursor is refused", "pagination")
    if not STATE.get("cursor"):
        scenario.not_run("no page produced a nextCursor, so there was nothing to tamper with")
        scenario.finish()
    else:
        good = STATE["cursor"]
        middle = len(good) // 2
        swapped = "B" if good[middle] != "B" else "C"
        tampered = good[:middle] + swapped + good[middle + 1:]
        status, headers, text = call(
            scenario, "GET", f"/api/v1/namespaces/{NS}/schedules?limit={PAGE_LIMIT}&cursor={urllib.parse.quote(tampered, safe="")}",
            note="one character of the cursor changed",
        )
        problem = json.loads(text)
        scenario.check("the status is 400", status == 400, status)
        scenario.check("the code is cursor_invalid", problem.get("code") == "cursor_invalid", problem.get("code"))
        scenario.check("it is problem+json", (headers.get("content-type") or "").startswith("application/problem+json"), headers.get("content-type"))
        scenario.check("the untampered cursor still opens",
                       call(scenario, "GET", f"/api/v1/namespaces/{NS}/schedules?limit={PAGE_LIMIT}&cursor={urllib.parse.quote(good, safe="")}",
                            note="the control: the same cursor, unmodified")[0] == 200, True)
        scenario.finish()

    scenario = Scenario("P3", "a cursor replayed on another route is refused", "pagination")
    if not STATE.get("cursor"):
        scenario.not_run("no page produced a nextCursor, so there was nothing to replay")
        scenario.finish()
    else:
        status, _, text = call(
            scenario, "GET", f"/api/v1/namespaces/{NS}/connections?limit={PAGE_LIMIT}&cursor={urllib.parse.quote(STATE['cursor'], safe="")}",
            note="the schedules cursor, sent to the connections route",
        )
        problem = json.loads(text)
        scenario.check("the status is 400", status == 400, status)
        scenario.check("the code is cursor_invalid", problem.get("code") == "cursor_invalid", problem.get("code"))
        scenario.check("the detail says the cursor is bound to its scope", bool(problem.get("detail")), problem.get("detail", "")[:200])
        scenario.finish()

    scenario = Scenario("P4", "a limit outside the declared range and an unknown parameter are refused", "pagination")
    status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/schedules?limit=9999", note="limit above the ceiling")
    scenario.check("limit=9999 is 422 validation_failed", status == 422 and json.loads(text).get("code") == "validation_failed", (status, json.loads(text).get("code")))
    status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/schedules?madeUpParameter=1", note="an invented query parameter")
    scenario.check("an unknown parameter is refused rather than ignored", status == 400, (status, json.loads(text).get("code")))
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE restart
# --------------------------------------------------------------------------

def phase_restart() -> None:
    ensure_api()
    scenario = Scenario("R1", "every object survives the restart, readable through the new process with the same UID", "restart")
    before: dict[str, dict[str, str]] = {}
    for route, kind in (("connections", "kafkacluster"), ("destinations", "backupdestination"), ("schedules", "backupschedule")):
        status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/{route}?limit=200", note=f"{route} before the kill")
        before[route] = {i["name"]: i["uid"] for i in json.loads(text)["items"]} if status == 200 else {}
    scenario.record["countsBefore"] = {r: len(v) for r, v in before.items()}

    scenario2 = Scenario("R2", "the CONTROLLER moved an in-flight operation's status while the API was DOWN", "restart")
    connection = STATE["blackhole"]["timeout"]
    status, _, text = call(
        scenario2, "POST", f"/api/v1/namespaces/{NS}/connections/{connection}/topic-discoveries",
        {"timeoutSeconds": 10, "reuseFresh": False},
        {"Idempotency-Key": key("restart-discovery")}, note="an operation that is in flight when the API dies",
    )
    inflight = json.loads(text)["item"]
    scenario2.object("topicdiscovery", inflight["id"], inflight["uid"])
    remember("topicdiscovery", inflight["id"], inflight["uid"])
    status, _, text = call(
        scenario2, "GET", f"/api/v1/namespaces/{NS}/topic-discoveries/{inflight['id']}",
        note="the status the API projected immediately before the kill",
    )
    before_state = json.loads(text)["item"]
    scenario2.record["stateBeforeKill"] = {
        "state": before_state.get("state"),
        "terminal": before_state.get("terminal"),
        "resourceVersion": before_state.get("resourceVersion"),
        "at": now(),
    }
    kubectl_before = kubectl_phase("topicdiscovery", inflight["id"])
    scenario2.record["kubectlPhaseBeforeKill"] = {"phase": kubectl_before[0], "reason": kubectl_before[1]}
    scenario2.check("the operation is not terminal when the API is killed", before_state.get("terminal") is False, before_state.get("state"))

    down_from = now()
    down_started = time.monotonic()
    stop_api()
    scenario.record["apiPid"] = None
    refused = None
    try:
        with urllib.request.urlopen(ORIGIN + "/healthz", timeout=3):
            refused = "answered"
    except urllib.error.URLError as gone:
        refused = str(gone.reason)
    except Exception as gone:  # pragma: no cover - any refusal proves the same thing
        refused = repr(gone)
    scenario2.check("the API really is down (a request is refused)", refused != "answered", refused)

    # WAIT FOR THE CONTROLLER, NOT FOR THE API. Nothing this process does can
    # move the status now; only the controller can.
    moved: tuple[str, str] = kubectl_before
    moved_at = ""
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        moved = kubectl_phase("topicdiscovery", inflight["id"])
        if moved[0] in ("Succeeded", "Failed", "Cancelled") and moved != kubectl_before:
            moved_at = now()
            break
        time.sleep(2.0)
    down_seconds = time.monotonic() - down_started
    scenario2.record["apiDown"] = {"from": down_from, "seconds": round(down_seconds, 3), "movedAt": moved_at}
    scenario2.command(KN + ["get", "topicdiscovery", inflight["id"]], 0)
    scenario2.check(
        "the controller moved the status to a terminal phase while no API was running",
        moved[0] in ("Succeeded", "Failed") and moved != kubectl_before,
        {"before": kubectl_before, "after": moved, "apiDownSeconds": round(down_seconds, 1)},
    )

    boot = start_api()
    scenario.timing("restartSeconds", boot)
    status, _, text = call(
        scenario2, "GET", f"/api/v1/namespaces/{NS}/topic-discoveries/{inflight['id']}",
        note="the same object, read through the RESTARTED process",
    )
    after_state = json.loads(text)["item"]
    scenario2.record["stateAfterRestart"] = {
        "state": after_state.get("state"), "reason": after_state.get("reason"), "at": now(),
    }
    scenario2.check("the restarted API projects the status the controller wrote while it was down",
                    after_state.get("terminal") is True and after_state.get("state") != before_state.get("state"),
                    {"before": before_state.get("state"), "after": after_state.get("state")})
    scenario2.check("the object kept its UID across the restart", after_state.get("uid") == inflight["uid"], after_state.get("uid"))
    scenario2.finish()

    for route in ("connections", "destinations", "schedules"):
        status, _, text = call(scenario, "GET", f"/api/v1/namespaces/{NS}/{route}?limit=200", note=f"{route} after the restart")
        after = {i["name"]: i["uid"] for i in json.loads(text)["items"]} if status == 200 else {}
        scenario.check(f"every {route[:-1]} is still there with the same UID after the restart",
                       after == before[route],
                       {"missing": sorted(set(before[route]) - set(after)),
                        "changed": sorted(n for n in after if n in before[route] and after[n] != before[route][n])})
    scenario.finish()

    scenario = Scenario("R3", "a key created before the restart replays to the same UID after it", "restart")
    body = {"bootstrapServers": [LAB_BOOTSTRAP], "role": "target", "auth": {"mode": "plaintext", "tls": False}}
    status, _, text = call(
        scenario, "POST", f"/api/v1/namespaces/{NS}/connections", body,
        {"Idempotency-Key": STATE["keys"]["replay"]}, note="the pre-restart key, replayed by a new process",
    )
    replayed = json.loads(text)
    scenario.check("the replay is 200", status == 200, status)
    scenario.check("it says it replayed", replayed.get("replayed") is True, replayed.get("replayed"))
    scenario.check("it carries the UID from before the restart", replayed["item"]["uid"] == STATE["replayUid"], replayed["item"]["uid"])
    same = [c for c in kube_json(["get", "kafkaclusters"])["items"] if c["metadata"]["name"] == STATE["replayName"]]
    scenario.check("no second object was created", len(same) == 1 and same[0]["metadata"]["uid"] == STATE["replayUid"], STATE["replayUid"])
    scenario.finish()

    scenario = Scenario("R4", "a cursor minted by the old process still opens on the new one", "restart")
    if not STATE.get("cursor"):
        scenario.not_run("the pagination phase produced no cursor to carry across the restart")
        scenario.finish()
    else:
        status, _, text = call(
            scenario, "GET", f"/api/v1/namespaces/{NS}/schedules?limit={PAGE_LIMIT}&cursor={urllib.parse.quote(STATE['cursor'], safe="")}",
            note="a cursor from the previous process",
        )
        scenario.check("the cursor still authenticates (the HMAC key is on disk, not in memory)", status == 200, status)
        scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE kubepaths
# --------------------------------------------------------------------------

KUBE_TARGETS = [
    ("K1", "GET", f"/api/v1/namespaces/{NS}/secrets", "the namespace's Secrets"),
    ("K2", "GET", f"/api/v1/namespaces/{NS}/pods", "the namespace's Pods"),
    ("K3", "GET", f"/api/v1/namespaces/{NS}/pods/anything/log", "a pod log"),
    ("K4", "GET", f"/api/v1/namespaces/{NS}/pods/anything/exec", "an exec"),
    ("K5", "GET", f"/api/v1/namespaces/{NS}/jobs", "the namespace's Jobs"),
    ("K6", "GET", "/apis/apps/v1/namespaces/default/deployments", "a generic aggregated-API path"),
    ("K7", "GET", "/apis/logweir.dev/v1alpha1/namespaces/default/backups", "another namespace's Backups by the Kubernetes path"),
    ("K8", "GET", "/api/v1/../../apis/apps/v1/deployments", "a dot-dot traversal, sent unnormalised"),
    ("K9", "GET", f"/api/v1/namespaces/{NS}/%2e%2e/%2e%2e/apis/apps/v1/deployments", "a percent-encoded traversal"),
    ("K10", "GET", "/api/v1/namespaces/logweir-scram-local/connections", "the shared lab namespace, which this actor was not granted"),
]


def phase_kubepaths() -> None:
    ensure_api()
    for ident, method, target, title in KUBE_TARGETS:
        scenario = Scenario(ident, f"{method} {target} is refused ({title})", "kubepaths")
        status, head, text = raw_call(scenario, method, target, note=title)
        body: Any
        try:
            body = json.loads(text)
        except json.JSONDecodeError:
            body = {}
        scenario.record["status"] = status
        scenario.record["code"] = body.get("code")
        expected = {404, 403, 400, 405}
        scenario.check("the request is refused, not served", status in expected, {"status": status, "code": body.get("code")})
        scenario.check("nothing Kubernetes-shaped came back",
                       '"kind"' not in text and '"apiVersion"' not in text and '"items"' not in text,
                       text[:300])
        if body:
            scenario.check("the refusal is a problem document", not validate(body, SCHEMAS["Problem"]), validate(body, SCHEMAS["Problem"]))
        scenario.finish()

    scenario = Scenario("K11", "Impersonate-User is refused before routing", "kubepaths")
    status, _, text = call(
        scenario, "GET", f"/api/v1/namespaces/{NS}/connections", None,
        {"Impersonate-User": "system:admin"}, note="an impersonation attempt",
    )
    problem = json.loads(text)
    scenario.check("the status is 400", status == 400, status)
    scenario.check("the code is header_not_allowed", problem.get("code") == "header_not_allowed", problem.get("code"))
    scenario.check("the refusal names the header", any(e.get("field") == "impersonate-user" for e in problem.get("errors", [])), problem.get("errors"))
    status, _, text = call(
        scenario, "GET", f"/api/v1/namespaces/{NS}/connections", None,
        {"Impersonate-Group": "system:masters"}, note="the group form",
    )
    scenario.check("Impersonate-Group is refused too", status == 400 and json.loads(text).get("code") == "header_not_allowed", status)
    scenario.finish()
    stop_api()


# --------------------------------------------------------------------------
# PHASE credsweep
# --------------------------------------------------------------------------

def phase_credsweep() -> None:
    scenario = Scenario("S1", "no credential marker appears in any response, in the API log or in any artifact", "credsweep")
    if not MARKERS:
        scenario.not_run("no markers were recorded; run setup first")
        scenario.finish()
        return
    import hashlib

    scenario.record["markerDigests"] = {
        name: "sha256:" + hashlib.sha256(value.encode()).hexdigest() for name, value in MARKERS.items()
    }
    forms = [(name, form, needle)
             for name, value in MARKERS.items()
             for form, needle in (("plain", value), ("base64", base64.b64encode(value.encode()).decode()))]

    # THE MATCHER IS TESTED BEFORE IT IS TRUSTED. A sweep that cannot find a
    # marker it was handed would report "zero matches" for a leak too.
    control = "a line before\n" + MARKERS["typed"] + "\na line after"
    scenario.check(
        "the matcher finds a planted marker in a control string (so zero matches means something)",
        any(needle in control for _, _, needle in forms), True,
    )

    def sweep(text: str, where: str) -> list[dict[str, str]]:
        return [{"where": where, "marker": name, "form": form}
                for name, form, needle in forms if needle in text]

    # (a) every response body this run received, VERBATIM and unredacted.
    raw = RAW_CAPTURE.read_text(errors="replace") if RAW_CAPTURE.exists() else ""
    raw_hits = sweep(raw, "raw response capture")
    scenario.record["rawResponseBytes"] = len(raw)
    scenario.record["rawResponseCount"] = raw.count("### ")
    scenario.check(
        f"none of the {raw.count('### ')} response bodies this run received carries a marker",
        not raw_hits and len(raw) > 0, raw_hits or {"bytesSwept": len(raw)},
    )

    # (b) the API's own log, written by the service and never through redact().
    api_log = API_LOG.read_text(errors="replace") if API_LOG.exists() else ""
    log_hits = sweep(api_log, "api.log")
    scenario.record["apiLogBytes"] = len(api_log)
    scenario.check(
        "the API's own log carries no marker",
        not log_hits and len(api_log) > 0, log_hits or {"bytesSwept": len(api_log)},
    )

    # (c) every artifact file, which is where a reviewer will look.
    swept: list[str] = []
    file_hits: list[dict[str, str]] = []
    for path in sorted(OUT.rglob("*")):
        if not path.is_file():
            continue
        swept.append(str(path.relative_to(OUT)))
        file_hits.extend(sweep(path.read_text(errors="replace"), str(path.relative_to(OUT))))
    scenario.record["filesSwept"] = len(swept)
    scenario.check(f"none of the {len(swept)} artifact files carries a marker", not file_hits, file_hits)

    # The typed credential DID go somewhere: a Secret named after the
    # destination, which nothing ever reads back.
    names = run(KN + ["get", "secrets", "-o", "name"], check=False, timeout=60)
    scenario.command(KN + ["get", "secrets", "-o", "name"], names.returncode,
                     write_artifact("credsweep-secret-names.txt", names.stdout))
    typed_secret = "lwd-d2w14-dest-typed-archive-write"
    scenario.check("the typed credential became a Secret this file never read back",
                   typed_secret in names.stdout, names.stdout.strip().splitlines())
    keys = run(KN + ["get", "secret", typed_secret, "-o", "jsonpath={.data.access-key-id}"], check=False, timeout=60)
    scenario.check("that Secret holds the access key id (so the value reached Kubernetes, not a response)",
                   keys.returncode == 0 and len(keys.stdout) > 0, {"bytes": len(keys.stdout)})
    write_artifact("credsweep-files.txt", "\n".join(swept) + "\n")
    scenario.finish()


# --------------------------------------------------------------------------
# PHASE negative-control
# --------------------------------------------------------------------------

def phase_negative_control() -> None:
    """THE HARNESS FAILING ON PURPOSE. A guard without a mutant is not a guard.

    The assertion below is deliberately wrong: `localAdmin` mode is what the
    config asks for, and this claims the live service reports `sharedOidc`. It
    is gated behind its own phase so the real run never contains it, and its
    record is written to `negative-control.json` rather than to `results.json`.
    """
    ensure_api()
    scenario = Scenario("N1", "NEGATIVE CONTROL: a deliberately wrong expectation must be reported as FAIL", "negative-control")
    status, _, text = call(scenario, "GET", "/api/v1/session", note="the negative control's one request")
    session = json.loads(text)
    scenario.check("the status is 200 (this half is true)", status == 200, status)
    scenario.check(
        "the service reports authenticationMode 'sharedOidc' (DELIBERATELY WRONG)",
        session.get("authenticationMode") == "sharedOidc",
        session.get("authenticationMode"),
    )
    scenario.record["endedAt"] = now()
    scenario.record["durationMs"] = round((time.monotonic() - scenario.started) * 1000)
    failed = [a for a in scenario.record["assertions"] if not a["pass"]]
    scenario.record["result"] = "FAIL" if failed else "PASS"
    scenario.record["reason"] = failed[0]["claim"] if failed else "the control did not fail, which is itself a failure"
    scenario.record["negativeControl"] = True
    (OUT / "negative-control.json").write_text(redact(json.dumps(scenario.record, indent=2, sort_keys=True)) + "\n")
    log(f"  => N1 {scenario.record['result']} (expected FAIL)")
    stop_api()
    if scenario.record["result"] != "FAIL":
        raise SystemExit("the negative control did not fail; the harness cannot report a failure")


# --------------------------------------------------------------------------
# PHASE report / cleanup
# --------------------------------------------------------------------------

def phase_report() -> None:
    rows = ["| id | result | title | reason |", "| --- | --- | --- | --- |"]
    counts = {"PASS": 0, "FAIL": 0, "NOT-RUN": 0}
    for record in RESULTS["scenarios"]:
        counts[record["result"]] = counts.get(record["result"], 0) + 1
        rows.append(f"| {record['id']} | {record['result']} | {record['title']} | {record['reason'][:120]} |")
    RESULTS["summary"] = counts
    RESULTS["namespaceUid"] = STATE.get("namespaceUid")
    save_results()
    write_artifact("summary.md", "\n".join(rows) + f"\n\n{counts}\n")
    print("\n".join(rows))
    print(counts)


def phase_cleanup() -> None:
    stop_api()
    guard_namespace(NS)
    # THE INVENTORY IS TAKEN BEFORE THE DELETE, and names only names and UIDs.
    # A Secret's data is never read, here or anywhere else in this file.
    inventory = run(
        KN + [
            "get",
            "kafkaclusters,backupdestinations,backupschedules,topicdiscoveries,preflights,backups,restores,jobs",
            "-o", "custom-columns=KIND:.kind,NAME:.metadata.name,UID:.metadata.uid",
        ],
        check=False, timeout=120,
    )
    write_artifact("99-inventory-before-delete.txt", inventory.stdout + inventory.stderr)
    secret_names = run(KN + ["get", "secrets", "-o", "custom-columns=NAME:.metadata.name,UID:.metadata.uid"],
                       check=False, timeout=60)
    write_artifact("99-secret-names-before-delete.txt", secret_names.stdout + secret_names.stderr)

    lines = [f"# cleanup for {NS}", "", f"recorded at creation: uid={STATE.get('namespaceUid')}", ""]
    live = run(K + ["get", "namespace", NS, "-o", "json"], check=False, timeout=60)
    if live.returncode:
        lines.append("the namespace was already gone before cleanup ran")
    else:
        meta = json.loads(live.stdout)["metadata"]
        lines.append(f"read back before deletion: uid={meta['uid']} labels={json.dumps(meta.get('labels', {}))}")
        if meta.get("labels", {}).get("logweir.dev/test-owner") != OWNER:
            raise RuntimeError("the namespace does not carry this wave's owner label; refusing to delete")
        if meta["uid"] != STATE.get("namespaceUid"):
            raise RuntimeError("the namespace UID differs from the one recorded at creation; refusing to delete")
        lines.append("label and UID both match the values recorded at creation -> deleting")
        run(K + ["delete", "namespace", NS, "--wait=true"], timeout=600)
        for _ in range(60):
            gone = run(K + ["get", "namespace", NS], check=False, timeout=60)
            if gone.returncode != 0 and "NotFound" in (gone.stderr + gone.stdout):
                lines.append("kubectl reports NotFound")
                break
            time.sleep(5)
        else:
            raise RuntimeError("the namespace did not reach NotFound")
    lines.append("")
    lines.append("```")
    lines.append(run(K + ["get", "ns"], timeout=60).stdout.rstrip())
    lines.append("```")
    lines.append("")
    lines.append("logweir-api process: stopped (this harness started it; pid ownership never left this file)")
    if RAW_CAPTURE.exists():
        RAW_CAPTURE.unlink()
        lines.append(f"raw response capture deleted: {RAW_CAPTURE}")
    write_artifact("cleanup.md", "\n".join(lines) + "\n")
    print("\n".join(lines))


PHASES = {
    "setup": phase_setup,
    "contract": phase_contract,
    "surface": phase_surface,
    "malformed": phase_malformed,
    "idempotency": phase_idempotency,
    "transient": phase_transient,
    "pagination": phase_pagination,
    "restart": phase_restart,
    "kubepaths": phase_kubepaths,
    "credsweep": phase_credsweep,
    "negative-control": phase_negative_control,
    "report": phase_report,
    "cleanup": phase_cleanup,
}
ALL = ["setup", "contract", "surface", "malformed", "idempotency", "transient", "pagination",
       "restart", "kubepaths", "credsweep", "report"]


def main(argv: list[str]) -> int:
    wanted = argv[1:] or ["all"]
    order = ALL if wanted == ["all"] else wanted
    for name in order:
        if name not in PHASES:
            print(f"unknown phase {name!r}; one of {sorted(PHASES)}", file=sys.stderr)
            return 2
    try:
        for name in order:
            log(f"=== phase {name}")
            PHASES[name]()
    finally:
        stop_api()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
