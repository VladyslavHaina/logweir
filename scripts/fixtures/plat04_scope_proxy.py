#!/usr/bin/env python3
"""Namespace-scoping and bounded fault barrier for the PLAT04/D1 live harnesses.

The real controller still talks HTTP to a Kubernetes API.  This proxy rewrites
cluster-wide list/watch URLs for namespaced resources to one namespace, then
forwards the requests to the real docker-desktop API server with the pod's
service-account credential.  Its small control API can pause one identified
controller request, inject a status code for one request or until released,
and it counts every request by resource and shape so a read-cost scenario can
be measured from the only place that sees them.

Why the rewrite table is explicit, and why an unknown resource is NOT rewritten
-----------------------------------------------------------------------------
The controller watches fourteen Logweir kinds.  Twelve are namespaced and are
watched with `Api::all`, so their collection URL carries no namespace and must
be rewritten or the fenced controller reconciles the whole cluster — including
the shared lab release this fence exists to stay out of.  TWO ARE CLUSTER-
SCOPED (`trustpolicies`, `trustrosters`): their collection URL also carries no
namespace, and rewriting it would produce a 404 for a kind the controller needs
to read.  A rule that guessed from the URL alone cannot tell those two cases
apart, so the table names every resource and an unknown one is forwarded
unchanged and recorded under `unknownResources` — a fence that silently
forwarded an unknown namespaced kind cluster-wide would be no fence at all.

This module is importable: importing it neither reads a cluster credential nor
binds a port.  `python3 plat04_scope_proxy.py` still serves, exactly as before.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import ssl
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


NAMESPACE = os.environ.get("PLAT04_NAMESPACE", "")
UPSTREAM_HOST = os.environ.get("KUBERNETES_SERVICE_HOST", "")
UPSTREAM_PORT = int(os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443"))
TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

# Every namespaced kind the controller watches with `Api::all` (one entry per
# CRD in `config/crd` whose scope is Namespaced, checked against the live CRD
# list by the fence harness before the controller is deployed).
NAMESPACED_LOGWEIR = frozenset(
    {
        "approvals",
        "backupdestinations",
        "backups",
        "backupschedules",
        "kafkaclusters",
        "preflights",
        "protectionpolicies",
        "recoverycatalogs",
        "rehearsalschedules",
        "restores",
        "retentionpolicies",
        "topicdiscoveries",
    }
)

# CLUSTER-SCOPED. Never rewritten: these have no namespace at all, and a
# rewritten URL is a 404 for a kind `trust.rs` and the Backup/Restore watchers
# read on every reconcile.
CLUSTER_SCOPED_LOGWEIR = frozenset({"trustpolicies", "trustrosters"})

# The non-Logweir collections the controller lists or watches cluster-wide.
# `jobs` is the only one built with `Api::all` in the tree today; the core
# kinds are listed because a namespaced-by-construction URL costs nothing to
# rewrite and a future `Api::all` on one of them must not escape the fence.
NAMESPACED_OTHER: dict[str, frozenset[str]] = {
    "batch/v1": frozenset({"jobs", "cronjobs"}),
    "": frozenset(
        {
            "pods",
            "configmaps",
            "events",
            "secrets",
            "services",
            "serviceaccounts",
            "endpoints",
            "persistentvolumeclaims",
        }
    ),
    "apps/v1": frozenset({"deployments", "replicasets", "statefulsets", "daemonsets"}),
    "events.k8s.io/v1": frozenset({"events"}),
}

def parse_arm(query: str) -> dict[str, object] | None:
    """Build an arming record from a `/__arm` query string.

    `PLAT04_ARM` sets one BEFORE the first request, which is the only way to
    hold a request the controller sends in the first second after a restart:
    the proxy and the controller start in the same pod, so a scenario that
    armed over the control API would always be racing it. L-05.2-3 needs
    exactly that — "the proxy answers the FIRST migration PATCH after restart
    with 409" — and a race would silently turn it into "some later PATCH".
    """
    if not query:
        return None
    values = urllib.parse.parse_qs(query.lstrip("?"))
    kind = values.get("kind", [""])[0]
    if not kind:
        return None
    return {
        "kind": kind,
        "name": values.get("name", [""])[0],
        "mode": values.get("mode", ["pause"])[0],
        "code": int(values.get("code", ["503"])[0]),
        "after": int(values.get("after", ["0"])[0]),
        "repeat": values.get("repeat", ["false"])[0] == "true",
        "seen": 0,
        "fired": 0,
    }


LOCK = threading.Lock()
RELEASE = threading.Event()
ARM: dict[str, object] | None = parse_arm(os.environ.get("PLAT04_ARM", ""))
FAULT_404_NAME: str | None = None
CAPTURES: list[dict[str, object]] = []
COUNTS: dict[str, int] = {}
REQUESTS: list[dict[str, object]] = []
UNKNOWN: dict[str, int] = {}
FORWARDED: dict[str, int] = {}
# Captures are the request-by-request record a scenario reads back, so they
# hold bodies and are capped. `other` is NOT captured: a reconciling controller
# issues thousands of uninteresting reads, and keeping them would evict the
# twenty migration PATCHes L-05.2-1's resourceVersion clause is checked over
# within a minute of them being sent. Everything is still COUNTED.
MAX_CAPTURES = 2000
MAX_REQUESTS = 40000
CAPTURED_KINDS = frozenset(
    {
        "reservation",
        "schedule_final",
        "backup_create",
        "backup_get",
        "backup_patch",
        "backup_status",
        "migration_patch",
        "configmap_create",
        "job_create",
    }
)


def parse_path(path: str) -> dict[str, object]:
    """Split a Kubernetes request path into the pieces every rule below needs.

    Returns `group` ("" for the core `/api/v1` group), `version`, `namespace`
    (None for a cluster-wide URL), `resource`, `name`, `subresource` and the
    query string. An unparseable path yields an empty `resource`, which every
    caller treats as "forward unchanged".
    """
    parsed = urllib.parse.urlsplit(path)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    out: dict[str, object] = {
        "group": "",
        "version": "",
        "namespace": None,
        "resource": "",
        "name": None,
        "subresource": None,
        "query": parsed.query,
        "watchPrefix": False,
    }
    if not parts:
        return out
    if parts[0] == "api" and len(parts) >= 2:
        out["version"] = parts[1]
        rest = parts[2:]
    elif parts[0] == "apis" and len(parts) >= 3:
        out["group"] = parts[1]
        out["version"] = parts[2]
        rest = parts[3:]
    else:
        return out
    # The deprecated `/watch/` prefix form. kube-rs does not emit it, but a
    # client that did would otherwise slip past the rewrite entirely.
    if rest and rest[0] == "watch":
        out["watchPrefix"] = True
        rest = rest[1:]
    if len(rest) >= 2 and rest[0] == "namespaces" and rest[1] != "":
        # `/namespaces/<ns>` alone addresses the Namespace object itself.
        if len(rest) == 2:
            out["resource"] = "namespaces"
            out["name"] = rest[1]
            return out
        out["namespace"] = rest[1]
        rest = rest[2:]
    if not rest:
        return out
    out["resource"] = rest[0]
    if len(rest) >= 2:
        out["name"] = rest[1]
    if len(rest) >= 3:
        out["subresource"] = "/".join(rest[2:])
    return out


def api_key(info: dict[str, object]) -> str:
    group = str(info["group"])
    version = str(info["version"])
    return f"{group}/{version}" if group else ""


def is_namespaced(info: dict[str, object]) -> bool | None:
    """True, False, or None when the table does not name this resource."""
    resource = str(info["resource"])
    if not resource:
        return None
    if str(info["group"]) == "logweir.dev":
        if resource in NAMESPACED_LOGWEIR:
            return True
        if resource in CLUSTER_SCOPED_LOGWEIR:
            return False
        return None
    table = NAMESPACED_OTHER.get(api_key(info))
    if table is None:
        return None
    if resource in table:
        return True
    return None


def rewrite(path: str, namespace: str | None = None) -> str:
    """Scope one cluster-wide collection URL to the fenced namespace.

    A URL that already names a namespace, a cluster-scoped kind, a named
    object, or a resource this table does not know is returned unchanged.
    """
    target = namespace if namespace is not None else NAMESPACE
    if not target:
        return path
    info = parse_path(path)
    if info["namespace"] is not None or info["name"] is not None:
        return path
    if is_namespaced(info) is not True:
        if str(info["group"]) == "logweir.dev" and is_namespaced(info) is None:
            with LOCK:
                UNKNOWN[str(info["resource"])] = UNKNOWN.get(str(info["resource"]), 0) + 1
        return path
    prefix = "/api/" + str(info["version"]) if not info["group"] else (
        "/apis/" + str(info["group"]) + "/" + str(info["version"])
    )
    if info["watchPrefix"]:
        prefix += "/watch"
    new_path = f"{prefix}/namespaces/{target}/{info['resource']}"
    return urllib.parse.urlunsplit(("", "", new_path, str(info["query"]), ""))


def shape(method: str, path: str) -> str:
    """The counting key: what kind of read or write this request is.

    `LIST` and `GET` are distinguished because D1 §6.7's read-cost criterion is
    stated in exactly those terms, and a watch is separated from a list because
    a watch is one long-lived request, not a per-reconcile cost.
    """
    info = parse_path(path)
    resource = str(info["resource"]) or "?"
    query = urllib.parse.parse_qs(str(info["query"]))
    if info["subresource"]:
        resource = f"{resource}/{info['subresource']}"
    if method == "GET":
        if info["name"] is not None:
            return f"GET {resource}"
        if query.get("watch", ["false"])[0] not in {"false", "0", ""}:
            return f"WATCH {resource}"
        return f"LIST {resource}"
    return f"{method} {resource}"


def classify(method: str, path: str, body: bytes) -> tuple[str, str, str]:
    """Name the one controller request a scenario wants to hold or fault."""
    info = parse_path(path)
    resource = str(info["resource"])
    name = str(info["name"] or "")
    schedule_name = ""
    payload: dict[str, object] = {}
    if body:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}
    status = payload.get("status") or {}
    if resource == "backupschedules" and info["subresource"] == "status":
        pending = (status.get("pendingBackupRef") or {}).get("name") if isinstance(status, dict) else None
        # A reservation is a PATCH today (`reservation_patch`, "A PATCH AND NOT
        # A PUT, AND THAT IS AN RBAC CONTRACT"); it was a PUT in the controller
        # `scripts/test-plat04-live.py` was written against. Both are the same
        # decision and both are named `reservation` so neither harness drifts.
        if method in {"PUT", "PATCH"} and pending:
            return "reservation", name, name
        if method in {"PUT", "PATCH"}:
            return "schedule_final", name, name
    if resource == "backups" and info["subresource"] is None:
        if method == "POST":
            meta = payload.get("metadata") or {}
            spec = payload.get("spec") or {}
            name = str(meta.get("name") or "")
            schedule_name = str(((spec.get("scheduleRef") or {}).get("name")) or "")
            return "backup_create", name, schedule_name
        if method == "PATCH" and name:
            # The retained-history migration is the only PATCH this controller
            # sends to a `Backup`'s main resource (`schedule_history.rs`
            # `migrate_one`); it rewrites `ownerReferences` and carries the
            # object's own `resourceVersion` as the precondition.
            meta = payload.get("metadata") or {}
            if isinstance(meta, dict) and "ownerReferences" in meta:
                return "migration_patch", name, ""
            return "backup_patch", name, ""
        if method == "GET" and name:
            return "backup_get", name, ""
        if method == "GET":
            return "backup_list", "", ""
    if resource == "backups" and info["subresource"] == "status":
        return "backup_status", name, ""
    if resource == "configmaps" and method == "POST":
        meta = payload.get("metadata") or {}
        return "configmap_create", str(meta.get("name") or ""), ""
    if resource == "jobs" and method == "POST":
        meta = payload.get("metadata") or {}
        return "job_create", str(meta.get("name") or ""), ""
    return "other", name, schedule_name


def capture(kind: str, name: str, schedule: str, method: str, path: str, body: bytes) -> dict[str, object]:
    item: dict[str, object] = {
        "at": time.time(),
        "kind": kind,
        "name": name,
        "schedule": schedule,
        "method": method,
        "path": path,
        "bodySha256": hashlib.sha256(body).hexdigest(),
    }
    if kind in {"reservation", "schedule_final", "backup_create", "migration_patch"} and body:
        try:
            item["body"] = json.loads(body)
        except json.JSONDecodeError:
            item["bodyUnparseable"] = True
    if kind not in CAPTURED_KINDS:
        return item
    with LOCK:
        CAPTURES.append(item)
        if len(CAPTURES) > MAX_CAPTURES:
            del CAPTURES[:-MAX_CAPTURES]
    return item


def count(method: str, path: str) -> None:
    key = shape(method, path)
    info = parse_path(path)
    with LOCK:
        COUNTS[key] = COUNTS.get(key, 0) + 1
        if len(REQUESTS) < MAX_REQUESTS:
            REQUESTS.append(
                {
                    "at": round(time.time(), 3),
                    "shape": key,
                    "resource": str(info["resource"]),
                    "name": info["name"],
                    "limit": urllib.parse.parse_qs(str(info["query"])).get("limit", [None])[0],
                    "continue": bool(
                        urllib.parse.parse_qs(str(info["query"])).get("continue")
                    ),
                }
            )


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def json_response(self, code: int, value: object) -> None:
        data = json.dumps(value, sort_keys=True).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def control(self, parsed: urllib.parse.SplitResult) -> bool:
        global ARM, FAULT_404_NAME
        if parsed.path == "/__state":
            with LOCK:
                value = {
                    "arm": ARM,
                    "fault404Name": FAULT_404_NAME,
                    "captures": list(CAPTURES),
                    "counts": dict(COUNTS),
                    "unknownResources": dict(UNKNOWN),
                    "forwarded": dict(FORWARDED),
                    "namespace": NAMESPACE,
                }
            self.json_response(200, value)
            return True
        if parsed.path == "/__requests":
            query = urllib.parse.parse_qs(parsed.query)
            since = float(query.get("since", ["0"])[0])
            with LOCK:
                value = {
                    "now": time.time(),
                    "truncated": len(REQUESTS) >= MAX_REQUESTS,
                    "requests": [r for r in REQUESTS if float(r["at"]) >= since],
                }
            self.json_response(200, value)
            return True
        if parsed.path == "/__reset":
            with LOCK:
                COUNTS.clear()
                REQUESTS.clear()
                CAPTURES.clear()
            self.json_response(200, {"reset": True, "at": time.time()})
            return True
        if parsed.path == "/__arm":
            armed = parse_arm(parsed.query) or {}
            with LOCK:
                ARM = armed
                FORWARDED.clear()
                RELEASE.clear()
            self.json_response(200, {"armed": armed})
            return True
        if parsed.path == "/__disarm":
            with LOCK:
                ARM = None
            RELEASE.set()
            self.json_response(200, {"disarmed": True})
            return True
        if parsed.path == "/__release":
            RELEASE.set()
            with LOCK:
                if ARM and ARM.get("repeat"):
                    ARM = None
            self.json_response(200, {"released": True})
            return True
        return False

    def decide(self, kind: str, name: str, schedule: str) -> tuple[dict[str, object] | None, bool]:
        """Does the armed injection apply to this request?

        Returns the arming record and whether it fires now. `after` forwards
        that many matching requests first (L-05.2-3 forwards seven migration
        PATCHes before the controller is stopped); `repeat` keeps firing until
        `/__release` (L-05.2-2's plan ConfigMap 503).
        """
        global ARM
        with LOCK:
            arm = ARM
            if not arm:
                return None, False
            if arm["kind"] != kind:
                return None, False
            wanted = str(arm["name"])
            if wanted and wanted not in {name, schedule}:
                return None, False
            arm["seen"] = int(arm["seen"]) + 1
            if int(arm["seen"]) <= int(arm["after"]):
                FORWARDED[kind] = FORWARDED.get(kind, 0) + 1
                return arm, False
            arm["fired"] = int(arm["fired"]) + 1
            if not arm["repeat"]:
                ARM = None
            return arm, True

    def handle_request(self) -> None:
        global FAULT_404_NAME
        parsed = urllib.parse.urlsplit(self.path)
        if self.control(parsed):
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        kind, name, schedule = classify(self.command, self.path, body)
        item = capture(kind, name, schedule, self.command, self.path, body)
        count(self.command, self.path)

        arm, matches = self.decide(kind, name, schedule)
        mode = str(arm["mode"]) if arm else ""

        if matches and mode == "conflict404":
            with LOCK:
                FAULT_404_NAME = name
            item["response"] = 409
            item["injected"] = "conflict404"
            self.json_response(
                409,
                {
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": "AlreadyExists",
                    "code": 409,
                    "message": f'backups.logweir.dev "{name}" already exists',
                },
            )
            return

        with LOCK:
            fault_404 = kind == "backup_get" and name == FAULT_404_NAME
            if fault_404:
                FAULT_404_NAME = None
        if fault_404:
            item["response"] = 404
            self.json_response(
                404,
                {
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": "NotFound",
                    "code": 404,
                    "message": f'backups.logweir.dev "{name}" not found',
                },
            )
            return

        if matches and mode == "status":
            code = int(arm["code"]) if arm else 503
            item["response"] = code
            item["injected"] = f"status {code}"
            self.json_response(
                code,
                {
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": {409: "Conflict", 503: "ServiceUnavailable"}.get(
                        code, "InternalError"
                    ),
                    "code": code,
                    "message": f"D1 fence injected {code} for {kind} {name}",
                },
            )
            return

        if matches and mode == "pause":
            item["paused"] = True
            if not RELEASE.wait(timeout=180):
                item["response"] = 504
                self.json_response(504, {"error": "D1 bounded pause expired"})
                return

        headers = {}
        for key, value in self.headers.items():
            if key.lower() not in {"host", "authorization", "connection", "content-length"}:
                headers[key] = value
        with open(TOKEN_PATH, encoding="utf-8") as handle:
            headers["Authorization"] = "Bearer " + handle.read().strip()
        if body:
            headers["Content-Length"] = str(len(body))
        connection = http.client.HTTPSConnection(
            UPSTREAM_HOST,
            UPSTREAM_PORT,
            context=ssl.create_default_context(cafile=CA_PATH),
            timeout=310,
        )
        try:
            connection.request(self.command, rewrite(self.path), body=body or None, headers=headers)
            response = connection.getresponse()
            item["response"] = response.status
            # The stale-finalizer case must observe the API server's 409 while
            # the controller task is still awaiting this exact request.  A
            # short bounded response hold prevents its queued watch event from
            # starting a fresh reconcile before the harness reads the newer
            # status.  The API outcome itself is already final and recorded.
            if kind == "schedule_final" and item.get("paused") and response.status == 409:
                time.sleep(5)
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"transfer-encoding", "connection", "content-length"}:
                    self.send_header(key, value)
            self.end_headers()
            while True:
                # read1 returns currently available decoded bytes.  read(n)
                # waits to fill n on a long-lived watch and would strand small
                # Kubernetes watch events until the next 30-second re-list.
                chunk = response.read1(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            item["clientClosed"] = True
        finally:
            connection.close()

    do_GET = handle_request
    do_POST = handle_request
    do_PUT = handle_request
    do_PATCH = handle_request
    do_DELETE = handle_request


def serve() -> None:
    if not NAMESPACE:
        raise SystemExit("PLAT04_NAMESPACE is required to serve")
    if not UPSTREAM_HOST:
        raise SystemExit("KUBERNETES_SERVICE_HOST is required to serve")
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    serve()
