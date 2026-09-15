#!/usr/bin/env python3
"""Namespace-scoping and bounded fault barrier for the PLAT04 live harness.

The real controller still talks HTTP to a Kubernetes API.  This proxy rewrites
cluster-wide list/watch URLs for namespaced Logweir resources to one namespace,
then forwards the requests to the real docker-desktop API server with the pod's
service-account credential.  Its small control API can pause one identified
reviewed-controller request, or inject exactly one POST-409/GET-404 pair.
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


NAMESPACE = os.environ["PLAT04_NAMESPACE"]
UPSTREAM_HOST = os.environ["KUBERNETES_SERVICE_HOST"]
UPSTREAM_PORT = int(os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443"))
TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

NAMESPACED_LOGWEIR = {
    "approvals",
    "backups",
    "backupschedules",
    "kafkaclusters",
    "restores",
}

LOCK = threading.Lock()
RELEASE = threading.Event()
ARM: dict[str, str] | None = None
FAULT_404_NAME: str | None = None
CAPTURES: list[dict[str, object]] = []


def classify(method: str, path: str, body: bytes) -> tuple[str, str, str]:
    parsed = urllib.parse.urlsplit(path)
    parts = parsed.path.strip("/").split("/")
    name = ""
    schedule_name = ""
    payload: dict[str, object] = {}
    if body:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}
    if "backupschedules" in parts:
        idx = parts.index("backupschedules")
        if len(parts) > idx + 1:
            name = parts[idx + 1]
        if method == "PUT" and parts[-1] == "status":
            pending = ((payload.get("status") or {}).get("pendingBackupRef") or {}).get("name")
            if pending:
                return "reservation", name, name
        if method == "PATCH" and parts[-1] == "status":
            return "schedule_final", name, name
    if "backups" in parts:
        idx = parts.index("backups")
        if len(parts) > idx + 1:
            name = parts[idx + 1]
        if method == "POST":
            meta = payload.get("metadata") or {}
            spec = payload.get("spec") or {}
            name = str(meta.get("name") or "")
            schedule_name = str(((spec.get("scheduleRef") or {}).get("name")) or "")
            return "backup_create", name, schedule_name
        if method == "GET" and name:
            return "backup_get", name, ""
    return "other", name, schedule_name


def rewrite(path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    parts = parsed.path.strip("/").split("/")
    if len(parts) == 4 and parts[:3] == ["apis", "logweir.dev", "v1alpha1"]:
        resource = parts[3]
        if resource in NAMESPACED_LOGWEIR:
            new_path = f"/apis/logweir.dev/v1alpha1/namespaces/{NAMESPACE}/{resource}"
            return urllib.parse.urlunsplit(("", "", new_path, parsed.query, ""))
    if parts == ["apis", "batch", "v1", "jobs"]:
        new_path = f"/apis/batch/v1/namespaces/{NAMESPACE}/jobs"
        return urllib.parse.urlunsplit(("", "", new_path, parsed.query, ""))
    return path


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
    if kind in {"reservation", "schedule_final", "backup_create"} and body:
        item["body"] = json.loads(body)
    with LOCK:
        CAPTURES.append(item)
        if len(CAPTURES) > 200:
            del CAPTURES[:-200]
    return item


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
                value = {"arm": ARM, "fault404Name": FAULT_404_NAME, "captures": list(CAPTURES)}
            self.json_response(200, value)
            return True
        if parsed.path == "/__arm":
            query = urllib.parse.parse_qs(parsed.query)
            kind = query.get("kind", [""])[0]
            name = query.get("name", [""])[0]
            mode = query.get("mode", ["pause"])[0]
            with LOCK:
                ARM = {"kind": kind, "name": name, "mode": mode}
                RELEASE.clear()
            self.json_response(200, {"armed": ARM})
            return True
        if parsed.path == "/__release":
            RELEASE.set()
            self.json_response(200, {"released": True})
            return True
        return False

    def handle_request(self) -> None:
        global ARM, FAULT_404_NAME
        parsed = urllib.parse.urlsplit(self.path)
        if self.control(parsed):
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        kind, name, schedule = classify(self.command, self.path, body)
        item = capture(kind, name, schedule, self.command, self.path, body)

        with LOCK:
            arm = ARM
            matches = bool(
                arm
                and arm["kind"] == kind
                and (not arm["name"] or arm["name"] in {name, schedule})
            )
            if matches:
                ARM = None

        if matches and arm and arm["mode"] == "conflict404":
            with LOCK:
                FAULT_404_NAME = name
            item["response"] = 409
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

        if matches and arm and arm["mode"] == "pause":
            item["paused"] = True
            if not RELEASE.wait(timeout=120):
                item["response"] = 504
                self.json_response(504, {"error": "PLAT04 bounded pause expired"})
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


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
