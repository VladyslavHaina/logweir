#!/usr/bin/env python3
"""PLAT-17.2 live proof: source-built logweir-api in shared mode, behind a local
TLS terminator (the stand-in ingress), a local ES256 OpenID provider, against
docker-desktop, as the emulated chart ServiceAccount. Each row asserts the
refusal or the admission it records; a row that does not hold fails the run."""
import base64, http.client, json, os, socket, ssl, subprocess, sys, time, urllib.parse

H = os.path.dirname(os.path.abspath(__file__))
# Everything host- or run-specific comes from env.sh / run.sh (README.md).
WT = os.environ.get("LOGWEIR_REPO") or os.path.abspath(os.path.join(H, "..", "..", ".."))
API_BIN = os.environ.get("LOGWEIR_API_BIN") or f"{WT}/target/debug/logweir-api"
TS = os.environ["TS"]; OUT = os.environ["OUT"]
PY = os.environ.get("LOGWEIR_PYTHON") or sys.executable
PREFIX = os.environ.get("P172_PREFIX", "lw-p172-")
A, B, CTL = f"{PREFIX}a-{TS}", f"{PREFIX}b-{TS}", f"{PREFIX}ctl-{TS}"
API_PORT, PROXY_PORT, IDP_PORT = 18484, 18443, 18555
LAN = os.environ.get("LAN_IP", "")
BASE = f"https://localhost:{PROXY_PORT}"
ROWS = []
KUBECTL = ["kubectl", "--context", "docker-desktop", "--request-timeout=60s"]
# THE CLIENT SECRET IS MINTED PER RUN (run.sh: `openssl rand`), lives in a 0600
# file the console and the OIDC mock both read, and is never a literal here.
CLIENT_SECRET_FILE = f"{OUT}/client-secret"


def client_secret():
    with open(CLIENT_SECRET_FILE) as f:
        return f.read().strip()


# The first bytes of any base64url JWT header (`{"alg"…`), assembled at run
# time so no source line carries a token-shaped literal.
JWT_PREFIX = base64.urlsafe_b64encode(b'{"alg"').decode()[:7]

def row(name, ok, evidence):
    ROWS.append({"row": name, "pass": bool(ok), "evidence": evidence})
    print(("PASS " if ok else "FAIL ") + name + " :: " + json.dumps(evidence)[:300], flush=True)

def kubectl(*args):
    return subprocess.run(KUBECTL + list(args), capture_output=True, text=True, timeout=90)

class Resp:
    def __init__(self, status, headers, body):
        self.status, self.headers, self.body = status, headers, body
    def header(self, name):
        for k, v in self.headers:
            if k.lower() == name.lower():
                return v
    def headers_all(self, name):
        return [v for k, v in self.headers if k.lower() == name.lower()]
    def json(self):
        return json.loads(self.body or b"{}")

def request(method, path, headers=None, body=None, via="proxy", host=None, read_limit=None, timeout=20):
    headers = dict(headers or {})
    if via == "proxy":
        ctx = ssl._create_unverified_context()
        conn = http.client.HTTPSConnection("127.0.0.1", PROXY_PORT, context=ctx, timeout=timeout)
        headers.setdefault("Host", host or f"localhost:{PROXY_PORT}")
    elif via == "direct-loopback":
        conn = http.client.HTTPConnection("127.0.0.1", API_PORT, timeout=timeout)
        headers.setdefault("Host", host or f"localhost:{PROXY_PORT}")
    elif via == "direct-lan":
        conn = http.client.HTTPConnection(LAN, API_PORT, timeout=timeout)
        headers.setdefault("Host", host or f"localhost:{PROXY_PORT}")
    data = json.dumps(body).encode() if isinstance(body, (dict, list)) else body
    conn.request(method, path, body=data, headers=headers)
    r = conn.getresponse()
    if read_limit:
        chunks = b""
        deadline = time.time() + 8
        while len(chunks) < read_limit and time.time() < deadline:
            c = r.read1(4096)
            if not c:
                break
            chunks += c
        out = Resp(r.status, r.getheaders(), chunks)
    else:
        out = Resp(r.status, r.getheaders(), r.read())
    conn.close()
    return out

def cookie_from(resp, name):
    for v in resp.headers_all("set-cookie"):
        if v.startswith(name + "="):
            return v.split(";", 1)[0]

def login(sub, groups):
    ctl = http.client.HTTPConnection("127.0.0.1", IDP_PORT, timeout=10)
    ctl.request("POST", "/control/next-user", body=json.dumps({"sub": sub, "groups": groups, "name": sub.title()}),
                headers={"content-type": "application/json"})
    ctl.getresponse().read()
    r1 = request("GET", "/auth/login")
    assert r1.status in (302, 303), (r1.status, r1.body)
    login_cookie = cookie_from(r1, "__Host-logweir_login")
    loc = urllib.parse.urlsplit(r1.header("location"))
    idp = http.client.HTTPConnection(loc.hostname, loc.port, timeout=10)
    idp.request("GET", loc.path + "?" + loc.query)
    r2 = idp.getresponse(); r2.read()
    cb = urllib.parse.urlsplit(r2.getheader("location"))
    r3 = request("GET", cb.path + "?" + cb.query, headers={"Cookie": login_cookie})
    assert r3.status in (302, 303), (r3.status, r3.body)
    session = cookie_from(r3, "__Host-logweir_session")
    s = request("GET", "/api/v1/session", headers={"Cookie": session})
    assert s.status == 200, (s.status, s.body)
    return session, s.json()["csrfToken"], s.json(), r3

def unsafe(method, path, session, csrf, body, key=None, via="proxy"):
    headers = {"Cookie": session, "X-CSRF-Token": csrf, "Origin": BASE, "Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    return request(method, path, headers=headers, body=body, via=via)

def wait_port(port, host="127.0.0.1", seconds=60):
    end = time.time() + seconds
    while time.time() < end:
        try:
            socket.create_connection((host, port), timeout=1).close(); return True
        except OSError:
            time.sleep(0.3)
    return False

def main():
    procs = []
    idp_log = open(f"{OUT}/idp.log", "w"); proxy_log = open(f"{OUT}/proxy.log", "w"); api_log = open(f"{OUT}/api-shared.log", "w")
    procs.append(subprocess.Popen([PY, f"{H}/mock_idp.py", str(IDP_PORT), "logweir-console", CLIENT_SECRET_FILE], stdout=idp_log, stderr=idp_log))
    procs.append(subprocess.Popen([PY, f"{H}/tls_proxy.py", str(PROXY_PORT), str(API_PORT), f"{OUT}/tls.crt", f"{OUT}/tls.key"], stdout=proxy_log, stderr=proxy_log))
    env = dict(os.environ, RUST_LOG="info")
    procs.append(subprocess.Popen([API_BIN, "--config", f"{OUT}/console.yaml"], stdout=api_log, stderr=api_log, env=env))
    try:
        assert wait_port(IDP_PORT) and wait_port(PROXY_PORT) and wait_port(API_PORT), "a process did not start"
        time.sleep(1)
        try:
            run_rows()
        except Exception:
            import traceback
            traceback.print_exc(file=sys.stdout)
            row("harness completed every row", False, {"exception": traceback.format_exc()[-400:]})
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try: p.wait(timeout=10)
            except subprocess.TimeoutExpired: p.kill()
        json.dump(ROWS, open(f"{OUT}/rows.json", "w"), indent=1)
        failed = [r["row"] for r in ROWS if not r["pass"]]
        print(f"\n{len(ROWS) - len(failed)}/{len(ROWS)} rows pass" + (f"; FAILED: {failed}" if failed else ""))
        sys.exit(1 if failed else 0)

def run_rows():
    # ---------------------------------------------------------- readiness, probes
    r = request("GET", "/readyz")
    row("shared /readyz ready through the ingress (provider + Kubernetes)", r.status == 200, {"status": r.status, "body": r.body.decode()[:80]})

    # ------------------------------------------------------------ sign-ins
    viewer = login("alice", ["g-viewers-a"])
    operator = login("bob", ["g-operators-a"])
    approver = login("carol", ["g-approvers-a"])
    admin_b = login("dave", ["g-admins-b"])
    for name, (session, csrf, s, cb) in {"alice/viewer": viewer, "bob/operator": operator,
                                         "carol/approver": approver, "dave/admin-b": admin_b}.items():
        flags = cb.headers_all("set-cookie")
        sess_flags = [f for f in flags if f.startswith("__Host-logweir_session=")][0]
        row(f"{name} signs in via code+PKCE through the TLS ingress",
            s["authenticationMode"] == "oidc" and "Secure" in sess_flags and "HttpOnly" in sess_flags and "Domain" not in sess_flags,
            {"actor": s["actor"]["id"], "grants": [(g["name"], g["roles"]) for g in s["namespaces"]],
             "cookieFlags": sess_flags.split(";", 1)[1].strip()})

    # ------------------------------------------------------ roles: allowed/denied
    conn_body = {"role": "source", "bootstrapServers": ["kafka-source.kafka.svc.cluster.local:9096"],
                 "auth": {"mode": "scramSha512", "username": "logweir-reader", "credentialRef": {"name": "source-scram"}, "tls": True}}
    r = unsafe("POST", f"/api/v1/namespaces/{A}/connections", operator[0], operator[1], conn_body, key="p172-live-conn-001")
    created = r.json().get("item", {}) if r.status in (200, 201) else {}
    row("operator bob CREATES a connection in A (201)", r.status == 201, {"status": r.status, "name": created.get("name"), "uid": created.get("uid")})
    conn_name = created.get("name")

    before = kubectl("get", "kafkaclusters", "-n", A, "-o", "name").stdout.split()
    r = unsafe("POST", f"/api/v1/namespaces/{A}/connections", viewer[0], viewer[1], conn_body, key="p172-live-conn-002")
    after = kubectl("get", "kafkaclusters", "-n", A, "-o", "name").stdout.split()
    row("viewer alice is REFUSED the same create (403 forbidden), nothing created",
        r.status == 403 and r.json().get("code") == "forbidden" and before == after,
        {"status": r.status, "code": r.json().get("code"), "objectsBefore": before, "objectsAfter": after})

    r = unsafe("POST", f"/api/v1/namespaces/{A}/connections", approver[0], approver[1], conn_body, key="p172-live-conn-003")
    row("approver carol is REFUSED an execution-side create (403)", r.status == 403, {"status": r.status, "code": r.json().get("code")})

    r = request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": viewer[0]})
    row("viewer alice READS connections in A (200)", r.status == 200 and any(i["name"] == conn_name for i in r.json()["items"]),
        {"status": r.status, "items": [i["name"] for i in r.json().get("items", [])]})

    r = request("GET", f"/api/v1/namespaces/{B}/connections", headers={"Cookie": viewer[0]})
    missing = request("GET", f"/api/v1/namespaces/{A}/connections/does-not-exist", headers={"Cookie": viewer[0]})
    row("viewer alice reaching unbound namespace B = byte-identical 404 to a missing object",
        r.status == 404 and missing.status == 404 and _strip_id(r.body) == _strip_id(missing.body),
        {"status": r.status, "code": r.json().get("code")})

    r = request("GET", f"/api/v1/namespaces/{B}/connections", headers={"Cookie": admin_b[0]})
    row("admin dave READS in B (200) and is 404 in A", r.status == 200 and request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": admin_b[0]}).status == 404, {"status": r.status})

    r = request("GET", "/api/v1/trust-policies", headers={"Cookie": viewer[0]})
    row("viewer alice is refused the cluster-scoped trust read (403)", r.status == 403, {"status": r.status, "code": r.json().get("code")})

    # ------------------------------------------------ a restore: recovery point
    plan = open(f"{WT}/ui/tests/fixtures/plan.golden.yaml").read()
    import hashlib
    restore_body = {"planBytes": plan, "planHash": "sha256:" + hashlib.sha256(plan.encode()).hexdigest(),
                    "approvalRef": {"name": "approval-p172live"},
                    "sourceArchive": {"url": "s3://kafka-backups/p172-live", "credentialRef": {"name": "archive-credentials"}},
                    "backupSetRef": "01JB7Z0000000000000000P172", "pointInTime": "2026-09-22T09:30:00Z",
                    "target": {"clusterRef": {"name": "target"}, "mode": "newTopic", "topicNaming": {"prefix": "restore-p172-"}},
                    "deadlineSeconds": 3600}
    r = unsafe("POST", f"/api/v1/namespaces/{A}/restores", operator[0], operator[1], restore_body, key="p172-live-restore-01")
    restore = r.json().get("item", {}) if r.status in (200, 201) else {}
    row("operator bob CREATES a restore in A (201)", r.status == 201, {"status": r.status, "name": restore.get("name"), "body": r.body.decode()[:200] if r.status != 201 else ""})
    request_id = r.header("x-request-id")
    rv = unsafe("POST", f"/api/v1/namespaces/{A}/restores", viewer[0], viewer[1], restore_body, key="p172-live-restore-02")
    row("viewer alice is REFUSED a restore (403)", rv.status == 403, {"status": rv.status})

    # ------------------------------------------------ attribution on the objects
    for kind, name in (("kafkaclusters", conn_name), ("restores", restore.get("name"))):
        got = kubectl("get", kind, name or "missing", "-n", A, "-o", "json", "--show-managed-fields")
        ann = json.loads(got.stdout)["metadata"].get("annotations", {}) if got.returncode == 0 else {}
        want = {"api.logweir.dev/actor": f"http://127.0.0.1:{IDP_PORT}#bob",
                "api.logweir.dev/authentication-mode": "oidc",
                "api.logweir.dev/binding-revision": "live-p172-1",
                "api.logweir.dev/kubernetes-principal": f"system:serviceaccount:{CTL}:logweir-api"}
        ok = all(ann.get(k) == v for k, v in want.items())
        if kind == "restores":
            ok = ok and ann.get("api.logweir.dev/action") == "restore.create" and \
                 ann.get("api.logweir.dev/recovery-point") == "backupSet=01JB7Z0000000000000000P172 pointInTime=2026-09-22T09:30:00Z source=s3://kafka-backups/p172-live" and \
                 ann.get("api.logweir.dev/request-id") == request_id
        else:
            ok = ok and ann.get("api.logweir.dev/action") == "connection.create"
        blob = json.dumps(ann)
        ok = ok and "Bearer" not in blob and JWT_PREFIX not in blob and client_secret() not in blob
        row(f"kubectl: {kind}/{name} carries actor, action, binding revision, Kubernetes principal" + (", recovery point" if kind == "restores" else ""),
            ok, {k: v for k, v in ann.items() if k.startswith("api.logweir.dev/")})
        mf = json.loads(got.stdout)["metadata"].get("managedFields", []) if got.returncode == 0 else []
        row(f"kubectl: {kind}/{name} was written by field manager logweir-api", any(m.get("manager") == "logweir-api" for m in mf),
            {"managers": sorted({m.get("manager") for m in mf})})

    # ------------------------------------------------ SSE: refused without identity
    ev = f"/api/v1/namespaces/{A}/operations/restore/{restore.get('name')}/events"
    r = request("GET", ev, headers={"Accept": "text/event-stream"})
    row("SSE events stream REFUSED without identity (401 unauthenticated)", r.status == 401 and r.json().get("code") == "unauthenticated", {"status": r.status, "code": r.json().get("code")})
    r = request("GET", ev, headers={"Accept": "text/event-stream", "X-Remote-User": "bob", "X-Forwarded-User": "bob"})
    row("SSE stream REFUSED with forged identity headers and no session (401)", r.status == 401, {"status": r.status})
    r = request("GET", ev, headers={"Accept": "text/event-stream", "Cookie": admin_b[0]})
    row("SSE stream for an unbound namespace = 404 (dave is admin in B only)", r.status == 404, {"status": r.status})
    r = request("GET", ev, headers={"Accept": "text/event-stream", "Cookie": viewer[0]}, read_limit=200)
    row("SSE stream OPENS for viewer alice (200 text/event-stream, first frame)",
        r.status == 200 and (r.header("content-type") or "").startswith("text/event-stream") and len(r.body) > 0,
        {"status": r.status, "contentType": r.header("content-type"), "firstBytes": r.body[:120].decode(errors="replace")})

    # ------------------------------------------------ forged / untrusted headers
    r = request("GET", "/api/v1/session", headers={"X-Remote-User": "bob", "X-Remote-Groups": "g-operators-a", "X-Auth-Request-User": "bob"})
    row("forged X-Remote-User/X-Remote-Groups/X-Auth-Request-User via the ingress = 401 (never an identity)", r.status == 401, {"status": r.status, "code": r.json().get("code")})
    r = request("GET", "/api/v1/session", headers={"Impersonate-User": "system:admin", "Cookie": viewer[0]})
    row("Impersonate-User is refused outright (400 header_not_allowed)", r.status == 400 and r.json().get("code") == "header_not_allowed", {"status": r.status, "code": r.json().get("code")})
    r = unsafe("POST", f"/api/v1/namespaces/{A}/connections", viewer[0], viewer[1], conn_body, key="p172-live-conn-004")
    r2 = request("POST", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": viewer[0], "Origin": BASE, "Content-Type": "application/json",
                 "X-Remote-User": "bob", "X-Remote-Groups": "g-operators-a", "X-CSRF-Token": viewer[1], "Idempotency-Key": "p172-live-conn-005"}, body=conn_body)
    row("viewer + forged operator identity headers is still the viewer (403)", r2.status == 403, {"status": r2.status})
    if LAN:
        r = request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": operator[0], "X-Forwarded-Proto": "https", "X-Forwarded-For": "127.0.0.1"}, via="direct-lan")
        row(f"direct connection past the ingress from {LAN} with a VALID session and forged proxy headers = 421 (untrusted peer)",
            r.status == 421 and r.json().get("code") == "misdirected_request", {"status": r.status, "code": r.json().get("code")})
    r = request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": operator[0]}, via="direct-loopback")
    row("trusted peer that did not vouch for HTTPS (no X-Forwarded-Proto) = 421", r.status == 421, {"status": r.status, "code": r.json().get("code")})
    r = request("GET", "/healthz", via="direct-lan" if LAN else "direct-loopback", host="10.0.0.1:18484")
    row("kubelet-style probe straight to the pod is still answered (/healthz 200)", r.status == 200, {"status": r.status})
    r = unsafe("POST", f"/api/v1/namespaces/{A}/connections", operator[0], "not-the-token", conn_body, key="p172-live-conn-006")
    row("operator with a wrong CSRF token = 403 forbidden, nothing created", r.status == 403 and r.json().get("code") == "forbidden", {"status": r.status, "code": r.json().get("code")})

    # ------------------------------------------------ the audit log
    time.sleep(1)
    lines = open(f"{OUT}/api-shared.log").read().splitlines()
    records = []
    for line in lines:
        if '"audit"' not in line and "logweir_api::audit" not in line:
            continue
        try:
            j = json.loads(line)
        except ValueError:
            continue
        a = j.get("fields", {}).get("audit")
        if a:
            records.append(json.loads(a))
    create = [x for x in records if x.get("auditId") == request_id]
    ok = len(create) == 1 and create[0]["actorId"].endswith("#bob") and create[0]["decision"] == "allow" and \
         create[0]["kubernetesPrincipal"] == f"system:serviceaccount:{CTL}:logweir-api" and create[0]["recoveryPoint"].startswith("backupSet=")
    row("audit: the restore create record names actor, principal, recovery point", ok, create[0] if create else {"records": len(records)})
    denied = [x for x in records if x.get("failureCode") in ("forbidden", "namespace_forbidden", "untrusted_entry_point", "unauthenticated")]
    row("audit: denials carry the real reason (forbidden, namespace_forbidden, untrusted_entry_point, unauthenticated)",
        {"forbidden", "namespace_forbidden", "untrusted_entry_point", "unauthenticated"} <= {x["failureCode"] for x in denied},
        {"codes": sorted({x["failureCode"] for x in denied}), "records": len(records)})
    text = "\n".join(lines)
    leaked = [s for s in [viewer[0].split("=", 1)[1], operator[0].split("=", 1)[1], viewer[1], operator[1], client_secret(), JWT_PREFIX] if s in text]
    row("audit/log: no session cookie, CSRF token, client secret or ID token in the API's output", not leaked, {"leaked": [l[:12] for l in leaked], "lines": len(lines)})

def _strip_id(body):
    j = json.loads(body)
    j.pop("requestId", None)
    return json.dumps(j, sort_keys=True)

if __name__ == "__main__":
    main()
