#!/usr/bin/env python3
"""PLAT-17.2 "expired session" — the row the plat17-2 live harness lacked
(lab-refresh-9 §5), run by `run.sh expired` (README.md). It reuses the
harness's own helpers (`live_p172.request/login/unsafe`), its local ES256
OpenID provider (`mock_idp.py`) and its TLS stand-in ingress (`tls_proxy.py`).

A source-built `logweir-api` in `mode: shared` with `sessionMaxAgeSeconds: 60`
(the minimum the config accepts) signs alice in as an operator of namespace A
through the real code+PKCE redirects. Then, with the SAME cookie value replayed
by this client (a browser would drop the cookie at Max-Age; the server must not
depend on that):

  control, inside the session's life:  GET /api/v1/session 200; the connections
    list 200; the operation-events stream OPENS (200 text/event-stream, first
    frame); a create with the CSRF token 201;
  past the signed `exp`:  the same GET, the same list and the same stream are
    each refused 401 `session_expired` (never 200, never `unauthenticated`), and
    the same create with the same CSRF token is refused 401 with nothing
    created.

Every row asserts the answer it records; the run fails on any row that does
not hold. Everything runs against docker-desktop in `<P172_PREFIX>exp-<TS>`
(owner label `$P172_OWNER`), deleted at the end after its label and UID are
read back; the run's key, secret and TLS-key files are removed with it.
"""
import json, os, subprocess, sys, time

H = os.path.dirname(os.path.abspath(__file__))
TS = os.environ["TS"]
OUT = os.environ["OUT"]
OWNER = os.environ.get("P172_OWNER", "plat17-2")
A = os.environ.get("P172_PREFIX", "lw-p172-") + f"exp-{TS}"
sys.path.insert(0, H)
import live_p172 as h  # noqa: E402  (reads TS/OUT at import)

MAIN = h.WT
h.API_PORT, h.PROXY_PORT, h.IDP_PORT = 18494, 18453, 18565
h.BASE = f"https://localhost:{h.PROXY_PORT}"
PY = h.PY
K = ["kubectl", "--context", "docker-desktop", "--request-timeout=60s"]
MAX_AGE = 60


def k(*args, stdin=None):
    return subprocess.run(K + list(args), capture_output=True, text=True, input=stdin, timeout=240)


def setup():
    assert k("create", "namespace", A).returncode == 0
    assert k("label", "namespace", A, f"logweir.dev/test-owner={OWNER}").returncode == 0
    uid = json.loads(k("get", "namespace", A, "-o", "json").stdout)["metadata"]["uid"]
    plan = open(f"{MAIN}/ui/tests/fixtures/plan.golden.yaml").read()
    restore = {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
               "metadata": {"name": "p172-expiry-stream", "namespace": A, "labels": {"logweir.dev/test-owner": OWNER}},
               "spec": {"planBytes": plan, "approvalRef": {"name": "approval-p172-expiry"},
                        "sourceArchive": {"url": "s3://kafka-backups/p172-expiry"},
                        "backupSetRef": "01JB7Z0000000000000000P172", "pointInTime": "2026-09-22T09:30:00Z",
                        "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                                   "topicNaming": {"prefix": "restore-p172x-"}}, "deadlineSeconds": 3600}}
    made = k("create", "-f", "-", "-o", "json", stdin=json.dumps(restore))
    assert made.returncode == 0, made.stderr
    import base64
    for name in ("session.key", "cursor.key"):
        with open(f"{OUT}/{name}", "w") as f:
            f.write(f'version: 1\nkey: "{base64.b64encode(os.urandom(32)).decode()}"\n')
        os.chmod(f"{OUT}/{name}", 0o600)
    # minted per run; the console and the OIDC mock read it by PATH
    with open(h.CLIENT_SECRET_FILE, "w") as f:
        f.write(os.urandom(24).hex())
    os.chmod(h.CLIENT_SECRET_FILE, 0o600)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                    "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                    "-keyout", f"{OUT}/tls.key", "-out", f"{OUT}/tls.crt"], capture_output=True, check=True, timeout=30)
    with open(f"{OUT}/console.yaml", "w") as f:
        f.write(f"""mode: shared
listen: "127.0.0.1:{h.API_PORT}"
publicBaseUrl: "https://localhost:{h.PROXY_PORT}"
uiDirectory: {MAIN}/ui
oidc:
  issuer: http://127.0.0.1:{h.IDP_PORT}
  clientId: logweir-console
  clientSecretFile: {OUT}/client-secret
  allowedAlgorithms: [ES256]
  scopes: [openid, profile, groups]
  groupsClaim: groups
  displayNameClaim: name
  insecureLoopbackIssuer: true
roles:
  revision: p172-expiry-1
  bindings:
    - {{role: operator, namespace: {A}, groups: [g-operators-a]}}
sessionKey: {{file: {OUT}/session.key, expectedVersion: 1}}
cursorKey: {{file: {OUT}/cursor.key, expectedVersion: 1}}
sessionMaxAgeSeconds: {MAX_AGE}
trustedProxyCidrs: ["127.0.0.1/32"]
requireTrustedProxy: true
namespaces: [{A}]
kubernetes:
  source: kubeconfig
  context: docker-desktop
""")
    return uid


def cleanup(uid):
    seen = k("get", "namespace", A, "-o", "json")
    if seen.returncode == 0:
        meta = json.loads(seen.stdout)["metadata"]
        if (meta.get("labels") or {}).get("logweir.dev/test-owner") == OWNER and meta["uid"] == uid:
            k("delete", "namespace", A, "--wait=true", "--timeout=180s")
    gone = k("get", "namespace", A).returncode != 0
    for name in ("session.key", "cursor.key", "client-secret", "tls.key"):
        try:
            os.remove(f"{OUT}/{name}")
        except FileNotFoundError:
            pass
    return {"namespace": A, "uid": uid, "deleted": gone, "credentialFilesRemoved": True}


def code(r):
    try:
        return r.json().get("code")
    except ValueError:
        return None


def rows():
    session, csrf, who, _ = h.login("alice", ["g-operators-a"])
    signed_in = time.time()
    ev = f"/api/v1/namespaces/{A}/operations/restore/p172-expiry-stream/events"
    conn_body = {"role": "source", "bootstrapServers": ["kafka-source.kafka.svc.cluster.local:9096"],
                 "auth": {"mode": "scramSha512", "username": "u", "credentialRef": {"name": "s"}, "tls": True}}
    # --- the control: inside the session's life -------------------------
    s1 = h.request("GET", "/api/v1/session", headers={"Cookie": session})
    l1 = h.request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": session})
    e1 = h.request("GET", ev, headers={"Accept": "text/event-stream", "Cookie": session}, read_limit=200)
    c1 = h.unsafe("POST", f"/api/v1/namespaces/{A}/connections", session, csrf, conn_body, key="p172-expiry-0001")
    h.row("control: inside its life the session reads (200), lists (200), opens the stream (200 text/event-stream) "
          "and creates (201)",
          s1.status == 200 and l1.status == 200 and e1.status == 200 and
          (e1.header("content-type") or "").startswith("text/event-stream") and len(e1.body) > 0 and c1.status == 201,
          {"session": s1.status, "list": l1.status, "stream": e1.status, "streamType": e1.header("content-type"),
           "create": c1.status, "actor": who.get("actor", {}).get("id"), "sessionMaxAgeSeconds": MAX_AGE})
    before = sorted(k("get", "kafkaclusters", "-n", A, "-o", "name").stdout.split())
    # --- past the signed exp ----------------------------------------------
    wait = signed_in + MAX_AGE + 3 - time.time()
    if wait > 0:
        time.sleep(wait)
    elapsed = round(time.time() - signed_in, 1)
    s2 = h.request("GET", "/api/v1/session", headers={"Cookie": session})
    l2 = h.request("GET", f"/api/v1/namespaces/{A}/connections", headers={"Cookie": session})
    e2 = h.request("GET", ev, headers={"Accept": "text/event-stream", "Cookie": session}, read_limit=400)
    c2 = h.unsafe("POST", f"/api/v1/namespaces/{A}/connections", session, csrf, conn_body, key="p172-expiry-0002")
    after = sorted(k("get", "kafkaclusters", "-n", A, "-o", "name").stdout.split())
    h.row("expired session: the same cookie past its signed exp is refused on the API — GET session and the list "
          "answer 401 session_expired",
          s2.status == 401 and code(s2) == "session_expired" and l2.status == 401 and code(l2) == "session_expired",
          {"elapsedSinceSignIn": elapsed, "session": [s2.status, code(s2)], "list": [l2.status, code(l2)]})
    h.row("expired session: the operation-events stream refuses the expired cookie (401 session_expired, no "
          "text/event-stream)",
          e2.status == 401 and code(e2) == "session_expired" and
          not (e2.header("content-type") or "").startswith("text/event-stream"),
          {"status": e2.status, "code": code(e2), "contentType": e2.header("content-type")})
    h.row("expired session: a create with the same CSRF token is refused 401 session_expired and nothing is created",
          c2.status == 401 and code(c2) == "session_expired" and before == after,
          {"status": c2.status, "code": code(c2), "objectsBefore": before, "objectsAfter": after})


def main():
    os.makedirs(OUT, exist_ok=True)
    try:
        uid = setup()
    except Exception:
        seen = k("get", "namespace", A, "-o", "json")
        cleanup(json.loads(seen.stdout)["metadata"]["uid"] if seen.returncode == 0 else "")
        raise
    procs = []
    logs = [open(f"{OUT}/{n}.log", "w") for n in ("idp", "proxy", "api-shared")]
    procs.append(subprocess.Popen([PY, f"{H}/mock_idp.py", str(h.IDP_PORT), "logweir-console", h.CLIENT_SECRET_FILE],
                                  stdout=logs[0], stderr=logs[0]))
    procs.append(subprocess.Popen([PY, f"{H}/tls_proxy.py", str(h.PROXY_PORT), str(h.API_PORT), f"{OUT}/tls.crt",
                                   f"{OUT}/tls.key"], stdout=logs[1], stderr=logs[1]))
    procs.append(subprocess.Popen([h.API_BIN, "--config", f"{OUT}/console.yaml"],
                                  stdout=logs[2], stderr=logs[2], env=dict(os.environ, RUST_LOG="info")))
    proof = {}
    try:
        assert h.wait_port(h.IDP_PORT) and h.wait_port(h.PROXY_PORT) and h.wait_port(h.API_PORT), "a process did not start"
        time.sleep(1)
        try:
            rows()
        except Exception:
            import traceback
            h.row("harness completed every row", False, {"exception": traceback.format_exc()[-600:]})
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        proof = cleanup(uid)
        json.dump({"rows": h.ROWS, "cleanup": proof, "namespace": A,
                   "apiBinarySha256": subprocess.run(["shasum", "-a", "256", h.API_BIN],
                                                     capture_output=True, text=True).stdout.split()[0]},
                  open(f"{OUT}/rows.json", "w"), indent=1)
    failed = [r["row"] for r in h.ROWS if not r["pass"]]
    print(f"\n{len(h.ROWS) - len(failed)}/{len(h.ROWS)} rows pass" + (f"; FAILED: {failed}" if failed else ""))
    sys.exit(1 if failed or not proof.get("deleted") else 0)


if __name__ == "__main__":
    main()
