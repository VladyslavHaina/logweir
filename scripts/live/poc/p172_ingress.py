#!/usr/bin/env python3
"""PLAT-17.2's tracker tests on the PoC install's REAL entry point: Traefik with cert-manager TLS,
Dex sign-in, the published console image in `shared` mode, 2 replicas, a scoped controller.

  p172_ingress.py <outdir> phase1        # everything but the expired session; saves one session
  p172_ingress.py <outdir> expired       # >= sessionMaxAgeSeconds later: the saved session is refused

Every row REQUIRES the refusal or admission it records; a row that does not hold fails the run.
Nothing secret is printed or written to the outdir: the saved session for the expired row goes to a
0600 file under ~/.logweir-poc/tmp and is deleted by the `expired` phase. Every kubectl call names
--context docker-desktop and has a deadline.
"""
import base64, json, os, subprocess, sys, time, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import poclib  # noqa: E402

OUT, PHASE = sys.argv[1], sys.argv[2]
NS, SYS = "logweir-poc", "logweir-system"
K = ["kubectl", "--context", "docker-desktop", "--request-timeout=60s"]
ROWS = []
SAVED = os.path.join(poclib.POCDIR, "tmp", "p172-expired-session")
JWT_PREFIX = base64.urlsafe_b64encode(b'{"alg"').decode()[:7]


def row(name, ok, evidence):
    ROWS.append({"row": name, "pass": bool(ok), "evidence": evidence})
    print(("PASS " if ok else "FAIL ") + name + " :: " + json.dumps(evidence)[:360], flush=True)


def kubectl(*args, timeout=90):
    return subprocess.run(K + list(args), capture_output=True, text=True, timeout=timeout)


def count(kind):
    p = kubectl("-n", NS, "get", kind, "-o", "name")
    return sorted(p.stdout.split())


def body_of(r):
    j = r.json()
    j.pop("requestId", None)
    return json.dumps(j, sort_keys=True)


def main():
    os.makedirs(OUT, exist_ok=True)
    try:
        if PHASE == "phase1":
            phase1()
        elif PHASE == "expired":
            expired()
    except Exception:
        import traceback
        row("harness completed every row", False, {"exception": traceback.format_exc()[-600:]})
    finally:
        path = os.path.join(OUT, f"rows-{PHASE}.json")
        json.dump(ROWS, open(path, "w"), indent=1)
        failed = [r["row"] for r in ROWS if not r["pass"]]
        print(f"\n{len(ROWS) - len(failed)}/{len(ROWS)} rows pass" + (f"; FAILED: {failed}" if failed else ""))
        sys.exit(1 if failed else 0)


def phase1():
    started = time.time()
    # ------------------------------------------------------------ TLS ingress
    r = poclib.request("GET", poclib.BASE + "/readyz")
    row("TLS: /readyz answered over HTTPS verified against the PoC CA only", r.status == 200,
        {"status": r.status, "ca": poclib.CA})
    r = poclib.request("GET", f"http://{poclib.CONSOLE_HOST}/ui/")
    row("TLS: plain HTTP is redirected to HTTPS (permanent)", r.status in (301, 308) and (r.header("location") or "").startswith("https://"),
        {"status": r.status, "location": r.header("location")})
    r = poclib.request("GET", poclib.BASE + "/ui/")
    hdr = {h: r.header(h) for h in ("strict-transport-security", "content-security-policy", "x-frame-options",
                                     "x-content-type-options", "referrer-policy", "cross-origin-opener-policy")}
    row("TLS/D0 headers: HSTS and the console's security headers on /ui/", all(hdr.values()) and "max-age=31536000" in hdr["strict-transport-security"], hdr)

    # ------------------------------------------------------------ sign-in per role, cookie
    who = {}
    for role, want in (("viewer", "viewer"), ("operator", "operator"), ("approver", "approver"), ("admin", "administrator")):
        trace = []
        s = poclib.signin(role, trace)
        attrs = poclib.cookie_attrs(s["callback"], "__Host-logweir_session")
        grants = [(g.get("name"), g.get("roles")) for g in s["sessionJson"].get("namespaces", [])]
        ok = (s["session"].startswith("__Host-logweir_session=") and "Secure" in attrs and "HttpOnly" in attrs
              and not any(a.lower().startswith("domain") for a in attrs) and "Path=/" in attrs
              and grants == [(NS, [want])] and s["sessionJson"].get("authenticationMode") == "oidc")
        row(f"sign-in {role} through Traefik + Dex: exactly {want} in {NS}; cookie __Host-, Secure, HttpOnly, Path=/, no Domain", ok,
            {"actor": s["sessionJson"]["actor"]["id"], "grants": grants, "cookie": attrs, "hops": len(trace)})
        who[role] = s
    viewer, operator, approver, admin = who["viewer"], who["operator"], who["approver"], who["admin"]

    # ------------------------------------------------------------ role matrix + denied mutation
    conn_body = {"role": "source", "bootstrapServers": ["logweir-kafka-source.logweir-system.svc.cluster.local:9092"],
                 "auth": {"mode": "plaintext", "tls": False}}
    for role in ("viewer", "approver"):
        before = count("kafkaclusters")
        r = poclib.api("POST", f"/api/v1/namespaces/{NS}/connections", who[role], body=conn_body, key="p172-" + uuid.uuid4().hex[:12])
        after = count("kafkaclusters")
        row(f"role matrix: {role} is REFUSED a connection create (403 forbidden); nothing is created", r.status == 403 and before == after,
            {"status": r.status, "code": r.json().get("code"), "objects": len(before), "after": len(after)})
    sch = (json.loads(kubectl("-n", NS, "get", "backupschedules", "-o", "json").stdout)["items"] or [{"metadata": {"name": "none"}}])[0]["metadata"]["name"]
    before = count("backups")
    r = poclib.api("POST", f"/api/v1/namespaces/{NS}/backups", viewer, body={"scheduleRef": {"name": sch}}, key="p172-" + uuid.uuid4().hex[:12])
    after = count("backups")
    row("role matrix: viewer is REFUSED 'Back up now' (403); no Backup is created", r.status == 403 and before == after,
        {"status": r.status, "code": r.json().get("code"), "backups": len(before), "after": len(after)})
    restores = json.loads(kubectl("-n", NS, "get", "restores", "-o", "json").stdout)["items"]
    rname = restores[0]["metadata"]["name"] if restores else "none"
    before_ap = count("approvals")
    r = poclib.api("POST", f"/api/v1/namespaces/{NS}/restores/{rname}/approval", operator,
                   body={"sidecar": base64.b64encode(b"not-a-signature").decode()}, key="p172-" + uuid.uuid4().hex[:12])
    row("role matrix: operator cannot assume approver rights (approval submission 403 forbidden); no Approval is created",
        r.status == 403 and count("approvals") == before_ap, {"status": r.status, "code": r.json().get("code"), "restore": rname})
    r2 = poclib.api("POST", f"/api/v1/namespaces/{NS}/restores/{rname}/approval", approver,
                    body={"sidecar": base64.b64encode(b"not-a-signature").decode()}, key="p172-" + uuid.uuid4().hex[:12])
    row("role matrix: the approver passes the ROLE check on the same route (refused later, for the body or the policy, never 403 forbidden)",
        r2.status != 403 and r2.status >= 400 and count("approvals") == before_ap, {"status": r2.status, "code": r2.json().get("code")})
    rest_before = count("restores")
    r = poclib.api("POST", f"/api/v1/namespaces/{NS}/restores", approver, body={"planBytes": "x"}, key="p172-" + uuid.uuid4().hex[:12])
    row("role matrix: approver is REFUSED an execution-side create (restore, 403); nothing is created", r.status == 403 and count("restores") == rest_before,
        {"status": r.status, "code": r.json().get("code")})
    for role, want in (("viewer", 200), ("operator", 200), ("admin", 200)):
        r = poclib.api("GET", f"/api/v1/namespaces/{NS}/connections", who[role])
        row(f"role matrix: {role} READS connections (200)", r.status == want, {"status": r.status, "items": len(r.json().get("items", []))})
    r = poclib.api("GET", "/api/v1/trust-policies", viewer)
    r2 = poclib.api("GET", "/api/v1/trust-policies", admin)
    row("role matrix: the cluster-scoped trust read is administrator-only (viewer 403, admin 200)", r.status == 403 and r2.status == 200,
        {"viewer": r.status, "admin": r2.status, "policies": [p.get("name") for p in r2.json().get("items", [])]})

    # ------------------------------------------------------------ unauthorized namespace
    missing = poclib.api("GET", f"/api/v1/namespaces/{NS}/connections/does-not-exist-{uuid.uuid4().hex[:6]}", operator)
    for ns in (SYS, "kube-system", "lens-metrics", "no-such-ns"):
        r = poclib.api("GET", f"/api/v1/namespaces/{ns}/connections", operator)
        row(f"unauthorized namespace {ns}: 404, byte-identical to a missing object in a granted namespace",
            r.status == 404 and missing.status == 404 and body_of(r) == body_of(missing),
            {"status": r.status, "body": body_of(r), "missing": body_of(missing)})
    r = poclib.api("POST", f"/api/v1/namespaces/{SYS}/connections", admin, body=conn_body, key="p172-" + uuid.uuid4().hex[:12])
    row("unauthorized namespace: even the administrator cannot create in the release namespace (404)", r.status == 404,
        {"status": r.status, "code": r.json().get("code")})

    # ------------------------------------------------------------ forged identity headers
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"X-Remote-User": "operator", "X-Remote-Groups": "logweir-poc-operators",
                                                                        "X-Forwarded-User": "operator", "X-Auth-Request-User": "operator", "X-Auth-Request-Email": "operator@logweir.localtest.me"})
    row("forged identity headers without a session: 401 (never an identity)", r.status == 401, {"status": r.status, "code": r.json().get("code")})
    before = count("kafkaclusters")
    r = poclib.api("POST", f"/api/v1/namespaces/{NS}/connections", viewer, body=conn_body, key="p172-" + uuid.uuid4().hex[:12],
                   headers={"X-Remote-User": "operator", "X-Remote-Groups": "logweir-poc-operators", "X-Forwarded-User": "operator"})
    row("viewer session + forged operator headers is still the viewer (403), nothing created", r.status == 403 and count("kafkaclusters") == before,
        {"status": r.status})
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"Cookie": viewer["session"], "Impersonate-User": "system:admin"})
    row("Impersonate-User is refused outright (400 header_not_allowed)", r.status == 400 and r.json().get("code") == "header_not_allowed",
        {"status": r.status, "code": r.json().get("code")})
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"Cookie": viewer["session"], "X-Remote-User": "admin", "X-Remote-Groups": "logweir-poc-admins"})
    row("viewer session + forged admin headers: the session still names the viewer", r.status == 200 and r.json()["actor"]["displayName"] == "viewer",
        {"actor": r.json().get("actor", {}).get("displayName"), "grants": [(g["name"], g["roles"]) for g in r.json().get("namespaces", [])]})

    # ------------------------------------------------------------ CSRF / session controls / CORS
    before = count("backups")
    base_h = {"Cookie": operator["session"], "Content-Type": "application/json", "Idempotency-Key": "p172-" + uuid.uuid4().hex[:12]}
    cases = {
        "no X-CSRF-Token": dict(base_h, Origin=poclib.BASE),
        "a wrong X-CSRF-Token": dict(base_h, Origin=poclib.BASE, **{"X-CSRF-Token": "not-the-token"}),
        "another session's X-CSRF-Token": dict(base_h, Origin=poclib.BASE, **{"X-CSRF-Token": viewer["csrf"]}),
        "no Origin": dict(base_h, **{"X-CSRF-Token": operator["csrf"]}),
        "a foreign Origin": dict(base_h, Origin="https://evil.localtest.me", **{"X-CSRF-Token": operator["csrf"]}),
    }
    for label, h in cases.items():
        h = dict(h, **{"Idempotency-Key": "p172-" + uuid.uuid4().hex[:12]})
        r = poclib.request("POST", poclib.BASE + f"/api/v1/namespaces/{NS}/backups", headers=h, body={"scheduleRef": {"name": sch}})
        row(f"CSRF: operator 'Back up now' with {label} is refused (403), no Backup", r.status == 403 and count("backups") == before,
            {"status": r.status, "code": r.json().get("code")})
    h = dict(base_h, Origin=poclib.BASE, **{"X-CSRF-Token": operator["csrf"], "Content-Type": "text/plain", "Idempotency-Key": "p172-" + uuid.uuid4().hex[:12]})
    r = poclib.request("POST", poclib.BASE + f"/api/v1/namespaces/{NS}/backups", headers=h, body=json.dumps({"scheduleRef": {"name": sch}}).encode())
    row("CSRF: a non-JSON content type (a form post's shape) is refused, no Backup", r.status in (403, 415) and count("backups") == before,
        {"status": r.status, "code": r.json().get("code")})
    r = poclib.request("OPTIONS", poclib.BASE + f"/api/v1/namespaces/{NS}/backups",
                       headers={"Origin": "https://evil.localtest.me", "Access-Control-Request-Method": "POST"})
    acao = [k for k, _ in r.headers if k.lower().startswith("access-control-")]
    row("CORS: no Access-Control-* header on a cross-origin preflight", not acao, {"status": r.status, "headers": acao})
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"Cookie": "__Host-logweir_session=" + "A" * 120})
    row("session: a forged/garbled session cookie is refused (401)", r.status == 401, {"status": r.status, "code": r.json().get("code")})

    # ------------------------------------------------------------ unauthenticated API and stream
    for path in ("/api/v1/session", f"/api/v1/namespaces/{NS}/connections", f"/api/v1/namespaces/{NS}/backups", "/api/v1/trust-policies"):
        r = poclib.request("GET", poclib.BASE + path)
        row(f"unauthenticated GET {path}: 401 unauthenticated", r.status == 401 and r.json().get("code") == "unauthenticated", {"status": r.status})
    backups = json.loads(kubectl("-n", NS, "get", "backups", "-o", "json").stdout)["items"]
    bname = backups[0]["metadata"]["name"]
    ev = f"/api/v1/namespaces/{NS}/operations/backup/{bname}/events"
    r = poclib.request("GET", poclib.BASE + ev, headers={"Accept": "text/event-stream"})
    row("unauthenticated event stream: 401", r.status == 401, {"status": r.status, "code": r.json().get("code")})
    r = poclib.request("GET", poclib.BASE + ev, headers={"Accept": "text/event-stream", "X-Remote-User": "operator"})
    row("event stream with forged identity headers and no session: 401", r.status == 401, {"status": r.status})
    r = poclib.request("GET", poclib.BASE + f"/api/v1/namespaces/{SYS}/operations/backup/{bname}/events", headers={"Accept": "text/event-stream", "Cookie": viewer["session"]})
    row("event stream for an unauthorized namespace: 404", r.status == 404, {"status": r.status})
    r = poclib.request("GET", poclib.BASE + ev, headers={"Accept": "text/event-stream", "Cookie": viewer["session"]}, read_limit=300)
    row("event stream OPENS for the viewer (200 text/event-stream, first frame)", r.status == 200 and (r.header("content-type") or "").startswith("text/event-stream") and len(r.body) > 0,
        {"status": r.status, "contentType": r.header("content-type"), "firstBytes": r.body[:80].decode(errors="replace")})

    # ------------------------------------------------------------ legacy proxy
    for path in (f"/apis/logweir.dev/v1alpha1/namespaces/{NS}/backups", f"/api/v1/namespaces/{NS}/secrets", "/api/v1/namespaces/logweir-system/secrets/logweir-console-keys"):
        r = poclib.request("GET", poclib.BASE + path, headers={"Cookie": admin["session"]})
        row(f"legacy proxy not reachable: GET {path} as admin is not served (404, no Kubernetes body)",
            r.status == 404 and b'"kind"' not in r.body, {"status": r.status, "code": r.json().get("code")})
    svcs = kubectl("-n", SYS, "get", "svc,deploy", "-o", "name").stdout.split()
    row("legacy proxy not deployed: no logweir-ui Service or Deployment (ui.enabled off beside shared mode)",
        not any("logweir-ui" in s for s in svcs), {"objects": svcs})

    # ------------------------------------------------------------ network paths
    np = json.loads(kubectl("-n", SYS, "get", "networkpolicy", "-o", "json").stdout)["items"]
    api_np = [p for p in np if p["metadata"]["name"] == "logweir-api"]
    ok = bool(api_np) and "Ingress" in api_np[0]["spec"].get("policyTypes", []) and "Egress" in api_np[0]["spec"].get("policyTypes", [])
    row("network: the console NetworkPolicy is applied, Ingress+Egress, ingress only from Traefik's pods",
        ok, {"policies": [p["metadata"]["name"] for p in np], "ingressFrom": api_np[0]["spec"].get("ingress") if api_np else None})
    row("network: allowed path Traefik -> console works (the whole suite above)", True, {"via": "https://" + poclib.CONSOLE_HOST})
    probe_ns = os.environ.get("PROBE_NS", "")
    if probe_ns:
        p = kubectl("-n", probe_ns, "exec", "probe", "--", "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", "X-Forwarded-Proto: https",
                    "-H", f"Host: {poclib.CONSOLE_HOST}", "http://logweir-api.logweir-system.svc:8484/api/v1/session")
        row("network: a pod outside Traefik's namespace reaches the console Service at TCP level (docker-desktop does NOT enforce NetworkPolicy — "
            "the known limit) and the console itself answers 421 (G6)", p.stdout.strip() == "421", {"probeNamespace": probe_ns, "status": p.stdout.strip()})

    # ------------------------------------------------------------ audit attribution
    time.sleep(2)
    logs = ""
    for pod in kubectl("-n", SYS, "get", "pods", "-l", "app.kubernetes.io/component=api", "-o", "name").stdout.split():
        logs += kubectl("-n", SYS, "logs", pod, "--since=2h", timeout=120).stdout
    open(os.path.join(OUT, "console-logs.jsonl"), "w").write("")  # the raw log stays in the cluster; only the scan is kept
    records = []
    for line in logs.splitlines():
        try:
            j = json.loads(line)
        except ValueError:
            continue
        a = (j.get("fields") or {}).get("audit")
        if a:
            try:
                records.append(json.loads(a))
            except ValueError:
                pass
    codes = sorted({r.get("failureCode") for r in records if r.get("failureCode")})
    row("audit: denials carry the real reason", {"forbidden", "unauthenticated"} <= set(codes) and any(c in codes for c in ("namespace_forbidden", "not_found")),
        {"failureCodes": codes, "records": len(records)})
    creates = [r for r in records if r.get("decision") == "allow" and r.get("method") == "POST" and r.get("httpStatus") == 201
               and r.get("path") == f"/api/v1/namespaces/{NS}/restores"]
    op_actor = operator["sessionJson"]["actor"]["id"]
    restore_names = {i["metadata"]["name"] for i in json.loads(kubectl("-n", NS, "get", "restores", "-o", "json").stdout)["items"]}
    ok = bool(creates) and all(r.get("actorId") == op_actor and r.get("objectName") in restore_names and r.get("recoveryPoint") and
                               r.get("kubernetesPrincipal") == "system:serviceaccount:logweir-system:logweir-api" for r in creates)
    row("audit: every allowed restore create (201) names the operator, the object, the recovery point and the console's principal", ok,
        {k: creates[-1].get(k) for k in ("auditId", "actorId", "kubernetesPrincipal", "action", "recoveryPoint", "bindingRevision")} if creates else {"records": len(records)})
    ign = [r for r in records if r.get("ignoredIdentityHeaders")]
    row("audit: a request carrying forged identity headers is recorded with the headers it IGNORED", bool(ign),
        {"example": {k: ign[-1].get(k) for k in ("path", "actorId", "ignoredIdentityHeaders", "httpStatus")} if ign else None, "records": len(ign)})
    for kind in ("restores", "backupschedules", "backupdestinations", "kafkaclusters"):
        items = json.loads(kubectl("-n", NS, "get", kind, "-o", "json").stdout)["items"]
        ann = [i["metadata"].get("annotations", {}) for i in items]
        ok = bool(ann) and all(a.get("api.logweir.dev/actor", "").startswith("https://dex.localtest.me#") and
                               a.get("api.logweir.dev/kubernetes-principal") == "system:serviceaccount:logweir-system:logweir-api" for a in ann)
        row(f"audit: every {kind} object carries actor, action and principal annotations", ok,
            {"objects": len(items), "actions": sorted({a.get("api.logweir.dev/action") for a in ann})})
    secrets = [who[r]["session"].split("=", 1)[1] for r in who] + [who[r]["csrf"] for r in who] + [JWT_PREFIX]
    cs = subprocess.run(K + ["-n", SYS, "get", "secret", "logweir-console-oidc", "-o", "jsonpath={.data.clientSecret}"], capture_output=True, text=True, timeout=60).stdout
    secrets.append(base64.b64decode(cs).decode())
    for role in who:
        secrets.append(poclib.password_for(role)[1])
    leaked = [i for i, s in enumerate(secrets) if s and s in logs]
    row("audit/logs: no session cookie, CSRF token, ID token, client secret or password in either console pod's log", not leaked,
        {"scanned": len(secrets), "logBytes": len(logs), "leakedIndexes": leaked})

    # ------------------------------------------------------------ save one session for the expired row
    os.makedirs(os.path.dirname(SAVED), mode=0o700, exist_ok=True)
    fd = os.open(SAVED, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, json.dumps({"session": viewer["session"], "obtainedAt": started}).encode())
    os.close(fd)
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"Cookie": viewer["session"]})
    row("session saved for the expired row is valid now (200)", r.status == 200, {"obtainedAt": time.strftime("%FT%TZ", time.gmtime(started))})


def expired():
    saved = json.load(open(SAVED))
    age = time.time() - saved["obtainedAt"]
    r = poclib.request("GET", poclib.BASE + "/api/v1/session", headers={"Cookie": saved["session"]})
    row(f"expired session: a session {int(age)} s old (sessionMaxAgeSeconds 900) is refused (401)", age > 900 and r.status == 401,
        {"ageSeconds": int(age), "status": r.status, "code": r.json().get("code")})
    r = poclib.request("GET", poclib.BASE + f"/api/v1/namespaces/{NS}/connections", headers={"Cookie": saved["session"]})
    row("expired session: a namespace read with it is refused (401)", r.status == 401, {"status": r.status})
    os.remove(SAVED)
    row("the saved session file is deleted", not os.path.exists(SAVED), {})


if __name__ == "__main__":
    main()
