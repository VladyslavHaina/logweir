#!/usr/bin/env python3
"""Shared helpers for the PoC install's live rows (deploy/poc/README.md): a real sign-in
through Traefik and Dex over the local CA, and HTTPS requests to the console.

Stdlib only. Nothing secret is printed: passwords come from the credentials file
(`$LOGWEIR_POC_CREDENTIALS`, default `$HOME/.logweir-poc/credentials.txt`, lines
`<user>@<domain>\\t<password>`), and a session is returned to the caller, never logged.

  python3 poclib.py signin <role>     # prints the session's actor, mode and grants (no cookie)
"""
import http.client
import json
import os
import re
import ssl
import sys
import time
import urllib.parse

CONSOLE_HOST = os.environ.get("CONSOLE_HOST", "logweir.localtest.me")
DEX_HOST = os.environ.get("DEX_HOST", "dex.localtest.me")
POCDIR = os.environ.get("LOGWEIR_POC_DIR", os.path.expanduser("~/.logweir-poc"))
CA = os.environ.get("LOGWEIR_POC_CA", os.path.join(POCDIR, "public", "ca.crt"))
CREDS = os.environ.get("LOGWEIR_POC_CREDENTIALS", os.path.join(POCDIR, "credentials.txt"))
BASE = f"https://{CONSOLE_HOST}"


def tls_context():
    ctx = ssl.create_default_context(cafile=CA)
    return ctx


class Resp:
    def __init__(self, status, headers, body, url):
        self.status, self.headers, self.body, self.url = status, headers, body, url

    def header(self, name):
        for k, v in self.headers:
            if k.lower() == name.lower():
                return v
        return None

    def headers_all(self, name):
        return [v for k, v in self.headers if k.lower() == name.lower()]

    def json(self):
        try:
            return json.loads(self.body or b"{}")
        except ValueError:
            return {}


def request(method, url, headers=None, body=None, timeout=20, read_limit=None, context=None):
    """One HTTPS (or HTTP) request, no redirect following."""
    u = urllib.parse.urlsplit(url)
    if u.scheme == "https":
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, context=context or tls_context(), timeout=timeout)
    else:
        conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=timeout)
    headers = dict(headers or {})
    data = json.dumps(body).encode() if isinstance(body, (dict, list)) else body
    if isinstance(body, (dict, list)):
        headers.setdefault("Content-Type", "application/json")
    path = u.path or "/"
    if u.query:
        path += "?" + u.query
    conn.request(method, path, body=data, headers=headers)
    r = conn.getresponse()
    if read_limit:
        chunks, deadline = b"", time.time() + 8
        while len(chunks) < read_limit and time.time() < deadline:
            c = r.read1(4096)
            if not c:
                break
            chunks += c
        out = Resp(r.status, r.getheaders(), chunks, url)
    else:
        out = Resp(r.status, r.getheaders(), r.read(), url)
    conn.close()
    return out


def cookie_value(resp, name):
    for v in resp.headers_all("set-cookie"):
        if v.startswith(name + "="):
            return v.split(";", 1)[0]
    return None


def cookie_attrs(resp, name):
    for v in resp.headers_all("set-cookie"):
        if v.startswith(name + "="):
            return [a.strip() for a in v.split(";")[1:]]
    return None


def password_for(role):
    with open(CREDS) as f:
        for line in f:
            if line.startswith("#") or "\t" not in line:
                continue
            user, pw = line.rstrip("\n").split("\t", 1)
            if user.split("@", 1)[0] == role:
                return user, pw
    raise KeyError(f"no credential for {role} in {CREDS}")


class Jar:
    """A per-host cookie jar, enough for the console and Dex."""

    def __init__(self):
        self.by_host = {}

    def take(self, resp):
        host = urllib.parse.urlsplit(resp.url).hostname
        for v in resp.headers_all("set-cookie"):
            name, _, rest = v.partition("=")
            value = rest.split(";", 1)[0]
            self.by_host.setdefault(host, {})[name] = value

    def header(self, url):
        host = urllib.parse.urlsplit(url).hostname
        c = self.by_host.get(host, {})
        return "; ".join(f"{k}={v}" for k, v in c.items()) if c else None


def signin(role, trace=None):
    """Sign in as `<role>@…` through the console's /auth/login, Dex's password form and the
    callback. Returns a dict: session (the cookie pair), csrf, session JSON, callback Resp.
    `trace` (a list) receives one line per hop: status and URL path, never a cookie or code."""
    user, pw = password_for(role)
    jar = Jar()

    def hop(method, url, body=None, headers=None):
        h = dict(headers or {})
        c = jar.header(url)
        if c:
            h["Cookie"] = c
        r = request(method, url, headers=h, body=body)
        jar.take(r)
        if trace is not None:
            trace.append(f"{method} {urllib.parse.urlsplit(url).netloc}{urllib.parse.urlsplit(url).path} -> {r.status}")
        return r

    r = hop("GET", f"{BASE}/auth/login")
    if r.status not in (302, 303):
        raise RuntimeError(f"/auth/login answered {r.status}")
    url = urllib.parse.urljoin(r.url, r.header("location"))
    # follow Dex until its password form
    for _ in range(6):
        r = hop("GET", url)
        if r.status in (302, 303):
            url = urllib.parse.urljoin(r.url, r.header("location"))
            continue
        break
    if r.status != 200 or b"password" not in r.body.lower():
        raise RuntimeError(f"Dex did not serve its login form ({r.status} at {urllib.parse.urlsplit(url).path})")
    m = re.search(rb'<form[^>]*action="([^"]*)"', r.body)
    action = urllib.parse.urljoin(url, m.group(1).decode().replace("&amp;", "&")) if m else url
    form = urllib.parse.urlencode({"login": user, "password": pw}).encode()
    del pw
    r = hop("POST", action, body=form, headers={"Content-Type": "application/x-www-form-urlencoded"})
    callback = None
    for _ in range(6):
        if r.status not in (302, 303):
            raise RuntimeError(f"Dex answered {r.status} after the password (wrong password?)")
        url = urllib.parse.urljoin(r.url, r.header("location"))
        if urllib.parse.urlsplit(url).hostname == CONSOLE_HOST:
            callback = url
            break
        r = hop("GET", url)
    if not callback:
        raise RuntimeError("Dex never redirected back to the console")
    cb = hop("GET", callback)
    if cb.status not in (302, 303):
        raise RuntimeError(f"/auth/callback answered {cb.status}: {cb.body[:200]!r}")
    session = cookie_value(cb, "__Host-logweir_session")
    s = request("GET", f"{BASE}/api/v1/session", headers={"Cookie": session})
    if s.status != 200:
        raise RuntimeError(f"/api/v1/session answered {s.status}")
    return {"user": user, "session": session, "csrf": s.json().get("csrfToken"), "sessionJson": s.json(),
            "callback": cb, "landing": cb.header("location")}


def api(method, path, who=None, body=None, headers=None, key=None, csrf=True, read_limit=None):
    h = dict(headers or {})
    if who:
        h["Cookie"] = who["session"]
    if method not in ("GET", "HEAD") and who and csrf:
        h.setdefault("X-CSRF-Token", who["csrf"])
        h.setdefault("Origin", BASE)
    if key:
        h["Idempotency-Key"] = key
    return request(method, BASE + path, headers=h, body=body, read_limit=read_limit)


def describe_session(sj):
    return {"actor": sj.get("actor"), "authenticationMode": sj.get("authenticationMode"),
            "grants": [(g.get("name"), g.get("roles")) for g in sj.get("namespaces", [])]}


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "signin":
        trace = []
        s = signin(sys.argv[2], trace)
        attrs = cookie_attrs(s["callback"], "__Host-logweir_session")
        print(json.dumps({"hops": trace, "landing": s["landing"], "sessionCookieAttributes": attrs,
                          **describe_session(s["sessionJson"])}, indent=1))
    else:
        print(__doc__)
        sys.exit(2)
