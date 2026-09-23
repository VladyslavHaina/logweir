#!/usr/bin/env python3
"""A local ES256 OpenID provider for the PLAT-17.2 live run. Loopback only.

Discovery, JWKS, an authorization endpoint that signs in whichever user the
harness registered last via POST /control/next-user, and a token endpoint that
checks client_secret_basic and PKCE S256 before minting an ID token. No token
ever leaves this process except in the token response to the API.
"""
import base64, hashlib, json, os, secrets, sys, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

PORT = int(sys.argv[1])
CLIENT_ID = sys.argv[2]
# argv[3] is the PATH of a 0600 file holding the run's client secret, so the
# value is never on a command line (a process listing) or in this source.
with open(sys.argv[3]) as _f:
    CLIENT_SECRET = _f.read().strip()
ISSUER = f"http://127.0.0.1:{PORT}"
KEY = ec.generate_private_key(ec.SECP256R1())
KID = "p172-es256"
LOCK = threading.Lock()
NEXT_USER = {}
CODES = {}

def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def jwk():
    n = KEY.public_key().public_numbers()
    return {"kty": "EC", "crv": "P-256", "kid": KID, "alg": "ES256", "use": "sig",
            "x": b64(n.x.to_bytes(32, "big")), "y": b64(n.y.to_bytes(32, "big"))}

def mint(claims):
    header = b64(json.dumps({"alg": "ES256", "kid": KID, "typ": "JWT"}).encode())
    payload = b64(json.dumps(claims).encode())
    der = KEY.sign(f"{header}.{payload}".encode(), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der)
    return f"{header}.{payload}.{b64(r.to_bytes(32, 'big') + s.to_bytes(32, 'big'))}"

class H(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("idp %s\n" % (fmt % args))

    def send_json(self, status, body, extra=()):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        for k, v in extra:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/.well-known/openid-configuration":
            return self.send_json(200, {
                "issuer": ISSUER, "authorization_endpoint": f"{ISSUER}/authorize",
                "token_endpoint": f"{ISSUER}/token", "jwks_uri": f"{ISSUER}/jwks",
                "response_types_supported": ["code"], "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["ES256"]})
        if u.path == "/jwks":
            return self.send_json(200, {"keys": [jwk()]})
        if u.path == "/authorize":
            q = dict(urllib.parse.parse_qsl(u.query))
            assert q["client_id"] == CLIENT_ID and q["code_challenge_method"] == "S256"
            with LOCK:
                user = dict(NEXT_USER)
                code = secrets.token_urlsafe(24)
                CODES[code] = {"user": user, "nonce": q["nonce"], "challenge": q["code_challenge"],
                               "redirect_uri": q["redirect_uri"]}
            loc = q["redirect_uri"] + "?" + urllib.parse.urlencode({"code": code, "state": q["state"]})
            self.send_response(302)
            self.send_header("location", loc)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length).decode()
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/control/next-user":
            with LOCK:
                NEXT_USER.clear()
                NEXT_USER.update(json.loads(body))
            return self.send_json(200, {"ok": True})
        if u.path == "/token":
            auth = self.headers.get("authorization", "")
            expected = "Basic " + base64.b64encode(
                f"{urllib.parse.quote(CLIENT_ID, safe='')}:{urllib.parse.quote(CLIENT_SECRET, safe='')}".encode()).decode()
            if auth != expected:
                return self.send_json(401, {"error": "invalid_client"})
            form = dict(urllib.parse.parse_qsl(body))
            with LOCK:
                grant = CODES.pop(form.get("code", ""), None)
            if not grant or form.get("redirect_uri") != grant["redirect_uri"]:
                return self.send_json(400, {"error": "invalid_grant"})
            challenge = b64(hashlib.sha256(form.get("code_verifier", "").encode()).digest())
            if challenge != grant["challenge"]:
                return self.send_json(400, {"error": "invalid_grant"})
            now = int(time.time())
            user = grant["user"]
            token = mint({"iss": ISSUER, "aud": CLIENT_ID, "sub": user["sub"], "iat": now,
                          "exp": now + 300, "auth_time": now, "nonce": grant["nonce"],
                          "groups": user.get("groups", []), "name": user.get("name", user["sub"])})
            return self.send_json(200, {"id_token": token, "access_token": "at-" + secrets.token_hex(8),
                                        "token_type": "Bearer", "expires_in": 300})
        self.send_json(404, {"error": "not_found"})

ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
