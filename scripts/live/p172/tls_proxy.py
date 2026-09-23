#!/usr/bin/env python3
"""A TLS-terminating reverse proxy standing in for the ingress controller.

Listens on 127.0.0.1:<port> with a self-signed certificate, forwards to the API
over plain HTTP, and — like an ingress controller — OVERWRITES X-Forwarded-For
and X-Forwarded-Proto rather than trusting what the client sent. Bodies are
streamed, so the SSE route streams through it.
"""
import http.client, ssl, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT, UPSTREAM_PORT, CERT, KEY = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]
HOP = {"connection", "keep-alive", "transfer-encoding", "te", "upgrade", "proxy-connection",
       "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "forwarded"}

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, fmt, *args):
        sys.stderr.write("proxy %s\n" % (fmt % args))
    def relay(self):
        length = int(self.headers.get("content-length", "0") or 0)
        body = self.rfile.read(length) if length else None
        conn = http.client.HTTPConnection("127.0.0.1", UPSTREAM_PORT, timeout=60)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        headers["X-Forwarded-For"] = self.client_address[0]
        headers["X-Forwarded-Proto"] = "https"
        conn.request(self.command, self.path, body=body, headers=headers)
        resp = conn.getresponse()
        self.send_response_only(resp.status, resp.reason)
        for k, v in resp.getheaders():
            if k.lower() not in HOP and k.lower() != "content-length":
                self.send_header(k, v)
        self.send_header("connection", "close")
        self.end_headers()
        self.close_connection = True
        while True:
            chunk = resp.read1(65536)
            if not chunk:
                break
            self.wfile.write(chunk)
            self.wfile.flush()
        conn.close()
    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = relay

srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(CERT, KEY)
srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
srv.serve_forever()
