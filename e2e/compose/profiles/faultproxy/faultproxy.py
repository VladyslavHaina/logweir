"""The produce-response fault proxy of the `faultproxy` profile (PROD-01.5d).

It forwards every byte between Kafka clients and ONE broker listener, and can
DROP, or HOLD for a while, the response to exactly one Produce request: the
Nth one it has seen since it was last armed, counted over every connection.
The broker advertises this proxy's published address for that listener, so
every connection a client makes to the broker crosses it. Dropping a whole
response frame keeps the stream framed: the client simply never hears back
about that one request, which the broker did process. Holding a response holds
every later response on the same connection behind it, as TCP would.

Control: a TCP port taking ONE line per connection and answering one JSON line.

    arm drop N       drop the response to the Nth Produce request (N >= 1)
    arm hold N MS    forward that response MS milliseconds late
    off              forward everything (the control)
    stats            what has been seen since the last arm or off

`arm` and `off` reset every counter. Anything else is answered with `error`.

Usage: faultproxy.py LISTEN_PORT CONTROL_PORT UPSTREAM_HOST:UPSTREAM_PORT
Standard library only: it runs on any pinned image that carries python3.
"""

import json
import socket
import struct
import sys
import threading
import time

PRODUCE = 0
LOCK = threading.Lock()
STATE = {}


def reset(mode, nth=0, hold_ms=0):
    with LOCK:
        STATE.clear()
        STATE.update(
            mode=mode,
            nth=nth,
            hold_ms=hold_ms,
            produce_requests=0,
            produce_responses_forwarded=0,
            faulted=[],
        )


def read_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def read_frame(sock):
    """One size-prefixed Kafka frame, prefix included; None at end of stream."""
    head = read_exact(sock, 4)
    if head is None:
        return None
    body = read_exact(sock, struct.unpack(">i", head)[0])
    return None if body is None else head + body


def requests(client, upstream, conn):
    """Client to broker: number each Produce request, mark the armed one."""
    while True:
        frame = read_frame(client)
        if frame is None:
            return
        api_key, _version, corr = struct.unpack(">hhi", frame[4:12])
        if api_key == PRODUCE:
            with LOCK:
                STATE["produce_requests"] += 1
                conn["produce"].add(corr)
                if STATE["mode"] != "off" and STATE["produce_requests"] == STATE["nth"]:
                    conn["fault"][corr] = (STATE["mode"], STATE["nth"])
        upstream.sendall(frame)


def responses(upstream, client, conn):
    """Broker to client: drop or hold the marked response, forward the rest."""
    while True:
        frame = read_frame(upstream)
        if frame is None:
            return
        corr = struct.unpack(">i", frame[4:8])[0]
        with LOCK:
            fault = conn["fault"].pop(corr, None)
            produce = corr in conn["produce"]
            conn["produce"].discard(corr)
            if fault:
                STATE["faulted"].append(
                    {"mode": fault[0], "produce_request": fault[1], "correlation_id": corr}
                )
            hold_ms = STATE["hold_ms"]
        if fault and fault[0] == "drop":
            continue
        if fault:
            time.sleep(hold_ms / 1000)
        if produce:
            with LOCK:
                STATE["produce_responses_forwarded"] += 1
        client.sendall(frame)


def pump(fn, a, b, conn):
    try:
        fn(a, b, conn)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def serve(listen_port, upstream):
    srv = socket.create_server(("0.0.0.0", listen_port))
    while True:
        client, _ = srv.accept()
        try:
            up = socket.create_connection(upstream, timeout=10)
            up.settimeout(None)
        except OSError:
            client.close()
            continue
        conn = {"produce": set(), "fault": {}}
        threading.Thread(target=pump, args=(requests, client, up, conn), daemon=True).start()
        threading.Thread(target=pump, args=(responses, up, client, conn), daemon=True).start()


def command(line):
    words = line.split()
    try:
        if words == ["off"]:
            reset("off")
        elif len(words) == 3 and words[:2] == ["arm", "drop"] and int(words[2]) >= 1:
            reset("drop", int(words[2]))
        elif len(words) == 4 and words[:2] == ["arm", "hold"] and int(words[2]) >= 1:
            reset("hold", int(words[2]), max(0, int(words[3])))
        elif words != ["stats"]:
            return {"error": f"unknown command: {line.strip()!r}"}
    except ValueError:
        return {"error": f"not a number in {line.strip()!r}"}
    with LOCK:
        return json.loads(json.dumps(STATE))


def control(port):
    srv = socket.create_server(("0.0.0.0", port))
    while True:
        c, _ = srv.accept()
        with c:
            c.settimeout(5)
            try:
                line = c.makefile("r").readline()
                c.sendall((json.dumps(command(line)) + "\n").encode())
            except OSError:
                pass


def main():
    listen_port, control_port = int(sys.argv[1]), int(sys.argv[2])
    host, port = sys.argv[3].rsplit(":", 1)
    reset("off")
    threading.Thread(target=control, args=(control_port,), daemon=True).start()
    print(f"faultproxy: :{listen_port} -> {host}:{port}, control :{control_port}", flush=True)
    serve(listen_port, (host, int(port)))


if __name__ == "__main__":
    main()
