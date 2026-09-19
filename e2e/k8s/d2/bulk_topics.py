#!/usr/bin/env python3
"""Create many Kafka topics in batches, over the wire, with no client library.

D2 §14.2 asks for a host helper that creates `bulk-00000` … `bulk-04999`
through a `kubectl port-forward` so that S7's "large catalog" is a real catalog
and not a smaller one the assertions were bent around. The decision doc names
an rdkafka `AdminClient`; this is the same request in 200 lines of the standard
library instead, because adding `rdkafka` as a non-dev dependency of the `e2e`
crate would change that crate's dependency graph, which Global Constraint 38
and `scripts/check-deps-count.sh` exist to prevent. The wire format is the one
the broker publishes for itself: this speaks `ApiVersions` first and refuses to
guess if the broker does not support `CreateTopics` v0.

Only the PLAINTEXT listener is used, and only through a port-forward to the
pod, which is why the fixture advertises it as `localhost:9092`: no credential
is involved, and nothing outside the pod can reach it.

    python3 bulk_topics.py --port 9092 --prefix bulk- --count 5000 --batch 500
    python3 bulk_topics.py --port 9092 --list
"""

from __future__ import annotations

import argparse
import socket
import struct
import sys
import time

API_VERSIONS = 18
CREATE_TOPICS = 19
METADATA = 3
CLIENT_ID = "d2w14-bulk-topics"


class Conn:
    def __init__(self, host: str, port: int, timeout: float = 30.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.correlation = 0

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def _send(self, api_key: int, api_version: int, body: bytes) -> bytes:
        self.correlation += 1
        header = struct.pack(">hhi", api_key, api_version, self.correlation)
        header += _string(CLIENT_ID)
        payload = header + body
        self.sock.sendall(struct.pack(">i", len(payload)) + payload)
        size = struct.unpack(">i", _recv_exactly(self.sock, 4))[0]
        frame = _recv_exactly(self.sock, size)
        correlation = struct.unpack(">i", frame[:4])[0]
        if correlation != self.correlation:
            raise RuntimeError(
                f"correlation id {correlation} != {self.correlation}: the stream is desynchronised"
            )
        return frame[4:]

    def api_versions(self) -> dict[int, tuple[int, int]]:
        body = self._send(API_VERSIONS, 0, b"")
        error, = struct.unpack(">h", body[:2])
        if error != 0:
            raise RuntimeError(f"ApiVersions returned error {error}")
        count, = struct.unpack(">i", body[2:6])
        out: dict[int, tuple[int, int]] = {}
        offset = 6
        for _ in range(count):
            key, low, high = struct.unpack(">hhh", body[offset:offset + 6])
            out[key] = (low, high)
            offset += 6
        return out

    def create_topics(self, names: list[str], *, partitions: int, timeout_ms: int) -> dict[str, int]:
        body = struct.pack(">i", len(names))
        for name in names:
            body += _string(name)
            body += struct.pack(">i", partitions)
            body += struct.pack(">h", 1)          # replication factor
            body += struct.pack(">i", 0)          # no explicit replica assignment
            body += struct.pack(">i", 0)          # no config entries
        body += struct.pack(">i", timeout_ms)
        response = self._send(CREATE_TOPICS, 0, body)
        count, = struct.unpack(">i", response[:4])
        offset = 4
        out: dict[str, int] = {}
        for _ in range(count):
            name, offset = _read_string(response, offset)
            code, = struct.unpack(">h", response[offset:offset + 2])
            offset += 2
            out[name] = code
        return out

    def topics(self) -> list[tuple[str, bool, int]]:
        """`Metadata` v1 with a null topic array: every topic the broker has,
        with its `is_internal` flag and its partition count.

        v1 and not v2: `cluster_id` arrived in v2, and reading a field the
        broker did not send is how a hand-written parser silently invents
        data."""
        body = struct.pack(">i", -1)
        response = self._send(METADATA, 1, body)
        broker_count, = struct.unpack(">i", response[:4])
        offset = 4
        for _ in range(broker_count):
            offset += 4                                 # node id
            _, offset = _read_string(response, offset)  # host
            offset += 4                                 # port
            _, offset = _read_string(response, offset)  # rack (nullable)
        offset += 4                                     # controller id
        topic_count, = struct.unpack(">i", response[offset:offset + 4])
        offset += 4
        out: list[tuple[str, bool, int]] = []
        for _ in range(topic_count):
            error, = struct.unpack(">h", response[offset:offset + 2])
            offset += 2
            name, offset = _read_string(response, offset)
            internal = response[offset] != 0
            offset += 1
            partition_count, = struct.unpack(">i", response[offset:offset + 4])
            offset += 4
            for _ in range(partition_count):
                offset += 2 + 4 + 4                     # error, partition, leader
                replicas, = struct.unpack(">i", response[offset:offset + 4])
                offset += 4 + 4 * replicas
                isr, = struct.unpack(">i", response[offset:offset + 4])
                offset += 4 + 4 * isr
            if error == 0:
                out.append((name, internal, partition_count))
        return out


def _string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack(">h", len(raw)) + raw


def _read_string(buf: bytes, offset: int) -> tuple[str, int]:
    length, = struct.unpack(">h", buf[offset:offset + 2])
    offset += 2
    if length < 0:
        return "", offset
    return buf[offset:offset + length].decode(), offset + length


def _recv_exactly(sock: socket.socket, count: int) -> bytes:
    chunks = []
    remaining = count
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("the broker closed the connection mid-frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def converged_topics(conn: "Conn", *, stable_for: float = 20.0, timeout: float = 300.0,
                     interval: float = 5.0) -> tuple[list[tuple[str, bool, int]], dict]:
    """The broker's topic list, once it has stopped moving.

    KAFKA TOPIC CREATION IS ASYNCHRONOUS, and `CreateTopics` returning 0 for
    five thousand names does not mean five thousand topics are in metadata.
    The previous `--list` took one snapshot in the same port-forward as the
    creation and recorded **503** of 5,000; a minute later the controller's own
    discovery listed 3,004. `fixtures/broker-topics-user.txt` is written from
    this output and S7 asserts `returned == len(that file)`, so a snapshot of a
    broker mid-convergence is not a slightly-wrong baseline — it is a baseline
    that guarantees the row fails (lab-refresh-5 §7.2).

    So the count has to hold still before anything is printed. `stable_for` is
    the quiet period, and a run that never reaches one exits **2** with the
    counts it saw rather than printing a listing somebody will save.
    """
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    counts: list[int] = []
    rows = sorted(conn.topics())
    last_change = time.monotonic()
    polls = 1
    counts.append(len(rows))
    while time.monotonic() < deadline:
        if time.monotonic() - last_change >= stable_for:
            break
        time.sleep(interval)
        current = sorted(conn.topics())
        polls += 1
        if len(current) != len(rows):
            last_change = time.monotonic()
        rows = current
        if not counts or counts[-1] != len(rows):
            counts.append(len(rows))
    stable = time.monotonic() - last_change
    return rows, {
        "converged": stable >= stable_for,
        "stableSeconds": stable,
        "polls": polls,
        "elapsedSeconds": time.monotonic() - started,
        "countsSeen": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--prefix", default="bulk-")
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--stable-for", type=float, default=20.0,
                        help="seconds the topic count must hold still before --list prints")
    parser.add_argument("--converge-timeout", type=float, default=300.0,
                        help="how long to wait for that; exit 2 if it never settles")
    parser.add_argument("--poll-interval", type=float, default=5.0)
    args = parser.parse_args()

    conn = Conn(args.host, args.port)
    try:
        versions = conn.api_versions()
        low, high = versions.get(CREATE_TOPICS, (None, None))
        if low is None or low > 0:
            raise RuntimeError(
                f"this broker supports CreateTopics v{low}..v{high}; this helper speaks v0 only"
            )
        if args.list:
            rows, settle = converged_topics(
                conn, stable_for=args.stable_for, timeout=args.converge_timeout,
                interval=args.poll_interval,
            )
            for name, internal, partitions in rows:
                print(f"{name}\t{'internal' if internal else 'user'}\t{partitions}")
            print(
                f"# {len(rows)} topics; metadata stable at that count for "
                f"{settle['stableSeconds']:.0f}s after {settle['polls']} poll(s) over "
                f"{settle['elapsedSeconds']:.0f}s; counts seen {settle['countsSeen']}",
                file=sys.stderr,
            )
            if not settle["converged"]:
                print(
                    f"# WARNING: the count was still moving when the "
                    f"{args.converge_timeout}s budget ran out; this listing is a snapshot of "
                    f"a broker that had not converged and must not be used as a baseline",
                    file=sys.stderr,
                )
                return 2
            return 0
        created = 0
        existed = 0
        errors: dict[int, int] = {}
        started = time.monotonic()
        for start in range(0, args.count, args.batch):
            names = [
                f"{args.prefix}{index:05d}"
                for index in range(start, min(start + args.batch, args.count))
            ]
            result = conn.create_topics(names, partitions=args.partitions, timeout_ms=60000)
            for code in result.values():
                if code == 0:
                    created += 1
                elif code == 36:            # TOPIC_ALREADY_EXISTS
                    existed += 1
                else:
                    errors[code] = errors.get(code, 0) + 1
            print(
                f"{start + len(names)}/{args.count} created={created} existed={existed} "
                f"errors={errors} {time.monotonic() - started:.0f}s",
                file=sys.stderr, flush=True,
            )
        if errors:
            print(f"ERRORS {errors}", file=sys.stderr)
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
