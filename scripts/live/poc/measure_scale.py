#!/usr/bin/env python3
"""PLAT-20.2's large-catalog measurements on the PoC install, through the product API only
(the deployed console, Traefik, TLS), as the viewer (reads) and the operator (starts):

  measure_scale.py <outdir> [catalog-name]

1. discovery   — a topic discovery on the source connection: start -> terminal, and the
                 inventory page read; then a Full RecoveryCatalog sync over the archive:
                 create -> Synced, with the sync Job's own start/finish and the point count.
2. history     — every page of GET /backups (limit 200), the catalog's points (limit 200),
                 and the schedule's detail; per request and in total.
3. status load — GET /operations/backup/{name} for a sample of runs, sequentially and 8 at a
                 time; the session, namespaces, destinations, protection and catalog reads.

No budget is asserted: the numbers are recorded (PLAT-20.2: "record the numbers; no budget is
required"). Every kubectl call names --context docker-desktop and has a deadline.
"""
import concurrent.futures as cf
import json
import os
import random
import statistics
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import poclib  # noqa: E402

OUT = sys.argv[1]
CATALOG = sys.argv[2] if len(sys.argv) > 2 else "archive"
NS = "logweir-poc"
K = ["kubectl", "--context", "docker-desktop", "--request-timeout=60s"]
RESULT = {}


def kj(*args):
    p = subprocess.run(K + ["-n", NS] + list(args) + ["-o", "json"], capture_output=True, text=True, timeout=120)
    return json.loads(p.stdout) if p.returncode == 0 else None


def timed(fn):
    t = time.perf_counter()
    r = fn()
    return r, round((time.perf_counter() - t) * 1000, 1)


def stats(ms):
    s = sorted(ms)
    return {"n": len(s), "p50": s[len(s) // 2], "p95": s[max(0, int(len(s) * 0.95) - 1)], "max": s[-1],
            "mean": round(statistics.mean(s), 1)} if s else {"n": 0}


def page_all(who, path, limit=200, cap=100):
    pages, cursor, total_ms, items = [], None, 0.0, 0
    for _ in range(cap):
        q = f"{path}?limit={limit}" + (f"&cursor={cursor}" if cursor else "")
        r, ms = timed(lambda: poclib.api("GET", q, who))
        if r.status != 200:
            pages.append({"status": r.status, "ms": ms, "code": r.json().get("code")})
            break
        d = r.json()
        n = len(d.get("items", []))
        items += n
        total_ms += ms
        pages.append({"status": 200, "ms": ms, "items": n, "bytes": len(r.body)})
        cursor = (d.get("page") or {}).get("nextCursor")
        if not cursor:
            break
    return {"items": items, "pages": len(pages), "totalMs": round(total_ms, 1), "perPage": stats([p["ms"] for p in pages if p.get("status") == 200]),
            "bytes": sum(p.get("bytes", 0) for p in pages), "refused": [p for p in pages if p.get("status") != 200]}


def main():
    os.makedirs(OUT, exist_ok=True)
    viewer, operator = poclib.signin("viewer"), poclib.signin("operator")
    backups = kj("get", "backups")["items"]
    RESULT["archive"] = {"backups": len(backups),
                         "succeeded": sum(1 for b in backups if (b.get("status") or {}).get("phase") == "Succeeded"),
                         "verified": sum(1 for b in backups if ((b.get("status") or {}).get("evidence") or {}).get("verification", {}).get("result") == "Valid")}
    print("archive:", RESULT["archive"], flush=True)

    # ------------------------------------------------------------ 1a. topic discovery
    src = next(c["metadata"]["name"] for c in kj("get", "kafkaclusters")["items"] if c["spec"]["role"] == "source")
    r, ms = timed(lambda: poclib.api("POST", f"/api/v1/namespaces/{NS}/connections/{src}/topic-discoveries", operator,
                                     body={}, key="scale-disc-" + uuid.uuid4().hex[:10]))
    d = r.json().get("item", {})
    did = d.get("id") or d.get("name")
    t0, state = time.time(), d.get("state")
    while did and time.time() - t0 < 300 and state not in ("succeeded", "failed", "Succeeded", "Failed", "ready"):
        time.sleep(1)
        g = poclib.api("GET", f"/api/v1/namespaces/{NS}/topic-discoveries/{did}", viewer).json().get("item", {})
        state = g.get("state") or g.get("phase")
    wall = round(time.time() - t0, 1)
    tp, tms = timed(lambda: poclib.api("GET", f"/api/v1/namespaces/{NS}/topic-discoveries/{did}/topics?limit=200", viewer))
    RESULT["topicDiscovery"] = {"startStatus": r.status, "startMs": ms, "reused": r.json().get("reused"), "id": did,
                                "terminalState": state, "startToTerminalSeconds": wall, "topicsPageMs": tms,
                                "topics": len(tp.json().get("items", []))}
    print("topicDiscovery:", RESULT["topicDiscovery"], flush=True)

    # ------------------------------------------------------------ 1b. catalog discovery (Full sync)
    existing = kj("get", "recoverycatalog", CATALOG)
    t0 = time.time()
    if not existing:
        r, ms = timed(lambda: poclib.api("POST", f"/api/v1/namespaces/{NS}/catalogs", operator,
                                         body={"name": CATALOG, "destinationRef": {"name": "primary"}, "syncMode": "full"},
                                         key="scale-cat-" + uuid.uuid4().hex[:10]))
        created = {"status": r.status, "ms": ms}
    else:
        created = {"status": "existing"}
    st = {}
    while time.time() - t0 < 900:
        c = kj("get", "recoverycatalog", CATALOG) or {}
        st = c.get("status") or {}
        synced = next((x for x in st.get("conditions", []) if x["type"] == "Synced"), {})
        if synced.get("status") == "True" and st.get("lastSyncJob", {}).get("finishedAt"):
            break
        time.sleep(2)
    job = st.get("lastSyncJob", {})
    RESULT["catalogSync"] = {"create": created, "createToSyncedSeconds": round(time.time() - t0, 1), "counts": st.get("counts"),
                             "pages": len(st.get("pages", [])), "truncated": st.get("truncated"), "job": job,
                             "synced": next((x.get("message") for x in st.get("conditions", []) if x["type"] == "Synced"), None)}
    print("catalogSync:", RESULT["catalogSync"], flush=True)

    # ------------------------------------------------------------ 2. history queries
    RESULT["historyBackups"] = page_all(viewer, f"/api/v1/namespaces/{NS}/backups")
    print("history /backups:", {k: v for k, v in RESULT["historyBackups"].items() if k != "refused"}, flush=True)
    RESULT["catalogPoints"] = page_all(viewer, f"/api/v1/namespaces/{NS}/catalogs/{CATALOG}/points")
    print("catalog points:", {k: v for k, v in RESULT["catalogPoints"].items() if k != "refused"}, flush=True)
    RESULT["catalogPointsSelectable"] = page_all(viewer, f"/api/v1/namespaces/{NS}/catalogs/{CATALOG}/points", limit=200)
    sch = kj("get", "backupschedules")["items"][0]["metadata"]["name"]
    ms = [timed(lambda: poclib.api("GET", f"/api/v1/namespaces/{NS}/schedules/{sch}", viewer))[1] for _ in range(10)]
    RESULT["scheduleDetail"] = stats(ms)
    print("schedule detail x10:", RESULT["scheduleDetail"], flush=True)

    # ------------------------------------------------------------ 3. status load
    names = [b["metadata"]["name"] for b in backups]
    random.seed(20260924)
    sample = random.sample(names, min(100, len(names)))
    seq = [timed(lambda n=n: poclib.api("GET", f"/api/v1/namespaces/{NS}/operations/backup/{n}", viewer))[1] for n in sample[:50]]
    RESULT["operationStatusSequential"] = stats(seq)

    def one(n):
        return timed(lambda: poclib.api("GET", f"/api/v1/namespaces/{NS}/operations/backup/{n}", viewer))

    t = time.perf_counter()
    with cf.ThreadPoolExecutor(8) as ex:
        res = list(ex.map(one, sample))
    RESULT["operationStatusConcurrent8"] = dict(stats([m for _, m in res]), wallMs=round((time.perf_counter() - t) * 1000, 1),
                                                statuses=sorted({r.status for r, _ in res}))
    print("operation status seq50:", RESULT["operationStatusSequential"], "conc8x100:", RESULT["operationStatusConcurrent8"], flush=True)
    reads = {}
    for label, path in (("session", "/api/v1/session"), ("namespaces", "/api/v1/namespaces"),
                        ("destinations", f"/api/v1/namespaces/{NS}/destinations"),
                        ("destination", f"/api/v1/namespaces/{NS}/destinations/primary"),
                        ("protectionPolicies", f"/api/v1/namespaces/{NS}/protection-policies"),
                        ("catalogs", f"/api/v1/namespaces/{NS}/catalogs"),
                        ("catalog", f"/api/v1/namespaces/{NS}/catalogs/{CATALOG}"),
                        ("signers", f"/api/v1/namespaces/{NS}/catalogs/{CATALOG}/signers"),
                        ("schedules", f"/api/v1/namespaces/{NS}/schedules"),
                        ("restores", f"/api/v1/namespaces/{NS}/restores")):
        ms, st = [], set()
        for _ in range(10):
            r, m = timed(lambda: poclib.api("GET", path, viewer))
            ms.append(m)
            st.add(r.status)
        reads[label] = dict(stats(ms), statuses=sorted(st))
    RESULT["statusReads"] = reads
    print("status reads x10:", json.dumps(reads), flush=True)
    json.dump(RESULT, open(os.path.join(OUT, "measurements.json"), "w"), indent=1)
    print("written", os.path.join(OUT, "measurements.json"))


if __name__ == "__main__":
    main()
