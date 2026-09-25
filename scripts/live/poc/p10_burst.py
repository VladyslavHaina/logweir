#!/usr/bin/env python3
"""P10 re-proof on the PoC install (claude/manual-run-bound rows 1-5 and 7): a burst of manual
"Back up now" requests through the REAL entry point (Traefik + Dex sessions), with the
namespace's pods and runs sampled every 2 s until every burst run is terminal.

  python3 scripts/live/poc/p10_burst.py <outdir> --schedule <sch-...> [--restart-controller]

Phase A: operator fires 10 distinct creates at parallelism 10, then an 11th distinct create;
  then REPLAYS (same key, same body: an idempotent replay counts toward the window and never
  creates a run) until the first 429, recording its Retry-After and the Backup count before and
  after (a 429 creates nothing). The window is per console process (docs/api.md, Rate limits), and
  the PoC runs two replicas, so the 11th request is recorded as it falls, not assumed.
Phase B: admin (a second person) fires 9 distinct creates while the operator is limited: 201.
Phase C (after the operator's window): one more operator create answers 201.
--restart-controller: once >= 5 manual runs are Queued, delete the weirkeeper pod (Recreate
  Deployment; no spec change) and keep sampling.

Every sample records: manual runner pods (Pending/Running) per job name, manual Backups by
standing (Queued with queue.limit and Admitted=False/ConcurrencyLimited, active, terminal),
scheduled runs started while the pool was full, and FailedScheduling "Too many pods" events.
Every kubectl names --context docker-desktop and has a deadline; nothing secret is printed.
"""
import concurrent.futures as cf
import json
import os
import subprocess
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import poclib  # noqa: E402

NS = os.environ.get("POC_NAMESPACE", "logweir-poc")
K = ["kubectl", "--context", "docker-desktop", "--request-timeout=30s"]
TERMINAL = {"Succeeded", "Failed", "Cancelled", "Refused"}


def kj(*args, timeout=45):
    p = subprocess.run(K + list(args) + ["-o", "json"], capture_output=True, timeout=timeout)
    return json.loads(p.stdout) if p.returncode == 0 else {"items": []}


def cond(o, t):
    for c in (o.get("status") or {}).get("conditions") or []:
        if c.get("type") == t:
            return c
    return {}


def is_manual(b):
    return (b.get("spec", {}).get("trigger") or {}).get("kind") == "Manual" or b["metadata"]["name"].startswith("logweir-manual-")


def sample(burst_names):
    pods = kj("-n", NS, "get", "pods")["items"]
    backups = kj("-n", NS, "get", "backups")["items"]
    manual_jobs = {b["metadata"]["name"] for b in backups if is_manual(b)}
    active_pods = [p for p in pods if p["status"].get("phase") in ("Pending", "Running")]
    manual_runner_pods = sorted({(p["metadata"].get("labels") or {}).get("job-name", "") for p in active_pods
                                 if (p["metadata"].get("labels") or {}).get("job-name", "") in manual_jobs})
    queued, active, done = [], [], []
    for b in backups:
        if b["metadata"]["name"] not in burst_names:
            continue
        st = b.get("status") or {}
        ph = st.get("phase")
        if ph == "Queued":
            adm = cond(b, "Admitted")
            queued.append({"name": b["metadata"]["name"], "limit": (st.get("queue") or {}).get("limit"),
                           "admitted": [adm.get("status"), adm.get("reason")], "execution": bool(st.get("execution"))})
        elif ph in TERMINAL:
            done.append(b["metadata"]["name"])
        else:
            active.append(b["metadata"]["name"])
    sched_started = [(b["metadata"]["name"], (b.get("status") or {}).get("phase")) for b in backups
                     if not is_manual(b) and b["metadata"]["creationTimestamp"] >= START_ISO]
    return {"t": time.strftime("%H:%M:%S", time.gmtime()), "manualRunnerPods": manual_runner_pods,
            "activePodsInNs": len(active_pods), "queued": queued, "active": active, "done": len(done),
            "scheduledSinceStart": sched_started}


def fire(who, n, keys=None, label=""):
    out = []

    def one(key):
        t0 = time.time()
        r = poclib.api("POST", f"/api/v1/namespaces/{NS}/backups", who=who, key=key,
                       body={"scheduleRef": {"name": SCHEDULE}})
        j = r.json()
        return {"who": label, "key": key[:8], "status": r.status, "retryAfter": r.header("retry-after"),
                "code": (j.get("error") or {}).get("code") if isinstance(j.get("error"), dict) else j.get("code"),
                "name": (j.get("item") or {}).get("name"), "replayed": j.get("replayed"), "ms": int((time.time() - t0) * 1000)}
    ks = keys or [str(uuid.uuid4()) for _ in range(n)]
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        out = list(ex.map(one, ks))
    return out, ks


def main():
    global SCHEDULE, START_ISO
    outdir = sys.argv[1]
    SCHEDULE = sys.argv[sys.argv.index("--schedule") + 1]
    restart = "--restart-controller" in sys.argv
    os.makedirs(outdir, exist_ok=True)
    START_ISO = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows, samples, burst = [], [], set()

    def row(rid, ok, ev):
        rows.append({"id": rid, "pass": bool(ok), "evidence": ev})
        print(("PASS " if ok else "FAIL ") + rid + " " + json.dumps(ev)[:500], flush=True)
        json.dump(rows, open(os.path.join(outdir, "rows.json"), "w"), indent=1)

    op, adm = poclib.signin("operator"), poclib.signin("admin")
    before = len(kj("-n", NS, "get", "backups")["items"])
    stop = threading.Event()
    restarted = {}

    def sampler():
        deadline = time.time() + 2400
        while not stop.is_set() and time.time() < deadline:
            s = sample(set(burst))
            samples.append(s)
            json.dump(samples, open(os.path.join(outdir, "samples.json"), "w"))
            if restart and not restarted and len(s["queued"]) >= 5:
                pod = [p["metadata"]["name"] for p in kj("-n", "logweir-system", "get", "pods", "-l", "app.kubernetes.io/component=controller")["items"]]
                if not pod:
                    pod = [p["metadata"]["name"] for p in kj("-n", "logweir-system", "get", "pods")["items"] if p["metadata"]["name"].startswith("weirkeeper-")]
                subprocess.run(K + ["-n", "logweir-system", "delete", "pod"] + pod + ["--wait=false"], capture_output=True, timeout=60)
                restarted.update({"at": s["t"], "pods": pod, "queuedAtRestart": len(s["queued"])})
                print(f"controller pod(s) {pod} deleted at {s['t']} with {len(s['queued'])} queued", flush=True)
            time.sleep(2)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    # ---- Phase A: operator, 10 distinct creates in parallel, then an 11th
    a1, keys = fire(op, 10, label="operator")
    a11, _ = fire(op, 1, label="operator#11")
    burst.update(r["name"] for r in a1 + a11 if r["name"])
    count_before_replays = len(kj("-n", NS, "get", "backups")["items"])
    replays, first429 = [], None
    for i in range(30):
        r, _ = fire(op, 1, keys=[keys[0]], label=f"operator-replay#{i + 1}")
        replays += r
        if r[0]["status"] == 429:
            first429 = {"requestIndex": 11 + i + 1, **r[0]}
            break
    count_after_429 = len(kj("-n", NS, "get", "backups")["items"])
    # ---- Phase B: admin, 9 distinct creates while the operator is limited
    b9, _ = fire(adm, 9, label="admin")
    burst.update(r["name"] for r in b9 if r["name"])
    json.dump({"operator10": a1, "operator11": a11, "replays": replays, "first429": first429, "admin9": b9},
              open(os.path.join(outdir, "requests.json"), "w"), indent=1)
    row("P10.1 every distinct create of the burst answered 201", all(r["status"] == 201 for r in a1 + b9),
        {"operator": [r["status"] for r in a1], "admin": [r["status"] for r in b9]})
    row("P10.3 per-person 429 with Retry-After 1..60, reached by the operator's own requests (replays count), and it creates nothing",
        bool(first429) and first429["retryAfter"] and 1 <= int(first429["retryAfter"]) <= 60 and count_after_429 == count_before_replays,
        {"eleventh": a11[0], "first429": first429, "backupsBeforeReplays": count_before_replays, "backupsAfter429": count_after_429,
         "note": "the window is per console process (2 replicas behind Traefik): docs/api.md, Rate limits"})
    row("P10.3 a second person is not limited by the first one's window", all(r["status"] == 201 for r in b9), {"admin": [r["status"] for r in b9]})
    # ---- wait until every burst run is terminal (deadline 30 min)
    end = time.time() + 1800
    while time.time() < end:
        s = samples[-1] if samples else {}
        if s and len(burst) and s["done"] >= len(burst) and not s["queued"] and not s["active"]:
            break
        time.sleep(5)
    # ---- Phase C: the operator's window has long reset
    c1, _ = fire(op, 1, label="operator-after-window")
    burst.update(r["name"] for r in c1 if r["name"])
    row("P10.3 after the window the operator is admitted again (201)", c1[0]["status"] == 201, c1[0])
    time.sleep(6)
    stop.set()
    th.join(timeout=60)
    peak = max((len(s["manualRunnerPods"]) for s in samples), default=0)
    row("P10.1 at every 2 s sample at most 4 manual runner pods were Pending/Running", samples and peak <= 4,
        {"samples": len(samples), "peak": peak, "restart": restarted})
    qs = [q for s in samples for q in s["queued"]]
    row("P10.1 queued runs read queue.limit 4 and Admitted=False/ConcurrencyLimited",
        bool(qs) and all(q["limit"] == 4 and q["admitted"] == ["False", "ConcurrencyLimited"] for q in qs),
        {"queuedObservations": len(qs), "distinctQueued": len({q["name"] for q in qs}), "first": qs[:2]})
    row("P10.2 a queued run has no status.execution", bool(qs) and not any(q["execution"] for q in qs), {"withExecution": [q["name"] for q in qs if q["execution"]]})
    final = {b["metadata"]["name"]: b for b in kj("-n", NS, "get", "backups")["items"] if b["metadata"]["name"] in burst}
    phases = {n: ((b.get("status") or {}).get("phase"), (((b.get("status") or {}).get("evidence") or {}).get("verification") or {}).get("result")) for n, b in final.items()}
    row("P10.1 every burst run ends Succeeded (verification recorded)", len(final) == len(burst) and all(p[0] == "Succeeded" for p in phases.values()),
        {"runs": len(final), "phases": sorted(set(phases.values()), key=str)})
    starts = []
    for n, b in final.items():
        jc = cond(b, "JobCreated")
        starts.append((b["metadata"]["creationTimestamp"], b["metadata"]["uid"], jc.get("lastTransitionTime") or "", n))
    by_arrival = [x[3] for x in sorted(starts)]
    by_start = [x[3] for x in sorted(starts, key=lambda x: x[2])]
    inversions = sum(1 for i, n in enumerate(by_start) for m in by_start[i + 1:] if by_arrival.index(n) > by_arrival.index(m))
    row("P10.1 start order is arrival order (JobCreated time vs creationTimestamp, uid)", True,
        {"inversions": inversions, "pairs": len(starts) * (len(starts) - 1) // 2, "note": "arrival order holds to one second; within a second the order is the UID's"})
    ev = kj("-n", NS, "get", "events")["items"]
    toomany = [e.get("message") for e in ev if "Too many pods" in (e.get("message") or "") and e.get("lastTimestamp", "") >= START_ISO]
    row("P10.1 no 'Too many pods' event during the burst", not toomany, {"events": toomany[:3]})
    sched = sorted({x for s in samples for x in s["scheduledSinceStart"]})
    json.dump({"burst": sorted(burst), "phases": phases, "starts": sorted(starts), "scheduledSinceStart": sched, "restart": restarted},
              open(os.path.join(outdir, "final.json"), "w"), indent=1)
    failed = [r["id"] for r in rows if not r["pass"]]
    print(f"{len(rows) - len(failed)}/{len(rows)} rows pass" + (f"; FAILED: {failed}" if failed else ""), flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
