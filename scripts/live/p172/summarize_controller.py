#!/usr/bin/env python3
"""Summarise the scoped controller's log: startup lines, and every refused call by resource."""
import json, re, sys, collections
lines = open(sys.argv[1]).read().splitlines()
refused = collections.Counter(); other_errors = collections.Counter(); starts = []
for line in lines:
    try:
        j = json.loads(line)
    except ValueError:
        continue
    msg = j.get("fields", {}).get("message", "")
    blob = json.dumps(j)
    if "watching and acting in these namespaces only" in msg or "weirkeeper started" in msg or "watching every namespace" in msg:
        starts.append({k: v for k, v in j.get("fields", {}).items()})
    if "forbidden" in blob.lower() or " 403" in blob:
        m = re.search(r'cannot (\w+) resource [\\"]*([a-z/]+)[\\"]* in API group [\\"]*([a-z.]*)[\\"]*( in the namespace [\\"]*([a-z0-9-]+)| at the cluster scope)?', blob)
        key = f"{m.group(1)} {m.group(2)} ({m.group(3) or 'core'}) {('ns ' + m.group(5)) if m and m.group(5) else 'cluster scope'}" if m else "unparsed:" + msg[:80]
        refused[key] += 1
    elif j.get("level") in ("ERROR", "WARN"):
        other_errors[msg[:120]] += 1
print("lines:", len(lines))
for s in starts:
    print("start:", json.dumps(s))
print("refused calls by (verb resource scope):")
for k, v in sorted(refused.items()):
    print(f"  {v:4d}  {k}")
print("other WARN/ERROR messages:")
for k, v in sorted(other_errors.items()):
    print(f"  {v:4d}  {k}")
