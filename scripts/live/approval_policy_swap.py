#!/usr/bin/env python3
"""Mount a run's approval-policy document into the SHARED lab controller, the
way the chart does, and take it out again — PLAT-19.2's live rows.

The chart renders the installation's approval-policy document into a ConfigMap
mounted at `/etc/logweir/approval-policy` and points the controller at it with
`LOGWEIR_APPROVAL_POLICY_FILE` (`charts/logweir/templates/approval-policy.yaml`).
The lab's values set no policy, so its controller binds nothing and every
namespace is `legacy-governed-v1`. A row that needs a BOUND namespace needs an
installation admin's change to the shared controller, and this is that change,
made reversibly:

    approval_policy_swap.py on   --owner O --policy FILE --record DIR [--acquire]
    approval_policy_swap.py edit --owner O --policy FILE --record DIR
    approval_policy_swap.py off  --owner O --record DIR [--release]
    approval_policy_swap.py status

`on` records the Deployment it found (`DIR/deploy-baseline.json`, written once
and never overwritten), creates the owner-labelled ConfigMap and adds exactly
one volume, one mount and one env entry. `edit` replaces the ConfigMap's
document and restarts the rollout ONCE, so the controller goes from the old
policy straight to the new one and never runs unbound in between (a row that
measures a policy EDIT must not be passed by the unbound refusal instead).
`off` removes the three entries, waits for the rollout, deletes the ConfigMap,
and exits non-zero unless the containers and volumes are byte-identical to the
recorded baseline.

THE CLUSTER LOCK. Every verb but `status` refuses to run unless
`k8s-lock.sh status` says the lock is held by `--owner`; `--acquire` takes it
first (waiting while another worker holds it) and `--release` gives it back
after a verified restore — never after a failed one, so the next worker finds
the lock held and the evidence in DIR. Every kubectl names `--context
docker-desktop`, and every subprocess has a timeout.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import time

CONTEXT = "docker-desktop"
LAB_NS = "logweir-scram-local"
DEPLOY = "weirkeeper"
MOUNT = "/etc/logweir/approval-policy"
ENV = "LOGWEIR_APPROVAL_POLICY_FILE"
VOLUME = "approval-policy"
LOCK = os.environ.get("LOGWEIR_K8S_LOCK", "/tmp/logweir-roadmap-run/claude/k8s-lock.sh")
BOUND_LINE = "the approval policies this controller enforces"


def run(argv: list[str], *, timeout: int = 120, stdin: str | None = None,
        check: bool = True) -> subprocess.CompletedProcess[str]:
    done = subprocess.run(argv, input=stdin, capture_output=True, text=True, timeout=timeout)
    if check and done.returncode != 0:
        raise SystemExit(f"{' '.join(argv[:6])}… exited {done.returncode}: {done.stderr.strip()[:800]}")
    return done


def k(*args: str, timeout: int = 120, stdin: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["kubectl", "--context", CONTEXT, "-n", LAB_NS, *args], timeout=timeout, stdin=stdin, check=check)


def deployment() -> dict:
    return json.loads(k("get", "deploy", DEPLOY, "-o", "json").stdout)


def configmap_name(owner: str) -> str:
    return f"logweir-approval-policy-{owner}"[:63].rstrip("-")


def lock_holder() -> str:
    out = run([LOCK, "status"], timeout=30).stdout.strip()
    return out.split()[1] if out.startswith("held:") and len(out.split()) > 1 else ""


def require_lock(owner: str) -> None:
    holder = lock_holder()
    if holder != owner:
        raise SystemExit(f"the cluster lock is held by {holder or 'nobody'}, not {owner}: "
                         f"a change to the shared controller needs it (WORKER-RULES)")


def rollout() -> None:
    k("rollout", "status", f"deploy/{DEPLOY}", "--timeout=240s", timeout=260)


def running_pod() -> dict:
    for _ in range(60):
        pods = json.loads(k("get", "pods", "-l", "app.kubernetes.io/component=control-plane",
                            "-o", "json").stdout)["items"]
        live = [p for p in pods if (p.get("status") or {}).get("phase") == "Running"
                and not (p["metadata"].get("deletionTimestamp"))]
        if len(live) == 1:
            return live[0]
        time.sleep(2)
    raise SystemExit("the controller never settled to exactly one Running pod")


def bound_line(pod: str, *, seconds: int = 90) -> dict:
    """The controller's own startup statement of what it binds."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        logs = k("logs", pod, check=False, timeout=60).stdout
        lines = [ln for ln in logs.splitlines() if BOUND_LINE in ln]
        if lines:
            try:
                return json.loads(lines[-1]).get("fields") or {}
            except ValueError:
                return {"raw": lines[-1][:600]}
        time.sleep(3)
    return {}


def facts(record: pathlib.Path, label: str) -> dict:
    pod = running_pod()
    status = (pod.get("status") or {}).get("containerStatuses") or [{}]
    out = {"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "pod": pod["metadata"]["name"],
           "imageID": status[0].get("imageID"), "bound": bound_line(pod["metadata"]["name"])}
    deploy = deployment()
    (record / f"deploy-after-{label}.json").write_text(json.dumps(deploy, indent=1))
    out["generation"] = deploy["metadata"].get("generation")
    return out


def apply_configmap(owner: str, policy: pathlib.Path) -> None:
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": configmap_name(owner), "namespace": LAB_NS,
                         "labels": {"logweir.dev/test-owner": owner}},
            "data": {"approval-policy.yaml": policy.read_text()}}
    existing = k("get", "configmap", configmap_name(owner), "-o", "json", check=False)
    if existing.returncode == 0:
        labels = json.loads(existing.stdout)["metadata"].get("labels") or {}
        if labels.get("logweir.dev/test-owner") != owner:
            raise SystemExit(f"ConfigMap {configmap_name(owner)} exists and is not labelled {owner}")
    k("apply", "-f", "-", stdin=json.dumps(body))


def on(args: argparse.Namespace) -> dict:
    record = pathlib.Path(args.record)
    record.mkdir(parents=True, exist_ok=True)
    deploy = deployment()
    spec = deploy["spec"]["template"]["spec"]
    container = spec["containers"][0]
    if any(e.get("name") == ENV for e in container.get("env") or []):
        raise SystemExit(f"the lab controller already carries {ENV}: someone else's policy is mounted; "
                         "refusing to stack a second one")
    baseline = record / "deploy-baseline.json"
    if not baseline.exists():
        baseline.write_text(json.dumps(deploy, indent=1))
    apply_configmap(args.owner, pathlib.Path(args.policy))
    patch = [
        {"op": "add", "path": "/spec/template/spec/volumes",
         "value": (spec.get("volumes") or []) + [{"name": VOLUME, "configMap": {"name": configmap_name(args.owner)}}]},
        {"op": "add", "path": "/spec/template/spec/containers/0/volumeMounts",
         "value": (container.get("volumeMounts") or []) + [{"name": VOLUME, "mountPath": MOUNT, "readOnly": True}]},
        {"op": "add", "path": "/spec/template/spec/containers/0/env/-",
         "value": {"name": ENV, "value": f"{MOUNT}/approval-policy.yaml"}},
    ]
    k("patch", "deploy", DEPLOY, "--type=json", "-p", json.dumps(patch))
    rollout()
    return facts(record, "on")


def edit(args: argparse.Namespace) -> dict:
    record = pathlib.Path(args.record)
    container = deployment()["spec"]["template"]["spec"]["containers"][0]
    if not any(e.get("name") == ENV for e in container.get("env") or []):
        raise SystemExit("edit needs a mounted policy; run `on` first")
    apply_configmap(args.owner, pathlib.Path(args.policy))
    # ONE rollout from the old document to the new: the controller reads the
    # file at start, so a restart is what makes the edit take effect.
    k("rollout", "restart", f"deploy/{DEPLOY}")
    rollout()
    return facts(record, "edit")


def off(args: argparse.Namespace) -> dict:
    record = pathlib.Path(args.record)
    baseline = json.loads((record / "deploy-baseline.json").read_text())
    deploy = deployment()
    spec = deploy["spec"]["template"]["spec"]
    container = spec["containers"][0]
    env = container.get("env") or []
    patch = []
    at = [i for i, e in enumerate(env) if e.get("name") == ENV]
    for i in reversed(at):
        patch.append({"op": "remove", "path": f"/spec/template/spec/containers/0/env/{i}"})
    base_spec = baseline["spec"]["template"]["spec"]
    for path, now, was in (("/spec/template/spec/containers/0/volumeMounts", container.get("volumeMounts"),
                            base_spec["containers"][0].get("volumeMounts")),
                           ("/spec/template/spec/volumes", spec.get("volumes"), base_spec.get("volumes"))):
        if now is not None and was is None:
            patch.append({"op": "remove", "path": path})
        elif now is not None and now != was:
            patch.append({"op": "replace", "path": path, "value": was})
    if patch:
        k("patch", "deploy", DEPLOY, "--type=json", "-p", json.dumps(patch))
        rollout()
    out = facts(record, "off")
    k("delete", "configmap", configmap_name(args.owner), "--ignore-not-found")
    after = deployment()["spec"]["template"]["spec"]
    out["containersIdentical"] = after["containers"] == base_spec["containers"]
    out["volumesIdentical"] = after.get("volumes") == base_spec.get("volumes")
    out["configMapGone"] = k("get", "configmap", configmap_name(args.owner), check=False).returncode != 0
    out["restored"] = out["containersIdentical"] and out["volumesIdentical"] and out["configMapGone"]
    return out


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("verb", choices=("on", "edit", "off", "status"))
    p.add_argument("--owner")
    p.add_argument("--policy")
    p.add_argument("--record")
    p.add_argument("--acquire", action="store_true")
    p.add_argument("--release", action="store_true")
    args = p.parse_args(argv)
    if args.verb == "status":
        pod = running_pod()
        print(json.dumps({"pod": pod["metadata"]["name"], "bound": bound_line(pod["metadata"]["name"], seconds=5),
                          "lock": lock_holder()}, indent=1))
        return 0
    if not args.owner or not args.record or (args.verb != "off" and not args.policy):
        p.error("--owner and --record are required, and --policy for on/edit")
    if args.acquire:
        run([LOCK, "acquire", args.owner, "240"], timeout=240 * 60 + 120)
    require_lock(args.owner)
    result = {"on": on, "edit": edit, "off": off}[args.verb](args)
    result["verb"] = args.verb
    record = pathlib.Path(args.record)
    with (record / "swaps.jsonl").open("a") as log:
        log.write(json.dumps(result) + "\n")
    print(json.dumps(result, indent=1))
    if args.verb == "off":
        if not result["restored"]:
            print("the lab controller is NOT back at its baseline; the lock stays held", file=sys.stderr)
            return 1
        if args.release:
            run([LOCK, "release", args.owner], timeout=60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
