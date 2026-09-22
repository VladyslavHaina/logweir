"""The impure half of the journey runner: kubectl, subprocesses, owned
namespaces and the shared lab's read-only fixture.

Every rule WORKER-RULES.md sets for live work is enforced here in code:

- every kubectl call carries `--context docker-desktop`, and nothing here can
  name another context;
- every subprocess has a timeout (`run`), and a return code is read from the
  completed process, never through a pipe;
- a namespace is created only with the run's owner label, and is deleted only
  after that label AND the UID recorded at creation are read back
  (`delete_owned_namespace`);
- the shared `logweir-scram-local` release is only ever READ: Secrets are
  copied out of it as opaque base64 and never printed, and nothing here writes
  to it.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess
import time
from typing import Any, Callable

from core import Needles, redact

CONTEXT = "docker-desktop"
K = ["kubectl", "--context", CONTEXT]
FIXTURE_NS = "logweir-scram-local"
OWNER_LABEL = "logweir.dev/test-owner"
# The Secrets a journey namespace copies. Their values are also loaded as
# sweep needles (`load_needles`), so a leak of any of them into an artifact is
# found by exact match and not only by pattern.
FIXTURE_SECRETS = ("source-scram", "target-scram", "logweir-s3", "logweir-signing-key", "minio-root")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Lab:
    """One run's handle on the cluster: its log, its needles, its namespaces."""

    def __init__(self, out: pathlib.Path, owner: str, needles: Needles, root: pathlib.Path):
        self.out = out
        self.owner = owner
        self.needles = needles
        self.root = root
        self.commands: list[dict[str, Any]] = []
        self.namespaces: dict[str, str] = {}  # name -> uid recorded at creation

    # -- plumbing -------------------------------------------------------------

    def log(self, message: str) -> None:
        line = f"{now()} {redact(message, self.needles)}"
        print(line, flush=True)
        with (self.out / "runner.log").open("a") as fh:
            fh.write(line + "\n")

    def run(self, args: list[str], *, data: str | None = None, check: bool = True,
            timeout: int = 180, env: dict[str, str] | None = None,
            cwd: pathlib.Path | None = None) -> subprocess.CompletedProcess:
        started = time.time()
        try:
            result = subprocess.run(args, input=data, text=True, capture_output=True,
                                    timeout=timeout, env=env, cwd=cwd or self.root)
        except subprocess.TimeoutExpired as exc:
            self.commands.append({"at": now(), "argv": [redact(a, self.needles) for a in args[:10]],
                                  "rc": "timeout", "seconds": timeout})
            raise RuntimeError(f"timed out after {timeout}s: {args[:6]}") from exc
        self.commands.append({"at": now(), "argv": [redact(a, self.needles) for a in args[:10]],
                              "rc": result.returncode, "seconds": round(time.time() - started, 2)})
        if check and result.returncode != 0:
            raise RuntimeError(
                f"rc={result.returncode}: {[redact(a, self.needles) for a in args[:8]]}\n"
                f"{redact(result.stdout[-2000:], self.needles)}\n"
                f"{redact(result.stderr[-2000:], self.needles)}")
        return result

    def kubectl(self, *args: str, ns: str | None = None, data: str | None = None,
                check: bool = True, timeout: int = 180) -> subprocess.CompletedProcess:
        prefix = K + (["-n", ns] if ns else [])
        return self.run(prefix + list(args), data=data, check=check, timeout=timeout)

    def get(self, kind: str, name: str, ns: str | None = None) -> dict[str, Any]:
        return json.loads(self.kubectl("get", kind, name, "-o", "json", ns=ns).stdout)

    def get_opt(self, kind: str, name: str, ns: str | None = None) -> dict[str, Any] | None:
        done = self.kubectl("get", kind, name, "-o", "json", ns=ns, check=False)
        return json.loads(done.stdout) if done.returncode == 0 else None

    def items(self, kind: str, ns: str, selector: str | None = None) -> list[dict[str, Any]]:
        args = ["get", kind, "-o", "json"] + (["-l", selector] if selector else [])
        return json.loads(self.kubectl(*args, ns=ns).stdout)["items"]

    def create(self, obj: dict[str, Any]) -> dict[str, Any]:
        """`create`, never `apply`: apply stamps a last-applied annotation that
        carries a Secret's whole `data` into the object's metadata."""
        return json.loads(self.kubectl("create", "-f", "-", "-o", "json", data=json.dumps(obj)).stdout)

    def wait_for(self, kind: str, name: str, ns: str, predicate: Callable[[dict[str, Any]], bool],
                 *, seconds: int, what: str,
                 every: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        deadline = time.time() + seconds
        last: dict[str, Any] | None = None
        while time.time() < deadline:
            last = self.get_opt(kind, name, ns)
            if last is not None:
                if every is not None:
                    every(last)
                if predicate(last):
                    return last
            time.sleep(2)
        self.write(f"timeout-{kind}-{name}.json", last or {})
        raise RuntimeError(f"timeout after {seconds}s waiting for {kind}/{name}: {what}")

    def write(self, rel: str, body: Any) -> str:
        path = self.out / rel
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        text = body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True, default=str)
        path.write_text(redact(text, self.needles))
        return str(path)

    # -- owned namespaces -----------------------------------------------------

    def labels(self) -> dict[str, str]:
        return {OWNER_LABEL: self.owner}

    def meta(self, name: str, ns: str | None) -> dict[str, Any]:
        meta: dict[str, Any] = {"name": name, "labels": self.labels()}
        if ns:
            meta["namespace"] = ns
        return meta

    def create_namespace(self, name: str) -> str:
        assert_owned_name(name)
        if self.get_opt("namespace", name) is not None:
            raise RuntimeError(f"refusing to reuse existing namespace {name}")
        created = self.create({"apiVersion": "v1", "kind": "Namespace", "metadata": self.meta(name, None)})
        self.namespaces[name] = created["metadata"]["uid"]
        self.log(f"created namespace {name} uid {created['metadata']['uid']}")
        return created["metadata"]["uid"]

    def delete_owned_namespace(self, name: str) -> dict[str, Any]:
        assert_owned_name(name)
        live = self.get_opt("namespace", name)
        if live is None:
            return {"namespace": name, "deleted": False, "why": "already absent"}
        label = (live["metadata"].get("labels") or {}).get(OWNER_LABEL)
        uid = live["metadata"]["uid"]
        if label != self.owner or uid != self.namespaces.get(name):
            return {"namespace": name, "deleted": False,
                    "why": f"refused: label {label!r} / uid {uid} is not this run's"}
        self.kubectl("delete", "namespace", name, "--wait=true", timeout=300, check=False)
        return {"namespace": name, "uid": uid, "ownerLabel": label, "deleted": True,
                "absentAfter": self.get_opt("namespace", name) is None}

    def copy_secret(self, name: str, ns: str) -> None:
        source = self.get("secret", name, FIXTURE_NS)
        self.create({"apiVersion": "v1", "kind": "Secret", "metadata": self.meta(name, ns),
                     "type": source.get("type", "Opaque"), "data": source["data"]})

    def load_needles(self, extra_files: list[pathlib.Path]) -> int:
        """The shared lab's Secret values and the given private-key files, read
        into memory as exact-match sweep needles. Nothing is written."""
        import base64

        before = len(self.needles)
        for name in FIXTURE_SECRETS:
            obj = self.get_opt("secret", name, FIXTURE_NS)
            for value in ((obj or {}).get("data") or {}).values():
                try:
                    self.needles.add(base64.b64decode(value).decode("utf-8", errors="replace"))
                except ValueError:
                    continue
                self.needles.add(value)  # the base64 form too
        for path in extra_files:
            if path.is_file():
                self.needles.add(path.read_text())
        return len(self.needles) - before


def assert_owned_name(name: str) -> None:
    if not name.startswith("lw-plat20-"):
        raise RuntimeError(f"this runner only ever touches lw-plat20-* namespaces, not {name}")
    if name in {"default", FIXTURE_NS} or name.startswith("kube-"):
        raise RuntimeError(f"refusing a system or shared namespace: {name}")


def condition(obj: dict[str, Any], kind: str) -> dict[str, Any]:
    for c in (obj.get("status") or {}).get("conditions") or []:
        if c.get("type") == kind:
            return c
    return {}
