#!/usr/bin/env python3
"""PLAT-20.1 — one command for the cross-layer regression journey set.

    export LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3
    $LOGWEIR_PYTHON e2e/journeys/run.py plan                  # what would run; touches nothing
    $LOGWEIR_PYTHON e2e/journeys/run.py run                   # every un-gated journey, live
    $LOGWEIR_PYTHON e2e/journeys/run.py run --journeys overlap,cr-loss
    $LOGWEIR_PYTHON e2e/journeys/run.py run --open lab-refresh-8   # after the refresh lands
    $LOGWEIR_PYTHON e2e/journeys/run.py summarise --out <run dir>  # re-judge a finished run offline

It composes the existing live harnesses (each in its own owned namespace,
bucket or archive prefix, with its output directed into this run's tree),
runs the journeys no harness proves (`native.py`, `console.mjs`), and writes
ONE machine-readable `summary.json`. It exits 0 only when `summary.ok` is
true: no FAIL, at least one PASS, a clean credential sweep over every
artifact, and a sweep that proved it can fail. See README.md.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import shutil
import sys
import time
import traceback
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import core  # noqa: E402
import suites as catalogue  # noqa: E402
from lab import FIXTURE_NS, Lab  # noqa: E402

ROOT = HERE.parents[1]
DEFAULT_ARTIFACTS = pathlib.Path("/tmp/logweir-roadmap-run/claude/artifacts/plat20-1")
DEFAULT_PRIVATE = pathlib.Path("/tmp/logweir-roadmap-run/claude/private")
KINDS = ("backups,restores,approvals,backupschedules,kafkaclusters,backupdestinations,topicdiscoveries,"
         "preflights,recoverycatalogs,jobs,pods,configmaps,events")


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dt%H%Mz")


def lab_key_dir() -> pathlib.Path:
    override = os.environ.get("LOGWEIR_SCRAM_OUT")
    if override:
        return pathlib.Path(override)
    durable = pathlib.Path.home() / ".logweir-lab" / "scram-e2e"
    return durable if durable.is_dir() else pathlib.Path("/tmp/logweir-scram-e2e")


def select(names: str | None) -> list[core.Journey]:
    if not names:
        return list(catalogue.JOURNEYS)
    wanted = [n.strip() for n in names.split(",") if n.strip()]
    known = {j.id: j for j in catalogue.JOURNEYS}
    unknown = [n for n in wanted if n not in known]
    if unknown:
        raise SystemExit(f"unknown journey(s) {unknown}; known: {sorted(known)}")
    return [known[n] for n in wanted]


def plan(journeys: list[core.Journey], opened: set[str]) -> dict[str, Any]:
    return {
        "journeys": [{"id": j.id, "tests": list(j.tests), "requires": j.requires,
                      "gated": j.requires is not None and j.requires not in opened,
                      "rows": [f"{r.suite}: {r.name}  [{', '.join(r.verifies)}]  {r.cite}" for r in j.rows]}
                     for j in journeys],
        "suites": {s: list(catalogue.SUITES[s].phases) + [f"(finally) {p}" for p in catalogue.SUITES[s].cleanup]
                   for s in catalogue.suites_for(journeys, opened)},
    }


class Runner:
    def __init__(self, args: argparse.Namespace):
        self.stamp = args.stamp or utc_stamp()
        self.out = pathlib.Path(args.out or DEFAULT_ARTIFACTS / self.stamp)
        self.private = pathlib.Path(args.private or DEFAULT_PRIVATE / f"plat20-1-{self.stamp}")
        for d in (self.out, self.private):
            d.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.needles = core.Needles()
        self.lab = Lab(self.out, "plat20-1", self.needles, ROOT)
        self.opened = set(args.open or [])
        self.journeys = select(args.journeys)
        self.only_suites = set((args.only_suites or "").split(",")) - {""}
        python = os.environ.get("LOGWEIR_PYTHON") or sys.executable
        self.ctx = catalogue.Ctx(
            stamp=self.stamp, run=self.out, private=self.private, root=ROOT, python=python,
            api_bin=str(pathlib.Path(args.api_bin or ROOT / "target/debug/logweir-api").resolve()),
            cli=str(pathlib.Path(args.cli or ROOT / "target/debug/logweir").resolve()),
            node_path=os.environ.get("NODE_PATH") or "/opt/homebrew/lib/node_modules",
            owner="plat20-1")
        self.results: dict[str, core.SuiteResult] = {}
        self.native = None

    # -- preflight -----------------------------------------------------------

    def preflight(self) -> dict[str, Any]:
        problems = core.catalogue_violations(catalogue.JOURNEYS)
        if problems:
            raise SystemExit("the journey catalogue breaks its own rules:\n  " + "\n  ".join(problems))
        core.summary_selftest()
        for binary in (self.ctx.api_bin, self.ctx.cli):
            if not pathlib.Path(binary).is_file():
                raise SystemExit(f"missing binary {binary}: build it first (cargo build -p logweir -p logweir-api)")
        keys = lab_key_dir()
        approver = keys / "approver.pem"
        if not approver.is_file():
            raise SystemExit(f"the lab approver private key is absent: {approver}")
        try:
            loaded = self.lab.load_needles([approver, keys / "signing.pem"])
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from None
        if loaded == 0:
            raise SystemExit("no credential value was loaded: the sweep would search for nothing")
        return {"context": "docker-desktop", "stamp": self.stamp, "revision": self._git("rev-parse", "HEAD"),
                "controller": self._controller(), "needlesLoaded": loaded, "approverKey": str(approver)}

    def _git(self, *args: str) -> str:
        return self.lab.run(["git", *args], check=False, timeout=30).stdout.strip()

    def _controller(self) -> dict[str, Any]:
        pods = self.lab.items("pods", FIXTURE_NS, "app.kubernetes.io/component=control-plane")
        deploy = self.lab.get("deployment", "weirkeeper", FIXTURE_NS)
        env = {e["name"]: e.get("value") for e in deploy["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        facts: dict[str, Any] = {"pods": [p["metadata"]["name"] for p in pods],
                                 "imageIDs": [s.get("imageID") for p in pods
                                              for s in (p.get("status") or {}).get("containerStatuses") or []],
                                 "runnerImage": env.get("LOGWEIR_RUNNER_IMAGE")}
        for label, image in (("controllerRevision", "weirkeeper:scram-reviewed"),
                             ("runnerRevision", env.get("LOGWEIR_RUNNER_IMAGE") or "")):
            done = self.lab.run(["docker", "image", "inspect", image, "--format",
                                 '{{index .Config.Labels "org.opencontainers.image.revision"}}'],
                                check=False, timeout=60)
            facts[label] = done.stdout.strip() if done.returncode == 0 else None
        return facts

    # -- suites ---------------------------------------------------------------

    def run_suite(self, suite_id: str) -> None:
        suite = catalogue.SUITES[suite_id]
        out = self.ctx.out(suite_id)
        out.mkdir(mode=0o700, parents=True, exist_ok=True)
        started = time.time()
        rcs: dict[str, int] = {}
        error = ""
        self.lab.log(f"==== suite {suite_id}: {', '.join(suite.phases)} (+ finally {', '.join(suite.cleanup) or '-'})")
        try:
            for phase in suite.phases:
                rcs[phase] = self._phase(suite, phase)
                if phase in suite.infra and rcs[phase] not in suite.accept_rcs:
                    error = f"{phase} exited {rcs[phase]}"
                    break
        except Exception as exc:  # the suite is FAILED, recorded, and the run continues
            error = f"{type(exc).__name__}: {exc}"
            self.lab.write(f"suites/{suite_id}/runner-error.txt", traceback.format_exc())
        rows = self._rows(suite, rcs)
        failing = [k for k, v in rows.items() if v != core.PASS] or ([] if not error else ["<suite>"])
        if failing or error:
            self._snapshot(suite)
        for phase in suite.cleanup:
            try:
                rcs[phase] = self._phase(suite, phase)
                if rcs[phase] != 0:
                    error = (error + "; " if error else "") + f"{phase} exited {rcs[phase]}"
            except Exception as exc:
                error = (error + "; " if error else "") + f"{phase}: {exc}"
        rows = self._rows(suite, rcs)
        rc = 0 if not error else 1
        notes = [f"{p} exited {c} (accepted: {suite.why_rcs})" for p, c in rcs.items()
                 if c != 0 and c in suite.accept_rcs]
        if notes and not error:
            error = "NOTE " + "; ".join(notes)
        self.results[suite_id] = core.SuiteResult(suite_id, rc, rows, error, round(time.time() - started, 1),
                                                  str(out))
        self.lab.write(f"suites/{suite_id}/phases.json", {"rcs": rcs, "error": error, "rows": rows})
        self.lab.log(f"==== suite {suite_id}: rc={rc} rows={rows} {error}")

    def _rows(self, suite: catalogue.Suite, rcs: dict[str, int]) -> dict[str, str]:
        try:
            return suite.adapter(self.ctx, rcs)
        except Exception as exc:
            self.lab.log(f"adapter for {suite.id} raised {exc}")
            return {}

    def _phase(self, suite: catalogue.Suite, phase: str) -> int:
        if suite.runner == "native":
            return self._native_phase(phase)
        env = dict(os.environ)
        env.update(suite.env(self.ctx))
        env["LOGWEIR_PYTHON"] = self.ctx.python
        env.setdefault("LOGWEIR_SCRAM_OUT", str(lab_key_dir()))
        if suite.runner == "python":
            argv = [self.ctx.python, str(ROOT / suite.script), phase]
        elif suite.runner == "node":
            argv = ["node", str(ROOT / suite.script)]
        else:  # console
            topic = self.native.topic if self.native else ""
            env.update({"JOURNEY_OUT": str(self.ctx.out("console")),
                        "JOURNEY_PRIVATE": str(self.private / "console"),
                        "JOURNEY_OWNER": self.ctx.owner, "JOURNEY_NS_A": f"lw-plat20-ca-{self.stamp}",
                        "JOURNEY_NS_B": f"lw-plat20-cb-{self.stamp}", "JOURNEY_TOPIC": topic,
                        "JOURNEY_PREFIX": self.ctx.prefix, "UI_E2E_API_BIN": self.ctx.api_bin,
                        "NODE_PATH": self.ctx.node_path})
            argv = ["node", str(ROOT / suite.script)]
        log = self.ctx.out(suite.id) / f"{phase}.log"
        self.lab.log(f"  {suite.id} {phase}")
        try:
            done = self.lab.run(argv, check=False, timeout=suite.timeout, env=env)
            text, rc = done.stdout + done.stderr, done.returncode
        except RuntimeError as exc:  # the timeout
            text, rc = str(exc), 124
        log.write_text(core.redact(text, self.needles))
        return rc

    def _native_phase(self, phase: str) -> int:
        import native

        if self.native is None:
            self.native = native.Native(self.lab, self.stamp, api_bin=pathlib.Path(self.ctx.api_bin),
                                        cli=pathlib.Path(self.ctx.cli), python=self.ctx.python,
                                        approver_key=lab_key_dir() / "approver.pem",
                                        private=self.private / "native")
            self.native.private.mkdir(mode=0o700, parents=True, exist_ok=True)
        n = self.native
        if phase == "setup":
            n.setup()
            return 0
        if phase == "journeys":
            wanted = {r.name for j in self.journeys if j.requires is None or j.requires in self.opened
                      for r in j.rows if r.suite == "native"}
            # Order matters: the offline row's listing control reads the
            # restore journey's objects.
            steps = [("old-point", n.old_point_restore, {"restore-cr-carries-the-selected-older-point",
                                                        "api-restore-replay-is-one-object",
                                                        "older-point-restore-restores-exactly-its-records",
                                                        "restore-evidence-verifies-with-two-verifiers",
                                                        "restore-progress-is-durable-and-projected",
                                                        "backup-from-an-offline-source-fails-and-writes-nothing"}),
                     ("source-offline", n.source_offline, {"backup-from-an-offline-source-fails-and-writes-nothing"})]
            for name, step, rows in steps:
                if not wanted & rows:
                    continue
                try:
                    step()
                except Exception as exc:
                    n.details[f"journey:{name}"] = {"error": f"{type(exc).__name__}: {exc}"}
                    self.lab.write(f"suites/native/{name}-error.txt", traceback.format_exc())
                    self.lab.log(f"native journey {name} raised {exc}")
                finally:
                    n.stop_api()
            self.lab.write("suites/native/result.json", n.result())
            return 0
        if phase == "cleanup":
            proof = n.cleanup([])
            self.lab.write("suites/native/cleanup.json", proof)
            self.lab.write("suites/native/result.json", n.result())
            return 0 if proof.get("namespace", {}).get("deleted") or \
                proof.get("namespace", {}).get("why") == "already absent" else 1
        raise RuntimeError(f"unknown native phase {phase}")

    def _snapshot(self, suite: catalogue.Suite) -> None:
        """REDACTED DIAGNOSTICS, taken before the suite's cleanup deletes the
        evidence. Secrets are never listed; everything else passes `redact`."""
        snap: dict[str, Any] = {}
        for ns in suite.namespaces(self.ctx):
            done = self.lab.kubectl("get", KINDS, "-o", "json", ns=ns, check=False, timeout=120)
            snap[ns] = json.loads(done.stdout) if done.returncode == 0 else {"error": done.stderr[-500:]}
        self.lab.write(f"suites/{suite.id}/diagnostics-snapshot.json", snap)
        logs = self.lab.kubectl("logs", "deploy/weirkeeper", "--since=30m", "--tail=4000", ns=FIXTURE_NS,
                                check=False, timeout=120)
        self.lab.write(f"suites/{suite.id}/diagnostics-controller.log", logs.stdout)

    # -- the whole run --------------------------------------------------------

    def execute(self) -> dict[str, Any]:
        facts = self.preflight()
        self.lab.write("run.json", {**facts, "journeys": [j.id for j in self.journeys],
                                    "opened": sorted(self.opened)})
        order = catalogue.suites_for(self.journeys, self.opened)
        if self.only_suites:
            # A debugging filter: the journeys whose rows live in a skipped
            # suite are judged FAIL ("never ran"), so a filtered run can
            # never read as green for them.
            order = [s for s in order if s in self.only_suites]
        self.lab.log(f"suites to run: {order}")
        try:
            # native's setup and journeys first, its cleanup LAST (see suites_for)
            for suite_id in order:
                if suite_id == "native":
                    self._native_head()
                else:
                    self.run_suite(suite_id)
        finally:
            if "native" in order:
                self._native_tail()
        return self.finish(facts)

    def _native_head(self) -> None:
        suite = catalogue.SUITES["native"]
        self.ctx.out("native").mkdir(mode=0o700, parents=True, exist_ok=True)
        self._native_started = time.time()
        self._native_rcs: dict[str, int] = {}
        self._native_error = ""
        for phase in suite.phases:
            try:
                self._native_rcs[phase] = self._native_phase(phase)
            except Exception as exc:
                self._native_rcs[phase] = 1
                self._native_error = f"{phase}: {type(exc).__name__}: {exc}"
                self.lab.write(f"suites/native/{phase}-error.txt", traceback.format_exc())
                break
        if self._native_error or any(v != core.PASS for v in (self.native.rows if self.native else {}).values()):
            self._snapshot(suite)

    def _native_tail(self) -> None:
        rcs = getattr(self, "_native_rcs", {})
        error = getattr(self, "_native_error", "")
        try:
            rcs["cleanup"] = self._native_phase("cleanup")
            if rcs["cleanup"]:
                error = (error + "; " if error else "") + "cleanup refused or failed"
        except Exception as exc:
            error = (error + "; " if error else "") + f"cleanup: {exc}"
        rows = self.native.rows if self.native else {}
        self.results["native"] = core.SuiteResult("native", 0 if not error else 1, dict(rows), error,
                                                  round(time.time() - getattr(self, "_native_started", time.time()), 1),
                                                  str(self.ctx.out("native")))
        self.lab.write("suites/native/phases.json", {"rcs": rcs, "error": error, "rows": rows})

    def finish(self, facts: dict[str, Any]) -> dict[str, Any]:
        self.lab.write("suite-results.json", {k: vars(v) for k, v in self.results.items()})
        self.lab.write("commands.json", self.lab.commands)
        summary = judge_and_write(self.out, self.journeys, self.results, self.opened, self.needles,
                                  self.private, facts)
        shutil.rmtree(self.private, ignore_errors=True)
        return summary


SECRET_STORE = re.compile(r"(?i)(password|credential|secret)[^/]*\.json$")


def private_needles(private: pathlib.Path, needles: core.Needles) -> int:
    """Every SECRET in the run's private tree becomes a sweep needle before
    the tree is deleted: whatever was private must not be in the artifacts.

    Only the harnesses' secret stores count — plat07's `passwords.json` and
    d2's `credentials.json` (every value in both is a credential), and every
    private key (`*.key`, or any file carrying a PEM private key). A state
    file is NOT one: its strings are namespace names and stamps, and the
    first cut of this function, which took every string in the tree, reported
    249 "hits" on ordinary artifacts in a live trial (2026-09-22, run t5)."""
    before = len(needles)
    if not private.is_dir():
        return 0
    for path in private.rglob("*"):
        if not path.is_file() or path.stat().st_size > 1_000_000:
            continue
        text = path.read_bytes().decode("utf-8", errors="replace")
        if SECRET_STORE.search(path.name):
            try:
                doc = json.loads(text)
            except json.JSONDecodeError:
                continue
            for value in (doc.values() if isinstance(doc, dict) else []):
                if isinstance(value, str):
                    needles.add(value)
        elif path.suffix == ".key" or "PRIVATE KEY-----" in text:
            needles.add(text)
    return len(needles) - before


def judge_and_write(out: pathlib.Path, journeys: list[core.Journey], results: dict[str, core.SuiteResult],
                    opened: set[str], needles: core.Needles, private: pathlib.Path,
                    facts: dict[str, Any]) -> dict[str, Any]:
    added = private_needles(private, needles)
    pre = core.summarise(journeys, results, opened, {"selfTest": {"killed": True}, "hits": []})
    for j in pre["journeys"]:
        if j["verdict"] == core.FAIL:
            write_diagnostics(out, j, results, needles)
    selftest = core.sweep_selftest(out, needles)
    swept = core.sweep(out, needles)
    swept["selfTest"] = selftest
    swept["privateNeedles"] = added
    summary = core.summarise(journeys, results, opened, swept)
    summary["run"] = facts
    summary["finishedAt"] = dt.datetime.now(dt.timezone.utc).isoformat()
    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    (out / "summary.txt").write_text(render(summary) + "\n")
    print(render(summary))
    return summary


def write_diagnostics(out: pathlib.Path, journey: dict[str, Any], results: dict[str, core.SuiteResult],
                      needles: core.Needles) -> None:
    """One directory per FAILED journey: the failing rows with the owning
    harness's own words for them, the tail of every phase log, and pointers
    to the pre-cleanup snapshots. All of it redacted."""
    d = out / "diagnostics" / journey["id"]
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    doc: dict[str, Any] = {"journey": journey["id"], "reason": journey["reason"], "rows": []}
    for r in journey["rows"]:
        if r["verdict"] == core.PASS:
            continue
        res = results.get(r["suite"])
        doc["rows"].append({**r, "suiteError": res.error if res else "never ran",
                            "harnessDetail": harness_detail(out, r["suite"], r["row"])})
        suite_dir = out / "suites" / r["suite"]
        for log in sorted(suite_dir.glob("*.log")) if suite_dir.is_dir() else []:
            tail = "\n".join(log.read_text(errors="replace").splitlines()[-150:])
            (d / f"{r['suite']}-{log.name}").write_text(core.redact(tail, needles))
        for snap in ("diagnostics-snapshot.json", "diagnostics-controller.log", "runner-error.txt"):
            if (suite_dir / snap).is_file():
                doc.setdefault("snapshots", []).append(str((suite_dir / snap).relative_to(out)))
    (d / "diagnostics.json").write_text(core.redact(json.dumps(doc, indent=2, default=str), needles))


def harness_detail(out: pathlib.Path, suite: str, row: str) -> Any:
    """What the owning harness itself said about a row, where it says it."""
    base = out / "suites" / suite
    try:
        if suite == "d3":
            return (json.loads((base / "state.json").read_text())["scenarios"].get(row) or {}).get("detail")
        if suite == "d1":
            s = json.loads((base / "results.json").read_text())["scenarios"].get(row) or {}
            return {k: s.get(k) for k in ("status", "failure", "reason", "unmet")}
        if suite == "d2":
            s = json.loads((base / "results.json").read_text())["scenarios"].get(row) or {}
            return {k: s.get(k) for k in ("outcome", "reason")}
        if suite in ("native", "console"):
            return json.loads((base / "result.json").read_text())["details"].get(row)
        for name in ("result.json", "live.json"):
            for f in base.rglob(name):
                return {"failure": json.loads(f.read_text()).get("failure")}
        for f in base.rglob("live-result-*.json"):
            return {"failure": json.loads(f.read_text()).get("failure")}
    except (OSError, ValueError, KeyError):
        return None
    return None


def render(summary: dict[str, Any]) -> str:
    lines = [f"PLAT-20.1 journeys — {summary['counts']}  ok={summary['ok']}", ""]
    for j in summary["journeys"]:
        lines.append(f"{j['verdict']:<8} {j['id']:<38} {j['reason'][:160]}")
    sweep = summary.get("credentialSweep") or {}
    lines += ["", f"credential sweep: {sweep.get('filesScanned')} files, {sweep.get('patterns')} patterns, "
                  f"{sweep.get('needles')} exact values, {len(sweep.get('hits') or [])} hits; self-test "
                  f"{(sweep.get('selfTest') or {}).get('killed')}"]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "run"):
        p = sub.add_parser(name)
        p.add_argument("--journeys", help="comma-separated journey ids (default: all)")
        p.add_argument("--open", action="append", choices=sorted(core.REQUIREMENTS),
                       help="treat a `requires:` gate as satisfied (run it; it can then FAIL)")
    run = sub.choices["run"]
    run.add_argument("--stamp")
    run.add_argument("--out")
    run.add_argument("--private")
    run.add_argument("--api-bin")
    run.add_argument("--cli")
    run.add_argument("--only-suites", help="debugging: run only these suites (others' journeys FAIL)")
    s = sub.add_parser("summarise")
    s.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.cmd == "plan":
        print(json.dumps(plan(select(args.journeys), set(args.open or [])), indent=2))
        return 0
    if args.cmd == "summarise":
        out = pathlib.Path(args.out)
        stored = json.loads((out / "suite-results.json").read_text())
        results = {k: core.SuiteResult(**v) for k, v in stored.items()}
        run_doc = json.loads((out / "run.json").read_text())
        journeys = select(",".join(run_doc["journeys"]))
        needles = core.Needles()
        Lab(out, "plat20-1", needles, ROOT).load_needles([lab_key_dir() / "approver.pem"])
        summary = judge_and_write(out, journeys, results, set(run_doc.get("opened") or []), needles,
                                  pathlib.Path("/nonexistent"), run_doc)
        return 0 if summary["ok"] else 1
    summary = Runner(args).execute()
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
